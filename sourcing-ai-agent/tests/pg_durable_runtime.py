from __future__ import annotations

import hashlib
import os
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Iterator
from unittest import mock

from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)

try:
    import psycopg
except Exception as exc:  # pragma: no cover - depends on optional local test env.
    psycopg = None
    _PSYCOPG_IMPORT_ERROR = exc
else:
    _PSYCOPG_IMPORT_ERROR = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _pg_required() -> bool:
    return os.getenv("SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS") == "1" or os.getenv(
        "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES"
    ) == "1"


def _resolve_test_postgres_dsn() -> tuple[str, object | None]:
    dsn = str(resolve_control_plane_postgres_dsn(_repo_root()) or "").strip()
    if dsn:
        return dsn, None
    if os.getenv("SOURCING_RUN_TESTCONTAINERS") == "1" or os.getenv("SOURCING_REQUIRE_TESTCONTAINERS") == "1":
        from tests.testcontainers_helpers import (
            libpq_dsn_from_container,
            require_testcontainers_pg,
            start_postgres_container,
        )

        require_testcontainers_pg()
        container = start_postgres_container()
        return libpq_dsn_from_container(container), container
    return "", None


def _close_store_connection(test_case: unittest.TestCase) -> None:
    store = getattr(test_case, "store", None)
    # B4.3f: the SQLite shadow is gone; store.close() disposes the PG adapter pool.
    close = getattr(store, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


class PGDurableRuntimeFixture:
    """Per-test PG-only durable runtime schema.

    This fixture is intentionally not a SQLite fallback. Tests that exercise
    workflow_events/workflow_current_state/workflow_commands/runtime_outbox
    must run against the same PG-only control-plane contract as service
    runtime. If no local PG is configured, tests skip by default and fail when
    SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 is set.
    """

    def __init__(self, *, runtime_dir: str | Path, schema_label: str) -> None:
        self.runtime_dir = Path(runtime_dir).expanduser().resolve()
        self.schema_label = str(schema_label or "durable_runtime").strip() or "durable_runtime"
        self.dsn = ""
        self.schema = ""
        self.container: object | None = None
        self._env_patch: mock._patch_dict | None = None

    def __enter__(self) -> "PGDurableRuntimeFixture":
        if psycopg is None:
            message = f"PG durable runtime tests require psycopg: {_PSYCOPG_IMPORT_ERROR}"
            if _pg_required():
                raise AssertionError(message)
            raise unittest.SkipTest(message)
        dsn, container = _resolve_test_postgres_dsn()
        if not dsn:
            message = "PG durable runtime tests require a local Postgres DSN or SOURCING_RUN_TESTCONTAINERS=1"
            if _pg_required():
                raise AssertionError(message)
            raise unittest.SkipTest(message)
        self.dsn = normalize_control_plane_postgres_connect_dsn(dsn)
        self.container = container
        digest = hashlib.sha1(f"{self.runtime_dir}:{self.schema_label}:{time.time_ns()}".encode("utf-8")).hexdigest()[:10]
        label = normalize_control_plane_postgres_schema(self.schema_label)[:32].strip("_") or "durable_runtime"
        self.schema = normalize_control_plane_postgres_schema(f"sourcing_test_{label}_{digest}")
        quoted_schema = quote_control_plane_postgres_identifier(self.schema)
        try:
            with psycopg.connect(self.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
                    cursor.execute(f"CREATE SCHEMA {quoted_schema}")
        except Exception as exc:
            self._stop_container()
            message = f"PG durable runtime tests could not prepare schema {self.schema}: {type(exc).__name__}: {exc}"
            if _pg_required():
                raise AssertionError(message) from exc
            raise unittest.SkipTest(message) from exc
        env = {
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN": self.dsn,
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": self.schema,
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
            "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
            "SOURCING_RUNTIME_ENVIRONMENT": "test",
            "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
        }
        self._env_patch = mock.patch.dict(os.environ, env, clear=False)
        self._env_patch.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self._env_patch is not None:
            self._env_patch.stop()
            self._env_patch = None
        if self.dsn and self.schema and psycopg is not None:
            try:
                with psycopg.connect(self.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(self.schema)} CASCADE"
                        )
            except Exception:
                pass
        self._stop_container()
        return False

    def _stop_container(self) -> None:
        stop = getattr(self.container, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                pass
        self.container = None


@contextmanager
def pg_durable_runtime_env(*, runtime_dir: str | Path, schema_label: str) -> Iterator[PGDurableRuntimeFixture]:
    with PGDurableRuntimeFixture(runtime_dir=runtime_dir, schema_label=schema_label) as fixture:
        adapter = LiveControlPlanePostgresAdapter(
            runtime_dir=fixture.runtime_dir,
            dsn=fixture.dsn,
            mode="postgres_only",
        )
        try:
            adapter.ensure_bootstrapped()
        finally:
            adapter.close()
        yield fixture


class PGDurableRuntimeTestMixin:
    _pg_durable_runtime_fixture: PGDurableRuntimeFixture | None
    _pg_durable_runtime_context: object | None

    def _start_pg_durable_runtime(self, *, runtime_dir: str | Path, schema_label: str = "") -> PGDurableRuntimeFixture:
        context = pg_durable_runtime_env(
            runtime_dir=runtime_dir,
            schema_label=schema_label or getattr(self, "_testMethodName", "durable_runtime_test"),
        )
        fixture = context.__enter__()
        self._pg_durable_runtime_context = context
        self._pg_durable_runtime_fixture = fixture
        return fixture

    def _stop_pg_durable_runtime(self) -> None:
        _close_store_connection(self)  # type: ignore[arg-type]
        context = getattr(self, "_pg_durable_runtime_context", None)
        if context is not None:
            context.__exit__(None, None, None)
        self._pg_durable_runtime_context = None
        self._pg_durable_runtime_fixture = None
