"""Shared PG-backed ControlPlaneStore provisioning for store-level tests.

This module layers on :class:`tests.pg_durable_runtime.PGDurableRuntimeFixture`
(which owns DSN resolution — including the docker local-postgres env file —
per-schema create/drop, and control-plane env patching) without duplicating
any of that logic. It exists so tests that previously did
``ControlPlaneStore(tmpdir / "x.db")`` against pure SQLite can run against the
same PG-only control-plane contract as service runtime.

Two granularities:

- :class:`PGControlPlaneStoreTestMixin` — ONE Postgres schema per TestCase
  class (``setUpClass``/``tearDownClass``), amortizing the ~1-3s schema
  bootstrap across the class. This is the DEFAULT choice for migrations.
  Isolation contract under per-class schema: all tests in the class share the
  schema's DDL and migration history, and the mixin's ``setUp`` truncates every
  domain-data table in the schema (cheap, no re-bootstrap) so each test still
  starts from empty domain state. Tests must therefore not rely on data
  surviving between test methods, and stores created via
  :meth:`PGControlPlaneStoreTestMixin.make_pg_store` are closed automatically
  after each test so truncation never blocks on stale connections.

- :func:`pg_backed_control_plane_store` — a context manager that provisions a
  FRESH per-test schema plus store. Use it for tests that need a pristine
  schema (not just empty tables) and for pytest-style function tests.

Skip/require behavior (mirrors the existing require-flag convention):

- When no local Postgres is available the underlying fixture raises
  ``unittest.SkipTest`` with a clear reason and tests skip.
- ``SOURCING_REQUIRE_PG_STORE_TESTS=1`` turns those skips into hard failures.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeFixture, psycopg

REQUIRE_PG_STORE_TESTS_ENV = "SOURCING_REQUIRE_PG_STORE_TESTS"
_MIGRATION_HISTORY_TABLE = "schema_migrations"


def pg_store_tests_required() -> bool:
    return os.getenv(REQUIRE_PG_STORE_TESTS_ENV) == "1"


def enter_pg_store_fixture(*, runtime_dir: str | Path, schema_label: str) -> PGDurableRuntimeFixture:
    """Enter a PGDurableRuntimeFixture, honoring SOURCING_REQUIRE_PG_STORE_TESTS.

    Reuses the durable-runtime fixture wholesale (DSN resolution, schema
    create/drop, env patching). The only added behavior is converting its
    skip outcome into a failure when this suite's require flag is set.
    """

    fixture = PGDurableRuntimeFixture(runtime_dir=runtime_dir, schema_label=schema_label)
    try:
        fixture.__enter__()
    except unittest.SkipTest as exc:
        if pg_store_tests_required():
            raise AssertionError(
                f"{REQUIRE_PG_STORE_TESTS_ENV}=1 but the PG-backed store fixture is unavailable: {exc}"
            ) from exc
        raise
    return fixture


def truncate_pg_store_schema_tables(fixture: PGDurableRuntimeFixture | None) -> None:
    """Cheap between-test reset for the per-class schema granularity.

    Truncates every domain-data table currently present in the fixture schema
    so the next test starts from empty domain state without paying schema
    re-bootstrap. The migration ledger is schema metadata owned by the shared
    class fixture, so it must survive until ``tearDownClass`` drops the schema;
    clearing it would make the next store reapply migrations against existing
    DDL. The data-table list is read from ``pg_tables`` (not hardcoded) so it
    stays correct as the control-plane schema evolves.
    """

    if psycopg is None or fixture is None or not fixture.dsn or not fixture.schema:
        return
    with psycopg.connect(fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename <> %s",
                (fixture.schema, _MIGRATION_HISTORY_TABLE),
            )
            table_names = [str(row[0]) for row in cursor.fetchall()]
            if not table_names:
                return
            quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
            joined = ", ".join(
                f"{quoted_schema}.{quote_control_plane_postgres_identifier(name)}" for name in table_names
            )
            cursor.execute(f"TRUNCATE TABLE {joined} CASCADE")


class PGControlPlaneStoreTestMixin:
    """unittest mixin: per-CLASS PG schema for ControlPlaneStore tests.

    Usage::

        class MyStoreTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
            def setUp(self) -> None:
                super().setUp()  # truncates shared-schema tables
                self.tempdir = tempfile.TemporaryDirectory()
                self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")

    ``setUpClass`` provisions the schema once (skip/fail semantics per module
    docstring); ``setUp`` truncates all domain-data tables so tests do not
    observe each other's rows while preserving the schema's migration history;
    ``make_pg_store`` builds a ControlPlaneStore under the fixture's patched
    env (PG-only mode) and registers ``store.close`` as a cleanup so adapter
    pools never outlive the test.
    """

    pg_store_schema_label: str = ""
    _pg_store_fixture: PGDurableRuntimeFixture | None = None
    _pg_store_class_tempdir: tempfile.TemporaryDirectory | None = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        class_tempdir = tempfile.TemporaryDirectory()
        try:
            cls._pg_store_fixture = enter_pg_store_fixture(
                runtime_dir=class_tempdir.name,
                schema_label=cls.pg_store_schema_label or cls.__name__.lower(),
            )
        except BaseException:
            class_tempdir.cleanup()
            raise
        cls._pg_store_class_tempdir = class_tempdir

    @classmethod
    def tearDownClass(cls) -> None:
        fixture = cls._pg_store_fixture
        cls._pg_store_fixture = None
        if fixture is not None:
            fixture.__exit__(None, None, None)
        class_tempdir = cls._pg_store_class_tempdir
        cls._pg_store_class_tempdir = None
        if class_tempdir is not None:
            class_tempdir.cleanup()
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        truncate_pg_store_schema_tables(self._pg_store_fixture)

    def make_pg_store(self, db_path: str | Path) -> ControlPlaneStore:
        store = ControlPlaneStore(db_path)
        store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
        self.addCleanup(store.close)
        return store


@contextmanager
def pg_backed_control_plane_store(
    *,
    schema_label: str = "",
    db_path: str | Path | None = None,
) -> Iterator[ControlPlaneStore]:
    """Per-TEST PG schema + ControlPlaneStore (pristine state).

    For pytest-style function tests and for unittest tests that need a fresh
    schema rather than the per-class truncate contract. Costs the full schema
    bootstrap per use, so prefer the mixin for whole-class migrations.
    """

    with tempfile.TemporaryDirectory() as tempdir:
        fixture = enter_pg_store_fixture(
            runtime_dir=tempdir,
            schema_label=schema_label or "pg_store",
        )
        try:
            store = ControlPlaneStore(Path(db_path) if db_path is not None else Path(tempdir) / "control_plane.db")
            store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
            try:
                yield store
            finally:
                store.close()
        finally:
            fixture.__exit__(None, None, None)
