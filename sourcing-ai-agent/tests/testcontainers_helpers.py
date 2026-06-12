from __future__ import annotations

import os
import re
import subprocess
import unittest

try:
    import psycopg
    from testcontainers.postgres import PostgresContainer
except Exception as exc:  # pragma: no cover - exercised only when optional deps are absent.
    psycopg = None
    PostgresContainer = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def libpq_dsn_from_container(postgres: object) -> str:
    raw_url = str(postgres.get_connection_url() or "")
    return re.sub(r"^postgresql\+[^:]+://", "postgresql://", raw_url)


def require_testcontainers_pg() -> None:
    if psycopg is None or PostgresContainer is None:
        if os.getenv("SOURCING_REQUIRE_TESTCONTAINERS") == "1":
            raise AssertionError(f"testcontainers postgres dependency unavailable: {_IMPORT_ERROR}")
        raise unittest.SkipTest(f"testcontainers postgres dependency unavailable: {_IMPORT_ERROR}")
    if os.getenv("SOURCING_REQUIRE_TESTCONTAINERS") == "1":
        return
    if os.getenv("SOURCING_RUN_TESTCONTAINERS") != "1":
        raise unittest.SkipTest("set SOURCING_RUN_TESTCONTAINERS=1 to run disposable PG contract harness")


def _apply_current_docker_context_host() -> None:
    if os.getenv("DOCKER_HOST"):
        return
    try:
        result = subprocess.run(
            [
                "docker",
                "context",
                "inspect",
                "--format",
                '{{ (index .Endpoints "docker").Host }}',
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return
    docker_host = (result.stdout or "").strip()
    if not docker_host:
        return
    if docker_host.startswith("unix://"):
        socket_path = docker_host.removeprefix("unix://")
        if not os.path.exists(socket_path):
            return
    os.environ["DOCKER_HOST"] = docker_host


def start_postgres_container() -> object:
    assert PostgresContainer is not None
    _apply_current_docker_context_host()
    try:
        postgres_image = os.getenv("SOURCING_TESTCONTAINERS_POSTGRES_IMAGE", "postgres:16-alpine")
        container = PostgresContainer(postgres_image)
        return container.start()
    except Exception as exc:
        if os.getenv("SOURCING_REQUIRE_TESTCONTAINERS") == "1":
            raise AssertionError(f"testcontainers postgres could not start: {exc}") from exc
        raise unittest.SkipTest(f"testcontainers postgres could not start: {exc}") from exc
