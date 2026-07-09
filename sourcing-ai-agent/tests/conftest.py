"""Session-wide test isolation guards.

Background: on 2026-06-27 a unit test run (test_pipeline against a shared Postgres,
provider mode defaulting to live, with runtime/secrets/providers.local.json present)
caused real, billed Apify calls — a detached workflow-runner subprocess resolved
AppSettings.from_env() to live mode and read the production Apify token from the
secrets file.

This conftest closes the test/prod *secrets* boundary: it guarantees that no test
(or any subprocess a test spawns, which inherits os.environ) can read the real
production secrets file. Tests that need specific credentials build settings
explicitly or set SOURCING_SECRETS_FILE themselves; setdefault() below respects any
such explicit override (e.g. the Makefile live-test lanes), so it only supplies a
safe empty default when nothing else has.

Combined with the fail-closed provider-mode default (runtime_environment.
SAFE_DEFAULT_PROVIDER_MODE) this means a bare test process is doubly safe: non-live
by default (which blanks resolved tokens) AND pointed at an empty secrets file.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

import pytest

# A stable, empty secrets file outside the repo. Stable path so subprocesses spawned
# during a test (which inherit this env) resolve to the same empty payload.
_ISOLATED_TEST_SECRETS = Path(tempfile.gettempdir()) / "sourcing_agent_test_empty_secrets.json"
try:
    if not _ISOLATED_TEST_SECRETS.exists():
        _ISOLATED_TEST_SECRETS.write_text("{}\n", encoding="utf-8")
    # Only supply the safe default when the caller has not pinned a secrets file.
    os.environ.setdefault("SOURCING_SECRETS_FILE", str(_ISOLATED_TEST_SECRETS))
except OSError:
    # If the temp file cannot be created, fall back to a definitely-nonexistent path
    # rather than leaving the real production secrets file reachable.
    os.environ.setdefault("SOURCING_SECRETS_FILE", str(_ISOLATED_TEST_SECRETS))


_PG_ISOLATED_SCHEMA_FLAG = "SOURCING_TEST_PG_ISOLATED_SCHEMA"


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _postgres_only_active() -> bool:
    """True when tests run against a live control-plane Postgres (postgres_only)."""
    mode = str(os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE") or "").strip().lower()
    dsn = str(os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    return mode == "postgres_only" and bool(dsn)


@pytest.fixture(autouse=True)
def _isolated_control_plane_pg_schema():
    """Per-test control-plane Postgres schema isolation.

    Root of the 2026-06-27 incident's collateral flakiness: when the suite runs in
    postgres_only mode it shares one Postgres schema ("public"), so tests pollute each
    other (and the shared/production rows) — leftover jobs/snapshots produced
    nondeterministic failures (e.g. baseline_snapshot_id drift, job row counts).

    When postgres_only is active, assign each test a unique schema via
    SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA *before* the store/connection is created in
    the test's setUp (configure_control_plane_postgres_session CREATEs it and sets
    search_path), then DROP it afterward. In the default (SQLite/simulate) test mode
    this is a no-op. Tests that pin the schema themselves keep their value.

    Opt-in (default OFF): enable with SOURCING_TEST_PG_ISOLATED_SCHEMA=1. Not yet
    default-on because a minority of suites are not self-contained against a fresh
    schema (latent cross-test data dependencies the shared "public" schema masked) and
    a few mock-cursor unit tests don't model schema DDL; default-on requires those to be
    isolation-ready first. With the flag on, the curated PG contract lane is green and
    broad suites (e.g. test_pipeline) get true per-test isolation.
    """
    if (
        not _truthy(os.environ.get(_PG_ISOLATED_SCHEMA_FLAG))
        or not _postgres_only_active()
        or os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA")
    ):
        yield
        return
    schema = f"test_{uuid.uuid4().hex[:24]}"
    previous = os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA")
    os.environ["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"] = schema
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA", None)
        else:
            os.environ["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"] = previous
        dsn = str(os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
        if dsn:
            try:
                import psycopg

                with psycopg.connect(dsn, autocommit=True) as connection:
                    connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            except Exception:
                # Best-effort cleanup; a leaked test_* schema is harmless and reaped
                # by the next prune. Never fail a test on teardown cleanup.
                pass
