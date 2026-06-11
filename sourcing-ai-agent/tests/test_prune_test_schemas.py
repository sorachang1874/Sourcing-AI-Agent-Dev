"""Orphan test-schema janitor (scripts/prune_test_schemas.py) contract tests.

PG-backed tests run against the live local docker control-plane Postgres and
skip cleanly when no DSN resolves. They scope every candidate query with a
unique --match token so concurrent test runs can never interfere (and so the
janitor never drops schemas owned by other in-flight tests).
"""

from __future__ import annotations

import importlib.util
import json
import re
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock

from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)

try:
    import psycopg
except Exception:  # pragma: no cover - depends on optional local test env.
    psycopg = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_janitor_module():
    module_path = _repo_root() / "scripts" / "prune_test_schemas.py"
    spec = importlib.util.spec_from_file_location("prune_test_schemas", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_JANITOR = _load_janitor_module()


def _resolve_local_dsn() -> str:
    return str(resolve_control_plane_postgres_dsn(_repo_root()) or "").strip()


def _schema_exists(dsn: str, schema: str) -> bool:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))
            return cursor.fetchone() is not None


def _create_schema(dsn: str, schema: str) -> None:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_control_plane_postgres_identifier(schema)}")


def _drop_schema(dsn: str, schema: str) -> None:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(schema)} CASCADE")


class DsnLocalityGuardTest(unittest.TestCase):
    """Locality refusal is pure parsing — no PG required."""

    def test_accepts_loopback_url_dsns(self) -> None:
        self.assertTrue(_JANITOR.dsn_is_local("postgresql://sourcing@127.0.0.1:55432/sourcing_agent"))
        self.assertTrue(_JANITOR.dsn_is_local("postgresql://sourcing@localhost/sourcing_agent"))
        self.assertTrue(_JANITOR.dsn_is_local("host=127.0.0.1 port=55432 dbname=sourcing_agent user=sourcing"))

    def test_rejects_remote_and_malformed_dsns(self) -> None:
        self.assertFalse(_JANITOR.dsn_is_local("postgresql://sourcing@db.internal.example.com/sourcing_agent"))
        self.assertFalse(_JANITOR.dsn_is_local("postgresql://sourcing@10.0.0.8:5432/sourcing_agent"))
        self.assertFalse(_JANITOR.dsn_is_local("host=db.internal.example.com dbname=x"))
        self.assertFalse(_JANITOR.dsn_is_local(""))
        self.assertFalse(_JANITOR.dsn_is_local("mysql://localhost/x"))

    def test_prune_refuses_non_local_dsn(self) -> None:
        with self.assertRaises(ValueError):
            _JANITOR.prune_test_schemas(
                dsn="postgresql://sourcing@control-plane.prod.example.com/sourcing_agent",
                runtime_roots=[],
            )


class JanitorOrderingAndGuardUnitTest(unittest.TestCase):
    """Race-ordering + active-query guard wiring, no PG required (mocked)."""

    _LOCAL_DSN = "postgresql://sourcing@127.0.0.1:55432/sourcing_agent"

    def test_pg_schema_snapshot_taken_before_runtime_dir_scan(self) -> None:
        # A runtime created between the two steps must not be classifiable as
        # an unpaired orphan: snapshot PG FIRST, then scan dirs (anything
        # created after the snapshot is simply not a candidate).
        calls: list[str] = []

        def _fake_list(dsn, *, match=None):
            calls.append("pg_snapshot")
            return []

        def _fake_collect(roots):
            calls.append("dir_scan")
            return {}

        with mock.patch.object(_JANITOR, "list_candidate_test_schemas", side_effect=_fake_list):
            with mock.patch.object(_JANITOR, "collect_referenced_schemas", side_effect=_fake_collect):
                report = _JANITOR.prune_test_schemas(dsn=self._LOCAL_DSN, runtime_roots=[])
        self.assertEqual(calls, ["pg_snapshot", "dir_scan"])
        self.assertEqual(report["schemas"], [])

    def test_active_query_guard_blocks_drop_and_force_overrides(self) -> None:
        schema = "sourcing_test_guard_unit"
        active = {schema: ["pid=4242,state=active"]}
        with mock.patch.object(_JANITOR, "list_candidate_test_schemas", return_value=[schema]):
            with mock.patch.object(_JANITOR, "collect_referenced_schemas", return_value={}):
                with mock.patch.object(
                    _JANITOR, "list_active_query_schema_references", return_value=active
                ):
                    with mock.patch.object(_JANITOR, "_drop_schema") as drop_mock:
                        report = _JANITOR.prune_test_schemas(
                            dsn=self._LOCAL_DSN, runtime_roots=[], apply=True
                        )
        drop_mock.assert_not_called()
        self.assertEqual(report["orphans"], [])
        self.assertEqual(report["skipped_active"], [schema])
        entry = report["schemas"][0]
        self.assertEqual(entry["verdict"], "skipped_active")
        self.assertEqual(entry["reason"], "active_query_reference")
        self.assertEqual(entry["active_backends"], ["pid=4242,state=active"])

        # --force disables the guard entirely (the guard query is not even run).
        with mock.patch.object(_JANITOR, "list_candidate_test_schemas", return_value=[schema]):
            with mock.patch.object(_JANITOR, "collect_referenced_schemas", return_value={}):
                with mock.patch.object(
                    _JANITOR, "list_active_query_schema_references", return_value=active
                ) as guard_mock:
                    with mock.patch.object(_JANITOR, "_drop_schema") as drop_mock:
                        forced = _JANITOR.prune_test_schemas(
                            dsn=self._LOCAL_DSN, runtime_roots=[], apply=True, force=True
                        )
        guard_mock.assert_not_called()
        drop_mock.assert_called_once_with(self._LOCAL_DSN, schema)
        self.assertEqual(forced["orphans"], [schema])
        self.assertEqual(forced["dropped"], [schema])
        self.assertTrue(forced["force"])

    def test_match_active_query_schemas_is_case_insensitive_containment(self) -> None:
        rows = [
            (101, "active", "SELECT pg_sleep(5) /* SOURCING_TEST_ABC */"),
            (102, "idle in transaction", "insert into sourcing_test_xyz.t values (1)"),
            (103, "active", None),
            (104, "active", "SELECT 1"),
        ]
        matched = _JANITOR._match_active_query_schemas(
            rows, ["sourcing_test_abc", "sourcing_test_xyz", "sourcing_test_unseen"]
        )
        self.assertEqual(
            matched,
            {
                "sourcing_test_abc": ["pid=101,state=active"],
                "sourcing_test_xyz": ["pid=102,state=idle in transaction"],
            },
        )


class PruneTestSchemasPGTest(unittest.TestCase):
    """Classification + apply semantics against the live local docker PG."""

    def setUp(self) -> None:
        if psycopg is None:
            self.skipTest("psycopg not installed in this test environment")
        self.dsn = _resolve_local_dsn()
        if not self.dsn:
            self.skipTest("no local control-plane Postgres DSN resolved (make local-pg-up)")
        if not _JANITOR.dsn_is_local(self.dsn):
            self.skipTest(f"resolved DSN is not local: {self.dsn}")
        self.token = f"jt{uuid.uuid4().hex[:10]}"
        self.match = re.compile(self.token)
        self._tmp = tempfile.TemporaryDirectory(prefix=f"prune-janitor-{self.token}-")
        self.addCleanup(self._tmp.cleanup)
        self.runtime_root = Path(self._tmp.name)
        self._schemas: list[str] = []

    def tearDown(self) -> None:
        for schema in self._schemas:
            try:
                _drop_schema(self.dsn, schema)
            except Exception:
                pass

    def _register_schema(self, schema: str) -> str:
        _create_schema(self.dsn, schema)
        self._schemas.append(schema)
        return schema

    def test_janitor_classifies_pairings_and_only_drops_orphans_with_apply(self) -> None:
        # 1. contract v2 marker pairing
        marker_schema = self._register_schema(f"sourcing_simulate_{self.token}_marker")
        marker_dir = self.runtime_root / f"{self.token}_marker_case"
        marker_dir.mkdir(parents=True)
        (marker_dir / _JANITOR.EPHEMERAL_TEST_ENV_MARKER_NAME).write_text(
            json.dumps({"schema": marker_schema, "created_at": "2026-06-11T00:00:00+00:00"}),
            encoding="utf-8",
        )
        # 2. legacy .scripted-local-postgres.env pairing
        legacy_schema = self._register_schema(f"sourcing_scripted_{self.token}_legacy")
        legacy_dir = self.runtime_root / f"{self.token}_legacy_case"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / ".scripted-local-postgres.env").write_text(
            f"export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA='{legacy_schema}'\n",
            encoding="utf-8",
        )
        # 3. derived dir->schema pairing (no marker, no env file)
        derived_dir = self.runtime_root / f"{self.token}_drv"
        derived_dir.mkdir(parents=True)
        derived_schema = self._register_schema(
            _JANITOR._default_isolated_postgres_schema(derived_dir, provider_mode="simulate")
        )
        self.assertIn(self.token, derived_schema)
        # 4. orphan: schema with no runtime dir pairing at all
        orphan_schema = self._register_schema(f"sourcing_test_{self.token}_orphan")

        report = _JANITOR.prune_test_schemas(
            dsn=self.dsn,
            runtime_roots=[self.runtime_root],
            match=self.match,
        )
        self.assertEqual(report["status"], "dry_run")
        verdicts = {entry["schema"]: entry["verdict"] for entry in report["schemas"]}
        self.assertEqual(
            verdicts,
            {
                marker_schema: "referenced",
                legacy_schema: "referenced",
                derived_schema: "referenced",
                orphan_schema: "orphan",
            },
        )
        self.assertEqual(report["orphans"], [orphan_schema])
        self.assertEqual(report["dropped"], [])
        # dry-run never drops
        self.assertTrue(_schema_exists(self.dsn, orphan_schema))

        applied = _JANITOR.prune_test_schemas(
            dsn=self.dsn,
            runtime_roots=[self.runtime_root],
            apply=True,
            match=self.match,
        )
        self.assertEqual(applied["status"], "applied")
        self.assertEqual(applied["dropped"], [orphan_schema])
        self.assertEqual(applied["drop_failures"], [])
        self.assertFalse(_schema_exists(self.dsn, orphan_schema))
        for kept in (marker_schema, legacy_schema, derived_schema):
            self.assertTrue(_schema_exists(self.dsn, kept), kept)

    def test_active_query_guard_skips_unpaired_schema_until_forced(self) -> None:
        active_schema = self._register_schema(f"sourcing_test_{self.token}_active")
        connect_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
        sleeper = psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5)

        def _run_sleeper() -> None:
            try:
                with sleeper.cursor() as cursor:
                    cursor.execute(f"SELECT pg_sleep(30) /* {active_schema} */")
            except Exception:
                pass  # cancelled at the end of the test

        sleeper_thread = threading.Thread(target=_run_sleeper, daemon=True)
        sleeper_thread.start()
        try:
            # Wait until the sleeping backend is visible as an active query.
            deadline = time.monotonic() + 10.0
            visible = False
            while time.monotonic() < deadline:
                with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5) as probe:
                    with probe.cursor() as cursor:
                        cursor.execute(
                            "SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                            "  AND state IS NOT NULL AND state <> 'idle' AND query ILIKE %s",
                            (f"%{active_schema}%",),
                        )
                        if cursor.fetchone() is not None:
                            visible = True
                            break
                time.sleep(0.05)
            self.assertTrue(visible, "sleeper backend never became visible in pg_stat_activity")

            report = _JANITOR.prune_test_schemas(
                dsn=self.dsn,
                runtime_roots=[self.runtime_root],
                apply=True,
                match=self.match,
            )
            verdicts = {entry["schema"]: entry["verdict"] for entry in report["schemas"]}
            self.assertEqual(verdicts[active_schema], "skipped_active")
            self.assertEqual(report["skipped_active"], [active_schema])
            self.assertNotIn(active_schema, report["dropped"])
            self.assertTrue(_schema_exists(self.dsn, active_schema))

            forced = _JANITOR.prune_test_schemas(
                dsn=self.dsn,
                runtime_roots=[self.runtime_root],
                apply=True,
                match=self.match,
                force=True,
            )
            self.assertIn(active_schema, forced["dropped"])
            self.assertFalse(_schema_exists(self.dsn, active_schema))
        finally:
            try:
                sleeper.cancel()
            except Exception:
                pass
            sleeper_thread.join(timeout=10)
            try:
                sleeper.close()
            except Exception:
                pass

    def test_cli_dry_run_reports_orphans_without_dropping(self) -> None:
        orphan_schema = self._register_schema(f"sourcing_replay_{self.token}_cliorphan")
        exit_code = _JANITOR.main(
            [
                "--dsn",
                self.dsn,
                "--runtime-root",
                str(self.runtime_root),
                "--match",
                self.token,
                "--json",
            ]
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue(_schema_exists(self.dsn, orphan_schema))


if __name__ == "__main__":
    unittest.main()
