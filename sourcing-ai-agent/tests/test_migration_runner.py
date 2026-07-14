"""Track B B1.2 — migration runner contract tests (PG-backed).

Pins the runner's four load-bearing behaviors:
  1. drift guard (Track B B4.1a, PG-native) — the live runtime bootstrap (the migration runner
     PLUS the writer/coordination ensures that run on bootstrap) is structurally IDENTICAL to a
     schema built by the migration runner ALONE. If a writer/coordination ensure starts creating
     schema that migrations/000N_*.sql do not, this fails — no schema may live outside the ledger.
     (Replaces the pre-B4.1a guard that compared against the SQLite init_schema source via
     _bootstrap_schema_from_sqlite_source.)
  2. idempotency — re-running applies nothing (ledger-gated).
  3. brownfield adoption — a schema that already has the baseline tables but no ledger is
     STAMPED at the baseline (not re-CREATE-d), so the B1.3 cutover is safe on live DBs.
  4. checksum tamper-detection — an applied migration whose file changed fails closed.

Skips without a local PG DSN; SOURCING_REQUIRE_PG_STORE_TESTS=1 turns the skip into a failure.
"""

from __future__ import annotations

import os
import unittest
from uuid import uuid4

from sourcing_agent import migration_runner as mr
from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)

try:
    import psycopg
except ImportError:  # pragma: no cover - exercised via skip path
    psycopg = None

_REQUIRE = os.getenv("SOURCING_REQUIRE_PG_STORE_TESTS") == "1"
_BASELINE_PATH = next(path for version, path in mr.discover_migrations() if version == "0001_baseline")


def _resolve_dsn() -> str | None:
    raw = resolve_control_plane_postgres_dsn()
    return normalize_control_plane_postgres_connect_dsn(raw) if raw else None


def _fingerprint(cursor, schema: str) -> dict:
    quoted = quote_control_plane_postgres_identifier(schema)
    cursor.execute(
        """
        SELECT table_name, column_name, data_type, is_nullable,
               replace(replace(coalesce(column_default, ''), %(p)s, ''), %(q)s, '')
        FROM information_schema.columns
        WHERE table_schema = %(s)s AND table_name <> 'schema_migrations'
        ORDER BY table_name, column_name
        """,
        {"p": f"{schema}.", "q": f"{quoted}.", "s": schema},
    )
    columns = cursor.fetchall()
    cursor.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename <> 'schema_migrations' ORDER BY 1",
        (schema,),
    )
    tables = [r[0] for r in cursor.fetchall()]
    cursor.execute(
        """
        SELECT tablename, indexname, indexdef
        FROM pg_indexes WHERE schemaname = %s AND tablename <> 'schema_migrations'
        ORDER BY tablename, indexname
        """,
        (schema,),
    )
    indexes = [(t, i, d.replace(f"{schema}.", "").replace(f"{quoted}.", "")) for t, i, d in cursor.fetchall()]
    return {"tables": tables, "columns": columns, "indexes": indexes}


@unittest.skipIf(psycopg is None, "psycopg not installed")
class MigrationRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.dsn = _resolve_dsn()
        if not cls.dsn:
            if _REQUIRE:
                raise AssertionError("SOURCING_REQUIRE_PG_STORE_TESTS=1 but no control-plane Postgres DSN resolved")
            raise unittest.SkipTest("no control-plane Postgres DSN available")

    def _fresh_schema(self, label: str) -> str:
        schema = f"track_b_mr_{label}_{uuid4().hex[:8]}"
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
                cur.execute(f"CREATE SCHEMA {quoted}")
        self.addCleanup(self._drop_schema, schema)
        return schema

    def _drop_schema(self, schema: str) -> None:
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")

    def test_runner_built_schema_matches_live_bootstrap(self) -> None:
        # Track B B4.1a: migrations are the sole schema source of truth, so there is no longer an
        # independent SQLite oracle to diff against — the migration files ARE the golden. The
        # meaningful remaining drift is "schema created outside the migration ledger": this builds
        # the real live runtime bootstrap (the migration runner + the writer/coordination ensures
        # that run on bootstrap) and asserts it is structurally identical to a schema built by the
        # migration runner ALONE. PG-native, no SQLite — the pre-B4.1a
        # _bootstrap_schema_from_sqlite_source comparison is retired.
        from tests.pg_store_fixture import pg_backed_control_plane_store

        with pg_backed_control_plane_store(schema_label="mr_live") as store:
            adapter = store._control_plane_postgres
            # Construction already ran ensure_bootstrapped (migration runner + coordination ensure);
            # force the writer ensure too so the full live schema is materialized.
            adapter._ensure_control_plane_writer_schema()
            live_schema = adapter.schema
            with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                with conn.cursor() as cur:
                    live_fp = _fingerprint(cur, live_schema)

        # runner side: a fresh schema built solely by applying the migrations
        runner_schema = self._fresh_schema("runner")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=runner_schema)
            with conn.cursor() as cur:
                runner_fp = _fingerprint(cur, runner_schema)

        self.assertEqual(result.applied, ["0001_baseline", "0002_action_request_schema_pins"])
        self.assertEqual(result.stamped, [])
        self.assertEqual(
            runner_fp["tables"], live_fp["tables"], "table set: schema created outside the migration ledger"
        )
        self.assertEqual(
            runner_fp["columns"], live_fp["columns"], "column: schema created outside the migration ledger"
        )
        self.assertEqual(runner_fp["indexes"], live_fp["indexes"], "index: schema created outside the migration ledger")
        self.assertEqual(len(runner_fp["tables"]), 83)

    def test_runner_is_idempotent(self) -> None:
        schema = self._fresh_schema("idem")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            first = mr.apply_pending_migrations(conn, schema=schema)
            second = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(first.applied, ["0001_baseline", "0002_action_request_schema_pins"])
        self.assertEqual(second.applied, [])
        self.assertEqual(second.stamped, [])
        self.assertEqual(
            second.already_applied,
            ["0001_baseline", "0002_action_request_schema_pins"],
        )

    def test_brownfield_schema_is_stamped_not_recreated(self) -> None:
        # Simulate a pre-runner DB: apply the baseline DDL DIRECTLY (no ledger), as the old
        # sqlite_master-derived bootstrap effectively did, then run the migration runner.
        schema = self._fresh_schema("brownfield")
        quoted = quote_control_plane_postgres_identifier(schema)
        baseline_sql = _BASELINE_PATH.read_text(encoding="utf-8")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(baseline_sql)  # also confirms multi-statement execute works
            conn.commit()
            # no schema_migrations ledger exists yet — the runner must STAMP, not re-CREATE
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY 1")
                ledger = [r[0] for r in cur.fetchall()]
        self.assertEqual(result.stamped, ["0001_baseline"])
        self.assertEqual(result.applied, ["0002_action_request_schema_pins"])
        self.assertEqual(ledger, ["0001_baseline", "0002_action_request_schema_pins"])

    def test_applied_migration_checksum_change_fails_closed(self) -> None:
        schema = self._fresh_schema("checksum")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("UPDATE schema_migrations SET checksum = 'tampered' WHERE version = '0001_baseline'")
            conn.commit()
            with self.assertRaises(mr.MigrationChecksumError):
                mr.apply_pending_migrations(conn, schema=schema)


if __name__ == "__main__":
    unittest.main()
