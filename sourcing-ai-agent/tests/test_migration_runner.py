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
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
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
_D3_COMMAND_MIGRATION = "0003_workflow_command_claim_fence_foundation"
_D3_SCOPED_ROOT_MIGRATION = "0004_d3_scoped_root_foundation"
_D3_ACTIVITY_MIGRATION = "0005_d3_activity_claim_chain_foundation"
_D3_EVENT_MIGRATION = "0006_d3_workflow_event_terminal_lineage_foundation"
_D0F_ENVELOPE_MIGRATION = "0007_model_invocation_envelopes"
_D1I_PARENT_UNIQUENESS_MIGRATION = "0008_acquisition_intent_parent_uniqueness"
_D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION = "0009_company_public_web_asset_run_idempotency"
_D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION = "0010_acquisition_plan_preview_uow"
_ALL_MIGRATIONS = [
    "0001_baseline",
    "0002_action_request_schema_pins",
    _D3_COMMAND_MIGRATION,
    _D3_SCOPED_ROOT_MIGRATION,
    _D3_ACTIVITY_MIGRATION,
    _D3_EVENT_MIGRATION,
    _D0F_ENVELOPE_MIGRATION,
    _D1I_PARENT_UNIQUENESS_MIGRATION,
    _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
    _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
]
_D3_COMMAND_COLUMNS = (
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("workspace_id", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("coordination_plan_review_id", "bigint", "YES", None),
    ("claim_authority_spec_digest", "text", "NO", "''::text"),
    ("expected_predecessor_intent_id", "text", "YES", None),
    ("expected_predecessor_phase_generation", "bigint", "YES", None),
    ("expected_predecessor_source_control_epoch", "bigint", "YES", None),
    ("expected_predecessor_decision_source_event_id", "text", "YES", None),
    ("d3_business_fence_digest", "text", "NO", "''::text"),
    ("claim_selection_generation", "bigint", "NO", "0"),
    ("consumed_claim_authority_id", "text", "NO", "''::text"),
    ("claim_generation", "bigint", "NO", "0"),
    ("claim_token_digest", "text", "NO", "''::text"),
    ("control_epoch", "bigint", "NO", "0"),
    ("heartbeat_sequence", "bigint", "NO", "0"),
    ("last_heartbeat_id", "text", "NO", "''::text"),
    ("terminal_event_id", "text", "YES", None),
    ("terminal_outcome_digest", "text", "YES", None),
)
_D3_COMMAND_CHECKS = {
    "workflow_commands_claim_authority_spec_digest_shape_ck": (
        "claim_authority_spec_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_commands_claim_generation_nonnegative_ck": ("claim_generation", ">= 0"),
    "workflow_commands_claim_selection_generation_nonnegative_ck": ("claim_selection_generation", ">= 0"),
    "workflow_commands_claim_token_digest_shape_ck": ("claim_token_digest", "[0-9a-f]{64}"),
    "workflow_commands_consumed_claim_authority_id_shape_ck": (
        "consumed_claim_authority_id",
        "[^[:space:]]",
    ),
    "workflow_commands_control_epoch_nonnegative_ck": ("control_epoch", ">= 0"),
    "workflow_commands_coordination_plan_review_id_shape_ck": ("coordination_plan_review_id", "> 0"),
    "workflow_commands_d3_business_fence_digest_shape_ck": ("d3_business_fence_digest", "[0-9a-f]{64}"),
    "workflow_commands_expected_predecessor_shape_ck": (
        "expected_predecessor_intent_id",
        "expected_predecessor_phase_generation",
        "expected_predecessor_source_control_epoch",
        "expected_predecessor_decision_source_event_id",
        "[^[:space:]]",
        "> 0",
        ">= 0",
    ),
    "workflow_commands_heartbeat_sequence_nonnegative_ck": ("heartbeat_sequence", ">= 0"),
    "workflow_commands_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "workflow_commands_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "workflow_commands_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "workflow_commands_terminal_outcome_digest_shape_ck": ("terminal_outcome_digest", "[0-9a-f]{64}"),
    "workflow_commands_terminal_pair_shape_ck": (
        "terminal_event_id IS NULL",
        "terminal_outcome_digest IS NULL",
        "terminal_event_id IS NOT NULL",
        "terminal_outcome_digest IS NOT NULL",
    ),
    "workflow_commands_workspace_id_shape_ck": ("workspace_id", "[^[:space:]]"),
}
_D3_SCOPED_SESSION_COLUMNS = (
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("workspace_id", "text", "NO", "''::text"),
    ("scope_issuer", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("creation_source_workflow_command_id", "text", "NO", "''::text"),
    ("creation_source_event_id", "text", "NO", "''::text"),
    ("creation_plan_id", "text", "NO", "''::text"),
    ("creation_plan_revision", "bigint", "NO", "0"),
    ("creation_plan_bundle_digest", "text", "NO", "''::text"),
    ("creation_idempotency_key", "text", "NO", "''::text"),
)
_D3_OPERATION_ROOT_COLUMNS = (
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("scope_issuer", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("coordination_plan_review_id", "bigint", "YES", None),
)
_D3_SCOPED_SESSION_CHECKS = {
    "plan_review_sessions_creation_idempotency_key_shape_ck": ("creation_idempotency_key", "[0-9a-f]{64}"),
    "plan_review_sessions_creation_plan_bundle_digest_shape_ck": (
        "creation_plan_bundle_digest",
        "[0-9a-f]{64}",
    ),
    "plan_review_sessions_creation_plan_id_shape_ck": ("creation_plan_id", "[^[:space:]]"),
    "plan_review_sessions_creation_plan_revision_nonnegative_ck": ("creation_plan_revision", ">= 0"),
    "plan_review_sessions_creation_source_event_id_shape_ck": ("creation_source_event_id", "[^[:space:]]"),
    "plan_review_sessions_creation_source_command_id_shape_ck": (
        "creation_source_workflow_command_id",
        "[^[:space:]]",
    ),
    "plan_review_sessions_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "plan_review_sessions_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "plan_review_sessions_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "plan_review_sessions_scope_issuer_shape_ck": ("scope_issuer", "plan_review_session"),
    "plan_review_sessions_workspace_id_shape_ck": ("workspace_id", "[^[:space:]]"),
}
_D3_OPERATION_ROOT_CHECKS = {
    "operation_runs_coordination_plan_review_id_shape_ck": ("coordination_plan_review_id", "> 0"),
    "operation_runs_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "operation_runs_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "operation_runs_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "operation_runs_scope_issuer_shape_ck": ("scope_issuer", "plan_review_session"),
}
_D3_ACTIVITY_RUN_COLUMNS = (
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("coordination_plan_review_id", "bigint", "YES", None),
    ("claim_authority_spec_digest", "text", "NO", "''::text"),
    ("d3_business_fence_digest", "text", "NO", "''::text"),
)
_D3_ACTIVITY_ATTEMPT_COLUMNS = (
    ("operation_run_id", "text", "NO", "''::text"),
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("coordination_plan_review_id", "bigint", "YES", None),
    ("claim_authority_spec_digest", "text", "NO", "''::text"),
    ("d3_business_fence_digest", "text", "NO", "''::text"),
    ("claim_generation", "bigint", "NO", "0"),
    ("command_attempt", "bigint", "NO", "0"),
    ("control_epoch", "bigint", "NO", "0"),
)
_D3_ACTIVITY_RUN_CHECKS = {
    "workflow_activity_runs_claim_authority_spec_digest_shape_ck": (
        "claim_authority_spec_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_activity_runs_coordination_plan_review_id_shape_ck": (
        "coordination_plan_review_id",
        "> 0",
    ),
    "workflow_activity_runs_d3_business_fence_digest_shape_ck": (
        "d3_business_fence_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_activity_runs_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "workflow_activity_runs_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "workflow_activity_runs_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "workflow_activity_runs_workspace_id_shape_ck": ("workspace_id", "[^[:space:]]"),
}
_D3_ACTIVITY_ATTEMPT_CHECKS = {
    "workflow_activity_attempts_claim_authority_spec_digest_shape_ck": (
        "claim_authority_spec_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_activity_attempts_claim_generation_nonnegative_ck": ("claim_generation", ">= 0"),
    "workflow_activity_attempts_command_attempt_nonnegative_ck": ("command_attempt", ">= 0"),
    "workflow_activity_attempts_control_epoch_nonnegative_ck": ("control_epoch", ">= 0"),
    "workflow_activity_attempts_coordination_plan_review_id_shape_ck": (
        "coordination_plan_review_id",
        "> 0",
    ),
    "workflow_activity_attempts_d3_business_fence_digest_shape_ck": (
        "d3_business_fence_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_activity_attempts_operation_run_id_shape_ck": ("operation_run_id", "[^[:space:]]"),
    "workflow_activity_attempts_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "workflow_activity_attempts_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "workflow_activity_attempts_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "workflow_activity_attempts_workspace_id_shape_ck": ("workspace_id", "[^[:space:]]"),
}
_D3_EVENT_COLUMNS = (
    ("runtime_namespace", "text", "NO", "''::text"),
    ("provider_mode", "text", "NO", "''::text"),
    ("workspace_id", "text", "NO", "''::text"),
    ("scope_digest", "text", "NO", "''::text"),
    ("coordination_plan_review_id", "bigint", "YES", None),
    ("activity_run_id", "text", "NO", "''::text"),
    ("claim_generation", "bigint", "NO", "0"),
    ("control_epoch", "bigint", "NO", "0"),
    ("claim_authority_spec_digest", "text", "NO", "''::text"),
    ("d3_business_fence_digest", "text", "NO", "''::text"),
    ("terminal_outcome_digest", "text", "YES", None),
)
_D3_EVENT_CHECKS = {
    "workflow_events_activity_run_id_shape_ck": ("activity_run_id", "[^[:space:]]"),
    "workflow_events_claim_authority_spec_digest_shape_ck": (
        "claim_authority_spec_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_events_claim_generation_nonnegative_ck": ("claim_generation", ">= 0"),
    "workflow_events_control_epoch_nonnegative_ck": ("control_epoch", ">= 0"),
    "workflow_events_coordination_plan_review_id_shape_ck": (
        "coordination_plan_review_id",
        "> 0",
    ),
    "workflow_events_d3_business_fence_digest_shape_ck": (
        "d3_business_fence_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_events_provider_mode_shape_ck": (
        "provider_mode",
        "live",
        "simulate",
        "scripted",
        "replay",
    ),
    "workflow_events_runtime_namespace_shape_ck": ("runtime_namespace", "[^[:space:]]"),
    "workflow_events_scope_digest_shape_ck": ("scope_digest", "[0-9a-f]{64}"),
    "workflow_events_terminal_outcome_digest_shape_ck": (
        "terminal_outcome_digest",
        "[0-9a-f]{64}",
    ),
    "workflow_events_workspace_id_shape_ck": ("workspace_id", "[^[:space:]]"),
}


def _copy_migrations_through(directory: Path, through: int) -> None:
    for version, path in mr.discover_migrations():
        if int(version[:4]) <= through:
            shutil.copy2(path, directory / path.name)


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

    def _insert_company_public_web_asset_run(
        self,
        cursor,
        *,
        run_id: str,
        idempotency_key: str,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO company_public_web_asset_runs (
                run_id, target_company, company_key, idempotency_key,
                status, phase, source_families_json, seed_urls_json,
                options_json, discovered_assets_json, summary_json,
                artifact_root, requested_by, force_refresh, last_error,
                metadata_json
            ) VALUES (
                %s, 'OpenAI', 'openai', %s,
                'queued', 'queued', '["company_homepage"]',
                '["https://openai.com/"]', '{}', '[]', '{}',
                '', 'migration-test', 0, '', '{}'
            )
            """,
            (run_id, idempotency_key),
        )

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

        self.assertEqual(result.applied, _ALL_MIGRATIONS)
        self.assertEqual(result.stamped, [])
        self.assertEqual(
            runner_fp["tables"], live_fp["tables"], "table set: schema created outside the migration ledger"
        )
        self.assertEqual(
            runner_fp["columns"], live_fp["columns"], "column: schema created outside the migration ledger"
        )
        self.assertEqual(runner_fp["indexes"], live_fp["indexes"], "index: schema created outside the migration ledger")
        self.assertEqual(len(runner_fp["tables"]), 85)

    def test_runner_is_idempotent(self) -> None:
        schema = self._fresh_schema("idem")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            first = mr.apply_pending_migrations(conn, schema=schema)
            second = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(first.applied, _ALL_MIGRATIONS)
        self.assertEqual(second.applied, [])
        self.assertEqual(second.stamped, [])
        self.assertEqual(
            second.already_applied,
            _ALL_MIGRATIONS,
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
        self.assertEqual(
            result.applied,
            [
                "0002_action_request_schema_pins",
                _D3_COMMAND_MIGRATION,
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(ledger, _ALL_MIGRATIONS)

    def test_request_schema_pin_constraints_install_not_valid_and_still_guard_new_writes(self) -> None:
        schema = self._fresh_schema("pin_not_valid")
        quoted = quote_control_plane_postgres_identifier(schema)
        baseline_sql = _BASELINE_PATH.read_text(encoding="utf-8")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(baseline_sql)
                cur.execute(
                    "INSERT INTO agent_actions (action_id, action_type, owner_module, operation_type, "
                    "idempotency_key) VALUES ('brownfield-action', 'legacy', 'legacy', 'legacy', 'legacy')"
                )
                cur.execute(
                    "INSERT INTO operation_runs (operation_run_id, action_id, owner_module, operation_type, "
                    "idempotency_key) VALUES ('brownfield-run', 'brownfield-action', 'legacy', 'legacy', 'legacy')"
                )
            conn.commit()
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT conname, convalidated FROM pg_constraint c "
                    "JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname IN "
                    "('agent_actions_request_schema_pin_pair_check', "
                    "'operation_runs_request_schema_pin_pair_check') ORDER BY conname",
                    (schema,),
                )
                constraints = cur.fetchall()
                with self.assertRaises(psycopg.errors.CheckViolation):
                    cur.execute(
                        "UPDATE agent_actions SET request_schema_version = 'v1', request_schema_digest = '' "
                        "WHERE action_id = 'brownfield-action'"
                    )
            conn.rollback()
        self.assertEqual(
            result.applied,
            [
                "0002_action_request_schema_pins",
                _D3_COMMAND_MIGRATION,
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(
            constraints,
            [
                ("agent_actions_request_schema_pin_pair_check", False),
                ("operation_runs_request_schema_pin_pair_check", False),
            ],
        )

    def test_request_schema_pin_constraints_validate_after_install_without_blocking_row_exclusive_dml(self) -> None:
        schema = self._fresh_schema("pin_validate")
        quoted = quote_control_plane_postgres_identifier(schema)
        baseline_sql = _BASELINE_PATH.read_text(encoding="utf-8")
        writer_ready = threading.Event()
        release_writer = threading.Event()

        with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
            with setup.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(baseline_sql)
            setup.commit()
            mr.apply_pending_migrations(setup, schema=schema)

        def hold_row_exclusive_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as writer:
                with writer.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO agent_actions (action_id, action_type, owner_module, operation_type, "
                        "idempotency_key) VALUES ('validate-writer', 'legacy', 'legacy', 'legacy', 'validate-writer')"
                    )
                    writer_ready.set()
                    release_writer.wait(timeout=10)
                writer.rollback()

        thread = threading.Thread(target=hold_row_exclusive_write, daemon=True)
        thread.start()
        self.assertTrue(writer_ready.wait(timeout=5), "concurrent row-exclusive writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as validator:
                with validator.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute("SET LOCAL lock_timeout = '5s'")
                    cur.execute(
                        "ALTER TABLE agent_actions VALIDATE CONSTRAINT agent_actions_request_schema_pin_pair_check"
                    )
                    cur.execute(
                        "ALTER TABLE operation_runs VALIDATE CONSTRAINT operation_runs_request_schema_pin_pair_check"
                    )
                validator.commit()
        finally:
            release_writer.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "concurrent row-exclusive writer did not exit")
        self.assertLess(elapsed, 5.0)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT conname, convalidated FROM pg_constraint c "
                    "JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname IN "
                    "('agent_actions_request_schema_pin_pair_check', "
                    "'operation_runs_request_schema_pin_pair_check') ORDER BY conname",
                    (schema,),
                )
                constraints = cur.fetchall()
        self.assertEqual(
            constraints,
            [
                ("agent_actions_request_schema_pin_pair_check", True),
                ("operation_runs_request_schema_pin_pair_check", True),
            ],
        )

    def test_request_schema_pin_migration_lock_wait_is_bounded(self) -> None:
        schema = self._fresh_schema("pin_lock_budget")
        quoted = quote_control_plane_postgres_identifier(schema)
        baseline_sql = _BASELINE_PATH.read_text(encoding="utf-8")
        blocker_ready = threading.Event()
        release_blocker = threading.Event()

        def hold_agent_action_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as blocker:
                with blocker.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO agent_actions (action_id, action_type, owner_module, operation_type, "
                        "idempotency_key) VALUES ('lock-action', 'legacy', 'legacy', 'legacy', 'lock')"
                    )
                    blocker_ready.set()
                    release_blocker.wait(timeout=15)
                blocker.rollback()

        with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
            with setup.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(baseline_sql)
            setup.commit()

        thread = threading.Thread(target=hold_agent_action_write, daemon=True)
        thread.start()
        self.assertTrue(blocker_ready.wait(timeout=5), "blocking writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    mr.apply_pending_migrations(conn, schema=schema)
        finally:
            release_blocker.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "blocking writer did not exit")
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 8.0)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"{schema}.schema_migrations",))
                migration_ledger = cur.fetchone()[0]
                cur.execute(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND column_name IN "
                    "('request_schema_version', 'request_schema_digest') ORDER BY 1, 2",
                    (schema,),
                )
                pin_columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c "
                    "JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname IN "
                    "('agent_actions_request_schema_pin_pair_check', "
                    "'operation_runs_request_schema_pin_pair_check') ORDER BY conname",
                    (schema,),
                )
                pin_constraints = cur.fetchall()
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_commands' "
                    "AND column_name = ANY(%s) ORDER BY column_name",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_COMMAND_COLUMNS]),
                )
                d3_command_columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c "
                    "JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY conname",
                    (schema, list(_D3_COMMAND_CHECKS)),
                )
                d3_command_constraints = cur.fetchall()
        self.assertIsNone(migration_ledger, "failed migration must roll back the ledger DDL and rows")
        self.assertEqual(pin_columns, [], "failed migration must roll back both physical pin columns")
        self.assertEqual(pin_constraints, [], "failed migration must roll back both pin constraints")
        self.assertEqual(d3_command_columns, [], "0002 failure must not partially apply later D3 columns")
        self.assertEqual(d3_command_constraints, [], "0002 failure must not partially apply later D3 checks")

    def test_d3_command_foundation_installs_on_populated_table_and_guards_new_writes(self) -> None:
        schema = self._fresh_schema("d3_foundation")
        quoted = quote_control_plane_postgres_identifier(schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 2)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix_result = mr.apply_pending_migrations(
                    conn,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_commands "
                        "(command_id, workflow_run_id, command_type, owner, idempotency_key) "
                        "VALUES ('legacy-cmd', 'workflow-legacy', 'legacy', 'legacy', 'legacy-cmd')"
                    )
                conn.commit()

        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:2])
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_commands' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_COMMAND_COLUMNS]),
                )
                columns = cur.fetchall()
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, workspace_id, scope_digest, "
                    "coordination_plan_review_id, claim_authority_spec_digest, "
                    "expected_predecessor_intent_id, expected_predecessor_phase_generation, "
                    "expected_predecessor_source_control_epoch, "
                    "expected_predecessor_decision_source_event_id, d3_business_fence_digest, "
                    "claim_selection_generation, consumed_claim_authority_id, claim_generation, "
                    "claim_token_digest, control_epoch, heartbeat_sequence, last_heartbeat_id, "
                    "terminal_event_id, terminal_outcome_digest "
                    "FROM workflow_commands WHERE command_id = 'legacy-cmd'"
                )
                sentinel = cur.fetchone()
                cur.execute(
                    "SELECT conname, convalidated, pg_get_constraintdef(c.oid) FROM pg_constraint c "
                    "JOIN pg_class t ON t.oid = c.conrelid "
                    "JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "WHERE n.nspname = %s AND t.relname = 'workflow_commands' "
                    "AND conname = ANY(%s) ORDER BY conname",
                    (schema, list(_D3_COMMAND_CHECKS)),
                )
                checks = cur.fetchall()

        self.assertEqual(
            result.applied,
            [
                _D3_COMMAND_MIGRATION,
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(columns, list(_D3_COMMAND_COLUMNS))
        self.assertEqual(
            sentinel,
            ("", "", "", "", None, "", None, None, None, None, "", 0, "", 0, "", 0, 0, "", None, None),
        )
        self.assertEqual(
            [(name, validated) for name, validated, _definition in checks],
            [(name, False) for name in sorted(_D3_COMMAND_CHECKS)],
        )
        for name, _validated, definition in checks:
            normalized_definition = " ".join(str(definition).split()).casefold()
            for required_fragment in _D3_COMMAND_CHECKS[name]:
                self.assertIn(required_fragment.casefold(), normalized_definition, (name, definition))

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "INSERT INTO workflow_commands "
                    "(command_id, workflow_run_id, command_type, owner, idempotency_key) "
                    "VALUES ('fresh-cmd', 'workflow-fresh', 'legacy', 'legacy', 'fresh-cmd')"
                )
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, workspace_id, scope_digest, "
                    "coordination_plan_review_id, claim_authority_spec_digest, "
                    "expected_predecessor_intent_id, expected_predecessor_phase_generation, "
                    "expected_predecessor_source_control_epoch, "
                    "expected_predecessor_decision_source_event_id, d3_business_fence_digest, "
                    "claim_selection_generation, consumed_claim_authority_id, claim_generation, "
                    "claim_token_digest, control_epoch, heartbeat_sequence, last_heartbeat_id, "
                    "terminal_event_id, terminal_outcome_digest "
                    "FROM workflow_commands WHERE command_id = 'fresh-cmd'"
                )
                fresh_sentinel = cur.fetchone()
        self.assertEqual(fresh_sentinel, sentinel)

        digest_a = "a" * 64
        digest_b = "b" * 64
        digest_c = "c" * 64
        digest_d = "d" * 64
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "UPDATE workflow_commands SET runtime_namespace = 'runtime-a', provider_mode = 'scripted', "
                    "workspace_id = 'workspace-a', scope_digest = %s, coordination_plan_review_id = 1, "
                    "claim_authority_spec_digest = %s, expected_predecessor_intent_id = 'intent-a', "
                    "expected_predecessor_phase_generation = 1, expected_predecessor_source_control_epoch = 0, "
                    "expected_predecessor_decision_source_event_id = 'event-source-a', "
                    "d3_business_fence_digest = %s, claim_selection_generation = 1, "
                    "consumed_claim_authority_id = 'authority-a', claim_generation = 1, "
                    "claim_token_digest = %s, control_epoch = 2, heartbeat_sequence = 1, "
                    "last_heartbeat_id = 'heartbeat-a', terminal_event_id = 'terminal-event-a', "
                    "terminal_outcome_digest = %s WHERE command_id = 'legacy-cmd'",
                    (digest_a, digest_b, digest_c, digest_d, digest_a),
                )

        invalid_updates = (
            "provider_mode = 'invalid'",
            "claim_generation = -1",
            "scope_digest = 'BAD'",
            "runtime_namespace = '   '",
            "workspace_id = '   '",
            "coordination_plan_review_id = 0",
            "claim_authority_spec_digest = 'BAD'",
            "d3_business_fence_digest = 'BAD'",
            "claim_selection_generation = -1",
            "consumed_claim_authority_id = '   '",
            "claim_token_digest = 'BAD'",
            "control_epoch = -1",
            "heartbeat_sequence = -1",
            "expected_predecessor_intent_id = NULL",
            "expected_predecessor_intent_id = '   '",
            "expected_predecessor_phase_generation = 0",
            "expected_predecessor_source_control_epoch = -1",
            "expected_predecessor_decision_source_event_id = '   '",
            "terminal_event_id = NULL",
            "terminal_outcome_digest = 'BAD'",
        )
        for assignment in invalid_updates:
            with self.subTest(assignment=assignment):
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        with self.assertRaises(psycopg.errors.CheckViolation):
                            cur.execute(f"UPDATE workflow_commands SET {assignment} WHERE command_id = 'legacy-cmd'")

    def test_d3_command_foundation_lock_wait_is_bounded_and_rolls_back_0003_and_later(self) -> None:
        schema = self._fresh_schema("d3_lock_budget")
        quoted = quote_control_plane_postgres_identifier(schema)
        blocker_ready = threading.Event()
        release_blocker = threading.Event()

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 2)
            with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
                prefix_result = mr.apply_pending_migrations(
                    setup,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:2])

        def hold_workflow_command_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as blocker:
                with blocker.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_commands "
                        "(command_id, workflow_run_id, command_type, owner, idempotency_key) "
                        "VALUES ('lock-cmd', 'workflow-lock', 'legacy', 'legacy', 'lock-cmd')"
                    )
                    blocker_ready.set()
                    release_blocker.wait(timeout=15)
                blocker.rollback()

        thread = threading.Thread(target=hold_workflow_command_write, daemon=True)
        thread.start()
        self.assertTrue(blocker_ready.wait(timeout=5), "blocking workflow-command writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    mr.apply_pending_migrations(conn, schema=schema)
        finally:
            release_blocker.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "blocking workflow-command writer did not exit")
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 8.0)

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_commands' "
                    "AND column_name = ANY(%s) ORDER BY column_name",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_COMMAND_COLUMNS]),
                )
                columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c "
                    "JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY conname",
                    (schema, list(_D3_COMMAND_CHECKS)),
                )
                checks = cur.fetchall()

        self.assertEqual(ledger, _ALL_MIGRATIONS[:2])
        self.assertEqual(columns, [])
        self.assertEqual(checks, [])

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            recovered = mr.apply_pending_migrations(conn, schema=schema)
            again = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(
            recovered.applied,
            [
                _D3_COMMAND_MIGRATION,
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(again.applied, [])
        self.assertEqual(again.already_applied, _ALL_MIGRATIONS)

    def test_d3_scoped_root_foundation_installs_on_populated_tables_and_guards_new_writes(self) -> None:
        schema = self._fresh_schema("d3_scoped_root")
        quoted = quote_control_plane_postgres_identifier(schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 3)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix_result = mr.apply_pending_migrations(
                    conn,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO plan_review_sessions "
                        "(target_company, status, risk_level, required_before_execution, request_json, plan_json, "
                        "gate_json, execution_bundle_json, matching_request_json) VALUES "
                        "('Legacy Co', 'pending', 'medium', 1, '{}', '{}', '{}', '{}', '{}') RETURNING review_id"
                    )
                    legacy_review_id = cur.fetchone()[0]
                    cur.execute(
                        "INSERT INTO operation_runs "
                        "(operation_run_id, action_id, owner_module, operation_type, idempotency_key) "
                        "VALUES ('legacy-root-run', 'legacy-action', 'legacy-owner', 'legacy-operation', "
                        "'legacy-root-run')"
                    )
                conn.commit()

        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:3])
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'plan_review_sessions' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_SCOPED_SESSION_COLUMNS]),
                )
                session_columns = cur.fetchall()
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'operation_runs' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_OPERATION_ROOT_COLUMNS]),
                )
                operation_columns = cur.fetchall()
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, workspace_id, scope_issuer, scope_digest, "
                    "creation_source_workflow_command_id, creation_source_event_id, creation_plan_id, "
                    "creation_plan_revision, creation_plan_bundle_digest, creation_idempotency_key "
                    "FROM plan_review_sessions WHERE review_id = %s",
                    (legacy_review_id,),
                )
                session_sentinel = cur.fetchone()
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, scope_issuer, scope_digest, "
                    "coordination_plan_review_id FROM operation_runs "
                    "WHERE operation_run_id = 'legacy-root-run'"
                )
                operation_sentinel = cur.fetchone()
                cur.execute(
                    "SELECT t.relname, conname, convalidated, pg_get_constraintdef(c.oid) "
                    "FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
                    "JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY t.relname, conname",
                    (
                        schema,
                        [*_D3_SCOPED_SESSION_CHECKS, *_D3_OPERATION_ROOT_CHECKS],
                    ),
                )
                checks = cur.fetchall()

        self.assertEqual(
            result.applied,
            [
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(session_columns, list(_D3_SCOPED_SESSION_COLUMNS))
        self.assertEqual(operation_columns, list(_D3_OPERATION_ROOT_COLUMNS))
        self.assertEqual(session_sentinel, ("", "", "", "", "", "", "", "", 0, "", ""))
        self.assertEqual(operation_sentinel, ("", "", "", "", None))
        expected_checks = {
            **{("plan_review_sessions", name): fragments for name, fragments in _D3_SCOPED_SESSION_CHECKS.items()},
            **{("operation_runs", name): fragments for name, fragments in _D3_OPERATION_ROOT_CHECKS.items()},
        }
        self.assertEqual(
            [(table, name, validated) for table, name, validated, _definition in checks],
            [(table, name, False) for table, name in sorted(expected_checks)],
        )
        for table, name, _validated, definition in checks:
            normalized_definition = " ".join(str(definition).split()).casefold()
            for required_fragment in expected_checks[(table, name)]:
                self.assertIn(required_fragment.casefold(), normalized_definition, (name, definition))

        digest_a = "a" * 64
        digest_b = "b" * 64
        digest_c = "c" * 64
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "UPDATE plan_review_sessions SET runtime_namespace = 'runtime-a', provider_mode = 'scripted', "
                    "workspace_id = 'workspace-a', scope_issuer = 'plan_review_session', scope_digest = %s, "
                    "creation_source_workflow_command_id = 'source-command-a', "
                    "creation_source_event_id = 'source-event-a', creation_plan_id = 'plan-a', "
                    "creation_plan_revision = 1, creation_plan_bundle_digest = %s, creation_idempotency_key = %s "
                    "WHERE review_id = %s",
                    (digest_a, digest_b, digest_c, legacy_review_id),
                )
                cur.execute(
                    "UPDATE operation_runs SET runtime_namespace = 'runtime-a', provider_mode = 'scripted', "
                    "scope_issuer = 'plan_review_session', scope_digest = %s, coordination_plan_review_id = %s "
                    "WHERE operation_run_id = 'legacy-root-run'",
                    (digest_a, legacy_review_id),
                )
                cur.execute("SELECT * FROM plan_review_sessions WHERE review_id = %s", (legacy_review_id,))
                raw_session = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))
                cur.execute("SELECT * FROM operation_runs WHERE operation_run_id = 'legacy-root-run'")
                raw_operation = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))

        from sourcing_agent.repositories.workflow_runtime import OPERATION_RUNS
        from sourcing_agent.storage import ControlPlaneStore

        public_session = ControlPlaneStore._plan_review_session_from_row(
            ControlPlaneStore.__new__(ControlPlaneStore), raw_session
        )
        public_operation = OPERATION_RUNS.from_row(raw_operation)
        for field_name, _data_type, _nullable, _default in _D3_SCOPED_SESSION_COLUMNS:
            self.assertNotIn(field_name, public_session)
        for field_name, _data_type, _nullable, _default in _D3_OPERATION_ROOT_COLUMNS:
            self.assertNotIn(field_name, public_operation)

        invalid_updates = (
            ("plan_review_sessions", "runtime_namespace = '   '", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "provider_mode = 'fake'", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "workspace_id = '   '", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "scope_issuer = 'operation_run'", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "scope_digest = 'BAD'", "review_id = %s", (legacy_review_id,)),
            (
                "plan_review_sessions",
                "creation_source_workflow_command_id = '   '",
                "review_id = %s",
                (legacy_review_id,),
            ),
            ("plan_review_sessions", "creation_source_event_id = '   '", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "creation_plan_id = '   '", "review_id = %s", (legacy_review_id,)),
            ("plan_review_sessions", "creation_plan_revision = -1", "review_id = %s", (legacy_review_id,)),
            (
                "plan_review_sessions",
                "creation_plan_bundle_digest = 'BAD'",
                "review_id = %s",
                (legacy_review_id,),
            ),
            (
                "plan_review_sessions",
                "creation_idempotency_key = 'BAD'",
                "review_id = %s",
                (legacy_review_id,),
            ),
            (
                "operation_runs",
                "runtime_namespace = '   '",
                "operation_run_id = %s",
                ("legacy-root-run",),
            ),
            ("operation_runs", "provider_mode = 'fake'", "operation_run_id = %s", ("legacy-root-run",)),
            (
                "operation_runs",
                "scope_issuer = 'operation_run'",
                "operation_run_id = %s",
                ("legacy-root-run",),
            ),
            ("operation_runs", "scope_digest = 'BAD'", "operation_run_id = %s", ("legacy-root-run",)),
            (
                "operation_runs",
                "coordination_plan_review_id = 0",
                "operation_run_id = %s",
                ("legacy-root-run",),
            ),
        )
        for table_name, assignment, where_sql, params in invalid_updates:
            with self.subTest(table=table_name, assignment=assignment):
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        with self.assertRaises(psycopg.errors.CheckViolation):
                            cur.execute(f"UPDATE {table_name} SET {assignment} WHERE {where_sql}", params)

    def test_d3_scoped_root_foundation_lock_wait_is_bounded_and_rolls_back_both_tables(self) -> None:
        schema = self._fresh_schema("d3_scoped_root_lock")
        quoted = quote_control_plane_postgres_identifier(schema)
        blocker_ready = threading.Event()
        release_blocker = threading.Event()

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 3)
            with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
                prefix_result = mr.apply_pending_migrations(
                    setup,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:3])

        def hold_operation_run_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as blocker:
                with blocker.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO operation_runs "
                        "(operation_run_id, action_id, owner_module, operation_type, idempotency_key) "
                        "VALUES ('lock-root-run', 'lock-action', 'lock-owner', 'lock-operation', 'lock-root-run')"
                    )
                    blocker_ready.set()
                    release_blocker.wait(timeout=15)
                blocker.rollback()

        thread = threading.Thread(target=hold_operation_run_write, daemon=True)
        thread.start()
        self.assertTrue(blocker_ready.wait(timeout=5), "blocking OperationRun writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    mr.apply_pending_migrations(conn, schema=schema)
        finally:
            release_blocker.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "blocking OperationRun writer did not exit")
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 8.0)

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND ((table_name = 'plan_review_sessions' AND column_name = ANY(%s)) "
                    "OR (table_name = 'operation_runs' AND column_name = ANY(%s))) ORDER BY 1, 2",
                    (
                        schema,
                        [name for name, _data_type, _nullable, _default in _D3_SCOPED_SESSION_COLUMNS],
                        [name for name, _data_type, _nullable, _default in _D3_OPERATION_ROOT_COLUMNS],
                    ),
                )
                columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY conname",
                    (schema, [*_D3_SCOPED_SESSION_CHECKS, *_D3_OPERATION_ROOT_CHECKS]),
                )
                checks = cur.fetchall()

        self.assertEqual(ledger, _ALL_MIGRATIONS[:3])
        self.assertEqual(columns, [], "0004 timeout must roll back both tables' columns")
        self.assertEqual(checks, [], "0004 timeout must roll back both tables' checks")

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            recovered = mr.apply_pending_migrations(conn, schema=schema)
            again = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(
            recovered.applied,
            [
                _D3_SCOPED_ROOT_MIGRATION,
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(again.applied, [])
        self.assertEqual(again.already_applied, _ALL_MIGRATIONS)

    def test_d3_activity_claim_chain_foundation_installs_on_populated_tables_and_guards_new_writes(self) -> None:
        schema = self._fresh_schema("d3_activity_chain")
        quoted = quote_control_plane_postgres_identifier(schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 4)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix_result = mr.apply_pending_migrations(
                    conn,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_activity_runs "
                        "(activity_run_id, workspace_id, workflow_run_id, operation_run_id, command_id, "
                        "activity_type, idempotency_key) VALUES "
                        "('legacy-activity', 'workspace-a', 'workflow-a', 'operation-a', 'command-a', "
                        "'company_identity_verification', 'legacy-activity')"
                    )
                    cur.execute(
                        "INSERT INTO workflow_activity_attempts "
                        "(attempt_id, workspace_id, activity_run_id, workflow_run_id, command_id, "
                        "attempt_number, idempotency_key) VALUES "
                        "('legacy-attempt', 'workspace-a', 'legacy-activity', 'workflow-a', 'command-a', "
                        "7, 'legacy-attempt')"
                    )
                conn.commit()

        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:4])
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_activity_runs' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_ACTIVITY_RUN_COLUMNS]),
                )
                run_columns = cur.fetchall()
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_activity_attempts' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_ACTIVITY_ATTEMPT_COLUMNS]),
                )
                attempt_columns = cur.fetchall()
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, scope_digest, coordination_plan_review_id, "
                    "claim_authority_spec_digest, d3_business_fence_digest "
                    "FROM workflow_activity_runs WHERE activity_run_id = 'legacy-activity'"
                )
                run_sentinel = cur.fetchone()
                cur.execute(
                    "SELECT operation_run_id, runtime_namespace, provider_mode, scope_digest, "
                    "coordination_plan_review_id, claim_authority_spec_digest, d3_business_fence_digest, "
                    "claim_generation, command_attempt, control_epoch "
                    "FROM workflow_activity_attempts WHERE attempt_id = 'legacy-attempt'"
                )
                attempt_sentinel = cur.fetchone()
                cur.execute(
                    "SELECT t.relname, conname, convalidated, pg_get_constraintdef(c.oid) "
                    "FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
                    "JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY t.relname, conname",
                    (
                        schema,
                        [*_D3_ACTIVITY_RUN_CHECKS, *_D3_ACTIVITY_ATTEMPT_CHECKS],
                    ),
                )
                checks = cur.fetchall()

        self.assertEqual(
            result.applied,
            [
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(run_columns, list(_D3_ACTIVITY_RUN_COLUMNS))
        self.assertEqual(attempt_columns, list(_D3_ACTIVITY_ATTEMPT_COLUMNS))
        self.assertEqual(run_sentinel, ("", "", "", None, "", ""))
        self.assertEqual(attempt_sentinel, ("", "", "", "", None, "", "", 0, 0, 0))
        expected_checks = {
            **{("workflow_activity_runs", name): fragments for name, fragments in _D3_ACTIVITY_RUN_CHECKS.items()},
            **{
                ("workflow_activity_attempts", name): fragments
                for name, fragments in _D3_ACTIVITY_ATTEMPT_CHECKS.items()
            },
        }
        self.assertEqual(
            [(table, name, validated) for table, name, validated, _definition in checks],
            [(table, name, False) for table, name in sorted(expected_checks)],
        )
        for table, name, _validated, definition in checks:
            normalized_definition = " ".join(str(definition).split()).casefold()
            for required_fragment in expected_checks[(table, name)]:
                self.assertIn(required_fragment.casefold(), normalized_definition, (name, definition))

        digest_a = "a" * 64
        digest_b = "b" * 64
        digest_c = "c" * 64
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "UPDATE workflow_activity_runs SET runtime_namespace = 'runtime-a', "
                    "provider_mode = 'scripted', scope_digest = %s, coordination_plan_review_id = 1, "
                    "claim_authority_spec_digest = %s, d3_business_fence_digest = %s "
                    "WHERE activity_run_id = 'legacy-activity'",
                    (digest_a, digest_b, digest_c),
                )
                cur.execute(
                    "UPDATE workflow_activity_attempts SET operation_run_id = 'operation-a', "
                    "runtime_namespace = 'runtime-a', provider_mode = 'scripted', scope_digest = %s, "
                    "coordination_plan_review_id = 1, claim_authority_spec_digest = %s, "
                    "d3_business_fence_digest = %s, claim_generation = 2, command_attempt = 3, "
                    "control_epoch = 4 WHERE attempt_id = 'legacy-attempt'",
                    (digest_a, digest_b, digest_c),
                )
                cur.execute("SELECT * FROM workflow_activity_runs WHERE activity_run_id = 'legacy-activity'")
                raw_run = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))
                cur.execute("SELECT * FROM workflow_activity_attempts WHERE attempt_id = 'legacy-attempt'")
                raw_attempt = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))
                cur.execute(
                    "SELECT attempt_number, command_attempt FROM workflow_activity_attempts "
                    "WHERE attempt_id = 'legacy-attempt'"
                )
                retry_and_claim_attempt = cur.fetchone()

        from sourcing_agent.repositories.workflow_runtime import (
            WORKFLOW_ACTIVITY_ATTEMPTS,
            WORKFLOW_ACTIVITY_RUNS,
        )

        mapped_run = WORKFLOW_ACTIVITY_RUNS.from_row(raw_run)
        mapped_attempt = WORKFLOW_ACTIVITY_ATTEMPTS.from_row(raw_attempt)
        for field_name, _data_type, _nullable, _default in _D3_ACTIVITY_RUN_COLUMNS:
            self.assertNotIn(field_name, mapped_run)
        for field_name, _data_type, _nullable, _default in _D3_ACTIVITY_ATTEMPT_COLUMNS:
            self.assertNotIn(field_name, mapped_attempt)
        self.assertEqual(retry_and_claim_attempt, (7, 3), "attempt_number and command_attempt must stay distinct")

        invalid_updates = (
            ("workflow_activity_runs", "runtime_namespace = '   '", "activity_run_id = 'legacy-activity'"),
            ("workflow_activity_runs", "provider_mode = 'fake'", "activity_run_id = 'legacy-activity'"),
            ("workflow_activity_runs", "workspace_id = '   '", "activity_run_id = 'legacy-activity'"),
            ("workflow_activity_runs", "scope_digest = 'BAD'", "activity_run_id = 'legacy-activity'"),
            (
                "workflow_activity_runs",
                "coordination_plan_review_id = 0",
                "activity_run_id = 'legacy-activity'",
            ),
            (
                "workflow_activity_runs",
                "claim_authority_spec_digest = 'BAD'",
                "activity_run_id = 'legacy-activity'",
            ),
            (
                "workflow_activity_runs",
                "d3_business_fence_digest = 'BAD'",
                "activity_run_id = 'legacy-activity'",
            ),
            (
                "workflow_activity_attempts",
                "operation_run_id = '   '",
                "attempt_id = 'legacy-attempt'",
            ),
            (
                "workflow_activity_attempts",
                "runtime_namespace = '   '",
                "attempt_id = 'legacy-attempt'",
            ),
            ("workflow_activity_attempts", "provider_mode = 'fake'", "attempt_id = 'legacy-attempt'"),
            ("workflow_activity_attempts", "workspace_id = '   '", "attempt_id = 'legacy-attempt'"),
            ("workflow_activity_attempts", "scope_digest = 'BAD'", "attempt_id = 'legacy-attempt'"),
            (
                "workflow_activity_attempts",
                "coordination_plan_review_id = 0",
                "attempt_id = 'legacy-attempt'",
            ),
            (
                "workflow_activity_attempts",
                "claim_authority_spec_digest = 'BAD'",
                "attempt_id = 'legacy-attempt'",
            ),
            (
                "workflow_activity_attempts",
                "d3_business_fence_digest = 'BAD'",
                "attempt_id = 'legacy-attempt'",
            ),
            ("workflow_activity_attempts", "claim_generation = -1", "attempt_id = 'legacy-attempt'"),
            ("workflow_activity_attempts", "command_attempt = -1", "attempt_id = 'legacy-attempt'"),
            ("workflow_activity_attempts", "control_epoch = -1", "attempt_id = 'legacy-attempt'"),
        )
        for table_name, assignment, where_sql in invalid_updates:
            with self.subTest(table=table_name, assignment=assignment):
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        with self.assertRaises(psycopg.errors.CheckViolation):
                            cur.execute(f"UPDATE {table_name} SET {assignment} WHERE {where_sql}")

    def test_d3_activity_claim_chain_lock_wait_rolls_back_both_tables_and_recovers_once(self) -> None:
        schema = self._fresh_schema("d3_activity_chain_lock")
        quoted = quote_control_plane_postgres_identifier(schema)
        blocker_ready = threading.Event()
        release_blocker = threading.Event()

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 4)
            with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
                prefix_result = mr.apply_pending_migrations(
                    setup,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:4])

        def hold_activity_attempt_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as blocker:
                with blocker.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_activity_attempts "
                        "(attempt_id, workspace_id, activity_run_id, workflow_run_id, command_id, idempotency_key) "
                        "VALUES ('lock-attempt', 'workspace-a', 'activity-a', 'workflow-a', 'command-a', "
                        "'lock-attempt')"
                    )
                    blocker_ready.set()
                    release_blocker.wait(timeout=15)
                blocker.rollback()

        thread = threading.Thread(target=hold_activity_attempt_write, daemon=True)
        thread.start()
        self.assertTrue(blocker_ready.wait(timeout=5), "blocking ActivityAttempt writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    mr.apply_pending_migrations(conn, schema=schema)
        finally:
            release_blocker.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "blocking ActivityAttempt writer did not exit")
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 8.0)

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND ((table_name = 'workflow_activity_runs' AND column_name = ANY(%s)) "
                    "OR (table_name = 'workflow_activity_attempts' AND column_name = ANY(%s))) ORDER BY 1, 2",
                    (
                        schema,
                        [name for name, _data_type, _nullable, _default in _D3_ACTIVITY_RUN_COLUMNS],
                        [name for name, _data_type, _nullable, _default in _D3_ACTIVITY_ATTEMPT_COLUMNS],
                    ),
                )
                columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY conname",
                    (schema, [*_D3_ACTIVITY_RUN_CHECKS, *_D3_ACTIVITY_ATTEMPT_CHECKS]),
                )
                checks = cur.fetchall()

        self.assertEqual(ledger, _ALL_MIGRATIONS[:4])
        self.assertEqual(columns, [], "0005 timeout must roll back both tables' columns")
        self.assertEqual(checks, [], "0005 timeout must roll back both tables' checks")

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            recovered = mr.apply_pending_migrations(conn, schema=schema)
            again = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(
            recovered.applied,
            [
                _D3_ACTIVITY_MIGRATION,
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(again.applied, [])
        self.assertEqual(again.already_applied, _ALL_MIGRATIONS)

    def test_d3_event_terminal_lineage_foundation_installs_on_populated_table_and_guards_new_writes(self) -> None:
        schema = self._fresh_schema("d3_event_lineage")
        quoted = quote_control_plane_postgres_identifier(schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 5)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix_result = mr.apply_pending_migrations(
                    conn,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_events "
                        "(event_id, workflow_run_id, event_family, event_type, idempotency_key) "
                        "VALUES ('legacy-event', 'workflow-a', 'activity', 'activity_completed', "
                        "'legacy-event')"
                    )
                conn.commit()

        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:5])
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_events' "
                    "AND column_name = ANY(%s) ORDER BY ordinal_position",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_EVENT_COLUMNS]),
                )
                columns = cur.fetchall()
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, workspace_id, scope_digest, "
                    "coordination_plan_review_id, activity_run_id, claim_generation, control_epoch, "
                    "claim_authority_spec_digest, d3_business_fence_digest, terminal_outcome_digest "
                    "FROM workflow_events WHERE event_id = 'legacy-event'"
                )
                legacy_sentinel = cur.fetchone()
                cur.execute(
                    "INSERT INTO workflow_events "
                    "(event_id, workflow_run_id, event_family, event_type, idempotency_key) "
                    "VALUES ('current-shape-event', 'workflow-b', 'activity', 'activity_completed', "
                    "'current-shape-event')"
                )
                cur.execute(
                    "SELECT runtime_namespace, provider_mode, workspace_id, scope_digest, "
                    "coordination_plan_review_id, activity_run_id, claim_generation, control_epoch, "
                    "claim_authority_spec_digest, d3_business_fence_digest, terminal_outcome_digest "
                    "FROM workflow_events WHERE event_id = 'current-shape-event'"
                )
                current_writer_sentinel = cur.fetchone()
                cur.execute(
                    "SELECT conname, convalidated, pg_get_constraintdef(c.oid) FROM pg_constraint c "
                    "JOIN pg_class t ON t.oid = c.conrelid "
                    "JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "WHERE n.nspname = %s AND t.relname = 'workflow_events' "
                    "AND conname = ANY(%s) ORDER BY conname",
                    (schema, list(_D3_EVENT_CHECKS)),
                )
                checks = cur.fetchall()
            conn.commit()

        sentinel = ("", "", "", "", None, "", 0, 0, "", "", None)
        self.assertEqual(
            result.applied,
            [
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(columns, list(_D3_EVENT_COLUMNS))
        self.assertEqual(legacy_sentinel, sentinel)
        self.assertEqual(current_writer_sentinel, sentinel)
        self.assertEqual(
            [(name, validated) for name, validated, _definition in checks],
            [(name, False) for name in sorted(_D3_EVENT_CHECKS)],
        )
        for name, _validated, definition in checks:
            normalized_definition = " ".join(str(definition).split()).casefold()
            for required_fragment in _D3_EVENT_CHECKS[name]:
                self.assertIn(required_fragment.casefold(), normalized_definition, (name, definition))

        digest_a = "a" * 64
        digest_b = "b" * 64
        digest_c = "c" * 64
        digest_d = "d" * 64
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "UPDATE workflow_events SET runtime_namespace = 'runtime-a', provider_mode = 'scripted', "
                    "workspace_id = 'workspace-a', scope_digest = %s, coordination_plan_review_id = 1, "
                    "activity_run_id = 'activity-a', claim_generation = 2, control_epoch = 3, "
                    "claim_authority_spec_digest = %s, d3_business_fence_digest = %s, "
                    "terminal_outcome_digest = %s WHERE event_id = 'legacy-event'",
                    (digest_a, digest_b, digest_c, digest_d),
                )
                cur.execute("SELECT * FROM workflow_events WHERE event_id = 'legacy-event'")
                raw_event = dict(zip([column.name for column in cur.description], cur.fetchone(), strict=True))

        from sourcing_agent.repositories.workflow_runtime import WORKFLOW_EVENTS

        mapped_event = WORKFLOW_EVENTS.from_row(raw_event)
        self.assertEqual(len(WORKFLOW_EVENTS.columns), 17)
        for field_name, _data_type, _nullable, _default in _D3_EVENT_COLUMNS:
            self.assertNotIn(field_name, mapped_event)

        invalid_updates = (
            "runtime_namespace = '   '",
            "provider_mode = 'fake'",
            "workspace_id = '   '",
            "scope_digest = 'BAD'",
            "coordination_plan_review_id = 0",
            "activity_run_id = '   '",
            "claim_generation = -1",
            "control_epoch = -1",
            "claim_authority_spec_digest = 'BAD'",
            "d3_business_fence_digest = 'BAD'",
            "terminal_outcome_digest = 'BAD'",
        )
        for assignment in invalid_updates:
            with self.subTest(assignment=assignment):
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        with self.assertRaises(psycopg.errors.CheckViolation):
                            cur.execute(f"UPDATE workflow_events SET {assignment} WHERE event_id = 'legacy-event'")

    def test_d3_event_terminal_lineage_lock_wait_rolls_back_ledger_and_recovers_once(self) -> None:
        schema = self._fresh_schema("d3_event_lineage_lock")
        quoted = quote_control_plane_postgres_identifier(schema)
        blocker_ready = threading.Event()
        release_blocker = threading.Event()

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 5)
            with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
                prefix_result = mr.apply_pending_migrations(
                    setup,
                    schema=schema,
                    migrations_dir=migrations_dir,
                )
        self.assertEqual(prefix_result.applied, _ALL_MIGRATIONS[:5])

        def hold_workflow_event_write() -> None:
            with psycopg.connect(self.dsn, client_encoding="utf8") as blocker:
                with blocker.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        "INSERT INTO workflow_events "
                        "(event_id, workflow_run_id, event_family, event_type, idempotency_key) "
                        "VALUES ('lock-event', 'workflow-lock', 'activity', 'activity_completed', 'lock-event')"
                    )
                    blocker_ready.set()
                    release_blocker.wait(timeout=15)
                blocker.rollback()

        thread = threading.Thread(target=hold_workflow_event_write, daemon=True)
        thread.start()
        self.assertTrue(blocker_ready.wait(timeout=5), "blocking WorkflowEvent writer did not start")
        started = time.monotonic()
        try:
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    mr.apply_pending_migrations(conn, schema=schema)
        finally:
            release_blocker.set()
            thread.join(timeout=5)
        elapsed = time.monotonic() - started
        self.assertFalse(thread.is_alive(), "blocking WorkflowEvent writer did not exit")
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 8.0)

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'workflow_events' "
                    "AND column_name = ANY(%s) ORDER BY column_name",
                    (schema, [name for name, _data_type, _nullable, _default in _D3_EVENT_COLUMNS]),
                )
                columns = cur.fetchall()
                cur.execute(
                    "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
                    "WHERE n.nspname = %s AND conname = ANY(%s) ORDER BY conname",
                    (schema, list(_D3_EVENT_CHECKS)),
                )
                checks = cur.fetchall()

        self.assertEqual(ledger, _ALL_MIGRATIONS[:5])
        self.assertEqual(columns, [], "0006 timeout must roll back all WorkflowEvent columns")
        self.assertEqual(checks, [], "0006 timeout must roll back all WorkflowEvent checks")

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            recovered = mr.apply_pending_migrations(conn, schema=schema)
            again = mr.apply_pending_migrations(conn, schema=schema)
        self.assertEqual(
            recovered.applied,
            [
                _D3_EVENT_MIGRATION,
                _D0F_ENVELOPE_MIGRATION,
                _D1I_PARENT_UNIQUENESS_MIGRATION,
                _D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION,
                _D1N_ACQUISITION_PLAN_PREVIEW_MIGRATION,
            ],
        )
        self.assertEqual(again.applied, [])
        self.assertEqual(again.already_applied, _ALL_MIGRATIONS)

    def test_acquisition_root_child_shape_trigger_has_exact_scope(self) -> None:
        schema = self._fresh_schema("d1i_parent_unique")
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT indexdef FROM pg_indexes "
                    "WHERE schemaname = %s AND tablename = 'workflow_commands' "
                    "AND indexname = 'workflow_commands_parent_command_idx'",
                    (schema,),
                )
                support_index_row = cur.fetchone()
                cur.execute(
                    "SELECT indexdef FROM pg_indexes "
                    "WHERE schemaname = %s AND tablename = 'workflow_commands' "
                    "AND indexname = 'workflow_commands_acquisition_intent_parent_uk'",
                    (schema,),
                )
                old_unique_index_row = cur.fetchone()
                cur.execute(
                    "SELECT pg_get_triggerdef(t.oid) "
                    "FROM pg_trigger t "
                    "JOIN pg_class c ON c.oid = t.tgrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = 'workflow_commands' "
                    "AND t.tgname = 'workflow_commands_acquisition_root_child_shape_trg' "
                    "AND NOT t.tgisinternal",
                    (schema,),
                )
                trigger_row = cur.fetchone()
                cur.execute(
                    "SELECT pg_get_functiondef(p.oid) "
                    "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = %s AND p.proname = 'enforce_acquisition_root_child_shape'",
                    (schema,),
                )
                function_row = cur.fetchone()

        self.assertEqual(result.applied, _ALL_MIGRATIONS)
        self.assertIsNotNone(support_index_row)
        self.assertIsNone(old_unique_index_row)
        normalized = " ".join(str((support_index_row or [""])[0]).replace(f"{schema}.", "").split())
        self.assertEqual(
            normalized,
            "CREATE INDEX workflow_commands_parent_command_idx "
            "ON workflow_commands USING btree (parent_command_id) "
            "WHERE (parent_command_id <> ''::text)",
        )
        self.assertIsNotNone(trigger_row)
        self.assertIn(
            "BEFORE INSERT OR UPDATE OF command_id, command_type, parent_command_id",
            str((trigger_row or [""])[0]),
        )
        function_definition = str((function_row or [""])[0])
        self.assertIn("SET search_path", function_definition)
        self.assertIn("acquisition-root-child-shape-v1:", function_definition)
        self.assertIn("workflow_commands_acquisition_root_child_shape_ck", function_definition)
        self.assertIn("workflow_commands_acquisition_root_single_child_uk", function_definition)
        self.assertIn("NEW.command_type <> 'acquisition.intent.resolve'", function_definition)

    def test_acquisition_root_child_shape_is_scoped_and_closes_orphan_type_bypass(self) -> None:
        schema = self._fresh_schema("d1i_parent_shape")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    INSERT INTO workflow_commands (
                        command_id, workflow_run_id, operation_id, command_type,
                        owner, parent_command_id, idempotency_key
                    ) VALUES
                        ('cmd_generic_parent', 'wf_generic', 'op_generic',
                         'generic.multi.child.parent', 'generic-owner', '', 'generic-parent'),
                        ('cmd_generic_child_a', 'wf_generic', 'op_generic',
                         'generic.child.a', 'generic-owner', 'cmd_generic_parent', 'generic-child-a'),
                        ('cmd_generic_child_b', 'wf_generic', 'op_generic',
                         'generic.child.b', 'generic-owner', 'cmd_generic_parent', 'generic-child-b'),
                        ('cmd_generic_intent_child_a', 'wf_generic', 'op_generic',
                         'acquisition.intent.resolve', 'generic-owner',
                         'cmd_generic_parent', 'generic-intent-child-a'),
                        ('cmd_generic_intent_child_b', 'wf_generic', 'op_generic',
                         'acquisition.intent.resolve', 'generic-owner',
                         'cmd_generic_parent', 'generic-intent-child-b')
                    """
                )
                cur.execute(
                    """
                    INSERT INTO workflow_commands (
                        command_id, workflow_run_id, operation_id, command_type,
                        owner, parent_command_id, idempotency_key
                    ) VALUES (
                        'cmd_orphan_wrong_type', 'wf_orphan', 'op_orphan',
                        'generic.child.bypass', 'unknown-producer',
                        'cmd_future_acquisition_root', 'orphan-wrong-type'
                    )
                    """
                )
                with self.assertRaises(psycopg.errors.CheckViolation) as orphan_bypass:
                    cur.execute(
                        """
                        INSERT INTO workflow_commands (
                            command_id, workflow_run_id, operation_id, command_type,
                            owner, idempotency_key
                        ) VALUES (
                            'cmd_future_acquisition_root', 'wf_orphan', 'op_orphan',
                            'acquisition.run.create', 'acquisition_run_writer', 'future-root'
                        )
                        """
                    )
            conn.rollback()

        self.assertEqual(
            orphan_bypass.exception.diag.constraint_name,
            "workflow_commands_acquisition_root_child_shape_ck",
        )

    def test_acquisition_root_child_shape_blocks_update_bypasses(self) -> None:
        schema = self._fresh_schema("d1i_parent_update")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    INSERT INTO workflow_commands (
                        command_id, workflow_run_id, operation_id, command_type,
                        owner, parent_command_id, idempotency_key
                    ) VALUES
                        ('cmd_update_root', 'wf_update', 'op_update',
                         'acquisition.run.create', 'acquisition_run_writer', '', 'update-root'),
                        ('cmd_update_intent_child', 'wf_update', 'op_update',
                         'acquisition.intent.resolve', 'acquisition_planner',
                         'cmd_update_root', 'update-intent-child'),
                        ('cmd_update_generic_parent', 'wf_update', 'op_update',
                         'generic.multi.child.parent', 'generic-owner', '', 'update-generic-parent'),
                        ('cmd_update_moving_child', 'wf_update', 'op_update',
                         'acquisition.intent.resolve', 'generic-owner',
                         'cmd_update_generic_parent', 'update-moving-child'),
                        ('cmd_update_wrong_type_child', 'wf_update', 'op_update',
                         'generic.child.bypass', 'generic-owner',
                         'cmd_update_generic_parent', 'update-wrong-type-child'),
                        ('cmd_update_future_root', 'wf_update', 'op_update',
                         'generic.multi.child.parent', 'generic-owner', '', 'update-future-root'),
                        ('cmd_update_future_child_a', 'wf_update', 'op_update',
                         'acquisition.intent.resolve', 'generic-owner',
                         'cmd_update_future_root', 'update-future-child-a'),
                        ('cmd_update_future_child_b', 'wf_update', 'op_update',
                         'acquisition.intent.resolve', 'generic-owner',
                         'cmd_update_future_root', 'update-future-child-b')
                    """
                )

                cur.execute("SAVEPOINT move_child")
                with self.assertRaises(psycopg.errors.UniqueViolation) as move_child:
                    cur.execute(
                        "UPDATE workflow_commands "
                        "SET parent_command_id = 'cmd_update_root' "
                        "WHERE command_id = 'cmd_update_moving_child'"
                    )
                self.assertEqual(
                    move_child.exception.diag.constraint_name,
                    "workflow_commands_acquisition_root_single_child_uk",
                )
                cur.execute("ROLLBACK TO SAVEPOINT move_child")

                cur.execute("SAVEPOINT wrong_type")
                with self.assertRaises(psycopg.errors.CheckViolation) as wrong_type:
                    cur.execute(
                        "UPDATE workflow_commands "
                        "SET parent_command_id = 'cmd_update_root' "
                        "WHERE command_id = 'cmd_update_wrong_type_child'"
                    )
                self.assertEqual(
                    wrong_type.exception.diag.constraint_name,
                    "workflow_commands_acquisition_root_child_shape_ck",
                )
                cur.execute("ROLLBACK TO SAVEPOINT wrong_type")

                cur.execute("SAVEPOINT promote_root")
                with self.assertRaises(psycopg.errors.UniqueViolation) as promote_root:
                    cur.execute(
                        "UPDATE workflow_commands "
                        "SET command_type = 'acquisition.run.create' "
                        "WHERE command_id = 'cmd_update_future_root'"
                    )
                self.assertEqual(
                    promote_root.exception.diag.constraint_name,
                    "workflow_commands_acquisition_root_single_child_uk",
                )
                cur.execute("ROLLBACK TO SAVEPOINT promote_root")
            conn.rollback()

    def test_acquisition_root_child_shape_migration_rejects_brownfield_wrong_type_child(self) -> None:
        schema = self._fresh_schema("d1i_parent_wrong_type")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 7)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        INSERT INTO workflow_commands (
                            command_id, workflow_run_id, operation_id, command_type,
                            owner, parent_command_id, idempotency_key
                        ) VALUES
                            ('cmd_brownfield_root', 'wf_brownfield', 'op_brownfield',
                             'acquisition.run.create', 'acquisition_run_writer', '', 'brownfield-root'),
                            ('cmd_brownfield_wrong_child', 'wf_brownfield', 'op_brownfield',
                             'generic.child.bypass', 'unknown-producer',
                             'cmd_brownfield_root', 'brownfield-wrong-child')
                        """
                    )
                conn.commit()
                with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                    mr.apply_pending_migrations(conn, schema=schema)

        self.assertEqual(prefix.applied, _ALL_MIGRATIONS[:-3])
        self.assertEqual(
            raised.exception.diag.constraint_name,
            "workflow_commands_acquisition_root_child_shape_ck",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT to_regclass('workflow_commands_parent_command_idx')")
                index_name = cur.fetchone()[0]
                cur.execute("SELECT to_regclass('workflow_commands_acquisition_intent_parent_uk')")
                old_unique_index_name = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM pg_trigger t "
                    "JOIN pg_class c ON c.oid = t.tgrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = 'workflow_commands' "
                    "AND t.tgname = 'workflow_commands_acquisition_root_child_shape_trg'",
                    (schema,),
                )
                trigger_count = int(cur.fetchone()[0])
        self.assertEqual(ledger, _ALL_MIGRATIONS[:-3])
        self.assertIsNone(index_name)
        self.assertIsNone(old_unique_index_name)
        self.assertEqual(trigger_count, 0)

    def test_acquisition_root_child_shape_migration_fails_closed_on_brownfield_duplicate_root_children(
        self,
    ) -> None:
        schema = self._fresh_schema("d1i_parent_duplicate")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 7)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        INSERT INTO workflow_commands (
                            command_id, workflow_run_id, operation_id, command_type,
                            owner, parent_command_id, idempotency_key
                        ) VALUES
                            ('cmd_brownfield_parent', 'wf_brownfield_root', 'op_brownfield_root',
                             'acquisition.run.create', 'acquisition_run_writer',
                             '', 'brownfield-parent'),
                            ('cmd_brownfield_intent_a', 'wf_brownfield_a', 'op_brownfield_a',
                             'acquisition.intent.resolve', 'acquisition_planner',
                             'cmd_brownfield_parent', 'brownfield-intent-a'),
                            ('cmd_brownfield_intent_b', 'wf_brownfield_b', 'op_brownfield_b',
                             'acquisition.intent.resolve', 'forged-owner',
                             'cmd_brownfield_parent', 'brownfield-intent-b')
                        """
                    )
                conn.commit()
                with self.assertRaises(psycopg.errors.UniqueViolation) as raised:
                    mr.apply_pending_migrations(conn, schema=schema)

        self.assertEqual(prefix.applied, _ALL_MIGRATIONS[:-3])
        self.assertEqual(
            raised.exception.diag.constraint_name,
            "workflow_commands_acquisition_root_single_child_uk",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute(
                    "SELECT COUNT(*) FROM workflow_commands "
                    "WHERE command_type = 'acquisition.intent.resolve' "
                    "AND parent_command_id = 'cmd_brownfield_parent'"
                )
                duplicate_count = int(cur.fetchone()[0])
                cur.execute("SELECT to_regclass('workflow_commands_parent_command_idx')")
                index_name = cur.fetchone()[0]
                cur.execute("SELECT to_regclass('workflow_commands_acquisition_intent_parent_uk')")
                old_unique_index_name = cur.fetchone()[0]
        self.assertEqual(ledger, _ALL_MIGRATIONS[:-3])
        self.assertEqual(duplicate_count, 2)
        self.assertIsNone(index_name)
        self.assertIsNone(old_unique_index_name)

    def test_acquisition_intent_parent_uniqueness_serializes_competing_transactions(self) -> None:
        schema = self._fresh_schema("d1i_parent_race")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
            mr.apply_pending_migrations(setup, schema=schema)
            with setup.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "INSERT INTO workflow_commands "
                    "(command_id, workflow_run_id, operation_id, command_type, owner, idempotency_key) "
                    "VALUES ('cmd_race_parent', 'wf_race_parent', 'op_race_parent', "
                    "'acquisition.run.create', 'acquisition_run_writer', 'race-parent')"
                )
            setup.commit()

        second_started = threading.Event()
        second_finished = threading.Event()
        second_outcome: dict[str, object] = {}

        def insert_competing_child() -> None:
            try:
                with psycopg.connect(
                    self.dsn,
                    client_encoding="utf8",
                    application_name="d1i-parent-unique-t2",
                ) as second:
                    with second.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        second_started.set()
                        cur.execute(
                            """
                            INSERT INTO workflow_commands (
                                command_id, workflow_run_id, operation_id, command_type,
                                owner, parent_command_id, idempotency_key
                            ) VALUES (%s, %s, %s, 'acquisition.intent.resolve', %s, %s, %s)
                            """,
                            (
                                "cmd_race_child_b",
                                "wf_race_b",
                                "op_race_b",
                                "forged-owner",
                                "cmd_race_parent",
                                "race-child-b",
                            ),
                        )
                    second.commit()
                    second_outcome["status"] = "committed"
            except Exception as exc:  # pragma: no cover - asserted below
                second_outcome["error"] = exc
            finally:
                second_finished.set()

        with psycopg.connect(self.dsn, client_encoding="utf8") as first:
            with first.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    INSERT INTO workflow_commands (
                        command_id, workflow_run_id, operation_id, command_type,
                        owner, parent_command_id, idempotency_key
                    ) VALUES (%s, %s, %s, 'acquisition.intent.resolve', %s, %s, %s)
                    """,
                    (
                        "cmd_race_child_a",
                        "wf_race_a",
                        "op_race_a",
                        "acquisition_planner",
                        "cmd_race_parent",
                        "race-child-a",
                    ),
                )

            thread = threading.Thread(target=insert_competing_child, daemon=True)
            thread.start()
            self.assertTrue(second_started.wait(timeout=5), "second parent-unique writer did not start")
            deadline = time.monotonic() + 5
            observed_lock_wait = False
            while time.monotonic() < deadline and not observed_lock_wait:
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as observer:
                    with observer.cursor() as cur:
                        cur.execute(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE application_name = 'd1i-parent-unique-t2'"
                        )
                        observed_lock_wait = any(row[0] == "Lock" for row in cur.fetchall())
                if not observed_lock_wait:
                    time.sleep(0.02)
            self.assertTrue(observed_lock_wait, "second writer never waited on the unique parent fence")
            self.assertFalse(second_finished.is_set())
            first.commit()

        self.assertTrue(second_finished.wait(timeout=5), "second parent-unique writer did not finish")
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(second_outcome.get("error"), psycopg.errors.UniqueViolation)
        self.assertNotIn("status", second_outcome)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT COUNT(*) FROM workflow_commands "
                    "WHERE command_type = 'acquisition.intent.resolve' "
                    "AND parent_command_id = 'cmd_race_parent'"
                )
                child_count = int(cur.fetchone()[0])
        self.assertEqual(child_count, 1)

    def test_acquisition_root_child_shape_serializes_a_different_type_competitor(self) -> None:
        schema = self._fresh_schema("d1i_parent_type_race")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as setup:
            mr.apply_pending_migrations(setup, schema=schema)
            with setup.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "INSERT INTO workflow_commands "
                    "(command_id, workflow_run_id, operation_id, command_type, owner, idempotency_key) "
                    "VALUES ('cmd_type_race_parent', 'wf_type_race', 'op_type_race', "
                    "'acquisition.run.create', 'acquisition_run_writer', 'type-race-parent')"
                )
            setup.commit()

        second_started = threading.Event()
        second_finished = threading.Event()
        second_outcome: dict[str, object] = {}

        def insert_different_type_child() -> None:
            try:
                with psycopg.connect(
                    self.dsn,
                    client_encoding="utf8",
                    application_name="d1i-parent-type-t2",
                ) as second:
                    with second.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        second_started.set()
                        cur.execute(
                            """
                            INSERT INTO workflow_commands (
                                command_id, workflow_run_id, operation_id, command_type,
                                owner, parent_command_id, idempotency_key
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                "cmd_type_race_wrong_child",
                                "wf_type_race",
                                "op_type_race",
                                "generic.child.bypass",
                                "unknown-producer",
                                "cmd_type_race_parent",
                                "type-race-wrong-child",
                            ),
                        )
                    second.commit()
                    second_outcome["status"] = "committed"
            except Exception as exc:  # pragma: no cover - asserted below
                second_outcome["error"] = exc
            finally:
                second_finished.set()

        with psycopg.connect(self.dsn, client_encoding="utf8") as first:
            with first.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    INSERT INTO workflow_commands (
                        command_id, workflow_run_id, operation_id, command_type,
                        owner, parent_command_id, idempotency_key
                    ) VALUES (%s, %s, %s, 'acquisition.intent.resolve', %s, %s, %s)
                    """,
                    (
                        "cmd_type_race_intent_child",
                        "wf_type_race",
                        "op_type_race",
                        "acquisition_planner",
                        "cmd_type_race_parent",
                        "type-race-intent-child",
                    ),
                )

            thread = threading.Thread(target=insert_different_type_child, daemon=True)
            thread.start()
            self.assertTrue(second_started.wait(timeout=5), "different-type writer did not start")
            deadline = time.monotonic() + 5
            observed_lock_wait = False
            while time.monotonic() < deadline and not observed_lock_wait:
                with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as observer:
                    with observer.cursor() as cur:
                        cur.execute(
                            "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = 'd1i-parent-type-t2'"
                        )
                        observed_lock_wait = any(row[0] == "Lock" for row in cur.fetchall())
                if not observed_lock_wait:
                    time.sleep(0.02)
            self.assertTrue(observed_lock_wait, "different-type writer never waited on the parent identity fence")
            self.assertFalse(second_finished.is_set())
            first.commit()

        self.assertTrue(second_finished.wait(timeout=5), "different-type writer did not finish")
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(second_outcome.get("error"), psycopg.errors.CheckViolation)
        self.assertEqual(
            getattr(second_outcome.get("error"), "diag", None).constraint_name,
            "workflow_commands_acquisition_root_child_shape_ck",
        )
        self.assertNotIn("status", second_outcome)
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT command_type FROM workflow_commands "
                    "WHERE parent_command_id = 'cmd_type_race_parent' ORDER BY command_id"
                )
                child_types = [row[0] for row in cur.fetchall()]
        self.assertEqual(child_types, ["acquisition.intent.resolve"])

    def test_company_public_web_asset_run_idempotency_migration_has_exact_partial_unique_scope(
        self,
    ) -> None:
        schema = self._fresh_schema("d1m_company_public_web_idempotency")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            result = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT indexdef FROM pg_indexes "
                    "WHERE schemaname = %s AND tablename = 'company_public_web_asset_runs' "
                    "AND indexname = 'company_public_web_asset_runs_idempotency_key_uk'",
                    (schema,),
                )
                index_row = cur.fetchone()
                cur.execute("SELECT to_regclass('company_public_web_source_projection_revision_seq')")
                revision_sequence_name = cur.fetchone()[0]
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-empty-a",
                    idempotency_key="   ",
                )
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-empty-b",
                    idempotency_key="   ",
                )
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-effective-a",
                    idempotency_key=" \t\nd1m-effective-key\r\f ",
                )
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-literal-v",
                    idempotency_key="d1m-literal-v",
                )
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-literal-without-v",
                    idempotency_key="d1m-literal-",
                )
                cur.execute("SAVEPOINT duplicate_effective_key")
                with self.assertRaises(psycopg.errors.UniqueViolation) as raised:
                    self._insert_company_public_web_asset_run(
                        cur,
                        run_id="d1m-effective-b",
                        idempotency_key="d1m-effective-key",
                    )
                self.assertEqual(
                    raised.exception.diag.constraint_name,
                    "company_public_web_asset_runs_idempotency_key_uk",
                )
                cur.execute("ROLLBACK TO SAVEPOINT duplicate_effective_key")
                self._insert_company_public_web_asset_run(
                    cur,
                    run_id="d1m-vt-a",
                    idempotency_key="\vd1m-vt-key\v",
                )
                cur.execute("SAVEPOINT duplicate_vt_key")
                with self.assertRaises(psycopg.errors.UniqueViolation) as vt_raised:
                    self._insert_company_public_web_asset_run(
                        cur,
                        run_id="d1m-vt-b",
                        idempotency_key="d1m-vt-key",
                    )
                self.assertEqual(
                    vt_raised.exception.diag.constraint_name,
                    "company_public_web_asset_runs_idempotency_key_uk",
                )
                cur.execute("ROLLBACK TO SAVEPOINT duplicate_vt_key")
            conn.rollback()

        self.assertEqual(result.applied, _ALL_MIGRATIONS)
        self.assertIsNotNone(index_row)
        self.assertEqual(revision_sequence_name, "company_public_web_source_projection_revision_seq")
        normalized_index = " ".join(str((index_row or [""])[0]).replace(f"{schema}.", "").split())
        self.assertIn("CREATE UNIQUE INDEX company_public_web_asset_runs_idempotency_key_uk", normalized_index)
        self.assertIn("btrim(idempotency_key,", normalized_index)
        self.assertIn("WHERE (btrim(idempotency_key,", normalized_index)

    def test_company_public_web_asset_run_idempotency_migration_fails_closed_on_brownfield_duplicates(
        self,
    ) -> None:
        schema = self._fresh_schema("d1m_company_public_web_brownfield_duplicate")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 8)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    self._insert_company_public_web_asset_run(
                        cur,
                        run_id="d1m-brownfield-a",
                        idempotency_key="d1m-brownfield-duplicate",
                    )
                    self._insert_company_public_web_asset_run(
                        cur,
                        run_id="d1m-brownfield-b",
                        idempotency_key="\t\nd1m-brownfield-duplicate\r\f",
                    )
                conn.commit()
                with self.assertRaises(psycopg.errors.UniqueViolation) as raised:
                    mr.apply_pending_migrations(conn, schema=schema)

        self.assertEqual(prefix.applied, _ALL_MIGRATIONS[:-2])
        self.assertEqual(
            raised.exception.diag.constraint_name,
            "company_public_web_asset_runs_idempotency_key_uk",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SELECT version FROM schema_migrations ORDER BY version")
                ledger = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT to_regclass('company_public_web_asset_runs_idempotency_key_uk')")
                index_name = cur.fetchone()[0]
                cur.execute("SELECT to_regclass('company_public_web_source_projection_revision_seq')")
                revision_sequence_name = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM company_public_web_asset_runs "
                    "WHERE btrim(idempotency_key, E' \\t\\n\\r\\f\\013') = 'd1m-brownfield-duplicate'"
                )
                duplicate_count = int(cur.fetchone()[0])
        self.assertEqual(ledger, _ALL_MIGRATIONS[:-2])
        self.assertIsNone(index_name)
        self.assertIsNone(revision_sequence_name)
        self.assertEqual(duplicate_count, 2)

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
