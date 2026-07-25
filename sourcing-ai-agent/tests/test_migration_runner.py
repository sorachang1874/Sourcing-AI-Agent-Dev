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
_D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION = "0011_agent_tool_result_slots"
_D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION = "0012_agent_tool_result_owner_revision_token"
_D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION = "0013_agent_tool_result_link_policy"
_D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION = (
    "0014_agent_tool_result_attempt_effect_contract"
)
_WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION = "0015_profile_refill_plan_division_id"
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
    _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
    _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
    _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
    _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
    _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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

    def _insert_agent_tool_pending_slot(
        self,
        cursor,
        *,
        result_slot_id: str,
        owner_target_revision_token: str | None = None,
        result_link_policy: str | None = "no_command_v1",
        tool_name: str = "inspect_operation",
        tool_kind: str = "query",
        effect_class: str = "read_only",
    ) -> None:
        token_column = ""
        token_value = ""
        policy_column = ""
        policy_value = ""
        parameters: dict[str, object] = {
            "result_slot_id": result_slot_id,
            "logical_occurrence_digest": uuid4().hex * 2,
            "tool_name": tool_name,
            "tool_kind": tool_kind,
            "effect_class": effect_class,
        }
        if owner_target_revision_token is not None:
            token_column = ", owner_target_revision_token"
            token_value = ", %(owner_target_revision_token)s"
            parameters["owner_target_revision_token"] = owner_target_revision_token
        if result_link_policy is not None:
            policy_column = ", result_link_policy"
            policy_value = ", %(result_link_policy)s"
            parameters["result_link_policy"] = result_link_policy
        cursor.execute(
            f"""
            INSERT INTO agent_tool_result_slots (
                result_slot_id, slot_generation, workspace_id, actor_id,
                runtime_namespace, provider_mode, turn_id, step_id,
                tool_name, tool_kind, effect_class,
                tool_spec_version, tool_spec_digest,
                canonical_args_json, canonical_args_digest, occurrence_ordinal,
                logical_occurrence_digest,
                request_schema_version, request_schema_digest,
                result_schema_version, result_schema_digest,
                serializer_owner, serializer_revision, serializer_contract_digest
                {token_column}{policy_column}
            ) VALUES (
                %(result_slot_id)s, 1, 'workspace-s1c', 'actor-s1c',
                'local_canary', 'simulate', 'turn-s1c', 'step-s1c',
                %(tool_name)s, %(tool_kind)s, %(effect_class)s,
                'inspect_operation_tool_v2', repeat('a', 64),
                '{{"operation_run_id":"operation-s1c"}}'::jsonb, repeat('b', 64), 1,
                %(logical_occurrence_digest)s,
                'inspect_operation_request_v1', repeat('c', 64),
                'inspect_operation_result_v2', repeat('d', 64),
                'operation_query_owner', 'operation_query_owner_v2', repeat('e', 64)
                {token_value}{policy_value}
            )
            """,
            parameters,
        )

    def _insert_agent_tool_result_attempt(
        self,
        cursor,
        *,
        result_slot_id: str,
        result_attempt_id: str,
        disposition: str,
        quarantine_reason: str,
        owner_target_revision: int,
        owner_target_generation: int,
        owner_target_revision_token: str | None,
        schema_version: str | None = None,
        result_link_policy: str | None = "no_command_v1",
        action_id: str = "",
        operation_run_id: str = "",
        workflow_command_id: str = "",
        activity_run_id: str = "",
        activity_attempt_id: str = "",
        command_attempt: int = 0,
        command_generation: int = 0,
        control_epoch: int = 0,
    ) -> None:
        token_column = ""
        token_value = ""
        schema_column = ""
        schema_value = ""
        policy_column = ""
        policy_value = ""
        parameters: dict[str, object] = {
            "result_slot_id": result_slot_id,
            "result_attempt_id": result_attempt_id,
            "disposition": disposition,
            "quarantine_reason": quarantine_reason,
            "owner_target_revision": owner_target_revision,
            "owner_target_generation": owner_target_generation,
            "action_id": action_id,
            "operation_run_id": operation_run_id,
            "workflow_command_id": workflow_command_id,
            "activity_run_id": activity_run_id,
            "activity_attempt_id": activity_attempt_id,
            "command_attempt": command_attempt,
            "command_generation": command_generation,
            "control_epoch": control_epoch,
        }
        if owner_target_revision_token is not None:
            token_column = ", owner_target_revision_token"
            token_value = ", %(owner_target_revision_token)s"
            parameters["owner_target_revision_token"] = owner_target_revision_token
        if schema_version is not None:
            schema_column = ", schema_version"
            schema_value = ", %(schema_version)s"
            parameters["schema_version"] = schema_version
        if result_link_policy is not None:
            policy_column = ", result_link_policy"
            policy_value = ", %(result_link_policy)s"
            parameters["result_link_policy"] = result_link_policy
        cursor.execute(
            f"""
            INSERT INTO agent_tool_result_attempts (
                result_attempt_id, result_slot_id, attempted_slot_generation,
                disposition, quarantine_reason, provider_call_id, tool_call_id,
                action_id, operation_run_id, workflow_command_id,
                activity_run_id, activity_attempt_id,
                command_attempt, command_generation, control_epoch,
                owner_target_kind, owner_target_id,
                owner_target_revision, owner_target_generation
                {token_column}{schema_column}{policy_column},
                terminal_winner_id, owner_result_ref_json, owner_result_digest,
                serialized_result_json, serialized_result_digest,
                tool_result_message_json, tool_result_message_digest, is_error
            ) VALUES (
                %(result_attempt_id)s, %(result_slot_id)s, 1,
                %(disposition)s, %(quarantine_reason)s, 'provider-call-s1c', 'tool-call-s1c',
                %(action_id)s, %(operation_run_id)s, %(workflow_command_id)s,
                %(activity_run_id)s, %(activity_attempt_id)s,
                %(command_attempt)s, %(command_generation)s, %(control_epoch)s,
                'projection_membership', 'projection-s1c',
                %(owner_target_revision)s, %(owner_target_generation)s
                {token_value}{schema_value}{policy_value},
                'winner-s1c', '{{"ref":"owner"}}'::jsonb, repeat('f', 64),
                '{{"ok":true}}', repeat('1', 64),
                '{{"content":"ok"}}'::jsonb, repeat('2', 64), FALSE
            )
            """,
            parameters,
        )

    def _insert_agent_tool_accepted_aggregate(
        self,
        cursor,
        *,
        result_slot_id: str,
        result_attempt_id: str,
        owner_target_revision: int,
        owner_target_generation: int,
        slot_token: str | None,
        attempt_token: str | None = None,
        journal_token: str | None = None,
        attempt_schema_version: str | None = None,
        journal_schema_version: str | None = None,
        result_link_policy: str | None = "no_command_v1",
        attempt_result_link_policy: str | None = None,
        journal_result_link_policy: str | None = None,
        omit_journal_result_link_policy: bool = False,
        action_id: str = "",
        operation_run_id: str = "",
        workflow_command_id: str = "",
        activity_run_id: str = "",
        activity_attempt_id: str = "",
        command_attempt: int = 0,
        command_generation: int = 0,
        control_epoch: int = 0,
        attempt_link_values: tuple[str, str, str, str, str, int, int, int] | None = None,
    ) -> None:
        effective_attempt_token = slot_token if attempt_token is None else attempt_token
        effective_journal_token = slot_token if journal_token is None else journal_token
        effective_attempt_policy = (
            result_link_policy if attempt_result_link_policy is None else attempt_result_link_policy
        )
        effective_journal_policy = None
        if not omit_journal_result_link_policy:
            effective_journal_policy = (
                result_link_policy if journal_result_link_policy is None else journal_result_link_policy
            )
        effective_attempt_links = attempt_link_values or (
            action_id,
            operation_run_id,
            workflow_command_id,
            activity_run_id,
            activity_attempt_id,
            command_attempt,
            command_generation,
            control_epoch,
        )
        self._insert_agent_tool_result_attempt(
            cursor,
            result_slot_id=result_slot_id,
            result_attempt_id=result_attempt_id,
            disposition="accepted",
            quarantine_reason="",
            owner_target_revision=owner_target_revision,
            owner_target_generation=owner_target_generation,
            owner_target_revision_token=effective_attempt_token,
            schema_version=attempt_schema_version,
            result_link_policy=effective_attempt_policy,
            action_id=effective_attempt_links[0],
            operation_run_id=effective_attempt_links[1],
            workflow_command_id=effective_attempt_links[2],
            activity_run_id=effective_attempt_links[3],
            activity_attempt_id=effective_attempt_links[4],
            command_attempt=effective_attempt_links[5],
            command_generation=effective_attempt_links[6],
            control_epoch=effective_attempt_links[7],
        )
        token_assignment = ""
        update_parameters: dict[str, object] = {
            "result_slot_id": result_slot_id,
            "result_attempt_id": result_attempt_id,
            "owner_target_revision": owner_target_revision,
            "owner_target_generation": owner_target_generation,
            "action_id": action_id,
            "operation_run_id": operation_run_id,
            "workflow_command_id": workflow_command_id,
            "activity_run_id": activity_run_id,
            "activity_attempt_id": activity_attempt_id,
            "command_attempt": command_attempt,
            "command_generation": command_generation,
            "control_epoch": control_epoch,
        }
        if slot_token is not None:
            token_assignment = ", owner_target_revision_token = %(slot_token)s"
            update_parameters["slot_token"] = slot_token
        cursor.execute(
            f"""
            UPDATE agent_tool_result_slots
            SET status = 'accepted',
                result_attempt_id = %(result_attempt_id)s,
                provider_call_id = 'provider-call-s1c',
                tool_call_id = 'tool-call-s1c',
                action_id = %(action_id)s,
                operation_run_id = %(operation_run_id)s,
                workflow_command_id = %(workflow_command_id)s,
                activity_run_id = %(activity_run_id)s,
                activity_attempt_id = %(activity_attempt_id)s,
                command_attempt = %(command_attempt)s,
                command_generation = %(command_generation)s,
                control_epoch = %(control_epoch)s,
                owner_target_kind = 'projection_membership',
                owner_target_id = 'projection-s1c',
                owner_target_revision = %(owner_target_revision)s,
                owner_target_generation = %(owner_target_generation)s
                {token_assignment},
                terminal_winner_id = 'winner-s1c',
                owner_result_ref_json = '{{"ref":"owner"}}'::jsonb,
                owner_result_digest = repeat('f', 64),
                serialized_result_json = '{{"ok":true}}',
                serialized_result_digest = repeat('1', 64),
                tool_result_message_json = '{{"content":"ok"}}'::jsonb,
                tool_result_message_digest = repeat('2', 64),
                is_error = FALSE,
                accepted_at = transaction_timestamp()
            WHERE result_slot_id = %(result_slot_id)s
            """,
            update_parameters,
        )
        token_column = ""
        token_value = ""
        schema_column = ""
        schema_value = ""
        policy_column = ""
        policy_value = ""
        journal_parameters: dict[str, object] = {
            "result_slot_id": result_slot_id,
            "journal_id": f"journal-{result_attempt_id}",
        }
        if effective_journal_token is not None:
            token_column = ", owner_target_revision_token"
            token_value = ", %(journal_token)s"
            journal_parameters["journal_token"] = effective_journal_token
        if journal_schema_version is not None:
            schema_column = ", schema_version"
            schema_value = ", %(schema_version)s"
            journal_parameters["schema_version"] = journal_schema_version
        if effective_journal_policy is not None:
            policy_column = ", result_link_policy"
            policy_value = ", %(result_link_policy)s"
            journal_parameters["result_link_policy"] = effective_journal_policy
        cursor.execute(
            f"""
            INSERT INTO agent_tool_result_journal (
                journal_id, result_slot_id, result_attempt_id,
                workspace_id, actor_id, runtime_namespace, provider_mode,
                turn_id, step_id, tool_name, tool_spec_version, tool_spec_digest,
                canonical_args_digest, occurrence_ordinal,
                request_schema_version, request_schema_digest,
                result_schema_version, result_schema_digest,
                serializer_owner, serializer_revision, serializer_contract_digest,
                action_id, operation_run_id, workflow_command_id,
                activity_run_id, activity_attempt_id,
                command_attempt, command_generation, control_epoch,
                owner_target_kind, owner_target_id,
                owner_target_revision, owner_target_generation
                {token_column}{schema_column}{policy_column},
                terminal_winner_id, owner_result_ref_json, owner_result_digest,
                serialized_result_json, serialized_result_digest,
                tool_result_message_json, tool_result_message_digest, is_error,
                accepted_at
            )
            SELECT
                %(journal_id)s, result_slot_id, result_attempt_id,
                workspace_id, actor_id, runtime_namespace, provider_mode,
                turn_id, step_id, tool_name, tool_spec_version, tool_spec_digest,
                canonical_args_digest, occurrence_ordinal,
                request_schema_version, request_schema_digest,
                result_schema_version, result_schema_digest,
                serializer_owner, serializer_revision, serializer_contract_digest,
                action_id, operation_run_id, workflow_command_id,
                activity_run_id, activity_attempt_id,
                command_attempt, command_generation, control_epoch,
                owner_target_kind, owner_target_id,
                owner_target_revision, owner_target_generation
                {token_value}{schema_value}{policy_value},
                terminal_winner_id, owner_result_ref_json, owner_result_digest,
                serialized_result_json, serialized_result_digest,
                tool_result_message_json, tool_result_message_digest, is_error,
                accepted_at
            FROM agent_tool_result_slots
            WHERE result_slot_id = %(result_slot_id)s
            """,
            journal_parameters,
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
        self.assertEqual(len(runner_fp["tables"]), 88)

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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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
                _D1N_AGENT_TOOL_RESULT_SLOT_MIGRATION,
                _D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION,
                _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                _WS7_S4_PROFILE_REFILL_PLAN_DIVISION_ID_MIGRATION,
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

        migrations_through_0007 = _ALL_MIGRATIONS[: _ALL_MIGRATIONS.index(_D1I_PARENT_UNIQUENESS_MIGRATION)]
        self.assertEqual(prefix.applied, migrations_through_0007)
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
        self.assertEqual(ledger, migrations_through_0007)
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

        migrations_through_0007 = _ALL_MIGRATIONS[: _ALL_MIGRATIONS.index(_D1I_PARENT_UNIQUENESS_MIGRATION)]
        self.assertEqual(prefix.applied, migrations_through_0007)
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
        self.assertEqual(ledger, migrations_through_0007)
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

        migrations_through_0008 = _ALL_MIGRATIONS[
            : _ALL_MIGRATIONS.index(_D1M_COMPANY_PUBLIC_WEB_RUN_IDEMPOTENCY_MIGRATION)
        ]
        self.assertEqual(prefix.applied, migrations_through_0008)
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
        self.assertEqual(ledger, migrations_through_0008)
        self.assertIsNone(index_name)
        self.assertIsNone(revision_sequence_name)
        self.assertEqual(duplicate_count, 2)

    def test_agent_tool_owner_revision_token_migration_discovery_fresh_and_rerun(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_fresh")
        quoted = quote_control_plane_postgres_identifier(schema)
        discovered = [version for version, _ in mr.discover_migrations()]
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            first = mr.apply_pending_migrations(conn, schema=schema)
            second = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name IN (
                          'agent_tool_result_slots',
                          'agent_tool_result_attempts',
                          'agent_tool_result_journal'
                      )
                      AND column_name = 'owner_target_revision_token'
                    ORDER BY table_name
                    """,
                    (schema,),
                )
                columns = cur.fetchall()
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1c-pending")
                cur.execute(
                    "SELECT status, owner_target_revision_token "
                    "FROM agent_tool_result_slots WHERE result_slot_id = 'slot-s1c-pending'"
                )
                pending = cur.fetchone()
            conn.rollback()

        self.assertEqual(discovered, _ALL_MIGRATIONS)
        self.assertEqual(first.applied, _ALL_MIGRATIONS)
        self.assertEqual(second.applied, [])
        self.assertEqual(second.already_applied, _ALL_MIGRATIONS)
        self.assertEqual(
            columns,
            [
                ("agent_tool_result_attempts", "owner_target_revision_token", "text", "NO", "''::text"),
                ("agent_tool_result_journal", "owner_target_revision_token", "text", "NO", "''::text"),
                ("agent_tool_result_slots", "owner_target_revision_token", "text", "NO", "''::text"),
            ],
        )
        self.assertEqual(pending, ("pending", ""))

    def test_agent_tool_owner_revision_token_upgrade_preserves_numeric_accepted_aggregate(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_upgrade")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 11)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1c-historical",
                        result_link_policy=None,
                    )
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id="slot-s1c-historical",
                        result_attempt_id="attempt-s1c-historical",
                        owner_target_revision=7,
                        owner_target_generation=0,
                        slot_token=None,
                        result_link_policy=None,
                    )
                conn.commit()
                _copy_migrations_through(migrations_dir, 12)
                upgraded = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        SELECT slot.status, attempt.disposition,
                               slot.owner_target_revision, attempt.owner_target_revision,
                               journal.owner_target_revision,
                               slot.owner_target_revision_token,
                               attempt.owner_target_revision_token,
                               journal.owner_target_revision_token,
                               attempt.schema_version,
                               journal.schema_version
                        FROM agent_tool_result_slots AS slot
                        JOIN agent_tool_result_attempts AS attempt
                          ON attempt.result_attempt_id = slot.result_attempt_id
                        JOIN agent_tool_result_journal AS journal
                          ON journal.result_attempt_id = attempt.result_attempt_id
                        WHERE slot.result_slot_id = 'slot-s1c-historical'
                        """
                    )
                    aggregate = cur.fetchone()

        migrations_through_0011 = _ALL_MIGRATIONS[
            : _ALL_MIGRATIONS.index(_D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION)
        ]
        self.assertEqual(prefix.applied, migrations_through_0011)
        self.assertEqual(upgraded.applied, [_D1N_AGENT_TOOL_OWNER_REVISION_TOKEN_MIGRATION])
        self.assertEqual(
            aggregate,
            (
                "accepted",
                "accepted",
                7,
                7,
                7,
                "",
                "",
                "",
                "agent_tool_result_attempt_v1",
                "agent_tool_result_journal_v1",
            ),
        )

    def test_agent_tool_owner_revision_token_old_writer_defaults_to_numeric_v1_aggregate(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_old_writer")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 12)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1c-old-writer",
                        result_link_policy=None,
                    )
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id="slot-s1c-old-writer",
                        result_attempt_id="attempt-s1c-old-writer",
                        owner_target_revision=9,
                        owner_target_generation=0,
                        slot_token=None,
                        result_link_policy=None,
                    )
                conn.commit()
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        SELECT slot.owner_target_revision_token,
                               attempt.owner_target_revision_token,
                               journal.owner_target_revision_token,
                               attempt.schema_version,
                               journal.schema_version
                        FROM agent_tool_result_slots AS slot
                        JOIN agent_tool_result_attempts AS attempt
                          ON attempt.result_attempt_id = slot.result_attempt_id
                        JOIN agent_tool_result_journal AS journal
                          ON journal.result_attempt_id = attempt.result_attempt_id
                        WHERE slot.result_slot_id = 'slot-s1c-old-writer'
                        """
                    )
                    aggregate = cur.fetchone()

        self.assertEqual(
            aggregate,
            ("", "", "", "agent_tool_result_attempt_v1", "agent_tool_result_journal_v1"),
        )

    def test_agent_tool_owner_revision_token_accepts_exact_token_only_aggregate(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_exact")
        quoted = quote_control_plane_postgres_identifier(schema)
        token = "projidxinput_membership.revision:opaque-7"
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1c-token-only")
                self._insert_agent_tool_accepted_aggregate(
                    cur,
                    result_slot_id="slot-s1c-token-only",
                    result_attempt_id="attempt-s1c-token-only",
                    owner_target_revision=0,
                    owner_target_generation=0,
                    slot_token=token,
                    attempt_schema_version="agent_tool_result_attempt_v2",
                    journal_schema_version="agent_tool_result_journal_v2",
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT slot.owner_target_revision, slot.owner_target_generation,
                           slot.owner_target_revision_token,
                           attempt.owner_target_revision_token,
                           journal.owner_target_revision_token,
                           attempt.schema_version,
                           journal.schema_version
                    FROM agent_tool_result_slots AS slot
                    JOIN agent_tool_result_attempts AS attempt
                      ON attempt.result_attempt_id = slot.result_attempt_id
                    JOIN agent_tool_result_journal AS journal
                      ON journal.result_attempt_id = attempt.result_attempt_id
                    WHERE slot.result_slot_id = 'slot-s1c-token-only'
                    """
                )
                aggregate = cur.fetchone()

        self.assertEqual(
            aggregate,
            (
                0,
                0,
                token,
                token,
                token,
                "agent_tool_result_attempt_v2",
                "agent_tool_result_journal_v2",
            ),
        )

    def test_agent_tool_owner_revision_token_rejects_missing_blank_invalid_and_oversized(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_shape")
        quoted = quote_control_plane_postgres_identifier(schema)
        invalid_tokens = ("", " ", "bad/token", "a" * 257)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1c-shape")
                for index, token in enumerate(invalid_tokens):
                    cur.execute(f"SAVEPOINT invalid_token_{index}")
                    with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                        self._insert_agent_tool_result_attempt(
                            cur,
                            result_slot_id="slot-s1c-shape",
                            result_attempt_id=f"attempt-s1c-invalid-{index}",
                            disposition="quarantined",
                            quarantine_reason="invalid-owner-token",
                            owner_target_revision=0,
                            owner_target_generation=0,
                            owner_target_revision_token=token,
                            schema_version="agent_tool_result_attempt_v2",
                        )
                    self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_attempts_shape_ck")
                    cur.execute(f"ROLLBACK TO SAVEPOINT invalid_token_{index}")

                cur.execute("SAVEPOINT pending_token_nonempty")
                with self.assertRaises(psycopg.errors.CheckViolation) as pending_raised:
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1c-pending-with-token",
                        owner_target_revision_token="membership-revision-present-too-early",
                    )
                self.assertEqual(pending_raised.exception.diag.constraint_name, "agent_tool_slots_terminal_shape_ck")
                cur.execute("ROLLBACK TO SAVEPOINT pending_token_nonempty")

                self._insert_agent_tool_result_attempt(
                    cur,
                    result_slot_id="slot-s1c-shape",
                    result_attempt_id="attempt-s1c-valid-quarantine",
                    disposition="quarantined",
                    quarantine_reason="late-winner",
                    owner_target_revision=0,
                    owner_target_generation=0,
                    owner_target_revision_token="membership-revision-valid",
                    schema_version="agent_tool_result_attempt_v2",
                )
            conn.commit()

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT owner_target_revision_token, schema_version FROM agent_tool_result_attempts "
                    "WHERE result_attempt_id = 'attempt-s1c-valid-quarantine'"
                )
                accepted_token = cur.fetchone()
        self.assertEqual(
            accepted_token,
            ("membership-revision-valid", "agent_tool_result_attempt_v2"),
        )

    def test_agent_tool_owner_revision_token_v1_rejects_nonempty_token(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_v1_reject")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1c-v1-token")
                with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id="slot-s1c-v1-token",
                        result_attempt_id="attempt-s1c-v1-token",
                        disposition="quarantined",
                        quarantine_reason="invalid-v1-owner-token",
                        owner_target_revision=11,
                        owner_target_generation=0,
                        owner_target_revision_token="membership-revision-v2-only",
                        schema_version="agent_tool_result_attempt_v1",
                    )

        self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_attempts_shape_ck")

    def test_agent_tool_owner_revision_token_v1_journal_rejects_nonempty_token(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_v1_journal_reject")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SAVEPOINT v1_journal_token")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1c-v1-journal-token")
                with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id="slot-s1c-v1-journal-token",
                        result_attempt_id="attempt-s1c-v1-journal-token",
                        owner_target_revision=11,
                        owner_target_generation=0,
                        slot_token="membership-revision-v2-only",
                        attempt_schema_version="agent_tool_result_attempt_v2",
                        journal_schema_version="agent_tool_result_journal_v1",
                    )
                cur.execute("ROLLBACK TO SAVEPOINT v1_journal_token")

        self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_journal_shape_ck")

    def test_agent_tool_owner_revision_token_deferred_aggregate_requires_exact_equality(self) -> None:
        mismatch_cases = (
            ("attempt", "membership-revision-slot", "membership-revision-attempt", "membership-revision-slot"),
            ("journal", "membership-revision-slot", "membership-revision-slot", "membership-revision-journal"),
        )
        for label, slot_token, attempt_token, journal_token in mismatch_cases:
            with self.subTest(mismatch=label):
                schema = self._fresh_schema(f"d1n_owner_token_mismatch_{label}")
                quoted = quote_control_plane_postgres_identifier(schema)
                with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                    mr.apply_pending_migrations(conn, schema=schema)
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        self._insert_agent_tool_pending_slot(cur, result_slot_id=f"slot-s1c-mismatch-{label}")
                        self._insert_agent_tool_accepted_aggregate(
                            cur,
                            result_slot_id=f"slot-s1c-mismatch-{label}",
                            result_attempt_id=f"attempt-s1c-mismatch-{label}",
                            owner_target_revision=0,
                            owner_target_generation=0,
                            slot_token=slot_token,
                            attempt_token=attempt_token,
                            journal_token=journal_token,
                            attempt_schema_version="agent_tool_result_attempt_v2",
                            journal_schema_version="agent_tool_result_journal_v2",
                        )
                    with self.assertRaises(psycopg.errors.RaiseException) as raised:
                        conn.commit()
                    conn.rollback()
                self.assertEqual(
                    raised.exception.diag.constraint_name,
                    "agent_tool_terminal_aggregate_incomplete",
                )

    def test_agent_tool_owner_revision_token_v2_rejects_numeric_only_attempts(self) -> None:
        schema = self._fresh_schema("d1n_owner_token_v2_numeric_attempt")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1d-v2-numeric-attempt")
                for disposition in ("accepted", "quarantined"):
                    with self.subTest(disposition=disposition):
                        cur.execute(f"SAVEPOINT numeric_v2_{disposition}")
                        with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                            self._insert_agent_tool_result_attempt(
                                cur,
                                result_slot_id="slot-s1d-v2-numeric-attempt",
                                result_attempt_id=f"attempt-s1d-v2-numeric-{disposition}",
                                disposition=disposition,
                                quarantine_reason="" if disposition == "accepted" else "numeric-v2-invalid",
                                owner_target_revision=13,
                                owner_target_generation=0,
                                owner_target_revision_token=None,
                                schema_version="agent_tool_result_attempt_v2",
                            )
                        self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_attempts_shape_ck")
                        cur.execute(f"ROLLBACK TO SAVEPOINT numeric_v2_{disposition}")
                cur.execute(
                    "SELECT count(*) FROM agent_tool_result_attempts "
                    "WHERE result_slot_id = 'slot-s1d-v2-numeric-attempt'"
                )
                attempt_count = cur.fetchone()[0]
            conn.rollback()
        self.assertEqual(attempt_count, 0)

    def test_agent_tool_owner_revision_token_rejects_numeric_only_v2_in_mixed_aggregates(self) -> None:
        mismatch_cases = (
            (
                "numeric_v2_journal",
                "agent_tool_result_attempt_v1",
                "agent_tool_result_journal_v2",
                "agent_tool_journal_shape_ck",
            ),
            (
                "numeric_v2_attempt",
                "agent_tool_result_attempt_v2",
                "agent_tool_result_journal_v1",
                "agent_tool_attempts_shape_ck",
            ),
        )
        for label, attempt_schema_version, journal_schema_version, expected_constraint in mismatch_cases:
            with self.subTest(mismatch=label):
                schema = self._fresh_schema(f"d1n_owner_token_schema_mismatch_{label}")
                quoted = quote_control_plane_postgres_identifier(schema)
                with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                    mr.apply_pending_migrations(conn, schema=schema)
                    with conn.cursor() as cur:
                        cur.execute(f"SET search_path TO {quoted}")
                        self._insert_agent_tool_pending_slot(cur, result_slot_id=f"slot-s1c-{label}")
                        with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                            self._insert_agent_tool_accepted_aggregate(
                                cur,
                                result_slot_id=f"slot-s1c-{label}",
                                result_attempt_id=f"attempt-s1c-{label}",
                                owner_target_revision=13,
                                owner_target_generation=0,
                                slot_token=None,
                                attempt_schema_version=attempt_schema_version,
                                journal_schema_version=journal_schema_version,
                            )
                    conn.rollback()
                self.assertEqual(
                    raised.exception.diag.constraint_name,
                    expected_constraint,
                )

    def test_agent_tool_result_link_policy_migration_discovery_fresh_and_rerun(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_fresh")
        quoted = quote_control_plane_postgres_identifier(schema)
        discovered = [version for version, _ in mr.discover_migrations()]
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            first = mr.apply_pending_migrations(conn, schema=schema)
            second = mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT table_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name IN (
                          'agent_tool_result_slots',
                          'agent_tool_result_attempts',
                          'agent_tool_result_journal'
                      )
                      AND column_name = 'result_link_policy'
                    ORDER BY table_name
                    """,
                    (schema,),
                )
                columns = cur.fetchall()
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1d-fresh")
                cur.execute(
                    "SELECT effect_class, result_link_policy FROM agent_tool_result_slots "
                    "WHERE result_slot_id = 'slot-s1d-fresh'"
                )
                pending = cur.fetchone()
            conn.rollback()

        self.assertEqual(discovered, _ALL_MIGRATIONS)
        self.assertEqual(first.applied, _ALL_MIGRATIONS)
        self.assertEqual(second.applied, [])
        self.assertEqual(second.already_applied, _ALL_MIGRATIONS)
        self.assertEqual(
            columns,
            [
                ("agent_tool_result_attempts", "text", "NO", None),
                ("agent_tool_result_journal", "text", "NO", None),
                ("agent_tool_result_slots", "text", "NO", None),
            ],
        )
        self.assertEqual(pending, ("read_only", "no_command_v1"))

    def test_agent_tool_result_link_policy_upgrade_backfills_all_existing_effect_classes(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_upgrade")
        quoted = quote_control_plane_postgres_identifier(schema)
        cases = (
            (
                "read",
                "inspect_operation",
                "query",
                "read_only",
                "no_command_v1",
                ("", "", "", "", "", 0, 0, 0),
            ),
            (
                "commandless",
                "plan_acquisition",
                "action",
                "commandless_action",
                "no_command_v1",
                ("action-s1d-commandless", "operation-s1d-commandless", "", "", "", 0, 0, 0),
            ),
            (
                "activity",
                "start_acquisition_run",
                "action",
                "command_backed_action",
                "activity_attempt_terminal_v1",
                (
                    "action-s1d-activity",
                    "operation-s1d-activity",
                    "command-s1d-activity",
                    "activity-run-s1d",
                    "activity-attempt-s1d",
                    1,
                    2,
                    3,
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 12)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    for label, tool_name, tool_kind, effect_class, _policy, links in cases:
                        self._insert_agent_tool_pending_slot(
                            cur,
                            result_slot_id=f"slot-s1d-backfill-{label}",
                            result_link_policy=None,
                            tool_name=tool_name,
                            tool_kind=tool_kind,
                            effect_class=effect_class,
                        )
                        self._insert_agent_tool_accepted_aggregate(
                            cur,
                            result_slot_id=f"slot-s1d-backfill-{label}",
                            result_attempt_id=f"attempt-s1d-backfill-{label}",
                            owner_target_revision=17,
                            owner_target_generation=0,
                            slot_token=None,
                            result_link_policy=None,
                            action_id=links[0],
                            operation_run_id=links[1],
                            workflow_command_id=links[2],
                            activity_run_id=links[3],
                            activity_attempt_id=links[4],
                            command_attempt=links[5],
                            command_generation=links[6],
                            control_epoch=links[7],
                        )
                conn.commit()
                _copy_migrations_through(migrations_dir, 13)
                upgraded = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        SELECT slot.effect_class,
                               slot.result_link_policy,
                               attempt.result_link_policy,
                               journal.result_link_policy
                        FROM agent_tool_result_slots AS slot
                        JOIN agent_tool_result_attempts AS attempt
                          ON attempt.result_attempt_id = slot.result_attempt_id
                        JOIN agent_tool_result_journal AS journal
                          ON journal.result_attempt_id = attempt.result_attempt_id
                        ORDER BY slot.effect_class
                        """
                    )
                    policies = cur.fetchall()

        migrations_through_0012 = _ALL_MIGRATIONS[
            : _ALL_MIGRATIONS.index(_D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION)
        ]
        self.assertEqual(prefix.applied, migrations_through_0012)
        self.assertEqual(upgraded.applied, [_D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION])
        self.assertEqual(
            policies,
            [
                (
                    "command_backed_action",
                    "activity_attempt_terminal_v1",
                    "activity_attempt_terminal_v1",
                    "activity_attempt_terminal_v1",
                ),
                ("commandless_action", "no_command_v1", "no_command_v1", "no_command_v1"),
                ("read_only", "no_command_v1", "no_command_v1", "no_command_v1"),
            ],
        )

    def test_agent_tool_result_link_policy_upgrade_rejects_preexisting_numeric_only_v2(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_noncanonical_v2")
        quoted = quote_control_plane_postgres_identifier(schema)
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 12)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1d-preexisting-numeric-v2",
                        result_link_policy=None,
                    )
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id="slot-s1d-preexisting-numeric-v2",
                        result_attempt_id="attempt-s1d-preexisting-numeric-v2",
                        disposition="quarantined",
                        quarantine_reason="preexisting-noncanonical-v2",
                        owner_target_revision=13,
                        owner_target_generation=0,
                        owner_target_revision_token=None,
                        schema_version="agent_tool_result_attempt_v2",
                        result_link_policy=None,
                    )
                conn.commit()

                _copy_migrations_through(migrations_dir, 13)
                with self.assertRaises(psycopg.errors.CheckViolation) as raised:
                    mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                self.assertEqual(raised.exception.diag.constraint_name, "agent_tool_attempts_shape_ck")

            with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    cur.execute(
                        """
                        SELECT
                            (SELECT count(*) FROM information_schema.columns
                             WHERE table_schema = %s
                               AND table_name = 'agent_tool_result_attempts'
                               AND column_name = 'result_link_policy'),
                            (SELECT count(*) FROM schema_migrations
                             WHERE version = %s),
                            (SELECT count(*) FROM agent_tool_result_attempts
                             WHERE result_attempt_id = 'attempt-s1d-preexisting-numeric-v2'
                               AND schema_version = 'agent_tool_result_attempt_v2'
                               AND owner_target_revision_token = '')
                        """,
                        (schema, _D1N_AGENT_TOOL_RESULT_LINK_POLICY_MIGRATION),
                    )
                    rollback_state = cur.fetchone()
        self.assertEqual(rollback_state, (0, 0, 1))

    def test_agent_tool_result_link_policy_old_writer_omission_fails_closed(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_old_writer")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                with self.assertRaises(psycopg.errors.NotNullViolation) as slot_raised:
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1d-old-writer",
                        result_link_policy=None,
                    )
                self.assertEqual(slot_raised.exception.diag.column_name, "result_link_policy")
            conn.rollback()

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id="slot-s1d-old-attempt-writer")
                cur.execute("SAVEPOINT old_attempt_writer")
                with self.assertRaises(psycopg.errors.NotNullViolation) as attempt_raised:
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id="slot-s1d-old-attempt-writer",
                        result_attempt_id="attempt-s1d-old-writer",
                        disposition="quarantined",
                        quarantine_reason="old-writer-omission",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        owner_target_revision_token=None,
                        result_link_policy=None,
                    )
                self.assertEqual(attempt_raised.exception.diag.column_name, "result_link_policy")
                cur.execute("ROLLBACK TO SAVEPOINT old_attempt_writer")
            conn.rollback()

        journal_slot_id = "slot-s1d-old-journal-writer"
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(cur, result_slot_id=journal_slot_id)
            conn.commit()

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                with self.assertRaises(psycopg.errors.NotNullViolation) as journal_raised:
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id=journal_slot_id,
                        result_attempt_id="attempt-s1d-old-journal-writer",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        slot_token=None,
                        omit_journal_result_link_policy=True,
                    )
                self.assertEqual(journal_raised.exception.diag.column_name, "result_link_policy")
            conn.rollback()

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT slot.status,
                           (SELECT count(*) FROM agent_tool_result_attempts AS attempt
                            WHERE attempt.result_slot_id = slot.result_slot_id),
                           (SELECT count(*) FROM agent_tool_result_journal AS journal
                            WHERE journal.result_slot_id = slot.result_slot_id)
                    FROM agent_tool_result_slots AS slot
                    WHERE slot.result_slot_id = %s
                    """,
                    (journal_slot_id,),
                )
                journal_omission_effects = cur.fetchone()
        self.assertEqual(journal_omission_effects, ("pending", 0, 0))

    def test_agent_tool_result_link_policy_accepts_all_three_terminal_shapes(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_positive")
        quoted = quote_control_plane_postgres_identifier(schema)
        cases = (
            (
                "no-command",
                "plan_acquisition",
                "commandless_action",
                "no_command_v1",
                ("action-s1d-no-command", "operation-s1d-no-command", "", "", "", 0, 0, 0),
            ),
            (
                "command-acceptance",
                "start_acquisition_run",
                "command_backed_action",
                "workflow_command_acceptance_v1",
                (
                    "action-s1d-command",
                    "operation-s1d-command",
                    "command-s1d-command",
                    "",
                    "",
                    0,
                    0,
                    0,
                ),
            ),
            (
                "activity-terminal",
                "start_acquisition_run_terminal",
                "command_backed_action",
                "activity_attempt_terminal_v1",
                (
                    "action-s1d-terminal",
                    "operation-s1d-terminal",
                    "command-s1d-terminal",
                    "activity-run-s1d-terminal",
                    "activity-attempt-s1d-terminal",
                    2,
                    3,
                    4,
                ),
            ),
        )
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                for label, tool_name, effect_class, policy, links in cases:
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id=f"slot-s1d-positive-{label}",
                        tool_name=tool_name,
                        tool_kind="action",
                        effect_class=effect_class,
                        result_link_policy=policy,
                    )
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id=f"slot-s1d-positive-{label}",
                        result_attempt_id=f"attempt-s1d-positive-{label}",
                        owner_target_revision=21,
                        owner_target_generation=0,
                        slot_token=None,
                        result_link_policy=policy,
                        action_id=links[0],
                        operation_run_id=links[1],
                        workflow_command_id=links[2],
                        activity_run_id=links[3],
                        activity_attempt_id=links[4],
                        command_attempt=links[5],
                        command_generation=links[6],
                        control_epoch=links[7],
                    )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    "SELECT result_link_policy, workflow_command_id, activity_attempt_id "
                    "FROM agent_tool_result_slots WHERE status = 'accepted' ORDER BY result_link_policy"
                )
                accepted = cur.fetchall()

        self.assertEqual(
            accepted,
            [
                ("activity_attempt_terminal_v1", "command-s1d-terminal", "activity-attempt-s1d-terminal"),
                ("no_command_v1", "", ""),
                ("workflow_command_acceptance_v1", "command-s1d-command", ""),
            ],
        )

    def test_agent_tool_result_link_policy_rejects_wrong_effect_and_terminal_shapes(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_wrong_shapes")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute("SAVEPOINT wrong_effect_policy")
                with self.assertRaises(psycopg.errors.CheckViolation) as identity_raised:
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id="slot-s1d-wrong-effect",
                        effect_class="read_only",
                        result_link_policy="workflow_command_acceptance_v1",
                    )
                self.assertEqual(identity_raised.exception.diag.constraint_name, "agent_tool_slots_identity_shape_ck")
                cur.execute("ROLLBACK TO SAVEPOINT wrong_effect_policy")

                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id="slot-s1d-wrong-attempt",
                    tool_name="start_acquisition_run",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
                cur.execute("SAVEPOINT wrong_attempt_shape")
                with self.assertRaises(psycopg.errors.CheckViolation) as attempt_raised:
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id="slot-s1d-wrong-attempt",
                        result_attempt_id="attempt-s1d-wrong-shape",
                        disposition="quarantined",
                        quarantine_reason="wrong-link-shape",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        owner_target_revision_token=None,
                        result_link_policy="workflow_command_acceptance_v1",
                        action_id="action-s1d-wrong",
                        operation_run_id="operation-s1d-wrong",
                        workflow_command_id="command-s1d-wrong",
                        activity_run_id="activity-run-s1d-wrong",
                        activity_attempt_id="activity-attempt-s1d-wrong",
                        command_attempt=1,
                        command_generation=1,
                        control_epoch=1,
                    )
                self.assertEqual(attempt_raised.exception.diag.constraint_name, "agent_tool_attempts_shape_ck")
                cur.execute("ROLLBACK TO SAVEPOINT wrong_attempt_shape")

                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id="slot-s1d-wrong-slot",
                    tool_name="start_acquisition_run_slot",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
                cur.execute("SAVEPOINT wrong_slot_shape")
                with self.assertRaises(psycopg.errors.CheckViolation) as slot_raised:
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id="slot-s1d-wrong-slot",
                        result_attempt_id="attempt-s1d-wrong-slot",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        slot_token=None,
                        result_link_policy="workflow_command_acceptance_v1",
                        attempt_result_link_policy="no_command_v1",
                        action_id="action-s1d-wrong-slot",
                        operation_run_id="operation-s1d-wrong-slot",
                        attempt_link_values=(
                            "action-s1d-wrong-slot",
                            "operation-s1d-wrong-slot",
                            "",
                            "",
                            "",
                            0,
                            0,
                            0,
                        ),
                    )
                self.assertEqual(slot_raised.exception.diag.constraint_name, "agent_tool_slots_terminal_shape_ck")
                cur.execute("ROLLBACK TO SAVEPOINT wrong_slot_shape")

                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id="slot-s1d-wrong-journal",
                    tool_name="start_acquisition_run_journal",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
                cur.execute("SAVEPOINT wrong_journal_shape")
                with self.assertRaises(psycopg.errors.CheckViolation) as journal_raised:
                    self._insert_agent_tool_accepted_aggregate(
                        cur,
                        result_slot_id="slot-s1d-wrong-journal",
                        result_attempt_id="attempt-s1d-wrong-journal",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        slot_token=None,
                        result_link_policy="workflow_command_acceptance_v1",
                        journal_result_link_policy="no_command_v1",
                        action_id="action-s1d-wrong-journal",
                        operation_run_id="operation-s1d-wrong-journal",
                        workflow_command_id="command-s1d-wrong-journal",
                    )
                self.assertEqual(journal_raised.exception.diag.constraint_name, "agent_tool_journal_shape_ck")
                cur.execute("ROLLBACK TO SAVEPOINT wrong_journal_shape")
            conn.rollback()

    def test_agent_tool_result_link_policy_quarantined_attempt_exact_matches_slot(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_quarantined_exact")
        quoted = quote_control_plane_postgres_identifier(schema)
        result_slot_id = "slot-s1d-quarantined-policy-mismatch"
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id=result_slot_id,
                    tool_name="start_acquisition_run_quarantined",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
            conn.commit()

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_result_attempt(
                    cur,
                    result_slot_id=result_slot_id,
                    result_attempt_id="attempt-s1d-quarantined-policy-mismatch",
                    disposition="quarantined",
                    quarantine_reason="policy-mismatch-probe",
                    owner_target_revision=1,
                    owner_target_generation=0,
                    owner_target_revision_token=None,
                    result_link_policy="no_command_v1",
                )
            with self.assertRaises(psycopg.errors.RaiseException) as mismatch_raised:
                conn.commit()
            conn.rollback()

        self.assertEqual(
            mismatch_raised.exception.diag.constraint_name,
            "agent_tool_result_attempt_slot_policy_mismatch",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT slot.status, slot.result_link_policy,
                           (SELECT count(*) FROM agent_tool_result_attempts AS attempt
                            WHERE attempt.result_slot_id = slot.result_slot_id),
                           (SELECT count(*) FROM agent_tool_result_journal AS journal
                            WHERE journal.result_slot_id = slot.result_slot_id)
                    FROM agent_tool_result_slots AS slot
                    WHERE slot.result_slot_id = %s
                    """,
                    (result_slot_id,),
                )
                persisted = cur.fetchone()
        self.assertEqual(persisted, ("pending", "workflow_command_acceptance_v1", 0, 0))

    def test_agent_tool_result_link_policy_is_immutable_and_deferred_exact(self) -> None:
        schema = self._fresh_schema("d1n_link_policy_exact")
        quoted = quote_control_plane_postgres_identifier(schema)
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id="slot-s1d-immutable",
                    tool_name="start_acquisition_run_immutable",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
                with self.assertRaises(psycopg.errors.RaiseException) as immutable_raised:
                    cur.execute(
                        "UPDATE agent_tool_result_slots "
                        "SET result_link_policy = 'activity_attempt_terminal_v1' "
                        "WHERE result_slot_id = 'slot-s1d-immutable'"
                    )
                self.assertEqual(immutable_raised.exception.diag.constraint_name, "agent_tool_result_slots_immutable")
            conn.rollback()

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id="slot-s1d-deferred-mismatch",
                    tool_name="start_acquisition_run_deferred",
                    tool_kind="action",
                    effect_class="command_backed_action",
                    result_link_policy="workflow_command_acceptance_v1",
                )
                self._insert_agent_tool_accepted_aggregate(
                    cur,
                    result_slot_id="slot-s1d-deferred-mismatch",
                    result_attempt_id="attempt-s1d-deferred-mismatch",
                    owner_target_revision=23,
                    owner_target_generation=0,
                    slot_token=None,
                    result_link_policy="workflow_command_acceptance_v1",
                    attempt_result_link_policy="no_command_v1",
                    action_id="action-s1d-deferred",
                    operation_run_id="operation-s1d-deferred",
                    workflow_command_id="command-s1d-deferred",
                    attempt_link_values=(
                        "action-s1d-deferred",
                        "operation-s1d-deferred",
                        "",
                        "",
                        "",
                        0,
                        0,
                        0,
                    ),
                )
            with self.assertRaises(psycopg.errors.RaiseException) as deferred_raised:
                conn.commit()
            conn.rollback()

        self.assertEqual(
            deferred_raised.exception.diag.constraint_name,
            "agent_tool_terminal_aggregate_incomplete",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM agent_tool_result_slots
                         WHERE result_slot_id = 'slot-s1d-deferred-mismatch'),
                        (SELECT count(*) FROM agent_tool_result_attempts
                         WHERE result_attempt_id = 'attempt-s1d-deferred-mismatch'),
                        (SELECT count(*) FROM agent_tool_result_journal
                         WHERE result_slot_id = 'slot-s1d-deferred-mismatch')
                    """
                )
                deferred_effects = cur.fetchone()
        self.assertEqual(deferred_effects, (0, 0, 0))

    def test_agent_tool_result_attempt_effect_contract_upgrade_rejects_ownerless_commandless_attempt(
        self,
    ) -> None:
        schema = self._fresh_schema("d1n_attempt_effect_upgrade")
        quoted = quote_control_plane_postgres_identifier(schema)
        result_slot_id = "slot-s1d-commandless-historical-ownerless"
        result_attempt_id = "attempt-s1d-commandless-historical-ownerless"
        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_dir = Path(temp_dir)
            _copy_migrations_through(migrations_dir, 13)
            with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
                prefix = mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {quoted}")
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id=result_slot_id,
                        tool_name="plan_acquisition_historical_ownerless",
                        tool_kind="action",
                        effect_class="commandless_action",
                        result_link_policy="no_command_v1",
                    )
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id=result_slot_id,
                        result_attempt_id=result_attempt_id,
                        disposition="quarantined",
                        quarantine_reason="historical-ownerless-commandless",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        owner_target_revision_token=None,
                        result_link_policy="no_command_v1",
                    )
                conn.commit()

                _copy_migrations_through(migrations_dir, 14)
                with self.assertRaises(psycopg.errors.RaiseException) as raised:
                    mr.apply_pending_migrations(conn, schema=schema, migrations_dir=migrations_dir)
                self.assertEqual(
                    raised.exception.diag.constraint_name,
                    "agent_tool_result_attempt_slot_effect_shape_mismatch",
                )

        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM schema_migrations WHERE version = %s),
                        (SELECT count(*) FROM agent_tool_result_attempts
                         WHERE result_attempt_id = %s),
                        (SELECT count(*) FROM agent_tool_result_journal
                         WHERE result_slot_id = %s)
                    """,
                    (
                        _D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION,
                        result_attempt_id,
                        result_slot_id,
                    ),
                )
                rollback_state = cur.fetchone()

        migrations_through_0013 = _ALL_MIGRATIONS[
            : _ALL_MIGRATIONS.index(_D1N_AGENT_TOOL_RESULT_ATTEMPT_EFFECT_CONTRACT_MIGRATION)
        ]
        self.assertEqual(prefix.applied, migrations_through_0013)
        self.assertEqual(rollback_state, (0, 1, 0))

    def test_agent_tool_result_attempt_effect_contract_rejects_fresh_ownerless_commandless_attempt(
        self,
    ) -> None:
        schema = self._fresh_schema("d1n_attempt_effect_fresh_invalid")
        quoted = quote_control_plane_postgres_identifier(schema)
        result_slot_id = "slot-s1d-commandless-fresh-ownerless"
        result_attempt_id = "attempt-s1d-commandless-fresh-ownerless"
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_pending_slot(
                    cur,
                    result_slot_id=result_slot_id,
                    tool_name="plan_acquisition_fresh_ownerless",
                    tool_kind="action",
                    effect_class="commandless_action",
                    result_link_policy="no_command_v1",
                )
            conn.commit()

        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                self._insert_agent_tool_result_attempt(
                    cur,
                    result_slot_id=result_slot_id,
                    result_attempt_id=result_attempt_id,
                    disposition="quarantined",
                    quarantine_reason="fresh-ownerless-commandless",
                    owner_target_revision=1,
                    owner_target_generation=0,
                    owner_target_revision_token=None,
                    result_link_policy="no_command_v1",
                )
            with self.assertRaises(psycopg.errors.RaiseException) as raised:
                conn.commit()
            conn.rollback()

        self.assertEqual(
            raised.exception.diag.constraint_name,
            "agent_tool_result_attempt_slot_effect_shape_mismatch",
        )
        with psycopg.connect(self.dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT slot.status,
                           (SELECT count(*) FROM agent_tool_result_attempts AS attempt
                            WHERE attempt.result_slot_id = slot.result_slot_id),
                           (SELECT count(*) FROM agent_tool_result_journal AS journal
                            WHERE journal.result_slot_id = slot.result_slot_id)
                    FROM agent_tool_result_slots AS slot
                    WHERE slot.result_slot_id = %s
                    """,
                    (result_slot_id,),
                )
                persisted = cur.fetchone()
        self.assertEqual(persisted, ("pending", 0, 0))

    def test_agent_tool_result_attempt_effect_contract_accepts_commandless_and_read_only_shapes(
        self,
    ) -> None:
        schema = self._fresh_schema("d1n_attempt_effect_positive")
        quoted = quote_control_plane_postgres_identifier(schema)
        cases = (
            (
                "commandless",
                "plan_acquisition_effect_contract",
                "action",
                "commandless_action",
                "action-s1d-effect-contract",
                "operation-s1d-effect-contract",
            ),
            (
                "read-ownerless",
                "inspect_operation_effect_contract_ownerless",
                "query",
                "read_only",
                "",
                "",
            ),
            (
                "read-owned",
                "inspect_operation_effect_contract_owned",
                "query",
                "read_only",
                "action-s1d-read-effect-contract",
                "operation-s1d-read-effect-contract",
            ),
        )
        with psycopg.connect(self.dsn, client_encoding="utf8") as conn:
            mr.apply_pending_migrations(conn, schema=schema)
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                for label, tool_name, tool_kind, effect_class, action_id, operation_run_id in cases:
                    result_slot_id = f"slot-s1d-effect-contract-{label}"
                    self._insert_agent_tool_pending_slot(
                        cur,
                        result_slot_id=result_slot_id,
                        tool_name=tool_name,
                        tool_kind=tool_kind,
                        effect_class=effect_class,
                        result_link_policy="no_command_v1",
                    )
                    self._insert_agent_tool_result_attempt(
                        cur,
                        result_slot_id=result_slot_id,
                        result_attempt_id=f"attempt-s1d-effect-contract-{label}",
                        disposition="quarantined",
                        quarantine_reason=f"positive-{label}",
                        owner_target_revision=1,
                        owner_target_generation=0,
                        owner_target_revision_token=None,
                        result_link_policy="no_command_v1",
                        action_id=action_id,
                        operation_run_id=operation_run_id,
                    )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quoted}")
                cur.execute(
                    """
                    SELECT slot.effect_class, attempt.action_id, attempt.operation_run_id
                    FROM agent_tool_result_slots AS slot
                    JOIN agent_tool_result_attempts AS attempt
                      ON attempt.result_slot_id = slot.result_slot_id
                    ORDER BY attempt.result_attempt_id
                    """
                )
                persisted = cur.fetchall()

        self.assertEqual(
            persisted,
            [
                (
                    "commandless_action",
                    "action-s1d-effect-contract",
                    "operation-s1d-effect-contract",
                ),
                (
                    "read_only",
                    "action-s1d-read-effect-contract",
                    "operation-s1d-read-effect-contract",
                ),
                ("read_only", "", ""),
            ],
        )

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
