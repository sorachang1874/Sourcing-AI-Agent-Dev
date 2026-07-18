from __future__ import annotations

import ast
import json
import math
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
from decimal import Decimal
from hashlib import sha1
from pathlib import Path
from typing import Any
from uuid import uuid4

from .company_registry import resolve_company_alias_key
from .control_plane_job_progress import update_job_progress_event_summary
from .control_plane_postgres import (
    ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
    ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES,
    LEGACY_TARGET_PUBLIC_WEB_TABLES,
    _configure_postgres_connection_utf8,
    _ensure_control_plane_unique_indexes,
    _import_psycopg,
    ensure_acquisition_shard_registry_split_schema,
    upsert_acquisition_shard_registry_rows,
)
from .json_contract import JsonContractShapeError, decode_json_contract, json_contract_equal
from .local_postgres import (
    configure_control_plane_postgres_session,
    ensure_local_postgres_started,
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    resolve_control_plane_postgres_dsn,
    resolve_control_plane_postgres_schema,
    resolve_default_control_plane_db_path,
)
from .migration_runner import apply_pending_migrations
from .projection_search_index_contract import (
    PROJECTION_SEARCH_INDEX_BINDING_KEYS,
    PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY,
    PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY,
    PROJECTION_SEARCH_INDEX_BUILD_STATUS_KEY,
    PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,
    projection_search_index_members_changed,
)
from .runtime_lease_utils import worker_lease_owner_is_dead_local_process

_COMPANY_PUBLIC_WEB_IDEMPOTENCY_ASCII_WHITESPACE = " \t\n\r\f\v"
_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_ID_KEY = "source_workflow_command_id"
_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_ATTEMPT_KEY = "source_workflow_command_attempt"
_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_LEASE_OWNER_KEY = "source_workflow_command_lease_owner"
_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY = "source_projection_revision"
_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_SEQUENCE = "company_public_web_source_projection_revision_seq"
_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_MAX = 9_223_372_036_854_775_807


def normalize_company_public_web_asset_run_idempotency_key(value: Any) -> str:
    """Match PostgreSQL's protocol-ASCII whitespace identity expression."""

    return str(value or "").strip(_COMPANY_PUBLIC_WEB_IDEMPOTENCY_ASCII_WHITESPACE)


def _company_public_web_source_activity_identity(command_id: str) -> tuple[str, str]:
    normalized_command_id = str(command_id or "").strip()
    activity_key = f"workflow_activity:company.public_web.source.collect:{normalized_command_id}"
    return "actrun_" + sha1(activity_key.encode("utf-8")).hexdigest()[:24], activity_key


def _company_public_web_source_attempt_identity(command_id: str, attempt_number: int) -> tuple[str, str]:
    normalized_command_id = str(command_id or "").strip()
    normalized_attempt = max(1, int(attempt_number or 1))
    attempt_key_hash = sha1(
        (f"company.public_web.source.collect:company_public_web_refresh:{normalized_attempt}").encode("utf-8")
    ).hexdigest()[:24]
    attempt_key = f"workflow_activity_attempt:{normalized_command_id}:{attempt_key_hash}"
    return "actattempt_" + sha1(attempt_key.encode("utf-8")).hexdigest()[:24], attempt_key


def _company_public_web_source_resume_attempt_identity(command_id: str, attempt_number: int) -> tuple[str, str]:
    """Return the exact owner-specific resume Attempt identity for one source command."""

    normalized_command_id = str(command_id or "").strip()
    normalized_attempt = max(1, int(attempt_number or 1))
    attempt_key_hash = sha1(
        (f"company.public_web.source.collect:owner_specific_resume:{normalized_attempt}").encode("utf-8")
    ).hexdigest()[:24]
    attempt_key = f"workflow_activity_attempt:{normalized_command_id}:{attempt_key_hash}"
    return "actattempt_" + sha1(attempt_key.encode("utf-8")).hexdigest()[:24], attempt_key


def _company_public_web_source_run_owner(metadata_payload: Any) -> tuple[dict[str, Any], bool]:
    metadata = _json_load_dict(metadata_payload)
    command_id = str(metadata.get(_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_ID_KEY) or "").strip()
    lease_owner = str(metadata.get(_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_LEASE_OWNER_KEY) or "").strip()
    raw_attempt = metadata.get(_COMPANY_PUBLIC_WEB_SOURCE_COMMAND_ATTEMPT_KEY)
    try:
        command_attempt = 0 if isinstance(raw_attempt, bool) else int(raw_attempt or 0)
    except (TypeError, ValueError):
        command_attempt = 0
    present = bool(command_id or lease_owner or raw_attempt not in (None, "", 0))
    valid = bool(command_id and lease_owner and command_attempt > 0)
    return (
        {
            "command_id": command_id,
            "command_attempt": command_attempt,
            "lease_owner": lease_owner,
        }
        if valid
        else {},
        bool(present and not valid),
    )


def _company_public_web_source_projection_revision(metadata_payload: Any) -> tuple[int, bool]:
    metadata = _json_load_dict(metadata_payload)
    if _COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY not in metadata:
        return 0, False
    raw_revision = metadata.get(_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY)
    if isinstance(raw_revision, bool) or not isinstance(raw_revision, int):
        return 0, True
    if not 1 <= raw_revision <= _COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_MAX:
        return 0, True
    return raw_revision, False


def _company_public_web_projection_revision_sql(metadata_expression: str) -> str:
    revision_text = f"COALESCE(({metadata_expression} ->> '{_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY}'), '')"
    return (
        f"(CASE WHEN {revision_text} ~ '^[1-9][0-9]{{0,18}}$' "
        f"AND ({revision_text})::numeric <= {_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_MAX}::numeric "
        f"THEN ({revision_text})::numeric ELSE 0::numeric END)"
    )


def company_public_web_projection_order_sql(
    *,
    metadata_expression: str,
    updated_at_expression: str,
    created_at_expression: str,
    run_id_expression: str,
) -> str:
    """Order revisioned projections first, with an explicit legacy fallback.

    Positive DB-owned revisions are the sole normal-path chronology. Brownfield
    rows without a valid revision retain their historical timestamp ordering;
    equal positive revisions use the immutable run id as their deterministic
    corruption/tie fallback and never consult wall clock time.
    """

    revision = _company_public_web_projection_revision_sql(metadata_expression)
    return (
        f"{revision} DESC, "
        f"CASE WHEN {revision} = 0 THEN {updated_at_expression} ELSE '' END DESC, "
        f"CASE WHEN {revision} = 0 THEN {created_at_expression} ELSE '' END DESC, "
        f"{run_id_expression} DESC"
    )


def _company_public_web_projection_incoming_wins_sql(
    *,
    incoming_metadata_expression: str,
    existing_metadata_expression: str,
    incoming_run_id_expression: str,
    existing_run_id_expression: str,
) -> str:
    incoming_revision = _company_public_web_projection_revision_sql(incoming_metadata_expression)
    existing_revision = _company_public_web_projection_revision_sql(existing_metadata_expression)
    return (
        f"ROW({incoming_revision}, {incoming_run_id_expression}) >= "
        f"ROW({existing_revision}, {existing_run_id_expression})"
    )


def _lock_current_company_public_web_source_command(
    cursor: Any,
    owner: dict[str, Any],
) -> dict[str, Any] | None:
    cursor.execute(
        """
        SELECT *
        FROM workflow_commands
        WHERE command_id = %s
          AND command_type = 'company.public_web.source.collect'
          AND owner = 'company_public_web_owner'
          AND status IN ('claimed', 'running')
          AND attempt = %s
          AND lease_owner = %s
          AND (NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()
        FOR UPDATE
        """,
        (
            str(owner.get("command_id") or "").strip(),
            int(owner.get("command_attempt") or 0),
            str(owner.get("lease_owner") or "").strip(),
        ),
    )
    return _fetch_one_dict_row(cursor, cursor.fetchone())


CONTROL_PLANE_LIVE_TABLES = (
    "candidates",
    "evidence",
    "jobs",
    "job_results",
    "job_events",
    "job_progress_event_summaries",
    "job_result_views",
    "job_result_lifecycle",
    "job_board_visible_patches",
    "job_materialization_items",
    "plan_review_sessions",
    "manual_review_items",
    "candidate_review_registry",
    "target_candidates",
    "asset_default_pointers",
    "asset_default_pointer_history",
    "crm_public_web_batches",
    "crm_public_web_runs",
    "person_public_web_assets",
    "person_public_web_signals",
    "person_assets",
    "person_evidence",
    "person_assertions",
    "raw_profile_index",
    "candidate_evidence_index",
    "projection_person_search_index",
    "crm_records",
    "crm_engagements",
    "crm_events",
    "crm_tasks",
    "crm_public_web_promotions",
    "company_public_web_asset_runs",
    "company_public_web_assets",
    "company_assets",
    "company_evidence",
    "company_assertions",
    "frontend_history_links",
    "agent_runtime_sessions",
    "agent_trace_spans",
    "agent_worker_runs",
    "workflow_job_leases",
    "workflow_recovery_intents",
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "agent_actions",
    "operation_runs",
    "acquisition_plan_previews",
    "agent_tool_result_slots",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
    "query_dispatches",
    "confidence_policy_runs",
    "confidence_policy_controls",
    "criteria_feedback",
    "criteria_patterns",
    "criteria_versions",
    "criteria_compiler_runs",
    "criteria_result_diffs",
    "criteria_pattern_suggestions",
    "organization_asset_registry",
    "organization_execution_profiles",
    "acquisition_shard_registry",
    "cloud_asset_operation_ledger",
    "asset_materialization_generations",
    "asset_membership_index",
    "candidate_materialization_state",
    "snapshot_materialization_runs",
    "serving_projections",
    "serving_projection_members",
    "projection_manifest_shards",
    "run_projection_links",
    "collection_authoritative_pointers",
    "linkedin_profile_registry",
    "linkedin_profile_registry_aliases",
    "linkedin_profile_registry_leases",
    "linkedin_profile_registry_events",
    "linkedin_profile_registry_backfill_runs",
    "runtime_provider_limiter_leases",
    "model_invocation_envelopes",
)

_PRIMARY_KEY_COLUMNS = {
    "candidates": ("candidate_id",),
    "evidence": ("evidence_id",),
    "jobs": ("job_id",),
    "job_results": ("job_id", "candidate_id"),
    "job_events": ("event_id",),
    "job_progress_event_summaries": ("job_id",),
    "job_result_views": ("view_id",),
    "job_result_lifecycle": ("job_id",),
    "job_board_visible_patches": ("patch_id",),
    "job_materialization_items": ("item_id",),
    "plan_review_sessions": ("review_id",),
    "manual_review_items": ("review_item_id",),
    "candidate_review_registry": ("record_id",),
    "target_candidates": ("record_id",),
    "asset_default_pointers": ("pointer_key",),
    "asset_default_pointer_history": ("history_id",),
    "target_candidate_public_web_batches": ("batch_id",),
    "target_candidate_public_web_runs": ("run_id",),
    "crm_public_web_batches": ("batch_id",),
    "crm_public_web_runs": ("run_id",),
    "person_public_web_assets": ("asset_id",),
    "person_public_web_signals": ("signal_id",),
    "person_assets": ("asset_id",),
    "person_evidence": ("evidence_id",),
    "person_assertions": ("assertion_id",),
    "raw_profile_index": ("person_identity_key",),
    "candidate_evidence_index": ("person_identity_key",),
    "projection_person_search_index": ("projection_id", "candidate_identity_key"),
    "crm_records": ("crm_record_id",),
    "crm_engagements": ("engagement_id",),
    "crm_events": ("event_id",),
    "crm_tasks": ("task_id",),
    "target_candidate_public_web_promotions": ("promotion_id",),
    "crm_public_web_promotions": ("promotion_id",),
    "company_public_web_asset_runs": ("run_id",),
    "company_public_web_assets": ("asset_id",),
    "company_assets": ("asset_id",),
    "company_evidence": ("evidence_id",),
    "company_assertions": ("assertion_id",),
    "frontend_history_links": ("history_id",),
    "agent_runtime_sessions": ("session_id",),
    "agent_trace_spans": ("span_id",),
    "agent_worker_runs": ("worker_id",),
    "workflow_job_leases": ("job_id",),
    "workflow_recovery_intents": ("job_id",),
    "workflow_events": ("event_id",),
    "workflow_current_state": ("workflow_run_id",),
    "workflow_commands": ("command_id",),
    "runtime_outbox": ("outbox_id",),
    "agent_actions": ("action_id",),
    "operation_runs": ("operation_run_id",),
    "acquisition_plan_previews": ("preview_id",),
    "agent_tool_result_slots": ("result_slot_id",),
    "agent_tool_result_attempts": ("result_attempt_id",),
    "agent_tool_result_journal": ("journal_id",),
    "acquisition_runs": ("acquisition_run_id",),
    "workflow_activity_runs": ("activity_run_id",),
    "workflow_activity_attempts": ("attempt_id",),
    "workflow_entity_deltas": ("delta_id",),
    "acquisition_discovery_lanes": ("lane_id",),
    "operation_events": ("event_id",),
    "query_dispatches": ("dispatch_id",),
    "confidence_policy_runs": ("policy_run_id",),
    "confidence_policy_controls": ("control_id",),
    "criteria_feedback": ("feedback_id",),
    "criteria_patterns": ("pattern_id",),
    "criteria_versions": ("version_id",),
    "criteria_compiler_runs": ("compiler_run_id",),
    "criteria_result_diffs": ("diff_id",),
    "criteria_pattern_suggestions": ("suggestion_id",),
    "organization_asset_registry": ("registry_id",),
    "organization_execution_profiles": ("profile_id",),
    "acquisition_shard_registry": ("shard_key",),
    "cloud_asset_operation_ledger": ("ledger_id",),
    "asset_materialization_generations": (
        "target_company",
        "snapshot_id",
        "asset_view",
        "artifact_kind",
        "artifact_key",
    ),
    "asset_membership_index": ("generation_key", "member_key"),
    "candidate_materialization_state": ("target_company", "snapshot_id", "asset_view", "candidate_id"),
    "snapshot_materialization_runs": ("run_id",),
    "serving_projections": ("projection_id",),
    "serving_projection_members": ("projection_id", "candidate_identity_key"),
    "projection_manifest_shards": ("shard_id",),
    "run_projection_links": ("run_id", "link_type"),
    "collection_authoritative_pointers": ("collection_id",),
    "linkedin_profile_registry": ("profile_url_key",),
    "linkedin_profile_registry_aliases": ("alias_url_key",),
    "linkedin_profile_registry_leases": ("profile_url_key",),
    "linkedin_profile_registry_events": ("event_id",),
    "linkedin_profile_registry_backfill_runs": ("run_key",),
    "runtime_provider_limiter_leases": ("lease_token",),
}

_JOB_RESULT_LIFECYCLE_DELTA_MONOTONIC_INT_FIELDS = {
    "delta_profile_required_count",
    "delta_profile_fetched_count",
    "delta_profile_applied_count",
    "delta_profile_materialized_count",
    "delta_profile_board_visible_count",
}
_JOB_RESULT_LIFECYCLE_STAGE1_MONOTONIC_INT_FIELDS = {
    "stage1_current_search_returned_count",
    "stage1_former_search_returned_count",
    "stage1_all_search_returned_count",
    "stage1_deduped_candidate_count",
    "stage1_deduped_profile_url_count",
    "stage1_profile_fetch_required_count",
    "stage1_profile_fetched_count",
}

# B4.3f: native PG DDL for the retired target-candidate Public Web tables,
# created on demand only inside the legacy-migration table context. Drift-free
# port of the sync-generated schema (SQLite shadow DDL -> TEXT/BIGINT/DOUBLE
# PRECISION, NOT NULL kept, no defaults, PK-only conflict target, no UNIQUE on
# idempotency_key). Deliberately NOT in migrations/0001_baseline.sql: these are
# migration-context-only tables, invisible to the normal runtime schema.
_LEGACY_TARGET_PUBLIC_WEB_MIGRATION_TABLE_DDL: dict[str, tuple[str, ...]] = {
    "target_candidate_public_web_batches": (
        """
        CREATE TABLE IF NOT EXISTS target_candidate_public_web_batches (
            batch_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_record_ids_json TEXT NOT NULL,
            source_families_json TEXT NOT NULL,
            options_json TEXT NOT NULL,
            run_ids_json TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            requested_by TEXT NOT NULL,
            force_refresh BIGINT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (batch_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_batches_updated
            ON target_candidate_public_web_batches (updated_at, status)
        """,
    ),
    "target_candidate_public_web_runs": (
        """
        CREATE TABLE IF NOT EXISTS target_candidate_public_web_runs (
            run_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            candidate_name TEXT NOT NULL,
            current_company TEXT NOT NULL,
            linkedin_url TEXT NOT NULL,
            linkedin_url_key TEXT NOT NULL,
            person_identity_key TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL,
            phase TEXT NOT NULL,
            source_families_json TEXT NOT NULL,
            options_json TEXT NOT NULL,
            query_manifest_json TEXT NOT NULL,
            search_checkpoint_json TEXT NOT NULL,
            fetch_checkpoint_json TEXT NOT NULL,
            analysis_checkpoint_json TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            artifact_root TEXT NOT NULL,
            worker_key TEXT NOT NULL,
            lease_owner TEXT NOT NULL,
            lease_expires_at TEXT,
            attempt_count BIGINT NOT NULL,
            last_error TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (run_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_batch
            ON target_candidate_public_web_runs (batch_id, updated_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_record
            ON target_candidate_public_web_runs (record_id, updated_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_status
            ON target_candidate_public_web_runs (status, updated_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_identity
            ON target_candidate_public_web_runs (linkedin_url_key, updated_at)
        """,
    ),
    "target_candidate_public_web_promotions": (
        """
        CREATE TABLE IF NOT EXISTS target_candidate_public_web_promotions (
            promotion_id TEXT NOT NULL,
            signal_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            asset_id TEXT NOT NULL,
            person_identity_key TEXT NOT NULL,
            record_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            candidate_name TEXT NOT NULL,
            current_company TEXT NOT NULL,
            linkedin_url_key TEXT NOT NULL,
            signal_kind TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            email_type TEXT NOT NULL,
            value TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            url TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_domain TEXT NOT NULL,
            source_family TEXT NOT NULL,
            source_title TEXT NOT NULL,
            confidence_label TEXT NOT NULL,
            confidence_score DOUBLE PRECISION NOT NULL,
            identity_match_label TEXT NOT NULL,
            identity_match_score DOUBLE PRECISION NOT NULL,
            publishable BIGINT NOT NULL,
            clean_profile_link BIGINT NOT NULL,
            link_shape_warnings_json TEXT NOT NULL,
            action TEXT NOT NULL,
            promotion_status TEXT NOT NULL,
            promoted_field TEXT NOT NULL,
            previous_value TEXT NOT NULL,
            new_value TEXT NOT NULL,
            operator TEXT NOT NULL,
            note TEXT NOT NULL,
            evidence_excerpt TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (promotion_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_record
            ON target_candidate_public_web_promotions (record_id, updated_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_signal
            ON target_candidate_public_web_promotions (signal_id, updated_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_run
            ON target_candidate_public_web_promotions (run_id, updated_at)
        """,
    ),
}

_READ_PREFERRED_MODES = {"prefer_postgres", "postgres_only"}
_AUTHORITATIVE_MODES = {"postgres_only"}
_RUNTIME_COORDINATION_TABLES = {
    "agent_trace_spans",
    "agent_worker_runs",
    "workflow_job_leases",
    "workflow_recovery_intents",
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "agent_actions",
    "operation_runs",
    "acquisition_plan_previews",
    "agent_tool_result_slots",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
    "crm_tasks",
    "company_assets",
    "company_evidence",
    "company_assertions",
    "job_materialization_items",
    "runtime_provider_limiter_leases",
}

_OPERATION_RUNTIME_TABLES = {
    "agent_actions",
    "operation_runs",
    "acquisition_plan_previews",
    "agent_tool_result_slots",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
}
_WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG = {
    "acquisition_runs": {
        "id_column": "acquisition_run_id",
        "columns": (
            "acquisition_run_id",
            "workspace_id",
            "operation_run_id",
            "workflow_run_id",
            "plan_id",
            "plan_review_id",
            "target_company",
            "query",
            "status",
            "current_phase",
            "request_json",
            "plan_json",
            "execution_bundle_json",
            "metadata_json",
            "idempotency_key",
            "created_at",
            "updated_at",
        ),
        "immutable_columns": (
            "acquisition_run_id",
            "workspace_id",
            "workflow_run_id",
            "operation_run_id",
            "idempotency_key",
        ),
        "json_object_columns": (
            "request_json",
            "plan_json",
            "execution_bundle_json",
            "metadata_json",
        ),
        "terminal_statuses": (
            "cancelled_before_probe",
            "cancelled_before_discovery",
            "cancelled_before_profile_fetch_activity",
            "projection_admitted",
            "completed",
            "failed",
            "cancelled",
        ),
        "write_once": False,
    },
    "acquisition_discovery_lanes": {
        "id_column": "lane_id",
        "columns": (
            "lane_id",
            "workspace_id",
            "acquisition_run_id",
            "workflow_run_id",
            "operation_run_id",
            "source_command_id",
            "activity_run_id",
            "target_company",
            "query",
            "provider",
            "status",
            "phase",
            "lane_plan_json",
            "provider_ref_json",
            "artifact_refs_json",
            "entity_counts_json",
            "downstream_command_ids_json",
            "idempotency_key",
            "metadata_json",
            "created_at",
            "updated_at",
        ),
        "immutable_columns": (
            "lane_id",
            "workspace_id",
            "acquisition_run_id",
            "workflow_run_id",
            "operation_run_id",
            "source_command_id",
            "activity_run_id",
            "idempotency_key",
        ),
        "json_object_columns": (
            "lane_plan_json",
            "provider_ref_json",
            "entity_counts_json",
            "metadata_json",
        ),
        "terminal_statuses": (
            "cancelled_before_discovery",
            "provider_discovery_completed",
            "completed",
            "failed",
            "cancelled",
        ),
        "write_once": False,
    },
    "workflow_activity_runs": {
        "id_column": "activity_run_id",
        "columns": (
            "activity_run_id",
            "workspace_id",
            "workflow_run_id",
            "operation_run_id",
            "acquisition_run_id",
            "command_id",
            "parent_activity_run_id",
            "activity_type",
            "owner",
            "status",
            "phase",
            "idempotency_key",
            "provider_ref_json",
            "input_json",
            "output_json",
            "artifact_refs_json",
            "entity_counts_json",
            "metadata_json",
            "created_at",
            "updated_at",
        ),
        "immutable_columns": (
            "activity_run_id",
            "workspace_id",
            "workflow_run_id",
            "operation_run_id",
            "acquisition_run_id",
            "command_id",
            "parent_activity_run_id",
            "activity_type",
            "owner",
            "idempotency_key",
        ),
        "json_object_columns": (
            "provider_ref_json",
            "input_json",
            "output_json",
            "entity_counts_json",
            "metadata_json",
        ),
        "terminal_statuses": (
            "succeeded",
            "completed",
            "failed",
            "cancelled",
            "cancelled_before_discovery",
            "cancelled_before_cache_lookup",
            "cancelled_poll_stopped",
            "cancelled_before_provider_attempt",
            "cancelled_before_domain_mutation",
            "cancelled_before_company_asset_sync",
            "cancelled_before_crm_mutation",
            "cancelled_before_fetch_upload",
        ),
        "write_once": False,
    },
    "workflow_activity_attempts": {
        "id_column": "attempt_id",
        "columns": (
            "attempt_id",
            "workspace_id",
            "activity_run_id",
            "workflow_run_id",
            "command_id",
            "attempt_number",
            "status",
            "provider",
            "provider_request_ref",
            "provider_run_ref",
            "started_at",
            "completed_at",
            "next_retry_at",
            "rate_limit_ref_json",
            "error_json",
            "input_json",
            "output_json",
            "artifact_refs_json",
            "idempotency_key",
            "metadata_json",
            "created_at",
            "updated_at",
        ),
        "immutable_columns": (
            "attempt_id",
            "workspace_id",
            "activity_run_id",
            "workflow_run_id",
            "command_id",
            "attempt_number",
            "provider",
            "idempotency_key",
        ),
        "json_object_columns": (
            "rate_limit_ref_json",
            "error_json",
            "input_json",
            "output_json",
            "metadata_json",
        ),
        "terminal_statuses": (
            "succeeded",
            "completed",
            "failed",
            "cancelled",
            "cancelled_remote_ignored",
        ),
        "write_once": False,
    },
    "workflow_entity_deltas": {
        "id_column": "delta_id",
        "columns": (
            "delta_id",
            "workspace_id",
            "workflow_run_id",
            "operation_run_id",
            "command_id",
            "activity_run_id",
            "attempt_id",
            "acquisition_run_id",
            "entity_type",
            "entity_key",
            "delta_kind",
            "status",
            "reason",
            "source_ref_json",
            "entity_payload_json",
            "projection_effect_json",
            "artifact_refs_json",
            "idempotency_key",
            "metadata_json",
            "created_at",
            "updated_at",
        ),
        "immutable_columns": (
            "delta_id",
            "workspace_id",
            "workflow_run_id",
            "operation_run_id",
            "command_id",
            "activity_run_id",
            "attempt_id",
            "acquisition_run_id",
            "entity_type",
            "entity_key",
            "delta_kind",
            "idempotency_key",
        ),
        "json_object_columns": (
            "source_ref_json",
            "entity_payload_json",
            "projection_effect_json",
            "metadata_json",
        ),
        "terminal_statuses": (),
        "write_once": True,
    },
}

# Fixed owner-specific cancellation modes. Callers choose a semantic boundary;
# table names, command identity, statuses, and patches remain adapter-owned so
# this UoW cannot become a caller-supplied cross-table mutation primitive.
_ACQUISITION_OWNER_CANCEL_CONFIG = {
    "acquisition_plan_commit_before_probe": {
        "command_type": "acquisition.plan.commit",
        "owner": "acquisition_planner",
        "run_status": "cancelled_before_probe",
        "run_metadata_flags": {"probe_command_planned": False},
        "activity_type": "",
        "activity_status": "",
        "downstream_block_reason": "acquisition_plan_commit_cancel_blocked_after_probe_planned",
        "effect_block_reason": "acquisition_plan_commit_cancel_blocked_after_probe_started",
        "resolve_run_from_command": True,
    },
    "acquisition_scale_plan_before_discovery": {
        "command_type": "acquisition.scale.plan",
        "owner": "acquisition_scale_planner",
        "run_status": "cancelled_before_discovery",
        "run_metadata_flags": {"discovery_command_planned": False},
        "activity_type": "",
        "activity_status": "cancelled_before_discovery",
        "downstream_block_reason": "acquisition_scale_plan_cancel_blocked_after_discovery_planned",
        "effect_block_reason": "acquisition_scale_plan_cancel_blocked_after_discovery_started",
        "resolve_run_from_command": True,
    },
    "profile_fetch_activity_before_cache_lookup_attempt": {
        "command_type": "linkedin.profile_fetch.activity.run",
        "owner": "linkedin_profile_activity_owner",
        "run_status": "cancelled_before_profile_fetch_activity",
        "run_metadata_flags": {
            "activity_attempt_started": False,
            "profile_entity_delta_recorded": False,
        },
        "activity_type": "linkedin.profile_fetch.activity.run",
        "activity_status": "cancelled_before_cache_lookup",
        "downstream_block_reason": "profile_fetch_activity_cancel_blocked_after_downstream_planned",
        "effect_block_reason": "profile_fetch_activity_cancel_blocked_after_cache_lookup_started",
        "resolve_run_from_command": False,
    },
}
_RUNNING_RECOVERABLE_WAIT_STAGES = {
    "submitting_remote_search",
    "submitting_remote_harvest",
    "waiting_remote_search",
    "waiting_remote_harvest",
    "persisting_terminal_harvest_profiles",
}
_RETRYABLE_POSTGRES_SQLSTATES = {"40P01", "40001"}

# Pool exhaustion (pool.getconn() timed out waiting for a free connection) is
# transient and must be retried like a deadlock/serialization failure. Import
# is guarded: psycopg_pool is optional in some unit-test environments.
try:
    from psycopg_pool import PoolTimeout as _PSYCOPG_POOL_TIMEOUT
except Exception:  # pragma: no cover - optional dependency missing
    _PSYCOPG_POOL_TIMEOUT = None

_CONTROL_PLANE_POSTGRES_MAX_RETRIES = 3
SESSION_ADVISORY_LOCK_ACQUISITION_TIMEOUT_SECONDS = 5.0
_SESSION_ADVISORY_LOCK_POLL_SECONDS = 0.05
_BULK_UPSERT_DIRECT_ROW_LIMIT = 1000
_BULK_UPSERT_DIRECT_PARAM_LIMIT = 20000
_SERIAL_SEQUENCE_NAMES = {
    "job_events": ("event_id", "job_events_event_id_seq"),
    "agent_runtime_sessions": ("session_id", "agent_runtime_sessions_session_id_seq"),
    "plan_review_sessions": ("review_id", "plan_review_sessions_review_id_seq"),
    "manual_review_items": ("review_item_id", "manual_review_items_review_item_id_seq"),
    "query_dispatches": ("dispatch_id", "query_dispatches_dispatch_id_seq"),
    "confidence_policy_runs": ("policy_run_id", "confidence_policy_runs_policy_run_id_seq"),
    "confidence_policy_controls": ("control_id", "confidence_policy_controls_control_id_seq"),
    "criteria_feedback": ("feedback_id", "criteria_feedback_feedback_id_seq"),
    "criteria_patterns": ("pattern_id", "criteria_patterns_pattern_id_seq"),
    "criteria_versions": ("version_id", "criteria_versions_version_id_seq"),
    "criteria_compiler_runs": ("compiler_run_id", "criteria_compiler_runs_compiler_run_id_seq"),
    "criteria_result_diffs": ("diff_id", "criteria_result_diffs_diff_id_seq"),
    "criteria_pattern_suggestions": ("suggestion_id", "criteria_pattern_suggestions_suggestion_id_seq"),
    "organization_asset_registry": ("registry_id", "organization_asset_registry_registry_id_seq"),
    "organization_execution_profiles": ("profile_id", "organization_execution_profiles_profile_id_seq"),
    "cloud_asset_operation_ledger": ("ledger_id", "cloud_asset_operation_ledger_ledger_id_seq"),
    "agent_trace_spans": ("span_id", "agent_trace_spans_span_id_seq"),
    "agent_worker_runs": ("worker_id", "agent_worker_runs_worker_id_seq"),
    "linkedin_profile_registry_events": ("event_id", "linkedin_profile_registry_events_event_id_seq"),
}


class _TransactionAdvisoryLockBusy(RuntimeError):
    """Internal signal used to retry without holding a pooled connection."""


class ControlPlaneAdvisoryLockBusy(RuntimeError):
    """Typed finite-budget failure for a direct session advisory lock."""

    def __init__(self, *, lock_key: str, timeout_seconds: float) -> None:
        self.lock_key = str(lock_key or "").strip()
        self.timeout_seconds = max(0.0, float(timeout_seconds))
        super().__init__(f"control-plane advisory lock busy after {self.timeout_seconds:.3f}s: {self.lock_key}")


def resolve_control_plane_postgres_live_mode(value: Any) -> str:
    normalized = _normalize_postgres_identifier(value).lower()
    if normalized in {"mirror", "prefer_postgres", "postgres_only"}:
        return normalized
    return "disabled"


_CONTROL_PLANE_PG_POOL_MIN_ENV = "SOURCING_CONTROL_PLANE_PG_POOL_MIN"
_CONTROL_PLANE_PG_POOL_MAX_ENV = "SOURCING_CONTROL_PLANE_PG_POOL_MAX"
_CONTROL_PLANE_PG_POOL_MIN_DEFAULT = 1
_CONTROL_PLANE_PG_POOL_MAX_DEFAULT = 8

_POOLED_CONNECTION_CLASS: Any = None


def _resolve_control_plane_pg_pool_size_limits() -> tuple[int, int]:
    def _read_limit(name: str, default: int) -> int:
        raw = str(os.getenv(name) or "").strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            return default
        return value if value > 0 else default

    min_size = _read_limit(_CONTROL_PLANE_PG_POOL_MIN_ENV, _CONTROL_PLANE_PG_POOL_MIN_DEFAULT)
    max_size = _read_limit(_CONTROL_PLANE_PG_POOL_MAX_ENV, _CONTROL_PLANE_PG_POOL_MAX_DEFAULT)
    return min_size, max(min_size, max_size)


def _resolve_pooled_connection_class(psycopg_module: Any) -> Any:
    """Connection class preserving the legacy ``client_encoding`` connect fallback."""

    global _POOLED_CONNECTION_CLASS
    if _POOLED_CONNECTION_CLASS is None:

        class _PooledControlPlaneConnection(psycopg_module.Connection):  # type: ignore[misc, name-defined]
            @classmethod
            def connect(cls, conninfo: str = "", **kwargs: Any) -> Any:
                try:
                    return super().connect(conninfo, **kwargs)
                except TypeError:
                    kwargs.pop("client_encoding", None)
                    return _configure_postgres_connection_utf8(super().connect(conninfo, **kwargs))

        _POOLED_CONNECTION_CLASS = _PooledControlPlaneConnection
    return _POOLED_CONNECTION_CLASS


def _import_psycopg_pool() -> Any:
    try:
        import psycopg_pool
    except ImportError as exc:  # pragma: no cover - exercised via caller
        raise RuntimeError(
            "psycopg_pool is required for Postgres control-plane pooling. "
            "Install psycopg-pool in the active environment."
        ) from exc
    return psycopg_pool


class _PooledConnectionHandle:
    """Checkout handle that mimics a dedicated ``psycopg.connect()`` connection.

    Semantics preserved from the pre-pool adapter:
    - ``with handle:`` commits on clean exit, rolls back on exception (psycopg
      ``Connection.__exit__`` parity), then releases to the pool instead of closing.
    - ``close()`` discards any uncommitted transaction (rollback) before returning
      the connection to the pool, matching the server-side effect of closing a
      dedicated connection mid-transaction.
    - All other attribute access (``cursor``, ``commit``, ``rollback``, ...) is
      proxied to the underlying pooled connection.
    """

    __slots__ = ("_pool", "_connection", "_released")

    def __init__(self, pool: Any, connection: Any) -> None:
        self._pool = pool
        self._connection = connection
        self._released = False

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        return self._connection.cursor(*args, **kwargs)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        if self._released:
            return
        self._released = True
        connection = self._connection
        try:
            try:
                connection.rollback()
            except Exception:
                pass
            self._pool.putconn(connection)
        except Exception:
            try:
                connection.close()
            except Exception:
                pass

    def __enter__(self) -> "_PooledConnectionHandle":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self.close()
        return False

    def __getattr__(self, name: str) -> Any:
        if name in _PooledConnectionHandle.__slots__:
            raise AttributeError(name)
        return getattr(self._connection, name)


class LiveControlPlanePostgresAdapter:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        sqlite_path: str | Path = "",
        dsn: str = "",
        mode: str = "disabled",
        tables: tuple[str, ...] = CONTROL_PLANE_LIVE_TABLES,
    ) -> None:
        self.runtime_dir = Path(runtime_dir).expanduser()
        self.sqlite_path = str(sqlite_path).strip() or str(
            resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.runtime_dir).expanduser()
        )
        self.dsn = str(dsn or resolve_control_plane_postgres_dsn(self.runtime_dir)).strip()
        self.schema = normalize_control_plane_postgres_schema(resolve_control_plane_postgres_schema(self.runtime_dir))
        self.mode = resolve_control_plane_postgres_live_mode(mode)
        self.tables = tuple(str(item).strip() for item in tables if str(item).strip())
        self._lock = threading.Lock()
        self._bootstrapped = False
        self._runtime_schema_ready = False
        self._control_plane_writer_schema_ready = False
        self._psycopg: Any | None = None
        self._pool: Any | None = None
        self._pool_pid: int | None = None
        self._pool_lock = threading.Lock()
        self._legacy_target_public_web_migration_table_depth = 0

    @property
    def enabled(self) -> bool:
        return bool(self.dsn) and self.mode != "disabled"

    def should_mirror(self, table_name: str) -> bool:
        normalized_table = _normalize_postgres_identifier(table_name)
        return self.enabled and (
            normalized_table in self.tables or self._legacy_target_public_web_migration_table_enabled(normalized_table)
        )

    def should_prefer_read(self, table_name: str) -> bool:
        return self.should_mirror(table_name) and self.mode in _READ_PREFERRED_MODES

    def is_authoritative(self, table_name: str) -> bool:
        return self.should_prefer_read(table_name) and self.mode in _AUTHORITATIVE_MODES

    def _legacy_target_public_web_migration_table_enabled(self, table_name: str) -> bool:
        return (
            int(getattr(self, "_legacy_target_public_web_migration_table_depth", 0) or 0) > 0
            and _normalize_postgres_identifier(table_name) in LEGACY_TARGET_PUBLIC_WEB_TABLES
        )

    @contextmanager
    def legacy_target_public_web_migration_table_context(self, reason: str = "") -> Any:
        self._legacy_target_public_web_migration_table_depth += 1
        try:
            yield {
                "status": "enabled",
                "reason": str(reason or "").strip() or "legacy_target_public_web_migration",
                "tables": list(LEGACY_TARGET_PUBLIC_WEB_TABLES),
            }
        finally:
            self._legacy_target_public_web_migration_table_depth = max(
                0,
                self._legacy_target_public_web_migration_table_depth - 1,
            )

    @contextmanager
    def profile_prefetch_scheduler_lock(self, *, source_job: str, snapshot_dir: str) -> Any:
        """Serialize profile-prefetch replan/claim critical sections for a job snapshot.

        The lock is transaction-scoped and fail-fast. PostgreSQL releases it
        automatically if the connection exits through an exception; callers must
        keep the protected region short, must not submit providers while holding
        this lock, and must yield when ``acquired`` is false instead of waiting.
        """

        normalized_source_job = str(source_job or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        if (
            not self.should_prefer_read("linkedin_profile_registry")
            or not normalized_source_job
            or not normalized_snapshot_dir
        ):
            yield {
                "kind": "none",
                "lock_kind": "none",
                "distributed": False,
                "reason": "postgres_profile_registry_not_authoritative_or_scope_missing",
            }
            return
        lock_key = f"profile_prefetch_scheduler:{normalized_source_job}:{normalized_snapshot_dir}"
        attempt = 0
        connection = None
        while True:
            try:
                connection = self._connect()
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (self._advisory_lock_key(lock_key),)
                    )
                    row = cursor.fetchone()
                    acquired = bool(row[0] if isinstance(row, (list, tuple)) and row else row)
                if not acquired:
                    try:
                        yield {
                            "kind": "pg_try_advisory_xact_lock",
                            "lock_kind": "pg_try_advisory_xact_lock",
                            "distributed": True,
                            "acquired": False,
                            "busy": True,
                            "lock_key": lock_key,
                            "source": "control_plane_live_postgres",
                            "scope": "source_job_snapshot_dir",
                            "reason": "profile_prefetch_scheduler_lock_busy",
                        }
                    finally:
                        if connection is not None:
                            try:
                                connection.close()
                            except Exception:
                                pass
                    return
                break
            except Exception as exc:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))
        try:
            yield {
                "kind": "pg_try_advisory_xact_lock",
                "lock_kind": "pg_try_advisory_xact_lock",
                "distributed": True,
                "acquired": True,
                "busy": False,
                "lock_key": lock_key,
                "source": "control_plane_live_postgres",
                "scope": "source_job_snapshot_dir",
            }
        except Exception:
            if connection is not None:
                connection.rollback()
            raise
        else:
            if connection is not None:
                connection.commit()
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    @contextmanager
    def board_visible_patch_publication_lock(self, *, job_id: str, snapshot_id: str) -> Any:
        """Serialize board-visible patch sequence/cumulative writes for a job snapshot."""

        normalized_job_id = str(job_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if (
            not self.should_prefer_read("job_board_visible_patches")
            or not normalized_job_id
            or not normalized_snapshot_id
        ):
            yield {
                "kind": "none",
                "lock_kind": "none",
                "distributed": False,
                "reason": "postgres_board_visible_patches_not_authoritative_or_scope_missing",
            }
            return
        lock_key = f"board_visible_patch_publication:{normalized_job_id}:{normalized_snapshot_id}"
        attempt = 0
        connection = None
        while True:
            try:
                connection = self._connect()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self._advisory_lock_key(lock_key),))
                break
            except Exception as exc:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))
        try:
            yield {
                "kind": "pg_advisory_xact_lock",
                "lock_kind": "pg_advisory_xact_lock",
                "distributed": True,
                "lock_key": lock_key,
                "source": "control_plane_live_postgres",
                "scope": "job_snapshot_board_visible_publication",
            }
        except Exception:
            if connection is not None:
                connection.rollback()
            raise
        else:
            if connection is not None:
                connection.commit()
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    def insert_model_invocation_envelope(
        self,
        *,
        table_name: str,
        row: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Plain INSERT or exact immutable replay for one full-PFX envelope.

        This table intentionally bypasses ``upsert_row``: a generic
        ``ON CONFLICT DO UPDATE`` would make canonical evidence replaceable.
        """

        if _normalize_postgres_identifier(table_name) != "model_invocation_envelopes":
            raise ValueError("insert_model_invocation_envelope requires table_name=model_invocation_envelopes")
        if not self.should_prefer_read("model_invocation_envelopes"):
            return None
        expected_keys = {
            "runtime_namespace",
            "provider_mode",
            "workspace_id",
            "scope_digest",
            "coordination_plan_review_id",
            "model_invocation_envelope_ref",
            "envelope_schema_version",
            "envelope_digest",
            "envelope_record_json",
            "retention_policy_version",
        }
        if set(row) != expected_keys:
            raise ValueError("model_invocation_envelope_insert_keyset_invalid")
        payload = dict(row)
        pfx_columns = (
            "runtime_namespace",
            "provider_mode",
            "workspace_id",
            "scope_digest",
            "coordination_plan_review_id",
        )
        pfx_values = tuple(payload[column] for column in pfx_columns)
        reference = str(payload["model_invocation_envelope_ref"])
        digest = str(payload["envelope_digest"])
        lock_key = f"model_invocation_envelopes:{reference}"
        self.ensure_bootstrapped()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, lock_key)
                        prefix_where = " AND ".join(f"{column} = %s" for column in pfx_columns)
                        cursor.execute(
                            f"SELECT * FROM model_invocation_envelopes WHERE {prefix_where} "
                            "AND envelope_digest = %s FOR UPDATE",
                            (*pfx_values, digest),
                        )
                        by_digest = _fetch_one_dict_row(cursor, cursor.fetchone())
                        cursor.execute(
                            f"SELECT * FROM model_invocation_envelopes WHERE {prefix_where} "
                            "AND model_invocation_envelope_ref = %s FOR UPDATE",
                            (*pfx_values, reference),
                        )
                        by_reference = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if by_digest is not None or by_reference is not None:
                            if by_digest is None or by_reference is None:
                                raise ValueError("model_invocation_envelope_collision:ref_or_digest")
                            if by_digest != by_reference:
                                raise ValueError("model_invocation_envelope_collision:identity_split")
                            immutable_identity_fields = (
                                *pfx_columns,
                                "model_invocation_envelope_ref",
                                "envelope_schema_version",
                                "envelope_digest",
                                "retention_policy_version",
                            )
                            mismatches = [
                                field_name
                                for field_name in immutable_identity_fields
                                if by_digest.get(field_name) != payload.get(field_name)
                            ]
                            if mismatches:
                                raise ValueError("model_invocation_envelope_collision:" + ",".join(sorted(mismatches)))
                            # A valid tombstone is an immutable terminal identity. Return it
                            # unchanged so the typed repository reports PurgedError; comparing
                            # its intentionally erased canonical JSON to the retry payload would
                            # misclassify the lifecycle terminal as an identity collision.
                            if by_digest.get("retention_state") == "purged_tombstone":
                                connection.commit()
                                return by_digest
                            if by_digest.get("envelope_record_json") != payload.get("envelope_record_json"):
                                raise ValueError("model_invocation_envelope_collision:envelope_record_json")
                            connection.commit()
                            return by_digest

                        columns = tuple(payload)
                        cursor.execute(
                            "INSERT INTO model_invocation_envelopes ("
                            + ", ".join(columns)
                            + ", retained_until) VALUES ("
                            + ", ".join(["%s"] * len(columns))
                            + ", transaction_timestamp() + interval '30 days') RETURNING *",
                            tuple(payload[column] for column in columns),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return inserted
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def get_model_invocation_envelope(
        self,
        *,
        table_name: str,
        runtime_namespace: str,
        provider_mode: str,
        workspace_id: str,
        scope_digest: str,
        coordination_plan_review_id: int,
        model_invocation_envelope_ref: str,
        envelope_digest: str,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "model_invocation_envelopes":
            raise ValueError("get_model_invocation_envelope requires table_name=model_invocation_envelopes")
        if not self.should_prefer_read("model_invocation_envelopes"):
            return None
        return self.select_one(
            "model_invocation_envelopes",
            where_sql=(
                "runtime_namespace = %s AND provider_mode = %s AND workspace_id = %s "
                "AND scope_digest = %s AND coordination_plan_review_id = %s "
                "AND model_invocation_envelope_ref = %s AND envelope_digest = %s"
            ),
            params=[
                runtime_namespace,
                provider_mode,
                workspace_id,
                scope_digest,
                coordination_plan_review_id,
                model_invocation_envelope_ref,
                envelope_digest,
            ],
        )

    def purge_expired_model_invocation_envelopes(
        self,
        *,
        table_name: str,
        retention_policy_version: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """DB-clock retained-to-tombstone CAS for the fixed 30-day policy."""

        if _normalize_postgres_identifier(table_name) != "model_invocation_envelopes":
            raise ValueError("purge_expired_model_invocation_envelopes requires table_name=model_invocation_envelopes")
        if not self.should_prefer_read("model_invocation_envelopes"):
            return []
        self.ensure_bootstrapped()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            WITH candidates AS (
                                SELECT runtime_namespace, provider_mode, workspace_id, scope_digest,
                                       coordination_plan_review_id, model_invocation_envelope_ref,
                                       state_version
                                FROM model_invocation_envelopes
                                WHERE retention_policy_version = %s
                                  AND retention_state = 'retained'
                                  AND retained_until <= transaction_timestamp()
                                ORDER BY retained_until, runtime_namespace, provider_mode, workspace_id,
                                         scope_digest, coordination_plan_review_id,
                                         model_invocation_envelope_ref
                                LIMIT %s
                                FOR UPDATE SKIP LOCKED
                            )
                            UPDATE model_invocation_envelopes AS envelopes
                            SET envelope_record_json = NULL,
                                retention_state = 'purged_tombstone',
                                purged_at = transaction_timestamp(),
                                state_version = envelopes.state_version + 1
                            FROM candidates
                            WHERE envelopes.runtime_namespace = candidates.runtime_namespace
                              AND envelopes.provider_mode = candidates.provider_mode
                              AND envelopes.workspace_id = candidates.workspace_id
                              AND envelopes.scope_digest = candidates.scope_digest
                              AND envelopes.coordination_plan_review_id = candidates.coordination_plan_review_id
                              AND envelopes.model_invocation_envelope_ref = candidates.model_invocation_envelope_ref
                              AND envelopes.state_version = candidates.state_version
                              AND envelopes.retention_state = 'retained'
                              AND envelopes.retained_until <= transaction_timestamp()
                            RETURNING envelopes.*
                            """,
                            (retention_policy_version, limit),
                        )
                        rows = _fetch_all_dict_rows(cursor)
                    connection.commit()
                    return rows
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def insert_row_with_generated_id(
        self,
        *,
        table_name: str,
        row: dict[str, Any] | None,
        id_column: str = "",
        sequence_name: str = "",
    ) -> dict[str, Any] | None:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_prefer_read(normalized_table):
            return None
        payload = _normalize_postgres_row_payload(dict(row or {}))
        resolved_id_column, resolved_sequence_name = self._resolve_serial_sequence(
            normalized_table,
            id_column=id_column,
            sequence_name=sequence_name,
        )
        if not resolved_id_column or not resolved_sequence_name:
            return None
        self._ensure_table_write_schema(normalized_table)
        columns = [column for column in payload.keys() if str(column or "").strip() and column != resolved_id_column]
        quoted_table_name = _quote_identifier(normalized_table)
        quoted_columns = [_quote_identifier(resolved_id_column), *(_quote_identifier(column) for column in columns)]
        placeholders = ", ".join(["%s"] * len(columns))
        sequence_expr = f"nextval({_quote_string_literal(resolved_sequence_name)}::regclass)"
        values_sql = sequence_expr if not placeholders else f"{sequence_expr}, {placeholders}"
        sql = f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({values_sql}) RETURNING *"
        return self._execute_returning_one(sql, tuple(payload.get(column) for column in columns))

    def upsert_row_with_generated_id(
        self,
        *,
        table_name: str,
        row: dict[str, Any] | None,
        conflict_columns: list[str] | tuple[str, ...],
        id_column: str = "",
        sequence_name: str = "",
        update_columns: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_prefer_read(normalized_table):
            return None
        payload = _normalize_postgres_row_payload(dict(row or {}))
        resolved_id_column, resolved_sequence_name = self._resolve_serial_sequence(
            normalized_table,
            id_column=id_column,
            sequence_name=sequence_name,
        )
        normalized_conflict_columns = [
            str(column or "").strip() for column in list(conflict_columns or []) if str(column or "").strip()
        ]
        if not resolved_id_column or not resolved_sequence_name or not normalized_conflict_columns:
            return None
        self._ensure_table_write_schema(normalized_table)
        provided_id_value = payload.get(resolved_id_column)
        columns = [column for column in payload.keys() if str(column or "").strip() and column != resolved_id_column]
        quoted_table_name = _quote_identifier(normalized_table)
        quoted_columns = [_quote_identifier(resolved_id_column), *(_quote_identifier(column) for column in columns)]
        placeholders = ", ".join(["%s"] * len(columns))
        id_expr = f"COALESCE(%s, nextval({_quote_string_literal(resolved_sequence_name)}::regclass))"
        values_sql = id_expr if not placeholders else f"{id_expr}, {placeholders}"
        normalized_update_columns = [
            str(column or "").strip()
            for column in list(update_columns or columns)
            if str(column or "").strip() and str(column or "").strip() != resolved_id_column
        ]
        if not normalized_update_columns:
            conflict_sql = f" ON CONFLICT ({', '.join(_quote_identifier(column) for column in normalized_conflict_columns)}) DO NOTHING"
        else:
            conflict_sql = (
                f" ON CONFLICT ({', '.join(_quote_identifier(column) for column in normalized_conflict_columns)}) DO UPDATE SET "
                + ", ".join(
                    f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
                    for column in normalized_update_columns
                )
            )
        sql = (
            f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({values_sql})"
            f"{conflict_sql} RETURNING *"
        )
        return self._execute_returning_one(
            sql,
            (
                None if provided_id_value in {None, "", 0} else provided_id_value,
                *(payload.get(column) for column in columns),
            ),
        )

    def update_row_returning(
        self,
        *,
        table_name: str,
        id_column: str,
        id_value: Any,
        row: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        normalized_table = str(table_name or "").strip()
        normalized_id_column = _normalize_postgres_identifier(id_column)
        if not self.should_prefer_read(normalized_table) or not normalized_id_column:
            return None
        payload = _normalize_postgres_row_payload(
            {
                str(column or "").strip(): value
                for column, value in dict(row or {}).items()
                if str(column or "").strip() and str(column or "").strip() != normalized_id_column
            }
        )
        if not payload:
            return None
        self._ensure_table_write_schema(normalized_table)
        assignments = ", ".join(f"{_quote_identifier(column)} = %s" for column in payload.keys())
        sql = (
            f"UPDATE {_quote_identifier(normalized_table)} SET {assignments} "
            f"WHERE {_quote_identifier(normalized_id_column)} = %s RETURNING *"
        )
        return self._execute_returning_one(sql, (*payload.values(), id_value))

    def review_criteria_suggestion_if_owned(
        self,
        *,
        table_name: str = "criteria_pattern_suggestions",
        suggestion_id: int,
        action: str,
        reviewer: str,
        notes: str,
        expected_requester_id: str,
        expected_tenant_id: str,
        additional_job_ids: list[str] | tuple[str, ...] = (),
    ) -> dict[str, Any] | None:
        """Authorize every suggestion job source and commit its review atomically.

        The locked suggestion and feedback rows are the frozen source snapshot
        returned to the caller. Every distinct non-blank direct, feedback, or
        caller-supplied job reference is locked and exact-owner checked before
        either the applied pattern or review row can be written.
        """

        if _normalize_postgres_identifier(table_name) != "criteria_pattern_suggestions":
            raise ValueError("review_criteria_suggestion_if_owned requires criteria_pattern_suggestions")
        normalized_suggestion_id = int(suggestion_id or 0)
        if normalized_suggestion_id <= 0:
            return {"status": "suggestion_not_found"}
        required_tables = ("criteria_pattern_suggestions", "criteria_feedback", "criteria_patterns", "jobs")
        if any(not self.should_prefer_read(required_table) for required_table in required_tables):
            return None
        self.ensure_bootstrapped()
        for required_table in required_tables:
            self._ensure_table_write_schema(required_table)

        normalized_requester = str(expected_requester_id or "").strip()
        normalized_tenant = str(expected_tenant_id or "").strip()
        normalized_action = str(action or "").strip().lower()
        if normalized_action in {"approve", "approved", "apply", "applied"}:
            review_status = "applied"
        elif normalized_action in {"reject", "rejected"}:
            review_status = "rejected"
        else:
            review_status = "suggested"

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            'SELECT * FROM "criteria_pattern_suggestions" WHERE suggestion_id = %s FOR UPDATE',
                            (normalized_suggestion_id,),
                        )
                        suggestion = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if suggestion is None:
                            connection.commit()
                            return {"status": "suggestion_not_found"}

                        source_feedback: dict[str, Any] | None = None
                        source_feedback_id = int(suggestion.get("source_feedback_id") or 0)
                        if source_feedback_id:
                            cursor.execute(
                                'SELECT * FROM "criteria_feedback" WHERE feedback_id = %s FOR UPDATE',
                                (source_feedback_id,),
                            )
                            source_feedback = _fetch_one_dict_row(cursor, cursor.fetchone())

                        job_ids: list[str] = []
                        for raw_job_id in (
                            suggestion.get("source_job_id"),
                            (source_feedback or {}).get("job_id"),
                            *additional_job_ids,
                        ):
                            job_id = str(raw_job_id or "").strip()
                            if job_id and job_id not in job_ids:
                                job_ids.append(job_id)
                        for job_id in sorted(job_ids):
                            cursor.execute('SELECT * FROM "jobs" WHERE job_id = %s FOR SHARE', (job_id,))
                            job = _fetch_one_dict_row(cursor, cursor.fetchone())
                            owner_matches = job is not None
                            if normalized_requester or normalized_tenant:
                                owner_matches = bool(
                                    normalized_requester
                                    and normalized_tenant
                                    and job is not None
                                    and str(job.get("requester_id") or "").strip() == normalized_requester
                                    and str(job.get("tenant_id") or "").strip() == normalized_tenant
                                )
                            if not owner_matches:
                                connection.commit()
                                return {"status": "owner_miss"}

                        now = _utc_now_sql_timestamp()
                        applied_pattern: dict[str, Any] | None = None
                        applied_pattern_id = 0
                        if review_status == "applied":
                            metadata = _json_load_dict(suggestion.get("metadata_json"))
                            metadata.update(
                                {
                                    "source_suggestion_id": normalized_suggestion_id,
                                    "reviewed_by": str(reviewer or "").strip(),
                                    "review_notes": str(notes or "").strip(),
                                    "suggestion_status": "applied",
                                }
                            )
                            cursor.execute(
                                """
                                INSERT INTO criteria_patterns (
                                    pattern_id, target_company, pattern_type, subject, value, status,
                                    confidence, source_feedback_id, metadata_json, created_at, updated_at
                                ) VALUES (
                                    nextval('criteria_patterns_pattern_id_seq'::regclass), %s, %s, %s, %s,
                                    'active', %s, %s, %s, %s, %s
                                )
                                ON CONFLICT (target_company, pattern_type, subject, value) DO UPDATE SET
                                    status = EXCLUDED.status,
                                    confidence = EXCLUDED.confidence,
                                    source_feedback_id = EXCLUDED.source_feedback_id,
                                    metadata_json = EXCLUDED.metadata_json,
                                    updated_at = EXCLUDED.updated_at
                                RETURNING *
                                """,
                                (
                                    str(suggestion.get("target_company") or ""),
                                    str(suggestion.get("pattern_type") or ""),
                                    str(suggestion.get("subject") or ""),
                                    str(suggestion.get("value") or ""),
                                    str(suggestion.get("confidence") or "medium"),
                                    source_feedback_id or None,
                                    json.dumps(metadata, ensure_ascii=False),
                                    now,
                                    now,
                                ),
                            )
                            applied_pattern = _fetch_one_dict_row(cursor, cursor.fetchone())
                            applied_pattern_id = int((applied_pattern or {}).get("pattern_id") or 0)

                        cursor.execute(
                            """
                            UPDATE criteria_pattern_suggestions SET
                                status = %s,
                                reviewed_by = %s,
                                review_notes = %s,
                                applied_pattern_id = %s,
                                reviewed_at = %s,
                                updated_at = %s
                            WHERE suggestion_id = %s
                            RETURNING *
                            """,
                            (
                                review_status,
                                str(reviewer or "").strip(),
                                str(notes or "").strip(),
                                applied_pattern_id or None,
                                now,
                                now,
                                normalized_suggestion_id,
                            ),
                        )
                        reviewed_suggestion = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                return {
                    "status": "applied",
                    "review_status": review_status,
                    "suggestion": reviewed_suggestion,
                    "source_feedback": source_feedback,
                    "applied_pattern": applied_pattern,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def delete_rows(
        self,
        *,
        table_name: str,
        where_sql: str,
        params: list[Any] | tuple[Any, ...] = (),
    ) -> int:
        normalized_table = _normalize_postgres_identifier(table_name)
        normalized_where_sql = _normalize_postgres_identifier(where_sql)
        if not self.should_prefer_read(normalized_table) or not normalized_where_sql:
            return 0
        self._ensure_table_write_schema(normalized_table)
        if normalized_table == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
            deleted_count = 0
            for split_table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES:
                deleted_count += self._execute_non_query(
                    f"DELETE FROM {_quote_identifier(split_table_name)} WHERE {normalized_where_sql}",
                    tuple(params),
                )
            return deleted_count
        return self._execute_non_query(
            f"DELETE FROM {_quote_identifier(normalized_table)} WHERE {normalized_where_sql}",
            tuple(params),
        )

    def update_rows(
        self,
        *,
        table_name: str,
        where_sql: str,
        params: list[Any] | tuple[Any, ...] = (),
        values: dict[str, Any] | None = None,
    ) -> int:
        normalized_table = _normalize_postgres_identifier(table_name)
        normalized_where_sql = _normalize_postgres_identifier(where_sql)
        if not self.should_prefer_read(normalized_table) or not normalized_where_sql:
            return 0
        payload = _normalize_postgres_row_payload(
            {
                str(column or "").strip(): value
                for column, value in dict(values or {}).items()
                if str(column or "").strip()
            }
        )
        if not payload:
            return 0
        self._ensure_table_write_schema(normalized_table)
        assignments = ", ".join(f"{_quote_identifier(column)} = %s" for column in payload.keys())
        return self._execute_non_query(
            f"UPDATE {_quote_identifier(normalized_table)} SET {assignments} WHERE {normalized_where_sql}",
            (*payload.values(), *tuple(params)),
        )

    def ensure_bootstrapped(self) -> None:
        # Track B B1.3: the PG schema is now created by the versioned migration runner
        # (migrations/0001_baseline.sql + schema_migrations ledger), the single source of
        # truth — NOT generated from the SQLite shadow's sqlite_master. Pre-runner databases
        # (prod `public`, local dev, already-bootstrapped test schemas) carry the baseline
        # tables but no ledger; the runner STAMPS them at the baseline rather than re-creating.
        if not self.enabled:
            return
        with self._lock:
            if self._bootstrapped:
                return
            self._apply_schema_migrations()
            self._bootstrapped = True
        self._ensure_runtime_coordination_schema()

    def _apply_schema_migrations(self) -> None:
        attempt = 0
        while True:
            connection = None
            try:
                connection = self._connect()
                apply_pending_migrations(connection, schema=self.schema)
                return
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass

    def _ensure_legacy_target_public_web_migration_table_schema(self, table_name: str) -> None:
        """Create the retired target-candidate Public Web table for explicit migration writes.

        Native PG DDL (B4.3f): the SQLite shadow that historically served as the
        DDL source for these three tables is retired. Column set/order, types
        (TEXT/BIGINT/DOUBLE PRECISION), NOT NULL flags, and the PK-only conflict
        target are drift-free against the sync-generated physical tables; the
        legacy tables stay outside migrations/0001_baseline.sql by design
        (migration-context-only surface).
        """
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self._legacy_target_public_web_migration_table_enabled(normalized_table):
            return
        ddl = _LEGACY_TARGET_PUBLIC_WEB_MIGRATION_TABLE_DDL.get(normalized_table)
        if not ddl:
            return
        self.ensure_bootstrapped()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                for statement in ddl:
                    cursor.execute(statement)

    def ensure_legacy_target_public_web_migration_write_schema(self, table_name: str) -> None:
        """Prepare one retired table before an explicit migration write's read-before-write."""

        normalized_table = _normalize_postgres_identifier(table_name)
        if not self._legacy_target_public_web_migration_table_enabled(normalized_table):
            raise RuntimeError("legacy target Public Web schema preparation requires an active migration table context")
        self._ensure_legacy_target_public_web_migration_table_schema(normalized_table)

    def _ensure_table_write_schema(self, table_name: str) -> None:
        normalized_table = str(table_name or "").strip()
        if normalized_table in _RUNTIME_COORDINATION_TABLES:
            self._ensure_runtime_coordination_schema()
            return
        self._ensure_control_plane_writer_schema()

    def _require_operation_runtime_table(self, table_name: str) -> bool:
        normalized_table = _normalize_postgres_identifier(table_name)
        if normalized_table not in _OPERATION_RUNTIME_TABLES:
            return False
        if not self.should_prefer_read(normalized_table):
            return False
        self._ensure_runtime_coordination_schema()
        return True

    def _resolve_serial_sequence(
        self,
        table_name: str,
        *,
        id_column: str = "",
        sequence_name: str = "",
    ) -> tuple[str, str]:
        configured = _SERIAL_SEQUENCE_NAMES.get(_normalize_postgres_identifier(table_name), ("", ""))
        if str(id_column or "").strip() and str(sequence_name or "").strip():
            return str(id_column or "").strip(), str(sequence_name or "").strip()
        resolved_id_column = _normalize_postgres_identifier(id_column or configured[0] or "")
        resolved_sequence_name = _normalize_postgres_identifier(sequence_name or configured[1] or "")
        return resolved_id_column, resolved_sequence_name

    def patch_serving_projection_publication_fields(
        self,
        *,
        table_name: str = "serving_projections",
        projection_id: str,
        collection_id_if_empty: str = "",
        counts_patch: dict[str, Any] | None = None,
        counts_remove: list[str] | tuple[str, ...] = (),
        readiness_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Row-lock and patch one projection while the caller owns its publication key."""

        if _normalize_postgres_identifier(table_name) != "serving_projections":
            raise ValueError("projection publication patch requires table_name=serving_projections")
        if not self.should_prefer_read("serving_projections"):
            return None
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            raise ValueError("projection_id is required")
        normalized_remove = tuple(str(key or "").strip() for key in counts_remove if str(key or "").strip())
        normalized_metadata_patch = dict(metadata_patch or {})
        reserved_metadata_keys = sorted(set(normalized_metadata_patch) & set(PROJECTION_SEARCH_INDEX_BINDING_KEYS))
        if reserved_metadata_keys:
            raise ValueError(
                "projection publication patch cannot overwrite search-index binding metadata: "
                + ", ".join(reserved_metadata_keys)
            )
        self._ensure_control_plane_writer_schema()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM serving_projections WHERE projection_id = %s FOR UPDATE",
                            (normalized_projection_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if current is None:
                            connection.rollback()
                            return None
                        counts = _json_load_dict(current.get("counts_json"))
                        for key in normalized_remove:
                            counts.pop(key, None)
                        counts.update(dict(counts_patch or {}))
                        readiness = {
                            **_json_load_dict(current.get("readiness_json")),
                            **dict(readiness_patch or {}),
                        }
                        metadata = {
                            **_json_load_dict(current.get("metadata_json")),
                            **normalized_metadata_patch,
                        }
                        collection_id = (
                            str(current.get("collection_id") or "").strip() or str(collection_id_if_empty or "").strip()
                        )
                        cursor.execute(
                            """
                            UPDATE serving_projections
                            SET collection_id = %s,
                                counts_json = %s,
                                readiness_json = %s,
                                metadata_json = %s,
                                updated_at = %s
                            WHERE projection_id = %s
                            RETURNING *
                            """,
                            (
                                collection_id,
                                _json_dump(counts),
                                _json_dump(readiness),
                                _json_dump(metadata),
                                _utc_now_sql_timestamp(),
                                normalized_projection_id,
                            ),
                        )
                        updated = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return updated
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_row(self, table_name: str, row: dict[str, Any] | None) -> None:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_mirror(normalized_table):
            return
        payload = _normalize_postgres_row_payload(dict(row or {}))
        if not payload:
            return
        primary_keys = list(_PRIMARY_KEY_COLUMNS.get(normalized_table) or [])
        if not primary_keys or any(payload.get(column) in {None, ""} for column in primary_keys):
            return
        self.ensure_bootstrapped()
        if normalized_table == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
            self._ensure_table_write_schema(normalized_table)
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    upsert_acquisition_shard_registry_rows(cursor, [payload], ensure_schema=False)
                connection.commit()
            return
        if self._legacy_target_public_web_migration_table_enabled(normalized_table):
            self._ensure_legacy_target_public_web_migration_table_schema(normalized_table)
        columns = [column for column in payload.keys() if str(column or "").strip()]
        quoted_table_name = _quote_identifier(normalized_table)
        quoted_columns = [_quote_identifier(column) for column in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [column for column in columns if column not in primary_keys]
        conflict_target = ", ".join(_quote_identifier(column) for column in primary_keys)
        company_public_web_projection_metadata = _json_load_dict(payload.get("metadata_json"))
        company_public_web_projection_upsert = bool(
            normalized_table in {"company_assets", "company_evidence"}
            and str(company_public_web_projection_metadata.get("source") or "").strip() == "company_public_web_assets"
            and str(company_public_web_projection_metadata.get("source_projection_order_key") or "").strip()
        )
        company_public_web_projection_incoming_wins = ""
        if company_public_web_projection_upsert:
            incoming_run_id = (
                "COALESCE(NULLIF(EXCLUDED.source_run_id, ''), "
                "EXCLUDED.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                if normalized_table == "company_assets"
                else "COALESCE(EXCLUDED.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
            )
            existing_run_id = (
                f"COALESCE(NULLIF({quoted_table_name}.source_run_id, ''), "
                f"{quoted_table_name}.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                if normalized_table == "company_assets"
                else (f"COALESCE({quoted_table_name}.metadata_json::jsonb ->> 'materialized_source_run_id', '')")
            )
            company_public_web_projection_incoming_wins = _company_public_web_projection_incoming_wins_sql(
                incoming_metadata_expression="EXCLUDED.metadata_json::jsonb",
                existing_metadata_expression=f"{quoted_table_name}.metadata_json::jsonb",
                incoming_run_id_expression=incoming_run_id,
                existing_run_id_expression=existing_run_id,
            )
        sql = f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
        if update_columns:
            update_assignments: list[str] = []
            for column in update_columns:
                quoted_column = _quote_identifier(column)
                if company_public_web_projection_upsert:
                    if column == "updated_at":
                        update_assignments.append(
                            f"{quoted_column} = GREATEST({quoted_table_name}.{quoted_column}, EXCLUDED.{quoted_column})"
                        )
                        continue
                    if column == "metadata_json":
                        update_assignments.append(
                            f"{quoted_column} = jsonb_set("
                            f"(CASE WHEN {company_public_web_projection_incoming_wins} "
                            f"THEN EXCLUDED.{quoted_column}::jsonb "
                            f"ELSE {quoted_table_name}.{quoted_column}::jsonb END), "
                            "'{source_run_ids}', "
                            "(SELECT COALESCE(jsonb_agg(source_run_id ORDER BY source_run_id), '[]'::jsonb) "
                            "FROM ("
                            "SELECT DISTINCT jsonb_array_elements_text("
                            f"COALESCE({quoted_table_name}.{quoted_column}::jsonb -> 'source_run_ids', "
                            "'[]'::jsonb)) AS source_run_id "
                            "UNION SELECT DISTINCT jsonb_array_elements_text("
                            f"COALESCE(EXCLUDED.{quoted_column}::jsonb -> 'source_run_ids', '[]'::jsonb)) "
                            ") company_public_web_canonical_source_runs "
                            "WHERE source_run_id <> ''), true)::text"
                        )
                    else:
                        update_assignments.append(
                            f"{quoted_column} = CASE WHEN {company_public_web_projection_incoming_wins} "
                            f"THEN EXCLUDED.{quoted_column} ELSE {quoted_table_name}.{quoted_column} END"
                        )
                    continue
                if (
                    normalized_table == "job_result_lifecycle"
                    and column in _JOB_RESULT_LIFECYCLE_DELTA_MONOTONIC_INT_FIELDS
                ):
                    update_assignments.append(
                        (
                            f"{quoted_column} = CASE "
                            "WHEN EXCLUDED.delta_profile_progress_applicable = 0 "
                            f"THEN EXCLUDED.{quoted_column} "
                            f"ELSE GREATEST({quoted_table_name}.{quoted_column}, EXCLUDED.{quoted_column}) "
                            "END"
                        )
                    )
                    continue
                if (
                    normalized_table == "job_result_lifecycle"
                    and column in _JOB_RESULT_LIFECYCLE_STAGE1_MONOTONIC_INT_FIELDS
                ):
                    update_assignments.append(
                        f"{quoted_column} = GREATEST({quoted_table_name}.{quoted_column}, EXCLUDED.{quoted_column})"
                    )
                    continue
                if normalized_table == "job_result_lifecycle" and column == "metadata_json":
                    update_assignments.append(
                        (
                            f"{quoted_column} = CASE "
                            f"WHEN COALESCE(({quoted_table_name}.metadata_json::jsonb ->> 'delta_profile_denominator_promoted')::boolean, false) IS TRUE "
                            f"THEN ({quoted_table_name}.metadata_json::jsonb || EXCLUDED.metadata_json::jsonb || "
                            "jsonb_build_object('delta_profile_denominator_promoted', true))::text "
                            f"ELSE EXCLUDED.{quoted_column} "
                            "END"
                        )
                    )
                    continue
                if normalized_table == "serving_projections" and column == "metadata_json":
                    update_assignments.append(
                        f"{quoted_column} = "
                        + _projection_metadata_merge_sql(
                            quoted_table_name=quoted_table_name,
                            quoted_column=quoted_column,
                            preserve_keys=(
                                PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY,
                                PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY,
                                PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,
                            ),
                        )
                    )
                    continue
                if normalized_table == "job_result_lifecycle" and column in {
                    "phase",
                    "state",
                    "served_snapshot_id",
                    "serving_projection_id",
                    "serving_projection_phase",
                    "background_snapshot_materialization_status",
                }:
                    update_assignments.append(
                        (
                            f"{quoted_column} = CASE "
                            "WHEN "
                            f"{quoted_table_name}.serving_projection_phase IN "
                            "('current_snapshot_row_shell_overlay', 'current_snapshot_serving', 'current_serving') "
                            "AND EXCLUDED.serving_projection_phase IN "
                            "('partial_delta_overlay', 'partial_delta_board_visible_overlay', 'partial_current_snapshot_overlay') "
                            f"AND {quoted_table_name}.current_snapshot_id <> '' "
                            f"AND EXCLUDED.current_snapshot_id = {quoted_table_name}.current_snapshot_id "
                            f"THEN {quoted_table_name}.{quoted_column} "
                            f"ELSE EXCLUDED.{quoted_column} "
                            "END"
                        )
                    )
                    continue
                if normalized_table == "job_result_lifecycle" and column in {
                    "served_candidate_count",
                    "expected_candidate_count",
                }:
                    update_assignments.append(
                        (
                            f"{quoted_column} = CASE "
                            "WHEN "
                            f"{quoted_table_name}.serving_projection_phase IN "
                            "('current_snapshot_row_shell_overlay', 'current_snapshot_serving', 'current_serving') "
                            "AND EXCLUDED.serving_projection_phase IN "
                            "('partial_delta_overlay', 'partial_delta_board_visible_overlay', 'partial_current_snapshot_overlay') "
                            f"AND {quoted_table_name}.current_snapshot_id <> '' "
                            f"AND EXCLUDED.current_snapshot_id = {quoted_table_name}.current_snapshot_id "
                            f"THEN GREATEST({quoted_table_name}.{quoted_column}, EXCLUDED.{quoted_column}) "
                            f"ELSE EXCLUDED.{quoted_column} "
                            "END"
                        )
                    )
                    continue
                update_assignments.append(f"{quoted_column} = EXCLUDED.{quoted_column}")
            sql += f" ON CONFLICT ({conflict_target}) DO UPDATE SET " + ", ".join(update_assignments)
        else:
            sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"
        self._execute_non_query(sql, tuple(_normalize_postgres_payload(payload.get(column)) for column in columns))

    def bulk_upsert_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        *,
        transaction_lock_key: str = "",
    ) -> int:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_mirror(normalized_table):
            return 0
        payload_rows = [
            _normalize_postgres_row_payload(dict(row or {}))
            for row in list(rows or [])
            if isinstance(row, dict) and dict(row)
        ]
        if not payload_rows:
            return 0
        if normalized_table == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
            self.ensure_bootstrapped()
            self._ensure_table_write_schema(normalized_table)
            with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                with connection.cursor() as cursor:
                    summary = upsert_acquisition_shard_registry_rows(cursor, payload_rows, ensure_schema=False)
                connection.commit()
            return int(summary.get("row_count") or 0)
        plan = self._bulk_upsert_plan(normalized_table, payload_rows)
        if plan is None:
            return 0

        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_table)
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        affected = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name=normalized_table,
                            payload_rows=payload_rows,
                            plan=plan,
                        )
                    connection.commit()
                return affected
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_row_and_upsert_rows(
        self,
        *,
        table_name: str,
        row: dict[str, Any],
        upsert_table_name: str,
        upsert_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        update_projection_index_input_revision: bool = False,
        transaction_lock_key: str = "",
    ) -> dict[str, int] | None:
        """Atomically publish a parent row and merge child rows under one lock."""

        normalized_parent_table = _normalize_postgres_identifier(table_name)
        normalized_child_table = _normalize_postgres_identifier(upsert_table_name)
        if not self.should_prefer_read(normalized_parent_table) or not self.should_prefer_read(normalized_child_table):
            return None
        parent_rows = self._normalize_bulk_upsert_rows([row])
        if len(parent_rows) != 1:
            raise ValueError("upsert_row_and_upsert_rows requires one non-empty row")
        if update_projection_index_input_revision and normalized_parent_table == "serving_projections":
            parent_rows[0] = _invalidate_projection_search_index_products(
                parent_rows[0],
                build_status="stale",
            )
        child_rows = self._normalize_bulk_upsert_rows(upsert_rows)
        parent_plan = self._bulk_upsert_plan(
            normalized_parent_table,
            parent_rows,
            require_primary_key_values=True,
        )
        child_plan = self._bulk_upsert_plan(
            normalized_child_table,
            child_rows,
            require_primary_key_values=True,
        )

        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_parent_table)
        if normalized_child_table != normalized_parent_table:
            self._ensure_table_write_schema(normalized_child_table)
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        parent_count = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name=normalized_parent_table,
                            payload_rows=parent_rows,
                            plan=parent_plan,
                            preserve_projection_index_input_revision=not bool(update_projection_index_input_revision),
                        )
                        child_count = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name=normalized_child_table,
                            payload_rows=child_rows,
                            plan=child_plan,
                        )
                    connection.commit()
                return {
                    "upserted_count": parent_count,
                    "child_upserted_count": child_count,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def replace_rows(
        self,
        *,
        table_name: str,
        where_sql: str,
        params: list[Any] | tuple[Any, ...] = (),
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        transaction_lock_key: str = "",
    ) -> int:
        """Replace one logical row scope in a single transaction.

        All replacement rows are validated before the delete. Every bulk chunk uses
        the same connection and transaction, so a later insert failure rolls back
        both earlier chunks and the destructive delete.
        """

        normalized_table = _normalize_postgres_identifier(table_name)
        normalized_where_sql = _normalize_postgres_identifier(where_sql)
        if not self.should_prefer_read(normalized_table) or not normalized_where_sql:
            return 0
        payload_rows = self._normalize_bulk_upsert_rows(rows)
        plan = self._bulk_upsert_plan(
            normalized_table,
            payload_rows,
            require_primary_key_values=True,
        )
        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_table)
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        affected = self._replace_rows_with_cursor(
                            cursor,
                            table_name=normalized_table,
                            where_sql=normalized_where_sql,
                            params=params,
                            payload_rows=payload_rows,
                            plan=plan,
                        )
                    connection.commit()
                return affected
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def write_serving_projection_members_with_input_revision(
        self,
        *,
        table_name: str,
        projection_id: str,
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        replace_members: bool,
        input_revision: str,
        transaction_lock_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_table = _normalize_postgres_identifier(table_name)
        normalized_projection_id = str(projection_id or "").strip()
        normalized_input_revision = str(input_revision or "").strip()
        if normalized_table != "serving_projection_members":
            raise ValueError(
                "write_serving_projection_members_with_input_revision requires table_name=serving_projection_members"
            )
        if not normalized_projection_id or not normalized_input_revision:
            return None
        if not self.should_prefer_read(normalized_table) or not self.should_prefer_read("serving_projections"):
            return None
        payload_rows = self._normalize_bulk_upsert_rows(rows)
        plan = self._bulk_upsert_plan(
            normalized_table,
            payload_rows,
            require_primary_key_values=True,
        )
        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_table)
        self._ensure_table_write_schema("serving_projections")
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM serving_projections WHERE projection_id = %s FOR UPDATE",
                            (normalized_projection_id,),
                        )
                        projection_row = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if projection_row is None:
                            result = {
                                "status": "projection_missing",
                                "applied": False,
                                "projection_id": normalized_projection_id,
                                "input_revision": normalized_input_revision,
                                "member_count": 0,
                            }
                        else:
                            metadata = _json_load_dict(projection_row.get("metadata_json"))
                            next_members_by_key = {
                                str(row.get("candidate_identity_key") or "").strip(): row
                                for row in payload_rows
                                if str(row.get("candidate_identity_key") or "").strip()
                            }
                            existing_members_by_key: dict[str, dict[str, Any]] = {}
                            if replace_members:
                                cursor.execute(
                                    'SELECT * FROM "serving_projection_members" WHERE projection_id = %s',
                                    (normalized_projection_id,),
                                )
                                existing_rows = _fetch_all_dict_rows(cursor)
                            elif next_members_by_key:
                                candidate_keys = sorted(next_members_by_key)
                                existing_rows = []
                                for offset in range(0, len(candidate_keys), 500):
                                    chunk = candidate_keys[offset : offset + 500]
                                    placeholders = ", ".join("%s" for _ in chunk)
                                    cursor.execute(
                                        'SELECT * FROM "serving_projection_members" '
                                        f"WHERE projection_id = %s AND candidate_identity_key IN ({placeholders})",
                                        (normalized_projection_id, *chunk),
                                    )
                                    existing_rows.extend(_fetch_all_dict_rows(cursor))
                            else:
                                existing_rows = []
                            for existing_row in existing_rows:
                                candidate_key = str(existing_row.get("candidate_identity_key") or "").strip()
                                if candidate_key:
                                    existing_members_by_key[candidate_key] = existing_row
                            semantic_changed = projection_search_index_members_changed(
                                existing_rows=existing_members_by_key,
                                next_rows=next_members_by_key,
                                replace_members=replace_members,
                            )
                            current_input_revision = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
                            ).strip()
                            effective_input_revision = (
                                normalized_input_revision
                                if semantic_changed or not current_input_revision
                                else current_input_revision
                            )
                            metadata[PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY] = effective_input_revision
                            projection_update = {
                                **projection_row,
                                "metadata_json": _json_dump(metadata),
                            }
                            if semantic_changed:
                                projection_update = _invalidate_projection_search_index_products(
                                    projection_update,
                                    build_status="stale",
                                )
                            cursor.execute(
                                """
                                UPDATE serving_projections
                                SET metadata_json = %s,
                                    counts_json = %s,
                                    readiness_json = %s,
                                    raw_profile_index_watermark = %s,
                                    evidence_index_watermark = %s,
                                    updated_at = %s
                                WHERE projection_id = %s
                                """,
                                (
                                    projection_update.get("metadata_json") or _json_dump(metadata),
                                    projection_update.get("counts_json")
                                    or projection_row.get("counts_json")
                                    or _json_dump({}),
                                    projection_update.get("readiness_json")
                                    or projection_row.get("readiness_json")
                                    or _json_dump({}),
                                    str(projection_update.get("raw_profile_index_watermark") or "").strip(),
                                    str(projection_update.get("evidence_index_watermark") or "").strip(),
                                    _utc_now_sql_timestamp(),
                                    normalized_projection_id,
                                ),
                            )
                            if replace_members:
                                member_count = self._replace_rows_with_cursor(
                                    cursor,
                                    table_name=normalized_table,
                                    where_sql="projection_id = %s",
                                    params=[normalized_projection_id],
                                    payload_rows=payload_rows,
                                    plan=plan,
                                )
                            else:
                                member_count = self._bulk_upsert_rows_with_cursor(
                                    cursor,
                                    table_name=normalized_table,
                                    payload_rows=payload_rows,
                                    plan=plan,
                                )
                            result = {
                                "status": "applied",
                                "applied": True,
                                "projection_id": normalized_projection_id,
                                "input_revision": effective_input_revision,
                                "semantic_changed": semantic_changed,
                                "member_count": int(member_count or 0),
                            }
                    connection.commit()
                return result
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def write_projection_person_search_index_generation(
        self,
        *,
        table_name: str,
        projection_id: str,
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        build_generation: str,
        reset_index: bool,
        expected_build_generation: str | None = None,
        expected_input_revision: str | None = None,
        transaction_lock_key: str = "",
    ) -> dict[str, Any] | None:
        """Compare and write one search-index build generation atomically.

        The generation marker lives on the projection rather than an index row,
        so an empty reset still fences delayed continuation pages.  The marker
        comparison and index mutation share the same transaction and advisory
        lock; serializing writes alone is insufficient because an old page can
        otherwise run after a newer reset.
        """

        normalized_table = _normalize_postgres_identifier(table_name)
        normalized_projection_id = str(projection_id or "").strip()
        normalized_generation = str(build_generation or "").strip()
        normalized_expected_generation = (
            None if expected_build_generation is None else str(expected_build_generation or "").strip()
        )
        normalized_expected_input_revision = (
            None if expected_input_revision is None else str(expected_input_revision or "").strip()
        )
        if normalized_table != "projection_person_search_index":
            raise ValueError("generation-fenced writer only supports projection_person_search_index")
        if not normalized_projection_id:
            raise ValueError("projection_id is required for generation-fenced index writes")
        if not normalized_generation:
            raise ValueError("build_generation is required for generation-fenced index writes")
        if reset_index and normalized_expected_generation is None:
            raise ValueError("expected_build_generation is required for generation-fenced index resets")
        if reset_index and normalized_expected_input_revision is None:
            raise ValueError("expected_input_revision is required for generation-fenced index resets")
        if not self.should_prefer_read(normalized_table) or not self.should_prefer_read("serving_projections"):
            return None

        payload_rows = self._normalize_bulk_upsert_rows(rows)
        plan = self._bulk_upsert_plan(
            normalized_table,
            payload_rows,
            require_primary_key_values=True,
        )
        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_table)
        self._ensure_table_write_schema("serving_projections")
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM serving_projections WHERE projection_id = %s FOR UPDATE",
                            (normalized_projection_id,),
                        )
                        projection_row = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if projection_row is None:
                            result = {
                                "status": "projection_missing",
                                "applied": False,
                                "projection_id": normalized_projection_id,
                                "build_generation": normalized_generation,
                                "indexed_count": 0,
                            }
                        else:
                            raw_metadata = projection_row.get("metadata_json")
                            if isinstance(raw_metadata, dict):
                                metadata = dict(raw_metadata)
                            else:
                                try:
                                    parsed_metadata = json.loads(str(raw_metadata or "{}"))
                                except (TypeError, ValueError, json.JSONDecodeError):
                                    parsed_metadata = {}
                                metadata = dict(parsed_metadata) if isinstance(parsed_metadata, dict) else {}
                            current_generation = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY) or ""
                            ).strip()
                            current_input_revision = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
                            ).strip()
                            build_input_revision = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY) or ""
                            ).strip()
                            current_build_status = str(metadata.get("search_index_build_status") or "").strip()
                            reset_already_applied = (
                                bool(reset_index)
                                and current_generation == normalized_generation
                                and build_input_revision == current_input_revision
                                and current_input_revision == normalized_expected_input_revision
                            )
                            stale_reset = bool(reset_index) and (
                                current_generation != normalized_expected_generation
                                or current_input_revision != normalized_expected_input_revision
                            )
                            stale_continuation = not reset_index and (
                                current_generation != normalized_generation
                                or build_input_revision != current_input_revision
                                or current_build_status == "completed"
                            )
                            if reset_already_applied:
                                result = {
                                    "status": "already_applied",
                                    "applied": True,
                                    "projection_id": normalized_projection_id,
                                    "build_generation": normalized_generation,
                                    "current_build_generation": current_generation,
                                    "current_input_revision": current_input_revision,
                                    "indexed_count": 0,
                                }
                            elif stale_reset or stale_continuation:
                                result = {
                                    "status": "stale_generation",
                                    "applied": False,
                                    "projection_id": normalized_projection_id,
                                    "build_generation": normalized_generation,
                                    "current_build_generation": current_generation,
                                    "current_input_revision": current_input_revision,
                                    "build_input_revision": build_input_revision,
                                    "indexed_count": 0,
                                }
                            else:
                                if reset_index:
                                    metadata[PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY] = normalized_generation
                                    metadata[PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY] = current_input_revision
                                    projection_update = _invalidate_projection_search_index_products(
                                        {
                                            **projection_row,
                                            "metadata_json": _json_dump(metadata),
                                        },
                                        build_status="building",
                                    )
                                    cursor.execute(
                                        """
                                        UPDATE serving_projections
                                        SET metadata_json = %s,
                                            counts_json = %s,
                                            readiness_json = %s,
                                            raw_profile_index_watermark = %s,
                                            evidence_index_watermark = %s,
                                            updated_at = %s
                                        WHERE projection_id = %s
                                        """,
                                        (
                                            projection_update["metadata_json"],
                                            projection_update["counts_json"],
                                            projection_update["readiness_json"],
                                            projection_update["raw_profile_index_watermark"],
                                            projection_update["evidence_index_watermark"],
                                            _utc_now_sql_timestamp(),
                                            normalized_projection_id,
                                        ),
                                    )
                                    affected = self._replace_rows_with_cursor(
                                        cursor,
                                        table_name=normalized_table,
                                        where_sql="projection_id = %s",
                                        params=[normalized_projection_id],
                                        payload_rows=payload_rows,
                                        plan=plan,
                                    )
                                else:
                                    affected = self._bulk_upsert_rows_with_cursor(
                                        cursor,
                                        table_name=normalized_table,
                                        payload_rows=payload_rows,
                                        plan=plan,
                                    )
                                result = {
                                    "status": "applied",
                                    "applied": True,
                                    "projection_id": normalized_projection_id,
                                    "build_generation": normalized_generation,
                                    "current_build_generation": normalized_generation,
                                    "current_input_revision": current_input_revision,
                                    "build_input_revision": current_input_revision,
                                    "indexed_count": int(affected or 0),
                                }
                    connection.commit()
                return result
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def update_projection_person_search_index_generation_state(
        self,
        *,
        table_name: str = "serving_projections",
        projection_id: str,
        build_generation: str,
        index_values: dict[str, Any] | None = None,
        counts_patch: dict[str, Any] | None = None,
        readiness_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        raw_profile_index_watermark: str = "",
        evidence_index_watermark: str = "",
        transaction_lock_key: str = "",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "serving_projections":
            raise ValueError(
                "update_projection_person_search_index_generation_state requires table_name=serving_projections"
            )
        normalized_projection_id = str(projection_id or "").strip()
        normalized_generation = str(build_generation or "").strip()
        if not normalized_projection_id or not normalized_generation:
            return None
        if not self.should_prefer_read("serving_projections") or not self.should_prefer_read(
            "projection_person_search_index"
        ):
            return None
        requested_index_values = dict(index_values or {})
        normalized_index_values = {
            _normalize_postgres_identifier(key): value
            for key, value in requested_index_values.items()
            if _normalize_postgres_identifier(key)
            in {
                "count_scope",
                "raw_profile_index_watermark",
                "evidence_index_watermark",
            }
        }
        unsupported_index_values = {
            str(key or "").strip()
            for key in requested_index_values
            if _normalize_postgres_identifier(key) not in normalized_index_values
        }
        if unsupported_index_values:
            raise ValueError(
                "unsupported projection search-index state columns: " + ", ".join(sorted(unsupported_index_values))
            )
        self.ensure_bootstrapped()
        self._ensure_table_write_schema("serving_projections")
        self._ensure_table_write_schema("projection_person_search_index")
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM serving_projections WHERE projection_id = %s FOR UPDATE",
                            (normalized_projection_id,),
                        )
                        projection_row = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if projection_row is None:
                            result = {
                                "status": "projection_missing",
                                "applied": False,
                                "projection_id": normalized_projection_id,
                                "build_generation": normalized_generation,
                                "updated_count": 0,
                            }
                        else:
                            metadata = _json_load_dict(projection_row.get("metadata_json"))
                            current_generation = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY) or ""
                            ).strip()
                            current_input_revision = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
                            ).strip()
                            build_input_revision = str(
                                metadata.get(PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY) or ""
                            ).strip()
                            current_build_status = str(metadata.get("search_index_build_status") or "").strip()
                            requested_build_status = str(
                                dict(metadata_patch or {}).get("search_index_build_status") or ""
                            ).strip()
                            if (
                                current_generation != normalized_generation
                                or build_input_revision != current_input_revision
                                or (
                                    current_build_status == "completed"
                                    and requested_build_status in {"building", "partial"}
                                )
                            ):
                                result = {
                                    "status": "stale_generation",
                                    "applied": False,
                                    "projection_id": normalized_projection_id,
                                    "build_generation": normalized_generation,
                                    "current_build_generation": current_generation,
                                    "current_input_revision": current_input_revision,
                                    "build_input_revision": build_input_revision,
                                    "updated_count": 0,
                                }
                            else:
                                now = _utc_now_sql_timestamp()
                                updated_count = 0
                                if normalized_index_values:
                                    assignments = ", ".join(
                                        f"{_quote_identifier(key)} = %s" for key in normalized_index_values
                                    )
                                    cursor.execute(
                                        f"UPDATE projection_person_search_index SET {assignments} "
                                        "WHERE projection_id = %s",
                                        (
                                            *(
                                                _normalize_postgres_payload(value)
                                                for value in normalized_index_values.values()
                                            ),
                                            normalized_projection_id,
                                        ),
                                    )
                                    updated_count = int(cursor.rowcount or 0)
                                counts = {
                                    **_json_load_dict(projection_row.get("counts_json")),
                                    **dict(counts_patch or {}),
                                }
                                readiness = {
                                    **_json_load_dict(projection_row.get("readiness_json")),
                                    **dict(readiness_patch or {}),
                                }
                                safe_metadata_patch = dict(metadata_patch or {})
                                for reserved_key in (
                                    PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY,
                                    PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY,
                                    PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,
                                ):
                                    safe_metadata_patch.pop(reserved_key, None)
                                metadata.update(safe_metadata_patch)
                                cursor.execute(
                                    """
                                    UPDATE serving_projections
                                    SET counts_json = %s,
                                        readiness_json = %s,
                                        metadata_json = %s,
                                        raw_profile_index_watermark = %s,
                                        evidence_index_watermark = %s,
                                        updated_at = %s
                                    WHERE projection_id = %s
                                    RETURNING *
                                    """,
                                    (
                                        _json_dump(counts),
                                        _json_dump(readiness),
                                        _json_dump(metadata),
                                        str(raw_profile_index_watermark or "").strip()
                                        or str(projection_row.get("raw_profile_index_watermark") or "").strip(),
                                        str(evidence_index_watermark or "").strip()
                                        or str(projection_row.get("evidence_index_watermark") or "").strip(),
                                        now,
                                        normalized_projection_id,
                                    ),
                                )
                                updated_projection = _fetch_one_dict_row(cursor, cursor.fetchone())
                                result = {
                                    "status": "applied",
                                    "applied": True,
                                    "projection_id": normalized_projection_id,
                                    "build_generation": normalized_generation,
                                    "current_build_generation": current_generation,
                                    "current_input_revision": current_input_revision,
                                    "build_input_revision": build_input_revision,
                                    "updated_count": updated_count,
                                    "projection_row": updated_projection,
                                }
                    connection.commit()
                return result
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_row_and_replace_rows(
        self,
        *,
        table_name: str,
        row: dict[str, Any],
        replace_table_name: str,
        replace_where_sql: str,
        replace_params: list[Any] | tuple[Any, ...] = (),
        replace_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        update_projection_index_input_revision: bool = False,
        transaction_lock_key: str = "",
    ) -> dict[str, int] | None:
        """Atomically publish a parent row and replace its child row scope."""

        normalized_upsert_table = _normalize_postgres_identifier(table_name)
        normalized_replace_table = _normalize_postgres_identifier(replace_table_name)
        normalized_replace_where_sql = _normalize_postgres_identifier(replace_where_sql)
        if (
            not self.should_prefer_read(normalized_upsert_table)
            or not self.should_prefer_read(normalized_replace_table)
            or not normalized_replace_where_sql
        ):
            return None
        upsert_payload_rows = self._normalize_bulk_upsert_rows([row])
        if len(upsert_payload_rows) != 1:
            raise ValueError("upsert_row_and_replace_rows requires one non-empty row")
        if update_projection_index_input_revision and normalized_upsert_table == "serving_projections":
            upsert_payload_rows[0] = _invalidate_projection_search_index_products(
                upsert_payload_rows[0],
                build_status="stale",
            )
        replacement_payload_rows = self._normalize_bulk_upsert_rows(replace_rows)
        upsert_plan = self._bulk_upsert_plan(
            normalized_upsert_table,
            upsert_payload_rows,
            require_primary_key_values=True,
        )
        replacement_plan = self._bulk_upsert_plan(
            normalized_replace_table,
            replacement_payload_rows,
            require_primary_key_values=True,
        )

        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_upsert_table)
        if normalized_replace_table != normalized_upsert_table:
            self._ensure_table_write_schema(normalized_replace_table)
        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(transaction_lock_key) as connection:
                    with connection.cursor() as cursor:
                        upserted_count = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name=normalized_upsert_table,
                            payload_rows=upsert_payload_rows,
                            plan=upsert_plan,
                            preserve_projection_index_input_revision=not bool(update_projection_index_input_revision),
                        )
                        replaced_count = self._replace_rows_with_cursor(
                            cursor,
                            table_name=normalized_replace_table,
                            where_sql=normalized_replace_where_sql,
                            params=replace_params,
                            payload_rows=replacement_payload_rows,
                            plan=replacement_plan,
                        )
                    connection.commit()
                return {
                    "upserted_count": upserted_count,
                    "replaced_count": replaced_count,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def publish_serving_projection(
        self,
        *,
        table_name: str = "serving_projections",
        scope_kind: str,
        scope_key: str,
        explicit_projection_id: str = "",
        active_collection_version: str = "",
        replace_members: bool,
        member_identity_keys: list[str] | tuple[str, ...] = (),
        projection_id_factory: Any,
        payload_builder: Any,
    ) -> dict[str, Any] | None:
        """Publish projection data and routing metadata in one locked transaction."""

        normalized_table = _normalize_postgres_identifier(table_name)
        if normalized_table != "serving_projections":
            raise ValueError("publish_serving_projection table_name must be serving_projections")
        normalized_scope_kind = str(scope_kind or "").strip().lower()
        normalized_scope_key = str(scope_key or "").strip()
        normalized_explicit_id = str(explicit_projection_id or "").strip()
        normalized_collection_version = str(active_collection_version or "").strip()
        route_specs = {
            "run_scope": {
                "table": "run_projection_links",
                "where": "run_id = %s AND link_type = %s",
                "params": (normalized_scope_key, "result"),
                "projection_field": "projection_id",
            },
            "collection_authoritative": {
                "table": "collection_authoritative_pointers",
                "where": "collection_id = %s",
                "params": (normalized_scope_key,),
                "projection_field": "active_projection_id",
            },
        }
        route_spec = route_specs.get(normalized_scope_kind)
        if route_spec is None:
            raise ValueError(f"unsupported serving projection publication scope: {normalized_scope_kind!r}")
        if not normalized_scope_key:
            raise ValueError("serving projection publication scope_key is required")
        if not callable(projection_id_factory):
            raise TypeError("projection_id_factory must be callable")
        if not callable(payload_builder):
            raise TypeError("payload_builder must be callable")
        route_table = str(route_spec["table"])
        required_tables = ("serving_projections", "serving_projection_members", route_table)
        if any(not self.should_prefer_read(table_name) for table_name in required_tables):
            return None

        normalized_member_keys = [
            str(item or "").strip() for item in dict.fromkeys(member_identity_keys or ()) if str(item or "").strip()
        ]
        self.ensure_bootstrapped()
        for table_name in required_tables:
            self._ensure_table_write_schema(table_name)

        random_projection_id = ""
        attempt = 0
        lock_contention_attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(
                    f"serving_projection_scope:{normalized_scope_kind}:{normalized_scope_key}"
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"SELECT * FROM {_quote_identifier(route_table)} WHERE {route_spec['where']}",
                            tuple(route_spec["params"]),
                        )
                        existing_route = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}

                        reusable_projection_id = str(
                            existing_route.get(str(route_spec["projection_field"])) or ""
                        ).strip()
                        if (
                            normalized_scope_kind == "collection_authoritative"
                            and str(existing_route.get("active_collection_version") or "").strip()
                            != normalized_collection_version
                        ):
                            reusable_projection_id = ""
                        identity_source = "explicit"
                        selected_projection_id = normalized_explicit_id
                        if not selected_projection_id and reusable_projection_id:
                            selected_projection_id = reusable_projection_id
                            identity_source = "existing_route"
                        if not selected_projection_id:
                            if not random_projection_id:
                                random_projection_id = str(projection_id_factory() or "").strip()
                            selected_projection_id = random_projection_id
                            identity_source = "random"
                        if not selected_projection_id:
                            raise RuntimeError("serving projection identity factory returned an empty id")

                        if not self._try_acquire_transaction_lock(
                            cursor,
                            f"serving_projection_publication:{selected_projection_id}",
                        ):
                            raise _TransactionAdvisoryLockBusy(
                                f"serving projection publication lock is busy: {selected_projection_id}"
                            )
                        publication_now = _utc_now_sql_timestamp()
                        cursor.execute(
                            'SELECT * FROM "serving_projections" WHERE projection_id = %s',
                            (selected_projection_id,),
                        )
                        existing_projection = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        existing_members_by_key: dict[str, dict[str, Any]] = {}
                        member_key_chunks = (
                            [None]
                            if replace_members
                            else [
                                normalized_member_keys[offset : offset + 500]
                                for offset in range(0, len(normalized_member_keys), 500)
                            ]
                        )
                        for chunk in member_key_chunks:
                            if chunk is None:
                                cursor.execute(
                                    'SELECT * FROM "serving_projection_members" WHERE projection_id = %s',
                                    (selected_projection_id,),
                                )
                            elif chunk:
                                placeholders = ", ".join("%s" for _ in chunk)
                                cursor.execute(
                                    'SELECT * FROM "serving_projection_members" '
                                    f"WHERE projection_id = %s AND candidate_identity_key IN ({placeholders})",
                                    (selected_projection_id, *chunk),
                                )
                            else:
                                continue
                            for row in _fetch_all_dict_rows(cursor):
                                candidate_key = str(row.get("candidate_identity_key") or "").strip()
                                if candidate_key:
                                    existing_members_by_key[candidate_key] = row

                        built_payload = payload_builder(
                            selected_projection_id=selected_projection_id,
                            existing_projection=existing_projection,
                            existing_members_by_key=existing_members_by_key,
                            existing_route=existing_route,
                            publication_now=publication_now,
                        )
                        if not isinstance(built_payload, dict):
                            raise TypeError("serving projection payload_builder must return a dict")
                        projection_rows = self._normalize_bulk_upsert_rows(
                            [dict(built_payload.get("projection_row") or {})]
                        )
                        member_rows = self._normalize_bulk_upsert_rows(list(built_payload.get("member_rows") or []))
                        routing_rows = self._normalize_bulk_upsert_rows([dict(built_payload.get("routing_row") or {})])
                        if len(projection_rows) != 1 or len(routing_rows) != 1:
                            raise ValueError("serving projection publication requires one parent and one routing row")
                        if str(projection_rows[0].get("projection_id") or "") != selected_projection_id:
                            raise ValueError("projection payload does not use the selected projection_id")
                        if any(str(row.get("projection_id") or "") != selected_projection_id for row in member_rows):
                            raise ValueError("member payload does not use the selected projection_id")
                        route_projection_id = str(routing_rows[0].get(str(route_spec["projection_field"])) or "")
                        if route_projection_id != selected_projection_id:
                            raise ValueError("routing payload does not use the selected projection_id")
                        next_members_by_key = {
                            str(row.get("candidate_identity_key") or "").strip(): row
                            for row in member_rows
                            if str(row.get("candidate_identity_key") or "").strip()
                        }
                        semantic_changed = projection_search_index_members_changed(
                            existing_rows=existing_members_by_key,
                            next_rows=next_members_by_key,
                            replace_members=replace_members,
                        )
                        if semantic_changed:
                            projection_rows[0] = _invalidate_projection_search_index_products(
                                projection_rows[0],
                                build_status="stale",
                            )

                        projection_plan = self._bulk_upsert_plan(
                            "serving_projections",
                            projection_rows,
                            require_primary_key_values=True,
                        )
                        member_plan = self._bulk_upsert_plan(
                            "serving_projection_members",
                            member_rows,
                            require_primary_key_values=True,
                        )
                        routing_plan = self._bulk_upsert_plan(
                            route_table,
                            routing_rows,
                            require_primary_key_values=True,
                        )
                        projection_count = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name="serving_projections",
                            payload_rows=projection_rows,
                            plan=projection_plan,
                            preserve_projection_index_input_revision=False,
                        )
                        if projection_count <= 0:
                            raise RuntimeError("serving projection parent upsert returned no confirmation")
                        if replace_members:
                            member_count = self._replace_rows_with_cursor(
                                cursor,
                                table_name="serving_projection_members",
                                where_sql="projection_id = %s",
                                params=(selected_projection_id,),
                                payload_rows=member_rows,
                                plan=member_plan,
                            )
                        else:
                            member_count = self._bulk_upsert_rows_with_cursor(
                                cursor,
                                table_name="serving_projection_members",
                                payload_rows=member_rows,
                                plan=member_plan,
                            )
                        routing_count = self._bulk_upsert_rows_with_cursor(
                            cursor,
                            table_name=route_table,
                            payload_rows=routing_rows,
                            plan=routing_plan,
                        )
                        if routing_count <= 0:
                            raise RuntimeError("serving projection routing upsert returned no confirmation")
                    connection.commit()
                return {
                    "projection_id": selected_projection_id,
                    "identity_source": identity_source,
                    "projection_row": projection_rows[0],
                    "routing_row": routing_rows[0],
                    "member_count": member_count,
                }
            except _TransactionAdvisoryLockBusy:
                lock_contention_attempt += 1
                time.sleep(_control_plane_postgres_retry_delay_seconds(min(lock_contention_attempt, 5)))
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def apply_projection_crm_selection(
        self,
        *,
        table_name: str = "crm_records",
        projection_id: str,
        expected_membership_revision: str,
        expected_source_candidate_count: int,
        candidate_identity_keys: list[str] | tuple[str, ...],
        workspace_id: str,
        selection_idempotency_keys: dict[str, str],
        payload_builder: Any,
    ) -> dict[str, Any] | None:
        """Validate projection membership and commit one CRM selection atomically."""

        if _normalize_postgres_identifier(table_name) != "crm_records":
            raise ValueError("apply_projection_crm_selection table_name must be crm_records")
        normalized_projection_id = str(projection_id or "").strip()
        normalized_revision = str(expected_membership_revision or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_candidate_keys = [
            str(item or "").strip() for item in dict.fromkeys(candidate_identity_keys or ()) if str(item or "").strip()
        ]
        normalized_idempotency_keys = {
            str(candidate_key or "").strip(): str(idempotency_key or "").strip()
            for candidate_key, idempotency_key in dict(selection_idempotency_keys or {}).items()
            if str(candidate_key or "").strip() and str(idempotency_key or "").strip()
        }
        if not normalized_projection_id or not normalized_revision or not normalized_candidate_keys:
            raise ValueError("projection_id, expected_membership_revision, and candidate_identity_keys are required")
        if set(normalized_idempotency_keys) != set(normalized_candidate_keys):
            raise ValueError("selection idempotency keys must cover every candidate identity key")
        if not callable(payload_builder):
            raise TypeError("payload_builder must be callable")
        required_tables = (
            "serving_projections",
            "serving_projection_members",
            "crm_records",
            "crm_engagements",
            "crm_events",
        )
        if any(not self.should_prefer_read(required_table) for required_table in required_tables):
            return None
        self.ensure_bootstrapped()
        for required_table in required_tables:
            self._ensure_table_write_schema(required_table)

        attempt = 0
        while True:
            try:
                with self._connect_with_transaction_lock(
                    f"serving_projection_publication:{normalized_projection_id}"
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            'SELECT * FROM "serving_projections" WHERE projection_id = %s',
                            (normalized_projection_id,),
                        )
                        projection_row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        if not projection_row:
                            connection.commit()
                            return {
                                "status": "not_ready",
                                "reason": "projection_not_found",
                                "projection_id": normalized_projection_id,
                            }
                        current_revision = str(
                            _json_load_dict(projection_row.get("metadata_json")).get(
                                PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY
                            )
                            or ""
                        ).strip()
                        cursor.execute(
                            'SELECT COUNT(*) AS visible_count FROM "serving_projection_members" '
                            "WHERE projection_id = %s "
                            "AND COALESCE(NULLIF(visibility_state, ''), 'visible') = 'visible'",
                            (normalized_projection_id,),
                        )
                        count_row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        source_candidate_count = int(count_row.get("visible_count") or 0)
                        if current_revision != normalized_revision or source_candidate_count != max(
                            0, int(expected_source_candidate_count or 0)
                        ):
                            connection.commit()
                            return {
                                "status": "not_ready",
                                "reason": "projection_membership_revision_stale",
                                "projection_id": normalized_projection_id,
                                "expected_membership_revision": normalized_revision,
                                "membership_revision": current_revision,
                                "expected_source_candidate_count": max(0, int(expected_source_candidate_count or 0)),
                                "source_candidate_count": source_candidate_count,
                            }
                        member_rows_by_key: dict[str, dict[str, Any]] = {}
                        for offset in range(0, len(normalized_candidate_keys), 500):
                            chunk = normalized_candidate_keys[offset : offset + 500]
                            placeholders = ", ".join("%s" for _ in chunk)
                            cursor.execute(
                                'SELECT * FROM "serving_projection_members" '
                                f"WHERE projection_id = %s AND candidate_identity_key IN ({placeholders}) "
                                "AND COALESCE(NULLIF(visibility_state, ''), 'visible') = 'visible'",
                                (normalized_projection_id, *chunk),
                            )
                            for member_row in _fetch_all_dict_rows(cursor):
                                candidate_key = str(member_row.get("candidate_identity_key") or "").strip()
                                if candidate_key:
                                    member_rows_by_key[candidate_key] = member_row
                        missing_candidate_keys = [
                            candidate_key
                            for candidate_key in normalized_candidate_keys
                            if candidate_key not in member_rows_by_key
                        ]
                        if missing_candidate_keys:
                            connection.commit()
                            return {
                                "status": "not_ready",
                                "reason": "projection_member_selection_not_visible",
                                "projection_id": normalized_projection_id,
                                "membership_revision": current_revision,
                                "missing_candidate_identity_keys": missing_candidate_keys,
                            }
                        person_key_by_candidate = {
                            key: str(member_rows_by_key[key].get("person_identity_key") or "").strip()
                            for key in normalized_candidate_keys
                        }
                        missing_person_candidates = [
                            key for key, person_key in person_key_by_candidate.items() if not person_key
                        ]
                        if missing_person_candidates:
                            connection.commit()
                            return {
                                "status": "not_ready",
                                "reason": "projection_member_person_identity_missing",
                                "projection_id": normalized_projection_id,
                                "membership_revision": current_revision,
                                "candidate_identity_keys": missing_person_candidates,
                            }
                        candidates_by_person: dict[str, list[str]] = {}
                        for candidate_key, person_key in person_key_by_candidate.items():
                            candidates_by_person.setdefault(person_key, []).append(candidate_key)
                        conflicting_people = {
                            person_key: candidate_keys
                            for person_key, candidate_keys in candidates_by_person.items()
                            if len(candidate_keys) > 1
                        }
                        if conflicting_people:
                            connection.commit()
                            return {
                                "status": "not_ready",
                                "reason": "projection_member_person_identity_conflict",
                                "projection_id": normalized_projection_id,
                                "membership_revision": current_revision,
                                "conflicting_person_identity_keys": conflicting_people,
                            }
                        person_identity_keys = sorted(candidates_by_person)
                        for person_identity_key in person_identity_keys:
                            self._acquire_transaction_lock(
                                cursor,
                                f"crm_record_identity:{normalized_workspace_id}:{person_identity_key}",
                            )
                        person_placeholders = ", ".join("%s" for _ in person_identity_keys)
                        cursor.execute(
                            'SELECT * FROM "crm_records" '
                            f"WHERE workspace_id = %s AND person_identity_key IN ({person_placeholders}) FOR UPDATE",
                            (normalized_workspace_id, *person_identity_keys),
                        )
                        existing_record_rows = _fetch_all_dict_rows(cursor)
                        existing_records_by_person: dict[str, dict[str, Any]] = {}
                        for record_row in existing_record_rows:
                            person_identity_key = str(record_row.get("person_identity_key") or "").strip()
                            if person_identity_key in existing_records_by_person:
                                raise RuntimeError("multiple CRM records exist for one workspace person identity")
                            existing_records_by_person[person_identity_key] = record_row
                        engagement_ids = sorted(
                            {
                                str(row.get("current_engagement_id") or "").strip()
                                for row in existing_record_rows
                                if str(row.get("current_engagement_id") or "").strip()
                            }
                        )
                        existing_engagements_by_id: dict[str, dict[str, Any]] = {}
                        if engagement_ids:
                            placeholders = ", ".join("%s" for _ in engagement_ids)
                            cursor.execute(
                                f'SELECT * FROM "crm_engagements" WHERE engagement_id IN ({placeholders}) FOR UPDATE',
                                tuple(engagement_ids),
                            )
                            existing_engagements_by_id = {
                                str(row.get("engagement_id") or "").strip(): row for row in _fetch_all_dict_rows(cursor)
                            }
                        idempotency_values = sorted(set(normalized_idempotency_keys.values()))
                        idempotency_placeholders = ", ".join("%s" for _ in idempotency_values)
                        cursor.execute(
                            'SELECT * FROM "crm_events" '
                            f"WHERE workspace_id = %s AND idempotency_key IN ({idempotency_placeholders}) FOR UPDATE",
                            (normalized_workspace_id, *idempotency_values),
                        )
                        existing_events_by_idempotency = {
                            str(row.get("idempotency_key") or "").strip(): row for row in _fetch_all_dict_rows(cursor)
                        }
                        selection_now = _utc_now_sql_timestamp()
                        built_payload = payload_builder(
                            projection_row=projection_row,
                            member_rows_by_key=member_rows_by_key,
                            existing_records_by_person=existing_records_by_person,
                            existing_engagements_by_id=existing_engagements_by_id,
                            existing_events_by_idempotency=existing_events_by_idempotency,
                            selection_now=selection_now,
                            source_candidate_count=source_candidate_count,
                        )
                        if not isinstance(built_payload, dict):
                            raise TypeError("projection CRM payload_builder must return a dict")
                        record_rows = self._normalize_bulk_upsert_rows(list(built_payload.get("record_rows") or []))
                        engagement_rows = self._normalize_bulk_upsert_rows(
                            list(built_payload.get("engagement_rows") or [])
                        )
                        event_rows = self._normalize_bulk_upsert_rows(list(built_payload.get("event_rows") or []))
                        item_results = [dict(item) for item in list(built_payload.get("item_results") or [])]
                        if [
                            str(item.get("candidate_identity_key") or "") for item in item_results
                        ] != normalized_candidate_keys:
                            raise ValueError("projection CRM item results must preserve the requested candidate order")
                        for write_table, payload_rows in (
                            ("crm_records", record_rows),
                            ("crm_engagements", engagement_rows),
                            ("crm_events", event_rows),
                        ):
                            plan = self._bulk_upsert_plan(
                                write_table,
                                payload_rows,
                                require_primary_key_values=True,
                            )
                            self._bulk_upsert_rows_with_cursor(
                                cursor,
                                table_name=write_table,
                                payload_rows=payload_rows,
                                plan=plan,
                            )
                        result_record_ids = sorted(
                            {str(item.get("crm_record_id") or "").strip() for item in item_results}
                        )
                        result_engagement_ids = sorted(
                            {str(item.get("engagement_id") or "").strip() for item in item_results}
                        )
                        result_event_ids = sorted({str(item.get("event_id") or "").strip() for item in item_results})

                        def _select_rows_by_ids(
                            result_table: str,
                            id_column: str,
                            result_ids: list[str],
                        ) -> list[dict[str, Any]]:
                            if not result_ids:
                                return []
                            placeholders = ", ".join("%s" for _ in result_ids)
                            cursor.execute(
                                f"SELECT * FROM {_quote_identifier(result_table)} "
                                f"WHERE {_quote_identifier(id_column)} IN ({placeholders})",
                                tuple(result_ids),
                            )
                            return _fetch_all_dict_rows(cursor)

                        final_record_rows = _select_rows_by_ids("crm_records", "crm_record_id", result_record_ids)
                        final_engagement_rows = _select_rows_by_ids(
                            "crm_engagements", "engagement_id", result_engagement_ids
                        )
                        final_event_rows = _select_rows_by_ids("crm_events", "event_id", result_event_ids)
                    connection.commit()
                return {
                    "status": "applied",
                    "projection_id": normalized_projection_id,
                    "membership_revision": current_revision,
                    "source_candidate_count": source_candidate_count,
                    "projection_row": projection_row,
                    "member_rows": [member_rows_by_key[key] for key in normalized_candidate_keys],
                    "record_rows": final_record_rows,
                    "engagement_rows": final_engagement_rows,
                    "event_rows": final_event_rows,
                    "item_results": item_results,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def apply_owned_crm_record_update(
        self,
        *,
        crm_record_id: str,
        expected_workspace_id: str,
        expected_owner_user_id: str,
        expected_crm_version: int,
        record_row: dict[str, Any] | None,
        engagement_row: dict[str, Any] | None,
        event_row: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Lock the CRM owner row and commit its record/engagement/event bundle.

        Blank stored ``owner_user_id`` remains the C2.5 compatibility contract,
        but workspace must still match exactly. The owner check, version check,
        and all bundle writes share one transaction, so an ownership transfer
        cannot land between authorization and the first durable mutation.
        """

        normalized_record_id = str(crm_record_id or "").strip()
        normalized_workspace = str(expected_workspace_id or "").strip()
        normalized_owner = str(expected_owner_user_id or "").strip()
        if not normalized_record_id or not normalized_workspace or not normalized_owner:
            return None
        required_tables = ("crm_records", "crm_engagements", "crm_events")
        if any(not self.should_prefer_read(table_name) for table_name in required_tables):
            return None
        self.ensure_bootstrapped()
        for table_name in required_tables:
            self._ensure_table_write_schema(table_name)

        normalized_record_rows = self._normalize_bulk_upsert_rows([dict(record_row or {})])
        normalized_engagement_rows = self._normalize_bulk_upsert_rows([dict(engagement_row or {})])
        normalized_event_rows = self._normalize_bulk_upsert_rows([dict(event_row or {})])
        if len(normalized_record_rows) != 1 or len(normalized_engagement_rows) != 1:
            raise ValueError("owned CRM update requires one record row and one engagement row")
        next_record = normalized_record_rows[0]
        next_engagement = normalized_engagement_rows[0]
        next_event = normalized_event_rows[0] if normalized_event_rows else {}
        if str(next_record.get("crm_record_id") or "") != normalized_record_id:
            raise ValueError("owned CRM update record id mismatch")
        if str(next_engagement.get("crm_record_id") or "") != normalized_record_id:
            raise ValueError("owned CRM update engagement record id mismatch")

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            'SELECT * FROM "crm_records" WHERE crm_record_id = %s FOR UPDATE',
                            (normalized_record_id,),
                        )
                        current_record = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        current_workspace = str(current_record.get("workspace_id") or "").strip()
                        current_owner = str(current_record.get("owner_user_id") or "").strip()
                        if (
                            not current_record
                            or current_workspace != normalized_workspace
                            or (current_owner and current_owner != normalized_owner)
                        ):
                            connection.commit()
                            return {"status": "not_found", "reason": "crm_record_not_found"}
                        current_version = int(current_record.get("crm_version") or 0)
                        if int(expected_crm_version or 0) > 0 and current_version != int(expected_crm_version):
                            connection.commit()
                            return {"status": "conflict", "reason": "crm_record_stale"}

                        # Ownership/identity columns are not writable through this
                        # update UoW. Blank owner remains blank until its separate
                        # migration owner is approved.
                        next_record.update(
                            {
                                "crm_record_id": normalized_record_id,
                                "workspace_id": current_workspace,
                                "owner_user_id": current_owner,
                                "person_identity_key": str(current_record.get("person_identity_key") or ""),
                                "crm_version": current_version + 1,
                                "created_at": current_record.get("created_at"),
                            }
                        )
                        engagement_id = str(next_engagement.get("engagement_id") or "").strip()
                        if not engagement_id:
                            raise ValueError("owned CRM update engagement id is required")
                        cursor.execute(
                            'SELECT * FROM "crm_engagements" WHERE engagement_id = %s FOR UPDATE',
                            (engagement_id,),
                        )
                        cursor.fetchone()

                        existing_event: dict[str, Any] = {}
                        event_idempotency_key = str(next_event.get("idempotency_key") or "").strip()
                        if event_idempotency_key:
                            cursor.execute(
                                'SELECT * FROM "crm_events" '
                                "WHERE workspace_id = %s AND idempotency_key = %s FOR UPDATE",
                                (normalized_workspace, event_idempotency_key),
                            )
                            existing_event = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}

                        rows_to_write = (
                            ("crm_records", [next_record]),
                            ("crm_engagements", [next_engagement]),
                            ("crm_events", [] if existing_event else ([next_event] if next_event else [])),
                        )
                        for table_name, payload_rows in rows_to_write:
                            plan = self._bulk_upsert_plan(
                                table_name,
                                payload_rows,
                                require_primary_key_values=True,
                            )
                            self._bulk_upsert_rows_with_cursor(
                                cursor,
                                table_name=table_name,
                                payload_rows=payload_rows,
                                plan=plan,
                            )

                        cursor.execute(
                            'SELECT * FROM "crm_records" WHERE crm_record_id = %s',
                            (normalized_record_id,),
                        )
                        final_record = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        cursor.execute(
                            'SELECT * FROM "crm_engagements" WHERE engagement_id = %s',
                            (engagement_id,),
                        )
                        final_engagement = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        final_event = existing_event
                        event_id = str(next_event.get("event_id") or "").strip()
                        if not final_event and event_id:
                            cursor.execute(
                                'SELECT * FROM "crm_events" WHERE event_id = %s',
                                (event_id,),
                            )
                            final_event = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    connection.commit()
                return {
                    "status": "applied",
                    "record_row": final_record,
                    "engagement_row": final_engagement,
                    "event_row": final_event,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_crm_public_web_promotion_if_owned(
        self,
        *,
        row: dict[str, Any] | None,
        crm_record_id: str,
        expected_workspace_id: str,
        expected_owner_user_id: str,
    ) -> dict[str, Any] | None:
        """Commit the promotion's first durable write under the CRM owner row lock."""

        normalized_record_id = str(crm_record_id or "").strip()
        normalized_workspace = str(expected_workspace_id or "").strip()
        normalized_owner = str(expected_owner_user_id or "").strip()
        payload_rows = self._normalize_bulk_upsert_rows([dict(row or {})])
        if not normalized_record_id or not normalized_workspace or not normalized_owner or len(payload_rows) != 1:
            return None
        if not self.should_prefer_read("crm_records") or not self.should_prefer_read("crm_public_web_promotions"):
            return None
        self.ensure_bootstrapped()
        self._ensure_table_write_schema("crm_records")
        self._ensure_table_write_schema("crm_public_web_promotions")
        promotion_row = payload_rows[0]
        if str(promotion_row.get("crm_record_id") or "").strip() != normalized_record_id:
            raise ValueError("owner-fenced CRM promotion record id mismatch")

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            'SELECT * FROM "crm_records" WHERE crm_record_id = %s FOR UPDATE',
                            (normalized_record_id,),
                        )
                        current_record = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        current_workspace = str(current_record.get("workspace_id") or "").strip()
                        current_owner = str(current_record.get("owner_user_id") or "").strip()
                        if (
                            not current_record
                            or current_workspace != normalized_workspace
                            or (current_owner and current_owner != normalized_owner)
                        ):
                            connection.commit()
                            return {"status": "not_found", "reason": "crm_record_not_found"}
                        promotion_row["workspace_id"] = current_workspace

                        promotion_id = str(promotion_row.get("promotion_id") or "").strip()
                        if not promotion_id:
                            raise ValueError("owner-fenced CRM promotion id is required")
                        # Fixed lock order: canonical CRM record row first, then
                        # a schema-scoped promotion-id advisory lock, then the
                        # promotion row. The advisory lock serializes the
                        # otherwise-unlockable absent-row case as well.
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))",
                            (self._advisory_lock_key(f"crm_public_web_promotion:{promotion_id}"),),
                        )
                        cursor.fetchone()
                        cursor.execute(
                            'SELECT * FROM "crm_public_web_promotions" WHERE promotion_id = %s FOR UPDATE',
                            (promotion_id,),
                        )
                        existing_promotion = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}

                        def _promotion_identity_matches(existing: dict[str, Any]) -> bool:
                            # A promotion id is a strict idempotency identity.
                            # Every submitted field except storage timestamps is
                            # immutable; exact replay returns the existing row.
                            return all(
                                _normalize_postgres_payload(existing.get(column)) == _normalize_postgres_payload(value)
                                for column, value in promotion_row.items()
                                if column not in {"created_at", "updated_at"}
                            )

                        if existing_promotion:
                            if not _promotion_identity_matches(existing_promotion):
                                connection.commit()
                                return {
                                    "status": "conflict",
                                    "reason": "crm_public_web_promotion_idempotency_conflict",
                                }
                            connection.commit()
                            return {
                                "status": "applied",
                                "row": existing_promotion,
                                "idempotent_replay": True,
                            }

                        plan = self._bulk_upsert_plan(
                            "crm_public_web_promotions",
                            [promotion_row],
                            require_primary_key_values=True,
                        )
                        if plan is None:
                            raise ValueError("owner-fenced CRM promotion insert plan is required")
                        columns, primary_keys = plan
                        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
                        placeholders = ", ".join(["%s"] * len(columns))
                        conflict_target = ", ".join(_quote_identifier(column) for column in primary_keys)
                        cursor.execute(
                            f'INSERT INTO "crm_public_web_promotions" ({quoted_columns}) '
                            f"VALUES ({placeholders}) ON CONFLICT ({conflict_target}) DO NOTHING RETURNING *",
                            tuple(_normalize_postgres_payload(promotion_row.get(column)) for column in columns),
                        )
                        inserted_promotion = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        if not inserted_promotion:
                            # Defensive closure for a concurrent legacy writer
                            # that does not yet share the advisory lock.
                            cursor.execute(
                                'SELECT * FROM "crm_public_web_promotions" WHERE promotion_id = %s FOR UPDATE',
                                (promotion_id,),
                            )
                            inserted_promotion = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                            if not inserted_promotion or not _promotion_identity_matches(inserted_promotion):
                                connection.commit()
                                return {
                                    "status": "conflict",
                                    "reason": "crm_public_web_promotion_idempotency_conflict",
                                }
                        cursor.execute(
                            'SELECT * FROM "crm_public_web_promotions" WHERE promotion_id = %s',
                            (promotion_id,),
                        )
                        final_row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    connection.commit()
                return {"status": "applied", "row": final_row, "idempotent_replay": False}
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    @staticmethod
    def _normalize_bulk_upsert_rows(
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> list[dict[str, Any]]:
        return [
            _normalize_postgres_row_payload(dict(row or {}))
            for row in list(rows or [])
            if isinstance(row, dict) and dict(row)
        ]

    @staticmethod
    def _bulk_upsert_plan(
        table_name: str,
        payload_rows: list[dict[str, Any]],
        *,
        require_primary_key_values: bool = False,
    ) -> tuple[list[str], list[str]] | None:
        if not payload_rows:
            return None
        primary_keys = list(_PRIMARY_KEY_COLUMNS.get(table_name) or [])
        if not primary_keys:
            if require_primary_key_values:
                raise ValueError(f"bulk upsert table has no registered primary key: {table_name}")
            return None
        columns: list[str] = []
        seen_columns: set[str] = set()
        for payload in payload_rows:
            for column in payload:
                if not str(column or "").strip() or column in seen_columns:
                    continue
                seen_columns.add(column)
                columns.append(column)
        missing_primary_keys = [column for column in primary_keys if column not in columns]
        if missing_primary_keys:
            if require_primary_key_values:
                raise ValueError(
                    f"bulk upsert rows missing primary key columns for {table_name}: {', '.join(missing_primary_keys)}"
                )
            return None
        if require_primary_key_values:
            for row_index, payload in enumerate(payload_rows):
                missing_values = [column for column in primary_keys if payload.get(column) in {None, ""}]
                if missing_values:
                    raise ValueError(
                        f"bulk upsert row {row_index} has empty primary key values for {table_name}: "
                        f"{', '.join(missing_values)}"
                    )
        return columns, primary_keys

    def _bulk_upsert_rows_with_cursor(
        self,
        cursor: Any,
        *,
        table_name: str,
        payload_rows: list[dict[str, Any]],
        plan: tuple[list[str], list[str]] | None,
        preserve_projection_index_input_revision: bool = True,
    ) -> int:
        if not payload_rows:
            return 0
        if plan is None:
            return 0
        columns, primary_keys = plan
        update_columns = [column for column in columns if column not in primary_keys]
        quoted_table_name = _quote_identifier(table_name)
        quoted_columns = [_quote_identifier(column) for column in columns]
        conflict_target = ", ".join(_quote_identifier(column) for column in primary_keys)
        chunks = _chunk_postgres_bulk_rows(payload_rows, column_count=len(columns))

        def _values_sql(chunk: list[dict[str, Any]]) -> str:
            return ", ".join("(" + ", ".join(["%s"] * len(columns)) + ")" for _ in chunk)

        def _write_params(chunk: list[dict[str, Any]]) -> tuple[Any, ...]:
            return tuple(_normalize_postgres_payload(payload.get(column)) for payload in chunk for column in columns)

        def _merge_clause() -> str:
            if update_columns:
                assignments: list[str] = []
                for column in update_columns:
                    quoted_column = _quote_identifier(column)
                    if table_name == "serving_projections" and column == "metadata_json":
                        preserve_keys = (
                            PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY,
                            PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY,
                            *(
                                (PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,)
                                if preserve_projection_index_input_revision
                                else ()
                            ),
                        )
                        assignments.append(
                            f"{quoted_column} = "
                            + _projection_metadata_merge_sql(
                                quoted_table_name=quoted_table_name,
                                quoted_column=quoted_column,
                                preserve_keys=preserve_keys,
                            )
                        )
                        continue
                    assignments.append(f"{quoted_column} = EXCLUDED.{quoted_column}")
                return f" ON CONFLICT ({conflict_target}) DO UPDATE SET " + ", ".join(assignments)
            return f" ON CONFLICT ({conflict_target}) DO NOTHING"

        if _bulk_upsert_prefers_direct_values(
            row_count=len(payload_rows),
            column_count=len(columns),
        ):
            merge_sql = (
                f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) "
                f"VALUES {_values_sql(payload_rows)}{_merge_clause()}"
            )
            cursor.execute(merge_sql, _write_params(payload_rows))
            return int(cursor.rowcount or 0)

        temp_table_name = _quote_identifier(
            f"_cp_bulk_{table_name}_{threading.get_ident()}_{int(time.time() * 1000000)}"
        )
        cursor.execute(
            f"CREATE TEMP TABLE {temp_table_name} (LIKE {quoted_table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        for chunk in chunks:
            cursor.execute(
                f"INSERT INTO {temp_table_name} ({', '.join(quoted_columns)}) VALUES {_values_sql(chunk)}",
                _write_params(chunk),
            )
        merge_sql = (
            f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) "
            f"SELECT {', '.join(quoted_columns)} FROM {temp_table_name}{_merge_clause()}"
        )
        cursor.execute(merge_sql)
        return int(cursor.rowcount or 0)

    def _replace_rows_with_cursor(
        self,
        cursor: Any,
        *,
        table_name: str,
        where_sql: str,
        params: list[Any] | tuple[Any, ...],
        payload_rows: list[dict[str, Any]],
        plan: tuple[list[str], list[str]] | None,
    ) -> int:
        cursor.execute(
            f"DELETE FROM {_quote_identifier(table_name)} WHERE {where_sql}",
            tuple(_normalize_postgres_payload(item) for item in list(params or [])),
        )
        return self._bulk_upsert_rows_with_cursor(
            cursor,
            table_name=table_name,
            payload_rows=payload_rows,
            plan=plan,
        )

    def _acquire_transaction_lock(self, cursor: Any, lock_key: str) -> None:
        normalized_lock_key = str(lock_key or "").strip()
        if not normalized_lock_key:
            return
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (self._advisory_lock_key(normalized_lock_key),),
        )

    def _try_acquire_transaction_lock(self, cursor: Any, lock_key: str) -> bool:
        normalized_lock_key = str(lock_key or "").strip()
        if not normalized_lock_key:
            return True
        cursor.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
            (self._advisory_lock_key(normalized_lock_key),),
        )
        row = cursor.fetchone()
        if isinstance(row, dict):
            return bool(next(iter(row.values()), False))
        if isinstance(row, (list, tuple)):
            return bool(row[0]) if row else False
        return bool(row)

    def _connect_with_transaction_lock(self, lock_key: str) -> Any:
        """Return a transaction holding ``lock_key`` without waiting inside the pool.

        A session-lock owner can need a pooled connection while a conflicting
        transaction is waiting. Blocking on ``pg_advisory_xact_lock`` after pool
        checkout can therefore create a circular wait. Try-lock attempts return
        the connection to the pool before backing off; the successful transaction
        keeps the same checked-out connection and lock for its complete unit of work.
        """

        normalized_lock_key = str(lock_key or "").strip()
        if not normalized_lock_key:
            return self._connect()
        contention_attempt = 0
        while True:
            connection = self._connect()
            try:
                with connection.cursor() as cursor:
                    acquired = self._try_acquire_transaction_lock(cursor, normalized_lock_key)
                if acquired:
                    return connection
                connection.rollback()
            except Exception:
                try:
                    connection.rollback()
                finally:
                    connection.close()
                raise
            connection.close()
            contention_attempt += 1
            time.sleep(_control_plane_postgres_retry_delay_seconds(min(contention_attempt, 5)))

    def replace_candidate_materialization_state_scope(
        self,
        *,
        table_name: str = "",
        target_company: str,
        company_key: str = "",
        snapshot_id: str,
        asset_view: str,
        rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_target_company, normalized_company_key = _normalized_company_scope(
            target_company,
            company_key,
        )
        normalized_snapshot_id = _normalize_postgres_identifier(snapshot_id)
        normalized_asset_view = _normalize_postgres_identifier(asset_view) or "canonical_merged"
        normalized_rows = [
            _normalize_postgres_row_payload(dict(row or {}))
            for row in list(rows or [])
            if isinstance(row, dict) and dict(row)
        ]
        if (
            not self.should_prefer_read("candidate_materialization_state")
            or not normalized_target_company
            or not normalized_snapshot_id
        ):
            return 0
        columns = [
            "target_company",
            "company_key",
            "snapshot_id",
            "asset_view",
            "candidate_id",
            "fingerprint",
            "shard_path",
            "list_page",
            "dirty_reason",
            "materialized_at",
            "metadata_json",
            "created_at",
            "updated_at",
        ]
        quoted_table_name = _quote_identifier("candidate_materialization_state")
        quoted_columns = [_quote_identifier(column) for column in columns]
        company_scope_clause, company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
            placeholder="%s",
        )
        delete_sql = (
            f"DELETE FROM {quoted_table_name} WHERE {company_scope_clause} AND snapshot_id = %s AND asset_view = %s"
        )
        delete_params = [
            *company_scope_params,
            normalized_snapshot_id,
            normalized_asset_view,
        ]
        self.ensure_bootstrapped()
        self._ensure_table_write_schema("candidate_materialization_state")
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(delete_sql, tuple(delete_params))
                        inserted = 0
                        for chunk in _chunk_postgres_bulk_rows(normalized_rows, column_count=len(columns)):
                            if not chunk:
                                continue
                            values_sql = ", ".join("(" + ", ".join(["%s"] * len(columns)) + ")" for _ in chunk)
                            insert_sql = (
                                f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES {values_sql}"
                            )
                            params: list[Any] = []
                            for payload in chunk:
                                params.extend(_normalize_postgres_payload(payload.get(column)) for column in columns)
                            cursor.execute(insert_sql, tuple(params))
                            inserted += len(chunk)
                    connection.commit()
                return inserted
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def select_one(
        self,
        table_name: str,
        *,
        where_sql: str,
        params: list[Any] | tuple[Any, ...],
        order_by_sql: str = "",
    ) -> dict[str, Any] | None:
        rows = self.select_many(
            table_name,
            where_sql=where_sql,
            params=params,
            order_by_sql=order_by_sql,
            limit=1,
        )
        return rows[0] if rows else None

    def count_rows(
        self,
        table_name: str,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
    ) -> int:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_prefer_read(normalized_table):
            return 0
        postgres_only_read = self.mode == "postgres_only"
        if not postgres_only_read:
            self.ensure_bootstrapped()
        query_parts = [f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(normalized_table)}"]
        normalized_where_sql = str(where_sql or "").strip()
        if normalized_where_sql:
            query_parts.append(f"WHERE {normalized_where_sql}")
        query = " ".join(query_parts)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if postgres_only_read and not _postgres_table_exists(
                    cursor,
                    normalized_table,
                    schema=self.schema,
                ):
                    raise RuntimeError(f"Postgres authoritative table is missing: {normalized_table}")
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in list(params or [])))
                row = cursor.fetchone()
        if isinstance(row, dict):
            return int(row.get("row_count") or 0)
        if isinstance(row, (list, tuple)):
            return int((row[0] if row else 0) or 0)
        return int(getattr(row, "row_count", 0) or 0)

    def select_many(
        self,
        table_name: str,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_prefer_read(normalized_table):
            return []
        postgres_only_read = self.mode == "postgres_only"
        if not postgres_only_read:
            self.ensure_bootstrapped()
        if not postgres_only_read and normalized_table == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
            self._ensure_table_write_schema(normalized_table)
        query_parts = [f"SELECT * FROM {_quote_identifier(normalized_table)}"]
        normalized_where_sql = str(where_sql or "").strip()
        if normalized_where_sql:
            query_parts.append(f"WHERE {normalized_where_sql}")
        normalized_order_by = str(order_by_sql or "").strip()
        if normalized_order_by:
            query_parts.append(f"ORDER BY {normalized_order_by}")
        normalized_limit = int(limit or 0)
        if normalized_limit > 0:
            query_parts.append("LIMIT %s")
        normalized_offset = max(0, int(offset or 0))
        if normalized_offset > 0:
            query_parts.append("OFFSET %s")
        query = " ".join(query_parts)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if postgres_only_read and not _postgres_table_exists(
                    cursor,
                    normalized_table,
                    schema=self.schema,
                ):
                    raise RuntimeError(f"Postgres authoritative table is missing: {normalized_table}")
                query_params = list(params)
                if normalized_limit > 0:
                    query_params.append(normalized_limit)
                if normalized_offset > 0:
                    query_params.append(normalized_offset)
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in query_params))
                return _fetch_all_dict_rows(cursor)

    def list_latest_target_candidate_public_web_runs_by_record_ids(
        self,
        record_ids: list[str] | tuple[str, ...],
        *,
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("target_candidate_public_web_runs"):
            return []
        normalized_record_ids: list[str] = []
        seen: set[str] = set()
        for raw_record_id in list(record_ids or []):
            record_id = str(raw_record_id or "").strip()
            if not record_id or record_id in seen:
                continue
            seen.add(record_id)
            normalized_record_ids.append(record_id)
        if not normalized_record_ids:
            return []
        normalized_limit = max(1, int(limit or 1000))
        normalized_record_ids = normalized_record_ids[:normalized_limit]
        status_filter = str(status or "").strip().lower()
        record_placeholders = ", ".join(["%s"] * len(normalized_record_ids))
        status_clause = "AND status = %s" if status_filter else ""
        query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY record_id
                           ORDER BY updated_at DESC, created_at DESC, run_id DESC
                       ) AS public_web_run_rank
                FROM target_candidate_public_web_runs
                WHERE record_id IN ({record_placeholders})
                {status_clause}
            ) ranked_public_web_runs
            WHERE public_web_run_rank = 1
            ORDER BY updated_at DESC, created_at DESC, run_id DESC
            LIMIT %s
        """
        params: list[Any] = [*normalized_record_ids]
        if status_filter:
            params.append(status_filter)
        params.append(normalized_limit)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if not _postgres_table_exists(cursor, "target_candidate_public_web_runs"):
                    return []
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in params))
                return _fetch_all_dict_rows(cursor)

    def list_latest_crm_public_web_runs_by_record_ids(
        self,
        crm_record_ids: list[str] | tuple[str, ...],
        *,
        workspace_id: str = "default",
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("crm_public_web_runs"):
            return []
        normalized_record_ids: list[str] = []
        seen: set[str] = set()
        for raw_record_id in list(crm_record_ids or []):
            record_id = str(raw_record_id or "").strip()
            if not record_id or record_id in seen:
                continue
            seen.add(record_id)
            normalized_record_ids.append(record_id)
        if not normalized_record_ids:
            return []
        normalized_limit = max(1, int(limit or 1000))
        normalized_record_ids = normalized_record_ids[:normalized_limit]
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        status_filter = str(status or "").strip().lower()
        record_placeholders = ", ".join(["%s"] * len(normalized_record_ids))
        status_clause = "AND status = %s" if status_filter else ""
        query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY crm_record_id
                           ORDER BY created_at DESC, run_id DESC
                       ) AS public_web_run_rank
                FROM crm_public_web_runs
                WHERE workspace_id = %s
                  AND crm_record_id IN ({record_placeholders})
                {status_clause}
            ) ranked_public_web_runs
            WHERE public_web_run_rank = 1
            ORDER BY created_at DESC, run_id DESC
            LIMIT %s
        """
        params: list[Any] = [normalized_workspace_id, *normalized_record_ids]
        if status_filter:
            params.append(status_filter)
        params.append(normalized_limit)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if not _postgres_table_exists(cursor, "crm_public_web_runs"):
                    return []
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in params))
                return _fetch_all_dict_rows(cursor)

    def list_latest_company_public_web_asset_runs_by_company_keys(
        self,
        company_keys: list[str] | tuple[str, ...],
        *,
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("company_public_web_asset_runs"):
            return []
        normalized_company_keys: list[str] = []
        seen: set[str] = set()
        for raw_company_key in list(company_keys or []):
            company_key = str(raw_company_key or "").strip()
            if not company_key or company_key in seen:
                continue
            seen.add(company_key)
            normalized_company_keys.append(company_key)
        if not normalized_company_keys:
            return []
        normalized_limit = max(1, int(limit or 1000))
        normalized_company_keys = normalized_company_keys[:normalized_limit]
        status_filter = str(status or "").strip().lower()
        company_placeholders = ", ".join(["%s"] * len(normalized_company_keys))
        status_clause = "AND status = %s" if status_filter else ""
        latest_order_sql = company_public_web_projection_order_sql(
            metadata_expression="metadata_json::jsonb",
            updated_at_expression="updated_at",
            created_at_expression="created_at",
            run_id_expression="run_id",
        )
        query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY company_key
                           ORDER BY {latest_order_sql}
                       ) AS company_public_web_run_rank
                FROM company_public_web_asset_runs
                WHERE company_key IN ({company_placeholders})
                {status_clause}
            ) ranked_company_public_web_runs
            WHERE company_public_web_run_rank = 1
            ORDER BY {latest_order_sql}
            LIMIT %s
        """
        params: list[Any] = [*normalized_company_keys]
        if status_filter:
            params.append(status_filter)
        params.append(normalized_limit)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if not _postgres_table_exists(cursor, "company_public_web_asset_runs"):
                    return []
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in params))
                return _fetch_all_dict_rows(cursor)

    def materialize_company_public_web_assets_for_exact_workflow_claim(
        self,
        *,
        command_id: str,
        expected_attempt: int,
        expected_lease_owner: str,
        company_asset_rows: list[dict[str, Any]],
        company_evidence_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Linearize the first canonical write behind one exact D1m claim.

        Locking the workflow command first makes the command claim an effect
        reservation: if takeover wins, this transaction writes no canonical
        rows; if this transaction wins, takeover waits until the whole asset and
        evidence projection commits. A retry of the same or a later valid claim
        can safely replay the deterministic row identities.
        """

        if not self.should_prefer_read("workflow_commands"):
            return None
        if not self.should_prefer_read("company_assets") or not self.should_prefer_read("company_evidence"):
            return None
        normalized_command_id = str(command_id or "").strip()
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_attempt = 0 if isinstance(expected_attempt, bool) else max(0, int(expected_attempt or 0))
        asset_rows = [
            _normalize_postgres_row_payload(dict(row or {}))
            for row in list(company_asset_rows or [])
            if isinstance(row, dict)
        ]
        evidence_rows = [
            _normalize_postgres_row_payload(dict(row or {}))
            for row in list(company_evidence_rows or [])
            if isinstance(row, dict)
        ]
        if not normalized_command_id or not normalized_lease_owner or normalized_attempt <= 0:
            raise ValueError("company_public_web_materialize_claim_identity_required")
        if not asset_rows or len(asset_rows) != len(evidence_rows):
            raise ValueError("company_public_web_materialize_rows_required")
        asset_ids = [str(row.get("asset_id") or "").strip() for row in asset_rows]
        evidence_ids = [str(row.get("evidence_id") or "").strip() for row in evidence_rows]
        if (
            any(not value for value in asset_ids)
            or any(not value for value in evidence_ids)
            or len(set(asset_ids)) != len(asset_ids)
            or len(set(evidence_ids)) != len(evidence_ids)
            or any(str(row.get("asset_id") or "").strip() not in set(asset_ids) for row in evidence_rows)
        ):
            raise ValueError("company_public_web_materialize_row_identity_invalid")
        asset_rows.sort(key=lambda row: str(row.get("asset_id") or ""))
        evidence_rows.sort(key=lambda row: str(row.get("evidence_id") or ""))
        self.ensure_bootstrapped()

        def _upsert_rows(
            cursor: Any,
            *,
            table_name: str,
            id_column: str,
            rows: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            written: list[dict[str, Any]] = []
            for row in rows:
                columns = [column for column in row if str(column or "").strip()]
                quoted_table_name = _quote_identifier(table_name)
                quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                incoming_run_id = (
                    "COALESCE(NULLIF(EXCLUDED.source_run_id, ''), "
                    "EXCLUDED.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                    if table_name == "company_assets"
                    else "COALESCE(EXCLUDED.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                )
                existing_run_id = (
                    f"COALESCE(NULLIF({quoted_table_name}.source_run_id, ''), "
                    f"{quoted_table_name}.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                    if table_name == "company_assets"
                    else f"COALESCE({quoted_table_name}.metadata_json::jsonb ->> 'materialized_source_run_id', '')"
                )
                incoming_wins = _company_public_web_projection_incoming_wins_sql(
                    incoming_metadata_expression="EXCLUDED.metadata_json::jsonb",
                    existing_metadata_expression=f"{quoted_table_name}.metadata_json::jsonb",
                    incoming_run_id_expression=incoming_run_id,
                    existing_run_id_expression=existing_run_id,
                )
                assignments: list[str] = []
                for column in columns:
                    if column in {id_column, "created_at"}:
                        continue
                    quoted_column = _quote_identifier(column)
                    if column == "updated_at":
                        assignments.append(
                            f"{quoted_column} = GREATEST({quoted_table_name}.{quoted_column}, EXCLUDED.{quoted_column})"
                        )
                    elif column == "metadata_json":
                        assignments.append(
                            f"{quoted_column} = jsonb_set("
                            f"(CASE WHEN {incoming_wins} "
                            f"THEN EXCLUDED.{quoted_column}::jsonb "
                            f"ELSE {quoted_table_name}.{quoted_column}::jsonb END), "
                            "'{source_run_ids}', "
                            "(SELECT COALESCE(jsonb_agg(source_run_id ORDER BY source_run_id), '[]'::jsonb) "
                            "FROM ("
                            "SELECT DISTINCT jsonb_array_elements_text("
                            f"COALESCE({quoted_table_name}.{quoted_column}::jsonb -> 'source_run_ids', "
                            "'[]'::jsonb)) AS source_run_id "
                            "UNION SELECT DISTINCT jsonb_array_elements_text("
                            f"COALESCE(EXCLUDED.{quoted_column}::jsonb -> 'source_run_ids', '[]'::jsonb)) "
                            ") company_public_web_canonical_source_runs "
                            "WHERE source_run_id <> ''), true)::text"
                        )
                    else:
                        assignments.append(
                            f"{quoted_column} = CASE WHEN {incoming_wins} "
                            f"THEN EXCLUDED.{quoted_column} ELSE {quoted_table_name}.{quoted_column} END"
                        )
                cursor.execute(
                    f"INSERT INTO {quoted_table_name} ({quoted_columns}) "
                    f"VALUES ({placeholders}) ON CONFLICT ({_quote_identifier(id_column)}) "
                    f"DO UPDATE SET {', '.join(assignments)} RETURNING *",
                    tuple(_normalize_postgres_payload(row.get(column)) for column in columns),
                )
                current = _fetch_one_dict_row(cursor, cursor.fetchone())
                if current is None:
                    raise RuntimeError("company_public_web_materialize_upsert_returned_no_row")
                written.append(current)
            return written

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT *
                            FROM workflow_commands
                            WHERE command_id = %s
                              AND command_type = 'company.public_web.assets.materialize'
                              AND owner = 'company_public_web_owner'
                              AND status = 'running'
                              AND attempt = %s
                              AND lease_owner = %s
                              AND (NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC')
                                  > clock_timestamp()
                            FOR UPDATE
                            """,
                            (normalized_command_id, normalized_attempt, normalized_lease_owner),
                        )
                        claim = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if claim is None:
                            cursor.execute(
                                "SELECT * FROM workflow_commands WHERE command_id = %s",
                                (normalized_command_id,),
                            )
                            current_command = _fetch_one_dict_row(cursor, cursor.fetchone())
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_materialize_command_claim_not_current",
                                "command": current_command,
                                "company_assets": [],
                                "company_evidence": [],
                            }
                        written_assets = _upsert_rows(
                            cursor,
                            table_name="company_assets",
                            id_column="asset_id",
                            rows=asset_rows,
                        )
                        written_evidence = _upsert_rows(
                            cursor,
                            table_name="company_evidence",
                            id_column="evidence_id",
                            rows=evidence_rows,
                        )
                    connection.commit()
                return {
                    "outcome": "materialized",
                    "reason": "company_public_web_assets_materialized_for_exact_claim",
                    "command": claim,
                    "company_assets": written_assets,
                    "company_evidence": written_evidence,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def create_company_public_web_asset_run_if_absent(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Atomically create one source-run identity or return its replay.

        Both the effective idempotency identity and the explicit run id are
        locked before probing absent rows.  The migration-backed effective-key
        unique index remains the final fence for writers that do not yet share
        these advisory locks.
        """

        table_name = "company_public_web_asset_runs"
        if not self.should_prefer_read(table_name):
            return None
        raw_payload = dict(payload or {})
        raw_idempotency_key = normalize_company_public_web_asset_run_idempotency_key(raw_payload.get("idempotency_key"))
        row_payload = _normalize_postgres_row_payload(raw_payload)
        run_id = str(row_payload.get("run_id") or "").strip()
        idempotency_key = raw_idempotency_key
        if not run_id or not idempotency_key:
            raise ValueError("company_public_web_asset_run_identity_required")
        row_payload["run_id"] = run_id
        row_payload["idempotency_key"] = idempotency_key
        incoming_owner, incoming_owner_invalid = _company_public_web_source_run_owner(row_payload.get("metadata_json"))
        if incoming_owner_invalid:
            raise ValueError("company_public_web_source_run_owner_invalid")
        self.ensure_bootstrapped()
        columns = [column for column in row_payload if str(column or "").strip()]
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        def _resolve_identity(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
            by_idempotency = [
                row
                for row in rows
                if normalize_company_public_web_asset_run_idempotency_key(row.get("idempotency_key")) == idempotency_key
            ]
            by_run_id = [row for row in rows if str(row.get("run_id") or "").strip() == run_id]
            if len(by_idempotency) > 1:
                raise ValueError("company_public_web_asset_run_identity_collision:idempotency_key_duplicate")
            if len(by_run_id) > 1:
                raise ValueError("company_public_web_asset_run_identity_collision:run_id_duplicate")
            idempotent_row = by_idempotency[0] if by_idempotency else None
            run_id_row = by_run_id[0] if by_run_id else None
            if idempotent_row is not None and str(idempotent_row.get("run_id") or "").strip() != run_id:
                raise ValueError("company_public_web_asset_run_identity_collision:idempotency_key")
            if (
                run_id_row is not None
                and normalize_company_public_web_asset_run_idempotency_key(run_id_row.get("idempotency_key"))
                != idempotency_key
            ):
                raise ValueError("company_public_web_asset_run_identity_collision:run_id")
            if idempotent_row is not None and run_id_row is not None and idempotent_row != run_id_row:
                raise ValueError("company_public_web_asset_run_identity_collision:identity_split")
            return run_id_row or idempotent_row

        def _select_identity_rows(cursor: Any) -> list[dict[str, Any]]:
            cursor.execute(
                "SELECT *, '~' || idempotency_key || '~' AS _idempotency_key_fenced "
                "FROM company_public_web_asset_runs "
                "WHERE btrim(idempotency_key, E' \\t\\n\\r\\f\\013') = %s OR run_id = %s "
                "ORDER BY run_id FOR UPDATE",
                (idempotency_key, run_id),
            )
            rows = _fetch_all_dict_rows(cursor)
            for row in rows:
                fenced_key = str(row.pop("_idempotency_key_fenced", "") or "")
                if len(fenced_key) >= 2:
                    row["idempotency_key"] = fenced_key[1:-1]
            return rows

        def _owner_lost(existing: dict[str, Any], reason: str) -> dict[str, Any]:
            return {
                "created": False,
                "reclaimed": False,
                "outcome": "owner_lost",
                "reason": reason,
                "run": existing,
            }

        def _resolve_existing_owner(cursor: Any, existing: dict[str, Any]) -> dict[str, Any]:
            existing_owner, existing_owner_invalid = _company_public_web_source_run_owner(existing.get("metadata_json"))
            existing_status = str(existing.get("status") or "").strip().lower()
            same_source_command = bool(
                incoming_owner and existing_owner and incoming_owner["command_id"] == existing_owner["command_id"]
            )

            # A completed replay can repair publication only while an exact,
            # unexpired physical claim for the same logical source command is
            # current. This includes a higher retry attempt after the original
            # attempt crashed between terminalization and publication. A
            # distinct command remains read-only with respect to this run.
            if existing_status == "completed":
                if not incoming_owner:
                    if not existing_owner and not existing_owner_invalid:
                        return {
                            "created": False,
                            "reclaimed": False,
                            "outcome": "read_only_join",
                            "reason": "company_public_web_completed_run_read_only_join",
                            "run": existing,
                        }
                    return _owner_lost(
                        existing,
                        "company_public_web_completed_run_replay_requires_exact_current_owner",
                    )
                current_command = _lock_current_company_public_web_source_command(cursor, incoming_owner)
                if current_command is None:
                    return _owner_lost(existing, "company_public_web_source_command_claim_not_current")
                if existing_owner_invalid or not existing_owner or not same_source_command:
                    return _owner_lost(
                        existing,
                        "company_public_web_completed_run_replay_read_only",
                    )
                if incoming_owner["command_attempt"] < existing_owner["command_attempt"] or (
                    incoming_owner["command_attempt"] == existing_owner["command_attempt"]
                    and incoming_owner["lease_owner"] != existing_owner["lease_owner"]
                ):
                    return _owner_lost(
                        existing,
                        "company_public_web_completed_run_replay_read_only",
                    )
                return {
                    "created": False,
                    "reclaimed": False,
                    "outcome": "joined",
                    "reason": "company_public_web_source_run_completed_repair_authorized",
                    "run": existing,
                }

            # A nonterminal source row is owned by a physical command claim, so
            # every join/reclaim must prove that the incoming claim is the exact
            # current, unexpired owner. In particular, an old execution with the
            # same attempt/lease values may not keep joining after the workflow
            # command has moved on.
            if existing_status in {"running", "failed"} and (
                incoming_owner or existing_owner or existing_owner_invalid
            ):
                if existing_owner_invalid or not incoming_owner or not existing_owner or not same_source_command:
                    return _owner_lost(existing, "company_public_web_source_run_owner_not_current")
                if incoming_owner["command_attempt"] < existing_owner["command_attempt"] or (
                    incoming_owner["command_attempt"] == existing_owner["command_attempt"]
                    and incoming_owner["lease_owner"] != existing_owner["lease_owner"]
                ):
                    return _owner_lost(existing, "company_public_web_source_run_owner_not_current")

                current_command = _lock_current_company_public_web_source_command(cursor, incoming_owner)
                if current_command is None:
                    return _owner_lost(existing, "company_public_web_source_command_claim_not_current")

                can_reclaim = incoming_owner["command_attempt"] > existing_owner["command_attempt"]
                if not can_reclaim:
                    return {
                        "created": False,
                        "reclaimed": False,
                        "outcome": "owner_busy",
                        "reason": "company_public_web_source_run_owned_by_current_command_attempt",
                        "run": existing,
                    }
                if can_reclaim:
                    reclaimed_metadata = _json_load_dict(row_payload.get("metadata_json"))
                    existing_revision, existing_revision_invalid = _company_public_web_source_projection_revision(
                        existing.get("metadata_json")
                    )
                    if existing_revision_invalid:
                        raise RuntimeError("company_public_web_source_projection_revision_invalid")
                    if existing_revision > 0:
                        reclaimed_metadata[_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY] = existing_revision
                    cursor.execute(
                        """
                        UPDATE company_public_web_asset_runs
                        SET status = 'running',
                            phase = %s,
                            discovered_assets_json = %s,
                            summary_json = %s,
                            artifact_root = %s,
                            started_at = %s,
                            completed_at = '',
                            last_error = '',
                            metadata_json = %s,
                            updated_at = GREATEST(updated_at, %s)
                        WHERE run_id = %s
                        RETURNING *
                        """,
                        tuple(
                            _normalize_postgres_payload(value)
                            for value in (
                                row_payload.get("phase") or "running",
                                row_payload.get("discovered_assets_json") or "[]",
                                row_payload.get("summary_json") or "{}",
                                row_payload.get("artifact_root") or "",
                                row_payload.get("started_at") or "",
                                _json_dump(reclaimed_metadata),
                                row_payload.get("updated_at") or "",
                                run_id,
                            )
                        ),
                    )
                    reclaimed = _fetch_one_dict_row(cursor, cursor.fetchone())
                    if reclaimed is None:
                        raise RuntimeError("company_public_web_source_run_reclaim_lost")
                    reclaimed["idempotency_key"] = idempotency_key
                    return {
                        "created": False,
                        "reclaimed": True,
                        "outcome": "reclaimed",
                        "reason": "company_public_web_source_run_reclaimed",
                        "run": reclaimed,
                    }

            return {
                "created": False,
                "reclaimed": False,
                "outcome": "joined",
                "reason": (
                    "company_public_web_source_run_completed"
                    if existing_status == "completed"
                    else "company_public_web_source_run_already_owned"
                ),
                "run": existing,
            }

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        lock_keys = sorted(
                            {
                                f"company_public_web_asset_run:idempotency:{idempotency_key}",
                                f"company_public_web_asset_run:run_id:{run_id}",
                            }
                        )
                        for lock_key in lock_keys:
                            self._acquire_transaction_lock(cursor, lock_key)
                        existing = _resolve_identity(_select_identity_rows(cursor))
                        if existing is not None:
                            resolved = _resolve_existing_owner(cursor, existing)
                            connection.commit()
                            return resolved

                        if (
                            incoming_owner
                            and _lock_current_company_public_web_source_command(
                                cursor,
                                incoming_owner,
                            )
                            is None
                        ):
                            connection.commit()
                            return {
                                "created": False,
                                "reclaimed": False,
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_command_claim_not_current",
                                "run": None,
                            }

                        cursor.execute(
                            f"INSERT INTO company_public_web_asset_runs ({quoted_columns}) "
                            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING RETURNING *",
                            tuple(
                                row_payload.get(column)
                                if column == "idempotency_key"
                                else _normalize_postgres_payload(row_payload.get(column))
                                for column in columns
                            ),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if inserted is not None:
                            inserted["idempotency_key"] = idempotency_key
                            connection.commit()
                            return {
                                "created": True,
                                "reclaimed": False,
                                "outcome": "created",
                                "reason": "company_public_web_source_run_created",
                                "run": inserted,
                            }

                        # A writer outside this lock protocol may have won a
                        # primary-key or effective-idempotency conflict.  Re-read
                        # both identities and accept only one exact binding.
                        existing = _resolve_identity(_select_identity_rows(cursor))
                        if existing is None:
                            raise RuntimeError("company_public_web_asset_run_insert_lost_without_owner")
                        resolved = _resolve_existing_owner(cursor, existing)
                    connection.commit()
                return resolved
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def get_company_public_web_asset_run_exact(
        self,
        *,
        run_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        """Read one effective source-run identity without Unicode trimming."""

        table_name = "company_public_web_asset_runs"
        if not self.should_prefer_read(table_name):
            return None
        normalized_run_id = str(run_id or "").strip()
        normalized_idempotency_key = normalize_company_public_web_asset_run_idempotency_key(idempotency_key)
        if not normalized_run_id and not normalized_idempotency_key:
            return None
        self.ensure_bootstrapped()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if normalized_run_id:
                    where_sql = "run_id = %s"
                    params = (normalized_run_id,)
                else:
                    where_sql = "btrim(idempotency_key, E' \\t\\n\\r\\f\\013') = %s"
                    params = (normalized_idempotency_key,)
                cursor.execute(
                    "SELECT *, '~' || idempotency_key || '~' AS _idempotency_key_fenced "
                    f"FROM company_public_web_asset_runs WHERE {where_sql} LIMIT 1",
                    params,
                )
                row = _fetch_one_dict_row(cursor, cursor.fetchone())
        if row is None:
            return None
        fenced_key = str(row.pop("_idempotency_key_fenced", "") or "")
        if len(fenced_key) >= 2:
            row["idempotency_key"] = normalize_company_public_web_asset_run_idempotency_key(fenced_key[1:-1])
        return row

    def reserve_company_public_web_source_projection_revision_if_owned(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        command_id: str = "",
        expected_attempt: int = 0,
        expected_lease_owner: str = "",
    ) -> dict[str, Any] | None:
        """Reserve one DB-owned logical projection revision on a running source run.

        Workflow-owned runs require the exact current physical source-command
        claim. Open-mode runs are permitted only when the durable run itself has
        no workflow owner. Once reserved, the revision remains attached to the
        logical run across a higher-attempt reclaim; sequence gaps are harmless.
        """

        table_name = "company_public_web_asset_runs"
        if not self.should_prefer_read(table_name):
            return None
        normalized_run_id = str(run_id or "").strip()
        normalized_idempotency_key = normalize_company_public_web_asset_run_idempotency_key(idempotency_key)
        normalized_command_id = str(command_id or "").strip()
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_attempt = 0 if isinstance(expected_attempt, bool) else int(expected_attempt or 0)
        owner_present = bool(normalized_command_id or normalized_lease_owner or normalized_attempt)
        if not normalized_run_id or not normalized_idempotency_key:
            raise ValueError("company_public_web_asset_run_identity_required")
        if owner_present and not (normalized_command_id and normalized_lease_owner and normalized_attempt > 0):
            raise ValueError("company_public_web_source_run_owner_invalid")
        incoming_owner = (
            {
                "command_id": normalized_command_id,
                "command_attempt": normalized_attempt,
                "lease_owner": normalized_lease_owner,
            }
            if owner_present
            else {}
        )
        self.ensure_bootstrapped()

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        for lock_key in sorted(
                            {
                                f"company_public_web_asset_run:idempotency:{normalized_idempotency_key}",
                                f"company_public_web_asset_run:run_id:{normalized_run_id}",
                            }
                        ):
                            self._acquire_transaction_lock(cursor, lock_key)
                        cursor.execute(
                            "SELECT *, '~' || idempotency_key || '~' AS _idempotency_key_fenced "
                            "FROM company_public_web_asset_runs "
                            "WHERE btrim(idempotency_key, E' \\t\\n\\r\\f\\013') = %s OR run_id = %s "
                            "ORDER BY run_id FOR UPDATE",
                            (normalized_idempotency_key, normalized_run_id),
                        )
                        rows = _fetch_all_dict_rows(cursor)
                        for row in rows:
                            fenced_key = str(row.pop("_idempotency_key_fenced", "") or "")
                            if len(fenced_key) >= 2:
                                row["idempotency_key"] = fenced_key[1:-1]
                        matching_rows = [
                            row
                            for row in rows
                            if str(row.get("run_id") or "").strip() == normalized_run_id
                            and normalize_company_public_web_asset_run_idempotency_key(row.get("idempotency_key"))
                            == normalized_idempotency_key
                        ]
                        if len(rows) != 1 or len(matching_rows) != 1:
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_identity_not_current",
                                "source_projection_revision": 0,
                                "run": matching_rows[0] if len(matching_rows) == 1 else None,
                            }
                        existing = matching_rows[0]
                        existing_owner, existing_owner_invalid = _company_public_web_source_run_owner(
                            existing.get("metadata_json")
                        )
                        if str(existing.get("status") or "").strip().lower() != "running":
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_not_running",
                                "source_projection_revision": 0,
                                "run": existing,
                            }
                        if incoming_owner:
                            if existing_owner_invalid or existing_owner != incoming_owner:
                                connection.commit()
                                return {
                                    "outcome": "owner_lost",
                                    "reason": "company_public_web_source_run_owner_not_current",
                                    "source_projection_revision": 0,
                                    "run": existing,
                                }
                            if _lock_current_company_public_web_source_command(cursor, incoming_owner) is None:
                                connection.commit()
                                return {
                                    "outcome": "owner_lost",
                                    "reason": "company_public_web_source_command_claim_not_current",
                                    "source_projection_revision": 0,
                                    "run": existing,
                                }
                        elif existing_owner or existing_owner_invalid:
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_owner_not_current",
                                "source_projection_revision": 0,
                                "run": existing,
                            }

                        revision, revision_invalid = _company_public_web_source_projection_revision(
                            existing.get("metadata_json")
                        )
                        if revision_invalid:
                            raise RuntimeError("company_public_web_source_projection_revision_invalid")
                        revision_was_reused = revision > 0
                        reserved = existing
                        if not revision_was_reused:
                            cursor.execute(
                                f"SELECT nextval('{_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_SEQUENCE}')"
                            )
                            revision = int(cursor.fetchone()[0])
                            if not 1 <= revision <= _COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_MAX:
                                raise RuntimeError("company_public_web_source_projection_revision_invalid")
                            metadata = _json_load_dict(existing.get("metadata_json"))
                            metadata[_COMPANY_PUBLIC_WEB_SOURCE_PROJECTION_REVISION_KEY] = revision
                            cursor.execute(
                                "UPDATE company_public_web_asset_runs "
                                "SET metadata_json = %s, updated_at = GREATEST(updated_at, %s) "
                                "WHERE run_id = %s AND status = 'running' RETURNING *",
                                (
                                    _json_dump(metadata),
                                    _utc_now_sql_timestamp(),
                                    normalized_run_id,
                                ),
                            )
                            reserved_row = _fetch_one_dict_row(cursor, cursor.fetchone())
                            if reserved_row is None:
                                raise RuntimeError("company_public_web_source_projection_revision_reservation_lost")
                            reserved_row["idempotency_key"] = normalized_idempotency_key
                            reserved = reserved_row
                    connection.commit()
                return {
                    "outcome": "reserved",
                    "reason": (
                        "company_public_web_source_projection_revision_reused"
                        if revision_was_reused
                        else "company_public_web_source_projection_revision_reserved"
                    ),
                    "source_projection_revision": revision,
                    "run": reserved,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def finalize_company_public_web_asset_run_if_owned(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Finalize a source run only for its current physical command claim."""

        table_name = "company_public_web_asset_runs"
        if not self.should_prefer_read(table_name):
            return None
        raw_payload = dict(payload or {})
        raw_idempotency_key = normalize_company_public_web_asset_run_idempotency_key(raw_payload.get("idempotency_key"))
        row_payload = _normalize_postgres_row_payload(raw_payload)
        run_id = str(row_payload.get("run_id") or "").strip()
        idempotency_key = raw_idempotency_key
        terminal_status = str(row_payload.get("status") or "").strip().lower()
        incoming_owner, incoming_owner_invalid = _company_public_web_source_run_owner(row_payload.get("metadata_json"))
        if not run_id or not idempotency_key:
            raise ValueError("company_public_web_asset_run_identity_required")
        if incoming_owner_invalid or not incoming_owner:
            raise ValueError("company_public_web_source_run_owner_required")
        if terminal_status not in {"completed", "failed"}:
            raise ValueError("company_public_web_source_run_terminal_status_required")
        row_payload["run_id"] = run_id
        row_payload["idempotency_key"] = idempotency_key
        self.ensure_bootstrapped()

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        for lock_key in sorted(
                            {
                                f"company_public_web_asset_run:idempotency:{idempotency_key}",
                                f"company_public_web_asset_run:run_id:{run_id}",
                            }
                        ):
                            self._acquire_transaction_lock(cursor, lock_key)
                        cursor.execute(
                            "SELECT *, '~' || idempotency_key || '~' AS _idempotency_key_fenced "
                            "FROM company_public_web_asset_runs "
                            "WHERE btrim(idempotency_key, E' \\t\\n\\r\\f\\013') = %s OR run_id = %s "
                            "ORDER BY run_id FOR UPDATE",
                            (idempotency_key, run_id),
                        )
                        rows = _fetch_all_dict_rows(cursor)
                        for row in rows:
                            fenced_key = str(row.pop("_idempotency_key_fenced", "") or "")
                            if len(fenced_key) >= 2:
                                row["idempotency_key"] = fenced_key[1:-1]
                        matching_rows = [
                            row
                            for row in rows
                            if str(row.get("run_id") or "").strip() == run_id
                            and normalize_company_public_web_asset_run_idempotency_key(row.get("idempotency_key"))
                            == idempotency_key
                        ]
                        if len(rows) != 1 or len(matching_rows) != 1:
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_identity_not_current",
                                "run": matching_rows[0] if len(matching_rows) == 1 else None,
                            }
                        existing = matching_rows[0]
                        existing_owner, _ = _company_public_web_source_run_owner(existing.get("metadata_json"))
                        if (
                            existing_owner != incoming_owner
                            or str(existing.get("status") or "").strip().lower() != "running"
                        ):
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_owner_not_current",
                                "run": existing,
                            }
                        if _lock_current_company_public_web_source_command(cursor, incoming_owner) is None:
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_command_claim_not_current",
                                "run": existing,
                            }
                        cursor.execute(
                            """
                            UPDATE company_public_web_asset_runs
                            SET status = %s,
                                phase = %s,
                                discovered_assets_json = %s,
                                summary_json = %s,
                                artifact_root = %s,
                                completed_at = %s,
                                last_error = %s,
                                metadata_json = %s,
                                updated_at = GREATEST(updated_at, %s)
                            WHERE run_id = %s
                              AND status = 'running'
                            RETURNING *
                            """,
                            tuple(
                                _normalize_postgres_payload(value)
                                for value in (
                                    terminal_status,
                                    row_payload.get("phase") or terminal_status,
                                    row_payload.get("discovered_assets_json") or "[]",
                                    row_payload.get("summary_json") or "{}",
                                    row_payload.get("artifact_root") or "",
                                    row_payload.get("completed_at") or "",
                                    row_payload.get("last_error") or "",
                                    row_payload.get("metadata_json") or "{}",
                                    row_payload.get("updated_at") or "",
                                    run_id,
                                )
                            ),
                        )
                        finalized = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if finalized is None:
                            connection.commit()
                            return {
                                "outcome": "owner_lost",
                                "reason": "company_public_web_source_run_owner_not_current",
                                "run": existing,
                            }
                        finalized["idempotency_key"] = idempotency_key
                    connection.commit()
                return {
                    "outcome": "finalized",
                    "reason": f"company_public_web_source_run_{terminal_status}",
                    "run": finalized,
                }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_company_public_web_asset_atomic_source_runs(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Upsert one mutable source asset without losing concurrent run lineage."""

        table_name = "company_public_web_assets"
        if not self.should_prefer_read(table_name):
            return None
        row_payload = _normalize_postgres_row_payload(dict(payload or {}))
        asset_id = str(row_payload.get("asset_id") or "").strip()
        if not asset_id:
            raise ValueError("company_public_web_asset_id_required")
        source_run_ids = sorted(
            {
                str(source_run_id or "").strip()
                for source_run_id in _json_load_list(row_payload.get("source_run_ids_json"))
                if str(source_run_id or "").strip()
            }
        )
        if not source_run_ids:
            raise ValueError("company_public_web_asset_source_run_id_required")
        row_payload["source_run_ids_json"] = _json_dump(source_run_ids)
        self.ensure_bootstrapped()

        columns = [column for column in row_payload if str(column or "").strip()]
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        incoming_projection_wins = _company_public_web_projection_incoming_wins_sql(
            incoming_metadata_expression="EXCLUDED.metadata_json::jsonb",
            existing_metadata_expression="company_public_web_assets.metadata_json::jsonb",
            incoming_run_id_expression="EXCLUDED.latest_run_id",
            existing_run_id_expression="company_public_web_assets.latest_run_id",
        )
        update_assignments: list[str] = []
        for column in columns:
            if column == "asset_id":
                continue
            quoted_column = _quote_identifier(column)
            if column == "source_run_ids_json":
                update_assignments.append(
                    f"""
                    {quoted_column} = (
                        SELECT COALESCE(
                            jsonb_agg(merged_source_run_id ORDER BY merged_source_run_id),
                            '[]'::jsonb
                        )::text
                        FROM (
                            SELECT DISTINCT jsonb_array_elements_text(
                                COALESCE(
                                    NULLIF(company_public_web_assets.source_run_ids_json, ''),
                                    '[]'
                                )::jsonb
                            ) AS merged_source_run_id
                            UNION
                            SELECT DISTINCT jsonb_array_elements_text(
                                COALESCE(NULLIF(EXCLUDED.source_run_ids_json, ''), '[]')::jsonb
                            ) AS merged_source_run_id
                        ) company_public_web_source_run_union
                        WHERE merged_source_run_id <> ''
                    )
                    """.strip()
                )
            elif column == "created_at":
                update_assignments.append(
                    f"{quoted_column} = COALESCE(NULLIF(company_public_web_assets.{quoted_column}, ''), "
                    f"EXCLUDED.{quoted_column})"
                )
            elif column == "updated_at":
                update_assignments.append(
                    f"{quoted_column} = GREATEST(company_public_web_assets.{quoted_column}, EXCLUDED.{quoted_column})"
                )
            else:
                update_assignments.append(
                    f"{quoted_column} = CASE WHEN {incoming_projection_wins} "
                    f"THEN EXCLUDED.{quoted_column} ELSE company_public_web_assets.{quoted_column} END"
                )

        sql = (
            f"INSERT INTO company_public_web_assets ({quoted_columns}) VALUES ({placeholders}) "
            "ON CONFLICT (asset_id) DO UPDATE SET " + ", ".join(update_assignments) + " RETURNING *"
        )
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            sql,
                            tuple(_normalize_postgres_payload(row_payload.get(column)) for column in columns),
                        )
                        written = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                return written
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def close_company_public_web_owner_lost_activity_attempt(
        self,
        *,
        table_name: str = "workflow_activity_attempts",
        command_id: str,
        expected_command_attempt: int,
        expected_lease_owner: str,
        expected_lease_expires_at: str,
        activity_run_id: str,
        activity_idempotency_key: str,
        attempt_id: str,
        attempt_idempotency_key: str,
        workspace_id: str = "default",
        reason: str = "company_public_web_source_run_owner_lost",
        terminalize_exact_command: bool = False,
        terminalize_exhausted_command: bool = False,
    ) -> dict[str, Any] | None:
        """Close a D1m owner-loss attempt without changing a newer physical owner.

        The ActivityAttempt is generation-specific and can be failed by exact
        attempt/lease CAS. The ActivityRun is shared across command attempts;
        it moves to ``retry_wait`` only while its current metadata still names
        the same lease owner. A newer attempt's ActivityRun therefore remains
        untouched even when the old execution reports owner loss later.

        ``terminalize_exact_command`` is reserved for deterministic, non-retryable
        owner-loss outcomes such as a typed source command colliding with a
        revisionless completed brownfield run. In that mode the current command,
        ActivityRun, and ActivityAttempt are failed in this one transaction only
        when the command still names the exact attempt/lease owner. If a newer
        command attempt already won, this method falls back to the stale-attempt
        closure above and never terminalizes the newer command or ActivityRun.

        ``terminalize_exhausted_command`` is the D1m-only final-attempt crash
        closure. It requires the same exact physical claim with an expired lease
        and ``attempt >= max_attempts``; command, ActivityRun, and ActivityAttempt
        then fail together. Missing Activity rows are allowed so a crash between
        command-running and Activity creation can still terminalize the command.
        """

        if _normalize_postgres_identifier(table_name) != "workflow_activity_attempts":
            raise ValueError(
                "close_company_public_web_owner_lost_activity_attempt requires table_name=workflow_activity_attempts"
            )
        if not self.should_prefer_read("workflow_activity_attempts") or not self.should_prefer_read(
            "workflow_activity_runs"
        ):
            return None
        if terminalize_exact_command and terminalize_exhausted_command:
            raise ValueError("company Public Web terminal closure modes are mutually exclusive")
        terminalize_command = bool(terminalize_exact_command or terminalize_exhausted_command)
        if terminalize_command and not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        normalized_attempt = max(0, int(expected_command_attempt or 0))
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_lease_expires_at = str(expected_lease_expires_at or "").strip()
        normalized_activity_run_id = str(activity_run_id or "").strip()
        normalized_activity_key = str(activity_idempotency_key or "").strip()
        normalized_attempt_id = str(attempt_id or "").strip()
        normalized_attempt_key = str(attempt_idempotency_key or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_reason = str(reason or "company_public_web_source_run_owner_lost").strip()
        activity_identity_values = (
            normalized_activity_run_id,
            normalized_activity_key,
            normalized_attempt_id,
            normalized_attempt_key,
        )
        activity_identity_complete = all(activity_identity_values)
        activity_identity_absent = not any(activity_identity_values)
        exhausted_attempt_identities: dict[int, tuple[str, str]] = {}
        if (
            not normalized_command_id
            or normalized_attempt <= 0
            or not normalized_lease_owner
            or (terminalize_command and not normalized_lease_expires_at)
            or (not terminalize_exhausted_command and not activity_identity_complete)
            or (terminalize_exhausted_command and not (activity_identity_complete or activity_identity_absent))
        ):
            return {
                "outcome": "invalid",
                "reason": "company_public_web_owner_lost_activity_identity_required",
                "attempt_closed": False,
                "activity_closed": False,
                "attempt": None,
                "activity": None,
            }
        if terminalize_exhausted_command:
            expected_activity_run_id, expected_activity_key = _company_public_web_source_activity_identity(
                normalized_command_id
            )
            exhausted_attempt_identities = {
                attempt_number: _company_public_web_source_attempt_identity(normalized_command_id, attempt_number)
                for attempt_number in range(1, normalized_attempt + 1)
            }
            expected_attempt_id, expected_attempt_key = exhausted_attempt_identities[normalized_attempt]
            expected_identity = (
                expected_activity_run_id,
                expected_activity_key,
                expected_attempt_id,
                expected_attempt_key,
            )
            if activity_identity_complete and activity_identity_values != expected_identity:
                return {
                    "outcome": "conflict",
                    "reason": "company_public_web_exhausted_activity_identity_conflict",
                    "attempt_closed": False,
                    "activity_closed": False,
                    "attempt": None,
                    "activity": None,
                }
            normalized_activity_run_id = expected_activity_run_id
            normalized_activity_key = expected_activity_key
            normalized_attempt_id = expected_attempt_id
            normalized_attempt_key = expected_attempt_key
            activity_identity_complete = True
        self._ensure_runtime_coordination_schema()

        retry_attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        # Match the generic workflow-runtime upsert lock
                        # identities so a newer ActivityRun start cannot race
                        # the shared-row ownership check below.
                        lock_key_values: set[str] = set()
                        if activity_identity_complete:
                            lock_key_values.update(
                                {
                                    f"workflow_runtime:workflow_activity_runs:id:{normalized_activity_run_id}",
                                    (
                                        "workflow_runtime:workflow_activity_runs:idempotency:"
                                        f"{normalized_workspace_id}:{normalized_activity_key}"
                                    ),
                                }
                            )
                            attempt_lock_identities = (
                                exhausted_attempt_identities.values()
                                if terminalize_exhausted_command
                                else [(normalized_attempt_id, normalized_attempt_key)]
                            )
                            for attempt_identity_id, attempt_identity_key in attempt_lock_identities:
                                lock_key_values.update(
                                    {
                                        (f"workflow_runtime:workflow_activity_attempts:id:{attempt_identity_id}"),
                                        (
                                            "workflow_runtime:workflow_activity_attempts:idempotency:"
                                            f"{normalized_workspace_id}:{attempt_identity_key}"
                                        ),
                                    }
                                )
                        lock_keys = sorted(lock_key_values)
                        for lock_key in lock_keys:
                            self._acquire_transaction_lock(cursor, lock_key)

                        current_attempt = None
                        current_activity = None
                        if terminalize_exhausted_command:
                            deterministic_attempt_ids = [
                                identity[0] for identity in exhausted_attempt_identities.values()
                            ]
                            deterministic_attempt_keys = [
                                identity[1] for identity in exhausted_attempt_identities.values()
                            ]
                            attempt_id_placeholders = ", ".join(["%s"] * len(deterministic_attempt_ids))
                            attempt_key_placeholders = ", ".join(["%s"] * len(deterministic_attempt_keys))
                            cursor.execute(
                                f"""
                                SELECT * FROM workflow_activity_attempts
                                WHERE command_id = %s
                                   OR attempt_id IN ({attempt_id_placeholders})
                                   OR (workspace_id = %s AND idempotency_key IN ({attempt_key_placeholders}))
                                ORDER BY attempt_number, attempt_id
                                FOR UPDATE
                                """,
                                (
                                    normalized_command_id,
                                    *deterministic_attempt_ids,
                                    normalized_workspace_id,
                                    *deterministic_attempt_keys,
                                ),
                            )
                            all_generation_attempts = _fetch_all_dict_rows(cursor)
                            attempt_terminal_statuses = set(
                                _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["workflow_activity_attempts"][
                                    "terminal_statuses"
                                ]
                            )

                            def _is_expected_attempt_identity(item: dict[str, Any]) -> bool:
                                item_attempt_number = int(item.get("attempt_number") or 0)
                                expected_attempt_identity = exhausted_attempt_identities.get(item_attempt_number)
                                if expected_attempt_identity is None:
                                    return False
                                expected_attempt_identity_id, expected_attempt_identity_key = expected_attempt_identity
                                item_metadata = _json_load_dict(item.get("metadata_json"))
                                item_lease_owner = str(item_metadata.get("lease_owner") or "").strip()
                                return bool(
                                    str(item.get("attempt_id") or "").strip() == expected_attempt_identity_id
                                    and (str(item.get("workspace_id") or "default").strip() or "default")
                                    == normalized_workspace_id
                                    and str(item.get("activity_run_id") or "").strip() == normalized_activity_run_id
                                    and str(item.get("command_id") or "").strip() == normalized_command_id
                                    and str(item.get("idempotency_key") or "").strip() == expected_attempt_identity_key
                                    and item_lease_owner
                                    and (
                                        item_attempt_number < normalized_attempt
                                        or item_lease_owner == normalized_lease_owner
                                    )
                                )

                            def _is_expected_resume_attempt_identity(item: dict[str, Any]) -> bool:
                                item_attempt_number = int(item.get("attempt_number") or 0)
                                if item_attempt_number <= 0:
                                    return False
                                expected_resume_attempt_id, expected_resume_attempt_key = (
                                    _company_public_web_source_resume_attempt_identity(
                                        normalized_command_id,
                                        item_attempt_number,
                                    )
                                )
                                item_metadata = _json_load_dict(item.get("metadata_json"))
                                item_input = _json_load_dict(item.get("input_json"))
                                item_output = _json_load_dict(item.get("output_json"))
                                item_input_force = item_input.get("force")
                                item_output_force = item_output.get("force")
                                return bool(
                                    item_attempt_number <= normalized_attempt
                                    and str(item.get("status") or "").strip() == "succeeded"
                                    and str(item.get("attempt_id") or "").strip() == expected_resume_attempt_id
                                    and (str(item.get("workspace_id") or "default").strip() or "default")
                                    == normalized_workspace_id
                                    and str(item.get("activity_run_id") or "").strip() == normalized_activity_run_id
                                    and str(item.get("command_id") or "").strip() == normalized_command_id
                                    and str(item.get("provider_request_ref") or "").strip() == normalized_command_id
                                    and str(item.get("idempotency_key") or "").strip() == expected_resume_attempt_key
                                    and str(item_metadata.get("lease_owner") or "").strip()
                                    and str(item_input.get("control_action") or "").strip() == "resume"
                                    and str(item_input.get("phase_command_type") or "").strip()
                                    == "company.public_web.source.collect"
                                    and str(item_output.get("control_action") or "").strip() == "resume"
                                    and str(item_output.get("command_id") or "").strip() == normalized_command_id
                                    and str(item_output.get("command_type") or "").strip()
                                    == "company.public_web.source.collect"
                                    and str(item_input.get("target_company") or "").strip()
                                    == str(item_output.get("target_company") or "").strip()
                                    and str(item_input.get("company_key") or "").strip()
                                    == str(item_output.get("company_key") or "").strip()
                                    and isinstance(item_input_force, bool)
                                    and isinstance(item_output_force, bool)
                                    and item_input_force is item_output_force
                                    and str(item_output.get("reason") or "").strip()
                                )

                            generation_attempts = [
                                item for item in all_generation_attempts if _is_expected_attempt_identity(item)
                            ]
                            resume_control_attempts = [
                                item for item in all_generation_attempts if _is_expected_resume_attempt_identity(item)
                            ]

                            def _is_current_owner_loss_terminal(item: dict[str, Any]) -> bool:
                                item_error = _json_load_dict(item.get("error_json"))
                                item_output = _json_load_dict(item.get("output_json"))
                                item_metadata = _json_load_dict(item.get("metadata_json"))
                                owner_loss_reason = str(item_error.get("reason") or "").strip()
                                return bool(
                                    int(item.get("attempt_number") or 0) == normalized_attempt
                                    and str(item.get("status") or "").strip() == "failed"
                                    and owner_loss_reason
                                    and item_error.get("owner_lost") is True
                                    and item_error.get("deterministic_terminal_failure") is False
                                    and str(item_output.get("status") or "").strip() == "skipped"
                                    and str(item_output.get("reason") or "").strip() == owner_loss_reason
                                    and item_metadata.get("owner_lost") is True
                                    and str(item_metadata.get("owner_lost_reason") or "").strip() == owner_loss_reason
                                )

                            unexpected_generation_attempts = [
                                item
                                for item in all_generation_attempts
                                if not _is_expected_attempt_identity(item)
                                and not _is_expected_resume_attempt_identity(item)
                            ]
                            alternate_nonterminal_attempts = [
                                item
                                for item in all_generation_attempts
                                if str(item.get("status") or "").strip() not in attempt_terminal_statuses
                                and not _is_expected_attempt_identity(item)
                            ]
                            attempt_identity_collisions = [
                                item
                                for item in all_generation_attempts
                                if (
                                    str(item.get("attempt_id") or "").strip() in deterministic_attempt_ids
                                    or (
                                        (str(item.get("workspace_id") or "default").strip() or "default")
                                        == normalized_workspace_id
                                        and str(item.get("idempotency_key") or "").strip() in deterministic_attempt_keys
                                    )
                                )
                                and not _is_expected_attempt_identity(item)
                            ]
                            cursor.execute(
                                """
                                SELECT * FROM workflow_activity_runs
                                WHERE command_id = %s
                                   OR activity_run_id = %s
                                   OR (workspace_id = %s AND idempotency_key = %s)
                                ORDER BY activity_run_id
                                FOR UPDATE
                                """,
                                (
                                    normalized_command_id,
                                    normalized_activity_run_id,
                                    normalized_workspace_id,
                                    normalized_activity_key,
                                ),
                            )
                            all_generation_activities = _fetch_all_dict_rows(cursor)
                            activity_terminal_statuses = set(
                                _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["workflow_activity_runs"]["terminal_statuses"]
                            )

                            def _is_expected_activity_identity(item: dict[str, Any]) -> bool:
                                item_metadata = _json_load_dict(item.get("metadata_json"))
                                item_lease_owner = str(item_metadata.get("lease_owner") or "").strip()
                                return bool(
                                    str(item.get("activity_run_id") or "").strip() == normalized_activity_run_id
                                    and (str(item.get("workspace_id") or "default").strip() or "default")
                                    == normalized_workspace_id
                                    and str(item.get("command_id") or "").strip() == normalized_command_id
                                    and str(item.get("idempotency_key") or "").strip() == normalized_activity_key
                                    and item_lease_owner
                                )

                            generation_activities = [
                                item for item in all_generation_activities if _is_expected_activity_identity(item)
                            ]
                            alternate_nonterminal_activities = [
                                item
                                for item in all_generation_activities
                                if str(item.get("status") or "").strip() not in activity_terminal_statuses
                                and not _is_expected_activity_identity(item)
                            ]
                            activity_identity_collisions = [
                                item
                                for item in all_generation_activities
                                if (
                                    str(item.get("activity_run_id") or "").strip() == normalized_activity_run_id
                                    or (
                                        (str(item.get("workspace_id") or "default").strip() or "default")
                                        == normalized_workspace_id
                                        and str(item.get("idempotency_key") or "").strip() == normalized_activity_key
                                    )
                                )
                                and not _is_expected_activity_identity(item)
                            ]
                            activity_identity_conflict = bool(
                                len(generation_activities) > 1
                                or unexpected_generation_attempts
                                or attempt_identity_collisions
                                or activity_identity_collisions
                                or alternate_nonterminal_attempts
                                or alternate_nonterminal_activities
                                or any(
                                    str(item.get("status") or "").strip() != "running"
                                    and str(item.get("status") or "").strip() not in attempt_terminal_statuses
                                    for item in generation_attempts
                                )
                                or any(
                                    int(item.get("attempt_number") or 0) == normalized_attempt
                                    and str(item.get("status") or "").strip() in attempt_terminal_statuses
                                    and not _is_current_owner_loss_terminal(item)
                                    for item in generation_attempts
                                )
                                or (
                                    generation_activities
                                    and str(generation_activities[0].get("status") or "").strip()
                                    not in {"planned", "queued", "running", "retry_wait", "failed"}
                                )
                                or (
                                    generation_activities
                                    and any(
                                        str(item.get("activity_run_id") or "").strip()
                                        != str(generation_activities[0].get("activity_run_id") or "").strip()
                                        for item in generation_attempts
                                    )
                                )
                                or (
                                    generation_activities
                                    and any(
                                        (str(item.get("workspace_id") or "default").strip() or "default")
                                        != (
                                            str(generation_activities[0].get("workspace_id") or "default").strip()
                                            or "default"
                                        )
                                        for item in generation_attempts
                                    )
                                )
                            )
                            if activity_identity_conflict:
                                connection.rollback()
                                return {
                                    "outcome": "conflict",
                                    "reason": "company_public_web_exhausted_activity_identity_conflict",
                                    "attempt_closed": False,
                                    "activity_closed": False,
                                    "command_closed": False,
                                    "attempt": next(
                                        (
                                            item
                                            for item in generation_attempts
                                            if int(item.get("attempt_number") or 0) == normalized_attempt
                                        ),
                                        None,
                                    ),
                                    "activity": generation_activities[0] if len(generation_activities) == 1 else None,
                                    "command": None,
                                }
                            current_attempt = next(
                                (
                                    item
                                    for item in generation_attempts
                                    if int(item.get("attempt_number") or 0) == normalized_attempt
                                ),
                                None,
                            )
                            current_activity = generation_activities[0] if generation_activities else None
                        elif activity_identity_complete:
                            cursor.execute(
                                "SELECT * FROM workflow_activity_attempts WHERE attempt_id = %s FOR UPDATE",
                                (normalized_attempt_id,),
                            )
                            current_attempt = _fetch_one_dict_row(cursor, cursor.fetchone())
                            cursor.execute(
                                "SELECT * FROM workflow_activity_runs WHERE activity_run_id = %s FOR UPDATE",
                                (normalized_activity_run_id,),
                            )
                            current_activity = _fetch_one_dict_row(cursor, cursor.fetchone())
                        current_command = None
                        exact_command_current = False
                        exact_command_guard: dict[str, Any] = {}
                        if terminalize_command:
                            cursor.execute(
                                "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
                                (normalized_command_id,),
                            )
                            current_command = _fetch_one_dict_row(cursor, cursor.fetchone())
                            exact_command_guard = {
                                "command_found": current_command is not None,
                                "command_type_matches": bool(
                                    current_command
                                    and str(current_command.get("command_type") or "").strip()
                                    == "company.public_web.source.collect"
                                ),
                                "owner_matches": bool(
                                    current_command
                                    and str(current_command.get("owner") or "").strip() == "company_public_web_owner"
                                ),
                                "status_matches": bool(
                                    current_command and str(current_command.get("status") or "").strip() == "running"
                                ),
                                "attempt_matches": bool(
                                    current_command and int(current_command.get("attempt") or 0) == normalized_attempt
                                ),
                                "lease_owner_matches": bool(
                                    current_command
                                    and str(current_command.get("lease_owner") or "").strip() == normalized_lease_owner
                                ),
                                "lease_expires_at_matches": bool(
                                    current_command
                                    and str(current_command.get("lease_expires_at") or "").strip()
                                    == normalized_lease_expires_at
                                ),
                            }
                            if terminalize_exhausted_command:
                                exact_command_guard.update(
                                    {
                                        "attempts_exhausted": bool(
                                            current_command
                                            and int(current_command.get("attempt") or 0)
                                            >= max(1, int(current_command.get("max_attempts") or 1))
                                        ),
                                    }
                                )
                            else:
                                exact_command_guard["lease_unexpired"] = bool(
                                    current_command
                                    and not _timestamp_is_expired(current_command.get("lease_expires_at"))
                                )
                            exact_command_current = all(exact_command_guard.values())

                        if terminalize_exhausted_command and current_command is not None:
                            command_payload = _json_load_dict(current_command.get("payload_json"))
                            command_options = _json_load_dict(command_payload.get("options"))
                            expected_workflow_run_id = str(current_command.get("workflow_run_id") or "").strip()
                            expected_operation_run_id = str(
                                current_command.get("operation_id") or command_payload.get("operation_run_id") or ""
                            ).strip()
                            expected_provider = str(
                                command_options.get("collection_mode")
                                or command_payload.get("collection_mode")
                                or "seed_url_only"
                            ).strip()
                            expected_target_company = str(
                                command_payload.get("target_company") or command_payload.get("company") or ""
                            ).strip()
                            expected_company_key = str(command_payload.get("company_key") or "").strip()
                            semantic_identity_conflict = bool(
                                current_activity
                                and (
                                    str(current_activity.get("workflow_run_id") or "").strip()
                                    != expected_workflow_run_id
                                    or str(current_activity.get("operation_run_id") or "").strip()
                                    != expected_operation_run_id
                                    or str(current_activity.get("acquisition_run_id") or "").strip()
                                    or str(current_activity.get("parent_activity_run_id") or "").strip()
                                    or str(current_activity.get("activity_type") or "").strip()
                                    != "company.public_web.source.collect"
                                    or str(current_activity.get("owner") or "").strip() != "company_public_web_owner"
                                )
                                or any(
                                    str(item.get("workflow_run_id") or "").strip() != expected_workflow_run_id
                                    or str(item.get("provider") or "").strip() != expected_provider
                                    or str(item.get("provider_request_ref") or "").strip() != normalized_command_id
                                    for item in [*generation_attempts, *resume_control_attempts]
                                )
                                or any(
                                    str(_json_load_dict(item.get("input_json")).get("target_company") or "").strip()
                                    != expected_target_company
                                    or str(_json_load_dict(item.get("output_json")).get("target_company") or "").strip()
                                    != expected_target_company
                                    or str(_json_load_dict(item.get("input_json")).get("company_key") or "").strip()
                                    != expected_company_key
                                    or str(_json_load_dict(item.get("output_json")).get("company_key") or "").strip()
                                    != expected_company_key
                                    for item in resume_control_attempts
                                )
                            )
                            if semantic_identity_conflict:
                                connection.rollback()
                                return {
                                    "outcome": "conflict",
                                    "reason": "company_public_web_exhausted_activity_semantic_identity_conflict",
                                    "attempt_closed": False,
                                    "activity_closed": False,
                                    "command_closed": False,
                                    "attempt": current_attempt,
                                    "activity": current_activity,
                                    "command": current_command,
                                }

                        # Terminalize the exact physical claim before mutating
                        # either Activity row.  The Python guard above is only
                        # diagnostic: a lease can expire after it is computed,
                        # so the SQL CAS is the sole authority for deterministic
                        # terminal closure.  Holding the command row lock keeps
                        # a takeover behind this decision until commit.
                        command_closed = False
                        if terminalize_command and exact_command_current and current_command is not None:
                            now = _utc_now_sql_timestamp()
                            command_result = {
                                **_json_load_dict(current_command.get("result_json")),
                                "status": "invalid",
                                "reason": normalized_reason,
                                "operation_completion_deferred": False,
                                "downstream_command_required": False,
                                "downstream_command_count": 0,
                                "downstream_command_ids": [],
                            }
                            lease_terminal_predicate = (
                                "(NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') <= clock_timestamp()"
                                if terminalize_exhausted_command
                                else "(NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()"
                            )
                            attempt_exhaustion_predicate = (
                                "AND attempt >= max_attempts" if terminalize_exhausted_command else ""
                            )
                            cursor.execute(
                                f"""
                                UPDATE workflow_commands
                                SET status = 'failed_terminal',
                                    lease_owner = '',
                                    lease_expires_at = '',
                                    heartbeat_at = %s,
                                    not_before_at = '',
                                    last_error = %s,
                                    result_json = %s,
                                    updated_at = %s
                                WHERE command_id = %s
                                  AND command_type = 'company.public_web.source.collect'
                                  AND owner = 'company_public_web_owner'
                                  AND status = 'running'
                                  AND attempt = %s
                                  AND lease_owner = %s
                                  AND lease_expires_at = %s
                                  {attempt_exhaustion_predicate}
                                  AND {lease_terminal_predicate}
                                RETURNING *
                                """,
                                (
                                    now,
                                    normalized_reason,
                                    _json_dump(command_result),
                                    now,
                                    normalized_command_id,
                                    normalized_attempt,
                                    normalized_lease_owner,
                                    normalized_lease_expires_at,
                                ),
                            )
                            closed_command = _fetch_one_dict_row(cursor, cursor.fetchone())
                            if closed_command is not None:
                                current_command = closed_command
                                command_closed = True

                        attempt_closed = False
                        attempts_closed_count = 0
                        reported_attempt = current_attempt
                        attempt_rows = (
                            generation_attempts
                            if terminalize_exhausted_command
                            else [current_attempt]
                            if current_attempt is not None
                            else []
                        )
                        for attempt_row in attempt_rows:
                            attempt_number = int(attempt_row.get("attempt_number") or 0)
                            expected_attempt_identity = (
                                exhausted_attempt_identities.get(attempt_number)
                                if terminalize_exhausted_command
                                else (normalized_attempt_id, normalized_attempt_key)
                            )
                            if expected_attempt_identity is None:
                                continue
                            expected_attempt_identity_id, expected_attempt_identity_key = expected_attempt_identity
                            attempt_metadata = _json_load_dict(attempt_row.get("metadata_json"))
                            attempt_lease_owner = str(attempt_metadata.get("lease_owner") or "").strip()
                            attempt_matches = bool(
                                (str(attempt_row.get("workspace_id") or "default").strip() or "default")
                                == normalized_workspace_id
                                and str(attempt_row.get("activity_run_id") or "").strip() == normalized_activity_run_id
                                and str(attempt_row.get("command_id") or "").strip() == normalized_command_id
                                and attempt_number > 0
                                and str(attempt_row.get("attempt_id") or "").strip() == expected_attempt_identity_id
                                and str(attempt_row.get("idempotency_key") or "").strip()
                                == expected_attempt_identity_key
                                and attempt_lease_owner
                                and (terminalize_exhausted_command or attempt_lease_owner == normalized_lease_owner)
                            )
                            if (
                                attempt_matches
                                and (not terminalize_exhausted_command or command_closed)
                                and str(attempt_row.get("status") or "").strip() == "running"
                            ):
                                now = _utc_now_sql_timestamp()
                                terminal_failure = bool(terminalize_command and command_closed)
                                cursor.execute(
                                    """
                                    UPDATE workflow_activity_attempts
                                    SET status = 'failed',
                                        completed_at = %s,
                                        error_json = %s,
                                        output_json = %s,
                                        metadata_json = %s,
                                        updated_at = %s
                                    WHERE attempt_id = %s
                                      AND status = 'running'
                                      AND attempt_number = %s
                                    RETURNING *
                                    """,
                                    (
                                        now,
                                        _json_dump(
                                            {
                                                **_json_load_dict(attempt_row.get("error_json")),
                                                "reason": normalized_reason,
                                                "owner_lost": not terminal_failure,
                                                "deterministic_terminal_failure": terminal_failure,
                                            }
                                        ),
                                        _json_dump(
                                            {
                                                **_json_load_dict(attempt_row.get("output_json")),
                                                "status": "invalid" if terminal_failure else "skipped",
                                                "reason": normalized_reason,
                                            }
                                        ),
                                        _json_dump(
                                            {
                                                **attempt_metadata,
                                                "owner_lost": not terminal_failure,
                                                (
                                                    "terminal_failure_reason"
                                                    if terminal_failure
                                                    else "owner_lost_reason"
                                                ): normalized_reason,
                                            }
                                        ),
                                        now,
                                        expected_attempt_identity_id,
                                        attempt_number,
                                    ),
                                )
                                closed_attempt = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if closed_attempt is not None:
                                    attempt_closed = True
                                    attempts_closed_count += 1
                                    reported_attempt = closed_attempt
                                    if attempt_number == normalized_attempt:
                                        current_attempt = closed_attempt

                        activity_closed = False
                        if current_activity is not None:
                            activity_metadata = _json_load_dict(current_activity.get("metadata_json"))
                            current_activity_status = str(current_activity.get("status") or "").strip()
                            activity_matches = bool(
                                str(current_activity.get("workspace_id") or "default").strip()
                                == normalized_workspace_id
                                and str(current_activity.get("command_id") or "").strip() == normalized_command_id
                                and str(current_activity.get("idempotency_key") or "").strip()
                                == normalized_activity_key
                                and (
                                    str(activity_metadata.get("lease_owner") or "").strip() == normalized_lease_owner
                                    or terminalize_exhausted_command
                                )
                            )
                            if (
                                activity_matches
                                and (not terminalize_command or command_closed)
                                and current_activity_status in {"planned", "queued", "running", "retry_wait"}
                            ):
                                now = _utc_now_sql_timestamp()
                                terminal_failure = bool(terminalize_command and command_closed)
                                cursor.execute(
                                    """
                                    UPDATE workflow_activity_runs
                                    SET status = %s,
                                        phase = %s,
                                        output_json = %s,
                                        metadata_json = %s,
                                        updated_at = %s
                                    WHERE activity_run_id = %s
                                      AND status IN ('planned', 'queued', 'running', 'retry_wait')
                                    RETURNING *
                                    """,
                                    (
                                        "failed" if terminal_failure else "retry_wait",
                                        normalized_reason,
                                        _json_dump(
                                            {
                                                **_json_load_dict(current_activity.get("output_json")),
                                                "status": "invalid" if terminal_failure else "skipped",
                                                "reason": normalized_reason,
                                                "latest_attempt_id": normalized_attempt_id,
                                            }
                                        ),
                                        _json_dump(
                                            {
                                                **activity_metadata,
                                                "latest_attempt_id": normalized_attempt_id,
                                                "owner_lost": not terminal_failure,
                                                (
                                                    "terminal_failure_reason"
                                                    if terminal_failure
                                                    else "owner_lost_reason"
                                                ): normalized_reason,
                                            }
                                        ),
                                        now,
                                        normalized_activity_run_id,
                                    ),
                                )
                                closed_activity = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if closed_activity is not None:
                                    current_activity = closed_activity
                                    activity_closed = True

                    connection.commit()
                return {
                    "outcome": (
                        "terminalized" if command_closed else "closed" if attempt_closed else "stale_or_already_closed"
                    ),
                    "reason": normalized_reason,
                    "attempt_closed": attempt_closed,
                    "attempts_closed_count": attempts_closed_count,
                    "activity_closed": activity_closed,
                    "command_closed": command_closed,
                    "attempt": reported_attempt,
                    "activity": current_activity,
                    "command": current_command,
                }
            except Exception as exc:
                retry_attempt += 1
                if not _is_retryable_postgres_exception(exc) or retry_attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(retry_attempt))

    def create_agent_trace_span(
        self,
        *,
        session_id: int,
        job_id: str,
        lane_id: str,
        span_name: str,
        stage: str,
        parent_span_id: int = 0,
        handoff_from_lane: str = "",
        handoff_to_lane: str = "",
        input_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_trace_spans"):
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO agent_trace_spans (
                span_id,
                session_id,
                job_id,
                parent_span_id,
                lane_id,
                handoff_from_lane,
                handoff_to_lane,
                span_name,
                stage,
                status,
                input_json,
                output_json,
                metadata_json,
                started_at,
                created_at
            ) VALUES (
                nextval('agent_trace_spans_span_id_seq'),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                int(session_id),
                str(job_id),
                int(parent_span_id) if int(parent_span_id or 0) > 0 else None,
                str(lane_id),
                str(handoff_from_lane or ""),
                str(handoff_to_lane or ""),
                str(span_name),
                str(stage),
                "running",
                _json_dump(input_payload or {}),
                _json_dump({}),
                _json_dump(metadata or {}),
                now,
                now,
            ),
        )

    def update_agent_trace_span(
        self,
        span_id: int,
        *,
        status: str,
        output_payload: dict[str, Any] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_trace_spans"):
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        normalized_status = str(status)
        sql = """
        UPDATE agent_trace_spans
        SET status = %s,
            output_json = %s,
            handoff_to_lane = CASE WHEN %s <> '' THEN %s ELSE COALESCE(handoff_to_lane, '') END,
            completed_at = %s
        WHERE span_id = %s
        RETURNING *
        """
        params = (
            normalized_status,
            _json_dump(output_payload or {}),
            str(handoff_to_lane or ""),
            str(handoff_to_lane or ""),
            now,
            int(span_id),
        )
        for attempt in range(_CONTROL_PLANE_POSTGRES_MAX_RETRIES):
            row = self._execute_returning_one(sql, params)
            if row is not None:
                return row
            existing = self.get_agent_trace_span(int(span_id))
            if existing is not None and str(existing.get("status") or "") == normalized_status:
                return existing
            if attempt + 1 < _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt + 1))
        return None

    def get_agent_trace_span(self, span_id: int) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_trace_spans") or int(span_id or 0) <= 0:
            return None
        return self.select_one("agent_trace_spans", where_sql="span_id = %s", params=[int(span_id)])

    def list_agent_trace_spans(self, *, job_id: str = "", session_id: int = 0) -> list[dict[str, Any]]:
        if not self.should_prefer_read("agent_trace_spans"):
            return []
        if int(session_id or 0) > 0:
            return self.select_many(
                "agent_trace_spans",
                where_sql="session_id = %s",
                params=[int(session_id)],
                order_by_sql="span_id",
                limit=0,
            )
        if job_id:
            return self.select_many(
                "agent_trace_spans",
                where_sql="job_id = %s",
                params=[str(job_id)],
                order_by_sql="span_id",
                limit=0,
            )
        return []

    def save_job_row(
        self,
        *,
        row: dict[str, Any] | None,
        protect_terminal_statuses: bool = True,
        terminal_statuses: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("jobs"):
            return None
        payload = dict(row or {})
        normalized_job_id = str(payload.get("job_id") or "").strip()
        if not normalized_job_id:
            return None
        terminal_status_set = {
            str(item or "").strip().lower() for item in list(terminal_statuses or []) if str(item or "").strip()
        }
        if protect_terminal_statuses and terminal_status_set:
            existing = self.select_one("jobs", where_sql="job_id = %s", params=[normalized_job_id])
            existing_status = str((existing or {}).get("status") or "").strip().lower()
            incoming_status = str(payload.get("status") or "").strip().lower()
            if existing_status in terminal_status_set and incoming_status not in terminal_status_set:
                return existing
        self._ensure_control_plane_writer_schema()
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO jobs (
                job_id,
                job_type,
                status,
                stage,
                request_json,
                plan_json,
                execution_bundle_json,
                matching_request_json,
                summary_json,
                artifact_path,
                request_signature,
                request_family_signature,
                matching_request_signature,
                matching_request_family_signature,
                requester_id,
                tenant_id,
                idempotency_key,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) DO UPDATE SET
                job_type = EXCLUDED.job_type,
                status = EXCLUDED.status,
                stage = EXCLUDED.stage,
                request_json = EXCLUDED.request_json,
                plan_json = EXCLUDED.plan_json,
                execution_bundle_json = CASE
                    WHEN EXCLUDED.execution_bundle_json <> '{}' THEN EXCLUDED.execution_bundle_json
                    ELSE jobs.execution_bundle_json
                END,
                matching_request_json = CASE
                    WHEN EXCLUDED.matching_request_json <> '{}' THEN EXCLUDED.matching_request_json
                    ELSE jobs.matching_request_json
                END,
                summary_json = EXCLUDED.summary_json,
                artifact_path = CASE
                    WHEN EXCLUDED.artifact_path <> '' THEN EXCLUDED.artifact_path
                    ELSE jobs.artifact_path
                END,
                request_signature = EXCLUDED.request_signature,
                request_family_signature = EXCLUDED.request_family_signature,
                matching_request_signature = EXCLUDED.matching_request_signature,
                matching_request_family_signature = EXCLUDED.matching_request_family_signature,
                requester_id = CASE
                    WHEN EXCLUDED.requester_id <> '' THEN EXCLUDED.requester_id
                    ELSE jobs.requester_id
                END,
                tenant_id = CASE
                    WHEN EXCLUDED.tenant_id <> '' THEN EXCLUDED.tenant_id
                    ELSE jobs.tenant_id
                END,
                idempotency_key = CASE
                    WHEN EXCLUDED.idempotency_key <> '' THEN EXCLUDED.idempotency_key
                    ELSE jobs.idempotency_key
                END,
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                normalized_job_id,
                str(payload.get("job_type") or "retrieval"),
                str(payload.get("status") or ""),
                str(payload.get("stage") or "pending"),
                str(payload.get("request_json") or "{}"),
                str(payload.get("plan_json") or "{}"),
                str(payload.get("execution_bundle_json") or "{}"),
                str(payload.get("matching_request_json") or "{}"),
                str(payload.get("summary_json") or "{}"),
                str(payload.get("artifact_path") or ""),
                str(payload.get("request_signature") or ""),
                str(payload.get("request_family_signature") or ""),
                str(payload.get("matching_request_signature") or ""),
                str(payload.get("matching_request_family_signature") or ""),
                str(payload.get("requester_id") or ""),
                str(payload.get("tenant_id") or ""),
                str(payload.get("idempotency_key") or ""),
                str(payload.get("created_at") or now),
                now,
            ),
        )

    def update_job_row_if_owned(
        self,
        *,
        row: dict[str, Any] | None,
        expected_requester_id: str,
        expected_tenant_id: str,
        expected_job_type: str = "",
        expected_statuses: list[str] | tuple[str, ...] = (),
        expected_stage: str = "",
        forbidden_statuses: list[str] | tuple[str, ...] = (),
        expected_summary_fields: dict[str, Any] | None = None,
        forbidden_summary_values: dict[str, list[str] | tuple[str, ...]] | None = None,
    ) -> dict[str, Any] | None:
        """Typed owner-and-state CAS for an existing job.

        Unlike ``save_job_row`` this path cannot insert a missing row and never
        rewrites owner columns. Owner and caller-supplied state preconditions are
        evaluated while the job row is locked, closing both owner and terminal
        state read-to-write races. ``owner_miss`` deliberately combines missing
        and foreign rows; ``state_conflict`` returns only an already-authorized
        row for the caller to project through its business contract.
        """

        if not self.should_prefer_read("jobs"):
            return None
        payload = dict(row or {})
        normalized_job_id = str(payload.get("job_id") or "").strip()
        normalized_requester = str(expected_requester_id or "").strip()
        normalized_tenant = str(expected_tenant_id or "").strip()
        if not normalized_job_id or not normalized_requester or not normalized_tenant:
            return {"status": "owner_miss"}
        self._ensure_control_plane_writer_schema()
        now = _utc_now_sql_timestamp()
        normalized_expected_type = str(expected_job_type or "").strip().lower()
        normalized_expected_statuses = {
            str(status or "").strip().lower() for status in expected_statuses if str(status or "").strip()
        }
        normalized_expected_stage = str(expected_stage or "").strip().lower()
        normalized_forbidden_statuses = {
            str(status or "").strip().lower() for status in forbidden_statuses if str(status or "").strip()
        }
        expected_summary = dict(expected_summary_fields or {})
        forbidden_summary = {
            str(field): {str(value or "").strip().lower() for value in values if str(value or "").strip()}
            for field, values in dict(forbidden_summary_values or {}).items()
        }

        def _normalized_guard_value(value: Any) -> str:
            return str(value or "").strip().lower()

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT * FROM jobs WHERE job_id = %s FOR UPDATE", (normalized_job_id,))
                        current = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        if (
                            not current
                            or str(current.get("requester_id") or "").strip() != normalized_requester
                            or str(current.get("tenant_id") or "").strip() != normalized_tenant
                        ):
                            connection.commit()
                            return {"status": "owner_miss"}

                        current_type = str(current.get("job_type") or "").strip().lower()
                        current_status = str(current.get("status") or "").strip().lower()
                        current_stage = str(current.get("stage") or "").strip().lower()
                        current_summary = _json_load_dict(current.get("summary_json"))
                        state_conflict = bool(
                            (normalized_expected_type and current_type != normalized_expected_type)
                            or (normalized_expected_statuses and current_status not in normalized_expected_statuses)
                            or (normalized_expected_stage and current_stage != normalized_expected_stage)
                            or (normalized_forbidden_statuses and current_status in normalized_forbidden_statuses)
                            or any(
                                _normalized_guard_value(current_summary.get(field))
                                != _normalized_guard_value(expected_value)
                                for field, expected_value in expected_summary.items()
                            )
                            or any(
                                _normalized_guard_value(current_summary.get(field)) in forbidden_values
                                for field, forbidden_values in forbidden_summary.items()
                                if forbidden_values
                            )
                        )
                        if state_conflict:
                            connection.commit()
                            return {"status": "state_conflict", "row": current}

                        cursor.execute(
                            """
                            UPDATE jobs SET
                                job_type = %s,
                                status = %s,
                                stage = %s,
                                request_json = %s,
                                plan_json = %s,
                                execution_bundle_json = CASE
                                    WHEN %s <> '{}' THEN %s
                                    ELSE jobs.execution_bundle_json
                                END,
                                matching_request_json = CASE
                                    WHEN %s <> '{}' THEN %s
                                    ELSE jobs.matching_request_json
                                END,
                                summary_json = %s,
                                artifact_path = CASE WHEN %s <> '' THEN %s ELSE jobs.artifact_path END,
                                request_signature = %s,
                                request_family_signature = %s,
                                matching_request_signature = %s,
                                matching_request_family_signature = %s,
                                idempotency_key = CASE WHEN %s <> '' THEN %s ELSE jobs.idempotency_key END,
                                updated_at = %s
                            WHERE job_id = %s
                            RETURNING *
                            """,
                            (
                                str(payload.get("job_type") or "retrieval"),
                                str(payload.get("status") or ""),
                                str(payload.get("stage") or "pending"),
                                str(payload.get("request_json") or "{}"),
                                str(payload.get("plan_json") or "{}"),
                                str(payload.get("execution_bundle_json") or "{}"),
                                str(payload.get("execution_bundle_json") or "{}"),
                                str(payload.get("matching_request_json") or "{}"),
                                str(payload.get("matching_request_json") or "{}"),
                                str(payload.get("summary_json") or "{}"),
                                str(payload.get("artifact_path") or ""),
                                str(payload.get("artifact_path") or ""),
                                str(payload.get("request_signature") or ""),
                                str(payload.get("request_family_signature") or ""),
                                str(payload.get("matching_request_signature") or ""),
                                str(payload.get("matching_request_family_signature") or ""),
                                str(payload.get("idempotency_key") or ""),
                                str(payload.get("idempotency_key") or ""),
                                now,
                                normalized_job_id,
                            ),
                        )
                        updated = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    connection.commit()
                return {"status": "applied", "row": updated}
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def append_job_event(
        self,
        *,
        job_id: str,
        stage: str,
        status: str,
        detail: str,
        payload_dict: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_events"):
            return None
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        self._ensure_control_plane_writer_schema()
        event_row = self._execute_returning_one(
            """
            INSERT INTO job_events (
                event_id,
                job_id,
                stage,
                status,
                detail,
                payload_json,
                created_at
            ) VALUES (
                nextval('job_events_event_id_seq'),
                %s, %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                normalized_job_id,
                str(stage or ""),
                str(status or ""),
                str(detail or ""),
                _json_dump(payload_dict or {}),
                _utc_now_sql_timestamp(),
            ),
        )
        if event_row is None:
            return None
        summary_row = self._upsert_job_progress_event_summary(
            normalized_job_id,
            event={
                "event_id": int(event_row.get("event_id") or 0),
                "stage": str(event_row.get("stage") or ""),
                "status": str(event_row.get("status") or ""),
                "detail": str(event_row.get("detail") or ""),
                "payload": dict(payload_dict or {}),
                "created_at": str(event_row.get("created_at") or ""),
            },
        )
        compacted = False
        if str(stage or "") in {"runtime_heartbeat", "runtime_control"}:
            compacted = self._compact_runtime_job_events(
                job_id=normalized_job_id,
                stage=str(stage or ""),
                payload=dict(payload_dict or {}),
            )
        return {
            "event": event_row,
            "summary": summary_row,
            "compacted": compacted,
        }

    def create_agent_runtime_session_row(self, *, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_runtime_sessions"):
            return None
        payload = dict(row or {})
        normalized_job_id = str(payload.get("job_id") or "").strip()
        if not normalized_job_id:
            return None
        self._ensure_control_plane_writer_schema()
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO agent_runtime_sessions (
                session_id,
                job_id,
                target_company,
                request_signature,
                request_family_signature,
                runtime_mode,
                status,
                lanes_json,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (
                nextval('agent_runtime_sessions_session_id_seq'),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) DO UPDATE SET
                target_company = EXCLUDED.target_company,
                request_signature = EXCLUDED.request_signature,
                request_family_signature = EXCLUDED.request_family_signature,
                runtime_mode = EXCLUDED.runtime_mode,
                status = EXCLUDED.status,
                lanes_json = EXCLUDED.lanes_json,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                normalized_job_id,
                str(payload.get("target_company") or ""),
                str(payload.get("request_signature") or ""),
                str(payload.get("request_family_signature") or ""),
                str(payload.get("runtime_mode") or "agent_runtime"),
                str(payload.get("status") or "running"),
                str(payload.get("lanes_json") or "[]"),
                str(payload.get("metadata_json") or "{}"),
                str(payload.get("created_at") or now),
                now,
            ),
        )

    def update_agent_runtime_session_status(self, job_id: str, status: str) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_runtime_sessions"):
            return None
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        self._ensure_control_plane_writer_schema()
        normalized_status = str(status or "")
        sql = """
        UPDATE agent_runtime_sessions
        SET status = %s,
            updated_at = %s
        WHERE job_id = %s
        RETURNING *
        """
        params = (
            normalized_status,
            _utc_now_sql_timestamp(),
            normalized_job_id,
        )
        for attempt in range(_CONTROL_PLANE_POSTGRES_MAX_RETRIES):
            row = self._execute_returning_one(sql, params)
            if row is not None:
                return row
            existing = self.select_one(
                "agent_runtime_sessions",
                where_sql="job_id = %s",
                params=[normalized_job_id],
            )
            if existing is not None and str(existing.get("status") or "") == normalized_status:
                return existing
            if attempt + 1 < _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt + 1))
        return None

    def create_or_resume_agent_worker(
        self,
        *,
        session_id: int,
        job_id: str,
        span_id: int,
        lane_id: str,
        worker_key: str,
        budget_payload: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO agent_worker_runs (
                worker_id,
                session_id,
                job_id,
                span_id,
                lane_id,
                worker_key,
                status,
                interrupt_requested,
                budget_json,
                checkpoint_json,
                input_json,
                output_json,
                metadata_json,
                attempt_count,
                created_at,
                updated_at
            ) VALUES (
                nextval('agent_worker_runs_worker_id_seq'),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id, lane_id, worker_key) DO UPDATE SET
                session_id = EXCLUDED.session_id,
                span_id = EXCLUDED.span_id,
                status = CASE
                    WHEN agent_worker_runs.status = 'completed' THEN agent_worker_runs.status
                    WHEN agent_worker_runs.status = 'interrupted' THEN 'queued'
                    ELSE EXCLUDED.status
                END,
                budget_json = EXCLUDED.budget_json,
                input_json = EXCLUDED.input_json,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                int(session_id),
                str(job_id),
                int(span_id) if int(span_id or 0) > 0 else None,
                str(lane_id),
                str(worker_key),
                "queued",
                0,
                _json_dump(budget_payload or {}),
                _json_dump({}),
                _json_dump(input_payload or {}),
                _json_dump({}),
                _json_dump(metadata or {}),
                0,
                now,
                now,
            ),
        )

    def mark_agent_worker_running(self, worker_id: int) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET status = CASE WHEN status = 'completed' THEN status ELSE 'running' END,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (_utc_now_sql_timestamp(), int(worker_id)),
        )

    def checkpoint_agent_worker(
        self,
        worker_id: int,
        *,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
        status: str = "running",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET status = %s,
                checkpoint_json = %s,
                output_json = %s,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (
                str(status),
                _json_dump(checkpoint_payload or {}),
                _json_dump(output_payload or {}),
                _utc_now_sql_timestamp(),
                int(worker_id),
            ),
        )

    def complete_agent_worker(
        self,
        worker_id: int,
        *,
        status: str,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET status = %s,
                checkpoint_json = %s,
                output_json = %s,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (
                str(status),
                _json_dump(checkpoint_payload or {}),
                _json_dump(output_payload or {}),
                _utc_now_sql_timestamp(),
                int(worker_id),
            ),
        )

    def get_agent_worker(
        self,
        *,
        worker_id: int = 0,
        job_id: str = "",
        lane_id: str = "",
        worker_key: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        if int(worker_id or 0) > 0:
            return self.select_one("agent_worker_runs", where_sql="worker_id = %s", params=[int(worker_id)])
        if job_id and lane_id and worker_key:
            return self.select_one(
                "agent_worker_runs",
                where_sql="job_id = %s AND lane_id = %s AND worker_key = %s",
                params=[str(job_id), str(lane_id), str(worker_key)],
            )
        return None

    def list_agent_workers(self, *, job_id: str = "", session_id: int = 0, lane_id: str = "") -> list[dict[str, Any]]:
        if not self.should_prefer_read("agent_worker_runs"):
            return []
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = %s")
            params.append(str(job_id))
        if int(session_id or 0) > 0:
            clauses.append("session_id = %s")
            params.append(int(session_id))
        if lane_id:
            clauses.append("lane_id = %s")
            params.append(str(lane_id))
        if not clauses:
            return []
        return self.select_many(
            "agent_worker_runs",
            where_sql=" AND ".join(clauses),
            params=params,
            order_by_sql="worker_id",
            limit=0,
        )

    def list_agent_workers_by_remote_provider_identifiers(
        self,
        *,
        run_id: str = "",
        dataset_id: str = "",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("agent_worker_runs"):
            return []
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        if not normalized_run_id and not normalized_dataset_id:
            return []
        checkpoint_json = "COALESCE(NULLIF(checkpoint_json, ''), '{}')::jsonb"
        clauses: list[str] = []
        params: list[Any] = []
        if normalized_run_id:
            clauses.append(
                "("
                f"{checkpoint_json} ->> 'run_id' = %s OR "
                f"{checkpoint_json} ->> 'actor_run_id' = %s OR "
                f"{checkpoint_json} ->> 'actorRunId' = %s"
                ")"
            )
            params.extend([normalized_run_id, normalized_run_id, normalized_run_id])
        if normalized_dataset_id:
            clauses.append(
                "("
                f"{checkpoint_json} ->> 'dataset_id' = %s OR "
                f"{checkpoint_json} ->> 'default_dataset_id' = %s OR "
                f"{checkpoint_json} ->> 'defaultDatasetId' = %s"
                ")"
            )
            params.extend([normalized_dataset_id, normalized_dataset_id, normalized_dataset_id])
        return self.select_many(
            "agent_worker_runs",
            where_sql=" OR ".join(clauses),
            params=params,
            order_by_sql="updated_at DESC, worker_id DESC",
            limit=max(1, int(limit or 50)),
        )

    def list_recoverable_agent_workers(
        self,
        *,
        limit: int = 100,
        stale_after_seconds: int = 300,
        lane_id: str = "",
        job_id: str = "",
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("agent_worker_runs"):
            return []
        fetch_limit = max(25, min(max(1, int(limit or 100)) * 5, 1000))
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = %s")
            params.append(str(job_id))
        if lane_id:
            clauses.append("lane_id = %s")
            params.append(str(lane_id))
        rows = self.select_many(
            "agent_worker_runs",
            where_sql=" AND ".join(clauses),
            params=params,
            order_by_sql="updated_at ASC, worker_id ASC",
            limit=fetch_limit,
        )
        if not rows:
            return []
        recoverable: list[dict[str, Any]] = []
        stale_cutoff_seconds = max(1, int(stale_after_seconds or 300))
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            lease_owner = str(row.get("lease_owner") or "").strip()
            lease_expires_at = str(row.get("lease_expires_at") or "").strip()
            checkpoint = _json_load_dict(row.get("checkpoint_json"))
            checkpoint_stage = str(checkpoint.get("stage") or "").strip()
            if checkpoint_stage == "waiting_profile_coalescing":
                continue
            updated_at = str(row.get("updated_at") or "").strip()
            is_running_wait = status == "running" and checkpoint_stage in _RUNNING_RECOVERABLE_WAIT_STAGES
            is_running_stale = status == "running" and _timestamp_age_seconds(updated_at) >= stale_cutoff_seconds
            is_recoverable_status = status in {"queued", "interrupted", "failed"}
            lease_available = (
                not lease_expires_at
                or _timestamp_is_expired(lease_expires_at)
                or worker_lease_owner_is_dead_local_process(lease_owner)
            )
            if (is_recoverable_status or is_running_wait or is_running_stale) and lease_available:
                recoverable.append(dict(row))
        recoverable.sort(key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)))
        return recoverable[: max(1, int(limit or 100))]

    def retire_agent_workers(
        self,
        *,
        worker_ids: list[int],
        status: str = "cancelled",
        reason: str = "",
        cleanup_metadata: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.should_prefer_read("agent_worker_runs"):
            return []
        normalized_worker_ids = [int(worker_id) for worker_id in list(worker_ids or []) if int(worker_id or 0) > 0]
        if not normalized_worker_ids:
            return []
        normalized_status = str(status or "cancelled").strip().lower() or "cancelled"
        normalized_reason = str(reason or "").strip()
        immutable_terminal_statuses = {"completed", "cancelled", "canceled", "superseded"}
        refreshed: list[dict[str, Any]] = []
        for worker_id in normalized_worker_ids:
            current = self.get_agent_worker(worker_id=worker_id)
            if current is None:
                continue
            current_status = str(current.get("status") or "").strip().lower()
            if current_status in immutable_terminal_statuses:
                refreshed.append(current)
                continue
            metadata = _json_load_dict(current.get("metadata_json"))
            cleanup_payload = dict(metadata.get("cleanup") or {})
            cleanup_payload.update(
                {
                    "reason": normalized_reason,
                    "status": normalized_status,
                    "cleaned_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            cleanup_payload.update(dict(cleanup_metadata or {}))
            metadata["cleanup"] = cleanup_payload
            output = _json_load_dict(current.get("output_json"))
            output["cleanup"] = cleanup_payload
            updated = self._execute_returning_one(
                """
                UPDATE agent_worker_runs
                SET status = %s,
                    output_json = %s,
                    metadata_json = %s,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = %s
                WHERE worker_id = %s
                RETURNING *
                """,
                (
                    normalized_status,
                    _json_dump(output),
                    _json_dump(metadata),
                    _utc_now_sql_timestamp(),
                    worker_id,
                ),
            )
            if updated is not None:
                refreshed.append(updated)
        return refreshed

    def claim_agent_worker(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        now = _utc_now_sql_timestamp()
        current = self.get_agent_worker(worker_id=int(worker_id))
        current_lease_owner = str(dict(current or {}).get("lease_owner") or "").strip()
        current_lease_expires_at = str(dict(current or {}).get("lease_expires_at") or "").strip()
        if (
            current_lease_owner
            and current_lease_expires_at
            and not _timestamp_is_expired(current_lease_expires_at)
            and worker_lease_owner_is_dead_local_process(current_lease_owner)
        ):
            self._execute_returning_one(
                """
                UPDATE agent_worker_runs
                SET lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = %s,
                    updated_at = %s
                WHERE worker_id = %s AND lease_owner = %s
                RETURNING *
                """,
                (
                    f"Released dead local worker lease owner {current_lease_owner}",
                    now,
                    int(worker_id),
                    current_lease_owner,
                ),
            )
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET lease_owner = %s,
                lease_expires_at = %s,
                attempt_count = COALESCE(attempt_count, 0) + 1,
                updated_at = %s
            WHERE worker_id = %s
              AND (lease_expires_at IS NULL OR lease_expires_at = '' OR lease_expires_at <= %s)
            RETURNING *
            """,
            (
                str(lease_owner),
                _expiry_timestamp(int(lease_seconds or 60)),
                now,
                int(worker_id),
                now,
            ),
        )

    def renew_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET lease_expires_at = %s,
                updated_at = %s
            WHERE worker_id = %s AND lease_owner = %s
            RETURNING *
            """,
            (
                _expiry_timestamp(int(lease_seconds or 60)),
                _utc_now_sql_timestamp(),
                int(worker_id),
                str(lease_owner),
            ),
        )

    def release_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str = "",
        error_text: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        normalized_owner = str(lease_owner or "").strip()
        if normalized_owner:
            return self._execute_returning_one(
                """
                UPDATE agent_worker_runs
                SET lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = CASE WHEN %s <> '' THEN %s ELSE last_error END,
                    updated_at = %s
                WHERE worker_id = %s AND lease_owner = %s
                RETURNING *
                """,
                (
                    str(error_text or ""),
                    str(error_text or ""),
                    _utc_now_sql_timestamp(),
                    int(worker_id),
                    normalized_owner,
                ),
            )
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = CASE WHEN %s <> '' THEN %s ELSE last_error END,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (
                str(error_text or ""),
                str(error_text or ""),
                _utc_now_sql_timestamp(),
                int(worker_id),
            ),
        )

    def request_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET interrupt_requested = 1,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (_utc_now_sql_timestamp(), int(worker_id)),
        )

    def clear_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if not self.should_prefer_read("agent_worker_runs"):
            return None
        return self._execute_returning_one(
            """
            UPDATE agent_worker_runs
            SET interrupt_requested = 0,
                updated_at = %s
            WHERE worker_id = %s
            RETURNING *
            """,
            (_utc_now_sql_timestamp(), int(worker_id)),
        )

    def claim_job_materialization_item(
        self,
        item_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        self._ensure_table_write_schema("job_materialization_items")
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE job_materialization_items
            SET status = 'running',
                phase = 'applying',
                lease_owner = %s,
                lease_expires_at = %s,
                attempt_count = COALESCE(attempt_count, 0) + 1,
                updated_at = %s
            WHERE item_id = %s
              AND status IN ('queued', 'deferred', 'failed_retryable', 'waiting_prerequisite', 'running')
              AND (not_before_at = '' OR not_before_at <= %s)
              AND (lease_expires_at = '' OR lease_expires_at <= %s)
            RETURNING *
            """,
            (
                str(lease_owner),
                _expiry_timestamp(int(lease_seconds or 300)),
                now,
                str(item_id),
                now,
                now,
            ),
        )

    def mark_job_materialization_item_completed(
        self,
        item_id: str,
        *,
        result_patch_id: str = "",
        result_view_id: str = "",
        serving_projection_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        self._ensure_table_write_schema("job_materialization_items")
        current = self.select_one("job_materialization_items", where_sql="item_id = %s", params=[str(item_id)])
        metadata_payload = {
            **_json_load_dict(dict(current or {}).get("metadata_json")),
            **dict(metadata or {}),
        }
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE job_materialization_items
            SET status = 'completed',
                phase = 'applied',
                result_patch_id = %s,
                result_view_id = %s,
                serving_projection_id = %s,
                lease_owner = '',
                lease_expires_at = '',
                last_error = '',
                metadata_json = %s,
                completed_at = %s,
                updated_at = %s
            WHERE item_id = %s
            RETURNING *
            """,
            (
                str(result_patch_id or dict(current or {}).get("result_patch_id") or ""),
                str(result_view_id or dict(current or {}).get("result_view_id") or ""),
                str(serving_projection_id or dict(current or {}).get("serving_projection_id") or ""),
                _json_dump(metadata_payload),
                now,
                now,
                str(item_id),
            ),
        )

    def mark_job_materialization_item_failed(
        self,
        item_id: str,
        *,
        error_text: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        self._ensure_table_write_schema("job_materialization_items")
        current = self.select_one("job_materialization_items", where_sql="item_id = %s", params=[str(item_id)])
        attempt_count = int(dict(current or {}).get("attempt_count") or 0)
        max_attempts = max(1, int(dict(current or {}).get("max_attempts") or 5))
        should_retry = bool(retryable) and attempt_count < max_attempts
        metadata_payload = {
            **_json_load_dict(dict(current or {}).get("metadata_json")),
            **dict(metadata or {}),
        }
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE job_materialization_items
            SET status = %s,
                phase = %s,
                lease_owner = '',
                lease_expires_at = '',
                not_before_at = %s,
                last_error = %s,
                metadata_json = %s,
                updated_at = %s
            WHERE item_id = %s
            RETURNING *
            """,
            (
                "failed_retryable" if should_retry else "failed",
                "retry_wait" if should_retry else "terminal",
                _expiry_timestamp(int(retry_delay_seconds or 30)) if should_retry else "",
                str(error_text or ""),
                _json_dump(metadata_payload),
                now,
                str(item_id),
            ),
        )

    def mark_job_materialization_item_waiting_prerequisite(
        self,
        item_id: str,
        *,
        retry_delay_seconds: int = 8,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        self._ensure_table_write_schema("job_materialization_items")
        current = self.select_one("job_materialization_items", where_sql="item_id = %s", params=[str(item_id)])
        attempt_count = max(0, int(dict(current or {}).get("attempt_count") or 0) - 1)
        metadata_payload = {
            **_json_load_dict(dict(current or {}).get("metadata_json")),
            **dict(metadata or {}),
        }
        metadata_payload.setdefault("failure_reason", "waiting_prerequisite_candidate_documents")
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE job_materialization_items
            SET status = 'waiting_prerequisite',
                phase = 'waiting_prerequisite',
                lease_owner = '',
                lease_expires_at = '',
                attempt_count = %s,
                not_before_at = %s,
                last_error = '',
                metadata_json = %s,
                updated_at = %s
            WHERE item_id = %s
            RETURNING *
            """,
            (
                attempt_count,
                _expiry_timestamp(min(30, max(1, int(retry_delay_seconds or 8)))),
                _json_dump(metadata_payload),
                now,
                str(item_id),
            ),
        )

    def mark_job_materialization_item_partial_progress(
        self,
        item_id: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        self._ensure_table_write_schema("job_materialization_items")
        current = self.select_one("job_materialization_items", where_sql="item_id = %s", params=[str(item_id)])
        attempt_count = max(0, int(dict(current or {}).get("attempt_count") or 0) - 1)
        metadata_payload = {
            **_json_load_dict(dict(current or {}).get("metadata_json")),
            **dict(metadata or {}),
        }
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE job_materialization_items
            SET status = 'queued',
                phase = 'queued',
                lease_owner = '',
                lease_expires_at = '',
                attempt_count = %s,
                not_before_at = '',
                last_error = '',
                metadata_json = %s,
                updated_at = %s
            WHERE item_id = %s
            RETURNING *
            """,
            (
                attempt_count,
                _json_dump(metadata_payload),
                now,
                str(item_id),
            ),
        )

    def reawaken_waiting_prerequisite_job_materialization_items(
        self,
        *,
        job_id: str,
        snapshot_id: str,
        item_kind: str = "local_apply_closure",
        source: str = "candidate_documents_prerequisite_ready",
    ) -> int | None:
        if not self.should_prefer_read("job_materialization_items"):
            return None
        normalized_job_id = str(job_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_kind = str(item_kind or "local_apply_closure").strip() or "local_apply_closure"
        normalized_source = str(source or "candidate_documents_prerequisite_ready").strip()
        if not normalized_job_id or not normalized_snapshot_id:
            return 0
        self._ensure_table_write_schema("job_materialization_items")
        now = _utc_now_sql_timestamp()
        return self._execute_non_query(
            """
            UPDATE job_materialization_items
            SET status = 'queued',
                phase = 'queued',
                not_before_at = '',
                metadata_json = jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            COALESCE(NULLIF(metadata_json, ''), '{}')::jsonb,
                            '{reawakened_by}',
                            to_jsonb(%s::text),
                            true
                        ),
                        '{prerequisite_ready_source}',
                        to_jsonb(%s::text),
                        true
                    ),
                    '{prerequisite_ready_at}',
                    to_jsonb(%s::text),
                    true
                )::text,
                updated_at = %s
            WHERE status = 'waiting_prerequisite'
              AND job_id = %s
              AND snapshot_id = %s
              AND item_kind = %s
            """,
            (
                normalized_source,
                normalized_source,
                now,
                now,
                normalized_job_id,
                normalized_snapshot_id,
                normalized_kind,
            ),
        )

    def reawaken_waiting_prerequisite_workflow_commands(
        self,
        *,
        workflow_run_id: str,
        snapshot_id: str,
        command_type: str,
        source: str = "candidate_documents_prerequisite_ready",
    ) -> int | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_run_id = str(workflow_run_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_command_type = str(command_type or "").strip()
        normalized_source = str(source or "candidate_documents_prerequisite_ready").strip()
        if not normalized_run_id or not normalized_snapshot_id or not normalized_command_type:
            return 0
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        return self._execute_non_query(
            """
            UPDATE workflow_commands
            SET status = 'queued',
                not_before_at = '',
                result_json = jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            COALESCE(NULLIF(result_json, ''), '{}')::jsonb,
                            '{reawakened_by}',
                            to_jsonb(%s::text),
                            true
                        ),
                        '{prerequisite_ready_source}',
                        to_jsonb(%s::text),
                        true
                    ),
                    '{prerequisite_ready_at}',
                    to_jsonb(%s::text),
                    true
                )::text,
                updated_at = %s
            WHERE status = 'retry_wait'
              AND workflow_run_id = %s
              AND command_type = %s
              AND COALESCE(COALESCE(NULLIF(payload_json, ''), '{}')::jsonb ->> 'snapshot_id', '') = %s
              AND COALESCE(last_error, '') = ''
              AND COALESCE(COALESCE(NULLIF(result_json, ''), '{}')::jsonb ->> 'status', '') = 'waiting_prerequisite'
            """,
            (
                normalized_source,
                normalized_source,
                now,
                now,
                normalized_run_id,
                normalized_command_type,
                normalized_snapshot_id,
            ),
        )

    def acquire_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 900,
        lease_token: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_job_leases"):
            return None
        self._ensure_runtime_coordination_schema()
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_job_id or not normalized_owner or not normalized_token:
            return None
        now = _utc_now_sql_timestamp()
        row = self._execute_returning_one(
            """
            INSERT INTO workflow_job_leases (
                job_id,
                lease_owner,
                lease_token,
                lease_expires_at,
                created_at,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                lease_owner = EXCLUDED.lease_owner,
                lease_token = EXCLUDED.lease_token,
                lease_expires_at = EXCLUDED.lease_expires_at,
                updated_at = EXCLUDED.updated_at
            WHERE workflow_job_leases.lease_expires_at <= %s
               OR workflow_job_leases.lease_owner = EXCLUDED.lease_owner
               OR workflow_job_leases.lease_token = EXCLUDED.lease_token
            RETURNING *
            """,
            (
                normalized_job_id,
                normalized_owner,
                normalized_token,
                _expiry_timestamp(int(lease_seconds or 900)),
                now,
                now,
                now,
            ),
        )
        if row is not None:
            return row
        return self.get_workflow_job_lease(normalized_job_id)

    def get_workflow_job_lease(self, job_id: str) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_job_leases") or not str(job_id or "").strip():
            return None
        return self.select_one("workflow_job_leases", where_sql="job_id = %s", params=[str(job_id)])

    def renew_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
        lease_token: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_job_leases"):
            return None
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_job_id or not normalized_owner:
            return None
        clauses = ["job_id = %s", "lease_owner = %s"]
        params: list[Any] = [
            _expiry_timestamp(int(lease_seconds or 900)),
            _utc_now_sql_timestamp(),
            normalized_job_id,
            normalized_owner,
        ]
        if normalized_token:
            clauses.append("lease_token = %s")
            params.append(normalized_token)
        return self._execute_returning_one(
            f"""
            UPDATE workflow_job_leases
            SET lease_expires_at = %s,
                updated_at = %s
            WHERE {" AND ".join(clauses)}
            RETURNING *
            """,
            tuple(params),
        )

    def release_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> None:
        if not self.should_prefer_read("workflow_job_leases"):
            return
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_job_id:
            return
        clauses = ["job_id = %s"]
        params: list[Any] = [normalized_job_id]
        if normalized_owner:
            clauses.append("lease_owner = %s")
            params.append(normalized_owner)
        if normalized_token:
            clauses.append("lease_token = %s")
            params.append(normalized_token)
        self._execute_non_query(
            f"DELETE FROM workflow_job_leases WHERE {' AND '.join(clauses)}",
            tuple(params),
        )

    def upsert_workflow_recovery_intent(
        self,
        job_id: str,
        *,
        table_name: str = "workflow_recovery_intents",
        classification: str = "",
        params: dict[str, Any] | None = None,
        requested_by: str = "",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "workflow_recovery_intents":
            raise ValueError("upsert_workflow_recovery_intent requires table_name=workflow_recovery_intents")
        if not self.should_prefer_read("workflow_recovery_intents"):
            return None
        self._ensure_runtime_coordination_schema()
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO workflow_recovery_intents (
                job_id,
                classification,
                status,
                requested_at,
                requested_by,
                params_json,
                lease_owner,
                lease_expires_at,
                claimed_at,
                schema_version,
                created_at,
                updated_at
            ) VALUES (%s, %s, 'pending', %s, %s, %s, '', '', '', 'workflow_recovery_intent_v1', %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                classification = EXCLUDED.classification,
                status = 'pending',
                requested_at = EXCLUDED.requested_at,
                requested_by = EXCLUDED.requested_by,
                params_json = EXCLUDED.params_json,
                lease_owner = '',
                lease_expires_at = '',
                claimed_at = '',
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                normalized_job_id,
                str(classification or ""),
                now,
                str(requested_by or ""),
                _json_dump(dict(params or {})),
                now,
                now,
            ),
        )

    def claim_workflow_recovery_intents(
        self,
        *,
        table_name: str = "workflow_recovery_intents",
        lease_owner: str,
        lease_seconds: int = 300,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        if _normalize_postgres_identifier(table_name) != "workflow_recovery_intents":
            raise ValueError("claim_workflow_recovery_intents requires table_name=workflow_recovery_intents")
        if not self.should_prefer_read("workflow_recovery_intents"):
            return []
        self._ensure_runtime_coordination_schema()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_owner:
            return []
        now = _utc_now_sql_timestamp()
        normalized_limit = max(1, int(limit or 1))
        sql = """
            UPDATE workflow_recovery_intents
            SET status = 'claimed',
                lease_owner = %s,
                lease_expires_at = %s,
                claimed_at = %s,
                updated_at = %s
            WHERE job_id IN (
                SELECT job_id FROM workflow_recovery_intents
                WHERE status = 'pending'
                   OR (status = 'claimed' AND lease_expires_at != '' AND lease_expires_at <= %s)
                ORDER BY requested_at, job_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
        """
        normalized_params = tuple(
            _normalize_postgres_payload(item)
            for item in (
                normalized_owner,
                _expiry_timestamp(int(lease_seconds or 300)),
                now,
                now,
                now,
                normalized_limit,
            )
        )
        self.ensure_bootstrapped()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql, normalized_params)
                        rows = _fetch_all_dict_rows(cursor)
                    connection.commit()
                return rows
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def mark_workflow_recovery_intent_consumed(
        self,
        job_id: str,
        *,
        table_name: str = "workflow_recovery_intents",
        lease_owner: str = "",
        claimed_at: str = "",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "workflow_recovery_intents":
            raise ValueError("mark_workflow_recovery_intent_consumed requires table_name=workflow_recovery_intents")
        if not self.should_prefer_read("workflow_recovery_intents"):
            return None
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        # Claim-identity-scoped consume: only the row this daemon actually
        # claimed. A newer upsert that re-armed the row to 'pending' between
        # claim and consume changes status/lease_owner/claimed_at, so this
        # UPDATE matches nothing and the fresh same-job intent survives.
        return self._execute_returning_one(
            """
            UPDATE workflow_recovery_intents
            SET status = 'consumed',
                lease_owner = '',
                lease_expires_at = '',
                updated_at = %s
            WHERE job_id = %s
              AND status = 'claimed'
              AND lease_owner = %s
              AND claimed_at = %s
            RETURNING *
            """,
            (
                _utc_now_sql_timestamp(),
                normalized_job_id,
                str(lease_owner or "").strip(),
                str(claimed_at or "").strip(),
            ),
        )

    def append_workflow_event(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "workflow_events",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "workflow_events":
            raise ValueError("append_workflow_event requires table_name=workflow_events")
        if not self.should_prefer_read("workflow_events"):
            return None
        self._ensure_runtime_coordination_schema()
        payload = _normalize_postgres_row_payload(dict(row or {}))
        workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        event_family = str(payload.get("event_family") or "").strip()
        event_type = str(payload.get("event_type") or "").strip()
        if not workflow_run_id or not idempotency_key or not event_family or not event_type:
            return None
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(
                            cursor,
                            f"workflow_events:{workflow_run_id}",
                        )
                        requested_sequence = max(0, int(payload.get("sequence_number") or 0))
                        cursor.execute(
                            """
                            SELECT * FROM workflow_events
                            WHERE workflow_run_id = %s AND idempotency_key = %s
                            LIMIT 1
                            FOR UPDATE
                            """,
                            (workflow_run_id, idempotency_key),
                        )
                        existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if existing is not None:
                            self._validate_workflow_event_identity(existing, expected=payload)
                            if requested_sequence and int(existing.get("sequence_number") or 0) != requested_sequence:
                                raise ValueError("workflow_events immutable identity collision: sequence_number")
                            connection.commit()
                            return existing

                        cursor.execute(
                            "SELECT COALESCE(MAX(sequence_number), 0) FROM workflow_events WHERE workflow_run_id = %s",
                            (workflow_run_id,),
                        )
                        row_value = cursor.fetchone()
                        max_sequence = int(
                            (row_value[0] if isinstance(row_value, (list, tuple)) and row_value else row_value) or 0
                        )
                        if requested_sequence:
                            if requested_sequence <= max_sequence:
                                raise ValueError(
                                    "workflow_events explicit sequence_number must advance the committed stream"
                                )
                            sequence_number = requested_sequence
                        else:
                            sequence_number = max_sequence + 1
                        event_id = str(payload.get("event_id") or "").strip() or (
                            "evt_"
                            + sha1(
                                f"{workflow_run_id}:{sequence_number}:{idempotency_key}".encode("utf-8")
                            ).hexdigest()[:24]
                        )
                        event_payload = {
                            "event_id": event_id,
                            "workflow_run_id": workflow_run_id,
                            "operation_id": str(payload.get("operation_id") or "").strip(),
                            "command_id": str(payload.get("command_id") or "").strip(),
                            "activity_attempt_id": str(payload.get("activity_attempt_id") or "").strip(),
                            "event_family": event_family,
                            "event_type": event_type,
                            "sequence_number": sequence_number,
                            "idempotency_key": idempotency_key,
                            "occurred_at": str(payload.get("occurred_at") or now).strip(),
                            "recorded_at": str(payload.get("recorded_at") or now).strip(),
                            "actor": str(payload.get("actor") or "").strip(),
                            "source": str(payload.get("source") or "").strip(),
                            "payload_json": str(payload.get("payload_json") or "{}"),
                            "artifact_refs_json": str(payload.get("artifact_refs_json") or "[]"),
                            "schema_version": str(payload.get("schema_version") or "workflow_event_v1").strip(),
                            "created_at": str(payload.get("created_at") or now).strip(),
                        }
                        cursor.execute(
                            "SELECT * FROM workflow_events WHERE event_id = %s LIMIT 1 FOR UPDATE",
                            (event_id,),
                        )
                        if _fetch_one_dict_row(cursor, cursor.fetchone()) is not None:
                            raise ValueError("workflow_events immutable identity collision: event_id")
                        columns = list(event_payload.keys())
                        cursor.execute(
                            (
                                f"INSERT INTO workflow_events ({', '.join(_quote_identifier(column) for column in columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) "
                                "RETURNING *"
                            ),
                            tuple(event_payload[column] for column in columns),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return inserted
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    @staticmethod
    def _validate_workflow_event_identity(
        event: dict[str, Any],
        *,
        expected: dict[str, Any],
    ) -> None:
        identity_fields = (
            "workflow_run_id",
            "operation_id",
            "command_id",
            "activity_attempt_id",
            "event_family",
            "event_type",
            "idempotency_key",
        )
        mismatches = [
            field
            for field in identity_fields
            if str(event.get(field) or "").strip() != str(expected.get(field) or "").strip()
        ]
        if mismatches:
            raise ValueError("workflow_events immutable identity collision: " + ", ".join(mismatches))

    def upsert_workflow_current_state(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "workflow_current_state",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "workflow_current_state":
            raise ValueError("upsert_workflow_current_state requires table_name=workflow_current_state")
        if not self.should_prefer_read("workflow_current_state"):
            return None
        self._ensure_runtime_coordination_schema()
        payload = _normalize_postgres_row_payload(dict(row or {}))
        allowed_columns = {
            "workflow_run_id",
            "operation_id",
            "workflow_type",
            "status",
            "current_stage_key",
            "completion_proofs_json",
            "active_command_counts_json",
            "terminal_command_counts_json",
            "read_model_pointers_json",
            "migration_status_json",
            "last_processed_sequence_number",
            "reducer_version",
            "schema_version",
            "metadata_json",
        }
        unknown_columns = set(payload) - allowed_columns
        if unknown_columns:
            raise ValueError("upsert_workflow_current_state unknown columns: " + ", ".join(sorted(unknown_columns)))
        workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
        if not workflow_run_id:
            return None
        checkpoint_provided = "last_processed_sequence_number" in payload
        incoming_sequence = (
            max(0, int(payload.get("last_processed_sequence_number") or 0)) if checkpoint_provided else None
        )
        now = _utc_now_sql_timestamp()
        json_columns = (
            "completion_proofs_json",
            "active_command_counts_json",
            "terminal_command_counts_json",
            "read_model_pointers_json",
            "migration_status_json",
            "metadata_json",
        )
        reducer_owned_columns = (
            "status",
            "current_stage_key",
            "completion_proofs_json",
            "reducer_version",
            "metadata_json",
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._acquire_transaction_lock(cursor, f"workflow_current_state:{workflow_run_id}")
                cursor.execute(
                    "SELECT * FROM workflow_current_state WHERE workflow_run_id = %s FOR UPDATE",
                    (workflow_run_id,),
                )
                current = _fetch_one_dict_row(cursor, cursor.fetchone())
                if current is None:
                    inserted_payload = {
                        "workflow_run_id": workflow_run_id,
                        "operation_id": str(payload.get("operation_id") or "").strip(),
                        "workflow_type": str(payload.get("workflow_type") or "").strip(),
                        "status": str(payload.get("status") or "pending").strip() or "pending",
                        "current_stage_key": str(payload.get("current_stage_key") or "").strip(),
                        "completion_proofs_json": str(payload.get("completion_proofs_json") or "{}"),
                        "active_command_counts_json": str(payload.get("active_command_counts_json") or "{}"),
                        "terminal_command_counts_json": str(payload.get("terminal_command_counts_json") or "{}"),
                        "read_model_pointers_json": str(payload.get("read_model_pointers_json") or "{}"),
                        "migration_status_json": str(payload.get("migration_status_json") or "{}"),
                        "last_processed_sequence_number": incoming_sequence or 0,
                        "reducer_version": str(payload.get("reducer_version") or "").strip(),
                        "schema_version": str(payload.get("schema_version") or "workflow_current_state_v1").strip(),
                        "metadata_json": str(payload.get("metadata_json") or "{}"),
                        "created_at": now,
                        "updated_at": now,
                    }
                    columns = list(inserted_payload)
                    cursor.execute(
                        (
                            f"INSERT INTO workflow_current_state "
                            f"({', '.join(_quote_identifier(column) for column in columns)}) "
                            f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
                        ),
                        tuple(inserted_payload[column] for column in columns),
                    )
                    committed = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return committed

                stored_sequence = max(0, int(current.get("last_processed_sequence_number") or 0))
                if incoming_sequence is None:
                    raise ValueError(
                        "workflow_current_state existing-row updates require last_processed_sequence_number"
                    )
                if incoming_sequence < stored_sequence:
                    connection.commit()
                    return current

                for identity_column in ("workflow_type",):
                    requested_value = str(payload.get(identity_column) or "").strip()
                    stored_value = str(current.get(identity_column) or "").strip()
                    if requested_value and stored_value and requested_value != stored_value:
                        raise ValueError(f"workflow_current_state immutable identity collision: {identity_column}")

                if incoming_sequence == stored_sequence:
                    mismatches: list[str] = []
                    for column in reducer_owned_columns:
                        if column not in payload:
                            continue
                        requested_value = payload[column]
                        if column in json_columns:
                            matches = _json_load_dict(requested_value) == _json_load_dict(current.get(column))
                        else:
                            normalized_requested = str(requested_value or "").strip()
                            matches = (
                                not normalized_requested
                                or normalized_requested == str(current.get(column) or "").strip()
                            )
                        if not matches:
                            mismatches.append(column)
                    if mismatches:
                        raise ValueError(
                            "workflow_current_state same-sequence reducer collision: " + ", ".join(mismatches)
                        )

                updated_payload = dict(current)
                for column, value in payload.items():
                    if column in {"workflow_run_id", "schema_version"}:
                        continue
                    if column in json_columns:
                        updated_payload[column] = str(value or "{}")
                    elif column == "last_processed_sequence_number":
                        updated_payload[column] = incoming_sequence
                    elif str(value or "").strip():
                        updated_payload[column] = str(value).strip()
                updated_payload["updated_at"] = now
                update_columns = [
                    "operation_id",
                    "workflow_type",
                    "status",
                    "current_stage_key",
                    *json_columns,
                    "last_processed_sequence_number",
                    "reducer_version",
                    "updated_at",
                ]
                if all(current.get(column) == updated_payload.get(column) for column in update_columns[:-1]):
                    connection.commit()
                    return current
                cursor.execute(
                    (
                        "UPDATE workflow_current_state SET "
                        + ", ".join(f"{_quote_identifier(column)} = %s" for column in update_columns)
                        + " WHERE workflow_run_id = %s RETURNING *"
                    ),
                    tuple(updated_payload.get(column) for column in update_columns) + (workflow_run_id,),
                )
                committed = _fetch_one_dict_row(cursor, cursor.fetchone())
            connection.commit()
        return committed

    def upsert_agent_action(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "agent_actions",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "agent_actions":
            raise ValueError("upsert_agent_action requires table_name=agent_actions")
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        payload = _normalize_postgres_row_payload(dict(row or {}))
        action_id = str(payload.get("action_id") or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        action_type = str(payload.get("action_type") or "").strip()
        owner_module = str(payload.get("owner_module") or "").strip()
        operation_type = str(payload.get("operation_type") or "").strip()
        if (
            not action_id
            or not workspace_id
            or not idempotency_key
            or not action_type
            or not owner_module
            or not operation_type
        ):
            return None
        now = _utc_now_sql_timestamp()
        row_payload = {
            "action_id": action_id,
            "workspace_id": workspace_id,
            "conversation_id": str(payload.get("conversation_id") or "").strip(),
            "action_type": action_type,
            "owner_module": owner_module,
            "operation_type": operation_type,
            "target_ref_json": str(payload.get("target_ref_json") or "{}"),
            "input_json": str(payload.get("input_json") or "{}"),
            "request_schema_version": str(payload.get("request_schema_version") or "").strip(),
            "request_schema_digest": str(payload.get("request_schema_digest") or "").strip(),
            "approval_status": str(payload.get("approval_status") or "not_required").strip() or "not_required",
            "approval_policy": str(payload.get("approval_policy") or "not_required").strip() or "not_required",
            "budget_json": str(payload.get("budget_json") or "{}"),
            "idempotency_key": idempotency_key,
            "status": str(payload.get("status") or "planned").strip() or "planned",
            "result_ref_json": str(payload.get("result_ref_json") or "{}"),
            "metadata_json": str(payload.get("metadata_json") or "{}"),
            "created_at": str(payload.get("created_at") or now).strip(),
            "updated_at": str(payload.get("updated_at") or now).strip(),
        }
        columns = list(row_payload.keys())
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM agent_actions
                    WHERE workspace_id = %s AND idempotency_key = %s
                    LIMIT 1
                    """,
                    (workspace_id, idempotency_key),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is not None:
                    _assert_request_schema_pin_identity(existing, row_payload, record_kind="agent_action")
                    connection.commit()
                    return existing
                cursor.execute(
                    (
                        f"INSERT INTO agent_actions ({', '.join(_quote_identifier(column) for column in columns)}) "
                        f"VALUES ({', '.join(['%s'] * len(columns))}) "
                        "ON CONFLICT DO NOTHING RETURNING *"
                    ),
                    tuple(row_payload[column] for column in columns),
                )
                inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                if inserted is not None:
                    connection.commit()
                    return inserted
                cursor.execute(
                    """
                    SELECT * FROM agent_actions
                    WHERE workspace_id = %s AND idempotency_key = %s
                    LIMIT 1
                    """,
                    (workspace_id, idempotency_key),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is None:
                    cursor.execute("SELECT * FROM agent_actions WHERE action_id = %s LIMIT 1", (action_id,))
                    existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is not None:
                    _assert_request_schema_pin_identity(existing, row_payload, record_kind="agent_action")
            connection.commit()
        return existing

    def create_acquisition_plan_preview_uow(
        self,
        *,
        table_name: str = "acquisition_plan_previews",
        action_id: str,
        operation_run_id: str,
        preview_id: str,
        workspace_id: str,
        requester_id: str,
        conversation_id: str,
        input_payload: dict[str, Any],
        target_ref: dict[str, Any],
        budget: dict[str, Any],
        idempotency_key: str,
        request_schema_version: str,
        request_schema_digest: str,
        tool_name: str,
        tool_spec_version: str,
        tool_spec_digest: str,
        result_schema_version: str,
        result_schema_digest: str,
        result_serializer_owner: str,
        result_serializer_revision: str,
        result_serializer_contract_digest: str,
        start_request_schema_version: str,
        start_request_schema_digest: str,
        actor: str,
        source: str,
        ttl_seconds: int,
        lock_timeout_seconds: float = 5.0,
        fault_injection_point: str = "",
    ) -> dict[str, Any] | None:
        """Create one immutable commandless preview aggregate in one PG transaction.

        The method is deliberately specialized.  Generic action submission writes
        its aggregate in several transactions and therefore cannot provide the
        F4a atomicity, replay, or lock-order contract.  This path performs no
        provider/model/network work and emits no outbox row because no result-ready
        outbox consumer exists.
        """

        from .acquisition_plan_preview import (
            ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
            ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
            ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
            ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION,
            build_acquisition_plan_preview,
        )
        from .agent_canary_registry import PLAN_ACQUISITION_TOOL_SPEC

        if _normalize_postgres_identifier(table_name) != "acquisition_plan_previews":
            raise ValueError("create_acquisition_plan_preview_uow requires table_name=acquisition_plan_previews")
        if not all(
            self._require_operation_runtime_table(required_table)
            for required_table in (
                "operation_events",
                "operation_runs",
                "agent_actions",
                "acquisition_plan_previews",
            )
        ):
            return None

        text_values = {
            "action_id": action_id,
            "operation_run_id": operation_run_id,
            "preview_id": preview_id,
            "workspace_id": workspace_id,
            "requester_id": requester_id,
            "idempotency_key": idempotency_key,
            "request_schema_version": request_schema_version,
            "request_schema_digest": request_schema_digest,
            "tool_name": tool_name,
            "tool_spec_version": tool_spec_version,
            "tool_spec_digest": tool_spec_digest,
            "result_schema_version": result_schema_version,
            "result_schema_digest": result_schema_digest,
            "result_serializer_owner": result_serializer_owner,
            "result_serializer_revision": result_serializer_revision,
            "result_serializer_contract_digest": result_serializer_contract_digest,
            "start_request_schema_version": start_request_schema_version,
            "start_request_schema_digest": start_request_schema_digest,
            "actor": actor,
            "source": source,
        }
        noncanonical = [
            name for name, value in text_values.items() if type(value) is not str or not value or value != value.strip()
        ]
        if noncanonical:
            raise ValueError(
                "acquisition plan preview UoW requires non-empty canonical text: " + ", ".join(noncanonical)
            )
        if type(conversation_id) is not str or conversation_id != conversation_id.strip():
            raise ValueError("acquisition plan preview conversation_id must be canonical")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 24 * 60 * 60:
            raise ValueError("acquisition plan preview ttl_seconds must be between 1 and 86400")
        if isinstance(lock_timeout_seconds, bool):
            raise ValueError("acquisition plan preview lock_timeout_seconds must be finite and positive")
        timeout_seconds = float(lock_timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("acquisition plan preview lock_timeout_seconds must be finite and positive")
        if not isinstance(input_payload, dict) or not isinstance(target_ref, dict) or not isinstance(budget, dict):
            raise ValueError("acquisition plan preview payload, target, and budget must be objects")

        identifier_pattern = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
        version_pattern = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
        digest_pattern = re.compile(r"[0-9a-f]{64}")
        for name in ("action_id", "operation_run_id", "preview_id"):
            if identifier_pattern.fullmatch(text_values[name]) is None:
                raise ValueError(f"acquisition plan preview {name} is invalid")
        for name in (
            "request_schema_version",
            "tool_name",
            "tool_spec_version",
            "result_schema_version",
            "result_serializer_owner",
            "result_serializer_revision",
            "start_request_schema_version",
        ):
            if version_pattern.fullmatch(text_values[name]) is None:
                raise ValueError(f"acquisition plan preview {name} is invalid")
        for name in (
            "request_schema_digest",
            "tool_spec_digest",
            "result_schema_digest",
            "result_serializer_contract_digest",
            "start_request_schema_digest",
        ):
            if digest_pattern.fullmatch(text_values[name]) is None:
                raise ValueError(f"acquisition plan preview {name} is invalid")

        expected_result_pins = {
            "request_schema_version": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
            "request_schema_digest": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
            "tool_name": PLAN_ACQUISITION_TOOL_SPEC.tool_name,
            "tool_spec_version": PLAN_ACQUISITION_TOOL_SPEC.tool_spec_version,
            "tool_spec_digest": PLAN_ACQUISITION_TOOL_SPEC.tool_spec_digest,
            "result_schema_version": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_version,
            "result_schema_digest": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_digest,
            "result_serializer_owner": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_owner,
            "result_serializer_revision": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_revision,
            "result_serializer_contract_digest": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_contract_digest,
        }
        mismatched_pins = [name for name, expected in expected_result_pins.items() if text_values[name] != expected]
        if mismatched_pins:
            raise ValueError("acquisition plan preview canonical result pin mismatch: " + ", ".join(mismatched_pins))
        if target_ref.get("workspace_id") != workspace_id or target_ref.get("requester_id") != requester_id:
            raise ValueError("acquisition plan preview owner target mismatch")
        if not json_contract_equal(input_payload.get("budget"), budget):
            raise ValueError("acquisition plan preview budget mismatch")

        allowed_faults = {
            "",
            "after_operation_write",
            "after_action_write",
            "after_preview_write",
            "after_event_write",
            "after_commit",
        }
        if fault_injection_point not in allowed_faults:
            raise ValueError("unsupported acquisition plan preview fault injection point")

        terminal_event_type = "AcquisitionPlanPreviewCreated"
        event_idempotency_key = f"{idempotency_key}:{terminal_event_type}"
        deadline_monotonic = time.monotonic() + timeout_seconds
        retry_attempt = 0
        last_busy_key = f"operation_events:{operation_run_id}"

        def _canonical_rows(
            *,
            preview_revision: int,
            created_at_iso: str,
            expires_at_iso: str,
        ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
            preview_value = build_acquisition_plan_preview(
                input_payload=input_payload,
                target_ref=target_ref,
                preview_id=preview_id,
                preview_revision=preview_revision,
                created_at=created_at_iso,
                expires_at=expires_at_iso,
                intended_start_request_schema_version=start_request_schema_version,
                intended_start_request_schema_digest=start_request_schema_digest,
            )
            preview_record = preview_value.to_record()
            schema_pins = dict(preview_record.get("schema_pins") or {})
            if (
                schema_pins.get("plan_request_schema_version") != request_schema_version
                or schema_pins.get("plan_request_schema_digest") != request_schema_digest
                or schema_pins.get("plan_result_schema_version") != result_schema_version
                or schema_pins.get("plan_result_schema_digest") != result_schema_digest
                or schema_pins.get("intended_start_request_schema_version") != start_request_schema_version
                or schema_pins.get("intended_start_request_schema_digest") != start_request_schema_digest
            ):
                raise ValueError("acquisition plan preview embedded schema pin mismatch")

            company_target = dict(preview_record.get("company_target") or {})
            effective_request = dict(preview_record.get("effective_request") or {})
            planning_manifest = dict(preview_record.get("provider_planning_manifest") or {})
            canonical_input = {
                "cohort_selection": effective_request.get("cohort_selection"),
                "source_preferences": effective_request.get("source_preferences"),
                "coverage_intent": effective_request.get("coverage_intent"),
                "thematic_constraints": effective_request.get("thematic_constraints"),
                "provider_mode_intent": effective_request.get("provider_mode_intent"),
                "budget": effective_request.get("budget"),
            }
            canonical_company_request = {
                key: value
                for key, value in company_target.items()
                if key not in {"schema_version", "company_target_digest"}
            }
            canonical_target = {
                "workspace_id": workspace_id,
                "requester_id": requester_id,
                "company_target": canonical_company_request,
            }
            result_ref = {
                "schema_version": "acquisition_plan_preview_result_ref.v1",
                "preview_id": preview_id,
                "preview_revision": preview_revision,
                "preview_digest": str(preview_record.get("preview_digest") or ""),
                "result_schema_version": result_schema_version,
                "result_schema_digest": result_schema_digest,
                "result_serializer_owner": result_serializer_owner,
                "result_serializer_revision": result_serializer_revision,
                "result_serializer_contract_digest": result_serializer_contract_digest,
            }
            aggregate_metadata = {
                "operation_runtime_contract": "track_d_d1n_f4a_commandless_preview_v1",
                "request_schema_status": "validated",
                "result_contract_pinned": True,
                "commandless": True,
                "outbox_required": False,
            }
            created_at_sql = created_at_iso.replace("T", " ").removesuffix("Z")
            action_row = {
                "action_id": action_id,
                "workspace_id": workspace_id,
                "conversation_id": conversation_id,
                "action_type": "plan_acquisition",
                "owner_module": "planner",
                "operation_type": "acquisition_plan",
                "target_ref_json": _json_dump(canonical_target),
                "input_json": _json_dump(canonical_input),
                "request_schema_version": request_schema_version,
                "request_schema_digest": request_schema_digest,
                "tool_name": tool_name,
                "tool_spec_version": tool_spec_version,
                "tool_spec_digest": tool_spec_digest,
                "result_schema_version": result_schema_version,
                "result_schema_digest": result_schema_digest,
                "result_serializer_owner": result_serializer_owner,
                "result_serializer_revision": result_serializer_revision,
                "result_serializer_contract_digest": result_serializer_contract_digest,
                "approval_status": "not_required",
                "approval_policy": "not_required",
                "budget_json": _json_dump(budget),
                "idempotency_key": idempotency_key,
                "status": "completed",
                "result_ref_json": _json_dump(result_ref),
                "metadata_json": _json_dump(aggregate_metadata),
                "created_at": created_at_sql,
                "updated_at": created_at_sql,
            }
            operation_row = {
                "operation_run_id": operation_run_id,
                "workspace_id": workspace_id,
                "action_id": action_id,
                "owner_module": "planner",
                "operation_type": "acquisition_plan",
                "request_schema_version": request_schema_version,
                "request_schema_digest": request_schema_digest,
                "tool_name": tool_name,
                "tool_spec_version": tool_spec_version,
                "tool_spec_digest": tool_spec_digest,
                "result_schema_version": result_schema_version,
                "result_schema_digest": result_schema_digest,
                "result_serializer_owner": result_serializer_owner,
                "result_serializer_revision": result_serializer_revision,
                "result_serializer_contract_digest": result_serializer_contract_digest,
                "status": "completed",
                "progress_json": _json_dump({"phase": "completed", "commandless": True}),
                "workflow_ref_json": _json_dump({}),
                "cost_budget_json": _json_dump(budget),
                "idempotency_key": idempotency_key,
                "result_ref_json": _json_dump(result_ref),
                "metadata_json": _json_dump(aggregate_metadata),
                "started_at": created_at_sql,
                "completed_at": created_at_sql,
                "created_at": created_at_sql,
                "updated_at": created_at_sql,
            }
            preview_row = {
                "preview_id": preview_id,
                "workspace_id": workspace_id,
                "requester_id": requester_id,
                "action_id": action_id,
                "operation_run_id": operation_run_id,
                "canonical_company_id": str(company_target.get("canonical_company_id") or ""),
                "company_registry_revision": str(company_target.get("company_registry_revision") or ""),
                "company_registry_digest": str(company_target.get("company_registry_digest") or ""),
                "company_target_digest": str(company_target.get("company_target_digest") or ""),
                "idempotency_key": idempotency_key,
                "preview_revision": preview_revision,
                "preview_digest": str(preview_record.get("preview_digest") or ""),
                "effective_request_digest": str(preview_record.get("effective_request_digest") or ""),
                "provider_manifest_digest": str(planning_manifest.get("manifest_digest") or ""),
                "physical_query_digest": str(planning_manifest.get("physical_query_digest") or ""),
                "request_schema_version": request_schema_version,
                "request_schema_digest": request_schema_digest,
                "result_schema_version": result_schema_version,
                "result_schema_digest": result_schema_digest,
                "result_serializer_owner": result_serializer_owner,
                "result_serializer_revision": result_serializer_revision,
                "result_serializer_contract_digest": result_serializer_contract_digest,
                "start_request_schema_version": start_request_schema_version,
                "start_request_schema_digest": start_request_schema_digest,
                "preview_json": preview_record,
                "schema_version": ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION,
                "created_at": created_at_iso,
                "expires_at": expires_at_iso,
            }
            event_payload = {
                "contract": "track_d_d1n_f4a_commandless_preview_v1",
                "action_type": "plan_acquisition",
                "owner_module": "planner",
                "operation_type": "acquisition_plan",
                "result_ref": result_ref,
                "module_state_mutated": False,
                "outbox_required": False,
            }
            event_row = {
                "workspace_id": workspace_id,
                "event_stream_id": operation_run_id,
                "operation_run_id": operation_run_id,
                "action_id": action_id,
                "event_family": "operation_event",
                "event_type": terminal_event_type,
                "idempotency_key": event_idempotency_key,
                "occurred_at": created_at_sql,
                "recorded_at": created_at_sql,
                "actor": actor,
                "source": source,
                "payload_json": _json_dump(event_payload),
                "schema_version": "operation_event_v1",
                "created_at": created_at_sql,
            }
            return action_row, operation_row, preview_row, event_row, preview_record

        def _validate_exact_bundle(
            *,
            action_row: dict[str, Any],
            operation_row: dict[str, Any],
            preview_row: dict[str, Any],
            event_row: dict[str, Any],
            expected_action: dict[str, Any],
            expected_operation: dict[str, Any],
            expected_preview: dict[str, Any],
            expected_event: dict[str, Any],
        ) -> None:
            scalar_groups = (
                (
                    "action",
                    action_row,
                    expected_action,
                    (
                        "action_id",
                        "workspace_id",
                        "conversation_id",
                        "action_type",
                        "owner_module",
                        "operation_type",
                        "request_schema_version",
                        "request_schema_digest",
                        "tool_name",
                        "tool_spec_version",
                        "tool_spec_digest",
                        "result_schema_version",
                        "result_schema_digest",
                        "result_serializer_owner",
                        "result_serializer_revision",
                        "result_serializer_contract_digest",
                        "approval_status",
                        "approval_policy",
                        "idempotency_key",
                        "status",
                        "created_at",
                        "updated_at",
                    ),
                ),
                (
                    "operation",
                    operation_row,
                    expected_operation,
                    (
                        "operation_run_id",
                        "workspace_id",
                        "action_id",
                        "owner_module",
                        "operation_type",
                        "request_schema_version",
                        "request_schema_digest",
                        "tool_name",
                        "tool_spec_version",
                        "tool_spec_digest",
                        "result_schema_version",
                        "result_schema_digest",
                        "result_serializer_owner",
                        "result_serializer_revision",
                        "result_serializer_contract_digest",
                        "status",
                        "idempotency_key",
                        "started_at",
                        "completed_at",
                        "created_at",
                        "updated_at",
                    ),
                ),
                (
                    "preview",
                    preview_row,
                    expected_preview,
                    (
                        "preview_id",
                        "workspace_id",
                        "requester_id",
                        "action_id",
                        "operation_run_id",
                        "canonical_company_id",
                        "company_registry_revision",
                        "company_registry_digest",
                        "company_target_digest",
                        "idempotency_key",
                        "preview_revision",
                        "preview_digest",
                        "effective_request_digest",
                        "provider_manifest_digest",
                        "physical_query_digest",
                        "request_schema_version",
                        "request_schema_digest",
                        "result_schema_version",
                        "result_schema_digest",
                        "result_serializer_owner",
                        "result_serializer_revision",
                        "result_serializer_contract_digest",
                        "start_request_schema_version",
                        "start_request_schema_digest",
                        "schema_version",
                    ),
                ),
                (
                    "event",
                    event_row,
                    expected_event,
                    (
                        "workspace_id",
                        "event_stream_id",
                        "operation_run_id",
                        "action_id",
                        "event_family",
                        "event_type",
                        "idempotency_key",
                        "occurred_at",
                        "recorded_at",
                        "actor",
                        "source",
                        "schema_version",
                        "created_at",
                    ),
                ),
            )
            for kind, actual, expected, fields in scalar_groups:
                mismatches = [
                    field
                    for field in fields
                    if str(actual.get(field) or "").strip() != str(expected.get(field) or "").strip()
                ]
                if mismatches:
                    raise ValueError(
                        f"acquisition plan preview {kind} immutable identity collision: " + ", ".join(mismatches)
                    )
            json_groups = (
                ("action.target_ref", action_row.get("target_ref_json"), expected_action["target_ref_json"]),
                ("action.input", action_row.get("input_json"), expected_action["input_json"]),
                ("action.budget", action_row.get("budget_json"), expected_action["budget_json"]),
                ("action.result_ref", action_row.get("result_ref_json"), expected_action["result_ref_json"]),
                ("action.metadata", action_row.get("metadata_json"), expected_action["metadata_json"]),
                (
                    "operation.progress",
                    operation_row.get("progress_json"),
                    expected_operation["progress_json"],
                ),
                (
                    "operation.workflow_ref",
                    operation_row.get("workflow_ref_json"),
                    expected_operation["workflow_ref_json"],
                ),
                (
                    "operation.cost_budget",
                    operation_row.get("cost_budget_json"),
                    expected_operation["cost_budget_json"],
                ),
                (
                    "operation.result_ref",
                    operation_row.get("result_ref_json"),
                    expected_operation["result_ref_json"],
                ),
                (
                    "operation.metadata",
                    operation_row.get("metadata_json"),
                    expected_operation["metadata_json"],
                ),
                ("preview.preview", preview_row.get("preview_json"), expected_preview["preview_json"]),
                ("event.payload", event_row.get("payload_json"), expected_event["payload_json"]),
            )
            for label, actual, expected in json_groups:
                if not json_contract_equal(_json_load_dict(actual), _json_load_dict(expected)):
                    raise ValueError(f"acquisition plan preview {label} immutable identity collision")
            sequence_number = int(event_row.get("sequence_number") or 0)
            expected_event_id = (
                "opevt_"
                + sha1(f"{operation_run_id}:{sequence_number}:{event_idempotency_key}".encode("utf-8")).hexdigest()[:24]
            )
            if sequence_number <= 0 or str(event_row.get("event_id") or "").strip() != expected_event_id:
                raise ValueError("acquisition plan preview event sequence identity collision")

        while True:
            remaining_seconds = deadline_monotonic - time.monotonic()
            if remaining_seconds <= 0:
                raise ControlPlaneAdvisoryLockBusy(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            connection = self._connect_with_timeout(remaining_seconds)
            committed_bundle: dict[str, Any] | None = None
            try:
                with connection.cursor() as cursor:
                    remaining_milliseconds = max(1, int((deadline_monotonic - time.monotonic()) * 1000))
                    cursor.execute("SELECT set_config('lock_timeout', %s, true)", (f"{remaining_milliseconds}ms",))
                    lock_groups = (
                        (f"operation_events:{operation_run_id}",),
                        tuple(
                            sorted(
                                {
                                    f"operation_runs:id:{operation_run_id}",
                                    f"operation_runs:idempotency:{workspace_id}:{idempotency_key}",
                                }
                            )
                        ),
                        tuple(
                            sorted(
                                {
                                    f"agent_actions:id:{action_id}",
                                    f"agent_actions:idempotency:{workspace_id}:{idempotency_key}",
                                }
                            )
                        ),
                        tuple(
                            sorted(
                                {
                                    f"acquisition_plan_previews:id:{preview_id}",
                                    f"acquisition_plan_previews:idempotency:{workspace_id}:{idempotency_key}",
                                }
                            )
                        ),
                    )
                    for lock_group in lock_groups:
                        for lock_key in lock_group:
                            last_busy_key = lock_key
                            if not self._try_acquire_transaction_lock(cursor, lock_key):
                                raise _TransactionAdvisoryLockBusy

                    cursor.execute(
                        """
                        SELECT * FROM operation_runs
                        WHERE operation_run_id = %s OR (workspace_id = %s AND idempotency_key = %s)
                        ORDER BY operation_run_id
                        FOR UPDATE
                        """,
                        (operation_run_id, workspace_id, idempotency_key),
                    )
                    operation_candidates = _fetch_all_dict_rows(cursor)
                    if len(operation_candidates) > 1:
                        raise ValueError("acquisition plan preview operation split identity collision")
                    existing_operation = operation_candidates[0] if operation_candidates else None

                    cursor.execute(
                        """
                        SELECT * FROM agent_actions
                        WHERE action_id = %s OR (workspace_id = %s AND idempotency_key = %s)
                        ORDER BY action_id
                        FOR UPDATE
                        """,
                        (action_id, workspace_id, idempotency_key),
                    )
                    action_candidates = _fetch_all_dict_rows(cursor)
                    if len(action_candidates) > 1:
                        raise ValueError("acquisition plan preview action split identity collision")
                    existing_action = action_candidates[0] if action_candidates else None

                    cursor.execute(
                        """
                        SELECT * FROM acquisition_plan_previews
                        WHERE preview_id = %s OR (workspace_id = %s AND idempotency_key = %s)
                        ORDER BY preview_id
                        FOR UPDATE
                        """,
                        (preview_id, workspace_id, idempotency_key),
                    )
                    preview_candidates = _fetch_all_dict_rows(cursor)
                    if len(preview_candidates) > 1:
                        raise ValueError("acquisition plan preview split identity collision")
                    existing_preview = preview_candidates[0] if preview_candidates else None

                    cursor.execute(
                        """
                        SELECT * FROM operation_events
                        WHERE (event_stream_id = %s AND idempotency_key = %s)
                           OR (operation_run_id = %s AND event_type = %s)
                        ORDER BY event_id
                        FOR UPDATE
                        """,
                        (operation_run_id, event_idempotency_key, operation_run_id, terminal_event_type),
                    )
                    event_candidates = _fetch_all_dict_rows(cursor)
                    if len(event_candidates) > 1:
                        raise ValueError("acquisition plan preview event split identity collision")
                    existing_event = event_candidates[0] if event_candidates else None

                    existing_rows = (
                        existing_operation,
                        existing_action,
                        existing_preview,
                        existing_event,
                    )
                    existing_count = sum(row is not None for row in existing_rows)
                    if existing_count not in {0, 4}:
                        raise ValueError("acquisition plan preview partial aggregate collision")

                    if existing_count == 4:
                        assert existing_preview is not None
                        stored_preview = _json_load_dict(existing_preview.get("preview_json"))
                        preview_revision = int(existing_preview.get("preview_revision") or 0)
                        created_at_iso = str(stored_preview.get("created_at") or "")
                        expires_at_iso = str(stored_preview.get("expires_at") or "")
                        expected_action, expected_operation, expected_preview, expected_event, _ = _canonical_rows(
                            preview_revision=preview_revision,
                            created_at_iso=created_at_iso,
                            expires_at_iso=expires_at_iso,
                        )
                        assert existing_action is not None
                        assert existing_operation is not None
                        assert existing_event is not None
                        _validate_exact_bundle(
                            action_row=existing_action,
                            operation_row=existing_operation,
                            preview_row=existing_preview,
                            event_row=existing_event,
                            expected_action=expected_action,
                            expected_operation=expected_operation,
                            expected_preview=expected_preview,
                            expected_event=expected_event,
                        )
                        committed_bundle = {
                            "outcome": "replayed",
                            "replayed": True,
                            "action": existing_action,
                            "operation_run": existing_operation,
                            "preview": existing_preview,
                            "event": existing_event,
                        }
                    else:
                        cursor.execute(
                            """
                            SELECT
                                nextval('acquisition_plan_preview_revision_seq') AS preview_revision,
                                date_trunc('second', transaction_timestamp()) AS created_at,
                                to_char(
                                    date_trunc('second', transaction_timestamp()) AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                                ) AS created_at_iso,
                                to_char(
                                    (date_trunc('second', transaction_timestamp()) + (%s * INTERVAL '1 second'))
                                        AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                                ) AS expires_at_iso
                            """,
                            (ttl_seconds,),
                        )
                        allocation = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                        preview_revision = int(allocation.get("preview_revision") or 0)
                        created_at_iso = str(allocation.get("created_at_iso") or "")
                        expires_at_iso = str(allocation.get("expires_at_iso") or "")
                        (
                            expected_action,
                            expected_operation,
                            expected_preview,
                            expected_event,
                            preview_record,
                        ) = _canonical_rows(
                            preview_revision=preview_revision,
                            created_at_iso=created_at_iso,
                            expires_at_iso=expires_at_iso,
                        )

                        operation_columns = list(expected_operation)
                        cursor.execute(
                            (
                                f"INSERT INTO operation_runs "
                                f"({', '.join(_quote_identifier(column) for column in operation_columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(operation_columns))}) RETURNING *"
                            ),
                            tuple(expected_operation[column] for column in operation_columns),
                        )
                        inserted_operation = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if fault_injection_point == "after_operation_write":
                            raise RuntimeError("injected acquisition plan preview fault after operation write")

                        action_columns = list(expected_action)
                        cursor.execute(
                            (
                                f"INSERT INTO agent_actions "
                                f"({', '.join(_quote_identifier(column) for column in action_columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(action_columns))}) RETURNING *"
                            ),
                            tuple(expected_action[column] for column in action_columns),
                        )
                        inserted_action = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if fault_injection_point == "after_action_write":
                            raise RuntimeError("injected acquisition plan preview fault after action write")

                        preview_insert = dict(expected_preview)
                        preview_insert["preview_json"] = _json_dump(preview_record)
                        preview_insert["created_at"] = created_at_iso
                        preview_insert["expires_at"] = expires_at_iso
                        preview_columns = list(preview_insert)
                        cursor.execute(
                            (
                                f"INSERT INTO acquisition_plan_previews "
                                f"({', '.join(_quote_identifier(column) for column in preview_columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(preview_columns))}) RETURNING *"
                            ),
                            tuple(preview_insert[column] for column in preview_columns),
                        )
                        inserted_preview = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if fault_injection_point == "after_preview_write":
                            raise RuntimeError("injected acquisition plan preview fault after preview write")

                        inserted_event = self._append_operation_event_with_cursor(
                            cursor,
                            payload=expected_event,
                            now=expected_event["created_at"],
                            acquire_stream_lock=False,
                        )
                        if fault_injection_point == "after_event_write":
                            raise RuntimeError("injected acquisition plan preview fault after event write")
                        if not all((inserted_operation, inserted_action, inserted_preview, inserted_event)):
                            raise RuntimeError("acquisition plan preview UoW failed to persist a complete bundle")
                        _validate_exact_bundle(
                            action_row=inserted_action,
                            operation_row=inserted_operation,
                            preview_row=inserted_preview,
                            event_row=inserted_event,
                            expected_action=expected_action,
                            expected_operation=expected_operation,
                            expected_preview=expected_preview,
                            expected_event=expected_event,
                        )
                        committed_bundle = {
                            "outcome": "created",
                            "replayed": False,
                            "action": inserted_action,
                            "operation_run": inserted_operation,
                            "preview": inserted_preview,
                            "event": inserted_event,
                        }
                connection.commit()
                if fault_injection_point == "after_commit":
                    raise RuntimeError("injected acquisition plan preview fault after commit")
                assert committed_bundle is not None
                return committed_bundle
            except _TransactionAdvisoryLockBusy:
                connection.rollback()
                remaining_seconds = deadline_monotonic - time.monotonic()
                if remaining_seconds <= 0:
                    raise ControlPlaneAdvisoryLockBusy(
                        lock_key=last_busy_key,
                        timeout_seconds=timeout_seconds,
                    )
                time.sleep(min(_SESSION_ADVISORY_LOCK_POLL_SECONDS, remaining_seconds))
            except Exception as exc:
                connection.rollback()
                sqlstate = str(getattr(exc, "sqlstate", "") or "").strip().upper()
                retry_attempt += 1
                if sqlstate == "55P03":
                    remaining_seconds = deadline_monotonic - time.monotonic()
                    if remaining_seconds <= 0:
                        raise ControlPlaneAdvisoryLockBusy(
                            lock_key=last_busy_key,
                            timeout_seconds=timeout_seconds,
                        ) from exc
                    time.sleep(min(_SESSION_ADVISORY_LOCK_POLL_SECONDS, remaining_seconds))
                elif _is_retryable_postgres_exception(exc) and retry_attempt < _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    remaining_seconds = deadline_monotonic - time.monotonic()
                    if remaining_seconds <= 0:
                        raise
                    time.sleep(
                        min(
                            _control_plane_postgres_retry_delay_seconds(retry_attempt),
                            remaining_seconds,
                        )
                    )
                else:
                    raise
            finally:
                connection.close()

    def submit_acquisition_start_v2_action_uow(
        self,
        *,
        table_name: str = "agent_actions",
        occurrence: Any,
        lock_timeout_seconds: float = 5.0,
        fault_injection_point: str = "",
    ) -> dict[str, Any] | None:
        """Create or exact-reload the pending v2 acquisition-start Action bundle."""

        if _normalize_postgres_identifier(table_name) != "agent_actions":
            raise ValueError("submit_acquisition_start_v2_action_uow requires table_name=agent_actions")
        from .acquisition_start_v2_postgres import submit_acquisition_start_v2_action_uow

        return submit_acquisition_start_v2_action_uow(
            self,
            occurrence=occurrence,
            lock_timeout_seconds=lock_timeout_seconds,
            fault_injection_point=fault_injection_point,
        )

    def reserve_agent_tool_result_slot(
        self,
        *,
        table_name: str = "agent_tool_result_slots",
        occurrence: Any,
        lock_timeout_seconds: float = 5.0,
        fault_injection_point: str = "",
    ) -> dict[str, Any] | None:
        """Reserve one exact logical Agent-tool occurrence without executing it."""

        if _normalize_postgres_identifier(table_name) != "agent_tool_result_slots":
            raise ValueError("reserve_agent_tool_result_slot requires table_name=agent_tool_result_slots")
        from .agent_tool_result_postgres import reserve_agent_tool_result_slot

        return reserve_agent_tool_result_slot(
            self,
            occurrence=occurrence,
            lock_timeout_seconds=lock_timeout_seconds,
            fault_injection_point=fault_injection_point,
        )

    def accept_acquisition_plan_tool_result_uow(
        self,
        *,
        table_name: str = "agent_tool_result_slots",
        occurrence: Any,
        terminal: Any,
        attempted_slot_generation: int,
        lock_timeout_seconds: float = 5.0,
        fault_injection_point: str = "",
    ) -> dict[str, Any] | None:
        """Accept the exact plan-preview owner result and journal it atomically."""

        if _normalize_postgres_identifier(table_name) != "agent_tool_result_slots":
            raise ValueError("accept_acquisition_plan_tool_result_uow requires table_name=agent_tool_result_slots")
        from .agent_tool_result_postgres import accept_acquisition_plan_tool_result_uow

        return accept_acquisition_plan_tool_result_uow(
            self,
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=attempted_slot_generation,
            lock_timeout_seconds=lock_timeout_seconds,
            fault_injection_point=fault_injection_point,
        )

    def prepare_inspect_operation_tool_result(
        self,
        *,
        table_name: str = "agent_tool_result_slots",
        occurrence: Any,
        result_attempt_id: str,
        provider_call_id: str,
        tool_call_id: str,
        action_id: str,
        operation_run_id: str,
        lock_timeout_seconds: float = 5.0,
    ) -> Any:
        """Build one exact read-only Operation query result from locked owner rows."""

        if _normalize_postgres_identifier(table_name) != "agent_tool_result_slots":
            raise ValueError("prepare_inspect_operation_tool_result requires table_name=agent_tool_result_slots")
        from .agent_tool_result_postgres import prepare_inspect_operation_tool_result

        return prepare_inspect_operation_tool_result(
            self,
            occurrence=occurrence,
            result_attempt_id=result_attempt_id,
            provider_call_id=provider_call_id,
            tool_call_id=tool_call_id,
            action_id=action_id,
            operation_run_id=operation_run_id,
            lock_timeout_seconds=lock_timeout_seconds,
        )

    def accept_inspect_operation_tool_result_uow(
        self,
        *,
        table_name: str = "agent_tool_result_slots",
        occurrence: Any,
        terminal: Any,
        attempted_slot_generation: int,
        lock_timeout_seconds: float = 5.0,
        fault_injection_point: str = "",
    ) -> dict[str, Any] | None:
        """Accept one exact Operation event-revision result and immutable journal."""

        if _normalize_postgres_identifier(table_name) != "agent_tool_result_slots":
            raise ValueError("accept_inspect_operation_tool_result_uow requires table_name=agent_tool_result_slots")
        from .agent_tool_result_postgres import accept_inspect_operation_tool_result_uow

        return accept_inspect_operation_tool_result_uow(
            self,
            occurrence=occurrence,
            terminal=terminal,
            attempted_slot_generation=attempted_slot_generation,
            lock_timeout_seconds=lock_timeout_seconds,
            fault_injection_point=fault_injection_point,
        )

    def upsert_workflow_runtime_identity_row(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str,
        id_column: str,
        immutable_columns: tuple[str, ...],
        terminal_statuses: tuple[str, ...],
        write_once: bool,
        expected_command_claim: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        normalized_table = _normalize_postgres_identifier(table_name)
        config = _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG.get(normalized_table)
        if config is None:
            raise ValueError("upsert_workflow_runtime_identity_row requires a registered workflow runtime table")
        configured_id_column = str(config["id_column"])
        configured_immutable_columns = tuple(config["immutable_columns"])
        configured_terminal_statuses = tuple(config["terminal_statuses"])
        configured_write_once = bool(config["write_once"])
        if _normalize_postgres_identifier(id_column) != configured_id_column:
            raise ValueError(f"upsert_workflow_runtime_identity_row invalid id_column for {normalized_table}")
        if tuple(immutable_columns) != configured_immutable_columns:
            raise ValueError(f"upsert_workflow_runtime_identity_row immutable contract mismatch for {normalized_table}")
        if tuple(terminal_statuses) != configured_terminal_statuses:
            raise ValueError(f"upsert_workflow_runtime_identity_row terminal contract mismatch for {normalized_table}")
        if bool(write_once) != configured_write_once:
            raise ValueError(
                f"upsert_workflow_runtime_identity_row write-once contract mismatch for {normalized_table}"
            )
        if not self._require_operation_runtime_table(normalized_table):
            return None

        payload = _normalize_postgres_row_payload(dict(row or {}))
        configured_columns = tuple(config["columns"])
        unknown_columns = set(payload) - set(configured_columns)
        if unknown_columns:
            raise ValueError(
                f"upsert_workflow_runtime_identity_row unknown columns for {normalized_table}: "
                + ", ".join(sorted(unknown_columns))
            )
        row_id = str(payload.get(configured_id_column) or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        if not row_id or not workspace_id or not idempotency_key:
            return None
        expected_claim = dict(expected_command_claim or {})
        if expected_claim and normalized_table not in {"workflow_activity_runs", "workflow_activity_attempts"}:
            raise ValueError("expected_command_claim is supported only for workflow activity rows")
        expected_command_id = str(expected_claim.get("command_id") or "").strip()
        expected_command_attempt = max(0, int(expected_claim.get("attempt") or 0))
        expected_lease_owner = str(expected_claim.get("lease_owner") or "").strip()
        expected_lease_expires_at = str(expected_claim.get("lease_expires_at") or "").strip()
        expected_command_type = str(expected_claim.get("command_type") or "").strip()
        expected_command_owner = str(expected_claim.get("owner") or "").strip()
        if expected_claim and (
            not expected_command_id
            or expected_command_attempt <= 0
            or not expected_lease_owner
            or not expected_lease_expires_at
            or not expected_command_type
            or not expected_command_owner
            or str(payload.get("command_id") or "").strip() != expected_command_id
        ):
            raise ValueError("expected_command_claim requires one exact workflow command identity")
        d1m_source_claim_guard = bool(
            expected_claim
            and normalized_table in {"workflow_activity_runs", "workflow_activity_attempts"}
            and expected_command_type == "company.public_web.source.collect"
            and expected_command_owner == "company_public_web_owner"
        )
        d1m_source_attempt_identities = (
            {
                attempt_number: _company_public_web_source_attempt_identity(expected_command_id, attempt_number)
                for attempt_number in range(1, expected_command_attempt + 1)
            }
            if d1m_source_claim_guard
            else {}
        )

        columns = [column for column in configured_columns if column in payload]
        if configured_id_column not in columns:
            return None
        quoted_table = _quote_identifier(normalized_table)
        quoted_id_column = _quote_identifier(configured_id_column)
        json_object_columns = tuple(config["json_object_columns"])
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        lock_key_values = {
                            f"workflow_runtime:{normalized_table}:id:{row_id}",
                            (f"workflow_runtime:{normalized_table}:idempotency:{workspace_id}:{idempotency_key}"),
                        }
                        for prior_attempt_id, prior_attempt_key in d1m_source_attempt_identities.values():
                            lock_key_values.update(
                                {
                                    f"workflow_runtime:workflow_activity_attempts:id:{prior_attempt_id}",
                                    (
                                        "workflow_runtime:workflow_activity_attempts:idempotency:"
                                        f"{workspace_id}:{prior_attempt_key}"
                                    ),
                                }
                            )
                        lock_keys = sorted(lock_key_values)
                        for lock_key in lock_keys:
                            self._acquire_transaction_lock(cursor, lock_key)

                        if expected_claim:
                            cursor.execute(
                                "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
                                (expected_command_id,),
                            )
                            current_command = _fetch_one_dict_row(cursor, cursor.fetchone())
                            cursor.execute(
                                "SELECT (NULLIF(%s, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()",
                                (str((current_command or {}).get("lease_expires_at") or "").strip(),),
                            )
                            lease_active_row = cursor.fetchone()
                            lease_active = bool(
                                lease_active_row[0]
                                if isinstance(lease_active_row, (list, tuple)) and lease_active_row
                                else next(iter(lease_active_row.values()), False)
                                if isinstance(lease_active_row, dict)
                                else lease_active_row
                            )
                            if (
                                current_command is None
                                or str(current_command.get("status") or "").strip() != "running"
                                or str(current_command.get("command_type") or "").strip() != expected_command_type
                                or str(current_command.get("owner") or "").strip() != expected_command_owner
                                or int(current_command.get("attempt") or 0) != expected_command_attempt
                                or str(current_command.get("lease_owner") or "").strip() != expected_lease_owner
                                or str(current_command.get("lease_expires_at") or "").strip()
                                != expected_lease_expires_at
                                or not lease_active
                            ):
                                connection.commit()
                                return None

                            if d1m_source_claim_guard:
                                command_payload = _json_load_dict(current_command.get("payload_json"))
                                command_options = _json_load_dict(command_payload.get("options"))
                                expected_activity_run_id, expected_activity_key = (
                                    _company_public_web_source_activity_identity(expected_command_id)
                                )
                                expected_attempt_id, expected_attempt_key = d1m_source_attempt_identities[
                                    expected_command_attempt
                                ]
                                expected_workflow_run_id = str(current_command.get("workflow_run_id") or "").strip()
                                expected_operation_run_id = str(
                                    current_command.get("operation_id") or command_payload.get("operation_run_id") or ""
                                ).strip()
                                expected_workspace_id = (
                                    str(command_payload.get("workspace_id") or "default").strip() or "default"
                                )
                                expected_provider = str(
                                    command_options.get("collection_mode")
                                    or command_payload.get("collection_mode")
                                    or "seed_url_only"
                                ).strip()
                                expected_target_company = str(
                                    command_payload.get("target_company") or command_payload.get("company") or ""
                                ).strip()
                                expected_company_key = str(command_payload.get("company_key") or "").strip()
                                requested_metadata = _json_load_dict(payload.get("metadata_json"))
                                if normalized_table == "workflow_activity_runs":
                                    requested_identity_valid = bool(
                                        row_id == expected_activity_run_id
                                        and idempotency_key == expected_activity_key
                                        and workspace_id == expected_workspace_id
                                        and str(payload.get("workflow_run_id") or "").strip()
                                        == expected_workflow_run_id
                                        and str(payload.get("operation_run_id") or "").strip()
                                        == expected_operation_run_id
                                        and not str(payload.get("acquisition_run_id") or "").strip()
                                        and not str(payload.get("parent_activity_run_id") or "").strip()
                                        and str(payload.get("activity_type") or "").strip()
                                        == "company.public_web.source.collect"
                                        and str(payload.get("owner") or "").strip() == "company_public_web_owner"
                                        and str(payload.get("status") or "").strip() == "running"
                                        and str(requested_metadata.get("lease_owner") or "").strip()
                                        == expected_lease_owner
                                    )
                                else:
                                    requested_identity_valid = bool(
                                        row_id == expected_attempt_id
                                        and idempotency_key == expected_attempt_key
                                        and workspace_id == expected_workspace_id
                                        and str(payload.get("activity_run_id") or "").strip()
                                        == expected_activity_run_id
                                        and str(payload.get("workflow_run_id") or "").strip()
                                        == expected_workflow_run_id
                                        and str(payload.get("command_id") or "").strip() == expected_command_id
                                        and int(payload.get("attempt_number") or 0) == expected_command_attempt
                                        and str(payload.get("provider") or "").strip() == expected_provider
                                        and str(payload.get("status") or "").strip() == "running"
                                        and str(requested_metadata.get("lease_owner") or "").strip()
                                        == expected_lease_owner
                                    )
                                if not requested_identity_valid:
                                    connection.rollback()
                                    return None

                                cursor.execute(
                                    """
                                    SELECT * FROM workflow_activity_runs
                                    WHERE command_id = %s
                                       OR activity_run_id = %s
                                       OR (workspace_id = %s AND idempotency_key = %s)
                                    ORDER BY activity_run_id
                                    FOR UPDATE
                                    """,
                                    (
                                        expected_command_id,
                                        expected_activity_run_id,
                                        expected_workspace_id,
                                        expected_activity_key,
                                    ),
                                )
                                existing_activity_rows = _fetch_all_dict_rows(cursor)
                                if len(existing_activity_rows) > 1:
                                    connection.rollback()
                                    return None
                                existing_activity = existing_activity_rows[0] if existing_activity_rows else None
                                if existing_activity is not None:
                                    existing_activity_metadata = _json_load_dict(existing_activity.get("metadata_json"))
                                    existing_activity_status = str(existing_activity.get("status") or "").strip()
                                    existing_activity_valid = bool(
                                        str(existing_activity.get("activity_run_id") or "").strip()
                                        == expected_activity_run_id
                                        and (
                                            str(existing_activity.get("workspace_id") or "default").strip() or "default"
                                        )
                                        == expected_workspace_id
                                        and str(existing_activity.get("workflow_run_id") or "").strip()
                                        == expected_workflow_run_id
                                        and str(existing_activity.get("operation_run_id") or "").strip()
                                        == expected_operation_run_id
                                        and not str(existing_activity.get("acquisition_run_id") or "").strip()
                                        and str(existing_activity.get("command_id") or "").strip()
                                        == expected_command_id
                                        and not str(existing_activity.get("parent_activity_run_id") or "").strip()
                                        and str(existing_activity.get("activity_type") or "").strip()
                                        == "company.public_web.source.collect"
                                        and str(existing_activity.get("owner") or "").strip()
                                        == "company_public_web_owner"
                                        and str(existing_activity.get("idempotency_key") or "").strip()
                                        == expected_activity_key
                                        and existing_activity_status in {"planned", "queued", "running", "retry_wait"}
                                        and str(existing_activity_metadata.get("lease_owner") or "").strip()
                                    )
                                    if normalized_table == "workflow_activity_attempts":
                                        existing_activity_valid = bool(
                                            existing_activity_valid
                                            and existing_activity_status == "running"
                                            and str(existing_activity_metadata.get("lease_owner") or "").strip()
                                            == expected_lease_owner
                                        )
                                    if not existing_activity_valid:
                                        connection.rollback()
                                        return None
                                elif normalized_table == "workflow_activity_attempts":
                                    connection.rollback()
                                    return None

                                deterministic_attempt_ids = [
                                    identity[0] for identity in d1m_source_attempt_identities.values()
                                ]
                                deterministic_attempt_keys = [
                                    identity[1] for identity in d1m_source_attempt_identities.values()
                                ]
                                attempt_id_placeholders = ", ".join(["%s"] * len(deterministic_attempt_ids))
                                attempt_key_placeholders = ", ".join(["%s"] * len(deterministic_attempt_keys))
                                cursor.execute(
                                    f"""
                                    SELECT * FROM workflow_activity_attempts
                                    WHERE command_id = %s
                                       OR attempt_id IN ({attempt_id_placeholders})
                                       OR (workspace_id = %s AND idempotency_key IN ({attempt_key_placeholders}))
                                    ORDER BY attempt_number, attempt_id
                                    FOR UPDATE
                                    """,
                                    (
                                        expected_command_id,
                                        *deterministic_attempt_ids,
                                        expected_workspace_id,
                                        *deterministic_attempt_keys,
                                    ),
                                )
                                command_attempt_rows = _fetch_all_dict_rows(cursor)
                                attempt_terminal_statuses = set(
                                    _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["workflow_activity_attempts"][
                                        "terminal_statuses"
                                    ]
                                )
                                superseded_running_attempts: list[dict[str, Any]] = []
                                for command_attempt in command_attempt_rows:
                                    command_attempt_number = int(command_attempt.get("attempt_number") or 0)
                                    command_attempt_status = str(command_attempt.get("status") or "").strip()
                                    command_attempt_id = str(command_attempt.get("attempt_id") or "").strip()
                                    command_attempt_key = str(command_attempt.get("idempotency_key") or "").strip()
                                    command_attempt_workspace = (
                                        str(command_attempt.get("workspace_id") or "default").strip() or "default"
                                    )
                                    command_attempt_identity = d1m_source_attempt_identities.get(command_attempt_number)
                                    deterministic_identity_collision = bool(
                                        command_attempt_id in deterministic_attempt_ids
                                        or (
                                            command_attempt_workspace == expected_workspace_id
                                            and command_attempt_key in deterministic_attempt_keys
                                        )
                                    )
                                    if deterministic_identity_collision and (
                                        not command_attempt_identity
                                        or command_attempt_id != command_attempt_identity[0]
                                        or command_attempt_key != command_attempt_identity[1]
                                        or str(command_attempt.get("command_id") or "").strip() != expected_command_id
                                    ):
                                        connection.rollback()
                                        return None
                                    command_attempt_metadata = _json_load_dict(command_attempt.get("metadata_json"))
                                    command_attempt_input = _json_load_dict(command_attempt.get("input_json"))
                                    command_attempt_output = _json_load_dict(command_attempt.get("output_json"))
                                    command_attempt_input_force = command_attempt_input.get("force")
                                    command_attempt_output_force = command_attempt_output.get("force")
                                    command_attempt_lease_owner = str(
                                        command_attempt_metadata.get("lease_owner") or ""
                                    ).strip()
                                    command_attempt_immutable_valid = bool(
                                        command_attempt_identity
                                        and command_attempt_id == command_attempt_identity[0]
                                        and command_attempt_key == command_attempt_identity[1]
                                        and command_attempt_workspace == expected_workspace_id
                                        and str(command_attempt.get("activity_run_id") or "").strip()
                                        == expected_activity_run_id
                                        and str(command_attempt.get("workflow_run_id") or "").strip()
                                        == expected_workflow_run_id
                                        and str(command_attempt.get("command_id") or "").strip() == expected_command_id
                                        and str(command_attempt.get("provider") or "").strip() == expected_provider
                                        and str(command_attempt.get("provider_request_ref") or "").strip()
                                        == expected_command_id
                                        and command_attempt_lease_owner
                                    )
                                    expected_resume_attempt_id, expected_resume_attempt_key = (
                                        _company_public_web_source_resume_attempt_identity(
                                            expected_command_id,
                                            command_attempt_number,
                                        )
                                    )
                                    resume_control_attempt_valid = bool(
                                        command_attempt_number > 0
                                        and command_attempt_number <= expected_command_attempt
                                        and command_attempt_status == "succeeded"
                                        and command_attempt_id == expected_resume_attempt_id
                                        and command_attempt_key == expected_resume_attempt_key
                                        and command_attempt_workspace == expected_workspace_id
                                        and str(command_attempt.get("activity_run_id") or "").strip()
                                        == expected_activity_run_id
                                        and str(command_attempt.get("workflow_run_id") or "").strip()
                                        == expected_workflow_run_id
                                        and str(command_attempt.get("command_id") or "").strip() == expected_command_id
                                        and str(command_attempt.get("provider") or "").strip() == expected_provider
                                        and str(command_attempt.get("provider_request_ref") or "").strip()
                                        == expected_command_id
                                        and command_attempt_lease_owner
                                        and str(command_attempt_input.get("control_action") or "").strip() == "resume"
                                        and str(command_attempt_input.get("phase_command_type") or "").strip()
                                        == "company.public_web.source.collect"
                                        and str(command_attempt_output.get("control_action") or "").strip() == "resume"
                                        and str(command_attempt_output.get("command_id") or "").strip()
                                        == expected_command_id
                                        and str(command_attempt_output.get("command_type") or "").strip()
                                        == "company.public_web.source.collect"
                                        and str(command_attempt_input.get("target_company") or "").strip()
                                        == expected_target_company
                                        and str(command_attempt_output.get("target_company") or "").strip()
                                        == expected_target_company
                                        and str(command_attempt_input.get("company_key") or "").strip()
                                        == expected_company_key
                                        and str(command_attempt_output.get("company_key") or "").strip()
                                        == expected_company_key
                                        and isinstance(command_attempt_input_force, bool)
                                        and isinstance(command_attempt_output_force, bool)
                                        and command_attempt_input_force is command_attempt_output_force
                                        and str(command_attempt_output.get("reason") or "").strip()
                                    )
                                    if command_attempt_status in attempt_terminal_statuses:
                                        if resume_control_attempt_valid:
                                            continue
                                        # Only an exact, fully validated prior
                                        # generation may already be terminal.
                                        # A terminal current/future generation
                                        # contradicts the active command claim;
                                        # an alternate prior identity is equally
                                        # impossible and must fail closed.
                                        if (
                                            not command_attempt_immutable_valid
                                            or command_attempt_number >= expected_command_attempt
                                        ):
                                            connection.rollback()
                                            return None
                                        continue
                                    command_attempt_identity_valid = bool(
                                        command_attempt_immutable_valid
                                        and command_attempt_status == "running"
                                        and (
                                            command_attempt_number < expected_command_attempt
                                            or command_attempt_lease_owner == expected_lease_owner
                                        )
                                    )
                                    if not command_attempt_identity_valid:
                                        connection.rollback()
                                        return None
                                    if command_attempt_number < expected_command_attempt:
                                        superseded_running_attempts.append(command_attempt)

                                for prior_attempt in superseded_running_attempts:
                                    now = _utc_now_sql_timestamp()
                                    prior_metadata = _json_load_dict(prior_attempt.get("metadata_json"))
                                    cursor.execute(
                                        """
                                        UPDATE workflow_activity_attempts
                                        SET status = 'failed',
                                            completed_at = %s,
                                            error_json = %s,
                                            output_json = %s,
                                            metadata_json = %s,
                                            updated_at = %s
                                        WHERE attempt_id = %s
                                          AND status = 'running'
                                          AND attempt_number = %s
                                        RETURNING attempt_id
                                        """,
                                        (
                                            now,
                                            _json_dump(
                                                {
                                                    **_json_load_dict(prior_attempt.get("error_json")),
                                                    "reason": "workflow_command_attempt_superseded_by_new_claim",
                                                    "owner_lost": True,
                                                    "deterministic_terminal_failure": False,
                                                }
                                            ),
                                            _json_dump(
                                                {
                                                    **_json_load_dict(prior_attempt.get("output_json")),
                                                    "status": "skipped",
                                                    "reason": "workflow_command_attempt_superseded_by_new_claim",
                                                }
                                            ),
                                            _json_dump(
                                                {
                                                    **prior_metadata,
                                                    "owner_lost": True,
                                                    "owner_lost_reason": (
                                                        "workflow_command_attempt_superseded_by_new_claim"
                                                    ),
                                                    "superseded_by_attempt": expected_command_attempt,
                                                }
                                            ),
                                            now,
                                            str(prior_attempt.get("attempt_id") or "").strip(),
                                            int(prior_attempt.get("attempt_number") or 0),
                                        ),
                                    )
                                    if cursor.fetchone() is None:
                                        connection.rollback()
                                        return None

                        cursor.execute(
                            (
                                f"SELECT * FROM {quoted_table} "
                                f"WHERE (workspace_id = %s AND idempotency_key = %s) OR {quoted_id_column} = %s "
                                f"ORDER BY {quoted_id_column} FOR UPDATE"
                            ),
                            (workspace_id, idempotency_key, row_id),
                        )
                        identity_rows = _fetch_all_dict_rows(cursor)
                        idempotent_row = next(
                            (
                                item
                                for item in identity_rows
                                if (str(item.get("workspace_id") or "default").strip() or "default") == workspace_id
                                and str(item.get("idempotency_key") or "").strip() == idempotency_key
                            ),
                            None,
                        )
                        primary_key_row = next(
                            (
                                item
                                for item in identity_rows
                                if str(item.get(configured_id_column) or "").strip() == row_id
                            ),
                            None,
                        )

                        if (
                            idempotent_row is not None
                            and str(idempotent_row.get(configured_id_column) or "").strip() != row_id
                        ):
                            raise ValueError(
                                f"{normalized_table} idempotency identity collision: "
                                f"{workspace_id}/{idempotency_key} is already bound"
                            )
                        current = primary_key_row or idempotent_row
                        if current is not None:
                            for column in configured_immutable_columns:
                                current_value = str(current.get(column) or "").strip()
                                requested_value = str(payload.get(column) or "").strip()
                                if current_value != requested_value:
                                    raise ValueError(
                                        f"{normalized_table} immutable identity collision for {row_id}: {column}"
                                    )
                            current_status = str(current.get("status") or "").strip()
                            if configured_write_once or current_status in configured_terminal_statuses:
                                connection.commit()
                                return current

                            updated_payload = dict(payload)
                            updated_payload["created_at"] = current.get("created_at")
                            for column in json_object_columns:
                                updated_payload[column] = _json_dump(
                                    {
                                        **_json_load_dict(current.get(column)),
                                        **_json_load_dict(payload.get(column)),
                                    }
                                )
                            update_columns = [
                                column
                                for column in columns
                                if column not in configured_immutable_columns and column != "created_at"
                            ]
                            assignments = ", ".join(f"{_quote_identifier(column)} = %s" for column in update_columns)
                            cursor.execute(
                                (f"UPDATE {quoted_table} SET {assignments} WHERE {quoted_id_column} = %s RETURNING *"),
                                tuple(
                                    _normalize_postgres_payload(updated_payload.get(column))
                                    for column in update_columns
                                )
                                + (row_id,),
                            )
                            committed = _fetch_one_dict_row(cursor, cursor.fetchone())
                            if committed is None:
                                raise RuntimeError(f"{normalized_table} locked row update returned no row")
                            connection.commit()
                            return committed

                        quoted_columns = [_quote_identifier(column) for column in columns]
                        cursor.execute(
                            (
                                f"INSERT INTO {quoted_table} ({', '.join(quoted_columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
                            ),
                            tuple(_normalize_postgres_payload(payload.get(column)) for column in columns),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if inserted is None:
                            raise RuntimeError(f"{normalized_table} insert returned no row")
                    connection.commit()
                return inserted
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def get_agent_action(self, action_id: str) -> dict[str, Any] | None:
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        return self.select_one("agent_actions", where_sql="action_id = %s", params=[str(action_id or "").strip()])

    def update_agent_action_state(
        self,
        action_id: str = "",
        *,
        table_name: str = "agent_actions",
        expected_status: str = "",
        status: str = "",
        approval_status: str = "",
        result_ref: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "agent_actions":
            raise ValueError("update_agent_action_state requires table_name=agent_actions")
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        normalized_action_id = str(action_id or "").strip()
        if not normalized_action_id:
            return None
        current = self.select_one("agent_actions", where_sql="action_id = %s", params=[normalized_action_id])
        if current is None:
            return None
        current_status = str(current.get("status") or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        terminal = current_status in {"completed", "failed", "cancelled", "rejected"}
        requested_status = str(status or current_status or "").strip() or "planned"
        if normalized_expected_status and normalized_expected_status != current_status:
            return current
        if terminal and requested_status != current_status:
            return current
        now = _utc_now_sql_timestamp()
        updated = self._execute_returning_one(
            """
            UPDATE agent_actions
            SET status = %s,
                approval_status = %s,
                result_ref_json = %s,
                metadata_json = %s,
                updated_at = %s
            WHERE action_id = %s AND status = %s
            RETURNING *
            """,
            (
                requested_status,
                str(approval_status or current.get("approval_status") or "not_required").strip() or "not_required",
                _json_dump(result_ref if result_ref is not None else _json_load_dict(current.get("result_ref_json"))),
                _json_dump(metadata if metadata is not None else _json_load_dict(current.get("metadata_json"))),
                now,
                normalized_action_id,
                current_status,
            ),
        )
        if updated is not None:
            return updated
        return self.select_one("agent_actions", where_sql="action_id = %s", params=[normalized_action_id])

    def requeue_agent_action_for_operation_retry(
        self,
        *,
        table_name: str = "agent_actions",
        action_id: str,
        workspace_id: str,
        expected_status: str,
        parent_operation_run_id: str,
        retry_operation_run_id: str,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "agent_actions":
            raise ValueError("requeue_agent_action_for_operation_retry requires table_name=agent_actions")
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        normalized_action_id = str(action_id or "").strip()
        normalized_workspace_id = str(workspace_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        normalized_parent_run_id = str(parent_operation_run_id or "").strip()
        normalized_retry_run_id = str(retry_operation_run_id or "").strip()
        if (
            not normalized_action_id
            or not normalized_workspace_id
            or normalized_expected_status not in {"queued", "failed", "cancelled"}
            or not normalized_parent_run_id
            or not normalized_retry_run_id
        ):
            return None
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE",
                            (normalized_action_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if current is None:
                            action = None
                        elif (str(current.get("workspace_id") or "default").strip() or "default") != (
                            normalized_workspace_id
                        ):
                            action = current
                        elif str(current.get("status") or "").strip() != normalized_expected_status:
                            action = current
                        elif str(current.get("approval_status") or "").strip() == "rejected":
                            action = current
                        else:
                            metadata = _json_load_dict(current.get("metadata_json"))
                            existing_retry_run_id = str(metadata.get("retry_operation_run_id") or "").strip()
                            if existing_retry_run_id and existing_retry_run_id not in {
                                normalized_parent_run_id,
                                normalized_retry_run_id,
                            }:
                                action = current
                            else:
                                metadata["retry_operation_run_id"] = normalized_retry_run_id
                                cursor.execute(
                                    """
                                    UPDATE agent_actions
                                    SET status = 'queued', metadata_json = %s, updated_at = %s
                                    WHERE action_id = %s AND status = %s
                                    RETURNING *
                                    """,
                                    (
                                        _json_dump(metadata),
                                        now,
                                        normalized_action_id,
                                        normalized_expected_status,
                                    ),
                                )
                                action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if action is None:
                                    cursor.execute(
                                        "SELECT * FROM agent_actions WHERE action_id = %s",
                                        (normalized_action_id,),
                                    )
                                    action = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                return action
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_operation_run(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "operation_runs",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "operation_runs":
            raise ValueError("upsert_operation_run requires table_name=operation_runs")
        if not self._require_operation_runtime_table("operation_runs"):
            return None
        payload = _normalize_postgres_row_payload(dict(row or {}))
        operation_run_id = str(payload.get("operation_run_id") or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        action_id = str(payload.get("action_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        owner_module = str(payload.get("owner_module") or "").strip()
        operation_type = str(payload.get("operation_type") or "").strip()
        if (
            not operation_run_id
            or not workspace_id
            or not action_id
            or not idempotency_key
            or not owner_module
            or not operation_type
        ):
            return None
        now = _utc_now_sql_timestamp()
        row_payload = {
            "operation_run_id": operation_run_id,
            "workspace_id": workspace_id,
            "action_id": action_id,
            "owner_module": owner_module,
            "operation_type": operation_type,
            "request_schema_version": str(payload.get("request_schema_version") or "").strip(),
            "request_schema_digest": str(payload.get("request_schema_digest") or "").strip(),
            "status": str(payload.get("status") or "queued").strip() or "queued",
            "progress_json": str(payload.get("progress_json") or "{}"),
            "workflow_ref_json": str(payload.get("workflow_ref_json") or "{}"),
            "cost_budget_json": str(payload.get("cost_budget_json") or "{}"),
            "idempotency_key": idempotency_key,
            "result_ref_json": str(payload.get("result_ref_json") or "{}"),
            "metadata_json": str(payload.get("metadata_json") or "{}"),
            "started_at": str(payload.get("started_at") or "").strip(),
            "completed_at": str(payload.get("completed_at") or "").strip(),
            "created_at": str(payload.get("created_at") or now).strip(),
            "updated_at": str(payload.get("updated_at") or now).strip(),
        }
        columns = list(row_payload.keys())
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM operation_runs
                    WHERE workspace_id = %s AND idempotency_key = %s
                    LIMIT 1
                    """,
                    (workspace_id, idempotency_key),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is not None:
                    _assert_request_schema_pin_identity(existing, row_payload, record_kind="operation_run")
                    connection.commit()
                    return existing
                cursor.execute(
                    (
                        f"INSERT INTO operation_runs ({', '.join(_quote_identifier(column) for column in columns)}) "
                        f"VALUES ({', '.join(['%s'] * len(columns))}) "
                        "ON CONFLICT DO NOTHING RETURNING *"
                    ),
                    tuple(row_payload[column] for column in columns),
                )
                inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                if inserted is not None:
                    connection.commit()
                    return inserted
                cursor.execute(
                    """
                    SELECT * FROM operation_runs
                    WHERE workspace_id = %s AND idempotency_key = %s
                    LIMIT 1
                    """,
                    (workspace_id, idempotency_key),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is None:
                    cursor.execute(
                        "SELECT * FROM operation_runs WHERE operation_run_id = %s LIMIT 1",
                        (operation_run_id,),
                    )
                    existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is not None:
                    _assert_request_schema_pin_identity(existing, row_payload, record_kind="operation_run")
            connection.commit()
        return existing

    def get_operation_run(self, operation_run_id: str) -> dict[str, Any] | None:
        if not self._require_operation_runtime_table("operation_runs"):
            return None
        return self.select_one(
            "operation_runs",
            where_sql="operation_run_id = %s",
            params=[str(operation_run_id or "").strip()],
        )

    def update_operation_run_state(
        self,
        operation_run_id: str = "",
        *,
        table_name: str = "operation_runs",
        expected_status: str = "",
        status: str = "",
        progress: dict[str, Any] | None = None,
        workflow_ref: dict[str, Any] | None = None,
        result_ref: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "operation_runs":
            raise ValueError("update_operation_run_state requires table_name=operation_runs")
        if not self._require_operation_runtime_table("operation_runs"):
            return None
        normalized_operation_id = str(operation_run_id or "").strip()
        if not normalized_operation_id:
            return None
        current = self.select_one("operation_runs", where_sql="operation_run_id = %s", params=[normalized_operation_id])
        if current is None:
            return None
        current_status = str(current.get("status") or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        requested_status = str(status or current_status or "queued").strip() or "queued"
        if normalized_expected_status and normalized_expected_status != current_status:
            return current
        if current_status in {"completed", "failed", "cancelled"} and requested_status != current_status:
            return current
        now = _utc_now_sql_timestamp()
        completed_at = str(current.get("completed_at") or "").strip()
        if requested_status in {"completed", "failed", "cancelled"} and not completed_at:
            completed_at = now
        updated = self._execute_returning_one(
            """
            UPDATE operation_runs
            SET status = %s,
                progress_json = %s,
                workflow_ref_json = %s,
                result_ref_json = %s,
                metadata_json = %s,
                completed_at = %s,
                updated_at = %s
            WHERE operation_run_id = %s AND status = %s
            RETURNING *
            """,
            (
                requested_status,
                _json_dump(progress if progress is not None else _json_load_dict(current.get("progress_json"))),
                _json_dump(
                    workflow_ref if workflow_ref is not None else _json_load_dict(current.get("workflow_ref_json"))
                ),
                _json_dump(result_ref if result_ref is not None else _json_load_dict(current.get("result_ref_json"))),
                _json_dump(metadata if metadata is not None else _json_load_dict(current.get("metadata_json"))),
                completed_at,
                now,
                normalized_operation_id,
                current_status,
            ),
        )
        if updated is not None:
            return updated
        return self.select_one(
            "operation_runs",
            where_sql="operation_run_id = %s",
            params=[normalized_operation_id],
        )

    def _acquire_operation_event_stream_lock(self, cursor: Any, event_stream_id: str) -> None:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (self._advisory_lock_key(f"operation_events:{event_stream_id}"),),
        )

    @contextmanager
    def _hold_session_advisory_lock(
        self,
        lock_key: str,
        *,
        timeout_seconds: float | None = None,
        deadline_monotonic: float | None = None,
    ) -> Any:
        normalized_lock_key = str(lock_key or "").strip()
        if not normalized_lock_key:
            raise ValueError("session advisory lock key is required")
        now_monotonic = time.monotonic()
        if deadline_monotonic is None:
            acquisition_timeout_seconds = max(
                0.0,
                float(
                    SESSION_ADVISORY_LOCK_ACQUISITION_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
                ),
            )
            deadline = now_monotonic + acquisition_timeout_seconds
        else:
            deadline = float(deadline_monotonic)
            acquisition_timeout_seconds = max(0.0, deadline - now_monotonic)
            if deadline <= now_monotonic:
                raise ControlPlaneAdvisoryLockBusy(
                    lock_key=normalized_lock_key,
                    timeout_seconds=acquisition_timeout_seconds,
                )
        psycopg_module = self._psycopg or _import_psycopg()
        connection = self._direct_connect(psycopg_module)
        acquired = False
        try:
            while True:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_try_advisory_lock(hashtext(%s))",
                        (self._advisory_lock_key(normalized_lock_key),),
                    )
                    lock_row = cursor.fetchone()
                if isinstance(lock_row, dict):
                    acquired = bool(next(iter(lock_row.values()), False))
                elif isinstance(lock_row, (list, tuple)):
                    acquired = bool(lock_row[0]) if lock_row else False
                else:
                    acquired = bool(lock_row)
                if acquired:
                    if deadline_monotonic is not None and time.monotonic() > deadline:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "SELECT pg_advisory_unlock(hashtext(%s))",
                                (self._advisory_lock_key(normalized_lock_key),),
                            )
                        connection.commit()
                        acquired = False
                        raise ControlPlaneAdvisoryLockBusy(
                            lock_key=normalized_lock_key,
                            timeout_seconds=acquisition_timeout_seconds,
                        )
                    connection.commit()
                    break
                connection.rollback()
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise ControlPlaneAdvisoryLockBusy(
                        lock_key=normalized_lock_key,
                        timeout_seconds=acquisition_timeout_seconds,
                    )
                time.sleep(min(_SESSION_ADVISORY_LOCK_POLL_SECONDS, remaining_seconds))
            try:
                yield
            finally:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))",
                        (self._advisory_lock_key(normalized_lock_key),),
                    )
                    unlock_row = cursor.fetchone()
                connection.commit()
                if isinstance(unlock_row, dict):
                    unlocked = bool(next(iter(unlock_row.values()), False))
                else:
                    unlocked = bool(unlock_row and unlock_row[0])
                if acquired and not unlocked:
                    raise RuntimeError(f"session advisory lock was not held: {normalized_lock_key}")
        finally:
            connection.close()

    @contextmanager
    def hold_operation_dispatch_lock(
        self,
        *,
        table_name: str = "operation_runs",
        operation_run_id: str,
        deadline_monotonic: float | None = None,
    ) -> Any:
        if _normalize_postgres_identifier(table_name) != "operation_runs":
            raise ValueError("hold_operation_dispatch_lock requires table_name=operation_runs")
        if not self._require_operation_runtime_table("operation_runs"):
            raise RuntimeError("operation dispatch lock requires authoritative operation_runs")
        normalized_operation_id = str(operation_run_id or "").strip()
        if not normalized_operation_id:
            raise ValueError("operation_run_id is required")
        with self._hold_session_advisory_lock(
            f"operation_dispatch:{normalized_operation_id}",
            deadline_monotonic=deadline_monotonic,
        ):
            yield

    @contextmanager
    def hold_serving_projection_publication_lock(
        self,
        *,
        table_name: str = "serving_projections",
        projection_id: str,
        deadline_monotonic: float | None = None,
    ) -> Any:
        if _normalize_postgres_identifier(table_name) != "serving_projections":
            raise ValueError("hold_serving_projection_publication_lock requires table_name=serving_projections")
        if not self.should_prefer_read("serving_projections"):
            raise RuntimeError("projection publication lock requires authoritative serving_projections")
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            raise ValueError("projection_id is required")
        with self._hold_session_advisory_lock(
            f"serving_projection_publication:{normalized_projection_id}",
            deadline_monotonic=deadline_monotonic,
        ):
            yield

    def _get_operation_event_with_cursor(
        self,
        cursor: Any,
        *,
        event_stream_id: str,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT * FROM operation_events
            WHERE event_stream_id = %s AND idempotency_key = %s
            LIMIT 1
            """,
            (event_stream_id, idempotency_key),
        )
        return _fetch_one_dict_row(cursor, cursor.fetchone())

    @staticmethod
    def _validate_operation_control_event_identity(
        event: dict[str, Any],
        *,
        expected: dict[str, Any],
    ) -> None:
        identity_fields = (
            "workspace_id",
            "event_stream_id",
            "operation_run_id",
            "action_id",
            "event_family",
            "event_type",
            "idempotency_key",
        )
        mismatches = [
            field
            for field in identity_fields
            if str(event.get(field) or "").strip() != str(expected.get(field) or "").strip()
        ]
        if mismatches:
            raise RuntimeError("operation control event identity collision: " + ", ".join(mismatches))

    def _append_operation_event_with_cursor(
        self,
        cursor: Any,
        *,
        payload: dict[str, Any],
        now: str,
        acquire_stream_lock: bool = True,
    ) -> dict[str, Any] | None:
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        event_stream_id = str(payload.get("event_stream_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        event_family = str(payload.get("event_family") or "").strip()
        event_type = str(payload.get("event_type") or "").strip()
        if not workspace_id or not event_stream_id or not idempotency_key or not event_family or not event_type:
            return None
        if acquire_stream_lock:
            self._acquire_operation_event_stream_lock(cursor, event_stream_id)
        existing = self._get_operation_event_with_cursor(
            cursor,
            event_stream_id=event_stream_id,
            idempotency_key=idempotency_key,
        )
        if existing is not None:
            return existing
        sequence_number = int(payload.get("sequence_number") or 0)
        if sequence_number <= 0:
            cursor.execute(
                "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM operation_events WHERE event_stream_id = %s",
                (event_stream_id,),
            )
            row_value = cursor.fetchone()
            sequence_number = int(
                (row_value[0] if isinstance(row_value, (list, tuple)) and row_value else row_value) or 1
            )
        event_id = str(payload.get("event_id") or "").strip() or (
            "opevt_" + sha1(f"{event_stream_id}:{sequence_number}:{idempotency_key}".encode("utf-8")).hexdigest()[:24]
        )
        row_payload = {
            "event_id": event_id,
            "workspace_id": workspace_id,
            "event_stream_id": event_stream_id,
            "operation_run_id": str(payload.get("operation_run_id") or "").strip(),
            "action_id": str(payload.get("action_id") or "").strip(),
            "event_family": event_family,
            "event_type": event_type,
            "sequence_number": sequence_number,
            "idempotency_key": idempotency_key,
            "occurred_at": str(payload.get("occurred_at") or now).strip(),
            "recorded_at": str(payload.get("recorded_at") or now).strip(),
            "actor": str(payload.get("actor") or "").strip(),
            "source": str(payload.get("source") or "").strip(),
            "payload_json": str(payload.get("payload_json") or "{}"),
            "schema_version": str(payload.get("schema_version") or "operation_event_v1").strip(),
            "created_at": str(payload.get("created_at") or now).strip(),
        }
        columns = list(row_payload.keys())
        cursor.execute(
            (
                f"INSERT INTO operation_events ({', '.join(_quote_identifier(column) for column in columns)}) "
                f"VALUES ({', '.join(['%s'] * len(columns))}) "
                "ON CONFLICT DO NOTHING RETURNING *"
            ),
            tuple(row_payload[column] for column in columns),
        )
        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
        if inserted is not None:
            return inserted
        existing = self._get_operation_event_with_cursor(
            cursor,
            event_stream_id=event_stream_id,
            idempotency_key=idempotency_key,
        )
        if existing is None:
            cursor.execute(
                """
                SELECT * FROM operation_events
                WHERE event_stream_id = %s AND sequence_number = %s
                LIMIT 1
                """,
                (event_stream_id, sequence_number),
            )
            existing = _fetch_one_dict_row(cursor, cursor.fetchone())
        return existing

    def append_operation_event(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "operation_events",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "operation_events":
            raise ValueError("append_operation_event requires table_name=operation_events")
        if not self._require_operation_runtime_table("operation_events"):
            return None
        payload = _normalize_postgres_row_payload(dict(row or {}))
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        event = self._append_operation_event_with_cursor(cursor, payload=payload, now=now)
                    connection.commit()
                    return event
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def reject_agent_action_with_event(
        self,
        *,
        table_name: str = "agent_actions",
        action_id: str,
        expected_status: str,
        status: str = "cancelled",
        approval_status: str = "rejected",
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        event_row: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "agent_actions":
            raise ValueError("reject_agent_action_with_event requires table_name=agent_actions")
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        if not self._require_operation_runtime_table("operation_events"):
            return None
        normalized_action_id = str(action_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        requested_status = str(status or "cancelled").strip() or "cancelled"
        requested_approval_status = str(approval_status or "rejected").strip() or "rejected"
        if requested_status != "cancelled" or requested_approval_status != "rejected":
            raise ValueError("reject action UoW only supports status=cancelled and approval_status=rejected")
        event_payload = _normalize_postgres_row_payload(dict(event_row or {}))
        event_stream_id = str(event_payload.get("event_stream_id") or "").strip()
        event_idempotency_key = str(event_payload.get("idempotency_key") or "").strip()
        if not normalized_action_id or not normalized_expected_status or not event_idempotency_key:
            return None
        if (
            event_stream_id != normalized_action_id
            or str(event_payload.get("action_id") or "").strip() != normalized_action_id
            or str(event_payload.get("event_family") or "").strip() != "operation_event"
            or str(event_payload.get("event_type") or "").strip() != "ActionRejected"
        ):
            raise ValueError("reject action event identity does not match the locked action")
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_operation_event_stream_lock(cursor, event_stream_id)
                        cursor.execute(
                            "SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE",
                            (normalized_action_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        action = current
                        event = None
                        outcome = "not_found"
                        if current is not None:
                            if (str(event_payload.get("workspace_id") or "default").strip() or "default") != (
                                str(current.get("workspace_id") or "default").strip() or "default"
                            ):
                                raise ValueError("reject action event workspace does not match the locked action")
                            current_status = str(current.get("status") or "").strip()
                            current_approval_status = str(current.get("approval_status") or "").strip()
                            target_already_applied = (
                                current_status == requested_status
                                and current_approval_status == requested_approval_status
                            )
                            if target_already_applied:
                                event = self._get_operation_event_with_cursor(
                                    cursor,
                                    event_stream_id=event_stream_id,
                                    idempotency_key=event_idempotency_key,
                                )
                                if event is None:
                                    event = self._append_operation_event_with_cursor(
                                        cursor,
                                        payload=event_payload,
                                        now=now,
                                        acquire_stream_lock=False,
                                    )
                                    if event is None:
                                        raise RuntimeError("operation control repair produced no rejection event")
                                    self._validate_operation_control_event_identity(
                                        event,
                                        expected=event_payload,
                                    )
                                    outcome = "repaired"
                                else:
                                    self._validate_operation_control_event_identity(
                                        event,
                                        expected=event_payload,
                                    )
                                    outcome = "already_applied"
                            elif current_status != normalized_expected_status or current_status in {
                                "completed",
                                "failed",
                                "cancelled",
                                "rejected",
                            }:
                                outcome = "conflict"
                            else:
                                result_ref = {
                                    **_json_load_dict(current.get("result_ref_json")),
                                    **dict(result_ref_patch or {}),
                                }
                                metadata = {
                                    **_json_load_dict(current.get("metadata_json")),
                                    **dict(metadata_patch or {}),
                                }
                                cursor.execute(
                                    """
                                    UPDATE agent_actions
                                    SET status = %s,
                                        approval_status = %s,
                                        result_ref_json = %s,
                                        metadata_json = %s,
                                        updated_at = %s
                                    WHERE action_id = %s AND status = %s
                                    RETURNING *
                                    """,
                                    (
                                        requested_status,
                                        requested_approval_status,
                                        _json_dump(result_ref),
                                        _json_dump(metadata),
                                        now,
                                        normalized_action_id,
                                        current_status,
                                    ),
                                )
                                action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if action is None:
                                    raise RuntimeError("operation rejection lost the locked action row")
                                event = self._append_operation_event_with_cursor(
                                    cursor,
                                    payload=event_payload,
                                    now=now,
                                    acquire_stream_lock=False,
                                )
                                if event is None:
                                    raise RuntimeError("operation control transition produced no rejection event")
                                self._validate_operation_control_event_identity(
                                    event,
                                    expected=event_payload,
                                )
                                outcome = "applied"
                    connection.commit()
                    return {
                        "outcome": outcome,
                        "applied": outcome == "applied",
                        "action": action,
                        "event": event,
                    }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def cancel_operation_run_with_event(
        self,
        *,
        table_name: str = "operation_runs",
        operation_run_id: str,
        expected_status: str,
        status: str = "cancelled",
        progress_patch: dict[str, Any] | None = None,
        workflow_ref_patch: dict[str, Any] | None = None,
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        linked_action_metadata_patch: dict[str, Any] | None = None,
        event_row: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        normalized_status = str(status or "cancelled").strip() or "cancelled"
        if normalized_status != "cancelled":
            raise ValueError("cancel operation UoW only supports status=cancelled")
        return self._transition_operation_run_with_linked_action_and_event(
            table_name=table_name,
            operation_run_id=operation_run_id,
            expected_status=expected_status,
            target_operation_status=normalized_status,
            target_action_status="cancelled",
            target_event_type="OperationCancelled",
            progress_patch=progress_patch,
            workflow_ref_patch=workflow_ref_patch,
            result_ref_patch=result_ref_patch,
            metadata_patch=metadata_patch,
            linked_action_result_ref_patch=None,
            linked_action_metadata_patch=linked_action_metadata_patch,
            event_row=event_row,
            require_linked_action=False,
            require_replay_patch_match=False,
        )

    def fail_operation_run_for_stale_input_with_event(
        self,
        *,
        table_name: str = "operation_runs",
        operation_run_id: str,
        expected_status: str,
        progress_patch: dict[str, Any] | None = None,
        workflow_ref_patch: dict[str, Any] | None = None,
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        linked_action_metadata_patch: dict[str, Any] | None = None,
        event_row: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        return self._transition_operation_run_with_linked_action_and_event(
            table_name=table_name,
            operation_run_id=operation_run_id,
            expected_status=expected_status,
            target_operation_status="failed",
            target_action_status="failed",
            target_event_type="OperationInputRevisionStale",
            progress_patch=progress_patch,
            workflow_ref_patch=workflow_ref_patch,
            result_ref_patch=result_ref_patch,
            metadata_patch=metadata_patch,
            linked_action_result_ref_patch=None,
            linked_action_metadata_patch=linked_action_metadata_patch,
            event_row=event_row,
            require_linked_action=True,
            require_replay_patch_match=True,
        )

    def finalize_projection_read_with_event(
        self,
        *,
        table_name: str = "operation_runs",
        operation_run_id: str,
        expected_status: str,
        terminal_status: str,
        progress_patch: dict[str, Any] | None = None,
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        linked_action_result_ref_patch: dict[str, Any] | None = None,
        linked_action_metadata_patch: dict[str, Any] | None = None,
        event_row: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        normalized_terminal_status = str(terminal_status or "").strip()
        if normalized_terminal_status == "completed":
            target_action_status = "completed"
            target_event_type = "OperationReadCompleted"
        elif normalized_terminal_status == "failed":
            target_action_status = "queued"
            target_event_type = "OperationReadFailed"
        else:
            raise ValueError("projection read terminal_status must be completed or failed")
        return self._transition_operation_run_with_linked_action_and_event(
            table_name=table_name,
            operation_run_id=operation_run_id,
            expected_status=expected_status,
            target_operation_status=normalized_terminal_status,
            target_action_status=target_action_status,
            target_event_type=target_event_type,
            progress_patch=progress_patch,
            workflow_ref_patch=None,
            result_ref_patch=result_ref_patch,
            metadata_patch=metadata_patch,
            linked_action_result_ref_patch=linked_action_result_ref_patch,
            linked_action_metadata_patch=linked_action_metadata_patch,
            event_row=event_row,
            require_linked_action=True,
            require_replay_patch_match=True,
        )

    def _transition_operation_run_with_linked_action_and_event(
        self,
        *,
        table_name: str,
        operation_run_id: str,
        expected_status: str,
        target_operation_status: str,
        target_action_status: str,
        target_event_type: str,
        progress_patch: dict[str, Any] | None,
        workflow_ref_patch: dict[str, Any] | None,
        result_ref_patch: dict[str, Any] | None,
        metadata_patch: dict[str, Any] | None,
        linked_action_result_ref_patch: dict[str, Any] | None,
        linked_action_metadata_patch: dict[str, Any] | None,
        event_row: dict[str, Any] | None,
        require_linked_action: bool,
        require_replay_patch_match: bool,
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "operation_runs":
            raise ValueError("operation transition UoW requires table_name=operation_runs")
        if not self._require_operation_runtime_table("operation_runs"):
            return None
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        if not self._require_operation_runtime_table("operation_events"):
            return None
        normalized_operation_id = str(operation_run_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        requested_status = str(target_operation_status or "").strip()
        requested_action_status = str(target_action_status or "").strip()
        requested_event_type = str(target_event_type or "").strip()
        supported_transition = (requested_status, requested_action_status, requested_event_type)
        if supported_transition not in {
            ("cancelled", "cancelled", "OperationCancelled"),
            ("failed", "failed", "OperationInputRevisionStale"),
            ("completed", "completed", "OperationReadCompleted"),
            ("failed", "queued", "OperationReadFailed"),
        }:
            raise ValueError("unsupported operation/action/event transition")
        event_payload = _normalize_postgres_row_payload(dict(event_row or {}))
        event_stream_id = str(event_payload.get("event_stream_id") or "").strip()
        event_idempotency_key = str(event_payload.get("idempotency_key") or "").strip()
        if not normalized_operation_id or not normalized_expected_status or not event_idempotency_key:
            return None
        if (
            event_stream_id != normalized_operation_id
            or str(event_payload.get("operation_run_id") or "").strip() != normalized_operation_id
            or str(event_payload.get("event_family") or "").strip() != "operation_event"
            or str(event_payload.get("event_type") or "").strip() != requested_event_type
        ):
            raise ValueError("operation transition event identity does not match the locked operation")

        def patch_matches(raw_value: Any, patch: dict[str, Any] | None) -> bool:
            current_value = _json_load_dict(raw_value)
            return all(current_value.get(key) == value for key, value in dict(patch or {}).items())

        now = _utc_now_sql_timestamp()
        attempt = 0
        transition_lock_key = (
            f"operation_dispatch:{normalized_operation_id}" if requested_event_type == "OperationCancelled" else ""
        )
        while True:
            try:
                with self._connect_with_transaction_lock(transition_lock_key) as connection:
                    with connection.cursor() as cursor:
                        self._acquire_operation_event_stream_lock(cursor, event_stream_id)
                        cursor.execute(
                            "SELECT * FROM operation_runs WHERE operation_run_id = %s FOR UPDATE",
                            (normalized_operation_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        operation = current
                        linked_action = None
                        locked_action = None
                        event = None
                        outcome = "not_found"
                        if current is not None:
                            current_action_id = str(current.get("action_id") or "").strip()
                            if (str(event_payload.get("workspace_id") or "default").strip() or "default") != (
                                str(current.get("workspace_id") or "default").strip() or "default"
                            ):
                                raise ValueError(
                                    "operation transition event workspace does not match the locked operation"
                                )
                            if str(event_payload.get("action_id") or "").strip() != current_action_id:
                                raise ValueError(
                                    "operation transition event action does not match the locked operation"
                                )
                            current_status = str(current.get("status") or "").strip()
                            target_already_applied = current_status == requested_status and (
                                not require_replay_patch_match
                                or (
                                    patch_matches(current.get("progress_json"), progress_patch)
                                    and patch_matches(current.get("workflow_ref_json"), workflow_ref_patch)
                                    and patch_matches(current.get("result_ref_json"), result_ref_patch)
                                    and patch_matches(current.get("metadata_json"), metadata_patch)
                                )
                            )
                            if target_already_applied:
                                outcome = "already_applied"
                            elif current_status != normalized_expected_status or current_status in {
                                "completed",
                                "failed",
                                "cancelled",
                            }:
                                outcome = "conflict"
                            else:
                                progress = {
                                    **_json_load_dict(current.get("progress_json")),
                                    **dict(progress_patch or {}),
                                }
                                workflow_ref = {
                                    **_json_load_dict(current.get("workflow_ref_json")),
                                    **dict(workflow_ref_patch or {}),
                                }
                                result_ref = {
                                    **_json_load_dict(current.get("result_ref_json")),
                                    **dict(result_ref_patch or {}),
                                }
                                metadata = {
                                    **_json_load_dict(current.get("metadata_json")),
                                    **dict(metadata_patch or {}),
                                }
                                completed_at = str(current.get("completed_at") or "").strip() or now
                                cursor.execute(
                                    """
                                    UPDATE operation_runs
                                    SET status = %s,
                                        progress_json = %s,
                                        workflow_ref_json = %s,
                                        result_ref_json = %s,
                                        metadata_json = %s,
                                        completed_at = %s,
                                        updated_at = %s
                                    WHERE operation_run_id = %s AND status = %s
                                    RETURNING *
                                    """,
                                    (
                                        requested_status,
                                        _json_dump(progress),
                                        _json_dump(workflow_ref),
                                        _json_dump(result_ref),
                                        _json_dump(metadata),
                                        completed_at,
                                        now,
                                        normalized_operation_id,
                                        current_status,
                                    ),
                                )
                                operation = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if operation is None:
                                    raise RuntimeError("operation transition lost the locked operation row")
                                outcome = "applied"
                            if outcome in {"applied", "already_applied"}:
                                action_id = str((operation or {}).get("action_id") or "").strip()
                                if not action_id and require_linked_action:
                                    connection.rollback()
                                    return {
                                        "outcome": "conflict",
                                        "applied": False,
                                        "operation": current,
                                        "linked_action": None,
                                        "event": None,
                                    }
                                if action_id:
                                    cursor.execute(
                                        "SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE",
                                        (action_id,),
                                    )
                                    linked_action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                    if linked_action is None and require_linked_action:
                                        connection.rollback()
                                        return {
                                            "outcome": "conflict",
                                            "applied": False,
                                            "operation": current,
                                            "linked_action": None,
                                            "event": None,
                                        }
                                    if linked_action is not None:
                                        locked_action = linked_action
                                        action_status = str(linked_action.get("status") or "").strip()
                                        if (
                                            str(linked_action.get("workspace_id") or "default").strip() or "default"
                                        ) != (str(current.get("workspace_id") or "default").strip() or "default"):
                                            raise ValueError(
                                                "operation transition linked action workspace does not match the operation"
                                            )
                                        current_action_metadata = _json_load_dict(linked_action.get("metadata_json"))
                                        current_action_result_ref = _json_load_dict(
                                            linked_action.get("result_ref_json")
                                        )
                                        action_result_ref = {
                                            **current_action_result_ref,
                                            **dict(linked_action_result_ref_patch or {}),
                                        }
                                        action_metadata = {
                                            **current_action_metadata,
                                            **dict(linked_action_metadata_patch or {}),
                                        }
                                        action_already_applied = action_status == requested_action_status and (
                                            not require_replay_patch_match
                                            or (
                                                patch_matches(
                                                    linked_action.get("result_ref_json"),
                                                    linked_action_result_ref_patch,
                                                )
                                                and patch_matches(
                                                    linked_action.get("metadata_json"),
                                                    linked_action_metadata_patch,
                                                )
                                            )
                                        )
                                        action_terminal_conflict = (
                                            action_status
                                            in {
                                                "completed",
                                                "failed",
                                                "rejected",
                                                "cancelled",
                                            }
                                            and not action_already_applied
                                        )
                                        if action_terminal_conflict and require_linked_action:
                                            connection.rollback()
                                            return {
                                                "outcome": "conflict",
                                                "applied": False,
                                                "operation": current,
                                                "linked_action": locked_action,
                                                "event": None,
                                            }
                                        action_needs_update = not action_terminal_conflict and (
                                            action_status != requested_action_status
                                            or action_result_ref != current_action_result_ref
                                            or action_metadata != current_action_metadata
                                        )
                                        if action_needs_update:
                                            cursor.execute(
                                                """
                                                UPDATE agent_actions
                                                SET status = %s,
                                                    result_ref_json = %s,
                                                    metadata_json = %s,
                                                    updated_at = %s
                                                WHERE action_id = %s AND status = %s
                                                RETURNING *
                                                """,
                                                (
                                                    requested_action_status,
                                                    _json_dump(action_result_ref),
                                                    _json_dump(action_metadata),
                                                    now,
                                                    action_id,
                                                    action_status,
                                                ),
                                            )
                                            linked_action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                            if linked_action is None:
                                                raise RuntimeError(
                                                    "operation transition lost the linked action row lock"
                                                )
                                            if outcome == "already_applied":
                                                outcome = "repaired"
                                event = self._get_operation_event_with_cursor(
                                    cursor,
                                    event_stream_id=event_stream_id,
                                    idempotency_key=event_idempotency_key,
                                )
                                if event is None:
                                    event = self._append_operation_event_with_cursor(
                                        cursor,
                                        payload=event_payload,
                                        now=now,
                                        acquire_stream_lock=False,
                                    )
                                    if event is None:
                                        raise RuntimeError("operation control transition produced no event")
                                    self._validate_operation_control_event_identity(
                                        event,
                                        expected=event_payload,
                                    )
                                    if outcome == "already_applied":
                                        outcome = "repaired"
                                else:
                                    self._validate_operation_control_event_identity(
                                        event,
                                        expected=event_payload,
                                    )
                                    if require_replay_patch_match and _json_load_dict(
                                        event.get("payload_json")
                                    ) != _json_load_dict(event_payload.get("payload_json")):
                                        connection.rollback()
                                        return {
                                            "outcome": "conflict",
                                            "applied": False,
                                            "operation": current,
                                            "linked_action": locked_action,
                                            "event": None,
                                        }
                    connection.commit()
                    return {
                        "outcome": outcome,
                        "applied": outcome == "applied",
                        "operation": operation,
                        "linked_action": linked_action,
                        "event": event,
                    }
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def upsert_workflow_command(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        self._ensure_runtime_coordination_schema()
        payload = _normalize_postgres_row_payload(dict(row or {}))
        workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        command_type = str(payload.get("command_type") or "").strip()
        owner = str(payload.get("owner") or "").strip()
        if not workflow_run_id or not idempotency_key or not command_type or not owner:
            return None
        now = _utc_now_sql_timestamp()
        command_id = str(payload.get("command_id") or "").strip() or (
            "cmd_" + sha1(f"{workflow_run_id}:{idempotency_key}".encode("utf-8")).hexdigest()[:24]
        )
        row_payload = {
            "command_id": command_id,
            "workflow_run_id": workflow_run_id,
            "operation_id": str(payload.get("operation_id") or "").strip(),
            "command_type": command_type,
            "owner": owner,
            **_workflow_command_causality_columns_from_payload(
                _json_load_dict(payload.get("payload_json")),
            ),
            "status": str(payload.get("status") or "queued").strip() or "queued",
            "idempotency_key": idempotency_key,
            "payload_json": str(payload.get("payload_json") or "{}"),
            "artifact_refs_json": str(payload.get("artifact_refs_json") or "[]"),
            "not_before_at": str(payload.get("not_before_at") or "").strip(),
            "attempt": int(payload.get("attempt") or 0),
            "max_attempts": max(1, int(payload.get("max_attempts") or 5)),
            "retry_policy_json": str(payload.get("retry_policy_json") or "{}"),
            "lease_owner": str(payload.get("lease_owner") or "").strip(),
            "lease_expires_at": str(payload.get("lease_expires_at") or "").strip(),
            "heartbeat_at": str(payload.get("heartbeat_at") or "").strip(),
            "last_error": str(payload.get("last_error") or "").strip(),
            "result_json": str(payload.get("result_json") or "{}"),
            "schema_version": str(payload.get("schema_version") or "workflow_command_v1").strip(),
            "created_at": str(payload.get("created_at") or now).strip(),
            "updated_at": str(payload.get("updated_at") or now).strip(),
        }
        columns = list(row_payload.keys())
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    (
                        f"INSERT INTO workflow_commands ({', '.join(_quote_identifier(column) for column in columns)}) "
                        f"VALUES ({', '.join(['%s'] * len(columns))}) "
                        "ON CONFLICT DO NOTHING RETURNING *"
                    ),
                    tuple(row_payload[column] for column in columns),
                )
                inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                if inserted is not None:
                    connection.commit()
                    return inserted
                cursor.execute(
                    """
                    SELECT * FROM workflow_commands
                    WHERE workflow_run_id = %s AND idempotency_key = %s
                    LIMIT 1
                    """,
                    (workflow_run_id, idempotency_key),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                if existing is None:
                    cursor.execute(
                        "SELECT * FROM workflow_commands WHERE command_id = %s LIMIT 1",
                        (command_id,),
                    )
                    existing = _fetch_one_dict_row(cursor, cursor.fetchone())
            connection.commit()
        return existing

    def update_workflow_command_payload(
        self,
        command_id: str,
        *,
        payload: dict[str, Any] | None = None,
        artifact_refs: list[Any] | tuple[Any, ...] | None = None,
        not_before_at: str = "",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        causality_columns = _workflow_command_causality_columns_from_payload(payload or {})
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET payload_json = %s,
                artifact_refs_json = %s,
                stage_id = %s,
                causal_group_id = %s,
                parent_command_id = %s,
                source_event_id = %s,
                source_event_type = %s,
                input_artifact_refs_json = %s,
                output_artifact_refs_json = %s,
                produced_entity_counts_json = %s,
                no_op_reason = %s,
                readiness_effect = %s,
                downstream_command_ids_json = %s,
                causality_schema_version = %s,
                not_before_at = %s,
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ('queued', 'retry_wait')
            RETURNING *
            """,
            (
                _json_dump(payload or {}),
                _json_dump(list(artifact_refs or [])),
                causality_columns["stage_id"],
                causality_columns["causal_group_id"],
                causality_columns["parent_command_id"],
                causality_columns["source_event_id"],
                causality_columns["source_event_type"],
                causality_columns["input_artifact_refs_json"],
                causality_columns["output_artifact_refs_json"],
                causality_columns["produced_entity_counts_json"],
                causality_columns["no_op_reason"],
                causality_columns["readiness_effect"],
                causality_columns["downstream_command_ids_json"],
                causality_columns["causality_schema_version"],
                str(not_before_at or "").strip(),
                _json_dump(result or {}),
                now,
                normalized_command_id,
            ),
        )

    def checkpoint_running_workflow_command_payload(
        self,
        command_id: str,
        *,
        lease_owner: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        normalized_lease_owner = str(lease_owner or "").strip()
        if not normalized_command_id or not normalized_lease_owner:
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        causality_columns = _workflow_command_causality_columns_from_payload(payload or {})
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET payload_json = %s,
                stage_id = %s,
                causal_group_id = %s,
                parent_command_id = %s,
                source_event_id = %s,
                source_event_type = %s,
                input_artifact_refs_json = %s,
                output_artifact_refs_json = %s,
                produced_entity_counts_json = %s,
                no_op_reason = %s,
                readiness_effect = %s,
                downstream_command_ids_json = %s,
                causality_schema_version = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status = 'running'
              AND lease_owner = %s
              AND (lease_expires_at = '' OR lease_expires_at > %s)
            RETURNING *
            """,
            (
                _json_dump(payload or {}),
                causality_columns["stage_id"],
                causality_columns["causal_group_id"],
                causality_columns["parent_command_id"],
                causality_columns["source_event_id"],
                causality_columns["source_event_type"],
                causality_columns["input_artifact_refs_json"],
                causality_columns["output_artifact_refs_json"],
                causality_columns["produced_entity_counts_json"],
                causality_columns["no_op_reason"],
                causality_columns["readiness_effect"],
                causality_columns["downstream_command_ids_json"],
                causality_columns["causality_schema_version"],
                now,
                normalized_command_id,
                normalized_lease_owner,
                now,
            ),
        )

    def claim_workflow_command(
        self,
        command_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 300,
        reclaim_claimed: bool = False,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        self._ensure_runtime_coordination_schema()
        normalized_command_id = str(command_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_command_id or not normalized_owner:
            return None
        now = _utc_now_sql_timestamp()
        # reclaim_claimed lets an explicitly opted-in owner reclaim an expired
        # claim left by a worker that crashed before mark_workflow_command_running;
        # the lease-expiry clause still protects active claims. Export and the
        # bounded, exact-claim-guarded D1m owner opt in today. It is not the global
        # default pending the remaining ownership-fencing hardening documented in
        # docs/DURABLE_COMMAND_OWNERSHIP_FENCING.md.
        claim_status_in = (
            "('queued', 'retry_wait', 'running', 'claimed')"
            if reclaim_claimed
            else "('queued', 'retry_wait', 'running')"
        )
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = 'claimed',
                lease_owner = %s,
                lease_expires_at = %s,
                attempt = attempt + 1,
                heartbeat_at = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN {claim_status_in}
              AND (not_before_at = '' OR not_before_at <= %s)
              AND (lease_expires_at = '' OR lease_expires_at <= %s)
            RETURNING *
            """,
            (
                normalized_owner,
                _expiry_timestamp(max(1, int(lease_seconds or 300))),
                now,
                now,
                normalized_command_id,
                now,
                now,
            ),
        )

    def mark_workflow_command_running(
        self,
        command_id: str,
        *,
        lease_owner: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        now = _utc_now_sql_timestamp()
        clauses = ["command_id = %s", "status = 'claimed'"]
        params: list[Any] = [now, now, normalized_command_id]
        normalized_owner = str(lease_owner or "").strip()
        if normalized_owner:
            clauses.append("lease_owner = %s")
            params.append(normalized_owner)
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = 'running',
                heartbeat_at = %s,
                updated_at = %s
            WHERE {" AND ".join(clauses)}
            RETURNING *
            """,
            tuple(params),
        )

    def mark_workflow_command_succeeded(
        self,
        command_id: str,
        *,
        result: dict[str, Any] | None = None,
        expected_attempt: int = 0,
        expected_lease_owner: str = "",
        expected_lease_expires_at: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_attempt = max(0, int(expected_attempt or 0))
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_lease_expires_at = str(expected_lease_expires_at or "").strip()
        exact_claim_requested = bool(normalized_attempt or normalized_lease_owner or normalized_lease_expires_at)
        if exact_claim_requested and not (
            normalized_attempt > 0 and normalized_lease_owner and normalized_lease_expires_at
        ):
            return None
        now = _utc_now_sql_timestamp()
        clauses = ["command_id = %s", "status IN ('claimed', 'running')"]
        params: list[Any] = [now, _json_dump(result or {}), now, str(command_id or "").strip()]
        if exact_claim_requested:
            clauses.extend(
                [
                    "attempt = %s",
                    "lease_owner = %s",
                    "lease_expires_at = %s",
                    "(NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()",
                ]
            )
            params.extend([normalized_attempt, normalized_lease_owner, normalized_lease_expires_at])
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = 'succeeded',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE {" AND ".join(clauses)}
            RETURNING *
            """,
            tuple(params),
        )

    def mark_workflow_command_failed(
        self,
        command_id: str,
        *,
        error_text: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
        expected_attempt: int = 0,
        expected_lease_owner: str = "",
        expected_lease_expires_at: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_attempt = max(0, int(expected_attempt or 0))
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_lease_expires_at = str(expected_lease_expires_at or "").strip()
        exact_claim_requested = bool(normalized_attempt or normalized_lease_owner or normalized_lease_expires_at)
        if exact_claim_requested and not (
            normalized_attempt > 0 and normalized_lease_owner and normalized_lease_expires_at
        ):
            return None
        current = self.select_one(
            "workflow_commands", where_sql="command_id = %s", params=[str(command_id or "").strip()]
        )
        if current is None:
            return None
        attempt = int(current.get("attempt") or 0)
        max_attempts = max(1, int(current.get("max_attempts") or 5))
        should_retry = bool(retryable) and attempt < max_attempts
        now = _utc_now_sql_timestamp()
        clauses = ["command_id = %s", "status IN ('claimed', 'running')"]
        params: list[Any] = [
            "retry_wait" if should_retry else "failed_terminal",
            now,
            _expiry_timestamp(max(0, int(retry_delay_seconds or 0))) if should_retry else "",
            str(error_text or "").strip(),
            now,
            str(command_id or "").strip(),
        ]
        if exact_claim_requested:
            clauses.extend(
                [
                    "attempt = %s",
                    "lease_owner = %s",
                    "lease_expires_at = %s",
                    "(NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()",
                ]
            )
            params.extend([normalized_attempt, normalized_lease_owner, normalized_lease_expires_at])
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = %s,
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = %s,
                last_error = %s,
                updated_at = %s
            WHERE {" AND ".join(clauses)}
            RETURNING *
            """,
            tuple(params),
        )

    def fail_acquisition_root_command_claim(
        self,
        command_id: str,
        *,
        table_name: str = "workflow_commands",
        expected_lease_owner: str,
        expected_lease_expires_at: str,
        expected_attempt: int,
        reason: str = "",
    ) -> dict[str, Any] | None:
        """Fail only an unexpired, exact acquisition-root claim."""

        if _normalize_postgres_identifier(table_name) != "workflow_commands":
            raise ValueError("fail_acquisition_root_command_claim requires table_name=workflow_commands")
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_lease_expires_at = str(expected_lease_expires_at or "").strip()
        normalized_attempt = max(0, int(expected_attempt or 0))
        normalized_reason = str(reason or "acquisition_root_command_target_conflict").strip()
        if (
            not normalized_command_id
            or not normalized_lease_owner
            or not normalized_lease_expires_at
            or normalized_attempt <= 0
        ):
            return None
        now = _utc_now_sql_timestamp()
        command = self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = 'failed_terminal',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = '',
                last_error = %s,
                updated_at = %s
            WHERE command_id = %s
              AND command_type = 'acquisition.run.create'
              AND owner = 'acquisition_run_writer'
              AND status = 'running'
              AND lease_owner = %s
              AND lease_expires_at = %s
              AND attempt = %s
              AND (NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()
            RETURNING *
            """,
            (
                now,
                normalized_reason,
                now,
                normalized_command_id,
                normalized_lease_owner,
                normalized_lease_expires_at,
                normalized_attempt,
            ),
        )
        if command is not None:
            return {
                "outcome": "applied",
                "reason": "acquisition_root_command_failed_for_exact_claim",
                "command": command,
            }
        current = self.select_one(
            "workflow_commands",
            where_sql="command_id = %s",
            params=[normalized_command_id],
        )
        return {
            "outcome": "stale_claim" if current is not None else "not_found",
            "reason": (
                "acquisition_root_command_claim_not_current" if current is not None else "workflow_command_not_found"
            ),
            "command": current,
        }

    def complete_acquisition_root_command(
        self,
        command_id: str,
        *,
        table_name: str = "workflow_commands",
        expected_lease_owner: str,
        expected_lease_expires_at: str,
        expected_attempt: int,
        expected_root_command: dict[str, Any] | None = None,
        plan_event: dict[str, Any] | None = None,
        child_command: dict[str, Any] | None = None,
        child_causality: dict[str, Any] | None = None,
        entity_deltas: list[dict[str, Any]] | None = None,
        root_result: dict[str, Any] | None = None,
        completion_contract: str = "acquisition_root",
    ) -> dict[str, Any] | None:
        """Commit one exact root/source plan event, child, and terminal row atomically."""

        if _normalize_postgres_identifier(table_name) != "workflow_commands":
            raise ValueError("complete_acquisition_root_command requires table_name=workflow_commands")
        if not self.should_prefer_read("workflow_commands"):
            return None
        contract_name = str(completion_contract or "acquisition_root").strip()
        completion_contracts = {
            "acquisition_root": {
                "parent_command_type": "acquisition.run.create",
                "parent_owner": "acquisition_run_writer",
                "child_command_type": "acquisition.intent.resolve",
                "child_owner": "acquisition_planner",
                "reason_prefix": "acquisition_root",
                "child_reason_prefix": "acquisition_root_intent_child",
                "committed_reason": "acquisition_root_intent_child_committed",
                "entity_delta_count": 0,
                "entity_delta_type": "",
            },
            "company_public_web_source": {
                "parent_command_type": "company.public_web.source.collect",
                "parent_owner": "company_public_web_owner",
                "child_command_type": "company.public_web.assets.materialize",
                "child_owner": "company_public_web_owner",
                "reason_prefix": "company_public_web_source",
                "child_reason_prefix": "company_public_web_source_materialize_child",
                "committed_reason": "company_public_web_source_materialize_child_committed",
                "entity_delta_count": 1,
                "entity_delta_type": "company_public_web_run",
            },
        }
        contract = completion_contracts.get(contract_name)
        if contract is None:
            raise ValueError("complete_acquisition_root_command completion_contract is not registered")
        parent_command_type = str(contract["parent_command_type"])
        parent_owner = str(contract["parent_owner"])
        child_command_type = str(contract["child_command_type"])
        child_owner = str(contract["child_owner"])
        reason_prefix = str(contract["reason_prefix"])
        child_reason_prefix = str(contract["child_reason_prefix"])
        committed_reason = str(contract["committed_reason"])
        expected_entity_delta_count = int(contract["entity_delta_count"])
        expected_entity_delta_type = str(contract["entity_delta_type"])
        self._ensure_runtime_coordination_schema()
        normalized_command_id = str(command_id or "").strip()
        normalized_lease_owner = str(expected_lease_owner or "").strip()
        normalized_lease_expires_at = str(expected_lease_expires_at or "").strip()
        normalized_attempt = max(0, int(expected_attempt or 0))
        event_spec = dict(plan_event or {})
        child_spec = dict(child_command or {})
        causality_template = dict(child_causality or {})
        delta_specs = [dict(item or {}) for item in list(entity_deltas or []) if isinstance(item, dict)]
        expected_root = dict(expected_root_command or {})
        terminal_result = dict(root_result or {})
        workflow_run_id = str(event_spec.get("workflow_run_id") or "").strip()
        operation_id = str(event_spec.get("operation_id") or "").strip()
        event_idempotency_key = str(event_spec.get("idempotency_key") or "").strip()
        child_command_id = str(child_spec.get("command_id") or "").strip()
        child_idempotency_key = str(child_spec.get("idempotency_key") or "").strip()
        if (
            not normalized_command_id
            or not normalized_lease_owner
            or not normalized_lease_expires_at
            or normalized_attempt <= 0
            or not workflow_run_id
            or not operation_id
            or not event_idempotency_key
            or not child_command_id
            or not child_idempotency_key
            or not expected_root
            or len(delta_specs) != expected_entity_delta_count
        ):
            return None
        if (
            str(event_spec.get("command_id") or "").strip() != normalized_command_id
            or str(event_spec.get("event_family") or "").strip() != "workflow_event"
            or str(event_spec.get("event_type") or "").strip() != "CommandPlanRequested"
            or str(child_spec.get("workflow_run_id") or "").strip() != workflow_run_id
            or str(child_spec.get("operation_id") or "").strip() != operation_id
            or str(child_spec.get("command_type") or "").strip() != child_command_type
            or str(child_spec.get("owner") or "").strip() != child_owner
            or str(causality_template.get("workflow_run_id") or "").strip() != workflow_run_id
            or str(causality_template.get("operation_id") or "").strip() != operation_id
            or str(causality_template.get("command_type") or "").strip() != child_command_type
            or str(causality_template.get("owner") or "").strip() != child_owner
            or str(causality_template.get("parent_command_id") or "").strip() != normalized_command_id
            or str(causality_template.get("idempotency_key") or "").strip() != child_idempotency_key
            or str(causality_template.get("source_event_id") or "").strip()
            or str(causality_template.get("source_event_type") or "").strip() != "CommandPlanRequested"
            or list(terminal_result.get("downstream_command_ids") or []) != [child_command_id]
            or int(terminal_result.get("completed_claim_attempt") or 0) != normalized_attempt
            or list(terminal_result.get("entity_delta_ids") or [])
            != [str(item.get("delta_id") or "").strip() for item in delta_specs]
        ):
            raise ValueError(f"{contract_name} UoW contract mismatch")
        for delta_spec in delta_specs:
            if (
                not str(delta_spec.get("delta_id") or "").strip()
                or not str(delta_spec.get("workspace_id") or "").strip()
                or str(delta_spec.get("workflow_run_id") or "").strip() != workflow_run_id
                or str(delta_spec.get("operation_run_id") or "").strip() != operation_id
                or str(delta_spec.get("command_id") or "").strip() != normalized_command_id
                or not str(delta_spec.get("activity_run_id") or "").strip()
                or not str(delta_spec.get("attempt_id") or "").strip()
                or str(delta_spec.get("entity_type") or "").strip() != expected_entity_delta_type
                or not str(delta_spec.get("entity_key") or "").strip()
                or not str(delta_spec.get("delta_kind") or "").strip()
                or str(delta_spec.get("status") or "").strip() != "recorded"
                or not str(delta_spec.get("idempotency_key") or "").strip()
            ):
                raise ValueError(f"{contract_name} entity-delta contract mismatch")

        def response(
            *,
            outcome: str,
            reason_code: str,
            command: dict[str, Any] | None,
            child: dict[str, Any] | None = None,
            event: dict[str, Any] | None = None,
            deltas: list[dict[str, Any]] | None = None,
        ) -> dict[str, Any]:
            return {
                "outcome": outcome,
                "reason": reason_code,
                "command": command,
                "child_command": child,
                "event": event,
                "entity_deltas": list(deltas or []),
            }

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
                            (normalized_command_id,),
                        )
                        root = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if root is None:
                            connection.commit()
                            return response(
                                outcome="not_found",
                                reason_code="workflow_command_not_found",
                                command=None,
                            )
                        cursor.execute("SELECT TO_CHAR(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')")
                        repository_now_row = cursor.fetchone()
                        repository_now = str(
                            repository_now_row[0]
                            if isinstance(repository_now_row, (list, tuple)) and repository_now_row
                            else repository_now_row or ""
                        ).strip()
                        cursor.execute(
                            "SELECT (NULLIF(%s, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()",
                            (str(root.get("lease_expires_at") or "").strip(),),
                        )
                        lease_active_row = cursor.fetchone()
                        lease_active = bool(
                            lease_active_row[0]
                            if isinstance(lease_active_row, (list, tuple)) and lease_active_row
                            else next(iter(lease_active_row.values()), False)
                            if isinstance(lease_active_row, dict)
                            else lease_active_row
                        )
                        claim_is_current = bool(
                            str(root.get("command_type") or "").strip() == parent_command_type
                            and str(root.get("owner") or "").strip() == parent_owner
                            and str(root.get("workflow_run_id") or "").strip() == workflow_run_id
                            and str(root.get("operation_id") or "").strip() == operation_id
                            and str(root.get("status") or "").strip() == "running"
                            and str(root.get("lease_owner") or "").strip() == normalized_lease_owner
                            and str(root.get("lease_expires_at") or "").strip() == normalized_lease_expires_at
                            and int(root.get("attempt") or 0) == normalized_attempt
                            and lease_active
                        )
                        if not claim_is_current:
                            connection.commit()
                            return response(
                                outcome="stale_claim",
                                reason_code=f"{reason_prefix}_command_claim_not_current",
                                command=root,
                            )
                        try:
                            root_input_artifact_refs = decode_json_contract(
                                root.get("input_artifact_refs_json"),
                                expected_type=list,
                            )
                            root_output_artifact_refs = decode_json_contract(
                                root.get("output_artifact_refs_json"),
                                expected_type=list,
                            )
                            root_produced_entity_counts = decode_json_contract(
                                root.get("produced_entity_counts_json"),
                                expected_type=dict,
                            )
                            root_downstream_command_ids = decode_json_contract(
                                root.get("downstream_command_ids_json"),
                                expected_type=list,
                            )
                            root_payload = decode_json_contract(
                                root.get("payload_json"),
                                expected_type=dict,
                            )
                            root_artifact_refs = decode_json_contract(
                                root.get("artifact_refs_json"),
                                expected_type=list,
                            )
                            root_retry_policy = decode_json_contract(
                                root.get("retry_policy_json"),
                                expected_type=dict,
                            )
                            root_result_payload = decode_json_contract(
                                root.get("result_json"),
                                expected_type=dict,
                            )
                        except JsonContractShapeError:
                            connection.commit()
                            return response(
                                outcome="conflict",
                                reason_code=f"{reason_prefix}_command_persisted_json_invalid",
                                command=root,
                            )
                        locked_root_identity = {
                            "command_id": str(root.get("command_id") or "").strip(),
                            "workflow_run_id": str(root.get("workflow_run_id") or "").strip(),
                            "operation_id": str(root.get("operation_id") or "").strip(),
                            "command_type": str(root.get("command_type") or "").strip(),
                            "owner": str(root.get("owner") or "").strip(),
                            "stage_id": str(root.get("stage_id") or "").strip(),
                            "causal_group_id": str(root.get("causal_group_id") or "").strip(),
                            "parent_command_id": str(root.get("parent_command_id") or "").strip(),
                            "source_event_id": str(root.get("source_event_id") or "").strip(),
                            "source_event_type": str(root.get("source_event_type") or "").strip(),
                            "input_artifact_refs": root_input_artifact_refs,
                            "output_artifact_refs": root_output_artifact_refs,
                            "produced_entity_counts": root_produced_entity_counts,
                            "no_op_reason": str(root.get("no_op_reason") or "").strip(),
                            "readiness_effect": str(root.get("readiness_effect") or "").strip(),
                            "downstream_command_ids": root_downstream_command_ids,
                            "causality_schema_version": str(root.get("causality_schema_version") or "").strip(),
                            "idempotency_key": str(root.get("idempotency_key") or "").strip(),
                            "payload": root_payload,
                            "artifact_refs": root_artifact_refs,
                            "not_before_at": str(root.get("not_before_at") or "").strip(),
                            "max_attempts": max(0, int(root.get("max_attempts") or 0)),
                            "retry_policy": root_retry_policy,
                            "result": root_result_payload,
                            "schema_version": str(root.get("schema_version") or "").strip(),
                        }
                        root_identity_matches = json_contract_equal(locked_root_identity, expected_root)
                        if not root_identity_matches:
                            connection.commit()
                            return response(
                                outcome="conflict",
                                reason_code=f"{reason_prefix}_command_locked_identity_mismatch",
                                command=root,
                            )

                        self._acquire_transaction_lock(cursor, f"workflow_events:{workflow_run_id}")
                        cursor.execute(
                            """
                            SELECT * FROM workflow_events
                            WHERE workflow_run_id = %s AND event_id = %s
                            LIMIT 1
                            FOR UPDATE
                            """,
                            (
                                workflow_run_id,
                                str(locked_root_identity.get("source_event_id") or "").strip(),
                            ),
                        )
                        root_source_event = _fetch_one_dict_row(cursor, cursor.fetchone())
                        root_source_sequence = max(
                            0,
                            int((root_source_event or {}).get("sequence_number") or 0),
                        )
                        if root_source_event is None or root_source_sequence <= 0:
                            connection.rollback()
                            return response(
                                outcome="conflict",
                                reason_code=f"{reason_prefix}_source_event_missing",
                                command=root,
                            )
                        cursor.execute(
                            """
                            SELECT * FROM workflow_events
                            WHERE workflow_run_id = %s
                              AND (
                                  idempotency_key = %s
                                  OR (command_id = %s AND event_type = 'CommandPlanRequested')
                              )
                            ORDER BY event_id
                            FOR UPDATE
                            """,
                            (
                                workflow_run_id,
                                event_idempotency_key,
                                normalized_command_id,
                            ),
                        )
                        candidate_events = _fetch_all_dict_rows(cursor)
                        if len(candidate_events) > 1:
                            connection.rollback()
                            return response(
                                outcome="conflict",
                                reason_code=f"{reason_prefix}_plan_event_ambiguous",
                                command=root,
                            )
                        event = candidate_events[0] if candidate_events else None
                        event_payload = dict(event_spec.get("payload") or {})
                        event_artifact_refs = list(event_spec.get("artifact_refs") or [])
                        if event is None:
                            cursor.execute(
                                "SELECT COALESCE(MAX(sequence_number), 0) FROM workflow_events WHERE workflow_run_id = %s",
                                (workflow_run_id,),
                            )
                            row_value = cursor.fetchone()
                            max_sequence = int(
                                (row_value[0] if isinstance(row_value, (list, tuple)) and row_value else row_value) or 0
                            )
                            sequence_number = max_sequence + 1
                            event_id = (
                                "evt_"
                                + sha1(
                                    f"{workflow_run_id}:{sequence_number}:{event_idempotency_key}".encode("utf-8")
                                ).hexdigest()[:24]
                            )
                            event_row = {
                                "event_id": event_id,
                                "workflow_run_id": workflow_run_id,
                                "operation_id": operation_id,
                                "command_id": normalized_command_id,
                                "activity_attempt_id": "",
                                "event_family": "workflow_event",
                                "event_type": "CommandPlanRequested",
                                "sequence_number": sequence_number,
                                "idempotency_key": event_idempotency_key,
                                "occurred_at": repository_now,
                                "recorded_at": repository_now,
                                "actor": str(event_spec.get("actor") or "").strip(),
                                "source": str(event_spec.get("source") or "").strip(),
                                "payload_json": _json_dump(event_payload),
                                "artifact_refs_json": _json_dump(event_artifact_refs),
                                "schema_version": "workflow_event_v1",
                                "created_at": repository_now,
                            }
                            columns = list(event_row)
                            cursor.execute(
                                f"INSERT INTO workflow_events "
                                f"({', '.join(_quote_identifier(column) for column in columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *",
                                tuple(event_row[column] for column in columns),
                            )
                            event = _fetch_one_dict_row(cursor, cursor.fetchone())
                        else:
                            sequence_number = int(event.get("sequence_number") or 0)
                            try:
                                persisted_event_payload = decode_json_contract(
                                    event.get("payload_json"),
                                    expected_type=dict,
                                )
                                persisted_event_artifact_refs = decode_json_contract(
                                    event.get("artifact_refs_json"),
                                    expected_type=list,
                                )
                            except JsonContractShapeError:
                                connection.rollback()
                                return response(
                                    outcome="conflict",
                                    reason_code=f"{reason_prefix}_plan_event_json_invalid",
                                    command=root,
                                    event=event,
                                )
                            expected_event_id = (
                                "evt_"
                                + sha1(
                                    f"{workflow_run_id}:{sequence_number}:{event_idempotency_key}".encode("utf-8")
                                ).hexdigest()[:24]
                            )
                            event_matches = bool(
                                sequence_number > 0
                                and sequence_number > root_source_sequence
                                and str(event.get("event_id") or "").strip() == expected_event_id
                                and str(event.get("operation_id") or "").strip() == operation_id
                                and str(event.get("command_id") or "").strip() == normalized_command_id
                                and str(event.get("activity_attempt_id") or "").strip() == ""
                                and str(event.get("event_family") or "").strip() == "workflow_event"
                                and str(event.get("event_type") or "").strip() == "CommandPlanRequested"
                                and str(event.get("idempotency_key") or "").strip() == event_idempotency_key
                                and str(event.get("actor") or "").strip() == str(event_spec.get("actor") or "").strip()
                                and str(event.get("source") or "").strip()
                                == str(event_spec.get("source") or "").strip()
                                and json_contract_equal(
                                    persisted_event_payload,
                                    event_payload,
                                )
                                and json_contract_equal(
                                    persisted_event_artifact_refs,
                                    event_artifact_refs,
                                )
                                and str(event.get("schema_version") or "").strip() == "workflow_event_v1"
                            )
                            if not event_matches:
                                connection.rollback()
                                return response(
                                    outcome="conflict",
                                    reason_code=f"{reason_prefix}_plan_event_identity_conflict",
                                    command=root,
                                    event=event,
                                )

                        assert event is not None
                        child_causality_payload = {
                            **causality_template,
                            "source_event_id": str(event.get("event_id") or "").strip(),
                            "source_event_type": "CommandPlanRequested",
                        }
                        child_payload = {
                            **dict(child_spec.get("payload") or {}),
                            "causality": child_causality_payload,
                        }
                        causality_columns = _workflow_command_causality_columns_from_payload(child_payload)
                        child_row = {
                            "command_id": child_command_id,
                            "workflow_run_id": workflow_run_id,
                            "operation_id": operation_id,
                            "command_type": child_command_type,
                            "owner": child_owner,
                            **causality_columns,
                            "status": "queued",
                            "idempotency_key": child_idempotency_key,
                            "payload_json": _json_dump(child_payload),
                            "artifact_refs_json": _json_dump(list(child_spec.get("artifact_refs") or [])),
                            "not_before_at": str(child_spec.get("not_before_at") or "").strip(),
                            "attempt": 0,
                            "max_attempts": max(1, int(child_spec.get("max_attempts") or 3)),
                            "retry_policy_json": _json_dump(dict(child_spec.get("retry_policy") or {})),
                            "lease_owner": "",
                            "lease_expires_at": "",
                            "heartbeat_at": "",
                            "last_error": "",
                            "result_json": "{}",
                            "schema_version": "workflow_command_v1",
                            "created_at": repository_now,
                            "updated_at": repository_now,
                        }
                        cursor.execute(
                            """
                            SELECT * FROM workflow_commands
                            WHERE parent_command_id = %s
                               OR command_id = %s
                               OR (workflow_run_id = %s AND idempotency_key = %s)
                            ORDER BY command_id
                            FOR UPDATE
                            """,
                            (
                                normalized_command_id,
                                child_command_id,
                                workflow_run_id,
                                child_idempotency_key,
                            ),
                        )
                        candidate_children = _fetch_all_dict_rows(cursor)
                        if len(candidate_children) > 1:
                            connection.rollback()
                            return response(
                                outcome="conflict",
                                reason_code=f"{child_reason_prefix}_ambiguous",
                                command=root,
                                event=event,
                            )
                        child = candidate_children[0] if candidate_children else None
                        child_requires_validation = child is not None
                        if child is None:
                            columns = list(child_row)
                            cursor.execute(
                                f"INSERT INTO workflow_commands "
                                f"({', '.join(_quote_identifier(column) for column in columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) "
                                f"ON CONFLICT DO NOTHING RETURNING *",
                                tuple(child_row[column] for column in columns),
                            )
                            child = _fetch_one_dict_row(cursor, cursor.fetchone())
                            if child is None:
                                # A concurrent producer won one of the command-id,
                                # run-idempotency, or acquisition-intent-parent
                                # uniqueness constraints after our initial read.
                                # Re-read under lock and accept only the same exact
                                # immutable child contract; an alternate winner is a
                                # durable conflict rather than a second child.
                                cursor.execute(
                                    """
                                    SELECT * FROM workflow_commands
                                    WHERE parent_command_id = %s
                                       OR command_id = %s
                                       OR (workflow_run_id = %s AND idempotency_key = %s)
                                    ORDER BY command_id
                                    FOR UPDATE
                                    """,
                                    (
                                        normalized_command_id,
                                        child_command_id,
                                        workflow_run_id,
                                        child_idempotency_key,
                                    ),
                                )
                                raced_children = _fetch_all_dict_rows(cursor)
                                if len(raced_children) != 1:
                                    connection.rollback()
                                    return response(
                                        outcome="conflict",
                                        reason_code=f"{child_reason_prefix}_unique_conflict",
                                        command=root,
                                        event=event,
                                    )
                                child = raced_children[0]
                                child_requires_validation = True
                        if child_requires_validation:
                            assert child is not None
                            try:
                                persisted_child_payload = decode_json_contract(
                                    child.get("payload_json"),
                                    expected_type=dict,
                                )
                                persisted_child_artifact_refs = decode_json_contract(
                                    child.get("artifact_refs_json"),
                                    expected_type=list,
                                )
                                persisted_child_input_artifact_refs = decode_json_contract(
                                    child.get("input_artifact_refs_json"),
                                    expected_type=list,
                                )
                                persisted_child_output_artifact_refs = decode_json_contract(
                                    child.get("output_artifact_refs_json"),
                                    expected_type=list,
                                )
                                persisted_child_produced_counts = decode_json_contract(
                                    child.get("produced_entity_counts_json"),
                                    expected_type=dict,
                                )
                                persisted_child_downstream_ids = decode_json_contract(
                                    child.get("downstream_command_ids_json"),
                                    expected_type=list,
                                )
                                persisted_child_retry_policy = decode_json_contract(
                                    child.get("retry_policy_json"),
                                    expected_type=dict,
                                )
                                decode_json_contract(
                                    child.get("result_json"),
                                    expected_type=dict,
                                )
                            except JsonContractShapeError:
                                connection.rollback()
                                return response(
                                    outcome="conflict",
                                    reason_code=f"{child_reason_prefix}_json_invalid",
                                    command=root,
                                    child=child,
                                    event=event,
                                )
                            immutable_child_fields = (
                                "command_id",
                                "workflow_run_id",
                                "operation_id",
                                "command_type",
                                "owner",
                                "stage_id",
                                "causal_group_id",
                                "parent_command_id",
                                "source_event_id",
                                "source_event_type",
                                "no_op_reason",
                                "readiness_effect",
                                "idempotency_key",
                                "causality_schema_version",
                                "schema_version",
                            )
                            child_matches = all(
                                str(child.get(field) or "").strip() == str(child_row.get(field) or "").strip()
                                for field in immutable_child_fields
                            ) and bool(
                                json_contract_equal(
                                    persisted_child_payload,
                                    child_payload,
                                )
                                and json_contract_equal(
                                    persisted_child_artifact_refs,
                                    list(child_spec.get("artifact_refs") or []),
                                )
                                and json_contract_equal(
                                    persisted_child_input_artifact_refs,
                                    list(child_causality_payload.get("input_artifact_refs") or []),
                                )
                                and json_contract_equal(
                                    persisted_child_output_artifact_refs,
                                    list(child_causality_payload.get("output_artifact_refs") or []),
                                )
                                and json_contract_equal(
                                    persisted_child_produced_counts,
                                    dict(child_causality_payload.get("produced_entity_counts") or {}),
                                )
                                and json_contract_equal(
                                    persisted_child_downstream_ids,
                                    list(child_causality_payload.get("downstream_command_ids") or []),
                                )
                                and int(child.get("max_attempts") or 0)
                                == max(1, int(child_spec.get("max_attempts") or 3))
                                and json_contract_equal(
                                    persisted_child_retry_policy,
                                    dict(child_spec.get("retry_policy") or {}),
                                )
                            )
                            if not child_matches:
                                connection.rollback()
                                return response(
                                    outcome="conflict",
                                    reason_code=f"{child_reason_prefix}_identity_conflict",
                                    command=root,
                                    child=child,
                                    event=event,
                                )

                        persisted_deltas: list[dict[str, Any]] = []
                        for delta_spec in delta_specs:
                            delta_row = {
                                "delta_id": str(delta_spec.get("delta_id") or "").strip(),
                                "workspace_id": str(delta_spec.get("workspace_id") or "").strip(),
                                "workflow_run_id": workflow_run_id,
                                "operation_run_id": operation_id,
                                "command_id": normalized_command_id,
                                "activity_run_id": str(delta_spec.get("activity_run_id") or "").strip(),
                                "attempt_id": str(delta_spec.get("attempt_id") or "").strip(),
                                "acquisition_run_id": str(delta_spec.get("acquisition_run_id") or "").strip(),
                                "entity_type": str(delta_spec.get("entity_type") or "").strip(),
                                "entity_key": str(delta_spec.get("entity_key") or "").strip(),
                                "delta_kind": str(delta_spec.get("delta_kind") or "").strip(),
                                "status": "recorded",
                                "reason": str(delta_spec.get("reason") or "").strip(),
                                "source_ref_json": _json_dump(dict(delta_spec.get("source_ref") or {})),
                                "entity_payload_json": _json_dump(dict(delta_spec.get("entity_payload") or {})),
                                "projection_effect_json": _json_dump(
                                    dict(delta_spec.get("projection_effect") or {"entered_projection": False})
                                ),
                                "artifact_refs_json": _json_dump(list(delta_spec.get("artifact_refs") or [])),
                                "idempotency_key": str(delta_spec.get("idempotency_key") or "").strip(),
                                "metadata_json": _json_dump(dict(delta_spec.get("metadata") or {})),
                                "created_at": repository_now,
                                "updated_at": repository_now,
                            }
                            cursor.execute(
                                """
                                SELECT * FROM workflow_entity_deltas
                                WHERE delta_id = %s
                                   OR (workspace_id = %s AND idempotency_key = %s)
                                ORDER BY delta_id
                                FOR UPDATE
                                """,
                                (
                                    delta_row["delta_id"],
                                    delta_row["workspace_id"],
                                    delta_row["idempotency_key"],
                                ),
                            )
                            candidate_deltas = _fetch_all_dict_rows(cursor)
                            if len(candidate_deltas) > 1:
                                connection.rollback()
                                return response(
                                    outcome="conflict",
                                    reason_code=f"{reason_prefix}_entity_delta_ambiguous",
                                    command=root,
                                    child=child,
                                    event=event,
                                )
                            persisted_delta = candidate_deltas[0] if candidate_deltas else None
                            delta_requires_validation = persisted_delta is not None
                            if persisted_delta is None:
                                delta_columns = list(delta_row)
                                cursor.execute(
                                    f"INSERT INTO workflow_entity_deltas "
                                    f"({', '.join(_quote_identifier(column) for column in delta_columns)}) "
                                    f"VALUES ({', '.join(['%s'] * len(delta_columns))}) "
                                    f"ON CONFLICT DO NOTHING RETURNING *",
                                    tuple(delta_row[column] for column in delta_columns),
                                )
                                persisted_delta = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if persisted_delta is None:
                                    cursor.execute(
                                        """
                                        SELECT * FROM workflow_entity_deltas
                                        WHERE delta_id = %s
                                           OR (workspace_id = %s AND idempotency_key = %s)
                                        ORDER BY delta_id
                                        FOR UPDATE
                                        """,
                                        (
                                            delta_row["delta_id"],
                                            delta_row["workspace_id"],
                                            delta_row["idempotency_key"],
                                        ),
                                    )
                                    raced_deltas = _fetch_all_dict_rows(cursor)
                                    if len(raced_deltas) != 1:
                                        connection.rollback()
                                        return response(
                                            outcome="conflict",
                                            reason_code=f"{reason_prefix}_entity_delta_unique_conflict",
                                            command=root,
                                            child=child,
                                            event=event,
                                        )
                                    persisted_delta = raced_deltas[0]
                                    delta_requires_validation = True
                            if delta_requires_validation:
                                assert persisted_delta is not None
                                try:
                                    persisted_source_ref = decode_json_contract(
                                        persisted_delta.get("source_ref_json"), expected_type=dict
                                    )
                                    persisted_entity_payload = decode_json_contract(
                                        persisted_delta.get("entity_payload_json"), expected_type=dict
                                    )
                                    persisted_projection_effect = decode_json_contract(
                                        persisted_delta.get("projection_effect_json"), expected_type=dict
                                    )
                                    persisted_artifact_refs = decode_json_contract(
                                        persisted_delta.get("artifact_refs_json"), expected_type=list
                                    )
                                    persisted_metadata = decode_json_contract(
                                        persisted_delta.get("metadata_json"), expected_type=dict
                                    )
                                except JsonContractShapeError:
                                    connection.rollback()
                                    return response(
                                        outcome="conflict",
                                        reason_code=f"{reason_prefix}_entity_delta_json_invalid",
                                        command=root,
                                        child=child,
                                        event=event,
                                    )
                                scalar_fields = (
                                    "delta_id",
                                    "workspace_id",
                                    "workflow_run_id",
                                    "operation_run_id",
                                    "command_id",
                                    "activity_run_id",
                                    "attempt_id",
                                    "acquisition_run_id",
                                    "entity_type",
                                    "entity_key",
                                    "delta_kind",
                                    "status",
                                    "reason",
                                    "idempotency_key",
                                )
                                delta_matches = all(
                                    str(persisted_delta.get(field) or "").strip()
                                    == str(delta_row.get(field) or "").strip()
                                    for field in scalar_fields
                                ) and bool(
                                    json_contract_equal(
                                        persisted_source_ref,
                                        dict(delta_spec.get("source_ref") or {}),
                                    )
                                    and json_contract_equal(
                                        persisted_entity_payload,
                                        dict(delta_spec.get("entity_payload") or {}),
                                    )
                                    and json_contract_equal(
                                        persisted_projection_effect,
                                        dict(delta_spec.get("projection_effect") or {"entered_projection": False}),
                                    )
                                    and json_contract_equal(
                                        persisted_artifact_refs,
                                        list(delta_spec.get("artifact_refs") or []),
                                    )
                                    and json_contract_equal(
                                        persisted_metadata,
                                        dict(delta_spec.get("metadata") or {}),
                                    )
                                )
                                if not delta_matches:
                                    connection.rollback()
                                    return response(
                                        outcome="conflict",
                                        reason_code=f"{reason_prefix}_entity_delta_identity_conflict",
                                        command=root,
                                        child=child,
                                        event=event,
                                    )
                            assert persisted_delta is not None
                            persisted_deltas.append(persisted_delta)

                        cursor.execute(
                            """
                            UPDATE workflow_commands
                            SET status = 'succeeded',
                                lease_owner = '',
                                lease_expires_at = '',
                                heartbeat_at = %s,
                                not_before_at = '',
                                last_error = '',
                                downstream_command_ids_json = %s,
                                result_json = %s,
                                updated_at = %s
                            WHERE command_id = %s
                              AND command_type = %s
                              AND owner = %s
                              AND status = 'running'
                              AND lease_owner = %s
                              AND lease_expires_at = %s
                              AND attempt = %s
                              AND (NULLIF(lease_expires_at, '')::timestamp AT TIME ZONE 'UTC') > clock_timestamp()
                            RETURNING *
                            """,
                            (
                                repository_now,
                                _json_dump([child_command_id]),
                                _json_dump(terminal_result),
                                repository_now,
                                normalized_command_id,
                                parent_command_type,
                                parent_owner,
                                normalized_lease_owner,
                                normalized_lease_expires_at,
                                normalized_attempt,
                            ),
                        )
                        completed_root = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if completed_root is None:
                            connection.rollback()
                            return response(
                                outcome="stale_claim",
                                reason_code=f"{reason_prefix}_command_claim_not_current",
                                command=root,
                            )

                    connection.commit()
                    return response(
                        outcome="applied",
                        reason_code=committed_reason,
                        command=completed_root,
                        child=child,
                        event=event,
                        deltas=persisted_deltas,
                    )
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def complete_company_public_web_source_command(
        self,
        command_id: str,
        *,
        table_name: str = "workflow_commands",
        expected_lease_owner: str,
        expected_lease_expires_at: str,
        expected_attempt: int,
        expected_root_command: dict[str, Any] | None = None,
        plan_event: dict[str, Any] | None = None,
        child_command: dict[str, Any] | None = None,
        child_causality: dict[str, Any] | None = None,
        entity_deltas: list[dict[str, Any]] | None = None,
        root_result: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Atomically publish one D1m materialize child and close its source claim."""

        return self.complete_acquisition_root_command(
            command_id,
            table_name=table_name,
            expected_lease_owner=expected_lease_owner,
            expected_lease_expires_at=expected_lease_expires_at,
            expected_attempt=expected_attempt,
            expected_root_command=expected_root_command,
            plan_event=plan_event,
            child_command=child_command,
            child_causality=child_causality,
            entity_deltas=entity_deltas,
            root_result=root_result,
            completion_contract="company_public_web_source",
        )

    def mark_workflow_command_partial_progress(
        self,
        command_id: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        current = self.select_one("workflow_commands", where_sql="command_id = %s", params=[normalized_command_id])
        if current is None:
            return None
        attempt = max(0, int(current.get("attempt") or 0) - 1)
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = 'queued',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = '',
                attempt = %s,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ('claimed', 'running')
            RETURNING *
            """,
            (now, attempt, _json_dump(result or {}), now, normalized_command_id),
        )

    def mark_workflow_command_waiting_prerequisite(
        self,
        command_id: str,
        *,
        retry_delay_seconds: int = 8,
        result: dict[str, Any] | None = None,
        from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        current = self.select_one("workflow_commands", where_sql="command_id = %s", params=[normalized_command_id])
        if current is None:
            return None
        attempt = max(0, int(current.get("attempt") or 0) - 1)
        now = _utc_now_sql_timestamp()
        allowed_statuses = [
            str(status or "").strip()
            for status in list(from_statuses or ("claimed", "running"))
            if str(status or "").strip()
        ]
        if not allowed_statuses:
            allowed_statuses = ["claimed", "running"]
        placeholders = ", ".join(["%s"] * len(allowed_statuses))
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = 'retry_wait',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = %s,
                attempt = %s,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ({placeholders})
            RETURNING *
            """,
            (
                now,
                _expiry_timestamp(min(30, max(1, int(retry_delay_seconds or 8)))),
                attempt,
                _json_dump(result or {}),
                now,
                normalized_command_id,
                *allowed_statuses,
            ),
        )

    def cancel_acquisition_owner_command(
        self,
        command_id: str,
        *,
        table_name: str = "workflow_commands",
        cancel_kind: str,
        actor: str = "",
        reason: str = "",
        force: bool = False,
    ) -> dict[str, Any] | None:
        """Cancel one acquisition owner command and its pre-effect module rows atomically."""

        if _normalize_postgres_identifier(table_name) != "workflow_commands":
            raise ValueError("cancel_acquisition_owner_command requires table_name=workflow_commands")
        if not self.should_prefer_read("workflow_commands"):
            return None
        self._ensure_runtime_coordination_schema()
        for required_table in (
            "acquisition_runs",
            "workflow_activity_runs",
            "workflow_activity_attempts",
            "workflow_entity_deltas",
            "acquisition_discovery_lanes",
        ):
            if not self._require_operation_runtime_table(required_table):
                return None
        normalized_command_id = str(command_id or "").strip()
        normalized_cancel_kind = str(cancel_kind or "").strip()
        config = _ACQUISITION_OWNER_CANCEL_CONFIG.get(normalized_cancel_kind)
        if config is None:
            raise ValueError("cancel_acquisition_owner_command requires a registered cancel_kind")
        if not normalized_command_id:
            return None
        normalized_actor = str(actor or "api").strip() or "api"
        normalized_reason = str(reason or "cancelled_by_command_control").strip() or "cancelled_by_command_control"
        force_cancel = bool(force)
        activity_terminal_statuses = set(
            _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["workflow_activity_runs"]["terminal_statuses"]
        )
        run_terminal_statuses = set(_WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["acquisition_runs"]["terminal_statuses"])
        lane_terminal_statuses = set(
            _WORKFLOW_RUNTIME_IDENTITY_UPSERT_CONFIG["acquisition_discovery_lanes"]["terminal_statuses"]
        )

        def response(
            *,
            outcome: str,
            reason_code: str,
            command: dict[str, Any] | None,
            acquisition_run: dict[str, Any] | None = None,
            activity_runs: list[dict[str, Any]] | None = None,
            discovery_lanes: list[dict[str, Any]] | None = None,
            downstream_command_ids: list[str] | None = None,
            lane_downstream_command_ids: list[str] | None = None,
            activity_attempt_count: int = 0,
            entity_delta_count: int = 0,
            module_state_mutated: bool = False,
        ) -> dict[str, Any]:
            return {
                "outcome": outcome,
                "applied": outcome == "applied",
                "reason": reason_code,
                "command": command,
                "acquisition_run": acquisition_run,
                "activity_runs": list(activity_runs or []),
                "discovery_lanes": list(discovery_lanes or []),
                "downstream_command_ids": list(downstream_command_ids or []),
                "lane_downstream_command_ids": list(lane_downstream_command_ids or []),
                "activity_attempt_count": max(0, int(activity_attempt_count or 0)),
                "entity_delta_count": max(0, int(entity_delta_count or 0)),
                "module_state_mutated": bool(module_state_mutated),
            }

        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
                            (normalized_command_id,),
                        )
                        command = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if command is None:
                            connection.commit()
                            return response(outcome="not_found", reason_code="workflow_command_not_found", command=None)

                        command_type = str(command.get("command_type") or "").strip()
                        command_owner = str(command.get("owner") or "").strip()
                        if command_type != config["command_type"] or command_owner != config["owner"]:
                            connection.commit()
                            return response(
                                outcome="conflict",
                                reason_code="workflow_command_owner_specific_cancel_identity_mismatch",
                                command=command,
                            )

                        command_payload = _json_load_dict(command.get("payload_json"))
                        command_result = _json_load_dict(command.get("result_json"))
                        command_status = str(command.get("status") or "").strip()
                        persisted_cancel_kind = str(
                            command_result.get("owner_cancel_kind") or command_result.get("cancel_boundary") or ""
                        ).strip()
                        command_target_applied = bool(
                            command_status == "cancelled"
                            and persisted_cancel_kind == normalized_cancel_kind
                            and str(command_result.get("control_source") or "").strip()
                            == "api.workflow_command_owner_specific_cancel"
                            and command_result.get("owner_specific_control") is True
                        )
                        if command_target_applied:
                            stored_actor = str(command_result.get("control_actor") or "").strip()
                            stored_reason = str(command_result.get("control_reason") or "").strip()
                            if (
                                (stored_actor and stored_actor != normalized_actor)
                                or (stored_reason and stored_reason != normalized_reason)
                                or ("force" in command_result and bool(command_result.get("force")) != force_cancel)
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="workflow_command_owner_specific_cancel_audit_identity_conflict",
                                    command=command,
                                )
                        if command_status == "cancelled" and not command_target_applied:
                            connection.commit()
                            return response(
                                outcome="conflict",
                                reason_code="workflow_command_owner_specific_cancel_boundary_conflict",
                                command=command,
                            )
                        if not command_target_applied and command_status not in {"claimed", "running"}:
                            connection.commit()
                            return response(
                                outcome="conflict",
                                reason_code="workflow_command_owner_specific_cancel_not_applied",
                                command=command,
                            )
                        lease_owner = str(command.get("lease_owner") or "").strip()
                        lease_expires_at = str(command.get("lease_expires_at") or "").strip()
                        lease_active = bool(
                            lease_owner and lease_expires_at and not _timestamp_is_expired(lease_expires_at)
                        )
                        if lease_active and not force_cancel and not command_target_applied:
                            connection.commit()
                            return response(
                                outcome="blocked",
                                reason_code="workflow_command_running_cancel_requires_expired_lease_or_force",
                                command=command,
                            )

                        downstream_ids = {
                            str(item or "").strip()
                            for item in [
                                *_json_load_list(command.get("downstream_command_ids_json")),
                                *list(command_result.get("downstream_command_ids") or []),
                            ]
                            if str(item or "").strip()
                        }

                        workflow_run_id = str(command.get("workflow_run_id") or "").strip()
                        operation_run_id = str(
                            command.get("operation_id") or command_payload.get("operation_run_id") or ""
                        ).strip()
                        requested_workspace_id = str(command_payload.get("workspace_id") or "").strip()
                        requested_run_id = str(
                            command_payload.get("acquisition_run_id") or command_result.get("acquisition_run_id") or ""
                        ).strip()
                        acquisition_run = None
                        if requested_run_id:
                            cursor.execute(
                                "SELECT * FROM acquisition_runs WHERE acquisition_run_id = %s FOR UPDATE",
                                (requested_run_id,),
                            )
                            acquisition_run = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if acquisition_run is None and bool(config["resolve_run_from_command"]):
                            candidate_clauses: list[str] = []
                            candidate_params: list[Any] = []
                            if operation_run_id:
                                candidate_clauses.append("operation_run_id = %s")
                                candidate_params.append(operation_run_id)
                            if workflow_run_id:
                                candidate_clauses.append("workflow_run_id = %s")
                                candidate_params.append(workflow_run_id)
                            candidate_rows: list[dict[str, Any]] = []
                            if candidate_clauses:
                                cursor.execute(
                                    "SELECT * FROM acquisition_runs WHERE "
                                    + " OR ".join(candidate_clauses)
                                    + " ORDER BY acquisition_run_id FOR UPDATE",
                                    tuple(candidate_params),
                                )
                                candidate_rows = _fetch_all_dict_rows(cursor)
                            matching_runs = [
                                row
                                for row in candidate_rows
                                if str(_json_load_dict(row.get("metadata_json")).get("source_command_id") or "").strip()
                                == normalized_command_id
                            ]
                            if len(matching_runs) > 1:
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_run_identity_ambiguous",
                                    command=command,
                                )
                            acquisition_run = matching_runs[0] if matching_runs else None
                        run_workspace_id = (
                            str((acquisition_run or {}).get("workspace_id") or "default").strip() or "default"
                        )
                        scope_workspace_id = requested_workspace_id or run_workspace_id or "default"
                        if acquisition_run is not None:
                            run_workflow_id = str(acquisition_run.get("workflow_run_id") or "").strip()
                            run_operation_id = str(acquisition_run.get("operation_run_id") or "").strip()
                            if (
                                (requested_workspace_id and run_workspace_id != scope_workspace_id)
                                or (workflow_run_id and run_workflow_id != workflow_run_id)
                                or (operation_run_id and run_operation_id and run_operation_id != operation_run_id)
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_run_identity_mismatch",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                )

                        acquisition_run_id = str(
                            (acquisition_run or {}).get("acquisition_run_id") or requested_run_id or ""
                        ).strip()
                        activities: list[dict[str, Any]] = []
                        if normalized_cancel_kind == "acquisition_scale_plan_before_discovery" and acquisition_run:
                            cursor.execute(
                                """
                                SELECT * FROM workflow_activity_runs
                                WHERE (acquisition_run_id = %s AND command_id = %s)
                                   OR activity_run_id IN (
                                       SELECT activity_run_id
                                       FROM acquisition_discovery_lanes
                                       WHERE acquisition_run_id = %s
                                         AND source_command_id = %s
                                         AND activity_run_id <> ''
                                   )
                                ORDER BY activity_run_id
                                FOR UPDATE
                                """,
                                (
                                    acquisition_run_id,
                                    normalized_command_id,
                                    acquisition_run_id,
                                    normalized_command_id,
                                ),
                            )
                            activities = _fetch_all_dict_rows(cursor)
                        elif normalized_cancel_kind == "profile_fetch_activity_before_cache_lookup_attempt":
                            activity_clauses = [
                                "workflow_run_id = %s",
                                "command_id = %s",
                                "activity_type = %s",
                            ]
                            activity_params: list[Any] = [
                                workflow_run_id,
                                normalized_command_id,
                                str(config["activity_type"]),
                            ]
                            if acquisition_run_id:
                                activity_clauses.append("acquisition_run_id = %s")
                                activity_params.append(acquisition_run_id)
                            cursor.execute(
                                "SELECT * FROM workflow_activity_runs WHERE "
                                + " AND ".join(activity_clauses)
                                + " ORDER BY activity_run_id FOR UPDATE",
                                tuple(activity_params),
                            )
                            activities = _fetch_all_dict_rows(cursor)

                        lanes: list[dict[str, Any]] = []
                        if normalized_cancel_kind == "acquisition_scale_plan_before_discovery" and acquisition_run:
                            cursor.execute(
                                """
                                SELECT * FROM acquisition_discovery_lanes
                                WHERE acquisition_run_id = %s AND source_command_id = %s
                                ORDER BY lane_id
                                FOR UPDATE
                                """,
                                (acquisition_run_id, normalized_command_id),
                            )
                            lanes = _fetch_all_dict_rows(cursor)

                        lane_activity_ids = {
                            str(lane.get("activity_run_id") or "").strip()
                            for lane in lanes
                            if str(lane.get("activity_run_id") or "").strip()
                        }
                        for activity in activities:
                            activity_id = str(activity.get("activity_run_id") or "").strip()
                            activity_workspace_id = str(activity.get("workspace_id") or "default").strip() or "default"
                            activity_workflow_id = str(activity.get("workflow_run_id") or "").strip()
                            activity_operation_id = str(activity.get("operation_run_id") or "").strip()
                            activity_acquisition_id = str(activity.get("acquisition_run_id") or "").strip()
                            activity_command_id = str(activity.get("command_id") or "").strip()
                            command_matches = activity_command_id == normalized_command_id
                            lane_link_matches = activity_id in lane_activity_ids
                            if (
                                activity_workspace_id != scope_workspace_id
                                or (workflow_run_id and activity_workflow_id != workflow_run_id)
                                or (
                                    operation_run_id
                                    and activity_operation_id
                                    and activity_operation_id != operation_run_id
                                )
                                or (
                                    acquisition_run_id
                                    and activity_acquisition_id
                                    and activity_acquisition_id != acquisition_run_id
                                )
                                or not (command_matches or lane_link_matches)
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_activity_identity_mismatch",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )
                        for lane in lanes:
                            if (
                                (str(lane.get("workspace_id") or "default").strip() or "default") != scope_workspace_id
                                or str(lane.get("workflow_run_id") or "").strip() != workflow_run_id
                                or str(lane.get("acquisition_run_id") or "").strip() != acquisition_run_id
                                or str(lane.get("source_command_id") or "").strip() != normalized_command_id
                                or (
                                    operation_run_id
                                    and str(lane.get("operation_run_id") or "").strip()
                                    and str(lane.get("operation_run_id") or "").strip() != operation_run_id
                                )
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_lane_identity_mismatch",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )

                        activity_ids = [
                            str(row.get("activity_run_id") or "").strip()
                            for row in activities
                            if str(row.get("activity_run_id") or "").strip()
                        ]
                        attempt_rows: list[dict[str, Any]] = []
                        if activity_ids:
                            placeholders = ", ".join(["%s"] * len(activity_ids))
                            cursor.execute(
                                f"SELECT * FROM workflow_activity_attempts "
                                f"WHERE command_id = %s OR activity_run_id IN ({placeholders}) "
                                "ORDER BY attempt_id FOR UPDATE",
                                (normalized_command_id, *activity_ids),
                            )
                        else:
                            cursor.execute(
                                """
                                SELECT * FROM workflow_activity_attempts
                                WHERE command_id = %s
                                ORDER BY attempt_id
                                FOR UPDATE
                                """,
                                (normalized_command_id,),
                            )
                        attempt_rows = _fetch_all_dict_rows(cursor)
                        delta_params: list[Any] = [normalized_command_id]
                        delta_where = "command_id = %s"
                        if activity_ids:
                            delta_placeholders = ", ".join(["%s"] * len(activity_ids))
                            delta_where += f" OR activity_run_id IN ({delta_placeholders})"
                            delta_params.extend(activity_ids)
                        cursor.execute(
                            "SELECT * FROM workflow_entity_deltas WHERE "
                            + delta_where
                            + " ORDER BY delta_id FOR UPDATE",
                            tuple(delta_params),
                        )
                        entity_delta_rows = _fetch_all_dict_rows(cursor)
                        child_params: list[Any] = [normalized_command_id]
                        child_where = "parent_command_id = %s"
                        if downstream_ids:
                            child_placeholders = ", ".join(["%s"] * len(downstream_ids))
                            child_where += f" OR command_id IN ({child_placeholders})"
                            child_params.extend(sorted(downstream_ids))
                        cursor.execute(
                            "SELECT * FROM workflow_commands WHERE " + child_where + " ORDER BY command_id FOR UPDATE",
                            tuple(child_params),
                        )
                        downstream_rows = _fetch_all_dict_rows(cursor)
                        downstream_ids.update(
                            str(row.get("command_id") or "").strip()
                            for row in downstream_rows
                            if str(row.get("command_id") or "").strip()
                        )
                        sorted_downstream_ids = sorted(downstream_ids)
                        lane_downstream_ids = sorted(
                            {
                                str(item or "").strip()
                                for lane in lanes
                                for item in _json_load_list(lane.get("downstream_command_ids_json"))
                                if str(item or "").strip()
                            }
                        )
                        if sorted_downstream_ids:
                            connection.commit()
                            return response(
                                outcome="blocked",
                                reason_code=str(config["downstream_block_reason"]),
                                command=command,
                                acquisition_run=acquisition_run,
                                activity_runs=activities,
                                discovery_lanes=lanes,
                                downstream_command_ids=sorted_downstream_ids,
                                lane_downstream_command_ids=lane_downstream_ids,
                                activity_attempt_count=len(attempt_rows),
                                entity_delta_count=len(entity_delta_rows),
                            )
                        if attempt_rows or entity_delta_rows or lane_downstream_ids:
                            connection.commit()
                            return response(
                                outcome="blocked",
                                reason_code=str(config["effect_block_reason"]),
                                command=command,
                                acquisition_run=acquisition_run,
                                activity_runs=activities,
                                discovery_lanes=lanes,
                                lane_downstream_command_ids=lane_downstream_ids,
                                activity_attempt_count=len(attempt_rows),
                                entity_delta_count=len(entity_delta_rows),
                            )

                        run_target_status = str(config["run_status"])
                        if acquisition_run is not None:
                            current_run_status = str(acquisition_run.get("status") or "").strip()
                            if current_run_status in run_terminal_statuses and current_run_status != run_target_status:
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_terminal_state_won",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )
                        activity_target_status = str(config["activity_status"])
                        for activity in activities:
                            current_activity_status = str(activity.get("status") or "").strip()
                            if (
                                current_activity_status in activity_terminal_statuses
                                and current_activity_status != activity_target_status
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_terminal_state_won",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )
                        for lane in lanes:
                            current_lane_status = str(lane.get("status") or "").strip()
                            if (
                                current_lane_status in lane_terminal_statuses
                                and current_lane_status != activity_target_status
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_terminal_state_won",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )
                        target_rows = [
                            (acquisition_run, run_target_status),
                            *((activity, activity_target_status) for activity in activities),
                            *((lane, activity_target_status) for lane in lanes),
                        ]
                        for target_row, target_status in target_rows:
                            if target_row is None or str(target_row.get("status") or "").strip() != target_status:
                                continue
                            target_metadata = _json_load_dict(target_row.get("metadata_json"))
                            persisted_actor = str(target_metadata.get("cancelled_by") or "").strip()
                            persisted_reason = str(target_metadata.get("cancel_reason") or "").strip()
                            if (persisted_actor and persisted_actor != normalized_actor) or (
                                persisted_reason and persisted_reason != normalized_reason
                            ):
                                connection.commit()
                                return response(
                                    outcome="conflict",
                                    reason_code="acquisition_owner_cancel_audit_identity_conflict",
                                    command=command,
                                    acquisition_run=acquisition_run,
                                    activity_runs=activities,
                                    discovery_lanes=lanes,
                                )

                        now = _utc_now_sql_timestamp()
                        control_metadata = {
                            "cancelled_by": normalized_actor,
                            "cancel_reason": normalized_reason,
                            "control_source": "api.workflow_command_owner_specific_cancel",
                        }
                        module_changed = False
                        partial_target_seen = command_target_applied
                        if acquisition_run is not None:
                            run_metadata = _json_load_dict(acquisition_run.get("metadata_json"))
                            run_was_target = str(acquisition_run.get("status") or "").strip() == run_target_status
                            partial_target_seen = partial_target_seen or run_was_target
                            desired_run_metadata = dict(run_metadata)
                            for key, value in control_metadata.items():
                                if not run_was_target or not desired_run_metadata.get(key):
                                    desired_run_metadata[key] = value
                            desired_run_metadata.update(dict(config["run_metadata_flags"]))
                            desired_run_metadata.update(
                                {
                                    "last_phase_command_id": normalized_command_id,
                                    "last_phase_command_type": command_type,
                                    "normal_path_executes_queue_workflow_inline": False,
                                    "legacy_job_shell_created": False,
                                }
                            )
                            if normalized_cancel_kind == "acquisition_scale_plan_before_discovery":
                                desired_run_metadata.update(
                                    {
                                        "cancelled_activity_run_count": len(activities),
                                        "cancelled_discovery_lane_count": len(lanes),
                                    }
                                )
                            run_needs_update = (
                                str(acquisition_run.get("status") or "").strip() != run_target_status
                                or str(acquisition_run.get("current_phase") or "").strip() != "cancelled"
                                or desired_run_metadata != run_metadata
                            )
                            if run_needs_update:
                                cursor.execute(
                                    """
                                    UPDATE acquisition_runs
                                    SET status = %s, current_phase = 'cancelled', metadata_json = %s, updated_at = %s
                                    WHERE acquisition_run_id = %s
                                    RETURNING *
                                    """,
                                    (
                                        run_target_status,
                                        _json_dump(desired_run_metadata),
                                        now,
                                        acquisition_run_id,
                                    ),
                                )
                                acquisition_run = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if acquisition_run is None:
                                    raise RuntimeError("acquisition owner cancel lost the locked acquisition run")
                                module_changed = True

                        updated_activities: list[dict[str, Any]] = []
                        for activity in activities:
                            activity_metadata = _json_load_dict(activity.get("metadata_json"))
                            activity_was_target = str(activity.get("status") or "").strip() == activity_target_status
                            partial_target_seen = partial_target_seen or activity_was_target
                            desired_activity_metadata = dict(activity_metadata)
                            for key, value in control_metadata.items():
                                if not activity_was_target or not desired_activity_metadata.get(key):
                                    desired_activity_metadata[key] = value
                            if normalized_cancel_kind == "acquisition_scale_plan_before_discovery":
                                desired_activity_metadata["discovery_command_planned"] = False
                            else:
                                desired_activity_metadata.update(
                                    {
                                        "activity_attempt_started": False,
                                        "profile_entity_delta_recorded": False,
                                    }
                                )
                            activity_needs_update = (
                                str(activity.get("status") or "").strip() != activity_target_status
                                or str(activity.get("phase") or "").strip() != "cancelled"
                                or desired_activity_metadata != activity_metadata
                            )
                            if activity_needs_update:
                                cursor.execute(
                                    """
                                    UPDATE workflow_activity_runs
                                    SET status = %s, phase = 'cancelled', metadata_json = %s, updated_at = %s
                                    WHERE activity_run_id = %s
                                    RETURNING *
                                    """,
                                    (
                                        activity_target_status,
                                        _json_dump(desired_activity_metadata),
                                        now,
                                        str(activity.get("activity_run_id") or "").strip(),
                                    ),
                                )
                                activity = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if activity is None:
                                    raise RuntimeError("acquisition owner cancel lost a locked activity run")
                                module_changed = True
                            updated_activities.append(activity)
                        activities = updated_activities

                        updated_lanes: list[dict[str, Any]] = []
                        for lane in lanes:
                            lane_metadata = _json_load_dict(lane.get("metadata_json"))
                            lane_was_target = str(lane.get("status") or "").strip() == activity_target_status
                            partial_target_seen = partial_target_seen or lane_was_target
                            desired_lane_metadata = dict(lane_metadata)
                            for key, value in control_metadata.items():
                                if not lane_was_target or not desired_lane_metadata.get(key):
                                    desired_lane_metadata[key] = value
                            desired_lane_metadata["discovery_command_planned"] = False
                            lane_plan = _json_load_dict(lane.get("lane_plan_json"))
                            desired_lane_plan = {
                                **lane_plan,
                                "status": activity_target_status,
                                "phase": "cancelled",
                            }
                            lane_needs_update = (
                                str(lane.get("status") or "").strip() != activity_target_status
                                or str(lane.get("phase") or "").strip() != "cancelled"
                                or desired_lane_metadata != lane_metadata
                                or desired_lane_plan != lane_plan
                            )
                            if lane_needs_update:
                                cursor.execute(
                                    """
                                    UPDATE acquisition_discovery_lanes
                                    SET status = %s,
                                        phase = 'cancelled',
                                        lane_plan_json = %s,
                                        metadata_json = %s,
                                        updated_at = %s
                                    WHERE lane_id = %s
                                    RETURNING *
                                    """,
                                    (
                                        activity_target_status,
                                        _json_dump(desired_lane_plan),
                                        _json_dump(desired_lane_metadata),
                                        now,
                                        str(lane.get("lane_id") or "").strip(),
                                    ),
                                )
                                lane = _fetch_one_dict_row(cursor, cursor.fetchone())
                                if lane is None:
                                    raise RuntimeError("acquisition owner cancel lost a locked discovery lane")
                                module_changed = True
                            updated_lanes.append(lane)
                        lanes = updated_lanes

                        result_patch: dict[str, Any] = {
                            "control_source": "api.workflow_command_owner_specific_cancel",
                            "control_action": "cancel",
                            "owner_specific_control": True,
                            "owner_cancel_kind": normalized_cancel_kind,
                            "cancel_boundary": normalized_cancel_kind,
                            "acquisition_run_id": acquisition_run_id,
                            "acquisition_run_cancelled": bool(acquisition_run),
                            "downstream_command_planned": False,
                            "force": force_cancel,
                        }
                        if normalized_cancel_kind == "acquisition_scale_plan_before_discovery":
                            result_patch.update(
                                {
                                    "activity_run_cancelled_count": len(activities),
                                    "discovery_lane_cancelled_count": len(lanes),
                                    "activity_attempt_started": False,
                                }
                            )
                        elif normalized_cancel_kind == "profile_fetch_activity_before_cache_lookup_attempt":
                            result_patch.update(
                                {
                                    "activity_run_cancelled_count": len(activities),
                                    "activity_attempt_started": False,
                                    "profile_entity_delta_recorded": False,
                                }
                            )
                        desired_result = {**command_result, **result_patch}
                        if command_target_applied:
                            desired_result["force"] = command_result.get("force", force_cancel)
                        desired_result.update(
                            {
                                "control_action": "cancel",
                                "control_reason": command_result.get("control_reason", normalized_reason)
                                if command_target_applied
                                else normalized_reason,
                                "control_actor": command_result.get("control_actor", normalized_actor)
                                if command_target_applied
                                else normalized_actor,
                            }
                        )
                        command_needs_update = (
                            command_status != "cancelled"
                            or str(command.get("lease_owner") or "").strip()
                            or str(command.get("lease_expires_at") or "").strip()
                            or str(command.get("not_before_at") or "").strip()
                            or str(command.get("last_error") or "").strip()
                            != str(desired_result.get("control_reason") or "").strip()
                            or desired_result != command_result
                        )
                        command_changed = False
                        if command_needs_update:
                            cursor.execute(
                                """
                                UPDATE workflow_commands
                                SET status = 'cancelled',
                                    lease_owner = '',
                                    lease_expires_at = '',
                                    heartbeat_at = %s,
                                    not_before_at = '',
                                    last_error = %s,
                                    result_json = %s,
                                    updated_at = %s
                                WHERE command_id = %s
                                RETURNING *
                                """,
                                (
                                    now,
                                    str(desired_result.get("control_reason") or "").strip(),
                                    _json_dump(desired_result),
                                    now,
                                    normalized_command_id,
                                ),
                            )
                            command = _fetch_one_dict_row(cursor, cursor.fetchone())
                            if command is None:
                                raise RuntimeError("acquisition owner cancel lost the locked workflow command")
                            command_changed = True
                    connection.commit()
                    if not module_changed and not command_changed:
                        outcome = "already_applied"
                    elif partial_target_seen:
                        outcome = "repaired"
                    else:
                        outcome = "applied"
                    return response(
                        outcome=outcome,
                        reason_code="",
                        command=command,
                        acquisition_run=acquisition_run,
                        activity_runs=activities,
                        discovery_lanes=lanes,
                        module_state_mutated=module_changed,
                    )
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def cancel_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
        from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        current = self.select_one("workflow_commands", where_sql="command_id = %s", params=[normalized_command_id])
        if current is None:
            return None
        existing_result = _json_load_dict(current.get("result_json"))
        next_result = {
            **existing_result,
            **dict(result or {}),
            "control_action": "cancel",
            "control_reason": str(reason or "").strip(),
            "control_actor": str(actor or "").strip(),
        }
        allowed_statuses = [
            str(status or "").strip()
            for status in list(from_statuses or ("queued", "retry_wait"))
            if str(status or "").strip()
        ]
        if not allowed_statuses:
            allowed_statuses = ["queued", "retry_wait"]
        placeholders = ", ".join(["%s"] * len(allowed_statuses))
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            f"""
            UPDATE workflow_commands
            SET status = 'cancelled',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = '',
                last_error = %s,
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ({placeholders})
            RETURNING *
            """,
            (
                now,
                str(reason or "cancelled_by_command_control").strip() or "cancelled_by_command_control",
                _json_dump(next_result),
                now,
                normalized_command_id,
                *allowed_statuses,
            ),
        )

    def retry_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        current = self.select_one("workflow_commands", where_sql="command_id = %s", params=[normalized_command_id])
        if current is None:
            return None
        existing_result = _json_load_dict(current.get("result_json"))
        next_result = {
            **existing_result,
            **dict(result or {}),
            "control_action": "retry",
            "control_reason": str(reason or "").strip(),
            "control_actor": str(actor or "").strip(),
        }
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = 'queued',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = '',
                not_before_at = '',
                attempt = 0,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ('failed_terminal', 'cancelled')
            RETURNING *
            """,
            (_json_dump(next_result), now, normalized_command_id),
        )

    def resume_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return None
        current = self.select_one("workflow_commands", where_sql="command_id = %s", params=[normalized_command_id])
        if current is None:
            return None
        existing_result = _json_load_dict(current.get("result_json"))
        attempt = max(0, int(current.get("attempt") or 0) - 1)
        next_result = {
            **existing_result,
            **dict(result or {}),
            "control_action": "resume",
            "control_reason": str(reason or "").strip(),
            "control_actor": str(actor or "").strip(),
        }
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = 'queued',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = '',
                not_before_at = '',
                attempt = %s,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status = 'retry_wait'
            RETURNING *
            """,
            (attempt, _json_dump(next_result), now, normalized_command_id),
        )

    def enqueue_runtime_outbox(
        self,
        row: dict[str, Any] | None = None,
        *,
        table_name: str = "runtime_outbox",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "runtime_outbox":
            raise ValueError("enqueue_runtime_outbox requires table_name=runtime_outbox")
        if not self.should_prefer_read("runtime_outbox"):
            return None
        self._ensure_runtime_coordination_schema()
        payload = _normalize_postgres_row_payload(dict(row or {}))
        outbox_type = str(payload.get("outbox_type") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        if not outbox_type or not idempotency_key:
            return None
        now = _utc_now_sql_timestamp()
        outbox_id = str(payload.get("outbox_id") or "").strip() or (
            "out_" + sha1(idempotency_key.encode("utf-8")).hexdigest()[:24]
        )
        row_payload = {
            "outbox_id": outbox_id,
            "workflow_run_id": str(payload.get("workflow_run_id") or "").strip(),
            "operation_id": str(payload.get("operation_id") or "").strip(),
            "command_id": str(payload.get("command_id") or "").strip(),
            "outbox_type": outbox_type,
            "status": str(payload.get("status") or "queued").strip() or "queued",
            "idempotency_key": idempotency_key,
            "payload_json": str(payload.get("payload_json") or "{}"),
            "not_before_at": str(payload.get("not_before_at") or "").strip(),
            "attempt": int(payload.get("attempt") or 0),
            "max_attempts": max(1, int(payload.get("max_attempts") or 5)),
            "lease_owner": str(payload.get("lease_owner") or "").strip(),
            "lease_expires_at": str(payload.get("lease_expires_at") or "").strip(),
            "dispatched_at": str(payload.get("dispatched_at") or "").strip(),
            "last_error": str(payload.get("last_error") or "").strip(),
            "schema_version": str(payload.get("schema_version") or "runtime_outbox_v1").strip(),
            "created_at": str(payload.get("created_at") or now).strip(),
            "updated_at": str(payload.get("updated_at") or now).strip(),
        }
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        for lock_key in sorted(
                            {
                                f"runtime_outbox:id:{outbox_id}",
                                f"runtime_outbox:idempotency:{idempotency_key}",
                            }
                        ):
                            self._acquire_transaction_lock(cursor, lock_key)
                        cursor.execute(
                            """
                            SELECT * FROM runtime_outbox
                            WHERE outbox_id = %s OR idempotency_key = %s
                            ORDER BY outbox_id
                            FOR UPDATE
                            """,
                            (outbox_id, idempotency_key),
                        )
                        identity_rows = _fetch_all_dict_rows(cursor)
                        if len(identity_rows) > 1:
                            raise ValueError("runtime_outbox identity collision: id and idempotency rows differ")
                        if identity_rows:
                            existing = identity_rows[0]
                            self._validate_runtime_outbox_identity(existing, expected=row_payload)
                            connection.commit()
                            return existing
                        columns = list(row_payload)
                        cursor.execute(
                            (
                                f"INSERT INTO runtime_outbox "
                                f"({', '.join(_quote_identifier(column) for column in columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
                            ),
                            tuple(row_payload[column] for column in columns),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return inserted
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    @staticmethod
    def _validate_runtime_outbox_identity(
        outbox: dict[str, Any],
        *,
        expected: dict[str, Any],
    ) -> None:
        identity_fields = (
            "outbox_id",
            "workflow_run_id",
            "operation_id",
            "command_id",
            "outbox_type",
            "idempotency_key",
        )
        mismatches = [
            field
            for field in identity_fields
            if str(outbox.get(field) or "").strip() != str(expected.get(field) or "").strip()
        ]
        if mismatches:
            raise ValueError("runtime_outbox immutable identity collision: " + ", ".join(mismatches))

    def mark_runtime_outbox_dispatched(
        self,
        outbox_id: str,
        *,
        table_name: str = "runtime_outbox",
        lease_owner: str = "",
    ) -> dict[str, Any] | None:
        if _normalize_postgres_identifier(table_name) != "runtime_outbox":
            raise ValueError("mark_runtime_outbox_dispatched requires table_name=runtime_outbox")
        if not self.should_prefer_read("runtime_outbox"):
            return None
        normalized_outbox_id = str(outbox_id or "").strip()
        if not normalized_outbox_id:
            return None
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, f"runtime_outbox:id:{normalized_outbox_id}")
                        cursor.execute(
                            "SELECT * FROM runtime_outbox WHERE outbox_id = %s FOR UPDATE",
                            (normalized_outbox_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if current is None:
                            connection.commit()
                            return None
                        current_status = str(current.get("status") or "").strip()
                        if current_status == "dispatched":
                            connection.commit()
                            return current
                        normalized_lease_owner = str(lease_owner or "").strip()
                        if current_status in {"claimed", "running"} and (
                            not normalized_lease_owner
                            or normalized_lease_owner != str(current.get("lease_owner") or "").strip()
                        ):
                            raise ValueError("runtime_outbox dispatch lease-owner mismatch")
                        if current_status not in {"queued", "claimed", "running"}:
                            raise ValueError(
                                f"runtime_outbox cannot dispatch from status {current_status or '<empty>'}"
                            )
                        cursor.execute(
                            """
                            UPDATE runtime_outbox
                            SET status = 'dispatched',
                                dispatched_at = %s,
                                lease_owner = '',
                                lease_expires_at = '',
                                last_error = '',
                                updated_at = %s
                            WHERE outbox_id = %s
                            RETURNING *
                            """,
                            (now, now, normalized_outbox_id),
                        )
                        committed = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return committed
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def acquire_linkedin_profile_registry_lease(
        self,
        profile_url_key: str,
        *,
        lease_owner: str,
        lease_seconds: int = 240,
        lease_token: str = "",
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("linkedin_profile_registry_leases"):
            return None
        normalized_key = str(profile_url_key or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_key or not normalized_owner or not normalized_token:
            return None
        self._ensure_control_plane_writer_schema()
        now = _utc_now_sql_timestamp()
        expires_at = _expiry_timestamp(int(lease_seconds or 240))
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))",
                            (self._advisory_lock_key(f"linkedin_profile:{normalized_key}"),),
                        )
                        cursor.execute(
                            """
                            INSERT INTO linkedin_profile_registry_leases (
                                profile_url_key,
                                lease_owner,
                                lease_token,
                                lease_expires_at,
                                created_at,
                                updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (profile_url_key) DO UPDATE SET
                                lease_owner = EXCLUDED.lease_owner,
                                lease_token = EXCLUDED.lease_token,
                                lease_expires_at = EXCLUDED.lease_expires_at,
                                updated_at = EXCLUDED.updated_at
                            WHERE linkedin_profile_registry_leases.lease_expires_at <= %s
                               OR linkedin_profile_registry_leases.lease_owner = EXCLUDED.lease_owner
                               OR linkedin_profile_registry_leases.lease_token = EXCLUDED.lease_token
                            RETURNING *
                            """,
                            (
                                normalized_key,
                                normalized_owner,
                                normalized_token,
                                expires_at,
                                now,
                                now,
                                now,
                            ),
                        )
                        row = cursor.fetchone()
                        result = _fetch_one_dict_row(cursor, row)
                        if result is None:
                            cursor.execute(
                                """
                                SELECT *
                                FROM linkedin_profile_registry_leases
                                WHERE profile_url_key = %s
                                LIMIT 1
                                """,
                                (normalized_key,),
                            )
                            result = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                return result
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def acquire_linkedin_profile_registry_leases(
        self,
        profile_url_keys: list[str] | tuple[str, ...],
        *,
        lease_owner: str,
        lease_seconds: int = 240,
        lease_token: str = "",
    ) -> list[dict[str, Any]] | None:
        if not self.should_prefer_read("linkedin_profile_registry_leases"):
            return None
        normalized_keys: list[str] = []
        for profile_url_key in list(profile_url_keys or []):
            normalized_key = str(profile_url_key or "").strip()
            if normalized_key and normalized_key not in normalized_keys:
                normalized_keys.append(normalized_key)
        normalized_keys = sorted(normalized_keys)
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_keys or not normalized_owner or not normalized_token:
            return []
        self._ensure_control_plane_writer_schema()
        now = _utc_now_sql_timestamp()
        expires_at = _expiry_timestamp(int(lease_seconds or 240))
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))",
                            (
                                self._advisory_lock_key(
                                    f"linkedin_profile_batch:{sha1('|'.join(normalized_keys).encode('utf-8')).hexdigest()}"
                                ),
                            ),
                        )
                        values_sql = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(normalized_keys))
                        values_params: list[Any] = []
                        for normalized_key in normalized_keys:
                            values_params.extend(
                                [
                                    normalized_key,
                                    normalized_owner,
                                    normalized_token,
                                    expires_at,
                                    now,
                                    now,
                                ]
                            )
                        cursor.execute(
                            f"""
                            INSERT INTO linkedin_profile_registry_leases (
                                profile_url_key,
                                lease_owner,
                                lease_token,
                                lease_expires_at,
                                created_at,
                                updated_at
                            ) VALUES {values_sql}
                            ON CONFLICT (profile_url_key) DO UPDATE SET
                                lease_owner = EXCLUDED.lease_owner,
                                lease_token = EXCLUDED.lease_token,
                                lease_expires_at = EXCLUDED.lease_expires_at,
                                updated_at = EXCLUDED.updated_at
                            WHERE linkedin_profile_registry_leases.lease_expires_at <= %s
                               OR linkedin_profile_registry_leases.lease_owner = EXCLUDED.lease_owner
                               OR linkedin_profile_registry_leases.lease_token = EXCLUDED.lease_token
                            """,
                            tuple(values_params + [now]),
                        )
                        key_placeholders = ", ".join(["%s"] * len(normalized_keys))
                        cursor.execute(
                            f"""
                            SELECT *
                            FROM linkedin_profile_registry_leases
                            WHERE profile_url_key IN ({key_placeholders})
                            """,
                            tuple(normalized_keys),
                        )
                        rows = _fetch_all_dict_rows(cursor)
                    connection.commit()
                return rows
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def release_linkedin_profile_registry_leases(
        self,
        profile_url_keys: list[str] | tuple[str, ...],
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> int:
        if not self.should_prefer_read("linkedin_profile_registry_leases"):
            return 0
        normalized_keys: list[str] = []
        for profile_url_key in list(profile_url_keys or []):
            normalized_key = str(profile_url_key or "").strip()
            if normalized_key and normalized_key not in normalized_keys:
                normalized_keys.append(normalized_key)
        normalized_keys = sorted(normalized_keys)
        if not normalized_keys:
            return 0
        clauses = [f"profile_url_key IN ({', '.join(['%s'] * len(normalized_keys))})"]
        params: list[Any] = list(normalized_keys)
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if normalized_owner:
            clauses.append("lease_owner = %s")
            params.append(normalized_owner)
        if normalized_token:
            clauses.append("lease_token = %s")
            params.append(normalized_token)
        return self._execute_non_query(
            f"DELETE FROM linkedin_profile_registry_leases WHERE {' AND '.join(clauses)}",
            tuple(params),
        )

    def acquire_runtime_provider_limiter_slot(
        self,
        limiter_key: str,
        *,
        lease_owner: str,
        budget: int,
        lease_seconds: int = 7200,
        lease_token: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("runtime_provider_limiter_leases"):
            return None
        normalized_key = str(limiter_key or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip() or f"lease_{uuid4().hex}"
        normalized_budget = max(1, int(budget or 1))
        if not normalized_key or not normalized_owner or not normalized_token:
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        expires_at = _expiry_timestamp(int(lease_seconds or 7200))
        metadata_json = _json_dump(metadata or {})
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))",
                            (self._advisory_lock_key(f"provider_limiter:{normalized_key}"),),
                        )
                        cursor.execute(
                            """
                            DELETE FROM runtime_provider_limiter_leases
                            WHERE limiter_key = %s AND lease_expires_at <= %s
                            """,
                            (normalized_key, now),
                        )
                        cursor.execute(
                            """
                            SELECT *
                            FROM runtime_provider_limiter_leases
                            WHERE lease_token = %s AND limiter_key = %s
                            LIMIT 1
                            """,
                            (normalized_token, normalized_key),
                        )
                        existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if existing is not None:
                            cursor.execute(
                                """
                                UPDATE runtime_provider_limiter_leases
                                SET lease_owner = %s,
                                    lease_expires_at = %s,
                                    metadata_json = %s,
                                    updated_at = %s
                                WHERE lease_token = %s
                                RETURNING *
                                """,
                                (normalized_owner, expires_at, metadata_json, now, normalized_token),
                            )
                            row = _fetch_one_dict_row(cursor, cursor.fetchone()) or existing
                            acquired = True
                        else:
                            cursor.execute(
                                """
                                SELECT COUNT(*) AS active_count
                                FROM runtime_provider_limiter_leases
                                WHERE limiter_key = %s AND lease_expires_at > %s
                                """,
                                (normalized_key, now),
                            )
                            count_row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                            active_before = int(count_row.get("active_count") or 0)
                            if active_before >= normalized_budget:
                                row = None
                                acquired = False
                            else:
                                cursor.execute(
                                    """
                                    INSERT INTO runtime_provider_limiter_leases (
                                        lease_token,
                                        limiter_key,
                                        lease_owner,
                                        lease_expires_at,
                                        metadata_json,
                                        created_at,
                                        updated_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    RETURNING *
                                    """,
                                    (
                                        normalized_token,
                                        normalized_key,
                                        normalized_owner,
                                        expires_at,
                                        metadata_json,
                                        now,
                                        now,
                                    ),
                                )
                                row = _fetch_one_dict_row(cursor, cursor.fetchone())
                                acquired = True
                        cursor.execute(
                            """
                            SELECT COUNT(*) AS active_count
                            FROM runtime_provider_limiter_leases
                            WHERE limiter_key = %s AND lease_expires_at > %s
                            """,
                            (normalized_key, now),
                        )
                        active_count = int(
                            (_fetch_one_dict_row(cursor, cursor.fetchone()) or {}).get("active_count") or 0
                        )
                    connection.commit()
                payload = dict(row or {})
                payload.update(
                    {
                        "acquired": acquired,
                        "limiter_key": normalized_key,
                        "lease_token": normalized_token,
                        "lease_owner": normalized_owner,
                        "active_count": active_count,
                        "budget": normalized_budget,
                        "db_limiter_enabled": True,
                    }
                )
                return payload
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def get_runtime_provider_limiter_status(
        self,
        limiter_key: str,
        *,
        budget: int,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("runtime_provider_limiter_leases"):
            return None
        normalized_key = str(limiter_key or "").strip()
        normalized_budget = max(1, int(budget or 1))
        if not normalized_key:
            return None
        self._ensure_runtime_coordination_schema()
        now = _utc_now_sql_timestamp()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS active_count
                    FROM runtime_provider_limiter_leases
                    WHERE limiter_key = %s AND lease_expires_at > %s
                    """,
                    (normalized_key, now),
                )
                active_count = int((_fetch_one_dict_row(cursor, cursor.fetchone()) or {}).get("active_count") or 0)
            connection.commit()
        available_count = max(0, normalized_budget - active_count)
        return {
            "limiter_key": normalized_key,
            "active_count": active_count,
            "budget": normalized_budget,
            "available_count": available_count,
            "available": available_count > 0,
            "db_limiter_enabled": True,
        }

    def release_runtime_provider_limiter_slot(
        self,
        lease_token: str,
        *,
        limiter_key: str = "",
        lease_owner: str = "",
    ) -> bool:
        if not self.should_prefer_read("runtime_provider_limiter_leases"):
            return False
        normalized_token = str(lease_token or "").strip()
        normalized_key = str(limiter_key or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_token:
            return False
        self._ensure_runtime_coordination_schema()
        clauses = ["lease_token = %s"]
        params: list[Any] = [normalized_token]
        if normalized_key:
            clauses.append("limiter_key = %s")
            params.append(normalized_key)
        if normalized_owner:
            clauses.append("lease_owner = %s")
            params.append(normalized_owner)
        deleted_count = self._execute_non_query(
            f"DELETE FROM runtime_provider_limiter_leases WHERE {' AND '.join(clauses)}",
            tuple(params),
        )
        return bool(deleted_count)

    def supersede_workflow_runtime_state(self, job_id: str) -> dict[str, Any]:
        if not (
            self.should_prefer_read("agent_worker_runs")
            and self.should_prefer_read("agent_trace_spans")
            and self.should_prefer_read("workflow_job_leases")
        ):
            return {"superseded_worker_count": 0, "superseded_trace_count": 0}
        now = _utc_now_sql_timestamp()
        superseded_worker_count = self._execute_non_query(
            """
            UPDATE agent_worker_runs
            SET status = 'superseded',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = %s
            WHERE job_id = %s
              AND status IN ('queued', 'running', 'interrupted', 'failed')
            """,
            (now, str(job_id)),
        )
        superseded_trace_count = self._execute_non_query(
            """
            UPDATE agent_trace_spans
            SET status = CASE WHEN status = 'running' THEN 'superseded' ELSE status END
            WHERE job_id = %s
            """,
            (str(job_id),),
        )
        self.release_workflow_job_lease(str(job_id))
        return {
            "superseded_worker_count": int(superseded_worker_count or 0),
            "superseded_trace_count": int(superseded_trace_count or 0),
        }

    def _ensure_control_plane_writer_schema(self) -> None:
        if self._control_plane_writer_schema_ready or not self.enabled:
            return
        self.ensure_bootstrapped()
        with self._lock:
            if self._control_plane_writer_schema_ready:
                return
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    for table_name, (id_column, sequence_name) in sorted(_SERIAL_SEQUENCE_NAMES.items()):
                        if table_name in _RUNTIME_COORDINATION_TABLES:
                            continue
                        self._repair_serial_identity_column(
                            cursor,
                            table_name=table_name,
                            id_column=id_column,
                            sequence_name=sequence_name,
                        )
                    self._dedupe_agent_runtime_sessions(cursor)
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_runtime_sessions_job_id
                        ON agent_runtime_sessions (job_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_job_events_job_id_event_id
                        ON job_events (job_id, event_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_job_progress_event_summaries_updated
                        ON job_progress_event_summaries (updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_job_results_job_id_rank_index
                        ON job_results (job_id, rank_index)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_target_candidates_updated
                        ON target_candidates (updated_at, follow_up_status)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_target_candidates_candidate
                        ON target_candidates (candidate_id, updated_at)
                        """
                    )
                    for column_name in (
                        "person_identity_key",
                        "candidate_identity_key",
                        "source_projection_id",
                        "source_run_id",
                        "source_collection_id",
                        "source_reason",
                    ):
                        cursor.execute(
                            f"""
                            ALTER TABLE target_candidates
                            ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} TEXT NOT NULL DEFAULT ''
                            """
                        )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_target_candidates_projection
                        ON target_candidates (source_projection_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_target_candidates_person
                        ON target_candidates (person_identity_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_asset_default_pointers_company
                        ON asset_default_pointers (company_key, scope_kind, scope_key, asset_kind)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_asset_default_pointer_history_pointer
                        ON asset_default_pointer_history (pointer_key, occurred_at)
                        """
                    )
                    if _postgres_table_exists(cursor, "target_candidate_public_web_batches"):
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_batches_updated
                            ON target_candidate_public_web_batches (updated_at, status)
                            """
                        )
                    if _postgres_table_exists(cursor, "target_candidate_public_web_runs"):
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_batch
                            ON target_candidate_public_web_runs (batch_id, updated_at)
                            """
                        )
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_record
                            ON target_candidate_public_web_runs (record_id, updated_at)
                            """
                        )
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_status
                            ON target_candidate_public_web_runs (status, updated_at)
                            """
                        )
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_runs_identity
                            ON target_candidate_public_web_runs (linkedin_url_key, updated_at)
                            """
                        )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_batches_updated
                        ON crm_public_web_batches (workspace_id, updated_at, status)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_public_web_batches_idempotency_unique
                        ON crm_public_web_batches (idempotency_key)
                        WHERE idempotency_key != ''
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_runs_batch
                        ON crm_public_web_runs (batch_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_runs_record
                        ON crm_public_web_runs (workspace_id, crm_record_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_runs_status
                        ON crm_public_web_runs (workspace_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_runs_identity
                        ON crm_public_web_runs (person_identity_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_public_web_runs_idempotency_unique
                        ON crm_public_web_runs (idempotency_key)
                        WHERE idempotency_key != ''
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_public_web_assets_identity
                        ON person_public_web_assets (linkedin_url_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_public_web_signals_run
                        ON person_public_web_signals (run_id, signal_kind, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_public_web_signals_record
                        ON person_public_web_signals (record_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_public_web_signals_identity
                        ON person_public_web_signals (person_identity_key, signal_kind, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_assets_identity
                        ON person_assets (person_identity_key, asset_type, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_evidence_identity
                        ON person_evidence (person_identity_key, evidence_type, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_person_assertions_identity
                        ON person_assertions (person_identity_key, assertion_type, verification_status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_raw_profile_index_watermark
                        ON raw_profile_index (raw_profile_index_watermark, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_candidate_evidence_index_watermark
                        ON candidate_evidence_index (evidence_index_watermark, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_projection_person_search_index_projection
                        ON projection_person_search_index (projection_id, count_scope, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_projection_person_search_index_person
                        ON projection_person_search_index (person_identity_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_records_identity
                        ON crm_records (workspace_id, person_identity_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_records_projection
                        ON crm_records (source_projection_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        ALTER TABLE crm_records
                        ADD COLUMN IF NOT EXISTS source_collection_id TEXT NOT NULL DEFAULT ''
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_records_collection
                        ON crm_records (source_collection_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_engagements_record
                        ON crm_engagements (crm_record_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_events_record
                        ON crm_events (crm_record_id, created_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_events_idempotency
                        ON crm_events (workspace_id, idempotency_key)
                        WHERE idempotency_key != ''
                        """
                    )
                    if _postgres_table_exists(cursor, "target_candidate_public_web_promotions"):
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_record
                            ON target_candidate_public_web_promotions (record_id, updated_at)
                            """
                        )
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_signal
                            ON target_candidate_public_web_promotions (signal_id, updated_at)
                            """
                        )
                        cursor.execute(
                            """
                            CREATE INDEX IF NOT EXISTS idx_target_candidate_public_web_promotions_run
                            ON target_candidate_public_web_promotions (run_id, updated_at)
                            """
                        )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_promotions_record
                        ON crm_public_web_promotions (workspace_id, crm_record_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_promotions_signal
                        ON crm_public_web_promotions (signal_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_public_web_promotions_run
                        ON crm_public_web_promotions (run_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_public_web_asset_runs_company
                        ON company_public_web_asset_runs (company_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_public_web_asset_runs_status
                        ON company_public_web_asset_runs (status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_public_web_assets_company
                        ON company_public_web_assets (company_key, source_family, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_public_web_assets_url
                        ON company_public_web_assets (normalized_url_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_frontend_history_links_review
                        ON frontend_history_links (review_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_frontend_history_links_job
                        ON frontend_history_links (job_id, updated_at)
                        """
                    )
                    self._dedupe_organization_asset_registry(cursor)
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_organization_asset_registry_target_snapshot_view
                        ON organization_asset_registry (target_company, snapshot_id, asset_view)
                        """
                    )
                    self._dedupe_organization_execution_profiles(cursor)
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_organization_execution_profiles_target_view
                        ON organization_execution_profiles (target_company, asset_view)
                        """
                    )
                    # Conflict-target unique indexes (with duplicate cleanup) that the
                    # snapshot-sync bootstrap may have skipped on long-lived databases
                    # (the sync short-circuits on unchanged source fingerprints).
                    # _ensure_control_plane_unique_indexes dedupes deliberately —
                    # keeping the newest row per conflict group — before creating
                    # each index; see its docstring for the policy.
                    _ensure_control_plane_unique_indexes(cursor, "criteria_patterns")
                    _ensure_control_plane_unique_indexes(cursor, "job_result_views")
                    ensure_acquisition_shard_registry_split_schema(cursor)
                    for column_name, column_type in (
                        ("refill_queue_state", "TEXT"),
                        ("last_refill_trigger_kind", "TEXT"),
                        ("last_refill_plan_reason", "TEXT"),
                        ("last_refill_deferred_reason", "TEXT"),
                        ("last_refill_planned_at", "TEXT"),
                        ("refill_not_before_at", "TEXT"),
                        ("refill_plan_batch_size", "BIGINT NOT NULL DEFAULT 0"),
                        ("refill_plan_batch_count", "BIGINT NOT NULL DEFAULT 0"),
                        ("refill_plan_window_url_count", "BIGINT NOT NULL DEFAULT 0"),
                        ("last_refill_attempt_count", "BIGINT NOT NULL DEFAULT 0"),
                        ("refill_owner_worker_id", "BIGINT NOT NULL DEFAULT 0"),
                        ("refill_owner_run_id", "TEXT"),
                        ("refill_owner_dataset_id", "TEXT"),
                        ("refill_owner_payload_hash", "TEXT"),
                        ("refill_terminal_status", "TEXT"),
                        ("refill_terminal_at", "TEXT"),
                    ):
                        cursor.execute(
                            f"""
                            ALTER TABLE linkedin_profile_registry
                            ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} {column_type}
                            """
                        )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_status
                        ON linkedin_profile_registry (status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_run
                        ON linkedin_profile_registry (last_run_id, last_dataset_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_refill_queue
                        ON linkedin_profile_registry (refill_queue_state, refill_not_before_at, last_refill_planned_at, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_alias_canonical
                        ON linkedin_profile_registry_aliases (profile_url_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_leases_expires
                        ON linkedin_profile_registry_leases (lease_expires_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_event_type
                        ON linkedin_profile_registry_events (event_type, created_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_event_profile
                        ON linkedin_profile_registry_events (profile_url_key, created_at)
                        """
                    )
                connection.commit()
            self._control_plane_writer_schema_ready = True

    def _dedupe_agent_runtime_sessions(self, cursor: Any) -> None:
        self._delete_duplicate_rows_by_group(
            cursor,
            table_name="agent_runtime_sessions",
            id_column="session_id",
            partition_columns=("job_id",),
            order_by_sql="updated_at DESC, created_at DESC, session_id DESC",
            where_sql="job_id IS NOT NULL AND job_id <> ''",
        )

    def _repair_serial_identity_column(
        self,
        cursor: Any,
        *,
        table_name: str,
        id_column: str,
        sequence_name: str,
    ) -> None:
        quoted_table_name = _quote_identifier(_normalize_postgres_identifier(table_name))
        quoted_id_column = _quote_identifier(_normalize_postgres_identifier(id_column))
        quoted_sequence_name = _quote_identifier(_normalize_postgres_identifier(sequence_name))
        cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {quoted_sequence_name} START WITH 1")
        cursor.execute(
            f"""
            UPDATE {quoted_table_name}
            SET {quoted_id_column} = nextval({_quote_string_literal(sequence_name)}::regclass)
            WHERE {quoted_id_column} IS NULL OR {quoted_id_column} <= 0
            """
        )
        cursor.execute(
            f"""
            WITH ranked AS (
                SELECT
                    ctid AS duplicate_ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY {quoted_id_column}
                        ORDER BY {quoted_id_column} DESC, ctid DESC
                    ) AS duplicate_rank
                FROM {quoted_table_name}
                WHERE {quoted_id_column} IS NOT NULL AND {quoted_id_column} > 0
            )
            UPDATE {quoted_table_name} AS target
            SET {quoted_id_column} = nextval({_quote_string_literal(sequence_name)}::regclass)
            FROM ranked
            WHERE target.ctid = ranked.duplicate_ctid
              AND ranked.duplicate_rank > 1
            """
        )
        cursor.execute(
            f"""
            ALTER TABLE {quoted_table_name}
            ALTER COLUMN {quoted_id_column}
            SET DEFAULT nextval({_quote_string_literal(sequence_name)}::regclass)
            """
        )
        cursor.execute(
            f"""
            SELECT setval(
                {_quote_string_literal(sequence_name)},
                COALESCE(
                    (
                        SELECT MAX({quoted_id_column})
                        FROM {quoted_table_name}
                    ),
                    0
                ) + 1,
                false
            )
            """
        )
        cursor.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_quote_identifier(f"idx_{table_name}_{id_column}_unique")}
            ON {quoted_table_name} ({quoted_id_column})
            """
        )

    def _dedupe_organization_asset_registry(self, cursor: Any) -> None:
        self._delete_duplicate_rows_by_group(
            cursor,
            table_name="organization_asset_registry",
            id_column="registry_id",
            partition_expression_sql=(
                "COALESCE(NULLIF(company_key, ''), regexp_replace(lower(target_company), '[^a-z0-9]+', '', 'g')), "
                "snapshot_id, "
                "asset_view"
            ),
            order_by_sql=(
                "CASE "
                "WHEN NULLIF(target_company, '') IS NOT NULL "
                "AND lower(target_company) <> lower(COALESCE(NULLIF(company_key, ''), target_company)) "
                "THEN 1 ELSE 0 END DESC, "
                "authoritative DESC, "
                "current_lane_effective_ready DESC, "
                "former_lane_effective_ready DESC, "
                "current_lane_effective_candidate_count DESC, "
                "former_lane_effective_candidate_count DESC, "
                "candidate_count DESC, "
                "coalesce(materialization_generation_sequence, 0) DESC, "
                "updated_at DESC, "
                "registry_id DESC"
            ),
        )

    def _dedupe_organization_execution_profiles(self, cursor: Any) -> None:
        self._delete_duplicate_rows_by_group(
            cursor,
            table_name="organization_execution_profiles",
            id_column="profile_id",
            partition_expression_sql=(
                "COALESCE(NULLIF(company_key, ''), regexp_replace(lower(target_company), '[^a-z0-9]+', '', 'g')), "
                "asset_view"
            ),
            order_by_sql=(
                "CASE "
                "WHEN NULLIF(target_company, '') IS NOT NULL "
                "AND lower(target_company) <> lower(COALESCE(NULLIF(company_key, ''), target_company)) "
                "THEN 1 ELSE 0 END DESC, "
                "completeness_score DESC, "
                "baseline_candidate_count DESC, "
                "current_lane_effective_candidate_count DESC, "
                "former_lane_effective_candidate_count DESC, "
                "coalesce(source_generation_sequence, 0) DESC, "
                "updated_at DESC, "
                "profile_id DESC"
            ),
        )

    def _delete_duplicate_rows_by_group(
        self,
        cursor: Any,
        *,
        table_name: str,
        id_column: str,
        partition_columns: tuple[str, ...] = (),
        partition_expression_sql: str = "",
        order_by_sql: str,
        where_sql: str = "",
    ) -> None:
        normalized_table_name = _normalize_postgres_identifier(table_name)
        normalized_id_column = _normalize_postgres_identifier(id_column)
        normalized_partition_columns = tuple(
            _normalize_postgres_identifier(column)
            for column in partition_columns
            if _normalize_postgres_identifier(column)
        )
        normalized_partition_expression_sql = str(partition_expression_sql or "").strip()
        if (
            not normalized_table_name
            or not normalized_id_column
            or (not normalized_partition_columns and not normalized_partition_expression_sql)
        ):
            return
        partition_sql = normalized_partition_expression_sql or ", ".join(
            _quote_identifier(column) for column in normalized_partition_columns
        )
        quoted_table_name = _quote_identifier(normalized_table_name)
        quoted_id_column = _quote_identifier(normalized_id_column)
        qualified_where_sql = f"WHERE {where_sql}" if str(where_sql or "").strip() else ""
        cursor.execute(
            f"""
            WITH ranked AS (
                SELECT
                    {quoted_id_column} AS duplicate_row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {order_by_sql}
                    ) AS duplicate_rank
                FROM {quoted_table_name}
                {qualified_where_sql}
            )
            DELETE FROM {quoted_table_name}
            WHERE {quoted_id_column} IN (
                SELECT duplicate_row_id
                FROM ranked
                WHERE duplicate_rank > 1
            )
            """
        )

    def _upsert_job_progress_event_summary(
        self,
        job_id: str,
        *,
        event: dict[str, Any],
    ) -> dict[str, Any] | None:
        existing = self.select_one("job_progress_event_summaries", where_sql="job_id = %s", params=[str(job_id)])
        summary = self._job_progress_event_summary_from_row(existing)
        updated_summary = update_job_progress_event_summary(summary, event=event)
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            INSERT INTO job_progress_event_summaries (
                job_id,
                event_count,
                latest_event_json,
                stage_sequence_json,
                stage_stats_json,
                latest_metrics_json,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) DO UPDATE SET
                event_count = EXCLUDED.event_count,
                latest_event_json = EXCLUDED.latest_event_json,
                stage_sequence_json = EXCLUDED.stage_sequence_json,
                stage_stats_json = EXCLUDED.stage_stats_json,
                latest_metrics_json = EXCLUDED.latest_metrics_json,
                updated_at = EXCLUDED.updated_at
            RETURNING *
            """,
            (
                str(job_id),
                int(updated_summary.get("event_count") or 0),
                _json_dump(updated_summary.get("latest_event") or {}),
                _json_dump(updated_summary.get("stage_sequence") or []),
                _json_dump(updated_summary.get("stage_stats") or {}),
                _json_dump(updated_summary.get("latest_metrics") or {}),
                str((existing or {}).get("created_at") or now),
                now,
            ),
        )

    def _compact_runtime_job_events(self, *, job_id: str, stage: str, payload: dict[str, Any]) -> bool:
        normalized_job_id = str(job_id or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_job_id or normalized_stage not in {"runtime_heartbeat", "runtime_control"}:
            return False
        grouping_key_name = "source" if normalized_stage == "runtime_heartbeat" else "control"
        keep_latest = 12 if normalized_stage == "runtime_heartbeat" else 8
        grouping_value = str(payload.get(grouping_key_name) or "").strip()
        rows = self.select_many(
            "job_events",
            where_sql="job_id = %s AND stage = %s",
            params=[normalized_job_id, normalized_stage],
            order_by_sql="event_id DESC",
            limit=0,
        )
        matched_ids: list[int] = []
        for row in rows:
            row_payload = _json_load_dict(row.get("payload_json"))
            row_grouping_value = str(row_payload.get(grouping_key_name) or "").strip()
            if row_grouping_value != grouping_value:
                continue
            matched_ids.append(int(row.get("event_id") or 0))
        delete_ids = [event_id for event_id in matched_ids[keep_latest:] if event_id > 0]
        if not delete_ids:
            return False
        placeholders = ", ".join(["%s"] * len(delete_ids))
        self._execute_non_query(
            f"DELETE FROM job_events WHERE event_id IN ({placeholders})",
            tuple(delete_ids),
        )
        return True

    def _ensure_runtime_coordination_schema(self) -> None:
        if self._runtime_schema_ready or not self.enabled:
            return
        self.ensure_bootstrapped()
        with self._lock:
            if self._runtime_schema_ready:
                return
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_job_leases (
                            job_id TEXT PRIMARY KEY,
                            lease_owner TEXT NOT NULL,
                            lease_token TEXT NOT NULL,
                            lease_expires_at TEXT NOT NULL,
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_recovery_intents (
                            job_id TEXT PRIMARY KEY,
                            classification TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'pending',
                            requested_at TEXT NOT NULL DEFAULT '',
                            requested_by TEXT NOT NULL DEFAULT '',
                            params_json TEXT NOT NULL DEFAULT '{}',
                            lease_owner TEXT NOT NULL DEFAULT '',
                            lease_expires_at TEXT NOT NULL DEFAULT '',
                            claimed_at TEXT NOT NULL DEFAULT '',
                            schema_version TEXT NOT NULL DEFAULT 'workflow_recovery_intent_v1',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runtime_provider_limiter_leases (
                            lease_token TEXT PRIMARY KEY,
                            limiter_key TEXT NOT NULL,
                            lease_owner TEXT NOT NULL,
                            lease_expires_at TEXT NOT NULL,
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS agent_trace_spans (
                            span_id BIGINT PRIMARY KEY,
                            session_id BIGINT NOT NULL,
                            job_id TEXT NOT NULL,
                            parent_span_id BIGINT,
                            lane_id TEXT NOT NULL,
                            handoff_from_lane TEXT,
                            handoff_to_lane TEXT,
                            span_name TEXT NOT NULL,
                            stage TEXT NOT NULL,
                            status TEXT NOT NULL,
                            input_json TEXT,
                            output_json TEXT,
                            metadata_json TEXT,
                            started_at TEXT,
                            completed_at TEXT,
                            created_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS agent_worker_runs (
                            worker_id BIGINT PRIMARY KEY,
                            session_id BIGINT NOT NULL,
                            job_id TEXT NOT NULL,
                            span_id BIGINT,
                            lane_id TEXT NOT NULL,
                            worker_key TEXT NOT NULL,
                            status TEXT NOT NULL,
                            interrupt_requested BIGINT NOT NULL DEFAULT 0,
                            budget_json TEXT,
                            checkpoint_json TEXT,
                            input_json TEXT,
                            output_json TEXT,
                            metadata_json TEXT,
                            lease_owner TEXT,
                            lease_expires_at TEXT,
                            attempt_count BIGINT NOT NULL DEFAULT 0,
                            last_error TEXT,
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_worker_runs_job_lane_worker_key
                        ON agent_worker_runs (job_id, lane_id, worker_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_agent_worker_runs_job_updated
                        ON agent_worker_runs (job_id, updated_at, worker_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_agent_trace_spans_job_span
                        ON agent_trace_spans (job_id, session_id, span_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_job_leases_expires
                        ON workflow_job_leases (lease_expires_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_recovery_intents_status
                        ON workflow_recovery_intents (status, lease_expires_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_events (
                            event_id TEXT PRIMARY KEY,
                            workflow_run_id TEXT NOT NULL,
                            operation_id TEXT NOT NULL DEFAULT '',
                            command_id TEXT NOT NULL DEFAULT '',
                            activity_attempt_id TEXT NOT NULL DEFAULT '',
                            event_family TEXT NOT NULL,
                            event_type TEXT NOT NULL,
                            sequence_number BIGINT NOT NULL DEFAULT 0,
                            idempotency_key TEXT NOT NULL,
                            occurred_at TEXT NOT NULL DEFAULT '',
                            recorded_at TEXT NOT NULL DEFAULT '',
                            actor TEXT NOT NULL DEFAULT '',
                            source TEXT NOT NULL DEFAULT '',
                            payload_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            schema_version TEXT NOT NULL DEFAULT 'workflow_event_v1',
                            created_at TEXT,
                            UNIQUE(workflow_run_id, sequence_number),
                            UNIQUE(workflow_run_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_current_state (
                            workflow_run_id TEXT PRIMARY KEY,
                            operation_id TEXT NOT NULL DEFAULT '',
                            workflow_type TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'pending',
                            current_stage_key TEXT NOT NULL DEFAULT '',
                            completion_proofs_json TEXT NOT NULL DEFAULT '{}',
                            active_command_counts_json TEXT NOT NULL DEFAULT '{}',
                            terminal_command_counts_json TEXT NOT NULL DEFAULT '{}',
                            read_model_pointers_json TEXT NOT NULL DEFAULT '{}',
                            migration_status_json TEXT NOT NULL DEFAULT '{}',
                            last_processed_sequence_number BIGINT NOT NULL DEFAULT 0,
                            reducer_version TEXT NOT NULL DEFAULT '',
                            schema_version TEXT NOT NULL DEFAULT 'workflow_current_state_v1',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_commands (
                            command_id TEXT PRIMARY KEY,
                            workflow_run_id TEXT NOT NULL,
                            operation_id TEXT NOT NULL DEFAULT '',
                            command_type TEXT NOT NULL,
                            owner TEXT NOT NULL,
                            stage_id TEXT NOT NULL DEFAULT '',
                            causal_group_id TEXT NOT NULL DEFAULT '',
                            parent_command_id TEXT NOT NULL DEFAULT '',
                            source_event_id TEXT NOT NULL DEFAULT '',
                            source_event_type TEXT NOT NULL DEFAULT '',
                            input_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            output_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            produced_entity_counts_json TEXT NOT NULL DEFAULT '{}',
                            no_op_reason TEXT NOT NULL DEFAULT '',
                            readiness_effect TEXT NOT NULL DEFAULT '',
                            downstream_command_ids_json TEXT NOT NULL DEFAULT '[]',
                            causality_schema_version TEXT NOT NULL DEFAULT 'command_causality_v1',
                            status TEXT NOT NULL DEFAULT 'queued',
                            idempotency_key TEXT NOT NULL,
                            payload_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            not_before_at TEXT NOT NULL DEFAULT '',
                            attempt BIGINT NOT NULL DEFAULT 0,
                            max_attempts BIGINT NOT NULL DEFAULT 5,
                            retry_policy_json TEXT NOT NULL DEFAULT '{}',
                            lease_owner TEXT NOT NULL DEFAULT '',
                            lease_expires_at TEXT NOT NULL DEFAULT '',
                            heartbeat_at TEXT NOT NULL DEFAULT '',
                            last_error TEXT NOT NULL DEFAULT '',
                            result_json TEXT NOT NULL DEFAULT '{}',
                            schema_version TEXT NOT NULL DEFAULT 'workflow_command_v1',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workflow_run_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runtime_outbox (
                            outbox_id TEXT PRIMARY KEY,
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            operation_id TEXT NOT NULL DEFAULT '',
                            command_id TEXT NOT NULL DEFAULT '',
                            outbox_type TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'queued',
                            idempotency_key TEXT NOT NULL,
                            payload_json TEXT NOT NULL DEFAULT '{}',
                            not_before_at TEXT NOT NULL DEFAULT '',
                            attempt BIGINT NOT NULL DEFAULT 0,
                            max_attempts BIGINT NOT NULL DEFAULT 5,
                            lease_owner TEXT NOT NULL DEFAULT '',
                            lease_expires_at TEXT NOT NULL DEFAULT '',
                            dispatched_at TEXT NOT NULL DEFAULT '',
                            last_error TEXT NOT NULL DEFAULT '',
                            schema_version TEXT NOT NULL DEFAULT 'runtime_outbox_v1',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS agent_actions (
                            action_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            conversation_id TEXT NOT NULL DEFAULT '',
                            action_type TEXT NOT NULL,
                            owner_module TEXT NOT NULL,
                            operation_type TEXT NOT NULL,
                            target_ref_json TEXT NOT NULL DEFAULT '{}',
                            input_json TEXT NOT NULL DEFAULT '{}',
                            approval_status TEXT NOT NULL DEFAULT 'not_required',
                            approval_policy TEXT NOT NULL DEFAULT 'not_required',
                            budget_json TEXT NOT NULL DEFAULT '{}',
                            idempotency_key TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'planned',
                            result_ref_json TEXT NOT NULL DEFAULT '{}',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS operation_runs (
                            operation_run_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            action_id TEXT NOT NULL,
                            owner_module TEXT NOT NULL,
                            operation_type TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'queued',
                            progress_json TEXT NOT NULL DEFAULT '{}',
                            workflow_ref_json TEXT NOT NULL DEFAULT '{}',
                            cost_budget_json TEXT NOT NULL DEFAULT '{}',
                            idempotency_key TEXT NOT NULL,
                            result_ref_json TEXT NOT NULL DEFAULT '{}',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            started_at TEXT NOT NULL DEFAULT '',
                            completed_at TEXT NOT NULL DEFAULT '',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS acquisition_runs (
                            acquisition_run_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            operation_run_id TEXT NOT NULL DEFAULT '',
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            plan_id TEXT NOT NULL DEFAULT '',
                            plan_review_id BIGINT NOT NULL DEFAULT 0,
                            target_company TEXT NOT NULL DEFAULT '',
                            query TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'planned',
                            current_phase TEXT NOT NULL DEFAULT '',
                            request_json TEXT NOT NULL DEFAULT '{}',
                            plan_json TEXT NOT NULL DEFAULT '{}',
                            execution_bundle_json TEXT NOT NULL DEFAULT '{}',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            idempotency_key TEXT NOT NULL,
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_activity_runs (
                            activity_run_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            operation_run_id TEXT NOT NULL DEFAULT '',
                            acquisition_run_id TEXT NOT NULL DEFAULT '',
                            command_id TEXT NOT NULL DEFAULT '',
                            parent_activity_run_id TEXT NOT NULL DEFAULT '',
                            activity_type TEXT NOT NULL,
                            owner TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'planned',
                            phase TEXT NOT NULL DEFAULT '',
                            idempotency_key TEXT NOT NULL,
                            provider_ref_json TEXT NOT NULL DEFAULT '{}',
                            input_json TEXT NOT NULL DEFAULT '{}',
                            output_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            entity_counts_json TEXT NOT NULL DEFAULT '{}',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_activity_attempts (
                            attempt_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            activity_run_id TEXT NOT NULL DEFAULT '',
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            command_id TEXT NOT NULL DEFAULT '',
                            attempt_number BIGINT NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'planned',
                            provider TEXT NOT NULL DEFAULT '',
                            provider_request_ref TEXT NOT NULL DEFAULT '',
                            provider_run_ref TEXT NOT NULL DEFAULT '',
                            started_at TEXT NOT NULL DEFAULT '',
                            completed_at TEXT NOT NULL DEFAULT '',
                            next_retry_at TEXT NOT NULL DEFAULT '',
                            rate_limit_ref_json TEXT NOT NULL DEFAULT '{}',
                            error_json TEXT NOT NULL DEFAULT '{}',
                            input_json TEXT NOT NULL DEFAULT '{}',
                            output_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            idempotency_key TEXT NOT NULL,
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_entity_deltas (
                            delta_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            operation_run_id TEXT NOT NULL DEFAULT '',
                            command_id TEXT NOT NULL DEFAULT '',
                            activity_run_id TEXT NOT NULL DEFAULT '',
                            attempt_id TEXT NOT NULL DEFAULT '',
                            acquisition_run_id TEXT NOT NULL DEFAULT '',
                            entity_type TEXT NOT NULL DEFAULT '',
                            entity_key TEXT NOT NULL DEFAULT '',
                            delta_kind TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'recorded',
                            reason TEXT NOT NULL DEFAULT '',
                            source_ref_json TEXT NOT NULL DEFAULT '{}',
                            entity_payload_json TEXT NOT NULL DEFAULT '{}',
                            projection_effect_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            idempotency_key TEXT NOT NULL,
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS acquisition_discovery_lanes (
                            lane_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            acquisition_run_id TEXT NOT NULL DEFAULT '',
                            workflow_run_id TEXT NOT NULL DEFAULT '',
                            operation_run_id TEXT NOT NULL DEFAULT '',
                            source_command_id TEXT NOT NULL DEFAULT '',
                            activity_run_id TEXT NOT NULL DEFAULT '',
                            target_company TEXT NOT NULL DEFAULT '',
                            query TEXT NOT NULL DEFAULT '',
                            provider TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'planned',
                            phase TEXT NOT NULL DEFAULT 'planned',
                            lane_plan_json TEXT NOT NULL DEFAULT '{}',
                            provider_ref_json TEXT NOT NULL DEFAULT '{}',
                            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
                            entity_counts_json TEXT NOT NULL DEFAULT '{}',
                            downstream_command_ids_json TEXT NOT NULL DEFAULT '[]',
                            idempotency_key TEXT NOT NULL,
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT,
                            UNIQUE(workspace_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS operation_events (
                            event_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            event_stream_id TEXT NOT NULL,
                            operation_run_id TEXT NOT NULL DEFAULT '',
                            action_id TEXT NOT NULL DEFAULT '',
                            event_family TEXT NOT NULL,
                            event_type TEXT NOT NULL,
                            sequence_number BIGINT NOT NULL DEFAULT 0,
                            idempotency_key TEXT NOT NULL,
                            occurred_at TEXT NOT NULL DEFAULT '',
                            recorded_at TEXT NOT NULL DEFAULT '',
                            actor TEXT NOT NULL DEFAULT '',
                            source TEXT NOT NULL DEFAULT '',
                            payload_json TEXT NOT NULL DEFAULT '{}',
                            schema_version TEXT NOT NULL DEFAULT 'operation_event_v1',
                            created_at TEXT,
                            UNIQUE(event_stream_id, sequence_number),
                            UNIQUE(event_stream_id, idempotency_key)
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS crm_tasks (
                            task_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            crm_record_id TEXT NOT NULL DEFAULT '',
                            engagement_id TEXT NOT NULL DEFAULT '',
                            person_identity_key TEXT NOT NULL DEFAULT '',
                            title TEXT NOT NULL DEFAULT '',
                            description TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT 'open',
                            priority TEXT NOT NULL DEFAULT 'normal',
                            due_at TEXT NOT NULL DEFAULT '',
                            completed_at TEXT NOT NULL DEFAULT '',
                            created_by_actor TEXT NOT NULL DEFAULT '',
                            created_by_actor_id TEXT NOT NULL DEFAULT '',
                            source_event_id TEXT NOT NULL DEFAULT '',
                            idempotency_key TEXT NOT NULL DEFAULT '',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS company_assets (
                            asset_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            company_key TEXT NOT NULL DEFAULT '',
                            target_company TEXT NOT NULL DEFAULT '',
                            asset_type TEXT NOT NULL DEFAULT '',
                            source_kind TEXT NOT NULL DEFAULT '',
                            source_run_id TEXT NOT NULL DEFAULT '',
                            source_command_id TEXT NOT NULL DEFAULT '',
                            activity_run_id TEXT NOT NULL DEFAULT '',
                            content_ref TEXT NOT NULL DEFAULT '',
                            content_hash TEXT NOT NULL DEFAULT '',
                            source_url TEXT NOT NULL DEFAULT '',
                            fetched_at TEXT NOT NULL DEFAULT '',
                            visibility_scope TEXT NOT NULL DEFAULT 'internal',
                            status TEXT NOT NULL DEFAULT 'available',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS company_evidence (
                            evidence_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            company_key TEXT NOT NULL DEFAULT '',
                            target_company TEXT NOT NULL DEFAULT '',
                            asset_id TEXT NOT NULL DEFAULT '',
                            evidence_type TEXT NOT NULL DEFAULT '',
                            value TEXT NOT NULL DEFAULT '',
                            normalized_value TEXT NOT NULL DEFAULT '',
                            source_url TEXT NOT NULL DEFAULT '',
                            source_domain TEXT NOT NULL DEFAULT '',
                            confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                            evidence_excerpt TEXT NOT NULL DEFAULT '',
                            artifact_refs_json TEXT NOT NULL DEFAULT '{}',
                            status TEXT NOT NULL DEFAULT 'observed',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS company_assertions (
                            assertion_id TEXT PRIMARY KEY,
                            workspace_id TEXT NOT NULL DEFAULT 'default',
                            company_key TEXT NOT NULL DEFAULT '',
                            target_company TEXT NOT NULL DEFAULT '',
                            assertion_type TEXT NOT NULL DEFAULT '',
                            value TEXT NOT NULL DEFAULT '',
                            normalized_value TEXT NOT NULL DEFAULT '',
                            authority TEXT NOT NULL DEFAULT 'provider_observed',
                            verification_status TEXT NOT NULL DEFAULT 'needs_review',
                            source_evidence_id TEXT NOT NULL DEFAULT '',
                            source_run_id TEXT NOT NULL DEFAULT '',
                            source_command_id TEXT NOT NULL DEFAULT '',
                            confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                            valid_from TEXT NOT NULL DEFAULT '',
                            valid_to TEXT NOT NULL DEFAULT '',
                            metadata_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_tasks_record_status_due
                        ON crm_tasks (workspace_id, crm_record_id, status, due_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_crm_tasks_status_due
                        ON crm_tasks (workspace_id, status, due_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_tasks_idempotency
                        ON crm_tasks (workspace_id, idempotency_key)
                        WHERE idempotency_key != ''
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_assets_company
                        ON company_assets (workspace_id, company_key, asset_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_assets_source
                        ON company_assets (source_run_id, source_command_id, activity_run_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_evidence_company
                        ON company_evidence (workspace_id, company_key, evidence_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_evidence_asset
                        ON company_evidence (asset_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_assertions_company
                        ON company_assertions (workspace_id, company_key, assertion_type, verification_status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_company_assertions_evidence
                        ON company_assertions (source_evidence_id, updated_at)
                        """
                    )
                    for column_name, column_type in (
                        ("stage_id", "TEXT NOT NULL DEFAULT ''"),
                        ("causal_group_id", "TEXT NOT NULL DEFAULT ''"),
                        ("parent_command_id", "TEXT NOT NULL DEFAULT ''"),
                        ("source_event_id", "TEXT NOT NULL DEFAULT ''"),
                        ("source_event_type", "TEXT NOT NULL DEFAULT ''"),
                        ("input_artifact_refs_json", "TEXT NOT NULL DEFAULT '[]'"),
                        ("output_artifact_refs_json", "TEXT NOT NULL DEFAULT '[]'"),
                        ("produced_entity_counts_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("no_op_reason", "TEXT NOT NULL DEFAULT ''"),
                        ("readiness_effect", "TEXT NOT NULL DEFAULT ''"),
                        ("downstream_command_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
                        ("causality_schema_version", "TEXT NOT NULL DEFAULT 'command_causality_v1'"),
                    ):
                        cursor.execute(
                            f"""
                            ALTER TABLE workflow_commands
                            ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} {column_type}
                            """
                        )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_events_run_sequence_unique
                        ON workflow_events (workflow_run_id, sequence_number)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_events_run_idempotency_unique
                        ON workflow_events (workflow_run_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_events_run_sequence
                        ON workflow_events (workflow_run_id, sequence_number)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_events_command
                        ON workflow_events (command_id, recorded_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_current_state_status
                        ON workflow_current_state (status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_commands_run_idempotency_unique
                        ON workflow_commands (workflow_run_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_commands_ready
                        ON workflow_commands (owner, command_type, status, not_before_at, lease_expires_at, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_commands_run
                        ON workflow_commands (workflow_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_commands_causal_group
                        ON workflow_commands (causal_group_id, source_event_id, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_commands_source_event
                        ON workflow_commands (source_event_id, command_type, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_runtime_outbox_ready
                        ON runtime_outbox (outbox_type, status, not_before_at, lease_expires_at, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_runtime_outbox_run
                        ON runtime_outbox (workflow_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_agent_actions_workspace_status
                        ON agent_actions (workspace_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_actions_workspace_idempotency_unique
                        ON agent_actions (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_agent_actions_owner
                        ON agent_actions (owner_module, action_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_operation_runs_action
                        ON operation_runs (action_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_operation_runs_workspace_idempotency_unique
                        ON operation_runs (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_operation_runs_owner
                        ON operation_runs (owner_module, operation_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_runs_operation
                        ON acquisition_runs (operation_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_runs_workflow
                        ON acquisition_runs (workflow_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_runs_company
                        ON acquisition_runs (workspace_id, target_company, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_runs_workspace_idempotency_unique
                        ON acquisition_runs (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_runs_workflow
                        ON workflow_activity_runs (workflow_run_id, activity_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_runs_acquisition
                        ON workflow_activity_runs (acquisition_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_runs_command
                        ON workflow_activity_runs (command_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_activity_runs_workspace_idempotency_unique
                        ON workflow_activity_runs (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_attempts_activity
                        ON workflow_activity_attempts (activity_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_attempts_workflow
                        ON workflow_activity_attempts (workflow_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_activity_attempts_command
                        ON workflow_activity_attempts (command_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_activity_attempts_workspace_idempotency_unique
                        ON workflow_activity_attempts (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_entity_deltas_activity
                        ON workflow_entity_deltas (activity_run_id, entity_type, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_entity_deltas_workflow
                        ON workflow_entity_deltas (workflow_run_id, entity_type, entity_key, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_entity_deltas_command
                        ON workflow_entity_deltas (command_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_entity_deltas_workspace_idempotency_unique
                        ON workflow_entity_deltas (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_discovery_lanes_acquisition
                        ON acquisition_discovery_lanes (acquisition_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_discovery_lanes_workflow
                        ON acquisition_discovery_lanes (workflow_run_id, status, updated_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_acquisition_discovery_lanes_source_command
                        ON acquisition_discovery_lanes (source_command_id)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_acquisition_discovery_lanes_workspace_idempotency_unique
                        ON acquisition_discovery_lanes (workspace_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_operation_events_stream_sequence
                        ON operation_events (event_stream_id, sequence_number)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_operation_events_stream_idempotency_unique
                        ON operation_events (event_stream_id, idempotency_key)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_operation_events_action
                        ON operation_events (action_id, recorded_at)
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_runtime_provider_limiter_key_expires
                        ON runtime_provider_limiter_leases (limiter_key, lease_expires_at)
                        """
                    )
                    cursor.execute("CREATE SEQUENCE IF NOT EXISTS agent_trace_spans_span_id_seq START WITH 1")
                    cursor.execute("CREATE SEQUENCE IF NOT EXISTS agent_worker_runs_worker_id_seq START WITH 1")
                    cursor.execute(
                        """
                        SELECT setval(
                            'agent_trace_spans_span_id_seq',
                            COALESCE((SELECT MAX(span_id) FROM agent_trace_spans), 0) + 1,
                            false
                        )
                        """
                    )
                    cursor.execute(
                        """
                        SELECT setval(
                            'agent_worker_runs_worker_id_seq',
                            COALESCE((SELECT MAX(worker_id) FROM agent_worker_runs), 0) + 1,
                            false
                        )
                        """
                    )
                connection.commit()
            self._runtime_schema_ready = True

    def _execute_returning_one(self, sql: str, params: tuple[Any, ...] | list[Any]) -> dict[str, Any] | None:
        self.ensure_bootstrapped()
        normalized_params = tuple(_normalize_postgres_payload(item) for item in params)
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql, normalized_params)
                        row = cursor.fetchone()
                        result = _fetch_one_dict_row(cursor, row)
                    connection.commit()
                return result
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def _execute_non_query(self, sql: str, params: tuple[Any, ...] | list[Any]) -> int:
        self.ensure_bootstrapped()
        normalized_params = tuple(_normalize_postgres_payload(item) for item in params)
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql, normalized_params)
                        affected = int(cursor.rowcount or 0)
                    connection.commit()
                return affected
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

    def execute_returning_one(self, sql: str, params: tuple[Any, ...] | list[Any]) -> dict[str, Any] | None:
        return self._execute_returning_one(sql, params)

    def execute_non_query(self, sql: str, params: tuple[Any, ...] | list[Any]) -> int:
        return self._execute_non_query(sql, params)

    def _advisory_lock_key(self, lock_key: str) -> str:
        # Advisory locks are database-global, not schema-scoped; prefixing the
        # adapter's schema keeps schema-isolated runs (per-test schemas,
        # parallel local envs) from contending on the same logical resource.
        # This changes lock identity: rolling out requires a full process stop
        # (no old/new overlap) — the current systemd full-restart deployment
        # satisfies that; do not hot-deploy alongside old processes.
        return f"{self.schema or 'public'}:{lock_key}"

    def _connect(self) -> Any:
        psycopg_module = self._psycopg
        if psycopg_module is not None and getattr(psycopg_module, "Connection", None) is None:
            # Test seams inject lightweight psycopg stubs exposing only
            # ``connect()``; keep the legacy one-connection-per-call path for
            # those instead of pooling.
            return self._direct_connect(psycopg_module)
        pool = self._ensure_pool()
        return _PooledConnectionHandle(pool, pool.getconn())

    def _connect_with_timeout(self, timeout_seconds: float) -> Any:
        """Checkout one connection within the caller's existing deadline budget."""

        psycopg_module = self._psycopg
        if psycopg_module is not None and getattr(psycopg_module, "Connection", None) is None:
            return self._direct_connect(psycopg_module)
        pool = self._ensure_pool()
        return _PooledConnectionHandle(pool, pool.getconn(timeout=max(0.001, float(timeout_seconds))))

    def _direct_connect(self, psycopg_module: Any) -> Any:
        """Legacy non-pooled connect path (used with injected psycopg stubs)."""

        if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
            try:
                ensure_local_postgres_started(self.runtime_dir)
            except Exception:
                pass
        effective_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
        try:
            return configure_control_plane_postgres_session(
                psycopg_module.connect(effective_dsn, client_encoding="utf8"),
                schema=self.schema,
            )
        except TypeError:
            return configure_control_plane_postgres_session(
                _configure_postgres_connection_utf8(psycopg_module.connect(effective_dsn)),
                schema=self.schema,
            )

    def _configure_pooled_connection(self, connection: Any) -> None:
        """Replicate the legacy per-connection session config for pooled connections.

        The pre-pool ``_connect`` applied, per fresh connection:
        ``client_encoding=utf8`` (connect kwarg), ``SET client_encoding TO 'UTF8'``,
        and (when the adapter schema differs from ``public``) ``CREATE SCHEMA IF NOT
        EXISTS <schema>`` followed by ``SET search_path TO <schema>, public``. Those
        statements used to commit together with the first unit of work; pooled
        connections must commit here so the session state survives later rollbacks
        and the pool receives the connection in IDLE state.
        """

        try:
            configure_control_plane_postgres_session(connection, schema=self.schema)
        except Exception as exc:
            # Two pool workers can race ``CREATE SCHEMA IF NOT EXISTS`` for a
            # fresh schema (PostgreSQL still raises unique_violation /
            # duplicate_schema on concurrent creation). Retry once: the schema
            # exists by then and only ``SET search_path`` remains to apply.
            sqlstate = str(getattr(exc, "sqlstate", "") or "").strip().upper()
            if sqlstate not in {"23505", "42P06"}:
                raise
            try:
                connection.rollback()
            except Exception:
                pass
            configure_control_plane_postgres_session(connection, schema=self.schema)
        connection.commit()

    def _on_pool_reconnect_failed(self, _pool: Any) -> None:
        if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
            try:
                ensure_local_postgres_started(self.runtime_dir)
            except Exception:
                pass

    def _ensure_pool(self) -> Any:
        current_pid = os.getpid()
        pool = self._pool
        if pool is not None and self._pool_pid == current_pid and not getattr(pool, "closed", False):
            return pool
        with self._pool_lock:
            pool = self._pool
            if pool is not None and self._pool_pid == current_pid and not getattr(pool, "closed", False):
                return pool
            if pool is not None:
                if self._pool_pid == current_pid:
                    try:
                        pool.close()
                    except Exception:
                        pass
                # else: pool object inherited across fork; abandon it without
                # touching sockets shared with the parent process.
                self._pool = None
                self._pool_pid = None
            if self._psycopg is None:
                self._psycopg = _import_psycopg()
            psycopg_pool = _import_psycopg_pool()
            if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
                try:
                    ensure_local_postgres_started(self.runtime_dir)
                except Exception:
                    pass
            effective_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
            min_size, max_size = _resolve_control_plane_pg_pool_size_limits()
            new_pool = psycopg_pool.ConnectionPool(
                effective_dsn,
                connection_class=_resolve_pooled_connection_class(self._psycopg),
                kwargs={"client_encoding": "utf8"},
                min_size=min_size,
                max_size=max_size,
                configure=self._configure_pooled_connection,
                check=psycopg_pool.ConnectionPool.check_connection,
                reconnect_failed=self._on_pool_reconnect_failed,
                name=f"control_plane_{self.schema or 'public'}",
                open=True,
            )
            self._pool = new_pool
            self._pool_pid = current_pid
            return new_pool

    def close(self) -> None:
        """Dispose the adapter's connection pool (tests/fixtures cleanup hook)."""

        with self._pool_lock:
            pool = self._pool
            self._pool = None
            self._pool_pid = None
        if pool is not None:
            try:
                pool.close()
            except Exception:
                pass

    def _job_progress_event_summary_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        latest_event = _json_load_dict(row.get("latest_event_json"))
        stage_stats = _json_load_dict(row.get("stage_stats_json"))
        latest_metrics = _json_load_dict(row.get("latest_metrics_json"))
        stage_sequence = _json_load_list(row.get("stage_sequence_json"))
        return {
            "job_id": str(row.get("job_id") or ""),
            "event_count": int(row.get("event_count") or 0),
            "latest_event": latest_event,
            "stage_sequence": [str(item).strip() for item in stage_sequence if str(item).strip()],
            "stage_stats": stage_stats,
            "latest_metrics": latest_metrics,
            "created_at": str(row.get("created_at") or ""),
            "updated_at": str(row.get("updated_at") or ""),
        }


def _fetch_all_dict_rows(cursor: Any) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [_normalize_postgres_identifier(item[0]) for item in list(getattr(cursor, "description", []) or [])]
    results: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            results.append(_normalize_postgres_row_payload(dict(row)))
            continue
        results.append(_normalize_postgres_row_payload({column: value for column, value in zip(columns, row)}))
    return results


def _fetch_one_dict_row(cursor: Any, row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    columns = [_normalize_postgres_identifier(item[0]) for item in list(getattr(cursor, "description", []) or [])]
    if isinstance(row, dict):
        return _normalize_postgres_row_payload(dict(row))
    return _normalize_postgres_row_payload({column: value for column, value in zip(columns, row)})


def _normalize_postgres_textual_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).decode("utf-8", errors="replace")
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return ""
    if (text.startswith("b'") and text.endswith("'")) or (text.startswith('b"') and text.endswith('"')):
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return text
        return _normalize_postgres_textual_value(parsed)
    return text


def _normalize_postgres_payload(value: Any) -> Any:
    normalized_scalar = _normalize_postgres_textual_value(value)
    if normalized_scalar is not value:
        return normalized_scalar
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(_normalize_postgres_textual_value(key) or ""): _normalize_postgres_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_postgres_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_postgres_payload(item) for item in value)
    if isinstance(value, set):
        return [_normalize_postgres_payload(item) for item in value]
    return value


def _normalize_postgres_row_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        normalized_key = str(_normalize_postgres_textual_value(key) or "").strip()
        if not normalized_key:
            continue
        normalized[normalized_key] = _normalize_postgres_payload(value)
    return normalized


def _assert_request_schema_pin_identity(
    existing: dict[str, Any],
    requested: dict[str, Any],
    *,
    record_kind: str,
) -> None:
    mismatches = [
        column
        for column in ("request_schema_version", "request_schema_digest")
        if str(existing.get(column) or "").strip() != str(requested.get(column) or "").strip()
    ]
    if mismatches:
        raise ValueError(
            f"{str(record_kind or 'operation_record').strip()} immutable request schema pin collision: "
            + ", ".join(mismatches)
        )


def _normalize_postgres_identifier(value: Any) -> str:
    return str(_normalize_postgres_textual_value(value) or "").strip()


def _is_retryable_postgres_exception(error: Exception) -> bool:
    if _PSYCOPG_POOL_TIMEOUT is not None and isinstance(error, _PSYCOPG_POOL_TIMEOUT):
        return True
    sqlstate = str(getattr(error, "sqlstate", "") or "").strip().upper()
    if sqlstate in _RETRYABLE_POSTGRES_SQLSTATES:
        return True
    return type(error).__name__ in {"DeadlockDetected", "SerializationFailure", "PoolTimeout"}


def _control_plane_postgres_retry_delay_seconds(attempt: int) -> float:
    normalized_attempt = max(1, int(attempt or 1))
    return min(0.5, 0.05 * (2 ** (normalized_attempt - 1)))


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier or "").replace('"', '""')
    return f'"{escaped}"'


def _projection_metadata_merge_sql(
    *,
    quoted_table_name: str,
    quoted_column: str,
    preserve_keys: tuple[str, ...],
) -> str:
    incoming = f"EXCLUDED.{quoted_column}::jsonb"
    for key in preserve_keys:
        incoming = f"({incoming} - '{key}')"
    preserved = " || ".join(
        (
            f"CASE WHEN {quoted_table_name}.{quoted_column}::jsonb ? '{key}' "
            f"THEN jsonb_build_object('{key}', "
            f"{quoted_table_name}.{quoted_column}::jsonb ->> '{key}') "
            "ELSE '{}'::jsonb END"
        )
        for key in preserve_keys
    )
    return f"(({incoming}) || {preserved})::text"


def _postgres_table_exists(cursor: Any, table_name: str, *, schema: str = "") -> bool:
    normalized_table = _normalize_postgres_identifier(table_name)
    if not normalized_table:
        return False
    normalized_schema = _normalize_postgres_identifier(schema)
    regclass_name = normalized_table
    if normalized_schema:
        regclass_name = f"{_quote_identifier(normalized_schema)}.{_quote_identifier(normalized_table)}"
    cursor.execute("SELECT to_regclass(%s)", (regclass_name,))
    row = cursor.fetchone()
    if row is None:
        return False
    if isinstance(row, dict):
        return bool(next(iter(row.values()), None))
    if isinstance(row, (list, tuple)):
        return bool(row[0] if row else None)
    return bool(row)


def _quote_string_literal(value: str) -> str:
    escaped = str(value or "").replace("'", "''")
    return f"'{escaped}'"


def _bulk_upsert_prefers_direct_values(*, row_count: int, column_count: int) -> bool:
    normalized_rows = max(0, int(row_count or 0))
    normalized_columns = max(0, int(column_count or 0))
    if normalized_rows <= 0 or normalized_columns <= 0:
        return False
    return (
        normalized_rows <= _BULK_UPSERT_DIRECT_ROW_LIMIT
        and (normalized_rows * normalized_columns) <= _BULK_UPSERT_DIRECT_PARAM_LIMIT
    )


def _chunk_postgres_bulk_rows(rows: list[dict[str, Any]], *, column_count: int) -> list[list[dict[str, Any]]]:
    normalized_rows = list(rows or [])
    normalized_columns = max(1, int(column_count or 1))
    max_rows_by_param_budget = max(1, _BULK_UPSERT_DIRECT_PARAM_LIMIT // normalized_columns)
    chunk_size = max(1, min(_BULK_UPSERT_DIRECT_ROW_LIMIT, max_rows_by_param_budget))
    return [normalized_rows[index : index + chunk_size] for index in range(0, len(normalized_rows), chunk_size)]


def _normalized_company_scope(target_company: Any, company_key: Any = "") -> tuple[str, str]:
    normalized_target_company = _normalize_postgres_identifier(target_company)
    normalized_company_key = _normalize_postgres_identifier(company_key) or resolve_company_alias_key(
        normalized_target_company
    )
    return normalized_target_company, normalized_company_key


def _company_scope_predicate(
    normalized_target_company: str,
    normalized_company_key: str,
    *,
    target_column: str = "target_company",
    company_key_column: str = "company_key",
    placeholder: str = "%s",
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if normalized_target_company:
        clauses.append(f"{target_column} = {placeholder}")
        params.append(normalized_target_company)
    if normalized_company_key:
        clauses.append(f"{company_key_column} = {placeholder}")
        params.append(normalized_company_key)
    if not clauses:
        return "1 = 0", []
    if len(clauses) == 1:
        return clauses[0], params
    return f"({' OR '.join(clauses)})", params


def _json_dump(payload: Any) -> str:
    return json.dumps(_json_safe_postgres_payload(payload if payload is not None else {}), ensure_ascii=False)


def _json_safe_postgres_payload(value: Any) -> Any:
    normalized_scalar = _normalize_postgres_textual_value(value)
    if normalized_scalar is not value:
        return _json_safe_postgres_payload(normalized_scalar)
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        return _json_safe_postgres_payload(to_record())
    if isinstance(value, dict):
        return {str(key): _json_safe_postgres_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_postgres_payload(item) for item in value]
    return value


def _workflow_command_causality_columns_from_payload(payload: Any) -> dict[str, Any]:
    payload_dict = dict(payload or {}) if isinstance(payload, dict) else {}
    causality = dict(payload_dict.get("causality") or {})
    return {
        "stage_id": str(causality.get("stage_id") or payload_dict.get("stage_id") or "").strip(),
        "causal_group_id": str(causality.get("causal_group_id") or payload_dict.get("causal_group_id") or "").strip(),
        "parent_command_id": str(causality.get("parent_command_id") or "").strip(),
        "source_event_id": str(causality.get("source_event_id") or "").strip(),
        "source_event_type": str(causality.get("source_event_type") or "").strip(),
        "input_artifact_refs_json": _json_dump(list(causality.get("input_artifact_refs") or [])),
        "output_artifact_refs_json": _json_dump(list(causality.get("output_artifact_refs") or [])),
        "produced_entity_counts_json": _json_dump(dict(causality.get("produced_entity_counts") or {})),
        "no_op_reason": str(causality.get("no_op_reason") or "").strip(),
        "readiness_effect": str(causality.get("readiness_effect") or "").strip(),
        "downstream_command_ids_json": _json_dump(list(causality.get("downstream_command_ids") or [])),
        "causality_schema_version": str(causality.get("schema_version") or "command_causality_v1").strip()
        or "command_causality_v1",
    }


def _json_load_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return dict(payload)
    if payload in {None, ""}:
        return {}
    try:
        loaded = json.loads(str(payload))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _invalidate_projection_search_index_products(
    projection_row: dict[str, Any],
    *,
    build_status: str,
) -> dict[str, Any]:
    """Make index-derived public products unavailable in the caller's transaction."""

    payload = dict(projection_row or {})
    metadata = _json_load_dict(payload.get("metadata_json"))
    counts = _json_load_dict(payload.get("counts_json"))
    readiness = _json_load_dict(payload.get("readiness_json"))
    metadata[PROJECTION_SEARCH_INDEX_BUILD_STATUS_KEY] = str(build_status or "stale").strip() or "stale"
    metadata["public_facet_counts_build_status"] = "pending"
    counts.pop("public_facet_counts", None)
    counts["facet_count_scope"] = "unavailable"
    counts["facet_build_status"] = "pending"
    counts["index_count_scope"] = "unavailable"
    readiness["index_count_scope"] = "unavailable"
    readiness.pop("profile_indexed_at", None)
    readiness.pop("evidence_indexed_at", None)
    for key in PROJECTION_SEARCH_INDEX_BINDING_KEYS:
        readiness.pop(key, None)
    payload.update(
        {
            "metadata_json": _json_dump(metadata),
            "counts_json": _json_dump(counts),
            "readiness_json": _json_dump(readiness),
            "raw_profile_index_watermark": "",
            "evidence_index_watermark": "",
        }
    )
    return payload


def _json_load_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return list(payload)
    if payload in {None, ""}:
        return []
    try:
        loaded = json.loads(str(payload))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _utc_now_sql_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _expiry_timestamp(seconds: int) -> str:
    ttl_seconds = max(1, int(seconds or 0))
    return (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=ttl_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _timestamp_is_expired(value: Any) -> bool:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return True
    return parsed <= datetime.now(timezone.utc)


def _timestamp_age_seconds(value: Any) -> float:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return 10**9
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _parse_timestamp(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for candidate in (normalized, normalized.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S",):
        try:
            parsed = datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None
