from __future__ import annotations

import ast
import json
import os
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
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
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
        sql = f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
        if update_columns:
            update_assignments: list[str] = []
            for column in update_columns:
                quoted_column = _quote_identifier(column)
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
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(cursor, transaction_lock_key)
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
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_transaction_lock(
                            cursor,
                            f"serving_projection_scope:{normalized_scope_kind}:{normalized_scope_key}",
                        )
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

                        self._acquire_transaction_lock(
                            cursor,
                            f"serving_projection_publication:{selected_projection_id}",
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
        query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY company_key
                           ORDER BY updated_at DESC, created_at DESC, run_id DESC
                       ) AS company_public_web_run_rank
                FROM company_public_web_asset_runs
                WHERE company_key IN ({company_placeholders})
                {status_clause}
            ) ranked_company_public_web_runs
            WHERE company_public_web_run_rank = 1
            ORDER BY updated_at DESC, created_at DESC, run_id DESC
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
        classification: str = "",
        params: dict[str, Any] | None = None,
        requested_by: str = "",
    ) -> dict[str, Any] | None:
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
        lease_owner: str,
        lease_seconds: int = 300,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
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
        self, job_id: str, lease_owner: str = "", claimed_at: str = ""
    ) -> dict[str, Any] | None:
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

    def append_workflow_event(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
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
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))",
                            (self._advisory_lock_key(f"workflow_events:{workflow_run_id}"),),
                        )
                        sequence_number = int(payload.get("sequence_number") or 0)
                        if sequence_number <= 0:
                            cursor.execute(
                                "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM workflow_events WHERE workflow_run_id = %s",
                                (workflow_run_id,),
                            )
                            row_value = cursor.fetchone()
                            sequence_number = int(
                                (row_value[0] if isinstance(row_value, (list, tuple)) and row_value else row_value) or 1
                            )
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
                        columns = list(event_payload.keys())
                        cursor.execute(
                            (
                                f"INSERT INTO workflow_events ({', '.join(_quote_identifier(column) for column in columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))}) "
                                "ON CONFLICT DO NOTHING RETURNING *"
                            ),
                            tuple(event_payload[column] for column in columns),
                        )
                        inserted = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if inserted is not None:
                            connection.commit()
                            return inserted
                        cursor.execute(
                            """
                            SELECT * FROM workflow_events
                            WHERE workflow_run_id = %s AND idempotency_key = %s
                            LIMIT 1
                            """,
                            (workflow_run_id, idempotency_key),
                        )
                        existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                        if existing is None:
                            cursor.execute(
                                """
                                SELECT * FROM workflow_events
                                WHERE workflow_run_id = %s AND sequence_number = %s
                                LIMIT 1
                                """,
                                (workflow_run_id, sequence_number),
                            )
                            existing = _fetch_one_dict_row(cursor, cursor.fetchone())
                    connection.commit()
                    return existing
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

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
            connection.commit()
        return existing

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
        if _normalize_postgres_identifier(table_name) != "operation_runs":
            raise ValueError("cancel_operation_run_with_event requires table_name=operation_runs")
        if not self._require_operation_runtime_table("operation_runs"):
            return None
        if not self._require_operation_runtime_table("agent_actions"):
            return None
        if not self._require_operation_runtime_table("operation_events"):
            return None
        normalized_operation_id = str(operation_run_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        requested_status = str(status or "cancelled").strip() or "cancelled"
        if requested_status != "cancelled":
            raise ValueError("cancel operation UoW only supports status=cancelled")
        event_payload = _normalize_postgres_row_payload(dict(event_row or {}))
        event_stream_id = str(event_payload.get("event_stream_id") or "").strip()
        event_idempotency_key = str(event_payload.get("idempotency_key") or "").strip()
        if not normalized_operation_id or not normalized_expected_status or not event_idempotency_key:
            return None
        if (
            event_stream_id != normalized_operation_id
            or str(event_payload.get("operation_run_id") or "").strip() != normalized_operation_id
            or str(event_payload.get("event_family") or "").strip() != "operation_event"
            or str(event_payload.get("event_type") or "").strip() != "OperationCancelled"
        ):
            raise ValueError("cancel operation event identity does not match the locked operation")
        now = _utc_now_sql_timestamp()
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        self._acquire_operation_event_stream_lock(cursor, event_stream_id)
                        cursor.execute(
                            "SELECT * FROM operation_runs WHERE operation_run_id = %s FOR UPDATE",
                            (normalized_operation_id,),
                        )
                        current = _fetch_one_dict_row(cursor, cursor.fetchone())
                        operation = current
                        linked_action = None
                        event = None
                        outcome = "not_found"
                        if current is not None:
                            current_action_id = str(current.get("action_id") or "").strip()
                            if (str(event_payload.get("workspace_id") or "default").strip() or "default") != (
                                str(current.get("workspace_id") or "default").strip() or "default"
                            ):
                                raise ValueError("cancel operation event workspace does not match the locked operation")
                            if str(event_payload.get("action_id") or "").strip() != current_action_id:
                                raise ValueError("cancel operation event action does not match the locked operation")
                            current_status = str(current.get("status") or "").strip()
                            target_already_applied = current_status == requested_status
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
                                    raise RuntimeError("operation cancellation lost the locked operation row")
                                outcome = "applied"
                            if outcome in {"applied", "already_applied"}:
                                action_id = str((operation or {}).get("action_id") or "").strip()
                                if action_id:
                                    cursor.execute(
                                        "SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE",
                                        (action_id,),
                                    )
                                    linked_action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                    if linked_action is not None:
                                        action_status = str(linked_action.get("status") or "").strip()
                                        if (
                                            str(linked_action.get("workspace_id") or "default").strip() or "default"
                                        ) != (str(current.get("workspace_id") or "default").strip() or "default"):
                                            raise ValueError(
                                                "cancel operation linked action workspace does not match the operation"
                                            )
                                        current_action_metadata = _json_load_dict(linked_action.get("metadata_json"))
                                        action_metadata = {
                                            **current_action_metadata,
                                            **dict(linked_action_metadata_patch or {}),
                                        }
                                        action_terminal_conflict = action_status in {
                                            "completed",
                                            "failed",
                                            "rejected",
                                        }
                                        action_needs_update = not action_terminal_conflict and (
                                            action_status != "cancelled" or action_metadata != current_action_metadata
                                        )
                                        if action_needs_update:
                                            cursor.execute(
                                                """
                                                UPDATE agent_actions
                                                SET status = %s,
                                                    metadata_json = %s,
                                                    updated_at = %s
                                                WHERE action_id = %s AND status = %s
                                                RETURNING *
                                                """,
                                                (
                                                    "cancelled",
                                                    _json_dump(action_metadata),
                                                    now,
                                                    action_id,
                                                    action_status,
                                                ),
                                            )
                                            linked_action = _fetch_one_dict_row(cursor, cursor.fetchone())
                                            if linked_action is None:
                                                raise RuntimeError("operation cancel lost the linked action row lock")
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
                                        raise RuntimeError(
                                            "operation control transition produced no cancellation event"
                                        )
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
        # reclaim_claimed (opt-in, currently export-only) lets a new owner reclaim an
        # expired-lease claim left by a worker that crashed before
        # mark_workflow_command_running; the lease-expiry clause still protects
        # active claims. Scoped pending general ownership-fencing hardening
        # (docs/DURABLE_COMMAND_OWNERSHIP_FENCING.md).
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
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
            return None
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = 'succeeded',
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                last_error = '',
                result_json = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ('claimed', 'running')
            RETURNING *
            """,
            (now, _json_dump(result or {}), now, str(command_id or "").strip()),
        )

    def mark_workflow_command_failed(
        self,
        command_id: str,
        *,
        error_text: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
    ) -> dict[str, Any] | None:
        if not self.should_prefer_read("workflow_commands"):
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
        return self._execute_returning_one(
            """
            UPDATE workflow_commands
            SET status = %s,
                lease_owner = '',
                lease_expires_at = '',
                heartbeat_at = %s,
                not_before_at = %s,
                last_error = %s,
                updated_at = %s
            WHERE command_id = %s
              AND status IN ('claimed', 'running')
            RETURNING *
            """,
            (
                "retry_wait" if should_retry else "failed_terminal",
                now,
                _expiry_timestamp(max(0, int(retry_delay_seconds or 0))) if should_retry else "",
                str(error_text or "").strip(),
                now,
                str(command_id or "").strip(),
            ),
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

    def enqueue_runtime_outbox(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
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
        columns = list(row_payload.keys())
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    (
                        f"INSERT INTO runtime_outbox ({', '.join(_quote_identifier(column) for column in columns)}) "
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
                    "SELECT * FROM runtime_outbox WHERE idempotency_key = %s LIMIT 1",
                    (idempotency_key,),
                )
                existing = _fetch_one_dict_row(cursor, cursor.fetchone())
            connection.commit()
        return existing

    def mark_runtime_outbox_dispatched(self, outbox_id: str) -> dict[str, Any] | None:
        if not self.should_prefer_read("runtime_outbox"):
            return None
        now = _utc_now_sql_timestamp()
        return self._execute_returning_one(
            """
            UPDATE runtime_outbox
            SET status = 'dispatched',
                dispatched_at = %s,
                lease_owner = '',
                lease_expires_at = '',
                last_error = '',
                updated_at = %s
            WHERE outbox_id = %s
              AND status IN ('queued', 'claimed', 'running')
            RETURNING *
            """,
            (now, now, str(outbox_id or "").strip()),
        )

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
