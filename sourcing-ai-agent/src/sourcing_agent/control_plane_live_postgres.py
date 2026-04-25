from __future__ import annotations

import ast
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .company_registry import resolve_company_alias_key
from .control_plane_job_progress import update_job_progress_event_summary
from .control_plane_postgres import (
    ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
    ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES,
    _configure_postgres_connection_utf8,
    _import_psycopg,
    ensure_acquisition_shard_registry_split_schema,
    sync_runtime_control_plane_to_postgres,
    upsert_acquisition_shard_registry_rows,
)
from .local_postgres import (
    configure_control_plane_postgres_session,
    ensure_local_postgres_started,
    normalize_control_plane_postgres_connect_dsn,
    resolve_control_plane_postgres_dsn,
    resolve_control_plane_postgres_schema,
    resolve_default_control_plane_db_path,
)

CONTROL_PLANE_LIVE_TABLES = (
    "candidates",
    "evidence",
    "jobs",
    "job_results",
    "job_events",
    "job_progress_event_summaries",
    "job_result_views",
    "plan_review_sessions",
    "manual_review_items",
    "candidate_review_registry",
    "target_candidates",
    "asset_default_pointers",
    "asset_default_pointer_history",
    "frontend_history_links",
    "agent_runtime_sessions",
    "agent_trace_spans",
    "agent_worker_runs",
    "workflow_job_leases",
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
    "linkedin_profile_registry",
    "linkedin_profile_registry_aliases",
    "linkedin_profile_registry_leases",
    "linkedin_profile_registry_events",
    "linkedin_profile_registry_backfill_runs",
)

_PRIMARY_KEY_COLUMNS = {
    "candidates": ("candidate_id",),
    "evidence": ("evidence_id",),
    "jobs": ("job_id",),
    "job_results": ("job_id", "candidate_id"),
    "job_events": ("event_id",),
    "job_progress_event_summaries": ("job_id",),
    "job_result_views": ("view_id",),
    "plan_review_sessions": ("review_id",),
    "manual_review_items": ("review_item_id",),
    "candidate_review_registry": ("record_id",),
    "target_candidates": ("record_id",),
    "asset_default_pointers": ("pointer_key",),
    "asset_default_pointer_history": ("history_id",),
    "frontend_history_links": ("history_id",),
    "agent_runtime_sessions": ("session_id",),
    "agent_trace_spans": ("span_id",),
    "agent_worker_runs": ("worker_id",),
    "workflow_job_leases": ("job_id",),
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
    "asset_materialization_generations": ("target_company", "snapshot_id", "asset_view", "artifact_kind", "artifact_key"),
    "asset_membership_index": ("generation_key", "member_key"),
    "candidate_materialization_state": ("target_company", "snapshot_id", "asset_view", "candidate_id"),
    "snapshot_materialization_runs": ("run_id",),
    "linkedin_profile_registry": ("profile_url_key",),
    "linkedin_profile_registry_aliases": ("alias_url_key",),
    "linkedin_profile_registry_leases": ("profile_url_key",),
    "linkedin_profile_registry_events": ("event_id",),
    "linkedin_profile_registry_backfill_runs": ("run_key",),
}

_READ_PREFERRED_MODES = {"prefer_postgres", "postgres_only"}
_AUTHORITATIVE_MODES = {"postgres_only"}
_RUNTIME_COORDINATION_TABLES = {"agent_trace_spans", "agent_worker_runs", "workflow_job_leases"}
_RUNNING_RECOVERABLE_WAIT_STAGES = {"waiting_remote_search", "waiting_remote_harvest"}
_RETRYABLE_POSTGRES_SQLSTATES = {"40P01", "40001"}
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


class LiveControlPlanePostgresAdapter:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        sqlite_path: str | Path,
        dsn: str = "",
        mode: str = "disabled",
        tables: tuple[str, ...] = CONTROL_PLANE_LIVE_TABLES,
    ) -> None:
        self.runtime_dir = Path(runtime_dir).expanduser()
        self.sqlite_path = str(sqlite_path).strip() or str(
            resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.runtime_dir).expanduser()
        )
        self.dsn = str(dsn or resolve_control_plane_postgres_dsn(self.runtime_dir)).strip()
        self.mode = resolve_control_plane_postgres_live_mode(mode)
        self.tables = tuple(str(item).strip() for item in tables if str(item).strip())
        self._lock = threading.Lock()
        self._bootstrapped = False
        self._runtime_schema_ready = False
        self._control_plane_writer_schema_ready = False
        self._psycopg: Any | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.dsn) and self.mode != "disabled"

    def should_mirror(self, table_name: str) -> bool:
        return self.enabled and _normalize_postgres_identifier(table_name) in self.tables

    def should_prefer_read(self, table_name: str) -> bool:
        return self.should_mirror(table_name) and self.mode in _READ_PREFERRED_MODES

    def is_authoritative(self, table_name: str) -> bool:
        return self.should_prefer_read(table_name) and self.mode in _AUTHORITATIVE_MODES

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
        normalized_conflict_columns = [str(column or "").strip() for column in list(conflict_columns or []) if str(column or "").strip()]
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
            conflict_sql = (
                f" ON CONFLICT ({', '.join(_quote_identifier(column) for column in normalized_conflict_columns)}) DO NOTHING"
            )
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
            (None if provided_id_value in {None, "", 0} else provided_id_value, *(payload.get(column) for column in columns)),
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
        payload = _normalize_postgres_row_payload({
            str(column or "").strip(): value
            for column, value in dict(row or {}).items()
            if str(column or "").strip() and str(column or "").strip() != normalized_id_column
        })
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

    def ensure_bootstrapped(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._bootstrapped:
                return
            sync_runtime_control_plane_to_postgres(
                runtime_dir=self.runtime_dir,
                sqlite_path=self.sqlite_path,
                dsn=self.dsn,
                tables=list(self.tables),
                min_interval_seconds=0.0,
                force=False,
            )
            self._bootstrapped = True
        self._ensure_runtime_coordination_schema()

    def replace_table_from_sqlite(self, table_name: str) -> None:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_mirror(normalized_table):
            return
        self.ensure_bootstrapped()
        sync_runtime_control_plane_to_postgres(
            runtime_dir=self.runtime_dir,
            sqlite_path=self.sqlite_path,
            dsn=self.dsn,
            tables=[normalized_table],
            min_interval_seconds=0.0,
            force=True,
        )
        if normalized_table in _RUNTIME_COORDINATION_TABLES:
            self._runtime_schema_ready = False
            self._ensure_runtime_coordination_schema()
            if normalized_table in {"job_events", "agent_runtime_sessions", "job_progress_event_summaries"}:
                self._control_plane_writer_schema_ready = False
            self._ensure_control_plane_writer_schema()

    def _ensure_table_write_schema(self, table_name: str) -> None:
        normalized_table = str(table_name or "").strip()
        if normalized_table in _RUNTIME_COORDINATION_TABLES:
            self._ensure_runtime_coordination_schema()
            return
        self._ensure_control_plane_writer_schema()

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
        columns = [column for column in payload.keys() if str(column or "").strip()]
        quoted_table_name = _quote_identifier(normalized_table)
        quoted_columns = [_quote_identifier(column) for column in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [column for column in columns if column not in primary_keys]
        conflict_target = ", ".join(_quote_identifier(column) for column in primary_keys)
        sql = f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
        if update_columns:
            sql += f" ON CONFLICT ({conflict_target}) DO UPDATE SET " + ", ".join(
                f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}" for column in update_columns
            )
        else:
            sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"
        self._execute_non_query(sql, tuple(_normalize_postgres_payload(payload.get(column)) for column in columns))

    def bulk_upsert_rows(self, table_name: str, rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
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
                    summary = upsert_acquisition_shard_registry_rows(cursor, payload_rows, ensure_schema=False)
                connection.commit()
            return int(summary.get("row_count") or 0)
        primary_keys = list(_PRIMARY_KEY_COLUMNS.get(normalized_table) or [])
        if not primary_keys:
            return 0
        columns: list[str] = []
        seen_columns: set[str] = set()
        for payload in payload_rows:
            for column in payload.keys():
                if not str(column or "").strip() or column in seen_columns:
                    continue
                seen_columns.add(column)
                columns.append(column)
        if any(column not in columns for column in primary_keys):
            return 0
        update_columns = [column for column in columns if column not in primary_keys]
        conflict_target = ", ".join(_quote_identifier(column) for column in primary_keys)
        quoted_table_name = _quote_identifier(normalized_table)
        quoted_columns = [_quote_identifier(column) for column in columns]
        insert_placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
        values_sql = ", ".join(insert_placeholders for _ in payload_rows)
        write_params: list[Any] = []
        for payload in payload_rows:
            write_params.extend(_normalize_postgres_payload(payload.get(column)) for column in columns)
        use_direct_values = _bulk_upsert_prefers_direct_values(row_count=len(payload_rows), column_count=len(columns))
        if use_direct_values:
            merge_sql = (
                f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) "
                f"VALUES {values_sql}"
            )
            if update_columns:
                merge_sql += f" ON CONFLICT ({conflict_target}) DO UPDATE SET " + ", ".join(
                    f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}" for column in update_columns
                )
            else:
                merge_sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"
            temp_table_name = ""
            temp_insert_sql = ""
        else:
            temp_table_name = _quote_identifier(
                f"_cp_bulk_{normalized_table}_{threading.get_ident()}_{int(time.time() * 1000000)}"
            )
            temp_insert_sql = f"INSERT INTO {temp_table_name} ({', '.join(quoted_columns)}) VALUES {values_sql}"
            merge_sql = (
                f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) "
                f"SELECT {', '.join(quoted_columns)} FROM {temp_table_name}"
            )
            if update_columns:
                merge_sql += f" ON CONFLICT ({conflict_target}) DO UPDATE SET " + ", ".join(
                    f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}" for column in update_columns
                )
            else:
                merge_sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"

        self.ensure_bootstrapped()
        self._ensure_table_write_schema(normalized_table)
        attempt = 0
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        if use_direct_values:
                            cursor.execute(merge_sql, tuple(write_params))
                        else:
                            cursor.execute(
                                f"CREATE TEMP TABLE {temp_table_name} (LIKE {quoted_table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
                            )
                            cursor.execute(temp_insert_sql, tuple(write_params))
                            cursor.execute(merge_sql)
                        affected = int(cursor.rowcount or 0)
                    connection.commit()
                return affected
            except Exception as exc:
                attempt += 1
                if not _is_retryable_postgres_exception(exc) or attempt >= _CONTROL_PLANE_POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(_control_plane_postgres_retry_delay_seconds(attempt))

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
                            values_sql = ", ".join(
                                "(" + ", ".join(["%s"] * len(columns)) + ")" for _ in chunk
                            )
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

    def select_many(
        self,
        table_name: str,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_table = _normalize_postgres_identifier(table_name)
        if not self.should_prefer_read(normalized_table):
            return []
        self.ensure_bootstrapped()
        if normalized_table == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
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
        query = " ".join(query_parts)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                query_params = list(params)
                if normalized_limit > 0:
                    query_params.append(normalized_limit)
                cursor.execute(query, tuple(_normalize_postgres_payload(item) for item in query_params))
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
        return self._execute_returning_one(
            """
            UPDATE agent_trace_spans
            SET status = %s,
                output_json = %s,
                handoff_to_lane = CASE WHEN %s <> '' THEN %s ELSE COALESCE(handoff_to_lane, '') END,
                completed_at = %s
            WHERE span_id = %s
            RETURNING *
            """,
            (
                str(status),
                _json_dump(output_payload or {}),
                str(handoff_to_lane or ""),
                str(handoff_to_lane or ""),
                now,
                int(span_id),
            ),
        )

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
                artifact_path = EXCLUDED.artifact_path,
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
        return self._execute_returning_one(
            """
            UPDATE agent_runtime_sessions
            SET status = %s,
                updated_at = %s
            WHERE job_id = %s
            RETURNING *
            """,
            (
                str(status or ""),
                _utc_now_sql_timestamp(),
                normalized_job_id,
            ),
        )

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
            lease_expires_at = str(row.get("lease_expires_at") or "").strip()
            checkpoint = _json_load_dict(row.get("checkpoint_json"))
            checkpoint_stage = str(checkpoint.get("stage") or "").strip()
            updated_at = str(row.get("updated_at") or "").strip()
            is_running_wait = status == "running" and checkpoint_stage in _RUNNING_RECOVERABLE_WAIT_STAGES
            is_running_stale = status == "running" and _timestamp_age_seconds(updated_at) >= stale_cutoff_seconds
            is_recoverable_status = status in {"queued", "interrupted", "failed"}
            lease_available = not lease_expires_at or _timestamp_is_expired(lease_expires_at)
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
                    ensure_acquisition_shard_registry_split_schema(cursor)
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
            CREATE UNIQUE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{table_name}_{id_column}_unique')}
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
            _normalize_postgres_identifier(column) for column in partition_columns if _normalize_postgres_identifier(column)
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

    def _connect(self) -> Any:
        if self._psycopg is None:
            self._psycopg = _import_psycopg()
        if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
            try:
                ensure_local_postgres_started(self.runtime_dir)
            except Exception:
                pass
        effective_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
        schema = resolve_control_plane_postgres_schema(self.runtime_dir)
        try:
            return configure_control_plane_postgres_session(
                self._psycopg.connect(effective_dsn, client_encoding="utf8"),
                schema=schema,
            )
        except TypeError:
            return configure_control_plane_postgres_session(
                _configure_postgres_connection_utf8(self._psycopg.connect(effective_dsn)),
                schema=schema,
            )

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
        results.append(
            _normalize_postgres_row_payload({column: value for column, value in zip(columns, row)})
        )
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
    sqlstate = str(getattr(error, "sqlstate", "") or "").strip().upper()
    if sqlstate in _RETRYABLE_POSTGRES_SQLSTATES:
        return True
    return type(error).__name__ in {"DeadlockDetected", "SerializationFailure"}


def _control_plane_postgres_retry_delay_seconds(attempt: int) -> float:
    normalized_attempt = max(1, int(attempt or 1))
    return min(0.5, 0.05 * (2 ** (normalized_attempt - 1)))


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier or "").replace('"', '""')
    return f'"{escaped}"'


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
    return [
        normalized_rows[index : index + chunk_size]
        for index in range(0, len(normalized_rows), chunk_size)
    ]


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
    return json.dumps(payload if payload is not None else {}, ensure_ascii=False)


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
