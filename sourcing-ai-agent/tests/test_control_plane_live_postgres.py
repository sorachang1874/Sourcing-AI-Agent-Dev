import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_job_progress import update_job_progress_event_summary
from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.domain import Candidate, EvidenceRecord, JobRequest
from sourcing_agent.retrieval_runtime import load_bootstrap_candidate_source
from sourcing_agent.storage import SQLiteStore


class _RetryablePostgresError(Exception):
    def __init__(self, sqlstate: str = "40P01") -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


class _FakeRetryCursor:
    def __init__(self, outcomes: list[object]) -> None:
        self._outcomes = outcomes
        self._current: object | None = None
        self.rowcount = 0
        self.description: list[tuple[str]] = [("value",)]

    def __enter__(self) -> "_FakeRetryCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        self._current = outcome
        if isinstance(outcome, dict):
            self.rowcount = int(outcome.get("rowcount") or 0)
            self.description = list(outcome.get("description") or [("value",)])
            return
        self.rowcount = 1
        self.description = [("value",)]

    def fetchone(self) -> object | None:
        if isinstance(self._current, dict):
            return self._current.get("row")
        return self._current


class _FakeRetryConnection:
    def __init__(self, outcomes: list[object], commit_counter: dict[str, int]) -> None:
        self._outcomes = outcomes
        self._commit_counter = commit_counter

    def __enter__(self) -> "_FakeRetryConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeRetryCursor:
        return _FakeRetryCursor(self._outcomes)

    def commit(self) -> None:
        self._commit_counter["count"] = int(self._commit_counter.get("count") or 0) + 1


class _FakeConnectPsycopg:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, dsn: str, **kwargs: object) -> _FakeRetryConnection:
        self.calls.append((dsn, dict(kwargs)))
        return _FakeRetryConnection([], {"count": 0})


class _FakeLiveControlPlanePostgresAdapter:
    _DEFAULT_TABLES = (
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
        "job_results": ("job_id", "candidate_id"),
        "job_result_views": ("view_id",),
        "plan_review_sessions": ("review_id",),
        "manual_review_items": ("review_item_id",),
        "candidate_review_registry": ("record_id",),
        "target_candidates": ("record_id",),
        "frontend_history_links": ("history_id",),
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
        "linkedin_profile_registry": ("profile_url_key",),
        "linkedin_profile_registry_aliases": ("alias_url_key",),
        "linkedin_profile_registry_leases": ("profile_url_key",),
        "linkedin_profile_registry_events": ("event_id",),
        "linkedin_profile_registry_backfill_runs": ("run_key",),
    }

    _GENERATED_ID_COLUMNS = {
        "plan_review_sessions": "review_id",
        "manual_review_items": "review_item_id",
        "query_dispatches": "dispatch_id",
        "confidence_policy_runs": "policy_run_id",
        "confidence_policy_controls": "control_id",
        "criteria_feedback": "feedback_id",
        "criteria_patterns": "pattern_id",
        "criteria_versions": "version_id",
        "criteria_compiler_runs": "compiler_run_id",
        "criteria_result_diffs": "diff_id",
        "criteria_pattern_suggestions": "suggestion_id",
        "organization_asset_registry": "registry_id",
        "organization_execution_profiles": "profile_id",
        "cloud_asset_operation_ledger": "ledger_id",
        "linkedin_profile_registry_events": "event_id",
    }

    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        sqlite_path: str | Path,
        dsn: str = "",
        mode: str = "disabled",
        tables: tuple[str, ...] = (),
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.sqlite_path = str(sqlite_path or "").strip()
        self.dsn = str(dsn or "")
        self.mode = str(mode or "disabled")
        configured_tables = tables or self._DEFAULT_TABLES
        self.tables = tuple(str(item).strip() for item in configured_tables if str(item).strip())
        self.upserts: list[tuple[str, dict[str, object]]] = []
        self.bulk_upserts: list[tuple[str, list[dict[str, object]]]] = []
        self.scope_replacements: list[dict[str, object]] = []
        self.delete_calls: list[tuple[str, str, tuple[object, ...]]] = []
        self.replaced_tables: list[str] = []
        self.select_one_rows: dict[str, dict[str, object] | None] = {}
        self.select_many_rows: dict[str, list[dict[str, object]]] = {}
        self.runtime_rows: dict[str, list[dict[str, object]]] = {
            "agent_trace_spans": [],
            "agent_worker_runs": [],
            "workflow_job_leases": [],
        }
        self.generic_rows: dict[str, list[dict[str, object]]] = {}
        self.native_rows: dict[str, list[dict[str, object]]] = {
            "jobs": [],
            "job_events": [],
            "job_progress_event_summaries": [],
            "agent_runtime_sessions": [],
        }
        self._job_event_id = 1
        self._session_id = 1
        self._trace_span_id = 1
        self._worker_id = 1
        self._generated_ids: dict[str, int] = {table_name: 1 for table_name in self._GENERATED_ID_COLUMNS}

    @property
    def enabled(self) -> bool:
        return bool(self.dsn) and self.mode != "disabled"

    def should_mirror(self, table_name: str) -> bool:
        return self.enabled and str(table_name or "").strip() in self.tables

    def should_prefer_read(self, table_name: str) -> bool:
        return self.should_mirror(table_name) and self.mode in {"prefer_postgres", "postgres_only"}

    def is_authoritative(self, table_name: str) -> bool:
        return self.should_prefer_read(table_name) and self.mode == "postgres_only"

    def ensure_bootstrapped(self) -> None:
        return

    def replace_table_from_sqlite(self, table_name: str) -> None:
        self.replaced_tables.append(str(table_name or "").strip())

    def upsert_row(self, table_name: str, row: dict[str, object] | None) -> None:
        normalized_table = str(table_name or "").strip()
        payload = dict(row or {})
        self.upserts.append((normalized_table, payload))
        if not payload:
            return
        existing_rows = list(self.generic_rows.get(normalized_table, []))
        primary_keys = self._PRIMARY_KEY_COLUMNS.get(normalized_table, ())
        if primary_keys and all(payload.get(column) not in {None, ""} for column in primary_keys):
            existing_rows = [
                item
                for item in existing_rows
                if not all(item.get(column) == payload.get(column) for column in primary_keys)
            ]
        existing_rows.append(payload)
        self.generic_rows[normalized_table] = existing_rows

    def bulk_upsert_rows(self, table_name: str, rows: list[dict[str, object]] | tuple[dict[str, object], ...]) -> int:
        normalized_table = str(table_name or "").strip()
        payload_rows = [dict(row or {}) for row in list(rows or []) if dict(row or {})]
        self.bulk_upserts.append((normalized_table, payload_rows))
        for payload in payload_rows:
            self.upsert_row(normalized_table, payload)
        return len(payload_rows)

    def replace_candidate_materialization_state_scope(
        self,
        *,
        table_name: str = "",
        target_company: str,
        company_key: str = "",
        snapshot_id: str,
        asset_view: str,
        rows: list[dict[str, object]] | tuple[dict[str, object], ...],
    ) -> int:
        normalized_table = "candidate_materialization_state"
        normalized_target_company = str(target_company or "").strip()
        normalized_company_key = str(company_key or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "").strip()
        payload_rows = [dict(row or {}) for row in list(rows or []) if dict(row or {})]
        self.scope_replacements.append(
            {
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "rows": payload_rows,
            }
        )
        existing_rows = list(self.generic_rows.get(normalized_table, []))
        kept_rows = [
            item
            for item in existing_rows
            if not (
                (
                    str(item.get("target_company") or "") == normalized_target_company
                    or (normalized_company_key and str(item.get("company_key") or "") == normalized_company_key)
                )
                and str(item.get("snapshot_id") or "") == normalized_snapshot_id
                and str(item.get("asset_view") or "") == normalized_asset_view
            )
        ]
        kept_rows.extend(payload_rows)
        self.generic_rows[normalized_table] = kept_rows
        return len(payload_rows)

    def insert_row_with_generated_id(
        self,
        *,
        table_name: str,
        row: dict[str, object] | None,
        id_column: str = "",
        sequence_name: str = "",
    ) -> dict[str, object] | None:
        normalized_table = str(table_name or "").strip()
        if not self.should_prefer_read(normalized_table):
            return None
        payload = dict(row or {})
        normalized_id_column = str(id_column or self._GENERATED_ID_COLUMNS.get(normalized_table) or "").strip()
        if not normalized_id_column:
            return None
        generated_id = int(payload.get(normalized_id_column) or 0)
        if generated_id <= 0:
            generated_id = self._next_generated_id(normalized_table)
        payload[normalized_id_column] = generated_id
        return self._persist_generic_row(normalized_table, payload)

    def upsert_row_with_generated_id(
        self,
        *,
        table_name: str,
        row: dict[str, object] | None,
        conflict_columns: list[str] | tuple[str, ...],
        id_column: str = "",
        sequence_name: str = "",
        update_columns: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, object] | None:
        normalized_table = str(table_name or "").strip()
        if not self.should_prefer_read(normalized_table):
            return None
        payload = dict(row or {})
        normalized_id_column = str(id_column or self._GENERATED_ID_COLUMNS.get(normalized_table) or "").strip()
        normalized_conflict_columns = [
            str(column or "").strip() for column in list(conflict_columns or []) if str(column or "").strip()
        ]
        if not normalized_id_column or not normalized_conflict_columns:
            return None
        rows = list(self.generic_rows.get(normalized_table, []))
        existing_index = next(
            (
                index
                for index, existing_row in enumerate(rows)
                if all(
                    str(existing_row.get(column) or "") == str(payload.get(column) or "")
                    for column in normalized_conflict_columns
                )
            ),
            None,
        )
        if existing_index is None:
            generated_id = int(payload.get(normalized_id_column) or 0)
            if generated_id <= 0:
                generated_id = self._next_generated_id(normalized_table)
            payload[normalized_id_column] = generated_id
            return self._persist_generic_row(normalized_table, payload)
        existing_row = dict(rows[existing_index])
        normalized_update_columns = [
            str(column or "").strip()
            for column in list(update_columns or payload.keys())
            if str(column or "").strip() and str(column or "").strip() != normalized_id_column
        ]
        for column in normalized_update_columns:
            if column in payload:
                existing_row[column] = payload[column]
        rows[existing_index] = existing_row
        self.generic_rows[normalized_table] = rows
        return dict(existing_row)

    def update_row_returning(
        self,
        *,
        table_name: str,
        id_column: str,
        id_value: object,
        row: dict[str, object] | None,
    ) -> dict[str, object] | None:
        normalized_table = str(table_name or "").strip()
        normalized_id_column = str(id_column or "").strip()
        if not self.should_prefer_read(normalized_table) or not normalized_id_column:
            return None
        rows = list(self.generic_rows.get(normalized_table, []))
        for index, existing_row in enumerate(rows):
            if str(existing_row.get(normalized_id_column) or "") != str(id_value or ""):
                continue
            updated_row = dict(existing_row)
            updated_row.update(dict(row or {}))
            rows[index] = updated_row
            self.generic_rows[normalized_table] = rows
            return dict(updated_row)
        return None

    def delete_rows(
        self,
        *,
        table_name: str,
        where_sql: str,
        params: list[object] | tuple[object, ...] = (),
    ) -> int:
        normalized_table = str(table_name or "").strip()
        if not self.should_prefer_read(normalized_table):
            return 0
        self.delete_calls.append((normalized_table, str(where_sql or "").strip(), tuple(params)))
        rows = list(self.generic_rows.get(normalized_table, []))
        kept_rows: list[dict[str, object]] = []
        deleted_count = 0
        for row in rows:
            if self._row_matches_where(normalized_table, row, where_sql=where_sql, params=params):
                deleted_count += 1
                continue
            kept_rows.append(row)
        self.generic_rows[normalized_table] = kept_rows
        return deleted_count

    def select_one(
        self,
        table_name: str,
        *,
        where_sql: str,
        params: list[object] | tuple[object, ...],
        order_by_sql: str = "",
    ) -> dict[str, object] | None:
        normalized_table = str(table_name or "").strip()
        preset = self.select_one_rows.get(normalized_table)
        if preset is not None:
            return preset
        rows = self.select_many(
            normalized_table,
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
        params: list[object] | tuple[object, ...] = (),
        order_by_sql: str = "",
        limit: int = 100,
    ) -> list[dict[str, object]]:
        normalized_table = str(table_name or "").strip()
        rows = list(self.select_many_rows.get(normalized_table, []))
        if not rows:
            rows = self._filtered_native_rows(normalized_table, where_sql=where_sql, params=params)
            if "DESC" in str(order_by_sql or "").upper():
                rows = list(reversed(rows))
        normalized_limit = int(limit or 0)
        if normalized_limit > 0:
            return rows[:normalized_limit]
        return rows

    def save_job_row(
        self,
        *,
        row: dict[str, object] | None,
        protect_terminal_statuses: bool = True,
        terminal_statuses: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> dict[str, object] | None:
        payload = dict(row or {})
        normalized_job_id = str(payload.get("job_id") or "").strip()
        if not normalized_job_id:
            return None
        terminal_status_set = {
            str(item or "").strip().lower() for item in list(terminal_statuses or []) if str(item or "").strip()
        }
        existing = next((item for item in self.native_rows["jobs"] if str(item["job_id"]) == normalized_job_id), None)
        existing_status = str((existing or {}).get("status") or "").strip().lower()
        incoming_status = str(payload.get("status") or "").strip().lower()
        if (
            protect_terminal_statuses
            and existing_status in terminal_status_set
            and incoming_status not in terminal_status_set
        ):
            return dict(existing) if existing is not None else None
        row_payload = {
            "job_id": normalized_job_id,
            "job_type": str(payload.get("job_type") or "workflow"),
            "status": str(payload.get("status") or ""),
            "stage": str(payload.get("stage") or ""),
            "request_json": str(payload.get("request_json") or "{}"),
            "plan_json": str(payload.get("plan_json") or "{}"),
            "execution_bundle_json": str(payload.get("execution_bundle_json") or "{}"),
            "matching_request_json": str(payload.get("matching_request_json") or "{}"),
            "summary_json": str(payload.get("summary_json") or "{}"),
            "artifact_path": str(payload.get("artifact_path") or ""),
            "request_signature": str(payload.get("request_signature") or ""),
            "request_family_signature": str(payload.get("request_family_signature") or ""),
            "matching_request_signature": str(payload.get("matching_request_signature") or ""),
            "matching_request_family_signature": str(payload.get("matching_request_family_signature") or ""),
            "requester_id": str(payload.get("requester_id") or ""),
            "tenant_id": str(payload.get("tenant_id") or ""),
            "idempotency_key": str(payload.get("idempotency_key") or ""),
            "created_at": str((existing or {}).get("created_at") or "2026-04-19T12:00:00Z"),
            "updated_at": "2026-04-19T12:00:00Z",
        }
        self.native_rows["jobs"] = [
            item for item in self.native_rows["jobs"] if str(item.get("job_id") or "") != normalized_job_id
        ]
        self.native_rows["jobs"].append(row_payload)
        return dict(row_payload)

    def append_job_event(
        self,
        *,
        job_id: str,
        stage: str,
        status: str,
        detail: str,
        payload_dict: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        event_row = {
            "event_id": self._job_event_id,
            "job_id": normalized_job_id,
            "stage": str(stage or ""),
            "status": str(status or ""),
            "detail": str(detail or ""),
            "payload_json": json.dumps(payload_dict or {}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
        }
        self._job_event_id += 1
        self.native_rows["job_events"].append(event_row)
        existing_summary = next(
            (
                item
                for item in self.native_rows["job_progress_event_summaries"]
                if str(item.get("job_id") or "") == normalized_job_id
            ),
            None,
        )
        summary = {
            "event_count": int((existing_summary or {}).get("event_count") or 0),
            "latest_event": json.loads(str((existing_summary or {}).get("latest_event_json") or "{}")),
            "stage_sequence": json.loads(str((existing_summary or {}).get("stage_sequence_json") or "[]")),
            "stage_stats": json.loads(str((existing_summary or {}).get("stage_stats_json") or "{}")),
            "latest_metrics": json.loads(str((existing_summary or {}).get("latest_metrics_json") or "{}")),
        }
        updated_summary = update_job_progress_event_summary(
            summary,
            event={
                "event_id": int(event_row["event_id"]),
                "stage": str(event_row["stage"]),
                "status": str(event_row["status"]),
                "detail": str(event_row["detail"]),
                "payload": dict(payload_dict or {}),
                "created_at": str(event_row["created_at"]),
            },
        )
        summary_row = {
            "job_id": normalized_job_id,
            "event_count": int(updated_summary.get("event_count") or 0),
            "latest_event_json": json.dumps(updated_summary.get("latest_event") or {}, ensure_ascii=False),
            "stage_sequence_json": json.dumps(updated_summary.get("stage_sequence") or [], ensure_ascii=False),
            "stage_stats_json": json.dumps(updated_summary.get("stage_stats") or {}, ensure_ascii=False),
            "latest_metrics_json": json.dumps(updated_summary.get("latest_metrics") or {}, ensure_ascii=False),
            "created_at": str((existing_summary or {}).get("created_at") or "2026-04-19T12:00:00Z"),
            "updated_at": "2026-04-19T12:00:00Z",
        }
        self.native_rows["job_progress_event_summaries"] = [
            item
            for item in self.native_rows["job_progress_event_summaries"]
            if str(item.get("job_id") or "") != normalized_job_id
        ]
        self.native_rows["job_progress_event_summaries"].append(summary_row)
        return {"event": dict(event_row), "summary": dict(summary_row), "compacted": False}

    def create_agent_runtime_session_row(self, *, row: dict[str, object] | None) -> dict[str, object] | None:
        payload = dict(row or {})
        normalized_job_id = str(payload.get("job_id") or "").strip()
        if not normalized_job_id:
            return None
        existing = next(
            (
                item
                for item in self.native_rows["agent_runtime_sessions"]
                if str(item.get("job_id") or "") == normalized_job_id
            ),
            None,
        )
        session_row = {
            "session_id": int((existing or {}).get("session_id") or self._session_id),
            "job_id": normalized_job_id,
            "target_company": str(payload.get("target_company") or ""),
            "request_signature": str(payload.get("request_signature") or ""),
            "request_family_signature": str(payload.get("request_family_signature") or ""),
            "runtime_mode": str(payload.get("runtime_mode") or "agent_runtime"),
            "status": str(payload.get("status") or "running"),
            "lanes_json": str(payload.get("lanes_json") or "[]"),
            "metadata_json": str(payload.get("metadata_json") or "{}"),
            "created_at": str((existing or {}).get("created_at") or "2026-04-19T12:00:00Z"),
            "updated_at": "2026-04-19T12:00:00Z",
        }
        if existing is None:
            self._session_id += 1
        self.native_rows["agent_runtime_sessions"] = [
            item
            for item in self.native_rows["agent_runtime_sessions"]
            if str(item.get("job_id") or "") != normalized_job_id
        ]
        self.native_rows["agent_runtime_sessions"].append(session_row)
        return dict(session_row)

    def update_agent_runtime_session_status(self, job_id: str, status: str) -> dict[str, object] | None:
        normalized_job_id = str(job_id or "").strip()
        for row in self.native_rows["agent_runtime_sessions"]:
            if str(row.get("job_id") or "") != normalized_job_id:
                continue
            row["status"] = str(status or "")
            row["updated_at"] = "2026-04-19T12:00:00Z"
            return dict(row)
        return None

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
        input_payload: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        row = {
            "span_id": self._trace_span_id,
            "session_id": session_id,
            "job_id": job_id,
            "parent_span_id": parent_span_id or None,
            "lane_id": lane_id,
            "handoff_from_lane": handoff_from_lane,
            "handoff_to_lane": handoff_to_lane,
            "span_name": span_name,
            "stage": stage,
            "status": "running",
            "input_json": json.dumps(input_payload or {}, ensure_ascii=False),
            "output_json": json.dumps({}, ensure_ascii=False),
            "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
            "started_at": "2026-04-19 12:00:00",
            "completed_at": "",
            "created_at": "2026-04-19 12:00:00",
        }
        self._trace_span_id += 1
        self.runtime_rows["agent_trace_spans"].append(row)
        return dict(row)

    def update_agent_trace_span(
        self,
        span_id: int,
        *,
        status: str,
        output_payload: dict[str, object] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_trace_spans"]:
            if int(row["span_id"]) != int(span_id):
                continue
            row["status"] = status
            row["output_json"] = json.dumps(output_payload or {}, ensure_ascii=False)
            if handoff_to_lane:
                row["handoff_to_lane"] = handoff_to_lane
            row["completed_at"] = "2026-04-19 12:01:00"
            return dict(row)
        return None

    def get_agent_trace_span(self, span_id: int) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_trace_spans"]:
            if int(row["span_id"]) == int(span_id):
                return dict(row)
        return None

    def list_agent_trace_spans(self, *, job_id: str = "", session_id: int = 0) -> list[dict[str, object]]:
        rows = [
            dict(row)
            for row in self.runtime_rows["agent_trace_spans"]
            if (not job_id or str(row["job_id"]) == job_id)
            and (not session_id or int(row["session_id"]) == int(session_id))
        ]
        return sorted(rows, key=lambda item: int(item["span_id"]))

    def create_or_resume_agent_worker(
        self,
        *,
        session_id: int,
        job_id: str,
        span_id: int,
        lane_id: str,
        worker_key: str,
        budget_payload: dict[str, object] | None = None,
        input_payload: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        for row in self.runtime_rows["agent_worker_runs"]:
            if str(row["job_id"]) == job_id and str(row["lane_id"]) == lane_id and str(row["worker_key"]) == worker_key:
                row["session_id"] = session_id
                row["span_id"] = span_id
                row["budget_json"] = json.dumps(budget_payload or {}, ensure_ascii=False)
                row["input_json"] = json.dumps(input_payload or {}, ensure_ascii=False)
                row["metadata_json"] = json.dumps(metadata or {}, ensure_ascii=False)
                if str(row["status"]) == "interrupted":
                    row["status"] = "queued"
                return dict(row)
        row = {
            "worker_id": self._worker_id,
            "session_id": session_id,
            "job_id": job_id,
            "span_id": span_id,
            "lane_id": lane_id,
            "worker_key": worker_key,
            "status": "queued",
            "interrupt_requested": 0,
            "budget_json": json.dumps(budget_payload or {}, ensure_ascii=False),
            "checkpoint_json": json.dumps({}, ensure_ascii=False),
            "input_json": json.dumps(input_payload or {}, ensure_ascii=False),
            "output_json": json.dumps({}, ensure_ascii=False),
            "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
            "lease_owner": "",
            "lease_expires_at": "",
            "attempt_count": 0,
            "last_error": "",
            "created_at": "2026-04-19 12:00:00",
            "updated_at": "2026-04-19 12:00:00",
        }
        self._worker_id += 1
        self.runtime_rows["agent_worker_runs"].append(row)
        return dict(row)

    def mark_agent_worker_running(self, worker_id: int) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) == int(worker_id):
                if str(row["status"]) != "completed":
                    row["status"] = "running"
                return dict(row)
        return None

    def checkpoint_agent_worker(
        self,
        worker_id: int,
        *,
        checkpoint_payload: dict[str, object] | None = None,
        output_payload: dict[str, object] | None = None,
        status: str = "running",
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) != int(worker_id):
                continue
            row["status"] = status
            row["checkpoint_json"] = json.dumps(checkpoint_payload or {}, ensure_ascii=False)
            row["output_json"] = json.dumps(output_payload or {}, ensure_ascii=False)
            return dict(row)
        return None

    def complete_agent_worker(
        self,
        worker_id: int,
        *,
        status: str,
        checkpoint_payload: dict[str, object] | None = None,
        output_payload: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        row = self.checkpoint_agent_worker(
            worker_id,
            checkpoint_payload=checkpoint_payload,
            output_payload=output_payload,
            status=status,
        )
        if row is None:
            return None
        row["lease_owner"] = ""
        row["lease_expires_at"] = ""
        for stored in self.runtime_rows["agent_worker_runs"]:
            if int(stored["worker_id"]) == int(worker_id):
                stored["lease_owner"] = ""
                stored["lease_expires_at"] = ""
        return row

    def get_agent_worker(
        self,
        *,
        worker_id: int = 0,
        job_id: str = "",
        lane_id: str = "",
        worker_key: str = "",
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if worker_id and int(row["worker_id"]) == int(worker_id):
                return dict(row)
            if job_id and lane_id and worker_key:
                if (
                    str(row["job_id"]) == job_id
                    and str(row["lane_id"]) == lane_id
                    and str(row["worker_key"]) == worker_key
                ):
                    return dict(row)
        return None

    def list_agent_workers(
        self, *, job_id: str = "", session_id: int = 0, lane_id: str = ""
    ) -> list[dict[str, object]]:
        rows = [
            dict(row)
            for row in self.runtime_rows["agent_worker_runs"]
            if (not job_id or str(row["job_id"]) == job_id)
            and (not session_id or int(row["session_id"]) == int(session_id))
            and (not lane_id or str(row["lane_id"]) == lane_id)
        ]
        return sorted(rows, key=lambda item: int(item["worker_id"]))

    def list_recoverable_agent_workers(
        self,
        *,
        limit: int = 100,
        stale_after_seconds: int = 300,
        lane_id: str = "",
        job_id: str = "",
    ) -> list[dict[str, object]]:
        rows = [
            row
            for row in self.list_agent_workers(job_id=job_id, lane_id=lane_id)
            if str(row["status"]) in {"queued", "interrupted", "failed"}
        ]
        return rows[: max(1, int(limit or 100))]

    def claim_agent_worker(self, worker_id: int, *, lease_owner: str, lease_seconds: int) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) != int(worker_id):
                continue
            row["lease_owner"] = lease_owner
            row["lease_expires_at"] = "2099-01-01 00:00:00"
            row["attempt_count"] = int(row["attempt_count"]) + 1
            return dict(row)
        return None

    def renew_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) == int(worker_id) and str(row["lease_owner"]) == lease_owner:
                row["lease_expires_at"] = "2099-01-01 00:00:00"
                return dict(row)
        return None

    def release_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str = "",
        error_text: str = "",
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) != int(worker_id):
                continue
            if lease_owner and str(row["lease_owner"]) != lease_owner:
                continue
            row["lease_owner"] = ""
            row["lease_expires_at"] = ""
            if error_text:
                row["last_error"] = error_text
            return dict(row)
        return None

    def request_interrupt_agent_worker(self, worker_id: int) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) == int(worker_id):
                row["interrupt_requested"] = 1
                return dict(row)
        return None

    def clear_interrupt_agent_worker(self, worker_id: int) -> dict[str, object] | None:
        for row in self.runtime_rows["agent_worker_runs"]:
            if int(row["worker_id"]) == int(worker_id):
                row["interrupt_requested"] = 0
                return dict(row)
        return None

    def acquire_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 900,
        lease_token: str = "",
    ) -> dict[str, object] | None:
        current = self.get_workflow_job_lease(job_id)
        if (
            current
            and str(current.get("lease_owner") or "") not in {"", lease_owner}
            and str(current.get("lease_token") or "") not in {"", lease_token}
        ):
            return current
        row = {
            "job_id": job_id,
            "lease_owner": lease_owner,
            "lease_token": lease_token,
            "lease_expires_at": "2099-01-01 00:00:00",
            "created_at": "2026-04-19 12:00:00",
            "updated_at": "2026-04-19 12:00:00",
        }
        self.runtime_rows["workflow_job_leases"] = [
            existing for existing in self.runtime_rows["workflow_job_leases"] if str(existing["job_id"]) != job_id
        ]
        self.runtime_rows["workflow_job_leases"].append(row)
        return dict(row)

    def get_workflow_job_lease(self, job_id: str) -> dict[str, object] | None:
        for row in self.runtime_rows["workflow_job_leases"]:
            if str(row["job_id"]) == job_id:
                return dict(row)
        return None

    def renew_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
        lease_token: str = "",
    ) -> dict[str, object] | None:
        for row in self.runtime_rows["workflow_job_leases"]:
            if str(row["job_id"]) != job_id or str(row["lease_owner"]) != lease_owner:
                continue
            if lease_token and str(row["lease_token"]) != lease_token:
                continue
            row["lease_expires_at"] = "2099-01-01 00:00:00"
            return dict(row)
        return None

    def release_workflow_job_lease(self, job_id: str, *, lease_owner: str = "", lease_token: str = "") -> None:
        kept: list[dict[str, object]] = []
        for row in self.runtime_rows["workflow_job_leases"]:
            if str(row["job_id"]) != job_id:
                kept.append(row)
                continue
            if lease_owner and str(row["lease_owner"]) != lease_owner:
                kept.append(row)
                continue
            if lease_token and str(row["lease_token"]) != lease_token:
                kept.append(row)
                continue
        self.runtime_rows["workflow_job_leases"] = kept

    def supersede_workflow_runtime_state(self, job_id: str) -> dict[str, object]:
        worker_count = 0
        trace_count = 0
        for row in self.runtime_rows["agent_worker_runs"]:
            if str(row["job_id"]) == job_id and str(row["status"]) in {"queued", "running", "interrupted", "failed"}:
                row["status"] = "superseded"
                row["lease_owner"] = ""
                row["lease_expires_at"] = ""
                worker_count += 1
        for row in self.runtime_rows["agent_trace_spans"]:
            if str(row["job_id"]) == job_id:
                if str(row["status"]) == "running":
                    row["status"] = "superseded"
                trace_count += 1
        self.release_workflow_job_lease(job_id)
        return {"superseded_worker_count": worker_count, "superseded_trace_count": trace_count}

    def _next_generated_id(self, table_name: str) -> int:
        normalized_table = str(table_name or "").strip()
        next_value = int(self._generated_ids.get(normalized_table) or 1)
        self._generated_ids[normalized_table] = next_value + 1
        return next_value

    def _persist_generic_row(self, table_name: str, payload: dict[str, object]) -> dict[str, object]:
        normalized_table = str(table_name or "").strip()
        stored_payload = dict(payload)
        stored_payload.setdefault("created_at", "2026-04-19T12:00:00Z")
        stored_payload.setdefault("updated_at", stored_payload.get("created_at") or "2026-04-19T12:00:00Z")
        rows = list(self.generic_rows.get(normalized_table, []))
        primary_keys = self._PRIMARY_KEY_COLUMNS.get(normalized_table, ())
        if primary_keys and all(stored_payload.get(column) not in {None, ""} for column in primary_keys):
            rows = [
                existing_row
                for existing_row in rows
                if not all(existing_row.get(column) == stored_payload.get(column) for column in primary_keys)
            ]
        rows.append(dict(stored_payload))
        self.generic_rows[normalized_table] = rows
        return dict(stored_payload)

    def _row_matches_where(
        self,
        table_name: str,
        row: dict[str, object],
        *,
        where_sql: str,
        params: list[object] | tuple[object, ...],
    ) -> bool:
        normalized_where = str(where_sql or "").strip()
        if not normalized_where:
            return True
        values = list(params)
        if table_name == "job_result_views" and normalized_where == "job_id = %s OR view_id = %s":
            return str(row.get("job_id") or "") == str(values[0] or "") or str(row.get("view_id") or "") == str(
                values[1] or ""
            )
        clauses = [item.strip() for item in normalized_where.split(" AND ") if item.strip()]
        value_index = 0
        for clause in clauses:
            if clause.startswith("(") and clause.endswith(")") and " OR " in clause:
                subclauses = [item.strip() for item in clause[1:-1].split(" OR ") if item.strip()]
                matched = False
                for subclause in subclauses:
                    if subclause.startswith("lower(") and subclause.endswith(") = lower(%s)"):
                        field_name = subclause[len("lower(") : subclause.index(") = lower(%s)")]
                        expected_value = str(values[value_index] or "").lower()
                        value_index += 1
                        if str(row.get(field_name) or "").lower() == expected_value:
                            matched = True
                        continue
                    if subclause.endswith(" = %s"):
                        field_name = subclause[: -len(" = %s")].strip()
                        expected_value = values[value_index]
                        value_index += 1
                        if str(row.get(field_name) or "") == str(expected_value or ""):
                            matched = True
                        continue
                if not matched:
                    return False
                continue
            if clause.startswith("lower(") and clause.endswith(") = lower(%s)"):
                field_name = clause[len("lower(") : clause.index(") = lower(%s)")]
                expected_value = str(values[value_index] or "").lower()
                value_index += 1
                if str(row.get(field_name) or "").lower() != expected_value:
                    return False
                continue
            if clause.endswith(" = %s"):
                field_name = clause[: -len(" = %s")].strip()
                expected_value = values[value_index]
                value_index += 1
                if str(row.get(field_name) or "") != str(expected_value or ""):
                    return False
                continue
            if " IN (" in clause and ("%s" in clause or "?" in clause):
                field_name, _, remainder = clause.partition(" IN ")
                placeholder_count = remainder.count("%s") + remainder.count("?")
                expected_values = {
                    str(item or "") for item in values[value_index : value_index + max(placeholder_count, 0)]
                }
                value_index += max(placeholder_count, 0)
                if str(row.get(field_name.strip()) or "") not in expected_values:
                    return False
                continue
            if clause.endswith(" = 1"):
                field_name = clause[: -len(" = 1")].strip()
                if int(bool(row.get(field_name))) != 1:
                    return False
                continue
            if clause.endswith(" = 'active'"):
                field_name = clause[: -len(" = 'active'")].strip()
                if str(row.get(field_name) or "") != "active":
                    return False
                continue
        return True

    def _filtered_native_rows(
        self,
        table_name: str,
        *,
        where_sql: str,
        params: list[object] | tuple[object, ...],
    ) -> list[dict[str, object]]:
        rows = list(self.native_rows.get(table_name, []))
        if not rows:
            rows = list(self.generic_rows.get(table_name, []))
        normalized_where = str(where_sql or "").strip()
        if table_name in {"jobs", "job_progress_event_summaries"} and normalized_where == "job_id = %s":
            return [row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)]
        if table_name == "job_result_views":
            if normalized_where == "job_id = %s":
                return [
                    row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)
                ]
            if normalized_where == "view_id = %s":
                return [
                    row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)
                ]
            if normalized_where == "job_id = %s OR view_id = %s":
                return [
                    row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)
                ]
        if table_name == "agent_runtime_sessions":
            if normalized_where == "job_id = %s":
                return [
                    row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)
                ]
            if normalized_where == "session_id = %s":
                return [
                    row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)
                ]
        if table_name == "job_events":
            values = list(params)
            filtered = [row for row in rows if str(row.get("job_id") or "") == str(values[0] or "")]
            if "stage = %s" in normalized_where and len(values) > 1:
                filtered = [row for row in filtered if str(row.get("stage") or "") == str(values[1] or "")]
            return filtered
        if table_name in {
            "candidates",
            "evidence",
            "job_results",
            "candidate_review_registry",
            "target_candidates",
            "frontend_history_links",
            "plan_review_sessions",
            "manual_review_items",
            "query_dispatches",
            "confidence_policy_controls",
            "organization_asset_registry",
            "organization_execution_profiles",
            "cloud_asset_operation_ledger",
            "asset_materialization_generations",
            "asset_membership_index",
            "candidate_materialization_state",
            "snapshot_materialization_runs",
        }:
            return [row for row in rows if self._row_matches_where(table_name, row, where_sql=where_sql, params=params)]
        return rows


def _organization_asset_registry_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "registry_id": 1,
        "target_company": "Acme",
        "company_key": "acme",
        "snapshot_id": "snap-1",
        "asset_view": "canonical_merged",
        "status": "ready",
        "authoritative": 0,
        "candidate_count": 0,
        "evidence_count": 0,
        "profile_detail_count": 0,
        "explicit_profile_capture_count": 0,
        "missing_linkedin_count": 0,
        "manual_review_backlog_count": 0,
        "profile_completion_backlog_count": 0,
        "source_snapshot_count": 0,
        "completeness_score": 0.0,
        "completeness_band": "low",
        "current_lane_coverage_json": "{}",
        "former_lane_coverage_json": "{}",
        "current_lane_effective_candidate_count": 0,
        "former_lane_effective_candidate_count": 0,
        "current_lane_effective_ready": 0,
        "former_lane_effective_ready": 0,
        "source_snapshot_selection_json": "{}",
        "selected_snapshot_ids_json": "[]",
        "source_path": "",
        "source_job_id": "",
        "materialization_generation_key": "",
        "materialization_generation_sequence": 0,
        "materialization_watermark": "",
        "summary_json": "{}",
        "created_at": "2026-04-19T12:00:00Z",
        "updated_at": "2026-04-19T12:00:00Z",
    }
    row.update(overrides)
    return row


def _organization_execution_profile_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "profile_id": 1,
        "target_company": "Acme",
        "company_key": "acme",
        "asset_view": "canonical_merged",
        "source_registry_id": 0,
        "source_snapshot_id": "",
        "source_job_id": "",
        "source_generation_key": "",
        "source_generation_sequence": 0,
        "source_generation_watermark": "",
        "status": "ready",
        "org_scale_band": "unknown",
        "default_acquisition_mode": "full_company_roster",
        "prefer_delta_from_baseline": 0,
        "current_lane_default": "live_acquisition",
        "former_lane_default": "live_acquisition",
        "baseline_candidate_count": 0,
        "current_lane_effective_candidate_count": 0,
        "former_lane_effective_candidate_count": 0,
        "completeness_score": 0.0,
        "completeness_band": "low",
        "profile_detail_ratio": 0.0,
        "company_employee_shard_count": 0,
        "current_profile_search_shard_count": 0,
        "former_profile_search_shard_count": 0,
        "company_employee_cap_hit_count": 0,
        "profile_search_cap_hit_count": 0,
        "reason_codes_json": "[]",
        "explanation_json": "{}",
        "summary_json": "{}",
        "created_at": "2026-04-19T12:00:00Z",
        "updated_at": "2026-04-19T12:00:00Z",
    }
    row.update(overrides)
    return row


def _job_result_view_row(*, job_id: str, target_company: str) -> dict[str, object]:
    return {
        "view_id": f"view-{job_id}",
        "job_id": job_id,
        "target_company": target_company,
        "company_key": target_company.lower().replace(" ", "-"),
        "source_kind": "artifact_registry",
        "view_kind": "latest_results",
        "snapshot_id": "20260419T120000",
        "asset_view": "canonical_merged",
        "source_path": "/tmp/source",
        "authoritative_snapshot_id": "20260419T120000",
        "materialization_generation_key": "gen-1",
        "request_signature": "req-1",
        "summary_json": json.dumps({"candidate_count": 1}, ensure_ascii=False),
        "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
        "created_at": "2026-04-19T12:00:00Z",
        "updated_at": "2026-04-19T12:00:00Z",
    }


def _plan_review_row(*, review_id: int, target_company: str) -> dict[str, object]:
    return {
        "review_id": review_id,
        "target_company": target_company,
        "request_signature": "req-1",
        "request_family_signature": "fam-1",
        "matching_request_signature": "match-1",
        "matching_request_family_signature": "match-fam-1",
        "status": "pending",
        "risk_level": "medium",
        "required_before_execution": 1,
        "request_json": json.dumps({"target_company": target_company}, ensure_ascii=False),
        "plan_json": json.dumps({"mode": "test"}, ensure_ascii=False),
        "gate_json": json.dumps({"required_before_execution": True}, ensure_ascii=False),
        "execution_bundle_json": json.dumps({}, ensure_ascii=False),
        "matching_request_json": json.dumps({"matching_request_signature": "match-1"}, ensure_ascii=False),
        "decision_json": json.dumps({}, ensure_ascii=False),
        "reviewer": "",
        "review_notes": "",
        "approved_at": "",
        "created_at": "2026-04-19T12:00:00Z",
        "updated_at": "2026-04-19T12:00:00Z",
    }


def _job_row(*, job_id: str, target_company: str) -> dict[str, object]:
    return {
        "job_id": job_id,
        "job_type": "workflow",
        "status": "running",
        "stage": "acquiring",
        "request_json": json.dumps({"target_company": target_company}, ensure_ascii=False),
        "plan_json": json.dumps({"mode": "postgres"}, ensure_ascii=False),
        "execution_bundle_json": json.dumps({}, ensure_ascii=False),
        "matching_request_json": json.dumps({}, ensure_ascii=False),
        "summary_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
        "artifact_path": "",
        "request_signature": "req-1",
        "request_family_signature": "fam-1",
        "matching_request_signature": "match-1",
        "matching_request_family_signature": "match-fam-1",
        "requester_id": "",
        "tenant_id": "",
        "idempotency_key": "",
        "created_at": "2026-04-19T12:00:00Z",
        "updated_at": "2026-04-19T12:00:00Z",
    }


class ControlPlaneLivePostgresStorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.db_path = self.root / "runtime" / "sourcing_agent.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _build_store(self, *, mode: str | None, extra_env: dict[str, str] | None = None) -> SQLiteStore:
        env = {
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://user:pass@localhost:5432/sourcing",
        }
        if mode is not None:
            env["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = mode
        else:
            env["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = ""
        env.update(dict(extra_env or {}))
        with mock.patch("sourcing_agent.storage.LiveControlPlanePostgresAdapter", _FakeLiveControlPlanePostgresAdapter):
            with mock.patch.dict(
                os.environ,
                env,
                clear=False,
            ):
                return SQLiteStore(self.db_path)

    def test_job_result_view_write_is_mirrored_to_live_postgres(self) -> None:
        store = self._build_store(mode="mirror")

        store.upsert_job_result_view(
            job_id="job-1",
            target_company="Acme",
            source_kind="artifact_registry",
            view_kind="latest_results",
            snapshot_id="20260419T120000",
        )

        adapter = store._control_plane_postgres
        self.assertIsInstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        mirrored = [row for table_name, row in adapter.upserts if table_name == "job_result_views"]
        self.assertEqual(len(mirrored), 1)
        self.assertEqual(str(mirrored[0]["job_id"] or ""), "job-1")
        self.assertEqual(str(mirrored[0]["target_company"] or ""), "Acme")

    def test_organization_company_key_lookup_matches_legacy_target_company_aliases(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["organization_asset_registry"] = [
            _organization_asset_registry_row(
                registry_id=1,
                target_company="Reflection AI",
                company_key="reflectionai",
                snapshot_id="20260410T225933",
                authoritative=0,
                candidate_count=0,
                updated_at="2026-04-10T22:59:33Z",
                created_at="2026-04-10T22:59:33Z",
            ),
            _organization_asset_registry_row(
                registry_id=2,
                target_company="reflectionai",
                company_key="reflectionai",
                snapshot_id="20260413T022548",
                authoritative=1,
                candidate_count=24,
                updated_at="2026-04-13T02:25:48Z",
                created_at="2026-04-13T02:25:48Z",
            ),
        ]
        adapter.generic_rows["organization_execution_profiles"] = [
            _organization_execution_profile_row(
                profile_id=1,
                target_company="Reflection AI",
                company_key="reflectionai",
                source_snapshot_id="20260410T225933",
                default_acquisition_mode="full_company_roster",
                updated_at="2026-04-10T22:59:33Z",
                created_at="2026-04-10T22:59:33Z",
            ),
            _organization_execution_profile_row(
                profile_id=528,
                target_company="reflectionai",
                company_key="reflectionai",
                source_snapshot_id="20260413T022548",
                default_acquisition_mode="scoped_search_roster",
                updated_at="2026-04-13T02:25:48Z",
                created_at="2026-04-13T02:25:48Z",
            ),
        ]

        registry = store.get_authoritative_organization_asset_registry(target_company="Reflection AI")
        profile = store.get_organization_execution_profile(target_company="Reflection AI")
        profiles = store.list_organization_execution_profiles(target_company="Reflection AI", limit=10)

        self.assertEqual(registry["snapshot_id"], "20260413T022548")
        self.assertEqual(registry["target_company"], "reflectionai")
        self.assertEqual(profile["source_snapshot_id"], "20260413T022548")
        self.assertEqual(profile["default_acquisition_mode"], "scoped_search_roster")
        self.assertEqual([int(item["profile_id"]) for item in profiles], [528, 1])

    def test_organization_company_key_lookup_resolves_aliases_to_canonical_company_keys(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["organization_asset_registry"] = [
            _organization_asset_registry_row(
                registry_id=41,
                target_company="Google",
                company_key="google",
                snapshot_id="20260415T020102",
                authoritative=1,
                candidate_count=600,
                updated_at="2026-04-15T02:01:02Z",
                created_at="2026-04-15T02:01:02Z",
            ),
            _organization_asset_registry_row(
                registry_id=42,
                target_company="humansand",
                company_key="humansand",
                snapshot_id="20260414T040101",
                authoritative=1,
                candidate_count=520,
                updated_at="2026-04-14T04:01:01Z",
                created_at="2026-04-14T04:01:01Z",
            ),
        ]
        adapter.generic_rows["organization_execution_profiles"] = [
            _organization_execution_profile_row(
                profile_id=61,
                target_company="Google",
                company_key="google",
                source_snapshot_id="20260415T020102",
                default_acquisition_mode="scoped_search_roster",
                updated_at="2026-04-15T02:01:02Z",
                created_at="2026-04-15T02:01:02Z",
            ),
            _organization_execution_profile_row(
                profile_id=62,
                target_company="humansand",
                company_key="humansand",
                source_snapshot_id="20260414T040101",
                default_acquisition_mode="full_company_roster",
                updated_at="2026-04-14T04:01:01Z",
                created_at="2026-04-14T04:01:01Z",
            ),
        ]

        google_registry = store.get_authoritative_organization_asset_registry(target_company="Google DeepMind")
        google_profile = store.get_organization_execution_profile(target_company="Google DeepMind")
        humans_registry = store.get_authoritative_organization_asset_registry(target_company="Humans&")
        humans_profile = store.get_organization_execution_profile(target_company="Humans&")

        self.assertEqual(google_registry["snapshot_id"], "20260415T020102")
        self.assertEqual(google_registry["company_key"], "google")
        self.assertEqual(google_profile["default_acquisition_mode"], "scoped_search_roster")
        self.assertEqual(humans_registry["snapshot_id"], "20260414T040101")
        self.assertEqual(humans_registry["company_key"], "humansand")
        self.assertEqual(humans_profile["default_acquisition_mode"], "full_company_roster")

    def test_organization_upserts_reuse_existing_company_key_rows(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["organization_asset_registry"] = [
            _organization_asset_registry_row(
                registry_id=7,
                target_company="reflectionai",
                company_key="reflectionai",
                snapshot_id="20260413T022548",
                authoritative=1,
                candidate_count=24,
                updated_at="2026-04-13T02:25:48Z",
                created_at="2026-04-13T02:25:48Z",
            )
        ]
        adapter.generic_rows["organization_execution_profiles"] = [
            _organization_execution_profile_row(
                profile_id=12,
                target_company="reflectionai",
                company_key="reflectionai",
                source_snapshot_id="20260413T022548",
                default_acquisition_mode="scoped_search_roster",
                updated_at="2026-04-13T02:25:48Z",
                created_at="2026-04-13T02:25:48Z",
            )
        ]

        registry = store.upsert_organization_asset_registry(
            {
                "target_company": "Reflection AI",
                "company_key": "reflectionai",
                "snapshot_id": "20260413T022548",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 29,
            },
            authoritative=True,
        )
        profile = store.upsert_organization_execution_profile(
            {
                "target_company": "Reflection AI",
                "company_key": "reflectionai",
                "asset_view": "canonical_merged",
                "source_registry_id": int(registry["registry_id"]),
                "source_snapshot_id": "20260413T022548",
                "default_acquisition_mode": "full_company_roster",
                "status": "ready",
            }
        )

        registry_rows = list(adapter.generic_rows.get("organization_asset_registry", []))
        profile_rows = list(adapter.generic_rows.get("organization_execution_profiles", []))

        self.assertEqual(len(registry_rows), 1)
        self.assertEqual(len(profile_rows), 1)
        self.assertEqual(registry_rows[0]["registry_id"], 7)
        self.assertEqual(registry_rows[0]["target_company"], "Reflection AI")
        self.assertEqual(int(registry_rows[0]["candidate_count"] or 0), 29)
        self.assertEqual(profile_rows[0]["profile_id"], 12)
        self.assertEqual(profile_rows[0]["target_company"], "Reflection AI")
        self.assertEqual(profile["default_acquisition_mode"], "full_company_roster")

    def test_organization_upserts_reuse_existing_alias_mapped_company_key_rows(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["organization_asset_registry"] = [
            _organization_asset_registry_row(
                registry_id=17,
                target_company="humansand",
                company_key="humansand",
                snapshot_id="20260414T040101",
                authoritative=1,
                candidate_count=520,
                updated_at="2026-04-14T04:01:01Z",
                created_at="2026-04-14T04:01:01Z",
            )
        ]
        adapter.generic_rows["organization_execution_profiles"] = [
            _organization_execution_profile_row(
                profile_id=27,
                target_company="humansand",
                company_key="humansand",
                source_snapshot_id="20260414T040101",
                default_acquisition_mode="full_company_roster",
                updated_at="2026-04-14T04:01:01Z",
                created_at="2026-04-14T04:01:01Z",
            )
        ]

        registry = store.upsert_organization_asset_registry(
            {
                "target_company": "Humans&",
                "snapshot_id": "20260414T040101",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 533,
            },
            authoritative=True,
        )
        profile = store.upsert_organization_execution_profile(
            {
                "target_company": "Humans&",
                "asset_view": "canonical_merged",
                "source_registry_id": int(registry["registry_id"]),
                "source_snapshot_id": "20260414T040101",
                "default_acquisition_mode": "full_company_roster",
                "status": "ready",
            }
        )

        registry_rows = list(adapter.generic_rows.get("organization_asset_registry", []))
        profile_rows = list(adapter.generic_rows.get("organization_execution_profiles", []))

        self.assertEqual(len(registry_rows), 1)
        self.assertEqual(len(profile_rows), 1)
        self.assertEqual(registry_rows[0]["registry_id"], 17)
        self.assertEqual(profile_rows[0]["profile_id"], 27)
        self.assertEqual(registry_rows[0]["company_key"], "humansand")
        self.assertEqual(profile_rows[0]["company_key"], "humansand")
        self.assertEqual(registry_rows[0]["target_company"], "Humans&")
        self.assertEqual(profile_rows[0]["target_company"], "Humans&")
        self.assertEqual(int(registry_rows[0]["candidate_count"] or 0), 533)
        self.assertEqual(profile["default_acquisition_mode"], "full_company_roster")

    def test_candidates_and_evidence_can_be_postgres_authoritative(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        candidate = Candidate(
            candidate_id="cand_pg_1",
            name_en="Ada Lovelace",
            target_company="Acme",
            display_name="Ada Lovelace",
            role="Research Engineer",
            metadata={"source": "postgres"},
        )
        evidence = EvidenceRecord(
            evidence_id="ev_pg_1",
            candidate_id="cand_pg_1",
            source_type="linkedin_profile",
            title="LinkedIn",
            url="https://linkedin.com/in/ada",
            summary="Profile",
            source_dataset="postgres_test",
            source_path="runtime://postgres/evidence/ada",
            metadata={"source": "postgres"},
        )

        store.upsert_candidate(candidate)
        store.upsert_evidence_records([evidence])

        stored_candidate = store.get_candidate("cand_pg_1")
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.display_name, "Ada Lovelace")
        self.assertEqual(stored_candidate.metadata.get("source"), "postgres")

        stored_evidence = store.list_evidence("cand_pg_1")
        self.assertEqual(len(stored_evidence), 1)
        self.assertEqual(stored_evidence[0]["title"], "LinkedIn")
        self.assertEqual(store.candidate_count_for_company("Acme"), 1)

    def test_job_results_can_be_postgres_authoritative_with_candidate_join(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        candidate = Candidate(
            candidate_id="cand_result_pg",
            name_en="Grace Hopper",
            target_company="Acme",
            display_name="Grace Hopper",
            role="Compiler Lead",
            metadata={"outreach_layer": 2},
        )
        store.upsert_candidate(candidate)
        store.upsert_evidence_records(
            [
                EvidenceRecord(
                    evidence_id="ev_result_pg",
                    candidate_id="cand_result_pg",
                    source_type="linkedin_profile",
                    title="Grace LinkedIn",
                    url="https://linkedin.com/in/grace",
                    summary="Profile",
                    source_dataset="postgres_test",
                    source_path="runtime://postgres/evidence/grace",
                    metadata={},
                )
            ]
        )
        store.replace_job_results(
            "job_pg_results",
            [
                {
                    "candidate_id": "cand_result_pg",
                    "rank": 1,
                    "score": 97.5,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "strong match",
                    "explanation": "Top match",
                    "matched_fields": ["role"],
                }
            ],
        )

        self.assertEqual(store.count_job_results("job_pg_results"), 1)
        results = store.get_job_results("job_pg_results")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["display_name"], "Grace Hopper")
        self.assertEqual(results[0]["confidence_label"], "high")
        self.assertEqual(len(results[0]["evidence"]), 1)

    def test_candidate_bulk_replace_writes_natively_in_prefer_postgres(self) -> None:
        store = self._build_store(mode="prefer_postgres")

        candidate_one = Candidate(
            candidate_id="cand_bulk_1",
            name_en="Bulk One",
            display_name="Bulk One",
            category="employee",
            target_company="Acme Bulk",
            organization="Acme Bulk",
            employment_status="current",
            role="Engineer",
        )
        evidence_one = EvidenceRecord(
            evidence_id="ev_bulk_1",
            candidate_id="cand_bulk_1",
            source_type="linkedin_profile",
            title="Bulk One LinkedIn",
            url="https://linkedin.com/in/bulk-one",
            summary="Profile",
            source_dataset="bulk_test",
            source_path="runtime://bulk/one",
            metadata={},
        )
        store.replace_bootstrap_data([candidate_one], [evidence_one])

        candidate_two = Candidate(
            candidate_id="cand_bulk_2",
            name_en="Bulk Two",
            display_name="Bulk Two",
            category="employee",
            target_company="Acme Bulk",
            organization="Acme Bulk",
            employment_status="current",
            role="Researcher",
        )
        evidence_two = EvidenceRecord(
            evidence_id="ev_bulk_2",
            candidate_id="cand_bulk_2",
            source_type="linkedin_profile",
            title="Bulk Two LinkedIn",
            url="https://linkedin.com/in/bulk-two",
            summary="Profile",
            source_dataset="bulk_test",
            source_path="runtime://bulk/two",
            metadata={},
        )
        store.replace_company_data("Acme Bulk", [candidate_two], [evidence_two])

        candidates = store.list_candidates_for_company("Acme Bulk")
        evidence_rows = store.list_evidence("cand_bulk_2")
        self.assertEqual([candidate.candidate_id for candidate in candidates], ["cand_bulk_2"])
        self.assertEqual(evidence_rows[0]["evidence_id"], "ev_bulk_2")

        sqlite_candidate_count = store._connection.execute(
            "SELECT COUNT(*) FROM candidates WHERE lower(target_company) = lower(?)",
            ("Acme Bulk",),
        ).fetchone()[0]
        sqlite_evidence_count = store._connection.execute(
            "SELECT COUNT(*) FROM evidence WHERE candidate_id IN (?, ?)",
            ("cand_bulk_1", "cand_bulk_2"),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_candidate_count or 0), 0)
        self.assertEqual(int(sqlite_evidence_count or 0), 0)

    def test_job_result_view_prefers_postgres_read_but_falls_back_to_sqlite(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        adapter.select_one_rows["job_result_views"] = _job_result_view_row(
            job_id="job-pg",
            target_company="Acme PG",
        )

        postgres_result = store.get_job_result_view(job_id="job-pg")
        assert postgres_result is not None
        self.assertEqual(postgres_result["target_company"], "Acme PG")
        self.assertEqual(postgres_result["metadata"]["source"], "postgres")

        adapter.select_one_rows["job_result_views"] = None
        store.upsert_job_result_view(
            job_id="job-sqlite",
            target_company="Acme SQLite",
            source_kind="artifact_registry",
            view_kind="latest_results",
            snapshot_id="20260419T120000",
        )
        sqlite_result = store.get_job_result_view(job_id="job-sqlite")
        assert sqlite_result is not None
        self.assertEqual(sqlite_result["target_company"], "Acme SQLite")

    def test_plan_review_session_list_prefers_postgres_when_available(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        adapter.select_many_rows["plan_review_sessions"] = [
            _plan_review_row(review_id=42, target_company="Acme"),
        ]

        sessions = store.list_plan_review_sessions(target_company="Acme")
        self.assertEqual(len(sessions), 1)
        self.assertEqual(int(sessions[0]["review_id"] or 0), 42)
        self.assertEqual(str(sessions[0]["target_company"] or ""), "Acme")

        adapter.select_many_rows["plan_review_sessions"] = []
        created = store.create_plan_review_session(
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            plan_payload={"mode": "sqlite"},
            gate_payload={"required_before_execution": True, "risk_level": "medium"},
        )
        fallback_sessions = store.list_plan_review_sessions(target_company="Acme")
        self.assertGreaterEqual(len(fallback_sessions), 1)
        self.assertEqual(int(created["review_id"] or 0), int(fallback_sessions[0]["review_id"] or 0))

    def test_jobs_and_progress_summaries_are_mirrored_and_can_prefer_postgres_reads(self) -> None:
        store = self._build_store(mode="mirror")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        store.save_job(
            "job-1",
            "workflow",
            "running",
            "acquiring",
            {"target_company": "Acme"},
            plan_payload={"mode": "sqlite"},
        )
        store.append_job_event(
            "job-1",
            "acquiring",
            "running",
            "worker alive",
            {"candidate_count": 3},
        )

        mirrored_tables = [table_name for table_name, _ in adapter.upserts]
        self.assertIn("jobs", mirrored_tables)
        self.assertIn("job_progress_event_summaries", mirrored_tables)

        prefer_store = self._build_store(mode="prefer_postgres")
        prefer_adapter = prefer_store._control_plane_postgres
        assert isinstance(prefer_adapter, _FakeLiveControlPlanePostgresAdapter)
        prefer_adapter.select_one_rows["jobs"] = _job_row(job_id="job-pg", target_company="Acme PG")
        prefer_adapter.select_one_rows["job_progress_event_summaries"] = {
            "job_id": "job-pg",
            "event_count": 4,
            "latest_event_json": json.dumps({"stage": "acquiring"}, ensure_ascii=False),
            "stage_sequence_json": json.dumps(["planning", "acquiring"], ensure_ascii=False),
            "stage_stats_json": json.dumps({"acquiring": {"event_count": 2}}, ensure_ascii=False),
            "latest_metrics_json": json.dumps({"candidate_count": 7}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
            "updated_at": "2026-04-19T12:00:00Z",
        }

        postgres_job = prefer_store.get_job("job-pg")
        assert postgres_job is not None
        self.assertEqual(str(postgres_job["request"]["target_company"] or ""), "Acme PG")
        postgres_progress = prefer_store.get_job_progress_event_summary("job-pg")
        self.assertEqual(int(postgres_progress["event_count"] or 0), 4)
        self.assertEqual(int(postgres_progress["latest_metrics"]["candidate_count"] or 0), 7)

    def test_registry_canonicalization_replaces_live_postgres_table_from_sqlite(self) -> None:
        store = self._build_store(mode="mirror")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        store.upsert_organization_asset_registry(
            {
                "target_company": "Acme",
                "company_key": "acme",
                "snapshot_id": "20260419T120000",
                "asset_view": "canonical_merged",
                "candidate_count": 5,
            },
            authoritative=True,
        )
        store.upsert_organization_asset_registry(
            {
                "target_company": "Acme AI",
                "company_key": "acme",
                "snapshot_id": "20260419T120000",
                "asset_view": "canonical_merged",
                "candidate_count": 3,
            },
            authoritative=False,
        )

        result = store.canonicalize_organization_asset_registry_target_company(
            target_company="Acme",
            company_key="acme",
            asset_view="canonical_merged",
        )

        self.assertGreaterEqual(int(result["updated_rows"] or 0), 0)
        self.assertIn("organization_asset_registry", adapter.replaced_tables)

    def test_registry_canonicalization_writes_natively_when_postgres_prefers_reads(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        store.upsert_organization_asset_registry(
            {
                "target_company": "Acme Alias",
                "company_key": "acme",
                "snapshot_id": "20260419T120000",
                "asset_view": "canonical_merged",
                "candidate_count": 3,
            },
            authoritative=True,
        )
        store.upsert_organization_asset_registry(
            {
                "target_company": "Acme",
                "company_key": "acme",
                "snapshot_id": "20260419T120000",
                "asset_view": "canonical_merged",
                "candidate_count": 5,
            },
            authoritative=False,
        )

        result = store.canonicalize_organization_asset_registry_target_company(
            target_company="Acme",
            company_key="acme",
            asset_view="canonical_merged",
        )

        self.assertEqual(int(result["deleted_rows"] or 0), 0)
        self.assertGreaterEqual(int(result["updated_rows"] or 0), 0)
        self.assertNotIn("organization_asset_registry", adapter.replaced_tables)
        remaining_rows = list(adapter.generic_rows.get("organization_asset_registry", []))
        self.assertEqual(len(remaining_rows), 1)
        self.assertEqual(str(remaining_rows[0]["target_company"] or ""), "Acme")
        self.assertTrue(bool(remaining_rows[0]["authoritative"]))
        sqlite_registry_count = store._connection.execute(
            "SELECT COUNT(*) FROM organization_asset_registry WHERE lower(target_company) = lower(?)",
            ("Acme",),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_registry_count or 0), 0)

    def test_default_live_mode_uses_postgres_only_when_dsn_is_present(self) -> None:
        store = self._build_store(mode=None)
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        self.assertEqual(adapter.mode, "postgres_only")

    def test_postgres_only_uses_ephemeral_sqlite_shadow_and_disables_legacy_fallbacks(self) -> None:
        store = self._build_store(mode="postgres_only")

        self.assertEqual(store.sqlite_shadow_backend(), "shared_memory")
        self.assertTrue(store.sqlite_shadow_is_ephemeral())
        self.assertTrue(store.sqlite_shadow_connect_target().startswith("file:sourcing-agent-shadow-"))
        self.assertFalse(self.db_path.exists())
        self.assertFalse(store.bootstrap_candidate_store_enabled())
        self.assertFalse(store.candidate_documents_fallback_enabled())

    def test_require_control_plane_postgres_rejects_missing_dsn(self) -> None:
        with mock.patch("sourcing_agent.storage.LiveControlPlanePostgresAdapter", _FakeLiveControlPlanePostgresAdapter):
            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                },
                clear=False,
            ):
                with self.assertRaisesRegex(RuntimeError, "no control-plane Postgres DSN was resolved"):
                    SQLiteStore(self.db_path)

    def test_production_runtime_requires_postgres_without_explicit_emergency_override(self) -> None:
        with mock.patch("sourcing_agent.storage.LiveControlPlanePostgresAdapter", _FakeLiveControlPlanePostgresAdapter):
            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "production",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "0",
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "",
                    "SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE": "",
                },
                clear=False,
            ):
                with self.assertRaisesRegex(RuntimeError, "no control-plane Postgres DSN was resolved"):
                    SQLiteStore(self.db_path)

    def test_production_sqlite_control_plane_requires_explicit_emergency_override(self) -> None:
        with mock.patch("sourcing_agent.storage.LiveControlPlanePostgresAdapter", _FakeLiveControlPlanePostgresAdapter):
            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "production",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "0",
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "",
                    "SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE": "1",
                },
                clear=False,
            ):
                store = SQLiteStore(self.db_path)

        self.assertEqual(store.control_plane_postgres_live_mode(), "disabled")
        self.assertEqual(store.sqlite_shadow_backend(), "disk")

    def test_require_control_plane_postgres_rejects_disk_backed_shadow(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "refuses disk-backed SQLite shadow storage"):
            self._build_store(
                mode="postgres_only",
                extra_env={
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "disk",
                },
            )

    def test_postgres_only_bootstrap_source_returns_empty_without_touching_store_rows(self) -> None:
        store = self._build_store(mode="postgres_only")
        store.list_candidates = mock.Mock(side_effect=AssertionError("bootstrap list should stay disabled"))  # type: ignore[method-assign]

        payload = load_bootstrap_candidate_source(
            store=store,
            request=JobRequest.from_payload(
                {
                    "raw_user_request": "帮我找 Acme 的推理人才",
                    "target_company": "Acme",
                }
            ),
        )

        self.assertEqual(payload["source_kind"], "empty_source")
        self.assertEqual(payload["reason"], "authoritative_snapshot_missing_bootstrap_disabled")
        self.assertEqual(payload["candidates"], [])
        store.list_candidates.assert_not_called()

    def test_postgres_only_profile_registry_stays_postgres_authoritative(self) -> None:
        store = self._build_store(mode="postgres_only")

        canonical_url = "https://www.linkedin.com/in/alice-example/"
        alias_url = "linkedin.com/in/ALICE-EXAMPLE"
        store.mark_linkedin_profile_registry_fetched(
            canonical_url,
            raw_path="/tmp/alice-example.json",
            alias_urls=[alias_url],
            raw_linkedin_url=alias_url,
        )
        by_alias = store.get_linkedin_profile_registry(alias_url) or {}

        self.assertEqual(by_alias["status"], "fetched")
        self.assertEqual(by_alias["last_raw_path"], "/tmp/alice-example.json")
        self.assertEqual(
            by_alias["profile_url_key"],
            store.normalize_linkedin_profile_url(canonical_url),
        )
        sqlite_registry_count = store._connection.execute(
            "SELECT COUNT(*) FROM linkedin_profile_registry",
        ).fetchone()[0]
        sqlite_alias_count = store._connection.execute(
            "SELECT COUNT(*) FROM linkedin_profile_registry_aliases",
        ).fetchone()[0]
        self.assertEqual(int(sqlite_registry_count or 0), 0)
        self.assertEqual(int(sqlite_alias_count or 0), 0)

    def test_postgres_only_profile_registry_leases_events_and_backfill_runs(self) -> None:
        store = self._build_store(mode="postgres_only")
        profile_url = "https://www.linkedin.com/in/lease-test/"

        lease = store.acquire_linkedin_profile_registry_lease(profile_url, lease_owner="worker-a", lease_seconds=120)
        self.assertTrue(bool(lease.get("acquired")))
        self.assertEqual(str(lease.get("lease_owner") or ""), "worker-a")
        released = store.release_linkedin_profile_registry_lease(profile_url, lease_owner="worker-a")
        self.assertTrue(released)

        store.record_linkedin_profile_registry_event(profile_url, event_type="lookup_attempt")
        metrics = store.get_linkedin_profile_registry_metrics(lookback_hours=0)
        self.assertGreaterEqual(int(metrics["event_count"] or 0), 1)

        run = store.upsert_linkedin_profile_registry_backfill_run(
            "run::pg-only::snapshot",
            scope_company="acme",
            scope_snapshot_id="20260421T000000",
            checkpoint={"cursor": 3},
            summary={"processed_total": 9},
            status="running",
        )
        self.assertEqual(str(dict(run or {}).get("scope_company") or ""), "acme")
        sqlite_lease_count = store._connection.execute(
            "SELECT COUNT(*) FROM linkedin_profile_registry_leases",
        ).fetchone()[0]
        sqlite_event_count = store._connection.execute(
            "SELECT COUNT(*) FROM linkedin_profile_registry_events",
        ).fetchone()[0]
        sqlite_backfill_count = store._connection.execute(
            "SELECT COUNT(*) FROM linkedin_profile_registry_backfill_runs",
        ).fetchone()[0]
        self.assertEqual(int(sqlite_lease_count or 0), 0)
        self.assertEqual(int(sqlite_event_count or 0), 0)
        self.assertEqual(int(sqlite_backfill_count or 0), 0)

    def test_prefer_postgres_profile_registry_backfill_batch_uses_bulk_upserts(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        processed = store.backfill_linkedin_profile_registry_batch(
            [
                {
                    "profile_url": "linkedin.com/in/ALICE-EXAMPLE",
                    "status": "failed_retryable",
                    "error": "temporary_backfill_error",
                    "retryable": True,
                    "raw_path": "/tmp/alice-retry.json",
                    "source_shards": ["backfill:a"],
                    "source_jobs": ["run-a"],
                    "alias_urls": ["https://www.linkedin.com/in/alice-example/"],
                    "raw_linkedin_url": "linkedin.com/in/ALICE-EXAMPLE",
                    "sanity_linkedin_url": "https://www.linkedin.com/in/alice-example/",
                    "snapshot_dir": "/tmp/snapshot-a",
                },
                {
                    "profile_url": "https://www.linkedin.com/in/alice-example/",
                    "status": "fetched",
                    "error": "",
                    "retryable": False,
                    "raw_path": "/tmp/alice-fetched.json",
                    "source_shards": ["backfill:b"],
                    "source_jobs": ["run-b"],
                    "alias_urls": ["linkedin.com/in/ALICE-EXAMPLE"],
                    "raw_linkedin_url": "linkedin.com/in/ALICE-EXAMPLE",
                    "sanity_linkedin_url": "https://www.linkedin.com/in/alice-example/",
                    "snapshot_dir": "/tmp/snapshot-b",
                },
            ]
        )

        self.assertEqual(processed, 2)
        registry_bulk_upserts = [
            rows for table_name, rows in adapter.bulk_upserts if table_name == "linkedin_profile_registry"
        ]
        alias_bulk_upserts = [
            rows for table_name, rows in adapter.bulk_upserts if table_name == "linkedin_profile_registry_aliases"
        ]
        self.assertEqual(len(registry_bulk_upserts), 1)
        self.assertEqual(len(alias_bulk_upserts), 1)

        by_alias = store.get_linkedin_profile_registry("linkedin.com/in/ALICE-EXAMPLE")
        self.assertIsNotNone(by_alias)
        assert by_alias is not None
        self.assertEqual(by_alias["status"], "fetched")
        self.assertIn("backfill:a", by_alias["source_shards"])
        self.assertIn("backfill:b", by_alias["source_shards"])
        alias_keys = {
            store.normalize_linkedin_profile_url(item)
            for item in list(by_alias.get("alias_urls") or [])
            if str(item or "").strip()
        }
        self.assertIn(store.normalize_linkedin_profile_url("linkedin.com/in/ALICE-EXAMPLE"), alias_keys)

    def test_postgres_only_skips_sqlite_fallback_for_control_plane_and_materialization_reads(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        sqlite_request_payload = {
            "raw_user_request": "找 SQLite Only 的推理人才",
            "target_company": "SQLite Only",
            "title_keywords": ["reasoning"],
        }
        sqlite_request_signature = "req-sqlite"
        sqlite_request_family_signature = "reqfam-sqlite"
        store._connection.execute(
            """
            INSERT INTO job_result_views (
                view_id, job_id, target_company, company_key, source_kind, view_kind, snapshot_id,
                asset_view, source_path, authoritative_snapshot_id, materialization_generation_key,
                request_signature, summary_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sqlite-view-only",
                "sqlite-job-only",
                "SQLite Only",
                "sqliteonly",
                "artifact_registry",
                "latest_results",
                "snap-sqlite",
                "canonical_merged",
                "/tmp/sqlite-only.json",
                "snap-sqlite",
                "gen-sqlite",
                "req-sqlite",
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
            ),
        )
        store._connection.execute(
            """
            INSERT INTO jobs (
                job_id, job_type, status, stage, request_json, plan_json, execution_bundle_json,
                matching_request_json, summary_json, request_signature, request_family_signature,
                matching_request_signature, matching_request_family_signature, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sqlite-job-completed",
                "workflow",
                "completed",
                "completed",
                json.dumps(sqlite_request_payload, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
                sqlite_request_signature,
                sqlite_request_family_signature,
                sqlite_request_signature,
                sqlite_request_family_signature,
                "2026-04-18T00:00:00+00:00",
            ),
        )
        store._connection.execute(
            """
            INSERT INTO jobs (
                job_id, job_type, status, stage, request_json, plan_json, execution_bundle_json,
                matching_request_json, summary_json, request_signature, request_family_signature,
                matching_request_signature, matching_request_family_signature, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sqlite-job-queued",
                "workflow",
                "queued",
                "planning",
                json.dumps(sqlite_request_payload, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
                sqlite_request_signature,
                sqlite_request_family_signature,
                sqlite_request_signature,
                sqlite_request_family_signature,
                "2026-04-18T00:00:00+00:00",
            ),
        )
        store._connection.execute(
            """
            INSERT INTO jobs (
                job_id, job_type, status, stage, request_json, plan_json, execution_bundle_json,
                matching_request_json, summary_json, request_signature, request_family_signature,
                matching_request_signature, matching_request_family_signature, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sqlite-job-acquiring",
                "workflow",
                "running",
                "acquiring",
                json.dumps(sqlite_request_payload, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
                sqlite_request_signature,
                sqlite_request_family_signature,
                sqlite_request_signature,
                sqlite_request_family_signature,
                "2026-04-18T00:00:00+00:00",
            ),
        )
        store._connection.execute(
            """
            INSERT INTO candidate_materialization_state (
                target_company, company_key, snapshot_id, asset_view, candidate_id,
                fingerprint, shard_path, list_page, dirty_reason, materialized_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SQLite Only",
                "sqliteonly",
                "snap-sqlite",
                "canonical_merged",
                "cand-sqlite",
                "fp-sqlite",
                "candidate_shards/cand-sqlite.json",
                1,
                "",
                "2026-04-19T12:00:00Z",
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
            ),
        )
        store._connection.execute(
            """
            INSERT INTO confidence_policy_controls (
                target_company, request_signature, request_family_signature,
                matching_request_signature, matching_request_family_signature,
                scope_kind, control_mode, status, high_threshold, medium_threshold,
                reviewer, notes, locked_policy_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SQLite Only",
                sqlite_request_signature,
                sqlite_request_family_signature,
                sqlite_request_signature,
                sqlite_request_family_signature,
                "request_family",
                "override",
                "active",
                0.9,
                0.6,
                "sqlite-reviewer",
                "sqlite-only",
                json.dumps({"source": "sqlite_only"}, ensure_ascii=False),
            ),
        )
        store._connection.commit()

        self.assertIsNone(store.get_job_result_view(job_id="sqlite-job-only"))
        self.assertIsNone(store.find_latest_completed_job(target_company="SQLite Only"))
        self.assertIsNone(
            store.find_latest_job_by_request_signature(
                request_signature_value=sqlite_request_signature,
                target_company="SQLite Only",
                limit=10,
            )
        )
        self.assertIsNone(
            store.find_latest_job_by_request_family_signature(
                request_family_signature_value=sqlite_request_family_signature,
                target_company="SQLite Only",
                limit=10,
            )
        )
        self.assertEqual(
            store.list_jobs_by_request_signature(
                request_signature_value=sqlite_request_signature,
                target_company="SQLite Only",
                limit=10,
            ),
            [],
        )
        self.assertEqual(store.list_stale_workflow_jobs_in_queue(stale_after_seconds=0, limit=10), [])
        self.assertEqual(store.list_stale_workflow_jobs_in_acquiring(stale_after_seconds=0, limit=10), [])
        self.assertIsNone(store.get_confidence_policy_control(1))
        self.assertEqual(store.list_confidence_policy_controls(target_company="SQLite Only"), [])
        self.assertEqual(
            store.get_candidate_materialization_state(
                target_company="SQLite Only",
                snapshot_id="snap-sqlite",
                asset_view="canonical_merged",
                candidate_id="cand-sqlite",
            ),
            {},
        )
        self.assertEqual(
            store.list_candidate_materialization_states(
                target_company="SQLite Only",
                snapshot_id="snap-sqlite",
                asset_view="canonical_merged",
            ),
            [],
        )

        pg_view = store.upsert_job_result_view(
            job_id="postgres-job-only",
            target_company="Postgres Only",
            source_kind="artifact_registry",
            view_kind="latest_results",
            snapshot_id="snap-postgres",
            asset_view="canonical_merged",
            source_path="/tmp/postgres-only.json",
            authoritative_snapshot_id="snap-postgres",
            materialization_generation_key="gen-postgres",
            request_signature_value="req-postgres",
            summary={"source": "postgres_only"},
            metadata={"source": "postgres_only"},
        )
        pg_state = store.upsert_candidate_materialization_state(
            target_company="Postgres Only",
            snapshot_id="snap-postgres",
            asset_view="canonical_merged",
            candidate_id="cand-postgres",
            fingerprint="fp-postgres",
            metadata={"source": "postgres_only"},
        )
        store.save_job(
            job_id="postgres-job-completed",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "找 Postgres Only 的推理人才",
                "target_company": "Postgres Only",
                "title_keywords": ["reasoning"],
            },
            plan_payload={},
            execution_bundle_payload={},
            summary_payload={"source": "postgres_only"},
        )
        pg_completed_job = store.get_job("postgres-job-completed") or {}
        pg_control = store.create_confidence_policy_control(
            target_company="Postgres Only",
            request_payload={
                "raw_user_request": "找 Postgres Only 的推理人才",
                "target_company": "Postgres Only",
                "title_keywords": ["reasoning"],
            },
            scope_kind="request_family",
            control_mode="override",
            high_threshold=0.93,
            medium_threshold=0.7,
            reviewer="postgres-reviewer",
            notes="postgres-only",
        )

        self.assertEqual(pg_view["job_id"], "postgres-job-only")
        self.assertEqual(
            store.get_job_result_view(job_id="postgres-job-only")["metadata"]["source"],
            "postgres_only",
        )
        self.assertEqual(pg_state["candidate_id"], "cand-postgres")
        self.assertEqual(
            store.get_candidate_materialization_state(
                target_company="Postgres Only",
                snapshot_id="snap-postgres",
                asset_view="canonical_merged",
                candidate_id="cand-postgres",
            )["metadata"]["source"],
            "postgres_only",
        )
        self.assertEqual(
            store.find_latest_completed_job(target_company="Postgres Only")["job_id"],
            "postgres-job-completed",
        )
        self.assertEqual(
            store.find_latest_job_by_request_signature(
                request_signature_value=str(pg_completed_job.get("matching_request_signature") or ""),
                target_company="Postgres Only",
                limit=10,
            )["job_id"],
            "postgres-job-completed",
        )
        self.assertEqual(
            store.find_latest_job_by_request_family_signature(
                request_family_signature_value=str(pg_completed_job.get("matching_request_family_signature") or ""),
                target_company="Postgres Only",
                limit=10,
            )["job_id"],
            "postgres-job-completed",
        )
        self.assertEqual(
            [
                row["job_id"]
                for row in store.list_jobs_by_request_signature(
                    request_signature_value=str(pg_completed_job.get("matching_request_signature") or ""),
                    target_company="Postgres Only",
                    limit=10,
                )
            ],
            ["postgres-job-completed"],
        )
        store.save_job(
            job_id="postgres-job-queued",
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={
                "raw_user_request": "找 Postgres Only 的平台人才",
                "target_company": "Postgres Only",
                "title_keywords": ["platform"],
            },
            plan_payload={},
            execution_bundle_payload={},
            summary_payload={"source": "postgres_only"},
        )
        pg_queued_job = store.get_job("postgres-job-queued") or {}
        store.save_job(
            job_id="postgres-job-acquiring",
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={
                "raw_user_request": "找 Postgres Only 的研究人才",
                "target_company": "Postgres Only",
                "title_keywords": ["research"],
            },
            plan_payload={},
            execution_bundle_payload={},
            summary_payload={"source": "postgres_only"},
        )
        pg_acquiring_job = store.get_job("postgres-job-acquiring") or {}
        adapter.select_many_rows["jobs"] = [
            row
            for row in adapter.native_rows["jobs"]
            if str(row.get("job_id") or "") == str(pg_queued_job.get("job_id") or "")
        ]
        self.assertEqual(
            [row["job_id"] for row in store.list_stale_workflow_jobs_in_queue(stale_after_seconds=0, limit=10)],
            [str(pg_queued_job["job_id"])],
        )
        adapter.select_many_rows["jobs"] = [
            row
            for row in adapter.native_rows["jobs"]
            if str(row.get("job_id") or "") == str(pg_acquiring_job.get("job_id") or "")
        ]
        self.assertEqual(
            [row["job_id"] for row in store.list_stale_workflow_jobs_in_acquiring(stale_after_seconds=0, limit=10)],
            [str(pg_acquiring_job["job_id"])],
        )
        self.assertEqual(int(pg_control["control_id"] or 0), 1)
        self.assertEqual(
            store.get_confidence_policy_control(int(pg_control["control_id"]))["notes"],
            "postgres-only",
        )
        self.assertEqual(
            [row["notes"] for row in store.list_confidence_policy_controls(target_company="Postgres Only")],
            ["postgres-only"],
        )

    def test_postgres_only_raises_instead_of_falling_back_when_native_job_writer_fails(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        adapter.save_job_row = mock.Mock(side_effect=RuntimeError("postgres unavailable"))  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "Postgres authoritative write failed for jobs"):
            store.save_job(
                "job-strict-fail",
                "workflow",
                "running",
                "acquiring",
                {"target_company": "Acme Strict"},
                plan_payload={"mode": "strict"},
            )

        sqlite_job_count = store._connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_id = ?",
            ("job-strict-fail",),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_job_count or 0), 0)

    def test_postgres_only_raises_instead_of_falling_back_when_generic_upsert_fails(self) -> None:
        store = self._build_store(mode="postgres_only")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)
        adapter.upsert_row = mock.Mock(side_effect=RuntimeError("postgres unavailable"))  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "Postgres authoritative write failed for job_result_views"):
            store.upsert_job_result_view(
                job_id="job-strict-view",
                target_company="Acme Strict",
                source_kind="artifact_registry",
                view_kind="latest_results",
                snapshot_id="snap-strict",
            )

        sqlite_view_count = store._connection.execute(
            "SELECT COUNT(*) FROM job_result_views WHERE job_id = ?",
            ("job-strict-view",),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_view_count or 0), 0)

    def test_prefer_postgres_uses_native_writers_for_jobs_events_and_runtime_sessions(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        store.save_job(
            "job-native",
            "workflow",
            "running",
            "acquiring",
            {"target_company": "Acme"},
            plan_payload={"mode": "postgres_native"},
        )
        session = store.create_agent_runtime_session(
            job_id="job-native",
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            plan_payload={"mode": "postgres_native"},
            runtime_mode="agent_runtime",
            lanes=[{"lane_id": "research"}],
        )
        store.append_job_event(
            "job-native",
            "acquiring",
            "running",
            "postgres event",
            {"candidate_count": 5},
        )
        updated_session = store.update_agent_runtime_session_status("job-native", "completed")

        stored_job = store.get_job("job-native")
        assert stored_job is not None
        self.assertEqual(str(stored_job["plan"]["mode"] or ""), "postgres_native")
        stored_summary = store.get_job_progress_event_summary("job-native")
        self.assertEqual(int(stored_summary["event_count"] or 0), 1)
        self.assertEqual(int(stored_summary["latest_metrics"]["candidate_count"] or 0), 5)
        stored_events = store.list_job_events("job-native")
        self.assertEqual(len(stored_events), 1)
        self.assertEqual(str(stored_events[0]["detail"] or ""), "postgres event")
        assert updated_session is not None
        self.assertEqual(str(updated_session["status"] or ""), "completed")
        self.assertEqual(int(session["session_id"] or 0), int(updated_session["session_id"] or 0))

        sqlite_job_count = store._connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_id = ?", ("job-native",)
        ).fetchone()[0]
        sqlite_event_count = store._connection.execute(
            "SELECT COUNT(*) FROM job_events WHERE job_id = ?",
            ("job-native",),
        ).fetchone()[0]
        sqlite_session_count = store._connection.execute(
            "SELECT COUNT(*) FROM agent_runtime_sessions WHERE job_id = ?",
            ("job-native",),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_job_count or 0), 0)
        self.assertEqual(int(sqlite_event_count or 0), 0)
        self.assertEqual(int(sqlite_session_count or 0), 0)
        self.assertFalse(any(table_name == "jobs" for table_name, _ in adapter.upserts))
        self.assertFalse(any(table_name == "job_events" for table_name, _ in adapter.upserts))
        self.assertFalse(any(table_name == "agent_runtime_sessions" for table_name, _ in adapter.upserts))

    def test_prefer_postgres_uses_native_writers_for_frontend_state_tables(self) -> None:
        store = self._build_store(mode="prefer_postgres")

        review = store.upsert_candidate_review_record(
            {
                "record_id": "review-native-1",
                "job_id": "job-native-state",
                "history_id": "hist-native-1",
                "candidate_id": "cand-native-1",
                "candidate_name": "Alice Native",
                "status": "approved",
                "metadata": {"source": "postgres_native"},
            }
        )
        target = store.upsert_target_candidate(
            {
                "record_id": "target-native-1",
                "job_id": "job-native-state",
                "history_id": "hist-native-1",
                "candidate_id": "cand-native-1",
                "candidate_name": "Alice Native",
                "follow_up_status": "queued",
                "metadata": {"source": "postgres_native"},
            }
        )
        history = store.upsert_frontend_history_link(
            {
                "history_id": "hist-native-1",
                "query_text": "find alice native",
                "target_company": "Acme Native",
                "job_id": "job-native-state",
                "phase": "results",
                "metadata": {"source": "postgres_native"},
            }
        )
        view = store.upsert_job_result_view(
            job_id="job-native-state",
            target_company="Acme Native",
            source_kind="artifact_registry",
            view_kind="latest_results",
            snapshot_id="20260419T120000",
            metadata={"source": "postgres_native"},
        )

        self.assertEqual(review["metadata"]["source"], "postgres_native")
        self.assertEqual(target["metadata"]["source"], "postgres_native")
        self.assertEqual(history["metadata"]["source"], "postgres_native")
        self.assertEqual(view["metadata"]["source"], "postgres_native")

        sqlite_review_count = store._connection.execute(
            "SELECT COUNT(*) FROM candidate_review_registry WHERE record_id = ?",
            ("review-native-1",),
        ).fetchone()[0]
        sqlite_target_count = store._connection.execute(
            "SELECT COUNT(*) FROM target_candidates WHERE record_id = ?",
            ("target-native-1",),
        ).fetchone()[0]
        sqlite_history_count = store._connection.execute(
            "SELECT COUNT(*) FROM frontend_history_links WHERE history_id = ?",
            ("hist-native-1",),
        ).fetchone()[0]
        sqlite_view_count = store._connection.execute(
            "SELECT COUNT(*) FROM job_result_views WHERE job_id = ?",
            ("job-native-state",),
        ).fetchone()[0]

        self.assertEqual(int(sqlite_review_count or 0), 0)
        self.assertEqual(int(sqlite_target_count or 0), 0)
        self.assertEqual(int(sqlite_history_count or 0), 0)
        self.assertEqual(int(sqlite_view_count or 0), 0)

    def test_job_events_ui_tables_and_runtime_sessions_mirror_and_prefer_postgres(self) -> None:
        mirror_store = self._build_store(mode="mirror")
        mirror_adapter = mirror_store._control_plane_postgres
        assert isinstance(mirror_adapter, _FakeLiveControlPlanePostgresAdapter)

        review = mirror_store.upsert_candidate_review_record(
            {
                "record_id": "review-1",
                "job_id": "job-1",
                "history_id": "hist-1",
                "candidate_id": "cand-1",
                "candidate_name": "Alice",
                "status": "approved",
            }
        )
        target = mirror_store.upsert_target_candidate(
            {
                "record_id": "target-1",
                "job_id": "job-1",
                "history_id": "hist-1",
                "candidate_id": "cand-1",
                "candidate_name": "Alice",
                "follow_up_status": "queued",
            }
        )
        history = mirror_store.upsert_frontend_history_link(
            {
                "history_id": "hist-1",
                "job_id": "job-1",
                "query_text": "find alice",
                "target_company": "Acme",
                "phase": "results",
            }
        )
        session = mirror_store.create_agent_runtime_session(
            job_id="job-1",
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            plan_payload={"mode": "agent_runtime"},
            runtime_mode="agent_runtime",
            lanes=[{"lane_id": "research"}],
        )
        manual_reviews = mirror_store.replace_manual_review_items(
            "job-1",
            [
                {
                    "candidate_id": "cand-1",
                    "target_company": "Acme",
                    "review_type": "profile_gap",
                    "summary": "Need more evidence",
                    "candidate": {"candidate_id": "cand-1"},
                    "evidence": [],
                    "metadata": {"history_id": "hist-1"},
                }
            ],
        )
        dispatch = mirror_store.record_query_dispatch(
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            strategy="reuse_or_queue",
            status="queued",
            created_job_id="job-1",
        )
        control = mirror_store.create_confidence_policy_control(
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            scope_kind="company",
            control_mode="override",
            high_threshold=0.85,
            medium_threshold=0.55,
            reviewer="tester",
        )
        mirror_store.update_agent_runtime_session_status("job-1", "completed")
        for index in range(13):
            mirror_store.append_job_event(
                "job-1",
                "runtime_heartbeat",
                "running",
                f"heartbeat {index}",
                {"source": "worker-1", "tick": index},
            )

        mirrored_tables = [table_name for table_name, _ in mirror_adapter.upserts]
        self.assertIn("candidate_review_registry", mirrored_tables)
        self.assertIn("target_candidates", mirrored_tables)
        self.assertIn("frontend_history_links", mirrored_tables)
        self.assertIn("agent_runtime_sessions", mirrored_tables)
        self.assertIn("job_events", mirrored_tables)
        self.assertIn("query_dispatches", mirrored_tables)
        self.assertIn("job_events", mirror_adapter.replaced_tables)
        self.assertIn("manual_review_items", mirror_adapter.replaced_tables)
        self.assertIn("confidence_policy_controls", mirror_adapter.replaced_tables)
        self.assertEqual(review["job_id"], "job-1")
        self.assertEqual(target["job_id"], "job-1")
        self.assertEqual(history["job_id"], "job-1")
        self.assertEqual(session["job_id"], "job-1")
        self.assertEqual(manual_reviews[0]["job_id"], "job-1")
        self.assertEqual(dispatch["created_job_id"], "job-1")
        self.assertEqual(control["target_company"], "Acme")

        prefer_store = self._build_store(mode="prefer_postgres")
        prefer_adapter = prefer_store._control_plane_postgres
        assert isinstance(prefer_adapter, _FakeLiveControlPlanePostgresAdapter)
        prefer_adapter.select_many_rows["manual_review_items"] = [
            {
                "review_item_id": 5,
                "job_id": "job-pg",
                "candidate_id": "cand-pg",
                "target_company": "Acme PG",
                "review_type": "profile_gap",
                "priority": "medium",
                "status": "open",
                "summary": "Need more evidence",
                "candidate_json": json.dumps({"candidate_id": "cand-pg"}, ensure_ascii=False),
                "evidence_json": json.dumps([], ensure_ascii=False),
                "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
                "reviewed_by": "",
                "review_notes": "",
                "reviewed_at": "",
                "created_at": "2026-04-19T12:00:00Z",
                "updated_at": "2026-04-19T12:00:00Z",
            }
        ]
        prefer_adapter.select_one_rows["manual_review_items"] = prefer_adapter.select_many_rows["manual_review_items"][
            0
        ]
        prefer_adapter.select_many_rows["candidate_review_registry"] = [
            {
                "record_id": "review-pg",
                "job_id": "job-pg",
                "history_id": "hist-pg",
                "candidate_id": "cand-pg",
                "candidate_name": "Alice PG",
                "headline": "Researcher",
                "current_company": "Acme PG",
                "avatar_url": "",
                "linkedin_url": "",
                "primary_email": "",
                "status": "approved",
                "comment": "",
                "source": "manual_review",
                "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
                "added_at": "2026-04-19T12:00:00Z",
                "updated_at": "2026-04-19T12:00:00Z",
            }
        ]
        prefer_adapter.select_one_rows["candidate_review_registry"] = prefer_adapter.select_many_rows[
            "candidate_review_registry"
        ][0]
        prefer_adapter.select_many_rows["target_candidates"] = [
            {
                "record_id": "target-pg",
                "candidate_id": "cand-pg",
                "history_id": "hist-pg",
                "job_id": "job-pg",
                "candidate_name": "Alice PG",
                "headline": "Researcher",
                "current_company": "Acme PG",
                "avatar_url": "",
                "linkedin_url": "",
                "primary_email": "",
                "follow_up_status": "queued",
                "quality_score": 0.9,
                "comment": "",
                "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
                "added_at": "2026-04-19T12:00:00Z",
                "updated_at": "2026-04-19T12:00:00Z",
            }
        ]
        prefer_adapter.select_one_rows["target_candidates"] = prefer_adapter.select_many_rows["target_candidates"][0]
        prefer_adapter.select_one_rows["frontend_history_links"] = {
            "history_id": "hist-pg",
            "query_text": "find alice pg",
            "target_company": "Acme PG",
            "review_id": 0,
            "job_id": "job-pg",
            "phase": "results",
            "request_json": json.dumps({"target_company": "Acme PG"}, ensure_ascii=False),
            "plan_json": json.dumps({"mode": "postgres"}, ensure_ascii=False),
            "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
            "updated_at": "2026-04-19T12:00:00Z",
        }
        prefer_adapter.select_one_rows["agent_runtime_sessions"] = {
            "session_id": 7,
            "job_id": "job-pg",
            "target_company": "Acme PG",
            "request_signature": "req-1",
            "request_family_signature": "fam-1",
            "runtime_mode": "agent_runtime",
            "status": "completed",
            "lanes_json": json.dumps([{"lane_id": "research"}], ensure_ascii=False),
            "metadata_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
            "updated_at": "2026-04-19T12:00:00Z",
        }
        prefer_adapter.select_one_rows["query_dispatches"] = {
            "dispatch_id": 9,
            "target_company": "Acme PG",
            "request_signature": "req-1",
            "request_family_signature": "fam-1",
            "matching_request_signature": "match-1",
            "matching_request_family_signature": "match-fam-1",
            "requester_id": "",
            "tenant_id": "",
            "idempotency_key": "",
            "strategy": "reuse_or_queue",
            "status": "queued",
            "source_job_id": "",
            "created_job_id": "job-pg",
            "matching_request_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
            "payload_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
            "updated_at": "2026-04-19T12:00:00Z",
        }
        prefer_adapter.select_many_rows["query_dispatches"] = [prefer_adapter.select_one_rows["query_dispatches"]]
        prefer_adapter.select_one_rows["confidence_policy_controls"] = {
            "control_id": 11,
            "target_company": "Acme PG",
            "request_signature": "req-1",
            "request_family_signature": "fam-1",
            "matching_request_signature": "match-1",
            "matching_request_family_signature": "match-fam-1",
            "scope_kind": "company",
            "control_mode": "override",
            "status": "active",
            "high_threshold": 0.8,
            "medium_threshold": 0.5,
            "reviewer": "tester",
            "notes": "",
            "locked_policy_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
            "created_at": "2026-04-19T12:00:00Z",
            "updated_at": "2026-04-19T12:00:00Z",
        }
        prefer_adapter.select_many_rows["confidence_policy_controls"] = [
            prefer_adapter.select_one_rows["confidence_policy_controls"]
        ]
        prefer_adapter.select_many_rows["job_events"] = [
            {
                "event_id": 101,
                "job_id": "job-pg",
                "stage": "runtime_control",
                "status": "running",
                "detail": "controller updated",
                "payload_json": json.dumps({"source": "postgres"}, ensure_ascii=False),
                "created_at": "2026-04-19T12:00:00Z",
            }
        ]

        manual_review_rows = prefer_store.list_manual_review_items(job_id="job-pg", status="")
        review_rows = prefer_store.list_candidate_review_records(job_id="job-pg")
        target_rows = prefer_store.list_target_candidates(job_id="job-pg")
        history_row = prefer_store.get_frontend_history_link("hist-pg")
        session_row = prefer_store.get_agent_runtime_session(job_id="job-pg")
        dispatch_row = prefer_store.get_query_dispatch(9)
        control_row = prefer_store.get_confidence_policy_control(11)
        event_rows = prefer_store.list_job_events("job-pg", stage="runtime_control")

        self.assertEqual(manual_review_rows[0]["metadata"]["source"], "postgres")
        self.assertEqual(review_rows[0]["metadata"]["source"], "postgres")
        self.assertEqual(target_rows[0]["metadata"]["source"], "postgres")
        assert history_row is not None
        self.assertEqual(history_row["metadata"]["source"], "postgres")
        assert session_row is not None
        self.assertEqual(session_row["metadata"]["source"], "postgres")
        assert dispatch_row is not None
        self.assertEqual(dispatch_row["payload"]["source"], "postgres")
        assert control_row is not None
        self.assertEqual(control_row["locked_policy"]["source"], "postgres")
        self.assertEqual(event_rows[0]["payload"]["source"], "postgres")

    def test_remaining_control_plane_tables_write_natively_in_prefer_postgres(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        review = store.create_plan_review_session(
            target_company="Acme Native",
            request_payload={"target_company": "Acme Native"},
            plan_payload={"mode": "review"},
            gate_payload={"required_before_execution": True, "risk_level": "high"},
            execution_bundle_payload={"source": "postgres_native"},
        )
        reviewed = store.review_plan_session(
            review_id=int(review["review_id"]),
            status="approved",
            reviewer="qa",
            notes="looks good",
            decision_payload={"approved": True},
        )

        manual_items = store.replace_manual_review_items(
            "job-native",
            [
                {
                    "candidate_id": "cand-native",
                    "target_company": "Acme Native",
                    "review_type": "profile_gap",
                    "summary": "Need confirmation",
                    "candidate": {"candidate_id": "cand-native"},
                    "evidence": [],
                    "metadata": {"source": "postgres_native"},
                }
            ],
        )
        reviewed_item = store.review_manual_review_item(
            review_item_id=int(manual_items[0]["review_item_id"]),
            action="resolve",
            reviewer="qa",
            notes="handled",
        )

        dispatch = store.record_query_dispatch(
            target_company="Acme Native",
            request_payload={"target_company": "Acme Native"},
            strategy="reuse_or_queue",
            status="queued",
            created_job_id="job-native",
            payload={"source": "postgres_native"},
        )

        control = store.create_confidence_policy_control(
            target_company="Acme Native",
            request_payload={"target_company": "Acme Native"},
            scope_kind="request_family",
            control_mode="override",
            high_threshold=0.8,
            medium_threshold=0.5,
            reviewer="qa",
            locked_policy={"source": "postgres_native"},
        )
        deactivated = store.deactivate_confidence_policy_control(control_id=int(control["control_id"]))

        store.upsert_organization_asset_registry(
            {
                "target_company": "Acme Native",
                "company_key": "acme-native",
                "snapshot_id": "snap-1",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 10,
            },
            authoritative=True,
        )
        registry = store.upsert_organization_asset_registry(
            {
                "target_company": "Acme Native",
                "company_key": "acme-native",
                "snapshot_id": "snap-2",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 14,
                "summary": {"source": "postgres_native"},
            },
            authoritative=True,
        )
        authoritative_registry = store.get_authoritative_organization_asset_registry(target_company="Acme Native")

        profile = store.upsert_organization_execution_profile(
            {
                "target_company": "Acme Native",
                "company_key": "acme-native",
                "asset_view": "canonical_merged",
                "source_registry_id": int(registry["registry_id"]),
                "source_snapshot_id": "snap-2",
                "source_generation_key": "gen-native",
                "source_generation_sequence": 2,
                "status": "ready",
                "default_acquisition_mode": "full_company_roster",
                "reason_codes": ["postgres_native"],
                "summary": {"source": "postgres_native"},
            }
        )
        profile_lookup = store.get_organization_execution_profile(target_company="Acme Native")

        ledger = store.record_cloud_asset_operation(
            operation_type="publish_generation",
            bundle_kind="company_snapshot",
            bundle_id="bundle-native",
            status="completed",
            scoped_companies=["Acme Native"],
            summary={"source": "postgres_native"},
        )
        feedback = store.record_criteria_feedback(
            {
                "target_company": "Acme Native",
                "feedback_type": "must_have_signal",
                "subject": "infra",
                "value": "distributed systems",
                "reviewer": "qa",
                "metadata": {"source": "postgres_native"},
            }
        )
        suggestions = store.record_pattern_suggestions(
            [
                {
                    "target_company": "Acme Native",
                    "request_signature": "req-native",
                    "request_family_signature": "family-native",
                    "matching_request_signature": "matching-native",
                    "matching_request_family_signature": "matching-family-native",
                    "source_feedback_id": int(feedback["feedback_id"]),
                    "source_job_id": "job-native",
                    "candidate_id": "cand-native",
                    "pattern_type": "must_signal",
                    "subject": "infra",
                    "value": "distributed systems",
                    "status": "suggested",
                    "confidence": "high",
                    "rationale": "pg-native suggestion",
                    "metadata": {"source": "postgres_native"},
                }
            ]
        )
        reviewed_suggestion = store.review_pattern_suggestion(
            suggestion_id=int(suggestions[0]["suggestion_id"]),
            action="apply",
            reviewer="qa",
            notes="promote",
        )
        version = store.create_criteria_version(
            target_company="Acme Native",
            request_payload={"target_company": "Acme Native", "keywords": ["infra"]},
            plan_payload={"mode": "postgres_native"},
            patterns=[],
            source_kind="plan",
        )
        compiler_run = store.record_criteria_compiler_run(
            version_id=int(version["version_id"]),
            job_id="job-native",
            provider_name="deterministic",
            compiler_kind="feedback_recompile",
            status="completed",
            input_payload={"source": "postgres_native"},
            output_payload={"compiled": True},
        )
        diff = store.record_criteria_result_diff(
            target_company="Acme Native",
            trigger_feedback_id=int(feedback["feedback_id"]),
            criteria_version_id=int(version["version_id"]),
            baseline_job_id="job-native-baseline",
            rerun_job_id="job-native-rerun",
            summary_payload={"source": "postgres_native"},
            diff_payload={"added": ["cand-native"]},
            artifact_path="runtime/company_assets/acme-native/snap-2/diff.json",
        )
        policy_run = store.record_confidence_policy_run(
            target_company="Acme Native",
            job_id="job-native",
            criteria_version_id=int(version["version_id"]),
            trigger_feedback_id=int(feedback["feedback_id"]),
            policy_payload={
                "request_signature": "req-native",
                "request_family_signature": "family-native",
                "matching_request_signature": "matching-native",
                "matching_request_family_signature": "matching-family-native",
                "scope_kind": "request_family",
                "high_threshold": 0.75,
                "medium_threshold": 0.5,
                "summary": {"source": "postgres_native"},
            },
        )
        feedback_rows = store.list_criteria_feedback(target_company="Acme Native", limit=5)
        pattern_rows = store.list_criteria_patterns(target_company="Acme Native", status="", limit=10)
        suggestion_row = store.get_pattern_suggestion(int(suggestions[0]["suggestion_id"]))
        version_row = store.get_criteria_version(int(version["version_id"]))
        compiler_rows = store.list_criteria_compiler_runs(target_company="Acme Native", limit=5)
        diff_rows = store.list_criteria_result_diffs(target_company="Acme Native", limit=5)
        policy_rows = store.list_confidence_policy_runs(target_company="Acme Native", limit=5)

        self.assertEqual(reviewed["status"], "approved")
        self.assertEqual(reviewed["execution_bundle"]["source"], "postgres_native")
        self.assertEqual(reviewed_item["status"], "resolved")
        self.assertEqual(dispatch["payload"]["source"], "postgres_native")
        self.assertEqual(deactivated["deactivated_count"], 1)
        self.assertEqual(authoritative_registry["snapshot_id"], "snap-2")
        self.assertEqual(authoritative_registry["summary"]["source"], "postgres_native")
        self.assertEqual(profile["reason_codes"], ["postgres_native"])
        self.assertEqual(profile_lookup["source_snapshot_id"], "snap-2")
        self.assertEqual(ledger["bundle_id"], "bundle-native")
        self.assertEqual(feedback_rows[0]["metadata"]["source"], "postgres_native")
        self.assertTrue(any(item["pattern_type"] == "must_signal" for item in pattern_rows))
        assert suggestion_row is not None
        self.assertEqual(suggestion_row["status"], "applied")
        self.assertEqual(suggestion_row["metadata"]["source"], "postgres_native")
        assert reviewed_suggestion is not None
        self.assertEqual(reviewed_suggestion["status"], "applied")
        assert version_row is not None
        self.assertEqual(version_row["source_kind"], "plan")
        self.assertEqual(compiler_rows[0]["compiler_run_id"], int(compiler_run["compiler_run_id"]))
        self.assertEqual(diff_rows[0]["diff_id"], int(diff["diff_id"]))
        self.assertEqual(policy_rows[0]["policy_run_id"], int(policy_run["policy_run_id"]))
        self.assertEqual(policy_rows[0]["summary"]["source"], "postgres_native")

        sqlite_review_count = store._connection.execute(
            "SELECT COUNT(*) FROM plan_review_sessions WHERE review_id = ?",
            (int(review["review_id"]),),
        ).fetchone()[0]
        sqlite_manual_review_count = store._connection.execute(
            "SELECT COUNT(*) FROM manual_review_items WHERE job_id = ?",
            ("job-native",),
        ).fetchone()[0]
        sqlite_dispatch_count = store._connection.execute(
            "SELECT COUNT(*) FROM query_dispatches WHERE dispatch_id = ?",
            (int(dispatch["dispatch_id"]),),
        ).fetchone()[0]
        sqlite_control_count = store._connection.execute(
            "SELECT COUNT(*) FROM confidence_policy_controls WHERE control_id = ?",
            (int(control["control_id"]),),
        ).fetchone()[0]
        sqlite_registry_count = store._connection.execute(
            "SELECT COUNT(*) FROM organization_asset_registry WHERE lower(target_company) = lower(?)",
            ("Acme Native",),
        ).fetchone()[0]
        sqlite_profile_count = store._connection.execute(
            "SELECT COUNT(*) FROM organization_execution_profiles WHERE lower(target_company) = lower(?)",
            ("Acme Native",),
        ).fetchone()[0]
        sqlite_ledger_count = store._connection.execute(
            "SELECT COUNT(*) FROM cloud_asset_operation_ledger WHERE bundle_id = ?",
            ("bundle-native",),
        ).fetchone()[0]
        sqlite_feedback_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_feedback WHERE feedback_id = ?",
            (int(feedback["feedback_id"]),),
        ).fetchone()[0]
        sqlite_pattern_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_patterns WHERE lower(target_company) = lower(?)",
            ("Acme Native",),
        ).fetchone()[0]
        sqlite_suggestion_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_pattern_suggestions WHERE suggestion_id = ?",
            (int(suggestions[0]["suggestion_id"]),),
        ).fetchone()[0]
        sqlite_version_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_versions WHERE version_id = ?",
            (int(version["version_id"]),),
        ).fetchone()[0]
        sqlite_compiler_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_compiler_runs WHERE compiler_run_id = ?",
            (int(compiler_run["compiler_run_id"]),),
        ).fetchone()[0]
        sqlite_diff_count = store._connection.execute(
            "SELECT COUNT(*) FROM criteria_result_diffs WHERE diff_id = ?",
            (int(diff["diff_id"]),),
        ).fetchone()[0]
        sqlite_policy_run_count = store._connection.execute(
            "SELECT COUNT(*) FROM confidence_policy_runs WHERE policy_run_id = ?",
            (int(policy_run["policy_run_id"]),),
        ).fetchone()[0]

        self.assertEqual(int(sqlite_review_count or 0), 0)
        self.assertEqual(int(sqlite_manual_review_count or 0), 0)
        self.assertEqual(int(sqlite_dispatch_count or 0), 0)
        self.assertEqual(int(sqlite_control_count or 0), 0)
        self.assertEqual(int(sqlite_registry_count or 0), 0)
        self.assertEqual(int(sqlite_profile_count or 0), 0)
        self.assertEqual(int(sqlite_ledger_count or 0), 0)
        self.assertEqual(int(sqlite_feedback_count or 0), 0)
        self.assertEqual(int(sqlite_pattern_count or 0), 0)
        self.assertEqual(int(sqlite_suggestion_count or 0), 0)
        self.assertEqual(int(sqlite_version_count or 0), 0)
        self.assertEqual(int(sqlite_compiler_count or 0), 0)
        self.assertEqual(int(sqlite_diff_count or 0), 0)
        self.assertEqual(int(sqlite_policy_run_count or 0), 0)

    def test_merge_manual_review_item_metadata_writes_natively_in_prefer_postgres(self) -> None:
        store = self._build_store(mode="prefer_postgres")

        manual_items = store.replace_manual_review_items(
            "job-native-metadata",
            [
                {
                    "candidate_id": "cand-native-metadata",
                    "target_company": "Acme Native",
                    "review_type": "profile_gap",
                    "summary": "Need metadata merge",
                    "candidate": {"candidate_id": "cand-native-metadata"},
                    "evidence": [],
                    "metadata": {"source": "postgres_native", "seed": 1},
                }
            ],
        )
        merged = store.merge_manual_review_item_metadata(
            int(manual_items[0]["review_item_id"]),
            {"merged": True, "seed": 2},
        )

        assert merged is not None
        self.assertEqual(merged["metadata"]["source"], "postgres_native")
        self.assertEqual(merged["metadata"]["seed"], 2)
        self.assertTrue(bool(merged["metadata"]["merged"]))
        sqlite_manual_review_count = store._connection.execute(
            "SELECT COUNT(*) FROM manual_review_items WHERE job_id = ?",
            ("job-native-metadata",),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_manual_review_count or 0), 0)

    def test_materialization_runtime_state_prefers_postgres_and_avoids_sqlite(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        state = store.upsert_candidate_materialization_state(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            candidate_id="cand-materialized",
            fingerprint="fp-1",
            shard_path="candidate_shards/cand-materialized.json",
            list_page=2,
            metadata={"source": "postgres_native"},
        )
        listed_states = store.list_candidate_materialization_states(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
        )
        fetched_state = store.get_candidate_materialization_state(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            candidate_id="cand-materialized",
        )
        pruned = store.prune_candidate_materialization_states(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            active_candidate_ids=[],
        )

        started_run = store.start_snapshot_materialization_run(
            run_id="run-materialized",
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            summary={"source": "postgres_native"},
        )
        completed_run = store.complete_snapshot_materialization_run(
            run_id="run-materialized",
            status="completed",
            dirty_candidate_count=1,
            completed_candidate_count=1,
            reused_candidate_count=0,
            summary={"source": "postgres_native", "status": "completed"},
        )
        fetched_run = store.get_snapshot_materialization_run("run-materialized")

        self.assertEqual(state["metadata"]["source"], "postgres_native")
        self.assertEqual(listed_states[0]["candidate_id"], "cand-materialized")
        self.assertEqual(fetched_state["fingerprint"], "fp-1")
        self.assertEqual(pruned[0]["candidate_id"], "cand-materialized")
        self.assertEqual(started_run["status"], "running")
        self.assertEqual(completed_run["status"], "completed")
        self.assertEqual(fetched_run["summary"]["source"], "postgres_native")

        sqlite_state_count = store._connection.execute(
            "SELECT COUNT(*) FROM candidate_materialization_state WHERE candidate_id = ?",
            ("cand-materialized",),
        ).fetchone()[0]
        sqlite_run_count = store._connection.execute(
            "SELECT COUNT(*) FROM snapshot_materialization_runs WHERE run_id = ?",
            ("run-materialized",),
        ).fetchone()[0]

        self.assertEqual(int(sqlite_state_count or 0), 0)
        self.assertEqual(int(sqlite_run_count or 0), 0)

    def test_bulk_materialization_runtime_state_prefers_postgres_and_batches_writes(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["candidate_materialization_state"] = [
            {
                "target_company": "Acme Materialized Canonical",
                "company_key": "acmematerialized",
                "snapshot_id": "snap-materialized",
                "asset_view": "canonical_merged",
                "candidate_id": "cand-materialized-1",
                "fingerprint": "fp-initial",
                "shard_path": "candidate_shards/cand-materialized-1.json",
                "list_page": 1,
                "dirty_reason": "",
                "materialized_at": "2026-04-22T00:00:00+00:00",
                "metadata_json": json.dumps({"source": "seed"}, ensure_ascii=False),
                "created_at": "2026-04-22T00:00:00+00:00",
                "updated_at": "2026-04-22T00:00:00+00:00",
            }
        ]

        written = store.bulk_upsert_candidate_materialization_states(
            states=[
                {
                    "target_company": "Acme Materialized!!!",
                    "snapshot_id": "snap-materialized",
                    "asset_view": "canonical_merged",
                    "candidate_id": "cand-materialized-1",
                    "fingerprint": "fp-updated",
                    "shard_path": "candidate_shards/cand-materialized-1-v2.json",
                    "list_page": 2,
                    "metadata": {"source": "bulk"},
                },
                {
                    "target_company": "Acme Materialized!!!",
                    "snapshot_id": "snap-materialized",
                    "asset_view": "canonical_merged",
                    "candidate_id": "cand-materialized-2",
                    "fingerprint": "fp-new",
                    "shard_path": "candidate_shards/cand-materialized-2.json",
                    "list_page": 3,
                    "metadata": {"source": "bulk"},
                },
            ]
        )

        listed_states = store.list_candidate_materialization_states(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
        )
        listed_by_candidate_id = {item["candidate_id"]: item for item in listed_states}

        self.assertEqual(written, 2)
        self.assertEqual(len(adapter.bulk_upserts), 1)
        self.assertEqual(adapter.bulk_upserts[0][0], "candidate_materialization_state")
        self.assertEqual(
            listed_by_candidate_id["cand-materialized-1"]["target_company"],
            "Acme Materialized Canonical",
        )
        self.assertEqual(listed_by_candidate_id["cand-materialized-1"]["fingerprint"], "fp-updated")
        self.assertEqual(listed_by_candidate_id["cand-materialized-2"]["list_page"], 3)

        sqlite_state_count = store._connection.execute(
            "SELECT COUNT(*) FROM candidate_materialization_state WHERE candidate_id IN (?, ?)",
            ("cand-materialized-1", "cand-materialized-2"),
        ).fetchone()[0]
        self.assertEqual(int(sqlite_state_count or 0), 0)

    def test_replace_materialization_runtime_state_scope_prefers_postgres_and_replaces_scope(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        adapter.generic_rows["candidate_materialization_state"] = [
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "snap-openai",
                "asset_view": "canonical_merged",
                "candidate_id": "cand-legacy",
                "fingerprint": "fp-legacy",
                "shard_path": "candidate_shards/cand-legacy.json",
                "list_page": 1,
                "dirty_reason": "",
                "materialized_at": "2026-04-22T00:00:00+00:00",
                "metadata_json": json.dumps({"source": "seed"}, ensure_ascii=False),
                "created_at": "2026-04-22 00:00:00",
                "updated_at": "2026-04-22 00:00:00",
            },
            {
                "target_company": "OtherCo",
                "company_key": "otherco",
                "snapshot_id": "snap-other",
                "asset_view": "canonical_merged",
                "candidate_id": "cand-other",
                "fingerprint": "fp-other",
                "shard_path": "candidate_shards/cand-other.json",
                "list_page": 1,
                "dirty_reason": "",
                "materialized_at": "2026-04-22T00:00:00+00:00",
                "metadata_json": "{}",
                "created_at": "2026-04-22 00:00:00",
                "updated_at": "2026-04-22 00:00:00",
            },
        ]

        written = store.replace_candidate_materialization_state_scope(
            target_company="OpenAI",
            snapshot_id="snap-openai",
            asset_view="canonical_merged",
            states=[
                {
                    "target_company": "OpenAI!!!",
                    "candidate_id": "cand-current",
                    "fingerprint": "fp-current",
                    "shard_path": "candidate_shards/cand-current.json",
                    "list_page": 2,
                    "metadata": {"source": "replace"},
                }
            ],
            existing_states_by_candidate_id={
                "cand-current": {
                    "target_company": "OpenAI Canonical",
                    "created_at": "2026-04-20 00:00:00",
                }
            },
        )

        listed_states = store.list_candidate_materialization_states(
            target_company="OpenAI",
            snapshot_id="snap-openai",
            asset_view="canonical_merged",
        )

        self.assertEqual(written, 1)
        self.assertEqual(len(adapter.scope_replacements), 1)
        self.assertEqual(len(listed_states), 1)
        self.assertEqual(listed_states[0]["candidate_id"], "cand-current")
        self.assertEqual(listed_states[0]["target_company"], "OpenAI Canonical")
        self.assertEqual(listed_states[0]["metadata"]["source"], "replace")
        self.assertEqual(
            {item["candidate_id"] for item in adapter.generic_rows["candidate_materialization_state"]},
            {"cand-current", "cand-other"},
        )

    def test_asset_materialization_generation_prefers_postgres_and_avoids_sqlite(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        primary_generation = store.register_asset_materialization(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="current",
            source_path="normalized_artifacts/current.json",
            summary={"source": "postgres_native", "lane": "current"},
            metadata={"scope": "current"},
            members=[
                {
                    "member_key": "cand-a",
                    "candidate_id": "cand-a",
                    "lane": "company_roster",
                    "employment_scope": "current",
                },
                {
                    "member_key": "cand-b",
                    "candidate_id": "cand-b",
                    "lane": "company_roster",
                    "employment_scope": "current",
                },
            ],
        )
        secondary_generation = store.register_asset_materialization(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="former",
            source_path="normalized_artifacts/former.json",
            summary={"source": "postgres_native", "lane": "former"},
            metadata={"scope": "former"},
            members=[
                {
                    "member_key": "cand-b",
                    "candidate_id": "cand-b",
                    "lane": "profile_search",
                    "employment_scope": "former",
                },
                {
                    "member_key": "cand-c",
                    "candidate_id": "cand-c",
                    "lane": "profile_search",
                    "employment_scope": "former",
                },
            ],
        )

        alias_generation = store.get_asset_materialization_generation(
            target_company="Acme Materialized!!!",
            snapshot_id="snap-materialized",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="current",
        )
        primary_summary = store.summarize_asset_membership_index(
            generation_key=str(primary_generation.get("generation_key") or "")
        )
        comparison = store.compare_asset_membership_generations(
            primary_generation_key=str(primary_generation.get("generation_key") or ""),
            secondary_generation_key=str(secondary_generation.get("generation_key") or ""),
        )

        self.assertEqual(alias_generation["company_key"], "acmematerialized")
        self.assertEqual(primary_generation["summary"]["source"], "postgres_native")
        self.assertEqual(primary_summary["member_count"], 2)
        self.assertEqual(primary_summary["employment_scope_counts"], {"current": 2})
        self.assertEqual(comparison["primary_member_count"], 2)
        self.assertEqual(comparison["secondary_member_count"], 2)
        self.assertEqual(comparison["overlap_member_count"], 1)
        self.assertEqual(comparison["primary_only_member_count"], 1)
        self.assertEqual(comparison["secondary_only_member_count"], 1)

        sqlite_generation_count = store._connection.execute(
            "SELECT COUNT(*) FROM asset_materialization_generations WHERE snapshot_id = ?",
            ("snap-materialized",),
        ).fetchone()[0]
        sqlite_membership_count = store._connection.execute(
            "SELECT COUNT(*) FROM asset_membership_index WHERE snapshot_id = ?",
            ("snap-materialized",),
        ).fetchone()[0]

        self.assertEqual(int(sqlite_generation_count or 0), 0)
        self.assertEqual(int(sqlite_membership_count or 0), 0)
        self.assertEqual(len(adapter.generic_rows.get("asset_materialization_generations", [])), 2)
        self.assertEqual(len(adapter.generic_rows.get("asset_membership_index", [])), 4)

    def test_patch_asset_materialization_generation_prefers_postgres_and_keeps_patch_rows_incremental(self) -> None:
        store = self._build_store(mode="prefer_postgres")
        adapter = store._control_plane_postgres
        assert isinstance(adapter, _FakeLiveControlPlanePostgresAdapter)

        base_generation = store.register_asset_materialization(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized-base",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            source_path="normalized_artifacts/base.json",
            summary={"source": "postgres_native", "lane": "baseline"},
            metadata={"scope": "baseline"},
            members=[
                {
                    "member_key": "cand-a",
                    "candidate_id": "cand-a",
                    "lane": "company_roster",
                    "employment_scope": "current",
                },
                {
                    "member_key": "cand-b",
                    "candidate_id": "cand-b",
                    "lane": "company_roster",
                    "employment_scope": "current",
                },
            ],
        )
        patched_generation = store.patch_asset_materialization(
            target_company="Acme Materialized",
            snapshot_id="snap-materialized-delta",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            base_generation_key=str(base_generation.get("generation_key") or ""),
            source_path="normalized_artifacts/delta.json",
            summary={"source": "postgres_patch"},
            metadata={"scope": "delta_overlay"},
            members=[
                {
                    "member_key": "cand-b",
                    "candidate_id": "cand-b",
                    "lane": "profile_search",
                    "employment_scope": "current",
                },
                {
                    "member_key": "cand-c",
                    "candidate_id": "cand-c",
                    "lane": "profile_search",
                    "employment_scope": "former",
                },
            ],
            removed_member_keys=["cand-a"],
        )

        patched_summary = store.summarize_asset_membership_index(
            generation_key=str(patched_generation.get("generation_key") or "")
        )
        comparison = store.compare_asset_membership_generations(
            primary_generation_key=str(base_generation.get("generation_key") or ""),
            secondary_generation_key=str(patched_generation.get("generation_key") or ""),
        )

        self.assertEqual(patched_summary["member_count"], 2)
        self.assertEqual(patched_summary["employment_scope_counts"], {"current": 1, "former": 1})
        self.assertEqual(comparison["primary_member_count"], 2)
        self.assertEqual(comparison["secondary_member_count"], 2)
        self.assertEqual(comparison["overlap_member_count"], 1)
        self.assertEqual(comparison["secondary_only_member_count"], 1)
        self.assertEqual(comparison["primary_only_member_count"], 1)
        self.assertEqual(
            str(dict(patched_generation.get("metadata") or {}).get("generation_patch", {}).get("base_generation_key") or ""),
            str(base_generation.get("generation_key") or ""),
        )

        patched_rows = [
            dict(row)
            for row in list(adapter.generic_rows.get("asset_membership_index", []))
            if str(dict(row).get("generation_key") or "").strip() == str(patched_generation.get("generation_key") or "")
        ]
        self.assertEqual(len(patched_rows), 2)
        self.assertEqual(len(adapter.generic_rows.get("asset_membership_index", [])), 4)

    def test_runtime_coordination_prefers_postgres_for_spans_workers_and_leases(self) -> None:
        store = self._build_store(mode="prefer_postgres")

        session = store.create_agent_runtime_session(
            job_id="job-runtime-pg",
            target_company="Acme",
            request_payload={"target_company": "Acme"},
            plan_payload={"mode": "agent_runtime"},
            runtime_mode="agent_runtime",
            lanes=[{"lane_id": "search_planner"}],
        )
        span = store.create_agent_trace_span(
            session_id=int(session["session_id"]),
            job_id="job-runtime-pg",
            lane_id="search_planner",
            span_name="bundle:01",
            stage="acquiring",
        )
        worker = store.create_or_resume_agent_worker(
            session_id=int(session["session_id"]),
            job_id="job-runtime-pg",
            span_id=int(span["span_id"]),
            lane_id="search_planner",
            worker_key="bundle::01",
            budget_payload={"max_results": 5},
            input_payload={"query": "acme"},
            metadata={"index": 1},
        )
        running = store.mark_agent_worker_running(int(worker["worker_id"]))
        self.assertIsNotNone(running)
        checkpointed = store.checkpoint_agent_worker(
            int(worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"seed_entry_count": 2},
        )
        self.assertIsNotNone(checkpointed)
        lease = store.acquire_workflow_job_lease(
            "job-runtime-pg",
            lease_owner="runner-1",
            lease_seconds=120,
            lease_token="lease-token-1",
        )
        completed_worker = store.complete_agent_worker(
            int(worker["worker_id"]),
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={"seed_entry_count": 3},
        )
        completed_span = store.complete_agent_trace_span(
            int(span["span_id"]),
            status="completed",
            output_payload={"summary": "done"},
        )

        resolved_span = store.get_agent_trace_span(int(span["span_id"]))
        self.assertIsNotNone(resolved_span)
        self.assertEqual(str(resolved_span["status"] or ""), "completed")
        self.assertEqual(len(store.list_agent_trace_spans(job_id="job-runtime-pg")), 1)
        resolved_worker = store.get_agent_worker(worker_id=int(worker["worker_id"]))
        self.assertIsNotNone(resolved_worker)
        self.assertEqual(str(resolved_worker["status"] or ""), "completed")
        self.assertEqual(len(store.list_agent_workers(job_id="job-runtime-pg")), 1)
        self.assertEqual(len(store.list_recoverable_agent_workers(job_id="job-runtime-pg")), 0)
        self.assertTrue(bool(lease.get("acquired")))
        resolved_lease = store.get_workflow_job_lease("job-runtime-pg")
        self.assertIsNotNone(resolved_lease)
        self.assertEqual(str(resolved_lease["lease_owner"] or ""), "runner-1")
        self.assertEqual(str(completed_worker["status"] or ""), "completed")
        self.assertEqual(str(completed_span["status"] or ""), "completed")

        store.release_workflow_job_lease("job-runtime-pg", lease_owner="runner-1", lease_token="lease-token-1")
        self.assertIsNone(store.get_workflow_job_lease("job-runtime-pg"))


class LiveControlPlanePostgresRetryTest(unittest.TestCase):
    def test_connect_disables_gss_for_loopback_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LiveControlPlanePostgresAdapter(
                runtime_dir=Path(temp_dir),
                sqlite_path=Path(temp_dir) / "shadow.db",
                dsn="postgresql://tester@127.0.0.1:5432/sourcing_agent",
                mode="postgres_only",
            )
            adapter._psycopg = _FakeConnectPsycopg()

            with adapter._connect():
                pass

            fake_psycopg = adapter._psycopg
            assert isinstance(fake_psycopg, _FakeConnectPsycopg)
            self.assertEqual(len(fake_psycopg.calls), 1)
            self.assertEqual(
                fake_psycopg.calls[0][0],
                "postgresql://tester@127.0.0.1:5432/sourcing_agent?gssencmode=disable",
            )
            self.assertEqual(fake_psycopg.calls[0][1], {"client_encoding": "utf8"})

    def test_execute_returning_one_retries_retryable_postgres_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LiveControlPlanePostgresAdapter(
                runtime_dir=Path(temp_dir),
                sqlite_path=Path(temp_dir) / "shadow.db",
                dsn="postgresql://example/test",
                mode="postgres_only",
            )
            adapter.ensure_bootstrapped = lambda: None  # type: ignore[method-assign]
            outcomes: list[object] = [
                _RetryablePostgresError("40P01"),
                {
                    "row": ("ok",),
                    "rowcount": 1,
                    "description": [("value",)],
                },
            ]
            commit_counter = {"count": 0}
            adapter._connect = lambda: _FakeRetryConnection(outcomes, commit_counter)  # type: ignore[method-assign]

            result = adapter._execute_returning_one("SELECT 1", ("value",))

            self.assertEqual(result, {"value": "ok"})
            self.assertEqual(commit_counter["count"], 1)

    def test_execute_non_query_does_not_retry_non_retryable_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = LiveControlPlanePostgresAdapter(
                runtime_dir=Path(temp_dir),
                sqlite_path=Path(temp_dir) / "shadow.db",
                dsn="postgresql://example/test",
                mode="postgres_only",
            )
            adapter.ensure_bootstrapped = lambda: None  # type: ignore[method-assign]
            outcomes: list[object] = [ValueError("boom")]
            commit_counter = {"count": 0}
            adapter._connect = lambda: _FakeRetryConnection(outcomes, commit_counter)  # type: ignore[method-assign]

            with self.assertRaisesRegex(ValueError, "boom"):
                adapter._execute_non_query("UPDATE test SET value = %s", ("value",))

            self.assertEqual(commit_counter["count"], 0)


if __name__ == "__main__":
    unittest.main()
