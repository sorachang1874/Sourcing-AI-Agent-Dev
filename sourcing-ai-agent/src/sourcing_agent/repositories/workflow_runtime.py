"""Track B B4.2 — workflow / operation runtime control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

import json
from hashlib import sha1
from typing import Any

from ..control_plane_repository import Column, Kind, Repository, TableDescriptor
from ..control_plane_serde import json_safe_payload
from ..control_plane_time import utc_now_timestamp


def _loads_json_list(value: Any, *, default: list[Any] | None = None) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return list(default or [])
    return list(parsed) if isinstance(parsed, list) else list(default or [])


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _normalize_json_object_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(json_safe_payload(value))
    return _loads_json_dict(value)


WORKFLOW_RECOVERY_INTENTS = TableDescriptor(
    table="workflow_recovery_intents",
    pk=("job_id",),
    columns=(
        Column("job_id"),
        Column("classification"),
        Column("status"),
        Column("requested_at"),
        Column("requested_by"),
        Column("params_json", Kind.JSON, field="params"),
        Column("lease_owner"),
        Column("lease_expires_at"),
        Column("claimed_at"),
        Column("schema_version", read_default="workflow_recovery_intent_v1"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


WORKFLOW_EVENTS = TableDescriptor(
    table="workflow_events",
    pk=("event_id",),
    columns=(
        Column("event_id"),
        Column("workflow_run_id"),
        Column("operation_id"),
        Column("command_id"),
        Column("activity_attempt_id"),
        Column("event_family"),
        Column("event_type"),
        Column("sequence_number", Kind.INT),
        Column("idempotency_key"),
        Column("occurred_at"),
        Column("recorded_at"),
        Column("actor"),
        Column("source"),
        Column("payload_json", Kind.JSON, field="payload"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("schema_version", read_default="workflow_event_v1"),
        Column("created_at"),
    ),
)


AGENT_ACTIONS = TableDescriptor(
    table="agent_actions",
    pk=("action_id",),
    columns=(
        Column("action_id"),
        Column("workspace_id", read_default="default"),
        Column("conversation_id"),
        Column("action_type"),
        Column("owner_module"),
        Column("operation_type"),
        Column("target_ref_json", Kind.JSON, field="target_ref"),
        Column("input_json", Kind.JSON, field="input"),
        Column("approval_status", default="not_required"),
        Column("approval_policy", default="not_required"),
        Column("budget_json", Kind.JSON, field="budget"),
        Column("idempotency_key"),
        Column("status", default="planned"),
        Column("result_ref_json", Kind.JSON, field="result_ref"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


OPERATION_RUNS = TableDescriptor(
    table="operation_runs",
    pk=("operation_run_id",),
    columns=(
        Column("operation_run_id"),
        Column("workspace_id", read_default="default"),
        Column("action_id"),
        Column("owner_module"),
        Column("operation_type"),
        Column("status", default="queued"),
        Column("progress_json", Kind.JSON, field="progress"),
        Column("workflow_ref_json", Kind.JSON, field="workflow_ref"),
        Column("cost_budget_json", Kind.JSON, field="cost_budget"),
        Column("idempotency_key"),
        Column("result_ref_json", Kind.JSON, field="result_ref"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("started_at"),
        Column("completed_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


ACQUISITION_RUNS = TableDescriptor(
    table="acquisition_runs",
    pk=("acquisition_run_id",),
    columns=(
        Column("acquisition_run_id"),
        Column("workspace_id", read_default="default"),
        Column("operation_run_id"),
        Column("workflow_run_id"),
        Column("plan_id"),
        Column("plan_review_id", Kind.INT),
        Column("target_company"),
        Column("query"),
        Column("status", default="planned"),
        Column("current_phase"),
        Column("request_json", Kind.JSON, field="request"),
        Column("plan_json", Kind.JSON, field="plan"),
        Column("execution_bundle_json", Kind.JSON, field="execution_bundle"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("idempotency_key"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


WORKFLOW_ACTIVITY_RUNS = TableDescriptor(
    table="workflow_activity_runs",
    pk=("activity_run_id",),
    columns=(
        Column("activity_run_id"),
        Column("workspace_id", read_default="default"),
        Column("workflow_run_id"),
        Column("operation_run_id"),
        Column("acquisition_run_id"),
        Column("command_id"),
        Column("parent_activity_run_id"),
        Column("activity_type"),
        Column("owner"),
        Column("status", default="planned"),
        Column("phase"),
        Column("idempotency_key"),
        Column("provider_ref_json", Kind.JSON, field="provider_ref"),
        Column("input_json", Kind.JSON, field="input"),
        Column("output_json", Kind.JSON, field="output"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("entity_counts_json", Kind.JSON, field="entity_counts"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


WORKFLOW_ACTIVITY_ATTEMPTS = TableDescriptor(
    table="workflow_activity_attempts",
    pk=("attempt_id",),
    columns=(
        Column("attempt_id"),
        Column("workspace_id", read_default="default"),
        Column("activity_run_id"),
        Column("workflow_run_id"),
        Column("command_id"),
        Column("attempt_number", Kind.INT),
        Column("status", default="planned"),
        Column("provider"),
        Column("provider_request_ref"),
        Column("provider_run_ref"),
        Column("started_at"),
        Column("completed_at"),
        Column("next_retry_at"),
        Column("rate_limit_ref_json", Kind.JSON, field="rate_limit_ref"),
        Column("error_json", Kind.JSON, field="error"),
        Column("input_json", Kind.JSON, field="input"),
        Column("output_json", Kind.JSON, field="output"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("idempotency_key"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


WORKFLOW_ENTITY_DELTAS = TableDescriptor(
    table="workflow_entity_deltas",
    pk=("delta_id",),
    columns=(
        Column("delta_id"),
        Column("workspace_id", read_default="default"),
        Column("workflow_run_id"),
        Column("operation_run_id"),
        Column("command_id"),
        Column("activity_run_id"),
        Column("attempt_id"),
        Column("acquisition_run_id"),
        Column("entity_type"),
        Column("entity_key"),
        Column("delta_kind"),
        Column("status", default="recorded"),
        Column("reason"),
        Column("source_ref_json", Kind.JSON, field="source_ref"),
        Column("entity_payload_json", Kind.JSON, field="entity_payload"),
        Column("projection_effect_json", Kind.JSON, field="projection_effect"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("idempotency_key"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


ACQUISITION_DISCOVERY_LANES = TableDescriptor(
    table="acquisition_discovery_lanes",
    pk=("lane_id",),
    columns=(
        Column("lane_id"),
        Column("workspace_id", read_default="default"),
        Column("acquisition_run_id"),
        Column("workflow_run_id"),
        Column("operation_run_id"),
        Column("source_command_id"),
        Column("activity_run_id"),
        Column("target_company"),
        Column("query"),
        Column("provider"),
        Column("status", default="planned"),
        Column("phase", default="planned"),
        Column("lane_plan_json", Kind.JSON, field="lane_plan"),
        Column("provider_ref_json", Kind.JSON, field="provider_ref"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("entity_counts_json", Kind.JSON, field="entity_counts"),
        Column("downstream_command_ids_json", Kind.JSON_LIST, field="downstream_command_ids"),
        Column("idempotency_key"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


OPERATION_EVENTS = TableDescriptor(
    table="operation_events",
    pk=("event_id",),
    columns=(
        Column("event_id"),
        Column("workspace_id", read_default="default"),
        Column("event_stream_id"),
        Column("operation_run_id"),
        Column("action_id"),
        Column("event_family"),
        Column("event_type"),
        Column("sequence_number", Kind.INT),
        Column("idempotency_key"),
        Column("occurred_at"),
        Column("recorded_at"),
        Column("actor"),
        Column("source"),
        Column("payload_json", Kind.JSON, field="payload"),
        Column("schema_version", read_default="operation_event_v1"),
        Column("created_at"),
    ),
)


WORKFLOW_CURRENT_STATE = TableDescriptor(
    table="workflow_current_state",
    pk=("workflow_run_id",),
    columns=(
        Column("workflow_run_id"),
        Column("operation_id"),
        Column("workflow_type"),
        Column("status", default="pending"),
        Column("current_stage_key"),
        Column("completion_proofs_json", Kind.JSON, field="completion_proofs"),
        Column("active_command_counts_json", Kind.JSON, field="active_command_counts"),
        Column("terminal_command_counts_json", Kind.JSON, field="terminal_command_counts"),
        Column("read_model_pointers_json", Kind.JSON, field="read_model_pointers"),
        Column("migration_status_json", Kind.JSON, field="migration_status"),
        Column("last_processed_sequence_number", Kind.INT),
        Column("reducer_version"),
        Column("schema_version", read_default="workflow_current_state_v1"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


WORKFLOW_COMMANDS = TableDescriptor(
    table="workflow_commands",
    pk=("command_id",),
    columns=(
        Column("command_id"),
        Column("workflow_run_id"),
        Column("operation_id"),
        Column("command_type"),
        Column("owner"),
        Column("stage_id"),
        Column("causal_group_id"),
        Column("parent_command_id"),
        Column("source_event_id"),
        Column("source_event_type"),
        Column("input_artifact_refs_json", Kind.JSON_LIST, field="input_artifact_refs"),
        Column("output_artifact_refs_json", Kind.JSON_LIST, field="output_artifact_refs"),
        Column("produced_entity_counts_json", Kind.JSON, field="produced_entity_counts"),
        Column("no_op_reason"),
        Column("readiness_effect"),
        Column("downstream_command_ids_json", Kind.JSON_LIST, field="downstream_command_ids"),
        Column("causality_schema_version", read_default="command_causality_v1"),
        Column("status"),
        Column("idempotency_key"),
        Column("payload_json", Kind.JSON, field="payload"),
        Column("artifact_refs_json", Kind.JSON_LIST, field="artifact_refs"),
        Column("not_before_at"),
        Column("attempt", Kind.INT),
        Column("max_attempts", Kind.INT),
        Column("retry_policy_json", Kind.JSON, field="retry_policy"),
        Column("lease_owner"),
        Column("lease_expires_at"),
        Column("heartbeat_at"),
        Column("last_error"),
        Column("result_json", Kind.JSON, field="result"),
        Column("schema_version", read_default="workflow_command_v1"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


RUNTIME_OUTBOX = TableDescriptor(
    table="runtime_outbox",
    pk=("outbox_id",),
    columns=(
        Column("outbox_id"),
        Column("workflow_run_id"),
        Column("operation_id"),
        Column("command_id"),
        Column("outbox_type"),
        Column("status"),
        Column("idempotency_key"),
        Column("payload_json", Kind.JSON, field="payload"),
        Column("not_before_at"),
        Column("attempt", Kind.INT),
        Column("max_attempts", Kind.INT),
        Column("lease_owner"),
        Column("lease_expires_at"),
        Column("dispatched_at"),
        Column("last_error"),
        Column("schema_version", read_default="runtime_outbox_v1"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
FROM_ROW_DESCRIPTORS = {
    "_workflow_recovery_intent_from_row": WORKFLOW_RECOVERY_INTENTS,
    "_workflow_event_from_row": WORKFLOW_EVENTS,
    "_workflow_activity_run_from_row": WORKFLOW_ACTIVITY_RUNS,
    "_workflow_activity_attempt_from_row": WORKFLOW_ACTIVITY_ATTEMPTS,
    "_workflow_entity_delta_from_row": WORKFLOW_ENTITY_DELTAS,
    "_workflow_current_state_from_row": WORKFLOW_CURRENT_STATE,
    "_workflow_command_from_row": WORKFLOW_COMMANDS,
    "_runtime_outbox_from_row": RUNTIME_OUTBOX,
}


class WorkflowRuntimeRepository(Repository):
    """PG-only repository for workflow and operation runtime state."""

    _ACQUISITION_RUN_IMMUTABLE_COLUMNS = (
        "acquisition_run_id",
        "workspace_id",
        "workflow_run_id",
        "operation_run_id",
        "idempotency_key",
    )
    _ACQUISITION_RUN_TERMINAL_STATUSES = (
        "cancelled_before_probe",
        "cancelled_before_discovery",
        "cancelled_before_profile_fetch_activity",
        "projection_admitted",
        "completed",
        "failed",
        "cancelled",
    )
    _DISCOVERY_LANE_IMMUTABLE_COLUMNS = (
        "lane_id",
        "workspace_id",
        "acquisition_run_id",
        "workflow_run_id",
        "operation_run_id",
        "source_command_id",
        "activity_run_id",
        "idempotency_key",
    )
    _DISCOVERY_LANE_TERMINAL_STATUSES = (
        "cancelled_before_discovery",
        "provider_discovery_completed",
        "completed",
        "failed",
        "cancelled",
    )

    def _require_postgres_for_durable_runtime(self, table_name: str) -> None:
        normalized_table = str(table_name or "").strip()
        if str(getattr(self._adapter, "mode", "") or "").strip() == "postgres_only":
            return
        raise RuntimeError(
            f"{normalized_table} is PG-only durable runtime storage. "
            "Set SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only with a resolved Postgres DSN; "
            "SQLite durable runtime execution is not a normal path."
        )

    def _upsert_acquisition_runtime_row(
        self,
        table_name: str,
        *,
        id_column: str,
        row_payload: dict[str, Any],
        row_builder: Any,
        immutable_columns: tuple[str, ...],
        terminal_statuses: tuple[str, ...],
    ) -> dict[str, Any]:
        if self._should_prefer_read(table_name):
            row = self._call_native_write(
                "upsert_acquisition_runtime_row",
                table_name=table_name,
                row=row_payload,
                id_column=id_column,
                immutable_columns=immutable_columns,
                terminal_statuses=terminal_statuses,
            )
            if row is not None:
                return row_builder(row)
            if self._strict_authoritative(table_name):
                self._raise_write_failure(
                    table_name=table_name,
                    method_name="upsert_acquisition_runtime_row",
                    reason="postgres-only: authoritative upsert returned no row",
                )
        self._raise_postgres_only_invariant(
            table_name=table_name,
            method_name="upsert_acquisition_runtime_row",
        )
        raise AssertionError("unreachable")

    def upsert_acquisition_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("acquisition_runs")
        normalized = dict(payload or {})
        workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"
        operation_run_id = str(normalized.get("operation_run_id") or "").strip()
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        plan_id = str(normalized.get("plan_id") or "").strip()
        plan_review_id = int(normalized.get("plan_review_id") or 0)
        target_company = str(normalized.get("target_company") or "").strip()
        query_text = str(normalized.get("query") or "").strip()
        idempotency_key = str(normalized.get("idempotency_key") or "").strip()
        if not idempotency_key:
            idempotency_seed = "|".join(
                [operation_run_id, workflow_run_id, str(plan_review_id), plan_id, target_company]
            )
            idempotency_key = f"acquisition_run:{sha1(idempotency_seed.encode('utf-8')).hexdigest()[:24]}"
        acquisition_run_id = str(
            normalized.get("acquisition_run_id")
            or normalized.get("run_id")
            or f"acqrun_{sha1(idempotency_key.encode('utf-8')).hexdigest()[:24]}"
        ).strip()
        if not acquisition_run_id or not workflow_run_id:
            return {}
        existing = self.get_acquisition_run(acquisition_run_id)
        now = utc_now_timestamp()
        row_payload = ACQUISITION_RUNS.to_columns(
            {
                **normalized,
                "acquisition_run_id": acquisition_run_id,
                "workspace_id": workspace_id,
                "operation_run_id": operation_run_id,
                "workflow_run_id": workflow_run_id,
                "plan_id": plan_id,
                "plan_review_id": plan_review_id,
                "target_company": target_company,
                "query": query_text,
                "request": _normalize_json_object_payload(normalized.get("request") or normalized.get("request_json")),
                "plan": _normalize_json_object_payload(normalized.get("plan") or normalized.get("plan_json")),
                "execution_bundle": _normalize_json_object_payload(
                    normalized.get("execution_bundle") or normalized.get("execution_bundle_json")
                ),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "idempotency_key": idempotency_key,
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_acquisition_runtime_row(
            "acquisition_runs",
            id_column="acquisition_run_id",
            row_payload=row_payload,
            row_builder=self._acquisition_run_from_row,
            immutable_columns=self._ACQUISITION_RUN_IMMUTABLE_COLUMNS,
            terminal_statuses=self._ACQUISITION_RUN_TERMINAL_STATUSES,
        )

    def get_acquisition_run(self, acquisition_run_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("acquisition_runs")
        normalized_run_id = str(acquisition_run_id or "").strip()
        if not normalized_run_id:
            return {}
        postgres_row = self._select_row(
            "acquisition_runs",
            row_builder=self._acquisition_run_from_row,
            where_sql="acquisition_run_id = %s",
            params=[normalized_run_id],
        )
        return postgres_row or {}

    def list_acquisition_runs(
        self,
        *,
        workspace_id: str = "default",
        operation_run_id: str = "",
        workflow_run_id: str = "",
        target_company: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("acquisition_runs")
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "").strip()
        if normalized_workspace_id:
            pg_clauses.append("workspace_id = %s")
            pg_params.append(normalized_workspace_id)
        normalized_operation_id = str(operation_run_id or "").strip()
        if normalized_operation_id:
            pg_clauses.append("operation_run_id = %s")
            pg_params.append(normalized_operation_id)
        normalized_workflow_run_id = str(workflow_run_id or "").strip()
        if normalized_workflow_run_id:
            pg_clauses.append("workflow_run_id = %s")
            pg_params.append(normalized_workflow_run_id)
        normalized_company = str(target_company or "").strip()
        if normalized_company:
            pg_clauses.append("target_company = %s")
            pg_params.append(normalized_company)
        normalized_statuses = [
            str(status or "").strip() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            pg_placeholders = ", ".join(["%s"] * len(normalized_statuses))
            pg_clauses.append(f"status IN ({pg_placeholders})")
            pg_params.extend(normalized_statuses)
        return self._select_rows(
            "acquisition_runs",
            row_builder=self._acquisition_run_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(0, int(limit or 0)),
            offset=max(0, int(offset or 0)),
        )

    def upsert_discovery_lane(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("acquisition_discovery_lanes")
        normalized = dict(payload or {})
        workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"
        acquisition_run_id = str(normalized.get("acquisition_run_id") or "").strip()
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        operation_run_id = str(normalized.get("operation_run_id") or normalized.get("operation_id") or "").strip()
        source_command_id = str(normalized.get("source_command_id") or normalized.get("command_id") or "").strip()
        activity_run_id = str(normalized.get("activity_run_id") or "").strip()
        target_company = str(normalized.get("target_company") or "").strip()
        query_text = str(normalized.get("query") or "").strip()
        provider = str(normalized.get("provider") or "").strip()
        idempotency_key = str(normalized.get("idempotency_key") or "").strip()
        if not idempotency_key:
            idempotency_seed = "|".join([acquisition_run_id, workflow_run_id, source_command_id, query_text, provider])
            idempotency_key = f"acquisition_discovery_lane:{sha1(idempotency_seed.encode('utf-8')).hexdigest()[:24]}"
        lane_id = str(
            normalized.get("lane_id") or f"lane_{sha1(idempotency_key.encode('utf-8')).hexdigest()[:24]}"
        ).strip()
        if not lane_id or not acquisition_run_id or not workflow_run_id:
            return {}
        existing = self.get_discovery_lane(lane_id)
        now = utc_now_timestamp()
        row_payload = ACQUISITION_DISCOVERY_LANES.to_columns(
            {
                **normalized,
                "lane_id": lane_id,
                "workspace_id": workspace_id,
                "acquisition_run_id": acquisition_run_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "source_command_id": source_command_id,
                "activity_run_id": activity_run_id,
                "target_company": target_company,
                "query": query_text,
                "provider": provider,
                "lane_plan": _normalize_json_object_payload(
                    normalized.get("lane_plan") or normalized.get("lane_plan_json")
                ),
                "provider_ref": _normalize_json_object_payload(
                    normalized.get("provider_ref") or normalized.get("provider_ref_json")
                ),
                "artifact_refs": _loads_json_list(
                    normalized.get("artifact_refs") or normalized.get("artifact_refs_json")
                ),
                "entity_counts": _normalize_json_object_payload(
                    normalized.get("entity_counts") or normalized.get("entity_counts_json")
                ),
                "downstream_command_ids": _loads_json_list(
                    normalized.get("downstream_command_ids") or normalized.get("downstream_command_ids_json")
                ),
                "idempotency_key": idempotency_key,
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_acquisition_runtime_row(
            "acquisition_discovery_lanes",
            id_column="lane_id",
            row_payload=row_payload,
            row_builder=self._discovery_lane_from_row,
            immutable_columns=self._DISCOVERY_LANE_IMMUTABLE_COLUMNS,
            terminal_statuses=self._DISCOVERY_LANE_TERMINAL_STATUSES,
        )

    def get_discovery_lane(self, lane_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("acquisition_discovery_lanes")
        normalized_lane_id = str(lane_id or "").strip()
        if not normalized_lane_id:
            return {}
        postgres_row = self._select_row(
            "acquisition_discovery_lanes",
            row_builder=self._discovery_lane_from_row,
            where_sql="lane_id = %s",
            params=[normalized_lane_id],
        )
        return postgres_row or {}

    def list_discovery_lanes(
        self,
        *,
        workspace_id: str = "default",
        acquisition_run_id: str = "",
        workflow_run_id: str = "",
        operation_run_id: str = "",
        source_command_id: str = "",
        activity_run_id: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("acquisition_discovery_lanes")
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "").strip()
        if normalized_workspace_id:
            pg_clauses.append("workspace_id = %s")
            pg_params.append(normalized_workspace_id)
        for column_name, value in (
            ("acquisition_run_id", acquisition_run_id),
            ("workflow_run_id", workflow_run_id),
            ("operation_run_id", operation_run_id),
            ("source_command_id", source_command_id),
            ("activity_run_id", activity_run_id),
        ):
            normalized_value = str(value or "").strip()
            if normalized_value:
                pg_clauses.append(f"{column_name} = %s")
                pg_params.append(normalized_value)
        normalized_statuses = [
            str(status or "").strip() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            pg_clauses.append("status IN (" + ", ".join(["%s"] * len(normalized_statuses)) + ")")
            pg_params.extend(normalized_statuses)
        return self._select_rows(
            "acquisition_discovery_lanes",
            row_builder=self._discovery_lane_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(0, int(limit or 0)),
            offset=max(0, int(offset or 0)),
        )

    def upsert_action(
        self,
        *,
        action_id: str,
        workspace_id: str = "default",
        conversation_id: str = "",
        action_type: str,
        owner_module: str,
        operation_type: str,
        target_ref: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        approval_status: str = "not_required",
        approval_policy: str = "not_required",
        budget: dict[str, Any] | None = None,
        idempotency_key: str,
        status: str = "planned",
        result_ref: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("agent_actions")
        normalized_action_id = str(action_id or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_type = str(action_type or "").strip()
        normalized_owner = str(owner_module or "").strip()
        normalized_operation_type = str(operation_type or "").strip()
        normalized_idempotency = str(idempotency_key or "").strip()
        if (
            not normalized_action_id
            or not normalized_workspace_id
            or not normalized_type
            or not normalized_owner
            or not normalized_operation_type
            or not normalized_idempotency
        ):
            return {}
        now = utc_now_timestamp()
        row_payload = AGENT_ACTIONS.to_columns(
            {
                "action_id": normalized_action_id,
                "workspace_id": normalized_workspace_id,
                "conversation_id": conversation_id,
                "action_type": normalized_type,
                "owner_module": normalized_owner,
                "operation_type": normalized_operation_type,
                "target_ref": target_ref or {},
                "input": input_payload or {},
                "approval_status": approval_status,
                "approval_policy": approval_policy,
                "budget": budget or {},
                "idempotency_key": normalized_idempotency,
                "status": status,
                "result_ref": result_ref or {},
                "metadata": metadata or {},
                "created_at": now,
                "updated_at": now,
            }
        )
        if self._should_prefer_read("agent_actions"):
            row = self._call_native_write(
                "upsert_agent_action",
                table_name="agent_actions",
                row=row_payload,
            )
            if row is not None:
                return self._action_from_row(row)
            if self._strict_authoritative("agent_actions"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for agent_actions in upsert_agent_action: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_action(self, action_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("agent_actions")
        normalized_action_id = str(action_id or "").strip()
        if not normalized_action_id:
            return {}
        postgres_row = self._select_row(
            "agent_actions",
            row_builder=self._action_from_row,
            where_sql="action_id = %s",
            params=[normalized_action_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_actions(
        self,
        *,
        workspace_id: str = "default",
        conversation_id: str = "",
        action_type: str = "",
        owner_module: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("agent_actions")
        clauses: list[str] = []
        params: list[Any] = []
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "").strip()
        if normalized_workspace_id:
            clauses.append("workspace_id = ?")
            params.append(normalized_workspace_id)
            pg_clauses.append("workspace_id = %s")
            pg_params.append(normalized_workspace_id)
        normalized_conversation_id = str(conversation_id or "").strip()
        if normalized_conversation_id:
            clauses.append("conversation_id = ?")
            params.append(normalized_conversation_id)
            pg_clauses.append("conversation_id = %s")
            pg_params.append(normalized_conversation_id)
        normalized_action_type = str(action_type or "").strip()
        if normalized_action_type:
            clauses.append("action_type = ?")
            params.append(normalized_action_type)
            pg_clauses.append("action_type = %s")
            pg_params.append(normalized_action_type)
        normalized_owner = str(owner_module or "").strip()
        if normalized_owner:
            clauses.append("owner_module = ?")
            params.append(normalized_owner)
            pg_clauses.append("owner_module = %s")
            pg_params.append(normalized_owner)
        normalized_statuses = [
            str(status or "").strip() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            placeholders = ", ".join(["?"] * len(normalized_statuses))
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
            pg_placeholders = ", ".join(["%s"] * len(normalized_statuses))
            pg_clauses.append(f"status IN ({pg_placeholders})")
            pg_params.extend(normalized_statuses)
        pg_rows = self._select_rows(
            "agent_actions",
            row_builder=self._action_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(0, int(limit or 0)),
            offset=max(0, int(offset or 0)),
        )
        if pg_rows:
            return pg_rows
        return []

    def update_action_state(
        self,
        action_id: str,
        *,
        status: str = "",
        approval_status: str = "",
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("agent_actions")
        normalized_action_id = str(action_id or "").strip()
        if not normalized_action_id:
            return {}
        existing = self.get_action(normalized_action_id)
        if not existing:
            return {}
        current_status = str(existing.get("status") or "").strip()
        requested_status = str(status or current_status or "planned").strip() or "planned"
        if current_status in {"completed", "failed", "cancelled", "rejected"} and requested_status != current_status:
            return existing
        result_ref = {**dict(existing.get("result_ref") or {}), **dict(result_ref_patch or {})}
        metadata = {**dict(existing.get("metadata") or {}), **dict(metadata_patch or {})}
        if self._should_prefer_read("agent_actions"):
            row = self._call_native_write(
                "update_agent_action_state",
                table_name="agent_actions",
                action_id=normalized_action_id,
                expected_status=current_status,
                status=requested_status,
                approval_status=str(approval_status or existing.get("approval_status") or "not_required").strip(),
                result_ref=result_ref,
                metadata=metadata,
            )
            if row is not None:
                return self._action_from_row(row)
            if self._strict_authoritative("agent_actions"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for agent_actions in update_agent_action_state: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def requeue_action_for_operation_retry(
        self,
        action_id: str,
        *,
        workspace_id: str,
        expected_status: str,
        parent_operation_run_id: str,
        retry_operation_run_id: str,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("agent_actions")
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
            return {}
        if self._should_prefer_read("agent_actions"):
            row = self._call_native_write(
                "requeue_agent_action_for_operation_retry",
                table_name="agent_actions",
                action_id=normalized_action_id,
                workspace_id=normalized_workspace_id,
                expected_status=normalized_expected_status,
                parent_operation_run_id=normalized_parent_run_id,
                retry_operation_run_id=normalized_retry_run_id,
            )
            if row is not None:
                return self._action_from_row(row)
            if self._strict_authoritative("agent_actions"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for agent_actions in requeue_agent_action_for_operation_retry: "
            "should_prefer_read returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_operation(
        self,
        *,
        operation_run_id: str,
        workspace_id: str = "default",
        action_id: str,
        owner_module: str,
        operation_type: str,
        status: str = "queued",
        progress: dict[str, Any] | None = None,
        workflow_ref: dict[str, Any] | None = None,
        cost_budget: dict[str, Any] | None = None,
        idempotency_key: str,
        result_ref: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        started_at: str = "",
        completed_at: str = "",
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("operation_runs")
        normalized_operation_id = str(operation_run_id or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_action_id = str(action_id or "").strip()
        normalized_owner = str(owner_module or "").strip()
        normalized_operation_type = str(operation_type or "").strip()
        normalized_idempotency = str(idempotency_key or "").strip()
        if (
            not normalized_operation_id
            or not normalized_workspace_id
            or not normalized_action_id
            or not normalized_owner
            or not normalized_operation_type
            or not normalized_idempotency
        ):
            return {}
        now = utc_now_timestamp()
        row_payload = OPERATION_RUNS.to_columns(
            {
                "operation_run_id": normalized_operation_id,
                "workspace_id": normalized_workspace_id,
                "action_id": normalized_action_id,
                "owner_module": normalized_owner,
                "operation_type": normalized_operation_type,
                "status": status,
                "progress": progress or {},
                "workflow_ref": workflow_ref or {},
                "cost_budget": cost_budget or {},
                "idempotency_key": normalized_idempotency,
                "result_ref": result_ref or {},
                "metadata": metadata or {},
                "started_at": started_at,
                "completed_at": completed_at,
                "created_at": now,
                "updated_at": now,
            }
        )
        if self._should_prefer_read("operation_runs"):
            row = self._call_native_write(
                "upsert_operation_run",
                table_name="operation_runs",
                row=row_payload,
            )
            if row is not None:
                return self._operation_from_row(row)
            if self._strict_authoritative("operation_runs"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for operation_runs in upsert_operation_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_operation(self, operation_run_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("operation_runs")
        normalized_operation_id = str(operation_run_id or "").strip()
        if not normalized_operation_id:
            return {}
        postgres_row = self._select_row(
            "operation_runs",
            row_builder=self._operation_from_row,
            where_sql="operation_run_id = %s",
            params=[normalized_operation_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_operations(
        self,
        *,
        workspace_id: str = "default",
        action_id: str = "",
        owner_module: str = "",
        operation_type: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("operation_runs")
        clauses: list[str] = []
        params: list[Any] = []
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "").strip()
        if normalized_workspace_id:
            clauses.append("workspace_id = ?")
            params.append(normalized_workspace_id)
            pg_clauses.append("workspace_id = %s")
            pg_params.append(normalized_workspace_id)
        normalized_action_id = str(action_id or "").strip()
        if normalized_action_id:
            clauses.append("action_id = ?")
            params.append(normalized_action_id)
            pg_clauses.append("action_id = %s")
            pg_params.append(normalized_action_id)
        normalized_owner = str(owner_module or "").strip()
        if normalized_owner:
            clauses.append("owner_module = ?")
            params.append(normalized_owner)
            pg_clauses.append("owner_module = %s")
            pg_params.append(normalized_owner)
        normalized_operation_type = str(operation_type or "").strip()
        if normalized_operation_type:
            clauses.append("operation_type = ?")
            params.append(normalized_operation_type)
            pg_clauses.append("operation_type = %s")
            pg_params.append(normalized_operation_type)
        normalized_statuses = [
            str(status or "").strip() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            placeholders = ", ".join(["?"] * len(normalized_statuses))
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
            pg_placeholders = ", ".join(["%s"] * len(normalized_statuses))
            pg_clauses.append(f"status IN ({pg_placeholders})")
            pg_params.extend(normalized_statuses)
        pg_rows = self._select_rows(
            "operation_runs",
            row_builder=self._operation_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(0, int(limit or 0)),
            offset=max(0, int(offset or 0)),
        )
        if pg_rows:
            return pg_rows
        return []

    def update_operation_state(
        self,
        operation_run_id: str,
        *,
        status: str = "",
        progress_patch: dict[str, Any] | None = None,
        workflow_ref_patch: dict[str, Any] | None = None,
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("operation_runs")
        normalized_operation_id = str(operation_run_id or "").strip()
        if not normalized_operation_id:
            return {}
        existing = self.get_operation(normalized_operation_id)
        if not existing:
            return {}
        current_status = str(existing.get("status") or "").strip()
        requested_status = str(status or current_status or "queued").strip() or "queued"
        if current_status in {"completed", "failed", "cancelled"} and requested_status != current_status:
            return existing
        progress = {**dict(existing.get("progress") or {}), **dict(progress_patch or {})}
        workflow_ref = {**dict(existing.get("workflow_ref") or {}), **dict(workflow_ref_patch or {})}
        result_ref = {**dict(existing.get("result_ref") or {}), **dict(result_ref_patch or {})}
        metadata = {**dict(existing.get("metadata") or {}), **dict(metadata_patch or {})}
        if self._should_prefer_read("operation_runs"):
            row = self._call_native_write(
                "update_operation_run_state",
                table_name="operation_runs",
                operation_run_id=normalized_operation_id,
                expected_status=current_status,
                status=requested_status,
                progress=progress,
                workflow_ref=workflow_ref,
                result_ref=result_ref,
                metadata=metadata,
            )
            if row is not None:
                return self._operation_from_row(row)
            if self._strict_authoritative("operation_runs"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for operation_runs in update_operation_run_state: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def _operation_event_row_payload(
        self,
        *,
        event_stream_id: str,
        event_family: str,
        event_type: str,
        idempotency_key: str,
        workspace_id: str = "default",
        operation_run_id: str = "",
        action_id: str = "",
        sequence_number: int = 0,
        occurred_at: str = "",
        actor: str = "",
        source: str = "",
        payload: dict[str, Any] | None = None,
        schema_version: str = "operation_event_v1",
    ) -> dict[str, Any]:
        normalized_stream_id = str(event_stream_id or "").strip()
        normalized_family = str(event_family or "").strip()
        normalized_type = str(event_type or "").strip()
        normalized_idempotency = str(idempotency_key or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if not normalized_stream_id or not normalized_family or not normalized_type or not normalized_idempotency:
            return {}
        now = utc_now_timestamp()
        row_payload = OPERATION_EVENTS.to_columns(
            {
                "event_id": "",
                "workspace_id": normalized_workspace_id,
                "event_stream_id": normalized_stream_id,
                "operation_run_id": operation_run_id,
                "action_id": action_id,
                "event_family": normalized_family,
                "event_type": normalized_type,
                "sequence_number": max(0, int(sequence_number or 0)),
                "idempotency_key": normalized_idempotency,
                "occurred_at": str(occurred_at or now).strip(),
                "recorded_at": now,
                "actor": actor,
                "source": source,
                "payload": payload or {},
                "schema_version": str(schema_version or "operation_event_v1").strip(),
                "created_at": now,
            }
        )
        return row_payload

    def append_operation_event(
        self,
        *,
        event_stream_id: str,
        event_family: str,
        event_type: str,
        idempotency_key: str,
        workspace_id: str = "default",
        operation_run_id: str = "",
        action_id: str = "",
        sequence_number: int = 0,
        occurred_at: str = "",
        actor: str = "",
        source: str = "",
        payload: dict[str, Any] | None = None,
        schema_version: str = "operation_event_v1",
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("operation_events")
        row_payload = self._operation_event_row_payload(
            event_stream_id=event_stream_id,
            event_family=event_family,
            event_type=event_type,
            idempotency_key=idempotency_key,
            workspace_id=workspace_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            sequence_number=sequence_number,
            occurred_at=occurred_at,
            actor=actor,
            source=source,
            payload=payload,
            schema_version=schema_version,
        )
        if not row_payload:
            return {}
        if self._should_prefer_read("operation_events"):
            row = self._call_native_write(
                "append_operation_event",
                table_name="operation_events",
                row=row_payload,
            )
            if row is not None:
                return self._operation_event_from_row(row)
            if self._strict_authoritative("operation_events"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for operation_events in append_operation_event: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def reject_action_with_event(
        self,
        action_id: str,
        *,
        expected_status: str,
        workspace_id: str = "default",
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        event_idempotency_key: str,
        actor: str = "",
        source: str = "",
        event_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("agent_actions")
        self._require_postgres_for_durable_runtime("operation_events")
        normalized_action_id = str(action_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        if not normalized_action_id or not normalized_expected_status:
            return {}
        event_row = self._operation_event_row_payload(
            event_stream_id=normalized_action_id,
            event_family="operation_event",
            event_type="ActionRejected",
            idempotency_key=event_idempotency_key,
            workspace_id=workspace_id,
            action_id=normalized_action_id,
            actor=actor,
            source=source,
            payload=event_payload,
        )
        if not event_row:
            return {}
        if self._should_prefer_read("agent_actions"):
            result = self._call_native_write(
                "reject_agent_action_with_event",
                table_name="agent_actions",
                action_id=normalized_action_id,
                expected_status=normalized_expected_status,
                status="cancelled",
                approval_status="rejected",
                result_ref_patch=dict(result_ref_patch or {}),
                metadata_patch=dict(metadata_patch or {}),
                event_row=event_row,
            )
            if result is not None:
                payload = dict(result)
                return {
                    "outcome": str(payload.get("outcome") or "conflict").strip() or "conflict",
                    "applied": bool(payload.get("applied")),
                    "action": self._action_from_row(payload.get("action")),
                    "event": self._operation_event_from_row(payload.get("event")),
                }
            if self._strict_authoritative("agent_actions"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for agent_actions in reject_action_with_event: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def cancel_operation_with_event(
        self,
        operation_run_id: str,
        *,
        expected_status: str,
        action_id: str = "",
        workspace_id: str = "default",
        progress_patch: dict[str, Any] | None = None,
        workflow_ref_patch: dict[str, Any] | None = None,
        result_ref_patch: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
        linked_action_metadata_patch: dict[str, Any] | None = None,
        event_idempotency_key: str,
        actor: str = "",
        source: str = "",
        event_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("operation_runs")
        self._require_postgres_for_durable_runtime("agent_actions")
        self._require_postgres_for_durable_runtime("operation_events")
        normalized_operation_id = str(operation_run_id or "").strip()
        normalized_expected_status = str(expected_status or "").strip()
        if not normalized_operation_id or not normalized_expected_status:
            return {}
        event_row = self._operation_event_row_payload(
            event_stream_id=normalized_operation_id,
            event_family="operation_event",
            event_type="OperationCancelled",
            idempotency_key=event_idempotency_key,
            workspace_id=workspace_id,
            operation_run_id=normalized_operation_id,
            action_id=action_id,
            actor=actor,
            source=source,
            payload=event_payload,
        )
        if not event_row:
            return {}
        if self._should_prefer_read("operation_runs"):
            result = self._call_native_write(
                "cancel_operation_run_with_event",
                table_name="operation_runs",
                operation_run_id=normalized_operation_id,
                expected_status=normalized_expected_status,
                status="cancelled",
                progress_patch=dict(progress_patch or {}),
                workflow_ref_patch=dict(workflow_ref_patch or {}),
                result_ref_patch=dict(result_ref_patch or {}),
                metadata_patch=dict(metadata_patch or {}),
                linked_action_metadata_patch=dict(linked_action_metadata_patch or {}),
                event_row=event_row,
            )
            if result is not None:
                payload = dict(result)
                return {
                    "outcome": str(payload.get("outcome") or "conflict").strip() or "conflict",
                    "applied": bool(payload.get("applied")),
                    "operation": self._operation_from_row(payload.get("operation")),
                    "linked_action": self._action_from_row(payload.get("linked_action")),
                    "event": self._operation_event_from_row(payload.get("event")),
                }
            if self._strict_authoritative("operation_runs"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for operation_runs in cancel_operation_with_event: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_operation_events(self, event_stream_id: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("operation_events")
        normalized_stream_id = str(event_stream_id or "").strip()
        if not normalized_stream_id:
            return []
        postgres_rows = self._select_rows(
            "operation_events",
            row_builder=self._operation_event_from_row,
            where_sql="event_stream_id = %s",
            params=[normalized_stream_id],
            order_by_sql="sequence_number ASC",
            limit=max(0, int(limit or 0)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_operation_events_for_action(self, action_id: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("operation_events")
        normalized_action_id = str(action_id or "").strip()
        if not normalized_action_id:
            return []
        postgres_rows = self._select_rows(
            "operation_events",
            row_builder=self._operation_event_from_row,
            where_sql="action_id = %s",
            params=[normalized_action_id],
            order_by_sql="recorded_at ASC, event_stream_id ASC, sequence_number ASC",
            limit=max(0, int(limit or 0)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def _action_from_row(self, row: Any) -> dict[str, Any]:
        return AGENT_ACTIONS.from_row(row)

    def _operation_from_row(self, row: Any) -> dict[str, Any]:
        return OPERATION_RUNS.from_row(row)

    def _operation_event_from_row(self, row: Any) -> dict[str, Any]:
        return OPERATION_EVENTS.from_row(row)

    def _acquisition_run_from_row(self, row: Any) -> dict[str, Any]:
        return ACQUISITION_RUNS.from_row(row)

    def _discovery_lane_from_row(self, row: Any) -> dict[str, Any]:
        return ACQUISITION_DISCOVERY_LANES.from_row(row)
