"""Track B B4.2 — workflow / operation runtime control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

from ..control_plane_repository import Column, Kind, TableDescriptor

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
        Column("approval_status"),
        Column("approval_policy"),
        Column("budget_json", Kind.JSON, field="budget"),
        Column("idempotency_key"),
        Column("status"),
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
        Column("status"),
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
        Column("status"),
        Column("phase"),
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
        Column("status"),
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
    "_agent_action_from_row": AGENT_ACTIONS,
    "_operation_run_from_row": OPERATION_RUNS,
    "_acquisition_run_from_row": ACQUISITION_RUNS,
    "_workflow_activity_run_from_row": WORKFLOW_ACTIVITY_RUNS,
    "_workflow_activity_attempt_from_row": WORKFLOW_ACTIVITY_ATTEMPTS,
    "_workflow_entity_delta_from_row": WORKFLOW_ENTITY_DELTAS,
    "_acquisition_discovery_lane_from_row": ACQUISITION_DISCOVERY_LANES,
    "_operation_event_from_row": OPERATION_EVENTS,
    "_workflow_current_state_from_row": WORKFLOW_CURRENT_STATE,
    "_workflow_command_from_row": WORKFLOW_COMMANDS,
    "_runtime_outbox_from_row": RUNTIME_OUTBOX,
}
