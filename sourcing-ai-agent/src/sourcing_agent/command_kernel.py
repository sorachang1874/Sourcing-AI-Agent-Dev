"""Command-execution kernel extracted from ``SourcingOrchestrator`` (Phase 1).

Store-only workflow-command helpers: every method here depends only on the
control-plane store (``self._store``), pure module helpers, and imported pure
functions/constants — no locks, no file IO, no cross-domain orchestrator
methods.  Bodies are moved verbatim from ``orchestrator.py``; the only edit is
``self.store`` -> ``self._store``.  ``SourcingOrchestrator`` keeps one-line
delegating wrappers with identical signatures.
"""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timezone
from typing import Any

from .durable_runtime import (
    WORKFLOW_COMMAND_CONTROL_POLICY_BOOLEAN_FIELDS,
    WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS,
    WORKFLOW_COMMAND_CONTROL_POLICY_STRING_FIELDS,
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
    workflow_command_control_state,
    workflow_command_display_contract,
)
from .operation_runtime import (
    DEFAULT_ACTION_REGISTRY,
    WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
    WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED,
)

# Public WorkflowCommand records are a security boundary. Keep this tuple
# literal and reviewable: new storage columns must not become public merely
# because a repository descriptor or row dictionary grew.
WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS = (
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
    "input_artifact_refs",
    "output_artifact_refs",
    "produced_entity_counts",
    "no_op_reason",
    "readiness_effect",
    "downstream_command_ids",
    "causality_schema_version",
    "status",
    "idempotency_key",
    "payload",
    "artifact_refs",
    "not_before_at",
    "attempt",
    "max_attempts",
    "retry_policy",
    "lease_owner",
    "lease_expires_at",
    "heartbeat_at",
    "last_error",
    "result",
    "schema_version",
    "created_at",
    "updated_at",
    "claim_generation",
    "control_epoch",
)

WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS = (
    "agent_exposure_gate",
    "agent_exposure_status",
    "display_contract",
    "control_policy",
    "control_state",
    "activity_spine_policy",
    "execution_summary",
)

WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS = (
    "status",
    "reason",
    "operation_run_id",
    "operation_status",
    "control_action",
    "command_status",
    "operation_run",
    "event",
    "workflow_command",
)

WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS = (
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
    "provider_ref",
    "input",
    "output",
    "artifact_refs",
    "entity_counts",
    "metadata",
    "created_at",
    "updated_at",
    "control_target",
    "module_state_mutated",
    "mutation_contract",
)

WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS = (
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
    "rate_limit_ref",
    "error",
    "input",
    "output",
    "artifact_refs",
    "idempotency_key",
    "metadata",
    "created_at",
    "updated_at",
    "activity_type",
    "owner",
    "control_target",
    "module_state_mutated",
    "mutation_contract",
)

WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS = (
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
    "source_ref",
    "entity_payload",
    "projection_effect",
    "artifact_refs",
    "idempotency_key",
    "metadata",
    "created_at",
    "updated_at",
    "activity_type",
    "owner",
    "control_target",
    "module_state_mutated",
    "mutation_contract",
)

WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS = (
    "target_type",
    "command_id",
    "command_type",
    "owner",
    "command_status",
    "display_contract",
    "control_policy",
    "control_state",
    "activity_spine_policy",
    "fallback_status",
)

WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS = frozenset(
    {
        "authority_id",
        "authority_seal",
        "bootstrap_authority",
        "bootstrap_authority_id",
        "bootstrap_authority_digest",
        "bootstrap_receipt",
        "claim_authority",
        "claim_authority_id",
        "claim_authority_seal",
        "claim_authority_spec_digest",
        "claim_capability",
        "claim_identity",
        "claim_receipt",
        "claim_secret",
        "claim_selection_generation",
        "claim_token",
        "claim_token_digest",
        "consumed_claim_authority_id",
        "issuer_digest",
        "issuer_revision",
        "last_heartbeat_id",
        "lease_identity",
        "lease_token",
        "scoped_review_session_bootstrap_authority",
        "scoped_review_session_bootstrap_receipt",
    }
)
_WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT = object()
_WORKFLOW_COMMAND_PUBLIC_SAFE_INTEGER_MAX = 9_007_199_254_740_991
_WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH = 32
_WORKFLOW_PUBLIC_MIRROR_MAX_NODES = 10_000
_WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS = 1_000


class _WorkflowPublicProjectionTraversal:
    """One bounded carrier walk shared by every nested public projector."""

    def __init__(self) -> None:
        self.remaining_nodes = _WORKFLOW_PUBLIC_MIRROR_MAX_NODES
        self.active_containers: set[int] = set()
        self.budget_omissions = 0

    def consume(self, *, depth: int) -> bool:
        if depth > _WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH or self.remaining_nodes <= 0:
            self.budget_omissions += 1
            return False
        self.remaining_nodes -= 1
        return True


_WORKFLOW_COMMAND_PUBLIC_SAFE_INTEGER_DIAGNOSTICS = frozenset({"claim_generation", "control_epoch"})
_WORKFLOW_COMMAND_PUBLIC_NUMBER_FIELDS = frozenset({"attempt", "max_attempts"})
_WORKFLOW_COMMAND_PUBLIC_STRING_ARRAY_FIELDS = frozenset(
    {"input_artifact_refs", "output_artifact_refs", "downstream_command_ids", "artifact_refs"}
)
_WORKFLOW_COMMAND_PUBLIC_OBJECT_FIELDS = frozenset({"produced_entity_counts", "payload", "retry_policy", "result"})
_WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_OBJECT_FIELDS = frozenset({"operation_run", "event", "workflow_command"})
_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_SAFE_INTEGER_FIELDS = frozenset({"attempt_number"})
_WORKFLOW_ACTIVITY_RUN_PUBLIC_OBJECT_FIELDS = frozenset(
    {"provider_ref", "input", "output", "entity_counts", "metadata"}
)
_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_OBJECT_FIELDS = frozenset({"rate_limit_ref", "error", "input", "output", "metadata"})
_WORKFLOW_ENTITY_DELTA_PUBLIC_OBJECT_FIELDS = frozenset(
    {"source_ref", "entity_payload", "projection_effect", "metadata"}
)
_WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_OBJECT_FIELDS = frozenset(
    {"display_contract", "control_policy", "control_state", "activity_spine_policy"}
)
_WORKFLOW_COMMAND_CONTROL_STATE_STRING_ARRAY_FIELDS = frozenset(
    {"allowed_actions", "running_cancel_prerequisites", "running_resume_prerequisites"}
)
_WORKFLOW_COMMAND_CONTROL_STATE_BOOLEAN_FIELDS = frozenset(
    {
        "can_cancel",
        "can_retry",
        "can_resume",
        "running_cancel_supported",
        "running_resume_supported",
        "module_state_mutated_on_cancel",
        "module_state_mutated_on_resume",
    }
)
_WORKFLOW_COMMAND_CONTROL_STATE_STRING_FIELDS = frozenset(
    {
        "schema_version",
        "command_type",
        "owner",
        "command_status",
        "cancel_mode",
        "retry_mode",
        "resume_mode",
        "running_cancel_delegate",
        "running_resume_delegate",
        "control_source_of_truth",
        "policy_source_of_truth",
        "fallback_status",
    }
)
_WORKFLOW_COMMAND_ACTIVITY_SPINE_BOOLEAN_FIELDS = frozenset(
    {
        "must_write_activity_run",
        "must_write_activity_attempt",
        "must_write_entity_delta",
        "downstream_activity_required",
        "agent_callable",
    }
)
_WORKFLOW_COMMAND_ACTIVITY_SPINE_STRING_FIELDS = frozenset(
    {
        "schema_version",
        "command_type",
        "owner",
        "requirement",
        "activity_table",
        "attempt_table",
        "entity_delta_table",
        "source_of_truth",
        "agent_callable_surface",
        "fallback_status",
        "migration_status",
        "deletion_condition",
    }
)
_WORKFLOW_COMMAND_DISPLAY_CONTRACT_STRING_FIELDS = frozenset(
    {
        "schema_version",
        "command_type",
        "owner",
        "display_label",
        "display_category",
        "description",
        "source_of_truth",
        "fallback_status",
    }
)
_WORKFLOW_COMMAND_EXECUTION_SUMMARY_STRING_FIELDS = frozenset({"source", "fallback_status", "latest_effect_status"})
_WORKFLOW_COMMAND_EXECUTION_SUMMARY_BOOLEAN_FIELDS = frozenset(
    {"fallback_used", "module_state_mutated", "sample_truncated"}
)
_WORKFLOW_COMMAND_EXECUTION_SUMMARY_NUMBER_FIELDS = frozenset(
    {"activity_count", "attempt_count", "entity_delta_count", "sample_limit"}
)
_WORKFLOW_COMMAND_EXECUTION_SUMMARY_NUMBER_RECORD_FIELDS = frozenset(
    {
        "activity_status_counts",
        "attempt_status_counts",
        "entity_delta_status_counts",
        "entity_delta_kind_counts",
    }
)
_WORKFLOW_COMMAND_EXECUTION_SUMMARY_OBJECT_FIELDS = frozenset(
    {
        "latest_activity",
        "latest_attempt",
        "latest_entity_delta",
    }
)
_OPERATION_ACTION_DISPLAY_CONTRACT_STRING_FIELDS = frozenset(
    {
        "schema_version",
        "action_type",
        "owner_module",
        "operation_type",
        "display_label",
        "display_category",
        "description",
        "source_of_truth",
        "fallback_status",
    }
)
_OPERATION_ACTION_PUBLIC_STRING_FIELDS = frozenset(
    {
        "action_id",
        "workspace_id",
        "conversation_id",
        "action_type",
        "owner_module",
        "operation_type",
        "approval_status",
        "approval_policy",
        "status",
        "request_schema_version",
        "request_schema_digest",
        "created_at",
        "updated_at",
    }
)
_OPERATION_ACTION_PUBLIC_OBJECT_FIELDS = frozenset(
    {"display_contract", "target_ref", "input", "budget", "result_ref", "metadata"}
)
_OPERATION_EVENT_PUBLIC_STRING_FIELDS = frozenset(
    {
        "event_id",
        "workspace_id",
        "event_stream_id",
        "operation_run_id",
        "action_id",
        "event_family",
        "event_type",
        "actor",
        "source",
        "occurred_at",
        "recorded_at",
    }
)
_OPERATION_RUN_PUBLIC_STRING_FIELDS = frozenset(
    {
        "operation_run_id",
        "workspace_id",
        "action_id",
        "owner_module",
        "operation_type",
        "status",
        "request_schema_version",
        "request_schema_digest",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    }
)
_OPERATION_RUN_PUBLIC_OBJECT_FIELDS = frozenset(
    {
        "display_contract",
        "progress",
        "workflow_ref",
        "cost_budget",
        "result_ref",
        "metadata",
        "control_state",
        "status_summary",
    }
)
_OPERATION_RUN_CONTROL_STATE_STRING_FIELDS = frozenset(
    {
        "operation_status",
        "action_status",
        "operation_phase",
        "control_source_of_truth",
        "fallback_status",
        "schema_version",
    }
)
_OPERATION_RUN_CONTROL_STATE_BOOLEAN_FIELDS = frozenset(
    {"can_dispatch", "can_cancel", "can_retry", "can_resume", "module_state_mutated_on_control"}
)
_OPERATION_RUN_STATUS_SUMMARY_STRING_FIELDS = frozenset(
    {"source", "fallback_status", "operation_status", "operation_phase", "latest_event_type"}
)
_OPERATION_RUN_STATUS_SUMMARY_BOOLEAN_FIELDS = frozenset({"fallback_used", "module_state_mutated"})
_OPERATION_RUN_STATUS_SUMMARY_NUMBER_FIELDS = frozenset({"workflow_command_count", "operation_event_count"})
_WORKFLOW_ACTIVITY_PUBLIC_JSON_ARRAY_FIELDS = frozenset({"artifact_refs"})
_WORKFLOW_ACTIVITY_PUBLIC_BOOLEAN_FIELDS = frozenset({"module_state_mutated"})
WORKFLOW_ACTIVITY_RUN_TRUSTED_DERIVED_FIELDS = frozenset(
    {"control_target", "module_state_mutated", "mutation_contract"}
)
WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS = frozenset(
    {"activity_type", "owner", "control_target", "module_state_mutated", "mutation_contract"}
)
WORKFLOW_ENTITY_DELTA_TRUSTED_DERIVED_FIELDS = WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS
_WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS = frozenset({"workflow_activity", "workflow_activity_run"})
_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS = frozenset({"workflow_activity_attempt"})
_WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS = frozenset({"workflow_entity_delta"})
_WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS = frozenset({"workflow_activities", "workflow_activity_runs"})
_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS = frozenset({"workflow_activity_attempts"})
_WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS = frozenset({"workflow_entity_deltas"})
_WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS = frozenset({"workflow_command", "latest_workflow_command"})
_WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS = frozenset({"workflow_commands"})
WORKFLOW_COMMAND_CONTROL_PUBLIC_ACTIVITY_CARRIER_FIELDS = (
    "workflow_activity",
    "workflow_activity_run",
    "workflow_activity_attempt",
    "workflow_entity_delta",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
)
_WORKFLOW_PUBLIC_HAZARDOUS_MIRROR_FIELDS = frozenset({"__proto__", "prototype", "constructor"})


def _normalized_workflow_public_safe_integer(value: Any) -> int | None:
    if type(value) not in {int, float}:
        return None
    if type(value) is float and not value.is_integer():
        return None
    normalized = int(value)
    if 0 <= normalized <= _WORKFLOW_COMMAND_PUBLIC_SAFE_INTEGER_MAX:
        return normalized
    return None


def _normalized_public_mirror_field_name(value: Any) -> str:
    if type(value) is not str:
        return ""
    raw = re.sub(r"[-\s]+", "_", value.strip())
    raw = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", raw)
    raw = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", raw)
    return re.sub(r"_+", "_", raw).strip("_").lower()


def _is_private_workflow_command_public_mirror_field(value: Any) -> bool:
    normalized = _normalized_public_mirror_field_name(value)
    compact = normalized.replace("_", "")
    for private_root in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS:
        compact_root = private_root.replace("_", "")
        if normalized == private_root or normalized.startswith(f"{private_root}_"):
            return True
        if compact == compact_root or compact.startswith(compact_root):
            return True
    return False


def _is_hazardous_workflow_public_mirror_field(value: Any) -> bool:
    return type(value) is str and value.strip().lower() in _WORKFLOW_PUBLIC_HAZARDOUS_MIRROR_FIELDS


def is_workflow_activity_public_carrier_field(value: Any) -> bool:
    normalized = _normalized_public_mirror_field_name(value)
    return normalized in (
        _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS
        | _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS
        | _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS
        | _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS
    )


def _sanitize_workflow_command_public_mirror(value: Any) -> Any:
    """Copy one bounded exact-built-in JSON tree into the public trust domain.

    Exact built-in checks are intentional: mapping/container/scalar subclasses can
    execute user code from ``items()``, iteration, ``__bool__`` or ``__str__``.
    Cycles and over-budget members are omitted locally so one malformed extension
    cannot abort the complete response.
    """

    remaining_nodes = _WORKFLOW_PUBLIC_MIRROR_MAX_NODES
    active_containers: set[int] = set()

    def _copy(item: Any, *, depth: int) -> Any:
        nonlocal remaining_nodes
        if depth > _WORKFLOW_PUBLIC_MIRROR_MAX_DEPTH or remaining_nodes <= 0:
            return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
        remaining_nodes -= 1
        item_type = type(item)
        if item is None or item_type in {str, bool, int}:
            return item
        if item_type is float:
            return item if math.isfinite(item) else _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
        if item_type is dict:
            container_id = id(item)
            if container_id in active_containers:
                return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
            active_containers.add(container_id)
            try:
                record: dict[str, Any] = {}
                for index, (key, member) in enumerate(item.items()):
                    if index >= _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS:
                        break
                    if type(key) is not str:
                        continue
                    if _is_private_workflow_command_public_mirror_field(key):
                        continue
                    if _is_hazardous_workflow_public_mirror_field(key):
                        continue
                    copied = _copy(member, depth=depth + 1)
                    if copied is not _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT:
                        record[key] = copied
                return record
            finally:
                active_containers.remove(container_id)
        if item_type in {list, tuple}:
            container_id = id(item)
            if container_id in active_containers:
                return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
            active_containers.add(container_id)
            try:
                result: list[Any] = []
                for index, member in enumerate(item):
                    if index >= _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS:
                        break
                    copied = _copy(member, depth=depth + 1)
                    if copied is not _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT:
                        result.append(copied)
                return result
            finally:
                active_containers.remove(container_id)
        return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT

    return _copy(value, depth=0)


def _project_workflow_public_mirror_fields(
    value: dict[str, Any] | None,
    *,
    fields: tuple[str, ...],
    safe_integer_fields: frozenset[str] = frozenset(),
    number_fields: frozenset[str] = frozenset(),
    string_array_fields: frozenset[str] = frozenset(),
    json_array_fields: frozenset[str] = frozenset(),
    object_fields: frozenset[str] = frozenset(),
    boolean_fields: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    sanitized_source = _sanitize_workflow_command_public_mirror(value if value is not None else {})
    source = sanitized_source if type(sanitized_source) is dict else {}
    record: dict[str, Any] = {}
    for field in fields:
        if field not in source or source[field] is None:
            continue
        if field in safe_integer_fields:
            integer_value = _normalized_workflow_public_safe_integer(source[field])
            if integer_value is not None:
                record[field] = integer_value
            continue
        value = source[field]
        if field in number_fields:
            if type(value) in {int, float} and (type(value) is int or math.isfinite(value)):
                record[field] = value
            continue
        if field in string_array_fields:
            if isinstance(value, (list, tuple)):
                record[field] = [item for item in value if isinstance(item, str)]
            continue
        sanitized = value
        if field in json_array_fields:
            if isinstance(sanitized, list):
                record[field] = sanitized
            continue
        if field in object_fields:
            if isinstance(sanitized, dict):
                record[field] = sanitized
            continue
        if field in boolean_fields:
            if type(value) is bool:
                record[field] = value
            continue
        if isinstance(sanitized, str):
            record[field] = sanitized
    return record


def _project_workflow_public_open_object(
    value: Any,
    *,
    string_fields: frozenset[str] = frozenset(),
    string_array_fields: frozenset[str] = frozenset(),
    object_fields: frozenset[str] = frozenset(),
    boolean_fields: frozenset[str] = frozenset(),
    number_fields: frozenset[str] = frozenset(),
    number_record_fields: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    sanitized = _sanitize_workflow_command_public_mirror(value)
    if not isinstance(sanitized, dict):
        return {}
    record = dict(sanitized)
    for field in string_fields:
        if field in record and not isinstance(record[field], str):
            record.pop(field, None)
    for field in string_array_fields:
        if field not in record:
            continue
        field_value = record[field]
        record[field] = [item for item in field_value if isinstance(item, str)] if isinstance(field_value, list) else []
    for field in object_fields:
        if field in record and not isinstance(record[field], dict):
            record.pop(field, None)
    for field in boolean_fields:
        if field in record and type(record[field]) is not bool:
            record.pop(field, None)
    for field in number_fields:
        if field not in record:
            continue
        field_value = record[field]
        if type(field_value) not in {int, float} or (type(field_value) is float and not math.isfinite(field_value)):
            record.pop(field, None)
    for field in number_record_fields:
        if field not in record:
            continue
        field_value = record[field]
        if not isinstance(field_value, dict):
            record[field] = {}
            continue
        record[field] = {
            str(key): item
            for key, item in field_value.items()
            if type(item) in {int, float} and (type(item) is int or math.isfinite(item))
        }
    return record


def _project_workflow_command_control_policy_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=WORKFLOW_COMMAND_CONTROL_POLICY_STRING_FIELDS,
        string_array_fields=WORKFLOW_COMMAND_CONTROL_POLICY_STRING_ARRAY_FIELDS,
        boolean_fields=WORKFLOW_COMMAND_CONTROL_POLICY_BOOLEAN_FIELDS,
    )


def _project_workflow_command_control_state_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=_WORKFLOW_COMMAND_CONTROL_STATE_STRING_FIELDS,
        string_array_fields=_WORKFLOW_COMMAND_CONTROL_STATE_STRING_ARRAY_FIELDS,
        object_fields=frozenset({"disabled_reasons"}),
        boolean_fields=_WORKFLOW_COMMAND_CONTROL_STATE_BOOLEAN_FIELDS,
    )


def _project_workflow_command_activity_spine_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=_WORKFLOW_COMMAND_ACTIVITY_SPINE_STRING_FIELDS,
        boolean_fields=_WORKFLOW_COMMAND_ACTIVITY_SPINE_BOOLEAN_FIELDS,
    )


def _project_workflow_command_display_contract_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=_WORKFLOW_COMMAND_DISPLAY_CONTRACT_STRING_FIELDS,
    )


def _project_operation_event_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=_OPERATION_EVENT_PUBLIC_STRING_FIELDS,
        object_fields=frozenset({"payload"}),
        number_fields=frozenset({"sequence_number"}),
    )


def _project_operation_action_public_mirror(value: Any) -> dict[str, Any]:
    record = _project_workflow_public_open_object(
        value,
        string_fields=_OPERATION_ACTION_PUBLIC_STRING_FIELDS,
        object_fields=_OPERATION_ACTION_PUBLIC_OBJECT_FIELDS,
    )
    if isinstance(record.get("display_contract"), dict):
        record["display_contract"] = _project_workflow_public_open_object(
            record["display_contract"],
            string_fields=_OPERATION_ACTION_DISPLAY_CONTRACT_STRING_FIELDS,
        )
    return record


def _project_operation_run_control_state_public_mirror(value: Any) -> dict[str, Any]:
    return _project_workflow_public_open_object(
        value,
        string_fields=_OPERATION_RUN_CONTROL_STATE_STRING_FIELDS,
        string_array_fields=frozenset({"allowed_actions"}),
        object_fields=frozenset({"disabled_reasons"}),
        boolean_fields=_OPERATION_RUN_CONTROL_STATE_BOOLEAN_FIELDS,
    )


def _project_operation_run_status_summary_public_mirror(value: Any) -> dict[str, Any]:
    record = _project_workflow_public_open_object(
        value,
        string_fields=_OPERATION_RUN_STATUS_SUMMARY_STRING_FIELDS,
        object_fields=frozenset({"latest_event", "latest_workflow_command"}),
        boolean_fields=_OPERATION_RUN_STATUS_SUMMARY_BOOLEAN_FIELDS,
        number_fields=_OPERATION_RUN_STATUS_SUMMARY_NUMBER_FIELDS,
        number_record_fields=frozenset({"command_status_counts"}),
    )
    if isinstance(record.get("latest_event"), dict):
        record["latest_event"] = _project_operation_event_public_mirror(record["latest_event"])
    return record


def _project_operation_run_public_mirror(value: Any) -> dict[str, Any]:
    record = _project_workflow_public_open_object(
        value,
        string_fields=_OPERATION_RUN_PUBLIC_STRING_FIELDS,
        object_fields=_OPERATION_RUN_PUBLIC_OBJECT_FIELDS,
    )
    if isinstance(record.get("display_contract"), dict):
        record["display_contract"] = _project_workflow_public_open_object(
            record["display_contract"],
            string_fields=_OPERATION_ACTION_DISPLAY_CONTRACT_STRING_FIELDS,
        )
    if isinstance(record.get("control_state"), dict):
        record["control_state"] = _project_operation_run_control_state_public_mirror(record["control_state"])
    if isinstance(record.get("status_summary"), dict):
        record["status_summary"] = _project_operation_run_status_summary_public_mirror(record["status_summary"])
    return record


# NOTE: the three helpers below duplicate module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class CommandKernel:
    """Store-only execution kernel for workflow-command bookkeeping."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def _workflow_command_observation(
        self,
        command: dict[str, Any] | None,
        *,
        migration_phase: str,
    ) -> dict[str, Any]:
        payload = self._workflow_command_api_record(command or {})
        record = {
            key: payload.get(key)
            for key in (
                "workflow_run_id",
                "operation_id",
                "command_id",
                "command_type",
                "owner",
                "stage_id",
                "causal_group_id",
                "parent_command_id",
                "source_event_id",
                "source_event_type",
                "input_artifact_refs",
                "output_artifact_refs",
                "produced_entity_counts",
                "no_op_reason",
                "readiness_effect",
                "downstream_command_ids",
                "causality_schema_version",
                "idempotency_key",
                "status",
                "attempt",
                "last_error",
            )
            if payload.get(key) not in (None, "", [], {})
        }
        if record:
            record.update(self._workflow_command_agent_exposure_record(str(record.get("command_type") or "")))
            record["migration_phase"] = str(migration_phase or "").strip() or "durable_runtime_command_owner"
            record["normal_path"] = True
        return record

    def _workflow_command_from_apply_result_or_store(
        self,
        *,
        apply_result: Any,
        workflow_run_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            return {}
        for command_payload in list(getattr(apply_result, "commands", ()) or ()):
            command = dict(command_payload or {})
            if str(command.get("idempotency_key") or "").strip() == normalized_idempotency_key:
                return command
        for existing_command in self._store.list_workflow_commands(
            workflow_run_id=str(workflow_run_id or "").strip(),
            limit=0,
        ):
            command = dict(existing_command or {})
            if str(command.get("idempotency_key") or "").strip() == normalized_idempotency_key:
                return command
        return {}

    @staticmethod
    def _command_owned_item_result(
        *,
        status: str,
        reason: str,
        item_id: str = "",
        metadata: dict[str, Any] | None = None,
        serving_projection_id: str = "",
        result_view_id: str = "",
        result_patch_id: str = "",
        last_error: str = "",
    ) -> dict[str, Any]:
        return {
            "item_id": str(item_id or "").strip(),
            "status": str(status or "").strip(),
            "phase": str(status or "").strip(),
            "reason": str(reason or "").strip(),
            "serving_projection_id": str(serving_projection_id or "").strip(),
            "result_view_id": str(result_view_id or "").strip(),
            "result_patch_id": str(result_patch_id or "").strip(),
            "last_error": str(last_error or "").strip(),
            "metadata": dict(metadata or {}),
            "command_owned_payload": True,
        }

    def _start_workflow_command_activity_attempt(
        self,
        command: dict[str, Any],
        *,
        activity_type: str,
        owner: str,
        phase: str,
        lease_owner: str,
        provider: str,
        provider_request_ref: str,
        input_payload: dict[str, Any],
        entity_counts: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        attempt_suffix: str = "",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(command_payload.get("operation_id") or payload.get("operation_run_id") or "").strip()
        if not command_id or not workflow_run_id:
            return {}, {}
        normalized_activity_type = str(activity_type or "").strip()
        normalized_owner = str(owner or "").strip()
        normalized_phase = str(phase or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        activity = self._store.repos.workflow_runtime.upsert_activity_run(
            {
                "workspace_id": workspace_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "command_id": command_id,
                "activity_type": normalized_activity_type,
                "owner": normalized_owner,
                "status": "running",
                "phase": normalized_phase,
                "idempotency_key": f"workflow_activity:{normalized_activity_type}:{command_id}",
                "input": dict(input_payload or {}),
                "output": {},
                "artifact_refs": [],
                "entity_counts": dict(entity_counts or {}),
                "metadata": {
                    **dict(metadata or {}),
                    "lease_owner": str(lease_owner or "").strip(),
                    "workflow_command_id": command_id,
                    "workflow_command_type": str(command_payload.get("command_type") or "").strip(),
                    "workflow_command_owner": str(command_payload.get("owner") or "").strip(),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
            }
        )
        activity_run_id = str(activity.get("activity_run_id") or "").strip()
        attempt_number = max(1, _coerce_int(command_payload.get("attempt"), 1))
        normalized_suffix = str(attempt_suffix or normalized_phase or "attempt").strip()
        attempt_key = hashlib.sha1(
            f"{normalized_activity_type}:{normalized_suffix}:{attempt_number}".encode("utf-8")
        ).hexdigest()[:24]
        attempt = self._store.repos.workflow_runtime.upsert_activity_attempt(
            {
                "workspace_id": workspace_id,
                "activity_run_id": activity_run_id,
                "workflow_run_id": workflow_run_id,
                "command_id": command_id,
                "attempt_number": attempt_number,
                "status": "running",
                "provider": str(provider or normalized_owner).strip(),
                "provider_request_ref": str(provider_request_ref or f"{normalized_activity_type}:{command_id}").strip(),
                "started_at": _utc_now_iso(),
                "input": dict(input_payload or {}),
                "output": {},
                "artifact_refs": [],
                "error": {},
                "idempotency_key": f"workflow_activity_attempt:{command_id}:{attempt_key}",
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
            }
        )
        return activity, attempt

    def _finish_workflow_command_activity_attempt(
        self,
        *,
        activity: dict[str, Any],
        attempt: dict[str, Any],
        status: str,
        phase: str,
        output: dict[str, Any],
        entity_counts: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | tuple[str, ...] | None = None,
        attempt_status: str = "",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not activity or not attempt:
            return {}, {}
        normalized_artifact_refs = [
            str(ref or "").strip() for ref in list(artifact_refs or []) if str(ref or "").strip()
        ]
        normalized_status = str(status or "").strip() or "succeeded"
        normalized_attempt_status = str(attempt_status or normalized_status).strip() or normalized_status
        completed_at = (
            _utc_now_iso() if normalized_attempt_status in {"succeeded", "failed", "retry_wait", "cancelled"} else ""
        )
        final_attempt = self._store.repos.workflow_runtime.upsert_activity_attempt(
            {
                **attempt,
                "status": normalized_attempt_status,
                "completed_at": completed_at,
                "output": dict(output or {}),
                "error": dict(error or {}),
                "artifact_refs": normalized_artifact_refs or list(attempt.get("artifact_refs") or []),
            }
        )
        final_activity = self._store.repos.workflow_runtime.upsert_activity_run(
            {
                **activity,
                "status": normalized_status,
                "phase": str(phase or normalized_status).strip(),
                "output": {
                    **dict(activity.get("output") or {}),
                    **dict(output or {}),
                    "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                },
                "entity_counts": {
                    **dict(activity.get("entity_counts") or {}),
                    **dict(entity_counts or {}),
                },
                "artifact_refs": normalized_artifact_refs or list(activity.get("artifact_refs") or []),
                "metadata": {
                    **dict(activity.get("metadata") or {}),
                    **dict(metadata or {}),
                    "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                },
            }
        )
        return final_activity, final_attempt

    def _workflow_command_control_policy_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_control_policy(command_type=command_type, owner=owner).to_record()

    def _workflow_command_control_state_record(
        self,
        *,
        command_status: str,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_control_state(
            command_status=command_status,
            command_type=command_type,
            owner=owner,
        ).to_record()

    def _workflow_command_activity_spine_policy_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_activity_spine_policy(command_type=command_type, owner=owner).to_record()

    def _workflow_command_display_contract_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_display_contract(command_type=command_type, owner=owner).to_record()

    def _workflow_command_agent_exposure_record(self, command_type: str) -> dict[str, Any]:
        normalized_type = str(command_type or "").strip()
        action_registry = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
        action_allowlisted_commands = {
            str(allowed_command_type or "").strip()
            for action_record in action_registry.values()
            for allowed_command_type in list(action_record.get("allowed_workflow_command_types") or [])
            if str(allowed_command_type or "").strip()
        }
        return {
            "agent_exposure_gate": WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
            "agent_exposure_status": (
                WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED
                if normalized_type in action_allowlisted_commands
                else "not_action_registry_allowlisted"
            ),
        }

    def _workflow_command_api_record(
        self,
        command: dict[str, Any],
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        record = _project_workflow_public_mirror_fields(
            command,
            fields=WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS,
            safe_integer_fields=_WORKFLOW_COMMAND_PUBLIC_SAFE_INTEGER_DIAGNOSTICS,
            number_fields=_WORKFLOW_COMMAND_PUBLIC_NUMBER_FIELDS,
            string_array_fields=_WORKFLOW_COMMAND_PUBLIC_STRING_ARRAY_FIELDS,
            object_fields=_WORKFLOW_COMMAND_PUBLIC_OBJECT_FIELDS,
        )
        command_type = str(record.get("command_type") or "").strip()
        owner = str(record.get("owner") or "").strip()
        record.update(self._workflow_command_agent_exposure_record(command_type))
        record["display_contract"] = self._workflow_command_display_contract_record(
            command_type=command_type,
            owner=owner,
        )
        record["control_policy"] = self._workflow_command_control_policy_record(
            command_type=command_type,
            owner=owner,
        )
        record["control_state"] = self._workflow_command_control_state_record(
            command_status=str(record.get("status") or "").strip(),
            command_type=command_type,
            owner=owner,
        )
        record["activity_spine_policy"] = self._workflow_command_activity_spine_policy_record(
            command_type=command_type,
            owner=owner,
        )
        sanitized = _sanitize_workflow_command_public_mirror(record)
        projected = self._workflow_public_carriers_api_record(
            sanitized,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_command_nested_public_carriers_api_record(
        self,
        value: Any,
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> Any:
        return self._workflow_public_carriers_api_record(
            value,
            traversal=_traversal or _WorkflowPublicProjectionTraversal(),
            depth=_depth,
        )

    def _workflow_activity_public_api_record(
        self,
        activity: dict[str, Any] | None,
        *,
        include_trusted_derived: bool = False,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        record = _project_workflow_public_mirror_fields(
            activity,
            fields=WORKFLOW_ACTIVITY_RUN_PUBLIC_FIELDS,
            json_array_fields=_WORKFLOW_ACTIVITY_PUBLIC_JSON_ARRAY_FIELDS,
            object_fields=_WORKFLOW_ACTIVITY_RUN_PUBLIC_OBJECT_FIELDS,
            boolean_fields=_WORKFLOW_ACTIVITY_PUBLIC_BOOLEAN_FIELDS,
        )
        record = self._workflow_activity_record_with_closed_control_target(record, source=activity)
        if not include_trusted_derived:
            for field in WORKFLOW_ACTIVITY_RUN_TRUSTED_DERIVED_FIELDS:
                record.pop(field, None)
        projected = self._workflow_public_carriers_api_record(
            record,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_activity_attempt_public_api_record(
        self,
        attempt: dict[str, Any] | None,
        *,
        include_trusted_derived: bool = False,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        record = _project_workflow_public_mirror_fields(
            attempt,
            fields=WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_FIELDS,
            safe_integer_fields=_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_SAFE_INTEGER_FIELDS,
            json_array_fields=_WORKFLOW_ACTIVITY_PUBLIC_JSON_ARRAY_FIELDS,
            object_fields=_WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_OBJECT_FIELDS,
            boolean_fields=_WORKFLOW_ACTIVITY_PUBLIC_BOOLEAN_FIELDS,
        )
        record = self._workflow_activity_record_with_closed_control_target(record, source=attempt)
        if not include_trusted_derived:
            for field in WORKFLOW_ACTIVITY_ATTEMPT_TRUSTED_DERIVED_FIELDS:
                record.pop(field, None)
        projected = self._workflow_public_carriers_api_record(
            record,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_entity_delta_public_api_record(
        self,
        delta: dict[str, Any] | None,
        *,
        include_trusted_derived: bool = False,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        record = _project_workflow_public_mirror_fields(
            delta,
            fields=WORKFLOW_ENTITY_DELTA_PUBLIC_FIELDS,
            json_array_fields=_WORKFLOW_ACTIVITY_PUBLIC_JSON_ARRAY_FIELDS,
            object_fields=_WORKFLOW_ENTITY_DELTA_PUBLIC_OBJECT_FIELDS,
            boolean_fields=_WORKFLOW_ACTIVITY_PUBLIC_BOOLEAN_FIELDS,
        )
        record = self._workflow_activity_record_with_closed_control_target(record, source=delta)
        if not include_trusted_derived:
            for field in WORKFLOW_ENTITY_DELTA_TRUSTED_DERIVED_FIELDS:
                record.pop(field, None)
        projected = self._workflow_public_carriers_api_record(
            record,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_activity_control_target_public_api_record(
        self,
        control_target: dict[str, Any] | None,
    ) -> dict[str, Any]:
        record = _project_workflow_public_mirror_fields(
            control_target,
            fields=WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_FIELDS,
            object_fields=_WORKFLOW_ACTIVITY_CONTROL_TARGET_PUBLIC_OBJECT_FIELDS,
        )
        nested_projectors = {
            "display_contract": _project_workflow_command_display_contract_public_mirror,
            "control_policy": _project_workflow_command_control_policy_public_mirror,
            "control_state": _project_workflow_command_control_state_public_mirror,
            "activity_spine_policy": _project_workflow_command_activity_spine_public_mirror,
        }
        for field, projector in nested_projectors.items():
            if isinstance(record.get(field), dict):
                record[field] = projector(record[field])
        return record

    def _workflow_activity_record_with_closed_control_target(
        self,
        record: dict[str, Any],
        *,
        source: dict[str, Any] | None,
    ) -> dict[str, Any]:
        record.pop("control_target", None)
        sanitized_source = _sanitize_workflow_command_public_mirror(source if source is not None else {})
        source_control_target = sanitized_source.get("control_target") if type(sanitized_source) is dict else None
        if type(source_control_target) is dict:
            projected_control_target = self._workflow_activity_control_target_public_api_record(source_control_target)
            if projected_control_target:
                record["control_target"] = projected_control_target
        return record

    def _workflow_command_trusted_execution_summary_api_record(
        self,
        execution_summary: dict[str, Any] | None,
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        sanitized_source = _sanitize_workflow_command_public_mirror(
            execution_summary if execution_summary is not None else {}
        )
        source = sanitized_source if type(sanitized_source) is dict else {}
        projected = _project_workflow_public_open_object(
            source,
            string_fields=_WORKFLOW_COMMAND_EXECUTION_SUMMARY_STRING_FIELDS,
            object_fields=_WORKFLOW_COMMAND_EXECUTION_SUMMARY_OBJECT_FIELDS,
            boolean_fields=_WORKFLOW_COMMAND_EXECUTION_SUMMARY_BOOLEAN_FIELDS,
            number_fields=_WORKFLOW_COMMAND_EXECUTION_SUMMARY_NUMBER_FIELDS,
            number_record_fields=_WORKFLOW_COMMAND_EXECUTION_SUMMARY_NUMBER_RECORD_FIELDS,
        )
        latest_projectors = {
            "latest_activity": lambda value: self._workflow_activity_public_api_record(
                value,
                include_trusted_derived=True,
                _traversal=traversal,
                _depth=_depth + 1,
            ),
            "latest_attempt": lambda value: self._workflow_activity_attempt_public_api_record(
                value,
                include_trusted_derived=True,
                _traversal=traversal,
                _depth=_depth + 1,
            ),
            "latest_entity_delta": lambda value: self._workflow_entity_delta_public_api_record(
                value,
                include_trusted_derived=True,
                _traversal=traversal,
                _depth=_depth + 1,
            ),
        }
        for field in latest_projectors:
            projected.pop(field, None)
        traversed = self._workflow_public_carriers_api_record(
            projected,
            traversal=traversal,
            depth=_depth,
        )
        record = traversed if type(traversed) is dict else {}
        for field, projector in latest_projectors.items():
            value = source.get(field)
            if type(value) is dict:
                record[field] = projector(value)
            else:
                record.pop(field, None)
        return record

    def _workflow_public_carriers_api_record(
        self,
        value: Any,
        *,
        traversal: _WorkflowPublicProjectionTraversal,
        depth: int,
    ) -> Any:
        """Project command and Activity carriers in one non-reentrant walk."""

        if not traversal.consume(depth=depth):
            return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
        value_type = type(value)
        if value is None or value_type in {str, bool, int, float}:
            return value
        if value_type not in {dict, list}:
            return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
        container_id = id(value)
        if container_id in traversal.active_containers:
            return _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT
        traversal.active_containers.add(container_id)
        try:
            if value_type is list:
                result: list[Any] = []
                for index, item in enumerate(value):
                    if index >= _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS:
                        break
                    projected = self._workflow_public_carriers_api_record(
                        item,
                        traversal=traversal,
                        depth=depth + 1,
                    )
                    if projected is not _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT:
                        result.append(projected)
                return result

            record: dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= _WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS:
                    break
                if type(key) is not str:
                    continue
                normalized_key = _normalized_public_mirror_field_name(key)
                if normalized_key == "execution_summary":
                    continue
                if normalized_key in _WORKFLOW_COMMAND_PUBLIC_CARRIER_FIELDS:
                    if type(item) is dict and traversal.remaining_nodes > 0:
                        omissions_before = traversal.budget_omissions
                        projected_command = self._workflow_command_api_record(
                            item,
                            _traversal=traversal,
                            _depth=depth + 1,
                        )
                        if projected_command or traversal.budget_omissions == omissions_before:
                            record[key] = projected_command
                    continue
                if normalized_key in _WORKFLOW_COMMAND_PUBLIC_CARRIER_LIST_FIELDS:
                    if type(item) is list:
                        members: list[dict[str, Any]] = []
                        for member in item[:_WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS]:
                            if type(member) is not dict or traversal.remaining_nodes <= 0:
                                continue
                            omissions_before = traversal.budget_omissions
                            projected_command = self._workflow_command_api_record(
                                member,
                                _traversal=traversal,
                                _depth=depth + 1,
                            )
                            if projected_command or traversal.budget_omissions == omissions_before:
                                members.append(projected_command)
                        record[key] = members
                    continue
                activity_projector = None
                if normalized_key in _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_FIELDS:
                    activity_projector = self._workflow_activity_public_api_record
                elif normalized_key in _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_FIELDS:
                    activity_projector = self._workflow_activity_attempt_public_api_record
                elif normalized_key in _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_FIELDS:
                    activity_projector = self._workflow_entity_delta_public_api_record
                if activity_projector is not None:
                    if type(item) is dict and traversal.remaining_nodes > 0:
                        omissions_before = traversal.budget_omissions
                        projected_activity = activity_projector(
                            item,
                            _traversal=traversal,
                            _depth=depth + 1,
                        )
                        if projected_activity or traversal.budget_omissions == omissions_before:
                            record[key] = projected_activity
                    continue
                activity_list_projector = None
                if normalized_key in _WORKFLOW_ACTIVITY_RUN_PUBLIC_CARRIER_LIST_FIELDS:
                    activity_list_projector = self._workflow_activity_public_api_record
                elif normalized_key in _WORKFLOW_ACTIVITY_ATTEMPT_PUBLIC_CARRIER_LIST_FIELDS:
                    activity_list_projector = self._workflow_activity_attempt_public_api_record
                elif normalized_key in _WORKFLOW_ENTITY_DELTA_PUBLIC_CARRIER_LIST_FIELDS:
                    activity_list_projector = self._workflow_entity_delta_public_api_record
                if activity_list_projector is not None:
                    if type(item) is list:
                        members = []
                        for member in item[:_WORKFLOW_PUBLIC_MIRROR_MAX_COLLECTION_ITEMS]:
                            if type(member) is not dict or traversal.remaining_nodes <= 0:
                                continue
                            omissions_before = traversal.budget_omissions
                            projected_activity = activity_list_projector(
                                member,
                                _traversal=traversal,
                                _depth=depth + 1,
                            )
                            if projected_activity or traversal.budget_omissions == omissions_before:
                                members.append(projected_activity)
                        record[key] = members
                    continue
                projected = self._workflow_public_carriers_api_record(
                    item,
                    traversal=traversal,
                    depth=depth + 1,
                )
                if projected is not _WORKFLOW_COMMAND_PUBLIC_MIRROR_OMIT:
                    record[key] = projected
            return record
        finally:
            traversal.active_containers.remove(container_id)

    def _workflow_activity_public_carriers_api_record(
        self,
        value: Any,
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> Any:
        return self._workflow_public_carriers_api_record(
            value,
            traversal=_traversal or _WorkflowPublicProjectionTraversal(),
            depth=_depth,
        )

    def _operation_action_public_api_record(
        self,
        action: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._workflow_command_public_carrier_api_record(_project_operation_action_public_mirror(action))

    def _operation_event_public_api_record(
        self,
        event: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._workflow_command_public_carrier_api_record(_project_operation_event_public_mirror(event))

    def _operation_run_public_api_record(
        self,
        operation_run: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._workflow_command_public_carrier_api_record(_project_operation_run_public_mirror(operation_run))

    def _workflow_command_operation_sync_api_record(
        self,
        operation_sync: dict[str, Any] | None,
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        sanitized_source = _sanitize_workflow_command_public_mirror(
            operation_sync if operation_sync is not None else {}
        )
        source = sanitized_source if type(sanitized_source) is dict else {}
        record = _project_workflow_public_mirror_fields(
            source,
            fields=WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS,
            object_fields=_WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_OBJECT_FIELDS,
        )
        if isinstance(record.get("operation_run"), dict):
            record["operation_run"] = _project_operation_run_public_mirror(record["operation_run"])
        if isinstance(record.get("event"), dict):
            record["event"] = _project_operation_event_public_mirror(record["event"])
        projected = self._workflow_public_carriers_api_record(
            record,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_command_public_carrier_api_record(
        self,
        carrier: dict[str, Any] | None,
        *,
        _traversal: _WorkflowPublicProjectionTraversal | None = None,
        _depth: int = 0,
    ) -> dict[str, Any]:
        traversal = _traversal or _WorkflowPublicProjectionTraversal()
        sanitized = _sanitize_workflow_command_public_mirror(carrier if carrier is not None else {})
        if type(sanitized) is not dict:
            return {}
        projected = self._workflow_public_carriers_api_record(
            sanitized,
            traversal=traversal,
            depth=_depth,
        )
        return projected if type(projected) is dict else {}

    def _workflow_command_control_response_policy_records(
        self,
        command: dict[str, Any],
    ) -> dict[str, Any]:
        command_type = str((command or {}).get("command_type") or "").strip()
        owner = str((command or {}).get("owner") or "").strip()
        command_status = str((command or {}).get("status") or "").strip()
        return {
            "control_policy": self._workflow_command_control_policy_record(
                command_type=command_type,
                owner=owner,
            ),
            "control_state": self._workflow_command_control_state_record(
                command_status=command_status,
                command_type=command_type,
                owner=owner,
            ),
            "display_contract": self._workflow_command_display_contract_record(
                command_type=command_type,
                owner=owner,
            ),
            "activity_spine_policy": self._workflow_command_activity_spine_policy_record(
                command_type=command_type,
                owner=owner,
            ),
        }

    def _workflow_command_lease_active(self, command: dict[str, Any]) -> bool:
        command_payload = dict(command or {})
        lease_owner = str(command_payload.get("lease_owner") or "").strip()
        lease_expires_at = str(command_payload.get("lease_expires_at") or "").strip()
        if not lease_owner or not lease_expires_at:
            return False
        parsed = _parse_timestamp(lease_expires_at)
        return bool(parsed and parsed > datetime.now(timezone.utc))

    def _workflow_command_is_cancelled(self, command_id: str) -> bool:
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return False
        current = self._store.get_workflow_command(normalized_command_id) or {}
        return str(current.get("status") or "").strip().lower() in {"cancelled", "canceled"}

    def _sync_operation_run_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self._store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            operation_run_id = str(dict(command_payload.get("payload") or {}).get("operation_id") or "").strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self._store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status == "succeeded":
            command_result = dict(command_payload.get("result") or {})
            result_operation_phase = str(command_result.get("operation_phase") or "").strip()
            if bool(command_result.get("operation_completion_deferred")):
                next_status = "running"
                event_type = "OperationCommandDownstreamQueued"
                phase = result_operation_phase or "workflow_command_downstream_queued"
                action_status = "running"
            else:
                next_status = "completed"
                event_type = "OperationCommandSucceeded"
                phase = result_operation_phase or "workflow_command_succeeded"
                action_status = "completed"
        elif command_status == "failed_terminal":
            next_status = "failed"
            event_type = "OperationCommandFailed"
            phase = "workflow_command_failed"
            action_status = "failed"
        elif command_status == "retry_wait":
            next_status = "planned"
            event_type = "OperationCommandRetryWaiting"
            phase = "workflow_command_retry_wait"
            action_status = "planned"
        else:
            return {
                "status": "skipped",
                "reason": "workflow_command_not_syncable",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        command_result = dict(command_payload.get("result") or {})
        operation_patch = self._store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status=next_status,
            progress_patch={
                "phase": phase,
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "workflow_command_last_error": str(command_payload.get("last_error") or "").strip(),
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self._store.repos.workflow_runtime.update_action_state(
                action_id,
                status=action_status,
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                },
                metadata_patch={"last_operation_command_status": command_status},
            )
        event = self._store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(
                operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default"
            ).strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type=event_type,
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:{event_type}:{command_id}:"
                f"{command_status}:{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "command_result": command_result,
                "last_error": str(command_payload.get("last_error") or "").strip(),
                "module_state_mutated": False,
            },
        )
        return self._workflow_command_operation_sync_api_record(
            {
                "status": next_status,
                "operation_run": operation_patch,
                "event": event,
                "workflow_command": self._workflow_command_api_record(command_payload),
            }
        )

    def _record_command_activity_entity_delta(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        entity_type: str,
        entity_key: str,
        delta_kind: str,
        status: str,
        reason: str,
        source_ref: dict[str, Any] | None = None,
        entity_payload: dict[str, Any] | None = None,
        projection_effect: dict[str, Any] | None = None,
        artifact_refs: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_scope: str = "",
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        if not activity:
            return {}
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        command_id = str(command_payload.get("command_id") or "").strip()
        normalized_entity_type = str(entity_type or "").strip()
        normalized_delta_kind = str(delta_kind or "").strip()
        normalized_entity_key = str(entity_key or "").strip()
        if not workflow_run_id or not command_id or not normalized_entity_type or not normalized_delta_kind:
            return {}
        return self._store.repos.workflow_runtime.upsert_entity_delta(
            {
                "workspace_id": str(dict(command_payload.get("payload") or {}).get("workspace_id") or "default").strip()
                or "default",
                "workflow_run_id": workflow_run_id,
                "operation_run_id": str(command_payload.get("operation_id") or "").strip(),
                "command_id": command_id,
                "activity_run_id": str(activity.get("activity_run_id") or "").strip(),
                "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                "entity_type": normalized_entity_type,
                "entity_key": normalized_entity_key,
                "delta_kind": normalized_delta_kind,
                "status": str(status or "recorded").strip() or "recorded",
                "reason": str(reason or "").strip(),
                "source_ref": dict(source_ref or {}),
                "entity_payload": dict(entity_payload or {}),
                "projection_effect": dict(projection_effect or {"entered_projection": False}),
                "artifact_refs": [
                    str(ref or "").strip() for ref in list(artifact_refs or []) if str(ref or "").strip()
                ],
                "metadata": {
                    **dict(metadata or {}),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
                "idempotency_key": (
                    f"workflow_command_entity_delta:{idempotency_scope or normalized_delta_kind}:"
                    f"{command_id}:{normalized_entity_type}:{normalized_entity_key}"
                ),
            }
        )

    def _append_completed_workflow_reconcile_event(
        self,
        job_id: str,
        *,
        phase: str,
        reconcile_kind: str,
        status: str = "running",
        detail: str = "",
        snapshot_id: str = "",
        workers: list[dict[str, Any]] | None = None,
        worker_ids: list[int] | None = None,
        payload: dict[str, Any] | None = None,
        materialize_call: bool = False,
        materialize_signature: str = "",
        marker_backfill_count: int = 0,
        lease_acquired: bool | None = None,
        skip_reason: str = "",
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        resolved_worker_ids: list[int] = []
        seen_worker_ids: set[int] = set()
        for value in list(worker_ids or []):
            try:
                worker_id = int(value or 0)
            except (TypeError, ValueError):
                continue
            if worker_id > 0 and worker_id not in seen_worker_ids:
                seen_worker_ids.add(worker_id)
                resolved_worker_ids.append(worker_id)
        for worker in list(workers or []):
            try:
                worker_id = int(dict(worker or {}).get("worker_id") or 0)
            except (TypeError, ValueError):
                continue
            if worker_id > 0 and worker_id not in seen_worker_ids:
                seen_worker_ids.add(worker_id)
                resolved_worker_ids.append(worker_id)
        extra_payload = dict(payload or {})
        sync_result = dict(extra_payload.get("sync_result") or {})
        resolved_snapshot_id = str(snapshot_id or extra_payload.get("snapshot_id") or "").strip()
        resolved_reconcile_kind = str(reconcile_kind or extra_payload.get("reconcile_kind") or "unknown").strip()
        resolved_phase = str(phase or extra_payload.get("phase") or "unknown").strip()
        resolved_materialize_signature = str(materialize_signature or "").strip()
        if materialize_call and not resolved_materialize_signature:
            materialize_reason = str(sync_result.get("reason") or extra_payload.get("reason") or resolved_phase).strip()
            materialize_worker_ids = ",".join(str(item) for item in sorted(resolved_worker_ids))
            resolved_materialize_signature = "|".join(
                item
                for item in (
                    resolved_snapshot_id,
                    resolved_reconcile_kind,
                    materialize_worker_ids,
                    materialize_reason,
                )
                if item
            )
        event_payload = dict(extra_payload)
        event_payload.update(
            {
                "event_family": "completed_workflow_reconcile",
                "phase": resolved_phase,
                "reconcile_kind": resolved_reconcile_kind,
                "snapshot_id": resolved_snapshot_id,
                "worker_ids": resolved_worker_ids,
                "worker_count": len(resolved_worker_ids),
                "materialize_call": bool(materialize_call),
                "materialize_signature": resolved_materialize_signature,
                "marker_backfill_count": max(0, int(marker_backfill_count or 0)),
            }
        )
        if lease_acquired is not None:
            event_payload["lease_acquired"] = bool(lease_acquired)
        if skip_reason:
            event_payload["skip_reason"] = str(skip_reason or "").strip()
        if sync_result:
            event_payload["sync_status"] = str(sync_result.get("status") or "").strip()
            event_payload["sync_reason"] = str(sync_result.get("reason") or "").strip()
            event_payload["sync_result"] = {key: value for key, value in sync_result.items() if key != "state_updates"}
        resolved_detail = str(detail or "").strip()
        if not resolved_detail:
            resolved_detail = f"Completed workflow reconcile {resolved_phase}."
        self._store.append_job_event(
            normalized_job_id,
            "completed",
            str(status or "running").strip() or "running",
            resolved_detail,
            payload=event_payload,
        )
        return event_payload
