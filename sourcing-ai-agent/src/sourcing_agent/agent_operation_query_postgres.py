"""Physical-owner adapter for the local Agent ``inspect_operation`` query.

The V3 query contract is intentionally storage-free.  This module is the
PostgreSQL integration leaf that rebuilds that contract from the exact linked
AgentAction, OperationRun, operation event revision, and bounded command
provenance.  It does not reserve or mutate a result slot; the shared result
acceptance UoW owns those writes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .acquisition_start_command_acceptance import (
    ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION,
    AcquisitionStartCommandAcceptanceError,
    AcquisitionStartCommandAcceptanceEvent,
)
from .acquisition_start_v2 import (
    ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
    AcquisitionConfirmationReceipt,
    AcquisitionParentBudgetEnvelopeRef,
    AcquisitionStartV2BoundRequest,
    AcquisitionStartV2Error,
    AcquisitionStartV2RootCommandPayload,
)
from .acquisition_start_v2_control import acquisition_start_v2_generic_operation_control_preflight
from .action_result_schema import ActionResultSpec
from .agent_projection_query import (
    InspectOperationBoundRequest,
    bind_inspect_operation_request,
    execute_inspect_operation_for_result_spec,
    inspect_operation_control_policy_projection,
    inspect_operation_error_result,
    operation_result_readiness_projection,
    resolve_inspect_operation_result_spec,
    serialize_inspect_operation_result_for_spec,
)
from .agent_tool_registry import AgentToolSpec
from .agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from .durable_runtime import (
    DEFAULT_COMMAND_OWNER_REGISTRY,
)
from .json_contract import json_contract_equal
from .operation_runtime import (
    DEFAULT_ACTION_REGISTRY,
    operation_run_control_state,
    operation_run_control_state_fail_closed,
)
from .workflow_progressed_child_contract import (
    build_acquisition_root_intent_plan,
    canonical_progressed_child_identity,
    expected_progressed_child_row,
    progressed_child_completion_contract_for,
    progressed_child_plan_event_violation,
)

INSPECT_OPERATION_OWNER_TARGET_KIND = "operation_state_event_v1"
INSPECT_OPERATION_MASKED_ABSENCE_OWNER_TARGET_KIND = "inspect_operation_masked_error_v1"

_WORKFLOW_REF_FIELDS = ("workflow_run_id", "command_id", "command_type", "owner")
_OPERATION_EVENT_SCHEMA_BY_TYPE = {
    "OperationCommandPlanned": {
        "operation_event_v1",
        ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION,
    },
}


def _operation_event_schema_matches(event_type: object, schema_version: object) -> bool:
    normalized_event_type = str(event_type or "").strip()
    normalized_schema_version = str(schema_version or "").strip()
    allowed_versions = _OPERATION_EVENT_SCHEMA_BY_TYPE.get(normalized_event_type, {"operation_event_v1"})
    return normalized_schema_version in allowed_versions


@dataclass(frozen=True, slots=True)
class _InspectOperationContractBinding:
    tool_spec: AgentToolSpec
    result_spec: ActionResultSpec
    physical_owner_revision: str
    include_progress_reason: bool


@dataclass(frozen=True, slots=True)
class _InspectOperationPreflight:
    """Exact historical contract plus the one server-bound request."""

    contract: _InspectOperationContractBinding
    request: InspectOperationBoundRequest


def _resolve_inspect_operation_contract(occurrence: AgentToolOccurrence) -> _InspectOperationContractBinding:
    """Resolve one exact retained tool/result contract without current-version inference."""

    from .agent_canary_registry import (
        INSPECT_OPERATION_TOOL_SPEC_V1,
        INSPECT_OPERATION_TOOL_SPEC_V2,
        INSPECT_OPERATION_TOOL_SPEC_V3,
        LOCAL_CANARY_AGENT_TOOL_REGISTRY,
    )

    occurrence = occurrence.revalidated_for_registry(LOCAL_CANARY_AGENT_TOOL_REGISTRY)
    tool_spec = LOCAL_CANARY_AGENT_TOOL_REGISTRY.require_historical(
        occurrence.tool_name,
        occurrence.tool_spec_version,
        occurrence.tool_spec_digest,
    )
    retained = {
        INSPECT_OPERATION_TOOL_SPEC_V1.historical_identity: ("v2", True),
        INSPECT_OPERATION_TOOL_SPEC_V2.historical_identity: ("v2", True),
        INSPECT_OPERATION_TOOL_SPEC_V3.historical_identity: ("v3", False),
    }
    physical_contract = retained.get(tool_spec.historical_identity)
    if tool_spec.tool_name != "inspect_operation" or tool_spec.tool_kind != "query" or physical_contract is None:
        raise ValueError("agent tool inspect result historical contract unavailable")
    result_spec = resolve_inspect_operation_result_spec(
        occurrence.result_schema_version,
        occurrence.result_schema_digest,
    )
    if (
        tool_spec.result.schema_version != result_spec.result_schema_version
        or tool_spec.result.schema_digest != result_spec.result_schema_digest
    ):
        raise ValueError("agent tool inspect result historical result contract mismatch")
    physical_owner_revision, include_progress_reason = physical_contract
    return _InspectOperationContractBinding(
        tool_spec=tool_spec,
        result_spec=result_spec,
        physical_owner_revision=physical_owner_revision,
        include_progress_reason=include_progress_reason,
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_dict(value: object, *, field: str) -> dict[str, Any]:
    if type(value) is dict:
        return dict(value)
    if type(value) is not str or not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"agent tool inspect result {field} invalid") from exc
    if type(decoded) is not dict:
        raise ValueError(f"agent tool inspect result {field} invalid")
    return dict(decoded)


def _required_json_dict(value: object, *, field: str) -> dict[str, Any]:
    if type(value) is dict:
        return dict(value)
    if type(value) is not str or not value:
        raise ValueError(f"agent tool inspect result {field} invalid")
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"agent tool inspect result {field} invalid") from exc
    if type(decoded) is not dict:
        raise ValueError(f"agent tool inspect result {field} invalid")
    return dict(decoded)


def _required_json_list(value: object, *, field: str) -> list[Any]:
    if type(value) is list:
        return list(value)
    if type(value) is not str or not value:
        raise ValueError(f"agent tool inspect result {field} invalid")
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"agent tool inspect result {field} invalid") from exc
    if type(decoded) is not list:
        raise ValueError(f"agent tool inspect result {field} invalid")
    return list(decoded)


def _required_identity_text(value: object, *, field: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"agent tool inspect result {field} invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError(f"agent tool inspect result {field} invalid") from exc
    if len(encoded) > 1024 or any(character in "\r\n\x00" for character in value):
        raise ValueError(f"agent tool inspect result {field} invalid")
    return value


def _optional_identity_text(value: object, *, field: str) -> str:
    if value == "":
        return ""
    return _required_identity_text(value, field=field)


def _required_sha256(value: object, *, field: str) -> str:
    normalized = _required_identity_text(value, field=field)
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"agent tool inspect result {field} invalid")
    return normalized


def _workflow_command_causal_identity(command: dict[str, Any]) -> dict[str, Any]:
    """Freeze the non-model workflow lineage used by the inspect owner."""

    required_fields = {
        "workflow_run_id": "workflow_run_id",
        "command_id": "command_id",
        "command_type": "command_type",
        "owner": "owner",
        "operation_run_id": "operation_id",
        "idempotency_key": "idempotency_key",
        "causality_schema_version": "causality_schema_version",
        "schema_version": "schema_version",
    }
    optional_fields = (
        "stage_id",
        "causal_group_id",
        "parent_command_id",
        "source_event_id",
        "source_event_type",
        "no_op_reason",
        "readiness_effect",
    )
    identity: dict[str, Any] = {
        name: _required_identity_text(command.get(column), field=f"workflow command {column}")
        for name, column in required_fields.items()
    }
    identity.update(
        {name: _optional_identity_text(command.get(name), field=f"workflow command {name}") for name in optional_fields}
    )
    for name in (
        "input_artifact_refs",
        "output_artifact_refs",
        "artifact_refs",
    ):
        identity[f"{name}_digest"] = _sha256_json(
            _required_json_list(command.get(f"{name}_json"), field=f"workflow command {name}")
        )
    downstream_command_ids = _required_json_list(
        command.get("downstream_command_ids_json"),
        field="workflow command downstream_command_ids",
    )
    if any(
        type(command_id) is not str or not command_id or command_id != command_id.strip()
        for command_id in downstream_command_ids
    ):
        raise ValueError("agent tool inspect result workflow command downstream_command_ids invalid")
    identity["downstream_command_ids_digest"] = _sha256_json(downstream_command_ids)
    produced_entity_counts = _required_json_dict(
        command.get("produced_entity_counts_json"),
        field="workflow command produced_entity_counts",
    )
    if any(
        type(name) is not str or not name or name != name.strip() or type(count) is not int or count < 0
        for name, count in produced_entity_counts.items()
    ):
        raise ValueError("agent tool inspect result workflow command produced_entity_counts invalid")
    identity["produced_entity_counts_digest"] = _sha256_json(produced_entity_counts)
    for name in ("payload", "retry_policy"):
        identity[f"{name}_digest"] = _sha256_json(
            _required_json_dict(command.get(f"{name}_json"), field=f"workflow command {name}")
        )
    return identity


def _workflow_ref_identity(
    value: dict[str, Any],
    *,
    field: str,
    allow_variant_fields: bool,
) -> dict[str, str]:
    """Validate the closed identity envelope without erasing event variants."""

    keys = set(value)
    required = set(_WORKFLOW_REF_FIELDS)
    if not value:
        if allow_variant_fields:
            raise ValueError(f"agent tool inspect result {field} invalid")
        return {}
    if (not allow_variant_fields and keys != required) or (allow_variant_fields and not required.issubset(keys)):
        raise ValueError(f"agent tool inspect result {field} invalid")
    return {name: _required_identity_text(value[name], field=f"{field} {name}") for name in _WORKFLOW_REF_FIELDS}


def _canonical_operation_event_contract(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **{
            field: row.get(field)
            for field in (
                "event_id",
                "workspace_id",
                "event_stream_id",
                "operation_run_id",
                "action_id",
                "event_family",
                "event_type",
                "sequence_number",
                "idempotency_key",
                "actor",
                "source",
                "schema_version",
            )
        },
        "payload": _required_json_dict(row.get("payload_json"), field="canonical operation event payload"),
    }


def _canonical_workflow_event_contract(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **{
            field: row.get(field)
            for field in (
                "event_id",
                "workflow_run_id",
                "operation_id",
                "command_id",
                "activity_attempt_id",
                "event_family",
                "event_type",
                "sequence_number",
                "idempotency_key",
                "actor",
                "source",
                "schema_version",
            )
        },
        "payload": _required_json_dict(row.get("payload_json"), field="canonical workflow event payload"),
        "artifact_refs": _required_json_list(
            row.get("artifact_refs_json"),
            field="canonical workflow event artifact refs",
        ),
    }


def _canonical_workflow_command_contract(row: dict[str, Any]) -> dict[str, Any]:
    scalar_fields = (
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
        "causality_schema_version",
        "idempotency_key",
        "not_before_at",
        "max_attempts",
        "schema_version",
    )
    list_fields = (
        "input_artifact_refs_json",
        "output_artifact_refs_json",
        "downstream_command_ids_json",
        "artifact_refs_json",
    )
    dict_fields = (
        "produced_entity_counts_json",
        "payload_json",
        "retry_policy_json",
    )
    return {
        **{field: row.get(field) for field in scalar_fields},
        **{
            field.removesuffix("_json"): _required_json_list(
                row.get(field),
                field=f"canonical workflow command {field}",
            )
            for field in list_fields
        },
        **{
            field.removesuffix("_json"): _required_json_dict(
                row.get(field),
                field=f"canonical workflow command {field}",
            )
            for field in dict_fields
        },
    }


_WORKFLOW_COMMAND_MUTABLE_LIFECYCLE_FIELDS = frozenset({"not_before_at", "downstream_command_ids"})


def _canonical_workflow_command_identity_contract(row: dict[str, Any]) -> dict[str, Any]:
    """Return the immutable command/acceptance identity fields of one command.

    ``not_before_at`` and ``downstream_command_ids`` are mutable lifecycle
    state: S1e2c result acceptance legitimately releases the creation-time
    sentinel hold, and root completion legitimately links downstream children.
    Those two fields must never be compared against the creation-time row;
    they are validated through the explicit lifecycle contract in
    ``_workflow_command_lifecycle_state`` instead.
    """

    contract = _canonical_workflow_command_contract(row)
    return {
        field: value for field, value in contract.items() if field not in _WORKFLOW_COMMAND_MUTABLE_LIFECYCLE_FIELDS
    }


def _is_workflow_command_release_timestamp(value: str) -> bool:
    text = str(value)
    if not text or text != text.strip():
        return False
    try:
        datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        return False
    return True


def _workflow_command_lifecycle_state(workflow_command: dict[str, Any], *, hold_until: str) -> str:
    """Validate the mutable lifecycle fields of one accepted root command.

    The accepted start-v2 root command is created under a sentinel hold
    (``pending_hold``), is released by S1e2c result acceptance (``released``,
    either cleared or moved to a retry-backoff timestamp), and may then link
    downstream children when the root completes (``progressed``).  Inspect must
    accept the current state of that ``pending_hold -> released -> progressed``
    contract rather than the creation-time snapshot; any value outside the
    explicit contract raises.
    """

    contract = _canonical_workflow_command_contract(workflow_command)
    not_before_at = contract["not_before_at"]
    downstream_ids = contract["downstream_command_ids"]
    if type(not_before_at) is not str or (
        not_before_at != hold_until
        and not_before_at != ""
        and not _is_workflow_command_release_timestamp(not_before_at)
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    if not downstream_ids:
        return "pending_hold" if not_before_at == hold_until else "released"
    if (
        not_before_at != ""
        or any(type(item) is not str or not item or item != item.strip() for item in downstream_ids)
        or len(set(downstream_ids)) != len(downstream_ids)
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    return "progressed"


def _verify_progressed_workflow_command_children(
    *,
    workflow_command: dict[str, Any],
    child_commands: list[dict[str, Any]],
    workflow_events: list[dict[str, Any]],
) -> None:
    """Require every progressed downstream edge to resolve to a real locked child.

    A ``progressed`` root links children only through the root-completion owner
    path, which commits the child command, its ``CommandPlanRequested`` workflow
    event, and the root ``downstream_command_ids`` edge in one transaction.
    Inspect must therefore re-verify that exact physical lineage for every
    referenced identifier instead of trusting the edge list, and it must do so
    against the same versioned progressed-child contract the owner path commits
    (``workflow_progressed_child_contract``): the complete expected child and
    plan event are reconstructed from the locked root through the registered
    pure builder, and every immutable child/event/causality field — including
    the full child payload, the payload causality mirror of every causality
    column, the immutable contract pin, the registered child command
    type/owner, the registered ``<child>:plan`` idempotency, the deterministic
    event id, the pinned actor/source, the sequence ordered after the root's
    own source event, empty artifact refs, and the complete plan payload — must
    equal that reconstruction exactly.  Only the explicitly mutable child
    lifecycle fields may differ.
    """

    root_contract = _canonical_workflow_command_contract(workflow_command)
    root_command_id = str(root_contract["command_id"] or "")
    root_workflow_run_id = str(root_contract["workflow_run_id"] or "")
    root_operation_id = str(root_contract["operation_id"] or "")
    downstream_ids = list(root_contract["downstream_command_ids"])
    resolved_contract = progressed_child_completion_contract_for(
        parent_command_type=str(root_contract["command_type"] or ""),
        parent_owner=str(root_contract["owner"] or ""),
    )
    if resolved_contract is None:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    completion_name, completion_contract = resolved_contract
    if completion_name != "acquisition_root":
        raise ValueError("agent tool inspect result workflow command link mismatch")
    root_source_events = [
        event
        for event in workflow_events
        if str(event.get("event_id") or "") == str(root_contract["source_event_id"] or "")
    ]
    if len(root_source_events) != 1:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    root_source_sequence = root_source_events[0].get("sequence_number")
    if type(root_source_sequence) is not int or root_source_sequence <= 0:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    expected_plan = build_acquisition_root_intent_plan(
        {**workflow_command, "payload": root_contract["payload"]},
        claim_attempt=max(1, int(workflow_command.get("attempt") or 0)),
    )
    expected_child_spec = dict(expected_plan.get("child_command") or {})
    expected_causality_template = dict(expected_plan.get("child_causality") or {})
    expected_event_spec = dict(expected_plan.get("plan_event") or {})
    if not expected_child_spec or not expected_causality_template or not expected_event_spec:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    children_by_id: dict[str, dict[str, Any]] = {}
    for child in child_commands:
        child_contract = _canonical_workflow_command_contract(child)
        child_id = str(child_contract["command_id"] or "")
        if not child_id or child_id in children_by_id:
            raise ValueError("agent tool inspect result workflow command link mismatch")
        children_by_id[child_id] = child_contract
    if set(children_by_id) != set(downstream_ids):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    for child_id in downstream_ids:
        child_contract = children_by_id[child_id]
        child_identity = canonical_progressed_child_identity(child_contract)
        if child_identity is None or child_identity["contract_name"] != completion_name:
            raise ValueError("agent tool inspect result workflow command link mismatch")
        if (
            child_identity["command_type"] != str(completion_contract["child_command_type"] or "")
            or child_identity["owner"] != str(completion_contract["child_owner"] or "")
            or child_identity["workflow_run_id"] != root_workflow_run_id
            or child_identity["operation_id"] != root_operation_id
            or child_identity["parent_command_id"] != root_command_id
            or not child_identity["source_event_id"]
            or child_identity["source_event_type"] != "CommandPlanRequested"
        ):
            raise ValueError("agent tool inspect result workflow command link mismatch")
        expected_child_identity = canonical_progressed_child_identity(
            expected_progressed_child_row(
                contract=completion_contract,
                parent_command_id=root_command_id,
                workflow_run_id=root_workflow_run_id,
                operation_id=root_operation_id,
                child_command=expected_child_spec,
                child_causality={
                    **expected_causality_template,
                    "source_event_id": child_identity["source_event_id"],
                    "source_event_type": "CommandPlanRequested",
                },
            )
        )
        if expected_child_identity is None or not json_contract_equal(child_identity, expected_child_identity):
            raise ValueError("agent tool inspect result workflow command link mismatch")
        source_events = [
            event for event in workflow_events if str(event.get("event_id") or "") == child_identity["source_event_id"]
        ]
        if len(source_events) != 1:
            raise ValueError("agent tool inspect result workflow command link mismatch")
        source_event = source_events[0]
        decoded_event = {
            **dict(source_event),
            "payload": _required_json_dict(
                source_event.get("payload_json"),
                field="workflow child plan event payload",
            ),
            "artifact_refs": _required_json_list(
                source_event.get("artifact_refs_json"),
                field="workflow child plan event artifact refs",
            ),
        }
        if progressed_child_plan_event_violation(
            contract=completion_contract,
            parent_command_id=root_command_id,
            parent_source_sequence=root_source_sequence,
            child_identity=child_identity,
            event=decoded_event,
            expected_payload=dict(expected_event_spec.get("payload") or {}),
        ):
            raise ValueError("agent tool inspect result workflow command link mismatch")


def _operation_command_planned_event_identity(
    event: dict[str, Any],
    event_payload: dict[str, Any],
    *,
    normalized_workflow_ref: dict[str, str],
    workspace_id: str,
    action_id: str,
    operation_run_id: str,
    action: dict[str, Any],
    operation: dict[str, Any],
    require_start_acceptance_event: bool,
) -> dict[str, str]:
    schema_version = str(event.get("schema_version") or "")
    if schema_version == "operation_event_v1":
        if require_start_acceptance_event:
            raise ValueError("agent tool inspect result operation command planned event invalid")
        allowed_fields = set(_WORKFLOW_REF_FIELDS) | {"module_state_mutated"}
        if not set(_WORKFLOW_REF_FIELDS).issubset(event_payload) or not set(event_payload).issubset(allowed_fields):
            raise ValueError("agent tool inspect result operation command planned event invalid")
        if "module_state_mutated" in event_payload and type(event_payload["module_state_mutated"]) is not bool:
            raise ValueError("agent tool inspect result operation command planned event invalid")
        return _workflow_ref_identity(
            event_payload,
            field="operation command planned event",
            allow_variant_fields=True,
        )
    if schema_version != ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION:
        raise ValueError("agent tool inspect result operation command planned event invalid")
    try:
        acceptance = AcquisitionStartCommandAcceptanceEvent.from_payload(event_payload)
    except AcquisitionStartCommandAcceptanceError as exc:
        raise ValueError("agent tool inspect result operation command planned event invalid") from exc
    owner_result_ref = acceptance.owner_result_ref.to_record()
    if not normalized_workflow_ref:
        raise ValueError("agent tool inspect result operation command planned event invalid")

    text_fields = (
        "schema_version",
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "action_id",
        "operation_run_id",
        "workflow_run_id",
        "workflow_command_id",
        "terminal_winner_id",
        "command_source_event_id",
    )
    normalized_ref = {
        field: _required_identity_text(
            owner_result_ref.get(field),
            field=f"operation command planned event owner_result_ref {field}",
        )
        for field in text_fields
    }
    for field in (
        "command_source_event_contract_digest",
        "start_snapshot_digest",
        "root_command_payload_digest",
    ):
        normalized_ref[field] = _required_sha256(
            owner_result_ref.get(field),
            field=f"operation command planned event owner_result_ref {field}",
        )

    confirmation_receipt_ref = owner_result_ref.get("confirmation_receipt_ref")
    result_occurrence_ref = owner_result_ref.get("result_occurrence_ref")
    if (
        type(confirmation_receipt_ref) is not dict
        or set(confirmation_receipt_ref) != {"receipt_id", "receipt_digest"}
        or type(result_occurrence_ref) is not dict
        or set(result_occurrence_ref) != {"result_slot_id", "slot_generation", "logical_occurrence_digest"}
    ):
        raise ValueError("agent tool inspect result operation command planned event invalid")
    receipt_id = _required_identity_text(
        confirmation_receipt_ref.get("receipt_id"),
        field="operation command planned event owner_result_ref receipt_id",
    )
    receipt_digest = _required_sha256(
        confirmation_receipt_ref.get("receipt_digest"),
        field="operation command planned event owner_result_ref receipt_digest",
    )
    _required_identity_text(
        result_occurrence_ref.get("result_slot_id"),
        field="operation command planned event owner_result_ref result_slot_id",
    )
    slot_generation = result_occurrence_ref.get("slot_generation")
    if type(slot_generation) is not int or slot_generation <= 0:
        raise ValueError("agent tool inspect result operation command planned event invalid")
    _required_sha256(
        result_occurrence_ref.get("logical_occurrence_digest"),
        field="operation command planned event owner_result_ref logical_occurrence_digest",
    )
    try:
        budget_ref = AcquisitionParentBudgetEnvelopeRef(owner_result_ref.get("parent_budget_envelope_ref") or {})
    except AcquisitionStartV2Error as exc:
        raise ValueError("agent tool inspect result operation command planned event invalid") from exc

    action_result_ref = _json_dict(action.get("result_ref_json"), field="action result ref")
    operation_result_ref = _json_dict(operation.get("result_ref_json"), field="operation result ref")
    action_target = _json_dict(action.get("target_ref_json"), field="action target ref")
    action_metadata = _json_dict(action.get("metadata_json"), field="action metadata")
    start_snapshot = action_target.get("start_snapshot")
    start_snapshot_digest = start_snapshot.get("snapshot_digest") if isinstance(start_snapshot, dict) else None
    event_sequence = event.get("sequence_number")
    if (
        normalized_ref["schema_version"] != "acquisition_start_command_acceptance_owner_result_ref.v1"
        or normalized_ref["runtime_namespace"] != "isolated_local_canary"
        or normalized_ref["provider_mode"] not in {"simulate", "scripted"}
        or normalized_ref["workspace_id"] != workspace_id
        or normalized_ref["action_id"] != action_id
        or normalized_ref["operation_run_id"] != operation_run_id
        or normalized_ref["workflow_run_id"] != normalized_workflow_ref["workflow_run_id"]
        or normalized_ref["workflow_command_id"] != normalized_workflow_ref["command_id"]
        or normalized_ref["terminal_winner_id"] != str(event.get("event_id") or "")
        or type(event_sequence) is not int
        or event_sequence != 1
        or not json_contract_equal(owner_result_ref.get("terminal_winner_sequence_number"), event_sequence)
        or not json_contract_equal(owner_result_ref.get("command_source_event_sequence_number"), 2)
        or normalized_ref["start_snapshot_digest"] != start_snapshot_digest
        or not json_contract_equal(action_metadata.get("result_occurrence_ref"), result_occurrence_ref)
        or not json_contract_equal(action_result_ref, owner_result_ref)
        or not json_contract_equal(operation_result_ref, owner_result_ref)
        or budget_ref.confirmation_receipt_id != receipt_id
        or budget_ref.confirmation_receipt_digest != receipt_digest
        or acceptance.owner_result_digest != _sha256_json(owner_result_ref)
    ):
        raise ValueError("agent tool inspect result operation command planned event invalid")
    return dict(normalized_workflow_ref)


def _masked_absence_terminal(
    *,
    binding: _InspectOperationContractBinding,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    action_id: str,
    operation_run_id: str,
) -> AgentToolTerminalResult:
    """Build the one non-enumerating result for every non-exact physical owner."""

    owner_output = inspect_operation_error_result(reason="operation_not_found")
    # Keep this physical adapter pinned to the same model-safe serializer as the
    # pure query owner even though the output is a fixed masked value.
    serialized_result = serialize_inspect_operation_result_for_spec(
        owner_output,
        result_spec=binding.result_spec,
    )
    owner_result_ref = {
        "schema_version": "inspect_operation_masked_error_owner_ref_v1",
        "lookup": {
            "workspace_id": occurrence.workspace_id,
            "action_id": action_id,
            "operation_run_id": operation_run_id,
        },
        "occurrence": {
            "result_slot_id": occurrence.result_slot_id,
            "slot_generation": occurrence.slot_generation,
            "logical_occurrence_digest": occurrence.logical_occurrence_digest,
            "result_link_policy": occurrence.result_link_policy,
            "tool_spec_digest": occurrence.tool_spec_digest,
            "request_schema_digest": occurrence.request_schema_digest,
            "result_schema_digest": occurrence.result_schema_digest,
            "serializer_contract_digest": occurrence.serializer_contract_digest,
        },
        "outcome": "operation_not_found",
        "serialized_result_digest": hashlib.sha256(serialized_result.encode("utf-8")).hexdigest(),
    }
    owner_result_digest = _sha256_json(owner_result_ref)
    return AgentToolTerminalResult.from_serialized_result(
        result_attempt_id=result_attempt_id,
        provider_call_id=provider_call_id,
        tool_call_id=tool_call_id,
        action_id=action_id,
        operation_run_id=operation_run_id,
        owner_target_kind=INSPECT_OPERATION_MASKED_ABSENCE_OWNER_TARGET_KIND,
        owner_target_id=occurrence.result_slot_id,
        owner_target_generation=occurrence.slot_generation,
        terminal_winner_id=f"inspecterror_{owner_result_digest}",
        owner_result_ref=owner_result_ref,
        owner_result_digest=owner_result_digest,
        serialized_result=owner_output,
        is_error=True,
    )


def validate_inspect_operation_occurrence(
    occurrence: AgentToolOccurrence,
    *,
    action_id: str,
    operation_run_id: str,
) -> _InspectOperationPreflight:
    """Require the exact isolated-canary query contract and target link."""

    binding = _resolve_inspect_operation_contract(occurrence)
    request = bind_inspect_operation_request(
        occurrence.canonical_args,
        workspace_id=occurrence.workspace_id,
        action_id=action_id,
        actor_id=occurrence.actor_id,
    )
    if request.operation_run_id != operation_run_id:
        raise ValueError("agent tool inspect result canonical args mismatch")
    return _InspectOperationPreflight(contract=binding, request=request)


def inspect_operation_result_lock_groups(
    *,
    occurrence: AgentToolOccurrence,
    action_id: str,
    operation_run_id: str,
    include_result_slot: bool,
    start_occurrence_refs: tuple[dict[str, Any], ...] = (),
) -> tuple[tuple[str, ...], ...]:
    groups: list[tuple[str, ...]] = [
        (f"operation_events:{operation_run_id}",),
        (f"operation_runs:id:{operation_run_id}",),
        (f"agent_actions:id:{action_id}",),
    ]
    result_slot_keys: set[str] = set()
    if include_result_slot:
        result_slot_keys.update(
            {
                f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
                f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
            }
        )
    for occurrence_ref in start_occurrence_refs:
        result_slot_id = occurrence_ref.get("result_slot_id")
        logical_occurrence_digest = occurrence_ref.get("logical_occurrence_digest")
        if type(result_slot_id) is str and result_slot_id:
            result_slot_keys.add(f"agent_tool_result_slots:id:{result_slot_id}")
        if type(logical_occurrence_digest) is str and logical_occurrence_digest:
            result_slot_keys.add(f"agent_tool_result_slots:occurrence:{logical_occurrence_digest}")
    if result_slot_keys:
        groups.append(tuple(sorted(result_slot_keys, key=lambda value: value.encode("utf-8"))))
    return tuple(groups)


def _start_occurrence_refs_from_owner_rows(
    *,
    action: dict[str, Any],
    operation_events_desc: list[dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    refs_by_canonical_json: dict[str, dict[str, Any]] = {}
    action_metadata = _json_dict(action.get("metadata_json"), field="action metadata")
    metadata_ref = action_metadata.get("result_occurrence_ref")
    if isinstance(metadata_ref, dict):
        refs_by_canonical_json[_canonical_json(metadata_ref)] = dict(metadata_ref)
    for event in operation_events_desc:
        if (
            str(event.get("event_type") or "") != "OperationCommandPlanned"
            or str(event.get("schema_version") or "") != ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION
        ):
            continue
        event_payload = _required_json_dict(event.get("payload_json"), field="operation event payload")
        owner_result_ref = event_payload.get("owner_result_ref")
        event_ref = owner_result_ref.get("result_occurrence_ref") if isinstance(owner_result_ref, dict) else None
        if isinstance(event_ref, dict):
            refs_by_canonical_json[_canonical_json(event_ref)] = dict(event_ref)
    return tuple(refs_by_canonical_json[key] for key in sorted(refs_by_canonical_json))


def preload_inspect_operation_start_occurrence_refs(
    cursor: Any,
    *,
    workspace_id: str,
    action_id: str,
    operation_run_id: str,
) -> tuple[dict[str, Any], ...]:
    """Read dynamic start-slot identities before the advisory-lock phase.

    The authoritative loader re-reads and exact-compares these identities after
    all advisory locks are held, so a concurrent owner-ref change cannot redirect
    the later row lock to an unfenced result slot.
    """

    from .control_plane_live_postgres import _fetch_all_dict_rows, _fetch_one_dict_row

    cursor.execute(
        "SELECT row_to_json(action) AS action_json, row_to_json(operation) AS operation_json "
        "FROM agent_actions AS action "
        "JOIN operation_runs AS operation "
        "ON operation.action_id = action.action_id AND operation.workspace_id = action.workspace_id "
        "WHERE action.action_id = %s AND action.workspace_id = %s "
        "AND operation.operation_run_id = %s",
        (action_id, workspace_id, operation_run_id),
    )
    owner_pair = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    action = owner_pair.get("action_json")
    operation = owner_pair.get("operation_json")
    if not isinstance(action, dict) or not isinstance(operation, dict):
        return ()
    generic_control_preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )
    if str(generic_control_preflight.get("status") or "") == "ready":
        return ()
    cursor.execute(
        "SELECT event_type, schema_version, payload_json FROM operation_events "
        "WHERE event_stream_id = %s ORDER BY sequence_number DESC LIMIT 100001",
        (operation_run_id,),
    )
    return _start_occurrence_refs_from_owner_rows(
        action=action,
        operation_events_desc=_fetch_all_dict_rows(cursor),
    )


def load_inspect_operation_base_owner(
    cursor: Any,
    *,
    workspace_id: str,
    action_id: str,
    operation_run_id: str,
    expected_start_occurrence_refs: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    """Lock every inspect owner row before the result slot can be locked."""

    from .control_plane_live_postgres import _fetch_all_dict_rows, _fetch_one_dict_row

    cursor.execute(
        "SELECT * FROM operation_runs WHERE operation_run_id = %s AND workspace_id = %s AND action_id = %s FOR UPDATE",
        (operation_run_id, workspace_id, action_id),
    )
    operation = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    cursor.execute(
        "SELECT * FROM agent_actions WHERE action_id = %s AND workspace_id = %s FOR UPDATE",
        (action_id, workspace_id),
    )
    action = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    exact_owner = (
        bool(action)
        and bool(operation)
        and str(action.get("action_id") or "") == action_id
        and str(action.get("workspace_id") or "") == workspace_id
        and str(operation.get("operation_run_id") or "") == operation_run_id
        and str(operation.get("workspace_id") or "") == workspace_id
        and str(operation.get("action_id") or "") == action_id
    )
    commands: list[dict[str, Any]] = []
    child_commands: list[dict[str, Any]] = []
    events_desc: list[dict[str, Any]] = []
    action_events_desc: list[dict[str, Any]] = []
    workflow_events: list[dict[str, Any]] = []
    start_result_slots: list[dict[str, Any]] = []
    command_id = ""
    workflow_run_id = ""
    if exact_owner:
        workflow_ref = _required_json_dict(
            operation.get("workflow_ref_json"),
            field="operation workflow ref",
        )
        normalized_workflow_ref = _workflow_ref_identity(
            workflow_ref,
            field="operation workflow ref",
            allow_variant_fields=False,
        )
        command_id = normalized_workflow_ref.get("command_id", "")
        workflow_run_id = normalized_workflow_ref.get("workflow_run_id", "")
    if command_id:
        cursor.execute(
            "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
            (command_id,),
        )
        commands = _fetch_all_dict_rows(cursor)
    if exact_owner:
        cursor.execute(
            "SELECT * FROM operation_events "
            "WHERE event_stream_id = %s "
            "ORDER BY sequence_number DESC LIMIT 100001 FOR UPDATE",
            (action_id,),
        )
        action_events_desc = _fetch_all_dict_rows(cursor)
    if workflow_run_id:
        cursor.execute(
            "SELECT * FROM workflow_events WHERE workflow_run_id = %s ORDER BY sequence_number LIMIT 100001 FOR UPDATE",
            (workflow_run_id,),
        )
        workflow_events = _fetch_all_dict_rows(cursor)
    if len(commands) == 1:
        # A progressed root links children through the root-completion owner
        # path; lock every referenced child row (after the root and workflow
        # event locks, mirroring that owner path's lock order) so the lifecycle
        # verification re-checks the exact physical lineage instead of trusting
        # the persisted identifier list.
        try:
            referenced_child_ids = _required_json_list(
                commands[0].get("downstream_command_ids_json"),
                field="workflow command downstream_command_ids",
            )
        except ValueError:
            referenced_child_ids = []
        locked_child_ids = sorted(
            {item for item in referenced_child_ids if type(item) is str and item},
            key=lambda value: value.encode("utf-8"),
        )
        for locked_child_id in locked_child_ids:
            cursor.execute(
                "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
                (locked_child_id,),
            )
            child_row = _fetch_one_dict_row(cursor, cursor.fetchone())
            if child_row is not None:
                child_commands.append(child_row)
    if exact_owner:
        cursor.execute(
            "SELECT * FROM operation_events "
            "WHERE event_stream_id = %s "
            "ORDER BY sequence_number DESC LIMIT 100001 FOR UPDATE",
            (operation_run_id,),
        )
        events_desc = _fetch_all_dict_rows(cursor)
    if exact_owner:
        generic_control_preflight = acquisition_start_v2_generic_operation_control_preflight(
            action=action,
            operation_run=operation,
        )
        if str(generic_control_preflight.get("status") or "") != "ready":
            occurrence_refs = _start_occurrence_refs_from_owner_rows(
                action=action,
                operation_events_desc=events_desc,
            )
            if not json_contract_equal(list(occurrence_refs), list(expected_start_occurrence_refs)):
                raise ValueError("agent tool inspect result workflow command link mismatch")
            locked_slots: dict[str, dict[str, Any]] = {}
            ordered_refs = sorted(
                occurrence_refs,
                key=lambda item: (
                    str(item.get("result_slot_id") or "").encode("utf-8"),
                    str(item.get("logical_occurrence_digest") or "").encode("utf-8"),
                ),
            )
            for occurrence_ref in ordered_refs:
                result_slot_id = str(occurrence_ref.get("result_slot_id") or "").strip()
                logical_occurrence_digest = str(occurrence_ref.get("logical_occurrence_digest") or "").strip()
                if not result_slot_id and not logical_occurrence_digest:
                    continue
                cursor.execute(
                    "SELECT * FROM agent_tool_result_slots "
                    "WHERE result_slot_id = %s OR logical_occurrence_digest = %s "
                    "ORDER BY result_slot_id FOR UPDATE",
                    (result_slot_id, logical_occurrence_digest),
                )
                for slot in _fetch_all_dict_rows(cursor):
                    locked_slots[str(slot.get("result_slot_id") or "")] = slot
            start_result_slots = [
                locked_slots[key] for key in sorted(locked_slots, key=lambda value: value.encode("utf-8"))
            ]
    return {
        "action": action,
        "operation_run": operation,
        "workflow_commands": commands,
        "workflow_child_commands": child_commands,
        "operation_events_desc": events_desc,
        "action_events_desc": action_events_desc,
        "workflow_events": workflow_events,
        "start_result_slots": start_result_slots,
    }


def _validate_start_acceptance_physical_dependencies(
    *,
    owner_result_ref: dict[str, Any],
    action: dict[str, Any],
    operation: dict[str, Any],
    workflow_command: dict[str, Any],
    normalized_workflow_ref: dict[str, str],
    action_events_desc: list[dict[str, Any]],
    workflow_events: list[dict[str, Any]],
    start_result_slots: list[dict[str, Any]],
    planned_event: dict[str, Any],
    child_commands: list[dict[str, Any]],
) -> None:
    """Bind the closed acceptance ref to every immutable physical owner."""

    action_target = _required_json_dict(action.get("target_ref_json"), field="action target ref")
    action_input = _required_json_dict(action.get("input_json"), field="action input")
    try:
        bound_request = AcquisitionStartV2BoundRequest(
            {
                "input_payload": action_input,
                "target_ref": action_target,
            }
        )
    except AcquisitionStartV2Error as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc
    snapshot = bound_request.snapshot.to_record()

    if len(start_result_slots) != 1:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    start_slot = dict(start_result_slots[0])
    try:
        canonical_args_json = start_slot.get("canonical_args_json")
        if isinstance(canonical_args_json, dict):
            canonical_args_json = _canonical_json(canonical_args_json)
        start_occurrence = AgentToolOccurrence(
            result_slot_id=start_slot.get("result_slot_id"),
            slot_generation=start_slot.get("slot_generation"),
            workspace_id=start_slot.get("workspace_id"),
            actor_id=start_slot.get("actor_id"),
            runtime_namespace=start_slot.get("runtime_namespace"),
            provider_mode=start_slot.get("provider_mode"),
            turn_id=start_slot.get("turn_id"),
            step_id=start_slot.get("step_id"),
            tool_name=start_slot.get("tool_name"),
            tool_kind=start_slot.get("tool_kind"),
            effect_class=start_slot.get("effect_class"),
            result_link_policy=start_slot.get("result_link_policy"),
            tool_spec_version=start_slot.get("tool_spec_version"),
            tool_spec_digest=start_slot.get("tool_spec_digest"),
            canonical_args_json=canonical_args_json,
            canonical_args_digest=start_slot.get("canonical_args_digest"),
            occurrence_ordinal=start_slot.get("occurrence_ordinal"),
            request_schema_version=start_slot.get("request_schema_version"),
            request_schema_digest=start_slot.get("request_schema_digest"),
            result_schema_version=start_slot.get("result_schema_version"),
            result_schema_digest=start_slot.get("result_schema_digest"),
            serializer_owner=start_slot.get("serializer_owner"),
            serializer_revision=start_slot.get("serializer_revision"),
            serializer_contract_digest=start_slot.get("serializer_contract_digest"),
        )
        from .acquisition_start_v2_postgres import revalidate_acquisition_start_v2_occurrence
        from .agent_tool_result_postgres import _assert_exact_slot

        _assert_exact_slot(start_slot, start_occurrence)
        start_binding = revalidate_acquisition_start_v2_occurrence(start_occurrence)
    except (TypeError, ValueError) as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc
    result_occurrence_ref = owner_result_ref.get("result_occurrence_ref")
    action_metadata = _required_json_dict(action.get("metadata_json"), field="action metadata")
    if (
        start_binding.action_id != str(action.get("action_id") or "")
        or start_binding.start_idempotency != str(action.get("idempotency_key") or "")
        or start_binding.start_idempotency != str(operation.get("idempotency_key") or "")
        or not json_contract_equal(start_binding.result_occurrence_ref, result_occurrence_ref)
        or not json_contract_equal(action_metadata.get("result_occurrence_ref"), result_occurrence_ref)
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")

    receipt_events = [
        event
        for event in action_events_desc
        if str(event.get("event_type") or "") == "ActionApproved" and event.get("sequence_number") == 2
    ]
    if len(receipt_events) != 1:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    receipt_event = receipt_events[0]
    receipt_payload = _required_json_dict(receipt_event.get("payload_json"), field="action approval receipt")
    try:
        receipt = AcquisitionConfirmationReceipt(receipt_payload)
    except AcquisitionStartV2Error as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc
    receipt_record = receipt.to_record()
    receipt_ref = owner_result_ref.get("confirmation_receipt_ref")
    if not isinstance(receipt_ref, dict):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    if (
        str(receipt_event.get("event_id") or "") != receipt.receipt_id
        or str(receipt_event.get("workspace_id") or "") != str(action.get("workspace_id") or "")
        or str(receipt_event.get("event_stream_id") or "") != str(action.get("action_id") or "")
        or str(receipt_event.get("operation_run_id") or "") != str(operation.get("operation_run_id") or "")
        or str(receipt_event.get("action_id") or "") != str(action.get("action_id") or "")
        or str(receipt_event.get("event_family") or "") != "operation_event"
        or str(receipt_event.get("schema_version") or "") != ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION
        or not json_contract_equal(
            receipt_ref,
            {"receipt_id": receipt.receipt_id, "receipt_digest": receipt.receipt_digest},
        )
        or receipt_record.get("action_id") != str(action.get("action_id") or "")
        or receipt_record.get("workspace_id") != str(action.get("workspace_id") or "")
        or receipt_record.get("requester_id") != str(bound_request.target_ref.get("requester_id") or "")
        or receipt_record.get("start_snapshot_digest") != snapshot.get("snapshot_digest")
        or owner_result_ref.get("start_snapshot_digest") != snapshot.get("snapshot_digest")
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")

    action_budget = _required_json_dict(action.get("budget_json"), field="action budget")
    operation_budget = _required_json_dict(operation.get("cost_budget_json"), field="operation budget")
    budget = receipt_record.get("budget")
    try:
        budget_ref = AcquisitionParentBudgetEnvelopeRef(owner_result_ref.get("parent_budget_envelope_ref") or {})
    except AcquisitionStartV2Error as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc
    if (
        not json_contract_equal(action_budget, budget)
        or not json_contract_equal(operation_budget, budget)
        or budget_ref.confirmation_receipt_id != receipt.receipt_id
        or budget_ref.confirmation_receipt_digest != receipt.receipt_digest
        or budget_ref.budget_digest != _sha256_json(budget)
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")

    source_events = [
        event
        for event in workflow_events
        if str(event.get("event_id") or "") == str(owner_result_ref.get("command_source_event_id") or "")
        and event.get("sequence_number") == owner_result_ref.get("command_source_event_sequence_number")
    ]
    if len(source_events) != 1:
        raise ValueError("agent tool inspect result workflow command link mismatch")
    source_event = source_events[0]
    source_payload = _required_json_dict(source_event.get("payload_json"), field="workflow source event payload")
    source_artifact_refs = _required_json_list(
        source_event.get("artifact_refs_json"),
        field="workflow source event artifact refs",
    )
    source_contract = {
        "event_id": source_event.get("event_id"),
        "workflow_run_id": source_event.get("workflow_run_id"),
        "operation_id": source_event.get("operation_id"),
        "command_id": source_event.get("command_id"),
        "activity_attempt_id": source_event.get("activity_attempt_id"),
        "event_family": source_event.get("event_family"),
        "event_type": source_event.get("event_type"),
        "sequence_number": source_event.get("sequence_number"),
        "idempotency_key": source_event.get("idempotency_key"),
        "actor": source_event.get("actor"),
        "source": source_event.get("source"),
        "payload": source_payload,
        "artifact_refs": source_artifact_refs,
        "schema_version": source_event.get("schema_version"),
    }
    command_payload = _required_json_dict(workflow_command.get("payload_json"), field="workflow command payload")
    command_causality = command_payload.get("causality")
    if not isinstance(command_causality, dict):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    if (
        str(source_event.get("workflow_run_id") or "") != normalized_workflow_ref["workflow_run_id"]
        or str(source_event.get("operation_id") or "") != str(operation.get("operation_run_id") or "")
        or str(source_event.get("event_family") or "") != "workflow_event"
        or str(source_event.get("event_type") or "") != "CommandPlanRequested"
        or str(source_event.get("schema_version") or "") != "workflow_event_v1"
        or str(workflow_command.get("source_event_id") or "") != str(source_event.get("event_id") or "")
        or str(workflow_command.get("source_event_type") or "") != "CommandPlanRequested"
        or not json_contract_equal(source_payload.get("payload"), command_payload)
        or source_payload.get("command_type") != str(workflow_command.get("command_type") or "")
        or source_payload.get("idempotency_key") != str(workflow_command.get("idempotency_key") or "")
        or command_payload.get("operation_id") != str(operation.get("operation_run_id") or "")
        or command_causality.get("workflow_run_id") != normalized_workflow_ref["workflow_run_id"]
        or command_causality.get("operation_id") != str(operation.get("operation_run_id") or "")
        or command_causality.get("command_type") != normalized_workflow_ref["command_type"]
        or command_causality.get("owner") != normalized_workflow_ref["owner"]
        or command_causality.get("source_event_id") != str(source_event.get("event_id") or "")
        or command_causality.get("source_event_type") != "CommandPlanRequested"
        or _sha256_json(source_contract) != owner_result_ref.get("command_source_event_contract_digest")
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")

    root_payload = {key: value for key, value in command_payload.items() if key not in {"operation_id", "causality"}}
    try:
        canonical_root = AcquisitionStartV2RootCommandPayload(root_payload).to_record()
    except AcquisitionStartV2Error as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc
    if (
        canonical_root.get("payload_digest") != owner_result_ref.get("root_command_payload_digest")
        or canonical_root.get("action_id") != str(action.get("action_id") or "")
        or canonical_root.get("operation_run_id") != str(operation.get("operation_run_id") or "")
        or canonical_root.get("workflow_run_id") != normalized_workflow_ref["workflow_run_id"]
        or not json_contract_equal(canonical_root.get("confirmation_receipt_ref"), receipt_ref)
        or not json_contract_equal(canonical_root.get("start_snapshot"), snapshot)
        or canonical_root.get("start_snapshot_digest") != owner_result_ref.get("start_snapshot_digest")
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")

    from .acquisition_start_v2_create_postgres import _RESULT_ACCEPTANCE_HOLD_UNTIL, _canonical_create_rows

    expected_rows = _canonical_create_rows(
        binding=start_binding,
        receipt=receipt,
        submitted_action=action,
    )
    expected_source_event = next(
        event
        for event in expected_rows["workflow_events"]
        if str(event.get("event_type") or "") == "CommandPlanRequested"
    )
    lifecycle_state = _workflow_command_lifecycle_state(workflow_command, hold_until=_RESULT_ACCEPTANCE_HOLD_UNTIL)
    if lifecycle_state == "progressed":
        _verify_progressed_workflow_command_children(
            workflow_command=workflow_command,
            child_commands=child_commands,
            workflow_events=workflow_events,
        )
    if (
        not json_contract_equal(owner_result_ref, expected_rows["owner_result_ref"])
        or not json_contract_equal(
            _canonical_operation_event_contract(receipt_event),
            _canonical_operation_event_contract(dict(expected_rows["receipt_event"])),
        )
        or not json_contract_equal(
            _canonical_workflow_event_contract(source_event),
            _canonical_workflow_event_contract(dict(expected_source_event)),
        )
        or not json_contract_equal(
            _canonical_workflow_command_identity_contract(workflow_command),
            _canonical_workflow_command_identity_contract(dict(expected_rows["workflow_command"])),
        )
        or not json_contract_equal(
            _canonical_operation_event_contract(planned_event),
            _canonical_operation_event_contract(dict(expected_rows["planned_event"])),
        )
    ):
        raise ValueError("agent tool inspect result workflow command link mismatch")


def terminal_from_locked_inspect_operation_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    action_id: str,
    operation_run_id: str,
    base_owner: dict[str, Any],
    preflight: _InspectOperationPreflight,
) -> AgentToolTerminalResult:
    """Rebuild one exact query result while all physical owner rows are fenced."""

    del cursor

    if not isinstance(preflight, _InspectOperationPreflight):
        raise ValueError("agent tool inspect result preflight required")
    binding = preflight.contract
    request = preflight.request
    if (
        request.workspace_id != occurrence.workspace_id
        or request.actor_id != occurrence.actor_id
        or request.action_id != action_id
        or request.operation_run_id != operation_run_id
        or occurrence.canonical_args != {"operation_run_id": request.operation_run_id}
    ):
        raise ValueError("agent tool inspect result preflight mismatch")
    action = dict(base_owner.get("action") or {})
    operation = dict(base_owner.get("operation_run") or {})
    exact_owner = (
        bool(action)
        and bool(operation)
        and str(action.get("action_id") or "") == action_id
        and str(action.get("workspace_id") or "") == occurrence.workspace_id
        and str(operation.get("operation_run_id") or "") == operation_run_id
        and str(operation.get("workspace_id") or "") == occurrence.workspace_id
        and str(operation.get("action_id") or "") == action_id
    )
    if not exact_owner:
        return _masked_absence_terminal(
            binding=binding,
            occurrence=occurrence,
            result_attempt_id=result_attempt_id,
            provider_call_id=provider_call_id,
            tool_call_id=tool_call_id,
            action_id=action_id,
            operation_run_id=operation_run_id,
        )

    action_type = str(action.get("action_type") or "").strip()
    generic_control_preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )
    fail_closed_start_v2 = str(generic_control_preflight.get("status") or "") != "ready"
    if fail_closed_start_v2:
        action_spec = DEFAULT_ACTION_REGISTRY.spec_for("start_acquisition_run")
        display_contract = DEFAULT_ACTION_REGISTRY.display_contract_for("start_acquisition_run").to_record()
    else:
        try:
            action_spec = DEFAULT_ACTION_REGISTRY.spec_for(action_type)
            display_contract = DEFAULT_ACTION_REGISTRY.display_contract_for(action_type).to_record()
        except KeyError as exc:
            raise ValueError("agent tool inspect result action contract not found") from exc
    owner_fields = {
        "owner_module": action_spec.owner_module,
        "operation_type": action_spec.operation_type,
    }
    for row in (action, operation):
        if str(generic_control_preflight.get("status") or "") == "ready" and any(
            str(row.get(field) or "") != value for field, value in owner_fields.items()
        ):
            raise ValueError("agent tool inspect result action operation owner mismatch")

    events_desc = list(base_owner.get("operation_events_desc") or [])
    if not events_desc or len(events_desc) > 100_000:
        raise ValueError("agent tool inspect result operation event revision unavailable")
    events = list(reversed(events_desc))
    previous_sequence = 0
    event_evidence: list[dict[str, Any]] = []
    audit_event_evidence: list[dict[str, Any]] = []
    for event in events:
        sequence_number = event.get("sequence_number")
        event_payload = _required_json_dict(event.get("payload_json"), field="operation event payload")
        if (
            str(event.get("event_stream_id") or "") != operation_run_id
            or str(event.get("workspace_id") or "") != occurrence.workspace_id
            or str(event.get("operation_run_id") or "") != operation_run_id
            or str(event.get("action_id") or "") != action_id
            or str(event.get("event_family") or "") != "operation_event"
            or not _operation_event_schema_matches(event.get("event_type"), event.get("schema_version"))
            or type(sequence_number) is not int
            or sequence_number != previous_sequence + 1
        ):
            raise ValueError("agent tool inspect result operation event owner mismatch")
        previous_sequence = sequence_number
        evidence = {
            "event_id": _required_identity_text(
                event.get("event_id"),
                field="operation event id",
            ),
            "event_type": _required_identity_text(
                event.get("event_type"),
                field="operation event type",
            ),
            "workspace_id": _required_identity_text(
                event.get("workspace_id"),
                field="operation event workspace_id",
            ),
            "event_stream_id": _required_identity_text(
                event.get("event_stream_id"),
                field="operation event event_stream_id",
            ),
            "operation_run_id": _required_identity_text(
                event.get("operation_run_id"),
                field="operation event operation_run_id",
            ),
            "action_id": _required_identity_text(
                event.get("action_id"),
                field="operation event action_id",
            ),
            "event_family": _required_identity_text(
                event.get("event_family"),
                field="operation event family",
            ),
            "sequence_number": sequence_number,
            "idempotency_key": _required_identity_text(
                event.get("idempotency_key"),
                field="operation event idempotency_key",
            ),
            "schema_version": _required_identity_text(
                event.get("schema_version"),
                field="operation event schema_version",
            ),
            "payload_digest": _sha256_json(event_payload),
        }
        event_evidence.append(evidence)
        if binding.physical_owner_revision == "v3":
            audit_event_evidence.append(
                {
                    **evidence,
                    "actor": _required_identity_text(
                        event.get("actor"),
                        field="operation event actor",
                    ),
                    "source": _required_identity_text(
                        event.get("source"),
                        field="operation event source",
                    ),
                }
            )
    latest_event = events[-1]
    latest_event_sequence = latest_event.get("sequence_number")
    if type(latest_event_sequence) is not int or latest_event_sequence <= 0:
        raise ValueError("agent tool inspect result operation event revision unavailable")
    action_metadata = _json_dict(action.get("metadata_json"), field="action metadata")
    progress = _json_dict(operation.get("progress_json"), field="operation progress")
    result_ref = _json_dict(operation.get("result_ref_json"), field="operation result ref")
    phase = str(progress.get("phase") or "").strip()
    if not phase:
        raise ValueError("agent tool inspect result operation progress unavailable")

    workflow_ref = _required_json_dict(operation.get("workflow_ref_json"), field="operation workflow ref")
    try:
        normalized_workflow_ref = _workflow_ref_identity(
            workflow_ref,
            field="operation workflow ref",
            allow_variant_fields=False,
        )
    except ValueError as exc:
        raise ValueError("agent tool inspect result workflow command link mismatch") from exc

    command_plan_events: list[tuple[dict[str, Any], dict[str, Any], dict[str, str]]] = []
    for event in events:
        if str(event.get("event_type") or "") != "OperationCommandPlanned":
            continue
        event_payload = _required_json_dict(event.get("payload_json"), field="operation event payload")
        try:
            event_identity = _operation_command_planned_event_identity(
                event,
                event_payload,
                normalized_workflow_ref=normalized_workflow_ref,
                workspace_id=occurrence.workspace_id,
                action_id=action_id,
                operation_run_id=operation_run_id,
                action=action,
                operation=operation,
                require_start_acceptance_event=fail_closed_start_v2,
            )
        except ValueError as exc:
            raise ValueError("agent tool inspect result workflow command link mismatch") from exc
        command_plan_events.append((event, event_payload, event_identity))

    latest_command: dict[str, Any] = {}
    commands: list[dict[str, Any]] = []
    selected_plan_event: dict[str, Any] = {}
    command_identity: dict[str, Any] = {}
    if normalized_workflow_ref:
        commands = list(base_owner.get("workflow_commands") or [])
        latest_command = commands[0] if len(commands) == 1 else {}
        try:
            command_identity = _workflow_command_causal_identity(latest_command)
        except ValueError as exc:
            raise ValueError("agent tool inspect result workflow command link mismatch") from exc
        command_type = command_identity.get("command_type", "")
        registered_owner = str(DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(command_type) or "")
        command_link_matches = bool(
            latest_command
            and command_identity["operation_run_id"] == operation_run_id
            and command_identity["workflow_run_id"] == normalized_workflow_ref["workflow_run_id"]
            and command_identity["command_id"] == normalized_workflow_ref["command_id"]
            and command_type == normalized_workflow_ref["command_type"]
            and command_type in set(action_spec.allowed_workflow_command_types)
            and command_identity["owner"] == normalized_workflow_ref["owner"]
            and command_identity["owner"] == registered_owner
        )
        if not command_link_matches or len(command_plan_events) != 1:
            raise ValueError("agent tool inspect result workflow command link mismatch")
        plan_event, _plan_payload, plan_event_identity = command_plan_events[0]
        if plan_event_identity != normalized_workflow_ref:
            raise ValueError("agent tool inspect result workflow command link mismatch")
        if str(plan_event.get("schema_version") or "") == ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION:
            plan_payload = _required_json_dict(plan_event.get("payload_json"), field="operation event payload")
            owner_result_ref = _required_json_dict(
                plan_payload.get("owner_result_ref"),
                field="operation command planned event owner_result_ref",
            )
            if fail_closed_start_v2:
                _validate_start_acceptance_physical_dependencies(
                    owner_result_ref=owner_result_ref,
                    action=action,
                    operation=operation,
                    workflow_command=latest_command,
                    normalized_workflow_ref=normalized_workflow_ref,
                    action_events_desc=list(base_owner.get("action_events_desc") or []),
                    workflow_events=list(base_owner.get("workflow_events") or []),
                    start_result_slots=list(base_owner.get("start_result_slots") or []),
                    planned_event=plan_event,
                    child_commands=list(base_owner.get("workflow_child_commands") or []),
                )
        selected_plan_event = next(
            evidence for evidence in event_evidence if evidence["event_id"] == str(plan_event.get("event_id") or "")
        )
    elif command_plan_events or phase == "workflow_command_planned":
        raise ValueError("agent tool inspect result workflow command link mismatch")
    control_state = operation_run_control_state(
        operation_status=str(operation.get("status") or "").strip(),
        operation_run_id=operation_run_id,
        action_status=str(action.get("status") or "").strip(),
        action_approval_status=str(action.get("approval_status") or "").strip(),
        action_retry_operation_run_id=str(action_metadata.get("retry_operation_run_id") or "").strip(),
        operation_phase=phase,
    )
    if str(generic_control_preflight.get("status") or "") != "ready":
        control_state = operation_run_control_state_fail_closed(
            control_state,
            disabled_reason=str(generic_control_preflight.get("reason") or "").strip(),
        )
    control_state_record = control_state.to_record()

    if latest_command:
        control_policy = inspect_operation_control_policy_projection(
            command_type=command_identity["command_type"],
            owner=command_identity["owner"],
        )
    else:
        control_policy = inspect_operation_control_policy_projection()

    operation_status = str(operation.get("status") or "").strip()
    result_readiness = operation_result_readiness_projection(
        operation_status=operation_status,
        result_ref_present=bool(result_ref),
    )

    progress_projection: dict[str, Any] = {
        "phase": phase,
        "source_of_truth": "operation_runs.progress",
    }
    if binding.include_progress_reason and str(progress.get("reason") or "").strip():
        progress_projection["reason"] = str(progress["reason"])

    fail_closed_identity_projection = fail_closed_start_v2
    projected_action_type = action_spec.action_type if fail_closed_identity_projection else action_type
    projected_owner_module = (
        action_spec.owner_module if fail_closed_identity_projection else str(action["owner_module"])
    )
    projected_operation_type = (
        action_spec.operation_type if fail_closed_identity_projection else str(action["operation_type"])
    )

    snapshot: dict[str, Any] = {
        "action": {
            "workspace_id": occurrence.workspace_id,
            "action_id": action_id,
            "action_type": projected_action_type,
            "owner_module": projected_owner_module,
            "operation_type": projected_operation_type,
            "status": str(action["status"]),
        },
        "operation_run": {
            "workspace_id": occurrence.workspace_id,
            "action_id": action_id,
            "operation_run_id": operation_run_id,
            "owner_module": projected_owner_module,
            "operation_type": projected_operation_type,
            "status": operation_status,
        },
        "control_state": control_state_record,
        "control_policy": control_policy,
        "display_contract": display_contract,
        "progress": progress_projection,
        "result_readiness": result_readiness,
        "provenance": {
            "source_of_truth": "operation_runs.agent_actions.workflow_commands.operation_events",
            "operation_event_count": len(events),
            "workflow_command_count": len(commands),
            "latest_event_type": str(latest_event.get("event_type") or ""),
            "truncated": False,
            **(
                {
                    "latest_workflow_command_id": str(latest_command.get("command_id") or ""),
                    "latest_workflow_command_type": str(latest_command.get("command_type") or ""),
                }
                if latest_command
                else {}
            ),
        },
    }
    owner_output = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=snapshot,
        result_spec=binding.result_spec,
    )
    event_stream_digest = _sha256_json(event_evidence)
    physical_owner_evidence = {
        "schema_version": f"inspect_operation_physical_owner_fingerprint_{binding.physical_owner_revision}",
        "snapshot": snapshot,
        "event_stream_digest": event_stream_digest,
        "workflow_ref": normalized_workflow_ref,
        "workflow_command_causal_identity": command_identity,
        "selected_plan_event": selected_plan_event,
    }
    if binding.physical_owner_revision == "v3":
        physical_owner_evidence["raw_progress_digest"] = _sha256_json(progress)
        if fail_closed_identity_projection:
            physical_owner_evidence["persisted_identity_drift_digest"] = _sha256_json(
                {
                    "action_type": action_type,
                    "action_owner_module": str(action.get("owner_module") or ""),
                    "action_operation_type": str(action.get("operation_type") or ""),
                    "operation_owner_module": str(operation.get("owner_module") or ""),
                    "operation_operation_type": str(operation.get("operation_type") or ""),
                }
            )
        physical_owner_evidence["audit_event_stream_digest"] = _sha256_json(
            {
                "schema_version": "inspect_operation_audit_event_stream_v1",
                "events": audit_event_evidence,
            }
        )
    owner_result_digest = _sha256_json(physical_owner_evidence)
    owner_result_ref = {
        "schema_version": f"inspect_operation_owner_result_ref_{binding.physical_owner_revision}",
        "workspace_id": occurrence.workspace_id,
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "latest_event_id": str(latest_event.get("event_id") or ""),
        "latest_event_sequence": latest_event_sequence,
        "event_stream_digest": event_stream_digest,
        "workflow_ref": normalized_workflow_ref,
        "workflow_command_causal_identity": command_identity,
        "selected_plan_event": selected_plan_event,
        "owner_snapshot_digest": _sha256_json(snapshot),
        "physical_owner_fingerprint_schema_version": (
            f"inspect_operation_physical_owner_fingerprint_{binding.physical_owner_revision}"
        ),
        "physical_owner_fingerprint_digest": owner_result_digest,
    }
    if binding.physical_owner_revision == "v3":
        owner_result_ref["raw_progress_digest"] = _sha256_json(progress)
        owner_result_ref["audit_event_stream_digest"] = physical_owner_evidence["audit_event_stream_digest"]
    return AgentToolTerminalResult.from_serialized_result(
        result_attempt_id=result_attempt_id,
        provider_call_id=provider_call_id,
        tool_call_id=tool_call_id,
        action_id=action_id,
        operation_run_id=operation_run_id,
        owner_target_kind=INSPECT_OPERATION_OWNER_TARGET_KIND,
        owner_target_id=operation_run_id,
        owner_target_revision=latest_event_sequence,
        terminal_winner_id=str(latest_event.get("event_id") or ""),
        owner_result_ref=owner_result_ref,
        owner_result_digest=owner_result_digest,
        serialized_result=owner_output,
        is_error=False,
    )


def assert_exact_inspect_operation_terminal(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
    preflight: _InspectOperationPreflight,
) -> None:
    expected = terminal_from_locked_inspect_operation_owner(
        cursor,
        occurrence=occurrence,
        result_attempt_id=terminal.result_attempt_id,
        provider_call_id=terminal.provider_call_id,
        tool_call_id=terminal.tool_call_id,
        action_id=terminal.action_id,
        operation_run_id=terminal.operation_run_id,
        base_owner=base_owner,
        preflight=preflight,
    )
    if not json_contract_equal(expected.to_record(), terminal.to_record()):
        raise ValueError("agent tool inspect result physical owner or serializer mismatch")


__all__ = [
    "INSPECT_OPERATION_MASKED_ABSENCE_OWNER_TARGET_KIND",
    "INSPECT_OPERATION_OWNER_TARGET_KIND",
    "assert_exact_inspect_operation_terminal",
    "inspect_operation_result_lock_groups",
    "load_inspect_operation_base_owner",
    "preload_inspect_operation_start_occurrence_refs",
    "terminal_from_locked_inspect_operation_owner",
    "validate_inspect_operation_occurrence",
]
