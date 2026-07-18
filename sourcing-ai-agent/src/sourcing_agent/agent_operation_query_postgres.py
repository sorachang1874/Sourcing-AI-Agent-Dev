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
from typing import Any

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

INSPECT_OPERATION_OWNER_TARGET_KIND = "operation_state_event_v1"
INSPECT_OPERATION_MASKED_ABSENCE_OWNER_TARGET_KIND = "inspect_operation_masked_error_v1"

_WORKFLOW_REF_FIELDS = ("workflow_run_id", "command_id", "command_type", "owner")
_OPERATION_EVENT_SCHEMA_BY_TYPE = {
    "OperationCommandPlanned": {
        "operation_event_v1",
        "acquisition_start_command_acceptance.v1",
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


def _operation_command_planned_event_identity(
    event_payload: dict[str, Any],
    *,
    normalized_workflow_ref: dict[str, str],
    workspace_id: str,
    action_id: str,
    operation_run_id: str,
) -> dict[str, str]:
    try:
        return _workflow_ref_identity(
            event_payload,
            field="operation command planned event",
            allow_variant_fields=True,
        )
    except ValueError:
        owner_result_ref = event_payload.get("owner_result_ref")
        if not isinstance(owner_result_ref, dict) or not normalized_workflow_ref:
            raise
        workflow_run_id = _required_identity_text(
            owner_result_ref.get("workflow_run_id"),
            field="operation command planned event owner_result_ref workflow_run_id",
        )
        workflow_command_id = _required_identity_text(
            owner_result_ref.get("workflow_command_id"),
            field="operation command planned event owner_result_ref workflow_command_id",
        )
        if (
            workflow_run_id != normalized_workflow_ref["workflow_run_id"]
            or workflow_command_id != normalized_workflow_ref["command_id"]
            or str(owner_result_ref.get("workspace_id") or "").strip() != workspace_id
            or str(owner_result_ref.get("action_id") or "").strip() != action_id
            or str(owner_result_ref.get("operation_run_id") or "").strip() != operation_run_id
            or str(owner_result_ref.get("schema_version") or "").strip()
            != "acquisition_start_command_acceptance_owner_result_ref.v1"
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
) -> tuple[tuple[str, ...], ...]:
    groups: list[tuple[str, ...]] = [
        (f"operation_events:{operation_run_id}",),
        (f"operation_runs:id:{operation_run_id}",),
        (f"agent_actions:id:{action_id}",),
    ]
    if include_result_slot:
        groups.append((f"agent_tool_result_slots:id:{occurrence.result_slot_id}",))
    return tuple(groups)


def load_inspect_operation_base_owner(
    cursor: Any,
    *,
    workspace_id: str,
    action_id: str,
    operation_run_id: str,
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
    events_desc: list[dict[str, Any]] = []
    command_id = ""
    if exact_owner:
        workflow_ref = _required_json_dict(
            operation.get("workflow_ref_json"),
            field="operation workflow ref",
        )
        command_id = _workflow_ref_identity(
            workflow_ref,
            field="operation workflow ref",
            allow_variant_fields=False,
        ).get("command_id", "")
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
            (operation_run_id,),
        )
        events_desc = _fetch_all_dict_rows(cursor)
    return {
        "action": action,
        "operation_run": operation,
        "workflow_commands": commands,
        "operation_events_desc": events_desc,
    }


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
        if any(str(row.get(field) or "") != value for field, value in owner_fields.items()):
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
                event_payload,
                normalized_workflow_ref=normalized_workflow_ref,
                workspace_id=occurrence.workspace_id,
                action_id=action_id,
                operation_run_id=operation_run_id,
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
    generic_control_preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
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

    snapshot: dict[str, Any] = {
        "action": {
            "workspace_id": occurrence.workspace_id,
            "action_id": action_id,
            "action_type": action_type,
            "owner_module": str(action["owner_module"]),
            "operation_type": str(action["operation_type"]),
            "status": str(action["status"]),
        },
        "operation_run": {
            "workspace_id": occurrence.workspace_id,
            "action_id": action_id,
            "operation_run_id": operation_run_id,
            "owner_module": str(operation["owner_module"]),
            "operation_type": str(operation["operation_type"]),
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
    "terminal_from_locked_inspect_operation_owner",
    "validate_inspect_operation_occurrence",
]
