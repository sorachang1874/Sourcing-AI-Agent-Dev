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
from typing import Any

from .agent_projection_query import (
    bind_inspect_operation_request,
    execute_inspect_operation,
    operation_result_readiness_projection,
)
from .agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from .durable_runtime import (
    DEFAULT_COMMAND_OWNER_REGISTRY,
    workflow_command_control_policy,
)
from .json_contract import json_contract_equal
from .operation_runtime import DEFAULT_ACTION_REGISTRY, operation_run_control_state

INSPECT_OPERATION_OWNER_TARGET_KIND = "operation_state_event_v1"


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


def validate_inspect_operation_occurrence(
    occurrence: AgentToolOccurrence,
    *,
    action_id: str,
    operation_run_id: str,
) -> None:
    """Require the exact isolated-canary query contract and target link."""

    from .agent_canary_registry import INSPECT_OPERATION_TOOL_SPEC

    expected = INSPECT_OPERATION_TOOL_SPEC
    expected_pins = {
        "tool_name": expected.tool_name,
        "tool_kind": expected.tool_kind,
        "effect_class": expected.behavior.effect_class,
        "tool_spec_version": expected.tool_spec_version,
        "tool_spec_digest": expected.tool_spec_digest,
        "request_schema_version": expected.request.schema_version,
        "request_schema_digest": expected.request.schema_digest,
        "result_schema_version": expected.result.schema_version,
        "result_schema_digest": expected.result.schema_digest,
        "serializer_owner": expected.result.serializer_owner.owner_id,
        "serializer_revision": expected.result.serializer_owner.owner_revision,
        "serializer_contract_digest": expected.result.serializer_owner.owner_contract_digest,
    }
    mismatches = [field for field, value in expected_pins.items() if str(getattr(occurrence, field)) != str(value)]
    if mismatches:
        raise ValueError("agent tool inspect result occurrence contract mismatch: " + ", ".join(mismatches))
    canonical_args = occurrence.canonical_args
    if canonical_args != {"operation_run_id": operation_run_id}:
        raise ValueError("agent tool inspect result canonical args mismatch")
    if not action_id or not operation_run_id:
        raise ValueError("agent tool inspect result action and operation links required")


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
    action_id: str,
    operation_run_id: str,
) -> dict[str, dict[str, Any]]:
    """Lock Operation then Action in the shared D1n order."""

    from .control_plane_live_postgres import _fetch_one_dict_row

    cursor.execute(
        "SELECT * FROM operation_runs WHERE operation_run_id = %s FOR UPDATE",
        (operation_run_id,),
    )
    operation = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    cursor.execute("SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE", (action_id,))
    action = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    return {"action": action, "operation_run": operation}


def terminal_from_locked_inspect_operation_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    action_id: str,
    operation_run_id: str,
    base_owner: dict[str, dict[str, Any]],
) -> AgentToolTerminalResult:
    """Rebuild one exact query result while all physical owner rows are fenced."""

    from .control_plane_live_postgres import _fetch_all_dict_rows

    validate_inspect_operation_occurrence(
        occurrence,
        action_id=action_id,
        operation_run_id=operation_run_id,
    )
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
        raise ValueError("agent tool inspect result exact owner not found")

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

    cursor.execute(
        "SELECT * FROM operation_events "
        "WHERE event_stream_id = %s AND workspace_id = %s "
        "ORDER BY sequence_number DESC LIMIT 100001 FOR UPDATE",
        (operation_run_id, occurrence.workspace_id),
    )
    events_desc = _fetch_all_dict_rows(cursor)
    if not events_desc or len(events_desc) > 100_000:
        raise ValueError("agent tool inspect result operation event revision unavailable")
    events = list(reversed(events_desc))
    previous_sequence = 0
    for event in events:
        sequence_number = event.get("sequence_number")
        if (
            str(event.get("operation_run_id") or "") != operation_run_id
            or str(event.get("action_id") or "") != action_id
            or str(event.get("event_family") or "") != "operation_event"
            or str(event.get("schema_version") or "") != "operation_event_v1"
            or type(sequence_number) is not int
            or sequence_number <= previous_sequence
        ):
            raise ValueError("agent tool inspect result operation event owner mismatch")
        previous_sequence = sequence_number
    latest_event = events[-1]
    latest_event_sequence = latest_event.get("sequence_number")
    if type(latest_event_sequence) is not int or latest_event_sequence <= 0:
        raise ValueError("agent tool inspect result operation event revision unavailable")
    workflow_ref = _json_dict(operation.get("workflow_ref_json"), field="operation workflow ref")

    workflow_ref_fields = ("workflow_run_id", "command_id", "command_type", "owner")
    normalized_workflow_ref = {field: str(workflow_ref.get(field) or "").strip() for field in workflow_ref_fields}
    if any(normalized_workflow_ref.values()) and not all(normalized_workflow_ref.values()):
        raise ValueError("agent tool inspect result workflow command link mismatch")
    latest_command: dict[str, Any] = {}
    commands: list[dict[str, Any]] = []
    if all(normalized_workflow_ref.values()):
        cursor.execute(
            "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
            (normalized_workflow_ref["command_id"],),
        )
        commands = _fetch_all_dict_rows(cursor)
        latest_command = commands[0] if len(commands) == 1 else {}
        command_type = str(latest_command.get("command_type") or "")
        registered_owner = str(DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(command_type) or "")
        command_link_matches = bool(
            latest_command
            and str(latest_command.get("operation_id") or "") == operation_run_id
            and str(latest_command.get("workflow_run_id") or "") == normalized_workflow_ref["workflow_run_id"]
            and command_type == normalized_workflow_ref["command_type"]
            and command_type in set(action_spec.allowed_workflow_command_types)
            and str(latest_command.get("owner") or "") == normalized_workflow_ref["owner"]
            and str(latest_command.get("owner") or "") == registered_owner
        )
        matching_plan_event = False
        if command_link_matches:
            for event in events:
                if str(event.get("event_type") or "") != "OperationCommandPlanned":
                    continue
                event_payload = _json_dict(event.get("payload_json"), field="operation event payload")
                if all(
                    str(event_payload.get(field) or "").strip() == normalized_workflow_ref[field]
                    for field in workflow_ref_fields
                ):
                    matching_plan_event = True
                    break
        if not command_link_matches or not matching_plan_event:
            raise ValueError("agent tool inspect result workflow command link mismatch")

    action_metadata = _json_dict(action.get("metadata_json"), field="action metadata")
    progress = _json_dict(operation.get("progress_json"), field="operation progress")
    result_ref = _json_dict(operation.get("result_ref_json"), field="operation result ref")
    phase = str(progress.get("phase") or "").strip()
    if not phase:
        raise ValueError("agent tool inspect result operation progress unavailable")
    control_state = operation_run_control_state(
        operation_status=str(operation.get("status") or "").strip(),
        operation_run_id=operation_run_id,
        action_status=str(action.get("status") or "").strip(),
        action_approval_status=str(action.get("approval_status") or "").strip(),
        action_retry_operation_run_id=str(action_metadata.get("retry_operation_run_id") or "").strip(),
        operation_phase=phase,
    ).to_record()

    if latest_command:
        policy = workflow_command_control_policy(
            command_type=str(latest_command.get("command_type") or "").strip(),
            owner=str(latest_command.get("owner") or "").strip(),
        ).to_record()
        control_policy = {
            "status": "available",
            "source_of_truth": str(policy["control_source_of_truth"]),
            "fallback_status": str(policy["fallback_status"]),
            "command_type": str(policy["command_type"]),
            "owner": str(policy["owner"]),
            "running_control_maturity": str(policy["running_control_maturity"]),
            "running_control_gap_status": str(policy["running_control_gap_status"]),
            "running_control_surface": str(policy["running_control_surface"]),
            "running_cancel_supported": bool(policy["running_cancel_supported"]),
            "running_resume_supported": bool(policy["running_resume_supported"]),
        }
    else:
        control_policy = {
            "status": "not_applicable",
            "source_of_truth": "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
            "fallback_status": "fail_closed",
        }

    operation_status = str(operation.get("status") or "").strip()
    result_readiness = operation_result_readiness_projection(
        operation_status=operation_status,
        result_ref_present=bool(result_ref),
    )

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
        "control_state": control_state,
        "control_policy": control_policy,
        "display_contract": display_contract,
        "progress": {
            "phase": phase,
            **({"reason": str(progress["reason"])} if str(progress.get("reason") or "").strip() else {}),
            "source_of_truth": "operation_runs.progress",
        },
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
    request = bind_inspect_operation_request(
        occurrence.canonical_args,
        workspace_id=occurrence.workspace_id,
        action_id=action_id,
        actor_id=occurrence.actor_id,
    )
    owner_output = execute_inspect_operation(request=request, owner_snapshot=snapshot)
    owner_result_digest = _sha256_json(snapshot)
    owner_result_ref = {
        "schema_version": "inspect_operation_owner_result_ref_v1",
        "workspace_id": occurrence.workspace_id,
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "latest_event_id": str(latest_event.get("event_id") or ""),
        "latest_event_sequence": latest_event_sequence,
        "owner_snapshot_digest": owner_result_digest,
    }
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
    base_owner: dict[str, dict[str, Any]],
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
    )
    if not json_contract_equal(expected.to_record(), terminal.to_record()):
        raise ValueError("agent tool inspect result physical owner or serializer mismatch")


__all__ = [
    "INSPECT_OPERATION_OWNER_TARGET_KIND",
    "assert_exact_inspect_operation_terminal",
    "inspect_operation_result_lock_groups",
    "load_inspect_operation_base_owner",
    "terminal_from_locked_inspect_operation_owner",
    "validate_inspect_operation_occurrence",
]
