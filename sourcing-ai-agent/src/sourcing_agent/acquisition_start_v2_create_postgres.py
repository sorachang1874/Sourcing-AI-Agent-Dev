"""Atomic PostgreSQL create UoW for the bounded acquisition-start v2 path.

The submit UoW persists an approval-required Action.  This module owns the
single follow-up transaction that records the human confirmation, creates the
linked OperationRun and root WorkflowCommand, materializes reducer state, and
records the command-acceptance winner.  It performs no provider/model/network
I/O and creates no runtime-outbox row.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Mapping
from typing import Any

from .acquisition_start_v2 import (
    ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
    ACQUISITION_START_ACTION_TYPE,
    AcquisitionConfirmationReceipt,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2OwnerBinder,
    build_acquisition_parent_budget_envelope_ref,
    build_acquisition_start_v2_root_command_payload,
)
from .acquisition_start_v2_postgres import (
    AcquisitionStartV2SubmissionBinding,
    _assert_exact_action,
    _assert_pending_result_slot,
    _assert_replayable_result_slot,
    _canonical_submit_rows,
    _insert_row,
    _LockedPreviewReader,
    _utc_second_iso,
    acquisition_start_v2_operation_event_id,
    revalidate_acquisition_start_v2_occurrence,
)
from .agent_tool_result_slot import AgentToolOccurrence
from .durable_runtime import (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    attach_command_causality,
    command_causality_for,
    command_id_for,
    default_stage_id_for_command_type,
    reduce_workflow_events,
    summarize_workflow_command_counts,
)
from .operation_runtime import operation_run_id_for

_ALLOWED_APPROVAL_ACTOR_KINDS = frozenset({"authenticated_user", "open_operator"})
_OWNER_MODULE = "acquisition_run_writer"
_OPERATION_TYPE = "acquisition_run"
_WORKFLOW_TYPE = "agent_callable_workflow_command"
_WORKFLOW_EVENT_SCHEMA_VERSION = "workflow_event_v1"
_WORKFLOW_COMMAND_SCHEMA_VERSION = "workflow_command_v1"
_WORKFLOW_CURRENT_STATE_SCHEMA_VERSION = "workflow_current_state_v1"
_WINNER_SCHEMA_VERSION = "acquisition_start_command_acceptance.v1"
_WINNER_OWNER_REF_SCHEMA_VERSION = "acquisition_start_command_acceptance_owner_result_ref.v1"
_WORKFLOW_ACTOR = "operation_workflow_command_planner"
_WORKFLOW_SOURCE = "operation_run_dispatch"
_CREATE_SOURCE = "agent_start_v2_create_uow"
_RESULT_ACCEPTANCE_HOLD_UNTIL = "9999-12-31 23:59:59"
_MAX_ATTEMPTS = 5
_RETRY_POLICY = {"kind": "operation_acquisition_run_create", "retry_delay_seconds": 30}
_OWNER_RESULT_REF_FIELDS = (
    "schema_version",
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "action_id",
    "operation_run_id",
    "workflow_run_id",
    "workflow_command_id",
    "terminal_winner_id",
    "terminal_winner_sequence_number",
    "command_source_event_id",
    "command_source_event_sequence_number",
    "command_source_event_contract_digest",
    "confirmation_receipt_ref",
    "parent_budget_envelope_ref",
    "start_snapshot_digest",
    "root_command_payload_digest",
    "result_occurrence_ref",
)

_ACTION_JSON_FIELDS = frozenset({"target_ref_json", "input_json", "budget_json", "result_ref_json", "metadata_json"})
_OPERATION_JSON_FIELDS = frozenset(
    {"progress_json", "workflow_ref_json", "cost_budget_json", "result_ref_json", "metadata_json"}
)
_COMMAND_JSON_FIELDS = frozenset(
    {
        "input_artifact_refs_json",
        "output_artifact_refs_json",
        "produced_entity_counts_json",
        "downstream_command_ids_json",
        "payload_json",
        "artifact_refs_json",
        "retry_policy_json",
        "result_json",
    }
)
_CURRENT_STATE_JSON_FIELDS = frozenset(
    {
        "completion_proofs_json",
        "active_command_counts_json",
        "terminal_command_counts_json",
        "read_model_pointers_json",
        "migration_status_json",
        "metadata_json",
    }
)

_FAULT_POINTS = frozenset(
    {
        "",
        "after_receipt_event_write",
        "after_action_update",
        "after_operation_run_write",
        "after_workflow_started_event_write",
        "after_command_plan_requested_event_write",
        "after_command_write",
        "after_current_state_write",
        "after_planned_event_write",
        "after_commit",
    }
)

class _StartCreateLockBusy(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _workflow_event_id(*, workflow_run_id: str, sequence_number: int, idempotency_key: str) -> str:
    seed = f"{workflow_run_id}:{sequence_number}:{idempotency_key}"
    return "evt_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _workflow_run_id(operation_run_id: str) -> str:
    return "wf_operation_" + hashlib.sha1(operation_run_id.encode("utf-8")).hexdigest()[:24]


def _thaw_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json_value(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json_value(child) for child in value]
    return value


def _json_value(value: Any) -> Any:
    if isinstance(value, (Mapping, list, tuple)):
        return _thaw_json_value(value)
    if value is None or value == "":
        return {}
    try:
        return _thaw_json_value(json.loads(str(value)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _assert_json_equal(label: str, actual: Any, expected: Any) -> None:
    if _json_value(actual) != _json_value(expected):
        raise ValueError(f"acquisition start create {label} immutable identity collision")


def _assert_row_exact(
    label: str,
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    json_fields: frozenset[str] = frozenset(),
) -> None:
    mismatches: list[str] = []
    for field, expected_value in expected.items():
        if field in json_fields:
            _assert_json_equal(f"{label}.{field}", actual.get(field), expected_value)
            continue
        actual_value = actual.get(field)
        if str(actual_value if actual_value is not None else "") != str(
            expected_value if expected_value is not None else ""
        ):
            mismatches.append(field)
    if mismatches:
        raise ValueError(f"acquisition start create {label} immutable identity collision: " + ", ".join(mismatches))


def _operation_event_contract_digest(event: Mapping[str, Any]) -> str:
    stable = {
        "event_id": event["event_id"],
        "workflow_run_id": event["workflow_run_id"],
        "operation_id": event["operation_id"],
        "command_id": event["command_id"],
        "activity_attempt_id": event["activity_attempt_id"],
        "event_family": event["event_family"],
        "event_type": event["event_type"],
        "sequence_number": event["sequence_number"],
        "idempotency_key": event["idempotency_key"],
        "actor": event["actor"],
        "source": event["source"],
        "payload": event["payload"],
        "artifact_refs": event["artifact_refs"],
        "schema_version": event["schema_version"],
    }
    return _sha256_json(stable)


def _receipt_event_key(binding: AcquisitionStartV2SubmissionBinding) -> str:
    return f"{binding.start_idempotency}:ActionApproved"


def _receipt_event_id(binding: AcquisitionStartV2SubmissionBinding) -> str:
    return acquisition_start_v2_operation_event_id(
        event_stream_id=binding.action_id,
        sequence_number=2,
        idempotency_key=_receipt_event_key(binding),
    )


def _pending_action_approval_view(
    action: Mapping[str, Any],
    *,
    binding: AcquisitionStartV2SubmissionBinding,
) -> dict[str, Any]:
    target_ref = _json_value(action.get("target_ref_json"))
    input_payload = _json_value(action.get("input_json"))
    if not isinstance(target_ref, dict) or not isinstance(input_payload, dict):
        raise ValueError("acquisition start create pending Action request is invalid")
    return {
        "action_id": binding.action_id,
        "action_type": ACQUISITION_START_ACTION_TYPE,
        "workspace_id": binding.occurrence.workspace_id,
        "requester_id": binding.occurrence.actor_id,
        "request_schema_version": binding.occurrence.request_schema_version,
        "request_schema_digest": binding.occurrence.request_schema_digest,
        "request": {"input_payload": input_payload, "target_ref": target_ref},
        "state": "pending_approval",
    }


def _build_receipt(
    *,
    binding: AcquisitionStartV2SubmissionBinding,
    action: Mapping[str, Any],
    preview: Mapping[str, Any],
    approval_actor_id: str,
    approval_actor_kind: str,
    approval_policy_revision: str,
    approved_at: str,
) -> AcquisitionConfirmationReceipt:
    return AcquisitionStartV2OwnerBinder(_LockedPreviewReader(preview)).confirm_exact_action(
        persisted_action=_pending_action_approval_view(action, binding=binding),
        context=AcquisitionStartV2BindContext(
            workspace_id=binding.occurrence.workspace_id,
            requester_id=binding.occurrence.actor_id,
        ),
        approval_actor_id=approval_actor_id,
        approval_actor_kind=approval_actor_kind,
        receipt_id=_receipt_event_id(binding),
        approval_policy_revision=approval_policy_revision,
        approved_at=approved_at,
    )


def _workflow_contracts(
    *,
    binding: AcquisitionStartV2SubmissionBinding,
    receipt: AcquisitionConfirmationReceipt,
    operation_run_id: str,
    workflow_run_id: str,
) -> dict[str, Any]:
    from .agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
    from .control_plane_live_postgres import _workflow_command_causality_columns_from_payload

    occurrence = binding.occurrence
    root = build_acquisition_start_v2_root_command_payload(
        snapshot=binding.bound_request.snapshot,
        receipt=receipt,
        operation_run_id=operation_run_id,
        workflow_run_id=workflow_run_id,
    ).to_record()
    command_key = f"acquisition.run.create:start-v2:{receipt.receipt_digest}"
    workflow_command_id = command_id_for(workflow_run_id, command_key)
    stage_key = default_stage_id_for_command_type(ACQUISITION_RUN_CREATE_COMMAND_TYPE)
    owner = DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(ACQUISITION_RUN_CREATE_COMMAND_TYPE)
    if stage_key != "acquisition_run_create" or owner != _OWNER_MODULE:
        raise ValueError("acquisition start create command registry drift")

    started_key = f"{workflow_run_id}:operation_command_started:{operation_run_id}"
    started = {
        "event_id": _workflow_event_id(
            workflow_run_id=workflow_run_id,
            sequence_number=1,
            idempotency_key=started_key,
        ),
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "command_id": "",
        "activity_attempt_id": "",
        "event_family": "workflow_event",
        "event_type": "WorkflowStarted",
        "sequence_number": 1,
        "idempotency_key": started_key,
        "actor": _WORKFLOW_ACTOR,
        "source": _WORKFLOW_SOURCE,
        "payload": {
            "workflow_type": _WORKFLOW_TYPE,
            "stage_key": stage_key,
            "operation_run_id": operation_run_id,
            "action_id": binding.action_id,
            "action_type": ACQUISITION_START_ACTION_TYPE,
            "migration_phase": "W11_agent_callable_workflow_command",
        },
        "artifact_refs": [],
        "schema_version": _WORKFLOW_EVENT_SCHEMA_VERSION,
    }
    source_key = f"{command_key}:plan"
    source_event_id = _workflow_event_id(
        workflow_run_id=workflow_run_id,
        sequence_number=2,
        idempotency_key=source_key,
    )
    root_command_payload = {**root, "operation_id": operation_run_id}
    causality = command_causality_for(
        workflow_run_id=workflow_run_id,
        operation_id=operation_run_id,
        stage_id=stage_key,
        command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        owner=owner,
        idempotency_key=command_key,
        source_event={
            "event_id": source_event_id,
            "event_type": "CommandPlanRequested",
            "command_id": "",
            "payload": {"stage_key": stage_key},
        },
        command_payload=root_command_payload,
        artifact_refs=(),
    )
    command_payload = attach_command_causality(root_command_payload, causality=causality)
    source = {
        "event_id": source_event_id,
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "command_id": "",
        "activity_attempt_id": "",
        "event_family": "workflow_event",
        "event_type": "CommandPlanRequested",
        "sequence_number": 2,
        "idempotency_key": source_key,
        "actor": _WORKFLOW_ACTOR,
        "source": _WORKFLOW_SOURCE,
        "payload": {
            "workflow_type": _WORKFLOW_TYPE,
            "stage_key": stage_key,
            "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE,
            "idempotency_key": command_key,
            "payload": command_payload,
            "artifact_refs": [],
            "not_before_at": _RESULT_ACCEPTANCE_HOLD_UNTIL,
            "max_attempts": _MAX_ATTEMPTS,
            "retry_policy": dict(_RETRY_POLICY),
        },
        "artifact_refs": [],
        "schema_version": _WORKFLOW_EVENT_SCHEMA_VERSION,
    }
    reduced = reduce_workflow_events(current_state={}, new_events=[started, source], existing_commands=[])
    if len(reduced.commands) != 1 or reduced.outbox:
        raise ValueError("acquisition start create reducer contract drift")
    planned_command = reduced.commands[0]
    if (
        planned_command.command_type != ACQUISITION_RUN_CREATE_COMMAND_TYPE
        or planned_command.owner != owner
        or planned_command.idempotency_key != command_key
        or planned_command.payload != command_payload
        or planned_command.artifact_refs
        or planned_command.not_before_at != _RESULT_ACCEPTANCE_HOLD_UNTIL
        or planned_command.max_attempts != _MAX_ATTEMPTS
        or planned_command.retry_policy != _RETRY_POLICY
    ):
        raise ValueError("acquisition start create reducer command drift")
    active_counts, terminal_counts = summarize_workflow_command_counts(
        [{"owner": owner, "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE, "status": "queued"}]
    )
    current_state = {
        "schema_version": _WORKFLOW_CURRENT_STATE_SCHEMA_VERSION,
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "workflow_type": _WORKFLOW_TYPE,
        "status": reduced.status,
        "current_stage_key": reduced.current_stage_key,
        "completion_proofs": reduced.completion_proofs,
        "active_command_counts": active_counts,
        "terminal_command_counts": terminal_counts,
        "read_model_pointers": {},
        "migration_status": {},
        "last_processed_sequence_number": 2,
        "reducer_version": reduced.reducer_version,
        "metadata": reduced.metadata,
    }

    budget_owner = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
    if budget_owner is None:
        raise ValueError("acquisition start create budget owner is unavailable")
    parent_budget_ref = build_acquisition_parent_budget_envelope_ref(receipt, budget_owner).to_record()
    planned_key = f"{binding.start_idempotency}:OperationCommandPlanned:{workflow_command_id}"
    planned_event_id = acquisition_start_v2_operation_event_id(
        event_stream_id=operation_run_id,
        sequence_number=1,
        idempotency_key=planned_key,
    )
    owner_result_ref = {
        "schema_version": _WINNER_OWNER_REF_SCHEMA_VERSION,
        "runtime_namespace": occurrence.runtime_namespace,
        "provider_mode": occurrence.provider_mode,
        "workspace_id": occurrence.workspace_id,
        "action_id": binding.action_id,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "workflow_command_id": workflow_command_id,
        "terminal_winner_id": planned_event_id,
        "terminal_winner_sequence_number": 1,
        "command_source_event_id": source_event_id,
        "command_source_event_sequence_number": 2,
        "command_source_event_contract_digest": _operation_event_contract_digest(source),
        "confirmation_receipt_ref": {
            "receipt_id": receipt.receipt_id,
            "receipt_digest": receipt.receipt_digest,
        },
        "parent_budget_envelope_ref": parent_budget_ref,
        "start_snapshot_digest": binding.bound_request.snapshot.snapshot_digest,
        "root_command_payload_digest": root["payload_digest"],
        "result_occurrence_ref": binding.result_occurrence_ref,
    }
    if tuple(owner_result_ref) != _OWNER_RESULT_REF_FIELDS:
        raise ValueError("acquisition start create owner-result contract drift")
    owner_result_digest = _sha256_json(owner_result_ref)

    causality_columns = _workflow_command_causality_columns_from_payload(command_payload)
    return {
        "root": root,
        "command_key": command_key,
        "workflow_command_id": workflow_command_id,
        "started": started,
        "source": source,
        "command_payload": command_payload,
        "command_causality_columns": causality_columns,
        "current_state": current_state,
        "owner_result_ref": owner_result_ref,
        "owner_result_digest": owner_result_digest,
        "planned_key": planned_key,
        "planned_event_id": planned_event_id,
    }


def _canonical_create_rows(
    *,
    binding: AcquisitionStartV2SubmissionBinding,
    receipt: AcquisitionConfirmationReceipt,
    submitted_action: Mapping[str, Any],
) -> dict[str, Any]:
    from .control_plane_live_postgres import _json_dump

    occurrence = binding.occurrence
    approved_at = receipt.to_record()["approved_at"]
    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type=_OPERATION_TYPE,
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)
    contracts = _workflow_contracts(
        binding=binding,
        receipt=receipt,
        operation_run_id=operation_run_id,
        workflow_run_id=workflow_run_id,
    )
    owner_result_ref = contracts["owner_result_ref"]
    owner_result_digest = contracts["owner_result_digest"]
    workflow_ref = {
        "workflow_run_id": workflow_run_id,
        "command_id": contracts["workflow_command_id"],
        "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        "owner": _OWNER_MODULE,
    }
    receipt_key = _receipt_event_key(binding)
    receipt_event = {
        "event_id": receipt.receipt_id,
        "workspace_id": occurrence.workspace_id,
        "event_stream_id": binding.action_id,
        "operation_run_id": operation_run_id,
        "action_id": binding.action_id,
        "event_family": "operation_event",
        "event_type": "ActionApproved",
        "sequence_number": 2,
        "idempotency_key": receipt_key,
        "occurred_at": approved_at,
        "recorded_at": approved_at,
        "actor": str(receipt.to_record()["approval_actor_id"]),
        "source": _CREATE_SOURCE,
        "payload_json": _json_dump(receipt.to_record()),
        "schema_version": ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
        "created_at": approved_at,
    }
    action = dict(submitted_action)
    action.update(
        {
            "approval_status": "approved",
            "status": "queued",
            "result_ref_json": _json_dump(owner_result_ref),
            "updated_at": approved_at,
        }
    )
    operation = {
        "operation_run_id": operation_run_id,
        "workspace_id": occurrence.workspace_id,
        "action_id": binding.action_id,
        "owner_module": _OWNER_MODULE,
        "operation_type": _OPERATION_TYPE,
        "request_schema_version": occurrence.request_schema_version,
        "request_schema_digest": occurrence.request_schema_digest,
        "tool_name": occurrence.tool_name,
        "tool_spec_version": occurrence.tool_spec_version,
        "tool_spec_digest": occurrence.tool_spec_digest,
        "result_schema_version": occurrence.result_schema_version,
        "result_schema_digest": occurrence.result_schema_digest,
        "result_serializer_owner": occurrence.serializer_owner,
        "result_serializer_revision": occurrence.serializer_revision,
        "result_serializer_contract_digest": occurrence.serializer_contract_digest,
        "status": "queued",
        "progress_json": _json_dump({"phase": "workflow_command_planned"}),
        "workflow_ref_json": _json_dump(workflow_ref),
        "cost_budget_json": _json_dump(receipt.to_record()["budget"]),
        "idempotency_key": binding.start_idempotency,
        "result_ref_json": _json_dump(owner_result_ref),
        "metadata_json": _json_dump({}),
        "started_at": "",
        "completed_at": "",
        "created_at": approved_at,
        "updated_at": approved_at,
    }

    workflow_events: list[dict[str, Any]] = []
    for contract in (contracts["started"], contracts["source"]):
        workflow_events.append(
            {
                "event_id": contract["event_id"],
                "workflow_run_id": workflow_run_id,
                "operation_id": operation_run_id,
                "command_id": "",
                "activity_attempt_id": "",
                "event_family": "workflow_event",
                "event_type": contract["event_type"],
                "sequence_number": contract["sequence_number"],
                "idempotency_key": contract["idempotency_key"],
                "occurred_at": approved_at,
                "recorded_at": approved_at,
                "actor": contract["actor"],
                "source": contract["source"],
                "payload_json": _json_dump(contract["payload"]),
                "artifact_refs_json": _json_dump(contract["artifact_refs"]),
                "schema_version": contract["schema_version"],
                "created_at": approved_at,
            }
        )
    command = {
        "command_id": contracts["workflow_command_id"],
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        "owner": _OWNER_MODULE,
        **contracts["command_causality_columns"],
        "status": "queued",
        "idempotency_key": contracts["command_key"],
        "payload_json": _json_dump(contracts["command_payload"]),
        "artifact_refs_json": _json_dump([]),
        "not_before_at": _RESULT_ACCEPTANCE_HOLD_UNTIL,
        "attempt": 0,
        "max_attempts": _MAX_ATTEMPTS,
        "retry_policy_json": _json_dump(_RETRY_POLICY),
        "lease_owner": "",
        "lease_expires_at": "",
        "heartbeat_at": "",
        "last_error": "",
        "result_json": _json_dump({}),
        "schema_version": _WORKFLOW_COMMAND_SCHEMA_VERSION,
        "created_at": approved_at,
        "updated_at": approved_at,
    }
    state = contracts["current_state"]
    current_state = {
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "workflow_type": state["workflow_type"],
        "status": state["status"],
        "current_stage_key": state["current_stage_key"],
        "completion_proofs_json": _json_dump(state["completion_proofs"]),
        "active_command_counts_json": _json_dump(state["active_command_counts"]),
        "terminal_command_counts_json": _json_dump(state["terminal_command_counts"]),
        "read_model_pointers_json": _json_dump(state["read_model_pointers"]),
        "migration_status_json": _json_dump(state["migration_status"]),
        "last_processed_sequence_number": state["last_processed_sequence_number"],
        "reducer_version": state["reducer_version"],
        "schema_version": state["schema_version"],
        "metadata_json": _json_dump(state["metadata"]),
        "created_at": approved_at,
        "updated_at": approved_at,
    }
    planned_event = {
        "event_id": contracts["planned_event_id"],
        "workspace_id": occurrence.workspace_id,
        "event_stream_id": operation_run_id,
        "operation_run_id": operation_run_id,
        "action_id": binding.action_id,
        "event_family": "operation_event",
        "event_type": "OperationCommandPlanned",
        "sequence_number": 1,
        "idempotency_key": contracts["planned_key"],
        "occurred_at": approved_at,
        "recorded_at": approved_at,
        "actor": _WORKFLOW_ACTOR,
        "source": _CREATE_SOURCE,
        "payload_json": _json_dump({"owner_result_ref": owner_result_ref, "owner_result_digest": owner_result_digest}),
        "schema_version": _WINNER_SCHEMA_VERSION,
        "created_at": approved_at,
    }
    return {
        "action": action,
        "operation_run": operation,
        "receipt_event": receipt_event,
        "workflow_events": workflow_events,
        "workflow_command": command,
        "workflow_current_state": current_state,
        "planned_event": planned_event,
        "confirmation_receipt": receipt.to_record(),
        "owner_result_ref": owner_result_ref,
        "owner_result_digest": owner_result_digest,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "workflow_command_id": contracts["workflow_command_id"],
        "command_key": contracts["command_key"],
    }


def _require_authoritative_dependencies(adapter: Any) -> None:
    required = (
        "operation_runs",
        "agent_actions",
        "operation_events",
        "agent_tool_result_slots",
        "acquisition_plan_previews",
        "workflow_events",
        "workflow_commands",
        "workflow_current_state",
    )
    authoritative = getattr(adapter, "is_authoritative", None)
    if not callable(authoritative) or not all(
        adapter.should_prefer_read(table_name) and authoritative(table_name) for table_name in required
    ):
        raise RuntimeError("acquisition start create requires authoritative PostgreSQL dependencies")


def _validate_invocation(
    *,
    approval_actor_id: str,
    approval_actor_kind: str,
    approval_policy_revision: str,
    lock_timeout_seconds: float,
    fault_injection_point: str,
) -> float:
    if (
        type(approval_actor_id) is not str
        or not approval_actor_id
        or approval_actor_id != approval_actor_id.strip()
        or any(character.isspace() or ord(character) <= 0x1F for character in approval_actor_id)
    ):
        raise ValueError("acquisition start create approval_actor_id is invalid")
    if approval_actor_kind not in _ALLOWED_APPROVAL_ACTOR_KINDS:
        raise ValueError("acquisition start create approval_actor_kind is invalid")
    if (
        type(approval_policy_revision) is not str
        or not approval_policy_revision
        or approval_policy_revision != approval_policy_revision.strip()
    ):
        raise ValueError("acquisition start create approval_policy_revision is invalid")
    if isinstance(lock_timeout_seconds, bool):
        raise ValueError("acquisition start create lock timeout must be finite and positive")
    timeout_seconds = float(lock_timeout_seconds)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("acquisition start create lock timeout must be finite and positive")
    if fault_injection_point not in _FAULT_POINTS:
        raise ValueError("unsupported acquisition start create fault injection point")
    return timeout_seconds


def _advisory_lock_groups(
    *,
    binding: AcquisitionStartV2SubmissionBinding,
    operation_run_id: str,
    workflow_run_id: str,
    workflow_command_id: str,
    command_key: str,
) -> tuple[tuple[str, ...], ...]:
    occurrence = binding.occurrence

    def _sorted(*values: str) -> tuple[str, ...]:
        return tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))

    return (
        _sorted(
            f"operation_events:{binding.action_id}",
            f"operation_events:{operation_run_id}",
            f"workflow_events:{workflow_run_id}",
        ),
        _sorted(
            f"operation_runs:id:{operation_run_id}",
            f"operation_runs:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        ),
        _sorted(
            f"agent_actions:id:{binding.action_id}",
            f"agent_actions:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        ),
        _sorted(
            f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
            f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
        ),
        _sorted(
            f"acquisition_plan_previews:id:{binding.preview_id}",
            f"acquisition_plan_previews:revision:{binding.preview_revision}",
        ),
        _sorted(
            f"workflow_commands:id:{workflow_command_id}",
            f"workflow_commands:idempotency:{workflow_run_id}:{command_key}",
        ),
        _sorted(f"workflow_current_state:{workflow_run_id}"),
    )


def _event_candidates(
    cursor: Any,
    *,
    table_name: str,
    stream_column: str,
    identities: list[tuple[str, int, str, str]],
) -> list[dict[str, Any]]:
    from .control_plane_live_postgres import _fetch_all_dict_rows

    clauses: list[str] = []
    params: list[Any] = []
    for event_id, sequence_number, stream_id, idempotency_key in identities:
        clauses.append(
            f"(event_id = %s OR ({stream_column} = %s AND sequence_number = %s) "
            f"OR ({stream_column} = %s AND idempotency_key = %s))"
        )
        params.extend((event_id, stream_id, sequence_number, stream_id, idempotency_key))
    cursor.execute(
        f"SELECT * FROM {table_name} WHERE {' OR '.join(clauses)} "
        f"ORDER BY {stream_column}, sequence_number, event_id FOR UPDATE",
        tuple(params),
    )
    rows = _fetch_all_dict_rows(cursor)
    unique = {str(row.get("event_id") or ""): row for row in rows}
    if len(unique) != len(rows):
        raise ValueError(f"acquisition start create {table_name} duplicate event collision")
    return rows


def _assert_event_set_exact(
    label: str,
    actual_rows: list[dict[str, Any]],
    expected_rows: list[dict[str, Any]],
    *,
    workflow: bool,
) -> None:
    actual = {str(row.get("event_id") or ""): row for row in actual_rows}
    expected = {str(row.get("event_id") or ""): row for row in expected_rows}
    if set(actual) != set(expected):
        raise ValueError(f"acquisition start create {label} aggregate collision")
    json_fields = frozenset({"payload_json", "artifact_refs_json"} if workflow else {"payload_json"})
    for event_id, expected_row in expected.items():
        _assert_row_exact(label, actual[event_id], expected_row, json_fields=json_fields)


def create_acquisition_start_v2_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    approval_actor_id: str,
    approval_actor_kind: str,
    approval_policy_revision: str,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any]:
    """Approve and command one pending v2 start, or exact-replay its bundle."""

    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    timeout_seconds = _validate_invocation(
        approval_actor_id=approval_actor_id,
        approval_actor_kind=approval_actor_kind,
        approval_policy_revision=approval_policy_revision,
        lock_timeout_seconds=lock_timeout_seconds,
        fault_injection_point=fault_injection_point,
    )
    _require_authoritative_dependencies(adapter)

    from .agent_tool_result_postgres import _with_retry_dependencies
    from .control_plane_live_postgres import _fetch_all_dict_rows, _fetch_one_dict_row

    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type=_OPERATION_TYPE,
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)
    deadline = time.monotonic() + timeout_seconds
    retry_attempt = 0
    last_busy_key = f"operation_events:{binding.action_id}"
    busy_error, is_retryable, retry_delay, max_retries, poll_seconds = _with_retry_dependencies()

    def _remaining() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        return remaining

    def _refresh_deadline(cursor: Any) -> None:
        milliseconds = max(1, int(_remaining() * 1000))
        cursor.execute(
            "SELECT set_config('lock_timeout', %s, true), set_config('statement_timeout', %s, true)",
            (f"{milliseconds}ms", f"{milliseconds}ms"),
        )

    while True:
        connection = adapter._connect_with_timeout(_remaining())
        try:
            outcome: dict[str, Any] | None = None
            with connection.cursor() as cursor:
                _refresh_deadline(cursor)
                event_stream_group = tuple(
                    sorted(
                        {
                            f"operation_events:{binding.action_id}",
                            f"operation_events:{operation_run_id}",
                            f"workflow_events:{workflow_run_id}",
                        },
                        key=lambda value: value.encode("utf-8"),
                    )
                )
                for lock_key in event_stream_group:
                    last_busy_key = lock_key
                    if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                        raise _StartCreateLockBusy

                # The command identity contains the receipt digest, which in turn
                # contains approved_at.  Under all event-stream locks, discover a
                # possible persisted receipt without a row lock solely to derive
                # the remaining advisory keys.  The mandatory FOR UPDATE probe
                # below re-reads and validates the complete event after all locks.
                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM operation_events WHERE event_id = %s LIMIT 1",
                    (_receipt_event_id(binding),),
                )
                discovered_receipt_event = _fetch_one_dict_row(cursor, cursor.fetchone())

                if discovered_receipt_event is None:
                    _refresh_deadline(cursor)
                    cursor.execute(
                        "SELECT to_char(date_trunc('second', transaction_timestamp()) AT TIME ZONE 'UTC', "
                        '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS transaction_timestamp_iso'
                    )
                    timestamp_row = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
                    approved_at = str(timestamp_row.get("transaction_timestamp_iso") or "")
                else:
                    discovered_payload = _json_value(discovered_receipt_event.get("payload_json"))
                    if not isinstance(discovered_payload, dict):
                        raise ValueError("acquisition start create persisted receipt immutable identity collision")
                    try:
                        discovered_receipt = AcquisitionConfirmationReceipt(discovered_payload)
                    except ValueError as exc:
                        raise ValueError(
                            "acquisition start create persisted receipt immutable identity collision"
                        ) from exc
                    if discovered_receipt.receipt_id != _receipt_event_id(binding):
                        raise ValueError("acquisition start create persisted receipt identity collision")
                    approved_at = str(discovered_receipt.to_record()["approved_at"])

                # A preliminary typed receipt provides the receipt-derived command
                # lock identities.  It is rebuilt after all official row probes.
                preview_seed = binding.bound_request.snapshot.preview.to_record()
                preview_seed_row = {
                    **preview_seed,
                    "preview_json": preview_seed,
                    "created_at": preview_seed["created_at"],
                    "expires_at": preview_seed["expires_at"],
                }
                seed_action = {
                    "target_ref_json": binding.bound_request.target_ref,
                    "input_json": binding.bound_request.input_payload,
                }
                preliminary_receipt = _build_receipt(
                    binding=binding,
                    action=seed_action,
                    preview=preview_seed_row,
                    approval_actor_id=approval_actor_id,
                    approval_actor_kind=approval_actor_kind,
                    approval_policy_revision=approval_policy_revision,
                    approved_at=approved_at,
                )
                preliminary_rows = _canonical_create_rows(
                    binding=binding,
                    receipt=preliminary_receipt,
                    submitted_action={},
                )
                lock_groups = _advisory_lock_groups(
                    binding=binding,
                    operation_run_id=operation_run_id,
                    workflow_run_id=workflow_run_id,
                    workflow_command_id=preliminary_rows["workflow_command_id"],
                    command_key=preliminary_rows["command_key"],
                )
                for lock_group in lock_groups[1:]:
                    for lock_key in lock_group:
                        last_busy_key = lock_key
                        if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                            raise _StartCreateLockBusy

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM operation_runs WHERE operation_run_id = %s "
                    "OR (workspace_id = %s AND idempotency_key = %s) ORDER BY operation_run_id FOR UPDATE",
                    (operation_run_id, binding.occurrence.workspace_id, binding.start_idempotency),
                )
                operation_candidates = _fetch_all_dict_rows(cursor)
                if len(operation_candidates) > 1:
                    raise ValueError("acquisition start create OperationRun split identity collision")
                existing_operation = operation_candidates[0] if operation_candidates else None

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM agent_actions WHERE action_id = %s "
                    "OR (workspace_id = %s AND idempotency_key = %s) ORDER BY action_id FOR UPDATE",
                    (binding.action_id, binding.occurrence.workspace_id, binding.start_idempotency),
                )
                action_candidates = _fetch_all_dict_rows(cursor)
                if len(action_candidates) != 1:
                    raise ValueError("acquisition start create Action identity collision")
                existing_action = action_candidates[0]

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM agent_tool_result_slots WHERE result_slot_id = %s "
                    "OR logical_occurrence_digest = %s ORDER BY result_slot_id FOR UPDATE",
                    (binding.occurrence.result_slot_id, binding.occurrence.logical_occurrence_digest),
                )
                slot_candidates = _fetch_all_dict_rows(cursor)
                if len(slot_candidates) != 1:
                    raise ValueError("acquisition start create result slot identity collision")

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM acquisition_plan_previews WHERE preview_id = %s AND workspace_id = %s "
                    "AND requester_id = %s AND preview_revision = %s AND preview_digest = %s "
                    "ORDER BY preview_id FOR UPDATE",
                    (
                        binding.preview_id,
                        binding.occurrence.workspace_id,
                        binding.occurrence.actor_id,
                        binding.preview_revision,
                        binding.preview_digest,
                    ),
                )
                preview_candidates = _fetch_all_dict_rows(cursor)
                if len(preview_candidates) != 1:
                    raise ValueError("acquisition start create preview identity collision")
                preview_row = preview_candidates[0]

                receipt = _build_receipt(
                    binding=binding,
                    action=existing_action,
                    preview=preview_row,
                    approval_actor_id=approval_actor_id,
                    approval_actor_kind=approval_actor_kind,
                    approval_policy_revision=approval_policy_revision,
                    approved_at=approved_at,
                )
                submitted_at = _utc_second_iso(existing_action.get("created_at"))
                pending_action, approval_required_event = _canonical_submit_rows(binding, submitted_at=submitted_at)
                expected = _canonical_create_rows(
                    binding=binding,
                    receipt=receipt,
                    submitted_action=pending_action,
                )
                if (
                    expected["workflow_command_id"] != preliminary_rows["workflow_command_id"]
                    or expected["command_key"] != preliminary_rows["command_key"]
                ):
                    raise ValueError("acquisition start create receipt-derived command identity collision")

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM workflow_commands WHERE command_id = %s "
                    "OR (workflow_run_id = %s AND idempotency_key = %s) ORDER BY command_id FOR UPDATE",
                    (expected["workflow_command_id"], workflow_run_id, expected["command_key"]),
                )
                command_candidates = _fetch_all_dict_rows(cursor)
                if len(command_candidates) > 1:
                    raise ValueError("acquisition start create WorkflowCommand split identity collision")
                existing_command = command_candidates[0] if command_candidates else None

                _refresh_deadline(cursor)
                cursor.execute(
                    "SELECT * FROM workflow_current_state WHERE workflow_run_id = %s FOR UPDATE",
                    (workflow_run_id,),
                )
                state_candidates = _fetch_all_dict_rows(cursor)
                if len(state_candidates) > 1:
                    raise ValueError("acquisition start create workflow state identity collision")
                existing_state = state_candidates[0] if state_candidates else None

                operation_event_rows = _event_candidates(
                    cursor,
                    table_name="operation_events",
                    stream_column="event_stream_id",
                    identities=[
                        (
                            approval_required_event["event_id"],
                            1,
                            binding.action_id,
                            approval_required_event["idempotency_key"],
                        ),
                        (
                            expected["receipt_event"]["event_id"],
                            2,
                            binding.action_id,
                            expected["receipt_event"]["idempotency_key"],
                        ),
                        (
                            expected["planned_event"]["event_id"],
                            1,
                            operation_run_id,
                            expected["planned_event"]["idempotency_key"],
                        ),
                    ],
                )
                workflow_event_rows = _event_candidates(
                    cursor,
                    table_name="workflow_events",
                    stream_column="workflow_run_id",
                    identities=[
                        (
                            expected["workflow_events"][0]["event_id"],
                            1,
                            workflow_run_id,
                            expected["workflow_events"][0]["idempotency_key"],
                        ),
                        (
                            expected["workflow_events"][1]["event_id"],
                            2,
                            workflow_run_id,
                            expected["workflow_events"][1]["idempotency_key"],
                        ),
                    ],
                )

                created_markers = (
                    existing_operation,
                    existing_command,
                    existing_state,
                    next(
                        (
                            row
                            for row in operation_event_rows
                            if str(row.get("event_id") or "") == expected["receipt_event"]["event_id"]
                        ),
                        None,
                    ),
                    next(
                        (
                            row
                            for row in operation_event_rows
                            if str(row.get("event_id") or "") == expected["planned_event"]["event_id"]
                        ),
                        None,
                    ),
                )
                has_created = [marker is not None for marker in created_markers]
                workflow_created_count = sum(
                    str(row.get("event_id") or "") in {event["event_id"] for event in expected["workflow_events"]}
                    for row in workflow_event_rows
                )
                if any(has_created) or workflow_created_count:
                    if not all(has_created) or workflow_created_count != 2:
                        raise ValueError("acquisition start create partial aggregate collision")
                    assert existing_operation is not None
                    assert existing_command is not None
                    assert existing_state is not None
                    _assert_replayable_result_slot(slot_candidates[0], binding.occurrence)
                    _assert_row_exact(
                        "Action",
                        existing_action,
                        expected["action"],
                        json_fields=_ACTION_JSON_FIELDS,
                    )
                    _assert_row_exact(
                        "OperationRun",
                        existing_operation,
                        expected["operation_run"],
                        json_fields=_OPERATION_JSON_FIELDS,
                    )
                    _assert_row_exact(
                        "WorkflowCommand",
                        existing_command,
                        expected["workflow_command"],
                        json_fields=_COMMAND_JSON_FIELDS,
                    )
                    _assert_row_exact(
                        "workflow current state",
                        existing_state,
                        expected["workflow_current_state"],
                        json_fields=_CURRENT_STATE_JSON_FIELDS,
                    )
                    _assert_event_set_exact(
                        "operation events",
                        operation_event_rows,
                        [approval_required_event, expected["receipt_event"], expected["planned_event"]],
                        workflow=False,
                    )
                    _assert_event_set_exact(
                        "workflow events",
                        workflow_event_rows,
                        expected["workflow_events"],
                        workflow=True,
                    )
                    outcome = {
                        "outcome": "replayed",
                        "replayed": True,
                        **{
                            key: value
                            for key, value in expected.items()
                            if key
                            in {
                                "confirmation_receipt",
                                "owner_result_ref",
                                "owner_result_digest",
                            }
                        },
                        "action": existing_action,
                        "operation_run": existing_operation,
                        "receipt_event": next(
                            row
                            for row in operation_event_rows
                            if str(row.get("event_id") or "") == expected["receipt_event"]["event_id"]
                        ),
                        "workflow_events": workflow_event_rows,
                        "workflow_command": existing_command,
                        "workflow_current_state": existing_state,
                        "planned_event": next(
                            row
                            for row in operation_event_rows
                            if str(row.get("event_id") or "") == expected["planned_event"]["event_id"]
                        ),
                    }
                else:
                    _assert_exact_action(existing_action, pending_action)
                    _assert_pending_result_slot(slot_candidates[0], binding.occurrence)
                    _assert_event_set_exact(
                        "operation events",
                        operation_event_rows,
                        [approval_required_event],
                        workflow=False,
                    )
                    if workflow_event_rows:
                        raise ValueError("acquisition start create workflow event collision")

                    receipt_event = _insert_row(
                        cursor,
                        table_name="operation_events",
                        row=expected["receipt_event"],
                    )
                    _assert_row_exact(
                        "receipt event",
                        receipt_event,
                        expected["receipt_event"],
                        json_fields=frozenset({"payload_json"}),
                    )
                    if fault_injection_point == "after_receipt_event_write":
                        raise RuntimeError("injected acquisition start create fault after receipt event write")
                    _refresh_deadline(cursor)
                    cursor.execute(
                        "UPDATE agent_actions SET approval_status = %s, status = %s, result_ref_json = %s, "
                        "updated_at = %s WHERE action_id = %s AND approval_status = 'required' "
                        "AND status = 'approval_required' RETURNING *",
                        (
                            "approved",
                            "queued",
                            expected["action"]["result_ref_json"],
                            approved_at,
                            binding.action_id,
                        ),
                    )
                    action = _fetch_one_dict_row(cursor, cursor.fetchone())
                    if action is None:
                        raise ValueError("acquisition start create Action CAS collision")
                    _assert_row_exact(
                        "Action",
                        action,
                        expected["action"],
                        json_fields=_ACTION_JSON_FIELDS,
                    )
                    if fault_injection_point == "after_action_update":
                        raise RuntimeError("injected acquisition start create fault after Action update")
                    operation = _insert_row(
                        cursor,
                        table_name="operation_runs",
                        row=expected["operation_run"],
                    )
                    _assert_row_exact(
                        "OperationRun",
                        operation,
                        expected["operation_run"],
                        json_fields=_OPERATION_JSON_FIELDS,
                    )
                    if fault_injection_point == "after_operation_run_write":
                        raise RuntimeError("injected acquisition start create fault after OperationRun write")
                    started_event = _insert_row(
                        cursor,
                        table_name="workflow_events",
                        row=expected["workflow_events"][0],
                    )
                    _assert_row_exact(
                        "WorkflowStarted event",
                        started_event,
                        expected["workflow_events"][0],
                        json_fields=frozenset({"payload_json", "artifact_refs_json"}),
                    )
                    if fault_injection_point == "after_workflow_started_event_write":
                        raise RuntimeError("injected acquisition start create fault after WorkflowStarted write")
                    source_event = _insert_row(
                        cursor,
                        table_name="workflow_events",
                        row=expected["workflow_events"][1],
                    )
                    _assert_row_exact(
                        "CommandPlanRequested event",
                        source_event,
                        expected["workflow_events"][1],
                        json_fields=frozenset({"payload_json", "artifact_refs_json"}),
                    )
                    if fault_injection_point == "after_command_plan_requested_event_write":
                        raise RuntimeError("injected acquisition start create fault after CommandPlanRequested write")
                    command = _insert_row(
                        cursor,
                        table_name="workflow_commands",
                        row=expected["workflow_command"],
                    )
                    _assert_row_exact(
                        "WorkflowCommand",
                        command,
                        expected["workflow_command"],
                        json_fields=_COMMAND_JSON_FIELDS,
                    )
                    if fault_injection_point == "after_command_write":
                        raise RuntimeError("injected acquisition start create fault after WorkflowCommand write")
                    current_state = _insert_row(
                        cursor,
                        table_name="workflow_current_state",
                        row=expected["workflow_current_state"],
                    )
                    _assert_row_exact(
                        "workflow current state",
                        current_state,
                        expected["workflow_current_state"],
                        json_fields=_CURRENT_STATE_JSON_FIELDS,
                    )
                    if fault_injection_point == "after_current_state_write":
                        raise RuntimeError("injected acquisition start create fault after current-state write")
                    planned_event = _insert_row(
                        cursor,
                        table_name="operation_events",
                        row=expected["planned_event"],
                    )
                    _assert_row_exact(
                        "planned event",
                        planned_event,
                        expected["planned_event"],
                        json_fields=frozenset({"payload_json"}),
                    )
                    if fault_injection_point == "after_planned_event_write":
                        raise RuntimeError("injected acquisition start create fault after planned-event write")
                    outcome = {
                        "outcome": "created",
                        "replayed": False,
                        "action": action,
                        "operation_run": operation,
                        "receipt_event": receipt_event,
                        "workflow_events": [started_event, source_event],
                        "workflow_command": command,
                        "workflow_current_state": current_state,
                        "planned_event": planned_event,
                        "confirmation_receipt": expected["confirmation_receipt"],
                        "owner_result_ref": expected["owner_result_ref"],
                        "owner_result_digest": expected["owner_result_digest"],
                    }

            _remaining()
            connection.commit()
            if fault_injection_point == "after_commit":
                raise RuntimeError("injected acquisition start create fault after commit")
            assert outcome is not None
            return outcome
        except _StartCreateLockBusy:
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            sqlstate = str(getattr(exc, "sqlstate", "") or "").strip().upper()
            remaining = deadline - time.monotonic()
            if sqlstate == "57014" or (sqlstate == "55P03" and remaining <= 0):
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds) from exc
            if sqlstate == "55P03" and remaining > 0:
                time.sleep(min(poll_seconds, remaining))
            elif is_retryable(exc) and retry_attempt < max_retries and remaining > 0:
                time.sleep(min(retry_delay(retry_attempt), remaining))
            else:
                raise
        finally:
            connection.close()


__all__ = ["create_acquisition_start_v2_uow"]
