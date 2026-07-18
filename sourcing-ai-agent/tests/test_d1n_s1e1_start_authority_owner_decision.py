from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
    ACQUISITION_START_V2_REQUEST_TOOL_SPEC,
    ACQUISITION_START_V2_RESULT_SPEC,
    AcquisitionConfirmationReceipt,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
    acquisition_start_v2_persisted_action_record,
    acquisition_start_v2_success_result,
    build_acquisition_start_v2_root_command_payload,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from sourcing_agent.durable_runtime import (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    attach_command_causality,
    command_causality_for,
    command_id_for,
    default_stage_id_for_command_type,
    reduce_workflow_events,
    summarize_workflow_command_counts,
)
from sourcing_agent.operation_runtime import operation_action_id, operation_run_id_for
from sourcing_agent.repositories.workflow_runtime import (
    AGENT_ACTIONS,
    AGENT_TOOL_RESULT_JOURNAL,
    AGENT_TOOL_RESULT_SLOTS,
    OPERATION_EVENTS,
    OPERATION_RUNS,
    RUNTIME_OUTBOX,
    WORKFLOW_COMMANDS,
    WORKFLOW_CURRENT_STATE,
    WORKFLOW_EVENTS,
)
from tests.test_d1n_start_acquisition_v2 import _CONTEXT, _NOW, _preview, _PreviewRepository, _reference

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
DECISION_DOC = REPO_ROOT / "docs" / "TRACK_D_D1N_S1E1_START_AUTHORITY_OWNER_DECISION.md"
PLAN = REPO_ROOT / "docs" / "TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md"
TODO = REPO_ROOT / "docs" / "NEXT_TODO.md"
LEDGER = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
INDEX = REPO_ROOT / "docs" / "INDEX.md"

BUDGET_FIELDS = (
    "max_provider_calls",
    "max_provider_items",
    "max_output_candidates",
    "max_cost_micro_usd",
    "max_elapsed_seconds",
)
OWNER_RESULT_FIELDS = (
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
SOURCE_EVENT_CONTRACT_FIELDS = (
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
    "payload",
    "artifact_refs",
    "schema_version",
)
APPROVAL_REQUIRED_PAYLOAD_FIELDS = (
    "schema_version",
    "action_id",
    "workspace_id",
    "requester_id",
    "action_type",
    "request",
    "request_schema_ref",
    "tool_spec_ref",
    "result_contract_ref",
    "result_occurrence_ref",
    "start_snapshot_digest",
)
LOCK_GROUPS = (
    "all action/operation/workflow event-stream advisory keys, bytewise sorted",
    "`operation_runs` identity",
    "`agent_actions` identity",
    "`agent_tool_result_slots` identity",
    "`acquisition_plan_previews` identity",
    "`workflow_commands` command-id and workflow/idempotency identities",
    "`workflow_current_state` workflow-run identity",
    "exact receipt/source/winner event rows in stream+sequence order",
    "result attempt and journal identities",
)
LOCK_KEY_TEMPLATE_GROUPS = (
    (
        "operation_events:{action_id}",
        "operation_events:{operation_run_id}",
        "workflow_events:{workflow_run_id}",
    ),
    (
        "operation_runs:id:{operation_run_id}",
        "operation_runs:idempotency:{workspace_id}:{start_idempotency}",
    ),
    (
        "agent_actions:id:{action_id}",
        "agent_actions:idempotency:{workspace_id}:{start_idempotency}",
    ),
    (
        "agent_tool_result_slots:id:{result_slot_id}",
        "agent_tool_result_slots:occurrence:{logical_occurrence_digest}",
    ),
    (
        "acquisition_plan_previews:id:{preview_id}",
        "acquisition_plan_previews:revision:{preview_revision}",
    ),
    (
        "workflow_commands:id:{workflow_command_id}",
        "workflow_commands:idempotency:{workflow_run_id}:{root_command_key}",
    ),
    ("workflow_current_state:{workflow_run_id}",),
)
ROW_PROBES = (
    ("1", "`operation_runs`", "`operation_run_id OR (workspace_id, start_idempotency)`"),
    ("2", "`agent_actions`", "`action_id OR (workspace_id, start_idempotency)`"),
    (
        "3",
        "`agent_tool_result_slots`",
        "`result_slot_id OR logical_occurrence_digest`; the server-revalidated digest binds the complete occurrence tuple",
    ),
    (
        "4",
        "`acquisition_plan_previews`",
        "exact `preview_id + workspace_id + requester_id + preview_revision + preview_digest`",
    ),
    ("5", "`workflow_commands`", "`workflow_command_id OR (workflow_run_id, root_command_key)`"),
    ("6", "`workflow_current_state`", "exact `workflow_run_id`"),
    (
        "7",
        "operation events",
        "`event_id OR (event_stream_id, sequence_number) OR (event_stream_id, idempotency_key)`, ordered by stream/sequence/event id",
    ),
    (
        "8",
        "workflow events",
        "`event_id OR (workflow_run_id, sequence_number) OR (workflow_run_id, idempotency_key)`, ordered by run/sequence/event id",
    ),
    (
        "9",
        "result attempt/journal",
        'attempt by exact `result_attempt_id`; journal by exact `result_slot_id`; deterministic `journal_id=tooljournal_{sha1(result_slot_id + ":" + result_attempt_id)[:24]}`; accepted-attempt uniqueness remains serialized by the locked result slot',
    ),
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _event_id(prefix: str, stream_id: str, sequence: int, idempotency_key: str) -> str:
    seed = f"{stream_id}:{sequence}:{idempotency_key}"
    return prefix + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _typed_budget_envelope_ref(receipt: AcquisitionConfirmationReceipt) -> dict[str, Any]:
    record = receipt.to_record()
    budget = record["budget"]
    assert set(budget) == set(BUDGET_FIELDS)
    owner = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
    assert owner is not None
    return {
        **owner.to_fingerprint_record(),
        "confirmation_receipt_id": receipt.receipt_id,
        "confirmation_receipt_digest": receipt.receipt_digest,
        "budget_digest": _digest(budget),
    }


def _section(document: str, start: str, end: str) -> str:
    return document[document.index(start) : document.index(end)]


def _table(section: str) -> tuple[tuple[str, ...], tuple[tuple[str, ...], ...]]:
    parsed: list[tuple[str, ...]] = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
        if all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            continue
        parsed.append(cells)
    assert parsed
    return parsed[0], tuple(parsed[1:])


def _build_executable_contract() -> dict[str, Any]:
    preview = _preview()
    repository = _PreviewRepository(preview)
    tool_pins = AcquisitionStartV2ToolPins(
        tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
        tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
    )
    bound = AcquisitionStartV2OwnerBinder(repository).bind(
        input_payload=_reference(preview),
        context=_CONTEXT,
        tool_pins=tool_pins,
        now=_NOW,
    )
    occurrence = AgentToolOccurrence.from_tool_spec(
        result_slot_id="slot_start_v2_1",
        slot_generation=1,
        workspace_id=_CONTEXT.workspace_id,
        actor_id=_CONTEXT.requester_id,
        runtime_namespace="isolated_local_canary",
        provider_mode="simulate",
        turn_id="turn_start_v2_1",
        step_id="step_start_v2_1",
        tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
        canonical_args=bound.to_record(),
        occurrence_ordinal=1,
    )
    start_key = f"agent-start-v2:{occurrence.logical_occurrence_digest}"
    action_id = operation_action_id(
        workspace_id=occurrence.workspace_id,
        action_type="start_acquisition_run",
        idempotency_key=start_key,
    )
    operation_run_id = operation_run_id_for(
        action_id=action_id,
        operation_type="acquisition_run",
        idempotency_key=start_key,
    )
    receipt_key = f"{start_key}:ActionApproved"
    receipt_id = _event_id("opevt_", action_id, 2, receipt_key)
    action = acquisition_start_v2_persisted_action_record(action_id=action_id, bound_request=bound)
    approval_required_key = f"{start_key}:ActionApprovalRequired"
    approval_required_contract = {
        "event_id": _event_id("opevt_", action_id, 1, approval_required_key),
        "workspace_id": occurrence.workspace_id,
        "event_stream_id": action_id,
        "operation_run_id": "",
        "action_id": action_id,
        "event_family": "operation_event",
        "event_type": "ActionApprovalRequired",
        "sequence_number": 1,
        "idempotency_key": approval_required_key,
        "actor": occurrence.actor_id,
        "source": "agent_start_v2_submit_uow",
        "payload": {
            "schema_version": "acquisition_start_approval_required.v1",
            "action_id": action_id,
            "workspace_id": occurrence.workspace_id,
            "requester_id": occurrence.actor_id,
            "action_type": "start_acquisition_run",
            "request": bound.to_record(),
            "request_schema_ref": {
                "schema_version": occurrence.request_schema_version,
                "schema_digest": occurrence.request_schema_digest,
            },
            "tool_spec_ref": {
                "tool_spec_version": occurrence.tool_spec_version,
                "tool_spec_digest": occurrence.tool_spec_digest,
            },
            "result_contract_ref": {
                "result_schema_version": occurrence.result_schema_version,
                "result_schema_digest": occurrence.result_schema_digest,
                "serializer_owner": occurrence.serializer_owner,
                "serializer_revision": occurrence.serializer_revision,
                "serializer_contract_digest": occurrence.serializer_contract_digest,
            },
            "result_occurrence_ref": {
                "result_slot_id": occurrence.result_slot_id,
                "slot_generation": occurrence.slot_generation,
                "logical_occurrence_digest": occurrence.logical_occurrence_digest,
            },
            "start_snapshot_digest": bound.snapshot.snapshot_digest,
        },
        "schema_version": "acquisition_start_approval_required.v1",
    }
    receipt = AcquisitionStartV2OwnerBinder(repository).confirm_exact_action(
        persisted_action=action,
        context=_CONTEXT,
        approval_actor_id="human_1",
        approval_actor_kind="authenticated_user",
        receipt_id=receipt_id,
        approval_policy_revision="acquisition_confirmation_policy_v1",
        approved_at="2026-07-17T00:31:00Z",
    )
    workflow_run_id = "wf_operation_" + hashlib.sha1(operation_run_id.encode("utf-8")).hexdigest()[:24]
    root = build_acquisition_start_v2_root_command_payload(
        snapshot=bound.snapshot,
        receipt=receipt,
        operation_run_id=operation_run_id,
        workflow_run_id=workflow_run_id,
    ).to_record()
    command_key = f"acquisition.run.create:start-v2:{receipt.receipt_digest}"
    command_id = command_id_for(workflow_run_id, command_key)
    workflow_type = "agent_callable_workflow_command"
    stage_key = default_stage_id_for_command_type(ACQUISITION_RUN_CREATE_COMMAND_TYPE)
    actor = "operation_workflow_command_planner"
    source = "operation_run_dispatch"
    started_key = f"{workflow_run_id}:operation_command_started:{operation_run_id}"
    started_contract = {
        "event_id": _event_id("evt_", workflow_run_id, 1, started_key),
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "command_id": "",
        "activity_attempt_id": "",
        "event_family": "workflow_event",
        "event_type": "WorkflowStarted",
        "sequence_number": 1,
        "idempotency_key": started_key,
        "actor": actor,
        "source": source,
        "payload": {
            "workflow_type": workflow_type,
            "stage_key": stage_key,
            "operation_run_id": operation_run_id,
            "action_id": action_id,
            "action_type": "start_acquisition_run",
            "migration_phase": "W11_agent_callable_workflow_command",
        },
        "artifact_refs": [],
        "schema_version": "workflow_event_v1",
    }
    source_key = f"{command_key}:plan"
    source_event_id = _event_id("evt_", workflow_run_id, 2, source_key)
    root_command_payload = {**root, "operation_id": operation_run_id}
    causality = command_causality_for(
        workflow_run_id=workflow_run_id,
        operation_id=operation_run_id,
        stage_id=stage_key,
        command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        owner=DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(ACQUISITION_RUN_CREATE_COMMAND_TYPE),
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
    source_contract = {
        "event_id": source_event_id,
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "command_id": "",
        "activity_attempt_id": "",
        "event_family": "workflow_event",
        "event_type": "CommandPlanRequested",
        "sequence_number": 2,
        "idempotency_key": source_key,
        "actor": actor,
        "source": source,
        "payload": {
            "workflow_type": workflow_type,
            "stage_key": stage_key,
            "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE,
            "idempotency_key": command_key,
            "payload": command_payload,
            "artifact_refs": [],
            "max_attempts": 5,
            "retry_policy": {"kind": "operation_acquisition_run_create", "retry_delay_seconds": 30},
        },
        "artifact_refs": [],
        "schema_version": "workflow_event_v1",
    }
    reducer = reduce_workflow_events(
        current_state={},
        new_events=[started_contract, source_contract],
        existing_commands=[],
    )
    assert len(reducer.commands) == 1
    planned_command = reducer.commands[0]
    active_counts, terminal_counts = summarize_workflow_command_counts(
        [
            {
                "owner": planned_command.owner,
                "command_type": planned_command.command_type,
                "status": "queued",
            }
        ]
    )
    current_state = {
        "schema_version": "workflow_current_state_v1",
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_run_id,
        "workflow_type": workflow_type,
        "status": reducer.status,
        "current_stage_key": reducer.current_stage_key,
        "completion_proofs": reducer.completion_proofs,
        "active_command_counts": active_counts,
        "terminal_command_counts": terminal_counts,
        "read_model_pointers": {},
        "migration_status": {},
        "last_processed_sequence_number": 2,
        "reducer_version": reducer.reducer_version,
        "metadata": reducer.metadata,
    }
    lock_values = {
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "workspace_id": occurrence.workspace_id,
        "start_idempotency": start_key,
        "result_slot_id": occurrence.result_slot_id,
        "logical_occurrence_digest": occurrence.logical_occurrence_digest,
        "preview_id": bound.snapshot.preview.preview_id,
        "preview_revision": bound.snapshot.preview.preview_revision,
        "workflow_command_id": command_id,
        "root_command_key": command_key,
    }
    lock_key_groups = tuple(
        tuple(sorted(template.format(**lock_values) for template in group)) for group in LOCK_KEY_TEMPLATE_GROUPS
    )
    planned_key = f"{start_key}:OperationCommandPlanned:{command_id}"
    planned_event_id = _event_id("opevt_", operation_run_id, 1, planned_key)
    budget = receipt.to_record()["budget"]
    owner_result_ref = {
        "schema_version": "acquisition_start_command_acceptance_owner_result_ref.v1",
        "runtime_namespace": occurrence.runtime_namespace,
        "provider_mode": occurrence.provider_mode,
        "workspace_id": occurrence.workspace_id,
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "workflow_command_id": command_id,
        "terminal_winner_id": planned_event_id,
        "terminal_winner_sequence_number": 1,
        "command_source_event_id": source_event_id,
        "command_source_event_sequence_number": 2,
        "command_source_event_contract_digest": _digest(source_contract),
        "confirmation_receipt_ref": {
            "receipt_id": receipt.receipt_id,
            "receipt_digest": receipt.receipt_digest,
        },
        "parent_budget_envelope_ref": _typed_budget_envelope_ref(receipt),
        "start_snapshot_digest": bound.snapshot.snapshot_digest,
        "root_command_payload_digest": root["payload_digest"],
        "result_occurrence_ref": {
            "result_slot_id": occurrence.result_slot_id,
            "slot_generation": occurrence.slot_generation,
            "logical_occurrence_digest": occurrence.logical_occurrence_digest,
        },
    }
    owner_result_digest = _digest(owner_result_ref)
    serialized_result = acquisition_start_v2_success_result(
        action_id=action_id,
        operation_run_id=operation_run_id,
        workflow_command_id=command_id,
        snapshot=bound.snapshot,
        receipt=receipt,
    )
    terminal = AgentToolTerminalResult.from_serialized_result(
        result_attempt_id="attempt_start_v2_1",
        provider_call_id="provider_call_start_v2_1",
        tool_call_id="tool_call_start_v2_1",
        action_id=action_id,
        operation_run_id=operation_run_id,
        workflow_command_id=command_id,
        owner_target_kind="acquisition_start_command_acceptance_v1",
        owner_target_id=command_id,
        owner_target_revision=1,
        terminal_winner_id=planned_event_id,
        owner_result_ref=owner_result_ref,
        owner_result_digest=owner_result_digest,
        serialized_result=serialized_result,
        is_error=False,
    )
    terminal.validate_for_occurrence(occurrence)
    return {
        "occurrence": occurrence,
        "bound": bound,
        "receipt": receipt,
        "root": root,
        "terminal": terminal,
        "owner_result_ref": owner_result_ref,
        "owner_result_digest": owner_result_digest,
        "budget": budget,
        "action": action,
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "command_id": command_id,
        "source_event_id": source_event_id,
        "planned_event_id": planned_event_id,
        "started_contract": started_contract,
        "source_contract": source_contract,
        "planned_command": planned_command,
        "current_state": current_state,
        "reducer": reducer,
        "lock_key_groups": lock_key_groups,
        "approval_required_contract": approval_required_contract,
    }


def test_zero_migration_decision_reuses_current_physical_descriptors() -> None:
    fields = lambda descriptor: {column.field or column.name for column in descriptor.columns}  # noqa: E731
    assert {
        "input",
        "target_ref",
        "request_schema_version",
        "tool_spec_version",
        "result_schema_version",
        "budget",
    }.issubset(fields(AGENT_ACTIONS))
    assert {"action_id", "workflow_ref", "cost_budget", "result_ref"}.issubset(fields(OPERATION_RUNS))
    assert {
        "event_stream_id",
        "operation_run_id",
        "action_id",
        "sequence_number",
        "payload",
        "schema_version",
    }.issubset(fields(OPERATION_EVENTS))
    assert {"workflow_run_id", "operation_id", "sequence_number", "payload", "schema_version"}.issubset(
        fields(WORKFLOW_EVENTS)
    )
    assert {"source_event_id", "source_event_type", "payload", "result"}.issubset(fields(WORKFLOW_COMMANDS))
    assert {"workflow_run_id", "status", "last_processed_sequence_number"}.issubset(fields(WORKFLOW_CURRENT_STATE))
    assert "terminal_winner_id" in fields(AGENT_TOOL_RESULT_SLOTS)
    assert "owner_result_ref" in fields(AGENT_TOOL_RESULT_JOURNAL)
    assert "outbox_id" in fields(RUNTIME_OUTBOX)

    migration_sql = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((SOURCE_ROOT / "migrations").glob("*.sql"))
    )
    assert "acquisition_start_command_acceptance" not in migration_sql
    assert "acquisition_parent_budget_envelope" not in migration_sql


def test_real_v2_helpers_form_one_receipt_budget_command_and_result_chain() -> None:
    contract = _build_executable_contract()
    occurrence = contract["occurrence"]
    receipt = contract["receipt"].to_record()
    terminal = contract["terminal"]

    assert occurrence.tool_spec_version == "start_acquisition_run_tool_v3"
    assert occurrence.result_link_policy == "workflow_command_acceptance_v1"
    assert occurrence.runtime_namespace == "isolated_local_canary"
    assert occurrence.provider_mode == "simulate"
    assert occurrence.canonical_args == contract["bound"].to_record()
    assert set(occurrence.canonical_args) == {"input_payload", "target_ref"}
    assert ACQUISITION_START_V2_REQUEST_TOOL_SPEC.validate_input(occurrence.canonical_args) == occurrence.canonical_args
    assert occurrence.canonical_args == contract["action"]["request"]
    assert occurrence.canonical_args["input_payload"] == contract["action"]["request"]["input_payload"]
    assert occurrence.canonical_args["target_ref"] == contract["action"]["request"]["target_ref"]
    assert receipt["schema_version"] == ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION
    assert receipt["receipt_id"].startswith("opevt_")
    assert set(receipt["budget"]) == set(BUDGET_FIELDS)
    assert receipt["budget"] == contract["budget"]
    assert receipt["start_snapshot_digest"] == contract["bound"].snapshot.snapshot_digest
    assert contract["root"]["confirmation_receipt_ref"] == {
        "receipt_id": receipt["receipt_id"],
        "receipt_digest": receipt["receipt_digest"],
    }
    assert contract["root"]["start_snapshot_digest"] == receipt["start_snapshot_digest"]
    approval_required = contract["approval_required_contract"]
    assert approval_required["event_id"] == _event_id(
        "opevt_", contract["action_id"], 1, approval_required["idempotency_key"]
    )
    assert approval_required["idempotency_key"] == (
        f"agent-start-v2:{occurrence.logical_occurrence_digest}:ActionApprovalRequired"
    )
    assert approval_required["actor"] == occurrence.actor_id
    assert approval_required["source"] == "agent_start_v2_submit_uow"
    assert approval_required["schema_version"] == "acquisition_start_approval_required.v1"
    assert tuple(approval_required["payload"]) == APPROVAL_REQUIRED_PAYLOAD_FIELDS
    assert approval_required["payload"]["request"] == occurrence.canonical_args
    assert approval_required["payload"]["start_snapshot_digest"] == contract["bound"].snapshot.snapshot_digest
    assert tuple(contract["source_contract"]) == SOURCE_EVENT_CONTRACT_FIELDS
    assert contract["started_contract"]["idempotency_key"] == (
        f"{contract['workflow_run_id']}:operation_command_started:{contract['operation_run_id']}"
    )
    assert contract["source_contract"]["idempotency_key"] == (
        f"acquisition.run.create:start-v2:{receipt['receipt_digest']}:plan"
    )
    assert contract["planned_command"].idempotency_key == (
        f"acquisition.run.create:start-v2:{receipt['receipt_digest']}"
    )
    assert contract["planned_command"].owner == "acquisition_run_writer"
    assert contract["planned_command"].payload == contract["source_contract"]["payload"]["payload"]
    assert contract["reducer"].outbox == ()
    assert contract["current_state"] == {
        "schema_version": "workflow_current_state_v1",
        "workflow_run_id": contract["workflow_run_id"],
        "operation_id": contract["operation_run_id"],
        "workflow_type": "agent_callable_workflow_command",
        "status": "running",
        "current_stage_key": "acquisition_run_create",
        "completion_proofs": {},
        "active_command_counts": {"acquisition_run_writer": {"acquisition.run.create": 1}},
        "terminal_command_counts": {},
        "read_model_pointers": {},
        "migration_status": {},
        "last_processed_sequence_number": 2,
        "reducer_version": "durable_runtime_reducer_v1",
        "metadata": {},
    }
    assert tuple(contract["owner_result_ref"]) == OWNER_RESULT_FIELDS
    assert terminal.terminal_winner_id == contract["planned_event_id"]
    assert terminal.workflow_command_id == contract["command_id"]
    assert terminal.owner_target_revision == 1
    assert terminal.owner_target_generation == 0
    assert terminal.owner_target_revision_token == ""
    assert terminal.owner_result_digest == _digest(terminal.owner_result_ref)
    assert terminal.serialized_result["confirmation_receipt_digest"] == receipt["receipt_digest"]
    assert ACQUISITION_START_V2_RESULT_SPEC.result_schema_version == occurrence.result_schema_version
    assert all(group == tuple(sorted(group)) for group in contract["lock_key_groups"])
    assert contract["lock_key_groups"][0] == tuple(
        sorted(
            (
                f"operation_events:{contract['action_id']}",
                f"operation_events:{contract['operation_run_id']}",
                f"workflow_events:{contract['workflow_run_id']}",
            )
        )
    )


def test_document_locks_exact_budget_owner_result_and_lock_order() -> None:
    document = DECISION_DOC.read_text(encoding="utf-8")
    budget_header, budget_rows = _table(_section(document, "### 4.1 Receipt-backed", "### 4.2 Exact"))
    assert budget_header == ("position", "field")
    assert tuple(row[1].strip("`") for row in budget_rows) == BUDGET_FIELDS

    result_header, result_rows = _table(_section(document, "### 4.2 Exact", "## 5. Result"))
    assert result_header == ("position", "field")
    assert tuple(row[1].strip("`") for row in result_rows) == OWNER_RESULT_FIELDS

    lock_header, lock_rows = _table(_section(document, "## 6. Lock order", "### 6.1 Exact"))
    assert lock_header == ("order", "lock group")
    assert tuple(row[1] for row in lock_rows) == LOCK_GROUPS
    assert "one monotonic five-second deadline" in document
    assert "runtime_outbox` delta is always zero" in document
    assert "one command and zero outbox rows" in document
    for template_group in LOCK_KEY_TEMPLATE_GROUPS:
        for template in template_group:
            assert f"`{template}`" in document
    probe_header, probe_rows = _table(_section(document, "After those locks", "Concurrent exact"))
    assert probe_header == ("order", "table", "exact alternate identity probe")
    assert probe_rows == ROW_PROBES
    assert "ActionApprovalRequired`" in document
    assert "date_trunc('second', transaction_timestamp())" in document
    assert "signal_recovery_for_committed_commands" in document


def test_decision_keeps_modes_serving_residuals_and_generic_paths_closed() -> None:
    document = DECISION_DOC.read_text(encoding="utf-8")
    for token in (
        "migration_delta=0",
        "allowed_provider_modes=simulate,scripted",
        "mode `live|replay`",
        "Generic `approve_action` and generic Agent",
        "R-019 and R-029 remain open",
        "Plan §6#6 action-root",
        "OB-2.2, OB-10.3, and OB-10.4 remain open",
        "10 schema-defined / 5 schema-less",
        "served=0",
        "provider/model/live invocation count at zero",
        "author tests do not constitute formal `GO`",
    ):
        assert token.casefold() in document.casefold()

    for path in (PLAN, TODO, LEDGER, INDEX):
        tracker = path.read_text(encoding="utf-8")
        assert "S1e1" in tracker, path.name
        assert "served=0" in tracker, path.name
        assert "R-019" in tracker and "R-029" in tracker, path.name
