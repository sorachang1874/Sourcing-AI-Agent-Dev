from __future__ import annotations

from dataclasses import replace

import pytest

from sourcing_agent.agent_canary_registry import (
    INSPECT_OPERATION_TOOL_SPEC,
    PLAN_ACQUISITION_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC,
)
from sourcing_agent.agent_tool_result_slot import (
    AgentToolOccurrence,
    AgentToolResultSlotError,
    AgentToolTerminalResult,
)


def _occurrence(*, tool_spec=PLAN_ACQUISITION_TOOL_SPEC, ordinal: int = 1) -> AgentToolOccurrence:
    return AgentToolOccurrence.from_tool_spec(
        result_slot_id=f"slot_{tool_spec.tool_name}_{ordinal}",
        slot_generation=1,
        workspace_id="workspace_1",
        actor_id="actor_1",
        runtime_namespace="isolated_local_canary",
        provider_mode="simulate",
        turn_id="turn_1",
        step_id="step_1",
        tool_spec=tool_spec,
        canonical_args={"company": "Thinking Machines Lab"},
        occurrence_ordinal=ordinal,
    )


def _terminal(**overrides: object) -> AgentToolTerminalResult:
    values: dict[str, object] = {
        "result_attempt_id": "attempt_1",
        "provider_call_id": "provider-call-opaque-1",
        "tool_call_id": "tool-call-opaque-1",
        "action_id": "action_1",
        "operation_run_id": "operation_1",
        "owner_target_kind": "acquisition_plan_preview_v1",
        "owner_target_id": "preview_1",
        "owner_target_revision": 7,
        "terminal_winner_id": "winner_1",
        "owner_result_ref": {"preview_id": "preview_1", "preview_revision": 7},
        "owner_result_digest": "a" * 64,
        "serialized_result": {"status": "success", "preview_id": "preview_1"},
        "is_error": False,
    }
    values.update(overrides)
    return AgentToolTerminalResult.from_serialized_result(**values)  # type: ignore[arg-type]


def test_occurrence_exact_copies_historical_tool_request_result_and_serializer_pins() -> None:
    occurrence = _occurrence()

    assert occurrence.tool_spec_version == PLAN_ACQUISITION_TOOL_SPEC.tool_spec_version
    assert occurrence.tool_spec_digest == PLAN_ACQUISITION_TOOL_SPEC.tool_spec_digest
    assert occurrence.request_schema_version == PLAN_ACQUISITION_TOOL_SPEC.request.schema_version
    assert occurrence.request_schema_digest == PLAN_ACQUISITION_TOOL_SPEC.request.schema_digest
    assert occurrence.result_schema_version == PLAN_ACQUISITION_TOOL_SPEC.result.schema_version
    assert occurrence.result_schema_digest == PLAN_ACQUISITION_TOOL_SPEC.result.schema_digest
    assert occurrence.serializer_owner == PLAN_ACQUISITION_TOOL_SPEC.result.serializer_owner.owner_id
    assert occurrence.serializer_revision == PLAN_ACQUISITION_TOOL_SPEC.result.serializer_owner.owner_revision
    assert occurrence.serializer_contract_digest == (
        PLAN_ACQUISITION_TOOL_SPEC.result.serializer_owner.owner_contract_digest
    )
    assert occurrence.effect_class == "commandless_action"


def test_logical_occurrence_uses_stable_ordinal_and_not_provider_call_id() -> None:
    first = _occurrence(ordinal=1)
    repeated = _occurrence(ordinal=1)
    duplicate = _occurrence(ordinal=2)

    assert first.logical_occurrence_digest == repeated.logical_occurrence_digest
    assert first.logical_occurrence_digest != duplicate.logical_occurrence_digest
    assert "provider_call_id" not in first.logical_identity_record()
    assert "result_slot_id" not in first.logical_identity_record()


def test_occurrence_rejects_noncanonical_arguments_and_digest_drift() -> None:
    occurrence = _occurrence()

    with pytest.raises(AgentToolResultSlotError, match="canonical_args_invalid"):
        replace(occurrence, canonical_args_json='{"z":1, "a":2}')
    with pytest.raises(AgentToolResultSlotError, match="canonical_args_digest_mismatch"):
        replace(occurrence, canonical_args_digest="f" * 64)


def test_terminal_result_builds_exact_tool_result_message_and_content_digest() -> None:
    terminal = _terminal()

    assert terminal.serialized_result_json == '{"preview_id":"preview_1","status":"success"}'
    assert terminal.serialized_result_digest != terminal.owner_result_digest
    assert terminal.tool_result_message_record() == {
        "message_type": "tool_result",
        "tool_call_id": "tool-call-opaque-1",
        "content": terminal.serialized_result_json,
        "is_error": False,
    }
    assert len(terminal.tool_result_message_digest) == 64


def test_commandless_action_requires_action_run_and_forbids_command_chain() -> None:
    terminal = _terminal()
    terminal.validate_for_occurrence(_occurrence())

    without_owner = _terminal(action_id="", operation_run_id="")
    with pytest.raises(AgentToolResultSlotError, match="commandless_link_invalid"):
        without_owner.validate_for_occurrence(_occurrence())
    with_command = _terminal(
        workflow_command_id="command_1",
        activity_run_id="activity_1",
        activity_attempt_id="activity_attempt_1",
        command_attempt=1,
        command_generation=1,
        control_epoch=1,
    )
    with pytest.raises(AgentToolResultSlotError, match="commandless_link_invalid"):
        with_command.validate_for_occurrence(_occurrence())


def test_command_backed_action_requires_complete_positive_command_chain() -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
    terminal = _terminal(
        workflow_command_id="command_1",
        activity_run_id="activity_1",
        activity_attempt_id="activity_attempt_1",
        command_attempt=1,
        command_generation=2,
        control_epoch=3,
    )
    terminal.validate_for_occurrence(occurrence)

    with pytest.raises(AgentToolResultSlotError, match="command_backed_link_required"):
        _terminal().validate_for_occurrence(occurrence)
    with pytest.raises(AgentToolResultSlotError, match="command_link_group_incomplete"):
        _terminal(workflow_command_id="command_1")


def test_read_only_query_allows_no_action_but_never_a_command_chain() -> None:
    occurrence = _occurrence(tool_spec=INSPECT_OPERATION_TOOL_SPEC)
    terminal = _terminal(action_id="", operation_run_id="")
    terminal.validate_for_occurrence(occurrence)

    with pytest.raises(AgentToolResultSlotError, match="read_only_command_link_forbidden"):
        _terminal(
            workflow_command_id="command_1",
            activity_run_id="activity_1",
            activity_attempt_id="activity_attempt_1",
            command_attempt=1,
            command_generation=1,
            control_epoch=1,
        ).validate_for_occurrence(occurrence)


def test_terminal_result_rejects_partial_owner_link_zero_target_version_and_payload_drift() -> None:
    with pytest.raises(AgentToolResultSlotError, match="action_link_group_incomplete"):
        _terminal(operation_run_id="")
    with pytest.raises(AgentToolResultSlotError, match="owner_target_version_missing"):
        _terminal(owner_target_revision=0, owner_target_generation=0)
    terminal = _terminal()
    with pytest.raises(AgentToolResultSlotError, match="serialized_result_digest_mismatch"):
        replace(terminal, serialized_result_digest="f" * 64)
