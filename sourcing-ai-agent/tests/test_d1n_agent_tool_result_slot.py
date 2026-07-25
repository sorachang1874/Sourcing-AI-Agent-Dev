from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import cast

import pytest

from sourcing_agent.agent_canary_registry import (
    INSPECT_OPERATION_TOOL_SPEC,
    LOCAL_CANARY_AGENT_TOOL_REGISTRY,
    PLAN_ACQUISITION_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC_V2,
)
from sourcing_agent.agent_tool_result_slot import (
    AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION,
    AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2,
    AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION,
    AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2,
    AgentToolOccurrence,
    AgentToolResultSlotError,
    AgentToolTerminalResult,
)


class _EqualityAliasString(str):
    """Carry one byte value while claiming equality with a different string."""

    equality_alias: str

    def __new__(cls, value: str, *, equality_alias: str) -> _EqualityAliasString:
        instance = super().__new__(cls, value)
        instance.equality_alias = equality_alias
        return instance

    def __eq__(self, other: object) -> bool:
        return other == self.equality_alias

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    __hash__ = str.__hash__


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
    assert occurrence.result_link_policy == PLAN_ACQUISITION_TOOL_SPEC.behavior.result_link_policy
    assert occurrence.to_record()["result_link_policy"] == occurrence.result_link_policy
    assert occurrence.revalidated().result_link_policy == occurrence.result_link_policy


def test_all_registered_historical_occurrences_rebind_exact_spec_pins() -> None:
    for ordinal, tool_spec in enumerate(LOCAL_CANARY_AGENT_TOOL_REGISTRY.specs, start=1):
        occurrence = _occurrence(tool_spec=tool_spec, ordinal=ordinal)
        rebound = occurrence.revalidated_for_registry(LOCAL_CANARY_AGENT_TOOL_REGISTRY)

        assert rebound == occurrence
        assert rebound is not occurrence
        assert type(rebound.canonical_args_json) is str
        assert rebound.canonical_args_json.encode("utf-8") == occurrence.canonical_args_json.encode("utf-8")
        assert rebound.canonical_args_digest == occurrence.canonical_args_digest
        assert rebound.logical_occurrence_digest == occurrence.logical_occurrence_digest
        assert rebound.to_record() == occurrence.to_record()


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda occurrence: replace(
                occurrence,
                provider_mode=_EqualityAliasString("simulate", equality_alias="live"),
            ),
            "provider_mode_invalid",
        ),
        (
            lambda occurrence: replace(
                occurrence,
                tool_kind=_EqualityAliasString("action", equality_alias="query"),
            ),
            "tool_kind_invalid",
        ),
        (
            lambda occurrence: replace(
                occurrence,
                effect_class=_EqualityAliasString("command_backed_action", equality_alias="read_only"),
            ),
            "effect_class_invalid",
        ),
        (
            lambda occurrence: replace(
                occurrence,
                result_link_policy=_EqualityAliasString(
                    "activity_attempt_terminal_v1",
                    equality_alias="workflow_command_acceptance_v1",
                ),
            ),
            "link_policy_invalid",
        ),
    ),
)
def test_occurrence_closed_literals_require_exact_plain_strings_before_equality(
    mutate: Callable[[AgentToolOccurrence], AgentToolOccurrence],
    message: str,
) -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)

    with pytest.raises(AgentToolResultSlotError, match=message):
        mutate(occurrence)


def test_occurrence_canonical_arguments_require_exact_plain_json_carrier() -> None:
    occurrence = _occurrence()
    forged_json = _EqualityAliasString(
        '{"company":"Not Thinking Machines Lab"}',
        equality_alias=occurrence.canonical_args_json,
    )

    with pytest.raises(AgentToolResultSlotError, match="canonical_args_invalid"):
        replace(occurrence, canonical_args_json=forged_json)


@pytest.mark.parametrize(
    ("field_name", "forged_value", "message"),
    (
        (
            "result_link_policy",
            _EqualityAliasString(
                "activity_attempt_terminal_v1",
                equality_alias="workflow_command_acceptance_v1",
            ),
            "link_policy_invalid",
        ),
        (
            "canonical_args_json",
            _EqualityAliasString(
                '{"company":"Not Thinking Machines Lab"}',
                equality_alias='{"company":"Thinking Machines Lab"}',
            ),
            "canonical_args_invalid",
        ),
    ),
)
def test_registry_revalidation_rejects_postconstruction_equality_alias_carriers(
    field_name: str,
    forged_value: str,
    message: str,
) -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
    object.__setattr__(occurrence, field_name, forged_value)

    with pytest.raises(AgentToolResultSlotError, match=message):
        occurrence.revalidated_for_registry(LOCAL_CANARY_AGENT_TOOL_REGISTRY)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda occurrence: replace(occurrence, result_link_policy="activity_attempt_terminal_v1"),
            "pin_mismatch:result_link_policy",
        ),
        (lambda occurrence: replace(occurrence, request_schema_digest="e" * 64), "pin_mismatch:request_schema_digest"),
        (
            lambda occurrence: replace(occurrence, result_schema_version="forged_result_v1"),
            "pin_mismatch:result_schema_version",
        ),
        (
            lambda occurrence: replace(occurrence, serializer_revision="forged_serializer_v1"),
            "pin_mismatch:serializer_revision",
        ),
        (lambda occurrence: replace(occurrence, tool_spec_digest="f" * 64), "historical_spec_missing"),
    ),
)
def test_registry_revalidation_rejects_compatible_or_forged_historical_spec_pins(
    mutate: Callable[[AgentToolOccurrence], AgentToolOccurrence],
    message: str,
) -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
    forged = mutate(occurrence)

    with pytest.raises(AgentToolResultSlotError, match=message):
        forged.revalidated_for_registry(LOCAL_CANARY_AGENT_TOOL_REGISTRY)


def test_logical_occurrence_uses_stable_ordinal_and_not_provider_call_id() -> None:
    first = _occurrence(ordinal=1)
    repeated = _occurrence(ordinal=1)
    duplicate = _occurrence(ordinal=2)

    assert first.logical_occurrence_digest == repeated.logical_occurrence_digest
    assert first.logical_occurrence_digest != duplicate.logical_occurrence_digest
    assert "provider_call_id" not in first.logical_identity_record()
    assert "result_slot_id" not in first.logical_identity_record()
    assert "result_link_policy" not in first.logical_identity_record()


def test_occurrence_rejects_noncanonical_arguments_and_digest_drift() -> None:
    occurrence = _occurrence()

    with pytest.raises(AgentToolResultSlotError, match="canonical_args_invalid"):
        replace(occurrence, canonical_args_json='{"z":1, "a":2}')
    with pytest.raises(AgentToolResultSlotError, match="canonical_args_digest_mismatch"):
        replace(occurrence, canonical_args_digest="f" * 64)
    with pytest.raises(AgentToolResultSlotError, match="link_policy_effect_mismatch"):
        replace(occurrence, result_link_policy="workflow_command_acceptance_v1")
    with pytest.raises(AgentToolResultSlotError, match="link_policy_effect_mismatch"):
        replace(
            _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC),
            result_link_policy="no_command_v1",
        )
    with pytest.raises(AgentToolResultSlotError, match="link_policy_invalid"):
        replace(occurrence, result_link_policy="model_selected_policy_v1")


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
    assert terminal.attempt_schema_version == AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION
    assert terminal.journal_schema_version == AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION
    assert terminal.to_record()["schema_version"] == AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION
    assert not hasattr(terminal, "result_link_policy")


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda terminal: replace(
                terminal,
                owner_result_ref_json=_EqualityAliasString(
                    '{"preview_id":"forged","preview_revision":7}',
                    equality_alias='{"preview_id":"preview_1","preview_revision":7}',
                ),
            ),
            "owner_result_ref_invalid",
        ),
        (
            lambda terminal: replace(
                terminal,
                serialized_result_json=_EqualityAliasString(
                    '{"preview_id":"forged","status":"success"}',
                    equality_alias='{"preview_id":"preview_1","status":"success"}',
                ),
            ),
            "serialized_result_invalid",
        ),
    ),
)
def test_terminal_json_carriers_require_exact_plain_strings_before_equality(
    mutate: Callable[[AgentToolTerminalResult], AgentToolTerminalResult],
    message: str,
) -> None:
    with pytest.raises(AgentToolResultSlotError, match=message):
        mutate(_terminal())


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda terminal: replace(
                terminal,
                owner_result_ref_json='{"preview_revision": 7, "preview_id": "preview_1"}',
            ),
            "owner_result_ref_invalid",
        ),
        (
            lambda terminal: replace(
                terminal,
                serialized_result_json='{"status": "success", "preview_id": "preview_1"}',
            ),
            "serialized_result_invalid",
        ),
    ),
)
def test_terminal_rejects_plain_noncanonical_json_carriers(
    mutate: Callable[[AgentToolTerminalResult], AgentToolTerminalResult],
    message: str,
) -> None:
    with pytest.raises(AgentToolResultSlotError, match=message):
        mutate(_terminal())


def test_terminal_revalidation_regenerates_plain_canonical_json_and_digest() -> None:
    terminal = _terminal()

    rebound = terminal.revalidated()

    assert rebound == terminal
    assert rebound is not terminal
    assert type(rebound.owner_result_ref_json) is str
    assert type(rebound.serialized_result_json) is str
    assert rebound.owner_result_ref_json.encode("utf-8") == terminal.owner_result_ref_json.encode("utf-8")
    assert rebound.serialized_result_json.encode("utf-8") == terminal.serialized_result_json.encode("utf-8")
    assert rebound.serialized_result_digest == terminal.serialized_result_digest


@pytest.mark.parametrize(
    ("field_name", "forged_value", "message"),
    (
        (
            "owner_result_ref_json",
            _EqualityAliasString(
                '{"preview_id":"forged","preview_revision":7}',
                equality_alias='{"preview_id":"preview_1","preview_revision":7}',
            ),
            "owner_result_ref_invalid",
        ),
        (
            "serialized_result_json",
            _EqualityAliasString(
                '{"preview_id":"forged","status":"success"}',
                equality_alias='{"preview_id":"preview_1","status":"success"}',
            ),
            "serialized_result_invalid",
        ),
    ),
)
def test_terminal_revalidation_rejects_postconstruction_equality_alias_carriers(
    field_name: str,
    forged_value: str,
    message: str,
) -> None:
    terminal = _terminal()
    object.__setattr__(terminal, field_name, forged_value)

    with pytest.raises(AgentToolResultSlotError, match=message):
        terminal.revalidated()


def test_no_command_policy_closes_commandless_and_read_only_link_shapes() -> None:
    terminal = _terminal()
    commandless = _occurrence()
    terminal.validate_for_occurrence(commandless)

    read_only = _occurrence(tool_spec=INSPECT_OPERATION_TOOL_SPEC)
    _terminal(action_id="", operation_run_id="").validate_for_occurrence(read_only)
    terminal.validate_for_occurrence(read_only)

    without_owner = _terminal(action_id="", operation_run_id="")
    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        without_owner.validate_for_occurrence(commandless)
    with_command = _terminal(
        workflow_command_id="command_1",
        activity_run_id="activity_1",
        activity_attempt_id="activity_attempt_1",
        command_attempt=1,
        command_generation=1,
        control_epoch=1,
    )
    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        with_command.validate_for_occurrence(commandless)
    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        with_command.validate_for_occurrence(read_only)


def test_workflow_command_acceptance_policy_requires_command_without_activity_or_fences() -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC)
    assert occurrence.result_link_policy == "workflow_command_acceptance_v1"
    terminal = _terminal(workflow_command_id="command_1")
    terminal.validate_for_occurrence(occurrence)

    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        _terminal().validate_for_occurrence(occurrence)
    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        _terminal(
            workflow_command_id="command_1",
            activity_run_id="activity_1",
            activity_attempt_id="activity_attempt_1",
            command_attempt=1,
            command_generation=2,
            control_epoch=3,
        ).validate_for_occurrence(occurrence)


def test_activity_attempt_terminal_policy_requires_full_positive_chain() -> None:
    occurrence = _occurrence(tool_spec=START_ACQUISITION_RUN_TOOL_SPEC_V2)
    assert occurrence.result_link_policy == "activity_attempt_terminal_v1"
    terminal = _terminal(
        workflow_command_id="command_1",
        activity_run_id="activity_1",
        activity_attempt_id="activity_attempt_1",
        command_attempt=1,
        command_generation=2,
        control_epoch=3,
    )
    terminal.validate_for_occurrence(occurrence)

    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        _terminal(workflow_command_id="command_1").validate_for_occurrence(occurrence)
    with pytest.raises(AgentToolResultSlotError, match="link_policy_shape_invalid"):
        _terminal().validate_for_occurrence(occurrence)


@pytest.mark.parametrize(
    "overrides",
    (
        {"operation_run_id": ""},
        {"activity_run_id": "activity_1"},
        {"activity_run_id": "activity_1", "activity_attempt_id": "attempt_1"},
        {"workflow_command_id": "command_1", "action_id": "", "operation_run_id": ""},
        {"workflow_command_id": "command_1", "command_attempt": 1},
        {
            "workflow_command_id": "command_1",
            "activity_run_id": "activity_1",
            "activity_attempt_id": "attempt_1",
            "command_attempt": 1,
            "command_generation": 1,
            "control_epoch": 0,
        },
    ),
)
def test_terminal_rejects_shapes_outside_the_three_closed_policy_unions(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(
        AgentToolResultSlotError,
        match="(action_link_group_incomplete|command_link_group_incomplete)",
    ):
        _terminal(**overrides)


def test_terminal_result_rejects_partial_owner_link_zero_target_version_and_payload_drift() -> None:
    with pytest.raises(AgentToolResultSlotError, match="action_link_group_incomplete"):
        _terminal(operation_run_id="")
    with pytest.raises(AgentToolResultSlotError, match="owner_target_version_missing"):
        _terminal(owner_target_revision=0, owner_target_generation=0, owner_target_revision_token="")
    terminal = _terminal()
    with pytest.raises(AgentToolResultSlotError, match="serialized_result_digest_mismatch"):
        replace(terminal, serialized_result_digest="f" * 64)


def test_terminal_result_accepts_opaque_equality_only_owner_revision_token() -> None:
    terminal = _terminal(
        owner_target_revision=0,
        owner_target_generation=0,
        owner_target_revision_token="membership_revision:01HZX.same-token",
    )

    assert terminal.owner_target_revision_token == "membership_revision:01HZX.same-token"
    assert terminal.revalidated().owner_target_revision_token == terminal.owner_target_revision_token
    assert terminal.to_record()["owner_target_revision_token"] == terminal.owner_target_revision_token
    assert terminal.attempt_schema_version == AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2
    assert terminal.journal_schema_version == AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2
    assert terminal.to_record()["schema_version"] == AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2


@pytest.mark.parametrize(
    "token",
    (
        " membership_revision:1",
        "membership revision:1",
        "membership_revision/1",
        "membership_revision:\n1",
        "r" * 257,
    ),
)
def test_terminal_result_rejects_noncanonical_owner_revision_token(token: str) -> None:
    with pytest.raises(AgentToolResultSlotError, match="owner_target_revision_token_invalid"):
        _terminal(owner_target_revision=0, owner_target_revision_token=token)


def test_optional_owner_revision_token_requires_an_exact_string_before_empty_sentinel() -> None:
    class EmptyStringSubclass(str):
        pass

    class EmptyEqualityAlias:
        def __eq__(self, other: object) -> bool:
            return other == ""

    for token in (cast(str, EmptyStringSubclass("")), cast(str, EmptyEqualityAlias())):
        with pytest.raises(AgentToolResultSlotError, match="owner_target_revision_token_invalid"):
            _terminal(owner_target_revision_token=token)
