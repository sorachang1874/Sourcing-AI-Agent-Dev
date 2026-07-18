from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from sourcing_agent import acquisition_start_v2_postgres as start_postgres
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_RESULT_SPEC,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
)
from sourcing_agent.acquisition_start_v2_control import (
    ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH,
    ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_NOT_ENABLED,
    acquisition_start_v2_generic_operation_control_preflight,
    classify_acquisition_start_v2_generic_control_provenance,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence
from sourcing_agent.operation_runtime import ACTION_START_ACQUISITION_RUN, DEFAULT_ACTION_REGISTRY
from tests.test_d1n_start_acquisition_v2 import _CONTEXT, _NOW, _preview, _PreviewRepository, _reference

_CURRENT_START_V2_PINS = {
    "request_schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    "request_schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    "tool_name": START_ACQUISITION_RUN_TOOL_SPEC.tool_name,
    "tool_spec_version": START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
    "tool_spec_digest": START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
    "result_schema_version": ACQUISITION_START_V2_RESULT_SPEC.result_schema_version,
    "result_schema_digest": ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest,
    "result_serializer_owner": ACQUISITION_START_V2_RESULT_SPEC.serializer_owner,
    "result_serializer_revision": ACQUISITION_START_V2_RESULT_SPEC.serializer_revision,
    "result_serializer_contract_digest": ACQUISITION_START_V2_RESULT_SPEC.serializer_contract_digest,
}
_NON_START_ACTION_TYPES = tuple(
    action_type
    for action_type in sorted(DEFAULT_ACTION_REGISTRY.to_record())
    if action_type != ACTION_START_ACQUISITION_RUN
)
_WELL_FORMED_OCCURRENCE = {
    "result_slot_id": "slot-unrelated-1",
    "slot_generation": 1,
    "logical_occurrence_digest": "7" * 64,
}
_MALFORMED_OCCURRENCE = {
    "result_slot_id": "slot-unrelated-1",
    "slot_generation": True,
    "logical_occurrence_digest": "not-a-digest",
}


def _exact_records() -> tuple[dict[str, Any], dict[str, Any]]:
    preview = _preview()
    bound = AcquisitionStartV2OwnerBinder(_PreviewRepository(preview)).bind(
        input_payload=_reference(preview),
        context=_CONTEXT,
        tool_pins=AcquisitionStartV2ToolPins(
            tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
            tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
        ),
        now=_NOW,
    )
    occurrence = AgentToolOccurrence.from_tool_spec(
        result_slot_id="slot-start-provenance-1",
        slot_generation=1,
        workspace_id=_CONTEXT.workspace_id,
        actor_id=_CONTEXT.requester_id,
        runtime_namespace="isolated_local_canary",
        provider_mode="simulate",
        turn_id="turn-start-provenance-1",
        step_id="step-start-provenance-1",
        tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
        canonical_args=bound.to_record(),
        occurrence_ordinal=1,
    )
    binding = start_postgres.revalidate_acquisition_start_v2_occurrence(occurrence)
    action, _ = start_postgres._canonical_submit_rows(  # noqa: SLF001
        binding,
        submitted_at="2026-07-17T00:30:00Z",
    )
    operation = {
        "operation_run_id": "op-start-provenance-1",
        "action_id": binding.action_id,
        "workspace_id": occurrence.workspace_id,
        "owner_module": "acquisition_run_writer",
        "operation_type": "acquisition_run",
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
        "idempotency_key": binding.start_idempotency,
    }
    return action, operation


def _schema_less_non_start_action(
    action_type: str = "fetch_profile_sample",
    *,
    occurrence_ref: object = _WELL_FORMED_OCCURRENCE,
) -> dict[str, Any]:
    return {
        "action_id": f"act-{action_type}",
        "action_type": action_type,
        "workspace_id": "workspace-1",
        "request_schema_version": "",
        "request_schema_digest": "",
        "input": {},
        "target_ref": {},
        "metadata": {"result_occurrence_ref": copy.deepcopy(occurrence_ref)},
    }


def test_exact_current_action_and_operation_remain_exact_and_unsupported() -> None:
    action, operation = _exact_records()

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "exact_v2"
    )
    preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )

    assert preflight["status"] == "unsupported"
    assert preflight["reason"] == ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_NOT_ENABLED
    assert preflight["module_state_mutated"] is False


@pytest.mark.parametrize(("field", "value"), tuple(_CURRENT_START_V2_PINS.items()))
def test_each_retained_operation_pin_independently_fails_closed(field: str, value: str) -> None:
    action = _schema_less_non_start_action()
    operation = {
        "operation_run_id": "op-retained-marker",
        "action_id": action["action_id"],
        "workspace_id": action["workspace_id"],
        field: value,
    }
    baseline = copy.deepcopy((action, operation))

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    )
    preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )

    assert preflight["status"] == "invalid"
    assert preflight["reason"] == ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH
    assert preflight["module_state_mutated"] is False
    assert (action, operation) == baseline


@pytest.mark.parametrize(("field", "value"), tuple(_CURRENT_START_V2_PINS.items()))
def test_each_retained_action_pin_independently_fails_closed(field: str, value: str) -> None:
    action = _schema_less_non_start_action()
    action[field] = value
    baseline = copy.deepcopy(action)

    assert classify_acquisition_start_v2_generic_control_provenance(action=action) == "partial_or_mixed_v2"
    preflight = acquisition_start_v2_generic_operation_control_preflight(action=action)

    assert preflight["status"] == "invalid"
    assert preflight["reason"] == ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH
    assert preflight["module_state_mutated"] is False
    assert action == baseline


@pytest.mark.parametrize("action_type", _NON_START_ACTION_TYPES)
@pytest.mark.parametrize("occurrence_ref", (_WELL_FORMED_OCCURRENCE, _MALFORMED_OCCURRENCE))
def test_occurrence_shaped_metadata_does_not_reserve_schema_less_non_start_actions(
    action_type: str,
    occurrence_ref: object,
) -> None:
    action = _schema_less_non_start_action(action_type, occurrence_ref=occurrence_ref)
    operation = {
        "operation_run_id": f"op-{action_type}",
        "action_id": action["action_id"],
        "workspace_id": action["workspace_id"],
        "request_schema_version": "",
        "request_schema_digest": "",
    }
    baseline = copy.deepcopy((action, operation))

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "non_v2"
    )
    assert acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    ) == {"status": "ready"}
    assert (action, operation) == baseline


@pytest.mark.parametrize(
    ("record", "field", "replacement"),
    (
        ("action", "tool_name", "legacy-start-tool"),
        ("operation", "tool_name", "legacy-start-tool"),
        ("action", "idempotency_key", "agent-start-v2:" + "0" * 64),
        ("operation", "idempotency_key", "agent-start-v2:" + "0" * 64),
    ),
)
def test_tool_name_and_occurrence_idempotency_drift_are_partial_not_exact(
    record: str,
    field: str,
    replacement: str,
) -> None:
    action, operation = _exact_records()
    target = action if record == "action" else operation
    target[field] = replacement

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    )


def test_swapped_occurrence_shape_cannot_remain_exact_without_matching_start_idempotency() -> None:
    action, operation = _exact_records()
    action_metadata = json.loads(action["metadata_json"])
    action_metadata["result_occurrence_ref"]["logical_occurrence_digest"] = "9" * 64
    action["metadata_json"] = action_metadata

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    )
