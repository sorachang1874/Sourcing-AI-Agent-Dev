from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from sourcing_agent import acquisition_start_v2_postgres as start_postgres
from sourcing_agent.acquisition_start_command_acceptance import (
    ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION,
)
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


def _erase_all_pins(record: dict[str, Any]) -> None:
    for field in _CURRENT_START_V2_PINS:
        record[field] = ""


def _erase_start_snapshot_and_input_keys(action: dict[str, Any]) -> None:
    action["input_json"] = {}
    action["target_ref_json"] = {}


def _erase_accepted_result_carriers(action: dict[str, Any], operation: dict[str, Any]) -> None:
    action["result_ref_json"] = {}
    action["idempotency_key"] = "legacy-drifted-idempotency"
    action["metadata_json"] = {}
    operation["result_ref_json"] = {}
    operation["idempotency_key"] = "legacy-drifted-idempotency"
    operation["workflow_ref_json"] = {}


def _owner_result_ref_carrier() -> dict[str, Any]:
    return {"schema_version": ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION}


def _start_workflow_ref_carrier() -> dict[str, Any]:
    return {
        "workflow_run_id": "wf_operation_" + "1" * 24,
        "command_id": "cmd_" + "2" * 24,
        "command_type": "acquisition.run.create",
        "owner": "acquisition_run_writer",
    }


@pytest.mark.parametrize(
    "carrier",
    (
        "action_result_ref",
        "operation_result_ref",
        "occurrence_idempotency_pair",
        "operation_idempotency_identity",
        "operation_workflow_reference",
    ),
)
def test_each_immutable_accepted_result_carrier_alone_survives_compound_pin_drift(carrier: str) -> None:
    action, operation = _exact_records()
    occurrence_ref = json.loads(action["metadata_json"])["result_occurrence_ref"]
    start_idempotency = action["idempotency_key"]
    # Erase every current pin and the input/snapshot markers so that exactly one
    # immutable accepted-result carrier survives alone; junk-valued pins are
    # covered separately by the drifted-pin rule below.
    for record in (action, operation):
        _erase_all_pins(record)
    _erase_start_snapshot_and_input_keys(action)
    _erase_accepted_result_carriers(action, operation)
    if carrier == "action_result_ref":
        action["result_ref_json"] = _owner_result_ref_carrier()
    elif carrier == "operation_result_ref":
        operation["result_ref_json"] = _owner_result_ref_carrier()
    elif carrier == "occurrence_idempotency_pair":
        action["metadata_json"] = {"result_occurrence_ref": occurrence_ref}
        action["idempotency_key"] = start_idempotency
    elif carrier == "operation_idempotency_identity":
        operation["idempotency_key"] = start_idempotency
    elif carrier == "operation_workflow_reference":
        operation["workflow_ref_json"] = _start_workflow_ref_carrier()

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    ), carrier
    preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )

    assert preflight["status"] == "invalid"
    assert preflight["reason"] == ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH
    assert preflight["module_state_mutated"] is False


def test_compound_pin_drift_without_any_carrier_still_fails_closed_on_start_candidate() -> None:
    action, operation = _exact_records()
    for record in (action, operation):
        for field in _CURRENT_START_V2_PINS:
            record[field] = f"drifted-{field}"
    _erase_start_snapshot_and_input_keys(action)
    _erase_accepted_result_carriers(action, operation)

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    )


def _legacy_schema_defined_start_records() -> tuple[dict[str, Any], dict[str, Any]]:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    action = {
        "action_id": "act-legacy-start-1",
        "action_type": ACTION_START_ACQUISITION_RUN,
        "workspace_id": "workspace-1",
        "request_schema_version": spec.request_schema_version,
        "request_schema_digest": spec.request_schema_digest,
        "input": {"target_company": "Acme", "query": "find people"},
        "target_ref": {"workspace_id": "workspace-1"},
        "metadata": {},
        "result_ref": {},
        "idempotency_key": "legacy-start-idempotency",
    }
    operation = {
        "operation_run_id": "op-legacy-start-1",
        "action_id": "act-legacy-start-1",
        "workspace_id": "workspace-1",
        "request_schema_version": spec.request_schema_version,
        "request_schema_digest": spec.request_schema_digest,
        "workflow_ref": _start_workflow_ref_carrier(),
        "result_ref": {},
        "idempotency_key": "legacy-start-idempotency",
    }
    return action, operation


def test_coherent_legacy_schema_defined_start_remains_generic_ready() -> None:
    action, operation = _legacy_schema_defined_start_records()
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


def test_drifted_legacy_start_pin_pair_fails_closed() -> None:
    action, operation = _legacy_schema_defined_start_records()
    operation["request_schema_digest"] = "f" * 64

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    )


def test_occurrence_reference_on_start_candidate_is_an_owner_bound_marker() -> None:
    action = {
        "action_id": "act-start-occurrence-only",
        "action_type": ACTION_START_ACQUISITION_RUN,
        "workspace_id": "workspace-1",
        "request_schema_version": "",
        "request_schema_digest": "",
        "input": {},
        "target_ref": {},
        "metadata": {"result_occurrence_ref": copy.deepcopy(_WELL_FORMED_OCCURRENCE)},
        "result_ref": {},
        "idempotency_key": "legacy-drifted-idempotency",
    }

    assert classify_acquisition_start_v2_generic_control_provenance(action=action) == "partial_or_mixed_v2"


def test_agent_start_v2_idempotency_identity_on_non_start_action_fails_closed() -> None:
    action = _schema_less_non_start_action()
    action["idempotency_key"] = "agent-start-v2:" + "b" * 64

    assert classify_acquisition_start_v2_generic_control_provenance(action=action) == "partial_or_mixed_v2"


def _erased_start_candidate_records() -> tuple[dict[str, Any], dict[str, Any]]:
    """Return start-candidate records with every pin erased and carriers cleared."""

    action, operation = _exact_records()
    for record in (action, operation):
        _erase_all_pins(record)
    _erase_start_snapshot_and_input_keys(action)
    _erase_accepted_result_carriers(action, operation)
    return action, operation


def _legacy_coherent_pins(record: dict[str, Any]) -> None:
    legacy_version, legacy_digest = (
        str(DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN).request_schema_version or ""),
        str(DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN).request_schema_digest or ""),
    )
    record["request_schema_version"] = legacy_version
    record["request_schema_digest"] = legacy_digest


_CORRUPT_CARRIER_CASES = (
    "action_result_ref_schema_family_drift",
    "operation_result_ref_schema_family_drift",
    "action_result_ref_malformed_json",
    "action_metadata_malformed_json",
    "operation_workflow_ref_malformed_json",
    "action_idempotency_blank_suffix",
    "operation_idempotency_blank_suffix",
    "occurrence_ref_present_but_malformed",
    "operation_workflow_ref_split_owner",
    "operation_workflow_ref_incomplete_ids",
    "action_metadata_decoded_raw_conflict",
    "action_result_ref_decoded_raw_conflict",
)


def _corrupt_carrier(records: tuple[dict[str, Any], dict[str, Any]], case: str) -> None:
    action, operation = records
    if case == "action_result_ref_schema_family_drift":
        action["result_ref_json"] = {"schema_version": "acquisition_start_command_acceptance_owner_result_ref.v999"}
    elif case == "operation_result_ref_schema_family_drift":
        operation["result_ref_json"] = {"schema_version": "acquisition_start_command_acceptance.v2"}
    elif case == "action_result_ref_malformed_json":
        action["result_ref_json"] = "{not-json"
    elif case == "action_metadata_malformed_json":
        action["metadata_json"] = '["not", "a", "mapping"]'
    elif case == "operation_workflow_ref_malformed_json":
        operation["workflow_ref_json"] = "{not-json"
    elif case == "action_idempotency_blank_suffix":
        action["idempotency_key"] = "agent-start-v2:"
    elif case == "operation_idempotency_blank_suffix":
        operation["idempotency_key"] = "agent-start-v2:   "
    elif case == "occurrence_ref_present_but_malformed":
        action["metadata_json"] = {"result_occurrence_ref": copy.deepcopy(_MALFORMED_OCCURRENCE)}
    elif case == "operation_workflow_ref_split_owner":
        carrier = _start_workflow_ref_carrier()
        carrier["owner"] = "drifted_owner"
        operation["workflow_ref_json"] = carrier
    elif case == "operation_workflow_ref_incomplete_ids":
        carrier = _start_workflow_ref_carrier()
        del carrier["command_id"]
        operation["workflow_ref_json"] = carrier
    elif case == "action_metadata_decoded_raw_conflict":
        action["metadata"] = {"unrelated_key": True}
        action["metadata_json"] = json.dumps({"other_key": 1})
    elif case == "action_result_ref_decoded_raw_conflict":
        action["result_ref"] = {"schema_version": "unrelated_ref.v1"}
        action["result_ref_json"] = json.dumps({"schema_version": "other_ref.v2"})
    else:  # pragma: no cover - parametrization guard
        raise AssertionError(f"unknown corrupt carrier case {case}")


@pytest.mark.parametrize("case", _CORRUPT_CARRIER_CASES)
@pytest.mark.parametrize("pins", ("erased", "legacy_coherent"))
def test_each_corrupt_start_carrier_forces_partial_or_mixed_v2(case: str, pins: str) -> None:
    records = _erased_start_candidate_records()
    if pins == "legacy_coherent":
        for record in records:
            _legacy_coherent_pins(record)
    _corrupt_carrier(records, case)
    action, operation = records
    baseline = copy.deepcopy(records)

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "partial_or_mixed_v2"
    ), (case, pins)
    preflight = acquisition_start_v2_generic_operation_control_preflight(
        action=action,
        operation_run=operation,
    )

    assert preflight["status"] == "invalid"
    assert preflight["reason"] == ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH
    assert preflight["module_state_mutated"] is False
    assert (action, operation) == baseline


def test_drifted_start_acceptance_schema_family_on_non_start_action_fails_closed() -> None:
    action = _schema_less_non_start_action()
    action["result_ref"] = {"schema_version": "acquisition_start_command_acceptance_owner_result_ref.v999"}

    assert classify_acquisition_start_v2_generic_control_provenance(action=action) == "partial_or_mixed_v2"


def test_malformed_json_carriers_on_non_start_action_remain_ignored() -> None:
    action = _schema_less_non_start_action()
    action["metadata_json"] = "{not-json"
    action["result_ref_json"] = "[1, 2]"
    operation = {
        "operation_run_id": "op-non-start-malformed",
        "action_id": action["action_id"],
        "workspace_id": action["workspace_id"],
        "workflow_ref_json": "{not-json",
    }

    assert (
        classify_acquisition_start_v2_generic_control_provenance(
            action=action,
            operation_run=operation,
        )
        == "non_v2"
    )
