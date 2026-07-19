from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .acquisition_start_command_acceptance import (
    ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION,
)
from .acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_RESULT_SPEC,
    AcquisitionStartV2BoundRequest,
    AcquisitionStartV2Error,
)
from .agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from .operation_runtime import ACTION_START_ACQUISITION_RUN, DEFAULT_ACTION_REGISTRY

ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_NOT_ENABLED = (
    "acquisition_start_v2_generic_operation_control_not_enabled"
)
ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH = (
    "acquisition_start_v2_generic_operation_control_identity_mismatch"
)


def action_mapping_field(record: Mapping[str, Any], decoded_field: str, json_field: str) -> dict[str, Any]:
    value = record.get(decoded_field)
    if isinstance(value, Mapping):
        return dict(value)
    raw_value = record.get(json_field)
    if isinstance(raw_value, Mapping):
        return dict(raw_value)
    if raw_value in {None, ""}:
        return {}
    try:
        loaded = json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _exact_result_contract_pins(record: Mapping[str, Any]) -> bool:
    return (
        str(record.get("result_schema_version") or "") == ACQUISITION_START_V2_RESULT_SPEC.result_schema_version
        and str(record.get("result_schema_digest") or "") == ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest
        and str(record.get("result_serializer_owner") or "") == ACQUISITION_START_V2_RESULT_SPEC.serializer_owner
        and str(record.get("result_serializer_revision") or "") == ACQUISITION_START_V2_RESULT_SPEC.serializer_revision
        and str(record.get("result_serializer_contract_digest") or "")
        == ACQUISITION_START_V2_RESULT_SPEC.serializer_contract_digest
    )


def _current_start_v2_contract_pin_expected() -> dict[str, str]:
    return {
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


def _has_any_current_start_v2_contract_pin(record: Mapping[str, Any]) -> bool:
    """Return whether one independently identifying current start-v2 pin survives."""

    expected = _current_start_v2_contract_pin_expected()
    return any(type(record.get(field)) is str and record.get(field) == value for field, value in expected.items())


def _legacy_start_request_pin_pair() -> tuple[str, str]:
    """Return the schema-defined legacy start request pin pair.

    The legacy API-submitted start path persists the closed
    ``acquisition_root_request_v1`` request pins and the same
    ``acquisition.run.create`` workflow reference as an accepted start-v2
    aggregate.  Those two request pins are the only start-candidate pin values
    a coherent legacy record may legitimately hold; anything else that is
    nonempty and non-current is drifted provenance.
    """

    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    return str(spec.request_schema_version or ""), str(spec.request_schema_digest or "")


def _has_drifted_start_v2_pin_on_start_candidate(record: Mapping[str, Any]) -> bool:
    """Return whether a start candidate carries a nonempty drifted pin value."""

    legacy_version, legacy_digest = _legacy_start_request_pin_pair()
    legacy_request_pins = {"request_schema_version": legacy_version, "request_schema_digest": legacy_digest}
    for field, expected in _current_start_v2_contract_pin_expected().items():
        value = record.get(field)
        if value is None or value == "":
            continue
        if type(value) is str and value == expected:
            continue
        if type(value) is str and value == legacy_request_pins.get(field, ""):
            continue
        return True
    return False


def _has_acceptance_owner_ref_marker(record: Mapping[str, Any]) -> bool:
    """Return whether the persisted acceptance owner-result-ref schema survives."""

    result_ref = action_mapping_field(record, "result_ref", "result_ref_json")
    return str(result_ref.get("schema_version") or "") == ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION


def _has_start_v2_idempotency_identity(record: Mapping[str, Any]) -> bool:
    """Return whether the owner-bound ``agent-start-v2:`` idempotency identity survives."""

    idempotency_key = record.get("idempotency_key")
    return (
        type(idempotency_key) is str
        and idempotency_key.startswith("agent-start-v2:")
        and bool(idempotency_key.removeprefix("agent-start-v2:").strip())
    )


def _has_owner_bound_occurrence_marker(action: Mapping[str, Any], *, action_metadata: Mapping[str, Any]) -> bool:
    """Return whether the owner-bound occurrence reference survives.

    A well-formed ``result_occurrence_ref`` is start-v2 provenance only when it
    is bound to the start owner: either the action carries the matching
    ``agent-start-v2:<logical_occurrence_digest>`` idempotency pair, or the
    action is itself a start candidate.  Stray occurrence-shaped metadata on
    non-start actions remains ignored so it cannot reserve legacy actions.
    """

    occurrence_ref = action_metadata.get("result_occurrence_ref")
    if not _exact_result_occurrence_ref(occurrence_ref):
        return False
    logical_occurrence_digest = str(dict(occurrence_ref)["logical_occurrence_digest"])
    return bool(
        str(action.get("idempotency_key") or "") == f"agent-start-v2:{logical_occurrence_digest}"
        or str(action.get("action_type") or "").strip() == ACTION_START_ACQUISITION_RUN
    )


def _has_start_workflow_reference_marker(operation: Mapping[str, Any]) -> bool:
    """Return whether the Operation start workflow reference survives.

    The legacy schema-defined start path persists the same
    ``acquisition.run.create`` workflow reference, so the reference alone is
    ambiguous.  It is start-v2 provenance only when the Operation cannot be
    explained as a coherent legacy start, i.e. when it does not carry the
    exact legacy request pin pair.
    """

    workflow_ref = action_mapping_field(operation, "workflow_ref", "workflow_ref_json")
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    if (
        str(workflow_ref.get("command_type") or "") != str(spec.default_workflow_command_type or "")
        or str(workflow_ref.get("owner") or "") != str(spec.owner_module or "")
        or not str(workflow_ref.get("workflow_run_id") or "")
        or not str(workflow_ref.get("command_id") or "")
    ):
        return False
    legacy_version, legacy_digest = _legacy_start_request_pin_pair()
    return not (
        str(operation.get("request_schema_version") or "") == legacy_version
        and str(operation.get("request_schema_digest") or "") == legacy_digest
    )


def _exact_result_occurrence_ref(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "result_slot_id",
        "slot_generation",
        "logical_occurrence_digest",
    }:
        return False
    result_slot_id = value.get("result_slot_id")
    slot_generation = value.get("slot_generation")
    logical_occurrence_digest = value.get("logical_occurrence_digest")
    return bool(
        type(result_slot_id) is str
        and result_slot_id
        and result_slot_id == result_slot_id.strip()
        and type(slot_generation) is int
        and slot_generation > 0
        and type(logical_occurrence_digest) is str
        and len(logical_occurrence_digest) == 64
        and all(character in "0123456789abcdef" for character in logical_occurrence_digest)
    )


def _exact_action_start_v2_contract(
    action: Mapping[str, Any],
    *,
    action_input: Mapping[str, Any],
    action_target: Mapping[str, Any],
    action_metadata: Mapping[str, Any],
) -> bool:
    try:
        request = AcquisitionStartV2BoundRequest(
            {
                "input_payload": dict(action_input),
                "target_ref": dict(action_target),
            }
        )
    except AcquisitionStartV2Error:
        return False
    occurrence_ref = action_metadata.get("result_occurrence_ref")
    snapshot_tool_pins = request.snapshot.tool_pins.to_record()
    logical_occurrence_digest = (
        occurrence_ref.get("logical_occurrence_digest") if isinstance(occurrence_ref, Mapping) else None
    )
    expected_start_idempotency = (
        f"agent-start-v2:{logical_occurrence_digest}" if type(logical_occurrence_digest) is str else ""
    )
    return bool(
        str(action.get("action_type") or "") == ACTION_START_ACQUISITION_RUN
        and str(action.get("owner_module") or "") == "acquisition_run_writer"
        and str(action.get("operation_type") or "") == "acquisition_run"
        and str(action.get("workspace_id") or "") == str(request.target_ref.get("workspace_id") or "")
        and str(action.get("request_schema_version") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        and str(action.get("request_schema_digest") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        and str(action.get("tool_name") or "") == START_ACQUISITION_RUN_TOOL_SPEC.tool_name
        and str(action.get("tool_spec_version") or "")
        == snapshot_tool_pins["tool_spec_version"]
        == START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version
        and str(action.get("tool_spec_digest") or "")
        == snapshot_tool_pins["tool_spec_digest"]
        == START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest
        and _exact_result_contract_pins(action)
        and _exact_result_occurrence_ref(occurrence_ref)
        and type(action.get("idempotency_key")) is str
        and action.get("idempotency_key") == expected_start_idempotency
    )


def _exact_operation_start_v2_contract(
    operation: Mapping[str, Any],
    *,
    action: Mapping[str, Any],
) -> bool:
    return bool(
        str(operation.get("action_id") or "") == str(action.get("action_id") or "")
        and str(operation.get("workspace_id") or "") == str(action.get("workspace_id") or "")
        and str(operation.get("owner_module") or "") == "acquisition_run_writer"
        and str(operation.get("operation_type") or "") == "acquisition_run"
        and str(operation.get("request_schema_version") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        and str(operation.get("request_schema_digest") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        and str(operation.get("tool_name") or "")
        == str(action.get("tool_name") or "")
        == START_ACQUISITION_RUN_TOOL_SPEC.tool_name
        and str(operation.get("tool_spec_version") or "") == str(action.get("tool_spec_version") or "")
        and str(operation.get("tool_spec_digest") or "") == str(action.get("tool_spec_digest") or "")
        and type(operation.get("idempotency_key")) is str
        and operation.get("idempotency_key") == action.get("idempotency_key")
        and _exact_result_contract_pins(operation)
    )


def classify_acquisition_start_v2_generic_control_provenance(
    *,
    action: Mapping[str, Any],
    operation_run: Mapping[str, Any] | None = None,
) -> str:
    """Classify persisted start-v2 provenance before generic operation controls.

    Return values:
    - ``non_v2``: no start-v2 provenance; legacy/open generic controls remain legal.
    - ``exact_v2``: the known current start-v2 provenance is complete enough to report the normal unsupported reason.
    - ``partial_or_mixed_v2``: any start-v2 marker is present, corrupt, downgraded, or split; generic controls fail closed.

    Provenance markers are intentionally independent so that a compound
    downgrade cannot erase them together: any exact current request/tool/result
    pin, a start-snapshot marker, preview input keys, the persisted acceptance
    owner-result-ref schema in Action/Operation result refs, the owner-bound
    ``agent-start-v2:`` idempotency identity, the owner-bound occurrence
    reference, the Operation start workflow reference that no coherent legacy
    start can explain, and any nonempty drifted pin on a start candidate.
    """

    action_type = str(action.get("action_type") or "").strip()
    action_input = action_mapping_field(action, "input", "input_json")
    action_target = action_mapping_field(action, "target_ref", "target_ref_json")
    action_metadata = action_mapping_field(action, "metadata", "metadata_json")
    operation = dict(operation_run or {})
    action_v2_input_keys = {"preview_id", "preview_revision", "preview_digest"}
    action_input_key_set = {str(key) for key in action_input}
    action_has_any_v2_input_key = action_type == ACTION_START_ACQUISITION_RUN and bool(
        action_v2_input_keys & action_input_key_set
    )
    action_has_any_current_v2_pin = _has_any_current_start_v2_contract_pin(action)
    operation_has_any_current_v2_pin = bool(operation) and _has_any_current_start_v2_contract_pin(operation)
    start_snapshot = action_target.get("start_snapshot")
    start_snapshot_mapping = dict(start_snapshot) if isinstance(start_snapshot, Mapping) else {}
    action_has_v2_start_snapshot = bool(start_snapshot_mapping) and bool(
        start_snapshot_mapping.get("schema_version") or start_snapshot_mapping.get("snapshot_digest")
    )
    start_candidate = action_type == ACTION_START_ACQUISITION_RUN
    action_has_v2_provenance = (
        action_has_any_current_v2_pin
        or action_has_v2_start_snapshot
        or action_has_any_v2_input_key
        or _has_acceptance_owner_ref_marker(action)
        or _has_start_v2_idempotency_identity(action)
        or _has_owner_bound_occurrence_marker(action, action_metadata=action_metadata)
        or (start_candidate and _has_drifted_start_v2_pin_on_start_candidate(action))
    )
    operation_has_v2_provenance = bool(operation) and (
        operation_has_any_current_v2_pin
        or _has_acceptance_owner_ref_marker(operation)
        or _has_start_v2_idempotency_identity(operation)
        or _has_start_workflow_reference_marker(operation)
        or (start_candidate and _has_drifted_start_v2_pin_on_start_candidate(operation))
    )
    if not action_has_v2_provenance and not operation_has_v2_provenance:
        return "non_v2"
    if _exact_action_start_v2_contract(
        action,
        action_input=action_input,
        action_target=action_target,
        action_metadata=action_metadata,
    ) and (not operation or _exact_operation_start_v2_contract(operation, action=action)):
        return "exact_v2"
    return "partial_or_mixed_v2"


def acquisition_start_v2_generic_operation_control_preflight(
    *,
    action: Mapping[str, Any],
    operation_run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    operation = dict(operation_run or {})
    classification = classify_acquisition_start_v2_generic_control_provenance(
        action=action,
        operation_run=operation,
    )
    if classification == "non_v2":
        return {"status": "ready"}
    if classification != "exact_v2":
        return {
            "status": "invalid",
            "reason": ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_IDENTITY_MISMATCH,
            "operation_run": operation,
            "action": dict(action),
            "module_state_mutated": False,
        }
    return {
        "status": "unsupported",
        "reason": ACQUISITION_START_V2_GENERIC_OPERATION_CONTROL_NOT_ENABLED,
        "operation_run": operation,
        "action": dict(action),
        "module_state_mutated": False,
    }
