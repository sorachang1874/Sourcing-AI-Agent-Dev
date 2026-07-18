from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_RESULT_SPEC,
    AcquisitionStartV2BoundRequest,
    AcquisitionStartV2Error,
)
from .operation_runtime import ACTION_START_ACQUISITION_RUN

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
        and str(record.get("result_serializer_revision") or "")
        == ACQUISITION_START_V2_RESULT_SPEC.serializer_revision
        and str(record.get("result_serializer_contract_digest") or "")
        == ACQUISITION_START_V2_RESULT_SPEC.serializer_contract_digest
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
    return bool(
        str(action.get("action_type") or "") == ACTION_START_ACQUISITION_RUN
        and str(action.get("owner_module") or "") == "acquisition_run_writer"
        and str(action.get("operation_type") or "") == "acquisition_run"
        and str(action.get("workspace_id") or "") == str(request.target_ref.get("workspace_id") or "")
        and str(action.get("request_schema_version") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        and str(action.get("request_schema_digest") or "") == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        and str(action.get("tool_spec_version") or "") == snapshot_tool_pins["tool_spec_version"]
        and str(action.get("tool_spec_digest") or "") == snapshot_tool_pins["tool_spec_digest"]
        and _exact_result_contract_pins(action)
        and _exact_result_occurrence_ref(occurrence_ref)
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
        and str(operation.get("tool_spec_version") or "") == str(action.get("tool_spec_version") or "")
        and str(operation.get("tool_spec_digest") or "") == str(action.get("tool_spec_digest") or "")
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
    action_has_any_current_v2_pin = (
        str(action.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        or str(action.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    operation_has_any_current_v2_pin = bool(operation) and (
        str(operation.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        or str(operation.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    start_snapshot = action_target.get("start_snapshot")
    start_snapshot_mapping = dict(start_snapshot) if isinstance(start_snapshot, Mapping) else {}
    action_has_v2_start_snapshot = bool(start_snapshot_mapping) and bool(
        start_snapshot_mapping.get("schema_version") or start_snapshot_mapping.get("snapshot_digest")
    )
    action_has_result_occurrence = isinstance(action_metadata.get("result_occurrence_ref"), Mapping)
    action_has_v2_result_pins = (
        str(action.get("result_schema_version") or "").strip() == ACQUISITION_START_V2_RESULT_SPEC.result_schema_version
        or str(action.get("result_schema_digest") or "").strip()
        == ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest
        or str(action.get("result_serializer_owner") or "").strip() == ACQUISITION_START_V2_RESULT_SPEC.serializer_owner
        or str(action.get("result_serializer_revision") or "").strip()
        == ACQUISITION_START_V2_RESULT_SPEC.serializer_revision
        or str(action.get("result_serializer_contract_digest") or "").strip()
        == ACQUISITION_START_V2_RESULT_SPEC.serializer_contract_digest
    )
    action_has_v2_provenance = (
        action_has_any_current_v2_pin
        or action_has_v2_start_snapshot
        or action_has_result_occurrence
        or action_has_v2_result_pins
        or action_has_any_v2_input_key
    )
    operation_has_v2_provenance = bool(operation) and (
        operation_has_any_current_v2_pin
        or (
            action_type == ACTION_START_ACQUISITION_RUN
            and str(operation.get("owner_module") or "").strip() == "acquisition_run_writer"
            and bool(action_has_v2_provenance)
        )
    )
    if not action_has_v2_provenance and not operation_has_v2_provenance:
        return "non_v2"
    if (
        _exact_action_start_v2_contract(
            action,
            action_input=action_input,
            action_target=action_target,
            action_metadata=action_metadata,
        )
        and (not operation or _exact_operation_start_v2_contract(operation, action=action))
    ):
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
