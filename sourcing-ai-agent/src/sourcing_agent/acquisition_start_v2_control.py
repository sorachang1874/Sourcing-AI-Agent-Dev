from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_RESULT_SPEC,
    ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION,
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
    action_has_v2_input_shape = action_v2_input_keys.issubset(action_input_key_set)
    action_has_any_v2_input_key = action_type == ACTION_START_ACQUISITION_RUN and bool(
        action_v2_input_keys & action_input_key_set
    )
    action_has_v2_pin_pair = (
        str(action.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        and str(action.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    action_has_any_current_v2_pin = (
        str(action.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        or str(action.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    operation_has_v2_pin_pair = (
        bool(operation)
        and str(operation.get("operation_type") or "").strip() == "acquisition_run"
        and str(operation.get("owner_module") or "").strip() == "acquisition_run_writer"
        and str(operation.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        and str(operation.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    operation_has_any_current_v2_pin = bool(operation) and (
        str(operation.get("request_schema_version") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        or str(operation.get("request_schema_digest") or "").strip() == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    )
    start_snapshot = action_target.get("start_snapshot")
    start_snapshot_mapping = dict(start_snapshot) if isinstance(start_snapshot, Mapping) else {}
    action_has_v2_start_snapshot = bool(start_snapshot_mapping) and (
        str(start_snapshot_mapping.get("schema_version") or "").strip() == ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION
        or bool(start_snapshot_mapping.get("snapshot_digest"))
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
    operation_has_v2_provenance = (
        bool(operation)
        and str(operation.get("operation_type") or "").strip() == "acquisition_run"
        and (
            operation_has_any_current_v2_pin
            or (
                action_type == ACTION_START_ACQUISITION_RUN
                and str(operation.get("owner_module") or "").strip() == "acquisition_run_writer"
                and bool(action_has_v2_provenance)
            )
        )
    )
    if not action_has_v2_provenance and not operation_has_v2_provenance:
        return "non_v2"
    if (
        action_type == ACTION_START_ACQUISITION_RUN
        and action_has_v2_pin_pair
        and action_has_v2_input_shape
        and action_has_v2_start_snapshot
        and action_has_result_occurrence
        and action_has_v2_result_pins
        and (
            not operation
            or (
                operation_has_v2_pin_pair
                and str(operation.get("action_id") or "").strip() == str(action.get("action_id") or "").strip()
            )
        )
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
