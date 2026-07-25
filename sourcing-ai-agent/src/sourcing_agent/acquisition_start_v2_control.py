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
from .json_contract import JsonContractShapeError, json_contract_equal, loads_json_contract_strict
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


_JSON_CARRIER_ABSENT = "absent"
_JSON_CARRIER_DECODED = "decoded"
_JSON_CARRIER_MALFORMED = "malformed"
_JSON_CARRIER_CONFLICT = "conflict"


def _json_carrier_state(
    record: Mapping[str, Any],
    decoded_field: str,
    json_field: str,
) -> tuple[str, dict[str, Any]]:
    """Decode one JSON mapping carrier into an explicit tri-state-plus-conflict value.

    ``action_mapping_field`` collapses malformed or conflicting carriers to
    ``{}``, which silently erases provenance.  Provenance classification must
    instead distinguish a genuinely absent carrier from a present-but-malformed
    one, and from a record whose decoded and raw JSON forms disagree, so that a
    corrupt start carrier can never be silently treated as missing.  Raw text is
    parsed by the canonical strict contract decoder (duplicate object keys and
    non-finite ``NaN``/``Infinity`` constants are malformed, never normalized),
    and decoded/raw comparison uses type-strict contract equality so
    ``1 == True`` and ``1 == 1.0`` can never alias a conflicting carrier into a
    matching one.
    """

    decoded_value = record.get(decoded_field)
    raw_value = record.get(json_field)
    decoded_mapping = dict(decoded_value) if isinstance(decoded_value, Mapping) else None
    raw_mapping: dict[str, Any] | None = None
    raw_present = raw_value is not None and raw_value != ""
    raw_malformed = False
    if isinstance(raw_value, Mapping):
        raw_mapping = dict(raw_value)
    elif raw_present:
        try:
            loaded = loads_json_contract_strict(str(raw_value))
        except JsonContractShapeError:
            raw_malformed = True
        else:
            if isinstance(loaded, Mapping):
                raw_mapping = dict(loaded)
            else:
                raw_malformed = True
    if decoded_mapping is not None:
        if raw_malformed or (raw_mapping is not None and not json_contract_equal(raw_mapping, decoded_mapping)):
            return _JSON_CARRIER_CONFLICT, {}
        return _JSON_CARRIER_DECODED, decoded_mapping
    if decoded_value is not None and decoded_value != "":
        return _JSON_CARRIER_MALFORMED, {}
    if raw_malformed or raw_present:
        return (_JSON_CARRIER_DECODED, raw_mapping) if raw_mapping is not None else (_JSON_CARRIER_MALFORMED, {})
    return _JSON_CARRIER_ABSENT, {}


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


_CARRIER_ABSENT = "absent"
_CARRIER_EXACT = "exact"
_CARRIER_CORRUPT = "corrupt"

_ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_FAMILY = "acquisition_start_command_acceptance"
_AGENT_START_V2_IDEMPOTENCY_PREFIX = "agent-start-v2:"


def _acceptance_owner_ref_carrier_state(result_ref: Mapping[str, Any]) -> str:
    """Tri-state the persisted acceptance owner-result-ref carrier.

    A ``schema_version`` from the start-acceptance schema family remains
    start-specific provenance even when its exact version drifted; only a
    completely unrelated or missing schema is genuinely absent.
    """

    schema_version = result_ref.get("schema_version")
    if schema_version == ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION:
        return _CARRIER_EXACT
    if type(schema_version) is str and schema_version.startswith(_ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_FAMILY):
        return _CARRIER_CORRUPT
    return _CARRIER_ABSENT


def _start_v2_idempotency_carrier_state(record: Mapping[str, Any]) -> str:
    """Tri-state the owner-bound ``agent-start-v2:`` idempotency identity."""

    idempotency_key = record.get("idempotency_key")
    if type(idempotency_key) is not str or not idempotency_key.startswith(_AGENT_START_V2_IDEMPOTENCY_PREFIX):
        return _CARRIER_ABSENT
    if idempotency_key.removeprefix(_AGENT_START_V2_IDEMPOTENCY_PREFIX).strip():
        return _CARRIER_EXACT
    return _CARRIER_CORRUPT


def _owner_bound_occurrence_carrier_state(
    action: Mapping[str, Any],
    *,
    action_metadata: Mapping[str, Any],
    start_candidate: bool,
) -> str:
    """Tri-state the owner-bound occurrence reference carrier.

    A well-formed ``result_occurrence_ref`` is start-v2 provenance only when it
    is bound to the start owner: either the action carries the matching
    ``agent-start-v2:<logical_occurrence_digest>`` idempotency pair, or the
    action is itself a start candidate.  Stray occurrence-shaped metadata on
    non-start actions remains ignored so it cannot reserve legacy actions; on a
    start candidate, or paired with an exact ``agent-start-v2:`` idempotency
    identity, a present-but-malformed occurrence reference is corrupt instead.
    """

    if "result_occurrence_ref" not in action_metadata or action_metadata.get("result_occurrence_ref") is None:
        return _CARRIER_ABSENT
    occurrence_ref = action_metadata.get("result_occurrence_ref")
    if _exact_result_occurrence_ref(occurrence_ref):
        logical_occurrence_digest = str(dict(occurrence_ref)["logical_occurrence_digest"])
        owner_bound = bool(
            str(action.get("idempotency_key") or "") == f"agent-start-v2:{logical_occurrence_digest}" or start_candidate
        )
        return _CARRIER_EXACT if owner_bound else _CARRIER_ABSENT
    if start_candidate or _start_v2_idempotency_carrier_state(action) == _CARRIER_EXACT:
        return _CARRIER_CORRUPT
    return _CARRIER_ABSENT


def _start_workflow_reference_carrier_state(
    operation: Mapping[str, Any],
    *,
    workflow_ref: Mapping[str, Any],
) -> str:
    """Tri-state the Operation start workflow reference carrier.

    The legacy schema-defined start path persists the same
    ``acquisition.run.create`` workflow reference, so a complete reference alone
    is ambiguous: it is start-v2 provenance only when the Operation cannot be
    explained as a coherent legacy start, i.e. when it does not carry the exact
    legacy request pin pair.  A split reference (exactly one of the
    command-type/owner pair survives) or an incomplete one (the pair survives
    without both identifiers) can never be legacy-coherent and is corrupt.
    """

    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    command_type_matches = str(workflow_ref.get("command_type") or "") == str(spec.default_workflow_command_type or "")
    owner_matches = str(workflow_ref.get("owner") or "") == str(spec.owner_module or "")
    if command_type_matches != owner_matches:
        return _CARRIER_CORRUPT
    if not command_type_matches:
        return _CARRIER_ABSENT
    if not str(workflow_ref.get("workflow_run_id") or "") or not str(workflow_ref.get("command_id") or ""):
        return _CARRIER_CORRUPT
    legacy_version, legacy_digest = _legacy_start_request_pin_pair()
    legacy_coherent = (
        str(operation.get("request_schema_version") or "") == legacy_version
        and str(operation.get("request_schema_digest") or "") == legacy_digest
    )
    return _CARRIER_ABSENT if legacy_coherent else _CARRIER_EXACT


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

    Every start carrier is tri-stated as ``absent`` / ``exact`` / ``corrupt``
    instead of collapsing unrecognized content to absent: any present-but-
    malformed start carrier on a start candidate, any start-specific
    schema/prefix family member (a drifted start-acceptance owner-ref schema or
    a blank-suffix ``agent-start-v2:`` idempotency key), any conflicting
    decoded/raw JSON carrier, any malformed owner-bound occurrence reference,
    and any split or incomplete start workflow reference all force
    ``partial_or_mixed_v2`` and can never silently classify as ``non_v2``.

    Exact provenance markers are intentionally independent so that a compound
    downgrade cannot erase them together: any exact current request/tool/result
    pin, a start-snapshot marker, preview input keys, the persisted acceptance
    owner-result-ref schema in Action/Operation result refs, the owner-bound
    ``agent-start-v2:`` idempotency identity, the owner-bound occurrence
    reference, the Operation start workflow reference that no coherent legacy
    start can explain, and any nonempty drifted pin on a start candidate.
    """

    action_type = str(action.get("action_type") or "").strip()
    start_candidate = action_type == ACTION_START_ACQUISITION_RUN
    operation = dict(operation_run or {})
    action_input_state, action_input = _json_carrier_state(action, "input", "input_json")
    action_target_state, action_target = _json_carrier_state(action, "target_ref", "target_ref_json")
    action_metadata_state, action_metadata = _json_carrier_state(action, "metadata", "metadata_json")
    action_result_ref_state, action_result_ref = _json_carrier_state(action, "result_ref", "result_ref_json")
    operation_result_ref_state, operation_result_ref = _json_carrier_state(operation, "result_ref", "result_ref_json")
    operation_workflow_ref_state, operation_workflow_ref = _json_carrier_state(
        operation, "workflow_ref", "workflow_ref_json"
    )
    json_carrier_states = (
        action_input_state,
        action_target_state,
        action_metadata_state,
        action_result_ref_state,
        operation_result_ref_state,
        operation_workflow_ref_state,
    )
    corrupt_json_carrier = start_candidate and any(
        state in {_JSON_CARRIER_MALFORMED, _JSON_CARRIER_CONFLICT} for state in json_carrier_states
    )
    carrier_states = (
        _acceptance_owner_ref_carrier_state(action_result_ref),
        _acceptance_owner_ref_carrier_state(operation_result_ref) if operation else _CARRIER_ABSENT,
        _start_v2_idempotency_carrier_state(action),
        _start_v2_idempotency_carrier_state(operation) if operation else _CARRIER_ABSENT,
        _owner_bound_occurrence_carrier_state(
            action,
            action_metadata=action_metadata,
            start_candidate=start_candidate,
        ),
        _start_workflow_reference_carrier_state(operation, workflow_ref=operation_workflow_ref)
        if operation
        else _CARRIER_ABSENT,
    )
    if corrupt_json_carrier or _CARRIER_CORRUPT in carrier_states:
        return "partial_or_mixed_v2"

    action_v2_input_keys = {"preview_id", "preview_revision", "preview_digest"}
    action_input_key_set = {str(key) for key in action_input}
    action_has_any_v2_input_key = start_candidate and bool(action_v2_input_keys & action_input_key_set)
    action_has_any_current_v2_pin = _has_any_current_start_v2_contract_pin(action)
    operation_has_any_current_v2_pin = bool(operation) and _has_any_current_start_v2_contract_pin(operation)
    start_snapshot = action_target.get("start_snapshot")
    start_snapshot_mapping = dict(start_snapshot) if isinstance(start_snapshot, Mapping) else {}
    action_has_v2_start_snapshot = bool(start_snapshot_mapping) and bool(
        start_snapshot_mapping.get("schema_version") or start_snapshot_mapping.get("snapshot_digest")
    )
    action_has_v2_provenance = (
        action_has_any_current_v2_pin
        or action_has_v2_start_snapshot
        or action_has_any_v2_input_key
        or _acceptance_owner_ref_carrier_state(action_result_ref) == _CARRIER_EXACT
        or _start_v2_idempotency_carrier_state(action) == _CARRIER_EXACT
        or _owner_bound_occurrence_carrier_state(
            action,
            action_metadata=action_metadata,
            start_candidate=start_candidate,
        )
        == _CARRIER_EXACT
        or (start_candidate and _has_drifted_start_v2_pin_on_start_candidate(action))
    )
    operation_has_v2_provenance = bool(operation) and (
        operation_has_any_current_v2_pin
        or _acceptance_owner_ref_carrier_state(operation_result_ref) == _CARRIER_EXACT
        or _start_v2_idempotency_carrier_state(operation) == _CARRIER_EXACT
        or _start_workflow_reference_carrier_state(operation, workflow_ref=operation_workflow_ref) == _CARRIER_EXACT
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
