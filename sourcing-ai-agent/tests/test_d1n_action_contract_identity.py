from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_REQUEST_TOOL_SPEC,
)
from sourcing_agent.action_contract_identity import (
    ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION,
    ActionContractIdentityError,
    action_contract_digest,
    action_contract_fingerprint_digest,
    build_action_contract_fingerprint,
    build_action_contract_pin,
    production_action_contract_manifest,
)
from sourcing_agent.operation_runtime import (
    ACTION_START_ACQUISITION_RUN,
    DEFAULT_ACTION_REGISTRY,
)


def _start_v2_spec():
    base = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    return replace(
        base,
        request_schema=ACQUISITION_START_V2_REQUEST_TOOL_SPEC.input_schema,
        request_schema_version=ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
        request_identity_target_fields=("workspace_id", "requester_id", "start_snapshot"),
        target_ref_field_aliases=(),
    )


def test_complete_manifest_fingerprints_all_15_actions_and_keeps_schema_less_debt_visible() -> None:
    manifest = production_action_contract_manifest()

    assert manifest["action_count"] == 15
    assert manifest["schema_defined_count"] == 10
    assert manifest["schema_less_count"] == 5
    assert len(manifest["manifest_digest"]) == 64
    actions = list(manifest["actions"])
    assert [row["action_type"] for row in actions] == sorted(
        DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    )
    assert len({row["action_contract_digest"] for row in actions}) == 15
    assert {row["action_type"] for row in actions if row["request_status"] == "schema_less"} == {
        "continue_acquisition_run",
        "external_intake",
        "fetch_profile_sample",
        "plan_acquisition",
        "promote_person_assertion",
    }


def test_successor_request_version_changes_only_request_identity_and_gets_a_distinct_digest() -> None:
    base = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)
    successor = _start_v2_spec()
    base_record = build_action_contract_fingerprint(DEFAULT_ACTION_REGISTRY, base)
    successor_record = build_action_contract_fingerprint(DEFAULT_ACTION_REGISTRY, successor)

    assert successor_record["schema_version"] == ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION
    assert successor_record["request"] == {
        "status": "schema_defined",
        "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
        "schema_digest": successor.request_schema_digest,
        "identity_target_fields": ["workspace_id", "requester_id", "start_snapshot"],
        "target_ref_field_aliases": [],
    }
    assert successor_record["allowed_workflow_command_contracts"] == base_record["allowed_workflow_command_contracts"]
    assert successor_record["display_contract"] == base_record["display_contract"]
    assert action_contract_digest(DEFAULT_ACTION_REGISTRY, successor) != action_contract_digest(
        DEFAULT_ACTION_REGISTRY, base
    )


def test_action_contract_pin_uses_exact_schema_and_full_contract_digest() -> None:
    successor = _start_v2_spec()
    pin = build_action_contract_pin(DEFAULT_ACTION_REGISTRY, successor)

    assert pin.action_type == ACTION_START_ACQUISITION_RUN
    assert pin.request_schema_version == ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
    assert pin.request_schema_digest == successor.request_schema_digest
    assert pin.action_contract_digest == action_contract_digest(DEFAULT_ACTION_REGISTRY, successor)

    schema_less = DEFAULT_ACTION_REGISTRY.spec_for("plan_acquisition")
    with pytest.raises(ActionContractIdentityError, match="request_schema_missing"):
        build_action_contract_pin(DEFAULT_ACTION_REGISTRY, schema_less)


def test_any_fingerprinted_command_display_or_request_drift_changes_the_digest() -> None:
    record = build_action_contract_fingerprint(DEFAULT_ACTION_REGISTRY, _start_v2_spec())
    baseline = action_contract_fingerprint_digest(record)
    mutations = [
        lambda candidate: candidate["request"].update({"schema_digest": "f" * 64}),
        lambda candidate: candidate["display_contract"].update({"display_label": "Changed"}),
        lambda candidate: candidate["allowed_workflow_command_contracts"][0].update({"owner": "forged_owner"}),
        lambda candidate: candidate["workflow_command_control_summary"].update({"fallback_status": "allow"}),
    ]
    for mutate in mutations:
        candidate = copy.deepcopy(record)
        mutate(candidate)
        assert action_contract_fingerprint_digest(candidate) != baseline


def test_versioned_spec_cannot_smuggle_non_request_base_semantic_drift() -> None:
    successor = _start_v2_spec()
    for field_name, value in (
        ("owner_module", "other_owner"),
        ("dispatch_adapter", "projection_read"),
        ("approval_policy", "not_required"),
        ("budget_required", False),
        ("display_label", "Changed"),
        ("allowed_workflow_command_types", ()),
    ):
        with pytest.raises(ActionContractIdentityError, match="base_semantics_mismatch"):
            build_action_contract_fingerprint(
                DEFAULT_ACTION_REGISTRY,
                replace(successor, **{field_name: value}),
            )


def test_manifest_and_records_are_defensive_across_calls() -> None:
    first = production_action_contract_manifest()
    second = production_action_contract_manifest()
    assert first == second
    with pytest.raises(TypeError):
        first["action_count"] = 0  # type: ignore[index]
