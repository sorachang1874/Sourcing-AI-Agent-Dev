from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from typing import cast

import pytest

from sourcing_agent.agent_contract_activation import (
    AGENT_CONTRACT_HISTORY_MAX_ENTRIES,
    POSTGRES_BIGINT_MAX,
    ActionContractPin,
    ActionRequestReleaseState,
    ActionResultContractPin,
    ActivationEntryKind,
    AgentContractActivationError,
    AgentContractActivationPolicyPin,
    AgentContractActivationSnapshot,
    AgentContractHostedGateDecisionReceipt,
    AgentContractOwnerPin,
    AgentContractRequiredPinSet,
    AgentContractReviewEvidence,
    AgentContractReviewVerificationReceipt,
    AgentContractSchemaBridgeGateDecisionReceipt,
    AgentToolContractPin,
    AgentToolReleaseState,
    required_pin_set_from_iterables,
    validate_activation_transition,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _request(
    action_type: str = "plan_acquisition",
    *,
    version: str = "plan_acquisition_request_v1",
    digest_label: str | None = None,
    contract_label: str | None = None,
) -> ActionContractPin:
    return ActionContractPin(
        action_type=action_type,
        request_schema_version=version,
        request_schema_digest=_digest(digest_label or version),
        action_contract_digest=_digest(contract_label or f"{action_type}-contract-{version}"),
    )


def _result(
    tool_name: str = "plan_acquisition",
    *,
    tool_kind: str = "action",
    action_type: str | None = None,
    query_owner: AgentContractOwnerPin | None = None,
    version: str = "plan_acquisition_result_v1",
    digest_label: str | None = None,
    serializer_label: str | None = None,
) -> ActionResultContractPin:
    return ActionResultContractPin(
        tool_name=tool_name,
        tool_kind=cast(ActivationEntryKind, tool_kind),
        action_type=(action_type or tool_name) if tool_kind == "action" else None,
        query_owner=query_owner,
        result_schema_version=version,
        result_schema_digest=_digest(digest_label or version),
        serializer_owner=f"agent.result.{tool_name}_serializer",
        serializer_revision=f"{tool_name}_serializer_v1",
        serializer_contract_digest=_digest(serializer_label or f"{tool_name}-serializer-v1"),
    )


def _tool(
    tool_name: str = "plan_acquisition",
    *,
    tool_kind: str = "action",
    action_type: str | None = None,
    query_owner: AgentContractOwnerPin | None = None,
    version: str = "plan_acquisition_tool_v1",
    digest_label: str | None = None,
) -> AgentToolContractPin:
    return AgentToolContractPin(
        tool_name=tool_name,
        tool_kind=cast(ActivationEntryKind, tool_kind),
        action_type=(action_type or tool_name) if tool_kind == "action" else None,
        query_owner=query_owner,
        tool_spec_version=version,
        tool_spec_digest=_digest(digest_label or version),
    )


def _owner(label: str, *, revision: str | None = None) -> AgentContractOwnerPin:
    return AgentContractOwnerPin(
        owner=f"agent.contract.{label}",
        revision=revision or f"{label}_v1",
        contract_digest=_digest(f"{label}-contract-v1"),
    )


def _policy(**overrides: object) -> AgentContractActivationPolicyPin:
    values: dict[str, object] = {
        "policy_owner": _owner("activation_policy"),
        "release_commit": _digest("release-commit"),
        "runtime_namespace": "hosted.primary",
        "allowed_provider_modes": ("simulate", "scripted"),
        "workspace_allowlist_digest": _digest("workspace-allowlist"),
        "requester_allowlist_digest": _digest("requester-allowlist"),
        "production_action_roster_digest": _digest("production-action-roster"),
        "production_action_count": 15,
        "review_verifier": _owner("review_verifier"),
        "hosted_gate": _owner("hosted_gate"),
        "schema_bridge_gate": _owner("schema_bridge_gate"),
    }
    values.update(overrides)
    return AgentContractActivationPolicyPin(**values)  # type: ignore[arg-type]


def _review(
    label: str,
    *,
    scope_digest: str | None = None,
    reviewed_commit: str | None = None,
    verifier_contract_digest: str | None = None,
    reviewed_at: str = "2026-07-17T03:00:00Z",
) -> AgentContractReviewEvidence:
    return AgentContractReviewEvidence(
        reviewed_commit=reviewed_commit or _digest("release-commit"),
        review_artifact_id=f"review.{label}",
        review_artifact_digest=_digest(f"{label}-artifact"),
        review_scope_digest=scope_digest or _digest(f"{label}-scope"),
        verifier_contract_digest=verifier_contract_digest or _digest("review_verifier-contract-v1"),
        reviewed_at=reviewed_at,
    )


def _receipt(
    label: str,
    *,
    activation_key: str,
    required_pins: AgentContractRequiredPinSet,
    policy: AgentContractActivationPolicyPin,
    evidence: AgentContractReviewEvidence | None = None,
    verified_at: str = "2026-07-17T03:01:00Z",
) -> AgentContractReviewVerificationReceipt:
    review_evidence = evidence or _review(
        label,
        reviewed_commit=policy.release_commit,
        verifier_contract_digest=policy.review_verifier.contract_digest,
    )
    return AgentContractReviewVerificationReceipt(
        activation_key=activation_key,
        required_pin_set_digest=required_pins.required_pin_set_digest,
        activation_policy_digest=policy.policy_digest,
        review_evidence=review_evidence,
        verifier=policy.review_verifier,
        verification_artifact_id=f"verification.{label}",
        verification_artifact_digest=_digest(f"{label}-verification"),
        verified_at=verified_at,
    )


def _hosted_gate_receipt(
    label: str,
    *,
    activation_key: str,
    required_pins: AgentContractRequiredPinSet,
    policy: AgentContractActivationPolicyPin,
    request_receipt: AgentContractReviewVerificationReceipt | None,
    tool_receipt: AgentContractReviewVerificationReceipt,
    schema_bridge_receipt: AgentContractSchemaBridgeGateDecisionReceipt,
    decision_epoch: int,
    decided_at: str = "2026-07-17T09:59:00Z",
) -> AgentContractHostedGateDecisionReceipt:
    return AgentContractHostedGateDecisionReceipt(
        activation_key=activation_key,
        required_pin_set_digest=required_pins.required_pin_set_digest,
        activation_policy_digest=policy.policy_digest,
        hosted_gate=policy.hosted_gate,
        schema_bridge_gate_receipt_digest=schema_bridge_receipt.receipt_digest,
        request_review_receipt_digest=(None if request_receipt is None else request_receipt.receipt_digest),
        tool_review_receipt_digest=tool_receipt.receipt_digest,
        decision_epoch=decision_epoch,
        decision_artifact_id=f"hosted_gate.{label}",
        decision_artifact_digest=_digest(f"{label}-hosted-gate"),
        decided_at=decided_at,
    )


def _schema_bridge_gate_receipt(
    label: str,
    *,
    policy: AgentContractActivationPolicyPin,
    decided_at: str = "2026-07-17T09:58:00Z",
    **overrides: object,
) -> AgentContractSchemaBridgeGateDecisionReceipt:
    values: dict[str, object] = {
        "activation_policy_digest": policy.policy_digest,
        "schema_bridge_gate": policy.schema_bridge_gate,
        "release_commit": policy.release_commit,
        "production_action_roster_digest": policy.production_action_roster_digest,
        "production_action_count": policy.production_action_count,
        "schema_defined_action_count": policy.production_action_count,
        "schema_less_action_count": 0,
        "compatibility_hit_count": 0,
        "observation_epoch": 1,
        "observation_started_at": "2026-07-16T00:00:00Z",
        "observed_through": "2026-07-17T09:57:00Z",
        "constraints_validated": True,
        "constraint_validation_artifact_id": f"constraint_validation.{label}",
        "constraint_validation_artifact_digest": _digest(f"{label}-constraint-validation"),
        "decision_artifact_id": f"schema_bridge_gate.{label}",
        "decision_artifact_digest": _digest(f"{label}-schema-bridge-gate"),
        "decided_at": decided_at,
    }
    values.update(overrides)
    return AgentContractSchemaBridgeGateDecisionReceipt(**values)  # type: ignore[arg-type]


def _pin_set(
    activation_key: str = "action:plan_acquisition",
    *,
    query_owner: AgentContractOwnerPin | None = None,
    requests: tuple[ActionContractPin, ...] | None = None,
    results: tuple[ActionResultContractPin, ...] | None = None,
    tools: tuple[AgentToolContractPin, ...] | None = None,
) -> AgentContractRequiredPinSet:
    entry_kind, _, entry_identity = activation_key.partition(":")
    route_owner = query_owner or (_owner("inspect_operation_query") if entry_kind == "query" else None)
    return AgentContractRequiredPinSet(
        activation_key=activation_key,
        requests=(_request(entry_identity),) if requests is None and entry_kind == "action" else (requests or ()),
        results=(
            _result(
                entry_identity,
                tool_kind=entry_kind,
                action_type=entry_identity if entry_kind == "action" else None,
                query_owner=route_owner,
                version=f"{entry_identity}_result_v1",
            ),
        )
        if results is None
        else results,
        tools=(
            _tool(
                entry_identity,
                tool_kind=entry_kind,
                action_type=entry_identity if entry_kind == "action" else None,
                query_owner=route_owner,
                version=f"{entry_identity}_tool_v1",
            ),
        )
        if tools is None
        else tools,
    )


def _snapshot(**overrides: object) -> AgentContractActivationSnapshot:
    entry_kind = overrides.get("entry_kind", "action")
    action_type = overrides.get("action_type", "plan_acquisition" if entry_kind == "action" else None)
    tool_name = overrides.get("tool_name", "plan_acquisition" if entry_kind == "action" else "inspect_operation")
    query_owner = overrides.get(
        "query_owner",
        None if entry_kind == "action" else _owner("inspect_operation_query"),
    )
    activation_key = overrides.get(
        "activation_key",
        f"action:{action_type}" if entry_kind == "action" else f"query:{tool_name}",
    )
    pin_action_type = action_type if isinstance(action_type, str) else cast(str, activation_key).partition(":")[2]
    request = _request(action_type) if entry_kind == "action" and isinstance(action_type, str) else None
    result = _result(
        cast(str, tool_name),
        tool_kind=cast(str, entry_kind),
        action_type=pin_action_type if entry_kind == "action" else None,
        query_owner=cast(AgentContractOwnerPin | None, query_owner),
        version=f"{tool_name}_result_v1",
    )
    tool = _tool(
        cast(str, tool_name),
        tool_kind=cast(str, entry_kind),
        action_type=pin_action_type if entry_kind == "action" else None,
        query_owner=cast(AgentContractOwnerPin | None, query_owner),
        version=f"{tool_name}_tool_v1",
    )
    default_required_pins = AgentContractRequiredPinSet(
        activation_key=cast(str, activation_key),
        requests=() if request is None else (request,),
        results=(result,),
        tools=(tool,),
    )
    required_pins = cast(
        AgentContractRequiredPinSet,
        overrides.get("required_pins", default_required_pins),
    )
    policy = cast(AgentContractActivationPolicyPin, overrides.get("activation_policy", _policy()))
    values: dict[str, object] = {
        "activation_key": activation_key,
        "entry_kind": entry_kind,
        "action_type": action_type,
        "tool_name": tool_name,
        "query_owner": query_owner,
        "request_state": "disabled" if entry_kind == "action" else "not_applicable",
        "tool_state": "disabled",
        "current_request": request,
        "current_result": result,
        "current_tool": tool,
        "required_pins": required_pins,
        "activation_policy": policy,
        "request_review_receipt": (
            _receipt(
                "request",
                activation_key=cast(str, activation_key),
                required_pins=required_pins,
                policy=policy,
            )
            if request is not None
            else None
        ),
        "tool_review_receipt": _receipt(
            "tool",
            activation_key=cast(str, activation_key),
            required_pins=required_pins,
            policy=policy,
        ),
        "schema_bridge_gate_decision_receipt": None,
        "hosted_gate_decision_receipt": None,
        "request_activated_at": "2026-07-17T09:00:00Z" if overrides.get("request_state") == "current" else None,
        "request_expires_at": "2026-07-17T12:00:00Z" if overrides.get("request_state") == "current" else None,
        "tool_activated_at": "2026-07-17T10:00:00Z" if overrides.get("tool_state") == "hosted" else None,
        "tool_expires_at": "2026-07-17T11:00:00Z" if overrides.get("tool_state") == "hosted" else None,
        "activation_epoch": 0,
        "update_revision": 0,
        "last_transition_actor": "operator.test",
        "last_transition_reason": "test fixture",
    }
    values.update(overrides)
    if values["tool_state"] == "hosted" and "schema_bridge_gate_decision_receipt" not in overrides:
        values["schema_bridge_gate_decision_receipt"] = _schema_bridge_gate_receipt(
            "snapshot",
            policy=policy,
        )
    if values["tool_state"] == "hosted" and "hosted_gate_decision_receipt" not in overrides:
        request_receipt = cast(AgentContractReviewVerificationReceipt | None, values["request_review_receipt"])
        tool_receipt = cast(AgentContractReviewVerificationReceipt, values["tool_review_receipt"])
        schema_bridge_receipt = cast(
            AgentContractSchemaBridgeGateDecisionReceipt,
            values["schema_bridge_gate_decision_receipt"],
        )
        values["hosted_gate_decision_receipt"] = _hosted_gate_receipt(
            "snapshot",
            activation_key=cast(str, activation_key),
            required_pins=required_pins,
            policy=policy,
            request_receipt=request_receipt,
            tool_receipt=tool_receipt,
            schema_bridge_receipt=schema_bridge_receipt,
            decision_epoch=cast(int, values["activation_epoch"]),
        )
    return AgentContractActivationSnapshot(**values)  # type: ignore[arg-type]


def _transition(
    before: AgentContractActivationSnapshot,
    **changes: object,
) -> AgentContractActivationSnapshot:
    if changes.get("request_state") == "current":
        changes.setdefault("request_activated_at", "2026-07-17T09:00:00Z")
        changes.setdefault("request_expires_at", "2026-07-17T12:00:00Z")
    if changes.get("tool_state") == "hosted":
        changes.setdefault("tool_activated_at", "2026-07-17T10:00:00Z")
        changes.setdefault("tool_expires_at", "2026-07-17T11:00:00Z")
        assert before.tool_review_receipt is not None
        schema_bridge_receipt = cast(
            AgentContractSchemaBridgeGateDecisionReceipt,
            changes.setdefault(
                "schema_bridge_gate_decision_receipt",
                _schema_bridge_gate_receipt("transition", policy=before.activation_policy),
            ),
        )
        changes.setdefault(
            "hosted_gate_decision_receipt",
            _hosted_gate_receipt(
                "transition",
                activation_key=before.activation_key,
                required_pins=before.required_pins,
                policy=before.activation_policy,
                request_receipt=before.request_review_receipt,
                tool_receipt=before.tool_review_receipt,
                schema_bridge_receipt=schema_bridge_receipt,
                decision_epoch=before.activation_epoch + 1,
            ),
        )
    after = replace(
        before,
        activation_epoch=before.activation_epoch + 1,
        update_revision=before.update_revision + 1,
        last_transition_reason="test transition",
        **changes,  # type: ignore[arg-type]
    )
    return validate_activation_transition(
        before,
        after,
        expected_revision=before.update_revision,
        expected_snapshot_digest=before.snapshot_digest,
    )


def test_required_pin_set_canonicalizes_all_pin_kinds_and_has_order_independent_digest() -> None:
    requests = (
        _request(version="plan_acquisition_request_v2"),
        _request("plan_acquisition"),
    )
    results = (
        _result(version="plan_acquisition_result_v2"),
        _result("plan_acquisition"),
    )
    tools = (
        _tool(version="plan_acquisition_tool_v2"),
        _tool("plan_acquisition"),
    )

    reversed_set = required_pin_set_from_iterables(
        activation_key="action:plan_acquisition",
        requests=reversed(requests),
        results=reversed(results),
        tools=reversed(tools),
    )
    ordered_set = required_pin_set_from_iterables(
        activation_key="action:plan_acquisition",
        requests=requests,
        results=results,
        tools=tools,
    )

    assert reversed_set.requests == ordered_set.requests
    assert reversed_set.results == ordered_set.results
    assert reversed_set.tools == ordered_set.tools
    assert reversed_set.to_record() == ordered_set.to_record()
    expected_digest = hashlib.sha256(
        json.dumps(
            ordered_set.to_record(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert reversed_set.required_pin_set_digest == expected_digest
    assert ordered_set.required_pin_set_digest == expected_digest


@pytest.mark.parametrize("field_name", ["requests", "results", "tools"])
def test_required_pin_set_iterable_builder_rejects_mappings(field_name: str) -> None:
    pin = {"requests": _request(), "results": _result(), "tools": _tool()}[field_name]
    kwargs = {field_name: {pin: "collapsed-by-key"}}

    with pytest.raises(AgentContractActivationError, match="agent_contract_required_pin_iterable_invalid"):
        required_pin_set_from_iterables(
            activation_key="action:plan_acquisition",
            **kwargs,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("field_name", "pins", "error"),
    [
        ("requests", (_request(), _request()), "agent_contract_required_request_duplicate"),
        ("results", (_result(), _result()), "agent_contract_required_result_duplicate"),
        ("tools", (_tool(), _tool()), "agent_contract_required_tool_duplicate"),
        (
            "requests",
            (_request(), _request(digest_label="drifted-request")),
            "agent_contract_required_request_version_drift",
        ),
        (
            "results",
            (_result(), _result(serializer_label="drifted-serializer")),
            "agent_contract_required_result_version_drift",
        ),
        (
            "tools",
            (_tool(), _tool(digest_label="drifted-tool")),
            "agent_contract_required_tool_version_drift",
        ),
    ],
)
def test_required_pin_set_rejects_duplicate_and_same_version_drift(
    field_name: str,
    pins: tuple[object, ...],
    error: str,
) -> None:
    kwargs = {field_name: pins}

    with pytest.raises(AgentContractActivationError, match=error):
        _pin_set(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("factory", "error"),
    [
        (
            lambda: _pin_set(requests=(_request("review_candidates"),)),
            "agent_contract_request_history_scope_mismatch",
        ),
        (
            lambda: _pin_set(
                results=(_result(), _result("review_candidates")),
                tools=(_tool(),),
            ),
            "agent_contract_tool_history_scope_mismatch",
        ),
        (
            lambda: _pin_set(
                activation_key="query:inspect_operation",
                requests=(_request(),),
                results=(_result("inspect_operation"),),
                tools=(_tool("inspect_operation"),),
            ),
            "agent_contract_query_request_history_forbidden",
        ),
        (
            lambda: _pin_set(
                activation_key="query:inspect_operation",
                requests=(),
                results=(_result("review_candidates"),),
                tools=(_tool("review_candidates"),),
            ),
            "agent_contract_tool_history_scope_mismatch",
        ),
    ],
)
def test_required_pin_histories_are_scoped_to_one_activation_identity(
    factory: Callable[[], object], error: str
) -> None:
    with pytest.raises(AgentContractActivationError, match=error):
        factory()


def test_required_pin_set_rejects_same_name_foreign_action_route() -> None:
    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_history_route_mismatch"):
        _pin_set(
            results=(_result(action_type="review_candidates"),),
            tools=(_tool(action_type="review_candidates"),),
        )


def test_required_pin_histories_and_record_bytes_are_bounded() -> None:
    requests = tuple(_request(version=f"{index}request") for index in range(AGENT_CONTRACT_HISTORY_MAX_ENTRIES + 1))
    with pytest.raises(AgentContractActivationError, match="agent_contract_required_requests_invalid"):
        _pin_set(requests=requests)

    def long_owner_version(index: int) -> str:
        prefix = f"{index}v"
        return f"{prefix}{'x' * (128 - len(prefix))}"

    def long_result_version(index: int) -> str:
        suffix = f"_v{index + 1}"
        return f"r{'x' * (128 - len(suffix) - 1)}{suffix}"

    def long_owner(index: int) -> str:
        prefix = f"o{index}."
        return f"{prefix}{'x' * (256 - len(prefix))}"

    max_requests = tuple(
        _request(version=long_owner_version(index)) for index in range(AGENT_CONTRACT_HISTORY_MAX_ENTRIES)
    )
    max_results = tuple(
        ActionResultContractPin(
            tool_name="plan_acquisition",
            tool_kind="action",
            action_type="plan_acquisition",
            query_owner=None,
            result_schema_version=long_result_version(index),
            result_schema_digest=_digest(f"result-{index}"),
            serializer_owner=long_owner(index),
            serializer_revision=long_result_version(index),
            serializer_contract_digest=_digest(f"serializer-{index}"),
        )
        for index in range(AGENT_CONTRACT_HISTORY_MAX_ENTRIES)
    )
    max_tools = tuple(
        _tool(version=long_owner_version(index), digest_label=f"tool-{index}")
        for index in range(AGENT_CONTRACT_HISTORY_MAX_ENTRIES)
    )
    with pytest.raises(AgentContractActivationError, match="agent_contract_required_pin_set_too_large"):
        _pin_set(requests=max_requests, results=max_results, tools=max_tools)


def test_owner_specific_version_grammars_retain_valid_request_and_tool_history() -> None:
    pins = _pin_set(
        requests=(_request(version="1.request"),),
        results=(_result(version="plan_acquisition_result_v2"),),
        tools=(_tool(version="3.tool"),),
    )
    assert pins.requests[0].request_schema_version == "1.request"
    assert pins.results[0].result_schema_version == "plan_acquisition_result_v2"
    assert pins.tools[0].tool_spec_version == "3.tool"

    with pytest.raises(AgentContractActivationError, match="agent_contract_result_schema_version_invalid"):
        _result(version="2.result")
    with pytest.raises(AgentContractActivationError, match="agent_contract_serializer_revision_invalid"):
        replace(_result(), serializer_revision="2.serializer")


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"activation_key": "action:other"}, "agent_contract_request_history_scope_mismatch"),
        ({"action_type": None}, "agent_contract_action_identity_invalid"),
        (
            {
                "entry_kind": "query",
                "activation_key": "query:inspect_operation",
                "action_type": "plan_acquisition",
                "tool_name": "inspect_operation",
            },
            "agent_contract_query_identity_invalid",
        ),
        (
            {
                "entry_kind": "query",
                "activation_key": "query:inspect_operation",
                "action_type": None,
                "tool_name": "inspect_operation",
                "request_state": "disabled",
            },
            "agent_contract_query_request_state_invalid",
        ),
    ],
)
def test_snapshot_rejects_action_and_query_identity_drift(overrides: dict[str, object], error: str) -> None:
    with pytest.raises(AgentContractActivationError, match=error):
        _snapshot(**overrides)


@pytest.mark.parametrize(
    ("field_name", "state"),
    [
        ("request_state", cast(ActionRequestReleaseState, "local_canary")),
        ("tool_state", cast(AgentToolReleaseState, "local_canary")),
    ],
)
def test_production_release_states_reject_local_canary(field_name: str, state: object) -> None:
    with pytest.raises(AgentContractActivationError, match=f"agent_contract_{field_name}_invalid"):
        _snapshot(**{field_name: state})


def test_activation_policy_canonicalizes_modes_and_pins_all_hosted_gate_owners() -> None:
    policy = _policy(allowed_provider_modes=("scripted", "simulate"))
    assert policy.allowed_provider_modes == ("scripted", "simulate")
    assert policy.release_commit == _digest("release-commit")
    assert policy.review_verifier.owner == "agent.contract.review_verifier"
    assert policy.hosted_gate.owner == "agent.contract.hosted_gate"
    assert policy.schema_bridge_gate.owner == "agent.contract.schema_bridge_gate"
    assert policy.workspace_allowlist_digest == _digest("workspace-allowlist")
    assert policy.requester_allowlist_digest == _digest("requester-allowlist")
    assert policy.production_action_count == 15
    assert policy.production_action_roster_digest == _digest("production-action-roster")

    for modes in ((), ("live", "live"), ("unknown",)):
        with pytest.raises(AgentContractActivationError, match="agent_contract_allowed_provider_modes_invalid"):
            _policy(allowed_provider_modes=modes)


@pytest.mark.parametrize(
    ("overrides", "error"),
    (
        ({"schema_defined_action_count": 14, "schema_less_action_count": 1}, "population_incomplete"),
        ({"compatibility_hit_count": 1}, "population_incomplete"),
        ({"constraints_validated": False}, "constraints_not_validated"),
        ({"observation_epoch": 0}, "observation_epoch_invalid"),
        (
            {
                "observation_started_at": "2026-07-17T09:57:00Z",
                "observed_through": "2026-07-17T09:57:00Z",
            },
            "observation_window_invalid",
        ),
    ),
)
def test_schema_bridge_gate_receipt_requires_complete_zero_hit_population_and_validated_constraints(
    overrides: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(AgentContractActivationError, match=error):
        _schema_bridge_gate_receipt("invalid", policy=_policy(), **overrides)


def test_request_and_shadow_tool_activation_do_not_pretend_schema_bridge_gate_is_complete() -> None:
    snapshot = _snapshot(request_state="current", tool_state="shadow")

    assert snapshot.schema_bridge_gate_decision_receipt is None
    assert snapshot.hosted_gate_decision_receipt is None


def test_hosted_tool_requires_exact_policy_bound_schema_bridge_gate_receipt() -> None:
    hosted = _snapshot(request_state="current", tool_state="hosted", activation_epoch=4, update_revision=4)
    assert hosted.schema_bridge_gate_decision_receipt is not None
    assert hosted.hosted_gate_decision_receipt is not None

    with pytest.raises(AgentContractActivationError, match="hosted_gate_decision_mismatch"):
        replace(hosted, schema_bridge_gate_decision_receipt=None)

    foreign_roster = replace(
        hosted.schema_bridge_gate_decision_receipt,
        production_action_roster_digest=_digest("foreign-production-action-roster"),
    )
    with pytest.raises(AgentContractActivationError, match="schema_bridge_gate_receipt_mismatch"):
        replace(hosted, schema_bridge_gate_decision_receipt=foreign_roster)

    wrong_bound_gate = replace(
        hosted.hosted_gate_decision_receipt,
        schema_bridge_gate_receipt_digest=_digest("wrong-schema-bridge-receipt"),
    )
    with pytest.raises(AgentContractActivationError, match="hosted_gate_decision_mismatch"):
        replace(hosted, hosted_gate_decision_receipt=wrong_bound_gate)


def test_schema_bridge_gate_must_precede_hosted_gate_and_tool_activation() -> None:
    hosted = _snapshot(request_state="current", tool_state="hosted", activation_epoch=4, update_revision=4)
    assert hosted.schema_bridge_gate_decision_receipt is not None
    assert hosted.hosted_gate_decision_receipt is not None
    late_schema_bridge = replace(
        hosted.schema_bridge_gate_decision_receipt,
        observed_through="2026-07-17T10:01:00Z",
        decided_at="2026-07-17T10:01:00Z",
    )
    rebound_hosted_gate = replace(
        hosted.hosted_gate_decision_receipt,
        schema_bridge_gate_receipt_digest=late_schema_bridge.receipt_digest,
    )

    with pytest.raises(AgentContractActivationError, match="hosted_gate_chronology_invalid"):
        replace(
            hosted,
            schema_bridge_gate_decision_receipt=late_schema_bridge,
            hosted_gate_decision_receipt=rebound_hosted_gate,
        )


def test_raw_go_evidence_cannot_substitute_for_a_verified_receipt() -> None:
    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_review_receipt_invalid"):
        _snapshot(tool_state="shadow", tool_review_receipt=_review("self-reported-go"))


def test_receipt_must_bind_exact_release_policy_verifier_and_pin_scope() -> None:
    before = _snapshot()
    assert before.request_review_receipt is not None
    assert before.tool_review_receipt is not None

    drifted_policy = replace(before.activation_policy, hosted_gate=_owner("hosted_gate_v2"))
    with pytest.raises(AgentContractActivationError, match="agent_contract_request_review_receipt_mismatch"):
        replace(before, activation_policy=drifted_policy)

    wrong_scope = replace(before.tool_review_receipt, required_pin_set_digest=_digest("wrong-scope"))
    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_review_receipt_mismatch"):
        replace(before, tool_review_receipt=wrong_scope)

    wrong_release_evidence = replace(
        before.tool_review_receipt.review_evidence,
        reviewed_commit=_digest("other-release"),
    )
    wrong_release = replace(before.tool_review_receipt, review_evidence=wrong_release_evidence)
    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_review_receipt_mismatch"):
        replace(before, tool_review_receipt=wrong_release)


def test_formal_review_scope_is_preserved_separately_from_verified_pin_set_binding() -> None:
    snapshot = _snapshot()
    assert snapshot.tool_review_receipt is not None
    receipt = snapshot.tool_review_receipt

    assert receipt.review_evidence.review_scope_digest == _digest("tool-scope")
    assert receipt.required_pin_set_digest == snapshot.required_pins.required_pin_set_digest
    assert receipt.review_evidence.review_scope_digest != receipt.required_pin_set_digest


def test_same_name_foreign_query_route_is_rejected() -> None:
    expected_owner = _owner("inspect_operation_query")
    foreign_owner = _owner("foreign_inspect_operation_query")
    foreign_pins = _pin_set(
        activation_key="query:inspect_operation",
        query_owner=foreign_owner,
        results=(
            _result(
                "inspect_operation",
                tool_kind="query",
                query_owner=foreign_owner,
                version="inspect_operation_result_v1",
            ),
        ),
        tools=(
            _tool(
                "inspect_operation",
                tool_kind="query",
                query_owner=foreign_owner,
                version="inspect_operation_tool_v1",
            ),
        ),
    )

    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_history_route_mismatch"):
        _snapshot(
            entry_kind="query",
            activation_key="query:inspect_operation",
            action_type=None,
            tool_name="inspect_operation",
            query_owner=expected_owner,
            required_pins=foreign_pins,
        )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        (
            {"request_state": "shadow", "current_request": None},
            "agent_contract_active_request_pins_missing",
        ),
        (
            {"request_state": "current", "request_review_receipt": None},
            "agent_contract_active_request_pins_missing",
        ),
        (
            {"request_state": "current", "request_review_receipt": _review("raw-request-go")},
            "agent_contract_request_review_receipt_invalid",
        ),
        (
            {
                "request_state": "shadow",
                "required_pins": _pin_set(requests=()),
            },
            "agent_contract_current_request_not_required",
        ),
        (
            {"tool_state": "shadow", "current_result": None},
            "agent_contract_active_tool_pins_missing",
        ),
        (
            {"tool_state": "shadow", "current_tool": None},
            "agent_contract_active_tool_pins_missing",
        ),
        (
            {"tool_state": "shadow", "tool_review_receipt": None},
            "agent_contract_active_tool_pins_missing",
        ),
        (
            {"tool_state": "shadow", "tool_review_receipt": _review("raw-tool-go")},
            "agent_contract_tool_review_receipt_invalid",
        ),
        (
            {
                "tool_state": "shadow",
                "required_pins": _pin_set(results=()),
            },
            "agent_contract_current_result_not_required",
        ),
    ],
)
def test_active_states_fail_closed_without_exact_pins_and_go_evidence(
    overrides: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(AgentContractActivationError, match=error):
        _snapshot(**overrides)


def test_hosted_action_requires_current_request_but_hosted_query_does_not() -> None:
    with pytest.raises(AgentContractActivationError, match="agent_contract_hosted_action_request_not_current"):
        _snapshot(
            request_state="shadow",
            tool_state="hosted",
            tool_activated_at="2026-07-17T10:00:00Z",
            tool_expires_at="2026-07-17T11:00:00Z",
        )

    query = _snapshot(
        entry_kind="query",
        activation_key="query:inspect_operation",
        action_type=None,
        tool_name="inspect_operation",
        request_state="not_applicable",
        tool_state="hosted",
        tool_activated_at="2026-07-17T10:00:00Z",
        tool_expires_at="2026-07-17T11:00:00Z",
    )

    assert query.entry_kind == "query"
    assert query.request_state == "not_applicable"
    assert query.tool_state == "hosted"


def test_request_and_tool_windows_use_utc_instants_and_require_strictly_later_expiry() -> None:
    hosted = _snapshot(
        request_state="current",
        tool_state="hosted",
        tool_activated_at="2026-07-17T10:00:00Z",
        tool_expires_at="2026-07-17T10:00:00.1Z",
    )
    assert hosted.tool_expires_at == "2026-07-17T10:00:00.1Z"

    for expires_at in ("2026-07-17T10:00:00Z", "2026-07-17T09:59:59.999999Z"):
        with pytest.raises(AgentContractActivationError, match="agent_contract_tool_activation_window_invalid"):
            _snapshot(
                request_state="current",
                tool_state="hosted",
                tool_activated_at="2026-07-17T10:00:00Z",
                tool_expires_at=expires_at,
            )

    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_expires_at_invalid"):
        _snapshot(
            request_state="current",
            tool_state="hosted",
            tool_activated_at="2026-07-17T10:00:00Z",
            tool_expires_at="2026-07-17T11:00:00+00:00",
        )

    with pytest.raises(AgentContractActivationError, match="agent_contract_request_activation_window_invalid"):
        _snapshot(request_state="current", request_activated_at=None, request_expires_at=None)

    with pytest.raises(AgentContractActivationError, match="agent_contract_tool_activation_window_invalid"):
        _snapshot(
            entry_kind="query",
            activation_key="query:inspect_operation",
            action_type=None,
            tool_name="inspect_operation",
            request_state="not_applicable",
            tool_state="hosted",
            tool_activated_at=None,
            tool_expires_at=None,
        )


def test_all_timestamp_fields_canonicalize_equivalent_utc_instants() -> None:
    policy = _policy()
    required_pins = _pin_set()
    evidence = _review(
        "canonical-time",
        scope_digest=required_pins.required_pin_set_digest,
        reviewed_commit=policy.release_commit,
        verifier_contract_digest=policy.review_verifier.contract_digest,
        reviewed_at="2026-07-17T03:00:00.000000Z",
    )
    receipt = _receipt(
        "canonical-time",
        activation_key=required_pins.activation_key,
        required_pins=required_pins,
        policy=policy,
        evidence=evidence,
        verified_at="2026-07-17T03:01:00.010000Z",
    )
    snapshot = _snapshot(
        required_pins=required_pins,
        activation_policy=policy,
        request_state="current",
        tool_state="hosted",
        request_review_receipt=receipt,
        tool_review_receipt=receipt,
        request_activated_at="2026-07-17T09:00:00.100000Z",
        request_expires_at="2026-07-17T12:00:00.200000Z",
        tool_activated_at="2026-07-17T10:00:00.100000Z",
        tool_expires_at="2026-07-17T11:00:00.200000Z",
    )

    assert evidence.reviewed_at == "2026-07-17T03:00:00Z"
    assert receipt.verified_at == "2026-07-17T03:01:00.01Z"
    assert snapshot.request_activated_at == "2026-07-17T09:00:00.1Z"
    assert snapshot.request_expires_at == "2026-07-17T12:00:00.2Z"
    assert snapshot.tool_activated_at == "2026-07-17T10:00:00.1Z"
    assert snapshot.tool_expires_at == "2026-07-17T11:00:00.2Z"


def test_review_verification_and_hosted_activation_chronology_fail_closed() -> None:
    policy = _policy()
    required_pins = _pin_set()
    evidence = _review(
        "future-review",
        scope_digest=required_pins.required_pin_set_digest,
        reviewed_commit=policy.release_commit,
        verifier_contract_digest=policy.review_verifier.contract_digest,
        reviewed_at="2026-07-17T12:00:00Z",
    )
    with pytest.raises(
        AgentContractActivationError,
        match="agent_contract_review_verification_chronology_invalid",
    ):
        _receipt(
            "early-verification",
            activation_key=required_pins.activation_key,
            required_pins=required_pins,
            policy=policy,
            evidence=evidence,
            verified_at="2026-07-17T11:59:59Z",
        )

    late_receipt = _receipt(
        "late-verification",
        activation_key=required_pins.activation_key,
        required_pins=required_pins,
        policy=policy,
        evidence=evidence,
        verified_at="2026-07-17T12:01:00Z",
    )
    with pytest.raises(AgentContractActivationError, match="agent_contract_request_review_chronology_invalid"):
        _snapshot(
            required_pins=required_pins,
            activation_policy=policy,
            request_state="current",
            tool_state="hosted",
            request_review_receipt=late_receipt,
            tool_review_receipt=late_receipt,
            request_activated_at="2026-07-17T10:00:00Z",
            request_expires_at="2026-07-17T13:00:00Z",
            tool_activated_at="2026-07-17T10:00:00Z",
            tool_expires_at="2026-07-17T11:00:00Z",
        )


def test_hosted_gate_receipt_binds_epoch_policy_pins_and_exact_review_receipts() -> None:
    hosted = _snapshot(request_state="current", tool_state="hosted", activation_epoch=7)
    gate_receipt = hosted.hosted_gate_decision_receipt
    assert gate_receipt is not None
    assert gate_receipt.decision_epoch == 7
    assert gate_receipt.required_pin_set_digest == hosted.required_pins.required_pin_set_digest
    assert gate_receipt.activation_policy_digest == hosted.activation_policy.policy_digest
    assert gate_receipt.tool_review_receipt_digest == hosted.tool_review_receipt.receipt_digest  # type: ignore[union-attr]

    drifted_gate = replace(gate_receipt, decision_artifact_digest=_digest("foreign-gate-decision"))
    assert drifted_gate.receipt_digest != gate_receipt.receipt_digest
    with pytest.raises(AgentContractActivationError, match="agent_contract_hosted_gate_decision_mismatch"):
        replace(hosted, hosted_gate_decision_receipt=replace(gate_receipt, decision_epoch=8))


def test_hosted_gate_decision_must_follow_review_verification_and_precede_tool_activation() -> None:
    hosted = _snapshot(request_state="current", tool_state="hosted")
    assert hosted.hosted_gate_decision_receipt is not None

    with pytest.raises(AgentContractActivationError, match="agent_contract_hosted_gate_chronology_invalid"):
        replace(
            hosted,
            hosted_gate_decision_receipt=replace(
                hosted.hosted_gate_decision_receipt,
                decided_at="2026-07-17T10:00:01Z",
            ),
        )


def test_transition_rejects_stale_revision_or_snapshot_digest() -> None:
    before = _snapshot()
    after = replace(before, activation_epoch=1, update_revision=1, request_state="shadow")

    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_cas_miss"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision + 1,
            expected_snapshot_digest=before.snapshot_digest,
        )

    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_cas_miss"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=_digest("stale-snapshot"),
        )


@pytest.mark.parametrize(
    ("revision", "digest"),
    [
        (True, "0" * 64),
        (-1, "0" * 64),
        (0, "not-a-digest"),
    ],
)
def test_transition_rejects_noncanonical_cas_inputs(revision: object, digest: object) -> None:
    before = _snapshot()
    after = replace(before, activation_epoch=1, update_revision=1, request_state="shadow")
    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_cas_input_invalid"):
        validate_activation_transition(
            before,
            after,
            expected_revision=revision,  # type: ignore[arg-type]
            expected_snapshot_digest=digest,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("field_name", ["activation_epoch", "update_revision"])
def test_snapshot_counters_are_bounded_by_postgres_bigint(field_name: str) -> None:
    assert getattr(_snapshot(**{field_name: POSTGRES_BIGINT_MAX}), field_name) == POSTGRES_BIGINT_MAX
    for invalid in (POSTGRES_BIGINT_MAX + 1, True):
        with pytest.raises(AgentContractActivationError, match=f"agent_contract_{field_name}_invalid"):
            _snapshot(**{field_name: invalid})


def test_transition_rejects_cas_revision_outside_postgres_bigint_and_counter_exhaustion() -> None:
    before = _snapshot()
    after = replace(before, activation_epoch=1, update_revision=1, request_state="shadow")
    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_cas_input_invalid"):
        validate_activation_transition(
            before,
            after,
            expected_revision=POSTGRES_BIGINT_MAX + 1,
            expected_snapshot_digest=before.snapshot_digest,
        )

    exhausted = _snapshot(activation_epoch=POSTGRES_BIGINT_MAX, update_revision=POSTGRES_BIGINT_MAX)
    impossible_next = replace(exhausted, request_state="shadow")
    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_revision_invalid"):
        validate_activation_transition(
            exhausted,
            impossible_next,
            expected_revision=exhausted.update_revision,
            expected_snapshot_digest=exhausted.snapshot_digest,
        )


def test_disabled_staging_rejects_revision_only_semantic_noop() -> None:
    before = _snapshot()
    after = replace(
        before,
        activation_epoch=1,
        update_revision=1,
        last_transition_actor="operator.other",
        last_transition_reason="audit text only",
    )
    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_semantic_noop"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=before.snapshot_digest,
        )


def test_transition_rejects_identity_drift() -> None:
    before = _snapshot()
    after = _snapshot(
        activation_key="action:review_candidates",
        action_type="review_candidates",
        tool_name="review_candidates",
        activation_epoch=1,
        update_revision=1,
    )

    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_identity_drift"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=before.snapshot_digest,
        )


@pytest.mark.parametrize(
    ("revision", "epoch"),
    [(0, 1), (2, 1), (1, 0), (1, 2)],
)
def test_transition_requires_exact_next_revision_and_epoch(revision: int, epoch: int) -> None:
    before = _snapshot()
    after = replace(before, request_state="shadow", update_revision=revision, activation_epoch=epoch)

    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_revision_invalid"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=before.snapshot_digest,
        )


def test_transition_changes_only_one_release_state_dimension() -> None:
    before = _snapshot()
    after = replace(
        before,
        request_state="shadow",
        tool_state="shadow",
        update_revision=1,
        activation_epoch=1,
    )

    with pytest.raises(AgentContractActivationError, match="agent_contract_transition_multiple_state_dimensions"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=before.snapshot_digest,
        )


def test_disabled_staging_then_forward_activation_and_backout_retains_pins() -> None:
    initial = _snapshot(
        current_request=None,
        current_result=None,
        current_tool=None,
        request_review_receipt=None,
        tool_review_receipt=None,
        required_pins=_pin_set(requests=(), results=(), tools=()),
    )
    staged = replace(
        _snapshot(),
        activation_epoch=1,
        update_revision=1,
        last_transition_reason="stage exact reviewed pins while disabled",
    )
    assert (
        validate_activation_transition(
            initial,
            staged,
            expected_revision=initial.update_revision,
            expected_snapshot_digest=initial.snapshot_digest,
        )
        is staged
    )

    request_shadow = _transition(staged, request_state="shadow")
    request_current = _transition(request_shadow, request_state="current")
    tool_shadow = _transition(request_current, tool_state="shadow")
    hosted = _transition(
        tool_shadow,
        tool_state="hosted",
        tool_activated_at="2026-07-17T10:00:00Z",
        tool_expires_at="2026-07-17T11:00:00Z",
    )
    tool_disabled = _transition(hosted, tool_state="disabled")
    fully_disabled = _transition(tool_disabled, request_state="disabled")

    assert request_shadow.request_state == "shadow"
    assert request_current.request_state == "current"
    assert hosted.tool_state == "hosted"
    assert fully_disabled.request_state == "disabled"
    assert fully_disabled.tool_state == "disabled"
    assert fully_disabled.required_pins == staged.required_pins
    assert fully_disabled.current_request == staged.current_request
    assert fully_disabled.current_result == staged.current_result
    assert fully_disabled.current_tool == staged.current_tool


def test_backout_cannot_remove_historical_required_pins() -> None:
    historical = _request(version="plan_acquisition_request_v0")
    current = _request()
    required_with_history = _pin_set(requests=(historical, current))
    required_without_history = _pin_set(requests=(current,))
    before = _snapshot(request_state="current", required_pins=required_with_history)
    after = replace(
        before,
        request_state="disabled",
        required_pins=required_without_history,
        request_review_receipt=_receipt(
            "request-current-only",
            activation_key=before.activation_key,
            required_pins=required_without_history,
            policy=before.activation_policy,
        ),
        tool_review_receipt=_receipt(
            "tool-current-only",
            activation_key=before.activation_key,
            required_pins=required_without_history,
            policy=before.activation_policy,
        ),
        activation_epoch=1,
        update_revision=1,
    )

    with pytest.raises(AgentContractActivationError, match="agent_contract_required_pin_removal_forbidden"):
        validate_activation_transition(
            before,
            after,
            expected_revision=before.update_revision,
            expected_snapshot_digest=before.snapshot_digest,
        )
