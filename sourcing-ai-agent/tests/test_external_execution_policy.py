from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sourcing_agent.external_execution_policy import (
    EXTERNAL_EXECUTION_AUTHORIZATION_AVAILABLE,
    EXTERNAL_EXECUTION_POLICY_HEADER_RECORD_KEYS,
    MODEL_EXECUTION_EVIDENCE_VARIANT,
    MODEL_EXECUTION_POLICY_VARIANT,
    MODEL_EXECUTION_TRANSPORT_KIND,
    PROVIDER_OPERATION_EVIDENCE_VARIANT,
    PROVIDER_OPERATION_POLICY_VARIANT,
    PROVIDER_OPERATION_TRANSPORT_KIND,
    ExternalExecutionPolicyError,
    ProviderOperationPolicyInput,
    compile_external_execution_policy_header,
    compile_model_execution_policy,
    compile_provider_operation_policy,
)
from sourcing_agent.model_route_registry import (
    EFFECTIVE_MODEL_ROUTE_SETTINGS_OWNER,
    EFFECTIVE_MODEL_ROUTE_SETTINGS_SCHEMA_VERSION,
    MODEL_ROUTE_SPECS_BY_ID,
    EffectiveModelRouteSettings,
    ModelRouteExecutionRejected,
    assert_d0a_route_execution_allowed,
    issue_effective_model_route_snapshot,
    model_route_registry_manifest,
)
from sourcing_agent.model_tool_runtime import (
    D0A_EFFECT_AUTHORIZATION_AVAILABLE,
    MODEL_TURN_BUDGET_SCHEMA_VERSION,
    ModelTurnBudget,
    model_turn_execution_context_for_route,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _route():
    return MODEL_ROUTE_SPECS_BY_ID["agent.planner.loop"]


def _settings() -> EffectiveModelRouteSettings:
    return EffectiveModelRouteSettings(
        schema_version=EFFECTIVE_MODEL_ROUTE_SETTINGS_SCHEMA_VERSION,
        settings_owner=EFFECTIVE_MODEL_ROUTE_SETTINGS_OWNER,
        settings_policy_revision="synthetic_settings_policy_v1",
        provider_family="model",
        live_gate_provider_name="model_provider",
        endpoint_identity_digest=_digest("synthetic-policy-endpoint"),
        request_timeout_ms=40_000,
        pricing_class="synthetic_zero_cost",
        circuit_policy_id="synthetic_model_circuit_v1",
    )


def _snapshot(*, request_timeout_ms: int = 40_000):
    route = _route()
    return issue_effective_model_route_snapshot(
        route_id=route.route_id,
        route_revision=route.revision,
        effective_settings=replace(_settings(), request_timeout_ms=request_timeout_ms),
    )


def _budget() -> ModelTurnBudget:
    return ModelTurnBudget(
        schema_version=MODEL_TURN_BUDGET_SCHEMA_VERSION,
        budget_class=_route().budget_class,
        max_input_tokens=4_096,
        max_output_tokens=512,
        max_total_tokens=4_608,
        monetary_ceiling="0",
        currency_code="USD",
        deadline_at=datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc),
    )


def _context(*, provider_mode: str = "scripted"):
    return model_turn_execution_context_for_route(
        _route(),
        _snapshot(),
        runtime_namespace="test:external-execution-policy",
        provider_mode=provider_mode,  # type: ignore[arg-type]
        workspace_id="workspace_policy_test",
        scope_digest=_digest("synthetic-policy-scope"),
        coordination_plan_review_id=107,
        actor_id="actor_policy_test",
        permission_scope="agent:plan",
        prompt_policy_version="prompt_policy_v1",
        permission_scope_revision="permission_scope_v1",
        outbound_policy_revision="outbound_policy_v1",
        model_safe_schema_revision="model_safe_v1",
        operation_run_id="operation_policy_001",
        turn_id="turn_policy_001",
        step_id="step_policy_001",
        workflow_command_id="command_policy_001",
        activity_run_id="activity_policy_001",
        activity_attempt_id="activity_attempt_policy_001",
        attempt=1,
        budget=_budget(),
        budget_reservation_ref="cost-reservation:policy:001",
        approval_ref=None,
    )


def _model_header():
    context = _context()
    return compile_external_execution_policy_header(
        policy_variant=MODEL_EXECUTION_POLICY_VARIANT,
        policy_id="agent_model_execution",
        policy_revision="external_model_policy_v1",
        transport_kind=MODEL_EXECUTION_TRANSPORT_KIND,
        evidence_variant=MODEL_EXECUTION_EVIDENCE_VARIANT,
        provider_family=_snapshot().provider_family,
        allowed_provider_modes=("scripted", "simulate"),
        retry_owner="workflow_activity_attempt",
        budget_policy_id="model_standard_small_v1",
        live_gate_provider_name=_snapshot().live_gate_provider_name,
        runtime_namespace=context.runtime_namespace,
        provider_mode=context.provider_mode,
        workspace_id=context.workspace_id,
        scope_digest=context.scope_digest,
        coordination_plan_review_id=context.coordination_plan_review_id,
    )


def test_external_execution_header_is_canonical_immutable_and_full_pfx_bound() -> None:
    header = _model_header()
    repeated = _model_header()

    assert header == repeated
    assert set(header.to_record()) == EXTERNAL_EXECUTION_POLICY_HEADER_RECORD_KEYS
    assert header.pfx_record() == _context().pfx_record()
    assert header.policy_id == "agent_model_execution"
    assert header.provider_family == _snapshot().provider_family == "model"
    assert header.live_gate_provider_name == _snapshot().live_gate_provider_name == "model_provider"
    assert header.allowed_provider_modes == ("scripted", "simulate")
    assert len(header.pfx_digest) == 64
    assert len(header.header_digest) == 64
    assert json.loads(json.dumps(header.to_record(), sort_keys=True)) == header.to_record()
    assert replace(header, provider_mode="simulate").pfx_digest != header.pfx_digest
    assert replace(header, workspace_id="workspace_other").pfx_digest != header.pfx_digest
    with pytest.raises(FrozenInstanceError):
        header.workspace_id = "workspace_other"  # type: ignore[misc]
    with pytest.raises(ExternalExecutionPolicyError, match="provider_mode_invalid"):
        replace(header, provider_mode="replay")  # type: ignore[arg-type]
    with pytest.raises(ExternalExecutionPolicyError, match="provider_mode_not_allowed"):
        replace(header, provider_mode="live")
    with pytest.raises(ExternalExecutionPolicyError, match="allowed_provider_modes_noncanonical"):
        replace(header, allowed_provider_modes=("simulate", "scripted"))
    with pytest.raises(ExternalExecutionPolicyError, match="review_id_invalid"):
        replace(header, coordination_plan_review_id=True)


def test_model_policy_compiler_exactly_binds_context_route_snapshot_and_budget() -> None:
    context = _context()
    policy = compile_model_execution_policy(
        context,
        _route(),
        _snapshot(),
        policy_id="agent_model_execution",
        policy_revision="external_model_policy_v1",
        allowed_provider_modes=("scripted", "simulate"),
        retry_owner="workflow_activity_attempt",
        budget_policy_id="model_standard_small_v1",
        header=_model_header(),
    )
    repeated = compile_model_execution_policy(
        context,
        _route(),
        _snapshot(),
        policy_id="agent_model_execution",
        policy_revision="external_model_policy_v1",
        allowed_provider_modes=("scripted", "simulate"),
        retry_owner="workflow_activity_attempt",
        budget_policy_id="model_standard_small_v1",
    )

    assert policy == repeated
    assert policy.header.transport_kind == MODEL_EXECUTION_TRANSPORT_KIND == "model_tool"
    assert policy.header.evidence_variant == MODEL_EXECUTION_EVIDENCE_VARIANT == "model_tool_v1"
    assert policy.execution_authorized is False
    assert policy.execution_context_digest == context.context_digest
    assert policy.budget_digest == context.budget.budget_digest
    assert len(policy.policy_digest) == 64

    changed_context = _context(provider_mode="simulate")
    with pytest.raises(ExternalExecutionPolicyError, match="header_context_mismatch"):
        compile_model_execution_policy(
            changed_context,
            _route(),
            _snapshot(),
            policy_id="agent_model_execution",
            policy_revision="external_model_policy_v1",
            allowed_provider_modes=("scripted", "simulate"),
            retry_owner="workflow_activity_attempt",
            budget_policy_id="model_standard_small_v1",
            header=_model_header(),
        )
    with pytest.raises(ExternalExecutionPolicyError, match="context_route_mismatch:snapshot"):
        compile_model_execution_policy(
            context,
            _route(),
            _snapshot(request_timeout_ms=40_001),
            policy_id="agent_model_execution",
            policy_revision="external_model_policy_v1",
            allowed_provider_modes=("scripted", "simulate"),
            retry_owner="workflow_activity_attempt",
            budget_policy_id="model_standard_small_v1",
        )
    with pytest.raises(ExternalExecutionPolicyError, match="header_context_pfx_mismatch"):
        replace(policy, header=replace(policy.header, provider_mode="simulate"))


def test_provider_operation_policy_cannot_masquerade_as_model_tool_transport() -> None:
    context = _context()
    policy_input = ProviderOperationPolicyInput(
        policy_id="harvest_profile_search",
        policy_revision="external_provider_policy_v1",
        provider_family="apify_harvest",
        allowed_provider_modes=("scripted", "simulate"),
        retry_owner="provider_task_runtime",
        budget_policy_id="provider_search_small_v1",
        live_gate_provider_name="harvest_apify",
        runtime_namespace=context.runtime_namespace,
        provider_mode=context.provider_mode,
        workspace_id=context.workspace_id,
        scope_digest=context.scope_digest,
        coordination_plan_review_id=context.coordination_plan_review_id,
        provider="harvestapi",
        operation_kind="harvest_profile_search",
        operation_schema_revision="harvest_profile_search_v1",
        request_schema_digest=_digest("synthetic-harvest-request-schema"),
        budget_class="provider_search_small",
        outbound_policy_revision="harvest_outbound_v1",
    )
    policy = compile_provider_operation_policy(policy_input)
    record = policy.to_record()

    assert record["policy_family"] == "provider_operation"
    assert policy.execution_authorized is False
    assert policy.header.policy_variant == PROVIDER_OPERATION_POLICY_VARIANT == "provider_operation_v1"
    assert policy.header.transport_kind == PROVIDER_OPERATION_TRANSPORT_KIND == "provider_operation"
    assert policy.header.evidence_variant == PROVIDER_OPERATION_EVIDENCE_VARIANT
    assert PROVIDER_OPERATION_EVIDENCE_VARIANT == "provider_operation_evidence_draft_v1"
    assert record["header"]["provider_family"] == "apify_harvest"  # type: ignore[index]
    assert record["header"]["live_gate_provider_name"] == "harvest_apify"  # type: ignore[index]
    assert "requested_model" not in record
    assert "model_tool_v1" not in json.dumps(record, sort_keys=True)
    assert len(policy.policy_digest) == 64
    with pytest.raises(ExternalExecutionPolicyError, match="input_type_invalid"):
        compile_provider_operation_policy(_model_header())  # type: ignore[arg-type]
    with pytest.raises(ExternalExecutionPolicyError, match="provider_mode_not_allowed"):
        replace(policy_input, allowed_provider_modes=("simulate",))
    with pytest.raises(ExternalExecutionPolicyError, match="header_variant_mismatch"):
        replace(policy, header=_model_header())
    with pytest.raises(ExternalExecutionPolicyError, match="variant_transport_evidence_mismatch"):
        replace(policy.header, evidence_variant="model_tool_v1")


def test_d0g_policy_foundation_has_zero_runtime_or_live_activation() -> None:
    assert EXTERNAL_EXECUTION_AUTHORIZATION_AVAILABLE is False
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False
    assert model_route_registry_manifest()["live_enabled"] is False
    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        assert_d0a_route_execution_allowed(
            route_id=_route().route_id,
            provider_mode="live",
            required_capabilities={"tools"},
        )

    policy_source = (SOURCE_ROOT / "external_execution_policy.py").read_text(encoding="utf-8")
    assert "import requests" not in policy_source
    assert "urllib" not in policy_source
    assert "os.environ" not in policy_source
    assert "assert_live_provider_access_allowed" not in policy_source

    production_references: dict[str, list[str]] = {}
    for path in sorted(SOURCE_ROOT.glob("*.py")):
        if path.name == "external_execution_policy.py":
            continue
        source = path.read_text(encoding="utf-8")
        matches = [
            symbol
            for symbol in ("compile_model_execution_policy", "compile_provider_operation_policy")
            if symbol in source
        ]
        if matches:
            production_references[path.name] = matches
    assert production_references == {}

    observed_imports: set[str] = set()
    for node in ast.walk(ast.parse(policy_source)):
        if isinstance(node, ast.Import):
            observed_imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            observed_imports.add(node.module)
    assert observed_imports == {
        "__future__",
        "dataclasses",
        "hashlib",
        "json",
        "model_route_registry",
        "model_tool_runtime",
        "re",
        "typing",
    }
