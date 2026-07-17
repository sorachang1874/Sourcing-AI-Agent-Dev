from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, fields, replace
from typing import Any

import pytest

from sourcing_agent.acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
    ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
)
from sourcing_agent.agent_tool_registry import (
    AGENT_TOOL_REGISTRY_SCHEMA_VERSION,
    AGENT_TOOL_SPEC_SCHEMA_VERSION,
    DEFAULT_AGENT_TOOL_REGISTRY,
    AgentActionToolRoute,
    AgentExecutionSubjectRequirement,
    AgentQueryToolRoute,
    AgentToolApprovalRequirement,
    AgentToolBehavior,
    AgentToolBudgetRequirement,
    AgentToolCapabilityRequirement,
    AgentToolOwnerPin,
    AgentToolRegistry,
    AgentToolRegistryError,
    AgentToolReleaseStateRef,
    AgentToolRequestPin,
    AgentToolResultPin,
    AgentToolSimulateFixturePin,
    AgentToolSpec,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _owner(owner_id: str, *, revision: str | None = None, digest_label: str | None = None) -> AgentToolOwnerPin:
    return AgentToolOwnerPin(
        owner_id=owner_id,
        owner_revision=revision or f"{owner_id.rsplit('.', maxsplit=1)[-1]}_v1",
        owner_contract_digest=_digest(digest_label or owner_id),
    )


def _subject(*, checkpoints: tuple[str, ...] | None = None) -> AgentExecutionSubjectRequirement:
    return AgentExecutionSubjectRequirement(
        subject_schema_version="agent_execution_subject_v1",
        subject_schema_digest=_digest("agent-execution-subject-v1"),
        subject_validator_owner=_owner("agent.auth.execution_subject_validator"),
        permission_policy=_owner("agent.auth.permission_policy"),
        authorization_checkpoints=(
            "catalog_projection",
            "invocation_acceptance",
        )
        if checkpoints is None
        else checkpoints,
    )


def _request(
    *,
    version: str = "synthetic_request_v1",
    digest: str | None = None,
    action_type: str | None = "synthetic_action",
    query_owner: AgentToolOwnerPin | None = None,
) -> AgentToolRequestPin:
    return AgentToolRequestPin(
        schema_version=version,
        schema_digest=digest or _digest(version),
        validator_owner=_owner("sourcing_agent.model_tool_runtime.ToolSpec.validate_input"),
        action_type=action_type,
        action_contract_digest=None if action_type is None else _digest(f"{action_type}-action-contract-v1"),
        query_owner=query_owner,
    )


def _result(
    *,
    tool_name: str = "synthetic_action",
    action_type: str | None = "synthetic_action",
    query_owner: AgentToolOwnerPin | None = None,
    version: str = "synthetic_result_v1",
    digest: str | None = None,
    validation_contract_version: str = "action_result_interpretation_contract_v3",
    validator_owner: AgentToolOwnerPin | None = None,
    max_serialized_bytes: int = 16 * 1024,
    max_items: int = 256,
    max_depth: int = 10,
) -> AgentToolResultPin:
    return AgentToolResultPin(
        tool_name=tool_name,
        tool_kind="action" if action_type is not None else "query",
        action_type=action_type,
        query_owner=query_owner,
        schema_version=version,
        schema_digest=digest or _digest(version),
        serializer_owner=_owner("agent.result.synthetic_serializer"),
        validator_owner=validator_owner
        or _owner("sourcing_agent.model_tool_runtime.InternalToolValidatorSpec.validate_input"),
        validation_contract_version=validation_contract_version,
        max_serialized_bytes=max_serialized_bytes,
        max_items=max_items,
        max_depth=max_depth,
    )


def _fixture(name: str) -> AgentToolSimulateFixturePin:
    return AgentToolSimulateFixturePin(
        fixture_id=f"agent.simulate.{name}",
        fixture_revision=f"{name}_fixture_v1",
        fixture_digest=_digest(f"{name}-fixture-v1"),
    )


def _release(release_key: str, *, owner: AgentToolOwnerPin | None = None) -> AgentToolReleaseStateRef:
    return AgentToolReleaseStateRef(
        release_owner=owner or _owner("agent.release.registry"),
        release_key=release_key,
    )


def _registry(
    specs: tuple[AgentToolSpec, ...],
    *,
    current_release_owner: AgentToolOwnerPin | None = None,
) -> AgentToolRegistry:
    return AgentToolRegistry.from_specs(
        specs,
        current_release_owner=current_release_owner or _owner("agent.release.registry"),
    )


def _approval(*, required: bool = False) -> AgentToolApprovalRequirement:
    return AgentToolApprovalRequirement(
        mode="human_confirmation_required" if required else "not_required",
        approval_policy=_owner("agent.approval.policy"),
    )


def _action_behavior(*, command_backed: bool = False, approval_required: bool = False) -> AgentToolBehavior:
    return AgentToolBehavior(
        effect_class="command_backed_action" if command_backed else "commandless_action",
        command_exposure="owner_command_only" if command_backed else "none",
        approval=_approval(required=approval_required),
        control_policy=_owner("agent.control.policy"),
    )


def _query_behavior() -> AgentToolBehavior:
    return AgentToolBehavior(
        effect_class="read_only",
        command_exposure="none",
        approval=_approval(),
        control_policy=_owner("agent.control.query_policy"),
    )


def _action_spec(
    *,
    tool_name: str = "synthetic_action",
    action_type: str | None = None,
    route: AgentActionToolRoute | AgentQueryToolRoute | None = None,
    request: AgentToolRequestPin | None = None,
    result: AgentToolResultPin | None = None,
    behavior: AgentToolBehavior | None = None,
    budget: AgentToolBudgetRequirement | None = None,
    capability: AgentToolCapabilityRequirement | None = None,
    execution_subject: AgentExecutionSubjectRequirement | None = None,
    model_description: str = "Perform one synthetic owner-bound action.",
    tool_spec_version: str = "synthetic_action_tool_v1",
) -> AgentToolSpec:
    resolved_action_type = action_type or tool_name
    resolved_route = route or AgentActionToolRoute(
        action_type=resolved_action_type,
        workspace_actor_binder=_owner(f"agent.binding.{resolved_action_type}"),
        adapter=_owner(f"agent.adapter.{resolved_action_type}"),
    )
    route_action_type = (
        resolved_route.action_type if isinstance(resolved_route, AgentActionToolRoute) else resolved_action_type
    )
    return AgentToolSpec(
        tool_spec_version=tool_spec_version,
        tool_name=tool_name,
        model_description=model_description,
        tool_kind="action",
        request=request or _request(action_type=route_action_type),
        result=result or _result(tool_name=tool_name, action_type=route_action_type),
        route=resolved_route,
        simulate_fixture=_fixture(tool_name),
        release_state_ref=_release(f"action:{route_action_type}"),
        execution_subject=execution_subject or _subject(),
        budget=budget or AgentToolBudgetRequirement(mode="not_required"),
        capability=capability or AgentToolCapabilityRequirement(mode="not_required"),
        behavior=behavior or _action_behavior(),
    )


def _query_spec(
    *,
    tool_name: str = "inspect_operation",
    query_owner_id: str = "operation.query.inspect",
    route: AgentActionToolRoute | AgentQueryToolRoute | None = None,
    behavior: AgentToolBehavior | None = None,
    budget: AgentToolBudgetRequirement | None = None,
    capability: AgentToolCapabilityRequirement | None = None,
    execution_subject: AgentExecutionSubjectRequirement | None = None,
) -> AgentToolSpec:
    resolved_route = route or AgentQueryToolRoute(
        query_owner=_owner(query_owner_id),
        workspace_actor_binder=_owner(f"agent.query_binding.{tool_name}"),
        adapter=_owner(f"agent.query_adapter.{tool_name}"),
    )
    resolved_query_owner = (
        resolved_route.query_owner if isinstance(resolved_route, AgentQueryToolRoute) else _owner(query_owner_id)
    )
    return AgentToolSpec(
        tool_spec_version="inspect_operation_tool_v1",
        tool_name=tool_name,
        model_description="Read bounded canonical operation state without repairing it.",
        tool_kind="query",
        request=_request(
            version="inspect_operation_request_v1",
            action_type=None,
            query_owner=resolved_query_owner,
        ),
        result=_result(
            tool_name=tool_name,
            action_type=None,
            query_owner=resolved_query_owner,
            version="inspect_operation_result_v1",
        ),
        route=resolved_route,
        simulate_fixture=_fixture(tool_name),
        release_state_ref=_release(f"query:{tool_name}"),
        execution_subject=execution_subject or _subject(),
        budget=budget or AgentToolBudgetRequirement(mode="not_required"),
        capability=capability or AgentToolCapabilityRequirement(mode="not_required"),
        behavior=behavior or _query_behavior(),
    )


def _plan_acquisition_description() -> AgentToolSpec:
    result_spec = ACQUISITION_PLAN_PREVIEW_RESULT_SPEC
    return AgentToolSpec(
        tool_spec_version="plan_acquisition_tool_v1",
        tool_name="plan_acquisition",
        model_description="Build a capability-free immutable acquisition plan preview for explicit user review.",
        tool_kind="action",
        request=AgentToolRequestPin(
            schema_version=ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
            schema_digest=ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
            validator_owner=_owner("sourcing_agent.model_tool_runtime.ToolSpec.validate_input"),
            action_type="plan_acquisition",
            action_contract_digest=_digest("plan-acquisition-action-contract-v1"),
            query_owner=None,
        ),
        result=AgentToolResultPin(
            tool_name="plan_acquisition",
            tool_kind="action",
            action_type="plan_acquisition",
            query_owner=None,
            schema_version=result_spec.result_schema_version,
            schema_digest=result_spec.result_schema_digest,
            serializer_owner=AgentToolOwnerPin(
                owner_id=result_spec.serializer_owner,
                owner_revision=result_spec.serializer_revision,
                owner_contract_digest=result_spec.serializer_contract_digest,
            ),
            validator_owner=_owner("sourcing_agent.model_tool_runtime.InternalToolValidatorSpec.validate_input"),
            validation_contract_version="action_result_interpretation_contract_v3",
            max_serialized_bytes=result_spec.max_serialized_bytes,
            max_items=result_spec.max_items,
            max_depth=result_spec.max_depth,
        ),
        route=AgentActionToolRoute(
            action_type="plan_acquisition",
            workspace_actor_binder=_owner("planner.acquisition_plan_preview.workspace_actor_binder"),
            adapter=_owner("planner.acquisition_plan_preview.adapter"),
        ),
        simulate_fixture=AgentToolSimulateFixturePin(
            fixture_id="planner.acquisition_plan_preview.simulate_fixture",
            fixture_revision="acquisition_plan_preview_fixture_v1",
            fixture_digest=_digest("acquisition-plan-preview-fixture-v1"),
        ),
        release_state_ref=_release("action:plan_acquisition"),
        execution_subject=_subject(),
        budget=AgentToolBudgetRequirement(mode="not_required"),
        capability=AgentToolCapabilityRequirement(mode="not_required"),
        behavior=_action_behavior(),
    )


def test_default_registry_is_empty_and_has_no_activation_or_serving_authority() -> None:
    assert DEFAULT_AGENT_TOOL_REGISTRY.declared_tool_count == 0
    assert DEFAULT_AGENT_TOOL_REGISTRY.tool_names == ()
    assert DEFAULT_AGENT_TOOL_REGISTRY.to_manifest_record() == {
        "schema_version": AGENT_TOOL_REGISTRY_SCHEMA_VERSION,
        "current_release_owner": None,
        "declared_tool_count": 0,
        "historical_spec_count": 0,
        "tools": [],
    }
    assert not hasattr(DEFAULT_AGENT_TOOL_REGISTRY, "served")
    assert not hasattr(DEFAULT_AGENT_TOOL_REGISTRY, "model_visible")
    assert {field.name for field in fields(AgentToolReleaseStateRef)} == {"release_owner", "release_key"}


def test_v1_plan_tool_can_be_described_with_exact_existing_pins_without_registration() -> None:
    spec = _plan_acquisition_description()

    assert spec.tool_name == "plan_acquisition"
    assert spec.action_type == "plan_acquisition"
    assert spec.query_owner_id is None
    assert spec.request.schema_version == ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION
    assert spec.request.schema_digest == ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST
    assert spec.result.schema_version == ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_version
    assert spec.result.schema_digest == ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_digest
    assert spec.result.max_serialized_bytes == ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.max_serialized_bytes
    assert spec.capability.required is False
    assert spec.budget.required is False
    assert DEFAULT_AGENT_TOOL_REGISTRY.specs_for_name(spec.tool_name) == ()
    assert DEFAULT_AGENT_TOOL_REGISTRY.get_historical(*spec.historical_identity) is None


def test_action_shape_has_exact_xor_and_explicit_structural_pins() -> None:
    spec = _action_spec(
        budget=AgentToolBudgetRequirement(
            mode="parent_reservation_required",
            budget_owner=_owner("agent.budget.parent_reservation"),
        ),
        capability=AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_type="cohort_execution_capability_v1",
            capability_issuer=_owner("cohort.capability.issuer"),
            required_provider_modes=("live",),
        ),
        behavior=_action_behavior(command_backed=True, approval_required=True),
        execution_subject=_subject(
            checkpoints=(
                "dispatch_acceptance",
                "invocation_acceptance",
                "approval_acceptance",
                "catalog_projection",
            )
        ),
    )
    record = spec.to_manifest_record()

    assert spec.action_type == "synthetic_action"
    assert spec.query_owner_id is None
    assert (spec.action_type is None) ^ (spec.query_owner_id is None)
    assert record["schema_version"] == AGENT_TOOL_SPEC_SCHEMA_VERSION
    assert record["tool_spec_version"] == "synthetic_action_tool_v1"
    assert len(str(record["tool_spec_digest"])) == 64
    assert record["action_type"] == "synthetic_action"
    assert record["query_owner_id"] is None
    assert record["workspace_actor_binder"] is not None
    assert record["adapter"] is not None
    assert record["budget"]["mode"] == "parent_reservation_required"  # type: ignore[index]
    assert record["capability"]["required_provider_modes"] == ["live"]  # type: ignore[index]
    assert record["execution_subject"]["authorization_checkpoints"] == [  # type: ignore[index]
        "catalog_projection",
        "invocation_acceptance",
        "approval_acceptance",
        "dispatch_acceptance",
    ]


def test_query_shape_has_exact_xor_owner_and_read_only_behavior() -> None:
    spec = _query_spec()
    record = spec.to_manifest_record()

    assert spec.action_type is None
    assert spec.query_owner_id == "operation.query.inspect"
    assert (spec.action_type is None) ^ (spec.query_owner_id is None)
    assert record["action_type"] is None
    assert record["workspace_actor_binder"] == {
        "owner_id": "agent.query_binding.inspect_operation",
        "owner_revision": "inspect_operation_v1",
        "owner_contract_digest": _digest("agent.query_binding.inspect_operation"),
    }
    assert record["adapter"] == {
        "owner_id": "agent.query_adapter.inspect_operation",
        "owner_revision": "inspect_operation_v1",
        "owner_contract_digest": _digest("agent.query_adapter.inspect_operation"),
    }
    assert record["query_owner_id"] == "operation.query.inspect"
    assert record["behavior"]["effect_class"] == "read_only"  # type: ignore[index]
    assert record["behavior"]["command_exposure"] == "none"  # type: ignore[index]


def test_query_route_binder_and_adapter_are_typed_exact_and_digest_bound() -> None:
    spec = _query_spec()
    assert isinstance(spec.route, AgentQueryToolRoute)

    with pytest.raises(AgentToolRegistryError, match="query_workspace_actor_binder_invalid"):
        replace(spec.route, workspace_actor_binder=None)  # type: ignore[arg-type]
    with pytest.raises(AgentToolRegistryError, match="query_adapter_invalid"):
        replace(spec.route, adapter=None)  # type: ignore[arg-type]

    rotated_binder = _owner(
        "agent.query_binding.inspect_operation",
        revision="inspect_operation_v2",
        digest_label="inspect-operation-query-binder-v2",
    )
    changed = replace(spec, route=replace(spec.route, workspace_actor_binder=rotated_binder))
    changed_record = changed.to_manifest_record()

    assert changed.tool_spec_digest != spec.tool_spec_digest
    assert changed_record["workspace_actor_binder"] == rotated_binder.to_fingerprint_record()
    assert changed_record["adapter"] == spec.route.adapter.to_fingerprint_record()


def test_specs_and_registry_are_deeply_immutable_but_manifest_exports_are_defensive() -> None:
    spec = _action_spec()
    registry = _registry((spec,))

    assert isinstance(registry.specs, tuple)
    assert isinstance(spec.execution_subject.authorization_checkpoints, tuple)
    assert isinstance(spec.capability.required_provider_modes, tuple)
    with pytest.raises(FrozenInstanceError):
        spec.tool_name = "forged"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        spec.route.action_type = "forged"  # type: ignore[union-attr,misc]
    with pytest.raises(TypeError):
        registry.specs[0] = spec  # type: ignore[index]

    exported = registry.to_manifest_record()
    exported["tools"][0]["model_description"] = "forged"  # type: ignore[index]
    assert registry.specs_for_name("synthetic_action")[0].model_description != "forged"


def test_digest_is_canonical_and_binds_every_structural_contract_group() -> None:
    baseline = _action_spec()
    dispatch_subject = _subject(
        checkpoints=(
            "catalog_projection",
            "invocation_acceptance",
            "dispatch_acceptance",
        )
    )
    reordered_subject = _subject(
        checkpoints=(
            "invocation_acceptance",
            "catalog_projection",
        )
    )
    reordered = _action_spec(execution_subject=reordered_subject)

    assert baseline.tool_spec_digest == reordered.tool_spec_digest
    assert baseline.tool_spec_digest == _action_spec().tool_spec_digest
    changed_specs = (
        _action_spec(tool_spec_version="synthetic_action_tool_v2"),
        _action_spec(model_description="A different model-visible description."),
        _action_spec(request=_request(version="synthetic_request_v2")),
        _action_spec(
            request=replace(
                _request(),
                action_contract_digest=_digest("synthetic-action-contract-v2"),
            )
        ),
        _action_spec(result=_result(version="synthetic_result_v2")),
        _action_spec(action_type="different_action"),
        replace(baseline, simulate_fixture=_fixture("other_action")),
        replace(
            baseline,
            release_state_ref=_release(
                "action:synthetic_action",
                owner=_owner(
                    "agent.release.registry",
                    revision="registry_v2",
                    digest_label="agent-release-registry-v2",
                ),
            ),
        ),
        _action_spec(execution_subject=replace(_subject(), subject_schema_digest=_digest("other-subject"))),
        _action_spec(
            behavior=_action_behavior(command_backed=True),
            budget=AgentToolBudgetRequirement(
                mode="parent_reservation_required",
                budget_owner=_owner("agent.budget.owner"),
            ),
            execution_subject=dispatch_subject,
        ),
        _action_spec(
            behavior=_action_behavior(command_backed=True),
            capability=AgentToolCapabilityRequirement(
                mode="exact_capability_required",
                capability_type="cohort_execution_capability_v1",
                capability_issuer=_owner("cohort.capability.issuer"),
                required_provider_modes=("scripted",),
            ),
            execution_subject=dispatch_subject,
        ),
        _action_spec(
            behavior=_action_behavior(command_backed=True),
            execution_subject=dispatch_subject,
        ),
    )
    assert all(changed.tool_spec_digest != baseline.tool_spec_digest for changed in changed_specs)


@pytest.mark.parametrize(
    ("tool_kind", "route", "behavior", "match"),
    (
        (
            "action",
            AgentQueryToolRoute(
                query_owner=_owner("query.owner"),
                workspace_actor_binder=_owner("query.owner.workspace_actor_binder"),
                adapter=_owner("query.owner.adapter"),
            ),
            _action_behavior(),
            "action_route_required",
        ),
        (
            "query",
            AgentActionToolRoute(
                action_type="synthetic_action",
                workspace_actor_binder=_owner("agent.binder.synthetic"),
                adapter=_owner("agent.adapter.synthetic"),
            ),
            _query_behavior(),
            "query_route_required",
        ),
    ),
)
def test_tool_kind_and_route_variant_must_match_exactly(
    tool_kind: str,
    route: AgentActionToolRoute | AgentQueryToolRoute,
    behavior: AgentToolBehavior,
    match: str,
) -> None:
    with pytest.raises(AgentToolRegistryError, match=match):
        replace(_action_spec(), tool_kind=tool_kind, route=route, behavior=behavior)  # type: ignore[arg-type]


def test_action_cannot_be_declared_as_read_only_and_query_cannot_mutate_or_require_approval() -> None:
    with pytest.raises(AgentToolRegistryError, match="action_effect_invalid"):
        replace(_action_spec(), behavior=_query_behavior())
    with pytest.raises(AgentToolRegistryError, match="query_behavior_invalid"):
        _query_spec(behavior=_action_behavior())
    with pytest.raises(AgentToolRegistryError, match="query_behavior_invalid"):
        _query_spec(
            behavior=AgentToolBehavior(
                effect_class="read_only",
                command_exposure="none",
                approval=_approval(required=True),
                control_policy=_owner("agent.control.query_policy"),
            )
        )


@pytest.mark.parametrize(
    ("effect_class", "command_exposure"),
    (
        ("read_only", "owner_command_only"),
        ("commandless_action", "owner_command_only"),
        ("command_backed_action", "none"),
    ),
)
def test_effect_and_command_exposure_are_coherent(effect_class: str, command_exposure: str) -> None:
    with pytest.raises(AgentToolRegistryError, match="command_exposure_effect_mismatch"):
        AgentToolBehavior(
            effect_class=effect_class,  # type: ignore[arg-type]
            command_exposure=command_exposure,  # type: ignore[arg-type]
            approval=_approval(),
            control_policy=_owner("agent.control.policy"),
        )


def test_execution_subject_is_mandatory_and_requires_catalog_and_invocation_checks() -> None:
    with pytest.raises(AgentToolRegistryError, match="execution_subject_invalid"):
        replace(_action_spec(), execution_subject=None)  # type: ignore[arg-type]
    for checkpoints in (
        (),
        ("catalog_projection",),
        ("invocation_acceptance",),
        ("catalog_projection", "invocation_acceptance", "invocation_acceptance"),
        ("catalog_projection", "invocation_acceptance", "unknown"),
    ):
        with pytest.raises(AgentToolRegistryError, match="authorization_checkpoints_invalid"):
            _subject(checkpoints=checkpoints)


def test_approval_dispatch_capability_and_live_budget_constraints_fail_closed() -> None:
    with pytest.raises(AgentToolRegistryError, match="approval_checkpoint_required"):
        _action_spec(behavior=_action_behavior(approval_required=True))

    dispatch_required_cases: tuple[dict[str, Any], ...] = (
        {"behavior": _action_behavior(command_backed=True)},
        {
            "behavior": _action_behavior(command_backed=True),
            "budget": AgentToolBudgetRequirement(
                mode="parent_reservation_required",
                budget_owner=_owner("agent.budget.parent"),
            ),
        },
        {
            "behavior": _action_behavior(command_backed=True),
            "capability": AgentToolCapabilityRequirement(
                mode="exact_capability_required",
                capability_type="cohort_execution_capability_v1",
                capability_issuer=_owner("cohort.capability.issuer"),
                required_provider_modes=("scripted",),
            ),
        },
    )
    for kwargs in dispatch_required_cases:
        with pytest.raises(AgentToolRegistryError, match="dispatch_checkpoint_required"):
            _action_spec(**kwargs)

    with pytest.raises(AgentToolRegistryError, match="live_budget_reservation_required"):
        _action_spec(
            behavior=_action_behavior(command_backed=True),
            capability=AgentToolCapabilityRequirement(
                mode="exact_capability_required",
                capability_type="cohort_execution_capability_v1",
                capability_issuer=_owner("cohort.capability.issuer"),
                required_provider_modes=("live",),
            ),
            execution_subject=_subject(
                checkpoints=(
                    "catalog_projection",
                    "invocation_acceptance",
                    "dispatch_acceptance",
                )
            ),
        )


@pytest.mark.parametrize("tool_kind", ("query", "commandless_action"))
@pytest.mark.parametrize("provider_requirement", ("budget", "scripted_capability", "live_capability"))
def test_provider_requirement_kind_matrix_rejects_non_command_backed_routes(
    tool_kind: str,
    provider_requirement: str,
) -> None:
    budget = AgentToolBudgetRequirement(mode="not_required")
    capability = AgentToolCapabilityRequirement(mode="not_required")
    if provider_requirement == "budget":
        budget = AgentToolBudgetRequirement(
            mode="parent_reservation_required",
            budget_owner=_owner("agent.budget.parent"),
        )
    else:
        capability = AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_type="cohort_execution_capability_v1",
            capability_issuer=_owner("cohort.capability.issuer"),
            required_provider_modes=("live",) if provider_requirement == "live_capability" else ("scripted",),
        )
        if provider_requirement == "live_capability":
            budget = AgentToolBudgetRequirement(
                mode="parent_reservation_required",
                budget_owner=_owner("agent.budget.parent"),
            )
    subject = _subject(checkpoints=("catalog_projection", "invocation_acceptance", "dispatch_acceptance"))

    with pytest.raises(AgentToolRegistryError, match="provider_requirement_effect_invalid"):
        if tool_kind == "query":
            _query_spec(budget=budget, capability=capability, execution_subject=subject)
        else:
            _action_spec(budget=budget, capability=capability, execution_subject=subject)


def test_command_backed_action_accepts_live_capability_with_budget_and_dispatch() -> None:
    spec = _action_spec(
        behavior=_action_behavior(command_backed=True),
        budget=AgentToolBudgetRequirement(
            mode="parent_reservation_required",
            budget_owner=_owner("agent.budget.parent"),
        ),
        capability=AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_type="cohort_execution_capability_v1",
            capability_issuer=_owner("cohort.capability.issuer"),
            required_provider_modes=("live",),
        ),
        execution_subject=_subject(checkpoints=("catalog_projection", "invocation_acceptance", "dispatch_acceptance")),
    )

    assert spec.behavior.effect_class == "command_backed_action"
    assert spec.capability.required_provider_modes == ("live",)
    assert spec.budget.required is True


def test_budget_requirement_is_closed_and_cannot_hide_an_owner() -> None:
    assert AgentToolBudgetRequirement(mode="not_required").required is False
    required = AgentToolBudgetRequirement(
        mode="parent_reservation_required",
        budget_owner=_owner("agent.budget.parent"),
    )
    assert required.required is True
    with pytest.raises(AgentToolRegistryError, match="budget_owner_forbidden"):
        AgentToolBudgetRequirement(mode="not_required", budget_owner=_owner("agent.budget.hidden"))
    with pytest.raises(AgentToolRegistryError, match="budget_owner_required"):
        AgentToolBudgetRequirement(mode="parent_reservation_required")


def test_capability_requirement_is_closed_and_provider_modes_are_canonical() -> None:
    assert AgentToolCapabilityRequirement(mode="not_required").required is False
    required = AgentToolCapabilityRequirement(
        mode="exact_capability_required",
        capability_type="cohort_execution_capability_v1",
        capability_issuer=_owner("cohort.capability.issuer"),
        required_provider_modes=("live", "scripted"),
    )
    assert required.required is True
    assert required.required_provider_modes == ("scripted", "live")
    with pytest.raises(AgentToolRegistryError, match="capability_metadata_forbidden"):
        AgentToolCapabilityRequirement(mode="not_required", capability_type="hidden_capability")
    with pytest.raises(AgentToolRegistryError, match="capability_type_required"):
        AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_issuer=_owner("cohort.capability.issuer"),
            required_provider_modes=("live",),
        )
    with pytest.raises(AgentToolRegistryError, match="capability_issuer_required"):
        AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_type="cohort_execution_capability_v1",
            required_provider_modes=("live",),
        )
    with pytest.raises(AgentToolRegistryError, match="capability_provider_modes_required"):
        AgentToolCapabilityRequirement(
            mode="exact_capability_required",
            capability_type="cohort_execution_capability_v1",
            capability_issuer=_owner("cohort.capability.issuer"),
        )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        ({"schema_digest": "x" * 64}, "request_schema_digest_invalid"),
        ({"schema_digest": "0" * 63}, "request_schema_digest_invalid"),
        ({"schema_version": " bad"}, "request_schema_version_invalid"),
    ),
)
def test_request_pin_rejects_noncanonical_identity(kwargs: dict[str, str], match: str) -> None:
    base = {
        "schema_version": "synthetic_request_v1",
        "schema_digest": _digest("synthetic_request_v1"),
        "validator_owner": _owner("agent.request.validator"),
        "action_type": "synthetic_action",
        "action_contract_digest": _digest("synthetic-action-contract-v1"),
        "query_owner": None,
    }
    base.update(kwargs)
    with pytest.raises(AgentToolRegistryError, match=match):
        AgentToolRequestPin(**base)  # type: ignore[arg-type]


def test_owner_specific_version_grammars_preserve_request_and_tool_history_only() -> None:
    request = _request(version="1_numeric_request")
    spec = _action_spec(
        request=request,
        tool_spec_version="3_numeric_tool_spec",
    )

    assert spec.request.schema_version == "1_numeric_request"
    assert spec.tool_spec_version == "3_numeric_tool_spec"
    with pytest.raises(AgentToolRegistryError, match="result_schema_version_invalid"):
        _result(version="2_numeric_result")


def test_result_validator_owner_is_current_and_legacy_history_is_explicit() -> None:
    current = _result()
    historical = _result(
        validation_contract_version="action_result_interpretation_contract_v2",
        validator_owner=_owner("sourcing_agent.model_tool_runtime.ToolSpec.validate_input"),
    )

    assert current.validator_owner.owner_id.endswith("InternalToolValidatorSpec.validate_input")
    assert historical.validator_owner.owner_id.endswith("ToolSpec.validate_input")
    with pytest.raises(AgentToolRegistryError, match="validator_owner_not_canonical"):
        _result(
            validator_owner=_owner("sourcing_agent.model_tool_runtime.ToolSpec.validate_input"),
        )
    with pytest.raises(AgentToolRegistryError, match="validation_contract_unsupported"):
        _result(validation_contract_version="action_result_interpretation_contract_v4")


def test_request_pin_owner_binding_is_closed_and_action_contract_digest_is_mandatory() -> None:
    base = {
        "schema_version": "synthetic_request_v1",
        "schema_digest": _digest("synthetic_request_v1"),
        "validator_owner": _owner("agent.request.validator"),
    }
    with pytest.raises(AgentToolRegistryError, match="request_action_contract_digest_required"):
        AgentToolRequestPin(
            **base,  # type: ignore[arg-type]
            action_type="synthetic_action",
            action_contract_digest=None,
            query_owner=None,
        )
    with pytest.raises(AgentToolRegistryError, match="request_owner_binding_invalid"):
        AgentToolRequestPin(
            **base,  # type: ignore[arg-type]
            action_type="synthetic_action",
            action_contract_digest=_digest("synthetic-action-contract-v1"),
            query_owner=_owner("operation.query.inspect"),
        )
    with pytest.raises(AgentToolRegistryError, match="request_owner_binding_invalid"):
        AgentToolRequestPin(
            **base,  # type: ignore[arg-type]
            action_type=None,
            action_contract_digest=None,
            query_owner=None,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("max_serialized_bytes", 1),
        ("max_serialized_bytes", True),
        ("max_items", 0),
        ("max_items", True),
        ("max_depth", 0),
        ("max_depth", 65),
    ),
)
def test_result_serializer_limits_fail_closed(field_name: str, value: object) -> None:
    kwargs: dict[str, Any] = {
        "tool_name": "synthetic_action",
        "tool_kind": "action",
        "action_type": "synthetic_action",
        "query_owner": None,
        "schema_version": "synthetic_result_v1",
        "schema_digest": _digest("synthetic_result_v1"),
        "serializer_owner": _owner("agent.result.serializer"),
        "validator_owner": _owner("sourcing_agent.model_tool_runtime.InternalToolValidatorSpec.validate_input"),
        "validation_contract_version": "action_result_interpretation_contract_v3",
        "max_serialized_bytes": 1024,
        "max_items": 100,
        "max_depth": 10,
    }
    kwargs[field_name] = value
    with pytest.raises(AgentToolRegistryError, match=f"result_{field_name}_invalid"):
        AgentToolResultPin(**kwargs)


@pytest.mark.parametrize(
    "tool_name",
    ("", " leading", "trailing ", "bad tool", "9starts_with_digit", "_private", "x" * 129),
)
def test_tool_name_is_normalized_and_model_safe(tool_name: str) -> None:
    with pytest.raises(AgentToolRegistryError, match="agent_tool_name_invalid"):
        replace(_action_spec(), tool_name=tool_name)


def test_description_is_nonempty_bounded_utf8_without_surrogates() -> None:
    for description in ("", " leading", "trailing ", "x" * (16 * 1024 + 1), "bad\ud800"):
        with pytest.raises(AgentToolRegistryError, match="model_description_invalid"):
            _action_spec(model_description=description)


def test_request_and_result_pins_must_match_the_exact_tool_route() -> None:
    with pytest.raises(AgentToolRegistryError, match="request_route_mismatch"):
        _action_spec(request=_request(action_type="different_action"))
    with pytest.raises(AgentToolRegistryError, match="result_route_mismatch"):
        _action_spec(result=_result(tool_name="different_tool"))

    query = _query_spec()
    foreign_query_owner = _owner(
        "operation.query.inspect",
        revision="inspect_v2",
        digest_label="operation-query-inspect-v2",
    )
    with pytest.raises(AgentToolRegistryError, match="request_route_mismatch"):
        replace(
            query,
            request=_request(
                version="inspect_operation_request_v1",
                action_type=None,
                query_owner=foreign_query_owner,
            ),
        )
    with pytest.raises(AgentToolRegistryError, match="result_route_mismatch"):
        replace(
            query,
            result=_result(
                tool_name=query.tool_name,
                action_type=None,
                query_owner=foreign_query_owner,
                version="inspect_operation_result_v1",
            ),
        )


def test_release_key_is_the_exact_action_or_query_route_key() -> None:
    with pytest.raises(AgentToolRegistryError, match="release_key_route_mismatch"):
        replace(_action_spec(), release_state_ref=_release("synthetic_action"))
    with pytest.raises(AgentToolRegistryError, match="release_key_route_mismatch"):
        replace(_query_spec(), release_state_ref=_release("action:inspect_operation"))


def test_registry_rejects_exact_duplicate_historical_specs_before_collection_collapse() -> None:
    spec = _action_spec()
    with pytest.raises(AgentToolRegistryError, match="duplicate_historical_spec:synthetic_action"):
        _registry((spec, spec))


def test_registry_retains_multiple_historical_versions_without_inferring_current() -> None:
    version_one = _action_spec()
    version_two = replace(
        version_one,
        tool_spec_version="synthetic_action_tool_v2",
        model_description="Version two description.",
    )
    registry = _registry((version_two, version_one))

    assert registry.declared_tool_count == 1
    assert registry.historical_spec_count == 2
    assert registry.specs_for_name("synthetic_action") == (version_one, version_two)
    assert registry.require_historical(*version_one.historical_identity) is version_one
    assert registry.require_historical(*version_two.historical_identity) is version_two
    assert not hasattr(registry, "current")


def test_registry_rejects_same_tool_version_with_digest_drift() -> None:
    original = _action_spec()
    drifted = replace(original, model_description="Unversioned structural drift.")
    with pytest.raises(AgentToolRegistryError, match="version_digest_conflict:synthetic_action"):
        _registry((original, drifted))


def test_historical_versions_cannot_drift_kind_route_or_release_key() -> None:
    original = _action_spec()
    version_two = replace(original, tool_spec_version="synthetic_action_tool_v2")

    with pytest.raises(AgentToolRegistryError, match="route_drift:synthetic_action"):
        _registry(
            (
                original,
                _action_spec(
                    action_type="different_action",
                    tool_spec_version="synthetic_action_tool_v2",
                ),
            )
        )
    with pytest.raises(AgentToolRegistryError, match="release_key_route_mismatch"):
        replace(version_two, release_state_ref=_release("action:different_action"))
    query_version = replace(
        _query_spec(tool_name="synthetic_action"),
        tool_spec_version="synthetic_action_tool_v2",
    )
    with pytest.raises(AgentToolRegistryError, match="kind_drift:synthetic_action"):
        _registry((original, query_version))


def test_registry_rejects_action_and_query_route_collisions() -> None:
    action_a = _action_spec(tool_name="action_a", action_type="shared_action")
    action_b = _action_spec(tool_name="action_b", action_type="shared_action")
    with pytest.raises(AgentToolRegistryError, match="route_collision:shared_action"):
        _registry((action_a, action_b))

    query_a = _query_spec(tool_name="query_a", query_owner_id="query.shared")
    query_b = _query_spec(tool_name="query_b", query_owner_id="query.shared")
    with pytest.raises(AgentToolRegistryError, match="route_collision:query.shared"):
        _registry((query_a, query_b))


def test_registry_release_owner_rotation_preserves_exact_history_without_ambiguous_current() -> None:
    release_v1 = _owner("agent.release.registry", revision="registry_v1")
    release_v2 = _owner(
        "agent.release.registry",
        revision="registry_v2",
        digest_label="agent-release-registry-v2",
    )
    version_one = _action_spec()
    version_one = replace(
        version_one,
        release_state_ref=_release("action:synthetic_action", owner=release_v1),
    )
    version_two = replace(
        version_one,
        tool_spec_version="synthetic_action_tool_v2",
        release_state_ref=_release("action:synthetic_action", owner=release_v2),
    )

    registry_v1 = _registry((version_one,), current_release_owner=release_v1)
    registry_v2 = _registry((version_one, version_two), current_release_owner=release_v2)
    rotated_without_rewrite = replace(registry_v2, current_release_owner=release_v1)

    assert registry_v2.current_release_owner is release_v2
    assert registry_v2.to_manifest_record()["current_release_owner"] == release_v2.to_fingerprint_record()
    assert registry_v2.require_historical(*version_one.historical_identity) is version_one
    assert registry_v2.require_historical(*version_two.historical_identity) is version_two
    assert (
        registry_v1.require_historical(*version_one.historical_identity).release_state_ref.release_owner is release_v1
    )
    assert rotated_without_rewrite.specs == registry_v2.specs
    assert rotated_without_rewrite.registry_digest != registry_v2.registry_digest
    with pytest.raises(AgentToolRegistryError, match="current_release_owner_required"):
        AgentToolRegistry((version_one,))


def test_registry_constructor_requires_a_tuple_so_mapping_inputs_cannot_hide_duplicates() -> None:
    invalid_mapping: Any = {"wrong_name": _action_spec()}
    with pytest.raises(AgentToolRegistryError, match="registry_specs_invalid"):
        AgentToolRegistry(invalid_mapping)
    with pytest.raises(AgentToolRegistryError, match="registry_specs_invalid"):
        AgentToolRegistry.from_specs(
            invalid_mapping,
            current_release_owner=_owner("agent.release.registry"),
        )


def test_registry_historical_lookup_is_exact_and_unknown_pins_fail_closed() -> None:
    action = _action_spec()
    query = _query_spec()
    registry = _registry((query, action))

    assert registry.tool_names == ("inspect_operation", "synthetic_action")
    assert registry.declared_tool_count == 2
    assert registry.specs_for_name(action.tool_name) == (action,)
    assert registry.require_historical(*action.historical_identity) is action
    assert registry.get_historical(action.tool_name, action.tool_spec_version, "f" * 64) is None
    assert registry.get_historical(query.tool_name, action.tool_spec_version, action.tool_spec_digest) is None
    assert registry.specs_for_name("missing") == ()
    with pytest.raises(AgentToolRegistryError, match="historical_spec_missing"):
        registry.require_historical(action.tool_name, action.tool_spec_version, "f" * 64)


def test_registry_manifest_and_digest_are_order_independent_and_bind_tool_specs() -> None:
    action = _action_spec()
    query = _query_spec()
    forward = _registry((action, query))
    reverse = _registry((query, action))

    assert forward.to_manifest_record() == reverse.to_manifest_record()
    assert forward.registry_digest == reverse.registry_digest
    changed = _registry((replace(action, model_description="Changed description."), query))
    assert changed.registry_digest != forward.registry_digest


def test_release_reference_contains_no_mutable_state_or_transition_fields() -> None:
    spec = _action_spec()
    release_record = spec.to_manifest_record()["release_state_ref"]

    assert isinstance(release_record, dict)
    assert set(release_record) == {"release_owner", "release_key"}
    assert not ({"state", "release_state", "activation_epoch", "served", "model_visible"} & set(release_record))
    assert "result_slot_id" not in spec.to_manifest_record()
    assert "invocation_id" not in spec.to_manifest_record()


def test_registry_has_no_provider_model_storage_or_release_transition_methods() -> None:
    forbidden = {
        "activate",
        "transition",
        "serve",
        "invoke",
        "dispatch",
        "persist",
        "reserve_budget",
        "issue_capability",
        "accept_result_slot",
    }
    assert forbidden.isdisjoint(dir(AgentToolRegistry))
    assert forbidden.isdisjoint(dir(AgentToolSpec))
