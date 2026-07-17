"""Exact four-tool declaration registry for the isolated local Agent canary.

This registry is intentionally separate from ``DEFAULT_AGENT_TOOL_REGISTRY``.
It provides immutable structural identities and deterministic simulate-fixture
pins, but owns no release decision, catalog projection, serving predicate, or
execution capability.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from .acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
    ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC,
    ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
)
from .acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    ACQUISITION_START_V2_REQUEST_TOOL_SPEC,
    ACQUISITION_START_V2_RESULT_SPEC,
)
from .action_contract_identity import action_contract_digest
from .action_result_schema import ACTION_RESULT_VALIDATOR_OWNER, ActionResultSpec
from .agent_projection_query import (
    FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION,
    FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC,
    FILTER_PROJECTION_V2_RESULT_SPEC,
    INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST,
    INSPECT_OPERATION_QUERY_OWNER_ID,
    INSPECT_OPERATION_QUERY_OWNER_REVISION,
    INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST,
    INSPECT_OPERATION_REQUEST_SCHEMA_VERSION,
    INSPECT_OPERATION_RESULT_SPEC,
)
from .agent_tool_registry import (
    AGENT_TOOL_SPEC_SCHEMA_VERSION_V2,
    AgentActionToolRoute,
    AgentExecutionSubjectRequirement,
    AgentQueryToolRoute,
    AgentToolApprovalRequirement,
    AgentToolBehavior,
    AgentToolBudgetRequirement,
    AgentToolCapabilityRequirement,
    AgentToolOwnerPin,
    AgentToolRegistry,
    AgentToolReleaseStateRef,
    AgentToolRequestPin,
    AgentToolResultPin,
    AgentToolSimulateFixturePin,
    AgentToolSpec,
)
from .operation_runtime import (
    ACTION_FILTER_PROJECTION,
    ACTION_PLAN_ACQUISITION,
    ACTION_START_ACQUISITION_RUN,
    DEFAULT_ACTION_REGISTRY,
    ActionRequestSpec,
)

LOCAL_CANARY_REGISTRY_SCHEMA_VERSION = "local_agent_canary_registry_v1"
LOCAL_CANARY_OWNER_CONTRACT_SCHEMA_VERSION = "local_agent_canary_owner_contract_v1"
LOCAL_CANARY_EXECUTION_SUBJECT_SCHEMA_VERSION = "local_agent_execution_subject_v1"
LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION = "local_agent_simulate_fixture_v1"
LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION_V2 = "local_agent_simulate_fixture_v2"
LOCAL_CANARY_TOOL_NAMES = (
    ACTION_PLAN_ACQUISITION,
    ACTION_START_ACQUISITION_RUN,
    "inspect_operation",
    ACTION_FILTER_PROJECTION,
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _owner_pin(
    owner_id: str,
    owner_revision: str,
    contract: Mapping[str, Any],
) -> AgentToolOwnerPin:
    record = {
        "schema_version": LOCAL_CANARY_OWNER_CONTRACT_SCHEMA_VERSION,
        "owner_id": owner_id,
        "owner_revision": owner_revision,
        "contract": dict(contract),
    }
    return AgentToolOwnerPin(
        owner_id=owner_id,
        owner_revision=owner_revision,
        owner_contract_digest=_sha256_json(record),
    )


_REQUEST_VALIDATOR_OWNER = _owner_pin(
    "sourcing_agent.model_tool_runtime.ToolSpec.validate_input",
    "tool_spec_validator_v1",
    {
        "input": "closed_json_schema",
        "output": "canonical_validated_json",
        "additional_properties": "reject",
        "side_effects": "none",
    },
)

_LOCAL_RELEASE_OWNER = _owner_pin(
    "agent.local_canary.release_owner",
    "local_canary_release_owner_v1",
    {
        "scope": "isolated_local_harness_only",
        "registry_presence_is_release": False,
        "global_serving_authority": False,
        "hosted_activation_authority": False,
    },
)

_EXECUTION_SUBJECT_CONTRACT = {
    "schema_version": LOCAL_CANARY_EXECUTION_SUBJECT_SCHEMA_VERSION,
    "required_fields": ["runtime_namespace", "workspace_id", "actor_id", "provider_mode"],
    "runtime_namespace": "isolated_local_canary",
    "provider_modes": ["simulate", "scripted", "live"],
    "global_serving_authority": False,
}
_EXECUTION_SUBJECT_VALIDATOR = _owner_pin(
    "agent.local_canary.execution_subject_validator",
    "local_canary_execution_subject_validator_v1",
    _EXECUTION_SUBJECT_CONTRACT,
)
_EXECUTION_PERMISSION_POLICY = _owner_pin(
    "agent.local_canary.permission_policy",
    "local_canary_permission_policy_v1",
    {
        "scope": "exact_workspace_actor_and_isolated_runtime",
        "hosted": False,
        "provider_modes": ["simulate", "scripted", "live"],
        "live_requires_paid_canary_receipt": True,
    },
)

_NO_APPROVAL_POLICY = _owner_pin(
    "agent.approval.not_required_policy",
    "approval_not_required_v1",
    {"mode": "not_required", "self_approval": False},
)
_START_APPROVAL_POLICY = _owner_pin(
    "acquisition.acquisition_confirmation_receipt_policy",
    "acquisition_confirmation_policy_v1",
    {
        "mode": "human_confirmation_required",
        "receipt_schema_version": "acquisition_confirmation_receipt.v1",
        "model_self_approval": False,
        "exact_preview_reload": True,
    },
)
_START_BUDGET_OWNER = _owner_pin(
    "acquisition.parent_budget_reservation",
    "acquisition_parent_budget_v1",
    {
        "source": "approved_acquisition_start_snapshot",
        "reservation_before_dispatch": True,
    },
)
_START_CAPABILITY_ISSUER = _owner_pin(
    "acquisition.cohort_provider_capability_issuer",
    "cohort_provider_capability_v1",
    {
        "capability_type": "cohort_provider_execution",
        "required_provider_modes": ["live"],
        "simulate_or_scripted_grants_live_authority": False,
    },
)


def _execution_subject(*, approval: bool = False, dispatch: bool = False) -> AgentExecutionSubjectRequirement:
    checkpoints = ["catalog_projection", "invocation_acceptance"]
    if approval:
        checkpoints.append("approval_acceptance")
    if dispatch:
        checkpoints.append("dispatch_acceptance")
    return AgentExecutionSubjectRequirement(
        subject_schema_version=LOCAL_CANARY_EXECUTION_SUBJECT_SCHEMA_VERSION,
        subject_schema_digest=_sha256_json(_EXECUTION_SUBJECT_CONTRACT),
        subject_validator_owner=_EXECUTION_SUBJECT_VALIDATOR,
        permission_policy=_EXECUTION_PERMISSION_POLICY,
        authorization_checkpoints=tuple(checkpoints),
    )


def _action_spec_with_request(
    action_type: str,
    *,
    request_schema: Mapping[str, Any],
    request_schema_version: str,
    request_identity_target_fields: tuple[str, ...],
) -> ActionRequestSpec:
    return replace(
        DEFAULT_ACTION_REGISTRY.spec_for(action_type),
        request_schema=request_schema,
        request_schema_version=request_schema_version,
        request_identity_target_fields=request_identity_target_fields,
        target_ref_field_aliases=(),
    )


PLAN_ACQUISITION_CANARY_ACTION_SPEC = _action_spec_with_request(
    ACTION_PLAN_ACQUISITION,
    request_schema=ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC.input_schema,
    request_schema_version=ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
    request_identity_target_fields=("workspace_id", "requester_id", "company_target"),
)
START_ACQUISITION_V2_CANARY_ACTION_SPEC = _action_spec_with_request(
    ACTION_START_ACQUISITION_RUN,
    request_schema=ACQUISITION_START_V2_REQUEST_TOOL_SPEC.input_schema,
    request_schema_version=ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    request_identity_target_fields=("workspace_id", "requester_id", "start_snapshot"),
)
FILTER_PROJECTION_V2_CANARY_ACTION_SPEC = _action_spec_with_request(
    ACTION_FILTER_PROJECTION,
    request_schema=FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.input_schema,
    request_schema_version=FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION,
    request_identity_target_fields=(
        "projection_id",
        "membership_revision",
        "cohort_selection_registry_version",
        "cohort_selection_registry_digest",
        "cohort_selection_digest",
    ),
)

LOCAL_CANARY_ACTION_SPECS: Mapping[str, ActionRequestSpec] = MappingProxyType(
    {
        ACTION_PLAN_ACQUISITION: PLAN_ACQUISITION_CANARY_ACTION_SPEC,
        ACTION_START_ACQUISITION_RUN: START_ACQUISITION_V2_CANARY_ACTION_SPEC,
        ACTION_FILTER_PROJECTION: FILTER_PROJECTION_V2_CANARY_ACTION_SPEC,
    }
)


def _action_contract_control_pin(spec: ActionRequestSpec) -> AgentToolOwnerPin:
    return _owner_pin(
        "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
        "action_control_policy_v1",
        {
            "action_type": spec.action_type,
            "action_contract_digest": action_contract_digest(DEFAULT_ACTION_REGISTRY, spec),
            "command_exposure": "owner_allowlist_only",
            "fallback_status": "fail_closed",
        },
    )


_QUERY_CONTROL_PIN = _owner_pin(
    "operation_query_service.canonical_control_projection",
    "operation_query_control_policy_v1",
    {
        "source": "operation_runtime.operation_run_control_state",
        "next_control_inference": False,
        "writes": False,
    },
)


def _request_pin(spec: ActionRequestSpec) -> AgentToolRequestPin:
    return AgentToolRequestPin(
        schema_version=spec.request_schema_version,
        schema_digest=spec.request_schema_digest,
        validator_owner=_REQUEST_VALIDATOR_OWNER,
        action_type=spec.action_type,
        action_contract_digest=action_contract_digest(DEFAULT_ACTION_REGISTRY, spec),
        query_owner=None,
    )


_INSPECT_QUERY_OWNER = AgentToolOwnerPin(
    owner_id=INSPECT_OPERATION_QUERY_OWNER_ID,
    owner_revision=INSPECT_OPERATION_QUERY_OWNER_REVISION,
    owner_contract_digest=INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST,
)


def _query_request_pin() -> AgentToolRequestPin:
    return AgentToolRequestPin(
        schema_version=INSPECT_OPERATION_REQUEST_SCHEMA_VERSION,
        schema_digest=INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST,
        validator_owner=_REQUEST_VALIDATOR_OWNER,
        action_type=None,
        action_contract_digest=None,
        query_owner=_INSPECT_QUERY_OWNER,
    )


def _result_pin(spec: ActionResultSpec) -> AgentToolResultPin:
    query_owner = _INSPECT_QUERY_OWNER if spec.tool_kind == "query" else None
    return AgentToolResultPin(
        tool_name=spec.tool_name,
        tool_kind=spec.tool_kind,
        action_type=spec.action_type,
        query_owner=query_owner,
        schema_version=spec.result_schema_version,
        schema_digest=spec.result_schema_digest,
        serializer_owner=AgentToolOwnerPin(
            owner_id=spec.serializer_owner,
            owner_revision=spec.serializer_revision,
            owner_contract_digest=spec.serializer_contract_digest,
        ),
        validator_owner=AgentToolOwnerPin(
            owner_id=ACTION_RESULT_VALIDATOR_OWNER,
            owner_revision="internal_tool_validator_v3",
            owner_contract_digest=spec.interpretation_contract_digest,
        ),
        validation_contract_version=spec.interpretation_contract_version,
        max_serialized_bytes=spec.max_serialized_bytes,
        max_items=spec.max_items,
        max_depth=spec.max_depth,
    )


def _fixture_record(
    tool_name: str,
    *,
    fixture_revision: str,
    approval_required: bool,
    effect_class: str,
    result_link_policy: str = "",
    schema_version: str = LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION,
) -> dict[str, Any]:
    record = {
        "schema_version": schema_version,
        "fixture_id": f"agent.local_canary.simulate.{tool_name}",
        "fixture_revision": fixture_revision,
        "tool_name": tool_name,
        "provider_mode": "simulate",
        "expected_terminal_variant": "success",
        "approval_required": approval_required,
        "effect_class": effect_class,
        "expected_live_provider_invocations": 0,
        "expected_live_model_invocations": 0,
    }
    if result_link_policy:
        record["result_link_policy"] = result_link_policy
    return record


_START_ACQUISITION_RUN_V2_FIXTURE_RECORD = _fixture_record(
    ACTION_START_ACQUISITION_RUN,
    fixture_revision="start_acquisition_run_fixture_v2",
    approval_required=True,
    effect_class="command_backed_action",
)


_FIXTURE_RECORDS = {
    ACTION_PLAN_ACQUISITION: _fixture_record(
        ACTION_PLAN_ACQUISITION,
        fixture_revision="plan_acquisition_fixture_v1",
        approval_required=False,
        effect_class="commandless_action",
    ),
    ACTION_START_ACQUISITION_RUN: _fixture_record(
        ACTION_START_ACQUISITION_RUN,
        fixture_revision="start_acquisition_run_fixture_v3",
        approval_required=True,
        effect_class="command_backed_action",
        result_link_policy="workflow_command_acceptance_v1",
        schema_version=LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION_V2,
    ),
    ACTION_FILTER_PROJECTION: _fixture_record(
        ACTION_FILTER_PROJECTION,
        fixture_revision="filter_projection_fixture_v2",
        approval_required=False,
        effect_class="read_only",
    ),
    "inspect_operation": _fixture_record(
        "inspect_operation",
        fixture_revision="inspect_operation_fixture_v2",
        approval_required=False,
        effect_class="read_only",
    ),
}
LOCAL_CANARY_SIMULATE_FIXTURES: Mapping[str, Mapping[str, Any]] = MappingProxyType(
    {name: MappingProxyType(record) for name, record in _FIXTURE_RECORDS.items()}
)


def _fixture_pin_from_record(record: Mapping[str, Any]) -> AgentToolSimulateFixturePin:
    canonical_record = dict(record)
    return AgentToolSimulateFixturePin(
        fixture_id=str(canonical_record["fixture_id"]),
        fixture_revision=str(canonical_record["fixture_revision"]),
        fixture_digest=_sha256_json(canonical_record),
    )


def _fixture_pin(tool_name: str) -> AgentToolSimulateFixturePin:
    return _fixture_pin_from_record(LOCAL_CANARY_SIMULATE_FIXTURES[tool_name])


def _release_ref(action_type: str | None, tool_name: str) -> AgentToolReleaseStateRef:
    return AgentToolReleaseStateRef(
        release_owner=_LOCAL_RELEASE_OWNER,
        release_key=f"action:{action_type}" if action_type is not None else f"query:{tool_name}",
    )


def _approval(required: bool) -> AgentToolApprovalRequirement:
    return AgentToolApprovalRequirement(
        mode="human_confirmation_required" if required else "not_required",
        approval_policy=_START_APPROVAL_POLICY if required else _NO_APPROVAL_POLICY,
    )


def _route_pin(owner_id: str, owner_revision: str, contract: Mapping[str, Any]) -> AgentToolOwnerPin:
    return _owner_pin(owner_id, owner_revision, contract)


PLAN_ACQUISITION_TOOL_SPEC = AgentToolSpec(
    tool_spec_version="plan_acquisition_tool_v1",
    tool_name=ACTION_PLAN_ACQUISITION,
    model_description="Build a capability-free immutable acquisition plan preview for explicit user review.",
    tool_kind="action",
    request=_request_pin(PLAN_ACQUISITION_CANARY_ACTION_SPEC),
    result=_result_pin(ACQUISITION_PLAN_PREVIEW_RESULT_SPEC),
    route=AgentActionToolRoute(
        action_type=ACTION_PLAN_ACQUISITION,
        workspace_actor_binder=_route_pin(
            "planner.acquisition_plan_preview.workspace_actor_binder",
            "acquisition_plan_preview_binder_v1",
            {
                "server_owned": ["workspace_id", "requester_id", "company_target"],
                "caller_owned": [
                    "cohort_selection",
                    "source_preferences",
                    "coverage_intent",
                    "thematic_constraints",
                    "provider_mode_intent",
                    "budget",
                ],
            },
        ),
        adapter=_route_pin(
            "planner.acquisition_plan_preview.pg_uow",
            "acquisition_plan_preview_pg_uow_v1",
            {
                "method": "WorkflowRuntimeRepository.create_acquisition_plan_preview_uow",
                "terminal_success": True,
                "workflow_command_count": 0,
                "provider_or_model_calls": 0,
            },
        ),
    ),
    simulate_fixture=_fixture_pin(ACTION_PLAN_ACQUISITION),
    release_state_ref=_release_ref(ACTION_PLAN_ACQUISITION, ACTION_PLAN_ACQUISITION),
    execution_subject=_execution_subject(),
    budget=AgentToolBudgetRequirement(mode="not_required"),
    capability=AgentToolCapabilityRequirement(mode="not_required"),
    behavior=AgentToolBehavior(
        effect_class="commandless_action",
        command_exposure="none",
        approval=_approval(False),
        control_policy=_action_contract_control_pin(PLAN_ACQUISITION_CANARY_ACTION_SPEC),
    ),
)

START_ACQUISITION_RUN_TOOL_SPEC_V2 = AgentToolSpec(
    tool_spec_version="start_acquisition_run_tool_v2",
    tool_name=ACTION_START_ACQUISITION_RUN,
    model_description="Start one acquisition only after exact immutable preview confirmation.",
    tool_kind="action",
    request=_request_pin(START_ACQUISITION_V2_CANARY_ACTION_SPEC),
    result=_result_pin(ACQUISITION_START_V2_RESULT_SPEC),
    route=AgentActionToolRoute(
        action_type=ACTION_START_ACQUISITION_RUN,
        workspace_actor_binder=_route_pin(
            "sourcing_agent.acquisition_start_v2.AcquisitionStartV2OwnerBinder",
            "acquisition_start_v2_binder_v1",
            {
                "preflight": "workspace+requester+preview_id+revision+digest",
                "approval_reload": True,
                "missing_foreign_conflict": "one_zero_write_class",
            },
        ),
        adapter=_route_pin(
            "acquisition.start_v2_pg_uow",
            "acquisition_start_v2_pg_uow_v1",
            {
                "writes": ["approval_receipt", "operation_run", "workflow_command", "budget", "event"],
                "single_transaction": True,
                "implementation_gate": "S1_pg_simulate",
            },
        ),
    ),
    simulate_fixture=_fixture_pin_from_record(_START_ACQUISITION_RUN_V2_FIXTURE_RECORD),
    release_state_ref=_release_ref(ACTION_START_ACQUISITION_RUN, ACTION_START_ACQUISITION_RUN),
    execution_subject=_execution_subject(approval=True, dispatch=True),
    budget=AgentToolBudgetRequirement(mode="parent_reservation_required", budget_owner=_START_BUDGET_OWNER),
    capability=AgentToolCapabilityRequirement(
        mode="exact_capability_required",
        capability_type="cohort_provider_execution",
        capability_issuer=_START_CAPABILITY_ISSUER,
        required_provider_modes=("live",),
    ),
    behavior=AgentToolBehavior(
        effect_class="command_backed_action",
        command_exposure="owner_command_only",
        approval=_approval(True),
        control_policy=_action_contract_control_pin(START_ACQUISITION_V2_CANARY_ACTION_SPEC),
    ),
)

START_ACQUISITION_RUN_TOOL_SPEC = replace(
    START_ACQUISITION_RUN_TOOL_SPEC_V2,
    tool_spec_version="start_acquisition_run_tool_v3",
    simulate_fixture=_fixture_pin(ACTION_START_ACQUISITION_RUN),
    behavior=replace(
        START_ACQUISITION_RUN_TOOL_SPEC_V2.behavior,
        explicit_result_link_policy="workflow_command_acceptance_v1",
    ),
    fingerprint_schema_version=AGENT_TOOL_SPEC_SCHEMA_VERSION_V2,
)

FILTER_PROJECTION_TOOL_SPEC = AgentToolSpec(
    tool_spec_version="filter_projection_tool_v2",
    tool_name=ACTION_FILTER_PROJECTION,
    model_description="Filter one exact canonical projection with the user-confirmed Cohort selection.",
    tool_kind="action",
    request=_request_pin(FILTER_PROJECTION_V2_CANARY_ACTION_SPEC),
    result=_result_pin(FILTER_PROJECTION_V2_RESULT_SPEC),
    route=AgentActionToolRoute(
        action_type=ACTION_FILTER_PROJECTION,
        workspace_actor_binder=_route_pin(
            "sourcing_agent.agent_projection_query.bind_filter_projection_v2_request",
            "filter_projection_v2_binder_v1",
            {
                "caller_owned": ["cohort_selection", "offset", "limit"],
                "owner_pins": ["projection_id", "membership_revision", "registry", "selection_digest"],
            },
        ),
        adapter=_route_pin(
            "sourcing_agent.agent_projection_query.execute_filter_projection_v2",
            "filter_projection_v2_adapter_v1",
            {"effects": "read_only", "raw_candidate_identity_serialized": False},
        ),
    ),
    simulate_fixture=_fixture_pin(ACTION_FILTER_PROJECTION),
    release_state_ref=_release_ref(ACTION_FILTER_PROJECTION, ACTION_FILTER_PROJECTION),
    execution_subject=_execution_subject(),
    budget=AgentToolBudgetRequirement(mode="not_required"),
    capability=AgentToolCapabilityRequirement(mode="not_required"),
    behavior=AgentToolBehavior(
        effect_class="read_only",
        command_exposure="none",
        approval=_approval(False),
        control_policy=_action_contract_control_pin(FILTER_PROJECTION_V2_CANARY_ACTION_SPEC),
    ),
)

INSPECT_OPERATION_TOOL_SPEC = AgentToolSpec(
    tool_spec_version="inspect_operation_tool_v2",
    tool_name="inspect_operation",
    model_description="Read bounded canonical operation state without repairing or controlling it.",
    tool_kind="query",
    request=_query_request_pin(),
    result=_result_pin(INSPECT_OPERATION_RESULT_SPEC),
    route=AgentQueryToolRoute(
        query_owner=_INSPECT_QUERY_OWNER,
        workspace_actor_binder=_route_pin(
            "sourcing_agent.agent_projection_query.bind_inspect_operation_request",
            "inspect_operation_binder_v1",
            {"owner_preflight": "workspace+action+operation_run", "actor_from_transport": True},
        ),
        adapter=_route_pin(
            "sourcing_agent.agent_projection_query.execute_inspect_operation",
            "inspect_operation_adapter_v2",
            {"effects": "read_only", "repair": False, "control_inference": False},
        ),
    ),
    simulate_fixture=_fixture_pin("inspect_operation"),
    release_state_ref=_release_ref(None, "inspect_operation"),
    execution_subject=_execution_subject(),
    budget=AgentToolBudgetRequirement(mode="not_required"),
    capability=AgentToolCapabilityRequirement(mode="not_required"),
    behavior=AgentToolBehavior(
        effect_class="read_only",
        command_exposure="none",
        approval=_approval(False),
        control_policy=_QUERY_CONTROL_PIN,
    ),
)

LOCAL_CANARY_AGENT_TOOL_REGISTRY = AgentToolRegistry.from_specs(
    (
        PLAN_ACQUISITION_TOOL_SPEC,
        START_ACQUISITION_RUN_TOOL_SPEC_V2,
        START_ACQUISITION_RUN_TOOL_SPEC,
        INSPECT_OPERATION_TOOL_SPEC,
        FILTER_PROJECTION_TOOL_SPEC,
    ),
    current_release_owner=_LOCAL_RELEASE_OWNER,
)


def local_canary_registry_record() -> dict[str, Any]:
    """Return a defensive declaration manifest with an explicit non-serving scope."""

    return {
        "schema_version": LOCAL_CANARY_REGISTRY_SCHEMA_VERSION,
        "scope": "isolated_local_harness_only",
        "global_serving_authority": False,
        "hosted_activation_authority": False,
        "registry": LOCAL_CANARY_AGENT_TOOL_REGISTRY.to_manifest_record(),
    }


__all__ = [
    "FILTER_PROJECTION_TOOL_SPEC",
    "FILTER_PROJECTION_V2_CANARY_ACTION_SPEC",
    "INSPECT_OPERATION_TOOL_SPEC",
    "LOCAL_CANARY_ACTION_SPECS",
    "LOCAL_CANARY_AGENT_TOOL_REGISTRY",
    "LOCAL_CANARY_EXECUTION_SUBJECT_SCHEMA_VERSION",
    "LOCAL_CANARY_OWNER_CONTRACT_SCHEMA_VERSION",
    "LOCAL_CANARY_REGISTRY_SCHEMA_VERSION",
    "LOCAL_CANARY_SIMULATE_FIXTURES",
    "LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION",
    "LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION_V2",
    "LOCAL_CANARY_TOOL_NAMES",
    "PLAN_ACQUISITION_CANARY_ACTION_SPEC",
    "PLAN_ACQUISITION_TOOL_SPEC",
    "START_ACQUISITION_RUN_TOOL_SPEC",
    "START_ACQUISITION_RUN_TOOL_SPEC_V2",
    "START_ACQUISITION_V2_CANARY_ACTION_SPEC",
    "local_canary_registry_record",
]
