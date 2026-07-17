from __future__ import annotations

import hashlib
import inspect
import json

from sourcing_agent.acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
)
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_RESULT_SPEC,
)
from sourcing_agent.action_contract_identity import action_contract_digest
from sourcing_agent.agent_canary_registry import (
    FILTER_PROJECTION_TOOL_SPEC,
    FILTER_PROJECTION_V2_CANARY_ACTION_SPEC,
    INSPECT_OPERATION_TOOL_SPEC,
    LOCAL_CANARY_ACTION_SPECS,
    LOCAL_CANARY_AGENT_TOOL_REGISTRY,
    LOCAL_CANARY_SIMULATE_FIXTURES,
    LOCAL_CANARY_TOOL_NAMES,
    PLAN_ACQUISITION_CANARY_ACTION_SPEC,
    PLAN_ACQUISITION_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC,
    START_ACQUISITION_V2_CANARY_ACTION_SPEC,
    local_canary_registry_record,
)
from sourcing_agent.agent_projection_query import (
    FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST,
    FILTER_PROJECTION_V2_RESULT_SPEC,
    INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST,
    INSPECT_OPERATION_QUERY_OWNER_ID,
    INSPECT_OPERATION_QUERY_OWNER_REVISION,
    INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST,
    INSPECT_OPERATION_RESULT_SPEC,
)
from sourcing_agent.agent_tool_registry import DEFAULT_AGENT_TOOL_REGISTRY
from sourcing_agent.operation_runtime import DEFAULT_ACTION_REGISTRY


def _fixture_digest(tool_name: str) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(LOCAL_CANARY_SIMULATE_FIXTURES[tool_name]),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def test_local_canary_registry_has_exact_four_tool_population_without_global_serving() -> None:
    assert LOCAL_CANARY_AGENT_TOOL_REGISTRY.declared_tool_count == 4
    assert set(LOCAL_CANARY_AGENT_TOOL_REGISTRY.tool_names) == set(LOCAL_CANARY_TOOL_NAMES)
    assert LOCAL_CANARY_AGENT_TOOL_REGISTRY.historical_spec_count == 4
    assert DEFAULT_AGENT_TOOL_REGISTRY.declared_tool_count == 0
    assert DEFAULT_AGENT_TOOL_REGISTRY.tool_names == ()

    record = local_canary_registry_record()
    assert record["scope"] == "isolated_local_harness_only"
    assert record["global_serving_authority"] is False
    assert record["hosted_activation_authority"] is False
    assert record["registry"]["declared_tool_count"] == 4
    assert not hasattr(LOCAL_CANARY_AGENT_TOOL_REGISTRY, "served")
    assert not hasattr(LOCAL_CANARY_AGENT_TOOL_REGISTRY, "model_visible")


def test_all_action_tool_request_pins_use_exact_successor_schema_and_full_action_contract() -> None:
    expected = {
        "plan_acquisition": (
            PLAN_ACQUISITION_CANARY_ACTION_SPEC,
            ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
            PLAN_ACQUISITION_TOOL_SPEC,
        ),
        "start_acquisition_run": (
            START_ACQUISITION_V2_CANARY_ACTION_SPEC,
            ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
            START_ACQUISITION_RUN_TOOL_SPEC,
        ),
        "filter_projection": (
            FILTER_PROJECTION_V2_CANARY_ACTION_SPEC,
            FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST,
            FILTER_PROJECTION_TOOL_SPEC,
        ),
    }
    assert set(LOCAL_CANARY_ACTION_SPECS) == set(expected)
    for action_type, (action_spec, schema_digest, tool_spec) in expected.items():
        assert tool_spec.request.action_type == action_type
        assert tool_spec.request.schema_version == action_spec.request_schema_version
        assert tool_spec.request.schema_digest == schema_digest == action_spec.request_schema_digest
        assert tool_spec.request.action_contract_digest == action_contract_digest(DEFAULT_ACTION_REGISTRY, action_spec)


def test_result_pins_exactly_match_each_f1_contract_and_historical_lookup() -> None:
    expected = {
        "plan_acquisition": (PLAN_ACQUISITION_TOOL_SPEC, ACQUISITION_PLAN_PREVIEW_RESULT_SPEC),
        "start_acquisition_run": (START_ACQUISITION_RUN_TOOL_SPEC, ACQUISITION_START_V2_RESULT_SPEC),
        "filter_projection": (FILTER_PROJECTION_TOOL_SPEC, FILTER_PROJECTION_V2_RESULT_SPEC),
        "inspect_operation": (INSPECT_OPERATION_TOOL_SPEC, INSPECT_OPERATION_RESULT_SPEC),
    }
    for tool_name, (tool_spec, result_spec) in expected.items():
        assert tool_spec.result.schema_version == result_spec.result_schema_version
        assert tool_spec.result.schema_digest == result_spec.result_schema_digest
        assert tool_spec.result.serializer_owner.owner_id == result_spec.serializer_owner
        assert tool_spec.result.serializer_owner.owner_revision == result_spec.serializer_revision
        assert tool_spec.result.serializer_owner.owner_contract_digest == result_spec.serializer_contract_digest
        assert LOCAL_CANARY_AGENT_TOOL_REGISTRY.require_historical(*tool_spec.historical_identity) is tool_spec
        assert tool_spec.tool_name == tool_name


def test_effects_cover_commandless_command_backed_action_read_and_query_without_semantic_faking() -> None:
    assert PLAN_ACQUISITION_TOOL_SPEC.tool_kind == "action"
    assert PLAN_ACQUISITION_TOOL_SPEC.behavior.effect_class == "commandless_action"
    assert PLAN_ACQUISITION_TOOL_SPEC.behavior.command_exposure == "none"

    assert START_ACQUISITION_RUN_TOOL_SPEC.tool_kind == "action"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.effect_class == "command_backed_action"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.approval.required is True
    assert START_ACQUISITION_RUN_TOOL_SPEC.budget.required is True
    assert START_ACQUISITION_RUN_TOOL_SPEC.capability.required_provider_modes == ("live",)

    assert FILTER_PROJECTION_TOOL_SPEC.tool_kind == "action"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.command_exposure == "none"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.approval.required is False

    assert INSPECT_OPERATION_TOOL_SPEC.tool_kind == "query"
    assert INSPECT_OPERATION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert INSPECT_OPERATION_TOOL_SPEC.action_type is None
    assert INSPECT_OPERATION_TOOL_SPEC.query_owner_id == INSPECT_OPERATION_QUERY_OWNER_ID


def test_inspect_query_pins_do_not_create_a_sixteenth_action() -> None:
    assert len(DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)) == 15
    assert INSPECT_OPERATION_TOOL_SPEC.tool_spec_version == "inspect_operation_tool_v2"
    assert INSPECT_OPERATION_TOOL_SPEC.request.schema_digest == INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST
    query_owner = INSPECT_OPERATION_TOOL_SPEC.request.query_owner
    assert query_owner is not None
    assert query_owner.owner_id == INSPECT_OPERATION_QUERY_OWNER_ID
    assert query_owner.owner_revision == INSPECT_OPERATION_QUERY_OWNER_REVISION == "inspect_operation_v2"
    assert query_owner.owner_contract_digest == INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST
    assert INSPECT_OPERATION_TOOL_SPEC.result.query_owner == query_owner
    assert INSPECT_OPERATION_RESULT_SPEC.result_schema_version == "inspect_operation_result_v2"
    assert INSPECT_OPERATION_RESULT_SPEC.serializer_revision == "inspect_operation_result_serializer_v2"
    assert INSPECT_OPERATION_TOOL_SPEC.route.adapter.owner_revision == "inspect_operation_adapter_v2"
    assert INSPECT_OPERATION_TOOL_SPEC.simulate_fixture.fixture_revision == "inspect_operation_fixture_v2"


def test_simulate_fixture_pins_are_real_content_digests_and_require_terminal_success() -> None:
    for tool_name in LOCAL_CANARY_TOOL_NAMES:
        fixture = dict(LOCAL_CANARY_SIMULATE_FIXTURES[tool_name])
        specs = LOCAL_CANARY_AGENT_TOOL_REGISTRY.specs_for_name(tool_name)
        assert len(specs) == 1
        assert fixture["provider_mode"] == "simulate"
        assert fixture["expected_terminal_variant"] == "success"
        assert fixture["expected_live_provider_invocations"] == 0
        assert fixture["expected_live_model_invocations"] == 0
        assert specs[0].simulate_fixture.fixture_id == fixture["fixture_id"]
        assert specs[0].simulate_fixture.fixture_revision == fixture["fixture_revision"]
        assert specs[0].simulate_fixture.fixture_digest == _fixture_digest(tool_name)


def test_global_historical_v1_contracts_are_not_rewritten_by_local_successors() -> None:
    assert DEFAULT_ACTION_REGISTRY.spec_for("plan_acquisition").request_schema is None
    assert (
        DEFAULT_ACTION_REGISTRY.spec_for("start_acquisition_run").request_schema_version
        == "acquisition_root_request_v1"
    )
    assert (
        DEFAULT_ACTION_REGISTRY.spec_for("filter_projection").request_schema_version == "projection_filter_request_v1"
    )
    assert START_ACQUISITION_RUN_TOOL_SPEC.request.schema_digest == ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST


def test_population_module_has_no_runtime_transport_or_serving_side_effect_surface() -> None:
    import sourcing_agent.agent_canary_registry as module

    source = inspect.getsource(module)
    forbidden = (
        "requests.",
        "urllib",
        "provider_client",
        "model_client",
        "served = True",
        "DEFAULT_AGENT_TOOL_REGISTRY =",
        "create_operation_run(",
        "create_workflow_command(",
    )
    for token in forbidden:
        assert token not in source
