from __future__ import annotations

import hashlib
import inspect
import json
from typing import cast

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
    INSPECT_OPERATION_TOOL_SPEC_V1,
    INSPECT_OPERATION_TOOL_SPEC_V2,
    INSPECT_OPERATION_TOOL_SPEC_V3,
    LOCAL_CANARY_ACTION_SPECS,
    LOCAL_CANARY_AGENT_TOOL_REGISTRY,
    LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION,
    LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION_V2,
    LOCAL_CANARY_SIMULATE_FIXTURES,
    LOCAL_CANARY_TOOL_NAMES,
    PLAN_ACQUISITION_CANARY_ACTION_SPEC,
    PLAN_ACQUISITION_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC,
    START_ACQUISITION_RUN_TOOL_SPEC_V2,
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
    INSPECT_OPERATION_RESULT_SPEC_V1,
    INSPECT_OPERATION_RESULT_SPEC_V2,
    INSPECT_OPERATION_RESULT_SPEC_V3,
)
from sourcing_agent.agent_tool_registry import (
    AGENT_TOOL_SPEC_SCHEMA_VERSION,
    AGENT_TOOL_SPEC_SCHEMA_VERSION_V2,
    DEFAULT_AGENT_TOOL_REGISTRY,
)
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
    assert LOCAL_CANARY_AGENT_TOOL_REGISTRY.historical_spec_count == 7
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
    assert PLAN_ACQUISITION_TOOL_SPEC.behavior.result_link_policy == "no_command_v1"

    assert START_ACQUISITION_RUN_TOOL_SPEC.tool_kind == "action"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.effect_class == "command_backed_action"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.result_link_policy == "workflow_command_acceptance_v1"
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.approval.required is True
    assert START_ACQUISITION_RUN_TOOL_SPEC.budget.required is True
    assert START_ACQUISITION_RUN_TOOL_SPEC.capability.required_provider_modes == ("live",)

    assert FILTER_PROJECTION_TOOL_SPEC.tool_kind == "action"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.result_link_policy == "no_command_v1"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.command_exposure == "none"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.approval.required is False

    assert INSPECT_OPERATION_TOOL_SPEC.tool_kind == "query"
    assert INSPECT_OPERATION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert INSPECT_OPERATION_TOOL_SPEC.behavior.result_link_policy == "no_command_v1"
    assert INSPECT_OPERATION_TOOL_SPEC.action_type is None
    assert INSPECT_OPERATION_TOOL_SPEC.query_owner_id == INSPECT_OPERATION_QUERY_OWNER_ID


def test_inspect_query_pins_do_not_create_a_sixteenth_action() -> None:
    assert len(DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)) == 15
    assert INSPECT_OPERATION_TOOL_SPEC.tool_spec_version == "inspect_operation_tool_v3"
    assert INSPECT_OPERATION_TOOL_SPEC.request.schema_digest == INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST
    query_owner = INSPECT_OPERATION_TOOL_SPEC.request.query_owner
    assert query_owner is not None
    assert query_owner.owner_id == INSPECT_OPERATION_QUERY_OWNER_ID
    assert query_owner.owner_revision == INSPECT_OPERATION_QUERY_OWNER_REVISION == "inspect_operation_v3"
    assert query_owner.owner_contract_digest == INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST
    assert INSPECT_OPERATION_TOOL_SPEC.result.query_owner == query_owner
    assert INSPECT_OPERATION_RESULT_SPEC.result_schema_version == "inspect_operation_result_v3"
    assert INSPECT_OPERATION_RESULT_SPEC.serializer_revision == "inspect_operation_result_serializer_v3"
    assert INSPECT_OPERATION_TOOL_SPEC.route.adapter.owner_revision == "inspect_operation_adapter_v3"
    assert INSPECT_OPERATION_TOOL_SPEC.simulate_fixture.fixture_revision == "inspect_operation_fixture_v3"


def test_simulate_fixture_pins_are_real_content_digests_and_require_terminal_success() -> None:
    current_specs = {
        "plan_acquisition": PLAN_ACQUISITION_TOOL_SPEC,
        "start_acquisition_run": START_ACQUISITION_RUN_TOOL_SPEC,
        "filter_projection": FILTER_PROJECTION_TOOL_SPEC,
        "inspect_operation": INSPECT_OPERATION_TOOL_SPEC,
    }
    for tool_name in LOCAL_CANARY_TOOL_NAMES:
        fixture = dict(LOCAL_CANARY_SIMULATE_FIXTURES[tool_name])
        specs = LOCAL_CANARY_AGENT_TOOL_REGISTRY.specs_for_name(tool_name)
        expected_spec = current_specs[tool_name]
        assert len(specs) == ({"start_acquisition_run": 2, "inspect_operation": 3}.get(tool_name, 1))
        assert fixture["provider_mode"] == "simulate"
        assert fixture["expected_terminal_variant"] == "success"
        assert fixture["expected_live_provider_invocations"] == 0
        assert fixture["expected_live_model_invocations"] == 0
        assert expected_spec.simulate_fixture.fixture_id == fixture["fixture_id"]
        assert expected_spec.simulate_fixture.fixture_revision == fixture["fixture_revision"]
        assert expected_spec.simulate_fixture.fixture_digest == _fixture_digest(tool_name)


def test_start_v2_history_is_byte_stable_and_v3_explicitly_fingerprints_command_acceptance() -> None:
    historical_behavior = cast(
        dict[str, object],
        START_ACQUISITION_RUN_TOOL_SPEC_V2.to_fingerprint_record()["behavior"],
    )
    assert START_ACQUISITION_RUN_TOOL_SPEC_V2.fingerprint_schema_version == AGENT_TOOL_SPEC_SCHEMA_VERSION
    assert START_ACQUISITION_RUN_TOOL_SPEC_V2.tool_spec_version == "start_acquisition_run_tool_v2"
    assert (
        START_ACQUISITION_RUN_TOOL_SPEC_V2.tool_spec_digest
        == "f834d7f3c04035dd012f7ee4a321b41fb120c7efeb51dfbe1ee68f7d665be7d2"
    )
    assert START_ACQUISITION_RUN_TOOL_SPEC_V2.behavior.result_link_policy == "activity_attempt_terminal_v1"
    assert "result_link_policy" not in historical_behavior
    assert START_ACQUISITION_RUN_TOOL_SPEC_V2.simulate_fixture.fixture_revision == "start_acquisition_run_fixture_v2"
    assert (
        START_ACQUISITION_RUN_TOOL_SPEC_V2.simulate_fixture.fixture_digest
        == "5e664547d3624c738b468948d9337dc3469634ceddccee30c19d625a52df85a0"
    )
    assert START_ACQUISITION_RUN_TOOL_SPEC_V2.simulate_fixture.fixture_digest != (
        START_ACQUISITION_RUN_TOOL_SPEC.simulate_fixture.fixture_digest
    )

    assert START_ACQUISITION_RUN_TOOL_SPEC.fingerprint_schema_version == AGENT_TOOL_SPEC_SCHEMA_VERSION_V2
    assert START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version == "start_acquisition_run_tool_v3"
    assert (
        START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest
        == "0ed14aa4a5b536606f7123a2b864900e5ab0ce37ebd79a79b49326c75669bcf1"
    )
    assert START_ACQUISITION_RUN_TOOL_SPEC.behavior.result_link_policy == "workflow_command_acceptance_v1"
    current_behavior = cast(dict[str, object], START_ACQUISITION_RUN_TOOL_SPEC.to_fingerprint_record()["behavior"])
    assert current_behavior["result_link_policy"] == "workflow_command_acceptance_v1"
    assert START_ACQUISITION_RUN_TOOL_SPEC.simulate_fixture.fixture_revision == "start_acquisition_run_fixture_v3"
    assert (
        START_ACQUISITION_RUN_TOOL_SPEC.simulate_fixture.fixture_digest
        == "44d18168e15ffeb84c2cdcdae317917974b6fdeab70046eaadc998d0f3292748"
    )
    assert LOCAL_CANARY_SIMULATE_FIXTURES["start_acquisition_run"]["schema_version"] == (
        LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION_V2
    )
    assert LOCAL_CANARY_SIMULATE_FIXTURES["start_acquisition_run"]["result_link_policy"] == (
        "workflow_command_acceptance_v1"
    )
    assert (
        LOCAL_CANARY_AGENT_TOOL_REGISTRY.require_historical(*START_ACQUISITION_RUN_TOOL_SPEC_V2.historical_identity)
        is START_ACQUISITION_RUN_TOOL_SPEC_V2
    )

    legacy_fixture_record = {
        "schema_version": LOCAL_CANARY_SIMULATE_FIXTURE_SCHEMA_VERSION,
        "fixture_id": "agent.local_canary.simulate.start_acquisition_run",
        "fixture_revision": "start_acquisition_run_fixture_v2",
        "tool_name": "start_acquisition_run",
        "provider_mode": "simulate",
        "expected_terminal_variant": "success",
        "approval_required": True,
        "effect_class": "command_backed_action",
        "expected_live_provider_invocations": 0,
        "expected_live_model_invocations": 0,
    }
    assert (
        hashlib.sha256(
            json.dumps(
                legacy_fixture_record,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        == START_ACQUISITION_RUN_TOOL_SPEC_V2.simulate_fixture.fixture_digest
    )


def test_non_start_historical_tool_fingerprints_remain_byte_identical() -> None:
    assert {
        spec.tool_name: spec.tool_spec_digest
        for spec in (
            PLAN_ACQUISITION_TOOL_SPEC,
            FILTER_PROJECTION_TOOL_SPEC,
        )
    } == {
        "plan_acquisition": "82562f99fa6c28a10b625756dfe8905c5d9dd9a016f8269e93b932321e5aeba0",
        "filter_projection": "0fd97ff1dcb546fc6fa6ed6ab730daac01c36987f21cae23a7832e1de82bc08c",
    }

    assert INSPECT_OPERATION_TOOL_SPEC_V1.tool_spec_version == "inspect_operation_tool_v1"
    assert INSPECT_OPERATION_TOOL_SPEC_V1.tool_spec_digest == (
        "03fb54ee910ecd8a3319c7326dd09a1dcf37e6258d103e60968f756676c0d2d8"
    )
    assert INSPECT_OPERATION_TOOL_SPEC_V1.result.schema_digest == INSPECT_OPERATION_RESULT_SPEC_V1.result_schema_digest
    assert INSPECT_OPERATION_RESULT_SPEC_V1.result_schema_digest == (
        "acd2538887715a9be0167c7d573345d74baea1e614a38ad62256b96b925100c8"
    )
    assert INSPECT_OPERATION_TOOL_SPEC_V1.simulate_fixture.fixture_digest == (
        "c7a0882459a671471a26ac9f1b45befd17883c8380adfd22b98b1e4c7b58303d"
    )

    assert INSPECT_OPERATION_TOOL_SPEC_V2.tool_spec_version == "inspect_operation_tool_v2"
    assert INSPECT_OPERATION_TOOL_SPEC_V2.tool_spec_digest == (
        "26b7e6a56f3461a605e68c7d16fa9d74bfec086ea4d9747382fc96ebe5a620d0"
    )
    assert INSPECT_OPERATION_TOOL_SPEC_V2.result.schema_digest == INSPECT_OPERATION_RESULT_SPEC_V2.result_schema_digest
    assert INSPECT_OPERATION_RESULT_SPEC_V2.result_schema_digest == (
        "8878c155577af1fbe9c665f28c1d5d8c16b66eec986c084686a356e361287b3e"
    )
    assert INSPECT_OPERATION_TOOL_SPEC_V2.simulate_fixture.fixture_digest == (
        "cc5d501d2dc503941f9112a4666d4536ae2817d54ea85f7ea4b34ddb2f77d120"
    )

    assert INSPECT_OPERATION_TOOL_SPEC is INSPECT_OPERATION_TOOL_SPEC_V3
    assert INSPECT_OPERATION_TOOL_SPEC_V3.tool_spec_version == "inspect_operation_tool_v3"
    assert INSPECT_OPERATION_TOOL_SPEC_V3.tool_spec_digest == (
        "37b649a45d9595d4703175c0304aaffcb3bf14d343bc5100fb312487e6fcb314"
    )
    assert INSPECT_OPERATION_RESULT_SPEC is INSPECT_OPERATION_RESULT_SPEC_V3
    assert INSPECT_OPERATION_TOOL_SPEC_V3.result.schema_digest == INSPECT_OPERATION_RESULT_SPEC_V3.result_schema_digest
    assert INSPECT_OPERATION_TOOL_SPEC_V3.simulate_fixture.fixture_digest == (
        "a6382e35a5183aeebfd6d15a7bf02afd6aee7bfd692fbb31484b2b3ed8ee6428"
    )

    inspect_history = LOCAL_CANARY_AGENT_TOOL_REGISTRY.specs_for_name("inspect_operation")
    assert set(inspect_history) == {
        INSPECT_OPERATION_TOOL_SPEC_V1,
        INSPECT_OPERATION_TOOL_SPEC_V2,
        INSPECT_OPERATION_TOOL_SPEC_V3,
    }
    assert INSPECT_OPERATION_TOOL_SPEC is not inspect_history[0]


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
