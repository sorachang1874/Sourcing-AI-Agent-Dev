from __future__ import annotations

import copy
import hashlib
import inspect
import json
from typing import Any

import pytest

from sourcing_agent.action_result_schema import (
    DEFAULT_ACTION_RESULT_REGISTRY,
    ActionResultQueryOwner,
    ActionResultSchemaError,
)
from sourcing_agent.agent_projection_query import (
    FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST,
    FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION,
    FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC,
    FILTER_PROJECTION_V2_RESULT_SPEC,
    INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST,
    INSPECT_OPERATION_QUERY_OWNER_ID,
    INSPECT_OPERATION_QUERY_OWNER_REVISION,
    INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST,
    INSPECT_OPERATION_REQUEST_TOOL_SPEC,
    INSPECT_OPERATION_RESULT_REGISTRY,
    INSPECT_OPERATION_RESULT_SPEC,
    INSPECT_OPERATION_RESULT_SPEC_V1,
    INSPECT_OPERATION_RESULT_SPEC_V2,
    INSPECT_OPERATION_RESULT_SPEC_V3,
    AgentProjectionQueryError,
    FilterProjectionV2BoundRequest,
    ProjectionCohortPredicate,
    bind_filter_projection_v2_request,
    bind_inspect_operation_request,
    execute_filter_projection_v2,
    execute_inspect_operation,
    execute_inspect_operation_for_result_spec,
    filter_projection_v2_request_schema,
    inspect_operation_request_schema,
    operation_result_readiness_projection,
    projection_candidate_ref,
    resolve_inspect_operation_result_spec,
    serialize_filter_projection_v2_result,
    serialize_inspect_operation_result,
    serialize_inspect_operation_result_for_spec,
    serialize_inspect_operation_result_v1,
    serialize_inspect_operation_result_v2,
    serialize_inspect_operation_result_v3,
)
from sourcing_agent.cohort_selection import (
    COHORT_SELECTION_REGISTRY_VERSION,
    COHORT_SELECTION_SCHEMA_VERSION,
    cohort_selection_digest,
    cohort_selection_registry_digest,
)
from sourcing_agent.model_tool_runtime import ModelToolSchemaError
from sourcing_agent.operation_runtime import (
    ACTION_SEARCH_PROJECTION,
    PROJECTION_READ_ACTION_REQUEST_CONTRACTS,
    operation_run_control_state,
    validate_operation_run_control_state_projection,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cohort(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
) -> dict[str, object]:
    return {
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "role_bucket_ids": list(["research"] if roles is None else roles),
        "employment_statuses": list(["current"] if statuses is None else statuses),
        "role_match": role_match,
        "source": "user_explicit",
    }


def _bound_filter_request(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
    offset: int = 0,
    limit: int = 50,
):
    cohort = _cohort(roles=roles, statuses=statuses, role_match=role_match)
    return bind_filter_projection_v2_request(
        input_payload={"cohort_selection": cohort, "offset": offset, "limit": limit},
        owner_target_ref={
            "projection_id": "projection-tml",
            "membership_revision": "membership-revision-7",
            "cohort_selection_registry_version": COHORT_SELECTION_REGISTRY_VERSION,
            "cohort_selection_registry_digest": cohort_selection_registry_digest(),
            "cohort_selection_digest": cohort_selection_digest(cohort),
        },
    )


def _membership(lane_id: str, status: str, role: str) -> dict[str, str]:
    return {"lane_id": lane_id, "employment_status": status, "role_bucket_id": role}


def _candidate(
    candidate_id: str,
    *memberships: dict[str, str],
    display_name: str | None = None,
    headline: str = "Public profile summary",
) -> dict[str, object]:
    return {
        "candidate_identity_key": candidate_id,
        "display_name": display_name or candidate_id.replace(":", " ").title(),
        "headline": headline,
        "public_profile_url": f"https://www.linkedin.com/in/{candidate_id.replace(':', '-')}",
        "cohort_lane_membership": list(memberships),
    }


def _filter_snapshot(request=None) -> dict[str, Any]:
    request = request or _bound_filter_request()
    target = dict(request.target_ref)
    return {
        "access_scope": "shared_canonical_read",
        "projection_id": target["projection_id"],
        "membership_revision": target["membership_revision"],
        "cohort_selection_registry_version": target["cohort_selection_registry_version"],
        "cohort_selection_registry_digest": target["cohort_selection_registry_digest"],
        "cohort_selection_digest": target["cohort_selection_digest"],
        "selection_digest": target["cohort_selection_digest"],
        "planning_digest": _digest("planning"),
        "execution_digest": _digest("execution"),
        "result_digest": _digest("result"),
        "publication_digest": _digest("publication"),
        "freshness": {
            "status": "fresh",
            "source_of_truth": "projection_search_service.publication_identity",
        },
        "readiness": {
            "status": "ready",
            "source_of_truth": "projection_search_service.readiness",
        },
        "provider_mode": "scripted",
        "runtime_namespace": "scripted-tml-isolated",
        "cache_provenance": {
            "cache_scope": "isolated_non_live",
            "source_of_truth": "projection_search_service.cache_provenance",
        },
        "requested_lane_coverage": {
            "status": "complete",
            "requested_lane_count": 4,
            "completed_lane_count": 4,
            "missing_lane_count": 0,
        },
        "lane_summaries": [
            {
                "lane_id": "lane-current-research",
                "employment_status": "current",
                "role_bucket_id": "research",
                "coverage_status": "complete",
                "result_count": 3,
            },
            {
                "lane_id": "lane-current-engineering",
                "employment_status": "current",
                "role_bucket_id": "engineering",
                "coverage_status": "complete",
                "result_count": 2,
            },
            {
                "lane_id": "lane-former-research",
                "employment_status": "former",
                "role_bucket_id": "research",
                "coverage_status": "complete",
                "result_count": 0,
            },
            {
                "lane_id": "lane-former-engineering",
                "employment_status": "former",
                "role_bucket_id": "engineering",
                "coverage_status": "complete",
                "result_count": 2,
            },
        ],
        "candidates": [
            _candidate("person:research-current", _membership("lane-a", "current", "research")),
            _candidate("person:engineering-current", _membership("lane-b", "current", "engineering")),
            _candidate(
                "person:both-current",
                _membership("lane-c1", "current", "research"),
                _membership("lane-c2", "current", "engineering"),
            ),
            _candidate(
                "person:split-status",
                _membership("lane-d1", "current", "research"),
                _membership("lane-d2", "former", "engineering"),
            ),
            _candidate("person:engineering-former", _membership("lane-e", "former", "engineering")),
        ],
    }


def _inspect_bound_request():
    return bind_inspect_operation_request(
        {"operation_run_id": "oprun-123"},
        workspace_id="workspace-a",
        action_id="action-123",
        actor_id="actor-a",
    )


def _candidate_refs(request, candidate_ids: list[str]) -> list[str]:
    target = dict(request.target_ref)
    return [
        projection_candidate_ref(
            projection_id=str(target["projection_id"]),
            membership_revision=str(target["membership_revision"]),
            candidate_identity_key=candidate_id,
        )
        for candidate_id in candidate_ids
    ]


def _inspect_snapshot(*, policy_status: str = "available") -> dict[str, Any]:
    if policy_status == "available":
        policy: dict[str, object] = {
            "status": "available",
            "source_of_truth": "durable_runtime.workflow_command_control_policy",
            "fallback_status": "fail_closed",
            "command_type": "acquisition.run.create",
            "owner": "acquisition_run_writer",
            "running_control_maturity": "owner_specific_cancel_resume",
            "running_control_gap_status": "closed",
            "running_control_surface": "workflow_command_control_api_only",
            "running_cancel_supported": True,
            "running_resume_supported": True,
        }
        command_count = 1
        latest_command = {
            "latest_workflow_command_id": "cmd-123",
            "latest_workflow_command_type": "acquisition.run.create",
        }
    else:
        policy = {
            "status": "not_applicable",
            "source_of_truth": "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
            "fallback_status": "fail_closed",
        }
        command_count = 0
        latest_command = {}
    return {
        "action": {
            "workspace_id": "workspace-a",
            "action_id": "action-123",
            "action_type": "start_acquisition_run",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "status": "planned",
        },
        "operation_run": {
            "workspace_id": "workspace-a",
            "action_id": "action-123",
            "operation_run_id": "oprun-123",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "status": "running",
        },
        "control_state": {
            "schema_version": "operation_run_control_state_v1",
            "operation_status": "running",
            "action_status": "planned",
            "operation_phase": "workflow_command_planned",
            "can_dispatch": False,
            "can_cancel": True,
            "can_retry": False,
            "can_resume": True,
            "allowed_actions": ["resume", "cancel"],
            "disabled_reasons": {
                "dispatch": "dispatch_requires_queued_operation",
                "retry": "retry_requires_failed_or_cancelled_operation",
            },
            "control_source_of_truth": "operation_runtime.operation_run_control_state",
            "fallback_status": "fail_closed",
            "module_state_mutated_on_control": False,
        },
        "control_policy": policy,
        "display_contract": {
            "schema_version": "operation_action_display_contract_v1",
            "action_type": "start_acquisition_run",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "display_label": "Start acquisition run",
            "display_category": "Acquisition",
            "description": "Start the approved sourcing acquisition plan.",
            "source_of_truth": "operation_runtime.ActionRegistry.display_contract_for",
            "fallback_status": "fail_closed",
        },
        "progress": {
            "phase": "workflow_command_planned",
            "source_of_truth": "operation_runs.progress",
        },
        "result_readiness": {
            "status": "pending",
            "result_ref_present": False,
            "source_of_truth": "operation_query_service.result_readiness_projection",
            "fallback_status": "fail_closed",
        },
        "provenance": {
            "source_of_truth": "operation_runs.agent_actions.workflow_commands.operation_events",
            "operation_event_count": 2,
            "workflow_command_count": command_count,
            "latest_event_type": "OperationCommandPlanned",
            "truncated": False,
            **latest_command,
        },
    }


def _mutate_inspect_phase(result) -> None:
    result["progress"]["phase"] = "different_phase"


def _mutate_inspect_allowed_actions(result) -> None:
    result["control_state"]["allowed_actions"] = ["cancel"]


def _mutate_inspect_allowed_action_order(result) -> None:
    result["control_state"]["allowed_actions"] = ["cancel", "resume"]


def _mutate_inspect_enabled_disabled_reason(result) -> None:
    result["control_state"]["disabled_reasons"]["cancel"] = "cancel_disabled"


def _mutate_inspect_missing_disabled_reason(result) -> None:
    result["control_state"]["disabled_reasons"].pop("dispatch")


def _mutate_inspect_impossible_terminal_controls(result) -> None:
    result["control_state"]["action_status"] = "completed"


def _mutate_inspect_disabled_reason_value(result) -> None:
    result["control_state"]["disabled_reasons"]["dispatch"] = "linked_action_missing"


def _mutate_inspect_readiness(result) -> None:
    result["result_readiness"]["status"] = "ready"


def _mutate_inspect_policy_command(result) -> None:
    result["provenance"]["latest_workflow_command_type"] = "other.command"


def _mutate_inspect_available_zero_provenance(result) -> None:
    result["provenance"]["workflow_command_count"] = 0
    result["provenance"].pop("latest_workflow_command_id")
    result["provenance"].pop("latest_workflow_command_type")


def _mutate_inspect_event_provenance(result) -> None:
    result["provenance"]["operation_event_count"] = 0


def test_v3_request_contracts_are_closed_and_leave_search_projection_v1_unchanged() -> None:
    search_v1 = dict(PROJECTION_READ_ACTION_REQUEST_CONTRACTS[ACTION_SEARCH_PROJECTION])
    assert search_v1["request_schema_version"] == "projection_search_request_v1"
    search_input = dict(dict(search_v1["request_schema"])["properties"])["input_payload"]
    assert set(dict(search_input)["properties"]) == {"search_keyword", "offset", "limit"}

    filter_schema = filter_projection_v2_request_schema()
    assert FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION == "projection_filter_request_v2"
    assert FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST == FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.input_schema_digest
    assert set(filter_schema["properties"]) == {"input_payload", "target_ref"}
    assert filter_schema["additionalProperties"] is False
    with pytest.raises(ModelToolSchemaError):
        FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.validate_input(
            {
                **_bound_filter_request().to_record(),
                "search_keyword": "must remain a v1-only text carrier",
            }
        )

    inspect_schema = inspect_operation_request_schema()
    assert INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST == INSPECT_OPERATION_REQUEST_TOOL_SPEC.input_schema_digest
    assert set(inspect_schema["properties"]) == {"operation_run_id"}
    with pytest.raises(ModelToolSchemaError):
        INSPECT_OPERATION_REQUEST_TOOL_SPEC.validate_input(
            {"operation_run_id": "oprun-123", "workspace_id": "caller-must-not-author"}
        )


def test_inspect_result_registry_retains_exact_v1_v2_v3_and_current_alias() -> None:
    import sourcing_agent.agent_projection_query as module

    expected_digests = {
        "inspect_operation_result_v1": "acd2538887715a9be0167c7d573345d74baea1e614a38ad62256b96b925100c8",
        "inspect_operation_result_v2": "8878c155577af1fbe9c665f28c1d5d8c16b66eec986c084686a356e361287b3e",
        "inspect_operation_result_v3": "a03c5cad763e31a196497b5141776b7e25cc207d0f60743150eca73773dd20af",
    }
    specs = (
        INSPECT_OPERATION_RESULT_SPEC_V1,
        INSPECT_OPERATION_RESULT_SPEC_V2,
        INSPECT_OPERATION_RESULT_SPEC_V3,
    )

    assert INSPECT_OPERATION_REQUEST_TOOL_SPEC.schema_version == "inspect_operation_request_v1"
    assert INSPECT_OPERATION_RESULT_SPEC is INSPECT_OPERATION_RESULT_SPEC_V3
    assert INSPECT_OPERATION_QUERY_OWNER_REVISION == "inspect_operation_v3"
    assert INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST == (
        "c7308a382105cecba770b5fdb61a1f31ba36161c308230e81e2ae675aa7f6243"
    )
    assert INSPECT_OPERATION_RESULT_REGISTRY.specs_for_tool("inspect_operation") == specs
    for spec in specs:
        assert spec.result_schema_digest == expected_digests[spec.result_schema_version]
        assert (
            resolve_inspect_operation_result_spec(
                spec.result_schema_version,
                spec.result_schema_digest,
            )
            is spec
        )
    with pytest.raises(ActionResultSchemaError, match="action_result_historical_spec_missing"):
        resolve_inspect_operation_result_spec("inspect_operation_result_v2", "0" * 64)

    assert module._INSPECT_OWNER_SNAPSHOT_TOOL_SPEC_V1.input_schema_digest == (
        "02d933eae94d2d48ef3435bb15d015866c3817021341c9268f46b185f4f788bb"
    )
    assert module._INSPECT_OWNER_SNAPSHOT_TOOL_SPEC_V2.input_schema_digest == (
        "b07bd8b4a15f7b0dd1b39b87897c1e4eed6df3e152700c6189c36bef47a8fc6c"
    )
    assert module._INSPECT_OWNER_SNAPSHOT_TOOL_SPEC_V3.schema_version == "inspect_operation_owner_snapshot_v3"


def test_filter_projection_any_matches_at_least_one_role_in_a_qualifying_status() -> None:
    request = _bound_filter_request(roles=["engineering", "research"], statuses=["current"], role_match="any")
    result = execute_filter_projection_v2(request=request, owner_snapshot=_filter_snapshot(request))

    assert result["status"] == "ready"
    assert result["cohort_selection"]["role_bucket_ids"] == ["research", "engineering"]
    assert [row["candidate_ref"] for row in result["candidates"]] == _candidate_refs(
        request,
        [
            "person:research-current",
            "person:engineering-current",
            "person:both-current",
            "person:split-status",
        ],
    )
    assert result["total_count"] == 4
    assert result["returned_count"] == 4
    assert result["truncated"] is False


def test_filter_projection_all_requires_every_role_in_the_same_qualifying_status() -> None:
    request = _bound_filter_request(
        roles=["engineering", "research"],
        statuses=["current", "former"],
        role_match="all",
    )
    result = execute_filter_projection_v2(request=request, owner_snapshot=_filter_snapshot(request))

    assert [row["candidate_ref"] for row in result["candidates"]] == _candidate_refs(request, ["person:both-current"])
    assert _candidate_refs(request, ["person:split-status"])[0] not in {
        row["candidate_ref"] for row in result["candidates"]
    }
    assert request.predicate.to_record()["role_mode"] == "all"


@pytest.mark.parametrize(
    ("roles", "statuses", "expected"),
    [
        ([], ["former"], ["person:split-status", "person:engineering-former"]),
        (["engineering"], ["current"], ["person:engineering-current", "person:both-current"]),
        (["engineering"], ["former"], ["person:split-status", "person:engineering-former"]),
        (
            [],
            ["current", "former"],
            [
                "person:research-current",
                "person:engineering-current",
                "person:both-current",
                "person:split-status",
                "person:engineering-former",
            ],
        ),
    ],
)
def test_filter_projection_empty_roles_means_all_roles_and_statuses_remain_exact(
    roles: list[str],
    statuses: list[str],
    expected: list[str],
) -> None:
    request = _bound_filter_request(roles=roles, statuses=statuses, role_match="all")
    result = execute_filter_projection_v2(request=request, owner_snapshot=_filter_snapshot(request))

    assert [row["candidate_ref"] for row in result["candidates"]] == _candidate_refs(request, expected)
    assert request.predicate.to_record()["role_mode"] == ("all_roles" if not roles else "all")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda snapshot: snapshot.update({"membership_revision": "membership-revision-8"}),
        lambda snapshot: snapshot.update({"cohort_selection_registry_digest": _digest("new-registry")}),
        lambda snapshot: snapshot.update({"selection_digest": _digest("other-selection")}),
        lambda snapshot: snapshot["freshness"].update({"status": "stale"}),
    ],
)
def test_filter_projection_identity_mismatch_is_stale_not_empty_success(mutation) -> None:
    request = _bound_filter_request(roles=["research"], statuses=["current"])
    snapshot = _filter_snapshot(request)
    mutation(snapshot)

    result = execute_filter_projection_v2(request=request, owner_snapshot=snapshot)

    assert result == {
        "variant": "deferred",
        "status": "stale",
        "reason": "projection_cohort_identity_stale",
        "retryable": False,
        "reselection_required": True,
    }
    assert json.loads(serialize_filter_projection_v2_result(result)) == result


def test_filter_projection_not_ready_is_explicit_and_never_an_empty_success() -> None:
    request = _bound_filter_request()
    snapshot = _filter_snapshot(request)
    snapshot["readiness"] = {
        "status": "not_ready",
        "reason": "projection_index_not_ready",
        "source_of_truth": "projection_search_service.readiness",
    }

    result = execute_filter_projection_v2(request=request, owner_snapshot=snapshot)

    assert result["variant"] == "deferred"
    assert result["status"] == "not_ready"
    assert result["reason"] == "projection_index_not_ready"
    assert result["reselection_required"] is False


def test_filter_projection_missing_and_unproved_access_are_indistinguishable() -> None:
    request = _bound_filter_request()
    foreign = _filter_snapshot(request)
    foreign["access_scope"] = "tenant_private"
    wrong_projection = _filter_snapshot(request)
    wrong_projection["projection_id"] = "projection-other"

    missing = execute_filter_projection_v2(request=request, owner_snapshot=None)
    assert execute_filter_projection_v2(request=request, owner_snapshot=foreign) == missing
    assert execute_filter_projection_v2(request=request, owner_snapshot=wrong_projection) == missing
    assert missing == {
        "variant": "error",
        "status": "failed",
        "reason": "projection_not_found",
        "retryable": False,
    }


def test_filter_projection_request_rejects_caller_authored_or_drifted_owner_pins() -> None:
    cohort = _cohort()
    target = {
        "projection_id": "projection-tml",
        "membership_revision": "membership-revision-7",
        "cohort_selection_registry_version": COHORT_SELECTION_REGISTRY_VERSION,
        "cohort_selection_registry_digest": cohort_selection_registry_digest(),
        "cohort_selection_digest": cohort_selection_digest(cohort),
    }
    for field, drifted in (
        ("cohort_selection_registry_digest", _digest("drifted-registry")),
        ("cohort_selection_digest", _digest("drifted-selection")),
    ):
        with pytest.raises(AgentProjectionQueryError, match="filter_projection_owner_target_identity_mismatch"):
            bind_filter_projection_v2_request(
                input_payload={"cohort_selection": cohort},
                owner_target_ref={**target, field: drifted},
            )

    bound = _bound_filter_request()
    forged = bound.to_record()
    forged["target_ref"]["cohort_selection_digest"] = _digest("forged-selection")
    with pytest.raises(AgentProjectionQueryError, match="filter_projection_owner_target_identity_mismatch"):
        FilterProjectionV2BoundRequest(forged)


def test_filter_projection_rejects_private_path_and_oversize_owner_results_without_input_mutation() -> None:
    request = _bound_filter_request(roles=["research"], statuses=["current"], limit=100)
    private_snapshot = _filter_snapshot(request)
    private_snapshot["candidates"][0]["raw_profile"] = {"email": "private@example.com"}
    private_before = copy.deepcopy(private_snapshot)
    with pytest.raises(AgentProjectionQueryError, match="filter_projection_owner_snapshot_invalid"):
        execute_filter_projection_v2(request=request, owner_snapshot=private_snapshot)
    assert private_snapshot == private_before

    path_snapshot = _filter_snapshot(request)
    path_snapshot["candidates"][0]["headline"] = "/Users/operator/private/candidate.json"
    with pytest.raises(AgentProjectionQueryError, match="filter_projection_result_not_model_safe"):
        execute_filter_projection_v2(request=request, owner_snapshot=path_snapshot)

    oversized_snapshot = _filter_snapshot(request)
    oversized_snapshot["candidates"] = [
        _candidate(
            f"person:oversize-{index}",
            _membership(f"lane-oversize-{index}", "current", "research"),
            headline=f"Public research summary {index} " + ("x" * 920),
        )
        for index in range(80)
    ]
    with pytest.raises(AgentProjectionQueryError, match="filter_projection_result_not_model_safe"):
        execute_filter_projection_v2(request=request, owner_snapshot=oversized_snapshot)

    valid = execute_filter_projection_v2(request=request, owner_snapshot=_filter_snapshot(request))
    with pytest.raises(ActionResultSchemaError):
        serialize_filter_projection_v2_result({**valid, "private_path": "/tmp/private.json"})


def test_filter_projection_pagination_counts_and_model_safe_public_fields_are_exact() -> None:
    request = _bound_filter_request(roles=[], statuses=["current", "former"], offset=1, limit=2)
    snapshot = _filter_snapshot(request)
    before = copy.deepcopy(snapshot)
    result = execute_filter_projection_v2(request=request, owner_snapshot=snapshot)

    assert snapshot == before
    assert result["total_count"] == 5
    assert result["returned_count"] == 2
    assert result["truncated"] is True
    assert set(result["candidates"][0]) == {
        "candidate_ref",
        "display_name",
        "headline",
        "public_profile_url",
        "employment_statuses",
        "role_bucket_ids",
    }
    assert "cohort_lane_membership" not in json.dumps(result)
    assert "candidate_identity_key" not in json.dumps(result)
    assert "raw_profile" not in json.dumps(result)
    assert json.loads(serialize_filter_projection_v2_result(result)) == result


def test_filter_projection_keeps_url_shaped_owner_identity_private_and_publishes_safe_ref() -> None:
    request = _bound_filter_request(roles=["research"], statuses=["current"])
    snapshot = _filter_snapshot(request)
    raw_identity = "linkedin:https://www.linkedin.com/in/ada-lovelace"
    snapshot["candidates"] = [
        {
            **_candidate(
                raw_identity,
                _membership("lane-url-owner", "current", "research"),
                display_name="Ada Lovelace",
            ),
            "public_profile_url": "https://www.linkedin.com/in/ada-lovelace",
        }
    ]

    result = execute_filter_projection_v2(request=request, owner_snapshot=snapshot)

    assert result["candidates"][0]["candidate_ref"] == _candidate_refs(request, [raw_identity])[0]
    assert len(result["candidates"][0]["candidate_ref"]) == 64
    assert result["candidates"][0]["candidate_ref"] != projection_candidate_ref(
        projection_id=str(request.target_ref["projection_id"]),
        membership_revision="membership-revision-8",
        candidate_identity_key=raw_identity,
    )
    assert raw_identity not in json.dumps(result)
    assert json.loads(serialize_filter_projection_v2_result(result)) == result


def test_projection_predicate_rejects_unowned_or_ambiguous_membership_shapes() -> None:
    predicate = ProjectionCohortPredicate(
        role_bucket_ids=("research",),
        employment_statuses=("current",),
        role_match="any",
        selection_digest=_digest("selection"),
    )
    invalid_memberships = [
        [],
        [{"lane_id": "lane-a", "employment_status": "current", "role_bucket_id": "research", "raw": True}],
        [_membership("lane-a", "contractor", "research")],
        [_membership("lane-a", "current", "sales")],
        [_membership("lane-a", "current", "research"), _membership("lane-a", "current", "research")],
    ]
    for memberships in invalid_memberships:
        with pytest.raises(AgentProjectionQueryError):
            predicate.matches(memberships)


def test_inspect_operation_exact_owner_preflight_and_closed_projection() -> None:
    request = _inspect_bound_request()
    snapshot = _inspect_snapshot()
    before = copy.deepcopy(snapshot)
    result = execute_inspect_operation(request=request, owner_snapshot=snapshot)

    assert snapshot == before
    assert result["action_id"] == "action-123"
    assert result["operation_run_id"] == "oprun-123"
    assert result["control_state"] == snapshot["control_state"]
    assert result["control_policy"] == snapshot["control_policy"]
    assert result["display_contract"] == snapshot["display_contract"]
    assert result["progress"] == snapshot["progress"]
    assert result["result_readiness"] == snapshot["result_readiness"]
    assert result["provenance"] == snapshot["provenance"]
    encoded = serialize_inspect_operation_result(result)
    assert json.loads(encoded) == result
    assert not ({"next_controls", "repair", "command", "events", "metadata", "workflow_ref"} & set(result))


def test_inspect_execution_and_serialization_are_exactly_result_spec_versioned() -> None:
    request = _inspect_bound_request()
    snapshot = _inspect_snapshot()
    snapshot["progress"]["reason"] = "retry_requested"

    v1 = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=snapshot,
        result_spec=INSPECT_OPERATION_RESULT_SPEC_V1,
    )
    v2 = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=snapshot,
        result_spec=INSPECT_OPERATION_RESULT_SPEC_V2,
    )
    v3 = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=snapshot,
        result_spec=INSPECT_OPERATION_RESULT_SPEC_V3,
    )

    assert v1["progress"]["reason"] == "retry_requested"
    assert v2["progress"]["reason"] == "retry_requested"
    assert v3["progress"] == {
        "phase": "workflow_command_planned",
        "source_of_truth": "operation_runs.progress",
    }
    assert execute_inspect_operation(request=request, owner_snapshot=snapshot) == v3
    for spec, result in (
        (INSPECT_OPERATION_RESULT_SPEC_V1, v1),
        (INSPECT_OPERATION_RESULT_SPEC_V2, v2),
        (INSPECT_OPERATION_RESULT_SPEC_V3, v3),
    ):
        assert json.loads(serialize_inspect_operation_result_for_spec(result, result_spec=spec)) == result


def test_inspect_v3_omits_arbitrary_operator_reason_without_reinterpreting_history() -> None:
    request = _inspect_bound_request()
    snapshot = _inspect_snapshot()
    snapshot["progress"]["reason"] = "Retry after operator review — keep this outside the model result"

    current = execute_inspect_operation(request=request, owner_snapshot=snapshot)

    assert "reason" not in current["progress"]
    assert "operator review" not in serialize_inspect_operation_result_v3(current)
    injected = copy.deepcopy(current)
    injected["progress"]["reason"] = "Retry after operator review"
    with pytest.raises(ActionResultSchemaError):
        serialize_inspect_operation_result_v3(injected)
    for historical in (INSPECT_OPERATION_RESULT_SPEC_V1, INSPECT_OPERATION_RESULT_SPEC_V2):
        with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_snapshot_invalid"):
            execute_inspect_operation_for_result_spec(
                request=request,
                owner_snapshot=snapshot,
                result_spec=historical,
            )


def test_inspect_v1_v2_serializer_semantics_remain_frozen_and_are_not_upgraded_to_v3() -> None:
    request = _inspect_bound_request()
    v1_snapshot = _inspect_snapshot()
    v1_snapshot["result_readiness"]["status"] = "not_applicable"
    v1 = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=v1_snapshot,
        result_spec=INSPECT_OPERATION_RESULT_SPEC_V1,
    )
    assert json.loads(serialize_inspect_operation_result_v1(v1)) == v1
    for newer in (INSPECT_OPERATION_RESULT_SPEC_V2, INSPECT_OPERATION_RESULT_SPEC_V3):
        with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_snapshot_invalid"):
            execute_inspect_operation_for_result_spec(
                request=request,
                owner_snapshot=v1_snapshot,
                result_spec=newer,
            )

    v2 = execute_inspect_operation_for_result_spec(
        request=request,
        owner_snapshot=_inspect_snapshot(),
        result_spec=INSPECT_OPERATION_RESULT_SPEC_V2,
    )
    _mutate_inspect_phase(v2)
    assert json.loads(serialize_inspect_operation_result_v2(v2)) == v2
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_result_phase_mismatch"):
        serialize_inspect_operation_result_v3(v2)


def test_canonical_control_projection_validator_accepts_every_owner_emitted_hidden_input_witness() -> None:
    operation_statuses = ("", "queued", "planned", "running", "completed", "failed", "cancelled")
    action_statuses = (
        "",
        "planned",
        "approval_required",
        "queued",
        "running",
        "completed",
        "failed",
        "cancelled",
        "rejected",
    )
    approval_statuses = ("", "not_required", "approved", "rejected", "pending")
    retry_ids = ("", "operation-run", "other-retry-run")
    accepted = 0

    for operation_status in operation_statuses:
        for action_status in action_statuses:
            for approval_status in approval_statuses:
                for retry_id in retry_ids:
                    owner_state = operation_run_control_state(
                        operation_status=operation_status,
                        operation_run_id="operation-run",
                        action_status=action_status,
                        action_approval_status=approval_status,
                        action_retry_operation_run_id=retry_id,
                        operation_phase="phase",
                    )
                    assert (
                        validate_operation_run_control_state_projection(
                            owner_state.to_record(),
                            operation_run_id="operation-run",
                        ).to_record()
                        == owner_state.to_record()
                    )
                    accepted += 1

    assert accepted == 945


@pytest.mark.parametrize(
    ("operation_status", "result_ref_present", "expected_status"),
    (
        ("queued", False, "pending"),
        ("planned", False, "pending"),
        ("running", True, "pending"),
        ("completed", False, "pending"),
        ("completed", True, "ready"),
        ("failed", True, "failed"),
        ("cancelled", True, "cancelled"),
    ),
)
def test_operation_result_readiness_is_canonical_and_completed_without_ref_fails_closed(
    operation_status: str,
    result_ref_present: bool,
    expected_status: str,
) -> None:
    assert operation_result_readiness_projection(
        operation_status=operation_status,
        result_ref_present=result_ref_present,
    ) == {
        "status": expected_status,
        "result_ref_present": result_ref_present,
        "source_of_truth": "operation_query_service.result_readiness_projection",
        "fallback_status": "fail_closed",
    }


@pytest.mark.parametrize("forged_status", ("ready", "failed", "cancelled"))
def test_inspect_operation_rejects_schema_valid_result_readiness_drift(forged_status: str) -> None:
    snapshot = _inspect_snapshot()
    snapshot["result_readiness"]["status"] = forged_status

    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_result_readiness_mismatch"):
        execute_inspect_operation(request=_inspect_bound_request(), owner_snapshot=snapshot)


def test_inspect_operation_completed_without_result_ref_cannot_be_ready_or_not_applicable() -> None:
    snapshot = _inspect_snapshot()
    snapshot["operation_run"]["status"] = "completed"
    snapshot["control_state"]["operation_status"] = "completed"
    snapshot["result_readiness"]["status"] = "ready"

    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_result_readiness_mismatch"):
        execute_inspect_operation(request=_inspect_bound_request(), owner_snapshot=snapshot)

    snapshot["result_readiness"]["status"] = "not_applicable"
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_snapshot_invalid"):
        execute_inspect_operation(request=_inspect_bound_request(), owner_snapshot=snapshot)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("source_of_truth", "invented.readiness.owner"),
        ("fallback_status", "allow_inference"),
    ),
)
def test_inspect_operation_rejects_result_readiness_owner_contract_drift(
    field: str,
    value: str,
) -> None:
    snapshot = _inspect_snapshot()
    snapshot["result_readiness"][field] = value

    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_snapshot_invalid"):
        execute_inspect_operation(request=_inspect_bound_request(), owner_snapshot=snapshot)


def test_inspect_operation_serializer_rechecks_readiness_semantics() -> None:
    result = execute_inspect_operation(
        request=_inspect_bound_request(),
        owner_snapshot=_inspect_snapshot(),
    )
    result["control_state"]["operation_status"] = "completed"
    result["result_readiness"]["status"] = "ready"

    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_result_readiness_mismatch"):
        serialize_inspect_operation_result(result)

    result["result_readiness"]["status"] = "not_applicable"
    with pytest.raises(ActionResultSchemaError):
        INSPECT_OPERATION_RESULT_SPEC.serialize(result)


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        (_mutate_inspect_phase, "inspect_operation_result_phase_mismatch"),
        (_mutate_inspect_allowed_actions, "inspect_operation_control_state_action_mismatch"),
        (_mutate_inspect_allowed_action_order, "inspect_operation_control_state_action_mismatch"),
        (_mutate_inspect_enabled_disabled_reason, "inspect_operation_control_state_disabled_reason_mismatch"),
        (_mutate_inspect_missing_disabled_reason, "inspect_operation_control_state_disabled_reason_mismatch"),
        (_mutate_inspect_impossible_terminal_controls, "inspect_operation_control_state_owner_mismatch"),
        (_mutate_inspect_disabled_reason_value, "inspect_operation_control_state_owner_mismatch"),
        (_mutate_inspect_readiness, "inspect_operation_result_readiness_mismatch"),
        (_mutate_inspect_policy_command, "inspect_operation_control_policy_command_mismatch"),
        (_mutate_inspect_available_zero_provenance, "inspect_operation_control_policy_command_mismatch"),
        (_mutate_inspect_event_provenance, "inspect_operation_result_provenance_mismatch"),
    ),
)
def test_inspect_v3_named_serializer_rechecks_every_success_cross_field_semantic(mutation, error: str) -> None:
    result = execute_inspect_operation(
        request=_inspect_bound_request(),
        owner_snapshot=_inspect_snapshot(),
    )
    mutation(result)

    with pytest.raises(AgentProjectionQueryError, match=error):
        serialize_inspect_operation_result_v3(result)


def test_inspect_v3_execution_uses_shared_success_semantics_and_not_applicable_has_zero_command_provenance() -> None:
    snapshot = _inspect_snapshot()
    snapshot["control_state"]["disabled_reasons"].pop("dispatch")
    with pytest.raises(
        AgentProjectionQueryError,
        match="inspect_operation_control_state_disabled_reason_mismatch",
    ):
        execute_inspect_operation(request=_inspect_bound_request(), owner_snapshot=snapshot)

    commandless = execute_inspect_operation(
        request=_inspect_bound_request(),
        owner_snapshot=_inspect_snapshot(policy_status="not_applicable"),
    )
    commandless["provenance"].update(
        {
            "workflow_command_count": 1,
            "latest_workflow_command_id": "invented-command",
            "latest_workflow_command_type": "invented.command",
        }
    )
    with pytest.raises(
        AgentProjectionQueryError,
        match="inspect_operation_control_policy_not_applicable_mismatch",
    ):
        serialize_inspect_operation_result_v3(commandless)


def test_inspect_operation_supports_explicit_commandless_policy_without_inference() -> None:
    request = _inspect_bound_request()
    snapshot = _inspect_snapshot(policy_status="not_applicable")
    result = execute_inspect_operation(request=request, owner_snapshot=snapshot)

    assert result["control_policy"] == {
        "status": "not_applicable",
        "source_of_truth": "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
        "fallback_status": "fail_closed",
    }
    assert result["provenance"]["workflow_command_count"] == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda snapshot: snapshot["action"].update({"workspace_id": "workspace-foreign"}),
        lambda snapshot: snapshot["operation_run"].update({"workspace_id": "workspace-foreign"}),
        lambda snapshot: snapshot["operation_run"].update({"action_id": "action-foreign"}),
        lambda snapshot: snapshot["operation_run"].update({"operation_run_id": "oprun-foreign"}),
    ],
)
def test_inspect_operation_foreign_and_missing_are_indistinguishable(mutation) -> None:
    request = _inspect_bound_request()
    missing = execute_inspect_operation(request=request, owner_snapshot=None)
    foreign = _inspect_snapshot()
    mutation(foreign)

    assert execute_inspect_operation(request=request, owner_snapshot=foreign) == missing
    assert missing == {
        "variant": "error",
        "status": "failed",
        "reason": "operation_not_found",
        "retryable": False,
    }
    assert json.loads(serialize_inspect_operation_result(missing)) == missing


def test_inspect_operation_rejects_owner_drift_private_fields_paths_and_policy_inference() -> None:
    request = _inspect_bound_request()

    private_snapshot = _inspect_snapshot()
    private_snapshot["operation_run"]["raw_path"] = "/tmp/private.json"
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_snapshot_invalid"):
        execute_inspect_operation(request=request, owner_snapshot=private_snapshot)

    path_snapshot = _inspect_snapshot()
    path_snapshot["display_contract"]["description"] = "/Users/operator/private/operation.json"
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_result_not_model_safe"):
        execute_inspect_operation(request=request, owner_snapshot=path_snapshot)

    drifted_state = _inspect_snapshot()
    drifted_state["control_state"]["operation_status"] = "completed"
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_owner_projection_mismatch"):
        execute_inspect_operation(request=request, owner_snapshot=drifted_state)

    invented_policy = _inspect_snapshot(policy_status="not_applicable")
    invented_policy["control_policy"]["command_type"] = "invented.next.command"
    with pytest.raises(AgentProjectionQueryError, match="inspect_operation_control_policy_projection_invalid"):
        execute_inspect_operation(request=request, owner_snapshot=invented_policy)


def test_inspect_operation_control_state_and_policy_consistency_fail_closed() -> None:
    request = _inspect_bound_request()
    invalid_snapshots = []

    action_drift = _inspect_snapshot()
    action_drift["control_state"]["allowed_actions"] = ["cancel"]
    invalid_snapshots.append(action_drift)

    disabled_drift = _inspect_snapshot()
    disabled_drift["control_state"]["disabled_reasons"]["cancel"] = "invented_disabled_reason"
    invalid_snapshots.append(disabled_drift)

    command_drift = _inspect_snapshot()
    command_drift["provenance"]["latest_workflow_command_type"] = "other.command"
    invalid_snapshots.append(command_drift)

    for snapshot in invalid_snapshots:
        with pytest.raises(AgentProjectionQueryError):
            execute_inspect_operation(request=request, owner_snapshot=snapshot)


def test_v3_result_specs_are_f1_compatible_but_do_not_populate_global_registries() -> None:
    assert FILTER_PROJECTION_V2_RESULT_SPEC.tool_kind == "action"
    assert FILTER_PROJECTION_V2_RESULT_SPEC.route_identity == ("action", "filter_projection")
    assert INSPECT_OPERATION_RESULT_SPEC.tool_kind == "query"
    assert isinstance(INSPECT_OPERATION_RESULT_SPEC.owner_binding, ActionResultQueryOwner)
    assert INSPECT_OPERATION_RESULT_SPEC.owner_binding.owner_id == INSPECT_OPERATION_QUERY_OWNER_ID
    assert INSPECT_OPERATION_RESULT_SPEC.owner_binding.owner_revision == INSPECT_OPERATION_QUERY_OWNER_REVISION
    assert (
        INSPECT_OPERATION_RESULT_SPEC.owner_binding.owner_contract_digest
        == INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST
    )
    assert DEFAULT_ACTION_RESULT_REGISTRY.tool_names == ()


def test_v3_leaf_has_no_storage_provider_network_or_served_side_effect_surface() -> None:
    import sourcing_agent.agent_projection_query as module

    source = inspect.getsource(module)
    forbidden_tokens = (
        "ControlPlaneStore",
        "requests.",
        "urllib",
        "provider.call",
        "served = True",
        "DEFAULT_AGENT_TOOL_REGISTRY",
        "update_operation_state",
        "append_operation_event",
    )
    for token in forbidden_tokens:
        assert token not in source
