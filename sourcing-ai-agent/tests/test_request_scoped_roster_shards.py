"""Request-scoped location + functionID wiring for the company-employees roster lane.

Covers the unified roster-lane parameter contract
(``company_shard_planning.build_request_scoped_company_employee_query_plan``)
and the fixed-forward review findings:

- F4: ONLY user-explicit cohort selections produce paid per-function shards
  (flat mirrors, soft/text-inferred roles never do);
- F5: ``exclude_target_locations`` flows as ``exclude_locations`` through the
  plan, task metadata, policies, manifest lanes, workers, and the provider
  payload (``excludeLocations``);
- F2: request-owned locations/excludes/functions are composed into every
  adaptive/keyword policy, and explicit functions expand into per-function
  probe roots before optional keyword subdivision;
- F3/F9: manifest lanes mirror the canonical roster query plan (cross-layer
  request → plan → review → manifest → worker parity);
- F1: the expected shard set is durably persisted before dispatch so recovery
  fails closed (partial) until every expected shard is terminal;
- F6: a capped/truncated shard keeps the roster partial, never completed;
- F7: union-dedupe preserves multi-function provenance on duplicates;
- F8: rows without stable person identity never collapse across shards.

All fake/scripted: zero provider/model/network calls.  Engine-level tests use
the local control-plane Postgres like the sibling pipeline suites and skip
when no DSN is resolvable.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
import unittest.mock
from dataclasses import replace
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_shard_planning import (
    DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS,
    REQUEST_FUNCTION_PARTITION_STRATEGY_ID,
    build_default_company_employee_shard_policy,
    build_request_scoped_company_employee_query_plan,
    build_request_scoped_keyword_union_shard_policy,
    plan_company_employee_shards_from_policy,
    request_scoped_roster_function_ids,
    resolve_segmented_roster_completion,
)
from sourcing_agent.connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    annotate_roster_entry_shard_provenance,
    roster_merge_dedupe_key,
    roster_stable_member_key,
    union_roster_entry_provenance,
)
from sourcing_agent.domain import AcquisitionStrategyPlan, AcquisitionTask, JobRequest
from sourcing_agent.harvest_connectors import _build_harvest_company_employees_payload
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.plan_review import apply_plan_review_decision
from sourcing_agent.planning import _build_provider_execution_manifest, build_sourcing_plan
from sourcing_agent.query_signal_knowledge import function_id_selectable_labels
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.snapshot_materializer import _expected_segmented_company_roster_shard_ids
from sourcing_agent.storage import ControlPlaneStore

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _cohort(*, roles: list[str] | None = None, statuses: list[str] | None = None) -> dict:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": list(roles if roles is not None else ["research"]),
        "employment_statuses": list(statuses if statuses is not None else ["current"]),
        "role_match": "any",
        "source": "user_explicit",
    }


def _cohort_request_payload(**overrides: object) -> dict:
    payload: dict[str, object] = {
        "raw_user_request": "Find Reflection AI researchers and engineers",
        "query": "Reflection AI research engineering roster",
        "target_company": "Reflection AI",
        "cohort_selection": _cohort(roles=["research", "engineering"]),
    }
    payload.update(overrides)
    return payload


class RosterQueryPlanContractTest(unittest.TestCase):
    """The unified abstract method: parameters (location, functionIDs), no company branches."""

    def test_two_function_ids_yield_exactly_two_sharded_lane_payloads(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=None,
            function_ids=["24", "8"],
            max_pages=20,
            page_limit=25,
        )

        self.assertEqual(plan["strategy_id"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)
        self.assertEqual(len(plan["shards"]), 2)
        by_function = {
            str(shard["company_filters"]["function_ids"][0]): shard for shard in plan["shards"]
        }
        self.assertEqual(sorted(by_function, key=int), ["8", "24"])
        research_shard = by_function["24"]
        engineer_shard = by_function["8"]
        self.assertEqual(research_shard["company_filters"], {"locations": ["United States"], "function_ids": ["24"]})
        self.assertEqual(engineer_shard["company_filters"], {"locations": ["United States"], "function_ids": ["8"]})
        self.assertEqual(research_shard["max_pages"], 20)
        self.assertEqual(research_shard["page_limit"], 25)
        self.assertEqual(research_shard["title"], "United States / Researcher")
        self.assertEqual(engineer_shard["title"], "United States / Engineer")
        self.assertEqual(research_shard["shard_id"], "function_24")
        self.assertEqual(engineer_shard["shard_id"], "function_8")

    def test_no_function_selection_yields_one_unsharded_location_scoped_query(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=None,
            function_ids=[],
            max_pages=20,
            page_limit=25,
        )

        self.assertEqual(plan["shards"], [])
        self.assertEqual(plan["strategy_id"], "")
        self.assertEqual(plan["company_filters"], {"locations": ["United States"]})

    def test_location_defaults_to_united_states_only_when_field_absent(self) -> None:
        self.assertEqual(DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS, ["United States"])
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=None, function_ids=["19"], max_pages=10, page_limit=25
        )
        self.assertEqual(plan["locations"], ["United States"])
        self.assertEqual(plan["shards"][0]["company_filters"]["locations"], ["United States"])

    def test_multi_region_locations_pass_through_to_every_shard(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=["United States", "Germany"],
            function_ids=["24", "8"],
            max_pages=50,
            page_limit=25,
        )

        self.assertEqual(plan["locations"], ["United States", "Germany"])
        self.assertEqual(len(plan["shards"]), 2)
        for shard in plan["shards"]:
            self.assertEqual(shard["company_filters"]["locations"], ["United States", "Germany"])
        self.assertEqual(plan["company_filters"], {"locations": ["United States", "Germany"]})
        self.assertEqual(plan["shards"][0]["title"], "United States, Germany / Researcher")

    def test_explicit_empty_location_list_opts_out_of_location_filtering(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=[],
            function_ids=["24"],
            max_pages=10,
            page_limit=25,
        )

        self.assertEqual(plan["locations"], [])
        self.assertEqual(plan["company_filters"], {})
        self.assertEqual(plan["shards"][0]["company_filters"], {"function_ids": ["24"]})
        self.assertEqual(plan["shards"][0]["title"], "All locations / Researcher")

    def test_exclude_locations_compose_independently_everywhere(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=["United States"],
            exclude_target_locations=["France"],
            function_ids=["24", "8"],
            max_pages=10,
            page_limit=25,
        )

        self.assertEqual(plan["exclude_locations"], ["France"])
        self.assertEqual(
            plan["company_filters"],
            {"locations": ["United States"], "exclude_locations": ["France"]},
        )
        for shard in plan["shards"]:
            self.assertEqual(shard["company_filters"]["exclude_locations"], ["France"])

        opted_out = build_request_scoped_company_employee_query_plan(
            target_locations=[],
            exclude_target_locations=["France"],
            function_ids=[],
            max_pages=10,
            page_limit=25,
        )
        self.assertEqual(opted_out["company_filters"], {"exclude_locations": ["France"]})

    def test_function_ids_are_deduped_order_preserved(self) -> None:
        plan = build_request_scoped_company_employee_query_plan(
            target_locations=["Canada"],
            function_ids=["8", "24", "8", " 24 "],
            max_pages=10,
            page_limit=25,
        )

        self.assertEqual([shard["company_filters"]["function_ids"] for shard in plan["shards"]], [["8"], ["24"]])

    def test_function_id_labels_come_from_the_canonical_registry(self) -> None:
        labels = function_id_selectable_labels(["24", "8", "19", "9", "999"])

        self.assertEqual(labels["24"], "Researcher")
        self.assertEqual(labels["8"], "Engineer")
        self.assertEqual(labels["19"], "Product Manager")
        self.assertEqual(labels["9"], "Founder")
        self.assertEqual(labels["999"], "")

    def test_provider_payload_carries_locations_and_exclude_locations(self) -> None:
        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )

        payload, normalized_filters, _requested_items = _build_harvest_company_employees_payload(
            HarvestActorSettings(enabled=False),
            identity,
            max_pages=20,
            page_limit=25,
            company_filters={
                "locations": ["United States", "Germany"],
                "exclude_locations": ["France"],
                "function_ids": ["24"],
            },
        )

        self.assertEqual(payload["locations"], ["United States", "Germany"])
        self.assertEqual(payload["excludeLocations"], ["France"])
        self.assertEqual(payload["functionIds"], ["24"])
        self.assertEqual(
            normalized_filters,
            {
                "locations": ["United States", "Germany"],
                "exclude_locations": ["France"],
                "function_ids": ["24"],
            },
        )


class RequestScopedRosterFunctionIdsTest(unittest.TestCase):
    """F4: only user-explicit/cohort-owned selections may produce paid shards."""

    def test_user_explicit_cohort_maps_roles_through_registry(self) -> None:
        request = JobRequest.from_payload({"target_company": "Acme", "cohort_selection": _cohort(roles=["research", "product_management"])})

        self.assertEqual(request_scoped_roster_function_ids(request.to_record()), ["24", "19"])

    def test_flat_mirror_without_cohort_is_not_a_paid_selection(self) -> None:
        self.assertEqual(request_scoped_roster_function_ids({"must_have_primary_role_buckets": ["research", "engineering"]}), [])
        self.assertEqual(request_scoped_roster_function_ids({}), [])
        self.assertEqual(request_scoped_roster_function_ids(None), [])
        self.assertEqual(request_scoped_roster_function_ids({"must_have_primary_role_buckets": ["investor"]}), [])

    def test_text_inferred_soft_role_is_not_a_paid_selection(self) -> None:
        # "multimodal researcher" resolves a soft text-explicit role into the
        # flat mirror; it must NOT buy a function_24 roster shard.
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Lovable multimodal researcher",
                "query": "Lovable multimodal researcher",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )
        self.assertEqual(request.must_have_primary_role_buckets, ["research"])
        self.assertEqual(request_scoped_roster_function_ids(request.to_record()), [])

    def test_unknown_cohort_buckets_map_to_no_ids(self) -> None:
        cohort = _cohort(roles=["research"])
        cohort["role_bucket_ids"] = ["research", "unknown_role"]
        payload = {"target_company": "Acme", "cohort_selection": cohort}
        try:
            ids = request_scoped_roster_function_ids(payload)
        except Exception:
            ids = ["validation-raised"]
        self.assertIn(ids, (["24"], ["validation-raised"]))


class RosterLanePlanningTest(unittest.TestCase):
    """Planner + plan-review wiring of the request-scoped roster contract."""

    def test_text_role_request_does_not_buy_shards_beyond_the_default(self) -> None:
        # F4 under the ratified default: a soft text-inferred role never
        # expands the shard set — the policy is byte-identical to a plain
        # request's technical default.
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Lovable multimodal researcher full roster",
                "query": "Lovable multimodal researcher roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["strategy_type"], "full_company_roster")
        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        policy = dict(acquire_task.metadata["company_employee_shard_policy"])
        self.assertEqual(sorted(policy["request_function_ids"]), ["24", "8"])
        self.assertEqual(acquire_task.metadata["company_employee_base_filters"], {"locations": ["United States"]})

    def test_plan_without_function_selection_gets_the_technical_default_policy(self) -> None:
        # Ratified default (2026-07-19/22): no function selection means the
        # TECHNICAL default per-function policy (['8','24']) — never the
        # 233a31a-era unsharded query.
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Lovable full roster",
                "query": "Lovable roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["strategy_type"], "full_company_roster")
        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        policy = dict(acquire_task.metadata["company_employee_shard_policy"])
        self.assertEqual(sorted(policy["request_function_ids"]), ["24", "8"])
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertTrue(acquire_task.metadata["company_employee_shard_strategy"])

    def test_large_org_policy_uses_request_locations_instead_of_us_default(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "xAI full roster",
                "query": "xAI roster",
                "target_company": "xAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "target_locations": ["Germany"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        policy = dict(acquire_task.metadata["company_employee_shard_policy"] or {})
        self.assertEqual(str(policy.get("strategy_id") or ""), "unified_function_partition")
        self.assertEqual(policy.get("root_filters", {}).get("locations"), ["Germany"])
        # The generic technical default now also owns its partition axis through
        # request_function_ids: engineering/research are ALWAYS planned as
        # separate per-function shard roots (operator directive 2026-07-20), so
        # the field is present even without a user-explicit cohort selection.
        self.assertEqual(policy.get("request_function_ids"), ["8", "24"])

    def test_large_org_policy_carries_explicit_function_selection_for_probe_expansion(self) -> None:
        request = JobRequest.from_payload(
            _cohort_request_payload(
                target_company="xAI",
                raw_user_request="xAI researchers and engineers full roster",
                query="xAI research engineering roster",
                target_locations=["Germany"],
                exclude_target_locations=["France"],
            )
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        policy = dict(acquire_task.metadata["company_employee_shard_policy"] or {})
        self.assertEqual(policy.get("request_function_ids"), ["24", "8"])
        self.assertEqual(policy.get("root_filters", {}).get("locations"), ["Germany"])
        self.assertEqual(policy.get("root_filters", {}).get("exclude_locations"), ["France"])

    def test_plan_review_sync_rebuilds_the_unified_policy_with_request_axes(self) -> None:
        # Since 2026-07-22 (strategy Step 1) review sync emits ONLY the
        # unified adaptive-policy shape; cohort roles resolve into the
        # policy's function ids, never into explicit metadata shards.
        request_payload = _cohort_request_payload()
        plan_payload = {
            "target_company": "Reflection AI",
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": ["Reflection AI"],
                "filter_hints": {"current_companies": ["Reflection AI"]},
                "cost_policy": {},
                "search_channel_order": ["provider_people_search_api"],
                "search_seed_queries": [],
            },
            "publication_coverage": {"source_families": []},
            "acquisition_tasks": [
                {
                    "task_type": "acquire_full_roster",
                    "status": "ready",
                    "metadata": {},
                }
            ],
        }

        _updated_request, updated_plan = apply_plan_review_decision(request_payload, plan_payload, {})
        acquire_task = updated_plan["acquisition_tasks"][0]

        metadata = acquire_task["metadata"]
        self.assertEqual(metadata["company_employee_shards"], [])
        policy = dict(metadata["company_employee_shard_policy"])
        self.assertEqual(policy["request_function_ids"], ["24", "8"])
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(metadata["company_employee_shard_strategy"], policy["strategy_id"])
        self.assertEqual(metadata["company_employee_base_filters"], {"locations": ["United States"]})
    def test_partition_policy_expands_one_probe_root_per_function(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
            locations=["Germany"],
            request_function_ids=["24", "8"],
        )
        self.assertEqual(policy.get("request_function_ids"), ["24", "8"])
        self.assertEqual(policy.get("root_filters", {}).get("locations"), ["Germany"])

        counts = {"24": 800, "8": 1100}
        probed_filters: list[dict] = []

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            probed_filters.append(dict(filters))
            function_ids = list(dict(filters).get("function_ids") or [])
            return {"status": "completed", "estimated_total_count": counts.get(function_ids[0], 0)}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "request_function_partition")
        # Exactly one probe per function root; never a combined-function probe.
        self.assertEqual(len(probed_filters), 2)
        for filters in probed_filters:
            self.assertEqual(len(list(filters.get("function_ids") or [])), 1)
            self.assertEqual(filters.get("locations"), ["Germany"])
        self.assertEqual(len(plan["shards"]), 2)
        by_id = {shard["shard_id"]: shard for shard in plan["shards"]}
        self.assertEqual(sorted(by_id), ["function_24__germany_researcher", "function_8__germany_engineer"])
        self.assertEqual(by_id["function_24__germany_researcher"]["company_filters"]["function_ids"], ["24"])
        self.assertEqual(by_id["function_8__germany_engineer"]["company_filters"]["function_ids"], ["8"])
        for shard in plan["shards"]:
            self.assertEqual(shard["strategy_id"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)

    def test_empty_function_scope_is_skipped_not_blocking(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
            request_function_ids=["24", "9"],
        )

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            function_ids = list(dict(filters).get("function_ids") or [])
            return {"status": "completed", "estimated_total_count": 500 if function_ids == ["24"] else 0}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan.get("skipped_function_ids"), ["9"])
        self.assertEqual(len(plan["shards"]), 1)

    def test_all_empty_function_scopes_block_closed(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
            request_function_ids=["24", "9"],
        )

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            return {"status": "completed", "estimated_total_count": 0}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "blocked")
        self.assertEqual(plan["reason"], "root_probe_empty")


class ManifestLaneParityTest(unittest.TestCase):
    """F3: manifest lanes mirror the canonical roster query plan."""

    def _strategy(self, **overrides: object) -> AcquisitionStrategyPlan:
        payload: dict[str, object] = {
            "strategy_type": "full_company_roster",
            "target_population": "",
            "filter_hints": {"current_companies": ["Lovable"]},
            "cost_policy": {},
        }
        payload.update(overrides)
        return AcquisitionStrategyPlan(**payload)  # type: ignore[arg-type]

    def test_roster_lane_carries_exact_base_filters_and_paging(self) -> None:
        # Current adaptive-policy metadata shape (the only shape the
        # planner/review writer has emitted since 2026-07-22).
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="x",
            status="ready",
            blocking=True,
            metadata={
                "max_pages": 20,
                "page_limit": 25,
                "company_employee_base_filters": {"locations": ["Germany"], "exclude_locations": ["France"]},
                "company_employee_shards": [],
                "company_employee_shard_policy": build_default_company_employee_shard_policy(
                    max_pages=20,
                    page_limit=25,
                    locations=["Germany"],
                    exclude_locations=["France"],
                    request_function_ids=["24", "8"],
                ),
            },
        )

        manifest = _build_provider_execution_manifest(
            acquisition_strategy=self._strategy(),
            acquisition_tasks=[task],
        )

        roster_lanes = [lane for lane in manifest["lanes"] if lane["provider"] == "harvest_company_employees"]
        self.assertEqual(len(roster_lanes), 1)
        lane = roster_lanes[0]
        self.assertEqual(lane["lane_id"], "current_company_employees")
        self.assertEqual(
            lane["company_filters"],
            {"current_companies": ["Lovable"], "locations": ["Germany"], "exclude_locations": ["France"]},
        )
        self.assertEqual(lane["max_pages"], 20)
        self.assertEqual(lane["page_limit"], 25)
        # No combined function axis on an unsharded lane.
        self.assertNotIn("function_ids", lane["company_filters"])
    def test_stored_explicit_shards_metadata_still_surfaces_the_lane_view(self) -> None:
        # READ contract for pre-unification stored plans (R-034 forensics
        # 2026-07-22): explicit-shards metadata no longer gets one manifest
        # lane per shard — the manifest is the lane view (base filters +
        # paging); per-shard execution parity is pinned by
        # test_planner_emitted_metadata_shards_drive_the_same_lane.
        shards = build_request_scoped_company_employee_query_plan(
            target_locations=["Germany"],
            function_ids=["24", "8"],
            max_pages=20,
            page_limit=25,
            exclude_target_locations=["France"],
        )["shards"]
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="x",
            status="ready",
            blocking=True,
            metadata={
                "max_pages": 20,
                "page_limit": 25,
                "company_employee_base_filters": {"locations": ["Germany"], "exclude_locations": ["France"]},
                "company_employee_shards": shards,
                "company_employee_shard_policy": {},
                "company_employee_shard_strategy": REQUEST_FUNCTION_PARTITION_STRATEGY_ID,
            },
        )

        manifest = _build_provider_execution_manifest(
            acquisition_strategy=self._strategy(),
            acquisition_tasks=[task],
        )

        roster_lanes = [lane for lane in manifest["lanes"] if lane["provider"] == "harvest_company_employees"]
        self.assertEqual(len(roster_lanes), 1)
        lane_filters = dict(roster_lanes[0].get("company_filters") or {})
        lane_filters.pop("current_companies", None)
        self.assertEqual(lane_filters, {"locations": ["Germany"], "exclude_locations": ["France"]})
        self.assertEqual(roster_lanes[0]["max_pages"], 20)
        self.assertEqual(roster_lanes[0]["page_limit"], 25)
    def test_adaptive_policy_lane_shows_planned_root_scope_and_function_expansion(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
            locations=["Germany"],
            request_function_ids=["24", "8"],
        )
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="x",
            status="ready",
            blocking=True,
            metadata={
                "max_pages": 100,
                "page_limit": 25,
                "company_employee_base_filters": {"locations": ["Germany"]},
                "company_employee_shards": [],
                "company_employee_shard_policy": policy,
            },
        )

        manifest = _build_provider_execution_manifest(
            acquisition_strategy=self._strategy(),
            acquisition_tasks=[task],
        )

        roster_lanes = [lane for lane in manifest["lanes"] if lane["provider"] == "harvest_company_employees"]
        self.assertEqual(len(roster_lanes), 1)
        lane = roster_lanes[0]
        self.assertEqual(lane["reason"], "adaptive_shard_probe_pending")
        self.assertEqual(lane["company_filters"]["locations"], ["Germany"])
        self.assertEqual(lane["request_function_ids"], ["24", "8"])

    def test_completed_only_when_all_expected_present_and_untruncated(self) -> None:
        completion = resolve_segmented_roster_completion(
            expected_shard_ids=["function_24", "function_8"],
            shard_summaries=[
                {"shard_id": "function_24", "stop_reason": "completed"},
                {"shard_id": "function_8", "stop_reason": "completed"},
            ],
            completed_stop_reason="completed_segmented",
            partial_stop_reason="partial_segmented",
        )
        self.assertEqual(completion["completion_status"], "completed")
        self.assertEqual(completion["stop_reason"], "completed_segmented")
        self.assertEqual(completion["truncated_shard_ids"], [])
        self.assertEqual(completion["missing_shard_ids"], [])

    def test_missing_shard_keeps_partial(self) -> None:
        completion = resolve_segmented_roster_completion(
            expected_shard_ids=["function_24", "function_8"],
            shard_summaries=[{"shard_id": "function_24", "stop_reason": "completed"}],
            completed_stop_reason="completed_segmented",
            partial_stop_reason="partial_segmented",
        )
        self.assertEqual(completion["completion_status"], "partial")
        self.assertEqual(completion["missing_shard_ids"], ["function_8"])

    def test_capped_shard_keeps_partial(self) -> None:
        for truncated_summary in (
            {"shard_id": "function_8", "stop_reason": "provider_cap_reached"},
            {"shard_id": "function_8", "stop_reason": "requested_limit_reached"},
            {"shard_id": "function_8", "stop_reason": "completed", "partial_result": True},
            {"shard_id": "function_8", "stop_reason": "completed", "provider_cap_hit": True},
            {"shard_id": "function_8", "stop_reason": "completed", "provider_cap_limited": True},
            {"shard_id": "function_8", "stop_reason": "completed", "requested_limit_would_truncate": True},
        ):
            completion = resolve_segmented_roster_completion(
                expected_shard_ids=["function_24", "function_8"],
                shard_summaries=[{"shard_id": "function_24", "stop_reason": "completed"}, dict(truncated_summary)],
                completed_stop_reason="completed_segmented",
                partial_stop_reason="partial_segmented",
            )
            self.assertEqual(completion["completion_status"], "partial", truncated_summary)
            self.assertEqual(completion["stop_reason"], "partial_segmented", truncated_summary)
            self.assertEqual(completion["truncated_shard_ids"], ["function_8"], truncated_summary)


class RosterEntryMergeContractTest(unittest.TestCase):
    """F7/F8: canonical merge keeps provenance unions and never collapses lookalikes."""

    def test_duplicate_member_unions_function_and_shard_provenance(self) -> None:
        base = {"full_name": "Bob Both", "linkedin_url": "https://www.linkedin.com/in/bob-both/"}
        research_view = annotate_roster_entry_shard_provenance(
            dict(base),
            shard_id="function_24",
            shard_title="United States / Researcher",
            company_filters={"locations": ["United States"], "function_ids": ["24"]},
        )
        engineering_view = annotate_roster_entry_shard_provenance(
            dict(base),
            shard_id="function_8",
            shard_title="United States / Engineer",
            company_filters={"locations": ["United States"], "function_ids": ["8"]},
        )

        merged = union_roster_entry_provenance(research_view, engineering_view)

        self.assertEqual(merged["function_ids"], ["24", "8"])
        self.assertEqual(merged["source_shard_ids"], ["function_24", "function_8"])
        self.assertEqual(
            [str(item.get("shard_id") or "") for item in merged["source_shard_provenance"]],
            ["function_24", "function_8"],
        )
        # Singular compatibility fields stay first-shard-wins.
        self.assertEqual(merged["source_shard_id"], "function_24")
        self.assertEqual(merged["source_shard_filters"], {"locations": ["United States"], "function_ids": ["24"]})

    def test_stable_identity_dedupes_across_shards(self) -> None:
        self.assertEqual(
            roster_stable_member_key({"linkedin_url": "HTTPS://www.linkedin.com/in/Ada/"}),
            "https://www.linkedin.com/in/ada/",
        )
        self.assertEqual(
            roster_merge_dedupe_key({"linkedin_url": "https://www.linkedin.com/in/ada/"}, shard_id="function_24"),
            roster_merge_dedupe_key({"linkedin_url": "https://www.linkedin.com/in/ada/"}, shard_id="function_8"),
        )

    def test_lookalike_rows_without_stable_identity_never_collapse_across_shards(self) -> None:
        row = {"full_name": "Opaque Member", "headline": "Engineer", "location": "United States"}
        self.assertEqual(roster_stable_member_key(row), "")
        key_shard_a = roster_merge_dedupe_key(dict(row), shard_id="function_24")
        key_shard_b = roster_merge_dedupe_key(dict(row), shard_id="function_8")
        self.assertNotEqual(key_shard_a, key_shard_b)
        # ...but the same row reappearing inside one shard still dedupes.
        self.assertEqual(key_shard_a, roster_merge_dedupe_key(dict(row), shard_id="function_24"))


class RosterLaneExecutionTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """Engine-level lane wiring, fully scripted: no provider/model/network calls.

    Uses the repo-standard per-class PG schema fixture; the 233a31a-era manual
    DSN patch left the schema unselected and blocked every engine run
    (R-034 forensics, 2026-07-22)."""

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(str(Path(self.tempdir.name) / "test.db"))
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.acquisition_engine = AcquisitionEngine(
            self.catalog, self.settings, self.store, DeterministicModelClient()
        )
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        self.identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )

    def _task(self, metadata: dict | None = None) -> AcquisitionTask:
        return AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": False,
                "cost_policy": {"allow_company_employee_api": True},
                # pgLegacy deletion (2026-07-23): mirror the planner, which
                # mints the unified policy on every roster task — policy-less
                # tasks now fail closed instead of adopting request shards.
                "company_employee_shard_policy": build_default_company_employee_shard_policy(
                    max_pages=10,
                    page_limit=50,
                ),
                **dict(metadata or {}),
            },
        )

    def _snapshot(self, snapshot_dir: Path, entries: list[dict], *, stop_reason: str = "") -> CompanyRosterSnapshot:
        snapshot_dir = Path(snapshot_dir)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        summary_path = snapshot_dir / "summary.json"
        merged_path.write_text(json.dumps(entries), encoding="utf-8")
        visible_path.write_text(json.dumps(entries), encoding="utf-8")
        headless_path.write_text("[]", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
        # Mirror the real connector artifact layout so restore/reconciliation
        # paths can discover this shard's raw payload.
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (harvest_dir / "harvest_company_employees_raw.json").write_text(json.dumps(entries), encoding="utf-8")
        return CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Lovable",
            company_identity=self.identity,
            snapshot_dir=snapshot_dir,
            raw_entries=entries,
            visible_entries=entries,
            headless_entries=[],
            page_summaries=[{"page": 1, "entry_count": len(entries)}],
            accounts_used=["harvest_company_employees"],
            errors=[],
            stop_reason=stop_reason,
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )

    def _run_roster(
        self,
        request_payload: dict,
        *,
        metadata: dict | None = None,
        stop_reason_by_function: dict[str, str] | None = None,
    ) -> tuple[object, list[dict]]:
        fetch_calls: list[dict] = []

        def _fake_fetch(
            _identity,
            _snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
            runtime_timing_overrides=None,
        ):
            fetch_calls.append({"company_filters": dict(company_filters or {})})
            function_ids = list(dict(company_filters or {}).get("function_ids") or [])
            if function_ids == ["24"]:
                entries = [
                    {
                        "full_name": "Ada Research",
                        "linkedin_url": "https://www.linkedin.com/in/ada-research/",
                        "headline": "Researcher at Lovable",
                    },
                    {
                        "full_name": "Bob Both",
                        "linkedin_url": "https://www.linkedin.com/in/bob-both/",
                        "headline": "Research Engineer at Lovable",
                    },
                    {
                        "full_name": "Opaque Twin",
                        "headline": "Engineer",
                        "location": "United States",
                    },
                ]
            elif function_ids == ["8"]:
                entries = [
                    {
                        "full_name": "Bob Both",
                        "linkedin_url": "https://www.linkedin.com/in/bob-both/",
                        "headline": "Research Engineer at Lovable",
                    },
                    {
                        "full_name": "Cara Eng",
                        "linkedin_url": "https://www.linkedin.com/in/cara-eng/",
                        "headline": "Engineer at Lovable",
                    },
                    {
                        "full_name": "Opaque Twin",
                        "headline": "Engineer",
                        "location": "United States",
                    },
                ]
            else:
                entries = [
                    {
                        "full_name": "Dana Plain",
                        "linkedin_url": "https://www.linkedin.com/in/dana-plain/",
                        "headline": "Lovable member",
                    }
                ]
            stop_reason = str((stop_reason_by_function or {}).get(function_ids[0] if function_ids else "", ""))
            return self._snapshot(Path(_snapshot_dir), entries, stop_reason=stop_reason)

        def _fake_probe(
            _identity,
            _snapshot_dir,
            *,
            asset_logger=None,
            company_filters=None,
            probe_id="",
            title="",
            max_pages=1,
            page_limit=25,
            runtime_timing_overrides=None,
        ):
            # Ratified roster contract (2026-07-19/22): every roster request is
            # probe-planned into per-function shards; this stub answers each
            # probe root with an under-cap estimate so planning always
            # proceeds to per-shard fetches.
            return {"status": "completed", "estimated_total_count": 100}

        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-request-scoped"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch,
        ), unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "probe_company_roster_query",
            side_effect=_fake_probe,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                self._task(metadata),
                {"company_identity": self.identity, "snapshot_dir": snapshot_dir},
                JobRequest.from_payload(request_payload),
            )
        return execution, fetch_calls

    def test_policy_less_roster_task_fails_closed(self) -> None:
        # pgLegacy deletion (2026-07-23): the silent no-policy fallback that
        # adopted request-plan shards is retired. Every live plan mints the
        # unified policy (planning.py/plan_review.py) and the last
        # non-terminal pre-policy carriers were terminalized; a policy-less,
        # shard-less roster task is malformed input and must not proceed —
        # plain deletion would have widened into the unsharded root fetch.
        execution = self.acquisition_engine._acquire_full_roster(
            self._task({"company_employee_shard_policy": {}}),
            {"company_identity": self.identity, "snapshot_dir": Path(self.tempdir.name)},
            JobRequest.from_payload(
                {
                    "raw_user_request": "Lovable full roster",
                    "query": "Lovable roster",
                    "target_company": "Lovable",
                }
            ),
        )
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload.get("reason"), "company_employee_shard_policy_missing")

    def test_former_only_strategy_routes_to_the_former_lane_not_keyword_pool(self) -> None:
        # WS1 Step 2a (2026-07-22): standalone former-only requests must take
        # the SAME per-function former lane as the full-roster companion seed.
        # Before this change strategy_type=former_employee_search on the roster
        # task dispatched into _acquire_search_seed_pool (keyword recall) and
        # never reached build_request_scoped_former_search_shard_plan — the
        # divergence pinned by test_strategy_contract_preflight PIN_step2.
        calls: list[str] = []
        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_acquire_former_search_seed",
            side_effect=lambda *a, **k: calls.append("former_lane") or "former-execution",
        ), unittest.mock.patch.object(
            self.acquisition_engine,
            "_acquire_search_seed_pool",
            side_effect=lambda *a, **k: calls.append("keyword_pool") or "pool-execution",
        ):
            task = self._task({"strategy_type": "former_employee_search"})
            result = self.acquisition_engine.execute_task(
                task,
                JobRequest.from_payload(
                    {
                        "raw_user_request": "Former Lovable people",
                        "query": "Lovable former employees",
                        "target_company": "Lovable",
                        "categories": ["employee"],
                        "employment_statuses": ["former"],
                    }
                ),
                "Lovable",
                {"company_identity": self.identity, "snapshot_dir": Path(self.tempdir.name)},
            )
        self.assertEqual(calls, ["former_lane"])
        self.assertEqual(result, "former-execution")

    def test_roster_defaults_to_per_function_shards_with_us_location(self) -> None:
        # Ratified contract (2026-07-19 directive, re-ratified 2026-07-22):
        # a plain roster request defaults to per-function shards (technical
        # default ['8','24']) with the US location default — never one
        # combined unsharded query (the 233a31a-era expectation).
        execution, fetch_calls = self._run_roster(
            {
                "raw_user_request": "Lovable full roster",
                "query": "Lovable roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(
            sorted(json.dumps(c["company_filters"], sort_keys=True) for c in fetch_calls),
            [
                json.dumps({"function_ids": ["24"], "locations": ["United States"]}, sort_keys=True),
                json.dumps({"function_ids": ["8"], "locations": ["United States"]}, sort_keys=True),
            ],
        )

    def test_roster_shards_all_carry_multi_region_locations_and_excludes(self) -> None:
        execution, fetch_calls = self._run_roster(
            {
                "raw_user_request": "Lovable full roster",
                "query": "Lovable roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "target_locations": ["United States", "Germany"],
                "exclude_target_locations": ["France"],
            }
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(fetch_calls), 2)
        for call in fetch_calls:
            filters = call["company_filters"]
            self.assertEqual(filters["locations"], ["United States", "Germany"])
            self.assertEqual(filters["exclude_locations"], ["France"])
            self.assertIn(filters["function_ids"], (["8"], ["24"]))

    def test_text_role_request_does_not_alter_the_default_shard_set(self) -> None:
        # F4 (updated to the ratified fn-sharded default, 2026-07-22): a soft
        # text-inferred role must not buy shards beyond the technical default —
        # the shard set is identical to a plain roster request (['8','24']),
        # with no role-derived additions.
        execution, fetch_calls = self._run_roster(
            {
                "raw_user_request": "Lovable multimodal researcher full roster",
                "query": "Lovable multimodal researcher roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(
            sorted(tuple(c["company_filters"].get("function_ids") or []) for c in fetch_calls),
            [("24",), ("8",)],
        )

    def test_cohort_functions_dispatch_one_query_per_function_with_provenance(self) -> None:
        execution, fetch_calls = self._run_roster(
            _cohort_request_payload(target_company="Lovable", exclude_target_locations=["France"])
        )

        self.assertEqual(execution.status, "completed")
        # Exactly two sharded lane payloads, one per registry function id.
        self.assertEqual(len(fetch_calls), 2)
        self.assertEqual(
            sorted(json.dumps(call["company_filters"], sort_keys=True) for call in fetch_calls),
            [
                json.dumps(
                    {"locations": ["United States"], "exclude_locations": ["France"], "function_ids": ["24"]},
                    sort_keys=True,
                ),
                json.dumps(
                    {"locations": ["United States"], "exclude_locations": ["France"], "function_ids": ["8"]},
                    sort_keys=True,
                ),
            ],
        )
        roster_snapshot = execution.state_updates.get("roster_snapshot")
        self.assertIsNotNone(roster_snapshot)
        merged_urls = sorted(
            str(entry.get("linkedin_url") or "") for entry in roster_snapshot.raw_entries if entry.get("linkedin_url")
        )
        self.assertEqual(
            merged_urls,
            [
                "https://www.linkedin.com/in/ada-research/",
                "https://www.linkedin.com/in/bob-both/",
                "https://www.linkedin.com/in/cara-eng/",
            ],
        )
        by_url = {str(entry.get("linkedin_url") or ""): entry for entry in roster_snapshot.raw_entries}
        # F7: the dual-function member keeps BOTH shards' provenance.
        bob = by_url["https://www.linkedin.com/in/bob-both/"]
        self.assertEqual(bob["function_ids"], ["24", "8"])
        # pgLegacy deletion (2026-07-23): shard ids are the unified policy
        # planner's slugs — the bare function_N ids were the retired
        # request-plan adoption path's artifact, never the production shape.
        self.assertEqual(
            bob["source_shard_ids"],
            ["function_24__united_states_researcher", "function_8__united_states_engineer"],
        )
        self.assertEqual(bob["source_shard_id"], "function_24__united_states_researcher")
        self.assertEqual(by_url["https://www.linkedin.com/in/ada-research/"]["function_ids"], ["24"])
        self.assertEqual(by_url["https://www.linkedin.com/in/cara-eng/"]["function_ids"], ["8"])
        # F8: the two opaque lookalike rows (no stable identity) both survive.
        opaque_rows = [entry for entry in roster_snapshot.raw_entries if entry.get("full_name") == "Opaque Twin"]
        self.assertEqual(len(opaque_rows), 2)
        self.assertEqual(
            sorted(str(entry.get("source_shard_id") or "") for entry in opaque_rows),
            ["function_24__united_states_researcher", "function_8__united_states_engineer"],
        )
        # Artifact manifest records which function lane produced which members.
        summary_payload = json.loads((roster_snapshot.summary_path).read_text(encoding="utf-8"))
        self.assertEqual(summary_payload["strategy_id"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)
        self.assertEqual(summary_payload["completion_status"], "completed")
        self.assertEqual(summary_payload["stop_reason"], "completed_segmented")
        shard_rows = {str(row.get("shard_id") or ""): dict(row) for row in summary_payload["shard_summaries"]}
        self.assertEqual(
            sorted(shard_rows), ["function_24__united_states_researcher", "function_8__united_states_engineer"]
        )
        self.assertEqual(shard_rows["function_24__united_states_researcher"]["unique_entry_count"], 3)
        self.assertEqual(shard_rows["function_8__united_states_engineer"]["unique_entry_count"], 2)
        self.assertEqual(shard_rows["function_8__united_states_engineer"]["duplicate_entry_count"], 1)
        self.assertFalse(shard_rows["function_24__united_states_researcher"]["partial_result"])
        self.assertFalse(shard_rows["function_8__united_states_engineer"]["partial_result"])
        # F1: the expected shard set is durably recorded for recovery.
        plan_path = (
            self.settings.company_assets_dir
            / "lovable"
            / "snapshot-request-scoped"
            / "harvest_company_employees"
            / "adaptive_shard_plan.json"
        )
        self.assertTrue(plan_path.exists())
        plan_payload = json.loads(plan_path.read_text(encoding="utf-8"))
        self.assertEqual(plan_payload["status"], "planned")
        # Policy-path plan persists the partition as reason + policy strategy;
        # the top-level strategy_id was the retired request-plan adoption shape.
        self.assertEqual(plan_payload["reason"], "request_function_partition")
        self.assertEqual(
            sorted(str(item.get("shard_id") or "") for item in plan_payload["shards"]),
            ["function_24__united_states_researcher", "function_8__united_states_engineer"],
        )
        self.assertEqual(
            _expected_segmented_company_roster_shard_ids(
                self.settings.company_assets_dir / "lovable" / "snapshot-request-scoped"
            ),
            ["function_24__united_states_researcher", "function_8__united_states_engineer"],
        )

    def test_capped_function_shard_keeps_roster_partial(self) -> None:
        execution, fetch_calls = self._run_roster(
            _cohort_request_payload(target_company="Lovable"),
            stop_reason_by_function={"8": "provider_cap_reached"},
        )

        self.assertEqual(len(fetch_calls), 2)
        roster_snapshot = execution.state_updates.get("roster_snapshot")
        self.assertIsNotNone(roster_snapshot)
        # F6: the capped function shard must not be reported as completed coverage.
        self.assertEqual(roster_snapshot.stop_reason, "partial_segmented")
        summary_payload = json.loads((roster_snapshot.summary_path).read_text(encoding="utf-8"))
        self.assertEqual(summary_payload["completion_status"], "partial")
        self.assertEqual(summary_payload["stop_reason"], "partial_segmented")
        self.assertEqual(summary_payload["truncated_shard_ids"], ["function_8__united_states_engineer"])
        shard_rows = {str(row.get("shard_id") or ""): dict(row) for row in summary_payload["shard_summaries"]}
        self.assertTrue(shard_rows["function_8__united_states_engineer"]["partial_result"])
        self.assertTrue(shard_rows["function_8__united_states_engineer"]["provider_cap_hit"])
        self.assertFalse(shard_rows["function_24__united_states_researcher"]["partial_result"])

    def test_recovery_stays_partial_until_every_expected_shard_is_terminal(self) -> None:
        execution, _fetch_calls = self._run_roster(_cohort_request_payload(target_company="Lovable"))
        self.assertEqual(execution.status, "completed")
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-request-scoped"

        # Simulate out-of-order completion: function_8's shard artifacts are
        # gone (still pending) while function_24 is complete, and the root
        # snapshot has not been materialized yet.
        shard_root = snapshot_dir / "harvest_company_employees" / "shards"
        self.assertTrue((shard_root / "function_8__united_states_engineer").exists())
        shutil.rmtree(shard_root / "function_8__united_states_engineer")
        for stale_root_artifact in (
            "harvest_company_employees_merged.json",
            "harvest_company_employees_visible.json",
            "harvest_company_employees_headless.json",
            "harvest_company_employees_summary.json",
        ):
            artifact_path = snapshot_dir / "harvest_company_employees" / stale_root_artifact
            if artifact_path.exists():
                artifact_path.unlink()

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=lambda *_args, **_kwargs: self._snapshot(
                shard_root / "function_24__united_states_researcher",
                [
                    {
                        "full_name": "Ada Research",
                        "linkedin_url": "https://www.linkedin.com/in/ada-research/",
                        "headline": "Researcher at Lovable",
                    }
                ],
            ),
        ):
            restored = orchestrator._restore_roster_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=self.identity,
            )

        self.assertIsNotNone(restored)
        # F1: recovery fails closed — one of two expected function shards is
        # not terminal, so the roster stays partial instead of completing.
        self.assertEqual(restored.stop_reason, "partial_segmented")
        restored_summary = json.loads(
            (snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(restored_summary["completion_status"], "partial")
        self.assertEqual(restored_summary["missing_shard_ids"], ["function_8__united_states_engineer"])

    def test_planner_emitted_metadata_shards_drive_the_same_lane(self) -> None:
        planner_shards = build_request_scoped_company_employee_query_plan(
            target_locations=["Germany"],
            function_ids=["24", "8"],
            max_pages=20,
            page_limit=25,
        )["shards"]

        execution, fetch_calls = self._run_roster(
            _cohort_request_payload(target_company="Lovable", target_locations=["Germany"]),
            metadata={"company_employee_shards": planner_shards},
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(fetch_calls), 2)
        for call in fetch_calls:
            self.assertEqual(call["company_filters"]["locations"], ["Germany"])
        self.assertEqual(
            {call["company_filters"]["function_ids"][0] for call in fetch_calls},
            {"24", "8"},
        )

    def test_manifest_task_worker_parity_for_default_sharded_request(self) -> None:
        # F3/F9 cross-layer preflight: the request's location axes must read
        # identically in task metadata, the provider manifest lane, and the
        # filters the worker actually submits.
        request_payload = {
            "raw_user_request": "Lovable full roster",
            "query": "Lovable roster",
            "target_company": "Lovable",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "target_locations": ["Germany"],
            "exclude_target_locations": ["France"],
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        expected_filters = {"locations": ["Germany"], "exclude_locations": ["France"]}
        self.assertEqual(acquire_task.metadata["company_employee_base_filters"], expected_filters)

        manifest = dict(plan.acquisition_strategy.provider_execution_manifest or {})
        roster_lanes = [lane for lane in manifest.get("lanes") or [] if lane.get("provider") == "harvest_company_employees"]
        self.assertEqual(len(roster_lanes), 1)
        lane_filters = dict(roster_lanes[0].get("company_filters") or {})
        lane_filters.pop("current_companies", None)
        self.assertEqual(lane_filters, expected_filters)

        execution, fetch_calls = self._run_roster(request_payload)
        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(fetch_calls), 2)
        for call in fetch_calls:
            filters = dict(call["company_filters"])
            self.assertIn(filters.pop("function_ids"), (["8"], ["24"]))
            self.assertEqual(filters, expected_filters)


class _KeywordUnionFakeHarvestSettings:
    enabled = True
    max_paid_items = 100


class _KeywordUnionFakeHarvestConnector:
    """Captures every ``search_profiles`` provider call (the paid payload seam)."""

    settings = _KeywordUnionFakeHarvestSettings()

    def __init__(
        self,
        *,
        rows_by_query: dict[str, list[dict]] | None = None,
        raise_for_queries: frozenset[str] | set[str] = frozenset(),
    ) -> None:
        self.calls: list[dict] = []
        self.rows_by_query = dict(rows_by_query or {})
        self.raise_for_queries = set(raise_for_queries)

    def _default_row(self, query_text: str) -> list[dict]:
        slug = query_text.lower().replace(" ", "-") or "person"
        return [
            {
                "full_name": f"Person {query_text}",
                "headline": "Researcher",
                "location": "United States",
                "profile_url": f"https://www.linkedin.com/in/{slug}/",
                "username": slug,
                "current_company": "Lovable",
            }
        ]

    def search_profiles(self, **kwargs):
        self.calls.append(dict(kwargs))
        query_text = str(kwargs.get("query_text") or "")
        if query_text in self.raise_for_queries:
            raise RuntimeError("scripted transient provider failure")
        rows = self.rows_by_query.get(query_text)
        if rows is None:
            rows = self._default_row(query_text)
        return {
            "raw_path": Path(kwargs["discovery_dir"]) / f"harvest_{len(self.calls):02d}.json",
            "rows": [dict(row) for row in rows],
            "pagination": {},
            "payload": {},
        }


class ScopedKeywordUnionSeedLaneTest(unittest.TestCase):
    """WS1 Step 4b-B (ruling RATIFIED 2026-07-23, B-then-A): scoped keyword
    shards execute as first-class shards of the paid people-search lane —
    plan persisted, per-shard dispatch on the EXISTING seed-pool provider
    surface, honest ``resolve_segmented_roster_completion`` merge — while
    policy-less legacy tasks keep the plain seed-pool.  All scripted: zero
    provider/model/network calls."""

    SCOPED_COST_POLICY = {
        "provider_people_search_mode": "primary_only",
        "provider_people_search_min_expected_results": 2,
        "provider_people_search_pages": 1,
        "provider_people_search_accept_zero_results": True,
    }

    def setUp(self) -> None:
        self.identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )
        self.policy = build_request_scoped_keyword_union_shard_policy(
            keywords=["Pre-train", "Robotics"],
            function_ids=[],
            max_pages=10,
            page_limit=50,
        )

    def _discover(
        self,
        connector: _KeywordUnionFakeHarvestConnector,
        snapshot_dir: Path,
        *,
        policy: dict | None,
        cost_policy_overrides: dict | None = None,
        keywords: list[str] | None = None,
    ):
        from sourcing_agent.asset_logger import AssetLogger
        from sourcing_agent.seed_discovery import SearchSeedAcquirer

        snapshot_dir.mkdir(parents=True, exist_ok=True)
        acquirer = SearchSeedAcquirer([], harvest_search_connector=connector)
        return acquirer.discover(
            self.identity,
            snapshot_dir,
            asset_logger=AssetLogger(snapshot_dir),
            search_seed_queries=list(keywords or ["Pre-train", "Robotics"]),
            query_bundles=[],
            filter_hints={},
            cost_policy={**self.SCOPED_COST_POLICY, **dict(cost_policy_overrides or {})},
            employment_status="current",
            scoped_keyword_union_shard_policy=policy,
        )

    def _plan_payload(self, snapshot_dir: Path) -> dict:
        plan_path = snapshot_dir / "search_seed_discovery" / "scoped_keyword_union_shard_plan.json"
        self.assertTrue(plan_path.exists(), "expected the persisted keyword-union shard plan")
        return json.loads(plan_path.read_text(encoding="utf-8"))

    def test_multi_keyword_dispatch_one_provider_query_per_shard(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            connector = _KeywordUnionFakeHarvestConnector()
            snapshot_dir = Path(tempdir) / "snap"
            snapshot = self._discover(connector, snapshot_dir, policy=self.policy)
            plan_payload = self._plan_payload(snapshot_dir)
        # One provider call per keyword shard (parallel dispatch — assert the set).
        self.assertEqual(sorted(call["query_text"] for call in connector.calls), ["Pre-train", "Robotics"])
        block = dict(snapshot.to_record().get("scoped_keyword_union") or {})
        self.assertEqual(block.get("strategy_id"), "request_scoped_keyword_union")
        # Shard ids come verbatim from the planner policy (single-writer rule).
        self.assertEqual(block.get("expected_shard_ids"), ["kw_pre_train", "kw_robotics"])
        by_shard = {item["shard_id"]: item for item in block.get("shards") or []}
        self.assertEqual(by_shard["kw_pre_train"]["dispatch_status"], "dispatched")
        self.assertEqual(by_shard["kw_pre_train"]["provider_query"], "Pre-train")
        self.assertEqual(by_shard["kw_robotics"]["dispatch_status"], "dispatched")
        completion = dict(block.get("completion") or {})
        self.assertEqual(completion.get("completion_status"), "completed")
        self.assertEqual(completion.get("stop_reason"), "completed_keyword_union")
        self.assertEqual(plan_payload.get("expected_shard_ids"), ["kw_pre_train", "kw_robotics"])
        self.assertEqual(
            dict(plan_payload.get("completion") or {}).get("completion_status"),
            "completed",
        )

    def test_truncated_shard_keeps_the_union_partial(self) -> None:
        # One keyword's provider query fails into the durable retry lane —
        # its shard carries truncation evidence, so the honest completion is
        # partial, never completed (same contract as the segmented roster).
        with tempfile.TemporaryDirectory() as tempdir:
            connector = _KeywordUnionFakeHarvestConnector(raise_for_queries={"Robotics"})
            snapshot_dir = Path(tempdir) / "snap"
            snapshot = self._discover(connector, snapshot_dir, policy=self.policy)
        completion = dict(dict(snapshot.to_record().get("scoped_keyword_union") or {}).get("completion") or {})
        self.assertEqual(completion.get("completion_status"), "partial")
        self.assertEqual(completion.get("stop_reason"), "partial_keyword_union")
        self.assertEqual(completion.get("truncated_shard_ids"), ["kw_robotics"])
        self.assertEqual(completion.get("missing_shard_ids"), [])

    def test_query_budget_cut_shard_is_missing_not_silently_dropped(self) -> None:
        # Pre-4b the seed pool silently dropped keywords beyond
        # provider_people_search_max_queries; under the shard contract the cut
        # keyword stays in the expected set and keeps the union partial.
        with tempfile.TemporaryDirectory() as tempdir:
            connector = _KeywordUnionFakeHarvestConnector()
            snapshot_dir = Path(tempdir) / "snap"
            snapshot = self._discover(
                connector,
                snapshot_dir,
                policy=self.policy,
                cost_policy_overrides={"provider_people_search_max_queries": 1},
            )
        self.assertEqual([call["query_text"] for call in connector.calls], ["Pre-train"])
        block = dict(snapshot.to_record().get("scoped_keyword_union") or {})
        by_shard = {item["shard_id"]: item for item in block.get("shards") or []}
        self.assertEqual(by_shard["kw_robotics"]["dispatch_status"], "not_dispatched_query_budget")
        completion = dict(block.get("completion") or {})
        self.assertEqual(completion.get("completion_status"), "partial")
        self.assertEqual(completion.get("missing_shard_ids"), ["kw_robotics"])

    def test_duplicate_person_across_keyword_shards_unions_provenance(self) -> None:
        duplicate_row = [
            {
                "full_name": "Dual Hit",
                "headline": "Research Engineer",
                "location": "United States",
                "profile_url": "https://www.linkedin.com/in/dual-hit/",
                "username": "dual-hit",
                "current_company": "Lovable",
            }
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            connector = _KeywordUnionFakeHarvestConnector(
                rows_by_query={"Pre-train": duplicate_row, "Robotics": duplicate_row}
            )
            snapshot_dir = Path(tempdir) / "snap"
            snapshot = self._discover(connector, snapshot_dir, policy=self.policy)
        self.assertEqual(len(snapshot.entries), 1)
        entry = dict(snapshot.entries[0])
        self.assertEqual(
            dict(entry.get("metadata") or {}).get("scoped_keyword_union_shard_ids"),
            ["kw_pre_train", "kw_robotics"],
        )
        completion = dict(dict(snapshot.to_record().get("scoped_keyword_union") or {}).get("completion") or {})
        self.assertEqual(completion.get("completion_status"), "completed")

    def test_policy_absent_falls_back_to_the_plain_seed_pool(self) -> None:
        # 4c owns seed-pool retirement; a policy-less (legacy/hydrated) task
        # must keep today's ungoverned dispatch with no shard-plan artifacts.
        with tempfile.TemporaryDirectory() as tempdir:
            connector = _KeywordUnionFakeHarvestConnector()
            snapshot_dir = Path(tempdir) / "snap"
            snapshot = self._discover(connector, snapshot_dir, policy=None)
            plan_path = snapshot_dir / "search_seed_discovery" / "scoped_keyword_union_shard_plan.json"
            self.assertFalse(plan_path.exists())
        # Parallel dispatch: assert the query SET (order is worker-timing).
        self.assertEqual(sorted(call["query_text"] for call in connector.calls), ["Pre-train", "Robotics"])
        self.assertNotIn("scoped_keyword_union", snapshot.to_record())
        self.assertNotIn("scoped_keyword_union", dict(snapshot.summary_payload or {}))

    def test_provider_payloads_are_byte_compatible_with_the_legacy_seed_pool(self) -> None:
        # The 4b-B ruling reuses the EXISTING seed-pool provider surface: for
        # the same request, the per-keyword connector payloads must be
        # byte-identical between the legacy (policy-less) path and the
        # shard-governed path — no payload guessing.
        def _sanitized_calls(connector: _KeywordUnionFakeHarvestConnector) -> list[str]:
            sanitized = []
            for call in connector.calls:
                payload = {
                    key: value
                    for key, value in call.items()
                    if key not in {"discovery_dir", "asset_logger"}
                }
                sanitized.append(json.dumps(payload, sort_keys=True, default=str))
            # Parallel dispatch order is worker-timing; the per-query payload
            # bytes are the contract under test.
            return sorted(sanitized)

        with tempfile.TemporaryDirectory() as tempdir:
            legacy_connector = _KeywordUnionFakeHarvestConnector()
            self._discover(legacy_connector, Path(tempdir) / "legacy", policy=None)
            sharded_connector = _KeywordUnionFakeHarvestConnector()
            self._discover(sharded_connector, Path(tempdir) / "sharded", policy=self.policy)
        self.assertTrue(legacy_connector.calls, "legacy seed pool must dispatch provider calls")
        self.assertEqual(_sanitized_calls(legacy_connector), _sanitized_calls(sharded_connector))

    def test_dispatch_records_planner_shard_ids_verbatim(self) -> None:
        # Shard-id single-writer rule (9544b69): execution records the
        # planner's ids verbatim and never re-normalizes them.
        from sourcing_agent.seed_discovery import SearchSeedAcquirer

        acquirer = SearchSeedAcquirer([], harvest_search_connector=_KeywordUnionFakeHarvestConnector())
        dispatch = acquirer.resolve_scoped_keyword_union_shard_dispatch(
            policy={
                "strategy_id": "request_scoped_keyword_union",
                "mode": "keyword_union",
                "keyword_shards": [
                    {"rule_id": "KW_Pre-Train.v2", "title": "Pre-train", "include_patch": {"keywords": ["Pre-train"]}}
                ],
            },
            identity=self.identity,
            filter_hints={},
            search_seed_queries=["Pre-train"],
            cost_policy=dict(self.SCOPED_COST_POLICY),
        )
        self.assertEqual(dispatch.get("expected_shard_ids"), ["KW_Pre-Train.v2"])
        self.assertEqual(dispatch["shards"][0]["dispatch_status"], "dispatched")


class _StopLaneProbe(RuntimeError):
    pass


class ScopedKeywordUnionSeedLaneWiringTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """Engine-level 4b-B wiring: ``_acquire_search_seed_pool`` forwards the
    planner-minted policy from task metadata into the discovery lane (and an
    empty policy for legacy tasks keeps the plain seed-pool)."""

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(str(Path(self.tempdir.name) / "test.db"))
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.engine = AcquisitionEngine(self.catalog, self.settings, self.store, DeterministicModelClient())
        self.identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )

    def _seed_pool_discover_kwargs(self, metadata: dict, *, employment_statuses: list[str] | None = None) -> dict:
        from sourcing_agent.seed_discovery import SearchSeedAcquirer

        statuses = list(employment_statuses or ["current"])
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire scoped roster",
            description="Scoped keyword roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "scoped_search_roster",
                "employment_statuses": statuses,
                "include_former_search_seed": False,
                "search_seed_queries": ["Pre-train", "Robotics"],
                "cost_policy": {"provider_people_search_mode": "primary_only"},
                **metadata,
            },
        )
        captured: dict = {}

        def _fake_discover(_identity, _snapshot_dir, **kwargs):
            captured.update(kwargs)
            raise _StopLaneProbe()

        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "lovable" / "snap"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        with unittest.mock.patch.object(SearchSeedAcquirer, "discover", side_effect=_fake_discover):
            with self.assertRaises(_StopLaneProbe):
                self.engine._acquire_search_seed_pool(
                    task,
                    {"company_identity": self.identity, "snapshot_dir": snapshot_dir},
                    JobRequest.from_payload(
                        {
                            "raw_user_request": "Lovable Pre-train people",
                            "query": "Lovable Pre-train people",
                            "target_company": "Lovable",
                            "categories": ["employee"],
                            "employment_statuses": statuses,
                            "keywords": ["Pre-train", "Robotics"],
                        }
                    ),
                )
        return captured

    def test_planner_minted_policy_reaches_the_discovery_lane(self) -> None:
        policy = build_request_scoped_keyword_union_shard_policy(
            keywords=["Pre-train", "Robotics"],
            function_ids=[],
            max_pages=10,
            page_limit=50,
        )
        captured = self._seed_pool_discover_kwargs({"scoped_keyword_union_shard_policy": policy})
        self.assertEqual(dict(captured.get("scoped_keyword_union_shard_policy") or {}), policy)

    def test_legacy_task_without_policy_passes_no_shard_governance(self) -> None:
        captured = self._seed_pool_discover_kwargs({})
        self.assertIn("scoped_keyword_union_shard_policy", captured)
        self.assertFalse(dict(captured.get("scoped_keyword_union_shard_policy") or {}))

    def test_former_companion_pass_is_not_keyword_union_governed(self) -> None:
        # The keyword-union policy governs the CURRENT-member scoped roster;
        # the former companion pass owns its own former shard-plan contract
        # and shares the discovery dir — forwarding the policy there would
        # double-write the persisted plan.
        policy = build_request_scoped_keyword_union_shard_policy(
            keywords=["Pre-train", "Robotics"],
            function_ids=[],
            max_pages=10,
            page_limit=50,
        )
        captured = self._seed_pool_discover_kwargs(
            {"scoped_keyword_union_shard_policy": policy},
            employment_statuses=["former"],
        )
        self.assertFalse(dict(captured.get("scoped_keyword_union_shard_policy") or {}))


if __name__ == "__main__":
    unittest.main()
