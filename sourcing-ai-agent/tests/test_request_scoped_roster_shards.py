"""Request-scoped location + functionID wiring for the company-employees roster lane.

Covers the unified roster-lane parameter contract
(``company_shard_planning.build_request_scoped_company_employee_query_plan``):

- request ``target_locations`` → lane ``locations`` filter (United States
  default when the field is absent, multi-region pass-through, explicit ``[]``
  opt-out — single-writer rule, never merged with the default);
- explicit function selection → one sharded company-employees query per
  canonical registry function id (research→"24", engineering→"8",
  product_management→"19"), each shard carrying honest paging limits;
- planner metadata + plan-review sync keep the request-scoped shards instead
  of silently dropping them;
- the segmented merge union-dedupes overlapping members across function
  shards and records shard provenance in queue/artifact manifests.

All fake/scripted: zero provider/model/network calls.  The engine-level
execution tests use the local control-plane Postgres like the sibling
pipeline suites and skip when no DSN is resolvable.
"""

from __future__ import annotations

import json
import os
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
    build_request_scoped_company_employee_query_plan,
    request_scoped_roster_function_ids,
)
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.domain import AcquisitionTask, JobRequest
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.plan_review import apply_plan_review_decision
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.query_signal_knowledge import function_id_selectable_labels
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _role_request_payload(**overrides: object) -> dict:
    payload: dict[str, object] = {
        "raw_user_request": "Find Reflection AI researchers and engineers",
        "query": "Reflection AI research engineering roster",
        "target_company": "Reflection AI",
        "categories": ["employee"],
        "employment_statuses": ["current"],
        "must_have_primary_role_buckets": ["research", "engineering"],
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
        # Each shard is its own company-employees query: single functionId +
        # the location filter + honest paging limits.
        self.assertEqual(research_shard["company_filters"], {"locations": ["United States"], "function_ids": ["24"]})
        self.assertEqual(engineer_shard["company_filters"], {"locations": ["United States"], "function_ids": ["8"]})
        self.assertEqual(research_shard["max_pages"], 20)
        self.assertEqual(research_shard["page_limit"], 25)
        self.assertEqual(engineer_shard["max_pages"], 20)
        self.assertEqual(engineer_shard["page_limit"], 25)
        # Registry-derived titles (no hand-maintained function-id table).
        self.assertEqual(research_shard["title"], "United States / Researcher")
        self.assertEqual(engineer_shard["title"], "United States / Engineer")
        self.assertEqual(research_shard["shard_id"], "function_24")
        self.assertEqual(engineer_shard["shard_id"], "function_8")
        self.assertEqual(research_shard["strategy_id"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)

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


class RequestScopedRosterFunctionIdsTest(unittest.TestCase):
    """Explicit selection mapping: structured roles → canonical registry ids, never invented."""

    def test_structured_role_buckets_map_through_registry(self) -> None:
        self.assertEqual(
            request_scoped_roster_function_ids({"must_have_primary_role_buckets": ["research", "engineering"]}),
            ["24", "8"],
        )
        self.assertEqual(
            request_scoped_roster_function_ids({"must_have_primary_role_buckets": ["product_management"]}),
            ["19"],
        )

    def test_user_explicit_cohort_mirrors_into_function_ids(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": {
                    "schema_version": "cohort_selection.v1",
                    "role_bucket_ids": ["research", "product_management"],
                    "employment_statuses": ["current"],
                    "role_match": "any",
                    "source": "user_explicit",
                },
            }
        )

        self.assertEqual(request_scoped_roster_function_ids(request.to_record()), ["24", "19"])

    def test_no_selection_or_unknown_buckets_yield_no_ids(self) -> None:
        self.assertEqual(request_scoped_roster_function_ids({}), [])
        self.assertEqual(request_scoped_roster_function_ids(None), [])
        self.assertEqual(request_scoped_roster_function_ids({"must_have_primary_role_buckets": ["investor"]}), [])


class RosterLanePlanningTest(unittest.TestCase):
    """Planner + plan-review wiring of the request-scoped roster contract."""

    def test_plan_builds_request_shards_for_explicit_role_request(self) -> None:
        request = JobRequest.from_payload(_role_request_payload())

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["strategy_type"], "full_company_roster")
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)
        self.assertEqual(acquire_task.metadata["company_employee_shard_policy"], {})
        shards = acquire_task.metadata["company_employee_shards"]
        self.assertEqual(len(shards), 2)
        self.assertEqual(
            [shard["company_filters"] for shard in shards],
            [
                {"locations": ["United States"], "function_ids": ["24"]},
                {"locations": ["United States"], "function_ids": ["8"]},
            ],
        )
        # The nested intent view mirror must carry the same shard contract.
        self.assertEqual(
            acquire_task.metadata["intent_view"]["company_employee_shards"],
            acquire_task.metadata["company_employee_shards"],
        )
        self.assertEqual(
            acquire_task.metadata["intent_view"]["company_employee_shard_strategy"],
            REQUEST_FUNCTION_PARTITION_STRATEGY_ID,
        )

    def test_plan_without_function_selection_keeps_unsharded_small_company_behavior(self) -> None:
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
        self.assertEqual(acquire_task.metadata["company_employee_shard_policy"], {})
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "")

    def test_plan_without_function_selection_keeps_large_org_adaptive_policy(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "xAI full roster",
                "query": "xAI roster",
                "target_company": "xAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        self.assertEqual(
            str(acquire_task.metadata["company_employee_shard_policy"].get("strategy_id") or ""),
            "adaptive_us_technical_partition",
        )

    def test_plan_multi_region_locations_flow_into_request_shards(self) -> None:
        request = JobRequest.from_payload(
            _role_request_payload(
                target_company="xAI",
                raw_user_request="xAI researchers and engineers full roster",
                query="xAI research engineering roster",
                target_locations=["United States", "Germany"],
                execution_preferences={"acquisition_strategy_override": "full_company_roster"},
            )
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        shards = acquire_task.metadata["company_employee_shards"]
        self.assertEqual(len(shards), 2)
        for shard in shards:
            self.assertEqual(shard["company_filters"]["locations"], ["United States", "Germany"])
        self.assertEqual(
            {shard["company_filters"]["function_ids"][0] for shard in shards},
            {"24", "8"},
        )
        # Request-scoped shards pre-empt the generic adaptive probe policy.
        self.assertEqual(acquire_task.metadata["company_employee_shard_policy"], {})
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)

    def test_plan_review_sync_preserves_request_scoped_shards(self) -> None:
        request_payload = _role_request_payload()
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

        self.assertEqual(acquire_task["metadata"]["company_employee_shard_strategy"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)
        self.assertEqual(acquire_task["metadata"]["company_employee_shard_policy"], {})
        self.assertEqual(
            [shard["company_filters"] for shard in acquire_task["metadata"]["company_employee_shards"]],
            [
                {"locations": ["United States"], "function_ids": ["24"]},
                {"locations": ["United States"], "function_ids": ["8"]},
            ],
        )


class RosterLaneExecutionTest(unittest.TestCase):
    """Engine-level lane wiring, fully scripted: no provider/model/network calls."""

    def setUp(self) -> None:
        dsn = str(resolve_control_plane_postgres_dsn(_REPO_ROOT) or "").strip()
        if not dsn:
            self.skipTest("no local control-plane Postgres DSN resolved (make local-pg-up)")
        self._env_patch = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn},
            clear=False,
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(Path(self.tempdir.name) / "test.db")
        self.addCleanup(self.store.close)
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
                **dict(metadata or {}),
            },
        )

    def _snapshot(self, snapshot_dir: Path, entries: list[dict]) -> CompanyRosterSnapshot:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        summary_path = snapshot_dir / "summary.json"
        merged_path.write_text(json.dumps(entries), encoding="utf-8")
        visible_path.write_text(json.dumps(entries), encoding="utf-8")
        headless_path.write_text("[]", encoding="utf-8")
        summary_path.write_text("{}", encoding="utf-8")
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
            stop_reason="",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )

    def _run_roster(self, request_payload: dict, *, metadata: dict | None = None) -> tuple[object, list[dict]]:
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
                ]
            else:
                entries = [
                    {
                        "full_name": "Dana Plain",
                        "linkedin_url": "https://www.linkedin.com/in/dana-plain/",
                        "headline": "Lovable member",
                    }
                ]
            return self._snapshot(Path(_snapshot_dir), entries)

        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-request-scoped"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                self._task(metadata),
                {"company_identity": self.identity, "snapshot_dir": snapshot_dir},
                JobRequest.from_payload(request_payload),
            )
        return execution, fetch_calls

    def test_unsharded_roster_defaults_to_united_states_location_filter(self) -> None:
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
        self.assertEqual(len(fetch_calls), 1)
        self.assertEqual(fetch_calls[0]["company_filters"], {"locations": ["United States"]})

    def test_unsharded_roster_passes_multi_region_request_locations(self) -> None:
        execution, fetch_calls = self._run_roster(
            {
                "raw_user_request": "Lovable full roster",
                "query": "Lovable roster",
                "target_company": "Lovable",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "target_locations": ["United States", "Germany"],
            }
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(fetch_calls), 1)
        self.assertEqual(fetch_calls[0]["company_filters"], {"locations": ["United States", "Germany"]})

    def test_explicit_functions_dispatch_one_query_per_function_and_merge_dedupes(self) -> None:
        execution, fetch_calls = self._run_roster(_role_request_payload(target_company="Lovable"))

        self.assertEqual(execution.status, "completed")
        # Exactly two sharded lane payloads, one per registry function id.
        self.assertEqual(len(fetch_calls), 2)
        self.assertEqual(
            sorted(json.dumps(call["company_filters"], sort_keys=True) for call in fetch_calls),
            [
                json.dumps({"locations": ["United States"], "function_ids": ["24"]}, sort_keys=True),
                json.dumps({"locations": ["United States"], "function_ids": ["8"]}, sort_keys=True),
            ],
        )
        # Union-dedupe: Bob Both appears in both shards but is merged once.
        roster_snapshot = execution.state_updates.get("roster_snapshot")
        self.assertIsNotNone(roster_snapshot)
        merged_urls = sorted(str(entry.get("linkedin_url") or "") for entry in roster_snapshot.raw_entries)
        self.assertEqual(
            merged_urls,
            [
                "https://www.linkedin.com/in/ada-research/",
                "https://www.linkedin.com/in/bob-both/",
                "https://www.linkedin.com/in/cara-eng/",
            ],
        )
        # Per-member shard provenance + function-id attribution backfill.
        by_url = {str(entry.get("linkedin_url") or ""): entry for entry in roster_snapshot.raw_entries}
        self.assertEqual(by_url["https://www.linkedin.com/in/ada-research/"]["source_shard_id"], "function_24")
        self.assertEqual(by_url["https://www.linkedin.com/in/ada-research/"]["function_ids"], ["24"])
        self.assertEqual(by_url["https://www.linkedin.com/in/cara-eng/"]["source_shard_id"], "function_8")
        self.assertEqual(by_url["https://www.linkedin.com/in/cara-eng/"]["function_ids"], ["8"])
        self.assertEqual(by_url["https://www.linkedin.com/in/bob-both/"]["source_shard_id"], "function_24")
        # Artifact manifest records which function lane produced which members.
        summary_payload = json.loads(
            (roster_snapshot.summary_path).read_text(encoding="utf-8")
        )
        self.assertEqual(summary_payload["strategy_id"], REQUEST_FUNCTION_PARTITION_STRATEGY_ID)
        shard_rows = {str(row.get("shard_id") or ""): dict(row) for row in summary_payload["shard_summaries"]}
        self.assertEqual(sorted(shard_rows), ["function_24", "function_8"])
        self.assertEqual(
            shard_rows["function_24"]["company_filters"],
            {"locations": ["United States"], "function_ids": ["24"]},
        )
        self.assertEqual(
            shard_rows["function_8"]["company_filters"],
            {"locations": ["United States"], "function_ids": ["8"]},
        )
        self.assertEqual(shard_rows["function_24"]["unique_entry_count"], 2)
        self.assertEqual(shard_rows["function_8"]["unique_entry_count"], 1)
        self.assertEqual(shard_rows["function_8"]["duplicate_entry_count"], 1)

    def test_planner_emitted_metadata_shards_drive_the_same_lane(self) -> None:
        planner_shards = build_request_scoped_company_employee_query_plan(
            target_locations=["Germany"],
            function_ids=["24", "8"],
            max_pages=20,
            page_limit=25,
        )["shards"]

        execution, fetch_calls = self._run_roster(
            _role_request_payload(target_company="Lovable", target_locations=["Germany"]),
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


if __name__ == "__main__":
    unittest.main()
