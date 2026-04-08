import json
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import time
import unittest
import unittest.mock
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.harvest_connectors import HarvestExecutionResult
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import AppSettings, HarvestActorSettings, HarvestSettings, QwenSettings, SemanticProviderSettings
from sourcing_agent.storage import SQLiteStore


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = SQLiteStore(f"{self.tempdir.name}/test.db")
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
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_bootstrap_and_job_execution(self) -> None:
        summary = self.orchestrator.bootstrap()
        self.assertGreaterEqual(summary["candidate_count"], 120)
        self.assertGreaterEqual(summary["candidate_breakdown"].get("employee", 0), 100)

        result = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练", "推理"],
                "top_k": 3,
            }
        )
        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["name_en"], "Da Yan")

    def test_run_job_prefers_latest_company_snapshot_over_broader_sqlite_pool(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260408T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        snapshot_candidate = Candidate(
            candidate_id="snap_1",
            name_en="Snapshot Researcher",
            display_name="Snapshot Researcher",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="alignment research",
            source_dataset="acme_snapshot",
        )
        off_snapshot_candidate = Candidate(
            candidate_id="sqlite_1",
            name_en="SQLite GPU Engineer",
            display_name="SQLite GPU Engineer",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="gpu cluster platform",
            source_dataset="acme_sqlite_only",
        )
        self.store.upsert_candidate(snapshot_candidate)
        self.store.upsert_candidate(off_snapshot_candidate)
        (normalized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["gpu"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertEqual(result["summary"]["candidate_source"]["snapshot_id"], "20260408T120000")
        self.assertEqual(result["summary"]["candidate_source"]["asset_view"], "canonical_merged")
        self.assertEqual(result["summary"]["total_matches"], 0)

    def test_run_job_can_explicitly_use_strict_roster_only_view(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260408T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        strict_dir = normalized_dir / "strict_roster_only"
        strict_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        canonical_candidate = Candidate(
            candidate_id="canon_1",
            name_en="Canonical Researcher",
            display_name="Canonical Researcher",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="alignment research",
            source_dataset="acme_snapshot",
        )
        strict_candidate = Candidate(
            candidate_id="strict_1",
            name_en="Strict Recruiter",
            display_name="Strict Recruiter",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Talent Recruiter",
            focus_areas="technical recruiting",
            source_dataset="acme_strict_snapshot",
        )
        (normalized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [canonical_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (strict_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [strict_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "asset_view": "strict_roster_only",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["recruiting"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertEqual(result["summary"]["candidate_source"]["asset_view"], "strict_roster_only")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "strict_1")
        self.assertEqual(result["matches"][0]["role_bucket"], "recruiting")
        self.assertIn("recruiting", result["matches"][0]["functional_facets"])

    def test_run_job_supports_must_have_facet_hard_filter(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="recruit_1",
                    name_en="Recruiter",
                    display_name="Recruiter",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Technical Recruiter",
                    focus_areas="technical recruiting",
                ),
                Candidate(
                    candidate_id="eng_1",
                    name_en="Engineer",
                    display_name="Engineer",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Software Engineer",
                    focus_areas="training systems",
                    notes="Occasionally helps with recruiting events.",
                ),
            ],
            [],
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "FacetCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_facet": "recruiting",
                "keywords": ["recruiting"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "recruit_1")

    def test_run_job_supports_primary_role_bucket_hard_filter(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="founder_1",
                    name_en="Founder",
                    display_name="Founder",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Founder and CTO",
                    focus_areas="infrastructure systems platform",
                ),
                Candidate(
                    candidate_id="infra_1",
                    name_en="Infra Engineer",
                    display_name="Infra Engineer",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Infrastructure Engineer",
                    focus_areas="distributed systems runtime platform",
                ),
            ],
            [],
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "FacetCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_facet": "infra_systems",
                "must_have_primary_role_bucket": "infra",
                "keywords": ["infra"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "infra_1")
        self.assertEqual(result["matches"][0]["role_bucket"], "infra_systems")

    def test_plan_and_async_workflow(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 xAI 当前偏基础设施和训练系统的核心技术成员，先做全量资产获取再检索。",
                "target_company": "xAI",
            }
        )
        self.assertEqual(plan_result["plan"]["target_company"], "xAI")
        self.assertEqual(plan_result["plan"]["retrieval_plan"]["strategy"], "hybrid")
        self.assertGreaterEqual(len(plan_result["plan"]["acquisition_tasks"]), 4)
        self.assertEqual(plan_result["plan_review_gate"]["status"], "requires_review")
        self.assertEqual(plan_result["plan_review_session"]["status"], "pending")

        gated = self.orchestrator.start_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的华人技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        self.assertEqual(gated["status"], "needs_plan_review")
        review_id = gated["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "extra_source_families": ["Engineering Blog"],
                    "allow_high_cost_sources": False,
                },
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")
        workflow = self.orchestrator.start_workflow({"plan_review_id": review_id})
        self.assertIn("job_id", workflow)
        for _ in range(20):
            snapshot = self.orchestrator.get_job_results(workflow["job_id"])
            if snapshot and snapshot["job"]["status"] in {"completed", "blocked", "failed"}:
                break
            time.sleep(0.1)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertGreaterEqual(len(snapshot["results"]), 1)
        self.assertTrue(snapshot["agent_runtime_session"])
        self.assertGreaterEqual(len(snapshot["agent_trace_spans"]), 2)

    def test_manual_review_queue_and_plan_review_flow(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="lead_1",
                    name_en="Open Lead",
                    display_name="Open Lead",
                    category="lead",
                    target_company="LeadCo",
                    organization="LeadCo",
                    employment_status="unknown",
                    role="Researcher",
                    focus_areas="reinforcement learning post-train",
                    notes="Mentioned on a company engineering blog but no confirmed LinkedIn profile.",
                )
            ],
            [],
        )
        result = self.orchestrator.run_job(
            {
                "target_company": "LeadCo",
                "keywords": ["reinforcement learning"],
                "top_k": 5,
            }
        )
        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(result["summary"]["manual_review_queue_count"], 1)
        queue = self.orchestrator.list_manual_review_items(job_id=result["job_id"])
        self.assertGreaterEqual(len(queue["manual_review_items"]), 1)
        self.assertEqual(queue["manual_review_items"][0]["review_type"], "manual_identity_resolution")
        reviewed = self.orchestrator.review_manual_review_item(
            {
                "review_item_id": queue["manual_review_items"][0]["review_item_id"],
                "action": "resolved",
                "reviewer": "tester",
                "notes": "Reviewed manually.",
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")
        self.assertEqual(reviewed["manual_review_item"]["status"], "resolved")

    def test_acquire_search_seed_pool_blocks_when_background_queue_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-queued"
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        queued_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "queued_lead",
                    "full_name": "Queued Lead",
                    "source_type": "harvest_profile_search",
                }
            ],
            query_summaries=[
                {
                    "query": "Thinking Machines Lab former employee",
                    "status": "queued",
                }
            ],
            accounts_used=[],
            errors=[],
            stop_reason="queued_background_search",
            summary_path=summary_path,
        )

        self.acquisition_engine.search_seed_acquirer.discover = lambda *args, **kwargs: queued_snapshot
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire search seed pool",
            description="Acquire former employee leads",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "former_employee_search",
                "search_seed_queries": ["Thinking Machines Lab former employee"],
                "cost_policy": {},
                "employment_statuses": ["former"],
            },
        )
        execution = self.acquisition_engine._acquire_search_seed_pool(
            task,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job_queued",
                "plan_payload": {},
                "runtime_mode": "workflow",
            },
            JobRequest(
                raw_user_request="Find former Thinking Machines Lab members",
                target_company="Thinking Machines Lab",
                categories=["former_employee"],
                employment_statuses=["former"],
            ),
        )

        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_query_count"], 1)
        self.assertEqual(execution.state_updates["search_seed_snapshot"].stop_reason, "queued_background_search")

    def test_acquire_full_roster_blocks_when_background_harvest_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-harvest-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster", "cost_policy": {"allow_company_employee_api": True}},
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "execute_with_checkpoint",
            return_value=HarvestExecutionResult(
                logical_name="harvest_company_employees",
                checkpoint={"run_id": "run-harvest", "dataset_id": "dataset-harvest", "status": "submitted"},
                pending=True,
                message="Submitted async harvest run.",
            ),
        ), unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=AssertionError("fetch_company_roster should not run while harvest worker is queued"),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(job_id="job_harvest_queued", lane_id="acquisition_specialist")
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_company_employees")

    def test_enrich_profiles_blocks_when_background_exploration_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-exploration-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-exploration-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Queued Exploration Lead",
                    "source_type": "harvest_profile_search",
                    "linkedin_url": "https://www.linkedin.com/in/queued-exploration/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        pending_candidate = Candidate(
            candidate_id="lead_1",
            name_en="Queued Exploration Lead",
            display_name="Queued Exploration Lead",
            category="lead",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            linkedin_url="https://www.linkedin.com/in/queued-exploration/",
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        self.acquisition_engine.multi_source_enricher.enrich = lambda *args, **kwargs: MultiSourceEnrichmentResult(
            candidates=[pending_candidate],
            evidence=[],
            artifact_paths={"exploration_summary": str(snapshot_dir / "exploration" / "summary.json")},
            queued_exploration_count=1,
            stop_reason="queued_background_exploration",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={"cost_policy": {}},
        )
        try:
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_enrichment_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_exploration_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_exploration")

    def test_enrich_profiles_blocks_when_background_harvest_profile_batch_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-harvest-profile-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-harvest-profile-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Queued Harvest Lead",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/queued-harvest/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        self.acquisition_engine.multi_source_enricher.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings = replace(
            self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={"profile_detail_limit": 5, "slug_resolution_limit": 5, "cost_policy": {}},
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
            "execute_batch_with_checkpoint",
            return_value=HarvestExecutionResult(
                logical_name="harvest_profile_scraper_batch",
                checkpoint={"run_id": "run-profile", "dataset_id": "dataset-profile", "status": "submitted"},
                pending=True,
                message="Submitted async harvest batch.",
            ),
        ), unittest.mock.patch.object(
            type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
            "fetch_profiles_by_urls",
            side_effect=AssertionError("fetch_profiles_by_urls should not run while harvest batch worker is queued"),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_profile_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(job_id="job_harvest_profile_queued", lane_id="enrichment_specialist")
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_profile_batch")

    def test_resume_blocked_workflow_waits_for_exploration_workers_then_resumes(self) -> None:
        request_payload = {
            "raw_user_request": "Find Thinking Machines Lab people",
            "target_company": "Thinking Machines Lab",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_resume_exploration_blocked"
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-resume-exploration"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued exploration workers", "blocked_task": "enrich_profiles_multisource"},
        )

        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key="candidate::queued_exploration",
            stage="enriching",
            span_name="explore_candidate:Queued Exploration Lead",
            budget_payload={"max_queries": 7},
            input_payload={
                "candidate_id": "candidate::queued_exploration",
                "display_name": "Queued Exploration Lead",
                "candidate": {"candidate_id": "candidate::queued_exploration", "display_name": "Queued Exploration Lead"},
            },
            metadata={
                "target_company": "Thinking Machines Lab",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="public_media_specialist",
        )

        waiting = self.orchestrator._resume_blocked_workflow_if_ready(job_id)
        self.assertEqual(waiting["status"], "waiting")
        self.assertEqual(waiting["pending_worker_count"], 1)

        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "candidate": {
                    "candidate_id": "candidate::queued_exploration",
                    "name_en": "Queued Exploration Lead",
                    "display_name": "Queued Exploration Lead",
                },
                "evidence": [],
                "summary": {"candidate_id": "candidate::queued_exploration", "status": "completed"},
                "errors": [],
            },
        )

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        original_execute_task = self.acquisition_engine.execute_task

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "company_identity": identity,
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"{task.task_type} completed.",
                payload={},
                state_updates={},
            )

        self.acquisition_engine.execute_task = fake_execute_task
        try:
            resume = self.orchestrator._resume_blocked_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["job_status"], "completed")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")

    def test_resume_blocked_workflow_after_workers_complete(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "organization_keywords": ["xAI"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_resume_blocked"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-resume"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )

        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::01",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {
                    "bundle_id": "relationship_web",
                    "query": "xAI former employee",
                    "source_family": "public_web_search",
                },
                "query": "xAI former employee",
                "index": 1,
            },
            metadata={
                "index": 1,
                "identity": CompanyIdentity(
                    requested_name="xAI",
                    canonical_name="xAI",
                    company_key="xai",
                    linkedin_slug="xai",
                    linkedin_company_url="https://www.linkedin.com/company/xai/",
                ).to_record(),
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "former",
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "result_limit": 10,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "raw_path": str(discovery_dir / "web_query_01.json")},
            output_payload={"summary": {"query": "xAI former employee", "status": "completed"}, "entries": [], "errors": []},
        )

        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand_resume_1",
                    name_en="Former XAI Engineer",
                    display_name="Former XAI Engineer",
                    category="former_employee",
                    target_company="xAI",
                    organization="xAI",
                    employment_status="former",
                    role="Engineer",
                    focus_areas="systems",
                )
            ],
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id("cand_resume_1", "seed", "xAI former employee", "https://example.com/xai"),
                    candidate_id="cand_resume_1",
                    source_type="web_search",
                    title="xAI former employee",
                    url="https://example.com/xai",
                    summary="Synthetic evidence for resume test.",
                    source_dataset="test",
                    source_path=str(snapshot_dir / "synthetic_evidence.json"),
                )
            ],
        )

        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        original_execute_task = self.acquisition_engine.execute_task

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "company_identity": identity,
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"{task.task_type} completed.",
                payload={},
                state_updates={},
            )

        self.acquisition_engine.execute_task = fake_execute_task
        try:
            resume = self.orchestrator._resume_blocked_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["job_status"], "completed")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertGreaterEqual(len(snapshot["results"]), 1)

    def test_manual_review_items_are_snapshot_scoped_and_cleanup_old_snapshots(self) -> None:
        old_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260406T120000" / "candidate_documents.json"
        )
        new_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260407T120000" / "candidate_documents.json"
        )
        old_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "Old snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": old_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }
        old_other_item = {
            "candidate_id": "cand_old_only",
            "target_company": "Acme",
            "review_type": "needs_human_validation",
            "priority": "medium",
            "status": "open",
            "summary": "Old-only snapshot item.",
            "candidate": {
                "candidate_id": "cand_old_only",
                "name_en": "Bob Old",
                "display_name": "Bob Old",
                "category": "employee",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [],
            "metadata": {},
        }
        new_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "New snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": new_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": new_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }

        self.store.replace_manual_review_items("job_old", [old_item, old_other_item])
        self.store.replace_manual_review_items("job_new", [new_item])

        all_items = self.store.list_manual_review_items(target_company="Acme", status="", limit=10)
        status_by_candidate = {(item["candidate_id"], item["job_id"]): item["status"] for item in all_items}
        snapshot_by_candidate = {
            (item["candidate_id"], item["job_id"]): item["metadata"].get("snapshot_id")
            for item in all_items
        }
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_old")], "20260406T120000")
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_new")], "20260407T120000")
        self.assertEqual(status_by_candidate[("cand_manual", "job_old")], "superseded")
        self.assertEqual(status_by_candidate[("cand_manual", "job_new")], "open")

        cleanup = self.store.cleanup_manual_review_items(target_company="Acme", snapshot_id="20260407T120000")
        self.assertGreaterEqual(cleanup["out_of_scope_count"], 1)
        open_items = self.store.list_manual_review_items(target_company="Acme", status="open", limit=10)
        self.assertEqual(len(open_items), 1)
        self.assertEqual(open_items[0]["candidate_id"], "cand_manual")
        self.assertEqual(open_items[0]["metadata"].get("snapshot_id"), "20260407T120000")

    def test_investor_firm_workflow_uses_tiered_firm_roster(self) -> None:
        self.orchestrator.bootstrap()
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出Anthropic投资机构里与Anthropic投资直接相关的全量成员，再后续筛选决策相关角色。",
                "target_company": "Anthropic",
                "categories": ["investor"],
                "keywords": ["Anthropic投资", "决策"],
            }
        )
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "investor_firm_roster")
        review_id = plan_result["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {"allow_high_cost_sources": False},
            }
        )
        workflow = self.orchestrator.run_workflow_blocking({"plan_review_id": review_id})
        self.assertEqual(workflow["job"]["status"], "completed")
        self.assertGreaterEqual(len(workflow["results"]), 1)
        self.assertGreaterEqual(workflow["job"]["summary"].get("manual_review_queue_count", 0), 0)

    def test_feedback_triggers_criteria_recompile(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 xAI 的 RL researcher",
                "target_company": "xAI",
                "keywords": ["RL"],
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
            }
        )
        self.assertEqual(feedback_result["status"], "recorded")
        self.assertEqual(feedback_result["recompile"]["status"], "recompiled")
        self.assertEqual(feedback_result["recompile"]["base_version_id"], plan_result["criteria_version_id"])
        self.assertGreater(feedback_result["recompile"]["criteria_version_id"], plan_result["criteria_version_id"])
        versions = self.store.list_criteria_versions(target_company="xAI", limit=2)
        self.assertEqual(versions[0]["evolution_stage"], "feedback_recompiled")
        self.assertEqual(versions[0]["parent_version_id"], plan_result["criteria_version_id"])
        runs = self.store.list_criteria_compiler_runs(target_company="xAI", limit=2)
        self.assertEqual(runs[0]["compiler_kind"], "feedback_recompile")

    def test_feedback_rerun_produces_result_diff(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand_rl_1",
                    name_en="Jane RL",
                    display_name="Jane RL",
                    category="employee",
                    target_company="TestCo",
                    organization="TestCo",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="reinforcement learning systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "TestCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["RL"],
                "top_k": 5,
            }
        )
        self.assertEqual(initial["summary"]["total_matches"], 0)

        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "target_company": "TestCo",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
                "rerun_retrieval": True,
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "completed")
        self.assertEqual(feedback_result["rerun"]["baseline_job_id"], initial["job_id"])
        self.assertEqual(feedback_result["rerun"]["policy"]["mode"], "cheap")
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["added_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["rerun_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["added_pattern_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["added"][0]["display_name"], "Jane RL")
        self.assertEqual(
            feedback_result["rerun"]["diff"]["rule_changes"]["pattern_changes"]["added"][0]["value"],
            "reinforcement learning",
        )
        self.assertIn("Criteria changes materially affected retrieval output", feedback_result["rerun"]["diff"]["impact_explanations"][0])
        self.assertEqual(feedback_result["rerun"]["diff"]["candidate_impacts"]["summary"]["candidate_impact_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["candidate_impacts"]["summary"]["attributed_candidate_count"], 1)
        self.assertEqual(
            feedback_result["rerun"]["diff"]["candidate_impacts"]["items"][0]["rule_triggers"][0]["pattern_type"],
            "alias",
        )
        self.assertIn(
            "Entered results because",
            feedback_result["rerun"]["diff"]["candidate_impacts"]["items"][0]["attribution_explanation"],
        )
        diffs = self.store.list_criteria_result_diffs(target_company="TestCo", limit=5)
        self.assertEqual(len(diffs), 1)
        self.assertEqual(diffs[0]["baseline_job_id"], initial["job_id"])
        self.assertEqual(diffs[0]["summary"]["added_pattern_count"], 1)
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["summary"]["summary_provider"], "deterministic")

    def test_low_signal_feedback_can_be_gated_off(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand1",
                    name_en="Alice",
                    display_name="Alice",
                    category="employee",
                    target_company="GateCo",
                    organization="GateCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="distributed systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "GateCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["systems"],
                "top_k": 5,
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "target_company": "GateCo",
                "feedback_type": "note_only",
                "subject": "observation",
                "value": "check later",
                "rerun_retrieval": True,
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "gated_off")
        self.assertEqual(feedback_result["rerun"]["policy"]["estimated_cost_tier"], "none")

    def test_recompile_rerun_selects_baseline_by_request_family(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="family_a",
                    name_en="Alice Systems",
                    display_name="Alice Systems",
                    category="employee",
                    target_company="FamilyCo",
                    organization="FamilyCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="distributed systems platform",
                ),
                Candidate(
                    candidate_id="family_b",
                    name_en="Bob Inference",
                    display_name="Bob Inference",
                    category="employee",
                    target_company="FamilyCo",
                    organization="FamilyCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="inference runtime",
                ),
            ],
            [],
        )
        systems_job = self.orchestrator.run_job(
            {
                "target_company": "FamilyCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["systems"],
                "top_k": 5,
            }
        )
        inference_job = self.orchestrator.run_job(
            {
                "target_company": "FamilyCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["inference"],
                "top_k": 5,
            }
        )
        self.assertNotEqual(systems_job["job_id"], inference_job["job_id"])
        self.assertEqual(inference_job["matches"][0]["display_name"], "Bob Inference")

        rerun = self.orchestrator.recompile_criteria(
            {
                "request": {
                    "target_company": "FamilyCo",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "keywords": ["systems"],
                    "top_k": 5,
                },
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(rerun["rerun"]["status"], "completed")
        self.assertEqual(rerun["rerun"]["baseline_job_id"], systems_job["job_id"])
        self.assertEqual(rerun["rerun"]["baseline_selection"]["matched_request_family_signature"], rerun["rerun"]["baseline_selection"]["request_family_signature"])
        self.assertIn("exact request signature match", rerun["rerun"]["baseline_selection"]["reason"].lower())

    def test_confidence_evolution_changes_label_on_rerun(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="conf_1",
                    name_en="Jane Research",
                    display_name="Jane Research",
                    category="employee",
                    target_company="ConfCo",
                    organization="ConfCo",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="systems platform",
                    linkedin_url="https://linkedin.com/in/jane-research",
                    metadata={"exploration_links": ["https://example.com/jane"]},
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "ConfCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["research"],
                "top_k": 5,
            }
        )
        self.assertEqual(initial["matches"][0]["confidence_label"], "medium")

        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "candidate_id": "conf_1",
                "target_company": "ConfCo",
                "feedback_type": "must_have_signal",
                "subject": "research",
                "value": "research engineer",
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "completed")
        self.assertGreaterEqual(len(feedback_result["suggestions"]), 1)
        self.assertEqual(feedback_result["suggestions"][0]["status"], "suggested")
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["matches"][0]["confidence_label"], "high")
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["moved_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["moved"][0]["old_confidence_label"], "medium")
        self.assertEqual(feedback_result["rerun"]["diff"]["moved"][0]["new_confidence_label"], "high")
        self.assertGreaterEqual(feedback_result["rerun"]["rerun_result"]["confidence_policy"]["high_threshold"], 0.63)
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["confidence_policy"]["scope_kind"], "request_family")
        feedback_rows = self.store.list_criteria_feedback(target_company="ConfCo", limit=5)
        self.assertTrue(feedback_rows[0]["metadata"].get("request_family_signature"))
        runs = self.store.list_confidence_policy_runs(target_company="ConfCo", limit=5)
        self.assertGreaterEqual(len(runs), 2)
        self.assertEqual(runs[0]["job_id"], feedback_result["rerun"]["rerun_job_id"])
        self.assertEqual(runs[0]["scope_kind"], "request_family")
        self.assertTrue(runs[0]["request_family_signature"])
        criteria_snapshot = self.orchestrator.list_criteria_patterns("ConfCo")
        self.assertGreaterEqual(len(criteria_snapshot["suggestions"]), 1)

    def test_suggestion_review_applies_pattern_and_recompiles(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="review_1",
                    name_en="Maya Systems",
                    display_name="Maya Systems",
                    category="employee",
                    target_company="ReviewCo",
                    organization="ReviewCo",
                    employment_status="current",
                    role="Engineer",
                    team="Training Infrastructure",
                    focus_areas="distributed training systems",
                    linkedin_url="https://linkedin.com/in/maya-systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "ReviewCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["infra"],
                "top_k": 5,
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "candidate_id": "review_1",
                "target_company": "ReviewCo",
                "feedback_type": "false_negative_pattern",
                "subject": "infra",
                "value": "training infrastructure",
            }
        )
        must_signal_suggestion = next(
            item for item in feedback_result["suggestions"] if item["pattern_type"] == "must_signal"
        )
        review_result = self.orchestrator.review_pattern_suggestion(
            {
                "suggestion_id": must_signal_suggestion["suggestion_id"],
                "action": "apply",
                "reviewer": "human",
                "notes": "This suggestion should become an active filter.",
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(review_result["status"], "reviewed")
        self.assertEqual(review_result["suggestion"]["status"], "applied")
        self.assertIsNotNone(review_result["applied_pattern"])
        self.assertEqual(review_result["applied_pattern"]["pattern_type"], "must_signal")
        self.assertEqual(review_result["recompile"]["status"], "recompiled")
        self.assertEqual(review_result["rerun"]["status"], "completed")
        active_patterns = self.store.list_criteria_patterns(target_company="ReviewCo", status="active", limit=50)
        self.assertTrue(any(item["pattern_id"] == review_result["applied_pattern"]["pattern_id"] for item in active_patterns))

    def test_manual_confidence_policy_freeze_blocks_auto_band_shift(self) -> None:
        request_payload = {
            "target_company": "FreezeCo",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
            "top_k": 5,
        }
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="freeze_1",
                    name_en="Freeze Candidate",
                    display_name="Freeze Candidate",
                    category="employee",
                    target_company="FreezeCo",
                    organization="FreezeCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="systems platform",
                    linkedin_url="https://linkedin.com/in/freeze-candidate",
                    metadata={"publication_id": "paper-1"},
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(request_payload)
        self.assertEqual(initial["matches"][0]["confidence_label"], "medium")
        freeze_result = self.orchestrator.configure_confidence_policy(
            {
                "action": "freeze_current",
                "target_company": "FreezeCo",
                "request_payload": request_payload,
                "scope_kind": "request_family",
                "reviewer": "human",
                "notes": "Freeze this family before more feedback arrives.",
            }
        )
        self.assertEqual(freeze_result["status"], "configured")
        self.assertEqual(freeze_result["control"]["control_mode"], "freeze")

        for idx in range(5):
            self.orchestrator.record_criteria_feedback(
                {
                    "target_company": "FreezeCo",
                    "request_payload": request_payload,
                    "feedback_type": "false_negative_pattern",
                    "subject": f"unrelated-{idx}",
                    "value": f"multimodal vision stack {idx}",
                }
            )

        frozen_run = self.orchestrator.run_job(request_payload)
        self.assertEqual(frozen_run["matches"][0]["confidence_label"], "medium")
        self.assertEqual(frozen_run["confidence_policy_control"]["control_mode"], "freeze")

        cleared = self.orchestrator.configure_confidence_policy(
            {
                "action": "clear",
                "target_company": "FreezeCo",
                "request_payload": request_payload,
                "scope_kind": "request_family",
            }
        )
        self.assertEqual(cleared["status"], "cleared")
        unfrozen_run = self.orchestrator.run_job(request_payload)
        self.assertEqual(unfrozen_run["matches"][0]["confidence_label"], "high")
        self.assertLess(unfrozen_run["confidence_policy"]["high_threshold"], 0.75)

    def test_http_api_smoke(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            bootstrap_req = urllib_request.Request(
                f"http://{host}:{port}/api/bootstrap",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(bootstrap_req) as response:
                bootstrap_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(bootstrap_resp["status"], "bootstrapped")

            plan_payload = json.dumps(
                {
                    "raw_user_request": "帮我为 xAI 设计一个 sourcing 工作流，先拿全量数据资产，再决定检索方式。",
                    "target_company": "xAI",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            plan_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan",
                data=plan_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(plan_req) as response:
                plan_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(plan_resp["plan"]["target_company"], "xAI")
            plan_reviews_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan/reviews",
                method="GET",
            )
            with opener.open(plan_reviews_req) as response:
                plan_reviews_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(plan_reviews_resp["plan_reviews"]), 1)

            job_payload = json.dumps(
                {
                    "target_company": "Anthropic",
                    "categories": ["investor"],
                    "keywords": ["领投", "决策", "Anthropic投资"],
                    "top_k": 3,
                },
                ensure_ascii=False,
            ).encode("utf-8")
            job_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs",
                data=job_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(job_req) as response:
                job_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(job_resp["status"], "completed")
            self.assertGreaterEqual(len(job_resp["matches"]), 1)
            self.assertIn("confidence_label", job_resp["matches"][0])

            result_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/results",
                method="GET",
            )
            with opener.open(result_req) as response:
                result_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(result_resp["job"]["job_id"], job_resp["job_id"])
            self.assertTrue(result_resp["agent_runtime_session"])
            self.assertIn("agent_workers", result_resp)

            worker_handle = self.orchestrator.agent_runtime.begin_worker(
                job_id=job_resp["job_id"],
                request=JobRequest.from_payload(self.orchestrator.get_job(job_resp["job_id"])["request"]),
                plan_payload=job_resp["plan"],
                runtime_mode="workflow",
                lane_id="exploration_specialist",
                worker_key="candidate::demo",
                stage="enriching",
                span_name="explore_candidate:demo",
                budget_payload={"max_queries": 2},
                input_payload={"candidate_id": "cand_demo"},
                handoff_from_lane="public_media_specialist",
            )
            self.orchestrator.agent_runtime.checkpoint_worker(
                worker_handle,
                checkpoint_payload={"completed_queries": ["1"]},
                output_payload={"explored_query_count": 1},
            )

            worker_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/workers",
                method="GET",
            )
            with opener.open(worker_req) as response:
                worker_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(worker_resp["agent_workers"]), 1)

            scheduler_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/scheduler",
                method="GET",
            )
            with opener.open(scheduler_req) as response:
                scheduler_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("scheduler", scheduler_resp)
            self.assertIn("resumable_workers", scheduler_resp["scheduler"])

            recoverable_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/recoverable",
                method="GET",
            )
            with opener.open(recoverable_req) as response:
                recoverable_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("recoverable_workers", recoverable_resp)
            self.assertIn("count", recoverable_resp)

            interrupt_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/interrupt",
                data=json.dumps({"worker_id": worker_handle.worker_id}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(interrupt_req) as response:
                interrupt_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(interrupt_resp["status"], "interrupt_requested")

            daemon_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/daemon/run-once",
                data=json.dumps({"owner_id": "test-daemon", "total_limit": 1}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(daemon_req) as response:
                daemon_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(daemon_resp["status"], "completed")
            self.assertIn("daemon", daemon_resp)

            daemon_status_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/daemon/status",
                method="GET",
            )
            with opener.open(daemon_status_req) as response:
                daemon_status_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", daemon_status_resp)
            self.assertIn("service_name", daemon_status_resp)

            manual_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review",
                method="GET",
            )
            with opener.open(manual_review_req) as response:
                manual_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("manual_review_items", manual_review_resp)

            provider_health_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/health",
                method="GET",
            )
            with opener.open(provider_health_req) as response:
                provider_health = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", provider_health)
            self.assertIn("semantic", provider_health["providers"])

            feedback_payload = json.dumps(
                {
                    "target_company": "Anthropic",
                    "feedback_type": "accepted_alias",
                    "subject": "华人",
                    "value": "Chinese",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            feedback_req = urllib_request.Request(
                f"http://{host}:{port}/api/criteria/feedback",
                data=feedback_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(feedback_req) as response:
                feedback_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(feedback_resp["status"], "recorded")
            self.assertEqual(feedback_resp["recompile"]["status"], "recompiled")

            pattern_req = urllib_request.Request(
                f"http://{host}:{port}/api/criteria/patterns",
                method="GET",
            )
            with opener.open(pattern_req) as response:
                pattern_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(pattern_resp["patterns"]), 1)
            self.assertGreaterEqual(len(pattern_resp["versions"]), 1)
            self.assertGreaterEqual(len(pattern_resp["compiler_runs"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_job_trace_endpoint_returns_spans(self) -> None:
        self.orchestrator.bootstrap()
        result = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施"],
                "top_k": 2,
            }
        )
        trace = self.orchestrator.get_job_trace(result["job_id"])
        self.assertIsNotNone(trace)
        self.assertTrue(trace["agent_runtime_session"])
        lane_ids = [item["lane_id"] for item in trace["agent_trace_spans"]]
        self.assertIn("retrieval_specialist", lane_ids)

    def test_normalize_snapshot_inherits_historical_explicit_profile_captures(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T120000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T120000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            focus_areas="Research Engineer | distributed systems",
            education="MIT",
            work_history="Acme | Example Labs",
            notes="Historical enriched snapshot.",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            source_dataset="acme_linkedin_company_people",
            source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={
                "public_identifier": "alice-example",
                "profile_url": "https://www.linkedin.com/in/alice-example/",
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
                "membership_review_rationale": "Profile content looks implausible for the target company.",
                "membership_review_triggers": ["suspicious_profile_content"],
                "membership_review_trigger_keywords": ["healer"],
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical profile detail capture.",
                source_dataset="linkedin_profile_detail",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_membership_review",
                    "Membership review",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_membership_review",
                title="Membership review",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical suspicious membership review.",
                source_dataset="linkedin_profile_membership_review",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
                metadata={"decision": "suspicious_member"},
            ),
        ]
        historical_lead = Candidate(
            candidate_id="cand_acme_lead",
            name_en="Historical Lead",
            display_name="Historical Lead",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Research lead from publication",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        historical_lead_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                historical_lead.candidate_id,
                "publication_match",
                "Great Paper",
                "https://example.com/paper",
            ),
            candidate_id=historical_lead.candidate_id,
            source_type="publication_match",
            title="Great Paper",
            url="https://example.com/paper",
            summary="Historical lead that should not be dropped by normalize.",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record(), historical_lead.to_record()],
                    "evidence": [item.to_record() for item in [*old_evidence, historical_lead_evidence]],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            focus_areas="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline. Location: San Francisco.",
            linkedin_url="https://www.linkedin.com/in/ACwAAExampleBaseline",
            source_dataset="acme_linkedin_company_people",
            source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            metadata={
                "profile_url": "https://www.linkedin.com/in/ACwAAExampleBaseline",
                "snapshot_id": current_snapshot_dir.name,
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_linkedin_company_people",
                    "Roster row",
                    "https://www.linkedin.com/company/acme/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="linkedin_company_people",
                title="Roster row",
                url="https://www.linkedin.com/company/acme/",
                summary="Current roster baseline.",
                source_dataset="acme_linkedin_company_people",
                source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            )
        ]
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": current_snapshot_dir.name},
                    "candidates": [baseline_candidate.to_record()],
                    "evidence": [item.to_record() for item in baseline_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_acme_1")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.education, "MIT")
        self.assertEqual(stored_candidate.work_history, "Acme | Example Labs")
        self.assertTrue(stored_candidate.metadata.get("membership_review_required"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "suspicious_member")
        self.assertIsNotNone(self.store.get_candidate("cand_acme_lead"))

        stored_evidence = self.store.list_evidence("cand_acme_1")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("linkedin_company_people", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        self.assertIn("linkedin_profile_membership_review", stored_source_types)
        lead_evidence = self.store.list_evidence("cand_acme_lead")
        self.assertEqual(lead_evidence[0]["source_type"], "publication_match")

        rewritten_payload = json.loads((current_snapshot_dir / "candidate_documents.json").read_text())
        rewritten_candidate = rewritten_payload["candidates"][0]
        rewritten_source_types = {item["source_type"] for item in rewritten_payload["evidence"]}
        self.assertEqual(rewritten_candidate["education"], "MIT")
        self.assertEqual(rewritten_candidate["work_history"], "Acme | Example Labs")
        self.assertTrue(rewritten_candidate["metadata"]["membership_review_required"])
        self.assertIn("linkedin_profile_detail", rewritten_source_types)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["matched_candidate_count"], 1)
        self.assertGreaterEqual(rewritten_payload["historical_profile_inheritance"]["inherited_evidence_count"], 2)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["carried_forward_candidate_count"], 1)
        rewritten_candidate_ids = {item["candidate_id"] for item in rewritten_payload["candidates"]}
        self.assertIn("cand_acme_lead", rewritten_candidate_ids)

    def test_normalize_snapshot_canonicalizes_same_name_current_candidates(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "20260407T130000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        lead_candidate = Candidate(
            candidate_id="lead_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(snapshot_dir / "publications" / "alice.html"),
        )
        employee_candidate = Candidate(
            candidate_id="employee_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            work_history="Acme",
            education="MIT",
            source_dataset="acme_linkedin_company_people",
            source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={"public_identifier": "alice-example"},
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id("lead_alice", "publication_match", "Paper", "https://example.com/paper"),
                candidate_id="lead_alice",
                source_type="publication_match",
                title="Paper",
                url="https://example.com/paper",
                summary="Lead from publication.",
                source_dataset="publication_match",
                source_path=str(snapshot_dir / "publications" / "alice.html"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    "employee_alice",
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id="employee_alice",
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Profile detail for Alice.",
                source_dataset="linkedin_profile_detail",
                source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        result = self.acquisition_engine._normalize_snapshot(
            AcquisitionTask(
                task_id="normalize_canonical",
                task_type="normalize_asset_snapshot",
                title="Normalize canonical",
                description="Persist snapshot with canonical dedupe.",
            ),
            {
                "company_identity": identity,
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidates": [lead_candidate, employee_candidate],
                "evidence": evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].candidate_id, "employee_alice")
        self.assertEqual(stored[0].category, "employee")
        self.assertEqual(stored[0].linkedin_url, "https://www.linkedin.com/in/alice-example/")
        stored_evidence = self.store.list_evidence("employee_alice")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("publication_match", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        payload = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertEqual(payload["canonicalization"]["canonical_candidate_count"], 1)
        self.assertEqual(payload["canonicalization"]["name_merge_count"], 1)

    def test_normalize_snapshot_inherits_manual_review_confirmed_membership(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T130000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T140000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Publication author lead",
            media_url="https://jeremy.example.com/",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "jeremy"),
                "manual_review_links": [{"label": "Homepage", "url": "https://jeremy.example.com/"}],
                "membership_review_required": False,
                "membership_review_decision": "manual_confirmed_member",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "Homepage",
                    "https://jeremy.example.com/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="Homepage",
                url="https://jeremy.example.com/",
                summary="Manual review confirmed current membership.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "publication_match",
                    "Blog post",
                    "https://example.com/jeremy",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="publication_match",
                title="Blog post",
                url="https://example.com/jeremy",
                summary="Current snapshot still only has publication lead evidence.",
                source_dataset="publication_match",
                source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
            )
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_manual_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "employee")
        self.assertEqual(stored_candidate.employment_status, "current")
        self.assertEqual(stored_candidate.media_url, "https://jeremy.example.com/")
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_confirmed_member")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("manual_review_artifact_root"))

    def test_normalize_snapshot_inherits_manual_non_member_resolution(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T140000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T150000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="non_member",
            target_company="Acme",
            organization="Other Org",
            employment_status="",
            role="Data Analyst",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "rabia"),
                "manual_review_links": [{"label": "LinkedIn", "url": "https://www.linkedin.com/in/rabia-example/"}],
                "target_company_mismatch": True,
                "membership_review_required": False,
                "membership_review_decision": "manual_non_member",
                "membership_review_rationale": "Manual review rejected this profile as unrelated to the target company.",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "LinkedIn",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="LinkedIn",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Manual review rejected this candidate as a non-member.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="OpenAI at Acme",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            metadata={
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_search_seed_candidates",
                    "Search seed",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="harvest_profile_search",
                title="Search seed",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Current snapshot still has the suspicious search-seed candidate.",
                source_dataset="acme_search_seed_candidates",
                source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            )
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_manual_non_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "non_member")
        self.assertEqual(stored_candidate.organization, "Other Org")
        self.assertEqual(stored_candidate.employment_status, "")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("target_company_mismatch"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_non_member")
