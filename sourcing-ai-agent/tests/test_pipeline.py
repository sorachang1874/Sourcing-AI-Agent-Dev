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
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.harvest_connectors import HarvestExecutionResult
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.planning import build_sourcing_plan
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
        self.assertIn("intent_rewrite", result)
        self.assertFalse(result["intent_rewrite"]["request"]["matched"])

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
        self.assertTrue(plan_result["plan"]["intent_brief"]["identified_request"])
        self.assertTrue(plan_result["plan"]["intent_brief"]["default_execution_strategy"])
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

    def test_queue_workflow_and_run_saved_job(self) -> None:
        gated = self.orchestrator.start_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        self.assertEqual(gated["status"], "needs_plan_review")
        review_id = gated["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "allow_high_cost_sources": False,
                },
            }
        )

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job_id = queued["job_id"]

        before = self.orchestrator.get_job(job_id)
        self.assertIsNotNone(before)
        assert before is not None
        self.assertEqual(before["status"], "queued")

        run_result = self.orchestrator.run_queued_workflow(job_id)
        self.assertEqual(run_result["status"], "completed")

        after = self.orchestrator.get_job_results(job_id)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(after["job"]["status"], "completed")
        self.assertGreaterEqual(len(after["results"]), 1)

    def test_model_assisted_request_normalization_feeds_structured_plan(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Google DeepMind",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "organization_keywords": ["Veo"],
                    "keywords": ["Post-train", "Math"],
                    "retrieval_strategy": "hybrid",
                    "query": "Google DeepMind Veo post-train math members",
                    "execution_preferences": {
                        "allow_high_cost_sources": False,
                        "precision_recall_bias": "precision_first",
                    },
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, RequestNormalizingModelClient()),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要Veo团队的Post-train和Math方向成员",
                "planning_mode": "model_assisted",
            }
        )

        self.assertEqual(plan_result["request"]["target_company"], "Google")
        self.assertEqual(plan_result["request"]["categories"], ["employee"])
        self.assertEqual(plan_result["request"]["employment_statuses"], ["current"])
        self.assertEqual(plan_result["request"]["organization_keywords"], ["Google DeepMind", "Veo"])
        self.assertEqual(plan_result["request"]["keywords"], ["Post-train", "Math"])
        self.assertEqual(plan_result["request"]["retrieval_strategy"], "hybrid")
        self.assertEqual(
            plan_result["request"]["execution_preferences"],
            {
                "allow_high_cost_sources": False,
                "precision_recall_bias": "precision_first",
            },
        )
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "scoped_search_roster")
        self.assertIn("Google", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertIn("Google DeepMind", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertIn("Veo", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertNotIn("Post-train", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["filter_hints"]["keywords"][:2],
            ["Post-train", "Math"],
        )
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["cost_policy"]["precision_recall_bias"],
            "precision_first",
        )
        self.assertFalse(plan_result["plan"]["acquisition_strategy"]["cost_policy"]["high_cost_requires_approval"])
        self.assertNotIn("high_cost_sources_need_approval", plan_result["plan_review_gate"]["reasons"])
        self.assertEqual(plan_result["plan_review_gate"]["status"], "ready")
        self.assertFalse(plan_result["plan_review_gate"]["required_before_execution"])
        self.assertIn("targeted_people_search", [item["bundle_id"] for item in plan_result["plan"]["search_strategy"]["query_bundles"]])
        self.assertTrue(any("团队或子组织范围：" in item and "Veo" in item for item in plan_result["plan"]["intent_brief"]["identified_request"]))

    def test_model_assisted_request_normalization_preserves_natural_language_shorthand_rewrite(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Anthropic",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "retrieval_strategy": "hybrid",
                    "query": "Anthropic members",
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, RequestNormalizingModelClient()),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 的华人成员",
                "target_company": "Anthropic",
                "planning_mode": "model_assisted",
            }
        )

        self.assertEqual(plan_result["request"]["target_company"], "Anthropic")
        self.assertEqual(plan_result["request"]["categories"], ["employee"])
        self.assertEqual(plan_result["request"]["employment_statuses"], ["current"])
        self.assertEqual(
            plan_result["request"]["keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )
        self.assertTrue(plan_result["intent_rewrite"]["request"]["matched"])
        self.assertEqual(
            plan_result["intent_rewrite"]["request"]["rewrite"]["rewrite_id"],
            "greater_china_outreach",
        )
        self.assertTrue(
            any("自然语言简称改写" in item for item in plan_result["plan"]["intent_brief"]["identified_request"]),
        )
        self.assertIn(
            "这类简称默认按公开的地区 / 语言 / 学习工作经历口径理解，而不是身份标签判断。",
            plan_result["plan"]["intent_brief"]["target_output"],
        )

    def test_plan_stage_colloquial_full_company_request_can_skip_review(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Humans& 公司全量成员，重新跑，不要高成本。",
                "target_company": "Humans&",
            }
        )

        self.assertEqual(plan_result["request"]["execution_preferences"]["acquisition_strategy_override"], "full_company_roster")
        self.assertTrue(plan_result["request"]["execution_preferences"]["force_fresh_run"])
        self.assertTrue(plan_result["request"]["execution_preferences"]["use_company_employees_lane"])
        self.assertFalse(plan_result["request"]["execution_preferences"]["allow_high_cost_sources"])
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "full_company_roster")
        self.assertEqual(plan_result["plan"]["open_questions"], [])
        self.assertEqual(plan_result["plan_review_gate"]["status"], "ready")
        self.assertFalse(plan_result["plan_review_gate"]["required_before_execution"])
        self.assertIn("review_id", plan_result["plan_review_session"])
        self.assertTrue(plan_result["plan"]["acquisition_strategy"]["cost_policy"]["allow_company_employee_api"])
        self.assertFalse(plan_result["plan"]["acquisition_strategy"]["cost_policy"]["high_cost_requires_approval"])

    def test_plan_workflow_surfaces_incremental_execution_hints_for_large_org(self) -> None:
        roster_dir = self.settings.company_assets_dir / "anthropic" / "20260409T000000" / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        visible_entries = [
            {
                "name": "Ada Lovelace",
                "fullName": "Ada Lovelace",
                "linkedinUrl": "https://www.linkedin.com/in/ada",
            }
        ]
        (roster_dir / "harvest_company_employees_visible.json").write_text(
            json.dumps(visible_entries, ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_merged.json").write_text(
            json.dumps(visible_entries, ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_headless.json").write_text(
            json.dumps([], ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260409T000000",
                    "target_company": "Anthropic",
                    "visible_entry_count": 1,
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出 Anthropic 的所有成员，先全量获取 roster。",
                "target_company": "Anthropic",
            }
        )

        hints = dict(plan_result["plan_review_gate"]["execution_mode_hints"])
        self.assertEqual(hints["segmented_company_employee_shard_strategy"], "adaptive_us_function_partition")
        self.assertTrue(hints["adaptive_probe_required_before_live_roster"])
        self.assertEqual(
            hints["adaptive_company_employee_shard_policy"]["root_filters"],
            {"locations": ["United States"]},
        )
        self.assertEqual(
            hints["adaptive_company_employee_shard_policy"]["partition_rules"][0]["include_patch"]["function_ids"],
            ["8"],
        )
        self.assertTrue(hints["incremental_rerun_recommended"])
        self.assertEqual(
            hints["recommended_decision_patch"],
            {
                "reuse_existing_roster": True,
                "run_former_search_seed": True,
            },
        )
        self.assertEqual(
            hints["local_reusable_roster_snapshot"]["snapshot_id"],
            "20260409T000000",
        )
        self.assertEqual(
            hints["local_reusable_roster_snapshot"]["source_kind"],
            "harvest_company_employees",
        )
        self.assertTrue(
            any("prefer incremental reruns" in item for item in plan_result["plan_review_gate"]["suggested_actions"])
        )

    def test_anthropic_force_fresh_run_skips_local_bootstrap_in_acquisition(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-fresh"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "execution_preferences": {"force_fresh_run": True},
            }
        )
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire full company roster",
            description="Acquire roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster"},
        )
        state = {
            "company_identity": CompanyIdentity(
                requested_name="Anthropic",
                canonical_name="Anthropic",
                company_key="anthropic",
                linkedin_slug="anthropicresearch",
                linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
            ),
            "snapshot_dir": snapshot_dir,
        }

        expected = AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail="live acquisition",
            payload={"source": "live"},
        )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_anthropic_local_asset_task",
            side_effect=AssertionError("should not use local Anthropic bootstrap"),
        ), unittest.mock.patch.object(
            self.acquisition_engine,
            "_acquire_full_roster",
            return_value=expected,
        ) as acquire_mock:
            result = self.acquisition_engine.execute_task(
                task,
                request,
                request.target_company,
                state,
                bootstrap_summary={"candidate_count": 258},
            )

        acquire_mock.assert_called_once()
        self.assertEqual(result.detail, "live acquisition")
        self.assertEqual(result.payload["source"], "live")

    def test_anthropic_force_fresh_run_workflow_skips_local_bootstrap(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="anthropic_live_1",
                    name_en="Live Candidate",
                    display_name="Live Candidate",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Researcher",
                    focus_areas="Greater China experience",
                )
            ],
            [],
        )
        request_payload = {
            "raw_user_request": "帮我重新跑 Anthropic 全量成员，不沿用旧资产。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Greater China experience"],
            "execution_preferences": {"force_fresh_run": True},
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_anthropic_force_fresh"

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Workflow queued"},
        )

        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-live"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
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
                payload={"bootstrap_summary_seen": bootstrap_summary or {}},
                state_updates={},
            )

        self.acquisition_engine.execute_task = fake_execute_task
        with unittest.mock.patch.object(
            self.orchestrator,
            "ensure_bootstrapped",
            side_effect=AssertionError("should not bootstrap local Anthropic assets"),
        ):
            try:
                result = self.orchestrator.run_queued_workflow(job_id)
            finally:
                self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(result["status"], "completed")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")

    def test_run_queued_workflow_skips_terminal_job_without_restarting(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 当前成员",
                "target_company": "Anthropic",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_already_completed"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "done"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_run_workflow",
            side_effect=AssertionError("completed workflow should not be re-executed"),
        ):
            result = self.orchestrator.run_queued_workflow(job_id)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "already_terminal")

    def test_post_acquisition_refinement_reruns_retrieval_against_existing_snapshot(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_id = "20260408T150000"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        multimodal = Candidate(
            candidate_id="acme_multi",
            name_en="Mina Vision",
            display_name="Mina Vision",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="multimodal foundation models",
            source_dataset="acme_snapshot",
        )
        recruiting = Candidate(
            candidate_id="acme_recruit",
            name_en="Rita Recruit",
            display_name="Rita Recruit",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Recruiter",
            focus_areas="technical recruiting",
            source_dataset="acme_snapshot",
        )
        infra = Candidate(
            candidate_id="acme_infra",
            name_en="Ian Systems",
            display_name="Ian Systems",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="distributed systems platform",
            source_dataset="acme_snapshot",
        )
        snapshot_payload = {
            "candidates": [multimodal.to_record(), recruiting.to_record(), infra.to_record()],
            "evidence": [],
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        for candidate in [multimodal, recruiting, infra]:
            self.store.upsert_candidate(candidate)

        baseline = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "top_k": 3,
            }
        )
        self.assertEqual(baseline["summary"]["returned_matches"], 3)

        compiled = self.orchestrator.compile_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看 multimodal researcher，top 1",
            }
        )
        self.assertEqual(compiled["status"], "compiled")
        self.assertEqual(compiled["request_patch"]["categories"], ["researcher"])
        self.assertEqual(compiled["request_patch"]["must_have_facets"], ["multimodal"])
        self.assertEqual(compiled["request_patch"]["top_k"], 1)
        self.assertIn("intent_rewrite", compiled)
        self.assertFalse(compiled["intent_rewrite"]["instruction"]["matched"])

        refined = self.orchestrator.apply_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看 multimodal researcher，top 1",
            }
        )
        self.assertEqual(refined["status"], "completed")
        self.assertEqual(refined["request"]["categories"], ["researcher"])
        self.assertEqual(refined["request"]["must_have_facets"], ["multimodal"])
        self.assertEqual(refined["rerun_result"]["summary"]["candidate_source"]["snapshot_id"], snapshot_id)
        self.assertEqual(refined["rerun_result"]["summary"]["returned_matches"], 1)
        self.assertEqual(refined["rerun_result"]["matches"][0]["candidate_id"], "acme_multi")
        self.assertGreaterEqual(refined["diff"]["summary"]["removed_count"], 1)
        self.assertIn("intent_rewrite", refined)
        self.assertFalse(refined["intent_rewrite"]["instruction"]["matched"])

    def test_refinement_compile_exposes_intent_rewrite_for_shorthand_instruction(self) -> None:
        baseline = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "top_k": 3,
            }
        )

        compiled = self.orchestrator.compile_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看更偏华人的成员",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertTrue(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["intent_rewrite"]["instruction"]["rewrite"]["rewrite_id"],
            "greater_china_outreach",
        )
        self.assertEqual(
            compiled["request_patch"]["must_have_keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )

    def test_manual_review_synthesis_is_cached_without_resolving_queue_item(self) -> None:
        class ManualReviewSynthesisModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def synthesize_manual_review(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "summary": "Public evidence supports the affiliation, but the LinkedIn profile still carries conflicting content.",
                    "confidence_takeaways": ["The queue exists because structured membership signals and profile content are not fully aligned."],
                    "conflict_points": ["LinkedIn content contains suspicious mixed signals."],
                    "recommended_checks": ["Verify the experience timeline directly before resolving the item."],
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ManualReviewSynthesisModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, ManualReviewSynthesisModelClient()),
        )
        candidate = Candidate(
            candidate_id="cand_manual_synth",
            name_en="Lia Example",
            display_name="Lia Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/lia-example/",
            metadata={
                "membership_review_decision": "uncertain",
                "membership_review_rationale": "Headline and supporting evidence disagree.",
                "membership_review_trigger_keywords": ["openai", "spam"],
            },
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id("cand_manual_synth", "linkedin_profile_detail", "LinkedIn", "https://www.linkedin.com/in/lia-example/"),
                candidate_id="cand_manual_synth",
                source_type="linkedin_profile_detail",
                title="LinkedIn profile detail",
                url="https://www.linkedin.com/in/lia-example/",
                summary="LinkedIn profile detail captured for Lia Example; target-company membership requires manual review.",
                source_dataset="linkedin_profile_detail",
                source_path="/tmp/lia-profile.json",
            ).to_record(),
            {
                "evidence_id": "ev_public",
                "candidate_id": "cand_manual_synth",
                "source_type": "public_web_search",
                "title": "Official page mention",
                "url": "https://example.com/lia",
                "summary": "Official page suggests Acme affiliation.",
                "source_dataset": "public_web_search",
                "source_path": "/tmp/lia-web.json",
            },
        ]
        stored_items = self.store.replace_manual_review_items(
            "job_manual_synth",
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "target_company": "Acme",
                    "review_type": "manual_identity_resolution",
                    "priority": "high",
                    "status": "open",
                    "summary": "Needs manual review.",
                    "candidate": candidate.to_record(),
                    "evidence": evidence,
                    "metadata": {
                        "reasons": ["suspicious_membership"],
                        "confidence_label": "medium",
                        "confidence_score": 0.62,
                        "explanation": "Structured signals exist, but the profile content is noisy.",
                    },
                }
            ],
        )
        review_item_id = int(stored_items[0]["review_item_id"])

        synthesized = orchestrator.synthesize_manual_review_item({"review_item_id": review_item_id})
        self.assertEqual(synthesized["status"], "compiled")
        self.assertIn("conflicting content", synthesized["synthesis"]["summary"])
        self.assertEqual(synthesized["manual_review_item"]["status"], "open")
        self.assertEqual(
            synthesized["manual_review_item"]["metadata"]["manual_review_synthesis"]["summary"],
            synthesized["synthesis"]["summary"],
        )

        cached = orchestrator.synthesize_manual_review_item({"review_item_id": review_item_id})
        self.assertEqual(cached["status"], "cached")
        self.assertEqual(cached["synthesis"]["summary"], synthesized["synthesis"]["summary"])

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

    def test_plan_review_can_force_full_company_roster_company_employees_and_fresh_run(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "acquisition_strategy_override": "full_company_roster",
                    "use_company_employees_lane": True,
                    "force_fresh_run": True,
                    "allow_high_cost_sources": False,
                },
            }
        )
        review_plan = reviewed["review"]["plan"]
        acquisition = review_plan["acquisition_strategy"]
        self.assertEqual(acquisition["strategy_type"], "full_company_roster")
        self.assertEqual(acquisition["company_scope"], ["Humans&"])
        self.assertTrue(acquisition["cost_policy"]["allow_company_employee_api"])
        self.assertFalse(acquisition["cost_policy"]["allow_cached_roster_fallback"])
        self.assertFalse(acquisition["cost_policy"]["allow_historical_profile_inheritance"])
        self.assertFalse(acquisition["cost_policy"]["allow_shared_provider_cache"])
        acquire_task = next(task for task in review_plan["acquisition_tasks"] if task["task_type"] == "acquire_full_roster")
        self.assertEqual(acquire_task["metadata"]["strategy_type"], "full_company_roster")
        self.assertTrue(acquire_task["metadata"]["cost_policy"]["allow_company_employee_api"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_cached_roster_fallback"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_historical_profile_inheritance"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_shared_provider_cache"])

    def test_plan_review_persists_execution_preferences_into_review_request_and_queued_job(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出 Anthropic 的全量成员，重新跑，不沿用旧资产。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "acquisition_strategy_override": "full_company_roster",
                    "use_company_employees_lane": True,
                    "force_fresh_run": True,
                    "allow_high_cost_sources": False,
                },
            }
        )

        reviewed_request = reviewed["review"]["request"]
        for key, value in {
            "acquisition_strategy_override": "full_company_roster",
            "use_company_employees_lane": True,
            "force_fresh_run": True,
            "allow_high_cost_sources": False,
        }.items():
            self.assertEqual(reviewed_request["execution_preferences"][key], value)

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job = self.orchestrator.get_job(queued["job_id"])
        self.assertIsNotNone(job)
        assert job is not None
        for key, value in {
            "acquisition_strategy_override": "full_company_roster",
            "use_company_employees_lane": True,
            "force_fresh_run": True,
            "allow_high_cost_sources": False,
        }.items():
            self.assertEqual(job["request"]["execution_preferences"][key], value)

    def test_compile_plan_review_instruction_uses_model_then_schema_validation(self) -> None:
        class ReviewInstructionModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "use_company_employees_lane": True,
                        "force_fresh_run": True,
                        "allow_high_cost_sources": False,
                        "unsupported_field": "ignored",
                    }
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ReviewInstructionModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, ReviewInstructionModelClient()),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        compiled = orchestrator.compile_plan_review_instruction(
            {
                "review_id": review_id,
                "reviewer": "tester",
                "instruction": "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertEqual(compiled["instruction_compiler"]["source"], "model")
        self.assertEqual(compiled["instruction_compiler"]["provider"], "test-model")
        self.assertIn("intent_rewrite", compiled)
        self.assertFalse(compiled["intent_rewrite"]["request"]["matched"])
        self.assertFalse(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["review_payload"]["decision"],
            {
                "acquisition_strategy_override": "full_company_roster",
                "use_company_employees_lane": True,
                "force_fresh_run": True,
                "allow_high_cost_sources": False,
            },
        )

    def test_compile_plan_review_instruction_inferrs_company_employees_lane_from_colloquial_request(self) -> None:
        class ReviewInstructionModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "confirmed_company_scope": ["Humans&"],
                        "force_fresh_run": True,
                    }
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ReviewInstructionModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, ReviewInstructionModelClient()),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        compiled = orchestrator.compile_plan_review_instruction(
            {
                "review_id": review_id,
                "reviewer": "tester",
                "instruction": "我要求 scope 大一些，要公司全量成员，要重新跑。",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertIn("intent_rewrite", compiled)
        self.assertFalse(compiled["intent_rewrite"]["request"]["matched"])
        self.assertFalse(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["review_payload"]["decision"],
            {
                "acquisition_strategy_override": "full_company_roster",
                "confirmed_company_scope": ["Humans&"],
                "force_fresh_run": True,
                "use_company_employees_lane": True,
            },
        )
        self.assertEqual(compiled["instruction_compiler"]["policy_inferred_keys"], ["use_company_employees_lane"])

    def test_workflow_progress_schema_summarizes_stage_and_worker_state(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的华人技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
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
        progress = self.orchestrator.get_job_progress(workflow["job"]["job_id"])
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["status"], "completed")
        self.assertEqual(progress["stage"], "completed")
        self.assertIn("elapsed_seconds", progress)
        self.assertIn("timing", progress["progress"])
        self.assertGreaterEqual(progress["progress"]["counters"]["result_count"], 1)
        self.assertTrue(progress["progress"]["milestones"])
        self.assertIn("worker_summary", progress["progress"])
        self.assertGreaterEqual(len(progress["progress"]["completed_stages"]), 1)

    def test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_id = "20260408T120000"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        baseline_candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alex Builder",
            display_name="Alex Builder",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            focus_areas="",
            source_dataset="baseline_snapshot",
            linkedin_url="https://www.linkedin.com/in/alex-builder/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [baseline_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.upsert_candidate(baseline_candidate)

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Acme 偏 GPU systems 的 current researcher",
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["GPU systems"],
                "top_k": 3,
            }
        )
        request = JobRequest.from_payload(dict(plan_result["request"]))
        job_id = "job_completed_reconcile"
        initial_artifact = self.orchestrator._execute_retrieval(
            job_id,
            request,
            dict(plan_result["plan"]),
            job_type="workflow",
            runtime_policy={"workflow_snapshot_id": snapshot_id, "summary_mode": "deterministic"},
        )
        self.assertEqual(initial_artifact["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertTrue(initial_artifact["summary"]["candidate_source"]["source_path"].endswith("candidate_documents.json"))
        initial_results = self.orchestrator.get_job_results(job_id)
        assert initial_results is not None
        self.assertEqual(initial_results["job"]["summary"]["returned_matches"], 0)
        self.assertEqual(initial_results["results"], [])

        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=dict(plan_result["plan"]),
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key=baseline_candidate.candidate_id,
            stage="enriching",
            span_name="explore_candidate:Alex Builder",
            budget_payload={"max_queries": 7},
            input_payload={
                "candidate_id": baseline_candidate.candidate_id,
                "display_name": baseline_candidate.display_name,
                "candidate": baseline_candidate.to_record(),
            },
            metadata={
                "target_company": "Acme",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": dict(plan_result["plan"]),
                "runtime_mode": "workflow",
            },
            handoff_from_lane="enrichment_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="running",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"status": "queued"}},
        )

        original_explore = self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate

        def _complete_background_exploration(**kwargs):
            candidate = kwargs["candidate"]
            enriched_candidate = Candidate(
                **{
                    **candidate.to_record(),
                    "focus_areas": "GPU systems",
                    "notes": "Exploration: validated GPU systems work",
                    "metadata": {
                        **dict(candidate.metadata or {}),
                        "exploration_validated_summaries": ["Validated Acme GPU systems affiliation."],
                    },
                }
            )
            evidence = [
                EvidenceRecord(
                    evidence_id=make_evidence_id(candidate.candidate_id, "exploration_summary", "Acme GPU systems", "https://example.com/acme"),
                    candidate_id=candidate.candidate_id,
                    source_type="exploration_summary",
                    title="Acme GPU systems",
                    url="https://example.com/acme",
                    summary="Validated Acme GPU systems affiliation.",
                    source_dataset="background_exploration",
                    source_path=str(snapshot_dir / "exploration" / candidate.candidate_id),
                    metadata={"provider": "test"},
                )
            ]
            worker = self.store.get_agent_worker(
                job_id=kwargs["job_id"],
                lane_id="exploration_specialist",
                worker_key=candidate.candidate_id,
            )
            if worker is not None:
                self.store.complete_agent_worker(
                    int(worker["worker_id"]),
                    status="completed",
                    checkpoint_payload={"stage": "completed", "completed_queries": ["1"]},
                    output_payload={
                        "candidate": enriched_candidate.to_record(),
                        "evidence": [item.to_record() for item in evidence],
                        "summary": {"candidate_id": candidate.candidate_id, "status": "completed"},
                        "errors": [],
                    },
                )
            return {
                "worker_status": "completed",
                "candidate": enriched_candidate,
                "evidence": evidence,
                "summary": {"candidate_id": candidate.candidate_id, "status": "completed"},
                "errors": [],
            }

        self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = _complete_background_exploration
        try:
            recovery = self.orchestrator.run_worker_recovery_once({"job_id": job_id, "total_limit": 1})
        finally:
            self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = original_explore

        self.assertEqual(recovery["daemon"]["executed_count"], 1)
        self.assertEqual(recovery["post_completion_reconcile"][0]["status"], "reconciled")
        refreshed = self.orchestrator.get_job_results(job_id)
        assert refreshed is not None
        self.assertEqual(refreshed["job"]["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertTrue(
            refreshed["job"]["summary"]["candidate_source"]["source_path"].endswith(
                "normalized_artifacts/materialized_candidate_documents.json"
            )
        )
        self.assertEqual(refreshed["job"]["summary"]["background_reconcile"]["applied_worker_count"], 1)
        self.assertEqual(refreshed["job"]["summary"]["returned_matches"], 1)
        self.assertEqual(refreshed["results"][0]["focus_areas"], "GPU systems")
        self.assertEqual(
            self.store.list_evidence(baseline_candidate.candidate_id)[0]["source_type"],
            "exploration_summary",
        )

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

    def test_acquire_full_roster_force_fresh_run_disables_shared_provider_cache_at_execution_time(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        seen: dict[str, object] = {}

        def _fake_fetch_company_roster(
            _identity,
            _snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            allow_shared_provider_cache=True,
        ):
            seen["allow_shared_provider_cache"] = allow_shared_provider_cache
            seen["max_pages"] = max_pages
            seen["page_limit"] = page_limit
            merged_path = snapshot_dir / "merged.json"
            visible_path = snapshot_dir / "visible.json"
            headless_path = snapshot_dir / "headless.json"
            summary_path = snapshot_dir / "summary.json"
            merged_path.write_text("[]", encoding="utf-8")
            visible_path.write_text("[]", encoding="utf-8")
            headless_path.write_text("[]", encoding="utf-8")
            summary_path.write_text("{}", encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                raw_entries=[{"full_name": "Ada Example"}],
                visible_entries=[{"full_name": "Ada Example"}],
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": 1}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="",
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "cost_policy": {
                    "allow_company_employee_api": True,
                    "allow_cached_roster_fallback": True,
                    "allow_shared_provider_cache": True,
                },
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch_company_roster,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic 全量 roster",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertFalse(bool(seen["allow_shared_provider_cache"]))

    def test_acquire_full_roster_can_reuse_existing_harvest_snapshot_for_incremental_run(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        existing_snapshot_dir = self.settings.company_assets_dir / "anthropic" / "20260409T000000" / "harvest_company_employees"
        existing_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (existing_snapshot_dir / "harvest_company_employees_merged.json").write_text(
            json.dumps([{"full_name": "Ada Example", "headline": "Researcher", "linkedin_url": "https://www.linkedin.com/in/ada-example/"}]),
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_visible.json").write_text(
            json.dumps([{"full_name": "Ada Example", "headline": "Researcher", "linkedin_url": "https://www.linkedin.com/in/ada-example/"}]),
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_headless.json").write_text(
            "[]",
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_summary.json").write_text(
            json.dumps(
                {
                    "company_identity": identity.to_record(),
                    "raw_entry_count": 1,
                    "visible_entry_count": 1,
                    "headless_entry_count": 0,
                    "page_summaries": [{"page": 1, "entry_count": 1}],
                    "accounts_used": ["harvest_company_employees"],
                    "errors": [],
                    "stop_reason": "completed",
                }
            ),
            encoding="utf-8",
        )

        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "20260409T111111"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        task = AcquisitionTask(
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
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=AssertionError("live roster fetch should be skipped when reusing existing roster"),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="基于现有 roster 做增量",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"reuse_existing_roster": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["acquisition_mode"], "reused_cached_roster")
        reused_summary = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json"
        self.assertTrue(reused_summary.exists())
        summary_payload = json.loads(reused_summary.read_text())
        self.assertEqual(summary_payload["reused_from_snapshot_id"], "20260409T000000")

    def test_acquire_full_roster_uses_adaptive_shard_policy_to_plan_live_shards(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-adaptive-shard-policy"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )

        planned_shards = [
            {
                "strategy_id": "adaptive_us_function_partition",
                "shard_id": "engineering",
                "title": "United States / Engineering",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
            },
            {
                "strategy_id": "adaptive_us_function_partition",
                "shard_id": "remaining_after_engineering",
                "title": "United States / Remaining after Engineering",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
            },
        ]
        seen_filters: list[dict[str, object]] = []

        def _fake_fetch_company_roster(
            _identity,
            shard_snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen_filters.append(dict(company_filters or {}))
            raw_entries = [
                {
                    "member_key": Path(shard_snapshot_dir).name,
                    "full_name": f"Member {Path(shard_snapshot_dir).name}",
                    "headline": "Engineer",
                    "location": "United States",
                    "linkedin_url": f"https://www.linkedin.com/in/{Path(shard_snapshot_dir).name}/",
                    "is_headless": False,
                    "page": 1,
                }
            ]
            shard_dir = Path(shard_snapshot_dir)
            merged_path = shard_dir / "merged.json"
            visible_path = shard_dir / "visible.json"
            headless_path = shard_dir / "headless.json"
            summary_path = shard_dir / "summary.json"
            for path, payload in [
                (merged_path, "[]"),
                (visible_path, "[]"),
                (headless_path, "[]"),
                (summary_path, "{}"),
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(payload, encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=shard_dir.name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=shard_dir,
                raw_entries=raw_entries,
                visible_entries=list(raw_entries),
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": 1}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="completed",
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shard_policy": {
                    "strategy_id": "adaptive_us_function_partition",
                    "root_title": "United States",
                    "root_filters": {"locations": ["United States"]},
                    "partition_rules": [
                        {
                            "rule_id": "engineering",
                            "title": "Engineering",
                            "include_patch": {"function_ids": ["8"]},
                            "remainder_exclude_patch": {"exclude_function_ids": ["8"]},
                        }
                    ],
                    "max_pages": 100,
                    "page_limit": 25,
                    "probe_max_pages": 1,
                    "probe_page_limit": 25,
                    "provider_result_cap": 2500,
                },
            },
        )
        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_resolve_adaptive_company_employee_shards",
            return_value={"status": "planned", "detail": "planned", "shards": planned_shards},
        ) as plan_mock, unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch_company_roster,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="Anthropic 全量 roster",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        plan_mock.assert_called_once()
        self.assertEqual(
            seen_filters,
            [
                {"locations": ["United States"], "function_ids": ["8"]},
                {"locations": ["United States"], "exclude_function_ids": ["8"]},
            ],
        )

    def test_acquire_full_roster_merges_segmented_harvest_shards(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-segmented-harvest"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        seen_filters: list[dict[str, object]] = []

        def _fake_fetch_company_roster(
            _identity,
            shard_snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen_filters.append(dict(company_filters or {}))
            shard_id = Path(shard_snapshot_dir).name
            merged_path = Path(shard_snapshot_dir) / "merged.json"
            visible_path = Path(shard_snapshot_dir) / "visible.json"
            headless_path = Path(shard_snapshot_dir) / "headless.json"
            summary_path = Path(shard_snapshot_dir) / "summary.json"
            for path, payload in [
                (merged_path, "[]"),
                (visible_path, "[]"),
                (headless_path, "[]"),
                (summary_path, "{}"),
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(payload, encoding="utf-8")
            if shard_id == "us_engineering":
                raw_entries = [
                    {
                        "member_key": "alice",
                        "full_name": "Alice Example",
                        "headline": "Engineer",
                        "location": "San Francisco Bay Area",
                        "linkedin_url": "https://www.linkedin.com/in/alice-example/",
                        "is_headless": False,
                        "page": 1,
                    },
                    {
                        "member_key": "shared",
                        "full_name": "Shared Person",
                        "headline": "Engineer",
                        "location": "New York City Metropolitan Area",
                        "linkedin_url": "https://www.linkedin.com/in/shared-person/",
                        "is_headless": False,
                        "page": 1,
                    },
                ]
            else:
                raw_entries = [
                    {
                        "member_key": "shared",
                        "full_name": "Shared Person",
                        "headline": "Staff",
                        "location": "New York City Metropolitan Area",
                        "linkedin_url": "https://www.linkedin.com/in/shared-person/",
                        "is_headless": False,
                        "page": 1,
                    },
                    {
                        "member_key": "bob",
                        "full_name": "Bob Example",
                        "headline": "Ops",
                        "location": "Mountain View, California, United States",
                        "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                        "is_headless": False,
                        "page": 1,
                    },
                ]
            return CompanyRosterSnapshot(
                snapshot_id=Path(shard_snapshot_dir).name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=Path(shard_snapshot_dir),
                raw_entries=raw_entries,
                visible_entries=list(raw_entries),
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": len(raw_entries)}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="completed",
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shards": [
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_engineering",
                        "title": "United States / Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                    },
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_non_engineering",
                        "title": "United States / Exclude Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
                    },
                ],
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch_company_roster,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="找 Anthropic 美国成员",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(execution.state_updates["roster_snapshot"].visible_entries), 3)
        self.assertEqual(
            seen_filters,
            [
                {"locations": ["United States"], "function_ids": ["8"]},
                {"locations": ["United States"], "exclude_function_ids": ["8"]},
            ],
        )
        self.assertIn("segmented Harvest company-employee shards", execution.detail)

    def test_acquire_full_roster_blocks_when_segmented_background_harvest_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-segmented-harvest-queued"
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
            metadata={
                "strategy_type": "full_company_roster",
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shards": [
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_engineering",
                        "title": "United States / Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                    },
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_non_engineering",
                        "title": "United States / Exclude Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
                    },
                ],
            },
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
            side_effect=AssertionError("fetch_company_roster should not run while segmented harvest workers are queued"),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_segmented_harvest_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Anthropic US people",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_segmented_harvest_queued",
            lane_id="acquisition_specialist",
        )
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 2)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest_shards")
        self.assertEqual(len(workers), 2)
        self.assertTrue(all(worker["metadata"]["root_snapshot_dir"] == str(snapshot_dir) for worker in workers))
        company_filters = [dict(worker["metadata"].get("company_filters") or {}) for worker in workers]
        self.assertIn({"locations": ["United States"], "function_ids": ["8"]}, company_filters)
        self.assertIn({"locations": ["United States"], "exclude_function_ids": ["8"]}, company_filters)

        resumed_state = self.orchestrator._build_acquisition_state("job_segmented_harvest_queued", {})
        self.assertEqual(resumed_state["snapshot_dir"], snapshot_dir)

    def test_acquire_search_seed_pool_allows_provider_only_former_pass_without_queries(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-former-provider-only"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        seeded_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former_1",
                    "full_name": "Former Example",
                    "source_type": "harvest_profile_search",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                }
            ],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=summary_path,
        )
        task = AcquisitionTask(
            task_id="former-seed-only",
            task_type="acquire_search_seed_pool",
            title="Acquire former seed",
            description="Former-only provider recall",
            status="ready",
            blocking=False,
            metadata={
                "strategy_type": "former_employee_search",
                "employment_statuses": ["former"],
                "search_seed_queries": [],
                "search_query_bundles": [],
                "filter_hints": {"past_companies": ["Anthropic"]},
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
            },
        )
        with unittest.mock.patch.object(
            self.acquisition_engine.search_seed_acquirer,
            "discover",
            return_value=seeded_snapshot,
        ):
            execution = self.acquisition_engine._acquire_search_seed_pool(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="找 Anthropic former",
                    target_company="Anthropic",
                    categories=["former_employee"],
                    employment_statuses=["former"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(execution.state_updates["search_seed_snapshot"].entries), 1)

    def test_acquire_full_roster_adds_default_former_search_seed_snapshot(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-roster-with-former"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        summary_path = snapshot_dir / "summary.json"
        for path, payload in [
            (merged_path, "[]"),
            (visible_path, "[]"),
            (headless_path, "[]"),
            (summary_path, "{}"),
        ]:
            path.write_text(payload, encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[{"full_name": "Ada Current"}],
            visible_entries=[{"full_name": "Ada Current"}],
            headless_entries=[],
            page_summaries=[{"page": 1, "entry_count": 1}],
            accounts_used=["harvest_company_employees"],
            errors=[],
            stop_reason="completed",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )
        former_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        former_summary_path.parent.mkdir(parents=True, exist_ok=True)
        former_summary_path.write_text("{}", encoding="utf-8")
        former_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former_1",
                    "full_name": "Former Ada",
                    "source_type": "harvest_profile_search",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-ada/",
                }
            ],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=former_summary_path,
        )
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
            metadata={
                "strategy_type": "full_company_roster",
                "cost_policy": {"allow_company_employee_api": True},
                "include_former_search_seed": True,
                "former_provider_people_search_min_expected_results": 50,
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            return_value=roster_snapshot,
        ), unittest.mock.patch.object(
            self.acquisition_engine,
            "_acquire_search_seed_pool",
            return_value=AcquisitionExecution(
                task_id="former-seed",
                status="completed",
                detail="Captured 1 former seed.",
                payload={},
                state_updates={"search_seed_snapshot": former_snapshot},
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="找 Anthropic 全量成员",
                    target_company="Anthropic",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertIn("search_seed_snapshot", execution.state_updates)
        self.assertEqual(execution.payload["former_search_seed_entry_count"], 1)

    def test_enrich_profiles_merges_roster_and_search_seed_candidates_before_enrichment(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-merge-enrichment"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        roster_summary_path = snapshot_dir / "roster_summary.json"
        for path, payload in [
            (merged_path, "[]"),
            (visible_path, "[]"),
            (headless_path, "[]"),
            (roster_summary_path, "{}"),
        ]:
            path.write_text(payload, encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[],
            visible_entries=[],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=roster_summary_path,
        )
        seed_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        seed_summary_path.parent.mkdir(parents=True, exist_ok=True)
        seed_summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=seed_summary_path,
        )
        roster_candidate = Candidate(
            candidate_id="roster_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/ada-example/",
            source_dataset="anthropic_linkedin_company_people",
            notes="LinkedIn company roster baseline.",
        )
        seed_candidate = Candidate(
            candidate_id="seed_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="former_employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="former",
            role="Former Research Scientist",
            linkedin_url="https://www.linkedin.com/in/ada-example/",
            source_dataset="anthropic_search_seed_candidates",
        )
        roster_evidence = EvidenceRecord(
            evidence_id="ev_roster_1",
            candidate_id="roster_1",
            source_type="linkedin_company_people",
            title="Roster row",
            url="https://www.linkedin.com/in/ada-example/",
            summary="Current roster row",
            source_dataset="anthropic_linkedin_company_people",
            source_path=str(roster_summary_path),
        )
        seed_evidence = EvidenceRecord(
            evidence_id="ev_seed_1",
            candidate_id="seed_1",
            source_type="harvest_profile_search",
            title="Former seed",
            url="https://www.linkedin.com/in/ada-example/",
            summary="Former seed row",
            source_dataset="anthropic_search_seed_candidates",
            source_path=str(seed_summary_path),
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            return MultiSourceEnrichmentResult(
                candidates=list(candidates_arg),
                evidence=[],
            )

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            with unittest.mock.patch(
                "sourcing_agent.acquisition.build_candidates_from_roster",
                return_value=([roster_candidate], [roster_evidence]),
            ), unittest.mock.patch(
                "sourcing_agent.acquisition.build_candidates_from_seed_snapshot",
                return_value=([seed_candidate], [seed_evidence]),
            ):
                execution = self.acquisition_engine._enrich_profiles(
                    AcquisitionTask(
                        task_id="enrich-profiles",
                        task_type="enrich_profiles_multisource",
                        title="Enrich profiles",
                        description="Run profile enrichment",
                        status="ready",
                        blocking=True,
                        metadata={"cost_policy": {}},
                    ),
                    {
                        "roster_snapshot": roster_snapshot,
                        "search_seed_snapshot": search_seed_snapshot,
                        "snapshot_dir": snapshot_dir,
                    },
                    JobRequest(
                        raw_user_request="找 Anthropic 成员",
                        target_company="Anthropic",
                        categories=["employee"],
                    ),
                )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(execution.payload["acquisition_canonicalization"]["canonical_candidate_count"], 1)

    def test_enrich_profiles_completes_when_background_exploration_is_pending(self) -> None:
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

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["queued_exploration_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_exploration")
        self.assertEqual(execution.payload["candidate_count"], 1)

    def test_enrich_profiles_completes_when_background_harvest_profile_batch_is_pending(self) -> None:
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
            metadata={
                "profile_detail_limit": 5,
                "slug_resolution_limit": 5,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
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
        ), unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher.slug_resolver,
            "resolve",
            return_value={"results": [], "errors": [], "summary_path": None},
        ), unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher.publication_connector,
            "enrich",
            return_value={
                "matched_candidates": [],
                "lead_candidates": [],
                "publication_matches": [],
                "coauthor_edges": [],
                "scholar_coauthor_prospects": [],
                "artifact_paths": {},
                "errors": [],
                "evidence": [],
            },
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
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_profile_batch")

    def test_enrich_profiles_blocks_when_full_roster_profile_prefetch_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-enrich-full-prefetch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-enrich-full-prefetch",
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Ada Example",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/ada-example/",
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
            metadata={
                "strategy_type": "full_company_roster",
                "full_roster_profile_prefetch": True,
                "profile_detail_limit": 5,
                "slug_resolution_limit": 5,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
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
            self.acquisition_engine.multi_source_enricher.slug_resolver,
            "resolve",
            side_effect=AssertionError("slug resolution should not continue before full roster prefetch completes"),
        ), unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher.publication_connector,
            "enrich",
            side_effect=AssertionError("publication enrichment should not continue before full roster prefetch completes"),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_profile_full_prefetch",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Anthropic people",
                    target_company="Anthropic",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_harvest_profile_full_prefetch",
            lane_id="enrichment_specialist",
        )
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_profile_batch")
        self.assertFalse((snapshot_dir / "candidate_documents.json").exists())

    def test_enrich_profiles_force_fresh_run_disables_shared_provider_cache_at_execution_time(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-enrich-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-enrich-fresh-runtime",
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Ada Example",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/ada-example/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        seen: dict[str, object] = {}
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        self.acquisition_engine.multi_source_enricher.enrich = lambda *args, **kwargs: (
            seen.update({"cost_policy": dict(kwargs.get("cost_policy") or {})}) or MultiSourceEnrichmentResult(
                candidates=[],
                evidence=[],
            )
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={"cost_policy": {"allow_shared_provider_cache": True}},
        )
        try:
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic enrichment",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertFalse(bool(seen["cost_policy"]["allow_shared_provider_cache"]))

    def test_reconcile_completed_workflow_after_background_harvest_prefetch(self) -> None:
        request_payload = {
            "raw_user_request": "Find Humans& coding researchers",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["coding"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_prefetch_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "humansand" / "snapshot-reconcile"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.settings.jobs_dir / f"{job_id}.result.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False), encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::abc123",
            stage="enriching",
            span_name="harvest_profile_batch:abc123",
            budget_payload={"requested_url_count": 2},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/test"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed"}},
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.build_company_candidate_artifacts",
            return_value={
                "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                "artifact_paths": {"materialized_candidate_documents": str(snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json")},
            },
        ):
            with unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after background harvest reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ):
                reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        refreshed_job = self.store.get_job(job_id)
        self.assertEqual(reconcile["status"], "reconciled_harvest_prefetch")
        self.assertIsNotNone(refreshed_job)
        assert refreshed_job is not None
        self.assertEqual(refreshed_job["status"], "completed")
        self.assertEqual(refreshed_job["stage"], "completed")
        summary = dict(refreshed_job.get("summary") or {})
        self.assertIn("background_reconcile", summary)
        self.assertIn("harvest_prefetch", dict(summary.get("background_reconcile") or {}))

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
            self.assertIn("intent_rewrite", plan_resp)
            self.assertIn("request", plan_resp["intent_rewrite"])
            compile_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan/review/compile-instruction",
                data=json.dumps(
                    {
                        "review_id": plan_resp["plan_review_session"]["review_id"],
                        "reviewer": "tester",
                        "instruction": "改成 full company roster，不允许高成本 source。",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(compile_review_req) as response:
                compile_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(compile_review_resp["status"], "compiled")
            self.assertIn("review_payload", compile_review_resp)
            self.assertIn("instruction_compiler", compile_review_resp)
            self.assertIn("intent_rewrite", compile_review_resp)
            self.assertIn("instruction", compile_review_resp["intent_rewrite"])
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
            self.assertIn("intent_rewrite", result_resp)

            refine_compile_req = urllib_request.Request(
                f"http://{host}:{port}/api/results/refine/compile-instruction",
                data=json.dumps(
                    {
                        "job_id": job_resp["job_id"],
                        "instruction": "top 1",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(refine_compile_req) as response:
                refine_compile_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(refine_compile_resp["status"], "compiled")
            self.assertEqual(refine_compile_resp["request_patch"]["top_k"], 1)
            self.assertEqual(refine_compile_resp["baseline_job_id"], job_resp["job_id"])
            self.assertIn("intent_rewrite", refine_compile_resp)

            refine_req = urllib_request.Request(
                f"http://{host}:{port}/api/results/refine",
                data=json.dumps(
                    {
                        "job_id": job_resp["job_id"],
                        "instruction": "top 1",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(refine_req) as response:
                refine_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(refine_resp["status"], "completed")
            self.assertEqual(refine_resp["baseline_job_id"], job_resp["job_id"])
            self.assertEqual(refine_resp["request_patch"]["top_k"], 1)
            self.assertEqual(refine_resp["rerun_result"]["summary"]["returned_matches"], 1)

            progress_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/progress",
                method="GET",
            )
            with opener.open(progress_req) as response:
                progress_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(progress_resp["job_id"], job_resp["job_id"])
            self.assertIn("progress", progress_resp)
            self.assertIn("counters", progress_resp["progress"])

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

            manual_review_items = self.store.replace_manual_review_items(
                job_resp["job_id"],
                [
                    {
                        "candidate_id": str(job_resp["matches"][0]["candidate_id"]),
                        "target_company": "Anthropic",
                        "review_type": "manual_identity_resolution",
                        "priority": "high",
                        "status": "open",
                        "summary": "Conflicting public and profile evidence needs synthesis.",
                        "candidate": dict(job_resp["matches"][0]),
                        "evidence": [
                            EvidenceRecord(
                                evidence_id=make_evidence_id(
                                    str(job_resp["matches"][0]["candidate_id"]),
                                    "linkedin_profile_detail",
                                    "LinkedIn profile",
                                    str(job_resp["matches"][0].get("linkedin_url") or "https://linkedin.com/in/example"),
                                ),
                                candidate_id=str(job_resp["matches"][0]["candidate_id"]),
                                source_type="linkedin_profile_detail",
                                title="LinkedIn profile",
                                url=str(job_resp["matches"][0].get("linkedin_url") or "https://linkedin.com/in/example"),
                                summary="LinkedIn profile exists, but target-company relationship still needs confirmation.",
                                source_dataset="linkedin_profile_detail",
                                source_path="/tmp/linkedin-profile.json",
                            ).to_record(),
                            {
                                "evidence_id": "ev_public_manual_smoke",
                                "candidate_id": str(job_resp["matches"][0]["candidate_id"]),
                                "source_type": "public_web_search",
                                "title": "Public web mention",
                                "url": "https://example.com/manual-review-smoke",
                                "summary": "A public page references the candidate alongside Anthropic-related work.",
                                "source_dataset": "public_web_search",
                                "source_path": "/tmp/public-search.json",
                            },
                        ],
                        "metadata": {
                            "reasons": ["suspicious_membership"],
                            "confidence_label": "medium",
                            "confidence_score": 0.61,
                            "explanation": "Signals are promising, but the evidence needs a human-authored conclusion.",
                        },
                    }
                ],
            )
            review_item_id = int(manual_review_items[0]["review_item_id"])

            manual_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review",
                method="GET",
            )
            with opener.open(manual_review_req) as response:
                manual_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("manual_review_items", manual_review_resp)
            self.assertGreaterEqual(len(manual_review_resp["manual_review_items"]), 1)

            synthesize_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review/synthesize",
                data=json.dumps({"review_item_id": review_item_id}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(synthesize_req) as response:
                synthesize_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(synthesize_resp["status"], "compiled")
            self.assertEqual(synthesize_resp["review_item_id"], review_item_id)
            self.assertIn("summary", synthesize_resp["synthesis"])
            self.assertEqual(
                synthesize_resp["manual_review_item"]["metadata"]["manual_review_synthesis"]["summary"],
                synthesize_resp["synthesis"]["summary"],
            )

            synthesize_cached_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review/synthesize",
                data=json.dumps({"review_item_id": review_item_id}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(synthesize_cached_req) as response:
                synthesize_cached_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(synthesize_cached_resp["status"], "cached")
            self.assertEqual(synthesize_cached_resp["review_item_id"], review_item_id)

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

    def test_normalize_snapshot_force_fresh_run_skips_historical_profile_inheritance_at_execution_time(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-normalize-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        candidate = Candidate(
            candidate_id="anthropic_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
            metadata={"cost_policy": {"allow_historical_profile_inheritance": True}},
        )
        with unittest.mock.patch(
            "sourcing_agent.acquisition._inherit_historical_profile_captures",
            side_effect=AssertionError("force_fresh_run should skip historical profile inheritance"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition.canonicalize_company_records",
            return_value=([candidate], [], {"merged_candidate_count": 0}),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [],
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic normalize",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("anthropic_1")
        self.assertIsNotNone(stored_candidate)

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
