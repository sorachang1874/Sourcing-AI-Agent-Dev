"""Pre-retrieval refresh contracts — shard-B port group D.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/NONBAND_OWNERSHIP_R2_SHARD_B_
2026-07-22.md group D): the pre-retrieval refresh inline apply+sync for
completed background search/company-roster outputs and its skip/defer ladder
(materialization-only preview defer, stage-candidate-document preview skip,
direct-finalization harvest-prefetch defer, equivalent-baseline / reused
serving-snapshot / reused-candidate-document materialization skips,
snapshot-id fallback) had no modern pins. Ported verbatim onto the
repo-standard PG fixture with the two snapshot/artifact helpers. Old->new
mapping in docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks
same-change.
"""

import json
import os
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class PreRetrievalRefreshTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")
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
        runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        runtime_env_patcher.start()
        self.addCleanup(runtime_env_patcher.stop)

    def _write_company_snapshot_candidate_documents(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidates: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        normalized_key = normalize_company_key(target_company)
        company_key = canonicalize_company_key(target_company) or normalized_key
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [normalized_key] if normalized_key and normalized_key != company_key else [],
        }
        company_dir = Path(self.tempdir.name) / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": target_company,
                        "snapshot_id": snapshot_id,
                        "company_identity": identity,
                    },
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidates": candidates,
                    "evidence": [],
                    "candidate_count": len(candidates),
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                    "target_company": target_company,
                    "company_key": company_key,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "retrieval_index_summary.json").write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return snapshot_dir, candidate_doc_path

    def _write_snapshot_normalized_artifacts(
        self,
        *,
        snapshot_dir: Path,
        target_company: str,
        include_strict: bool = True,
        include_serving_docs: bool = False,
    ) -> None:
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": target_company,
            "candidate_count": 1,
        }
        (normalized_dir / "manifest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if include_serving_docs:
            serving_payload = {
                "target_company": target_company,
                "snapshot_id": snapshot_dir.name,
                "candidates": [],
                "evidence": [],
                "candidate_count": 0,
                "evidence_count": 0,
            }
            (normalized_dir / "materialized_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (normalized_dir / "reusable_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        if include_strict:
            strict_dir = normalized_dir / "strict_roster_only"
            strict_dir.mkdir(parents=True, exist_ok=True)
            (strict_dir / "manifest.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (strict_dir / "artifact_summary.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def test_refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_refresh"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-pre-retrieval-refresh"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (discovery_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "seed_key": "baseline",
                        "full_name": "Baseline Lead",
                        "source_type": "harvest_profile_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/baseline-lead/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "completed",
                    "queued_query_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-candidate",
                            name_en="Baseline Lead",
                            display_name="Baseline Lead",
                            category="employee",
                            target_company="Reflection AI",
                            organization="Reflection AI",
                            employment_status="current",
                            role="Infrastructure Engineer",
                            focus_areas="infra systems",
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle-infra::01",
            stage="acquiring",
            span_name="search_bundle:bundle-infra",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "Reflection AI infra"}},
            metadata={
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "query": "Reflection AI infra",
                    "bundle_id": "bundle-infra",
                    "source_family": "people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "new-lead",
                        "full_name": "Infra Builder",
                        "headline": "Platform Engineer",
                        "source_type": "web_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/infra-builder/",
                    }
                ],
                "errors": [],
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.build_company_candidate_artifacts",
                return_value={
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "materialized_candidate_documents": str(
                            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                        )
                    },
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                return_value={
                    "status": "queued",
                    "requested_url_count": 2,
                    "dispatched_url_count": 1,
                    "cached_profile_count": 1,
                    "queued_worker_count": 1,
                },
            ),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        self.assertEqual(refresh["status"], "completed")
        self.assertEqual(int(refresh["search_seed"]["added_entry_count"]), 1)
        self.assertEqual(int(dict(refresh["search_seed"].get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)
        # CALIBRATED 2026-07-22: with a prefetch worker queued the store sync
        # defers (deferred-materialization contract, same family as the
        # defers-sync-while-same-kind-worker-pending pin in
        # test_worker_completion_pipeline).
        self.assertEqual(str(refresh["sync"]["status"]), "deferred")
        updated_candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertGreaterEqual(int(updated_candidate_doc["candidate_count"]), 2)
        # CALIBRATED 2026-07-22: store visibility rides the DEFERRED sync now
        # (it lands when the queued prefetch worker completes — pinned in
        # test_worker_completion_pipeline); the inline apply's observable is
        # the candidate-document update above.
        self.assertIsNone(
            self.store.find_candidate_by_name(
                target_company="Reflection AI",
                name_en="Infra Builder",
            )
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("search_seed", background_reconcile)
        self.assertEqual(
            int(dict(background_reconcile["search_seed"].get("profile_prefetch") or {}).get("queued_worker_count") or 0),
            1,
        )
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        latest_metrics = dict(dict(progress.get("progress") or {}).get("latest_metrics") or {})
        self.assertIn("pre_retrieval_refresh", latest_metrics)
        self.assertIn("refresh_metrics", latest_metrics)
        refresh_metrics = dict(latest_metrics.get("refresh_metrics") or {})
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("inline_search_seed_worker_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_search_seed_reconcile_count") or 0), 1)

    def test_refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find MiroMind.ai people",
            "target_company": "MiroMind.ai",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_company_roster_refresh"
        snapshot_dir = self.settings.company_assets_dir / "miromindai" / "snapshot-company-roster-refresh"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "us_core"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.parent.mkdir(parents=True, exist_ok=True)
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "MiroMind.ai",
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        identity = CompanyIdentity(
            requested_name="MiroMind.ai",
            canonical_name="MiroMind.ai",
            company_key="miromindai",
            linkedin_slug="miromind-ai",
            linkedin_company_url="https://www.linkedin.com/company/miromind-ai/",
        )
        dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "miromind_member_1",
                        "linkedinUrl": "https://www.linkedin.com/in/miromind-member-1/",
                        "firstName": "Mira",
                        "lastName": "Agent",
                        "summary": "Research Engineer at MiroMind.ai",
                        "currentPositions": [
                            {
                                "companyName": "MiroMind.ai",
                                "title": "Research Engineer",
                                "current": True,
                            }
                        ],
                        "location": {"linkedinText": "San Francisco Bay Area"},
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "previousElements": 0,
                                "pageSize": 25,
                            },
                            "query": {
                                "currentCompanies": ["https://www.linkedin.com/company/miromind-ai/"],
                                "locations": ["United States"],
                            },
                        },
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (shard_harvest_dir / "harvest_company_employees_queue_summary.json").write_text(
            json.dumps(
                {
                    "logical_name": "harvest_company_employees",
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {"locations": ["United States"]},
                    "artifact_paths": {"dataset_items": str(dataset_items_path)},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::miromindai::us_core",
            stage="acquiring",
            span_name="harvest_company_employees:miromindai:us_core",
            budget_payload={"max_pages": 1, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(shard_snapshot_dir),
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "company_filters": {"locations": ["United States"]},
                "max_pages": 1,
                "page_limit": 25,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {"locations": ["United States"]},
                    "snapshot_dir": str(shard_snapshot_dir),
                    "root_snapshot_dir": str(snapshot_dir),
                    "shard_id": "us_core",
                    "title": "United States",
                    "strategy_id": "small_org_roster",
                }
            },
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={
                    "status": "queued",
                    "requested_url_count": 1,
                    "dispatched_url_count": 1,
                    "cached_profile_count": 0,
                    "queued_worker_count": 1,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError("pre-retrieval profile completion must not run full snapshot sync"),
            ),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        self.assertEqual(refresh["status"], "completed")
        self.assertEqual(int(refresh["company_roster"]["added_entry_count"]), 1)
        self.assertEqual(
            int(dict(refresh["company_roster"].get("profile_prefetch") or {}).get("queued_worker_count") or 0),
            1,
        )
        candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertGreaterEqual(int(candidate_doc["candidate_count"]), 1)
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Mira Agent" for item in list(candidate_doc.get("candidates") or []))
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("company_roster", background_reconcile)
        self.assertEqual(int(background_reconcile["company_roster"]["applied_worker_count"]), 1)
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        latest_metrics = dict(dict(progress.get("progress") or {}).get("latest_metrics") or {})
        refresh_metrics = dict(latest_metrics.get("refresh_metrics") or {})
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("inline_company_roster_worker_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_company_roster_reconcile_count") or 0), 1)

    def test_refresh_running_workflow_before_retrieval_defers_materialization_only_refresh_for_preview(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_materialization_deferred"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-preview"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("preview refresh should not force a materialization-only sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                include_materialization_refresh=False,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "materialization_refresh_deferred")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        self.assertFalse(bool(dict(refreshed_job.get("summary") or {}).get("pre_retrieval_refresh")))

    def test_refresh_running_workflow_before_retrieval_skips_sync_when_preview_can_use_stage_candidate_documents(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI coding people",
            "target_company": "OpenAI",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Coding"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_preview_stage_candidate_docs"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-preview-stage-candidate-docs"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {"company_identity": identity.to_record()},
                    "candidates": [
                        Candidate(
                            candidate_id="openai-preview-base",
                            name_en="Preview Base",
                            display_name="Preview Base",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-preview-base/",
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        linkedin_stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        linkedin_stage_candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-preview-stage",
                            name_en="Preview Stage",
                            display_name="Preview Stage",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-preview-stage/",
                            metadata={"headline": "Engineer at OpenAI"},
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::preview-skip",
            stage="enriching",
            span_name="harvest_profile_batch:preview-skip",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-preview-stage/"]},
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("stage-1 preview should skip sync when stage candidate docs are ready"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_candidate_doc_path": linkedin_stage_candidate_doc_path,
                },
                include_materialization_refresh=False,
                allow_stage_candidate_document_fast_path=True,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "stage_candidate_documents_ready")
        self.assertEqual(refresh["snapshot_id"], snapshot_dir.name)
        self.assertEqual(int(refresh["harvest_prefetch_worker_count"] or 0), 1)
        self.assertEqual(refresh["stage_candidate_doc_path"], str(linkedin_stage_candidate_doc_path))

    def test_refresh_running_workflow_before_retrieval_defers_harvest_prefetch_for_direct_finalization(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI multimodal people",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["multimodal"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_direct_finalization_harvest_deferred"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-direct-finalization-harvest-deferred"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-direct-finalization",
                            name_en="Direct Finalization Candidate",
                            display_name="Direct Finalization Candidate",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Research Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-direct-finalization/",
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::direct-finalization",
            stage="enriching",
            span_name="harvest_profile_batch:direct-finalization",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-direct-finalization/"]},
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("direct finalization should not block on harvest-prefetch sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                include_materialization_refresh=False,
                defer_harvest_prefetch_refresh_to_background=True,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "harvest_prefetch_refresh_deferred_to_background")
        self.assertEqual(refresh["snapshot_id"], snapshot_dir.name)
        self.assertEqual(int(refresh["harvest_prefetch_worker_count"] or 0), 1)
        self.assertTrue(bool(refresh.get("materialization_refresh_pending")))
        self.assertTrue(bool(refresh.get("materialization_refresh_deferred")))

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_equivalent_baseline_reuse(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_equivalent_baseline_reuse"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-equivalent-baseline"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "normalized_artifacts").mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "normalized_artifacts" / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("equivalent baseline reuse should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "equivalent_baseline_reused")

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_reused_serving_snapshot(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_reused_serving_snapshot"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-pre-retrieval-reused-serving",
            candidates=[],
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="Reflection AI",
            include_strict=True,
            include_serving_docs=True,
        )
        stale_time = time.time() - 60
        fresh_time = time.time() + 60
        os.utime(candidate_doc_path, (fresh_time, fresh_time))
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
            snapshot_dir / "normalized_artifacts" / "reusable_candidate_documents.json",
        ):
            os.utime(path, (stale_time, stale_time))
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        materialized_candidate_doc_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("reused serving snapshot should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "reused_snapshot_serving_artifacts")

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_reused_snapshot_candidate_documents(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI post-train people",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Post-train"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_reused_snapshot_candidate_docs"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-pre-retrieval-reused-candidate-docs",
            candidates=[
                Candidate(
                    candidate_id="cand_reuse_checkpoint",
                    name_en="Reuse Checkpoint",
                    display_name="Reuse Checkpoint",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/reuse-checkpoint/",
                ).to_record()
            ],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("reused snapshot candidate documents should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "reused_snapshot_checkpoint")

    def test_refresh_running_workflow_before_retrieval_uses_snapshot_id_fallback_for_equivalent_baseline_reuse(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_equivalent_baseline_reuse_fallback"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-equivalent-baseline-fallback"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "xAI",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "normalized_artifacts").mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "normalized_artifacts" / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("equivalent baseline reuse should skip refresh when snapshot_id fallback matches"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "manifest_path": manifest_path,
                    "candidate_doc_path": candidate_doc_path,
                    "equivalent_baseline_reused": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "equivalent_baseline_reused")


if __name__ == "__main__":
    unittest.main()
