"""Acquisition-resume controller & readiness contracts — salvage wave group 1.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
group 1): `_resume_*_if_ready` / `_assess_acquisition_resume_readiness` and the
post-profile resume barriers (post_profile_local_apply_pending /
post_profile_board_visible_pending) had ZERO non-frozen coverage. Wave 1a ports the five contracts that hold
verbatim on the PG fixture (lease reclaim, three readiness barriers, terminal
worker predicate); the other six group-1 candidates fail with three distinct
modern-contract shifts (deferred materialization availability, async resume
completion, tick auto-resume rerouting) and stay frozen pending wave 1b
forensic calibration — see RECOVERY_BAND_OWNERSHIP_2026-07-22.md. Old->new
mapping in docs/governance/REGRESSION_INDEX.md; the freeze ratchet shrinks in
the same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.planning import build_sourcing_plan, hydrate_sourcing_plan
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.workflow_refresh import _worker_is_terminal_for_acquisition_resume
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AcquisitionResumeTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def test_resume_running_workflow_reclaims_stale_job_lease_when_runner_is_dead(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_resume_reclaims_stale_lease"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={
                "message": "runner died after acquisition worker completed",
                "runtime_controls": {
                    "workflow_runner": {
                        "status": "started",
                        "pid": 999999,
                        "process_alive": False,
                        "job_id": job_id,
                    },
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {
                            "status": "advanced",
                            "job_status": "running",
                            "job_stage": "acquiring",
                        },
                    },
                },
            },
        )
        lease = self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner="stale-runner-owner",
            lease_seconds=900,
            lease_token="stale-runner-token",
        )
        self.assertTrue(bool(lease.get("acquired")))

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_assess_acquisition_resume_readiness",
                return_value={
                    "status": "ready",
                    "request": request,
                    "plan": plan,
                    "baseline_ready": True,
                    "baseline_reason": "all_workers_completed",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_workflow_from_acquisition",
                return_value={"status": "completed"},
            ) as resume_mock,
        ):
            resume = self.orchestrator._resume_running_workflow_if_ready(job_id)

        self.assertEqual(resume["status"], "resumed")
        resume_mock.assert_called_once()
        self.assertIsNone(self.store.get_workflow_job_lease(job_id))
        recovery_events = [
            event
            for event in self.store.list_job_events(job_id, stage="runtime_control")
            if str(event.get("status") or "") == "recovered"
        ]
        self.assertTrue(recovery_events)

    def test_acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        plan = hydrate_sourcing_plan(plan_payload)
        job_id = "job_resume_readiness_background_roster"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-baseline-ready"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        summary_path = discovery_dir / "summary.json"
        entries_path = discovery_dir / "entries.json"
        entries_payload = [
            {
                "seed_key": "reflection-infra-01",
                "full_name": "Infra Builder",
                "headline": "Infrastructure Engineer",
                "source_type": "web_search",
                "source_query": "Reflection AI infra",
                "profile_url": "https://www.linkedin.com/in/infra-builder/",
            }
        ]
        summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [{"query": "Reflection AI infra", "status": "completed"}],
                    "queued_query_count": 0,
                    "errors": [],
                    "stop_reason": "",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        entries_path.write_text(json.dumps(entries_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries_payload,
            query_summaries=[{"query": "Reflection AI infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for background roster worker.",
                "blocked_task": "acquire_full_roster",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                        "company_identity": identity.to_record(),
                        "search_seed_snapshot": search_seed_snapshot.to_record(),
                    }
                },
            },
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::reflectionai",
            stage="acquiring",
            span_name="harvest_company_employees:Reflection AI",
            budget_payload={"max_pages": 100, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )

        job = self.store.get_job(job_id)
        assert job is not None
        workers = self.orchestrator.agent_runtime.list_workers(job_id=job_id)
        readiness = self.orchestrator._assess_acquisition_resume_readiness(
            job=job,
            blocked_task="acquire_full_roster",
            workers=workers,
        )

        self.assertEqual(readiness["status"], "ready")
        self.assertTrue(bool(readiness["baseline_ready"]))
        self.assertEqual(readiness["baseline_reason"], "search_seed_entries_present")
        self.assertEqual(int(readiness["pending_worker_count"] or 0), 1)
        self.assertEqual(len(list(readiness["pending_workers"] or [])), 1)
        self.assertEqual(
            next(
                task.task_type
                for task in list(plan.acquisition_tasks)
                if str(task.task_type or "") == "acquire_full_roster"
            ),
            "acquire_full_roster",
        )

    def test_acquisition_resume_readiness_waits_for_post_profile_local_apply(self) -> None:
        request_payload = {
            "raw_user_request": "Find Lovable members",
            "target_company": "Lovable",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        job_id = "job_resume_waits_for_post_profile_local_apply"
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-post-profile-barrier"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for profile detail.",
                "blocked_task": "enrich_linkedin_profiles",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                    }
                },
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="local_apply_profile_tail",
            job_id=job_id,
            target_company="Lovable",
            snapshot_id=snapshot_dir.name,
            item_kind="local_apply_closure",
            source="worker_completion_event",
            reason="provider_worker_completed_needs_local_apply_closure",
            status="queued",
            phase="queued",
            source_worker_ids=[7],
            metadata={"recovery_kind": "harvest_profile_batch"},
        )

        job = self.store.get_job(job_id)
        assert job is not None
        readiness = self.orchestrator._assess_acquisition_resume_readiness(
            job=job,
            blocked_task="enrich_linkedin_profiles",
            workers=[],
        )

        self.assertEqual(readiness["status"], "waiting")
        self.assertEqual(readiness["reason"], "post_profile_local_apply_pending")
        barrier = dict(readiness.get("post_profile_resume_barrier") or {})
        self.assertEqual(int(barrier.get("open_count") or 0), 1)
        self.assertEqual(dict(barrier.get("status_counts") or {}).get("queued"), 1)
        self.assertEqual(dict(barrier.get("item_kind_counts") or {}).get("local_apply_closure"), 1)

    def test_acquisition_resume_readiness_waits_for_post_profile_board_visible_apply(self) -> None:
        request_payload = {
            "raw_user_request": "Find Lovable members",
            "target_company": "Lovable",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        job_id = "job_resume_waits_for_post_profile_board_visible"
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-post-profile-board-visible-barrier"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for board-visible profile delta.",
                "blocked_task": "enrich_linkedin_profiles",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                    }
                },
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="board_visible_profile_tail",
            job_id=job_id,
            target_company="Lovable",
            snapshot_id=snapshot_dir.name,
            item_kind="board_visible_delta_apply",
            source="inline_harvest_prefetch_delta_control_plane_sync",
            reason="inline_background_harvest_prefetch_final_tail_board_visible",
            status="queued",
            phase="queued",
            source_worker_ids=[7],
            metadata={"recovery_kind": "harvest_profile_batch"},
        )

        job = self.store.get_job(job_id)
        assert job is not None
        readiness = self.orchestrator._assess_acquisition_resume_readiness(
            job=job,
            blocked_task="enrich_linkedin_profiles",
            workers=[],
        )

        self.assertEqual(readiness["status"], "waiting")
        self.assertEqual(readiness["reason"], "post_profile_board_visible_pending")
        barrier = dict(readiness.get("post_profile_resume_barrier") or {})
        self.assertEqual(int(barrier.get("open_count") or 0), 1)
        self.assertEqual(dict(barrier.get("status_counts") or {}).get("queued"), 1)
        self.assertEqual(dict(barrier.get("item_kind_counts") or {}).get("board_visible_delta_apply"), 1)

    def test_superseded_worker_is_terminal_for_acquisition_resume(self) -> None:
        self.assertTrue(_worker_is_terminal_for_acquisition_resume({"status": "superseded"}))


if __name__ == "__main__":
    unittest.main()
