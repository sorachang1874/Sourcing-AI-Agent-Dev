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
import time
from datetime import datetime, timezone
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

    def _settle_completed(self, job_id: str) -> dict:
        stored_job = self.store.get_job(job_id)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            stored_job = self.store.get_job(job_id)
            if str((stored_job or {}).get("status")) == "completed":
                break
            time.sleep(0.2)
        assert stored_job is not None
        self.assertEqual(stored_job["status"], "completed")
        return stored_job

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
            output_payload={
                "summary": {"query": "xAI former employee", "status": "completed"},
                "entries": [],
                "errors": [],
            },
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
                    evidence_id=make_evidence_id(
                        "cand_resume_1", "seed", "xAI former employee", "https://example.com/xai"
                    ),
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
        # CALIBRATED 2026-07-22 (wave 1b, RECOVERY_BAND_OWNERSHIP recipe): the
        # terminal flip is completion-policy gated and asynchronous now — the
        # resume call reports running while the policy verifies the produced
        # results, then the store settles to completed. Poll the durable row
        # instead of expecting an inline terminal payload.
        self.assertEqual(resume["job_status"], "running")
        stored_job = self.store.get_job(job_id)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            stored_job = self.store.get_job(job_id)
            if str((stored_job or {}).get("status")) == "completed":
                break
            time.sleep(0.2)
        assert stored_job is not None
        self.assertEqual(stored_job["status"], "completed")
        acquisition_progress = dict(dict(stored_job.get("summary") or {}).get("acquisition_progress") or {})
        self.assertEqual(acquisition_progress.get("status"), "completed")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertGreaterEqual(len(snapshot["results"]), 1)

    def test_resume_running_planning_workflow_continues_from_acquisition_when_planning_completed(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee", "former_employee"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        job_id = "job_resume_running_planning"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Workflow is running"},
        )
        self.store.append_job_event(job_id, "planning", "queued", "Workflow created from user request.")
        self.store.append_job_event(job_id, "planning", "completed", "Planning stage completed.")

        original_run_from_acquisition = self.orchestrator._run_workflow_from_acquisition
        captured: dict[str, object] = {}

        # Calibrated 2026-07-22: the real method grew assume_job_run_lock
        # (Phase 4 lock threading); absorb future keyword growth.
        def fake_run_from_acquisition(job_id_arg, request_arg, plan_arg, *, resume_mode=False, **_kwargs):
            captured["job_id"] = job_id_arg
            captured["resume_mode"] = resume_mode
            captured["request"] = request_arg
            captured["plan"] = plan_arg
            self.store.save_job(
                job_id=job_id_arg,
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload=request_arg.to_record(),
                plan_payload=plan_arg.to_record(),
                summary_payload={"message": "Recovered from stale planning runner."},
            )
            return {"status": "completed"}

        self.orchestrator._run_workflow_from_acquisition = fake_run_from_acquisition
        try:
            resume = self.orchestrator._resume_planning_workflow_if_ready(job_id)
        finally:
            self.orchestrator._run_workflow_from_acquisition = original_run_from_acquisition

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["resume_mode"], "running_planning_to_acquisition_recovery")
        self.assertTrue(bool(resume["planning_completed"]))
        self.assertEqual(captured["job_id"], job_id)
        self.assertTrue(bool(captured["resume_mode"]))
        self.assertIsNotNone(snapshot)
        assert snapshot is not None

    def test_resume_running_workflow_skips_completed_acquisition_tasks_and_defers_materialization(self) -> None:
        request_payload = {
            "raw_user_request": "Find Acme infra researchers",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 1,
            "analysis_stage_mode": "two_stage",
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_resume_checkpoint_skip"
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260410T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        manifest_path = snapshot_dir / "manifest.json"
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="cand_resume_checkpoint",
            name_en="Alice Infra",
            display_name="Alice Infra",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Research Engineer",
            focus_areas="infra systems",
            linkedin_url="https://www.linkedin.com/in/alice-infra",
            source_dataset="checkpoint_test",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "checkpoint_test",
                "Acme infra",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Acme infra",
            url=candidate.linkedin_url,
            summary="Checkpoint recovery candidate",
            source_dataset="checkpoint_test",
            source_path=str(candidate_doc_path),
        )
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence.to_record()],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "company_identity": identity.to_record(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Workflow runner died during acquisition"},
        )

        tasks_by_type = {task.task_type: task for task in plan.acquisition_tasks}
        acquisition_state = {
            "snapshot_id": snapshot_dir.name,
            "snapshot_dir": snapshot_dir,
            "company_identity": identity,
        }
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["resolve_company_identity"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["resolve_company_identity"].task_id,
                status="completed",
                detail="Resolved company identity.",
                payload={"snapshot_dir": str(snapshot_dir)},
                state_updates=dict(acquisition_state),
            ),
            acquisition_state=dict(acquisition_state),
        )
        acquisition_state["candidates"] = [candidate]
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["acquire_full_roster"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["acquire_full_roster"].task_id,
                status="completed",
                detail="Acquired roster.",
                payload={"candidate_count": 1},
                state_updates={"candidates": [candidate]},
            ),
            acquisition_state=dict(acquisition_state),
        )
        acquisition_state["candidate_doc_path"] = candidate_doc_path
        acquisition_state["evidence"] = [evidence]
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["enrich_linkedin_profiles"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["enrich_linkedin_profiles"].task_id,
                status="completed",
                detail="Enriched LinkedIn profiles.",
                payload={"candidate_doc_path": str(candidate_doc_path)},
                state_updates={
                    "candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_completed": True,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
            ),
            acquisition_state=dict(acquisition_state),
        )
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["enrich_public_web_signals"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["enrich_public_web_signals"].task_id,
                status="completed",
                detail="Enriched public-web signals.",
                payload={"candidate_doc_path": str(candidate_doc_path)},
                state_updates={
                    "candidate_doc_path": candidate_doc_path,
                    "public_web_stage_candidate_doc_path": candidate_doc_path,
                    "public_web_stage_completed": True,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
            ),
            acquisition_state={
                **dict(acquisition_state),
                "candidate_doc_path": candidate_doc_path,
                "linkedin_stage_candidate_doc_path": candidate_doc_path,
                "linkedin_stage_completed": True,
            },
        )

        executed_task_types: list[str] = []
        original_execute_task = self.acquisition_engine.execute_task
        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ):

                def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):
                    executed_task_types.append(task.task_type)
                    self.assertIn(task.task_type, {"normalize_asset_snapshot", "build_retrieval_index"})
                    self.assertEqual(str(state.get("candidate_doc_path")), str(candidate_doc_path))
                    self.assertEqual(len(list(state.get("candidates") or [])), 1)
                    self.assertEqual(len(list(state.get("evidence") or [])), 1)
                    if task.task_type == "normalize_asset_snapshot":
                        manifest_path.write_text(
                            json.dumps(
                                {
                                    "snapshot_id": snapshot_dir.name,
                                    "company_identity": identity.to_record(),
                                },
                                ensure_ascii=False,
                                indent=2,
                            )
                        )
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="completed",
                            detail="Normalized snapshot.",
                            payload={"manifest_path": str(manifest_path)},
                            state_updates={"manifest_path": manifest_path},
                        )
                    (snapshot_dir / "retrieval_index_summary.json").write_text(
                        json.dumps({"status": "built"}, ensure_ascii=False, indent=2)
                    )
                    return AcquisitionExecution(
                        task_id=task.task_id,
                        status="completed",
                        detail="Built retrieval index.",
                        payload={"retrieval_index_summary": str(snapshot_dir / "retrieval_index_summary.json")},
                        state_updates={},
                    )

                self.acquisition_engine.execute_task = fake_execute_task
                resume = self.orchestrator._resume_running_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        self.assertEqual(resume["status"], "resumed")
        # CALIBRATED 2026-07-22 (wave 1b recipe): the all-tasks-skipped resume
        # completes inline when nothing re-executes; the settle-poll tolerates
        # either the inline or the async policy-gated flavor.
        self.assertIn(resume["job_status"], {"running", "completed"})
        stored_job = self._settle_completed(job_id)
        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(executed_task_types, [])
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        skipped_task_ids = {
            str((event.get("payload") or {}).get("task_id") or "")
            for event in list(snapshot["events"])
            if str(event.get("status") or "") == "skipped"
        }
        expected_skipped = {
            tasks_by_type[task_type].task_id
            for task_type in (
                "resolve_company_identity",
                "acquire_full_roster",
                "enrich_linkedin_profiles",
                "enrich_public_web_signals",
                "normalize_asset_snapshot",
                "build_retrieval_index",
            )
            # public-web stage 2 is a two_stage/require_stage2_confirmation
            # opt-in since the planner evolution (R-035 finding).
            if task_type in tasks_by_type
        }
        self.assertTrue(expected_skipped.issubset(skipped_task_ids))
        # CALIBRATED 2026-07-22 (wave 1b mechanism 1): deferred materialization
        # means asset-population availability arrives with the background
        # materialization pass, not inline with completion — drive one
        # recovery tick to run it, then assert availability.
        asset_population = dict(snapshot.get("asset_population") or {})
        if not asset_population.get("available"):
            self.orchestrator.run_worker_recovery_once({"explicit_job_id": job_id})
            refreshed = self.orchestrator.get_job_results(job_id)
            asset_population = dict((refreshed or {}).get("asset_population") or {})
        self.assertTrue(asset_population.get("available"))
        self.assertGreaterEqual(int(asset_population.get("candidate_count") or 0), 1)


    # -- salvage wave 8 (2026-07-22): hosted acquisition-resume dispatch
    # dedupe/lease contracts (detached runner, marker dedupe, lease-alive
    # skip, fresh-dispatch skip in progress auto-recovery, no inline resume
    # under the job lock) — same work-list, same fixture.

    def test_resume_queued_hosted_workflow_dispatches_without_holding_job_lock(self) -> None:
        job_id = "job_resume_queued_hosted_no_outer_lock"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "Workflow queued", "runtime_execution_mode": "hosted"},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_hosted_workflow_thread",
                return_value={"job_id": job_id, "status": "started", "mode": "workflow", "source": "workflow_recovery"},
            ) as hosted_mock,
            unittest.mock.patch.object(self.orchestrator, "_job_run_lock") as lock_mock,
        ):
            result = self.orchestrator._resume_queued_workflow_if_ready(job_id)

        hosted_mock.assert_called_once_with(job_id, source="workflow_recovery")
        lock_mock.assert_not_called()
        self.assertEqual(result["status"], "takeover_started")
        self.assertEqual(result["mode"], "hosted")

    def test_resume_acquiring_hosted_workflow_dispatches_thread_without_inline_resume(self) -> None:
        job_id = "job_resume_acquiring_hosted"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload={"target_company": "OpenAI"},
            plan_payload={},
            summary_payload={
                "message": "waiting for profile detail",
                "runtime_execution_mode": "hosted",
                "blocked_task": "enrich_linkedin_profiles",
            },
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_hosted_acquisition_resume_thread",
                return_value={
                    "job_id": job_id,
                    "status": "started",
                    "mode": "acquisition_resume",
                    "source": "workflow_recovery",
                },
            ) as hosted_mock,
            unittest.mock.patch.object(self.orchestrator, "_job_run_lock") as lock_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_workflow_from_acquisition",
                side_effect=AssertionError("recovery tick must not run workflow inline"),
            ),
        ):
            result = self.orchestrator._resume_acquiring_workflow_if_ready(job_id)

        hosted_mock.assert_called_once_with(job_id, source="workflow_recovery")
        lock_mock.assert_not_called()
        self.assertEqual(result["status"], "takeover_started")
        self.assertEqual(result["mode"], "hosted")

    def test_start_hosted_acquisition_resume_thread_dispatches_detached_runner_and_dedupes_marker(self) -> None:
        job_id = "job_hosted_acquisition_resume_deduped"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload={"target_company": "OpenAI"},
            plan_payload={},
            summary_payload={
                "message": "waiting for profile detail",
                "runtime_execution_mode": "hosted",
                "blocked_task": "enrich_linkedin_profiles",
            },
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_spawn_workflow_takeover_runner",
            return_value={"status": "started", "pid": 24680, "log_path": "/tmp/workflow.log"},
        ) as spawn_mock:
            first = self.orchestrator._start_hosted_acquisition_resume_thread(job_id, source="workflow_recovery")
            second = self.orchestrator._start_hosted_acquisition_resume_thread(job_id, source="progress_poll")

        self.assertEqual(first["status"], "started")
        self.assertEqual(first["mode"], "acquisition_resume")
        self.assertEqual(first["dispatch_kind"], "detached_execute_workflow")
        self.assertEqual(second["status"], "skipped")
        self.assertEqual(second["reason"], "hosted_dispatch_inflight")
        spawn_mock.assert_called_once_with(job_id, auto_job_daemon=False)

        stored_job = self.store.get_job(job_id) or {}
        hosted_dispatch = dict(dict(stored_job.get("summary") or {}).get("hosted_dispatch") or {})
        self.assertEqual(hosted_dispatch.get("mode"), "acquisition_resume")
        self.assertEqual(hosted_dispatch.get("source"), "workflow_recovery")
        self.assertEqual(hosted_dispatch.get("dispatch_kind"), "detached_execute_workflow")
        self.assertEqual(hosted_dispatch.get("pid"), 24680)

    def test_start_hosted_acquisition_resume_thread_skips_when_workflow_lease_alive(self) -> None:
        job_id = "job_hosted_acquisition_resume_lease_inflight"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload={"target_company": "OpenAI"},
            plan_payload={},
            summary_payload={
                "message": "waiting for profile detail",
                "runtime_execution_mode": "hosted",
                "blocked_task": "enrich_linkedin_profiles",
                "hosted_dispatch": {
                    "status": "started",
                    "mode": "acquisition_resume",
                    "source": "workflow_recovery",
                    "dispatched_at": "2000-01-01T00:00:00+00:00",
                },
            },
        )
        self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner=self.orchestrator._workflow_job_lease_owner(),  # noqa: SLF001
            lease_seconds=120,
            lease_token="lease-token",
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_spawn_workflow_takeover_runner",
            side_effect=AssertionError("active workflow lease must not spawn another runner"),
        ):
            result = self.orchestrator._start_hosted_acquisition_resume_thread(job_id, source="progress_poll")

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "workflow_job_lease_inflight")

    def test_progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job(self) -> None:
        job_id = "job_hosted_dispatch_progress_guard"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "MiroMind.ai"},
            plan_payload={},
            summary_payload={
                "message": "Workflow queued",
                "runtime_execution_mode": "hosted",
                "hosted_dispatch": {
                    "status": "started",
                    "mode": "workflow",
                    "source": "start_workflow",
                    "dispatched_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        )

        job = self.store.get_job(job_id) or {}
        result = self.orchestrator._maybe_auto_recover_workflow_on_progress(
            job=job,
            events=[],
            workers=[],
            runtime_controls={},
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "runtime_not_takeover_eligible")
        self.assertEqual(result["classification"], "hosted_dispatch_inflight")

    def test_resume_blocked_workflow_ignores_pending_exploration_workers(self) -> None:
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
            summary_payload={
                "message": "Waiting for queued exploration workers",
                "blocked_task": "enrich_profiles_multisource",
            },
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
                "candidate": {
                    "candidate_id": "candidate::queued_exploration",
                    "display_name": "Queued Exploration Lead",
                },
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

        # CALIBRATED 2026-07-22 (deep forensics): the modern completion gate
        # fail-closes on the missing durable serving-finalized proof
        # (anti-spoofing: a completed-looking job without a durable run is
        # non-terminal). The original inline-completion tail is replaced by
        # pinning BOTH halves of the modern contract: the resume ignored the
        # pending exploration worker, and the terminal flip is gated on
        # serving_finalized_proof_missing.
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["job_status"], "running")
        worker = self.orchestrator.agent_runtime.get_worker(handle.worker_id)
        assert worker is not None
        self.assertEqual(str(worker.get("status") or ""), "running")
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={"summary": {"status": "completed"}, "entries": [], "errors": []},
        )
        self.orchestrator.run_worker_recovery_once({"explicit_job_id": job_id})
        stored_job = self.store.get_job(job_id)
        assert stored_job is not None
        blockers = self.orchestrator._workflow_completion_promotion_blockers(stored_job)
        self.assertEqual(str(blockers.get("reason") or ""), "serving_finalized_proof_missing")

if __name__ == "__main__":
    unittest.main()
