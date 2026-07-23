"""Completed-workflow reconcile + open-work/lease-repair contracts — salvage waves 5+6.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
groups 5-6): completed-workflow background reconcile (pending-discovery on the
tick, exploration-results reconcile, completed-worker snapshot beating a stale
result view, harvest reconcile consumed-marker idempotency), the daemon-owned
open-work classifier boundaries (provider-owned refill tails excluded,
board-visible delta apply counted), and dead local worker/job lease preflight
repair had no modern coverage — the service-daemon suites stub the summaries
these classify. Ported verbatim onto the repo-standard PG fixture. Old->new
mapping in docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks in the
same change.
"""

import json
import os
import socket
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.durable_runtime import legacy_job_operation_id, legacy_job_workflow_run_id
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class CompletedReconcileAndOpenWorkTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def test_run_worker_recovery_once_discovers_completed_workflow_pending_background_reconcile(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_background_reconcile_scan"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={
                "background_reconcile": {
                    "harvest_prefetch": {
                        "status": "completed",
                        "last_worker_updated_at": "2026-04-11 05:40:00",
                    }
                }
            },
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=JobRequest.from_payload(request_payload),
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::scan-me",
            stage="enriching",
            span_name="harvest_profile_batch:scan-me",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/reflection-scan-me/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(self.settings.company_assets_dir / "reflectionai" / "snapshot-scan-me"),
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
            "_reconcile_completed_workflow_if_needed",
            return_value={"job_id": job_id, "status": "reconciled_harvest_prefetch"},
        ) as reconcile_mock:
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "workflow_auto_resume_enabled": False,
                    "workflow_queue_auto_takeover_enabled": False,
                }
            )

        self.assertEqual(recovery["status"], "completed")
        reconcile_mock.assert_called_once_with(job_id)
        self.assertEqual(
            list(recovery.get("post_completion_reconcile") or []),
            [{"job_id": job_id, "status": "reconciled_harvest_prefetch"}],
        )

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
        self.assertTrue(initial_artifact["summary"]["candidate_source"]["source_path"].endswith("manifest.json"))
        initial_results = self.orchestrator.get_job_results(job_id)
        assert initial_results is not None
        self.assertEqual(initial_results["job"]["summary"]["returned_matches"], 1)
        self.assertTrue(initial_results["asset_population"]["available"])
        self.assertEqual(len(initial_results["asset_population"]["candidates"]), 1)
        self.assertEqual(initial_results["asset_population"]["candidates"][0].get("focus_areas", ""), "")

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
                    evidence_id=make_evidence_id(
                        candidate.candidate_id, "exploration_summary", "Acme GPU systems", "https://example.com/acme"
                    ),
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

        self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = (
            _complete_background_exploration
        )
        try:
            recovery = self.orchestrator.run_worker_recovery_once({"job_id": job_id, "total_limit": 1})
        finally:
            self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = original_explore

        self.assertEqual(recovery["daemon"]["executed_count"], 1)
        self.assertEqual(recovery["post_completion_reconcile"][0]["status"], "reconciled")
        refreshed = self.orchestrator.get_job_results(job_id)
        assert refreshed is not None
        self.assertEqual(refreshed["job"]["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertTrue(refreshed["job"]["summary"]["candidate_source"]["source_path"].endswith("manifest.json"))
        self.assertEqual(refreshed["job"]["summary"]["background_reconcile"]["applied_worker_count"], 1)
        self.assertEqual(refreshed["job"]["summary"]["returned_matches"], 1)
        self.assertTrue(refreshed["asset_population"]["available"])
        self.assertEqual(refreshed["asset_population"]["candidates"][0].get("focus_areas"), "GPU systems")
        self.assertEqual(
            self.store.list_evidence(baseline_candidate.candidate_id)[0]["source_type"],
            "exploration_summary",
        )

    def test_reconcile_completed_workflow_prefers_completed_worker_snapshot_over_stale_result_view(self) -> None:
        request_payload = {
            "raw_user_request": "Find Meta agent researchers",
            "target_company": "Meta",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Agent"],
            "top_k": 10,
        }
        request = JobRequest.from_payload(request_payload)
        old_snapshot_id = "snapshot-meta-old-two"
        new_snapshot_id = "snapshot-meta-new-full"
        old_snapshot_dir = self.settings.company_assets_dir / "meta" / old_snapshot_id
        new_snapshot_dir = self.settings.company_assets_dir / "meta" / new_snapshot_id
        old_snapshot_dir.mkdir(parents=True, exist_ok=True)
        new_snapshot_dir.mkdir(parents=True, exist_ok=True)
        old_candidates = [
            Candidate(
                candidate_id=f"meta_old_{idx}",
                name_en=f"Old Meta {idx}",
                display_name=f"Old Meta {idx}",
                category="employee",
                target_company="Meta",
                organization="Meta",
                employment_status="current",
                role="Imported contact",
                linkedin_url=f"https://www.linkedin.com/in/old-meta-{idx}/",
            ).to_record()
            for idx in range(2)
        ]
        new_candidates = [
            Candidate(
                candidate_id=f"meta_new_{idx}",
                name_en=f"New Meta {idx}",
                display_name=f"New Meta {idx}",
                category="employee",
                target_company="Meta",
                organization="Meta",
                employment_status="current",
                role="Agent Researcher",
                focus_areas="Agent systems",
                linkedin_url=f"https://www.linkedin.com/in/new-meta-{idx}/",
            ).to_record()
            for idx in range(3)
        ]
        (old_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": old_candidates, "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (new_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": new_candidates, "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        plan_payload = {
            "organization_execution_profile": {
                "target_company": "Meta",
                "asset_view": "canonical_merged",
                "source_snapshot_id": new_snapshot_id,
                "source_generation_key": "gen_meta_new",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 3,
            },
        }
        job_id = "job_harvest_prefetch_reconcile_prefers_worker_snapshot"
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
                "analysis_stage": "stage_2_final",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": old_snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 2,
                    "source_path": str(old_snapshot_dir / "candidate_documents.json"),
                },
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Meta",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id=old_snapshot_id,
            source_path=str(old_snapshot_dir / "candidate_documents.json"),
            authoritative_snapshot_id=old_snapshot_id,
            summary={"candidate_count": 2},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::meta_new",
            stage="enriching",
            span_name="harvest_profile_batch:meta_new",
            budget_payload={"requested_url_count": 3},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/new-meta-0/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(new_snapshot_dir),
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

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_handle_harvest_profile_completion_event",
                return_value={"profile_prefetch": {"status": "skipped", "reason": "test"}},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_harvest_prefetch_workers_to_snapshot",
                return_value={"status": "applied", "worker_ids": [worker_handle.worker_id], "candidate_ids": []},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                return_value={"status": "completed", "candidate_count": 3, "evidence_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_harvest_prefetch")
        self.assertEqual(reconcile["snapshot_id"], new_snapshot_id)
        result_view = self.store.get_job_result_view(job_id=job_id)
        assert result_view is not None
        self.assertEqual(result_view["snapshot_id"], new_snapshot_id)
        served_candidate_count = int(dict(result_view.get("summary") or {}).get("candidate_count") or 0)
        self.assertGreater(served_candidate_count, 2)
        lifecycle = self.store.get_job_result_lifecycle(job_id)
        assert lifecycle is not None
        self.assertEqual(lifecycle["served_snapshot_id"], new_snapshot_id)
        self.assertEqual(lifecycle["served_candidate_count"], served_candidate_count)
        self.assertEqual(lifecycle["serving_projection_phase"], "current_snapshot_serving")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        candidate_source = dict(dict(refreshed_job.get("summary") or {}).get("candidate_source") or {})
        self.assertEqual(candidate_source["snapshot_id"], new_snapshot_id)
        self.assertGreater(int(candidate_source["candidate_count"]), 2)

    def test_job_scoped_open_work_does_not_treat_provider_owned_profile_refill_tail_as_daemon_owned(self) -> None:
        job_id = "job_open_work_provider_owned_profile_refill"
        snapshot_dir = "/tmp/snapshot-provider-owned-profile-refill"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={},
            plan_payload={},
            summary_payload={},
        )
        lease = self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner="test-running-owner",
            lease_seconds=900,
            lease_token="test-running-owner-token",
        )
        self.assertTrue(bool(lease.get("acquired")))
        self.store.repos.linkedin_profile_registry.mark_queued(
            "https://www.linkedin.com/in/provider-owned-tail/",
            source_jobs=[job_id],
            run_id="run-provider-owned",
            dataset_id="dataset-provider-owned",
            snapshot_dir=snapshot_dir,
        )
        self.store.repos.linkedin_profile_registry.record_refill_plan_items(
            active_profile_urls=["https://www.linkedin.com/in/provider-owned-tail/"],
            source_jobs=[job_id],
            snapshot_dir=snapshot_dir,
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
            active_owner_worker_id=123,
            active_owner_run_id="run-provider-owned",
            active_owner_dataset_id="dataset-provider-owned",
            active_owner_payload_hash="provider-owned-payload",
        )
        self.store.repos.linkedin_profile_registry.mark_queued(
            "https://www.linkedin.com/in/actionable-tail/",
            source_jobs=[job_id],
            snapshot_dir=snapshot_dir,
        )
        self.store.repos.linkedin_profile_registry.record_refill_plan_items(
            deferred_profile_urls=["https://www.linkedin.com/in/actionable-tail/"],
            source_jobs=[job_id],
            snapshot_dir=snapshot_dir,
            trigger_kind="profile_prefetch_refill",
            plan_reason="worker_budget_deferred",
            deferred_reason="worker_budget_deferred",
            deferred_queue_state="deferred_budget",
        )

        summary = self.orchestrator._job_scoped_recovery_open_work_summary(job_id=job_id)

        self.assertEqual(summary["profile_refill_open_item_count"], 2)
        self.assertEqual(summary["profile_refill_ready_item_count"], 1)
        self.assertEqual(summary["profile_refill_state_counts"]["planned_dispatch"], 1)
        self.assertEqual(summary["profile_refill_state_counts"]["deferred_budget"], 1)
        self.assertEqual(summary["pending_worker_count"], 0)
        self.assertEqual(summary["workflow_open_count"], 1)
        self.assertTrue(summary["workflow_lease_alive"])
        self.assertFalse(summary["workflow_resume_actionable"])
        self.assertEqual(summary["daemon_owned_open_work_count"], 1)

    def test_job_scoped_open_work_excludes_provider_owned_only_profile_refill_tail_from_daemon_owned(self) -> None:
        job_id = "job_open_work_provider_owned_only_profile_refill"
        snapshot_dir = "/tmp/snapshot-provider-owned-only-profile-refill"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={},
            plan_payload={},
            summary_payload={},
        )
        self.store.repos.workflow_runtime.upsert_workflow_current_state(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            operation_id=legacy_job_operation_id(job_id),
            status="completed",
            current_stage_key="serving_finalized",
            completion_proofs={
                "serving_finalized": {
                    "status": "proved",
                    "event_id": "evt_provider_owned_tail_serving_finalized",
                    "sequence_number": 1,
                }
            },
            last_processed_sequence_number=1,
        )
        self.store.repos.linkedin_profile_registry.mark_queued(
            "https://www.linkedin.com/in/provider-owned-only-tail/",
            source_jobs=[job_id],
            run_id="run-provider-owned-only",
            dataset_id="dataset-provider-owned-only",
            snapshot_dir=snapshot_dir,
        )
        self.store.repos.linkedin_profile_registry.record_refill_plan_items(
            active_profile_urls=["https://www.linkedin.com/in/provider-owned-only-tail/"],
            source_jobs=[job_id],
            snapshot_dir=snapshot_dir,
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
            active_owner_worker_id=124,
            active_owner_run_id="run-provider-owned-only",
            active_owner_dataset_id="dataset-provider-owned-only",
            active_owner_payload_hash="provider-owned-only-payload",
        )

        summary = self.orchestrator._job_scoped_recovery_open_work_summary(job_id=job_id)

        self.assertEqual(summary["profile_refill_open_item_count"], 1)
        self.assertEqual(summary["profile_refill_ready_item_count"], 0)
        self.assertEqual(summary["profile_refill_state_counts"]["planned_dispatch"], 1)
        self.assertEqual(summary["workflow_open_count"], 0)
        self.assertEqual(summary["daemon_owned_open_work_count"], 0)
        self.assertEqual(summary["non_daemon_open_work_count"], 1)

    def test_recovery_preflight_repairs_dead_local_worker_and_job_leases(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_repair_dead_local_recovery_leases"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-dead-local-repair"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={},
            artifact_path=str(snapshot_dir),
        )
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::dead-local-repair",
            stage="enriching",
            span_name="harvest_profile_batch:dead-local-repair",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-dead-local-repair/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/openai-dead-local-repair/"],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        dead_worker_owner = f"worker-recovery-daemon-{socket.gethostname()}-999999"
        self.store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={"stage": "waiting_remote_harvest", "summary_path": str(snapshot_dir / "summary.json")},
            output_payload={},
            status="queued",
        )
        self.store.claim_agent_worker(
            handle.worker_id,
            lease_owner=dead_worker_owner,
            lease_seconds=900,
        )
        dead_job_owner = f"{socket.gethostname()}:999999:111"
        self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner=dead_job_owner,
            lease_seconds=900,
            lease_token="dead-local-repair-token",
        )

        result = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "search_seed_discovery_enabled": False,
                "profile_prefetch_refill_enabled": False,
                "snapshot_full_materialization_enabled": False,
                "excel_intake_recovery_enabled": False,
                "post_recovery_housekeeping_enabled": False,
                "post_completion_reconcile_enabled": False,
            }
        )

        repair = dict(result.get("dead_local_recovery_lease_repair") or {})
        self.assertEqual(repair["status"], "active")
        self.assertEqual(repair["worker_lease_released_count"], 1)
        self.assertEqual(repair["workflow_job_lease_released_count"], 1)
        self.assertEqual(self.store.get_agent_worker(worker_id=handle.worker_id)["lease_owner"], "")
        self.assertIsNone(self.store.get_workflow_job_lease(job_id))
        events = [
            event
            for event in self.store.list_job_events(job_id, stage="runtime_control")
            if dict(event.get("payload") or {}).get("event_family") == "dead_local_recovery_lease_repair"
        ]
        self.assertGreaterEqual(len(events), 2)


if __name__ == "__main__":
    unittest.main()
