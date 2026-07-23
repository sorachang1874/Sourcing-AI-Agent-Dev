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
from sourcing_agent.connectors import CompanyIdentity
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


    def test_completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI audio people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["audio"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_harvest_reconcile_consumes_once"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-completed-harvest-reconcile-once"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/openai-completed-reconcile/"
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": snapshot_dir.name, "target_company": "OpenAI"},
                    "candidates": [
                        Candidate(
                            candidate_id="openai-completed-reconcile",
                            name_en="Completed Reconcile",
                            display_name="Completed Reconcile",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Audio Engineer",
                            linkedin_url=profile_url,
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
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "completed",
                    "request": request.to_record(),
                    "plan": plan_payload,
                    "summary": {
                        "analysis_stage": "stage_2_final",
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": snapshot_dir.name,
                            "candidate_count": 1,
                        },
                    },
                    "matches": [],
                    "manual_review_items": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "analysis_stage": "stage_2_final",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "candidate_count": 1,
                },
            },
            artifact_path=str(artifact_path),
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::completed-reconcile",
            stage="enriching",
            span_name="harvest_profile_batch:completed-reconcile",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )

        apply_result = {
            "status": "applied",
            "snapshot_id": snapshot_dir.name,
            "worker_ids": [worker.worker_id],
            "candidate_ids": ["openai-completed-reconcile"],
        }
        sync_result = {
            "status": "completed",
            "reason": "background_harvest_prefetch_reconcile",
            "snapshot_id": snapshot_dir.name,
            "candidate_count": 1,
            "evidence_count": 1,
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {},
            "state_updates": {
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
            },
        }
        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_handle_harvest_profile_completion_event",
                return_value={"profile_prefetch": {"status": "completed", "dispatched_url_count": 0}},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_harvest_prefetch_workers_to_snapshot",
                return_value=apply_result,
            ) as apply_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value=sync_result,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "skipped", "reason": "test"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"artifact_path": str(artifact_path), "status": "completed"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_outreach_layering_requires_background_reconcile",
                return_value=False,
            ),
        ):
            first = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)
            second = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(str(first.get("status") or ""), "reconciled_harvest_prefetch")
        # CALIBRATED 2026-07-22 (waves 5+6 forensics): the second invocation
        # now reports the settled state ("completed") instead of the old
        # skip-reason vocabulary; idempotency is proven by the single apply
        # call and the consumed marker below (probe: apply_call_count == 1).
        self.assertEqual(str(second.get("status") or ""), "completed")
        apply_mock.assert_called_once()
        refreshed_worker = self.store.get_agent_worker(worker_id=worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("worker_kind") or ""), "harvest_prefetch")
        self.assertEqual(str(inline_ingest.get("sync_status") or ""), "completed")
        self.assertEqual([int(item) for item in list(inline_ingest.get("applied_worker_ids") or [])], [worker.worker_id])
        structured_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "completed_workflow_reconcile"
        ]
        harvest_events = [
            event for event in structured_events if str(event.get("reconcile_kind") or "") == "harvest_prefetch"
        ]
        # CALIBRATED 2026-07-22: the harvest_prefetch reconcile now narrates
        # the delta-serving contract (started -> profile_delta_serving_started
        # -> profile_delta_served -> completed) and materialization moved to
        # its own snapshot_materialization reconcile_kind.
        harvest_phases = {str(event.get("phase") or "") for event in harvest_events}
        self.assertIn("started", harvest_phases)
        self.assertIn("profile_delta_served", harvest_phases)
        self.assertIn("completed", harvest_phases)
        self.assertTrue(
            any(
                str(event.get("reconcile_kind") or "") == "snapshot_materialization"
                for event in structured_events
            )
        )


    # -- shard-B port groups B+C (2026-07-22): outreach-layering reconcile
    # retry/continue loop + the per-kind completed-workflow reconcile drains
    # (reconciled_search_seed / reconciled_company_roster, no-candidate-delta
    # skip) — durable-command-surface calibration risk was flagged; ported
    # verbatim first.

    def test_background_outreach_layering_reconcile_retries_completion_lease_inflight(self) -> None:
        with (
            unittest.mock.patch.dict(
                os.environ,
                {
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_MAX_ATTEMPTS": "3",
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_RETRY_SECONDS": "0",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflow_if_needed",
                side_effect=[
                    {
                        "job_id": "job_outreach_layering_retry",
                        "status": "skipped",
                        "reason": "completed_workflow_reconcile_inflight",
                    },
                    {
                        "job_id": "job_outreach_layering_retry",
                        "status": "reconciled_outreach_layering",
                    },
                ],
            ) as reconcile,
        ):
            result = self.orchestrator._run_background_outreach_layering_reconcile(
                job_id="job_outreach_layering_retry",
                source="workflow_completion",
            )

        self.assertEqual(result["status"], "reconciled_outreach_layering")
        self.assertEqual(result["attempt_count"], 2)
        self.assertTrue(result["background_retry"])
        self.assertEqual(reconcile.call_count, 2)

    def test_background_outreach_layering_reconcile_continues_after_adjacent_reconcile(self) -> None:
        job_id = "job_outreach_layering_after_adjacent_reconcile"
        request_payload = {
            "raw_user_request": "Find current and former researchers",
            "target_company": "Acme",
            "target_scope": "full_company_asset",
        }
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request_payload,
            plan_payload={},
            summary_payload={
                "candidate_source": {"snapshot_id": "snapshot-adjacent-reconcile"},
                "outreach_layering": {
                    "status": "scheduled",
                    "snapshot_id": "snapshot-adjacent-reconcile",
                    "reason": "deferred_for_asset_population_fast_path",
                },
            },
            artifact_path="",
        )
        initial_event_count = len(self.store.list_job_events(job_id))
        with (
            unittest.mock.patch.dict(
                os.environ,
                {
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_MAX_ATTEMPTS": "3",
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_RETRY_SECONDS": "0",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflow_if_needed",
                side_effect=[
                    {"job_id": job_id, "status": "reconciled_harvest_prefetch"},
                    {"job_id": job_id, "status": "reconciled_outreach_layering"},
                ],
            ) as reconcile,
        ):
            result = self.orchestrator._run_background_outreach_layering_reconcile(
                job_id=job_id,
                source="workflow_completion",
            )

        self.assertEqual(result["status"], "reconciled_outreach_layering")
        self.assertEqual(result["attempt_count"], 2)
        self.assertTrue(result["background_retry"])
        self.assertEqual(reconcile.call_count, 2)
        retry_events = [
            event
            for event in self.store.list_job_events(job_id)[initial_event_count:]
            if str(dict(event.get("payload") or {}).get("retry_reason") or "")
            == "background_outreach_layering_pending_after_adjacent_reconcile"
        ]
        self.assertEqual(len(retry_events), 1)

    def test_reconcile_completed_workflow_after_background_search_seed(self) -> None:
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
        job_id = "job_search_seed_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-reconcile"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
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
                    "query_summaries": [
                        {
                            "query": "Reflection AI infra",
                            "bundle_id": "bundle-infra",
                            "source_family": "people_search",
                            "execution_mode": "web_search",
                            "mode": "web_search",
                            "status": "queued",
                            "seed_entry_count": 0,
                        }
                    ],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "queued_background_search",
                    "queued_query_count": 1,
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_apply_background_search_seed_workers_to_snapshot",
            side_effect=AssertionError("completed workflow search-seed worker-summary merge is retired"),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(str(reconcile.get("status") or ""), "skipped")
        self.assertEqual(
            str(reconcile.get("reason") or ""),
            "search_seed_reconcile_requires_durable_local_apply_closure_item",
        )
        self.assertEqual(int(reconcile.get("local_apply_closure_item_count") or 0), 0)
        self.assertEqual(int(reconcile.get("search_seed_discovery_item_count") or 0), 0)
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "search_seed"
        )
        self.assertFalse(search_reconcile)
        updated_entries = json.loads((discovery_dir / "entries.json").read_text())
        self.assertEqual(len(updated_entries), 1)
        updated_summary = json.loads((discovery_dir / "summary.json").read_text())
        self.assertEqual(int(updated_summary["queued_query_count"]), 1)
        candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertEqual(int(candidate_doc["candidate_count"]), 1)
        structured_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "completed_workflow_reconcile"
        ]
        self.assertTrue(
            any(str(event.get("phase") or "") == "worker_summary_merge_retired" for event in structured_events)
        )

    def test_completed_search_seed_no_candidate_delta_skips_prefetch_and_materialize(self) -> None:
        request_payload = {
            "raw_user_request": "帮我找OpenAI做Infra方向的人",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_no_candidate_delta"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-no-delta"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        baseline_entry = {
            "seed_key": "baseline-infra",
            "full_name": "Baseline Infra",
            "source_type": "harvest_profile_search",
            "source_query": "OpenAI Infra",
            "profile_url": "https://www.linkedin.com/in/baseline-infra/",
        }
        (discovery_dir / "entries.json").write_text(
            json.dumps([baseline_entry], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "provider_people_search_fallback",
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
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-infra-candidate",
                            name_en="Baseline Infra",
                            display_name="Baseline Infra",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Infrastructure Engineer",
                            linkedin_url="https://www.linkedin.com/in/baseline-infra/",
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
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "source_path": str(candidate_doc_path),
                    "candidate_count": 1,
                },
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="former::seed_queries::01",
            stage="acquiring",
            span_name="search_bundle:former-seed",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "Infra"}},
            metadata={
                "recovery_kind": "search_seed_discovery",
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
            checkpoint_payload={
                "stage": "completed",
                "recovery_kind": "search_seed_discovery",
                "provider_name": "dataforseo_google_organic",
            },
            output_payload={
                "summary": {
                    "query": "Infra",
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "result_count": 9,
                    "linkedin_result_count": 0,
                    "seed_entry_count": 0,
                },
                "entries": [],
                "errors": [],
                "seed_entry_count": 0,
            },
        )

        enqueue = self.orchestrator._enqueue_local_apply_closure_item_for_completed_worker_result(
            {"worker_id": worker_handle.worker_id, "worker_status": "completed", "source": "unit_test"}
        )
        self.assertEqual(str(enqueue.get("status") or ""), "enqueued")

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=AssertionError("zero-delta search seed must not queue profile prefetch"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=AssertionError("zero-delta search seed must not full materialize"),
            ),
        ):
            queue_result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {"job_id": job_id, "local_apply_closure_item_limit": 1}
            )

        self.assertEqual(int(queue_result.get("completed_count") or 0), 1)
        reconcile = dict(dict(queue_result["items"][0]).get("callback_result") or {})
        self.assertEqual(reconcile["status"], "reconciled_search_seed")
        self.assertEqual(reconcile["sync_status"], "skipped")
        self.assertEqual(reconcile["sync_reason"], "search_seed_no_candidate_delta")
        worker = self.store.get_agent_worker(worker_id=worker_handle.worker_id)
        assert worker is not None
        inline_ingest = dict(dict(worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(inline_ingest["sync_reason"], "search_seed_no_candidate_delta")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "search_seed"
        )
        self.assertEqual(int(search_reconcile["added_entry_count"]), 0)
        self.assertEqual(
            str(dict(search_reconcile.get("profile_prefetch") or {}).get("reason") or ""),
            "search_seed_no_candidate_delta",
        )
        self.assertEqual(len(json.loads((discovery_dir / "entries.json").read_text(encoding="utf-8"))), 1)

    def test_reconcile_completed_workflow_after_background_company_roster(self) -> None:
        request_payload = {
            "raw_user_request": "Find Manus AI people",
            "target_company": "Manus AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_company_roster_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "manusai" / "snapshot-company-roster-reconcile"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "all_people"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Manus AI",
            canonical_name="Manus AI",
            company_key="manusai",
            linkedin_slug="manus-ai",
            linkedin_company_url="https://www.linkedin.com/company/manus-ai/",
        )
        dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "manus_member_1",
                        "linkedinUrl": "https://www.linkedin.com/in/manus-member-1/",
                        "firstName": "Ada",
                        "lastName": "Planner",
                        "summary": "Engineer at Manus AI",
                        "currentPositions": [
                            {
                                "companyName": "Manus AI",
                                "title": "Software Engineer",
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
                                "currentCompanies": ["https://www.linkedin.com/company/manus-ai/"],
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
                    "company_filters": {},
                    "artifact_paths": {"dataset_items": str(dataset_items_path)},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
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
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::manusai::all_people",
            stage="acquiring",
            span_name="harvest_company_employees:manusai:all_people",
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
                    "company_filters": {},
                    "snapshot_dir": str(shard_snapshot_dir),
                    "root_snapshot_dir": str(snapshot_dir),
                    "shard_id": "all_people",
                    "title": "All People",
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
                return_value={
                    "status": "completed",
                    "candidate_count": 1,
                    "evidence_count": 1,
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "materialized_candidate_documents": str(
                            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                        )
                    },
                    "state_updates": {},
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={
                    "status": "completed",
                    "snapshot_id": snapshot_dir.name,
                    "layer_counts": {"layer_0_roster": 1},
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after company-roster reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_company_roster")
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertGreaterEqual(int(candidate_doc["candidate_count"]), 1)
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Ada Planner" for item in list(candidate_doc.get("candidates") or []))
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        company_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "company_roster"
        )
        self.assertEqual(int(company_reconcile["applied_worker_count"]), 1)
        self.assertEqual(int(dict(company_reconcile.get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)

if __name__ == "__main__":
    unittest.main()
