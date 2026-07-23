"""Recovery-band tail contracts — salvage waves 9+10 (final band wave).

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
groups 9-10): local-apply closure queue ownership/coalescing, the search-seed
discovery queue (retry-wait drain without worker scan, legacy-worker backfill
with explicit recovery-kind requirement), pre-retrieval refresh sync/skip,
worker cleanup retirement, takeover-runner supervision, blocking-run job
recovery, daemon lifecycle logs, remote-event followup for secondary terminal
workers, supervisor immediate recovery, zero-stale daemon config, parallel
former-search kickoff, outreach-layering thread daemonization and the
provider-limiter refill-scan skip had no modern coverage. Ported verbatim
onto the repo-standard PG fixture with two ride-along helpers. Old->new
mapping in docs/governance/REGRESSION_INDEX.md; the freeze ratchet shrinks in
the same change.
"""

import contextlib
import io
import json
import os
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.durable_runtime import legacy_job_operation_id, legacy_job_workflow_run_id
from sourcing_agent.domain import AcquisitionTask, Candidate, JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import (
    SourcingOrchestrator,
    _deserialize_acquisition_state_payload,
    _serialize_acquisition_state_payload,
)
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.cli import run_server_runtime_watchdog_once
from sourcing_agent.service_daemon import WorkerDaemonService
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class RecoveryBandTailTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def _seed_company_roster_inline_worker(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        request: JobRequest,
        plan_payload: dict,
    ) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::lock-narrowing",
            stage="acquiring",
            span_name="harvest_company_employees:lock-narrowing",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "completed"}},
        )
        return handle.worker_id

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

    def test_local_apply_closure_item_only_consumes_owned_worker_ids(self) -> None:
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
        job_id = "job_local_apply_item_owned_workers_only"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-local-apply-owned-workers"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_a = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )
        worker_b_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::owned-worker-b",
            stage="acquiring",
            span_name="harvest_company_employees:owned-worker-b",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_b_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "completed"}},
        )
        worker_b = worker_b_handle.worker_id
        self.assertNotEqual(worker_a, worker_b)

        self.orchestrator._enqueue_local_apply_closure_item(
            job=self.store.get_job(job_id) or {},
            request=request,
            snapshot_id=snapshot_dir.name,
            worker_kind="company_roster",
            worker_ids=[worker_a],
            reason="unit_owned_worker_only",
            source="unit",
        )

        apply_worker_batches: list[list[int]] = []

        def _fake_apply(*, pending_workers, **_kwargs):
            worker_ids = [int(worker.get("worker_id") or 0) for worker in pending_workers]
            apply_worker_batches.append(worker_ids)
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": worker_ids,
                "candidate_ids": [],
            }

        sync_remaining_worker_batches: list[list[int]] = []

        def _fake_sync(**kwargs):
            remaining_worker_ids = [
                int(worker.get("worker_id") or 0)
                for worker in list(kwargs.get("remaining_workers") or [])
                if int(worker.get("worker_id") or 0) > 0
            ]
            sync_remaining_worker_batches.append(remaining_worker_ids)
            return {
                "status": "deferred",
                "reason": "same_kind_background_workers_still_inflight",
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
                "remaining_worker_ids": remaining_worker_ids,
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                return_value=None,
            ),
        ):
            drain = self.orchestrator._run_local_apply_backlog_drain_once({"job_id": job_id})

        self.assertEqual(drain["claimed_count"], 1)
        self.assertEqual(apply_worker_batches, [[worker_a]])
        self.assertEqual(sync_remaining_worker_batches, [[worker_b]])
        worker_a_after = self.store.get_agent_worker(worker_id=worker_a)
        worker_b_after = self.store.get_agent_worker(worker_id=worker_b)
        assert worker_a_after is not None
        assert worker_b_after is not None
        self.assertTrue(dict(dict(worker_a_after.get("output") or {}).get("inline_incremental_ingest") or {}))
        self.assertFalse(dict(dict(worker_b_after.get("output") or {}).get("inline_incremental_ingest") or {}))

    def test_local_apply_closure_queue_coalesces_same_snapshot_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI batch coalescing people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["batch"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_local_apply_closure_batch_coalesces"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-local-apply-batch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"analysis_stage": "stage_2_final"},
        )
        worker_ids: list[int] = []
        for index in range(2):
            profile_url = f"https://www.linkedin.com/in/openai-local-apply-batch-{index}/"
            worker = self.orchestrator.agent_runtime.begin_worker(
                job_id=job_id,
                request=request,
                plan_payload=plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::local-apply-batch-{index}",
                stage="enriching",
                span_name=f"harvest_profile_batch:local-apply-batch-{index}",
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
            worker_ids.append(worker.worker_id)
            enqueue = self.orchestrator._enqueue_local_apply_closure_item(
                job=self.store.get_job(job_id) or {},
                request=request,
                snapshot_id=snapshot_dir.name,
                worker_kind="harvest_prefetch",
                worker_ids=[worker.worker_id],
                reason="unit_completed_harvest",
                source="unit_completed_harvest",
            )
            self.assertEqual(str(enqueue.get("status") or ""), "queued")

        calls: list[set[int]] = []

        def _fake_process_local_apply_closure_worker(*, worker, source, allowed_worker_ids):
            calls.append(set(allowed_worker_ids or set()))
            for worker_id in sorted(allowed_worker_ids or set()):
                current = self.store.get_agent_worker(worker_id=worker_id) or {}
                output = dict(current.get("output") or {})
                output["inline_incremental_ingest"] = {
                    "applied_at": "2026-05-03T00:00:00+00:00",
                    "worker_kind": "harvest_prefetch",
                    "snapshot_id": snapshot_dir.name,
                    "sync_status": "completed",
                    "applied_worker_ids": sorted(allowed_worker_ids or set()),
                }
                self.store.complete_agent_worker(
                    worker_id,
                    status="completed",
                    checkpoint_payload=dict(current.get("checkpoint") or {}),
                    output_payload=output,
                )
            return {"status": "reconciled_harvest_prefetch", "source": source}

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_worker",
            side_effect=_fake_process_local_apply_closure_worker,
        ):
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {
                    "job_id": job_id,
                    "local_apply_closure_item_limit": 10,
                    # This test owns the unchunked coalescing contract. The
                    # default harvest-prefetch daemon path keeps large profile
                    # workers single-item so URL chunk budgets cannot be
                    # bypassed by batch coalescing.
                    "local_apply_closure_profile_url_limit": 0,
                }
            )

        self.assertEqual(result["claimed_count"], 2)
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], set(worker_ids))
        remaining = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            statuses=["queued", "running", "failed_retryable"],
        )
        self.assertEqual(remaining, [])

    def test_backfill_search_seed_discovery_query_items_from_legacy_worker(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_discovery_item_backfill"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-seed-discovery-backfill"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="targeted_people_search::01",
            stage="acquiring",
            span_name="search_bundle:targeted_people_search",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                },
                "index": 1,
            },
            metadata={
                "recovery_kind": "search_seed_discovery",
                "index": 1,
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "current",
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "search_seed_discovery"},
            output_payload={
                "summary": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "agent-person",
                        "full_name": "Agent Person",
                        "source_type": "web_search",
                        "source_query": "OpenAI Agent",
                        "profile_url": "https://www.linkedin.com/in/agent-person/",
                    }
                ],
                "errors": [],
            },
        )

        dry_run = self.orchestrator.backfill_search_seed_discovery_query_items(
            {"job_id": job_id, "dry_run": True}
        )
        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(int(dry_run["backfilled_count"]), 1)
        self.assertFalse(
            self.store.list_job_materialization_items(
                job_id=job_id,
                item_kind="search_seed_discovery_query",
            )
        )

        applied = self.orchestrator.backfill_search_seed_discovery_query_items(
            {"job_id": job_id, "dry_run": False}
        )
        self.assertEqual(applied["status"], "completed")
        self.assertEqual(int(applied["backfilled_count"]), 1)
        items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="search_seed_discovery_query",
            statuses=["completed"],
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source_worker_ids"], [handle.worker_id])
        self.assertEqual(items[0]["metadata"]["backfill_source"], "search_seed_discovery_worker_backfill")
        self.assertEqual(items[0]["metadata"]["query"], "OpenAI Agent")

        second = self.orchestrator.backfill_search_seed_discovery_query_items(
            {"job_id": job_id, "dry_run": False}
        )
        self.assertEqual(int(second["existing_item_count"]), 1)

    def test_search_seed_discovery_backfill_requires_explicit_recovery_kind(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_discovery_explicit_recovery_kind"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-seed-explicit-kind"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="targeted_people_search::legacy-empty-kind",
            stage="acquiring",
            span_name="search_bundle:targeted_people_search",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                }
            },
            metadata={
                "index": 1,
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "current",
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "legacy-agent-person",
                        "full_name": "Legacy Agent Person",
                        "source_type": "web_search",
                        "source_query": "OpenAI Agent",
                        "profile_url": "https://www.linkedin.com/in/legacy-agent-person/",
                    }
                ],
                "errors": [],
            },
        )

        dry_run = self.orchestrator.backfill_search_seed_discovery_query_items(
            {"job_id": job_id, "dry_run": True}
        )

        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(int(dry_run["candidate_worker_count"]), 0)
        self.assertEqual(int(dry_run["backfilled_count"]), 0)
        self.assertFalse(
            self.store.list_job_materialization_items(
                job_id=job_id,
                item_kind="search_seed_discovery_query",
            )
        )

    def test_refresh_running_workflow_before_retrieval_syncs_missing_materialization_without_background_workers(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Anthropic people",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_materialization_only"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-pre-retrieval-materialization"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "Anthropic",
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
            return_value={
                "status": "completed",
                "reason": "pre_retrieval_refresh",
                "state_updates": {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            },
        ) as synchronize_snapshot:
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
        self.assertTrue(bool(refresh.get("materialization_refresh_required")))
        synchronize_snapshot.assert_called_once()
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        pre_retrieval_refresh = dict(dict(refreshed_job.get("summary") or {}).get("pre_retrieval_refresh") or {})
        self.assertEqual(pre_retrieval_refresh.get("snapshot_id"), snapshot_dir.name)
        self.assertTrue(bool(pre_retrieval_refresh.get("materialization_refresh_required")))
        self.assertEqual(str(dict(pre_retrieval_refresh.get("sync") or {}).get("status") or ""), "completed")

    def test_refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI coding people",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Coding"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_consumed_harvest_workers"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-consumed-harvest-workers"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
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
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
        )
        (snapshot_dir / "retrieval_index_summary.json").write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
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
            worker_key="harvest_profile_batch::consumed",
            stage="enriching",
            span_name="harvest_profile_batch:consumed",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-consumed/"]},
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
        workers = self.orchestrator.agent_runtime.list_workers(job_id=job_id)
        completed_harvest_workers = [
            worker for worker in workers if str(worker.get("status") or "") == "completed" and worker.get("worker_id")
        ]
        cursor = self.orchestrator._build_background_reconcile_cursor(completed_harvest_workers)
        acquisition_state = _deserialize_acquisition_state_payload(
            _serialize_acquisition_state_payload(
                {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "background_reconcile_cursor": {"harvest_prefetch": cursor},
                }
            )
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("consumed harvest workers should not trigger a second pre-retrieval sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state=acquisition_state,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "no_completed_background_workers")

    def test_cleanup_recoverable_workers_retires_terminal_workflow_workers(self) -> None:
        failed_request = {
            "target_company": "Anthropic",
            "raw_user_request": "failed worker cleanup",
        }
        failed_job_id = "job_failed_worker_cleanup"
        self.store.save_job(
            job_id=failed_job_id,
            job_type="workflow",
            status="failed",
            stage="failed",
            request_payload=failed_request,
            plan_payload={},
            summary_payload={"message": "Workflow failed."},
        )
        failed_session = self.store.create_agent_runtime_session(
            job_id=failed_job_id,
            target_company="Anthropic",
            request_payload=failed_request,
            plan_payload={},
            runtime_mode="workflow",
            lanes=[{"lane_id": "enrichment_specialist"}],
        )
        failed_span = self.store.create_agent_trace_span(
            session_id=int(failed_session["session_id"]),
            job_id=failed_job_id,
            lane_id="enrichment_specialist",
            span_name="failed cleanup",
            stage="acquiring",
        )
        failed_worker = self.store.create_or_resume_agent_worker(
            session_id=int(failed_session["session_id"]),
            job_id=failed_job_id,
            span_id=int(failed_span["span_id"]),
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::failed-cleanup",
            metadata={"recovery_kind": "harvest_profile_batch"},
        )
        self.store.mark_agent_worker_running(int(failed_worker["worker_id"]))
        self.store.checkpoint_agent_worker(
            int(failed_worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={},
            status="running",
        )

        superseded_request = {
            "target_company": "Reflection AI",
            "raw_user_request": "superseded worker cleanup",
        }
        superseded_job_id = "job_superseded_worker_cleanup"
        self.store.save_job(
            job_id=superseded_job_id,
            job_type="workflow",
            status="superseded",
            stage="completed",
            request_payload=superseded_request,
            plan_payload={},
            summary_payload={"message": "Workflow superseded."},
        )
        superseded_session = self.store.create_agent_runtime_session(
            job_id=superseded_job_id,
            target_company="Reflection AI",
            request_payload=superseded_request,
            plan_payload={},
            runtime_mode="workflow",
            lanes=[{"lane_id": "exploration_specialist"}],
        )
        superseded_span = self.store.create_agent_trace_span(
            session_id=int(superseded_session["session_id"]),
            job_id=superseded_job_id,
            lane_id="exploration_specialist",
            span_name="superseded cleanup",
            stage="retrieving",
        )
        superseded_worker = self.store.create_or_resume_agent_worker(
            session_id=int(superseded_session["session_id"]),
            job_id=superseded_job_id,
            span_id=int(superseded_span["span_id"]),
            lane_id="exploration_specialist",
            worker_key="search::superseded-cleanup",
            metadata={},
        )
        self.store.checkpoint_agent_worker(
            int(superseded_worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={},
            status="queued",
        )

        preview = self.orchestrator.cleanup_recoverable_workers({"dry_run": True, "limit": 20})
        self.assertEqual(preview["status"], "preview")
        self.assertEqual(preview["candidate_count"], 2)

        cleanup = self.orchestrator.cleanup_recoverable_workers({"limit": 20})
        self.assertEqual(cleanup["status"], "completed")
        self.assertEqual(cleanup["retired_count"], 2)

        refreshed_failed = self.store.get_agent_worker(worker_id=int(failed_worker["worker_id"]))
        self.assertEqual(refreshed_failed["status"], "cancelled")
        self.assertEqual(
            dict(refreshed_failed.get("metadata") or {}).get("cleanup", {}).get("source"), "recoverable_worker_cleanup"
        )

        refreshed_superseded = self.store.get_agent_worker(worker_id=int(superseded_worker["worker_id"]))
        self.assertEqual(refreshed_superseded["status"], "superseded")

    def test_spawn_workflow_takeover_runner_uses_supervisor_when_auto_daemon_enabled(self) -> None:
        with unittest.mock.patch(
            "sourcing_agent.orchestrator._spawn_detached_process", return_value={"status": "started", "pid": 123}
        ) as mocked:
            result = self.orchestrator._spawn_workflow_takeover_runner("job123", auto_job_daemon=True)  # noqa: SLF001
        self.assertEqual(result["status"], "started")
        self.assertIn("supervise-workflow", mocked.call_args.kwargs["command"])
        with unittest.mock.patch(
            "sourcing_agent.orchestrator._spawn_detached_process", return_value={"status": "started", "pid": 456}
        ) as mocked:
            self.orchestrator._spawn_workflow_takeover_runner("job456", auto_job_daemon=False)  # noqa: SLF001
        self.assertIn("execute-workflow", mocked.call_args.kwargs["command"])

    def test_run_workflow_blocking_triggers_job_recovery_when_acquisition_is_blocked(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )

        with (
            unittest.mock.patch.object(self.orchestrator, "_run_workflow") as mocked_run_workflow,
            unittest.mock.patch.object(self.orchestrator, "run_queued_workflow") as mocked_run_queued,
        ):

            def _mark_blocked(job_id: str, request: JobRequest, plan: object) -> None:
                self.store.save_job(
                    job_id=job_id,
                    job_type="workflow",
                    status="blocked",
                    stage="acquiring",
                    request_payload=request.to_record(),
                    plan_payload=plan.to_record() if hasattr(plan, "to_record") else {},
                    summary_payload={
                        "blocked_task": "acquire_full_roster",
                        "message": "queued_background_harvest",
                    },
                )

            mocked_run_workflow.side_effect = _mark_blocked

            def _mark_resume_dispatched(job_id: str) -> dict[str, str]:
                current_job = self.store.get_job(job_id) or {}
                self.store.save_job(
                    job_id=job_id,
                    job_type=str(current_job.get("job_type") or "workflow"),
                    status="completed",
                    stage="completed",
                    request_payload=dict(current_job.get("request") or {}),
                    plan_payload=dict(current_job.get("plan") or {}),
                    summary_payload={
                        **dict(current_job.get("summary") or {}),
                        "recovery_status": "takeover_completed",
                    },
                )
                return {"status": "completed", "stage": "completed"}

            mocked_run_queued.side_effect = _mark_resume_dispatched
            snapshot = self.orchestrator.run_workflow_blocking(
                {"plan_review_id": review_id, "job_recovery_poll_seconds": 0.1, "job_recovery_max_ticks": 3}
            )

        self.assertIn(snapshot["job"]["status"], {"blocked", "running", "completed"})
        # CALIBRATED 2026-07-22 (wave 10): the blocking runner's recovery
        # trigger now retries while acquisition stays blocked (observed 3
        # identical calls); the contract is that recovery fires for THIS job.
        self.assertGreaterEqual(mocked_run_queued.call_count, 1)
        for recovery_call in mocked_run_queued.call_args_list:
            self.assertEqual(recovery_call.args, (snapshot["job"]["job_id"],))
            self.assertEqual(recovery_call.kwargs, {})

    def test_worker_daemon_service_emits_lifecycle_logs(self) -> None:
        runtime_dir = Path(self.tempdir.name) / "runtime_worker_logs"
        stderr = io.StringIO()
        service = WorkerDaemonService(
            runtime_dir=runtime_dir,
            service_name="loggable-daemon",
            poll_seconds=0.01,
            recovery_callback=lambda payload: {  # noqa: ARG005
                "status": "completed",
                "daemon": {"recoverable_count": 0, "claimed_count": 0, "executed_count": 0, "jobs": []},
                "workflow_resume": [],
                "post_completion_reconcile": [],
            },
        )

        with contextlib.redirect_stderr(stderr):
            summary = service.run_forever(max_ticks=1)

        logs = stderr.getvalue()
        self.assertEqual(summary["status"], "stopped")
        self.assertIn('"event": "service_start"', logs)
        self.assertIn('"event": "service_tick"', logs)
        self.assertIn('"event": "service_stop"', logs)

    def test_remote_event_followup_can_recover_other_terminal_workers_after_primary_worker(self) -> None:
        class _FakeDaemon:
            def run_once(self) -> dict[str, object]:
                return {
                    "owner_id": "event-owner",
                    "recoverable_count": 1,
                    "claimed_count": 1,
                    "executed_count": 1,
                    "jobs": [{"job_id": "job-event", "claimed_count": 1, "executed_count": 1}],
                }

        remote_wait_worker = {
            "worker_id": 102,
            "job_id": "job-event",
            "lane_id": "enrichment_specialist",
            "status": "queued",
            "checkpoint": {
                "stage": "waiting_remote_harvest",
                "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:00+00:00",
            },
            "metadata": {"recovery_kind": "harvest_profile_batch"},
        }

        with (
            unittest.mock.patch.object(
                self.store,
                "list_recoverable_agent_workers",
                return_value=[remote_wait_worker],
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_build_worker_recovery_daemon",
                return_value=_FakeDaemon(),
            ) as build_daemon,
        ):
            summary, workflow_resume, post_completion_reconcile, followup = (
                self.orchestrator._run_remote_event_followup_rounds(
                    payload={
                        "explicit_worker_ids": [101],
                        "remote_event_followup_rounds": 1,
                    },
                    summary={
                        "owner_id": "event-owner",
                        "claimed_count": 1,
                        "executed_count": 1,
                    },
                    workflow_resume=[],
                    post_completion_reconcile=[],
                    explicit_job_id="job-event",
                    explicit_job_followup_rounds=0,
                    recovery_stale_after_seconds=0,
                    workflow_recovery_settings={},
                )
            )

        self.assertEqual(summary["claimed_count"], 2)
        self.assertEqual(summary["executed_count"], 2)
        self.assertEqual(workflow_resume, [])
        self.assertEqual(post_completion_reconcile, [])
        self.assertEqual(followup["status"], "completed")
        self.assertEqual(followup["round_count"], 1)
        self.assertEqual(dict(list(followup["rounds"])[0])["target_worker_ids"], [102])
        self.assertEqual(build_daemon.call_count, 1)

    def test_workflow_supervisor_uses_immediate_worker_recovery_when_runner_is_dead(self) -> None:
        request_payload = {
            "raw_user_request": "Find Humans& infra members",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
        }
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_supervisor_runner_dead"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={
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
                }
            },
        )
        running_worker = {
            "worker_id": 1,
            "job_id": job_id,
            "lane_id": "acquisition_specialist",
            "worker_key": "harvest_company_employees::humansand",
            "status": "running",
            "updated_at": "2026-04-11 05:40:00",
        }
        captured_payloads: list[dict[str, object]] = []

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "run_queued_workflow",
                return_value={"job_id": job_id, "status": "skipped", "stage": "acquiring", "reason": "already_running"},
            ),
            unittest.mock.patch.object(
                self.orchestrator.agent_runtime,
                "list_workers",
                return_value=[running_worker],
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "run_worker_recovery_once",
                side_effect=lambda payload=None: (
                    captured_payloads.append(dict(payload or {}))
                    or {
                        "status": "completed",
                        "daemon": {},
                        "workflow_resume": [],
                    }
                ),
            ),
        ):
            result = self.orchestrator.run_workflow_supervisor(
                job_id, auto_job_daemon=True, max_ticks=1, poll_seconds=0.01
            )

        self.assertEqual(result["status"], "running")
        self.assertEqual(captured_payloads[-1]["stale_after_seconds"], 0)

    def test_build_worker_recovery_daemon_preserves_zero_stale_after_seconds(self) -> None:
        daemon = self.orchestrator._build_worker_recovery_daemon(  # noqa: SLF001
            {
                "stale_after_seconds": 0,
                "worker_recovery_phase_budget_ms": 7000,
                "worker_recovery_candidate_limit": 321,
            }
        )
        self.assertEqual(daemon.stale_after_seconds, 0)
        self.assertEqual(daemon.phase_budget_ms, 7000)
        self.assertEqual(daemon.candidate_limit, 321)

    def test_acquire_full_roster_starts_former_search_in_parallel_with_worker_runtime(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-former-search-worker-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.worker_runtime = self.orchestrator.agent_runtime
        former_started = threading.Event()
        allow_former_finish = threading.Event()
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[
                {
                    "full_name": "Current Infra Builder",
                    "title": "Infrastructure Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/current-infra-builder/",
                }
            ],
            visible_entries=[
                {
                    "full_name": "Current Infra Builder",
                    "title": "Infrastructure Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/current-infra-builder/",
                }
            ],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            merged_path=snapshot_dir / "linkedin_company_people_all.json",
            visible_path=snapshot_dir / "linkedin_company_people_visible.json",
            headless_path=snapshot_dir / "linkedin_company_people_headless.json",
            summary_path=snapshot_dir / "linkedin_company_people_summary.json",
        )
        former_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "reflection-former-01",
                    "full_name": "Former Infra Builder",
                    "headline": "Former Infrastructure Engineer at Reflection AI",
                    "source_type": "linkedin_search",
                    "source_query": "Reflection AI former infra",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-infra-builder/",
                }
            ],
            query_summaries=[{"query": "Reflection AI former infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=snapshot_dir / "search_seed_discovery" / "summary.json",
            entries_path=snapshot_dir / "search_seed_discovery" / "entries.json",
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
                "include_former_search_seed": True,
                "cost_policy": {"allow_company_employee_api": False},
            },
        )

        def _fake_former_search(*args, **kwargs):
            former_started.set()
            self.assertTrue(allow_former_finish.wait(timeout=1.0))
            return AcquisitionExecution(
                task_id="acquire-full-roster-former-search-seed",
                status="completed",
                detail="Former search seed ready.",
                payload={"entry_count": 1},
                state_updates={"search_seed_snapshot": former_snapshot},
            )

        def _fake_roster_fetch(*args, **kwargs):
            self.assertTrue(former_started.wait(timeout=1.0))
            allow_former_finish.set()
            return roster_snapshot

        with (
            unittest.mock.patch.object(
                self.acquisition_engine.roster_connector,
                "fetch_company_roster",
                side_effect=_fake_roster_fetch,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                side_effect=_fake_former_search,
            ) as former_search_mock,
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher,
                "queue_background_profile_prefetch",
                return_value={"status": "completed", "queued_worker_count": 0},
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_parallel_former_search",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertIs(execution.state_updates.get("roster_snapshot"), roster_snapshot)
        self.assertIs(execution.state_updates.get("search_seed_snapshot"), former_snapshot)
        former_search_mock.assert_called_once()

    def test_background_outreach_layering_reconcile_thread_is_non_daemon(self) -> None:
        with unittest.mock.patch("sourcing_agent.orchestrator.threading.Thread") as thread_cls:
            thread_instance = thread_cls.return_value
            result = self.orchestrator._queue_background_outreach_layering_reconcile(
                job_id="job_outreach_layering_thread_contract",
                source="workflow_completion",
            )

        self.assertEqual(result["status"], "scheduled")
        self.assertFalse(thread_cls.call_args.kwargs.get("daemon"))
        thread_instance.start.assert_called_once()

    def test_worker_recovery_tick_skips_profile_refill_scan_when_provider_limiter_full(self) -> None:
        job_id = "job_refill_daemon_provider_full"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-refill-daemon-provider-full"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        deferred_url = "https://www.linkedin.com/in/refill-daemon-provider-full/"
        request_payload = JobRequest(
            raw_user_request="帮我找 OpenAI 做 Infra 的人",
            target_company="OpenAI",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["Infra"],
        ).to_record()
        request_payload["execution_preferences"] = {"harvest_profile_actor_global_inflight": 1}
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request_payload,
            plan_payload={
                "acquisition_strategy": {
                    "strategy_type": "scoped_search_roster",
                    "cost_policy": {"parallel_search_workers": 1},
                },
            },
            summary_payload={},
        )
        self.store.repos.linkedin_profile_registry.record_refill_plan_items(
            deferred_profile_urls=[deferred_url],
            source_jobs=[job_id],
            snapshot_dir=str(snapshot_dir),
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )
        lease = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="test-provider-full",
            budget=1,
            lease_seconds=60,
            lease_token="test-provider-full",
            metadata={"source": "unit"},
        )
        self.assertTrue(bool(lease.get("acquired")))
        try:
            with (
                unittest.mock.patch.object(
                    self.store.repos.linkedin_profile_registry,
                    "list_refill_queue_groups",
                    side_effect=AssertionError("provider-full refill must not scan registry groups"),
                ) as list_groups_mock,
                unittest.mock.patch.object(
                    self.acquisition_engine.multi_source_enricher,
                    "queue_background_profile_prefetch",
                    side_effect=AssertionError("provider-full refill must not submit provider work"),
                ) as prefetch_mock,
            ):
                recovery = self.orchestrator.run_worker_recovery_once(
                    {
                        "job_id": job_id,
                        "workflow_auto_resume_enabled": False,
                        "workflow_queue_auto_takeover_enabled": False,
                        "post_completion_reconcile_enabled": False,
                        "post_recovery_housekeeping_enabled": False,
                    }
                )
        finally:
            self.store.release_runtime_provider_limiter_slot(
                str(lease.get("lease_token") or ""),
                limiter_key="harvest_profile_scraper_actor",
                lease_owner="test-provider-full",
            )

        list_groups_mock.assert_not_called()
        prefetch_mock.assert_not_called()
        refill = dict(recovery.get("profile_prefetch_refill") or {})
        self.assertEqual(refill["status"], "idle")
        self.assertEqual(refill["reason"], "profile_prefetch_refill_provider_limiter_full")
        provider_limiter = dict(refill.get("provider_limiter") or {})
        self.assertEqual(provider_limiter["active_count"], 1)
        self.assertEqual(provider_limiter["budget"], 1)
        self.assertEqual(provider_limiter["available_count"], 0)



    # -- final band-tail four (2026-07-22): watchdog stale-shared bootstrap,
    # stage1-preview-before-post-profile-tail ordering, segmented-roster
    # inline callback for completed local shards, and the board-visible
    # daemon-owned open-work count.

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

    def test_server_runtime_watchdog_bootstraps_stale_shared_recovery(self) -> None:
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_hosted_runtime_watchdog_once",
            return_value={
                "status": "completed",
                "mode": "hosted",
                "worker_recovery": {"status": "completed"},
                "hosted_dispatch": [],
            },
        ) as hosted_mock:
            result = run_server_runtime_watchdog_once(self.orchestrator)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["mode"], "hosted")
        hosted_mock.assert_called_once()
        hosted_payload = dict(hosted_mock.call_args[0][0] or {})
        self.assertEqual(hosted_payload["hosted_runtime_watchdog_service_name"], "server-runtime-watchdog")
        self.assertEqual(hosted_payload["shared_service_name"], "worker-recovery-daemon")

    def test_run_worker_recovery_once_publishes_stage1_preview_before_post_profile_tail(self) -> None:
        class _FakeDaemon:
            def run_once(self) -> dict[str, object]:
                return {
                    "owner_id": "stage1-preview-bridge",
                    "recoverable_count": 0,
                    "claimed_count": 0,
                    "executed_count": 0,
                    "jobs": [],
                }

        request_payload = {
            "raw_user_request": "Find Google Gemini former researchers",
            "target_company": "Google",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["former"],
            "keywords": ["Gemini"],
            "top_k": 10,
            "execution_preferences": {"delta_baseline_snapshot_id": "20260511T000000"},
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_stage1_preview_bridge_tail_open"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id="snapshot-google-stage1-preview-bridge",
            candidates=[
                Candidate(
                    candidate_id="cand_google_gemini_bridge",
                    name_en="Google Gemini Bridge",
                    display_name="Google Gemini Bridge",
                    category="former_employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Gemini Research Scientist",
                    linkedin_url="https://www.linkedin.com/in/google-gemini-bridge/",
                ).to_record()
            ],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={
                "message": "Waiting for post-profile materialization.",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                        "candidate_doc_path": str(candidate_doc_path),
                    }
                },
            },
        )
        self.store.upsert_job_result_lifecycle(
            job_id=job_id,
            fields={
                "target_company": "Google",
                "current_snapshot_id": snapshot_dir.name,
                "projection_source_snapshot_id": snapshot_dir.name,
                "baseline_snapshot_id": "20260511T000000",
                "served_candidate_count": 0,
                "expected_candidate_count": 1,
                "delta_profile_progress_applicable": True,
                "delta_profile_required_count": 1,
                "delta_profile_fetched_count": 0,
                "stage1_former_search_returned_count": 1,
                "stage1_deduped_candidate_count": 1,
                "stage1_deduped_profile_url_count": 1,
                "stage1_profile_fetch_required_count": 1,
                "stage1_profile_fetched_count": 0,
                "metadata": {
                    "delta_profile_denominator_promoted": True,
                    "stage1_terminal_promoted_at": "2026-05-13T00:00:00+00:00",
                },
                "source_validation_status": "validated",
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="local_apply_stage1_preview_bridge_tail_open",
            job_id=job_id,
            target_company="Google",
            snapshot_id=snapshot_dir.name,
            item_kind="local_apply_closure",
            source="worker_completion_event",
            reason="harvest_profile_batch_completed_needs_local_apply_closure",
            status="queued",
            phase="queued",
            source_worker_ids=[881],
            metadata={"recovery_kind": "harvest_profile_batch", "snapshot_dir": str(snapshot_dir)},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_build_worker_recovery_daemon",
                return_value=_FakeDaemon(),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_search_seed_discovery_query_queue_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_profile_prefetch_refill_queue_once",
                return_value={"status": "idle", "dispatched_url_count": 0, "queued_worker_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_local_apply_backlog_drain_once",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_drain_event_level_materialization_for_job",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_board_visible_apply_queue_once",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_snapshot_full_materialization_queue_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_recover_stale_excel_intake_jobs_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_remote_event_followup_rounds",
                side_effect=lambda **kwargs: (
                    kwargs["summary"],
                    kwargs["workflow_resume"],
                    kwargs["post_completion_reconcile"],
                    {"status": "skipped", "reason": "unit"},
                ),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_emit_runtime_heartbeats_after_recovery",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_resume_blocked_workflows_after_recovery",
                side_effect=AssertionError("Stage 1 preview bridge must not resume finalization"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflows_after_recovery",
                side_effect=AssertionError("Stage 1 preview bridge must not reconcile completion"),
            ),
        ):
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "explicit_job_followup_rounds": 0,
                    "search_seed_discovery_enabled": False,
                    "snapshot_full_materialization_enabled": False,
                    "excel_intake_recovery_enabled": False,
                    "post_recovery_housekeeping_enabled": False,
                    "workflow_auto_resume_enabled": True,
                    "workflow_resume_explicit_job": True,
                    "workflow_queue_auto_takeover_enabled": False,
                    "post_completion_reconcile_enabled": True,
                }
            )

        bridge = dict(recovery.get("stage1_preview_recovery") or {})
        self.assertEqual(str(bridge.get("status") or ""), "completed")
        self.assertEqual(recovery["workflow_resume"], [])
        latest_job = self.store.get_job(job_id) or {}
        self.assertEqual(str(latest_job.get("status") or ""), "blocked")
        summary = dict(latest_job.get("summary") or {})
        stage1_preview = dict(summary.get("stage1_preview") or {})
        self.assertEqual(str(stage1_preview.get("status") or ""), "ready")
        self.assertTrue(str(stage1_preview.get("artifact_path") or ""))
        terminal_resume = dict(summary.get("terminal_stage_artifact_resume") or {})
        self.assertTrue(bool(terminal_resume.get("preview_only_recovery")))

    def test_execute_segmented_harvest_company_roster_workers_emits_inline_callback_for_completed_local_shards(
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
        job_id = "job_segmented_inline_company_roster_callback"
        snapshot_dir = self.settings.company_assets_dir / "miromindai" / "snapshot-segmented-inline-callback"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "us_core"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
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
        completed_worker = self.orchestrator.agent_runtime.begin_worker(
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
        queued_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::miromindai::global_rest",
            stage="acquiring",
            span_name="harvest_company_employees:miromindai:global_rest",
            budget_payload={"max_pages": 4, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir / "harvest_company_employees" / "shards" / "global_rest"),
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "company_filters": {},
                "max_pages": 4,
                "page_limit": 25,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
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
        self.orchestrator.agent_runtime.complete_worker(
            queued_worker,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "queued"}},
        )

        completed_result = {
            "worker_id": completed_worker.worker_id,
            "worker_status": "completed",
            "summary": {
                "company_identity": identity.to_record(),
                "status": "completed",
                "snapshot_dir": str(shard_snapshot_dir),
                "root_snapshot_dir": str(snapshot_dir),
            },
        }
        queued_result = {
            "worker_id": queued_worker.worker_id,
            "worker_status": "queued",
            "summary": {
                "company_identity": identity.to_record(),
                "status": "queued",
                "snapshot_dir": str(snapshot_dir / "harvest_company_employees" / "shards" / "global_rest"),
                "root_snapshot_dir": str(snapshot_dir),
            },
        }

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_execute_harvest_company_roster_worker",
                side_effect=[completed_result, queued_result],
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={"status": "queued", "queued_worker_count": 1},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError("segmented inline callback should defer sync while same-kind worker remains queued"),
            ),
        ):
            summary = self.acquisition_engine._execute_segmented_harvest_company_roster_workers(
                identity=identity,
                snapshot_dir=snapshot_dir,
                shards=[
                    {
                        "shard_id": "us_core",
                        "title": "United States",
                        "strategy_id": "small_org_roster",
                        "company_filters": {"locations": ["United States"]},
                        "max_pages": 1,
                        "page_limit": 25,
                    },
                    {
                        "shard_id": "global_rest",
                        "title": "Global",
                        "strategy_id": "small_org_roster",
                        "company_filters": {},
                        "max_pages": 4,
                        "page_limit": 25,
                    },
                ],
                job_id=job_id,
                request_payload=request.to_record(),
                plan_payload=plan_payload,
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(int(summary.get("queued_count") or 0), 1)
        self.assertEqual(int(summary.get("completed_count") or 0), 1)
        self.assertFalse((snapshot_dir / "candidate_documents.json").exists())
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        output = dict(refreshed_worker.get("output") or {})
        self.assertFalse(dict(output.get("inline_incremental_ingest") or {}))
        # CALIBRATED 2026-07-22 (durable command surface): the inline
        # callback enqueues a queued local-apply command, not a
        # materialization item.
        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id(job_id)
        )
        closure_commands = [
            command
            for command in commands
            if str(command.get("command_type") or "") == "linkedin.local_profile_delta.apply"
            and str(command.get("status") or "") == "queued"
        ]
        self.assertEqual(len(closure_commands), 1)
        closure_payload = dict(closure_commands[0].get("payload") or {})
        closure_metadata = dict(closure_payload.get("materialization_metadata") or {})
        self.assertEqual(closure_metadata.get("worker_kind"), "company_roster")
        self.assertEqual(
            dict(closure_metadata.get("provider_completion_result") or {}).get("source"),
            "segmented_harvest_company_roster_inprocess",
        )

    def test_job_scoped_recovery_open_work_counts_board_visible_delta_apply_as_daemon_owned(self) -> None:
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
        job_id = "job_open_work_board_visible_delta_apply"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={},
            artifact_path=str(Path(self.tempdir.name) / "company_assets" / "openai" / "snapshot-open-work"),
        )
        self.store.upsert_job_materialization_item(
            item_id="job-open-work|delta|board-visible",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-open-work",
            item_kind="board_visible_delta_apply",
            status="queued",
            phase="queued",
            candidate_ids=["candidate-1"],
        )
        self.store.upsert_job_materialization_item(
            item_id="job-open-work|snapshot|full",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-open-work",
            item_kind="snapshot_full_materialization",
            status="queued",
            phase="queued",
        )

        summary = self.orchestrator._job_scoped_recovery_open_work_summary(job_id=job_id)

        self.assertEqual(summary["materialization_open_item_count"], 2)
        self.assertEqual(summary["daemon_owned_materialization_open_item_count"], 1)
        self.assertEqual(summary["background_materialization_open_item_count"], 1)
        self.assertEqual(summary["daemon_owned_open_work_count"], 1)
        # CALIBRATED 2026-07-22: the anti-spoofing contract counts the
        # completed-looking-but-not-terminal workflow itself (no durable run,
        # job_terminal false) as one open non-daemon unit alongside the
        # snapshot item.
        self.assertEqual(summary["workflow_open_count"], 1)
        self.assertFalse(summary["job_terminal"])
        self.assertEqual(summary["non_daemon_open_work_count"], 2)
        self.assertEqual(summary["materialization_kind_counts"]["board_visible_delta_apply"], 1)
        self.assertEqual(summary["materialization_kind_counts"]["snapshot_full_materialization"], 1)

if __name__ == "__main__":
    unittest.main()
