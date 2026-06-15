import os
import socket
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import (
    SourcingOrchestrator,
    _resolve_local_apply_waiting_prerequisite_delay_seconds,
)
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from sourcing_agent.workflow_event_response import SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class LocalApplyClosurePrerequisiteTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self._start_pg_durable_runtime(runtime_dir=Path(self.tempdir.name))
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
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
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _completed_profile_worker(
        self,
        *,
        job_id: str,
        snapshot_id: str = "snapshot-a",
        worker_key_suffix: str = "",
        output_payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        request = JobRequest.from_payload({"target_company": "OpenAI", "keywords": ["Agent"]})
        suffix = str(worker_key_suffix or snapshot_id).strip()
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload={},
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key=f"harvest_profile_batch::{suffix}",
            stage="enriching",
            span_name="harvest_profile_batch",
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_id": snapshot_id,
                "snapshot_dir": str(self.settings.company_assets_dir / "openai" / snapshot_id),
            },
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload=output_payload or {"summary": {"status": "completed"}},
        )
        return dict(self.store.get_agent_worker(worker_id=int(worker.worker_id)) or {})

    def _queued_local_apply_item(
        self,
        *,
        job_id: str,
        worker_id: int,
        snapshot_id: str = "snapshot-a",
        snapshot_dir: Path | None = None,
    ) -> dict[str, object]:
        metadata = {"snapshot_id": snapshot_id}
        if snapshot_dir is not None:
            metadata["snapshot_dir"] = str(snapshot_dir)
        return self.store.upsert_job_materialization_item(
            item_id=f"local-apply::{job_id}::{worker_id}",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            item_kind="local_apply_closure",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            priority=10,
            source_worker_ids=[worker_id],
            metadata=metadata,
        )

    def _queued_durable_item(
        self,
        *,
        item_id: str,
        job_id: str,
        item_kind: str,
        snapshot_dir: Path,
    ) -> dict[str, object]:
        return self.store.upsert_job_materialization_item(
            item_id=item_id,
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind=item_kind,
            source="unit_test",
            reason="runtime_namespace_guard",
            status="queued",
            phase="queued",
            priority=10,
            metadata={"snapshot_dir": str(snapshot_dir)},
        )

    def test_waiting_prerequisite_delay_env_override_is_capped(self) -> None:
        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_LOCAL_APPLY_WAITING_PREREQUISITE_DELAY_SECONDS": "999"},
            clear=False,
        ):
            self.assertEqual(_resolve_local_apply_waiting_prerequisite_delay_seconds(), 30)

    def test_candidate_documents_missing_marks_waiting_without_retry_failure(self) -> None:
        job_id = "job-local-apply-prereq"
        worker = self._completed_profile_worker(job_id=job_id)
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_worker",
            return_value={
                "status": "waiting_prerequisite",
                "reason": "candidate_documents_missing",
                "prerequisite_path": "/tmp/snapshot-a/candidate_documents.json",
                "snapshot_id": "snapshot-a",
            },
        ):
            result = self.orchestrator._process_local_apply_closure_item(
                item=item,
                lease_owner=f"{socket.gethostname()}:unit-test",
            )

        self.assertEqual(result["status"], "waiting_prerequisite")
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "waiting_prerequisite")
        self.assertEqual(refreshed["phase"], "waiting_prerequisite")
        self.assertEqual(refreshed["attempt_count"], 0)
        self.assertEqual(refreshed["last_error"], "")
        self.assertTrue(str(refreshed["not_before_at"] or "").strip())
        self.assertIn("candidate_documents", refreshed["metadata"]["failure_reason"])

    def test_candidate_documents_incomplete_for_profile_batch_marks_waiting(self) -> None:
        job_id = "job-local-apply-incomplete-docs"
        worker = self._completed_profile_worker(job_id=job_id)
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_worker",
            return_value={
                "status": "waiting_prerequisite",
                "reason": "candidate_documents_incomplete_for_profile_batch",
                "prerequisite_path": "/tmp/snapshot-a/candidate_documents.json",
                "snapshot_id": "snapshot-a",
                "requested_url_count": 50,
                "fetched_profile_url_count": 50,
                "resolved_candidate_count": 0,
                "unmatched_profile_url_count": 50,
            },
        ):
            result = self.orchestrator._process_local_apply_closure_item(
                item=item,
                lease_owner=f"{socket.gethostname()}:unit-test",
            )

        self.assertEqual(result["status"], "waiting_prerequisite")
        self.assertEqual(result["reason"], "candidate_documents_incomplete_for_profile_batch")
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "waiting_prerequisite")
        self.assertEqual(refreshed["phase"], "waiting_prerequisite")
        self.assertEqual(refreshed["attempt_count"], 0)
        self.assertIn("candidate_documents", refreshed["metadata"]["failure_reason"])

    def test_profile_local_apply_partial_progress_requeues_without_retry_penalty(self) -> None:
        job_id = "job-local-apply-partial-profile-chunk"
        urls = [
            f"https://www.linkedin.com/in/openai-partial-{index}/"
            for index in range(4)
        ]
        worker = self._completed_profile_worker(
            job_id=job_id,
            output_payload={"summary": {"status": "completed", "requested_urls": urls}},
        )
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))
        item = self.store.upsert_job_materialization_item(
            item_id=str(item["item_id"]),
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            source_worker_ids=[int(worker["worker_id"])],
            metadata={"worker_kind": "harvest_prefetch"},
        )
        captured: dict[str, object] = {}

        def _fake_process_worker(**kwargs):
            captured.update(kwargs)
            progress = dict(kwargs.get("profile_apply_progress") or {})
            return {
                "status": "partial_local_apply",
                "candidate_count": len(list(kwargs.get("profile_url_filter") or [])),
                "apply_result": {"profile_apply_progress": progress},
                "profile_apply_progress": progress,
            }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_worker",
            side_effect=_fake_process_worker,
        ):
            result = self.orchestrator._process_local_apply_closure_item(
                item=item,
                lease_owner=f"{socket.gethostname()}:unit-test",
                profile_url_limit=2,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(list(captured["profile_url_filter"]), urls[:2])
        progress = dict(captured["profile_apply_progress"])
        self.assertFalse(progress["profile_url_apply_complete"])
        self.assertEqual(progress["processed_profile_url_count"], 2)
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "queued")
        self.assertEqual(refreshed["phase"], "queued")
        self.assertEqual(refreshed["attempt_count"], 0)
        self.assertEqual(
            refreshed["metadata"]["local_apply_profile_progress"]["processed_profile_url_count"],
            2,
        )
        next_urls, next_progress = self.orchestrator._local_apply_profile_url_chunk_for_worker(
            item=refreshed,
            worker=worker,
            snapshot_id="snapshot-a",
            profile_url_limit=2,
        )
        self.assertEqual(next_urls, urls[2:])
        self.assertTrue(next_progress["profile_url_apply_complete"])
        self.assertEqual(next_progress["processed_profile_url_count"], 4)
        self.assertEqual(next_progress["remaining_profile_url_count"], 0)

    def test_profile_local_apply_completion_rewrites_stale_partial_progress(self) -> None:
        job_id = "job-local-apply-completion-profile-chunk"
        urls = [
            f"https://www.linkedin.com/in/openai-complete-{index}/"
            for index in range(4)
        ]
        worker = self._completed_profile_worker(
            job_id=job_id,
            output_payload={"summary": {"status": "completed", "requested_urls": urls}},
        )
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))
        item = self.store.upsert_job_materialization_item(
            item_id=str(item["item_id"]),
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            priority=10,
            source_worker_ids=[int(worker["worker_id"])],
            metadata={
                "worker_kind": "harvest_prefetch",
                "local_apply_profile_progress": {
                    "profile_url_apply_complete": False,
                    "total_profile_url_count": 4,
                    "processed_profile_url_count": 2,
                    "remaining_profile_url_count": 2,
                },
            },
        )
        final_progress = {
            "profile_url_apply_complete": True,
            "total_profile_url_count": 4,
            "processed_profile_url_count": 4,
            "remaining_profile_url_count": 0,
            "applied_profile_url_count": 2,
            "profile_url_chunk_size": 2,
            "profile_url_chunk_limit": 2,
            "cumulative_applied_profile_url_keys": urls,
        }

        def _fake_process_worker(**kwargs):
            worker_id = int(dict(worker).get("worker_id") or 0)
            self.store.checkpoint_agent_worker(
                worker_id,
                checkpoint_payload=dict(dict(worker).get("checkpoint") or {}),
                output_payload={
                    **dict(dict(worker).get("output") or {}),
                    "inline_incremental_ingest": {
                        "snapshot_id": "snapshot-a",
                        "profile_url_apply_complete": True,
                        "total_profile_url_count": 4,
                        "processed_profile_url_count": 4,
                        "remaining_profile_url_count": 0,
                        "applied_profile_url_count": 4,
                        "profile_url_chunk_size": 4,
                        "profile_url_chunk_limit": 4,
                        "cumulative_applied_profile_url_keys": urls,
                    },
                },
                status="completed",
            )
            return {
                "status": "processed",
                "candidate_count": len(list(kwargs.get("profile_url_filter") or [])),
                "apply_result": {"profile_apply_progress": final_progress},
                "profile_apply_progress": final_progress,
            }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_worker",
            side_effect=_fake_process_worker,
        ):
            result = self.orchestrator._process_local_apply_closure_item(
                item=item,
                lease_owner=f"{socket.gethostname()}:unit-test",
                profile_url_limit=4,
            )

        self.assertEqual(result["status"], "completed")
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "completed")
        self.assertEqual(refreshed["phase"], "applied")
        progress = dict(refreshed["metadata"]["local_apply_profile_progress"])
        self.assertTrue(progress["profile_url_apply_complete"])
        self.assertEqual(progress["processed_profile_url_count"], 4)
        self.assertEqual(progress["remaining_profile_url_count"], 0)

    def test_reawaken_waiting_prerequisite_resets_item_to_ready_queue(self) -> None:
        job_id = "job-local-apply-reawaken"
        worker = self._completed_profile_worker(job_id=job_id)
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))
        self.store.mark_job_materialization_item_waiting_prerequisite(
            str(item["item_id"]),
            retry_delay_seconds=30,
            metadata={"snapshot_id": "snapshot-a", "failure_reason": "waiting_prerequisite_candidate_documents"},
        )

        reawakened = self.orchestrator._reawaken_waiting_prerequisite_local_apply_closure_items(
            job_id=job_id,
            snapshot_id="snapshot-a",
        )

        self.assertEqual(reawakened, 1)
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "queued")
        self.assertEqual(
            refreshed["metadata"]["reawakened_by"],
            "orchestrator_candidate_documents_prerequisite_ready",
        )
        ready_items = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
        )
        self.assertEqual([ready["item_id"] for ready in ready_items], [item["item_id"]])

    def test_reawaken_after_apply_also_reawaken_board_visible_delta_items(self) -> None:
        job_id = "job-local-apply-reawaken-board-visible"
        snapshot_id = "snapshot-board-visible"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        worker = self._completed_profile_worker(job_id=job_id, snapshot_id=snapshot_id)
        local_apply_item = self._queued_local_apply_item(
            job_id=job_id,
            worker_id=int(worker["worker_id"]),
            snapshot_id=snapshot_id,
            snapshot_dir=snapshot_dir,
        )
        board_visible_item = self.store.upsert_job_materialization_item(
            item_id=f"board-visible::{job_id}::{worker['worker_id']}",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            item_kind="board_visible_delta_apply",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            priority=10,
            candidate_ids=["candidate-a", "candidate-b"],
            source_worker_ids=[int(worker["worker_id"])],
            metadata={"snapshot_id": snapshot_id, "snapshot_dir": str(snapshot_dir)},
        )
        self.store.mark_job_materialization_item_waiting_prerequisite(
            str(local_apply_item["item_id"]),
            retry_delay_seconds=30,
            metadata={"snapshot_id": snapshot_id, "failure_reason": "waiting_prerequisite_candidate_documents"},
        )
        self.store.mark_job_materialization_item_waiting_prerequisite(
            str(board_visible_item["item_id"]),
            retry_delay_seconds=30,
            metadata={"snapshot_id": snapshot_id, "failure_reason": "candidate_delta_candidates_missing"},
        )

        reawakened = self.orchestrator._maybe_reawaken_waiting_prerequisite_after_apply(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            apply_result={"status": "applied"},
        )

        self.assertEqual(reawakened, 2)
        refreshed_local = self.store.get_job_materialization_item(str(local_apply_item["item_id"]))
        refreshed_board = self.store.get_job_materialization_item(str(board_visible_item["item_id"]))
        self.assertEqual(refreshed_local["status"], "queued")
        self.assertEqual(refreshed_board["status"], "queued")
        self.assertEqual(
            refreshed_local["metadata"]["reawakened_by"],
            "orchestrator_candidate_documents_prerequisite_ready",
        )
        self.assertEqual(
            refreshed_board["metadata"]["reawakened_by"],
            "orchestrator_candidate_documents_prerequisite_ready",
        )
        ready_board_visible_items = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="board_visible_delta_apply",
            limit=10,
        )
        self.assertEqual([ready["item_id"] for ready in ready_board_visible_items], [board_visible_item["item_id"]])

    def test_local_apply_closure_drain_does_not_claim_when_job_drain_is_in_flight(self) -> None:
        job_id = "job-local-apply-single-flight"
        worker = self._completed_profile_worker(job_id=job_id)
        item = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker["worker_id"]))
        drain_lock = self.orchestrator._local_apply_closure_drain_lock_for_job(job_id)
        self.assertTrue(drain_lock.acquire(blocking=False))
        try:
            result = self.orchestrator._run_local_apply_closure_item_queue_once({"job_id": job_id})
        finally:
            drain_lock.release()

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["reason"], "local_apply_closure_drain_in_progress")
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "queued")
        self.assertEqual(refreshed["lease_owner"], "")

    def test_mixed_apply_marker_batch_replays_existing_candidate_delta_without_overwrite(self) -> None:
        job_id = "job-local-apply-mixed-marker"
        snapshot_id = "snapshot-mixed-marker"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload({"target_company": "OpenAI", "keywords": ["Agent"]})
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={"message": "Acquiring"},
        )
        worker_a = self._completed_profile_worker(
            job_id=job_id,
            snapshot_id=snapshot_id,
            worker_key_suffix="mixed-marker-a",
            output_payload={
                "summary": {"status": "completed"},
                "inline_incremental_apply": {
                    "worker_kind": "harvest_prefetch",
                    "snapshot_id": snapshot_id,
                    "apply_status": "applied",
                    "candidate_ids": ["candidate-a"],
                    "candidate_count": 1,
                    "applied_worker_ids": [],
                    "applied_worker_count": 1,
                },
            },
        )
        worker_b = self._completed_profile_worker(
            job_id=job_id,
            snapshot_id=snapshot_id,
            worker_key_suffix="mixed-marker-b",
        )
        worker_a_id = int(worker_a["worker_id"])
        worker_b_id = int(worker_b["worker_id"])
        for worker_id in (worker_a_id, worker_b_id):
            item = self.orchestrator._enqueue_local_apply_closure_item(
                job=self.store.get_job(job_id) or {},
                request=request,
                snapshot_id=snapshot_id,
                worker_kind="harvest_prefetch",
                worker_ids=[worker_id],
                reason="unit_mixed_marker",
                source="unit",
            )
            self.assertEqual(str(item.get("status") or ""), "queued")

        apply_worker_batches: list[list[int]] = []
        sync_calls: list[dict[str, object]] = []

        def _fake_apply(*, pending_workers, **_kwargs):
            worker_ids = [int(worker.get("worker_id") or 0) for worker in pending_workers]
            apply_worker_batches.append(worker_ids)
            return {
                "status": "applied",
                "snapshot_id": snapshot_id,
                "worker_ids": worker_ids,
                "candidate_ids": ["candidate-b"],
                "resolved_candidate_ids": ["candidate-b"],
                "candidate_count": 1,
                "resolved_candidate_count": 1,
                "requested_url_count": 1,
                "fetched_profile_url_count": 1,
            }

        def _fake_sync(**kwargs):
            sync_calls.append(
                {
                    "candidate_ids": list(kwargs.get("candidate_ids") or []),
                    "applied_worker_ids": list(kwargs.get("applied_worker_ids") or []),
                    "remaining_worker_ids": [
                        int(worker.get("worker_id") or 0)
                        for worker in list(kwargs.get("remaining_workers") or [])
                    ],
                }
            )
            return {
                "status": "completed",
                "reason": "unit_sync_completed",
                "materialization_contract": "board_visible_profile_delta",
                "full_snapshot_materialization_performed": False,
                "board_visible_patch": {"status": "completed", "candidate_count": 2},
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_handle_harvest_profile_completion_event",
                return_value={"profile_prefetch": {"status": "queued", "queued_worker_count": 0}},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_harvest_prefetch_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_after_harvest_ingest",
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
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "local_apply_closure_item_limit": 10,
                    # Mixed-marker coalescing is the unchunked batch contract.
                    # The default bounded URL chunk path intentionally keeps
                    # harvest-prefetch items single-worker so a large worker
                    # cannot bypass profile URL budgeting.
                    "local_apply_closure_profile_url_limit": 0,
                }
            )

        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(apply_worker_batches, [[worker_b_id]])
        self.assertEqual(len(sync_calls), 1)
        self.assertEqual(sync_calls[0]["candidate_ids"], ["candidate-a", "candidate-b"])
        self.assertEqual(sync_calls[0]["applied_worker_ids"], [worker_a_id, worker_b_id])
        self.assertEqual(sync_calls[0]["remaining_worker_ids"], [])
        worker_a_after = self.store.get_agent_worker(worker_id=worker_a_id)
        worker_b_after = self.store.get_agent_worker(worker_id=worker_b_id)
        assert worker_a_after is not None
        assert worker_b_after is not None
        worker_a_apply = dict(dict(worker_a_after.get("output") or {}).get("inline_incremental_apply") or {})
        worker_b_apply = dict(dict(worker_b_after.get("output") or {}).get("inline_incremental_apply") or {})
        self.assertEqual(worker_a_apply["candidate_ids"], ["candidate-a"])
        self.assertEqual(worker_b_apply["candidate_ids"], ["candidate-b"])
        self.assertEqual(worker_b_apply["applied_worker_ids"], [worker_b_id])

    def test_durable_materialization_drains_skip_nested_test_runtime_items_before_claim(self) -> None:
        root_runtime = self.orchestrator.runtime_dir
        nested_snapshot_dir = root_runtime / "test_env" / "scripted_case" / "company_assets" / "openai" / "snapshot-a"
        nested_snapshot_dir.mkdir(parents=True, exist_ok=True)
        job_id = "job-runtime-namespace-guard"
        worker = self._completed_profile_worker(job_id=job_id)
        local_apply = self._queued_local_apply_item(
            job_id=job_id,
            worker_id=int(worker["worker_id"]),
            snapshot_dir=nested_snapshot_dir,
        )
        search_seed = self._queued_durable_item(
            item_id="search-seed::runtime-namespace",
            job_id=job_id,
            item_kind=SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
            snapshot_dir=nested_snapshot_dir,
        )
        board_visible = self._queued_durable_item(
            item_id="board-visible::runtime-namespace",
            job_id=job_id,
            item_kind="board_visible_delta_apply",
            snapshot_dir=nested_snapshot_dir,
        )
        snapshot_full = self._queued_durable_item(
            item_id="snapshot-full::runtime-namespace",
            job_id=job_id,
            item_kind="snapshot_full_materialization",
            snapshot_dir=nested_snapshot_dir,
        )

        cases = [
            (local_apply, self.orchestrator._run_local_apply_closure_item_queue_once),
            (search_seed, self.orchestrator._run_search_seed_discovery_query_queue_once),
            (board_visible, self.orchestrator._run_board_visible_apply_queue_once),
            (snapshot_full, self.orchestrator._run_snapshot_full_materialization_queue_once),
        ]
        for item, drain in cases:
            with self.subTest(item_id=item["item_id"]):
                result = drain({"job_id": job_id, "legacy_materialization_adapter_enabled": True})
                self.assertEqual(result["claimed_count"], 0)
                self.assertEqual(result["runtime_namespace_skipped_count"], 1)
                self.assertEqual(result["items"][0]["reason"], "runtime_namespace_mismatch")
                refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
                self.assertEqual(refreshed["status"], "queued")
                self.assertEqual(refreshed["lease_owner"], "")
                self.assertEqual(refreshed["attempt_count"], 0)

    def test_local_apply_closure_queue_splits_groups_when_phase_budget_is_exhausted(self) -> None:
        job_id = "job-local-apply-budget"
        worker_a = self._completed_profile_worker(job_id=job_id, worker_key_suffix="a")
        worker_b = self._completed_profile_worker(job_id=job_id, worker_key_suffix="b")
        self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_a["worker_id"]))
        self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_b["worker_id"]))

        processed_groups: list[list[str]] = []

        def _fake_process(*, items, lease_owner, profile_url_limit=0, **_kwargs):  # noqa: ARG001
            processed_groups.append([str(item.get("item_id") or "") for item in items])
            return {
                "status": "completed",
                "item_ids": [str(item.get("item_id") or "") for item in items],
                "item_count": len(items),
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_process_local_apply_closure_item_batch",
                side_effect=_fake_process,
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.time.perf_counter",
                side_effect=[100.0, 100.0, 113.0, 113.0],
            ),
        ):
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "local_apply_closure_item_limit": 2,
                    "local_apply_closure_item_group_limit": 1,
                    "local_apply_closure_phase_budget_ms": 1000,
                }
            )

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["completed_count"], 1)
        self.assertTrue(result["elapsed_budget_exhausted"])
        self.assertEqual(result["reason"], "local_profile_delta_apply_phase_budget_exhausted")
        self.assertEqual(result["group_limit"], 1)
        self.assertEqual(len(processed_groups), 1)
        remaining = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            limit=10,
        )
        self.assertEqual(len(remaining), 1)

    def test_local_apply_closure_queue_splits_groups_by_candidate_budget(self) -> None:
        job_id = "job-local-apply-candidate-budget"
        worker_a = self._completed_profile_worker(
            job_id=job_id,
            worker_key_suffix="a",
            output_payload={"summary": {"candidate_count": 400}},
        )
        worker_b = self._completed_profile_worker(
            job_id=job_id,
            worker_key_suffix="b",
            output_payload={"summary": {"candidate_count": 400}},
        )
        self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_a["worker_id"]))
        self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_b["worker_id"]))

        processed_groups: list[list[str]] = []

        def _fake_process(*, items, lease_owner, profile_url_limit=0, **_kwargs):  # noqa: ARG001
            processed_groups.append([str(item.get("item_id") or "") for item in items])
            return {
                "status": "completed",
                "item_ids": [str(item.get("item_id") or "") for item in items],
                "item_count": len(items),
            }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_item_batch",
            side_effect=_fake_process,
        ):
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "local_apply_closure_item_limit": 2,
                    "local_apply_closure_item_group_limit": 2,
                    "local_apply_closure_candidate_limit": 500,
                }
            )

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["candidate_count"], 400)
        self.assertTrue(result["candidate_budget_exhausted"])
        self.assertEqual(result["reason"], "local_profile_delta_apply_candidate_budget_exhausted")
        self.assertEqual(len(processed_groups), 1)
        remaining = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            limit=10,
        )
        self.assertEqual(len(remaining), 1)

    def test_profile_local_apply_budget_uses_requested_urls_when_candidate_ids_missing(self) -> None:
        job_id = "job-local-apply-requested-url-budget"
        worker_a = self._completed_profile_worker(
            job_id=job_id,
            worker_key_suffix="requested-a",
            output_payload={
                "summary": {
                    "status": "completed",
                    "requested_url_count": 200,
                },
            },
        )
        worker_b = self._completed_profile_worker(
            job_id=job_id,
            worker_key_suffix="requested-b",
            output_payload={
                "summary": {
                    "status": "completed",
                    "requested_url_count": 200,
                },
            },
        )
        item_a = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_a["worker_id"]))
        item_b = self._queued_local_apply_item(job_id=job_id, worker_id=int(worker_b["worker_id"]))
        item_a = self.store.upsert_job_materialization_item(
            item_id=str(item_a["item_id"]),
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            priority=10,
            source_worker_ids=[int(worker_a["worker_id"])],
            metadata={"worker_kind": "harvest_prefetch"},
        )
        item_b = self.store.upsert_job_materialization_item(
            item_id=str(item_b["item_id"]),
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="unit_test",
            reason="harvest_profile_batch_completed",
            status="queued",
            phase="queued",
            priority=10,
            source_worker_ids=[int(worker_b["worker_id"])],
            metadata={"worker_kind": "harvest_prefetch"},
        )

        self.assertEqual(self.orchestrator._materialization_item_candidate_count(item_a), 200)
        self.assertEqual(self.orchestrator._materialization_item_candidate_count(item_b), 200)

        processed_groups: list[list[str]] = []

        def _fake_process(*, items, lease_owner, profile_url_limit=0, **_kwargs):  # noqa: ARG001
            processed_groups.append([str(item.get("item_id") or "") for item in items])
            return {
                "status": "completed",
                "item_ids": [str(item.get("item_id") or "") for item in items],
                "item_count": len(items),
            }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_local_apply_closure_item_batch",
            side_effect=_fake_process,
        ):
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "local_apply_closure_item_limit": 2,
                    "local_apply_closure_item_group_limit": 2,
                    "local_apply_closure_candidate_limit": 250,
                    "local_apply_closure_profile_url_limit": 0,
                }
            )

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["candidate_count"], 200)
        self.assertTrue(result["candidate_budget_exhausted"])
        self.assertEqual(result["reason"], "local_profile_delta_apply_candidate_budget_exhausted")
        self.assertEqual(len(processed_groups), 1)
        remaining = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            limit=10,
        )
        self.assertEqual(len(remaining), 1)

    def test_enqueue_local_apply_closure_persists_profile_url_budget_count(self) -> None:
        job_id = "job-local-apply-enqueue-budget-count"
        request = JobRequest.from_payload({"target_company": "OpenAI", "keywords": ["Agent"]})
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={"message": "Acquiring"},
        )
        worker = self._completed_profile_worker(
            job_id=job_id,
            worker_key_suffix="enqueue-budget",
            output_payload={
                "summary": {
                    "status": "completed",
                    "requested_url_count": 200,
                },
            },
        )

        item = self.orchestrator._enqueue_local_apply_closure_item(
            job=self.store.get_job(job_id) or {},
            request=request,
            snapshot_id="snapshot-a",
            worker_kind="harvest_prefetch",
            worker_ids=[int(worker["worker_id"])],
            reason="unit_enqueue_budget",
            source="unit",
        )

        self.assertEqual(str(item.get("status") or ""), "queued")
        metadata = dict(item.get("metadata") or {})
        self.assertEqual(metadata["local_apply_budget_unit"], "profile_url")
        self.assertEqual(metadata["profile_url_count_for_budget"], 200)
        self.assertEqual(metadata["requested_url_count"], 200)
        self.assertEqual(self.orchestrator._materialization_item_candidate_count(item), 200)

    def test_board_visible_apply_queue_splits_by_candidate_budget(self) -> None:
        job_id = "job-board-visible-candidate-budget"
        for index in range(2):
            candidate_ids = [f"candidate-{index}-{offset}" for offset in range(400)]
            self.store.upsert_job_materialization_item(
                item_id=f"board-visible::{job_id}::{index}",
                job_id=job_id,
                target_company="OpenAI",
                snapshot_id="snapshot-a",
                item_kind="board_visible_delta_apply",
                source="unit_test",
                reason="candidate_budget",
                status="queued",
                phase="queued",
                priority=10,
                candidate_ids=candidate_ids,
            )

        processed_items: list[str] = []

        def _fake_process(*, item, lease_owner):  # noqa: ARG001
            processed_items.append(str(item.get("item_id") or ""))
            return {"status": "completed", "board_visible_patch": {"status": "completed"}}

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_board_visible_delta_apply_item",
            side_effect=_fake_process,
        ):
            result = self.orchestrator._run_board_visible_apply_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "board_visible_apply_item_limit": 2,
                    "board_visible_apply_candidate_limit": 500,
                }
            )

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["candidate_count"], 400)
        self.assertTrue(result["candidate_budget_exhausted"])
        self.assertEqual(result["reason"], "board_visible_apply_candidate_budget_exhausted")
        self.assertEqual(len(processed_items), 1)
        remaining = self.store.list_ready_job_materialization_items(
            job_id=job_id,
            item_kind="board_visible_delta_apply",
            limit=10,
        )
        self.assertEqual(len(remaining), 1)

    def test_board_visible_apply_queue_coalesces_same_snapshot_items_before_publish(self) -> None:
        job_id = "job-board-visible-coalescing"
        snapshot_id = "snapshot-board-visible-coalescing"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="materializing",
            request_payload={"target_company": "OpenAI", "keywords": ["Agent"]},
            plan_payload={},
            summary_payload={},
        )
        for index in range(2):
            self.store.upsert_job_materialization_item(
                item_id=f"board-visible::{job_id}::{index}",
                job_id=job_id,
                target_company="OpenAI",
                snapshot_id=snapshot_id,
                item_kind="board_visible_delta_apply",
                source="unit_test",
                reason="coalescing_contract",
                status="queued",
                phase="queued",
                priority=10,
                candidate_ids=[f"candidate-{index}-a", f"candidate-{index}-b"],
                metadata={
                    "snapshot_dir": str(snapshot_dir),
                    "profile_delta_candidate_records": [
                        {"candidate_id": f"candidate-{index}-a", "profile": {"name": f"Candidate {index} A"}},
                        {"candidate_id": f"candidate-{index}-b", "profile": {"name": f"Candidate {index} B"}},
                    ],
                },
            )

        publish_calls: list[dict[str, object]] = []

        def _fake_publish(**kwargs):
            candidate_ids = [str(item) for item in list(kwargs.get("delta_candidate_ids") or [])]
            publish_calls.append(
                {
                    "candidate_ids": candidate_ids,
                    "profile_delta_candidate_records": list(kwargs.get("profile_delta_candidate_records") or []),
                }
            )
            return {
                "status": "completed",
                "patch_id": "patch-coalesced",
                "result_view_id": "view-coalesced",
                "overlay_path": str(snapshot_dir / "asset_population.json"),
                "candidate_ids": candidate_ids,
                "candidate_count": len(candidate_ids),
            }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_ensure_board_visible_delta_control_plane_sync",
            return_value={"status": "completed", "candidate_ids": [
                "candidate-0-a",
                "candidate-0-b",
                "candidate-1-a",
                "candidate-1-b",
            ]},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_publish_partial_board_visible_delta_overlay",
            side_effect=_fake_publish,
        ):
            result = self.orchestrator._run_board_visible_apply_queue_once(
                {
                    "job_id": job_id,
                    "legacy_materialization_adapter_enabled": True,
                    "board_visible_apply_item_limit": 2,
                    "board_visible_apply_candidate_limit": 10,
                }
            )

        self.assertEqual(result["claimed_count"], 2)
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(result["coalesced_group_count"], 1)
        self.assertEqual(result["coalesced_item_count"], 2)
        self.assertEqual(len(publish_calls), 1)
        self.assertEqual(
            publish_calls[0]["candidate_ids"],
            ["candidate-0-a", "candidate-0-b", "candidate-1-a", "candidate-1-b"],
        )
        self.assertEqual(len(publish_calls[0]["profile_delta_candidate_records"]), 4)
        completed_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="board_visible_delta_apply",
            statuses=["completed"],
        )
        self.assertEqual(len(completed_items), 2)
        self.assertTrue(
            all(
                dict(item.get("metadata") or {}).get("coalesced_board_visible_apply_group")
                for item in completed_items
            )
        )

    def test_terminal_worker_missing_remains_failed(self) -> None:
        item = self.store.upsert_job_materialization_item(
            item_id="local-apply::missing-worker",
            job_id="job-local-apply-missing-worker",
            target_company="OpenAI",
            snapshot_id="snapshot-a",
            item_kind="local_apply_closure",
            source="unit_test",
            reason="missing_worker",
            status="queued",
            phase="queued",
            source_worker_ids=[999999],
        )

        result = self.orchestrator._process_local_apply_closure_item(
            item=item,
            lease_owner=f"{socket.gethostname()}:unit-test",
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "worker_missing")
        refreshed = self.store.get_job_materialization_item(str(item["item_id"]))
        self.assertEqual(refreshed["status"], "failed")
        self.assertEqual(refreshed["last_error"], "worker_missing")


if __name__ == "__main__":
    unittest.main()
