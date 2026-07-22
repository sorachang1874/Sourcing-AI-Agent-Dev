import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.storage import ControlPlaneStore
from sourcing_agent.worker_daemon import PersistentWorkerRecoveryDaemon
from tests.pg_durable_runtime import psycopg
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class _FakeSearchSeedAcquirer:
    def __init__(self, store: ControlPlaneStore) -> None:
        self.store = store
        self.calls: list[dict[str, str]] = []
        self.refresh_calls: list[list[int]] = []

    def _execute_query_spec(
        self,
        *,
        index: int,
        query_spec: dict[str, str],
        identity: CompanyIdentity,
        discovery_dir: Path,
        logger,
        employment_status: str,
        worker_runtime,
        job_id: str,
        request_payload: dict,
        plan_payload: dict,
        runtime_mode: str,
        result_limit: int,
        prefetched_search_state: dict | None = None,
        prefetched_search_artifact_paths: dict | None = None,
        prefetched_search_raw_path: str = "",
        prefetched_search_manifest_path: str = "",
        prefetched_search_manifest_key: str = "",
    ) -> dict[str, object]:
        self.calls.append(
            {
                "job_id": job_id,
                "query": str(query_spec.get("query") or ""),
                "runtime_mode": runtime_mode,
                "company_key": identity.company_key,
                "prefetched_search_manifest_path": prefetched_search_manifest_path,
                "prefetched_search_manifest_key": prefetched_search_manifest_key,
            }
        )
        lane_id = "public_media_specialist" if query_spec.get("source_family") in {"public_interviews", "publication_and_blog"} else "search_planner"
        worker = None
        for worker_key in (
            f"{employment_status}::{query_spec['bundle_id']}::{index:02d}",
            f"{query_spec['bundle_id']}::{index:02d}",
        ):
            worker = self.store.get_agent_worker(
                job_id=job_id,
                lane_id=lane_id,
                worker_key=worker_key,
            )
            if worker is not None:
                break
        if worker is not None:
            self.store.complete_agent_worker(
                int(worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "resumed_by": "persistent_daemon"},
                output_payload={"summary": {"query": str(query_spec.get("query") or "")}, "entries": [], "errors": []},
            )
        return {"worker_status": "completed", "summary": {"query": str(query_spec.get("query") or "")}}

    def refresh_background_search_workers(self, workers: list[dict[str, object]]) -> dict[str, object]:
        self.refresh_calls.append([int(worker.get("worker_id") or 0) for worker in workers])
        updates: dict[int, dict[str, object]] = {}
        for worker in workers:
            worker_id = int(worker.get("worker_id") or 0)
            worker_key = str(worker.get("worker_key") or "")
            checkpoint = dict(worker.get("checkpoint") or {})
            updates[worker_id] = {
                "search_state": {
                    **dict(checkpoint.get("search_state") or {}),
                    "status": "ready_cached",
                    "task_id": "task_ready_1",
                    "ready_poll_token": "20260408T110000Z",
                },
                "search_artifact_paths": {
                    "tasks_ready_batch_20260408T110000Z": "/tmp/tasks_ready_batch.json",
                },
                "raw_path": "",
                "search_manifest_path": "/tmp/web_search_batch_manifest.json",
                "search_manifest_key": worker_key,
            }
        return {"errors": [], "worker_updates": updates}


class _FakeExploratoryEnricher:
    def __init__(self, store: ControlPlaneStore) -> None:
        self.store = store
        self.calls: list[dict[str, str]] = []
        self.refresh_calls: list[list[int]] = []

    def refresh_background_search_workers(self, workers: list[dict[str, object]]) -> dict[str, object]:
        self.refresh_calls.append([int(worker.get("worker_id") or 0) for worker in workers])
        updates: dict[int, dict[str, object]] = {}
        for worker in workers:
            worker_id = int(worker.get("worker_id") or 0)
            updates[worker_id] = {
                "prefetched_queries": {
                    "1": {
                        "task_key": f"{worker.get('worker_key')}::01",
                        "query": "Queued Exploration Lead xAI",
                        "search_state": {
                            "provider_name": "dataforseo_google_organic",
                            "task_id": "task_explore_ready_1",
                            "status": "ready_cached",
                        },
                        "artifact_paths": {
                            "tasks_ready_batch_20260408T120000Z": "/tmp/exploration_tasks_ready_batch.json",
                        },
                        "raw_path": "/tmp/exploration_prefetched_query_01.json",
                    }
                }
            }
        return {"errors": [], "worker_updates": updates}

    def _explore_candidate(
        self,
        *,
        snapshot_dir: Path,
        candidate: Candidate,
        target_company: str,
        logger,
        job_id: str,
        request_payload: dict,
        plan_payload: dict,
        runtime_mode: str,
        prefetched_search_queries: dict | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "job_id": job_id,
                "candidate_id": candidate.candidate_id,
                "runtime_mode": runtime_mode,
                "target_company": target_company,
                "prefetched_query_count": str(len(dict(prefetched_search_queries or {}))),
            }
        )
        worker = self.store.get_agent_worker(
            job_id=job_id,
            lane_id="exploration_specialist",
            worker_key=candidate.candidate_id,
        )
        if worker is not None:
            self.store.complete_agent_worker(
                int(worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "candidate_id": candidate.candidate_id},
                output_payload={"signals": ["public_profile"], "candidate_id": candidate.candidate_id},
            )
        return {"worker_status": "completed", "signals": ["public_profile"], "candidate_id": candidate.candidate_id}


class _FakeAcquisitionEngine:
    def __init__(self, store: ControlPlaneStore) -> None:
        self.search_seed_acquirer = _FakeSearchSeedAcquirer(store)
        self.multi_source_enricher = SimpleNamespace(
            exploratory_enricher=_FakeExploratoryEnricher(store),
            _execute_harvest_profile_batch_worker=self._execute_harvest_profile_batch_worker,
        )
        self.store = store
        self.harvest_company_calls: list[dict[str, str]] = []
        self.harvest_profile_batch_calls: list[dict[str, str]] = []

    def _execute_harvest_company_roster_worker(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        max_pages: int,
        page_limit: int,
        job_id: str,
        request_payload: dict,
        plan_payload: dict,
        runtime_mode: str,
        allow_shared_provider_cache: bool = True,
    ) -> dict[str, object]:
        self.harvest_company_calls.append(
            {
                "job_id": job_id,
                "company_key": identity.company_key,
                "runtime_mode": runtime_mode,
                "snapshot_dir": str(snapshot_dir),
                "allow_shared_provider_cache": str(bool(allow_shared_provider_cache)).lower(),
            }
        )
        worker = self.store.get_agent_worker(
            job_id=job_id,
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::{identity.company_key}",
        )
        if worker is not None:
            self.store.complete_agent_worker(
                int(worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
                output_payload={"summary": {"status": "completed", "company_key": identity.company_key}},
            )
        return {"worker_status": "completed", "summary": {"status": "completed", "company_key": identity.company_key}}

    def _execute_harvest_profile_batch_worker(
        self,
        *,
        profile_urls: list[str],
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict,
        plan_payload: dict,
        runtime_mode: str,
        allow_shared_provider_cache: bool = True,
        prefetch_batch_context: dict | None = None,
    ) -> dict[str, object]:
        self.harvest_profile_batch_calls.append(
            {
                "job_id": job_id,
                "runtime_mode": runtime_mode,
                "snapshot_dir": str(snapshot_dir),
                "requested_url_count": str(len(profile_urls)),
                "allow_shared_provider_cache": str(bool(allow_shared_provider_cache)).lower(),
                "prefetch_requested_url_count": str(dict(prefetch_batch_context or {}).get("requested_url_count") or ""),
                "prefetch_candidate_count": str(dict(prefetch_batch_context or {}).get("candidate_count") or ""),
                "nonblocking_submit": str(bool(dict(prefetch_batch_context or {}).get("nonblocking_submit"))).lower(),
                "recovery_submit_policy": str(dict(prefetch_batch_context or {}).get("recovery_submit_policy") or ""),
            }
        )
        worker = self.store.list_agent_workers(job_id=job_id, lane_id="enrichment_specialist")
        target = worker[0] if worker else None
        if target is not None:
            self.store.complete_agent_worker(
                int(target["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
                output_payload={"summary": {"status": "completed", "requested_url_count": len(profile_urls)}},
            )
        return {
            "worker_status": "completed",
            "summary": {"status": "completed", "requested_url_count": len(profile_urls)},
        }


class PersistentWorkerRecoveryDaemonTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tempdir.name}/runtime.db"
        # Two stores over one control plane on purpose: controller vs daemon
        # connections must coordinate through shared durable state.
        self.controller_store = self.make_pg_store(self.db_path)
        self.daemon_store = self.make_pg_store(self.db_path)
        self.controller_runtime = AgentRuntimeCoordinator(self.controller_store)
        self.daemon_runtime = AgentRuntimeCoordinator(self.daemon_store)
        self.request = JobRequest(
            raw_user_request="帮我找 xAI 的 RL researcher",
            target_company="xAI",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["RL", "researcher"],
        )
        self.plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "scoped_search_roster",
                "cost_policy": {
                    "parallel_search_workers": 2,
                    "parallel_exploration_workers": 1,
                    "worker_retry_limit": 1,
                },
            },
            "retrieval_plan": {"strategy": "hybrid"},
        }
        self.fake_engine = _FakeAcquisitionEngine(self.daemon_store)

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def _save_job(self, job_id: str, *, stage: str = "acquiring", status: str = "running") -> None:
        self.controller_store.save_job(
            job_id=job_id,
            job_type="workflow",
            status=status,
            stage=stage,
            request_payload=self.request.to_record(),
            plan_payload=self.plan_payload,
            summary_payload={},
        )

    def test_db_lease_coordinates_across_store_connections(self) -> None:
        job_id = "job_lease"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:bundle",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"bundle_id": "bundle", "query": "xAI RL"}, "query": "xAI RL", "index": 1},
            metadata={"index": 1},
            handoff_from_lane="triage_planner",
        )

        claimed_a = self.controller_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="daemon-a",
            lease_seconds=120,
        )
        self.assertIsNotNone(claimed_a)
        self.assertEqual(claimed_a["lease_owner"], "daemon-a")

        claimed_b = self.daemon_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="daemon-b",
            lease_seconds=120,
        )
        self.assertIsNone(claimed_b)

        released = self.controller_store.release_agent_worker_lease(handle.worker_id, lease_owner="daemon-a")
        self.assertIsNotNone(released)
        self.assertEqual(released["lease_owner"], "")

        claimed_c = self.daemon_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="daemon-b",
            lease_seconds=120,
        )
        self.assertIsNotNone(claimed_c)
        self.assertEqual(claimed_c["lease_owner"], "daemon-b")
        self.assertEqual(int(claimed_c["attempt_count"]), 2)

    def test_explicit_worker_ids_bypass_recoverable_scan_for_terminal_provider_event(self) -> None:
        job_id = "job_explicit_remote_event_worker"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-explicit-worker"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::explicit",
            stage="enriching",
            span_name="harvest_profile_batch:explicit",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/explicit-worker/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/explicit-worker/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": "run-explicit-worker",
                "dataset_id": "dataset-explicit-worker",
                "recovery_kind": "harvest_profile_batch",
            },
            output_payload={"summary": {"status": "queued"}},
            status="running",
        )
        claimed = self.controller_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="active-worker-that-would-block-scan",
            lease_seconds=120,
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(
            self.controller_store.list_recoverable_agent_workers(
                job_id=job_id,
                stale_after_seconds=0,
            ),
            [],
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="remote-event-explicit",
            total_limit=1,
            stale_after_seconds=0,
            job_id=job_id,
            explicit_worker_ids=[handle.worker_id],
            force_release_explicit_worker_leases=True,
        )
        summary = daemon.run_once()

        self.assertEqual(summary["explicit_worker_count"], 1)
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(self.fake_engine.harvest_profile_batch_calls[0]["requested_url_count"], "1")

    def test_explicit_worker_scope_leaves_submitted_remote_wait_to_event_owner_after_completion(self) -> None:
        job_id = "job_explicit_remote_event_scope"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-explicit-scope"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        def _begin_profile_worker(worker_key: str, profile_url: str):
            handle = self.controller_runtime.begin_worker(
                job_id=job_id,
                request=self.request,
                plan_payload=self.plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=worker_key,
                stage="enriching",
                span_name=worker_key,
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [profile_url]},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": [profile_url],
                    "request_payload": self.request.to_record(),
                    "plan_payload": self.plan_payload,
                    "runtime_mode": "workflow",
                },
                handoff_from_lane="acquisition_specialist",
            )
            self.controller_store.checkpoint_agent_worker(
                handle.worker_id,
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": f"run-{worker_key}",
                    "dataset_id": f"dataset-{worker_key}",
                    "recovery_kind": "harvest_profile_batch",
                },
                output_payload={"summary": {"status": "queued"}},
                status="running",
            )
            return handle

        explicit = _begin_profile_worker(
            "harvest_profile_batch::explicit-scope",
            "https://www.linkedin.com/in/explicit-scope/",
        )
        other = _begin_profile_worker(
            "harvest_profile_batch::other-recoverable",
            "https://www.linkedin.com/in/other-recoverable/",
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="remote-event-explicit-scope",
            total_limit=4,
            stale_after_seconds=0,
            job_id=job_id,
            explicit_worker_ids=[explicit.worker_id],
            force_release_explicit_worker_leases=True,
        )
        summary = daemon.run_once()

        self.assertTrue(summary["explicit_worker_scope"])
        self.assertTrue(summary["explicit_worker_scan_suppressed"])
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_profile_batch_calls), 1)
        self.assertEqual(
            self.controller_store.get_agent_worker(worker_id=explicit.worker_id)["status"],
            "completed",
        )
        self.assertEqual(
            self.controller_store.get_agent_worker(worker_id=other.worker_id)["status"],
            "running",
        )

        followup = daemon.run_once()

        self.assertTrue(followup["explicit_worker_scope"])
        self.assertFalse(followup["explicit_worker_scan_suppressed"])
        self.assertEqual(followup["remote_wait_skipped_count"], 1)
        self.assertEqual(followup["remote_wait_skipped_worker_ids"], [other.worker_id])
        self.assertEqual(followup["claimed_count"], 0)
        self.assertEqual(followup["executed_count"], 0)
        self.assertEqual(len(self.fake_engine.harvest_profile_batch_calls), 1)
        self.assertEqual(
            self.controller_store.get_agent_worker(worker_id=other.worker_id)["status"],
            "running",
        )

    def test_non_explicit_submitted_remote_wait_is_not_polled_without_terminal_event(self) -> None:
        job_id = "job_remote_wait_provider_owned"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-remote-wait-provider-owned"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::provider-owned",
            stage="enriching",
            span_name="harvest_profile_batch:provider-owned",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/provider-owned/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/provider-owned/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": "run-provider-owned",
                "dataset_id": "dataset-provider-owned",
                "recovery_kind": "harvest_profile_batch",
            },
            output_payload={"summary": {"status": "queued"}},
            status="running",
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="generic-recovery",
            total_limit=2,
            stale_after_seconds=0,
            job_id=job_id,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["recoverable_count"], 0)
        self.assertEqual(summary["remote_wait_skipped_count"], 1)
        self.assertEqual(summary["remote_wait_skipped_worker_ids"], [handle.worker_id])
        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(summary["executed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_profile_batch_calls, [])
        self.assertEqual(worker["status"], "running")

    def test_terminal_remote_event_marker_allows_remote_wait_recovery_owner_to_resume(self) -> None:
        job_id = "job_remote_wait_terminal_marker"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-remote-wait-terminal"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::terminal-marker",
            stage="enriching",
            span_name="harvest_profile_batch:terminal-marker",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/terminal-marker/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/terminal-marker/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": "run-terminal-marker",
                "dataset_id": "dataset-terminal-marker",
                "recovery_kind": "harvest_profile_batch",
                "remote_provider_terminal_event": {
                    "event_type": "ACTOR.RUN.SUCCEEDED",
                    "status": "succeeded",
                    "run_id": "run-terminal-marker",
                    "dataset_id": "dataset-terminal-marker",
                },
                "remote_provider_terminal_event_seen_at": "2026-05-23T00:00:00+00:00",
            },
            output_payload={"summary": {"status": "queued"}},
            status="running",
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="remote-event-recovery",
            total_limit=2,
            stale_after_seconds=0,
            job_id=job_id,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["remote_wait_skipped_count"], 0)
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_profile_batch_calls), 1)
        self.assertEqual(worker["status"], "completed")

    # -- orphaned remote-wait safety net (2026-07-20 production gap) ----------
    #
    # A submitted remote-wait worker is terminal-event owned (provider webhook
    # or in-process long-poll watcher) and is deliberately NOT re-polled by the
    # recovery daemon. After a serve restart the watcher thread is dead and,
    # without a webhook, no terminal event ever arrives: the worker must become
    # eligible for one bounded status re-poll once it is older than
    # remote_wait_orphan_seconds, or its terminal dataset is never collected
    # and the blocked job never resumes.

    def _begin_company_roster_remote_wait_worker(
        self,
        job_id: str,
        *,
        worker_key_suffix: str = "",
        run_id: str = "run-orphan",
        dataset_id: str = "dataset-orphan",
    ):
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / f"snapshot-orphan{worker_key_suffix}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::xai{worker_key_suffix}",
            stage="acquiring",
            span_name=f"harvest_company_employees:xai{worker_key_suffix}",
            budget_payload={"max_pages": 1, "page_limit": 25},
            input_payload={"company_identity": {"company_key": "xai"}},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": {
                    "requested_name": "xAI",
                    "canonical_name": "xAI",
                    "company_key": "xai",
                    "linkedin_slug": "xai",
                    "linkedin_company_url": "https://www.linkedin.com/company/xai/",
                },
                "snapshot_dir": str(snapshot_dir),
                "root_snapshot_dir": str(snapshot_dir),
                "max_pages": 1,
                "page_limit": 25,
                "company_filters": {},
                "worker_key_suffix": worker_key_suffix,
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
                "allow_shared_provider_cache": True,
            },
            handoff_from_lane="triage_planner",
        )
        self.controller_store.complete_agent_worker(
            handle.worker_id,
            status="queued",
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": run_id,
                "dataset_id": dataset_id,
                "recovery_kind": "harvest_company_employees",
            },
            output_payload={"summary": {"status": "queued", "run_id": run_id, "dataset_id": dataset_id}},
        )
        return handle

    def _age_worker_updated_at(self, worker_id: int, *, age_seconds: int) -> None:
        fixture = self._pg_store_fixture
        assert fixture is not None and psycopg is not None
        aged = (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {quoted_schema}.agent_worker_runs SET updated_at = %s WHERE worker_id = %s",
                    (aged, int(worker_id)),
                )

    def _orphan_daemon(self, job_id: str, **overrides) -> PersistentWorkerRecoveryDaemon:
        return PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="orphan-recovery",
            total_limit=8,
            stale_after_seconds=0,
            job_id=job_id,
            **overrides,
        )

    def test_orphaned_submitted_remote_wait_worker_is_admitted_and_collected(self) -> None:
        job_id = "job_orphan_remote_wait_collected"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)
        self._age_worker_updated_at(handle.worker_id, age_seconds=3600)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=4)
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 1)
        self.assertEqual(summary["remote_wait_orphan_admitted_worker_ids"], [handle.worker_id])
        self.assertEqual(summary["remote_wait_skipped_count"], 0)
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_company_calls), 1)
        self.assertEqual(worker["status"], "completed")

    def test_fresh_submitted_remote_wait_worker_is_not_orphan_admitted(self) -> None:
        job_id = "job_fresh_remote_wait_not_orphan"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=4)
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        # The event-owner contract is preserved for fresh remote-wait workers:
        # no eager re-poll while a webhook/watcher can still deliver.
        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 0)
        self.assertEqual(summary["remote_wait_skipped_count"], 1)
        self.assertEqual(summary["remote_wait_skipped_worker_ids"], [handle.worker_id])
        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_company_calls, [])
        self.assertEqual(worker["status"], "queued")

    def test_orphan_admission_is_bounded_by_per_tick_limit(self) -> None:
        job_id = "job_orphan_admission_limit"
        self._save_job(job_id)
        handles = [
            self._begin_company_roster_remote_wait_worker(
                job_id,
                worker_key_suffix=f"::shard{index}",
                run_id=f"run-orphan-{index}",
                dataset_id=f"dataset-orphan-{index}",
            )
            for index in range(3)
        ]
        for handle in handles:
            self._age_worker_updated_at(handle.worker_id, age_seconds=3600)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=2)
        summary = daemon.run_once()

        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 2)
        self.assertEqual(summary["remote_wait_skipped_count"], 1)
        admitted_ids = set(summary["remote_wait_orphan_admitted_worker_ids"])
        skipped_ids = set(summary["remote_wait_skipped_worker_ids"])
        self.assertEqual(admitted_ids | skipped_ids, {int(handle.worker_id) for handle in handles})
        self.assertEqual(summary["claimed_count"], 2)

    def test_alt_ref_only_checkpoint_is_never_orphan_admitted(self) -> None:
        # The submitted-remote-wait predicate accepts metadata/summary/dataset
        # refs, but the connector resume path only honors a checkpoint-level
        # run id (run_id/actor_run_id/actorRunId). Admitting a worker whose
        # checkpoint lacks one hands the connector nothing to resume and its
        # fallback is a duplicate PAID actor submit — so such orphans must
        # stay in the skipped (event-owner) lane, fail-closed.
        job_id = "job_orphan_alt_ref_only"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "dataset_id": "dataset-orphan",
                "recovery_kind": "harvest_company_employees",
            },
            output_payload={"summary": {"status": "queued", "run_id": "run-orphan", "dataset_id": "dataset-orphan"}},
            status="queued",
        )
        self._age_worker_updated_at(handle.worker_id, age_seconds=86400)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=4)
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 0)
        self.assertEqual(summary["remote_wait_skipped_count"], 1)
        self.assertEqual(summary["remote_wait_skipped_worker_ids"], [handle.worker_id])
        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_company_calls, [])
        self.assertEqual(worker["status"], "queued")

    def test_actor_run_id_checkpoint_is_orphan_admitted(self) -> None:
        # actor_run_id is a legitimate resume key for the connector (its cache
        # and scripted branches already honor it; the live path now does too),
        # so an aged alt-key checkpoint must be admitted like a run_id one.
        job_id = "job_orphan_actor_run_id"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "actor_run_id": "run-orphan-alt",
                "dataset_id": "dataset-orphan",
                "recovery_kind": "harvest_company_employees",
            },
            output_payload={"summary": {"status": "queued", "run_id": "run-orphan-alt"}},
            status="queued",
        )
        self._age_worker_updated_at(handle.worker_id, age_seconds=3600)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=4)
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 1)
        self.assertEqual(summary["remote_wait_orphan_admitted_worker_ids"], [handle.worker_id])
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_company_calls), 1)
        self.assertEqual(worker["status"], "completed")

    def test_orphan_admission_disabled_preserves_event_owner_skip(self) -> None:
        job_id = "job_orphan_admission_disabled"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)
        self._age_worker_updated_at(handle.worker_id, age_seconds=86400)

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=0, remote_wait_orphan_limit=4)
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 0)
        self.assertEqual(summary["remote_wait_skipped_count"], 1)
        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_company_calls, [])
        self.assertEqual(worker["status"], "queued")

    def test_orphaned_remote_wait_worker_with_terminal_marker_still_uses_event_path(self) -> None:
        job_id = "job_orphan_with_terminal_marker"
        self._save_job(job_id)
        handle = self._begin_company_roster_remote_wait_worker(job_id)
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_remote_harvest",
                "run_id": "run-orphan",
                "dataset_id": "dataset-orphan",
                "recovery_kind": "harvest_company_employees",
                "remote_provider_terminal_event": {
                    "event_type": "ACTOR.RUN.SUCCEEDED",
                    "status": "succeeded",
                    "run_id": "run-orphan",
                    "dataset_id": "dataset-orphan",
                },
                "remote_provider_terminal_event_seen_at": "2026-07-20T19:00:00+00:00",
            },
            output_payload={"summary": {"status": "queued"}},
            status="queued",
        )

        daemon = self._orphan_daemon(job_id, remote_wait_orphan_seconds=900, remote_wait_orphan_limit=4)
        summary = daemon.run_once()

        # Terminal-event-marked workers are not remote-wait at all: they take the
        # normal event-driven claim path, not the orphan safety net.
        self.assertEqual(summary["remote_wait_orphan_admitted_count"], 0)
        self.assertEqual(summary["remote_wait_skipped_count"], 0)
        self.assertEqual(summary["claimed_count"], 1)

    def test_terminal_profile_persist_stage_is_immediately_recoverable_after_partial_yield(self) -> None:
        job_id = "job_terminal_persist_recoverable"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-terminal-persist"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_urls = [
            "https://www.linkedin.com/in/terminal-persist-a/",
            "https://www.linkedin.com/in/terminal-persist-b/",
        ]
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::terminal-persist",
            stage="enriching",
            span_name="harvest_profile_batch:terminal-persist",
            budget_payload={"requested_url_count": len(profile_urls)},
            input_payload={"profile_urls": profile_urls},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": profile_urls,
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "persisting_terminal_harvest_profiles",
                "run_id": "run-terminal-persist",
                "dataset_id": "dataset-terminal-persist",
                "terminal_persist_progress": {
                    "processed_url_count": 1,
                    "remaining_url_count": 1,
                },
                "recovery_kind": "harvest_profile_batch",
            },
            output_payload={
                "summary": {
                    "status": "running",
                    "terminal_persist_progress": {"processed_url_count": 1, "remaining_url_count": 1},
                }
            },
            status="running",
        )
        self.controller_store.release_agent_worker_lease(handle.worker_id)

        recoverable = self.daemon_store.list_recoverable_agent_workers(
            job_id=job_id,
            stale_after_seconds=300,
        )
        claimed = self.daemon_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="terminal-persist-recovery",
            lease_seconds=120,
        )

        self.assertEqual([int(worker["worker_id"]) for worker in recoverable], [handle.worker_id])
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["lease_owner"], "terminal-persist-recovery")

    def test_persistent_daemon_defers_completion_callbacks_until_selected_markers_are_written(self) -> None:
        job_id = "job_remote_event_marker_batch_before_callback"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-marker-batch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        handles = []
        for index in (1, 2):
            url = f"https://www.linkedin.com/in/batched-marker-{index}/"
            handle = self.controller_runtime.begin_worker(
                job_id=job_id,
                request=self.request,
                plan_payload=self.plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::marker-{index}",
                stage="enriching",
                span_name=f"harvest_profile_batch:marker-{index}",
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [url]},
                metadata={
                    "index": index,
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": [url],
                    "request_payload": self.request.to_record(),
                    "plan_payload": self.plan_payload,
                    "runtime_mode": "workflow",
                },
                handoff_from_lane="acquisition_specialist",
            )
            self.controller_store.checkpoint_agent_worker(
                handle.worker_id,
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "run_id": f"run-marker-{index}",
                    "dataset_id": f"dataset-marker-{index}",
                    "recovery_kind": "harvest_profile_batch",
                },
                output_payload={"summary": {"status": "queued"}},
                status="running",
            )
            handles.append(handle)

        def _complete_matching_profile_worker(**kwargs):
            profile_urls = list(kwargs.get("profile_urls") or [])
            profile_url = str(profile_urls[0] if profile_urls else "")
            worker = next(
                (
                    item
                    for item in self.daemon_store.list_agent_workers(
                        job_id=str(kwargs.get("job_id") or ""),
                        lane_id="enrichment_specialist",
                    )
                    if profile_url in list(dict(item.get("metadata") or {}).get("profile_urls") or [])
                ),
                None,
            )
            self.assertIsNotNone(worker)
            assert worker is not None
            self.daemon_store.complete_agent_worker(
                int(worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
                output_payload={"summary": {"status": "completed", "requested_url_count": len(profile_urls)}},
            )
            return {
                "worker_status": "completed",
                "summary": {"status": "completed", "requested_url_count": len(profile_urls)},
            }

        self.fake_engine.multi_source_enricher._execute_harvest_profile_batch_worker = _complete_matching_profile_worker
        callback_observations: list[list[str]] = []

        def _completion_callback(result: dict[str, object]) -> dict[str, object]:
            callback_observations.append(
                [
                    str(
                        dict(self.daemon_store.get_agent_worker(worker_id=int(handle.worker_id)) or {}).get("status")
                        or ""
                    )
                    for handle in handles
                ]
            )
            return {"status": "enqueued", "worker_id": int(dict(result or {}).get("worker_id") or 0)}

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="remote-event-marker-batch",
            completion_callback=_completion_callback,
            total_limit=2,
            stale_after_seconds=0,
            job_id=job_id,
            explicit_worker_ids=[int(handle.worker_id) for handle in handles],
        )
        summary = daemon.run_once()

        self.assertEqual(summary["claimed_count"], 2)
        self.assertEqual(summary["executed_count"], 2)
        self.assertEqual(summary["jobs"][0]["completion_callback_count"], 2)
        self.assertEqual(callback_observations, [["completed", "completed"], ["completed", "completed"]])

    def test_root_runtime_recovery_daemon_skips_nested_test_runtime_worker(self) -> None:
        job_id = "job_nested_test_runtime_worker"
        self._save_job(job_id)
        root_runtime = Path(self.tempdir.name) / "runtime"
        snapshot_dir = root_runtime / "test_env" / "scripted_case" / "company_assets" / "openai" / "snap-1"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::nested-test",
            stage="enriching",
            span_name="harvest_profile_batch:nested-test",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-agent-current-0189/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/openai-agent-current-0189/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "queued"}},
            status="queued",
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="root-runtime-daemon",
            total_limit=1,
            stale_after_seconds=0,
            runtime_dir=root_runtime,
        )
        summary = daemon.run_once()

        self.assertEqual(summary["runtime_namespace_skipped_count"], 1)
        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(summary["executed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_profile_batch_calls, [])
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)
        self.assertEqual(worker["status"], "queued")

    def test_profile_coalescing_worker_stage_is_not_timer_recoverable(self) -> None:
        job_id = "job_profile_coalescing_legacy_worker"
        self._save_job(job_id)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "openai" / "snapshot-coalescing"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/coalesced-tail/"
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::coalescing",
            stage="enriching",
            span_name="harvest_profile_coalescing:coalescing",
            budget_payload={"requested_url_count": 1, "coalescing_min_age_ms": 60000},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
                "allow_shared_provider_cache": True,
                "prefetch_batch_context": {
                    "requested_url_count": 100,
                    "candidate_count": 100,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                },
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={
                "stage": "waiting_profile_coalescing",
                "not_before_at": "2000-01-01 00:00:00",
                "prefetch_batch_context": {
                    "requested_url_count": 100,
                    "candidate_count": 100,
                    "planned_deferred_url_count": 0,
                    "planned_dispatch_worker_count": 1,
                },
            },
            output_payload={"summary": {"status": "queued", "message": "waiting for coalescing"}},
            status="running",
        )

        self.assertEqual(
            self.daemon_store.list_recoverable_agent_workers(
                job_id=job_id,
                stale_after_seconds=999999,
            ),
            [],
        )
        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="coalescing-daemon",
            total_limit=1,
            stale_after_seconds=999999,
            job_id=job_id,
        )
        summary = daemon.run_once()

        self.assertEqual(summary["claimed_count"], 0)
        self.assertEqual(summary["executed_count"], 0)
        self.assertEqual(self.fake_engine.harvest_profile_batch_calls, [])

    def test_dead_local_worker_daemon_lease_does_not_block_recovery(self) -> None:
        job_id = "job_dead_local_lease"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::dead-local",
            stage="enriching",
            span_name="harvest_profile_batch:dead-local",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/example"]},
            metadata={"recovery_kind": "harvest_profile_batch"},
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={},
            status="queued",
        )
        claimed_a = self.controller_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="worker-recovery-daemon-local-host-999999",
            lease_seconds=120,
        )
        self.assertIsNotNone(claimed_a)

        with (
            # B4.3f: storage.py no longer re-imports the helper (its dead SQLite leg is
            # deleted); the PG-native path binds its own reference in the live module.
            mock.patch(
                "sourcing_agent.control_plane_live_postgres.worker_lease_owner_is_dead_local_process",
                return_value=True,
            ),
        ):
            recoverable = self.daemon_store.list_recoverable_agent_workers(job_id=job_id)
            self.assertEqual([int(worker["worker_id"]) for worker in recoverable], [handle.worker_id])
            claimed_b = self.daemon_store.claim_agent_worker(
                handle.worker_id,
                lease_owner="daemon-b",
                lease_seconds=120,
            )

        self.assertIsNotNone(claimed_b)
        self.assertEqual(claimed_b["lease_owner"], "daemon-b")
        self.assertEqual(int(claimed_b["attempt_count"]), 2)

    def test_persistent_daemon_recovers_stale_running_search_worker(self) -> None:
        job_id = "job_search_recovery"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-01"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:bundle",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {"bundle_id": "bundle", "query": "xAI RL researcher", "source_family": "web_search"},
                "query": "xAI RL researcher",
                "index": 1,
            },
            metadata={
                "index": 1,
                "identity": CompanyIdentity(
                    requested_name="xAI",
                    canonical_name="xAI",
                    company_key="xai",
                    linkedin_slug="xai",
                ).to_record(),
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "current",
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
                "result_limit": 10,
            },
            handoff_from_lane="triage_planner",
        )
        # Backdate the AUTHORITATIVE row (PG) — a SQLite-shadow UPDATE would
        # leave the row the recovery scan actually reads untouched.
        backdated_at = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        self.controller_store._control_plane_postgres._execute_non_query(
            "UPDATE agent_worker_runs SET updated_at = %s WHERE worker_id = %s",
            (backdated_at, int(handle.worker_id)),
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-search",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.search_seed_acquirer.calls), 1)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["lease_owner"], "")
        self.assertEqual(worker["checkpoint"]["resumed_by"], "persistent_daemon")

    def test_persistent_daemon_resumes_interrupted_exploration_worker(self) -> None:
        job_id = "job_exploration_recovery"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-02"
        candidate = Candidate(
            candidate_id="cand_open_1",
            name_en="Open Lead",
            display_name="Open Lead",
            category="lead",
            target_company="xAI",
            organization="xAI",
            employment_status="unknown",
            role="Researcher",
            focus_areas="reinforcement learning",
        )
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key=candidate.candidate_id,
            stage="enriching",
            span_name="explore_candidate:Open Lead",
            budget_payload={"max_queries": 6},
            input_payload={
                "candidate_id": candidate.candidate_id,
                "display_name": candidate.display_name,
                "candidate": candidate.to_record(),
            },
            metadata={
                "target_company": "xAI",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="enrichment_specialist",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="interrupted",
            checkpoint_payload={"stage": "interrupted", "completed_queries": ["Open Lead xAI"]},
            output_payload={"result_summaries": [{"query": "Open Lead xAI"}]},
            handoff_to_lane="review_specialist",
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-explore",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.multi_source_enricher.exploratory_enricher.calls), 1)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["candidate_id"], candidate.candidate_id)
        self.assertEqual(worker["output"]["candidate_id"], candidate.candidate_id)

    def test_persistent_daemon_refreshes_exploration_prefetch_before_resuming_worker(self) -> None:
        job_id = "job_exploration_prefetch_refresh"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-exploration-prefetch"
        candidate = Candidate(
            candidate_id="cand_prefetch_1",
            name_en="Queued Exploration Lead",
            display_name="Queued Exploration Lead",
            category="lead",
            target_company="xAI",
            organization="xAI",
        )
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key=candidate.candidate_id,
            stage="enriching",
            span_name="explore_candidate:Queued Exploration Lead",
            budget_payload={"max_queries": 6},
            input_payload={
                "candidate_id": candidate.candidate_id,
                "display_name": candidate.display_name,
                "candidate": candidate.to_record(),
            },
            metadata={
                "target_company": "xAI",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="enrichment_specialist",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"status": "queued"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-exploration-prefetch",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(
            self.fake_engine.multi_source_enricher.exploratory_enricher.refresh_calls,
            [[handle.worker_id]],
        )
        self.assertEqual(
            self.fake_engine.multi_source_enricher.exploratory_enricher.calls[0]["prefetched_query_count"],
            "1",
        )
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")

    def test_persistent_daemon_can_filter_single_job(self) -> None:
        target_job_id = "job_target_only"
        other_job_id = "job_should_skip"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-03"
        discovery_dir = snapshot_dir / "search_seed_discovery"

        self._save_job(target_job_id)
        self._save_job(other_job_id)

        def _create_worker(job_id: str, index: int, query: str):
            return self.controller_runtime.begin_worker(
                job_id=job_id,
                request=self.request,
                plan_payload=self.plan_payload,
                runtime_mode="workflow",
                lane_id="search_planner",
                worker_key=f"bundle::{index:02d}",
                stage="acquiring",
                span_name="search_bundle:bundle",
                budget_payload={"max_results": 10},
                input_payload={
                    "query_spec": {"bundle_id": "bundle", "query": query, "source_family": "web_search"},
                    "query": query,
                    "index": index,
                },
                metadata={
                    "index": index,
                    "identity": CompanyIdentity(
                        requested_name="xAI",
                        canonical_name="xAI",
                        company_key="xai",
                        linkedin_slug="xai",
                    ).to_record(),
                    "snapshot_dir": str(snapshot_dir),
                    "discovery_dir": str(discovery_dir),
                    "employment_status": "current",
                    "request_payload": self.request.to_record(),
                    "plan_payload": self.plan_payload,
                    "runtime_mode": "workflow",
                    "result_limit": 10,
                },
                handoff_from_lane="triage_planner",
            )

        target_handle = _create_worker(target_job_id, 1, "xAI RL target")
        other_handle = _create_worker(other_job_id, 2, "xAI RL skip")
        self.controller_runtime.complete_worker(
            target_handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"query": "xAI RL target"}},
        )
        self.controller_runtime.complete_worker(
            other_handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"query": "xAI RL skip"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-filtered",
            stale_after_seconds=180,
            total_limit=2,
            job_id=target_job_id,
        )
        summary = daemon.run_once()
        target_worker = self.controller_store.get_agent_worker(worker_id=target_handle.worker_id)
        other_worker = self.controller_store.get_agent_worker(worker_id=other_handle.worker_id)

        self.assertEqual(summary["job_id"], target_job_id)
        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(summary["jobs"]), 1)
        self.assertEqual(summary["jobs"][0]["job_id"], target_job_id)
        self.assertEqual(len(self.fake_engine.search_seed_acquirer.calls), 1)
        self.assertIsNotNone(target_worker)
        self.assertEqual(target_worker["status"], "completed")
        self.assertIsNotNone(other_worker)
        self.assertEqual(other_worker["status"], "queued")

    def test_persistent_daemon_refreshes_search_prefetch_before_resuming_worker(self) -> None:
        job_id = "job_search_prefetch_refresh"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-prefetch"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:bundle",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {"bundle_id": "bundle", "query": "xAI RL researcher", "source_family": "web_search"},
                "query": "xAI RL researcher",
                "index": 1,
            },
            metadata={
                "index": 1,
                "identity": CompanyIdentity(
                    requested_name="xAI",
                    canonical_name="xAI",
                    company_key="xai",
                    linkedin_slug="xai",
                ).to_record(),
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "current",
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
                "result_limit": 10,
            },
            handoff_from_lane="triage_planner",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="queued",
            checkpoint_payload={
                "stage": "waiting_remote_search",
                "search_manifest_path": str(discovery_dir / "web_search_batch_manifest.json"),
                "search_manifest_key": "bundle::01",
                "search_state": {"task_id": "task_submitted_1", "status": "waiting_for_ready_cached"},
            },
            output_payload={"summary": {"query": "xAI RL researcher", "status": "queued"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-prefetch-refresh",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(self.fake_engine.search_seed_acquirer.refresh_calls, [[handle.worker_id]])
        self.assertEqual(len(self.fake_engine.search_seed_acquirer.calls), 1)
        self.assertEqual(
            self.fake_engine.search_seed_acquirer.calls[0]["prefetched_search_manifest_key"],
            "bundle::01",
        )
        self.assertEqual(
            self.fake_engine.search_seed_acquirer.calls[0]["prefetched_search_manifest_path"],
            "/tmp/web_search_batch_manifest.json",
        )
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")

    def test_persistent_daemon_resumes_harvest_company_worker(self) -> None:
        job_id = "job_harvest_company_recovery"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-harvest-company"
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::xai",
            stage="acquiring",
            span_name="harvest_company_employees:xAI",
            budget_payload={"max_pages": 5, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "max_pages": 5,
                "page_limit": 25,
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={"summary": {"status": "queued"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-harvest-company",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_company_calls), 1)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["recovery_kind"], "harvest_company_employees")

    def test_persistent_daemon_resumes_harvest_profile_batch_worker(self) -> None:
        job_id = "job_harvest_profile_recovery"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-harvest-profile"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::abc123",
            stage="enriching",
            span_name="harvest_profile_batch:abc123",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/test-user/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/test-user/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={"summary": {"status": "queued"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-harvest-profile",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_profile_batch_calls), 1)
        self.assertEqual(self.fake_engine.harvest_profile_batch_calls[0]["nonblocking_submit"], "true")
        self.assertEqual(
            self.fake_engine.harvest_profile_batch_calls[0]["recovery_submit_policy"],
            "nonblocking_provider_handoff",
        )
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["recovery_kind"], "harvest_profile_batch")

    def test_persistent_daemon_splits_large_profile_recovery_by_candidate_limit(self) -> None:
        job_id = "job_harvest_profile_recovery_candidate_budget"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-harvest-profile-budget"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._save_job(job_id)
        handles = []
        for index in range(2):
            profile_urls = [
                f"https://www.linkedin.com/in/profile-budget-{index}-{url_index:03d}/"
                for url_index in range(200)
            ]
            handle = self.controller_runtime.begin_worker(
                job_id=job_id,
                request=self.request,
                plan_payload=self.plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::budget-{index}",
                stage="enriching",
                span_name=f"harvest_profile_batch:budget-{index}",
                budget_payload={"requested_url_count": len(profile_urls)},
                input_payload={"profile_urls": profile_urls},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": profile_urls,
                    "request_payload": self.request.to_record(),
                    "plan_payload": self.plan_payload,
                    "runtime_mode": "workflow",
                },
                handoff_from_lane="acquisition_specialist",
            )
            self.controller_runtime.complete_worker(
                handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "recovery_kind": "harvest_profile_batch",
                },
                output_payload={"summary": {"status": "queued", "requested_url_count": len(profile_urls)}},
            )
            handles.append(handle)

        def _complete_matching_profile_worker(**kwargs):
            profile_urls = list(kwargs.get("profile_urls") or [])
            matching_worker = next(
                (
                    worker
                    for worker in self.daemon_store.list_agent_workers(
                        job_id=str(kwargs.get("job_id") or ""),
                        lane_id="enrichment_specialist",
                    )
                    if list(dict(worker.get("metadata") or {}).get("profile_urls") or []) == profile_urls
                ),
                None,
            )
            self.assertIsNotNone(matching_worker)
            assert matching_worker is not None
            self.daemon_store.complete_agent_worker(
                int(matching_worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
                output_payload={"summary": {"status": "completed", "requested_url_count": len(profile_urls)}},
            )
            return {
                "worker_status": "completed",
                "summary": {"status": "completed", "requested_url_count": len(profile_urls)},
            }

        self.fake_engine.multi_source_enricher._execute_harvest_profile_batch_worker = _complete_matching_profile_worker

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-harvest-profile-budget",
            stale_after_seconds=0,
            total_limit=4,
            candidate_limit=256,
            job_id=job_id,
        )
        summary = daemon.run_once()
        worker_statuses = [
            str(dict(self.controller_store.get_agent_worker(worker_id=handle.worker_id) or {}).get("status") or "")
            for handle in handles
        ]

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(summary["candidate_count"], 200)
        self.assertTrue(summary["candidate_budget_exhausted"])
        self.assertEqual(worker_statuses.count("completed"), 1)
        self.assertEqual(worker_statuses.count("queued"), 1)

    def test_persistent_daemon_checks_phase_budget_between_profile_workers(self) -> None:
        job_id = "job_harvest_profile_recovery_elapsed_budget"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-harvest-profile-elapsed"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._save_job(job_id)
        handles = []
        for index in range(2):
            profile_urls = [f"https://www.linkedin.com/in/profile-elapsed-{index}-{url_index:03d}/" for url_index in range(80)]
            handle = self.controller_runtime.begin_worker(
                job_id=job_id,
                request=self.request,
                plan_payload=self.plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::elapsed-{index}",
                stage="enriching",
                span_name=f"harvest_profile_batch:elapsed-{index}",
                budget_payload={"requested_url_count": len(profile_urls)},
                input_payload={"profile_urls": profile_urls},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": profile_urls,
                    "request_payload": self.request.to_record(),
                    "plan_payload": self.plan_payload,
                    "runtime_mode": "workflow",
                },
                handoff_from_lane="acquisition_specialist",
            )
            self.controller_runtime.complete_worker(
                handle,
                status="queued",
                checkpoint_payload={
                    "stage": "waiting_remote_harvest",
                    "recovery_kind": "harvest_profile_batch",
                },
                output_payload={"summary": {"status": "queued", "requested_url_count": len(profile_urls)}},
            )
            handles.append(handle)

        def _slow_complete_profile_worker(**kwargs):
            profile_urls = list(kwargs.get("profile_urls") or [])
            matching_worker = next(
                (
                    worker
                    for worker in self.daemon_store.list_agent_workers(
                        job_id=str(kwargs.get("job_id") or ""),
                        lane_id="enrichment_specialist",
                    )
                    if list(dict(worker.get("metadata") or {}).get("profile_urls") or []) == profile_urls
                ),
                None,
            )
            self.assertIsNotNone(matching_worker)
            assert matching_worker is not None
            time.sleep(0.03)
            self.daemon_store.complete_agent_worker(
                int(matching_worker["worker_id"]),
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
                output_payload={"summary": {"status": "completed", "requested_url_count": len(profile_urls)}},
            )
            return {
                "worker_status": "completed",
                "summary": {"status": "completed", "requested_url_count": len(profile_urls)},
            }

        self.fake_engine.multi_source_enricher._execute_harvest_profile_batch_worker = _slow_complete_profile_worker

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-harvest-profile-elapsed",
            stale_after_seconds=0,
            total_limit=4,
            phase_budget_ms=10,
            job_id=job_id,
        )
        summary = daemon.run_once()
        worker_statuses = [
            str(dict(self.controller_store.get_agent_worker(worker_id=handle.worker_id) or {}).get("status") or "")
            for handle in handles
        ]

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertTrue(summary["elapsed_budget_exhausted"])
        self.assertEqual(worker_statuses.count("completed"), 1)
        self.assertEqual(worker_statuses.count("queued"), 1)

    def test_persistent_daemon_persists_completed_harvest_profile_batch_result_without_nested_worker_update(self) -> None:
        job_id = "job_harvest_profile_recovery_cached"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-harvest-profile-cached"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::cached",
            stage="enriching",
            span_name="harvest_profile_batch:cached",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/cached-user/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/cached-user/"],
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={"summary": {"status": "queued"}},
        )

        def _resume_without_store_update(**kwargs):
            self.fake_engine.harvest_profile_batch_calls.append(
                {
                    "job_id": str(kwargs.get("job_id") or ""),
                    "runtime_mode": str(kwargs.get("runtime_mode") or ""),
                    "snapshot_dir": str(kwargs.get("snapshot_dir") or ""),
                    "requested_url_count": str(len(list(kwargs.get("profile_urls") or []))),
                }
            )
            return {
                "worker_status": "completed",
                "summary": {"status": "completed", "requested_url_count": 1, "message": "reused_local_raw_cache"},
            }

        self.fake_engine.multi_source_enricher._execute_harvest_profile_batch_worker = _resume_without_store_update

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-harvest-profile-cached",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.harvest_profile_batch_calls), 1)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["stage"], "completed")
        self.assertEqual(worker["checkpoint"]["status"], "completed")
        self.assertEqual(worker["output"]["summary"]["message"], "reused_local_raw_cache")

    def test_persistent_daemon_processes_remote_wait_worker_for_completed_job(self) -> None:
        job_id = "job_completed_exploration_followup"
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / "xai" / "snapshot-completed-followup"
        candidate = Candidate(
            candidate_id="cand_completed_1",
            name_en="Completed Follow Up",
            display_name="Completed Follow Up",
            category="lead",
            target_company="xAI",
            organization="xAI",
        )
        self._save_job(job_id, stage="completed", status="completed")
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key=candidate.candidate_id,
            stage="enriching",
            span_name="explore_candidate:Completed Follow Up",
            budget_payload={"max_queries": 6},
            input_payload={
                "candidate_id": candidate.candidate_id,
                "display_name": candidate.display_name,
                "candidate": candidate.to_record(),
            },
            metadata={
                "target_company": "xAI",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": self.request.to_record(),
                "plan_payload": self.plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="enrichment_specialist",
        )
        self.controller_runtime.complete_worker(
            handle,
            status="running",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"status": "queued"}},
        )

        daemon = PersistentWorkerRecoveryDaemon(
            store=self.daemon_store,
            agent_runtime=self.daemon_runtime,
            acquisition_engine=self.fake_engine,
            owner_id="daemon-completed-job",
            stale_after_seconds=180,
            total_limit=2,
        )
        summary = daemon.run_once()
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)
        job = self.controller_store.get_job(job_id)

        self.assertEqual(summary["claimed_count"], 1)
        self.assertEqual(summary["executed_count"], 1)
        self.assertEqual(len(self.fake_engine.multi_source_enricher.exploratory_enricher.calls), 1)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "completed")
