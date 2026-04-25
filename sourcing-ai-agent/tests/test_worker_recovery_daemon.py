import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.storage import SQLiteStore
from sourcing_agent.worker_daemon import PersistentWorkerRecoveryDaemon


class _FakeSearchSeedAcquirer:
    def __init__(self, store: SQLiteStore) -> None:
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
        worker = self.store.get_agent_worker(
            job_id=job_id,
            lane_id=lane_id,
            worker_key=f"{query_spec['bundle_id']}::{index:02d}",
        )
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
    def __init__(self, store: SQLiteStore) -> None:
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
    def __init__(self, store: SQLiteStore) -> None:
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
    ) -> dict[str, object]:
        self.harvest_profile_batch_calls.append(
            {
                "job_id": job_id,
                "runtime_mode": runtime_mode,
                "snapshot_dir": str(snapshot_dir),
                "requested_url_count": str(len(profile_urls)),
                "allow_shared_provider_cache": str(bool(allow_shared_provider_cache)).lower(),
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


class PersistentWorkerRecoveryDaemonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tempdir.name}/runtime.db"
        self.controller_store = SQLiteStore(self.db_path)
        self.daemon_store = SQLiteStore(self.db_path)
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
        self.controller_store._connection.close()
        self.daemon_store._connection.close()
        self.tempdir.cleanup()

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
        with self.controller_store._lock, self.controller_store._connection:
            self.controller_store._connection.execute(
                "UPDATE agent_worker_runs SET updated_at = datetime('now', '-600 seconds') WHERE worker_id = ?",
                (handle.worker_id,),
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
        self.assertIsNotNone(worker)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["checkpoint"]["recovery_kind"], "harvest_profile_batch")

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
