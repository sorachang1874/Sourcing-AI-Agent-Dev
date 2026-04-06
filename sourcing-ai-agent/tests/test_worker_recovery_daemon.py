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
    ) -> dict[str, object]:
        self.calls.append(
            {
                "job_id": job_id,
                "query": str(query_spec.get("query") or ""),
                "runtime_mode": runtime_mode,
                "company_key": identity.company_key,
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


class _FakeExploratoryEnricher:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.calls: list[dict[str, str]] = []

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
    ) -> dict[str, object]:
        self.calls.append(
            {
                "job_id": job_id,
                "candidate_id": candidate.candidate_id,
                "runtime_mode": runtime_mode,
                "target_company": target_company,
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
        self.multi_source_enricher = SimpleNamespace(exploratory_enricher=_FakeExploratoryEnricher(store))


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
