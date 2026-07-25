import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch

from sourcing_agent.worker_daemon import AutonomousWorkerDaemon, PersistentWorkerRecoveryDaemon

REPO_ROOT = Path(__file__).resolve().parents[1]


class WorkerDaemonTest(unittest.TestCase):
    def test_crm_public_web_worker_imports_crm_runtime_boundary(self) -> None:
        source = (REPO_ROOT / "src/sourcing_agent/worker_daemon.py").read_text(encoding="utf-8")
        self.assertIn("from .crm_public_web_runtime import", source)
        crm_import_section = source[source.index("from .crm_public_web_runtime import") : source.index("from .domain import")]
        self.assertIn("CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND", crm_import_section)
        self.assertNotIn("execute_crm_public_web_run_to_local_idle", source)
        self.assertNotIn("from .legacy_target_candidate_public_web_runtime import", source)
        self.assertNotIn("from .target_candidate_public_web import", source)

    def test_daemon_retries_failed_worker_within_lane_budget(self) -> None:
        daemon = AutonomousWorkerDaemon(
            existing_workers=[],
            lane_limits={"search_planner": 1},
            lane_budget_caps={"search_planner": 3},
            total_limit=1,
            retry_limit=2,
        )
        attempts = {"bundle::01": 0}

        def execute(spec):
            key = spec["worker_key"]
            attempts[key] += 1
            if attempts[key] == 1:
                return {"worker_status": "failed", "lane_id": "search_planner", "worker_key": key}
            return {"worker_status": "completed", "lane_id": "search_planner", "worker_key": key}

        result = daemon.run(
            [{"index": 1, "lane_id": "search_planner", "worker_key": "bundle::01", "label": "query"}],
            executor=execute,
        )
        self.assertEqual(attempts["bundle::01"], 2)
        self.assertEqual(len(result["retried"]), 1)
        self.assertEqual(result["results"][0]["worker_status"], "completed")
        self.assertEqual(result["lane_budget_used"]["search_planner"], 2)

    def test_daemon_stops_when_lane_budget_exhausted(self) -> None:
        daemon = AutonomousWorkerDaemon(
            existing_workers=[],
            lane_limits={"exploration_specialist": 1},
            lane_budget_caps={"exploration_specialist": 1},
            total_limit=1,
            retry_limit=3,
        )
        attempts = {"cand1": 0}

        def execute(spec):
            attempts[spec["worker_key"]] += 1
            return {"worker_status": "failed", "lane_id": "exploration_specialist", "worker_key": spec["worker_key"]}

        result = daemon.run(
            [{"index": 1, "lane_id": "exploration_specialist", "worker_key": "cand1", "label": "candidate"}],
            executor=execute,
        )
        self.assertEqual(attempts["cand1"], 1)
        self.assertEqual(len(result["backlog"]), 1)
        self.assertEqual(result["lane_budget_used"]["exploration_specialist"], 1)

    def test_daemon_result_callback_fires_as_workers_finish(self) -> None:
        daemon = AutonomousWorkerDaemon(
            existing_workers=[],
            lane_limits={"search_planner": 2},
            lane_budget_caps={"search_planner": 2},
            total_limit=2,
            retry_limit=0,
        )
        allow_slow_finish = Event()
        callback_order: list[str] = []

        def execute(spec):
            worker_key = str(spec["worker_key"])
            if worker_key == "slow":
                allow_slow_finish.wait(timeout=1.0)
            return {"worker_status": "completed", "lane_id": "search_planner", "worker_key": worker_key}

        def on_result(result):
            callback_order.append(str(result.get("worker_key") or ""))
            if str(result.get("worker_key") or "") == "fast":
                allow_slow_finish.set()

        result = daemon.run(
            [
                {"index": 1, "lane_id": "search_planner", "worker_key": "slow", "label": "slow"},
                {"index": 2, "lane_id": "search_planner", "worker_key": "fast", "label": "fast"},
            ],
            executor=execute,
            result_callback=on_result,
        )

        self.assertEqual(callback_order[0], "fast")
        self.assertEqual(sorted(item["worker_key"] for item in result["results"]), ["fast", "slow"])

    def test_daemon_can_execute_selected_workers_serially(self) -> None:
        daemon = AutonomousWorkerDaemon(
            existing_workers=[],
            lane_limits={"enrichment_specialist": 4},
            lane_budget_caps={"enrichment_specialist": 4},
            total_limit=4,
            retry_limit=0,
            executor_parallelism=1,
        )
        execution_order: list[str] = []

        def execute(spec):
            worker_key = str(spec["worker_key"])
            execution_order.append(worker_key)
            return {"worker_status": "completed", "lane_id": "enrichment_specialist", "worker_key": worker_key}

        result = daemon.run(
            [
                {"index": 1, "lane_id": "enrichment_specialist", "worker_key": "batch-1", "label": "batch-1"},
                {"index": 2, "lane_id": "enrichment_specialist", "worker_key": "batch-2", "label": "batch-2"},
                {"index": 3, "lane_id": "enrichment_specialist", "worker_key": "batch-3", "label": "batch-3"},
            ],
            executor=execute,
        )

        self.assertEqual(execution_order, ["batch-1", "batch-2", "batch-3"])
        self.assertEqual(len(result["results"]), 3)
        self.assertEqual(result["lane_budget_used"]["enrichment_specialist"], 3)

    def test_serial_daemon_fires_callback_after_each_selected_worker(self) -> None:
        daemon = AutonomousWorkerDaemon(
            existing_workers=[],
            lane_limits={"enrichment_specialist": 4},
            lane_budget_caps={"enrichment_specialist": 4},
            total_limit=4,
            retry_limit=0,
            executor_parallelism=1,
        )
        events: list[str] = []

        def execute(spec):
            worker_key = str(spec["worker_key"])
            events.append(f"execute:{worker_key}")
            return {
                "worker_status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": worker_key,
            }

        def on_result(result):
            events.append(f"callback:{result['worker_key']}")

        result = daemon.run(
            [
                {"index": 1, "lane_id": "enrichment_specialist", "worker_key": "batch-1", "label": "batch-1"},
                {"index": 2, "lane_id": "enrichment_specialist", "worker_key": "batch-2", "label": "batch-2"},
            ],
            executor=execute,
            result_callback=on_result,
        )

        self.assertEqual(
            events,
            ["execute:batch-1", "callback:batch-1", "execute:batch-2", "callback:batch-2"],
        )
        self.assertEqual(len(result["results"]), 2)

    def test_persistent_recovery_daemon_splits_selected_workers_by_candidate_budget(self) -> None:
        workers = [
            {
                "worker_id": 1,
                "job_id": "job-budget",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::1",
                "status": "running",
                "input": {},
                "output": {"summary": {"requested_url_count": 400}},
                "metadata": {},
            },
            {
                "worker_id": 2,
                "job_id": "job-budget",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::2",
                "status": "running",
                "input": {},
                "output": {"summary": {"requested_url_count": 400}},
                "metadata": {},
            },
        ]

        class _Store:
            def list_recoverable_agent_workers(self, **kwargs):  # noqa: ARG002
                return list(workers)

            def get_job(self, job_id):  # noqa: ARG002
                return {
                    "status": "running",
                    "artifact_path": "",
                    "plan": {"scheduler_lane_limits": {"enrichment_specialist": 4}},
                }

            def list_agent_workers(self, job_id):  # noqa: ARG002
                return list(workers)

            def get_agent_worker(self, worker_id):
                return next((worker for worker in workers if int(worker["worker_id"]) == int(worker_id)), None)

            def claim_agent_worker(self, worker_id, **kwargs):  # noqa: ARG002
                return self.get_agent_worker(worker_id)

        class _ExploratoryEnricher:
            refresh_background_search_workers = None

        class _MultiSourceEnricher:
            exploratory_enricher = _ExploratoryEnricher()

        class _AcquisitionEngine:
            search_seed_acquirer = object()
            multi_source_enricher = _MultiSourceEnricher()

        daemon = PersistentWorkerRecoveryDaemon(
            store=_Store(),
            agent_runtime=None,
            acquisition_engine=_AcquisitionEngine(),
            owner_id="unit",
            total_limit=4,
            candidate_limit=500,
        )
        daemon._execute_claimed_worker = lambda worker_id: {  # type: ignore[method-assign]
            "worker_id": worker_id,
            "worker_status": "completed",
            "lane_id": "enrichment_specialist",
            "worker_key": f"harvest_profile_batch::{worker_id}",
        }

        result = daemon.run_once()

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["executed_count"], 1)
        self.assertEqual(result["candidate_count"], 400)
        self.assertTrue(result["candidate_budget_exhausted"])

    def test_persistent_recovery_daemon_quarantines_crm_public_web_agent_worker(self) -> None:
        worker = {
            "worker_id": 1,
            "job_id": "job-crm-public-web",
            "lane_id": "exploration_specialist",
            "worker_key": "crm-public-web-run-1",
            "status": "running",
            "input": {"run_id": "crm-public-web-run-1"},
            "output": {},
            "checkpoint": {},
            "metadata": {
                "recovery_kind": "crm_public_web_search",
                "run_id": "crm-public-web-run-1",
                "runtime_dir": "/tmp/crm-public-web",
            },
        }

        class _Store:
            def list_recoverable_agent_workers(self, **kwargs):  # noqa: ARG002
                return [worker]

            def get_job(self, job_id):  # noqa: ARG002
                return {
                    "status": "running",
                    "artifact_path": "",
                    "plan": {"scheduler_lane_limits": {"exploration_specialist": 1}},
                }

            def list_agent_workers(self, job_id):  # noqa: ARG002
                return [worker]

            def get_agent_worker(self, worker_id):
                return worker if int(worker_id) == 1 else None

            def claim_agent_worker(self, worker_id, **kwargs):  # noqa: ARG002
                return self.get_agent_worker(worker_id)

            def release_agent_worker_lease(self, *args, **kwargs):  # noqa: ARG002
                return None

            def complete_agent_worker(self, worker_id, *, status, checkpoint_payload, output_payload):  # noqa: ARG002
                worker["status"] = status
                worker["checkpoint"] = dict(checkpoint_payload or {})
                worker["output"] = dict(output_payload or {})
                return dict(worker)

        class _SearchSeedAcquirer:
            refresh_background_search_workers = None

        class _ExploratoryEnricher:
            refresh_background_search_workers = None

        class _MultiSourceEnricher:
            exploratory_enricher = _ExploratoryEnricher()

        class _AcquisitionEngine:
            search_seed_acquirer = _SearchSeedAcquirer()
            multi_source_enricher = _MultiSourceEnricher()
            search_provider = object()
            model_client = None

        daemon = PersistentWorkerRecoveryDaemon(
            store=_Store(),
            agent_runtime=None,
            acquisition_engine=_AcquisitionEngine(),
            owner_id="unit",
            total_limit=1,
        )

        result = daemon.run_once()

        self.assertEqual(result["claimed_count"], 1)
        self.assertEqual(result["executed_count"], 1)
        self.assertEqual(result["jobs"][0]["daemon_events"][0]["status"], "completed")
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["output"]["summary"]["owner"], "crm_public_web_phase_commands")
        self.assertEqual(
            worker["output"]["summary"]["reason"],
            "crm_public_web_agent_worker_recovery_retired",
        )
        self.assertTrue(worker["output"]["summary"]["legacy_worker_quarantined"])
