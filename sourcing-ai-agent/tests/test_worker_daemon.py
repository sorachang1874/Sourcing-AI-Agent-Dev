import unittest

from sourcing_agent.worker_daemon import AutonomousWorkerDaemon


class WorkerDaemonTest(unittest.TestCase):
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
