import unittest
from threading import Event

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
