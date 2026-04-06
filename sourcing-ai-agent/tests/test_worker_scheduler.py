import unittest

from sourcing_agent.worker_scheduler import infer_resume_mode, lane_limits_from_plan, schedule_work_specs, summarize_scheduler


class WorkerSchedulerTest(unittest.TestCase):
    def test_schedule_prioritizes_resume_workers(self) -> None:
        specs = [
            {"index": 1, "lane_id": "search_planner", "worker_key": "a", "label": "a"},
            {"index": 2, "lane_id": "public_media_specialist", "worker_key": "b", "label": "b"},
            {"index": 3, "lane_id": "exploration_specialist", "worker_key": "c", "label": "c"},
        ]
        existing_workers = [
            {"worker_id": 1, "lane_id": "search_planner", "worker_key": "a", "status": "interrupted", "checkpoint": {"html_path": "/tmp/a"}},
            {"worker_id": 2, "lane_id": "public_media_specialist", "worker_key": "b", "status": "completed", "checkpoint": {"completed_urls": ["u1"]}},
        ]
        scheduled = schedule_work_specs(
            specs,
            existing_workers=existing_workers,
            lane_limits={"search_planner": 1, "public_media_specialist": 1, "exploration_specialist": 1},
            total_limit=2,
        )
        self.assertEqual(len(scheduled["selected"]), 2)
        self.assertEqual(scheduled["selected"][0]["worker_key"], "b")
        self.assertEqual(scheduled["selected"][0]["resume_mode"], "reuse_checkpoint")
        self.assertEqual(scheduled["selected"][1]["worker_key"], "a")
        self.assertEqual(scheduled["selected"][1]["resume_mode"], "resume_from_checkpoint")

    def test_scheduler_summary_exposes_resumable_workers(self) -> None:
        plan = {"acquisition_strategy": {"cost_policy": {"parallel_search_workers": 4, "parallel_exploration_workers": 2}}}
        workers = [
            {"worker_id": 1, "lane_id": "search_planner", "worker_key": "a", "status": "interrupted", "checkpoint": {"html_path": "/tmp/a"}},
            {"worker_id": 2, "lane_id": "exploration_specialist", "worker_key": "c", "status": "running", "checkpoint": {}},
        ]
        summary = summarize_scheduler(plan_payload=plan, workers=workers)
        self.assertEqual(summary["lane_limits"]["search_planner"], 4)
        self.assertEqual(summary["lane_limits"]["exploration_specialist"], 2)
        self.assertEqual(summary["lane_budget_caps"]["search_planner"], 8)
        self.assertEqual(summary["resumable_workers"][0]["worker_key"], "a")
        self.assertEqual(summary["lane_summary"][0]["lane_id"], "exploration_specialist")

    def test_resume_mode_classifier(self) -> None:
        self.assertEqual(infer_resume_mode(None), "fresh_start")
        self.assertEqual(infer_resume_mode({"status": "running", "checkpoint": {}}), "already_running")
        self.assertEqual(infer_resume_mode({"status": "completed", "checkpoint": {"stage": "done"}}), "reuse_checkpoint")
        self.assertEqual(
            lane_limits_from_plan({"acquisition_strategy": {"cost_policy": {"parallel_search_workers": 5}}})["search_planner"],
            5,
        )
