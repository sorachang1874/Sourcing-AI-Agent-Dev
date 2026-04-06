import tempfile
import unittest

from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.domain import JobRequest
from sourcing_agent.storage import SQLiteStore


class AgentRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(f"{self.tempdir.name}/runtime.db")
        self.runtime = AgentRuntimeCoordinator(self.store)
        self.request = JobRequest(
            raw_user_request="帮我找 xAI 的 RL researcher",
            target_company="xAI",
            categories=["employee"],
            keywords=["RL", "researcher"],
        )
        self.plan_payload = {
            "acquisition_strategy": {"strategy_type": "scoped_search_roster"},
            "retrieval_plan": {"strategy": "hybrid"},
            "search_strategy": {"query_bundles": [{"bundle_id": "public_media", "source_family": "public_interviews"}]},
        }

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_worker_interrupt_and_resume_preserves_checkpoint(self) -> None:
        handle = self.runtime.begin_worker(
            job_id="job123",
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:01",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI RL Researcher"},
            handoff_from_lane="triage_planner",
        )
        self.runtime.checkpoint_worker(
            handle,
            checkpoint_payload={"completed_urls": ["https://example.com/1"], "stage": "search_html_fetched"},
            output_payload={"seed_entry_count": 2},
        )
        interrupted = self.runtime.interrupt_worker(handle.worker_id)
        self.assertIsNotNone(interrupted)
        self.assertTrue(self.runtime.should_interrupt_worker(handle))
        self.runtime.complete_worker(
            handle,
            status="interrupted",
            checkpoint_payload={"completed_urls": ["https://example.com/1"], "stage": "interrupted"},
            output_payload={"seed_entry_count": 2},
            handoff_to_lane="review_specialist",
        )

        resumed = self.runtime.begin_worker(
            job_id="job123",
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:01",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI RL Researcher"},
            handoff_from_lane="triage_planner",
        )
        worker = self.runtime.get_worker(resumed.worker_id)
        self.assertIsNotNone(worker)
        self.assertEqual(worker["worker_id"], handle.worker_id)
        self.assertEqual(worker["status"], "running")
        self.assertEqual(worker["checkpoint"]["completed_urls"], ["https://example.com/1"])
        self.assertFalse(worker["interrupt_requested"])

        self.runtime.complete_worker(
            resumed,
            status="completed",
            checkpoint_payload={"completed_urls": ["https://example.com/1", "https://example.com/2"], "stage": "completed"},
            output_payload={"seed_entry_count": 4},
            handoff_to_lane="acquisition_specialist",
        )
        workers = self.runtime.list_workers(job_id="job123")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["status"], "completed")
        self.assertEqual(workers[0]["checkpoint"]["completed_urls"][-1], "https://example.com/2")

        spans = self.store.list_agent_trace_spans(job_id="job123")
        self.assertGreaterEqual(len(spans), 2)
        self.assertEqual(spans[-1]["status"], "completed")
