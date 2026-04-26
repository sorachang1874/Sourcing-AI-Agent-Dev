import tempfile
import unittest

from sourcing_agent.storage import ControlPlaneStore


class StorageJobGuardTest(unittest.TestCase):
    def test_save_job_does_not_regress_terminal_job_to_running(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            store = ControlPlaneStore(f"{tempdir}/test.db")
            job_id = "workflow-terminal-1"
            request_payload = {"raw_user_request": "我想要OpenAI做Reasoning方向的人"}
            plan_payload = {"target_company": "OpenAI"}

            store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload=request_payload,
                plan_payload=plan_payload,
                summary_payload={"message": "Workflow completed."},
                artifact_path="/tmp/results.json",
            )
            store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="acquiring",
                request_payload=request_payload,
                plan_payload=plan_payload,
                summary_payload={"message": "Late acquisition checkpoint."},
            )

            job = store.get_job(job_id)
            self.assertIsNotNone(job)
            assert job is not None
            self.assertEqual(job["status"], "completed")
            self.assertEqual(job["stage"], "completed")
            self.assertEqual((job.get("summary") or {}).get("message"), "Workflow completed.")
            self.assertEqual(job.get("artifact_path"), "/tmp/results.json")


if __name__ == "__main__":
    unittest.main()
