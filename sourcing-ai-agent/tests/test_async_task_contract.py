"""Contract tests for the unified async-task envelope shape (Track C, decision c)."""

import unittest

from sourcing_agent.async_task_contract import (
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILED,
    TASK_STATUS_QUEUED,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
    TERMINAL_TASK_STATUSES,
    async_task_accepted,
    async_task_artifact,
    async_task_status,
    is_terminal_task_status,
    normalize_task_status,
)
from sourcing_agent.durable_runtime import (
    TERMINAL_COMMAND_STATUSES,
    TERMINAL_WORKFLOW_STATUSES,
)


class NormalizeTaskStatusTest(unittest.TestCase):
    def test_known_domain_statuses_map_to_lifecycle(self) -> None:
        cases = {
            "pending": TASK_STATUS_QUEUED,
            "queued": TASK_STATUS_QUEUED,
            "running": TASK_STATUS_RUNNING,
            "claimed": TASK_STATUS_RUNNING,
            "joined_existing_job": TASK_STATUS_RUNNING,
            "completed": TASK_STATUS_SUCCEEDED,
            "succeeded": TASK_STATUS_SUCCEEDED,
            "reused_completed_job": TASK_STATUS_SUCCEEDED,
            "failed": TASK_STATUS_FAILED,
            "failed_terminal": TASK_STATUS_FAILED,
            "cancelled": TASK_STATUS_CANCELLED,
            "superseded": TASK_STATUS_CANCELLED,
        }
        for domain_status, expected in cases.items():
            self.assertEqual(normalize_task_status(domain_status), expected, msg=domain_status)

    def test_case_and_whitespace_insensitive(self) -> None:
        self.assertEqual(normalize_task_status("  COMPLETED "), TASK_STATUS_SUCCEEDED)

    def test_unknown_and_empty_default_to_running_non_terminal(self) -> None:
        self.assertEqual(normalize_task_status("some_future_status"), TASK_STATUS_RUNNING)
        self.assertEqual(normalize_task_status(""), TASK_STATUS_RUNNING)
        self.assertEqual(normalize_task_status(None), TASK_STATUS_RUNNING)
        self.assertFalse(is_terminal_task_status("some_future_status"))

    def test_mapping_is_total_over_existing_terminal_vocabularies(self) -> None:
        # Every substrate terminal status must normalize to a terminal task status,
        # or a completed task would look perpetually in-flight to the client.
        for domain_status in TERMINAL_WORKFLOW_STATUSES | TERMINAL_COMMAND_STATUSES:
            self.assertIn(
                normalize_task_status(domain_status),
                TERMINAL_TASK_STATUSES,
                msg=f"terminal domain status {domain_status!r} normalized to non-terminal",
            )


class EnvelopeBuilderTest(unittest.TestCase):
    def test_accepted_envelope_has_required_handle_fields(self) -> None:
        env = async_task_accepted(
            task_id="cmd-1",
            task_type="export.projection",
            idempotency_key="export.projection:abc",
            domain_status="queued",
            dispatch={"strategy": "new_job"},
        )
        self.assertEqual(env["task_id"], "cmd-1")
        self.assertEqual(env["task_type"], "export.projection")
        self.assertEqual(env["status"], TASK_STATUS_QUEUED)
        self.assertEqual(env["domain_status"], "queued")
        self.assertEqual(env["idempotency_key"], "export.projection:abc")
        self.assertEqual(env["dispatch"], {"strategy": "new_job"})

    def test_accepted_preserves_domain_outcome(self) -> None:
        env = async_task_accepted(
            task_id="job-9", task_type="workflow", domain_status="joined_existing_job"
        )
        self.assertEqual(env["status"], TASK_STATUS_RUNNING)
        self.assertEqual(env["domain_status"], "joined_existing_job")

    def test_status_envelope_carries_error_only_when_failed(self) -> None:
        ok = async_task_status(task_id="cmd-1", task_type="export.projection", domain_status="claimed")
        self.assertEqual(ok["status"], TASK_STATUS_RUNNING)
        self.assertIsNone(ok["error"])
        self.assertIsNone(ok["artifact"])

        bad = async_task_status(
            task_id="cmd-1",
            task_type="export.projection",
            domain_status="failed_terminal",
            error={"reason": "boom"},
        )
        self.assertEqual(bad["status"], TASK_STATUS_FAILED)
        self.assertEqual(bad["error"], {"reason": "boom"})

    def test_status_envelope_carries_artifact_when_succeeded(self) -> None:
        artifact = async_task_artifact(
            handle="/api/exports/cmd-1/artifact",
            content_type="application/zip",
            filename="export.zip",
            byte_size=2048,
            headers={"X-Sourcing-Export-Record-Count": "3"},
        )
        env = async_task_status(
            task_id="cmd-1",
            task_type="export.projection",
            domain_status="succeeded",
            artifact=artifact,
        )
        self.assertEqual(env["status"], TASK_STATUS_SUCCEEDED)
        self.assertEqual(env["artifact"]["handle"], "/api/exports/cmd-1/artifact")
        self.assertEqual(env["artifact"]["content_type"], "application/zip")
        self.assertEqual(env["artifact"]["filename"], "export.zip")
        self.assertEqual(env["artifact"]["byte_size"], 2048)
        self.assertEqual(env["artifact"]["headers"], {"X-Sourcing-Export-Record-Count": "3"})

    def test_artifact_defaults_are_safe(self) -> None:
        artifact = async_task_artifact(handle="", content_type="", filename="")
        self.assertEqual(artifact["content_type"], "application/octet-stream")
        self.assertEqual(artifact["filename"], "download.bin")
        self.assertEqual(artifact["byte_size"], 0)
        self.assertEqual(artifact["headers"], {})


if __name__ == "__main__":
    unittest.main()
