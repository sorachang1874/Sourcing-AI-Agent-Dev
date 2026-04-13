import tempfile
import threading
import time
import unittest
from unittest import mock
import json
from pathlib import Path

from sourcing_agent.service_daemon import SingleInstanceError, WorkerDaemonService, read_service_status, render_systemd_unit


class ServiceDaemonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name) / "runtime"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_service_run_writes_status_and_systemd_unit(self) -> None:
        calls: list[dict] = []

        def callback(payload: dict) -> dict:
            calls.append(payload)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
            total_limit=2,
        )
        summary = service.run_forever(max_ticks=1)
        status = read_service_status(self.runtime_dir, "worker-recovery-daemon")

        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(status["status"], "stopped")
        self.assertEqual(status["tick"], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["total_limit"], 2)
        self.assertEqual(status["last_summary"]["status"], "completed")
        self.assertEqual(status["last_nonempty_summary"], {})
        self.assertEqual(status["last_nonempty_tick"], 0)
        self.assertEqual(status["cumulative_summary"]["tick_count"], 1)
        self.assertEqual(status["cumulative_summary"]["active_tick_count"], 0)
        self.assertEqual(status["cumulative_summary"]["total_claimed_count"], 0)
        self.assertEqual(status["cumulative_summary"]["total_executed_count"], 0)

        unit_path = Path(self.tempdir.name) / "worker-daemon.service"
        written = service.write_systemd_unit(
            project_root=Path(self.tempdir.name) / "project",
            output_path=unit_path,
            user_name="tester",
        )
        self.assertEqual(written, unit_path)
        unit_text = unit_path.read_text()
        self.assertIn("ExecStart=/usr/bin/env python3 -m sourcing_agent.cli run-worker-daemon-service", unit_text)
        self.assertIn("WorkingDirectory=", unit_text)
        self.assertIn("User=tester", unit_text)

    def test_single_instance_lock_rejects_second_service(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocking_callback(payload: dict) -> dict:  # noqa: ARG001
            entered.set()
            release.wait(timeout=2.0)
            return {"status": "completed", "daemon": {"claimed_count": 0}}

        service_a = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=blocking_callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )
        service_b = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: {"status": "completed", "daemon": payload},
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )
        thread = threading.Thread(target=lambda: service_a.run_forever(max_ticks=1), daemon=True)
        thread.start()
        self.assertTrue(entered.wait(timeout=1.0))
        try:
            with self.assertRaises(SingleInstanceError):
                service_b.run_forever(max_ticks=1)
        finally:
            release.set()
            thread.join(timeout=2.0)

    def test_service_reports_running_while_callback_is_in_progress(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocking_callback(payload: dict) -> dict:  # noqa: ARG001
            entered.set()
            release.wait(timeout=2.0)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=blocking_callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )
        thread = threading.Thread(target=lambda: service.run_forever(max_ticks=1), daemon=True)
        thread.start()
        self.assertTrue(entered.wait(timeout=1.0))
        try:
            status = read_service_status(self.runtime_dir, "worker-recovery-daemon")
            self.assertEqual(status["status"], "running")
            self.assertEqual(status["tick"], 1)
            self.assertEqual(status["cycle_state"], "running_callback")
            self.assertEqual(status["callback_payload"]["owner_id"], service.owner_id)
        finally:
            release.set()
            thread.join(timeout=2.0)

    def test_service_refreshes_heartbeat_while_callback_is_in_progress(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocking_callback(payload: dict) -> dict:  # noqa: ARG001
            entered.set()
            release.wait(timeout=2.0)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=blocking_callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.25,
        )
        thread = threading.Thread(target=lambda: service.run_forever(max_ticks=1), daemon=True)
        thread.start()
        self.assertTrue(entered.wait(timeout=1.0))
        try:
            first = read_service_status(self.runtime_dir, "worker-recovery-daemon")
            first_updated_at = str(first["updated_at"])
            time.sleep(0.6)
            second = read_service_status(self.runtime_dir, "worker-recovery-daemon")
            self.assertEqual(second["status"], "running")
            self.assertEqual(second["cycle_state"], "running_callback")
            self.assertNotEqual(str(second["updated_at"]), first_updated_at)
            self.assertLess(float(second["heartbeat_age_seconds"]), float(second["heartbeat_timeout_seconds"]))
        finally:
            release.set()
            thread.join(timeout=2.0)

    def test_service_merges_static_callback_payload_into_callback(self) -> None:
        calls: list[dict] = []

        def callback(payload: dict) -> dict:
            calls.append(payload)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            callback_payload={"job_id": "job-123"},
            poll_seconds=0.1,
        )
        service.run_forever(max_ticks=1)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["job_id"], "job-123")
        self.assertEqual(calls[0]["owner_id"], service.owner_id)

    def test_render_systemd_unit_contains_service_configuration(self) -> None:
        unit = render_systemd_unit(
            project_root="/tmp/sourcing-ai-agent",
            service_name="worker-recovery-daemon",
            poll_seconds=7.5,
            lease_seconds=240,
            stale_after_seconds=150,
            total_limit=6,
            user_name="svcuser",
        )
        self.assertIn("User=svcuser", unit)
        self.assertIn("WorkingDirectory=/tmp/sourcing-ai-agent", unit)
        self.assertIn("--poll-seconds 7.5", unit)
        self.assertIn("--lease-seconds 240", unit)
        self.assertIn("--stale-after-seconds 150", unit)
        self.assertIn("--total-limit 6", unit)

    def test_service_status_retains_last_nonempty_summary_and_cumulative_totals(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {
                        "claimed_count": 1,
                        "executed_count": 1,
                        "recoverable_count": 2,
                        "jobs": [
                            {
                                "job_id": "job-123",
                                "claimed_count": 1,
                                "executed_count": 1,
                                "backlog_count": 3,
                            }
                        ],
                    },
                    "workflow_resume": [{"job_id": "job-123", "status": "resumed"}],
                },
                {
                    "status": "completed",
                    "daemon": {
                        "claimed_count": 0,
                        "executed_count": 0,
                        "recoverable_count": 0,
                        "jobs": [],
                    },
                    "workflow_resume": [],
                },
            ]
        )

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: next(callbacks),  # noqa: ARG005
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )
        summary = service.run_forever(max_ticks=2)

        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["tick"], 2)
        self.assertEqual(summary["last_summary"]["daemon"]["claimed_count"], 0)
        self.assertEqual(summary["last_nonempty_summary"]["daemon"]["claimed_count"], 1)
        self.assertEqual(summary["last_nonempty_summary"]["daemon"]["executed_count"], 1)
        self.assertEqual(summary["last_nonempty_tick"], 1)
        self.assertTrue(summary["last_nonempty_at"])
        self.assertEqual(summary["activity_summary_source"], "recent")
        self.assertEqual(summary["activity_summary"]["daemon"]["claimed_count"], 1)
        self.assertFalse(summary["last_nonempty_summary_is_historical"])

        cumulative = summary["cumulative_summary"]
        self.assertEqual(cumulative["tick_count"], 2)
        self.assertEqual(cumulative["active_tick_count"], 1)
        self.assertEqual(cumulative["total_claimed_count"], 1)
        self.assertEqual(cumulative["total_executed_count"], 1)
        self.assertEqual(cumulative["max_recoverable_count"], 2)
        self.assertEqual(cumulative["workflow_resume_status_counts"], {"resumed": 1})
        self.assertEqual(
            cumulative["job_totals"],
            {
                "job-123": {
                    "claimed_count": 1,
                    "executed_count": 1,
                    "max_backlog_count": 3,
                }
            },
        )

    def test_read_service_status_marks_running_status_without_lock_as_stale(self) -> None:
        state_dir = self.runtime_dir / "services" / "worker-recovery-daemon"
        state_dir.mkdir(parents=True, exist_ok=True)
        status_path = state_dir / "status.json"
        status_path.write_text(
            """
{
  "service_name": "worker-recovery-daemon",
  "status": "running",
  "status_path": "__STATUS__",
  "lock_path": "__LOCK__"
}
""".replace("__STATUS__", str(status_path)).replace("__LOCK__", str(state_dir / "service.lock")),
            encoding="utf-8",
        )
        status = read_service_status(self.runtime_dir, "worker-recovery-daemon")

        self.assertEqual(status["status"], "stale")
        self.assertEqual(status["reported_status"], "running")
        self.assertEqual(status["lock_status"], "missing")

    def test_read_service_status_preserves_running_status_when_lock_is_held(self) -> None:
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: {"status": "completed", "daemon": payload},  # noqa: ARG005
            service_name="worker-recovery-daemon",
        )
        with service._acquire_lock():
            service._write_status(
                "running",
                tick=1,
                last_summary={},
                last_nonempty_summary={},
                last_nonempty_tick=0,
                last_nonempty_at="",
                cumulative_summary={
                    "tick_count": 1,
                    "active_tick_count": 0,
                    "total_claimed_count": 0,
                    "total_executed_count": 0,
                    "max_recoverable_count": 0,
                    "workflow_resume_status_counts": {},
                    "job_totals": {},
                },
            )
            status = read_service_status(self.runtime_dir, "worker-recovery-daemon")

        self.assertEqual(status["status"], "running")
        self.assertEqual(status["lock_status"], "locked")

    def test_read_service_status_moves_stale_last_nonempty_summary_to_historical_fields(self) -> None:
        state_dir = self.runtime_dir / "services" / "worker-recovery-daemon"
        state_dir.mkdir(parents=True, exist_ok=True)
        status_path = state_dir / "status.json"
        status_payload = {
            "service_name": "worker-recovery-daemon",
            "status": "running",
            "status_path": str(status_path),
            "lock_path": str(state_dir / "service.lock"),
            "poll_seconds": 5.0,
            "updated_at": "2026-04-13T02:00:00+00:00",
            "last_summary": {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}},
            "last_nonempty_summary": {
                "status": "completed",
                "daemon": {"claimed_count": 1, "executed_count": 1},
                "workflow_resume": [{"job_id": "job-1", "status": "resumed"}],
            },
            "last_nonempty_tick": 12,
            "last_nonempty_at": "2026-04-13T00:00:00+00:00",
            "cumulative_summary": {
                "tick_count": 12,
                "active_tick_count": 1,
                "total_claimed_count": 1,
                "total_executed_count": 1,
                "max_recoverable_count": 1,
                "workflow_resume_status_counts": {"resumed": 1},
                "job_totals": {},
            },
        }
        status_path.write_text(json.dumps(status_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        with mock.patch("sourcing_agent.service_daemon._heartbeat_age_seconds") as age_mock:
            def _fake_age(value):  # noqa: ANN001
                if str(value or "") == "2026-04-13T00:00:00+00:00":
                    return 7200.0
                return 1.0
            age_mock.side_effect = _fake_age
            status = read_service_status(self.runtime_dir, "worker-recovery-daemon")

        self.assertEqual(status["last_nonempty_summary"], {})
        self.assertEqual(status["activity_summary"], {})
        self.assertEqual(status["activity_summary_source"], "none")
        self.assertTrue(status["last_nonempty_summary_is_historical"])
        self.assertEqual(status["historical_last_nonempty_tick"], 12)
        self.assertEqual(status["historical_last_nonempty_summary"]["daemon"]["claimed_count"], 1)

    def test_service_skips_idle_sleep_when_previous_tick_had_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 1, "executed_count": 1, "recoverable_count": 1, "jobs": []},
                    "workflow_resume": [],
                },
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                },
            ]
        )
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: next(callbacks),  # noqa: ARG005
            service_name="worker-recovery-daemon",
            poll_seconds=5.0,
        )
        with mock.patch("sourcing_agent.service_daemon.time.sleep", return_value=None) as sleep_mock:
            summary = service.run_forever(max_ticks=2)

        self.assertEqual(summary["tick"], 2)
        sleep_mock.assert_not_called()
