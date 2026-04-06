import tempfile
import threading
import unittest
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
