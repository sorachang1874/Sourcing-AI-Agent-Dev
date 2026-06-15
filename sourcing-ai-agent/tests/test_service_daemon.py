import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.service_daemon import (
    SingleInstanceError,
    WorkerDaemonService,
    compact_service_status,
    read_service_status,
    read_service_stop_request,
    read_service_wakeup_request,
    render_systemd_unit,
    request_service_stop,
    request_service_wakeup,
)


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

    def test_service_status_compacts_domain_objects_in_callback_summary(self) -> None:
        class FakeDomainObject:
            def to_record(self) -> dict:
                return {"candidate_id": "cand-1", "display_name": "Ada Candidate"}

        def callback(payload: dict) -> dict:  # noqa: ARG001
            return {
                "status": "completed",
                "daemon": {"claimed_count": 1, "executed_count": 1},
                "jobs": [{"candidate": FakeDomainObject()}],
            }

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )

        service.run_forever(max_ticks=1)
        status = read_service_status(self.runtime_dir, "worker-recovery-daemon")
        serialized = json.dumps(status)

        self.assertEqual(status["last_summary_projection"], "compact_v1")
        self.assertEqual(status["last_summary"]["daemon"]["claimed_count"], 1)
        self.assertNotIn("jobs", status["last_summary"])
        self.assertNotIn("cand-1", serialized)

    def test_service_status_preserves_phase_metrics_without_large_payloads(self) -> None:
        large_profile_urls = [f"https://www.linkedin.com/in/candidate-{index}" for index in range(500)]

        def callback(payload: dict) -> dict:  # noqa: ARG001
            return {
                "status": "completed",
                "daemon": {"claimed_count": 1, "executed_count": 1},
                "local_apply_backlog": {
                    "claimed_count": 1,
                    "completed_count": 1,
                    "candidate_count": 500,
                    "results": [{"profile_urls": large_profile_urls}],
                },
                "recovery_phase_metrics": {
                    "local_apply_backlog": {
                        "phase": "local_apply_backlog",
                        "owner": "local_apply_closure_queue",
                        "status": "active",
                        "elapsed_ms": 1234,
                        "counts": {
                            "claimed_count": 1,
                            "completed_count": 1,
                            "candidate_count": 500,
                            "profile_urls": large_profile_urls,
                        },
                        "results": [{"profile_urls": large_profile_urls}],
                    }
                },
            }

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )

        service.run_forever(max_ticks=1)
        status = read_service_status(self.runtime_dir, "worker-recovery-daemon")
        serialized = json.dumps(status)
        phase = status["last_summary"]["recovery_phase_metrics"]["local_apply_backlog"]

        self.assertEqual(status["last_summary_projection"], "compact_v1")
        self.assertEqual(status["last_summary"]["local_apply_backlog"]["candidate_count"], 500)
        self.assertEqual(phase["counts"]["candidate_count"], 500)
        self.assertEqual(phase["elapsed_ms"], 1234)
        self.assertNotIn("profile_urls", serialized)
        self.assertLess(len(serialized), 10000)

    def test_service_preserves_zero_stale_after_seconds_in_status_and_callback(self) -> None:
        calls: list[dict] = []

        def callback(payload: dict) -> dict:
            calls.append(payload)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-zero-stale",
            poll_seconds=0.1,
            stale_after_seconds=0,
        )

        service.run_forever(max_ticks=1)
        status = read_service_status(self.runtime_dir, "job-recovery-zero-stale")

        self.assertEqual(calls[0]["stale_after_seconds"], 0)
        self.assertEqual(status["stale_after_seconds"], 0)
        self.assertEqual(status["callback_payload"]["stale_after_seconds"], 0)

    def test_job_recovery_workflow_open_count_keeps_job_scoped_sidecar_alive(self) -> None:
        calls = 0

        def callback(payload: dict) -> dict:  # noqa: ARG001
            nonlocal calls
            calls += 1
            return {
                "status": "completed",
                "daemon": {"claimed_count": 0, "executed_count": 0},
                "job_recovery_open_work": {
                    "open_work_count": 1,
                    "daemon_owned_open_work_count": 0,
                    "workflow_open_count": 1,
                    "job_terminal": False,
                },
            }

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-workflow-open",
            poll_seconds=0.1,
            idle_stop_ticks=1,
        )

        summary = service.run_forever(max_ticks=3)
        status = read_service_status(self.runtime_dir, "job-recovery-workflow-open")

        self.assertEqual(calls, 3)
        self.assertEqual(summary["tick"], 3)
        self.assertFalse(status["current_activity_has_work"])
        self.assertEqual(status["last_nonempty_tick"], 0)
        self.assertEqual(status["idle_tick_count"], 0)
        self.assertEqual(status["cumulative_summary"]["active_tick_count"], 0)
        self.assertEqual(status["last_summary"]["job_recovery_open_work"]["workflow_open_count"], 1)
        self.assertEqual(status["last_summary"]["job_recovery_open_work"]["daemon_owned_open_work_count"], 0)

    def test_job_recovery_workflow_open_keepalive_sleeps_between_ticks(self) -> None:
        calls = 0

        def callback(payload: dict) -> dict:  # noqa: ARG001
            nonlocal calls
            calls += 1
            return {
                "status": "completed",
                "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                "workflow_resume": [],
                "job_recovery_open_work": {
                    "status": "active",
                    "open_work_count": 1,
                    "daemon_owned_open_work_count": 0,
                    "workflow_open_count": 1,
                    "job_terminal": False,
                },
            }

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-workflow-open-sleep",
            poll_seconds=5.0,
            idle_stop_ticks=1,
        )
        sleep_calls = 0

        def tracked_sleep() -> bool:
            nonlocal sleep_calls
            sleep_calls += 1
            return False

        with mock.patch.object(service, "_sleep_until_next_tick", side_effect=tracked_sleep):
            summary = service.run_forever(max_ticks=2)

        self.assertEqual(calls, 2)
        self.assertEqual(summary["tick"], 2)
        self.assertEqual(summary["idle_tick_count"], 0)
        self.assertEqual(sleep_calls, 1)

    def test_compact_service_status_replaces_large_activity_payloads_with_counters(self) -> None:
        raw_status = {
            "service_name": "worker-recovery-daemon",
            "status": "running",
            "pid": 12345,
            "pid_alive": True,
            "lock_status": "locked",
            "last_summary": {
                "status": "completed",
                "daemon": {"recoverable_count": 3, "claimed_count": 2, "executed_count": 1},
                "jobs": [
                    {
                        "job_id": "job-large",
                        "candidate": {"candidate_id": "cand-large", "profile": "x" * 10000},
                    }
                ],
                "workflow_resume": [{"job_id": "job-large"}],
                "post_completion_reconcile": [{"job_id": "job-large"}],
            },
            "activity_summary": {
                "status": "completed",
                "daemon": {"recoverable_count": 1, "claimed_count": 1, "executed_count": 1},
                "jobs": [{"candidate": {"profile": "y" * 10000}}],
            },
            "cumulative_summary": {
                "tick_count": 4,
                "active_tick_count": 2,
                "job_totals": {"job-large": {"claimed_count": 2}},
            },
        }

        compact = compact_service_status(raw_status)
        serialized = json.dumps(compact)

        self.assertEqual(compact["status"], "running")
        self.assertEqual(compact["last_summary"]["daemon_claimed_count"], 2)
        self.assertEqual(compact["last_summary"]["workflow_resume_count"], 1)
        self.assertEqual(compact["activity_summary"]["daemon_claimed_count"], 1)
        self.assertEqual(compact["cumulative_summary"]["job_total_count"], 1)
        self.assertNotIn("cand-large", serialized)
        self.assertNotIn("x" * 100, serialized)
        self.assertNotIn("jobs", compact["last_summary"])

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

    def test_service_accepts_persisted_cooperative_stop_request(self) -> None:
        ticks: list[int] = []
        first_tick_done = threading.Event()

        def callback(payload: dict) -> dict:  # noqa: ARG001
            ticks.append(len(ticks) + 1)
            first_tick_done.set()
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.2,
        )
        thread = threading.Thread(target=lambda: service.run_forever(), daemon=True)
        thread.start()
        self.assertTrue(first_tick_done.wait(timeout=1.0))

        running_status = read_service_status(self.runtime_dir, "worker-recovery-daemon")
        stop_request = request_service_stop(
            self.runtime_dir,
            "worker-recovery-daemon",
            reason="test stop",
            requested_by="unit-test",
            target_status=running_status,
        )
        self.assertEqual(stop_request["status"], "requested")
        self.assertEqual(read_service_stop_request(self.runtime_dir, "worker-recovery-daemon")["status"], "requested")

        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        stopped_status = read_service_status(self.runtime_dir, "worker-recovery-daemon")
        self.assertEqual(stopped_status["status"], "stopped")
        self.assertFalse(stopped_status["stop_requested"])
        self.assertLessEqual(len(ticks), 2)

    def test_service_ignores_stale_shutdown_fence_from_previous_process(self) -> None:
        service_dir = self.runtime_dir / "services" / "worker-recovery-daemon"
        service_dir.mkdir(parents=True, exist_ok=True)
        (service_dir / "stop_request.json").write_text(
            json.dumps(
                {
                    "status": "requested",
                    "service_name": "worker-recovery-daemon",
                    "requested_at": "2000-01-01T00:00:00+00:00",
                    "requested_by": "previous-cleanup",
                    "reason": "isolated_hosted_test_runtime_cleanup",
                    "target_pid": 999999,
                    "target_owner_id": "old-owner",
                    "target_started_at": "2000-01-01T00:00:00+00:00",
                    "target_status": "running",
                    "target_scope": "service_shutdown_fence",
                }
            ),
            encoding="utf-8",
        )
        calls: list[dict] = []

        def callback(payload: dict) -> dict:
            calls.append(payload)
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="worker-recovery-daemon",
            poll_seconds=0.1,
        )
        summary = service.run_forever(max_ticks=1)

        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(len(calls), 1)
        self.assertEqual(read_service_stop_request(self.runtime_dir, "worker-recovery-daemon")["status"], "not_requested")

    def test_service_wakeup_request_interrupts_idle_poll_sleep(self) -> None:
        ticks: list[float] = []
        first_tick_done = threading.Event()
        second_tick_done = threading.Event()

        def callback(payload: dict) -> dict:  # noqa: ARG001
            ticks.append(time.monotonic())
            if len(ticks) == 1:
                first_tick_done.set()
            if len(ticks) == 2:
                second_tick_done.set()
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-wakeup",
            poll_seconds=5.0,
        )
        thread = threading.Thread(target=lambda: service.run_forever(max_ticks=2), daemon=True)
        thread.start()
        self.assertTrue(first_tick_done.wait(timeout=1.0))

        request_service_wakeup(
            self.runtime_dir,
            "job-recovery-wakeup",
            reason="remote_provider_event",
            requested_by="unit-test",
        )

        self.assertTrue(second_tick_done.wait(timeout=1.0))
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(ticks), 2)
        self.assertLess(ticks[1] - ticks[0], 2.0)
        self.assertEqual(read_service_wakeup_request(self.runtime_dir, "job-recovery-wakeup")["status"], "not_requested")

    def test_service_wakeup_payload_scopes_next_callback_tick(self) -> None:
        payloads: list[dict] = []
        first_tick_done = threading.Event()
        second_tick_done = threading.Event()

        def callback(payload: dict) -> dict:
            payloads.append(dict(payload))
            if len(payloads) == 1:
                first_tick_done.set()
            if len(payloads) == 2:
                second_tick_done.set()
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-targeted-wakeup",
            poll_seconds=5.0,
            callback_payload={
                "job_id": "job-targeted",
                "search_seed_discovery_enabled": True,
                "profile_prefetch_refill_before_worker_recovery": True,
            },
        )
        thread = threading.Thread(target=lambda: service.run_forever(max_ticks=2), daemon=True)
        thread.start()
        self.assertTrue(first_tick_done.wait(timeout=1.0))

        request_service_wakeup(
            self.runtime_dir,
            "job-recovery-targeted-wakeup",
            reason="remote_provider_event",
            requested_by="unit-test",
            callback_payload={
                "explicit_worker_ids": [101, 102],
                "remote_provider_event_worker_ids": [102, 103],
                "total_limit": 3,
                "search_seed_discovery_enabled": False,
                "profile_prefetch_refill_before_worker_recovery": False,
            },
        )

        self.assertTrue(second_tick_done.wait(timeout=1.0))
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(payloads[0].get("search_seed_discovery_enabled"), True)
        self.assertEqual(payloads[1]["job_id"], "job-targeted")
        self.assertEqual(payloads[1]["explicit_worker_ids"], [101, 102, 103])
        self.assertEqual(payloads[1]["remote_provider_event_worker_ids"], [102, 103])
        self.assertEqual(payloads[1]["total_limit"], 3)
        self.assertEqual(payloads[1]["search_seed_discovery_enabled"], False)
        self.assertEqual(payloads[1]["profile_prefetch_refill_before_worker_recovery"], False)

    def test_existing_service_wakeup_payload_scopes_first_callback_tick(self) -> None:
        request_service_wakeup(
            self.runtime_dir,
            "job-recovery-startup-wakeup",
            reason="remote_provider_event",
            requested_by="unit-test",
            callback_payload={
                "explicit_worker_ids": [201, 202],
                "remote_provider_event_worker_ids": [202],
                "total_limit": 2,
                "profile_prefetch_refill_before_worker_recovery": True,
            },
        )
        payloads: list[dict] = []

        def callback(payload: dict) -> dict:
            payloads.append(dict(payload))
            return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-startup-wakeup",
            poll_seconds=5.0,
            callback_payload={"job_id": "job-startup-wakeup"},
        )

        service.run_forever(max_ticks=1)

        self.assertEqual(read_service_wakeup_request(self.runtime_dir, "job-recovery-startup-wakeup")["status"], "not_requested")
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["job_id"], "job-startup-wakeup")
        self.assertEqual(payloads[0]["explicit_worker_ids"], [201, 202])
        self.assertEqual(payloads[0]["remote_provider_event_worker_ids"], [202])
        self.assertEqual(payloads[0]["total_limit"], 2)
        self.assertEqual(payloads[0]["profile_prefetch_refill_before_worker_recovery"], True)

    def test_service_wakeup_payload_raises_limit_for_merged_event_workers(self) -> None:
        with mock.patch.dict(os.environ, {"WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT": "4"}, clear=False):
            request_service_wakeup(
                self.runtime_dir,
                "job-recovery-targeted-wakeup-merge",
                reason="remote_provider_event",
                requested_by="unit-test",
                callback_payload={
                    "explicit_worker_ids": [101],
                    "remote_provider_event_worker_ids": [101],
                    "total_limit": 1,
                },
            )
            request_service_wakeup(
                self.runtime_dir,
                "job-recovery-targeted-wakeup-merge",
                reason="remote_provider_event",
                requested_by="unit-test",
                callback_payload={
                    "remote_provider_event_worker_ids": [102, 103],
                    "total_limit": 1,
                },
            )

        wakeup = read_service_wakeup_request(self.runtime_dir, "job-recovery-targeted-wakeup-merge")
        self.assertEqual(wakeup["callback_payload"]["explicit_worker_ids"], [101, 102, 103])
        self.assertEqual(wakeup["callback_payload"]["remote_provider_event_worker_ids"], [101, 102, 103])
        self.assertEqual(wakeup["callback_payload"]["total_limit"], 3)

    def test_service_drains_merged_event_workers_across_bounded_burst_ticks(self) -> None:
        payloads: list[dict] = []
        first_tick_done = threading.Event()
        third_tick_done = threading.Event()

        def callback(payload: dict) -> dict:
            payloads.append(dict(payload))
            if len(payloads) == 1:
                first_tick_done.set()
            if len(payloads) == 3:
                third_tick_done.set()
            explicit_worker_ids = list(payload.get("explicit_worker_ids") or [])
            claimed = 1 if explicit_worker_ids and explicit_worker_ids != [999] else 0
            return {"status": "completed", "daemon": {"claimed_count": claimed, "executed_count": claimed}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-small-step-wakeup",
            poll_seconds=5.0,
            total_limit=1,
            callback_payload={
                "job_id": "job-targeted",
                "explicit_worker_ids": [999],
            },
        )
        thread = threading.Thread(target=lambda: service.run_forever(max_ticks=3), daemon=True)
        thread.start()
        self.assertTrue(first_tick_done.wait(timeout=1.0))

        with mock.patch.dict(os.environ, {"WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT": "4"}, clear=False):
            request_service_wakeup(
                self.runtime_dir,
                "job-recovery-small-step-wakeup",
                reason="remote_provider_event",
                requested_by="unit-test",
                callback_payload={
                    "remote_provider_event_worker_ids": [101, 102, 103, 104, 105],
                    "total_limit": 1,
                },
            )

        self.assertTrue(third_tick_done.wait(timeout=1.0))
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(payloads[1]["explicit_worker_ids"], [101, 102, 103, 104])
        self.assertEqual(payloads[1]["remote_provider_event_worker_ids"], [101, 102, 103, 104])
        self.assertEqual(payloads[1]["total_limit"], 4)
        self.assertEqual(payloads[2]["explicit_worker_ids"], [105])
        self.assertEqual(payloads[2]["remote_provider_event_worker_ids"], [105])
        self.assertEqual(payloads[2]["total_limit"], 4)

    def test_cleanup_shutdown_fence_stops_new_service_generation(self) -> None:
        request_service_stop(
            self.runtime_dir,
            "job-recovery-job123",
            reason="isolated_hosted_test_runtime_cleanup",
            requested_by="scripted_test_runtime",
            target_scope="service_shutdown_fence",
        )
        calls: list[dict] = []

        def callback(payload: dict) -> dict:
            calls.append(payload)
            return {"status": "completed", "daemon": {"claimed_count": 1, "executed_count": 1}}

        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=callback,
            service_name="job-recovery-job123",
            poll_seconds=0.05,
        )

        status = service.run_forever(max_ticks=5)

        self.assertEqual(calls, [])
        self.assertEqual(status["status"], "stopped")
        self.assertTrue(status["stop_requested"])
        self.assertEqual(
            read_service_stop_request(self.runtime_dir, "job-recovery-job123").get("target_scope"),
            "service_shutdown_fence",
        )

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

    def test_render_systemd_unit_carries_pg_only_env_when_dsn_is_present(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://svc@127.0.0.1:5432/sourcing_agent",
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "prod_app",
                "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
            },
            clear=False,
        ):
            unit = render_systemd_unit(
                project_root="/tmp/sourcing-ai-agent",
                service_name="worker-recovery-daemon",
            )

        self.assertIn(
            "Environment=SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://svc@127.0.0.1:5432/sourcing_agent",
            unit,
        )
        self.assertIn("Environment=SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only", unit)
        self.assertIn("Environment=SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=prod_app", unit)
        self.assertIn("Environment=SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1", unit)
        self.assertIn("Environment=SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory", unit)

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
                    "profile_prefetch_refill": {
                        "dispatched_url_count": 3,
                        "queued_worker_count": 1,
                    },
                    "local_apply_backlog": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
                    "board_visible_apply": {
                        "claimed_count": 2,
                        "completed_count": 1,
                    },
                    "snapshot_full_materialization": {
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
                    "excel_intake_recovery": {
                        "recovered_count": 1,
                    },
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
        self.assertEqual(cumulative["total_profile_refill_dispatched_url_count"], 3)
        self.assertEqual(cumulative["total_profile_refill_queued_worker_count"], 1)
        self.assertEqual(cumulative["total_local_apply_backlog_claimed_count"], 1)
        self.assertEqual(cumulative["total_local_apply_backlog_completed_count"], 1)
        self.assertEqual(cumulative["total_board_visible_apply_claimed_count"], 2)
        self.assertEqual(cumulative["total_board_visible_apply_completed_count"], 1)
        self.assertEqual(cumulative["total_snapshot_full_materialization_claimed_count"], 1)
        self.assertEqual(cumulative["total_snapshot_full_materialization_completed_count"], 1)
        self.assertEqual(cumulative["total_excel_intake_recovered_count"], 1)
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

    def test_service_treats_profile_refill_dispatch_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "profile_prefetch_refill": {
                        "dispatched_url_count": 2,
                        "queued_worker_count": 1,
                    },
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

        self.assertEqual(summary["last_nonempty_summary"]["profile_prefetch_refill"]["dispatched_url_count"], 2)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_profile_refill_dispatched_url_count"], 2)
        sleep_mock.assert_not_called()

    def test_job_scoped_service_idles_when_only_live_workflow_owner_remains(self) -> None:
        summary_payload = {
            "status": "completed",
            "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
            "workflow_resume": [{"status": "skipped", "reason": "job_not_resumable", "job_id": "job-live-owner"}],
            "job_recovery_open_work": {
                "status": "active",
                "reason": "job_scope_open_work",
                "job_id": "job-live-owner",
                "job_status": "running",
                "job_stage": "retrieving",
                "job_terminal": False,
                "open_work_count": 1,
                "daemon_owned_open_work_count": 0,
                "non_daemon_open_work_count": 1,
                "workflow_open_count": 1,
                "workflow_lease_alive": True,
                "workflow_resume_actionable": False,
                "pending_worker_count": 0,
                "profile_refill_open_item_count": 0,
                "materialization_open_item_count": 0,
            },
        }
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: dict(summary_payload),  # noqa: ARG005
            service_name="job-recovery-live-owner",
            poll_seconds=5.0,
            idle_stop_ticks=2,
        )
        with mock.patch("sourcing_agent.service_daemon.time.sleep", return_value=None):
            summary = service.run_forever(max_ticks=10)

        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["tick"], 2)
        self.assertEqual(summary["idle_tick_count"], 2)
        self.assertEqual(summary["last_nonempty_summary"], {})
        self.assertEqual(summary["last_summary"]["job_recovery_open_work"]["workflow_open_count"], 1)
        self.assertFalse(summary["last_summary"]["job_recovery_open_work"]["workflow_resume_actionable"])

    def test_service_treats_board_visible_apply_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "board_visible_apply": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
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

        self.assertEqual(summary["last_nonempty_summary"]["board_visible_apply"]["completed_count"], 1)
        self.assertEqual(summary["last_nonempty_summary"]["board_visible_apply"]["executed_command_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_board_visible_apply_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_board_visible_apply_completed_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_board_visible_patch_publish_command_owner_executed_count"],
            1,
        )
        sleep_mock.assert_not_called()

    def test_service_treats_local_apply_backlog_drain_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "local_apply_backlog": {
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
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

        self.assertEqual(summary["last_nonempty_summary"]["local_apply_backlog"]["completed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_local_apply_backlog_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_local_apply_backlog_completed_count"], 1)
        sleep_mock.assert_not_called()

    def test_service_treats_local_profile_delta_apply_command_owner_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "local_apply_backlog": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
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

        self.assertEqual(summary["last_nonempty_summary"]["local_apply_backlog"]["executed_command_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_local_profile_delta_apply_command_owner_executed_count"],
            1,
        )
        sleep_mock.assert_not_called()

    def test_service_treats_profile_url_terminal_record_owner_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "profile_url_terminal_record_command_owner": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "recorded_count": 2,
                        "fetched_count": 1,
                        "failed_count": 1,
                    },
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

        owner = summary["last_nonempty_summary"]["profile_url_terminal_record_command_owner"]
        self.assertEqual(owner["recorded_count"], 2)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_profile_url_terminal_record_command_owner_executed_count"],
            1,
        )
        self.assertEqual(summary["cumulative_summary"]["total_profile_url_terminal_recorded_count"], 2)
        sleep_mock.assert_not_called()

    def test_service_treats_projection_person_search_index_owner_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "projection_person_search_index": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                        "candidate_count": 2,
                        "indexed_count": 2,
                    },
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

        owner = summary["last_nonempty_summary"]["projection_person_search_index"]
        self.assertEqual(owner["completed_count"], 1)
        self.assertEqual(owner["executed_command_count"], 1)
        self.assertEqual(owner["indexed_count"], 2)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_projection_person_search_index_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_projection_person_search_index_completed_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_projection_person_search_index_build_command_owner_executed_count"],
            1,
        )
        self.assertEqual(summary["cumulative_summary"]["total_projection_person_search_index_indexed_count"], 2)
        sleep_mock.assert_not_called()

    def test_service_treats_run_scope_projection_finalize_owner_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "run_scope_projection_finalize": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                        "candidate_count": 2,
                    },
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

        owner = summary["last_nonempty_summary"]["run_scope_projection_finalize"]
        self.assertEqual(owner["completed_count"], 1)
        self.assertEqual(owner["executed_command_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_run_scope_projection_finalize_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_run_scope_projection_finalize_completed_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_run_scope_projection_finalize_command_owner_executed_count"],
            1,
        )
        self.assertEqual(summary["cumulative_summary"]["total_run_scope_projection_finalize_candidate_count"], 2)
        sleep_mock.assert_not_called()

    def test_service_treats_collection_authoritative_merge_owner_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "collection_authoritative_merge": {
                        "command_count": 1,
                        "executed_command_count": 1,
                        "claimed_count": 1,
                        "completed_count": 1,
                        "candidate_count": 2,
                    },
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

        owner = summary["last_nonempty_summary"]["collection_authoritative_merge"]
        self.assertEqual(owner["completed_count"], 1)
        self.assertEqual(owner["executed_command_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_collection_authoritative_merge_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_collection_authoritative_merge_completed_count"], 1)
        self.assertEqual(
            summary["cumulative_summary"]["total_collection_authoritative_merge_command_owner_executed_count"],
            1,
        )
        sleep_mock.assert_not_called()

    def test_service_treats_snapshot_full_materialization_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "snapshot_full_materialization": {
                        "claimed_count": 1,
                        "completed_count": 1,
                    },
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

        self.assertEqual(
            summary["last_nonempty_summary"]["snapshot_full_materialization"]["completed_count"],
            1,
        )
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_snapshot_full_materialization_claimed_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_snapshot_full_materialization_completed_count"], 1)
        sleep_mock.assert_not_called()

    def test_service_treats_excel_intake_recovery_as_activity(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "excel_intake_recovery": {
                        "recovered_count": 1,
                    },
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

        self.assertEqual(summary["last_nonempty_summary"]["excel_intake_recovery"]["recovered_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["total_excel_intake_recovered_count"], 1)
        sleep_mock.assert_not_called()

    def test_service_treats_job_scoped_actionable_open_work_as_activity_and_stops_after_quiescence(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "active",
                        "open_work_count": 3,
                        "daemon_owned_open_work_count": 3,
                        "pending_worker_count": 1,
                        "profile_refill_open_item_count": 2,
                        "profile_refill_ready_item_count": 2,
                    },
                },
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "idle",
                        "open_work_count": 0,
                    },
                },
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "idle",
                        "open_work_count": 0,
                    },
                },
            ]
        )
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: next(callbacks),  # noqa: ARG005
            service_name="job-recovery-open-work",
            poll_seconds=5.0,
            idle_stop_ticks=2,
        )
        summary = service.run_forever(max_ticks=10)

        self.assertEqual(summary["tick"], 3)
        self.assertEqual(summary["idle_stop_ticks"], 2)
        self.assertEqual(summary["idle_tick_count"], 2)
        self.assertEqual(summary["last_nonempty_summary"]["job_recovery_open_work"]["open_work_count"], 3)
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 1)
        self.assertEqual(summary["cumulative_summary"]["max_job_recovery_open_work_count"], 3)

    def test_service_does_not_keep_sidecar_alive_for_provider_owned_profile_refill_tail(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "active",
                        "open_work_count": 501,
                        "daemon_owned_open_work_count": 0,
                        "workflow_open_count": 0,
                        "pending_worker_count": 0,
                        "profile_refill_open_item_count": 500,
                        "profile_refill_ready_item_count": 0,
                    },
                },
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "active",
                        "open_work_count": 501,
                        "daemon_owned_open_work_count": 0,
                        "workflow_open_count": 0,
                        "pending_worker_count": 0,
                        "profile_refill_open_item_count": 500,
                        "profile_refill_ready_item_count": 0,
                    },
                },
            ]
        )
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: next(callbacks),  # noqa: ARG005
            service_name="job-recovery-provider-owned-refill-tail",
            poll_seconds=5.0,
            idle_stop_ticks=2,
        )
        summary = service.run_forever(max_ticks=10)

        self.assertEqual(summary["tick"], 2)
        self.assertEqual(summary["idle_tick_count"], 2)
        self.assertEqual(summary["last_nonempty_summary"], {})
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 0)
        self.assertEqual(summary["cumulative_summary"]["max_job_recovery_open_work_count"], 501)

    def test_service_does_not_keep_sidecar_alive_for_non_daemon_non_workflow_open_work(self) -> None:
        callbacks = iter(
            [
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "active",
                        "open_work_count": 1,
                        "daemon_owned_open_work_count": 0,
                        "workflow_open_count": 0,
                        "materialization_open_item_count": 1,
                    },
                },
                {
                    "status": "completed",
                    "daemon": {"claimed_count": 0, "executed_count": 0, "recoverable_count": 0, "jobs": []},
                    "workflow_resume": [],
                    "job_recovery_open_work": {
                        "status": "active",
                        "open_work_count": 1,
                        "daemon_owned_open_work_count": 0,
                        "workflow_open_count": 0,
                        "materialization_open_item_count": 1,
                    },
                },
            ]
        )
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: next(callbacks),  # noqa: ARG005
            service_name="job-recovery-non-owned-open-work",
            poll_seconds=5.0,
            idle_stop_ticks=2,
        )
        summary = service.run_forever(max_ticks=10)

        self.assertEqual(summary["tick"], 2)
        self.assertEqual(summary["idle_tick_count"], 2)
        self.assertEqual(summary["last_nonempty_summary"], {})
        self.assertEqual(summary["cumulative_summary"]["active_tick_count"], 0)
        self.assertEqual(summary["cumulative_summary"]["max_job_recovery_open_work_count"], 1)

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
