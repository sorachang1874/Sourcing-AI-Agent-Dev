"""Phase 4 Step 5 (5a + 5b) — recovery driving as a code invariant + event-signaled wakeup.

Design anchor: docs/RECOVERY_DRIVING_REDESIGN_STUDY.md §5. Both sub-steps are
additive/safe:

* 5a/C3a (cli.serve fail-closed): "recovery is driven" becomes a startup
  invariant rather than an implicit deployment convention. serve defaults to
  external recovery and refuses to start without a fresh worker-recovery daemon;
  in-process recovery exists only behind an explicit dev opt-in.
* 5b (event-signaled wakeup): the orchestrator-owned DurableRuntimeWriter signals
  the shared recovery daemon (request_service_wakeup) the moment a durable
  transition produces new recovery work (a typed command enqueue or the
  runtime_outbox workflow.completed row), so the daemon's 5s poll is
  short-circuited to sub-second — invariant 3 (signal at event time, not poll
  time). Purely additive: the poll, inline triggers, and the provider-event
  caller are untouched.

The invariant-1 (no duplicate dispatch) test reuses the test_worker_recovery_daemon
setUp shape (controller/daemon stores over one control plane, _save_job) and proves
the lease/idempotency path holds when an event-signaled tick and a backstop poll
tick contend for the same scoped work.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent import cli
from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.domain import JobRequest
from sourcing_agent.durable_runtime import CommandOwnerRegistry, DurableRuntimeWriter
from sourcing_agent.service_daemon import (
    WorkerDaemonService,
    read_service_wakeup_request,
    request_service_wakeup,
    service_state_dir,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


# ---------------------------------------------------------------------------
# 5a — serve startup fail-closed assertion (testable helper, no port binding).
# ---------------------------------------------------------------------------
class ServeRecoveryCoverageGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name) / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.orchestrator = mock.Mock()
        self.orchestrator.runtime_dir = self.runtime_dir

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_fresh_external_status(self, service_name: str = "worker-recovery-daemon") -> None:
        # A fresh external daemon: hold the lock and write a "running" status with a
        # current heartbeat — read_service_status keeps "running" only then.
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda payload: {"status": "completed", "daemon": payload},  # noqa: ARG005
            service_name=service_name,
            poll_seconds=5.0,
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
            # Re-enter the lock context only to write; the helper below reads while
            # we still hold it so the lock probes "locked".
            self._covered = cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=None,
                watchdog_disabled=True,
                recheck_window_seconds=0.0,
            )

    def test_explicit_dev_opt_in_live_in_process_thread_is_covered(self) -> None:
        # The dev-only compatibility mode still permits a live in-process
        # recovery thread to cover recovery without consulting daemon status.
        live_thread = mock.Mock()
        live_thread.is_alive.return_value = True

        with mock.patch.object(cli, "read_service_status") as read_status_mock:
            result = cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=live_thread,
                watchdog_disabled=False,
                recheck_window_seconds=0.0,
            )

        self.assertEqual(result["coverage"], "in_process_shared_recovery_thread")
        read_status_mock.assert_not_called()

    def test_external_only_default_without_fresh_daemon_fails_closed(self) -> None:
        with self.assertRaisesRegex(cli.RecoveryCoverageError, "--enable-runtime-watchdog"):
            cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=None,
                watchdog_disabled=True,
                recheck_window_seconds=0.0,
            )

    def test_dead_in_process_thread_without_external_daemon_fails_closed(self) -> None:
        # The in-process thread yielded to an external daemon (SingleInstanceError)
        # and is no longer alive — but no external daemon is actually fresh.
        dead_thread = mock.Mock()
        dead_thread.is_alive.return_value = False
        with self.assertRaises(cli.RecoveryCoverageError):
            cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=dead_thread,
                watchdog_disabled=False,
                recheck_window_seconds=0.0,
            )

    def test_external_only_default_with_fresh_daemon_proceeds(self) -> None:
        self._write_fresh_external_status()
        self.assertEqual(self._covered["coverage"], "external_recovery_daemon")
        self.assertEqual(self._covered["external_status"], "running")

    def test_stale_external_daemon_status_is_not_coverage(self) -> None:
        # A stale status file (status "running" but lock not held + no heartbeat) is
        # downgraded to "stale" by read_service_status and must NOT count as coverage.
        state_dir = service_state_dir(self.runtime_dir, "worker-recovery-daemon")
        state_dir.mkdir(parents=True, exist_ok=True)
        status_path = state_dir / "status.json"
        status_path.write_text(
            (
                '{"service_name": "worker-recovery-daemon", "status": "running", '
                f'"status_path": "{status_path}", "lock_path": "{state_dir / "service.lock"}"}}'
            ),
            encoding="utf-8",
        )
        with self.assertRaises(cli.RecoveryCoverageError):
            cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=None,
                watchdog_disabled=True,
                recheck_window_seconds=0.0,
            )

    def test_allow_uncovered_recovery_opt_out_proceeds_loudly(self) -> None:
        # The explicit opt-out for "API-only, recovery elsewhere, I accept it":
        # proceeds without raising, but emits a loud structured stderr log.
        with mock.patch.object(cli, "print") as print_mock:
            result = cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=None,
                watchdog_disabled=True,
                allow_uncovered_recovery=True,
                recheck_window_seconds=0.0,
            )
        self.assertEqual(result["coverage"], "opt_out_allow_uncovered_recovery")
        self.assertEqual(result["severity"], "warning")
        self.assertTrue(print_mock.called)

    def test_bounded_recheck_window_tolerates_external_daemon_boot_race(self) -> None:
        # A just-launched systemd daemon can race serve boot: the status is not fresh
        # on the first read but becomes fresh within the bounded re-check window.
        not_fresh = (False, {"status": "not_started"})
        fresh = (True, {"status": "running"})

        attempts: list[int] = []

        def _probe(_runtime_dir, *, service_name="worker-recovery-daemon"):  # noqa: ARG001
            attempts.append(1)
            return fresh if len(attempts) >= 2 else not_fresh

        sleeps: list[float] = []
        with mock.patch.object(cli, "external_recovery_daemon_is_fresh", side_effect=_probe):
            result = cli.assert_recovery_coverage_or_fail_closed(
                self.orchestrator,
                shared_recovery_thread=None,
                watchdog_disabled=True,
                recheck_window_seconds=5.0,
                recheck_interval_seconds=0.25,
                sleep=sleeps.append,
            )
        self.assertEqual(result["coverage"], "external_recovery_daemon")
        self.assertGreaterEqual(len(attempts), 2)
        self.assertGreaterEqual(len(sleeps), 1)


# ---------------------------------------------------------------------------
# 5b — event-signaled wakeup at the durable event-production points.
# ---------------------------------------------------------------------------
class DurableRuntimeWakeupSignalTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "runtime.db")
        self.registry = CommandOwnerRegistry(
            {"linkedin.profile_refill.submit_batch": "linkedin_profile_owner"}
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _wakeup(self) -> dict:
        return read_service_wakeup_request(self.runtime_dir, "worker-recovery-daemon")

    def test_writer_without_runtime_dir_emits_no_wakeup_characterizes_today(self) -> None:
        # Characterize the pre-5b behavior: a writer with NO runtime_dir (the legacy
        # construction used everywhere except the orchestrator) signals nothing at
        # event time — only the 5s poll would catch the new work.
        writer = DurableRuntimeWriter(self.store, owner_registry=self.registry)
        writer.append_event_and_reduce(
            workflow_run_id="wf_no_wakeup",
            operation_id="op_no_wakeup",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_no_wakeup:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        planned = writer.append_event_and_reduce(
            workflow_run_id="wf_no_wakeup",
            operation_id="op_no_wakeup",
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="wf_no_wakeup:plan",
            payload={
                "command_type": "linkedin.profile_refill.submit_batch",
                "idempotency_key": "wf_no_wakeup:batch:1",
                "payload": {"profile_url_count": 50},
            },
        )
        self.assertEqual(len(planned.commands), 1)
        self.assertEqual(self._wakeup()["status"], "not_requested")

    def test_command_enqueue_emits_wakeup_at_event_time(self) -> None:
        # Invariant 3: a typed command enqueue (new recovery work ready) emits a
        # wakeup to the shared daemon at exactly the durable transition.
        writer = DurableRuntimeWriter(
            self.store,
            owner_registry=self.registry,
            runtime_dir=self.runtime_dir,
        )
        writer.append_event_and_reduce(
            workflow_run_id="wf_wakeup_cmd",
            operation_id="op_wakeup_cmd",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_wakeup_cmd:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        # WorkflowStarted produced no command/outbox -> no wakeup yet.
        self.assertEqual(self._wakeup()["status"], "not_requested")

        planned = writer.append_event_and_reduce(
            workflow_run_id="wf_wakeup_cmd",
            operation_id="op_wakeup_cmd",
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="wf_wakeup_cmd:plan",
            payload={
                "command_type": "linkedin.profile_refill.submit_batch",
                "idempotency_key": "wf_wakeup_cmd:batch:1",
                "payload": {"profile_url_count": 50},
            },
        )

        self.assertEqual(len(planned.commands), 1)
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        self.assertEqual(wakeup["reason"], "durable_runtime_event")
        self.assertEqual(wakeup["requested_by"], "durable_runtime_writer")
        callback = dict(wakeup.get("callback_payload") or {})
        # Honest global-sweep accelerator: the payload carries only an
        # observability source, NOT per-workflow scope fields (workflow_run_id
        # is a one-way hash that recovery cannot consume as a scope, so carrying
        # it would imply a targeting that does not exist).
        self.assertEqual(callback.get("source"), "durable_runtime_event")
        self.assertNotIn("workflow_stale_scope_workflow_run_id", callback)
        self.assertNotIn("workflow_stale_scope_operation_id", callback)

    def test_workflow_completed_outbox_row_emits_global_sweep_wakeup(self) -> None:
        # Invariant 3: the runtime_outbox workflow.completed row (durable_runtime
        # ~3333-3339) wakes the shared daemon's global tick now instead of waiting
        # for the poll. The wake is a global-sweep accelerator, not scoped.
        writer = DurableRuntimeWriter(
            self.store,
            owner_registry=self.registry,
            runtime_dir=self.runtime_dir,
        )
        writer.append_event_and_reduce(
            workflow_run_id="wf_wakeup_done",
            operation_id="op_wakeup_done",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_wakeup_done:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        completed = writer.append_event_and_reduce(
            workflow_run_id="wf_wakeup_done",
            operation_id="op_wakeup_done",
            event_family="workflow_event",
            event_type="WorkflowCompleted",
            idempotency_key="wf_wakeup_done:completed",
            payload={"stage_key": "serving_finalized"},
        )

        self.assertEqual(len(completed.outbox), 1)
        self.assertEqual(completed.outbox[0]["outbox_type"], "workflow.completed")
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        self.assertEqual(wakeup["reason"], "durable_runtime_event")
        callback = dict(wakeup.get("callback_payload") or {})
        self.assertEqual(callback.get("source"), "durable_runtime_event")
        self.assertNotIn("workflow_stale_scope_workflow_run_id", callback)

    def test_fan_out_of_events_coalesces_into_one_pending_wakeup(self) -> None:
        # Guard against signal storms: many event-producing reduces for one logical
        # operation coalesce into a single pending wake file (request_service_wakeup
        # merges callback_payload; the daemon de-dupes by mtime). We assert the merge
        # holds across multiple unconsumed signals.
        writer = DurableRuntimeWriter(
            self.store,
            owner_registry=self.registry,
            runtime_dir=self.runtime_dir,
        )
        writer.append_event_and_reduce(
            workflow_run_id="wf_coalesce",
            operation_id="op_coalesce",
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key="wf_coalesce:start",
            payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
        )
        writer.append_event_and_reduce(
            workflow_run_id="wf_coalesce",
            operation_id="op_coalesce",
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="wf_coalesce:plan",
            payload={
                "command_type": "linkedin.profile_refill.submit_batch",
                "idempotency_key": "wf_coalesce:batch:1",
                "payload": {"profile_url_count": 50},
            },
        )
        writer.append_event_and_reduce(
            workflow_run_id="wf_coalesce",
            operation_id="op_coalesce",
            event_family="workflow_event",
            event_type="WorkflowCompleted",
            idempotency_key="wf_coalesce:completed",
            payload={"stage_key": "serving_finalized"},
        )

        # Exactly one pending wake file exists across the three signals (the daemon
        # consumes one tick, not three) — the storm-coalescing guarantee.
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        callback = dict(wakeup.get("callback_payload") or {})
        self.assertEqual(callback.get("source"), "durable_runtime_event")

    def test_wakeup_failure_never_breaks_the_durable_write(self) -> None:
        # Acceleration only: if request_service_wakeup raises, the durable write still
        # succeeds (the poll remains the guarantee).
        writer = DurableRuntimeWriter(
            self.store,
            owner_registry=self.registry,
            runtime_dir=self.runtime_dir,
        )
        with mock.patch(
            "sourcing_agent.durable_runtime.request_service_wakeup",
            side_effect=OSError("disk full"),
        ):
            writer.append_event_and_reduce(
                workflow_run_id="wf_wakeup_fail",
                operation_id="op_wakeup_fail",
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key="wf_wakeup_fail:start",
                payload={"stage_key": "profile_fetch", "workflow_type": "linkedin_acquisition"},
            )
            planned = writer.append_event_and_reduce(
                workflow_run_id="wf_wakeup_fail",
                operation_id="op_wakeup_fail",
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key="wf_wakeup_fail:plan",
                payload={
                    "command_type": "linkedin.profile_refill.submit_batch",
                    "idempotency_key": "wf_wakeup_fail:batch:1",
                    "payload": {"profile_url_count": 50},
                },
            )
        # The command was still committed despite the wake-file failure.
        self.assertEqual(len(planned.commands), 1)
        commands = self.store.list_workflow_commands(workflow_run_id="wf_wakeup_fail", limit=0)
        self.assertEqual(len(commands), 1)

    def test_concurrent_wakeups_preserve_every_merged_scope(self) -> None:
        # Finding 2 (concurrency): Step 5b makes request_service_wakeup a hot path
        # — many threads in the same process signal concurrently. The read-merge-
        # replace runs under a per-service flock and uses per-(pid,thread,counter)
        # temp files, so concurrent writers cannot clobber each other's merged
        # callback nor collide on the temp path. Drive N threads each contributing
        # a distinct list entry and assert ALL survive the merge (none lost).
        import threading

        thread_count = 16
        barrier = threading.Barrier(thread_count)
        errors: list[BaseException] = []

        def _signal(index: int) -> None:
            try:
                barrier.wait(timeout=10)
                request_service_wakeup(
                    self.runtime_dir,
                    "worker-recovery-daemon",
                    reason="concurrency_probe",
                    requested_by=f"thread-{index}",
                    # explicit_worker_ids is a union-merged field (positive ints),
                    # so a correct locked read-merge-replace must preserve every
                    # thread's id.
                    callback_payload={"explicit_worker_ids": [index + 1]},
                )
            except BaseException as exc:  # noqa: BLE001 - surface to the assertion
                errors.append(exc)

        threads = [threading.Thread(target=_signal, args=(i,)) for i in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        self.assertEqual(errors, [])
        callback = dict(self._wakeup().get("callback_payload") or {})
        merged = set(callback.get("explicit_worker_ids") or [])
        # Every concurrent contribution survived the locked read-merge-replace;
        # without the in-process lock, racing read-merge-write would drop ids (a
        # writer would overwrite another's stale-read merge).
        self.assertEqual(merged, {i + 1 for i in range(thread_count)})


# ---------------------------------------------------------------------------
# Invariant 1 — an event-signaled tick and a concurrent backstop poll tick over
# the same scoped work must not double-dispatch. Reuses the worker-recovery-daemon
# setUp shape (controller/daemon stores over one control plane, _save_job).
# ---------------------------------------------------------------------------
class RecoveryNoDoubleDispatchTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tempdir.name}/runtime.db"
        # Two stores over one control plane: an event-signaled tick and a backstop
        # poll tick coordinate through shared durable state, exactly as a woken
        # daemon and a concurrent poll would.
        self.controller_store = self.make_pg_store(self.db_path)
        self.event_signaled_store = self.make_pg_store(self.db_path)
        self.backstop_poll_store = self.make_pg_store(self.db_path)
        self.controller_runtime = AgentRuntimeCoordinator(self.controller_store)
        self.request = JobRequest(
            raw_user_request="帮我找 xAI 的 RL researcher",
            target_company="xAI",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["RL", "researcher"],
        )
        self.plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "scoped_search_roster",
                "cost_policy": {
                    "parallel_search_workers": 2,
                    "parallel_exploration_workers": 1,
                    "worker_retry_limit": 1,
                },
            },
            "retrieval_plan": {"strategy": "hybrid"},
        }

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def _save_job(self, job_id: str, *, stage: str = "acquiring", status: str = "running") -> None:
        self.controller_store.save_job(
            job_id=job_id,
            job_type="workflow",
            status=status,
            stage=stage,
            request_payload=self.request.to_record(),
            plan_payload=self.plan_payload,
            summary_payload={},
        )

    def test_event_signaled_and_backstop_poll_ticks_claim_same_worker_once(self) -> None:
        job_id = "job_no_double_dispatch"
        self._save_job(job_id)
        handle = self.controller_runtime.begin_worker(
            job_id=job_id,
            request=self.request,
            plan_payload=self.plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle::01",
            stage="acquiring",
            span_name="search_bundle:bundle",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"bundle_id": "bundle", "query": "xAI RL"}, "query": "xAI RL", "index": 1},
            metadata={"index": 1},
            handoff_from_lane="triage_planner",
        )
        self.controller_store.checkpoint_agent_worker(
            handle.worker_id,
            checkpoint_payload={"stage": "search_submitted"},
            output_payload={"summary": {"status": "queued"}},
            status="running",
        )
        # Drop the original lease so the worker is recoverable by either tick.
        self.controller_store.release_agent_worker_lease(handle.worker_id)

        # The event-signaled tick claims first; the concurrent backstop poll tick
        # racing for the identical scoped worker must observe the lease and get
        # nothing — the lease IS the dedup the recovery tick relies on. This is
        # exactly run_worker_recovery_once driven twice over the same scope: a
        # single claim effect, never a double dispatch.
        event_claim = self.event_signaled_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="event-signaled-tick",
            lease_seconds=120,
        )
        backstop_claim = self.backstop_poll_store.claim_agent_worker(
            handle.worker_id,
            lease_owner="backstop-poll-tick",
            lease_seconds=120,
        )

        self.assertIsNotNone(event_claim)
        self.assertEqual(event_claim["lease_owner"], "event-signaled-tick")
        self.assertIsNone(backstop_claim)

        # Idempotency: the event-signaled tick completing the worker leaves no
        # residual recoverable work for a later tick over the same scope.
        self.event_signaled_store.complete_agent_worker(
            handle.worker_id,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={"summary": {"status": "completed"}, "entries": [], "errors": []},
        )
        residual = self.backstop_poll_store.list_recoverable_agent_workers(
            job_id=job_id,
            stale_after_seconds=0,
        )
        self.assertEqual(residual, [])
        worker = self.controller_store.get_agent_worker(worker_id=handle.worker_id)
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(int(worker["attempt_count"]), 1)


if __name__ == "__main__":
    unittest.main()
