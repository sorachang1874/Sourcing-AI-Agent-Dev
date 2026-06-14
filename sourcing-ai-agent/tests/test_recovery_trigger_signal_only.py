"""Phase 4 Step 5e — request/read-path recovery triggers become SIGNAL-ONLY.

Design anchor: docs/RECOVERY_DRIVING_REDESIGN_STUDY.md §5. Steps 5a-5c make this
safe:

* 5a (cli.serve fail-closed) GUARANTEES the shared ``worker-recovery-daemon`` is
  running whenever serve is up (or serve refuses to start).
* 5b (request_service_wakeup) wakes that daemon sub-second.
* 5c (30s poll) is the crash/lease/stuck backstop.

Because the daemon is guaranteed (5a), the request path (``start_workflow`` /
``run_queued_workflow``) and the read path (progress-poll auto takeover) no longer
ENSURE/SPAWN/RUN recovery inline. They only SIGNAL the guaranteed daemon. This
suite:

1. Characterizes the CURRENT observable behavior (response-field shapes, the
   ``progress_auto_takeover`` runtime_control event) and pins it — these stay
   green before AND after the rewrite where the contract is preserved.
2. Pins the TARGET signal-only behavior: the rewritten paths emit a
   ``worker-recovery-daemon`` wakeup file and DO NOT call
   ``ensure_shared_recovery`` / ``ensure_job_scoped_recovery`` (request path) nor
   ``run_worker_recovery_once`` (read path).
3. Proves the fallback is not lost: the same daemon tick
   (``run_worker_recovery_once``) — which the signal wakes — still drives a
   queued workflow's recovery via ``run_queued_workflow``, so recovery STILL
   happens, just off the request/read path.

The orchestrator is built over a PG-backed control plane (per repo migration
direction) with the durable runtime wired to the same runtime_dir so the wakeup
file lands under ``runtime_dir``.
"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.service_daemon import (
    clear_service_wakeup_request,
    read_service_wakeup_request,
)
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore

from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


SHARED_DAEMON = "worker-recovery-daemon"


class RecoveryTriggerSignalOnlyTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "runtime.db")
        self.catalog = AssetCatalog.discover()
        self.settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "providers.local.json",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            db_path=self.runtime_dir / "runtime.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.agent_runtime = AgentRuntimeCoordinator(self.store)
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
            worker_runtime=self.agent_runtime,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
            agent_runtime=self.agent_runtime,
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    # -- helpers ----------------------------------------------------------------

    def _request(self) -> JobRequest:
        return JobRequest(
            raw_user_request="帮我找 xAI 的 RL researcher",
            target_company="xAI",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["RL", "researcher"],
        )

    def _save_queued_workflow(self, job_id: str) -> None:
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=self._request().to_record(),
            plan_payload={
                "acquisition_strategy": {"strategy_type": "scoped_search_roster"},
                "retrieval_plan": {"strategy": "hybrid"},
            },
            summary_payload={"runtime_execution_mode": "hosted"},
        )

    def _wakeup(self) -> dict:
        return read_service_wakeup_request(self.runtime_dir, SHARED_DAEMON)

    # =====================================================================
    # Trigger ① — REQUEST PATH: start_workflow
    # =====================================================================

    def test_start_workflow_response_preserves_recovery_field_contract(self) -> None:
        # Response-contract characterization: queued["shared_recovery"] /
        # queued["job_recovery"] must remain present with a sensible status shape.
        # Stub the hosted thread so the deterministic workflow does not run
        # synchronously; we only assert the recovery-engagement fields here.
        with mock.patch.object(
            self.orchestrator,
            "_start_hosted_workflow_thread",
            return_value={"status": "started", "mode": "workflow"},
        ):
            workflow = self.orchestrator.start_workflow(
                {
                    "target_company": "Reflection AI",
                    "categories": ["employee"],
                    "skip_plan_review": True,
                }
            )

        self.assertEqual(workflow["status"], "queued")
        self.assertIn("shared_recovery", workflow)
        self.assertIn("job_recovery", workflow)
        shared = dict(workflow["shared_recovery"])
        job_scoped = dict(workflow["job_recovery"])
        # TARGET: signal-only status shape. scope is preserved; job slot carries
        # the job_id like the previous ensure-result did.
        self.assertEqual(shared["status"], "signaled")
        self.assertEqual(shared["scope"], "shared")
        self.assertEqual(shared["mode"], "signal_only")
        self.assertEqual(job_scoped["status"], "signaled")
        self.assertEqual(job_scoped["scope"], "job_scoped")
        self.assertEqual(job_scoped["job_id"], str(workflow["job_id"]))

    def test_start_workflow_signals_daemon_and_never_ensures_a_daemon(self) -> None:
        # TARGET: the request path SIGNALS the guaranteed shared daemon (5b) and
        # never ENSURES/SPAWNS one. A wakeup file for worker-recovery-daemon must
        # appear; ensure_shared_recovery / ensure_job_scoped_recovery must not be
        # called by the request path.
        self.assertEqual(self._wakeup()["status"], "not_requested")

        ensure_shared = mock.Mock(name="ensure_shared_recovery")
        ensure_job = mock.Mock(name="ensure_job_scoped_recovery")
        with (
            mock.patch.object(
                self.orchestrator,
                "_start_hosted_workflow_thread",
                return_value={"status": "started", "mode": "workflow"},
            ),
            mock.patch.object(self.orchestrator, "ensure_shared_recovery", ensure_shared),
            mock.patch.object(self.orchestrator, "ensure_job_scoped_recovery", ensure_job),
        ):
            workflow = self.orchestrator.start_workflow(
                {
                    "target_company": "Reflection AI",
                    "categories": ["employee"],
                    "skip_plan_review": True,
                }
            )

        self.assertEqual(workflow["status"], "queued")
        ensure_shared.assert_not_called()
        ensure_job.assert_not_called()
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        self.assertEqual(wakeup["service_name"], SHARED_DAEMON)
        self.assertEqual(wakeup["reason"], "start_workflow")
        self.assertEqual(wakeup["requested_by"], "start_workflow")

    def test_start_workflow_request_path_never_runs_recovery_tick_inline(self) -> None:
        # The defining 5e regression: the request path must NOT run the global
        # recovery tick (run_worker_recovery_once) on the request thread.
        tick = mock.Mock(name="run_worker_recovery_once")
        with (
            mock.patch.object(
                self.orchestrator,
                "_start_hosted_workflow_thread",
                return_value={"status": "started", "mode": "workflow"},
            ),
            mock.patch.object(self.orchestrator, "run_worker_recovery_once", tick),
        ):
            self.orchestrator.start_workflow(
                {
                    "target_company": "Reflection AI",
                    "categories": ["employee"],
                    "skip_plan_review": True,
                }
            )
        tick.assert_not_called()

    # =====================================================================
    # Trigger ① — REQUEST PATH: run_queued_workflow recovery_payload branch
    # =====================================================================

    def test_run_queued_workflow_signals_instead_of_ensuring(self) -> None:
        job_id = "job_run_queued_signal"
        self._save_queued_workflow(job_id)
        self.assertEqual(self._wakeup()["status"], "not_requested")

        ensure_shared = mock.Mock(name="ensure_shared_recovery")
        ensure_job = mock.Mock(name="ensure_job_scoped_recovery")
        # Stub the actual run so the test stays fast and isolated to the trigger.
        with (
            mock.patch.object(self.orchestrator, "ensure_shared_recovery", ensure_shared),
            mock.patch.object(self.orchestrator, "ensure_job_scoped_recovery", ensure_job),
            mock.patch.object(
                self.orchestrator,
                "_run_workflow",
                return_value=None,
            ),
        ):
            result = self.orchestrator.run_queued_workflow(
                job_id,
                recovery_payload={"workflow_queue_auto_takeover_enabled": True},
            )

        self.assertEqual(result["job_id"], job_id)
        ensure_shared.assert_not_called()
        ensure_job.assert_not_called()
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        self.assertEqual(wakeup["reason"], "run_queued_workflow")
        self.assertEqual(wakeup["service_name"], SHARED_DAEMON)

    # =====================================================================
    # Trigger ② — READ PATH: progress-poll auto takeover
    # =====================================================================

    def test_read_path_takeover_response_shape_is_preserved(self) -> None:
        # Response-contract characterization for the takeover entry: the queued
        # shape (status/classification/execution-mode/recovery_status/queued_at)
        # is preserved after the signal-only rewrite.
        job_id = "job_read_takeover_shape"
        self._save_queued_workflow(job_id)
        result = self.orchestrator._queue_progress_auto_takeover(  # noqa: SLF001
            job_id=job_id,
            classification="runner_not_alive",
            requested_execution_mode="hosted",
            effective_execution_mode="hosted",
            recovery_payload={"workflow_queue_auto_takeover_enabled": True},
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["classification"], "runner_not_alive")
        self.assertEqual(result["requested_execution_mode"], "hosted")
        self.assertEqual(result["effective_execution_mode"], "hosted")
        self.assertEqual(result["recovery_status"], "queued")
        self.assertTrue(str(result["queued_at"]))

    def test_read_path_takeover_signals_daemon_and_does_not_run_tick(self) -> None:
        # TARGET: the read path SIGNALS the guaranteed daemon instead of running
        # run_worker_recovery_once in a request-serving thread. No tick runs; a
        # wakeup file appears; the progress_auto_takeover runtime_control event
        # (cooldown anchor) is still appended.
        job_id = "job_read_takeover_signal"
        self._save_queued_workflow(job_id)
        self.assertEqual(self._wakeup()["status"], "not_requested")

        tick = mock.Mock(name="run_worker_recovery_once")
        with mock.patch.object(self.orchestrator, "run_worker_recovery_once", tick):
            self.orchestrator._queue_progress_auto_takeover(  # noqa: SLF001
                job_id=job_id,
                classification="runner_not_alive",
                requested_execution_mode="hosted",
                effective_execution_mode="hosted",
                recovery_payload={"workflow_queue_auto_takeover_enabled": True},
            )

        tick.assert_not_called()
        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        self.assertEqual(wakeup["service_name"], SHARED_DAEMON)
        self.assertEqual(wakeup["reason"], "progress_auto_takeover")
        self.assertEqual(wakeup["requested_by"], "progress_poll")

        # The cooldown-anchor event must still be written so consecutive polls
        # throttle (get_job_progress reads control=="progress_auto_takeover").
        control_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(
                job_id, stage="runtime_control", limit=20, descending=True
            )
        ]
        controls = {str(payload.get("control") or "") for payload in control_events}
        self.assertIn("progress_auto_takeover", controls)
        self.assertIn("progress_auto_takeover_result", controls)
        # The inflight marker is released after a signal-only takeover (no
        # long-running server thread holds it).
        self.assertEqual(self.orchestrator._get_progress_takeover_inflight(job_id), {})  # noqa: SLF001

    def test_read_path_takeover_skips_when_already_inflight(self) -> None:
        # Preserve the already_inflight skip contract.
        job_id = "job_read_takeover_inflight"
        self._save_queued_workflow(job_id)
        with self.orchestrator._progress_takeover_lock:  # noqa: SLF001
            self.orchestrator._progress_takeover_inflight[job_id] = {  # noqa: SLF001
                "classification": "runner_not_alive",
                "requested_execution_mode": "hosted",
                "effective_execution_mode": "hosted",
                "queued_at": "2026-06-14T00:00:00+00:00",
            }
        result = self.orchestrator._queue_progress_auto_takeover(  # noqa: SLF001
            job_id=job_id,
            classification="runner_not_alive",
            requested_execution_mode="hosted",
            effective_execution_mode="hosted",
            recovery_payload={"workflow_queue_auto_takeover_enabled": True},
        )
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "already_inflight")
        self.assertEqual(self._wakeup()["status"], "not_requested")

    # =====================================================================
    # Trigger ③ — BOOTSTRAP: inert by default (operator opt-in only)
    # =====================================================================

    def test_bootstrap_stays_operator_opt_in_and_unreached_by_request_read_paths(self) -> None:
        # The inline bootstrap (run_worker_recovery_once inside
        # _start_recovery_sidecar_with_fallback) is reachable only via ensure_*
        # with auto_job_daemon AND recovery_bootstrap_enabled. The signal-only
        # request/read paths no longer call ensure_*, so they can never reach it;
        # and even ensure_* defaults bootstrap OFF. Prove default ensure_shared
        # with auto_job_daemon=False short-circuits to disabled (never bootstraps).
        tick = mock.Mock(name="run_worker_recovery_once")
        with mock.patch.object(self.orchestrator, "run_worker_recovery_once", tick):
            status = self.orchestrator.ensure_shared_recovery({})
        self.assertEqual(status["status"], "disabled")
        tick.assert_not_called()

    # =====================================================================
    # FALLBACK PROOF — recovery STILL happens, driven by the DAEMON tick
    # =====================================================================

    def test_signal_then_daemon_tick_drives_queued_workflow_recovery(self) -> None:
        # End-to-end safety: the request path only SIGNALS, but the guaranteed
        # daemon's tick (run_worker_recovery_once — the same body 5a guarantees
        # runs) STILL drives the queued workflow's recovery via run_queued_workflow.
        # We model the woken daemon by invoking the tick directly after the signal
        # and asserting it reaches the queued workflow.
        job_id = "job_fallback_daemon_drives"
        self._save_queued_workflow(job_id)

        # Step 1: request-side signal (no inline driving on this path).
        clear_service_wakeup_request(self.runtime_dir, SHARED_DAEMON)
        signal = self.orchestrator._signal_shared_recovery_wakeup(  # noqa: SLF001
            reason="start_workflow",
            requested_by="start_workflow",
            job_id=job_id,
            scope="job_scoped",
        )
        self.assertEqual(signal["status"], "signaled")
        self.assertEqual(self._wakeup()["status"], "requested")

        # Step 2: the woken daemon runs the global recovery tick. Its
        # workflow_resume (queue-takeover) phase must reach the queued workflow
        # and take it over — i.e. recovery is DAEMON-DRIVEN, not request-driven.
        # We assert on the synchronous tick result (the takeover-dispatch record),
        # not on the background thread, so the proof is race-free. The takeover's
        # spawned run_queued_workflow thread is replaced with a kwargs-tolerant
        # no-op so the assertion is deterministic and no background thread races
        # the test (the takeover DECISION is what proves daemon-driven recovery).
        drove_via_daemon: list[str] = []

        def _noop_run_queued(**kwargs: object) -> dict:
            jid = str(kwargs.get("job_id") or "")
            drove_via_daemon.append(jid)
            return {"job_id": jid, "status": "skipped", "reason": "stubbed_thread_target"}

        with mock.patch.object(self.orchestrator, "run_queued_workflow", side_effect=_noop_run_queued):
            result = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "workflow_stale_scope_job_id": job_id,
                    "workflow_queue_auto_takeover_enabled": True,
                    "workflow_queue_resume_stale_after_seconds": 0,
                    "workflow_queue_resume_limit": 1,
                    "workflow_auto_resume_enabled": True,
                    "workflow_resume_stale_after_seconds": 0,
                    "workflow_resume_limit": 1,
                }
            )
            # Let the takeover's spawned (now no-op) thread run to completion.
            for thread in threading.enumerate():
                if thread.name == f"hosted-workflow-{job_id}":
                    thread.join(timeout=5.0)

        # The daemon tick drove recovery for the queued workflow — the fallback is
        # intact even though the request/read paths never run the tick themselves.
        resumed = {
            str(item.get("job_id") or ""): item
            for item in list(result.get("workflow_resume") or [])
        }
        self.assertIn(job_id, resumed)
        self.assertEqual(resumed[job_id]["status"], "takeover_started")
        self.assertEqual(
            dict(resumed[job_id].get("hosted_dispatch") or {}).get("source"),
            "workflow_recovery",
        )
        # And the daemon-driven dispatch actually invoked run_queued_workflow for
        # this job (off the request/read path, on the daemon's behalf).
        self.assertIn(job_id, drove_via_daemon)


if __name__ == "__main__":
    unittest.main()
