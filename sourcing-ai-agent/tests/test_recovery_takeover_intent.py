"""Option B — durable per-job recovery-takeover-intent (Phase 4 Step 5e).

Design anchor: docs/RECOVERY_TAKEOVER_INTENT_DESIGN.md. This suite proves the
owner-ratified Option B that replaces the two NO-GO'd 5e wake-file forwarding
attempts:

* F1 (injection) — GET /jobs/{id}/progress never forwards a request payload into
  the internal recovery controls; the durable intent's params are all server-side
  literals; the wake file's callback_payload carries NO recovery-control keys.
* F2 (clobber) — two concurrent upserts for distinct jobs keep two independent
  rows (PRIMARY KEY job_id), and a scoped takeover intent coexists with a generic
  global nudge without narrowing each other (NOTIFICATION vs INTENT separation).
* Invariant 1 (no duplicate dispatch) — two concurrent
  claim_workflow_recovery_intents calls claim disjoint sets (single-winner
  RETURNING / conditional UPDATE); the drain phase takes over a job exactly once.
* Invariant 3 (timely takeover) — a classified dead-runner job carrying a pending
  intent is taken over by the workflow_takeover_intent_drain phase with stale=0
  (NOT the 60s default), driven by a PLAIN global tick — proven by the intent,
  not a hand-built stronger payload.
* Oracle — the characterization stays 10/10 with the additive drain phase.

The orchestrator is built over a PG-backed control plane (per repo migration
direction) with the durable runtime wired to the same runtime_dir.
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
from sourcing_agent.service_daemon import read_service_wakeup_request
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

# The 8 server-side bounded-recovery contract fields the read path writes into
# the durable intent (docs/RECOVERY_TAKEOVER_INTENT_DESIGN.md §3) — these are the
# fields that MUST NOT be request-injectable and MUST NOT ride the wake file.
RECOVERY_CONTROL_KEYS = (
    "workflow_stale_scope_job_id",
    "workflow_resume_explicit_job",
    "workflow_resume_stale_after_seconds",
    "workflow_resume_limit",
    "workflow_auto_resume_enabled",
    "workflow_queue_resume_stale_after_seconds",
    "workflow_queue_resume_limit",
    "workflow_queue_auto_takeover_enabled",
)


class RecoveryTakeoverIntentTest(PGDurableRuntimeTestMixin, unittest.TestCase):
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
    # F1 — no request-payload injection into recovery controls
    # =====================================================================

    def test_f1_read_path_writes_server_side_literal_intent_not_a_request_payload(self) -> None:
        # The read path's progress auto-takeover writes the durable intent with
        # ALL server-side literal params (scoped to job_id, zero stale). It never
        # forwards a request payload into the recovery controls.
        job_id = "job_f1_intent_literals"
        self._save_queued_workflow(job_id)

        self.orchestrator._queue_progress_auto_takeover(  # noqa: SLF001
            job_id=job_id,
            classification="runner_not_alive",
            requested_execution_mode="hosted",
            effective_execution_mode="hosted",
            # Even if the caller passed a (legacy) recovery_payload, the read path
            # writes its OWN server-side literals — the durable intent does not
            # echo this dict.
            recovery_payload={"workflow_resume_limit": 999, "injected": "attacker"},
        )

        intent = self.store.get_workflow_recovery_intent(job_id)
        self.assertEqual(intent.get("status"), "pending")
        self.assertEqual(intent.get("classification"), "runner_not_alive")
        self.assertEqual(intent.get("requested_by"), "progress_poll")
        params = dict(intent.get("params") or {})
        # Server-side literals, scoped to THIS job, zero stale, limit 1.
        self.assertEqual(params.get("workflow_stale_scope_job_id"), job_id)
        self.assertTrue(params.get("workflow_resume_explicit_job"))
        self.assertTrue(params.get("workflow_auto_resume_enabled"))
        self.assertEqual(params.get("workflow_resume_stale_after_seconds"), 0)
        self.assertEqual(params.get("workflow_resume_limit"), 1)
        self.assertTrue(params.get("workflow_queue_auto_takeover_enabled"))
        self.assertEqual(params.get("workflow_queue_resume_stale_after_seconds"), 0)
        self.assertEqual(params.get("workflow_queue_resume_limit"), 1)
        # The attacker-controlled fields are NOT present in the intent params.
        self.assertNotIn("injected", params)
        self.assertNotEqual(params.get("workflow_resume_limit"), 999)

    def test_f1_wake_file_callback_payload_carries_no_recovery_control_keys(self) -> None:
        # The wake file is a PURE NUDGE: callback_payload is exactly
        # {source, request_path_job_id} — NO recovery-control keys.
        job_id = "job_f1_pure_nudge"
        self._save_queued_workflow(job_id)
        self.assertEqual(self._wakeup()["status"], "not_requested")

        self.orchestrator._queue_progress_auto_takeover(  # noqa: SLF001
            job_id=job_id,
            classification="runner_not_alive",
            requested_execution_mode="hosted",
            effective_execution_mode="hosted",
            recovery_payload={"workflow_stale_scope_job_id": job_id},
        )

        wakeup = self._wakeup()
        self.assertEqual(wakeup["status"], "requested")
        callback = dict(wakeup.get("callback_payload") or {})
        self.assertEqual(callback.get("source"), "progress_auto_takeover")
        self.assertEqual(callback.get("request_path_job_id"), job_id)
        for key in RECOVERY_CONTROL_KEYS:
            self.assertNotIn(key, callback, f"wake file must not carry recovery-control key {key}")

    def test_f1_signal_helper_never_forwards_recovery_controls_even_when_passed(self) -> None:
        # Direct unit on the pure-nudge helper: even if a caller passes a payload
        # containing recovery-control fields, the callback_payload converges to
        # {source, request_path_job_id}.
        job_id = "job_f1_helper_direct"
        result = self.orchestrator._signal_shared_recovery_wakeup(  # noqa: SLF001
            reason="progress_auto_takeover",
            requested_by="progress_poll",
            job_id=job_id,
            payload={
                "workflow_stale_scope_job_id": job_id,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_queue_auto_takeover_enabled": True,
            },
            scope="job_scoped",
        )
        self.assertEqual(result["status"], "signaled")
        callback = dict(self._wakeup().get("callback_payload") or {})
        self.assertEqual(set(callback.keys()), {"source", "request_path_job_id"})

    # =====================================================================
    # F2 — independent per-job rows; intent vs nudge coexist
    # =====================================================================

    def test_f2_concurrent_upserts_for_distinct_jobs_keep_independent_rows(self) -> None:
        job_a = "job_f2_a"
        job_b = "job_f2_b"
        barrier = threading.Barrier(2)

        def _upsert(jid: str, classification: str) -> None:
            barrier.wait(timeout=5.0)
            self.store.upsert_workflow_recovery_intent(
                jid,
                classification=classification,
                params={"workflow_stale_scope_job_id": jid, "workflow_resume_limit": 1},
                requested_by="progress_poll",
            )

        threads = [
            threading.Thread(target=_upsert, args=(job_a, "runner_not_alive")),
            threading.Thread(target=_upsert, args=(job_b, "queued_waiting_for_runner")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        intent_a = self.store.get_workflow_recovery_intent(job_a)
        intent_b = self.store.get_workflow_recovery_intent(job_b)
        # Two independent rows — neither clobbers the other.
        self.assertEqual(intent_a.get("status"), "pending")
        self.assertEqual(intent_b.get("status"), "pending")
        self.assertEqual(intent_a.get("classification"), "runner_not_alive")
        self.assertEqual(intent_b.get("classification"), "queued_waiting_for_runner")
        self.assertEqual(dict(intent_a.get("params") or {}).get("workflow_stale_scope_job_id"), job_a)
        self.assertEqual(dict(intent_b.get("params") or {}).get("workflow_stale_scope_job_id"), job_b)

    def test_f2_scoped_intent_and_generic_nudge_do_not_narrow_each_other(self) -> None:
        # A scoped takeover INTENT (durable, per-job) and a generic global NUDGE
        # (request-path wake, no scope) are two separate mechanisms; the nudge
        # never narrows or overwrites the scoped intent's contract.
        scoped_job = "job_f2_scoped"
        self.store.upsert_workflow_recovery_intent(
            scoped_job,
            classification="runner_not_alive",
            params={
                "workflow_stale_scope_job_id": scoped_job,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_resume_limit": 1,
            },
            requested_by="progress_poll",
        )

        # A generic global nudge (request path: start_workflow style, no scope).
        self.orchestrator._signal_shared_recovery_wakeup(  # noqa: SLF001
            reason="start_workflow",
            requested_by="start_workflow",
        )

        # The durable intent is untouched by the unscoped nudge.
        intent = self.store.get_workflow_recovery_intent(scoped_job)
        self.assertEqual(intent.get("status"), "pending")
        params = dict(intent.get("params") or {})
        self.assertEqual(params.get("workflow_stale_scope_job_id"), scoped_job)
        self.assertEqual(params.get("workflow_resume_stale_after_seconds"), 0)
        # And the nudge wake file carries no scope (it cannot clobber the intent).
        callback = dict(self._wakeup().get("callback_payload") or {})
        self.assertNotIn("workflow_stale_scope_job_id", callback)
        self.assertEqual(callback.get("source"), "start_workflow")

    # =====================================================================
    # Invariant 1 — single-winner claim; no duplicate dispatch
    # =====================================================================

    def test_invariant1_concurrent_claims_take_disjoint_sets(self) -> None:
        # Three pending intents; two concurrent claim calls (each limit=3). The
        # single-winner conditional UPDATE guarantees no intent is claimed twice —
        # the union of both claim sets has no duplicate job_id.
        job_ids = [f"job_inv1_{i}" for i in range(3)]
        for jid in job_ids:
            self.store.upsert_workflow_recovery_intent(
                jid,
                classification="runner_not_alive",
                params={"workflow_stale_scope_job_id": jid},
                requested_by="progress_poll",
            )

        results: dict[str, list[str]] = {}
        barrier = threading.Barrier(2)

        def _claim(owner: str) -> None:
            barrier.wait(timeout=5.0)
            claimed = self.store.claim_workflow_recovery_intents(
                lease_owner=owner,
                lease_seconds=120,
                limit=3,
            )
            results[owner] = [str(dict(row).get("job_id") or "") for row in claimed]

        threads = [
            threading.Thread(target=_claim, args=("owner-A",)),
            threading.Thread(target=_claim, args=("owner-B",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        claimed_a = results.get("owner-A", [])
        claimed_b = results.get("owner-B", [])
        # Disjoint: no job claimed by both owners.
        self.assertEqual(set(claimed_a) & set(claimed_b), set())
        # Every pending intent was claimed exactly once across both owners.
        self.assertEqual(sorted(claimed_a + claimed_b), sorted(job_ids))
        # Each claimed row is now 'claimed' (not still 'pending').
        for jid in job_ids:
            self.assertEqual(self.store.get_workflow_recovery_intent(jid).get("status"), "claimed")

    def test_invariant1_drain_phase_takes_over_a_job_exactly_once(self) -> None:
        # Drive the drain helper twice. The first tick claims + consumes the
        # intent; the second finds nothing pending → the job is not re-dispatched.
        job_id = "job_inv1_once"
        self._save_queued_workflow(job_id)
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={"workflow_stale_scope_job_id": job_id},
            requested_by="progress_poll",
        )

        drove: list[str] = []

        def _noop_run_queued(**kwargs: object) -> dict:
            drove.append(str(kwargs.get("job_id") or ""))
            return {"job_id": str(kwargs.get("job_id") or ""), "status": "skipped"}

        with mock.patch.object(self.orchestrator, "run_queued_workflow", side_effect=_noop_run_queued):
            first = self.orchestrator._drain_workflow_takeover_intents(  # noqa: SLF001
                {"jobs": []}, payload={}
            )
            self._join_takeover_thread(job_id)
            self.assertEqual(self.store.get_workflow_recovery_intent(job_id).get("status"), "consumed")
            # Second drain: nothing pending → no result rows, no re-dispatch.
            second = self.orchestrator._drain_workflow_takeover_intents(  # noqa: SLF001
                {"jobs": []}, payload={}
            )

        self.assertTrue(any(str(item.get("job_id") or "") == job_id for item in first))
        self.assertEqual(second, [])
        # The queued workflow's takeover dispatched run_queued_workflow at most
        # once across the two drains (exactly-once takeover).
        self.assertEqual(drove.count(job_id), 1)

    # =====================================================================
    # Invariant 3 — timely takeover via the intent (stale=0), not the 60s default
    # =====================================================================

    def test_invariant3_pending_intent_drives_stale0_takeover_on_plain_tick(self) -> None:
        # A classified dead-runner job carrying a pending intent is taken over by
        # the workflow_takeover_intent_drain phase with stale=0 (the scoped resume
        # the read path wrote into the intent), driven by a PLAIN global tick —
        # NOT a hand-built stronger payload. The takeover is attributed to the
        # intent (the drain phase tags each resume item with takeover_intent_job_id
        # and consumes the intent), and the generic workflow_resume phase keeps its
        # default/unscoped settings.
        job_id = "job_inv3_stale0"
        self._save_queued_workflow(job_id)

        drove: list[str] = []

        def _noop_run_queued(**kwargs: object) -> dict:
            drove.append(str(kwargs.get("job_id") or ""))
            return {"job_id": str(kwargs.get("job_id") or ""), "status": "skipped"}

        # Write the durable intent (server-side literals, scoped, zero stale).
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={
                "workflow_stale_scope_job_id": job_id,
                "workflow_resume_explicit_job": True,
                "workflow_auto_resume_enabled": True,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_resume_limit": 1,
                "workflow_queue_auto_takeover_enabled": True,
                "workflow_queue_resume_stale_after_seconds": 0,
                "workflow_queue_resume_limit": 1,
            },
            requested_by="progress_poll",
        )

        # Drive a PLAIN global tick (no scoped/zero-stale payload anywhere).
        with mock.patch.object(self.orchestrator, "run_queued_workflow", side_effect=_noop_run_queued):
            result = self.orchestrator.run_worker_recovery_once({})
            self._join_takeover_thread(job_id)

        # The drain phase ran (metered) and consumed the intent → taken over via
        # the durable intent, exactly once.
        self.assertIn("workflow_takeover_intent_drain", result.get("recovery_phase_metrics", {}))
        self.assertEqual(self.store.get_workflow_recovery_intent(job_id).get("status"), "consumed")
        self.assertEqual(drove.count(job_id), 1)

        # The takeover surfaced in workflow_resume AND is attributed to the intent
        # (the drain phase tags each resume row with takeover_intent_job_id) — this
        # is the stale=0 scoped resume the intent carried, not the generic phase.
        intent_attributed = [
            item
            for item in list(result.get("workflow_resume") or [])
            if str(item.get("job_id") or "") == job_id
            and str(item.get("takeover_intent_job_id") or "") == job_id
        ]
        self.assertTrue(
            intent_attributed,
            "the job must be taken over via the durable intent (drain phase), not the generic resume",
        )
        self.assertEqual(intent_attributed[0]["status"], "takeover_started")
        self.assertEqual(intent_attributed[0].get("takeover_intent_classification"), "runner_not_alive")
        self.assertEqual(
            dict(intent_attributed[0].get("hosted_dispatch") or {}).get("source"),
            "workflow_recovery",
        )

    def test_invariant3_generic_resume_phase_keeps_default_unscoped_settings(self) -> None:
        # The generic workflow_resume phase MUST keep its default/unscoped stale
        # window — the drain phase is the only stale=0 scoped path. Spy the resume
        # callback and assert that on a plain tick (no payload), the GENERIC phase
        # is invoked with no stale_job_scope_job_id and the default 60s windows,
        # while the drain phase invokes it scoped with stale=0.
        job_id = "job_inv3_generic_default"
        self._save_queued_workflow(job_id)
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={
                "workflow_stale_scope_job_id": job_id,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_resume_limit": 1,
                "workflow_queue_resume_stale_after_seconds": 0,
            },
            requested_by="progress_poll",
        )

        calls: list[dict[str, object]] = []
        real_resume = self.orchestrator._resume_blocked_workflows_after_recovery  # noqa: SLF001

        def _spy(summary: object, **kwargs: object) -> list:
            calls.append(dict(kwargs))
            return real_resume(summary, **kwargs)

        with (
            mock.patch.object(self.orchestrator, "_resume_blocked_workflows_after_recovery", side_effect=_spy),
            mock.patch.object(self.orchestrator, "run_queued_workflow", side_effect=lambda **k: {"job_id": str(k.get("job_id") or ""), "status": "skipped"}),
        ):
            self.orchestrator.run_worker_recovery_once({})
            self._join_takeover_thread(job_id)

        scoped_zero_stale = [
            c
            for c in calls
            if str(c.get("stale_job_scope_job_id") or "") == job_id
            and int(c["stale_after_seconds"]) == 0
        ]
        generic_default = [
            c
            for c in calls
            if not str(c.get("stale_job_scope_job_id") or "")
            and int(c["stale_after_seconds"]) == 60
        ]
        self.assertTrue(scoped_zero_stale, "drain phase must invoke a scoped stale=0 resume")
        self.assertTrue(generic_default, "generic resume phase must keep default unscoped 60s settings")

    # =====================================================================
    # Claim/consume lifecycle (Option B NO-GO fixes)
    # =====================================================================

    def test_consume_is_claim_identity_scoped_and_does_not_clobber_a_newer_intent(self) -> None:
        # NO-GO finding 1 (stale-claim clobber): a daemon claims intent v1, then a
        # NEWER same-job intent v2 is upserted (re-arming the row to 'pending')
        # before the daemon consumes. mark_workflow_recovery_intent_consumed must
        # match the CLAIM identity (lease_owner + claimed_at), so the stale
        # consume is a no-op and v2 survives as pending for the next drain.
        job_id = "job_consume_identity"
        self._save_queued_workflow(job_id)
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={"workflow_stale_scope_job_id": job_id, "generation": "v1"},
            requested_by="progress_poll",
        )
        claimed = self.store.claim_workflow_recovery_intents(
            lease_owner="daemon-A", lease_seconds=120, limit=10
        )
        self.assertEqual(len(claimed), 1)
        claim_owner = str(claimed[0].get("lease_owner") or "")
        claim_claimed_at = str(claimed[0].get("claimed_at") or "")
        self.assertEqual(claim_owner, "daemon-A")
        self.assertTrue(claim_claimed_at)

        # A newer same-job intent arrives between claim and consume → re-armed pending.
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={"workflow_stale_scope_job_id": job_id, "generation": "v2"},
            requested_by="progress_poll",
        )
        self.assertEqual(self.store.get_workflow_recovery_intent(job_id).get("status"), "pending")

        # The stale claim's consume must NOT clobber v2.
        self.store.mark_workflow_recovery_intent_consumed(
            job_id, lease_owner=claim_owner, claimed_at=claim_claimed_at
        )
        survived = self.store.get_workflow_recovery_intent(job_id)
        self.assertEqual(survived.get("status"), "pending")
        self.assertEqual(dict(survived.get("params") or {}).get("generation"), "v2")
        # The fresh intent is re-claimable.
        reclaimed = self.store.claim_workflow_recovery_intents(
            lease_owner="daemon-B", lease_seconds=120, limit=10
        )
        self.assertEqual([str(r.get("job_id") or "") for r in reclaimed], [job_id])

    def test_drain_leaves_intent_for_retry_when_resume_fails(self) -> None:
        # NO-GO finding 2 (consume-on-failure): a transient takeover_failed must
        # NOT consume the intent — the drain leaves the claim's lease to expire so
        # the immediate-takeover intent is re-claimed and retried next tick,
        # instead of dropping the job to the generic default-stale window.
        job_id = "job_resume_fails_retry"
        self._save_queued_workflow(job_id)
        self.store.upsert_workflow_recovery_intent(
            job_id,
            classification="runner_not_alive",
            params={"workflow_stale_scope_job_id": job_id},
            requested_by="progress_poll",
        )

        def _failing_resume(summary: object, **kwargs: object) -> list:
            return [{"status": "takeover_failed", "job_id": job_id, "reason": "dispatch_raced"}]

        with mock.patch.object(
            self.orchestrator, "_resume_blocked_workflows_after_recovery", side_effect=_failing_resume
        ):
            result = self.orchestrator._drain_workflow_takeover_intents(  # noqa: SLF001
                {"jobs": []}, payload={}
            )

        # The intent was NOT consumed; it stays claimed with its lease so it is
        # re-claimable once the lease expires (retryable), not lost.
        self.assertTrue(any(str(item.get("status") or "") == "takeover_failed" for item in result))
        self.assertNotEqual(self.store.get_workflow_recovery_intent(job_id).get("status"), "consumed")

    # -- internal: join the takeover's spawned no-op thread deterministically ----

    def _join_takeover_thread(self, job_id: str) -> None:
        for thread in threading.enumerate():
            if thread.name == f"hosted-workflow-{job_id}":
                thread.join(timeout=5.0)


if __name__ == "__main__":
    unittest.main()
