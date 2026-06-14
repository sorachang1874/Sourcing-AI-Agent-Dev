"""Whole-tick characterization (golden snapshot) for ``run_worker_recovery_once``.

This module pins the CURRENT observable behavior of the recovery tick strongly
enough that Phase 4 Step 2's mechanical phase-object extraction is provably
behavior-preserving. It is the one-level-up analogue of
``tests/test_recovery_drain_registry.py``: the drain-registry test records every
domain drain in order with arguments and asserts summary aggregation; this test
does the same for the WHOLE tick — the ordered phase sequence, per-phase owner /
status / skip-reason / ``max_sync_work`` observables, the summary-key mapping,
and the threaded cross-phase state (handoff yields, the refill-submit suppression
ladder, the search-seed resume-skip threading, and the budget-exhaustion epilogue
that drives ``next_tick_requested``).

Design anchor: ``docs/PHASE4_ENTANGLED_CORE_DESIGN.md`` §1 A-band and §3 Step 1.

The robust observable is ``recovery_phase_metrics`` (a dict whose insertion order
== phase invocation order, produced uniformly by ``_run_recovery_phase`` /
``_skipped_phase`` at orchestrator.py:38100-38198) plus the returned summary keys
and the epilogue flags (orchestrator.py:40013-40063). We do not patch the phase
runner; we read its uniform output, which is exactly what Step 2 must reproduce.

CHARACTERIZATION CONTRACT: this test must pass UNCHANGED against the current
orchestrator. It was built green on the current tree and proven pinned by
mutation (reorder two phases / flip a gate in a /tmp copy of the tick) — each
asserted observable catches at least one such mutation.

No product code is touched; every input is a controlled payload or an injected
fake over external provider effects, never wall-clock dependence.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


# ---------------------------------------------------------------------------
# Golden snapshot: the exact ordered phase sequence the tick records into
# recovery_phase_metrics, paired with each phase's pinned owner label.
#
# Sequence and owner labels were captured empirically against the current tree
# (orchestrator.py:38028-40063). The sequence is identical for a default/global
# tick and an explicit_job_id tick — only per-phase status/reason differ under
# the bespoke gating (pinned separately below). ``total`` is the epilogue
# rollup row (orchestrator.py:39993).
# ---------------------------------------------------------------------------
CHARACTERIZED_PHASE_SEQUENCE: tuple[tuple[str, str], ...] = (
    ("dead_local_recovery_lease_repair", "recovery_lease_repair"),
    ("search_seed_discovery", "search_seed_discovery_query_queue"),
    ("pre_worker_profile_prefetch_refill", "profile_refill_daemon"),
    ("worker_recovery", "worker_recovery_daemon"),
    ("blocked_workflow_cleanup", "blocked_workflow_residue_cleanup"),
    ("profile_prefetch_refill", "profile_refill_daemon"),
    ("profile_refill_command_owner", "linkedin_profile_refill_command_owner"),
    (
        "profile_url_terminal_record_command_owner",
        "linkedin_profile_url_terminal_record_command_owner",
    ),
    ("stage1_preview_recovery_bridge", "workflow_preview_projection"),
    (
        "profile_refill_event_level_materialization_followup",
        "event_level_local_apply_to_board_visible",
    ),
    ("local_apply_backlog", "profile_local_apply_command_owner"),
    ("legacy_materialization_adapter", "durable_runtime_migration_adapter"),
    ("event_level_materialization_followup", "event_level_local_apply_to_board_visible"),
    ("post_event_level_profile_prefetch_refill", "profile_refill_daemon"),
    ("workflow_takeover_intent_drain", "workflow_takeover_intent_drain"),
    ("workflow_resume", "workflow_resume_controller"),
    ("post_completion_reconcile", "completed_workflow_reconcile"),
    ("excel_intake_recovery", "excel_intake_recovery"),
    ("crm_public_web_queue_batch", "crm_public_web_owner"),
    ("crm_public_web_phase_commands", "crm_public_web_owner"),
    # --- the 14-binding uniform drain block (test_recovery_drain_registry pins
    # its internals; here we pin its position and order in the whole tick) ---
    ("company_public_web_refresh_command_owner", "company_public_web_owner"),
    ("company_logo_profile_experience_discover_command_owner", "company_asset_owner"),
    ("media_asset_cache_command_owner", "media_asset_owner"),
    ("acquisition_run_create_command_owner", "acquisition_run_writer"),
    ("acquisition_intent_resolve_command_owner", "acquisition_planner"),
    ("acquisition_plan_build_command_owner", "acquisition_planner"),
    ("acquisition_plan_review_request_command_owner", "acquisition_planner"),
    ("acquisition_plan_commit_command_owner", "acquisition_planner"),
    ("acquisition_probe_command_owner", "acquisition_probe_owner"),
    ("acquisition_scale_plan_command_owner", "acquisition_scale_planner"),
    ("operation_native_discovery_activity_owner", "linkedin_acquisition_owner"),
    ("operation_native_profile_fetch_activity_owner", "linkedin_profile_activity_owner"),
    ("operation_native_projection_admission_owner", "serving_projection_owner"),
    # crm_writer_command_owner is metered here but intentionally absent from the
    # returned summary (the only summary-invisible drain) — pinned below.
    ("crm_writer_command_owner", "crm_writer"),
    # --- one-durable-unit-per-tick projection串 (39264-39597) ---
    ("board_visible_apply", "board_visible_projection_owner"),
    ("run_scope_projection_finalize", "serving_projection_owner"),
    ("post_projection_workflow_resume", "workflow_resume_controller"),
    ("snapshot_full_materialization", "snapshot_full_materialization_queue"),
    ("projection_facet_layering", "projection_facet_layering_queue"),
    ("projection_person_search_index", "projection_index_owner"),
    ("collection_authoritative_merge", "collection_writer_owner"),
    # --- followup尾环 (39603-39955) ---
    ("explicit_job_followup_rounds", "worker_recovery_daemon"),
    ("remote_event_followup", "remote_event_followup_worker_recovery"),
    (
        "post_followup_event_level_materialization_followup",
        "event_level_local_apply_to_board_visible",
    ),
    ("post_followup_profile_prefetch_refill", "profile_refill_daemon"),
    ("post_followup_workflow_resume", "workflow_resume_controller"),
    ("post_followup_post_completion_reconcile", "completed_workflow_reconcile"),
    ("post_recovery_housekeeping", "runtime_heartbeat_and_metrics"),
    # --- epilogue rollup ---
    ("total", "run_worker_recovery_once"),
)

CHARACTERIZED_PHASE_NAMES: tuple[str, ...] = tuple(
    name for name, _ in CHARACTERIZED_PHASE_SEQUENCE
)

# Phases whose metrics-phase name differs from the returned-summary key.
#   - stage1_preview_recovery_bridge -> summary key "stage1_preview_recovery"
SUMMARY_KEY_OVERRIDES: dict[str, str] = {"stage1_preview_recovery_bridge": "stage1_preview_recovery"}

# Metrics phases that are deliberately ABSENT from the returned summary as their
# own top-level key — each is either rolled up into another summary slot or kept
# metrics-only (orchestrator.py:40013-40063). Pinning this exact set is part of
# the summary-aggregation contract Step 2 must preserve:
#   - pre_worker_profile_prefetch_refill -> merged into "profile_prefetch_refill"
#   - worker_recovery                    -> returned under the "daemon" key
#   - crm_writer_command_owner           -> only summary-invisible registry drain
#   - explicit_job_followup_rounds       -> folded into the "daemon" summary
#   - post_recovery_housekeeping         -> "runtime_heartbeat" + "runtime_metrics"
#   - total                              -> recovery_phase_metrics rollup row only
PHASES_ABSENT_FROM_SUMMARY: frozenset[str] = frozenset(
    {
        "pre_worker_profile_prefetch_refill",
        "worker_recovery",
        "crm_writer_command_owner",
        "explicit_job_followup_rounds",
        "post_recovery_housekeeping",
        # workflow_takeover_intent_drain is metered into recovery_phase_metrics
        # but never returned as its own summary key (metrics-only drain phase).
        "workflow_takeover_intent_drain",
        "total",
    }
)


class RecoveryTickWholeTickCharacterizationTest(
    PGDurableRuntimeTestMixin, unittest.TestCase
):
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
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    # -- helpers ----------------------------------------------------------------

    def _save_job(self, job_id: str) -> None:
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=JobRequest(
                raw_user_request="帮我找 xAI 的 RL researcher",
                target_company="xAI",
                categories=["employee"],
                employment_statuses=["current"],
                keywords=["RL", "researcher"],
            ).to_record(),
            plan_payload={},
            summary_payload={},
        )

    @staticmethod
    def _phase_sequence(result: dict[str, Any]) -> list[str]:
        return list(result["recovery_phase_metrics"].keys())

    @staticmethod
    def _phase(result: dict[str, Any], name: str) -> dict[str, Any]:
        return dict(result["recovery_phase_metrics"][name])

    # -- 1. PHASE SEQUENCE ------------------------------------------------------

    def test_global_scope_tick_runs_characterized_phase_sequence_with_owners(self) -> None:
        result = self.orchestrator.run_worker_recovery_once({})

        # Exact ordered phase sequence (insertion order of recovery_phase_metrics
        # == phase invocation order). A reorder mutation in the tick changes this.
        self.assertEqual(self._phase_sequence(result), list(CHARACTERIZED_PHASE_NAMES))

        # Per-phase owner label and a non-empty max_sync_work string, in order.
        metrics = result["recovery_phase_metrics"]
        for name, owner in CHARACTERIZED_PHASE_SEQUENCE:
            entry = dict(metrics[name])
            self.assertEqual(entry["phase"], name)
            self.assertEqual(entry["owner"], owner, f"owner drift for phase {name}")
            self.assertTrue(
                str(entry["max_sync_work"]).strip(),
                f"phase {name} must surface a max_sync_work contract string",
            )

        # A quiescent global tick neither exhausts budget nor requests a re-tick.
        self.assertFalse(result["recovery_tick_budget_exhausted"])
        self.assertFalse(result["phase_budget_exhausted"])
        self.assertFalse(result["durable_work_handoff_yield"])
        self.assertFalse(result["next_tick_requested"])
        self.assertEqual(result["status"], "completed")

    def test_explicit_job_tick_runs_same_phase_sequence_with_job_scoped_gating(self) -> None:
        self._save_job("job_explicit_seq")
        result = self.orchestrator.run_worker_recovery_once({"job_id": "job_explicit_seq"})

        # The phase sequence is identical to the global tick; the explicit-job
        # scope changes per-phase gating, not the ordered phase set.
        self.assertEqual(self._phase_sequence(result), list(CHARACTERIZED_PHASE_NAMES))

        # Pin the gating deltas that distinguish an explicit-job tick from a
        # global tick (the bespoke job-scope skip ladder, A-band §1):
        #   - blocked_workflow_cleanup is skipped under explicit job scope
        #   - pre-worker refill reports "not requested" rather than "no scope"
        #   - post_projection_workflow_resume reports the projection-proof gate
        self.assertEqual(
            self._phase(result, "blocked_workflow_cleanup")["status"], "skipped"
        )
        self.assertEqual(
            self._phase(result, "blocked_workflow_cleanup")["reason"], "explicit_job_scope"
        )
        self.assertEqual(
            self._phase(result, "pre_worker_profile_prefetch_refill")["reason"],
            "pre_worker_refill_not_requested",
        )
        self.assertEqual(
            self._phase(result, "post_projection_workflow_resume")["reason"],
            "no_canonical_terminal_projection_proof",
        )
        self.assertEqual(result["next_tick_requested"], False)

    def test_global_tick_pins_distinct_global_scope_skip_reasons(self) -> None:
        # The mirror of the explicit-job gating: a global tick runs
        # blocked_workflow_cleanup and reports job-scope-missing skips on the
        # job-scoped phases. Pinning both directions catches a gate-flip
        # mutation that inverts the explicit_job_id branch.
        result = self.orchestrator.run_worker_recovery_once({})
        self.assertEqual(
            self._phase(result, "blocked_workflow_cleanup")["status"], "completed"
        )
        self.assertEqual(
            self._phase(result, "pre_worker_profile_prefetch_refill")["reason"],
            "job_scope_missing",
        )
        self.assertEqual(
            self._phase(result, "stage1_preview_recovery_bridge")["reason"],
            "job_scope_missing",
        )
        self.assertEqual(
            self._phase(result, "post_projection_workflow_resume")["reason"],
            "job_scope_missing",
        )

    # -- 2. PER-PHASE OBSERVABLES (skip reasons under controlled flags) ---------

    def test_payload_disable_flags_skip_phases_with_pinned_reasons(self) -> None:
        # Each disabled-by-payload phase records a pinned skip reason and keeps
        # its owner. These are the per-phase gates surfaced through the uniform
        # _skipped_phase metric.
        payload = {
            "search_seed_discovery_enabled": False,
            "profile_prefetch_refill_enabled": False,
            "profile_refill_command_owner_enabled": False,
            "profile_url_terminal_record_command_owner_enabled": False,
            "post_completion_reconcile_enabled": False,
            "excel_intake_recovery_enabled": False,
            "post_recovery_housekeeping_enabled": False,
        }
        result = self.orchestrator.run_worker_recovery_once(payload)

        expected_skips = {
            "search_seed_discovery": "search_seed_discovery_disabled_by_payload",
            "profile_prefetch_refill": "profile_prefetch_refill_disabled_by_payload",
            "profile_refill_command_owner": "profile_refill_command_owner_disabled_by_payload",
            "profile_url_terminal_record_command_owner": (
                "profile_url_terminal_record_command_owner_disabled_by_payload"
            ),
            "post_completion_reconcile": "post_completion_reconcile_disabled_by_payload",
            "excel_intake_recovery": "excel_intake_recovery_disabled_by_payload",
            "post_recovery_housekeeping": "post_recovery_housekeeping_disabled_by_payload",
        }
        for phase, reason in expected_skips.items():
            entry = self._phase(result, phase)
            self.assertEqual(entry["status"], "skipped", f"{phase} should be skipped")
            self.assertEqual(entry["reason"], reason, f"{phase} skip reason drift")

        # The sequence is unchanged: disabling a phase records a skip row in the
        # same position, it does not remove the phase from the ordered metrics.
        self.assertEqual(self._phase_sequence(result), list(CHARACTERIZED_PHASE_NAMES))

    # -- 3. SUMMARY AGGREGATION -------------------------------------------------

    def test_summary_keys_map_each_phase_result_to_its_pinned_summary_slot(self) -> None:
        result = self.orchestrator.run_worker_recovery_once({})

        for name in CHARACTERIZED_PHASE_NAMES:
            if name in PHASES_ABSENT_FROM_SUMMARY:
                # crm_writer_command_owner is metered but not returned; the
                # followup-rounds counter and the total rollup live only in
                # recovery_phase_metrics.
                self.assertNotIn(name, result, f"{name} must not be a summary key")
                self.assertIn(name, result["recovery_phase_metrics"])
                continue
            summary_key = SUMMARY_KEY_OVERRIDES.get(name, name)
            self.assertIn(
                summary_key,
                result,
                f"phase {name} must surface under summary key {summary_key}",
            )

        # The rolled-up phases land under their pinned summary slots.
        self.assertIn("daemon", result)  # worker_recovery + explicit_job_followup_rounds
        self.assertIn("runtime_heartbeat", result)  # post_recovery_housekeeping
        self.assertIn("runtime_metrics", result)  # post_recovery_housekeeping
        self.assertIn("provider_control_open_work", result)

        # The epilogue exposes exactly these tick-level flags (orchestrator.py
        # 40013-40023); next_tick_requested is their OR.
        for flag in (
            "recovery_tick_budget_exhausted",
            "phase_budget_exhausted",
            "durable_work_handoff_yield",
            "next_tick_requested",
            "recovery_tick_total_budget_ms",
        ):
            self.assertIn(flag, result)

    def test_remote_event_followup_rebinds_four_tuple_summary_locals(self) -> None:
        # remote_event_followup is the 4-tuple rebind point (orchestrator.py
        # 37332 / 39702): its phase result lands under "remote_event_followup"
        # while it also rebinds summary/workflow_resume/post_completion_reconcile.
        # Pin that all four destination summary keys are present and that the
        # followup phase itself ran.
        self._save_job("job_remote_event")
        result = self.orchestrator.run_worker_recovery_once({"job_id": "job_remote_event"})
        self.assertEqual(
            self._phase(result, "remote_event_followup")["owner"],
            "remote_event_followup_worker_recovery",
        )
        for key in ("daemon", "workflow_resume", "post_completion_reconcile", "remote_event_followup"):
            self.assertIn(key, result)

    # -- 4. THREADED CROSS-PHASE STATE ------------------------------------------

    def test_worker_recovery_handoff_yields_and_cascades_skip_reason_downstream(self) -> None:
        # An early worker_recovery that consumes terminal worker evidence sets
        # worker_recovery_handoff_required, which threads the
        # "worker_recovery_durable_handoff_to_daemon_tick" reason into the later
        # materialization / local-apply / workflow-resume / board-visible /
        # finalize phases and flips next_tick_requested. Pinning this proves the
        # cross-phase nonlocal yield ladder (38629 / 39106 / 39264-39296).
        self._save_job("job_handoff")
        handoff_summary = {
            "status": "completed",
            "claimed_count": 1,
            "executed_count": 1,
            "jobs": [
                {
                    "job_id": "job_handoff",
                    "daemon_events": [{"status": "completed"}],
                    "completion_callback_results": [],
                }
            ],
        }
        self.orchestrator._build_worker_recovery_daemon = (  # noqa: SLF001
            lambda payload: SimpleNamespace(run_once=lambda: dict(handoff_summary))
        )

        result = self.orchestrator.run_worker_recovery_once({"job_id": "job_handoff"})

        # worker_recovery itself ran; the yield was requested.
        self.assertEqual(self._phase(result, "worker_recovery")["status"], "completed")
        self.assertTrue(result["durable_work_handoff_yield"])
        self.assertTrue(result["next_tick_requested"])

        # Downstream durable phases are skipped with the threaded handoff reason.
        handoff_reason = "worker_recovery_durable_handoff_to_daemon_tick"
        for phase in (
            "profile_refill_event_level_materialization_followup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "workflow_resume",
            "board_visible_apply",
            "run_scope_projection_finalize",
            "snapshot_full_materialization",
        ):
            entry = self._phase(result, phase)
            self.assertEqual(entry["status"], "skipped", f"{phase} should be handoff-skipped")
            self.assertEqual(
                entry["reason"], handoff_reason, f"{phase} did not inherit the handoff reason"
            )

        # post_completion_reconcile is gated by the same daemon-owned-work
        # barrier with its own pinned reason.
        self.assertEqual(
            self._phase(result, "post_completion_reconcile")["reason"],
            "daemon_owned_work_open_before_post_completion_reconcile",
        )

        # The ordered phase sequence is preserved even though work yielded.
        self.assertEqual(self._phase_sequence(result), list(CHARACTERIZED_PHASE_NAMES))

    def test_profile_refill_submit_observed_suppresses_later_refill_and_apply(self) -> None:
        # profile_refill_submit_observed_this_tick is the canonical threaded
        # local (set at 38616, re-read at 38674/38807/38889): a pre-worker refill
        # that submits provider work (queued_worker_count > 0) must suppress the
        # later profile_prefetch_refill, the event-level materialization
        # follow-up, and the local_apply_backlog with their pinned handoff
        # reasons — without any of them being disabled by payload.
        self._save_job("job_refill_gate")
        self.orchestrator._run_profile_prefetch_refill_queue_once = (  # noqa: SLF001
            lambda payload: {"status": "active", "queued_worker_count": 2}
        )

        result = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": "job_refill_gate",
                "profile_prefetch_refill_before_worker_recovery": True,
            }
        )

        self.assertEqual(
            self._phase(result, "pre_worker_profile_prefetch_refill")["status"], "active"
        )
        self.assertEqual(
            self._phase(result, "profile_prefetch_refill")["reason"],
            "profile_refill_submit_budget_exhausted",
        )
        self.assertEqual(
            self._phase(result, "profile_refill_event_level_materialization_followup")["reason"],
            "profile_refill_submit_handoff_to_next_tick",
        )
        self.assertEqual(
            self._phase(result, "local_apply_backlog")["reason"],
            "profile_refill_submit_handoff_to_next_tick",
        )
        # The submit observation also drives a re-tick request.
        self.assertTrue(result["next_tick_requested"])

    def test_search_seed_resume_skip_job_ids_thread_into_workflow_resume(self) -> None:
        # search_seed_resume_skip_job_ids (38547) excludes jobs touched by
        # search-seed discovery from the subsequent workflow_resume's skip set.
        # Spy the resume callback to capture the skip_job_ids it receives and
        # pin that the threaded set arrives intact.
        self._save_job("job_seed_thread")
        captured_skip_sets: list[set[str]] = []

        def _resume_spy(summary: Any, **kwargs: Any) -> list[Any]:
            captured_skip_sets.append(set(kwargs.get("skip_job_ids") or set()))
            return []

        self.orchestrator._resume_blocked_workflows_after_recovery = _resume_spy  # noqa: SLF001
        self.orchestrator._run_search_seed_discovery_query_queue_once = (  # noqa: SLF001
            lambda payload: {"status": "active", "executed_count": 1}
        )
        self.orchestrator._search_seed_discovery_resume_skip_job_ids = (  # noqa: SLF001
            lambda discovery_result: {"job_seed_skip_me"}
        )

        self.orchestrator.run_worker_recovery_once({"job_id": "job_seed_thread"})

        self.assertTrue(captured_skip_sets, "workflow_resume must be invoked")
        self.assertIn("job_seed_skip_me", captured_skip_sets[0])

    # -- 1c. budget-exhaustion yield --------------------------------------------

    def test_tick_budget_exhaustion_skips_later_phases_and_requests_next_tick(self) -> None:
        # A 1ms total budget exhausts after the first metered phase; every later
        # phase records a "recovery_tick_budget_exhausted" skip and the epilogue
        # flips recovery_tick_budget_exhausted -> next_tick_requested (40018).
        result = self.orchestrator.run_worker_recovery_once(
            {"recovery_tick_total_budget_ms": 1}
        )

        self.assertTrue(result["recovery_tick_budget_exhausted"])
        self.assertTrue(result["next_tick_requested"])

        # The sequence is still fully recorded (each phase yields a skip row).
        self.assertEqual(self._phase_sequence(result), list(CHARACTERIZED_PHASE_NAMES))

        budget_skipped = [
            name
            for name in CHARACTERIZED_PHASE_NAMES
            if name != "total"
            and self._phase(result, name).get("reason") == "recovery_tick_budget_exhausted"
        ]
        # The overwhelming majority of phases yield to the exhausted budget.
        self.assertGreaterEqual(len(budget_skipped), 30)
        # The total rollup carries the budget-exhausted marker.
        total_row = self._phase(result, "total")
        self.assertTrue(total_row["budget_exhausted"])


if __name__ == "__main__":
    unittest.main()
