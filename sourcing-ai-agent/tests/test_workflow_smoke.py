import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError

from sourcing_agent.service_gate_coverage import validate_service_gate_coverage
from sourcing_agent.workflow_service_metrics import build_workflow_service_metrics
from sourcing_agent.workflow_smoke import (
    _build_board_probe_report,
    _build_candidate_page_filter_probe_sample,
    _build_case_level_smoke_exports,
    _build_post_preview_finalization_report,
    _build_progress_observability_report,
    _build_provider_case_report,
    _build_smoke_explain_digest,
    _build_synthetic_terminal_progress_sample,
    _build_workflow_benchmark_report,
    _build_workflow_wall_clock_report,
    _case_allows_early_results_ready,
    _drive_smoke_remote_provider_late_watcher_duplicates,
    _drive_smoke_remote_provider_webhook_recovery_once,
    _effective_asset_population_candidate_count,
    _ensure_results_runtime_details_for_smoke_report,
    _evaluate_smoke_expectations,
    _evaluate_smoke_record_completion,
    _fetch_smoke_candidate_page_payload,
    _fetch_smoke_dashboard_payload,
    _fetch_smoke_results_payload,
    _post_terminal_materialization_wait_state,
    _projection_board_runtime_state_for_smoke,
    _public_web_batches_for_smoke_window,
    _recovery_runs_from_service_status_payload,
    _run_company_public_web_smoke_action,
    _run_target_public_web_smoke_action,
    _settle_board_probe,
    _settle_post_terminal_worker_recovery,
    _should_auto_run_worker_recovery,
    _should_drive_smoke_remote_provider_events,
    _smoke_remote_provider_webhook_payload,
    _smoke_shared_recovery_signal_payload,
    _synchronize_post_terminal_recovery_with_service_metrics,
    _worker_can_receive_smoke_provider_webhook,
    load_smoke_cases,
    run_hosted_smoke_case,
    run_hosted_smoke_matrix,
    stage_summary_digest,
    summarize_smoke_timings,
)


class WorkflowSmokeTest(unittest.TestCase):
    def test_post_terminal_wait_state_tracks_projection_person_search_index(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path == "/api/jobs/job-public-read-model/materialization-items":
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "jproj_idx_ready",
                                "item_kind": "projection_person_search_index_build",
                                "status": "queued",
                                "phase": "queued",
                            },
                            {
                                "item_id": "jproj_layer_ready",
                                "item_kind": "projection_facet_layering_build",
                                "status": "queued",
                                "phase": "queued",
                            },
                        ]
                    }
                raise AssertionError(f"unexpected GET {path}")

        state = _post_terminal_materialization_wait_state(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-public-read-model",
        )

        self.assertEqual(state["pending_count"], 2)
        self.assertEqual(state["handoff_pending_count"], 0)
        self.assertEqual(state["projection_facet_layering_pending_count"], 1)
        self.assertEqual(state["projection_person_search_index_pending_count"], 1)

    def test_post_terminal_wait_state_uses_workflow_commands_as_canonical_source(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path == "/api/jobs/job-command-read-model/materialization-items":
                    return {
                        "job_materialization_items": [],
                        "workflow_commands": [
                            {
                                "command_id": "cmd_snapshot",
                                "command_type": "snapshot.compaction.run",
                                "owner": "snapshot_materialization_owner",
                                "status": "queued",
                                "payload": {
                                    "item_id": "snapshot-item",
                                    "stage_key": "snapshot_full_materialization",
                                },
                            },
                            {
                                "command_id": "cmd_layer",
                                "command_type": "projection.facet_layering.build",
                                "owner": "projection_facet_layering_owner",
                                "status": "running",
                                "payload": {
                                    "item_id": "layer-item",
                                    "stage_key": "projection_facet_layering_build",
                                },
                            },
                            {
                                "command_id": "cmd_terminal",
                                "command_type": "projection.person_search_index.build",
                                "owner": "projection_index_owner",
                                "status": "succeeded",
                                "payload": {"item_id": "index-item"},
                            },
                        ],
                    }
                raise AssertionError(f"unexpected GET {path}")

        state = _post_terminal_materialization_wait_state(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-command-read-model",
        )

        self.assertEqual(state["pending_count"], 2)
        self.assertEqual(state["background_snapshot_full_materialization_pending_count"], 1)
        self.assertEqual(state["projection_facet_layering_pending_count"], 1)
        self.assertEqual(state["projection_person_search_index_pending_count"], 0)
        self.assertEqual(state["running_count"], 1)
        self.assertEqual({item["source"] for item in state["items"]}, {"workflow_commands"})

    def test_recovery_runs_from_service_status_payload_preserves_job_daemon_tick_metrics(self) -> None:
        service_status_payload = {
            "status": "ok",
            "recovery_services": {
                "job_scoped": {
                    "service_name": "job-recovery-job-google-gemini",
                    "status": "stopped",
                    "tick": 5,
                    "updated_at": "2026-05-14T12:24:47+00:00",
                    "last_nonempty_summary": {
                        "status": "completed",
                        "recovery_tick_budget_exhausted": True,
                        "recovery_phase_metrics": {
                            "board_visible_apply": {
                                "phase": "board_visible_apply",
                                "owner": "board_visible_delta_apply_queue",
                                "status": "active",
                                "elapsed_ms": 76283,
                                "budget_exhausted": True,
                                "counts": {"candidate_count": 40, "claimed_count": 1, "completed_count": 1},
                            },
                            "total": {
                                "phase": "total",
                                "owner": "run_worker_recovery_once",
                                "status": "completed",
                                "elapsed_ms": 76735,
                                "budget_exhausted": True,
                            },
                        },
                    },
                }
            },
        }

        recovery_runs = _recovery_runs_from_service_status_payload(service_status_payload)
        metrics = build_workflow_service_metrics(worker_recovery_runs=recovery_runs)
        recovery = metrics["recovery_phase_metrics"]

        self.assertEqual(len(recovery_runs), 1)
        self.assertEqual(recovery_runs[0]["phase"], "job_scoped_recovery")
        self.assertEqual(recovery["phase_elapsed_ms_max"]["board_visible_apply"], 76283)
        self.assertEqual(recovery["total_elapsed_ms"]["max"], 76735)
        self.assertEqual(recovery["recovery_tick_budget_exhausted_count"], 1)

    def test_run_hosted_smoke_matrix_honors_case_max_poll_seconds(self) -> None:
        seen: list[float] = []

        def _fake_run_case(**kwargs):
            seen.append(float(kwargs["max_poll_seconds"]))
            return {"case": kwargs["case_name"], "final": {"smoke_ready": True}}

        with mock.patch("sourcing_agent.workflow_smoke.run_hosted_smoke_case", side_effect=_fake_run_case):
            summaries, failures = run_hosted_smoke_matrix(
                client=object(),
                cases=[
                    {"case": "default_timeout", "payload": {"raw_user_request": "OpenAI Agent"}},
                    {
                        "case": "long_latency_timeout",
                        "payload": {"raw_user_request": "Google Vision-language"},
                        "max_poll_seconds": 3000,
                    },
                ],
                reviewer="smoke-test",
                poll_seconds=1.0,
                max_poll_seconds=900.0,
            )

        self.assertEqual(failures, [])
        self.assertEqual([item["case"] for item in summaries], ["default_timeout", "long_latency_timeout"])
        self.assertEqual(seen, [900.0, 3000.0])

    def test_smoke_explain_digest_preserves_delta_baseline_contract_fields(self) -> None:
        digest = _build_smoke_explain_digest(
            explain={
                "status": "needs_plan_review",
                "request_preview": {
                    "target_company": "Google",
                    "target_scope": "full_company_asset",
                    "keywords": ["Vision-language"],
                },
                "asset_reuse_plan": {
                    "planner_mode": "delta_from_snapshot",
                    "requires_delta_acquisition": True,
                    "baseline_snapshot_id": "20260511T000000",
                },
                "dispatch_preview": {
                    "strategy": "delta_from_snapshot",
                    "matched_snapshot_id": "20260511T000000",
                    "matched_job": {},
                    "request_after_dispatch_hints": {
                        "execution_preferences": {
                            "delta_baseline_snapshot_id": "20260511T000000",
                        }
                    },
                },
                "effective_execution_semantics": {
                    "effective_acquisition_mode": "baseline_reuse_with_delta",
                    "default_results_mode": "asset_population",
                },
            },
            effective_payload={
                "analysis_stage_mode": "single_stage",
                "execution_preferences": {
                    "harvest_profile_actor_global_inflight": 4,
                    "harvest_profile_batch_submit_global_inflight": 4,
                },
            },
        )

        self.assertEqual(digest["asset_reuse_baseline_snapshot_id"], "20260511T000000")
        self.assertEqual(digest["dispatch_matched_snapshot_id"], "20260511T000000")
        self.assertEqual(digest["request_delta_baseline_snapshot_id"], "20260511T000000")
        self.assertEqual(digest["dispatch_strategy"], "delta_from_snapshot")
        self.assertTrue(digest["requires_delta_acquisition"])

    def test_provider_case_report_exposes_projection_cutover_contract(self) -> None:
        projection = {
            "projection_id": "proj_contract",
            "source_run_id": "job-contract",
            "visible_member_count": 2,
            "counts": {"result_count": 2},
            "readiness": {"profile_required_count": 1, "profile_ready_count": 1, "card_ready_count": 1},
        }
        projection_read_contract = {
            "source": "serving_projection_members",
            "fallback_used": False,
            "fail_closed": True,
            "legacy_endpoint": "retired",
        }
        report = _build_provider_case_report(
            explain_payload={},
            job_summary={},
            results_payload={
                "status": "ready",
                "job": {"job_id": "job-contract", "status": "completed"},
                "asset_population": {
                    "available": True,
                    "source_projection_id": "proj_contract",
                    "candidate_count": 2,
                },
                "projection": projection,
                "read_contract": projection_read_contract,
            },
            dashboard_payload={
                "status": "ok",
                "projection": projection,
                "read_contract": projection_read_contract,
                "board_runtime_state": {
                    "expected_candidate_count": 2,
                    "served_candidate_count": 2,
                    "published_candidate_count": 2,
                },
            },
            candidate_page_payload={
                "status": "ready",
                "total_candidates": 2,
                "returned_count": 1,
                "projection": projection,
                "read_contract": {
                    "source": "serving_projection_members",
                    "fallback_used": False,
                    "fail_closed": True,
                },
                "candidates": [{"candidate_id": "cand-a"}],
            },
            progress_payload={
                "status": "completed",
                "board_runtime_state": {
                    "expected_candidate_count": 2,
                    "served_candidate_count": 2,
                    "published_candidate_count": 2,
                },
            },
            timings_ms={},
        )

        cutover = report["projection_cutover"]
        self.assertTrue(cutover["report_available"])
        self.assertTrue(cutover["run_projection_link_present"])
        self.assertFalse(cutover["projection_missing"])
        self.assertFalse(cutover["legacy_public_reader_fallback_used"])
        self.assertFalse(cutover["legacy_endpoint_normal_path_used"])
        self.assertEqual(cutover["projection_ids"], ["proj_contract"])
        artifact_coherence = report["legacy_artifact_coherence"]
        self.assertTrue(artifact_coherence["report_available"])
        self.assertTrue(artifact_coherence["terminal_coherent"])
        self.assertFalse(artifact_coherence["terminal_drift_detected"])

    def test_provider_case_report_exposes_legacy_artifact_terminal_drift(self) -> None:
        projection = {
            "projection_id": "proj_contract",
            "source_run_id": "job-contract",
            "visible_member_count": 140,
            "counts": {"result_count": 140},
        }
        report = _build_provider_case_report(
            explain_payload={},
            job_payload={"job_id": "job-contract", "status": "running", "stage": "retrieving"},
            job_summary={},
            results_payload={
                "status": "ready",
                "job": {
                    "job_id": "job-contract",
                    "status": "completed",
                    "stage": "completed",
                },
                "asset_population": {
                    "available": True,
                    "source_projection_id": "proj_contract",
                    "candidate_count": 140,
                },
                "projection": projection,
                "read_contract": {
                    "source": "serving_projection_members",
                    "fallback_used": False,
                    "fail_closed": True,
                    "legacy_endpoint": "retired",
                },
            },
            dashboard_payload={
                "status": "ok",
                "projection": projection,
                "read_contract": {
                    "source": "serving_projection_members",
                    "fallback_used": False,
                    "fail_closed": True,
                },
                "board_runtime_state": {
                    "expected_candidate_count": 140,
                    "served_candidate_count": 140,
                    "published_candidate_count": 140,
                },
            },
            candidate_page_payload={
                "status": "ready",
                "total_candidates": 140,
                "returned_count": 24,
                "projection": projection,
                "read_contract": {
                    "source": "serving_projection_members",
                    "fallback_used": False,
                    "fail_closed": True,
                },
                "candidates": [{"candidate_id": "cand-a"}],
            },
            progress_payload={"status": "running", "stage": "retrieving"},
            timings_ms={},
        )

        artifact_coherence = report["legacy_artifact_coherence"]
        self.assertTrue(artifact_coherence["report_available"])
        self.assertFalse(artifact_coherence["terminal_coherent"])
        self.assertTrue(artifact_coherence["terminal_drift_detected"])
        self.assertTrue(artifact_coherence["blocking_violation"])

        failures = _evaluate_smoke_expectations(
            record={"provider_case_report": report, "final": {"raw_job_status": "running"}},
            expectations={"require_legacy_artifact_coherence_report": True},
            provider_invocations=[],
        )
        self.assertTrue(any("legacy artifact coherence" in failure for failure in failures))

    def test_smoke_shared_recovery_signal_payload_carries_no_recovery_controls(self) -> None:
        self.assertEqual(_smoke_shared_recovery_signal_payload(), {})

    def test_smoke_reader_helpers_follow_projection_cutover_410(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.gets: list[str] = []

            def _retired(self, path: str) -> HTTPError:
                payload = {
                    "status": "retired",
                    "reason": "legacy_job_result_endpoint_retired",
                    "run_id": "job-proj",
                    "projection_id": "proj_smoke",
                    "legacy_endpoint": path.split("?", 1)[0],
                    "read_contract": {
                        "source": "run_projection_links+serving_projections",
                        "fallback_used": False,
                        "fail_closed": True,
                    },
                }
                return HTTPError(
                    url=path,
                    code=410,
                    msg="retired",
                    hdrs=None,
                    fp=BytesIO(json.dumps(payload).encode("utf-8")),
                )

            def get(self, path: str) -> dict:
                self.gets.append(path)
                if path.startswith("/api/jobs/job-proj/results"):
                    raise self._retired(path)
                if path.startswith("/api/jobs/job-proj/dashboard"):
                    raise self._retired(path)
                if path.startswith("/api/jobs/job-proj/candidates"):
                    raise self._retired(path)
                if path == "/api/jobs/job-proj/progress":
                    return {
                        "job_id": "job-proj",
                        "status": "completed",
                        "board_runtime_state": {
                            "schema_version": "projection_smoke_v1",
                            "job_id": "job-proj",
                            "result_mode": "asset_population",
                            "phase": "current_snapshot_serving",
                            "publication_status": "complete",
                            "expected_candidate_count": 2,
                            "served_candidate_count": 2,
                            "published_candidate_count": 2,
                            "display_ready_candidate_count": 2,
                            "row_hydration_target_count": 2,
                        },
                        "result_view_lifecycle": {
                            "job_id": "job-proj",
                            "expected_candidate_count": 2,
                            "served_candidate_count": 2,
                        },
                    }
                if path == "/api/projections/proj_smoke":
                    return {
                        "status": "ready",
                        "projection": {
                            "projection_id": "proj_smoke",
                            "source_run_id": "job-proj",
                            "visible_member_count": 2,
                            "scope_spec": {
                                "target_company": "OpenAI",
                                "snapshot_id": "snap-1",
                                "asset_view": "canonical_merged",
                            },
                            "counts": {
                                "result_count": 2,
                                "profile_fetch_required_count": 1,
                                "profile_fetched_count": 1,
                                "card_materialized_count": 1,
                                "count_scope": "exact_projection",
                            },
                            "readiness": {
                                "row_count": 2,
                                "profile_required_count": 1,
                                "profile_ready_count": 1,
                                "card_ready_count": 1,
                            },
                        },
                    }
                if path.startswith("/api/projections/proj_smoke/candidates"):
                    return {
                        "status": "ready",
                        "total_candidates": 2,
                        "filtered_candidate_count": 2,
                        "candidates": [
                            {
                                "projection_id": "proj_smoke",
                                "candidate_identity_key": "linkedin:a",
                                "person_identity_key": "linkedin:a",
                                "candidate_id": "cand-a",
                                "employment_scope": "current",
                                "profile_readiness": "ready",
                                "card_readiness": "ready",
                                "public_summary": {
                                    "candidate_id": "cand-a",
                                    "display_name": "Ada",
                                    "headline": "Engineer",
                                    "linkedin_url": "https://www.linkedin.com/in/a",
                                },
                                "projection_metrics": {"has_profile_detail": True},
                            }
                        ],
                        "facet_summary": {"status": "complete", "count_scope": "exact_projection"},
                        "filter_contract": {
                            "source": "serving_projection_members",
                            "fallback_used": False,
                        },
                    }
                raise AssertionError(f"unexpected GET {path}")

        client = FakeClient()
        job_payload = {"job": {"job_id": "job-proj", "status": "completed"}}

        results = _fetch_smoke_results_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            job_payload=job_payload,
            include_runtime_details=False,
            include_candidates=False,
        )
        dashboard = _fetch_smoke_dashboard_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            job_payload=job_payload,
        )
        candidates = _fetch_smoke_candidate_page_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            path="/api/jobs/job-proj/candidates?offset=0&limit=24&lightweight=1",
            job_payload=job_payload,
        )

        self.assertEqual(results["asset_population"]["source_kind"], "serving_projection")
        self.assertEqual(results["asset_population"]["candidate_count"], 2)
        self.assertEqual(dashboard["board_runtime_state"]["expected_candidate_count"], 2)
        self.assertEqual(candidates["total_candidates"], 2)
        self.assertEqual(candidates["returned_count"], 1)
        self.assertEqual(candidates["candidates"][0]["display_name"], "Ada")
        self.assertFalse(candidates["read_contract"]["fallback_used"])

    def test_smoke_reader_helpers_prefer_ready_projection_before_legacy_cutover(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.gets: list[str] = []

            def get(self, path: str) -> dict:
                self.gets.append(path)
                if path.startswith("/api/jobs/job-proj/results"):
                    return {
                        "status": "ready",
                        "job": {"job_id": "job-proj", "status": "running", "stage": "acquiring"},
                    }
                if path.startswith("/api/jobs/job-proj/dashboard"):
                    raise AssertionError("legacy dashboard endpoint should not be used when projection is ready")
                if path.startswith("/api/jobs/job-proj/candidates"):
                    raise AssertionError("legacy candidates endpoint should not be used when projection is ready")
                if path == "/api/runs/job-proj/projection-link":
                    return {"status": "ready", "run_id": "job-proj", "projection_id": "proj_smoke"}
                if path == "/api/jobs/job-proj/progress":
                    return {
                        "job_id": "job-proj",
                        "status": "completed",
                        "stage": "completed",
                        "board_runtime_state": {
                            "schema_version": "projection_smoke_v1",
                            "job_id": "job-proj",
                            "phase": "current_snapshot_serving",
                            "publication_status": "complete",
                            "expected_candidate_count": 1,
                            "served_candidate_count": 1,
                            "published_candidate_count": 1,
                            "display_ready_candidate_count": 1,
                            "row_hydration_target_count": 1,
                        },
                    }
                if path == "/api/projections/proj_smoke":
                    return {
                        "status": "ready",
                        "projection": {
                            "projection_id": "proj_smoke",
                            "source_run_id": "job-proj",
                            "visible_member_count": 1,
                            "scope_spec": {"target_company": "OpenAI"},
                            "counts": {
                                "result_count": 1,
                                "profile_fetch_required_count": 1,
                                "profile_fetched_count": 1,
                                "card_materialized_count": 1,
                            },
                            "readiness": {
                                "row_count": 1,
                                "profile_required_count": 1,
                                "profile_ready_count": 1,
                                "card_ready_count": 1,
                            },
                        },
                    }
                if path.startswith("/api/projections/proj_smoke/candidates"):
                    return {
                        "status": "ready",
                        "total_candidates": 1,
                        "filtered_candidate_count": 1,
                        "candidates": [
                            {
                                "projection_id": "proj_smoke",
                                "candidate_identity_key": "linkedin:a",
                                "person_identity_key": "linkedin:a",
                                "candidate_id": "cand-a",
                                "employment_scope": "current",
                                "profile_readiness": "ready",
                                "card_readiness": "ready",
                                "public_summary": {"display_name": "Ada"},
                                "projection_metrics": {"has_profile_detail": True},
                            }
                        ],
                        "facet_summary": {"status": "complete", "count_scope": "exact_projection"},
                        "filter_contract": {
                            "source": "serving_projection_members",
                            "fallback_used": False,
                        },
                    }
                raise AssertionError(f"unexpected GET {path}")

        client = FakeClient()
        stale_job_payload = {"job": {"job_id": "job-proj", "status": "running", "stage": "acquiring"}}

        results = _fetch_smoke_results_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            job_payload=stale_job_payload,
            include_runtime_details=False,
            include_candidates=False,
        )
        dashboard = _fetch_smoke_dashboard_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            job_payload=stale_job_payload,
        )
        candidates = _fetch_smoke_candidate_page_payload(
            client,  # type: ignore[arg-type]
            job_id="job-proj",
            path="/api/jobs/job-proj/candidates?offset=0&limit=24&lightweight=1",
            job_payload=stale_job_payload,
        )

        self.assertEqual(results["job"]["status"], "running")
        self.assertEqual(dashboard["projection"]["projection_id"], "proj_smoke")
        self.assertEqual(candidates["total_candidates"], 1)
        self.assertNotIn("/api/jobs/job-proj/dashboard?include_candidates=0", client.gets)

    def test_run_hosted_smoke_case_preserves_runtime_mode_after_plan_review(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[tuple[str, dict]] = []
                self.gets: list[str] = []
                self.shared_signal_count = 0

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                self.posts.append((path, dict(payload)))
                if path == "/api/workflows/explain":
                    return {"status": "ready"}
                if path == "/api/plan/submit":
                    # C1: plan compile is async — submit returns pending + a history
                    # id the runner polls below for the hydrated review.
                    return {"status": "pending", "history_id": "history-managed-review"}
                if path == "/api/plan/review":
                    return {"status": "reviewed", "review_id": 42}
                if path == "/api/workflows":
                    return {"status": "queued", "job_id": "job-managed-review"}
                if path == "/api/workers/daemon/run-once":
                    self.assert_signal_payload(payload)
                    self.shared_signal_count += 1
                    return {
                        "status": "accepted",
                        "mode": "shared_recovery_signal",
                        "shared_recovery_signal": {
                            "status": "signaled",
                            "service_name": "worker-recovery-daemon",
                        },
                    }
                raise AssertionError(f"unexpected POST {path}")

            @staticmethod
            def assert_signal_payload(payload: dict) -> None:
                if payload != {}:
                    raise AssertionError(f"shared recovery signal carried controls: {payload}")

            def get(self, path: str) -> dict:
                self.gets.append(path)
                if path == "/api/frontend-history/history-managed-review":
                    # Hydrated plan recovery the async submit flow polls for.
                    return {
                        "status": "found",
                        "recovery": {
                            "history_id": "history-managed-review",
                            "review_id": 42,
                            "plan": {"target_company": "Google Gemini"},
                            "plan_review_gate": {"status": "required", "risk_level": "medium"},
                            "plan_review_session": {"review_id": 42},
                            "metadata": {"plan_generation": {"status": "completed"}},
                        },
                    }
                if path == "/api/jobs/job-managed-review/progress":
                    if self.shared_signal_count <= 0:
                        return {
                            "job_id": "job-managed-review",
                            "status": "blocked",
                            "stage": "acquiring",
                            "current_message": "waiting for shared recovery",
                            "progress": {"counters": {"queued_worker_count": 1}},
                        }
                    return {
                        "job_id": "job-managed-review",
                        "status": "completed",
                        "stage": "completed",
                        "current_message": "completed",
                    }
                if path == "/api/jobs/job-managed-review":
                    return {"job": {"job_id": "job-managed-review", "status": "completed"}, "events": []}
                if path.split("?", 1)[0] == "/api/jobs/job-managed-review/results":
                    return {
                        "job": {"job_id": "job-managed-review", "status": "completed"},
                        "results": [],
                        "workflow_stage_summaries": {"summaries": {}},
                    }
                if path == "/api/dashboard" or path.split("?", 1)[0] == "/api/jobs/job-managed-review/dashboard":
                    return {"status": "ok"}
                if path.startswith("/api/jobs/job-managed-review/candidates"):
                    return {"items": [], "candidates": [], "summary": {}}
                if path.startswith("/api/jobs/job-managed-review/board-patches"):
                    return {"patches": []}
                if path == "/api/jobs/job-managed-review/workers":
                    return {"agent_workers": []}
                if path.startswith("/api/workers/daemon/status?"):
                    return {
                        "status": "ok",
                        "recovery_services": {
                            "shared": {
                                "service_name": "worker-recovery-daemon",
                                "status": "running",
                                "tick": self.shared_signal_count,
                                "last_nonempty_summary": {
                                    "status": "completed",
                                    "recovery_phase_metrics": {
                                        "total": {
                                            "status": "completed",
                                            "owner": "run_worker_recovery_once",
                                            "elapsed_ms": 1,
                                            "counts": {},
                                        }
                                    },
                                },
                            }
                        },
                    }
                raise AssertionError(f"unexpected GET {path}")

        client = FakeClient()
        record = run_hosted_smoke_case(
            client,  # type: ignore[arg-type]
            case_name="managed_review_start",
            payload={
                "raw_user_request": "Find Google Gemini people",
                "runtime_execution_mode": "managed_subprocess",
                "auto_job_daemon": False,
            },
            reviewer="smoke-test",
            poll_seconds=0.01,
            max_poll_seconds=1.0,
        )

        workflow_posts = [payload for path, payload in client.posts if path == "/api/workflows"]
        self.assertEqual(len(workflow_posts), 1)
        self.assertEqual(workflow_posts[0]["plan_review_id"], 42)
        self.assertEqual(workflow_posts[0]["runtime_execution_mode"], "managed_subprocess")
        self.assertFalse(workflow_posts[0]["auto_job_daemon"])
        self.assertEqual(record["start"]["job_id"], "job-managed-review")
        self.assertEqual(record["job_id"], "job-managed-review")
        self.assertEqual(record["final"]["job_id"], "job-managed-review")
        self.assertEqual(record["status"], "completed")
        self.assertEqual(len(record["shared_recovery_signals"]), 1)
        self.assertEqual(record["shared_recovery_signals"][0]["signal_status"], "signaled")
        self.assertEqual(len(record["worker_recovery"]), 1)
        self.assertIn("recovery_phase_metrics", record["worker_recovery"][0])
        self.assertNotIn("recovery_phase_metrics", record["shared_recovery_signals"][0])

    def test_load_smoke_cases_preserves_expectations(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            matrix_path = Path(tempdir) / "matrix.json"
            matrix_path.write_text(
                json.dumps(
                    {
                        "cases": [
                            {
                                "case": "openai_agent_delta",
                                "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
                                "seed_reference_runtime": True,
                                "runtime_isolation": "case",
                                "max_poll_seconds": 1800,
                                "coverage_tags": ["openai_agent_scoped_delta_profile_tail"],
                                "runtime_env": {
                                    "HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3",
                                    "HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE": 3,
                                },
                                "payload": {"raw_user_request": "帮我找OpenAI做Agent方向的人"},
                                "expectations": {
                                    "allow_results_ready_nonterminal": False,
                                    "min_profile_url_total_count": 200,
                                },
                                "target_public_web_action": {
                                    "enabled": True,
                                    "record_limit": 1,
                                    "max_recovery_rounds": 8,
                                },
                                "company_public_web_action": {
                                    "enabled": True,
                                    "collection_mode": "collector_bundle",
                                },
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            cases = load_smoke_cases(str(matrix_path), {"openai_agent_delta"})

        self.assertEqual(cases[0]["expectations"]["min_profile_url_total_count"], 200)
        self.assertFalse(cases[0]["expectations"]["allow_results_ready_nonterminal"])
        self.assertTrue(cases[0]["seed_reference_runtime"])
        self.assertEqual(cases[0]["runtime_isolation"], "case")
        self.assertEqual(cases[0]["max_poll_seconds"], 1800)
        self.assertEqual(cases[0]["coverage_tags"], ["openai_agent_scoped_delta_profile_tail"])
        self.assertEqual(
            cases[0]["scripted_scenario"],
            "configs/scripted/openai_agent_scoped_delta_streaming.json",
        )
        self.assertEqual(
            cases[0]["runtime_env"],
            {
                "HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3",
                "HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE": "3",
            },
        )
        self.assertEqual(
            cases[0]["target_public_web_action"],
            {"enabled": True, "record_limit": 1, "max_recovery_rounds": 8},
        )
        self.assertEqual(
            cases[0]["company_public_web_action"],
            {"enabled": True, "collection_mode": "collector_bundle"},
        )

    def test_company_public_web_smoke_action_refreshes_and_lists_company_assets(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[tuple[str, dict]] = []

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                self.posts.append((path, payload))
                return {
                    "status": "completed",
                    "run": {"run_id": "company-public-web-run-1"},
                    "summary": {"asset_count": 1, "collector_record_count": 1},
                }

            def get(self, path: str) -> dict:
                self.last_get = path
                return {
                    "status": "ok",
                    "runs": [
                        {
                            "run_id": "company-public-web-run-1",
                            "status": "completed",
                            "summary": {"asset_count": 1, "collector_record_count": 1},
                        }
                    ],
                    "assets": [{"asset_id": "asset-1"}],
                }

        client = FakeClient()
        action, timings_ms = _run_company_public_web_smoke_action(
            client,  # type: ignore[arg-type]
            action={
                "enabled": True,
                "collector_documents": [
                    {
                        "url": "https://openai.com/research/rss.xml",
                        "content": "<rss><channel><item><title>Research</title><link>https://openai.com/research</link></item></channel></rss>",
                    }
                ],
                "collector_sources": [{"url": "https://openai.com/research/arxiv.xml", "collector_type": "arxiv"}],
                "options": {"discover_collector_sources": True},
            },
            case_name="company_public_web_case",
            explain={"request_preview": {"target_company": "OpenAI"}},
        )

        self.assertGreaterEqual(timings_ms, 0.0)
        self.assertEqual(action["status"], "completed")
        self.assertEqual(client.posts[0][0], "/api/company-assets/public-web")
        self.assertEqual(client.posts[0][1]["target_company"], "OpenAI")
        self.assertEqual(client.posts[0][1]["options"]["collection_mode"], "collector_bundle")
        self.assertTrue(client.posts[0][1]["options"]["discover_collector_sources"])
        self.assertEqual(client.posts[0][1]["collector_sources"][0]["collector_type"], "arxiv")
        self.assertIn("/api/company-assets/public-web?target_company=OpenAI", client.last_get)

    def test_target_public_web_smoke_action_imports_and_signals_shared_worker(self) -> None:
        test_case = self

        class FakeClient:
            def __init__(self) -> None:
                self.signal_calls = 0
                self.daemon_completed = False
                self.batch = {
                    "batch_id": "batch-1",
                    "status": "queued",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "summary": {"run_count": 1, "status": "queued"},
                }

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                if path == "/api/crm/records":
                    test_case.assertEqual(payload["projection_id"], "proj-job-1")
                    test_case.assertEqual(payload["candidate_identity_key"], "linkedin:target-1")
                    return {
                        "status": "upserted",
                        "crm_record": {"crm_record_id": "crm-target-1", "record_id": "crm-target-1"},
                    }
                if path == "/api/crm/records/public-web-search":
                    test_case.assertEqual(payload["crm_record_ids"], ["crm-target-1"])
                    test_case.assertTrue(payload["force_refresh"])
                    return {
                        "status": "queued",
                        "batch": self.batch,
                        "job": {"job_id": "tc-public-web-batch-1"},
                        "workflow_command": {
                            "command_id": "cmd_crm_public_web_queue_batch_1",
                            "workflow_run_id": "wf_crm_public_web_batch_1",
                            "operation_id": "op_crm_public_web_batch_1",
                            "command_type": "crm.public_web.queue_batch",
                            "owner": "crm_public_web_owner",
                            "stage_id": "crm_public_web_queue_batch",
                            "causal_group_id": "crm.public_web.queue_batch:batch-1",
                            "source_event_id": "evt_crm_public_web_queue_batch_1",
                            "source_event_type": "CommandPlanRequested",
                            "produced_entity_counts": {"crm_public_web_run": 1, "crm_record": 1},
                            "readiness_effect": "crm_public_web_workers_queued",
                            "causality_schema_version": "command_causality_v1",
                            "status": "succeeded",
                            "idempotency_key": "crm.public_web.queue_batch:batch-1",
                        },
                        "worker_summary": {
                            "command_type": "crm.public_web.queue_batch",
                            "owner": "crm_public_web_owner",
                            "legacy_bridge_used": False,
                        },
                    }
                if path == "/api/crm/records/public-web-search/poll":
                    test_case.assertEqual(payload["batch_id"], "batch-1")
                    return {"status": "ok", "batches": [self.batch], "runs": []}
                if path == "/api/workers/daemon/run-once":
                    test_case.assertEqual(payload, {})
                    self.signal_calls += 1
                    return {
                        "status": "accepted",
                        "mode": "shared_recovery_signal",
                        "shared_recovery_signal": {
                            "status": "signaled",
                            "service_name": "worker-recovery-daemon",
                        },
                    }
                raise AssertionError(f"unexpected POST {path}")

            def get(self, path: str) -> dict:
                if path.startswith("/api/workers/daemon/status?"):
                    self.daemon_completed = self.signal_calls > 0
                    if self.daemon_completed:
                        self.batch = {
                            **self.batch,
                            "status": "completed",
                            "summary": {
                                "run_count": 1,
                                "completed_count": 1,
                                "phase_metrics": {
                                    "service_guardrail_violation_detected": False,
                                    "metric_run_count": 1,
                                },
                            },
                        }
                    return {
                        "status": "ok",
                        "recovery_services": {
                            "shared": {
                                "service_name": "worker-recovery-daemon",
                                "status": "running",
                                "tick": self.signal_calls,
                            }
                        },
                    }
                if path == "/api/runs/job-1/projection-link":
                    return {"status": "ready", "run_id": "job-1", "projection_id": "proj-job-1"}
                if path == "/api/projections/proj-job-1/candidates?offset=0&limit=1":
                    return {
                        "status": "ready",
                        "projection_id": "proj-job-1",
                        "candidates": [{"candidate_identity_key": "linkedin:target-1"}],
                        "filtered_candidate_count": 1,
                    }
                if path == "/api/jobs/tc-public-web-batch-1/workers":
                    status = "completed" if self.daemon_completed else "running"
                    return {
                        "agent_workers": [
                            {
                                "worker_id": 7,
                                "status": status,
                                "metadata": {"recovery_kind": "target_candidate_public_web_search"},
                            }
                        ]
                    }
                raise AssertionError(f"unexpected GET {path}")

        action, recovery_runs, timings_ms = _run_target_public_web_smoke_action(
            FakeClient(),  # type: ignore[arg-type]
            source_job_id="job-1",
            action={
                "enabled": True,
                "record_limit": 1,
                "max_recovery_rounds": 4,
                "options": {"fetch_content": False, "ai_extraction": "off"},
            },
            poll_seconds=0.01,
            case_name="target_public_web_case",
        )

        self.assertEqual(action["status"], "completed")
        self.assertEqual(action["search"]["batch_id"], "batch-1")
        self.assertEqual(action["import"]["source_projection_id"], "proj-job-1")
        self.assertEqual(action["search"]["crm_record_ids"], ["crm-target-1"])
        self.assertEqual(action["workflow_command"]["command_type"], "crm.public_web.queue_batch")
        self.assertEqual(action["search"]["workflow_command"]["owner"], "crm_public_web_owner")
        self.assertTrue(action["recovery"]["settled"])
        self.assertEqual(len(recovery_runs), 1)
        self.assertEqual(recovery_runs[0]["signal_status"], "signaled")
        self.assertEqual(recovery_runs[0]["shared_recovery_signal_count"], 1)
        self.assertGreaterEqual(timings_ms, 0.0)

    def test_load_smoke_cases_rejects_unknown_expectation_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            matrix_path = Path(tempdir) / "matrix.json"
            matrix_path.write_text(
                json.dumps(
                    {
                        "cases": [
                            {
                                "case": "bad_gate",
                                "payload": {"raw_user_request": "帮我找OpenAI做Agent方向的人"},
                                "expectations": {"max_typo_provider_lag_ms": 1},
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "unknown_expectation:max_typo_provider_lag_ms"):
                load_smoke_cases(str(matrix_path), {"bad_gate"})

    def test_nightly_google_large_opts_into_background_snapshot_drain_for_finalization_report(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/nightly_long_latency_smoke_matrix.json",
            {"nightly_google_vision_language_large_baseline_shard_long_latency"},
        )

        expectations = dict(cases[0].get("expectations") or {})
        self.assertTrue(expectations["require_post_preview_finalization_observed"])
        self.assertTrue(expectations["drain_background_snapshot_full_materialization_post_terminal"])

    def test_openai_infra_matrix_declares_provider_quality_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/openai_infra_stage1_lane_skew_smoke_matrix.json",
            {"openai_infra_stage1_lane_skew_zero_current_former_delta"},
        )

        expectations = dict(cases[0].get("expectations") or {})
        self.assertEqual(expectations["max_provider_io_actor_run_duration_ms"], 120000)
        self.assertEqual(expectations["max_provider_io_dataset_download_duration_ms"], 6000)
        self.assertEqual(expectations["max_provider_anomaly_count"], 6)
        self.assertEqual(expectations["max_provider_zero_result_retry_count"], 4)
        self.assertEqual(expectations["max_provider_zero_result_accepted_count"], 1)
        self.assertEqual(expectations["max_provider_empty_page_range_count"], 0)

    def test_openai_infra_provider_quality_scoped_live_matrix_declares_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/openai_infra_provider_quality_scoped_live_smoke_matrix.json",
            {"openai_infra_provider_quality_scoped_live"},
        )

        expectations = dict(cases[0].get("expectations") or {})
        self.assertEqual(cases[0]["coverage_tags"], ["openai_infra_provider_quality_scoped_live"])
        self.assertEqual(cases[0]["review_decision"], {"force_fresh_run": True})
        self.assertEqual(expectations["expect_explain_dispatch_strategy"], "new_job")
        self.assertEqual(expectations["expect_explain_effective_acquisition_mode"], "scoped_live_search")
        self.assertEqual(expectations["max_provider_io_actor_run_duration_ms"], 120000)
        self.assertEqual(expectations["max_provider_anomaly_count"], 6)

    def test_google_gemini_provider_quality_matrix_declares_page_coverage_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/google_gemini_provider_quality_smoke_matrix.json",
            {"google_gemini_provider_quality_scoped_live"},
        )

        expectations = dict(cases[0].get("expectations") or {})
        self.assertEqual(cases[0]["coverage_tags"], ["google_gemini_provider_quality_scoped_live"])
        self.assertEqual(cases[0]["review_decision"], {"force_fresh_run": True})
        self.assertEqual(expectations["expect_explain_dispatch_strategy"], "new_job")
        self.assertEqual(expectations["expect_explain_effective_acquisition_mode"], "scoped_live_search")
        self.assertTrue(expectations["require_no_remote_provider_event_driver_failure"])
        self.assertEqual(expectations["max_remote_provider_event_lag_ms"], 30000)
        self.assertEqual(expectations["max_provider_empty_scale_count"], 1)
        self.assertEqual(expectations["max_provider_probe_total_drift_count"], 1)
        self.assertEqual(expectations["max_provider_empty_page_range_count"], 1)
        self.assertEqual(expectations["max_provider_single_page_retry_count"], 2)
        self.assertEqual(expectations["min_provider_anomaly_count"], 10)
        self.assertEqual(expectations["min_provider_zero_result_retry_count"], 4)
        self.assertEqual(expectations["min_provider_zero_result_retry_exhausted_count"], 1)
        self.assertEqual(expectations["min_provider_empty_scale_count"], 1)
        self.assertEqual(expectations["min_provider_probe_total_drift_count"], 1)
        self.assertEqual(expectations["min_provider_empty_page_range_count"], 1)
        self.assertEqual(expectations["min_provider_single_page_retry_count"], 2)
        self.assertEqual(expectations["min_provider_invocations_by_logical_name"]["harvest_profile_scraper_batch"], 1)
        self.assertEqual(expectations["min_agent_worker_count"], 1)
        self.assertEqual(expectations["min_remote_provider_event_count"], 1)

    def test_google_gemini_large_baseline_small_former_matrix_declares_pressure_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/google_gemini_large_baseline_small_former_real_asset_smoke_matrix.json",
            {"google_gemini_large_baseline_small_former_real_asset"},
        )

        case = cases[0]
        expectations = dict(case.get("expectations") or {})
        runtime_env = dict(case.get("runtime_env") or {})
        self.assertIn("large_baseline_small_former_shard", case["coverage_tags"])
        self.assertEqual(runtime_env["SOURCING_SEED_GOOGLE_LARGE_BASELINE_MAX_CANDIDATES"], "9000")
        self.assertEqual(runtime_env["SOURCING_SEED_GOOGLE_LARGE_BASELINE_EXCLUDE_TERMS"], "gemini")
        self.assertEqual(expectations["expect_latest_stage1_current_search_returned_count"], 0)
        self.assertEqual(expectations["expect_latest_stage1_former_search_returned_count"], 120)
        self.assertEqual(expectations["min_latest_lifecycle_baseline_candidate_count"], 8000)
        self.assertEqual(expectations["min_profile_batch_size_max"], 120)
        self.assertEqual(expectations["max_profile_batch_size_max"], 200)
        self.assertEqual(expectations["min_agent_worker_count"], 1)
        self.assertEqual(expectations["min_remote_actor_worker_count"], 1)

    def test_target_public_web_service_matrix_declares_real_action_and_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/target_public_web_service_smoke_matrix.json",
            {"target_public_web_service_slo_from_workflow_result"},
        )

        action = dict(cases[0].get("target_public_web_action") or {})
        expectations = dict(cases[0].get("expectations") or {})
        self.assertEqual(cases[0]["coverage_tags"], ["target_candidate_public_web_service_slo"])
        self.assertTrue(action["enabled"])
        self.assertEqual(action["record_limit"], 1)
        self.assertEqual(action["options"]["ai_extraction"], "off")
        self.assertTrue(expectations["require_no_target_public_web_guardrail_violation"])
        self.assertTrue(expectations["require_crm_public_web_queue_batch_command"])
        self.assertTrue(expectations["require_legacy_public_web_retirement_ready"])
        self.assertEqual(expectations["max_target_public_web_remote_pending_run_count"], 0)
        self.assertEqual(expectations["max_target_public_web_missing_phase_metric_count"], 0)
        self.assertEqual(expectations["max_target_public_web_queue_batch_command_pending_count"], 0)
        self.assertIn("document_fetch", expectations["max_target_public_web_duration_by_phase_ms"])

    def test_company_public_web_service_matrix_declares_real_collector_action_and_slos(self) -> None:
        cases = load_smoke_cases(
            "configs/scripted/company_public_web_service_smoke_matrix.json",
            {"company_public_web_collector_bundle_from_workflow_result"},
        )

        action = dict(cases[0].get("company_public_web_action") or {})
        expectations = dict(cases[0].get("expectations") or {})
        self.assertEqual(cases[0]["coverage_tags"], ["company_public_web_collector_bundle_service_slo"])
        self.assertTrue(action["enabled"])
        self.assertEqual(action["collection_mode"], "collector_bundle")
        self.assertGreaterEqual(len(action["collector_documents"]), 4)
        self.assertTrue(expectations["require_no_company_public_web_guardrail_violation"])
        self.assertEqual(expectations["min_company_public_web_collector_record_count"], 4)
        self.assertEqual(expectations["max_company_public_web_failed_run_count"], 0)

    def test_settle_board_probe_waits_for_expected_candidate_total(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.candidate_totals = [300, 312]
                self.candidate_call_count = 0

            def get(self, path: str) -> dict:
                if path.split("?", 1)[0].endswith("/dashboard"):
                    total = self.candidate_totals[
                        min(self.candidate_call_count, len(self.candidate_totals) - 1)
                    ]
                    return {
                        "asset_population": {
                            "available": True,
                            "candidate_count": total,
                        }
                    }
                if "/candidates?" in path:
                    total = self.candidate_totals[
                        min(self.candidate_call_count, len(self.candidate_totals) - 1)
                    ]
                    self.candidate_call_count += 1
                    return {
                        "result_mode": "asset_population",
                        "total_candidates": total,
                        "returned_count": 24,
                    }
                raise AssertionError(f"unexpected path {path}")

        dashboard, candidate_page, timings = _settle_board_probe(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            results_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 312,
                }
            },
            poll_seconds=0.01,
        )

        self.assertEqual(candidate_page["total_candidates"], 312)
        self.assertEqual(dashboard["asset_population"]["candidate_count"], 312)
        self.assertEqual(timings["attempt_count"], 2.0)

    def test_settle_board_probe_waits_for_layering_when_required(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.dashboard_statuses = ["running", "completed"]
                self.dashboard_call_count = 0
                self.candidate_call_count = 0

            def get(self, path: str) -> dict:
                if path.split("?", 1)[0].endswith("/dashboard"):
                    status = self.dashboard_statuses[
                        min(self.dashboard_call_count, len(self.dashboard_statuses) - 1)
                    ]
                    self.dashboard_call_count += 1
                    return {
                        "asset_population": {
                            "available": True,
                            "candidate_count": 47,
                        },
                        "board_runtime_state": {"layering_status": status},
                    }
                if "/candidates?" in path:
                    self.candidate_call_count += 1
                    return {
                        "result_mode": "asset_population",
                        "total_candidates": 47,
                        "returned_count": 24,
                    }
                raise AssertionError(f"unexpected path {path}")

        dashboard, candidate_page, timings = _settle_board_probe(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            results_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 47,
                }
            },
            poll_seconds=0.01,
            wait_for_layering_visible=True,
        )

        self.assertEqual(dashboard["board_runtime_state"]["layering_status"], "completed")
        self.assertEqual(candidate_page["total_candidates"], 47)
        self.assertEqual(timings["attempt_count"], 2.0)
        self.assertEqual(timings["layering_ready"], 1.0)

    def test_settle_board_probe_uses_configured_layering_slo(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.dashboard_call_count = 0

            def get(self, path: str) -> dict:
                if path.split("?", 1)[0].endswith("/dashboard"):
                    self.dashboard_call_count += 1
                    return {
                        "asset_population": {
                            "available": True,
                            "candidate_count": 47,
                        },
                        "board_runtime_state": {"layering_status": "running"},
                    }
                if "/candidates?" in path:
                    return {
                        "result_mode": "asset_population",
                        "total_candidates": 47,
                        "returned_count": 24,
                    }
                raise AssertionError(f"unexpected path {path}")

        _dashboard, _candidate_page, timings = _settle_board_probe(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            results_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 47,
                }
            },
            poll_seconds=0.01,
            wait_for_layering_visible=True,
            layering_visible_timeout_seconds=0.2,
        )

        self.assertGreaterEqual(timings["wait_ms"], 190.0)
        self.assertGreaterEqual(timings["attempt_count"], 2.0)
        self.assertEqual(timings["layering_ready"], 0.0)
        self.assertEqual(timings["layering_visible_timed_out"], 1.0)
        self.assertEqual(timings["layering_visible_timeout_ms"], 200.0)
        self.assertLess(timings["board_ready_wait_ms"], timings["wait_ms"])
        self.assertLess(timings["board_nonempty_wait_ms"], timings["wait_ms"])

    def test_settle_board_probe_separates_board_readiness_from_layering_timeout(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path.split("?", 1)[0].endswith("/dashboard"):
                    return {
                        "asset_population": {
                            "available": True,
                            "candidate_count": 47,
                        },
                        "board_runtime_state": {"layering_status": "deferred"},
                    }
                if "/candidates?" in path:
                    return {
                        "result_mode": "asset_population",
                        "total_candidates": 47,
                        "returned_count": 24,
                    }
                raise AssertionError(f"unexpected path {path}")

        _dashboard, _candidate_page, timings = _settle_board_probe(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            results_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 47,
                }
            },
            poll_seconds=0.01,
            wait_for_layering_visible=True,
            layering_visible_timeout_seconds=0.2,
        )

        self.assertGreaterEqual(timings["wait_ms"], 190.0)
        self.assertLess(timings["board_ready_wait_ms"], 50.0)
        self.assertLess(timings["board_nonempty_wait_ms"], 50.0)
        self.assertLess(timings["board_expected_total_wait_ms"], 50.0)
        self.assertEqual(timings["layering_visible_wait_ms"], 0.0)
        self.assertEqual(timings["layering_visible_timed_out"], 1.0)

    def test_drive_provider_webhook_can_send_watcher_duplicate_concurrently(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {
                        "agent_workers": [
                            {
                                "worker_id": 7,
                                "status": "running",
                                "metadata": {"recovery_kind": "harvest_profile_batch"},
                                "checkpoint": {
                                    "run_id": "run-7",
                                    "dataset_id": "dataset-7",
                                    "scripted_remote_ready_epoch_ms": 0,
                                },
                            }
                        ]
                    }
                raise AssertionError(f"unexpected path {path}")

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                if path != "/api/providers/apify/webhook":
                    raise AssertionError(f"unexpected path {path}")
                self.posts.append(dict(payload))
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "recovery_count": 0,
                    "recovery_dispatch_count": 0,
                    "shared_recovery_signal_count": 1,
                }

        client = FakeClient()

        events, accepted_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            event_sequence=3,
            include_watcher_duplicate=True,
        )

        self.assertEqual(len(events), 2)
        self.assertEqual(
            {event["source"] for event in events},
            {"provider_webhook", "local_provider_event_watcher"},
        )
        self.assertEqual(len(accepted_workers), 1)
        self.assertEqual(len(client.posts), 2)
        self.assertEqual({post["source"] for post in client.posts}, {"provider_webhook", "local_provider_event_watcher"})

    def test_drive_provider_webhook_can_start_from_watcher_then_send_provider_late_duplicate(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {
                        "agent_workers": [
                            {
                                "worker_id": 11,
                                "status": "running",
                                "metadata": {"recovery_kind": "harvest_profile_batch"},
                                "checkpoint": {
                                    "run_id": "run-11",
                                    "dataset_id": "dataset-11",
                                    "scripted_remote_ready_epoch_ms": 0,
                                },
                            }
                        ]
                    }
                raise AssertionError(f"unexpected path {path}")

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                if path != "/api/providers/apify/webhook":
                    raise AssertionError(f"unexpected path {path}")
                self.posts.append(dict(payload))
                if payload["source"] == "local_provider_event_watcher":
                    return {
                        "status": "accepted",
                        "mode": "shared_recovery_signal",
                        "recovery_count": 0,
                        "recovery_dispatch_count": 0,
                        "shared_recovery_signal_count": 1,
                    }
                return {
                    "status": "accepted",
                    "reason": "matching_remote_provider_workers_not_recoverable",
                    "recovery_count": 0,
                    "recovery_dispatch_count": 0,
                    "shared_recovery_signal_count": 0,
                }

        client = FakeClient()

        events, accepted_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            event_sequence=5,
            primary_source="local_provider_event_watcher",
        )
        late_events = _drive_smoke_remote_provider_late_watcher_duplicates(
            client,  # type: ignore[arg-type]
            accepted_workers=accepted_workers,
            start_sequence=6,
            duplicate_source="provider_webhook",
        )

        self.assertEqual([event["source"] for event in events], ["local_provider_event_watcher"])
        self.assertEqual(events[0]["recovery_count"], 0)
        self.assertEqual(events[0]["recovery_dispatch_count"], 0)
        self.assertEqual(events[0]["shared_recovery_signal_count"], 1)
        self.assertEqual(len(accepted_workers), 1)
        self.assertEqual([event["source"] for event in late_events], ["provider_webhook"])
        self.assertEqual(late_events[0]["reason"], "matching_remote_provider_workers_not_recoverable")
        self.assertEqual(late_events[0]["recovery_count"], 0)
        self.assertEqual([post["source"] for post in client.posts], ["local_provider_event_watcher", "provider_webhook"])

    def test_smoke_provider_webhook_payload_uses_remote_ready_time_as_actor_finished_at(self) -> None:
        payload = _smoke_remote_provider_webhook_payload(
            {
                "worker_id": 7,
                "checkpoint": {
                    "run_id": "run-7",
                    "dataset_id": "dataset-7",
                    "scripted_remote_ready_epoch_ms": 1_777_766_410_000,
                },
            },
            event_sequence=3,
        )

        self.assertEqual(payload["eventData"]["actorRunId"], "run-7")
        self.assertEqual(payload["eventData"]["defaultDatasetId"], "dataset-7")
        self.assertEqual(payload["eventData"]["finishedAt"], "2026-05-03T00:00:10+00:00")

    def test_drive_provider_webhook_posts_ready_workers_concurrently(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {
                        "agent_workers": [
                            {
                                "worker_id": worker_id,
                                "status": "running",
                                "metadata": {"recovery_kind": "harvest_profile_batch"},
                                "checkpoint": {
                                    "run_id": f"run-{worker_id}",
                                    "dataset_id": f"dataset-{worker_id}",
                                    "scripted_remote_ready_epoch_ms": 1,
                                },
                            }
                            for worker_id in range(1, 5)
                        ]
                    }
                raise AssertionError(f"unexpected path {path}")

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                if path != "/api/providers/apify/webhook":
                    raise AssertionError(f"unexpected path {path}")
                time.sleep(0.05)
                self.posts.append(dict(payload))
                return {"status": "accepted", "mode": "async_recovery"}

        started_at = time.perf_counter()
        events, accepted_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            event_sequence=10,
        )
        elapsed = time.perf_counter() - started_at

        self.assertLess(elapsed, 0.15)
        self.assertEqual(len(events), 4)
        self.assertEqual(len(accepted_workers), 4)
        self.assertEqual({event["event_sequence"] for event in events}, {10, 11, 12, 13})

    def test_drive_provider_webhook_skips_already_accepted_remote_runs(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.posts: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {
                        "agent_workers": [
                            {
                                "worker_id": 7,
                                "status": "running",
                                "metadata": {"recovery_kind": "harvest_profile_batch"},
                                "checkpoint": {
                                    "run_id": "run-7",
                                    "dataset_id": "dataset-7",
                                    "scripted_remote_ready_epoch_ms": 1,
                                },
                            }
                        ]
                    }
                raise AssertionError(f"unexpected path {path}")

            def post(self, path: str, payload: dict, headers: dict | None = None) -> dict:
                if path != "/api/providers/apify/webhook":
                    raise AssertionError(f"unexpected path {path}")
                self.posts.append(dict(payload))
                return {"status": "accepted", "mode": "job_scoped_recovery"}

        client = FakeClient()
        accepted_keys: set[tuple[str, str]] = set()
        first_events, first_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            event_sequence=1,
            already_accepted_remote_keys=accepted_keys,
        )
        for _worker in first_workers:
            accepted_keys.add(("run-7", "dataset-7"))
        second_events, second_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            event_sequence=2,
            already_accepted_remote_keys=accepted_keys,
        )

        self.assertEqual(len(first_events), 1)
        self.assertEqual(len(first_workers), 1)
        self.assertEqual(second_events, [])
        self.assertEqual(second_workers, [])
        self.assertEqual(len(client.posts), 1)

    def test_scripted_smoke_matrices_enable_service_recovery_hard_gates(self) -> None:
        config_dir = Path(__file__).resolve().parents[1] / "configs" / "scripted"
        matrix_paths = sorted(config_dir.glob("*smoke_matrix.json"))
        self.assertGreaterEqual(len(matrix_paths), 1)
        missing: list[str] = []
        for matrix_path in matrix_paths:
            payload = json.loads(matrix_path.read_text(encoding="utf-8"))
            for case in list(payload.get("cases") or []):
                coverage_tags = {
                    str(item or "").strip()
                    for item in list(case.get("coverage_tags") or [])
                    if str(item or "").strip()
                }
                expectations = dict(case.get("expectations") or {})
                if not bool(expectations.get("require_no_service_recovery_violation")):
                    missing.append(f"{matrix_path.name}:{case.get('case')}:service")
                if not bool(expectations.get("require_no_progress_contract_violation")):
                    missing.append(f"{matrix_path.name}:{case.get('case')}:progress")
                if (
                    "provider_handoff_slo" in coverage_tags
                    and float(expectations.get("max_remote_provider_event_lag_ms") or 0.0) <= 0.0
                ):
                    missing.append(f"{matrix_path.name}:{case.get('case')}:remote_event_lag")
                if float(expectations.get("max_final_results_to_board_nonempty_ms") or 0.0) <= 0.0:
                    missing.append(f"{matrix_path.name}:{case.get('case')}:final_results_board_nonempty_slo")
                if bool(expectations.get("require_layering_visible_after_results_within_slo")):
                    if float(expectations.get("max_final_results_to_layering_visible_ms") or 0.0) <= 0.0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:final_results_layering_visible_slo")
                if float(expectations.get("max_job_to_board_nonempty_ms") or 0.0) <= 0.0:
                    missing.append(f"{matrix_path.name}:{case.get('case')}:job_board_nonempty_slo")
                handoff_gate = (
                    "provider_handoff_slo" in coverage_tags
                    or int(expectations.get("min_agent_worker_count") or 0) > 1
                    or int(expectations.get("min_remote_actor_worker_count") or 0) > 1
                )
                if handoff_gate and float(expectations.get("max_global_next_worker_start_gap_ms") or 0.0) <= 0.0:
                    missing.append(f"{matrix_path.name}:{case.get('case')}:worker_handoff_slo")
                if bool(expectations.get("require_post_preview_finalization_observed")):
                    if float(expectations.get("max_stage_1_preview_to_final_results_ms") or 0.0) <= 0.0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:preview_finalization_slo")
                if "out_of_order_profile_completion" in coverage_tags:
                    if int(expectations.get("min_out_of_order_profile_completion_count") or 0) <= 0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:out_of_order_profile_completion")
                if "partial_delta_board_row_streaming" in coverage_tags:
                    if not bool(expectations.get("require_partial_board_visible_before_final_results")):
                        missing.append(f"{matrix_path.name}:{case.get('case')}:partial_board_before_final")
                    if float(expectations.get("max_job_to_board_visible_partial_ms") or 0.0) <= 0.0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:partial_board_visible_slo")
                    if int(expectations.get("min_board_visible_patch_count") or 0) <= 0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:partial_board_patch_count")
                    if int(expectations.get("min_delta_profile_board_visible_count") or 0) <= 0:
                        missing.append(f"{matrix_path.name}:{case.get('case')}:partial_board_delta_visible_count")

        self.assertEqual(missing, [])

    def test_service_gate_coverage_manifest_matches_local_smoke_matrices(self) -> None:
        config_dir = Path(__file__).resolve().parents[1] / "configs" / "scripted"
        report = validate_service_gate_coverage(config_dir)

        self.assertEqual(report["metadata_errors"] + report["coverage_errors"], [])
        self.assertEqual(report["status"], "ok")
        self.assertIn("openai_agent_scoped_delta_profile_tail", report["covered_tags"])
        self.assertIn("openai_infra_stage1_lane_skew_zero_current_former_delta", report["required_now_tags"])
        self.assertIn("openai_infra_stage1_lane_skew_zero_current_former_delta", report["covered_tags"])
        self.assertIn("openai_health_whisper_zero_current_overlay", report["required_now_tags"])
        self.assertIn("openai_health_whisper_zero_current_overlay", report["covered_tags"])
        self.assertIn("meta_agent_full_reuse_execution_timeline_browser", report["required_now_tags"])
        self.assertIn("meta_agent_full_reuse_execution_timeline_browser", report["covered_tags"])
        self.assertIn("google_gemini_provider_quality_scoped_live", report["required_now_tags"])
        self.assertIn("google_gemini_provider_quality_scoped_live", report["covered_tags"])
        self.assertIn("partial_delta_board_row_streaming", report["required_now_tags"])
        self.assertIn("partial_delta_board_row_streaming", report["covered_tags"])
        self.assertIn("canonical_public_reader_source_contract", report["required_now_tags"])
        self.assertIn("canonical_public_reader_source_contract", report["covered_tags"])
        self.assertEqual(report["planned_gaps"], [])

    def test_service_gate_coverage_manifest_passes_before_ecs_sync_when_all_required_tags_are_covered(self) -> None:
        config_dir = Path(__file__).resolve().parents[1] / "configs" / "scripted"
        report = validate_service_gate_coverage(config_dir, require_before_ecs_sync=True)

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["missing_required_now_tags"], [])
        self.assertEqual(report["missing_required_before_ecs_sync_tags"], [])

    def test_service_gate_coverage_rejects_unknown_manifest_or_case_expectations(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "service_gate_coverage_manifest.json").write_text(
                json.dumps(
                    {
                        "coverage_tags": [
                            {
                                "tag": "bad_tag",
                                "status": "required_now",
                                "required_true_expectations": ["require_unknown_gate"],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (root / "bad_smoke_matrix.json").write_text(
                json.dumps(
                    {
                        "cases": [
                            {
                                "case": "bad_case",
                                "coverage_tags": ["bad_tag"],
                                "payload": {"raw_user_request": "x"},
                                "expectations": {"max_unknown_latency_ms": 1},
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            report = validate_service_gate_coverage(root)

        self.assertEqual(report["status"], "failed")
        self.assertIn("manifest:bad_tag:unknown_expectation:require_unknown_gate", report["metadata_errors"])
        self.assertIn("bad_smoke_matrix.json:bad_case:unknown_expectation:max_unknown_latency_ms", report["coverage_errors"])

    def test_smoke_provider_webhook_driver_allows_active_remote_worker_lease(self) -> None:
        worker = {
            "worker_id": 901,
            "status": "queued",
            "lease_owner": "scripted-active-worker",
            "lease_expires_at": "2099-01-01T00:00:00+00:00",
            "checkpoint": {
                "stage": "waiting_remote_harvest",
                "run_id": "run-active-lease",
                "dataset_id": "dataset-active-lease",
            },
            "metadata": {"recovery_kind": "harvest_profile_batch"},
        }

        self.assertTrue(_worker_can_receive_smoke_provider_webhook(worker))

    def test_smoke_provider_webhook_driver_skips_terminal_marker_duplicate_by_default(self) -> None:
        worker = {
            "worker_id": 901,
            "status": "queued",
            "lease_owner": "scripted-active-worker",
            "lease_expires_at": "2099-01-01T00:00:00+00:00",
            "checkpoint": {
                "stage": "waiting_remote_harvest",
                "run_id": "run-active-lease",
                "dataset_id": "dataset-active-lease",
                "remote_provider_terminal_event": {
                    "run_id": "run-active-lease",
                    "dataset_id": "dataset-active-lease",
                    "is_terminal": True,
                },
                "remote_provider_terminal_event_seen_at": "2026-05-11T14:48:17+00:00",
            },
            "metadata": {"recovery_kind": "harvest_profile_batch"},
        }

        self.assertFalse(_worker_can_receive_smoke_provider_webhook(worker))

    def test_smoke_provider_webhook_driver_keeps_in_flight_worker_for_late_duplicate(self) -> None:
        class _Client:
            def get(self, path: str) -> dict[str, object]:
                self.path = path
                return {
                    "agent_workers": [
                        {
                            "worker_id": 902,
                            "status": "queued",
                            "lease_owner": "scripted-active-worker",
                            "lease_expires_at": "2099-01-01T00:00:00+00:00",
                            "checkpoint": {
                                "stage": "waiting_remote_harvest",
                                "run_id": "run-active-lease",
                                "dataset_id": "dataset-active-lease",
                            },
                            "metadata": {"recovery_kind": "harvest_profile_batch"},
                        }
                    ]
                }

            def post(self, path: str, payload: dict[str, object], headers=None) -> dict[str, object]:
                self.posted = {"path": path, "payload": payload, "headers": headers}
                return {
                    "status": "accepted",
                    "reason": "remote_provider_event_recovery_already_in_flight",
                    "recovery_count": 0,
                }

        client = _Client()
        events, accepted_workers = _drive_smoke_remote_provider_webhook_recovery_once(
            client,
            job_id="job-active-lease",
            event_sequence=1,
        )

        self.assertEqual(client.posted["path"], "/api/providers/apify/webhook")
        self.assertEqual(len(accepted_workers), 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "accepted")
        self.assertEqual(events[0]["reason"], "remote_provider_event_recovery_already_in_flight")

    def test_smoke_recovery_triggers_for_running_acquiring_remote_tail(self) -> None:
        snapshot = {
            "status": "running",
            "stage": "acquiring",
            "progress": {
                "runtime_health": {
                    "classification": "workers_running",
                },
                "counters": {
                    "queued_worker_count": 2,
                    "waiting_remote_harvest_count": 2,
                    "active_worker_count": 2,
                },
            },
        }

        self.assertTrue(_should_auto_run_worker_recovery(snapshot))

    def test_smoke_recovery_does_not_trigger_for_running_without_remote_tail(self) -> None:
        snapshot = {
            "status": "running",
            "stage": "acquiring",
            "progress": {
                "runtime_health": {
                    "classification": "workers_running",
                },
                "counters": {
                    "active_worker_count": 2,
                },
            },
        }

        self.assertFalse(_should_auto_run_worker_recovery(snapshot))

    def test_smoke_provider_event_driver_can_run_without_progress_worker_counters(self) -> None:
        snapshot = {
            "status": "running",
            "stage": "acquiring",
            "progress": {
                "runtime_health": {
                    "classification": "healthy",
                },
                "counters": {
                    "active_worker_count": 0,
                },
            },
        }

        self.assertFalse(_should_auto_run_worker_recovery(snapshot))
        self.assertTrue(_should_drive_smoke_remote_provider_events(snapshot))

    def test_long_tail_expectations_disable_early_results_ready_by_default(self) -> None:
        self.assertFalse(_case_allows_early_results_ready({"min_profile_url_total_count": 200}))
        self.assertFalse(_case_allows_early_results_ready({"require_terminal_job": True}))
        self.assertTrue(_case_allows_early_results_ready({}))

    def test_evaluate_smoke_expectations_requires_provider_tail_metrics(self) -> None:
        record = {
            "final": {"raw_job_status": "running"},
            "provider_case_report": {
                "behavior_guardrails": {
                    "duplicate_provider_dispatch": {"violation_detected": False},
                    "disabled_stage_violations": {"unexpected_public_web_stage": False},
                },
                "workflow_benchmark": {
                    "profile_url_total_count": 4,
                    "fetched_profile_count": 2,
                    "board_total_candidates": 4,
                },
                "event_level_efficiency": {
                    "violation_detected": False,
                    "max_remote_actor_worker_count": 2,
                },
                "service_metrics": {
                    "worker_timeline": {"worker_count": 2},
                },
            },
        }
        failures = _evaluate_smoke_expectations(
            record=record,
            expectations={
                "require_terminal_job": True,
                "min_profile_url_total_count": 200,
                "min_fetched_profile_count": 150,
                "min_board_total_candidates": 150,
                "min_agent_worker_count": 6,
                "min_provider_invocations_by_logical_name": {
                    "harvest_profile_search": 4,
                    "harvest_profile_scraper_batch": 6,
                },
            },
            provider_invocations=[
                {"logical_name": "harvest_profile_search"},
                {"logical_name": "harvest_profile_search"},
            ],
        )

        self.assertTrue(any("require_terminal_job" in failure for failure in failures))
        self.assertTrue(any("profile_url_total_count" in failure for failure in failures))
        self.assertTrue(any("provider_invocations.harvest_profile_scraper_batch" in failure for failure in failures))

    def test_evaluate_smoke_expectations_checks_company_public_web_collector_source_metrics(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "company_public_web": {
                            "report_available": True,
                            "collector_source_count": 1,
                            "collector_document_fetch_count": 1,
                            "collector_document_fetch_failure_count": 1,
                            "collector_fetch_duration_ms_max": 2500.0,
                        }
                    }
                },
            },
            expectations={
                "min_company_public_web_collector_source_count": 2,
                "min_company_public_web_collector_document_fetch_count": 2,
                "max_company_public_web_collector_document_fetch_failure_count": 0,
                "max_company_public_web_collector_fetch_duration_ms": 1000,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("company_public_web.collector_source_count" in failure for failure in failures))
        self.assertTrue(any("company_public_web.collector_document_fetch_count" in failure for failure in failures))
        self.assertTrue(
            any("company_public_web.collector_document_fetch_failure_count" in failure for failure in failures)
        )
        self.assertTrue(any("company_public_web.collector_fetch_duration_ms_max" in failure for failure in failures))

    def test_evaluate_smoke_expectations_rejects_unknown_expectation_keys(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={"final": {"raw_job_status": "completed"}},
            expectations={"require_unknown_service_gate": True},
            provider_invocations=[],
        )

        self.assertEqual(failures, ["expectations:unknown_expectation:require_unknown_service_gate"])

    def test_evaluate_smoke_expectations_requires_out_of_order_profile_completion_coverage(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "worker_timeline": {
                            "out_of_order_completion": {
                                "profile_batch_inversion_count": 0,
                            }
                        },
                    }
                },
            },
            expectations={"min_out_of_order_profile_completion_count": 1},
            provider_invocations=[],
        )

        self.assertTrue(any("out_of_order_profile_completion_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_rejects_tiny_batch_and_slot_underuse_metrics(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "profile_batch_envelopes": {
                            "envelope_count": 0,
                            "unexplained_tiny_batch_count": 1,
                            "provider_slot_underuse_with_backlog_count": 1,
                            "tiny_batch_coalesced_count": 0,
                        },
                    }
                },
            },
            expectations={
                "min_profile_batch_envelope_count": 1,
                "max_profile_unexplained_tiny_batch_count": 0,
                "max_provider_slot_underuse_with_backlog_count": 0,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("profile_batch_envelope_count" in failure for failure in failures))
        self.assertTrue(any("profile_unexplained_tiny_batch_count" in failure for failure in failures))
        self.assertTrue(any("provider_slot_underuse_with_backlog_count" in failure for failure in failures))

        missing_report_failures = _evaluate_smoke_expectations(
            record={"final": {"raw_job_status": "completed"}, "provider_case_report": {}},
            expectations={"max_profile_unexplained_tiny_batch_count": 0},
            provider_invocations=[],
        )

        self.assertEqual(
            missing_report_failures,
            ["profile_unexplained_tiny_batch_count: event-level efficiency report unavailable"],
        )

    def test_evaluate_smoke_expectations_checks_profile_batch_size_window(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "profile_batch_envelopes": {
                            "envelope_count": 8,
                            "batch_size": {"max": 50, "values": [50, 50, 50]},
                        },
                    }
                },
            },
            expectations={
                "min_profile_batch_size_max": 100,
                "max_profile_batch_size_max": 250,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("profile_batch_size_max: expected >= 100" in failure for failure in failures))
        self.assertFalse(any("profile_batch_size_max: expected <= 250" in failure for failure in failures))

        oversize_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "profile_batch_envelopes": {
                            "envelope_count": 1,
                            "batch_size": {"max": 800, "values": [800]},
                        },
                    }
                },
            },
            expectations={"max_profile_batch_size_max": 250},
            provider_invocations=[],
        )

        self.assertTrue(any("profile_batch_size_max: expected <= 250" in failure for failure in oversize_failures))

    def test_evaluate_smoke_expectations_uses_provider_worker_batch_size_window(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "profile_batch_envelopes": {
                            "envelope_count": 5,
                            "batch_size": {"max": 2384, "values": [2384, 529, 619, 619, 617]},
                            "provider_worker_batch_size": {
                                "max": 619,
                                "values": [529, 619, 619, 617],
                            },
                        },
                    }
                },
            },
            expectations={
                "min_profile_batch_size_max": 600,
                "max_profile_batch_size_max": 900,
            },
            provider_invocations=[],
        )

        self.assertFalse(failures)

    def test_evaluate_smoke_expectations_rejects_profile_scheduler_contract_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": True,
                            "small_normal_batch_without_reason_count": 1,
                            "retry_wave_isolation_violation_count": 1,
                        },
                    }
                },
            },
            expectations={"require_no_profile_scheduler_contract_violation": True},
            provider_invocations=[],
        )

        self.assertTrue(any("profile scheduler contract violation" in failure for failure in failures))
        self.assertTrue(any("small_normal_batch_without_reason_count=1" in failure for failure in failures))
        self.assertTrue(any("retry_wave_isolation_violation_count=1" in failure for failure in failures))

        missing_report_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"event_level_efficiency": {"report_available": True}},
            },
            expectations={"require_no_profile_scheduler_contract_violation": True},
            provider_invocations=[],
        )

        self.assertEqual(missing_report_failures, ["profile scheduler contract: report missing"])

    def test_evaluate_smoke_expectations_checks_latest_stage1_counts(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "latest_linkedin_stage_1_progress": {
                        "current_search_returned_count": 0,
                        "former_search_returned_count": 10,
                        "all_search_returned_count": 0,
                        "deduped_candidate_count": 10,
                        "deduped_profile_url_count": 10,
                        "profile_fetch_required_count": 10,
                            "profile_fetched_count": 10,
                        }
                    }
                },
            },
            expectations={
                "expect_latest_stage1_current_search_returned_count": 0,
                "expect_latest_stage1_former_search_returned_count": 10,
                "expect_latest_stage1_all_search_returned_count": 0,
                "expect_latest_stage1_profile_fetch_required_count": 77,
            },
            provider_invocations=[],
        )

        self.assertFalse(any("current_search_returned_count" in failure for failure in failures))
        self.assertTrue(any("latest_stage1.profile_fetch_required_count" in failure for failure in failures))

        missing_failures = _evaluate_smoke_expectations(
            record={"final": {"raw_job_status": "completed"}, "provider_case_report": {}},
            expectations={"expect_latest_stage1_current_search_returned_count": 0},
            provider_invocations=[],
        )

        self.assertEqual(
            missing_failures,
            ["latest_stage1.current_search_returned_count: latest LinkedIn Stage 1 progress missing"],
        )

    def test_evaluate_smoke_expectations_checks_latest_result_view_lifecycle_overlay(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "latest_result_view_lifecycle": {
                            "baseline_snapshot_id": "baseline-openai",
                            "served_snapshot_id": "current-openai",
                            "baseline_candidate_count": 300,
                            "served_candidate_count": 302,
                            "delta_profile_materialized_count": 2,
                            "delta_profile_board_visible_count": 2,
                        }
                    }
                },
            },
            expectations={
                "require_latest_lifecycle_served_snapshot_not_baseline": True,
                "expect_latest_lifecycle_baseline_candidate_count": 300,
                "min_latest_lifecycle_baseline_candidate_count": 300,
                "min_latest_lifecycle_served_candidate_count": 301,
                "min_latest_lifecycle_delta_profile_materialized_count": 2,
                "min_latest_lifecycle_delta_profile_board_visible_count": 3,
            },
            provider_invocations=[],
        )

        self.assertFalse(any("served snapshot must differ" in failure for failure in failures))
        self.assertFalse(any("baseline_candidate_count" in failure for failure in failures))
        self.assertFalse(any("served_candidate_count" in failure for failure in failures))
        self.assertFalse(any("delta_profile_materialized_count" in failure for failure in failures))
        self.assertTrue(any("delta_profile_board_visible_count" in failure for failure in failures))

        no_baseline_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "latest_result_view_lifecycle": {
                            "baseline_candidate_count": 300,
                        }
                    }
                },
            },
            expectations={"expect_latest_lifecycle_baseline_candidate_count": 0},
            provider_invocations=[],
        )

        self.assertEqual(
            no_baseline_failures,
            ["latest_lifecycle.baseline_candidate_count: expected 0, actual=300"],
        )

        baseline_only_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "latest_result_view_lifecycle": {
                            "baseline_snapshot_id": "baseline-openai",
                            "served_snapshot_id": "baseline-openai",
                        }
                    }
                },
            },
            expectations={"require_latest_lifecycle_served_snapshot_not_baseline": True},
            provider_invocations=[],
        )

        self.assertEqual(
            baseline_only_failures,
            [
                "latest_lifecycle.served_snapshot_id: served snapshot must differ from baseline "
                "(served=baseline-openai, baseline=baseline-openai)"
            ],
        )

    def test_build_progress_observability_report_rejects_mixed_stage1_lane_denominator(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "linkedin_stage_1_progress": {
                        "current_search_returned_count": 0,
                        "former_search_returned_count": 10,
                        "all_search_returned_count": 0,
                        "deduped_candidate_count": 77,
                        "deduped_profile_url_count": 77,
                        "profile_fetch_required_count": 77,
                        "profile_fetched_count": 10,
                    },
                }
            ]
        )

        self.assertTrue(report["contract_violation_detected"])
        violation_counts = report["contract_violation_counts"]
        self.assertEqual(violation_counts["stage1_deduped_exceeds_lane_returned_population"], 1)
        self.assertEqual(violation_counts["stage1_deduped_profile_urls_exceed_lane_returned_population"], 1)

    def test_build_progress_observability_report_rejects_card_text_ahead_of_card_readiness(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "publication_status": "complete",
                        "expected_candidate_count": 145,
                        "served_candidate_count": 145,
                        "published_candidate_count": 145,
                        "display_ready_candidate_count": 80,
                        "profile_fetch_required_count": 140,
                        "profile_fetched_count": 140,
                        "delta_profile_required_count": 0,
                        "card_materialization_status_text": "卡片详情已合入看板 140/140",
                        "delta_profile_denominator_promoted": True,
                    },
                }
            ]
        )

        self.assertTrue(report["contract_violation_detected"])
        self.assertEqual(
            report["contract_violation_counts"]["board_runtime_card_text_exceeds_card_readiness"],
            1,
        )

    def test_build_progress_observability_report_rejects_legacy_counters_lifting_exact_card_text(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "publication_status": "complete",
                        "expected_candidate_count": 140,
                        "served_candidate_count": 140,
                        "published_candidate_count": 140,
                        "display_ready_candidate_count": 115,
                        "profile_detail_candidate_count": 140,
                        "explicit_profile_capture_candidate_count": 140,
                        "profile_fetch_required_count": 140,
                        "profile_fetched_count": 140,
                        "delta_profile_required_count": 140,
                        "delta_profile_materialized_count": 140,
                        "delta_profile_board_visible_count": 140,
                        "card_materialization_status_text": "卡片详情已合入看板 140/140",
                        "delta_profile_denominator_promoted": True,
                    },
                }
            ]
        )

        self.assertTrue(report["contract_violation_detected"])
        self.assertEqual(
            report["contract_violation_counts"]["board_runtime_card_text_exceeds_card_readiness"],
            1,
        )
        violation = report["contract_violations"][0]
        self.assertEqual(violation["card_ready_count"], 115)
        self.assertEqual(violation["card_required_count"], 140)

    def test_build_progress_observability_report_rejects_card_denominator_above_canonical_population(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "publication_status": "complete",
                        "expected_candidate_count": 140,
                        "served_candidate_count": 140,
                        "published_candidate_count": 140,
                        "display_ready_candidate_count": 115,
                        "profile_fetch_required_count": 160,
                        "delta_profile_required_count": 160,
                        "card_materialization_status_text": "卡片详情已合入看板 115/160",
                        "delta_profile_denominator_promoted": True,
                    },
                }
            ]
        )

        self.assertTrue(report["contract_violation_detected"])
        violation = report["contract_violations"][0]
        self.assertEqual(violation["text_ready_count"], 115)
        self.assertEqual(violation["text_required_count"], 160)
        self.assertEqual(violation["card_required_count"], 140)

    def test_build_progress_observability_report_rejects_invalid_card_fraction_scope_and_bounds(self) -> None:
        invalid_status_texts = (
            "卡片详情已合入看板 115/100",
            "卡片详情已合入看板 90/90",
            "卡片详情已合入看板 115/99",
        )
        for status_text in invalid_status_texts:
            with self.subTest(status_text=status_text):
                report = _build_progress_observability_report(
                    [
                        {
                            "tick": 0,
                            "board_runtime_state": {
                                "publication_status": "complete",
                                "expected_candidate_count": 140,
                                "served_candidate_count": 140,
                                "published_candidate_count": 140,
                                "display_ready_candidate_count": 115,
                                "profile_fetch_required_count": 100,
                                "delta_profile_required_count": 100,
                                "card_materialization_status_text": status_text,
                                "delta_profile_denominator_promoted": True,
                            },
                        }
                    ]
                )

                self.assertTrue(report["contract_violation_detected"])
                self.assertEqual(
                    report["contract_violation_counts"]["board_runtime_card_text_exceeds_card_readiness"],
                    1,
                )

    def test_build_progress_observability_report_accepts_smaller_delta_scoped_card_text(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "publication_status": "complete",
                        "expected_candidate_count": 597,
                        "served_candidate_count": 597,
                        "published_candidate_count": 597,
                        "display_ready_candidate_count": 397,
                        "baseline_candidate_count": 300,
                        "profile_fetch_required_count": 297,
                        "delta_profile_required_count": 297,
                        "delta_profile_materialized_count": 297,
                        "delta_profile_board_visible_count": 297,
                        "card_materialization_status_text": "卡片详情已合入看板 97/297",
                        "delta_profile_denominator_promoted": True,
                    },
                }
            ]
        )

        self.assertFalse(report["contract_violation_detected"])

    def test_build_progress_observability_report_accepts_full_roster_card_text_without_delta_denominator(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "publication_status": "complete",
                        "expected_candidate_count": 297,
                        "served_candidate_count": 297,
                        "published_candidate_count": 297,
                        "display_ready_candidate_count": 297,
                        "profile_fetch_required_count": 0,
                        "delta_profile_required_count": 0,
                        "card_materialization_status_text": "卡片详情已合入看板 297/297",
                        "delta_profile_denominator_promoted": True,
                    },
                }
            ]
        )

        self.assertFalse(report["contract_violation_detected"])

    def test_post_terminal_worker_recovery_keeps_draining_when_workers_remain(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_payloads: list[dict] = []
                self.post_count = 0

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    if self.post_count < 2:
                        return {
                            "agent_workers": [
                                {
                                    "worker_id": 33,
                                    "status": "queued",
                                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                                    "output": {},
                                }
                            ]
                        }
                    return {"agent_workers": []}
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                self.assert_signal(path, payload)
                self.post_payloads.append(dict(payload))
                self.post_count += 1
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {"status": "signaled"},
                }

            @staticmethod
            def assert_signal(path: str, payload: dict) -> None:
                if path != "/api/workers/daemon/run-once" or payload != {}:
                    raise AssertionError(f"unexpected signal request {path}: {payload}")

        client = FakeClient()
        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
        )

        self.assertEqual(len(recovery_runs), 2)
        self.assertTrue(recovery_state["settled"])
        self.assertEqual(recovery_state["remaining_recoverable_worker_count"], 0)
        self.assertEqual(client.post_payloads, [{}, {}])

    def test_post_terminal_worker_recovery_reports_unsettled_when_workers_remain(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_count = 0

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {
                        "agent_workers": [
                            {
                                "worker_id": 44,
                                "status": "completed",
                                "metadata": {"recovery_kind": "harvest_profile_batch"},
                                "output": {},
                            }
                        ]
                    }
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                if path != "/api/workers/daemon/run-once" or payload != {}:
                    raise AssertionError(f"unexpected signal request {path}: {payload}")
                self.post_count += 1
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {"status": "signaled"},
                }

        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
        )

        self.assertEqual(len(recovery_runs), 3)
        self.assertFalse(recovery_state["settled"])
        self.assertEqual(recovery_state["remaining_recoverable_worker_ids"], [44])
        self.assertEqual(recovery_state["no_progress_rounds"], 3)

    def test_post_terminal_worker_recovery_drains_snapshot_full_materialization(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_payloads: list[dict] = []
                self.post_count = 0

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {"agent_workers": []}
                if path.endswith("/materialization-items"):
                    if self.post_count <= 0:
                        return {
                            "job_materialization_items": [
                                {
                                    "item_id": "full-1",
                                    "item_kind": "snapshot_full_materialization",
                                    "status": "queued",
                                    "phase": "queued",
                                }
                            ]
                        }
                    return {"job_materialization_items": []}
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                if path != "/api/workers/daemon/run-once" or payload != {}:
                    raise AssertionError(f"unexpected signal request {path}: {payload}")
                self.post_payloads.append(dict(payload))
                self.post_count += 1
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {"status": "signaled"},
                }

        client = FakeClient()
        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
            drain_background_snapshot_full_materialization=True,
        )

        self.assertEqual(len(recovery_runs), 1)
        self.assertTrue(recovery_state["settled"])
        self.assertEqual(client.post_payloads, [{}])

    def test_post_terminal_worker_recovery_does_not_block_on_background_full_materialization_by_default(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_payloads: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {"agent_workers": []}
                if path.endswith("/materialization-items"):
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "full-1",
                                "item_kind": "snapshot_full_materialization",
                                "status": "queued",
                                "phase": "queued",
                            }
                        ]
                    }
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                self.post_payloads.append(dict(payload))
                return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        client = FakeClient()
        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
        )

        self.assertEqual(recovery_runs, [])
        self.assertTrue(recovery_state["settled"])
        self.assertEqual(recovery_state["remaining_materialization_item_count"], 0)
        self.assertEqual(recovery_state["background_snapshot_full_materialization_pending_count"], 1)
        self.assertEqual(client.post_payloads, [])

    def test_post_terminal_worker_recovery_drains_projection_facet_layering_when_requested(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_payloads: list[dict] = []
                self.post_count = 0

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {"agent_workers": []}
                if path.endswith("/materialization-items"):
                    if self.post_count <= 0:
                        return {
                            "job_materialization_items": [
                                {
                                    "item_id": "proj-1",
                                    "item_kind": "projection_facet_layering_build",
                                    "status": "queued",
                                    "phase": "queued",
                                }
                            ]
                        }
                    return {"job_materialization_items": []}
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                if path != "/api/workers/daemon/run-once" or payload != {}:
                    raise AssertionError(f"unexpected signal request {path}: {payload}")
                self.post_payloads.append(dict(payload))
                self.post_count += 1
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {"status": "signaled"},
                }

        client = FakeClient()
        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
            drain_projection_facet_layering=True,
        )

        self.assertEqual(len(recovery_runs), 1)
        self.assertTrue(recovery_state["settled"])
        self.assertTrue(recovery_state["projection_facet_layering_drain_requested"])
        self.assertEqual(client.post_payloads, [{}])

    def test_post_terminal_worker_recovery_does_not_block_on_projection_layering_by_default(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_payloads: list[dict] = []

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {"agent_workers": []}
                if path.endswith("/materialization-items"):
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "proj-1",
                                "item_kind": "projection_facet_layering_build",
                                "status": "queued",
                                "phase": "queued",
                            }
                        ]
                    }
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                self.post_payloads.append(dict(payload))
                return {"status": "completed", "daemon": {"claimed_count": 0, "executed_count": 0}}

        client = FakeClient()
        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            client,  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
        )

        self.assertEqual(recovery_runs, [])
        self.assertTrue(recovery_state["settled"])
        self.assertEqual(recovery_state["projection_facet_layering_pending_count"], 1)
        self.assertFalse(recovery_state["projection_facet_layering_drain_requested"])
        self.assertEqual(client.post_payloads, [])

    def test_post_terminal_worker_recovery_waits_for_running_handoff_materialization(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.post_count = 0

            def get(self, path: str) -> dict:
                if path.endswith("/workers"):
                    return {"agent_workers": []}
                if path.endswith("/materialization-items"):
                    if self.post_count < 2:
                        return {
                            "job_materialization_items": [
                                {
                                    "item_id": "board-1",
                                    "item_kind": "board_visible_delta_apply",
                                    "status": "running",
                                    "phase": "applying",
                                }
                            ]
                        }
                    return {"job_materialization_items": []}
                if path.endswith("/results"):
                    return {"job": {"status": "completed", "stage": "completed"}}
                return {"job_id": "job-1", "summary": {}}

            def post(self, path: str, payload: dict) -> dict:
                if path != "/api/workers/daemon/run-once" or payload != {}:
                    raise AssertionError(f"unexpected signal request {path}: {payload}")
                self.post_count += 1
                return {
                    "status": "accepted",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {"status": "signaled"},
                }

        _, _, recovery_runs, _, recovery_state = _settle_post_terminal_worker_recovery(
            FakeClient(),  # type: ignore[arg-type]
            job_id="job-1",
            poll_seconds=0.01,
            max_rounds=4,
        )

        self.assertEqual(len(recovery_runs), 2)
        self.assertTrue(recovery_state["settled"])
        self.assertEqual(recovery_state["no_progress_rounds"], 0)

    def test_post_terminal_materialization_wait_state_reports_retry_waiting_items(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path.endswith("/materialization-items"):
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "item-1",
                                "item_kind": "local_apply_closure",
                                "status": "failed_retryable",
                                "phase": "retry_wait",
                                "not_before_at": "2099-01-01 00:00:00",
                            }
                        ]
                    }
                return {"job_id": "job-1", "summary": {}}

        state = _post_terminal_materialization_wait_state(FakeClient(), job_id="job-1")  # type: ignore[arg-type]

        self.assertEqual(state["pending_count"], 1)
        self.assertEqual(state["retry_wait_count"], 1)
        self.assertGreater(state["seconds_until_next_ready"], 0.0)
        self.assertEqual(state["items"][0]["item_id"], "item-1")

    def test_post_terminal_materialization_wait_state_includes_background_full_compaction(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path.endswith("/materialization-items"):
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "full-1",
                                "item_kind": "snapshot_full_materialization",
                                "status": "queued",
                                "phase": "queued",
                            },
                            {
                                "item_id": "board-1",
                                "item_kind": "board_visible_delta_apply",
                                "status": "queued",
                                "phase": "queued",
                            },
                        ]
                    }
                return {"job_id": "job-1", "summary": {}}

        state = _post_terminal_materialization_wait_state(FakeClient(), job_id="job-1")  # type: ignore[arg-type]

        self.assertEqual(state["pending_count"], 2)
        self.assertEqual(state["handoff_pending_count"], 1)
        self.assertEqual(state["snapshot_full_materialization_pending_count"], 1)
        self.assertEqual(state["background_snapshot_full_materialization_pending_count"], 1)
        self.assertEqual([item["item_id"] for item in state["items"]], ["full-1", "board-1"])

    def test_post_terminal_materialization_wait_state_counts_applying_and_waiting_prerequisite(self) -> None:
        class FakeClient:
            def get(self, path: str) -> dict:
                if path.endswith("/materialization-items"):
                    return {
                        "job_materialization_items": [
                            {
                                "item_id": "full-applying",
                                "item_kind": "snapshot_full_materialization",
                                "status": "applying",
                                "phase": "applying",
                            },
                            {
                                "item_id": "board-waiting",
                                "item_kind": "board_visible_delta_apply",
                                "status": "waiting_prerequisite",
                                "phase": "waiting_prerequisite",
                            },
                        ]
                    }
                return {"job_id": "job-1", "summary": {}}

        state = _post_terminal_materialization_wait_state(FakeClient(), job_id="job-1")  # type: ignore[arg-type]

        self.assertEqual(state["pending_count"], 2)
        self.assertEqual(state["running_count"], 1)
        self.assertEqual(state["handoff_pending_count"], 1)
        self.assertEqual(state["snapshot_full_materialization_pending_count"], 1)
        self.assertEqual(state["background_snapshot_full_materialization_pending_count"], 1)
        self.assertEqual([item["item_id"] for item in state["items"]], ["full-applying", "board-waiting"])

    def test_post_terminal_recovery_syncs_late_background_snapshot_queue_from_service_metrics(self) -> None:
        recovery_state = {
            "settled": True,
            "remaining_recoverable_worker_count": 0,
            "background_snapshot_full_materialization_pending_count": 0,
            "background_snapshot_full_materialization_drain_requested": True,
            "round_count": 4,
            "max_rounds": 4,
        }

        synchronized = _synchronize_post_terminal_recovery_with_service_metrics(
            recovery_state,
            service_metrics={
                "snapshot_full_materialization_queue": {
                    "report_available": True,
                    "backlog_count": 1,
                    "running_count": 1,
                    "status_counts": {"running": 1},
                    "phase_counts": {"applying": 1},
                    "retry_backlog_present": False,
                    "stale_running_present": False,
                }
            },
            materialization_items=[
                {
                    "item_id": "full-running",
                    "item_kind": "snapshot_full_materialization",
                    "status": "running",
                    "phase": "applying",
                }
            ],
        )

        self.assertFalse(synchronized["settled"])
        self.assertTrue(synchronized["max_rounds_exhausted"])
        self.assertEqual(synchronized["background_snapshot_full_materialization_pending_count"], 1)
        self.assertEqual(synchronized["background_snapshot_full_materialization_count_source"], "service_metrics")
        self.assertEqual(synchronized["background_snapshot_full_materialization_items"][0]["item_id"], "full-running")

    def test_evaluate_smoke_expectations_requires_post_terminal_recovery_settled(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "post_terminal_recovery": {
                    "settled": False,
                    "remaining_recoverable_worker_count": 2,
                    "remaining_recoverable_worker_ids": [1, 2],
                },
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "event_level_efficiency": {"violation_detected": False},
                },
            },
            expectations={"require_post_terminal_recovery_settled": True},
            provider_invocations=[],
        )

        self.assertTrue(any("post_terminal_recovery_settled" in failure for failure in failures))

    def test_evaluate_smoke_expectations_requires_post_preview_finalization_observed(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "event_level_efficiency": {"violation_detected": False},
                    "post_preview_finalization": {
                        "report_available": True,
                        "materialize_completed_count": 0,
                        "preview_to_finalization_completed_ms": 0.0,
                    },
                },
            },
            expectations={
                "require_post_preview_finalization_observed": True,
                "min_materialize_completed_count": 2,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("post_preview_finalization_observed" in failure for failure in failures))
        self.assertTrue(any("materialize_completed_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_accepts_canonical_projection_completion_without_legacy_materialize(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "projection_cutover": {
                        "report_available": True,
                        "run_projection_link_present": True,
                        "projection_missing": False,
                        "legacy_public_reader_fallback_used": False,
                        "legacy_endpoint_normal_path_used": False,
                    },
                    "service_metrics": {
                        "report_available": True,
                        "board_visible_projection": {
                            "report_available": True,
                            "served_candidate_count": 140,
                            "expected_candidate_count": 140,
                            "delta_profile_required_count": 140,
                            "delta_profile_materialized_count": 140,
                            "delta_profile_board_visible_count": 140,
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                            "patch_sequence_values": [1, 2],
                            "patch_sequence_contiguous": True,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"count": 1, "max": 0}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"count": 1, "max": 20}},
                        },
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "normal_path_write_count": 0,
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "legacy_bridge_used_count": 0,
                            "legacy_bridge_used_present": False,
                        },
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "materialize_completed_count": 0,
                    },
                },
            },
            expectations={
                "require_post_preview_finalization_observed": True,
                "min_materialize_completed_count": 1,
                "max_profile_file_visible_to_board_patch_visible_ms": 30000,
            },
            provider_invocations=[],
        )

        self.assertFalse(
            [
                failure
                for failure in failures
                if "post_preview_finalization_observed" in failure
                or "materialize_completed_count" in failure
                or "profile_file_visible_to_board_patch_visible" in failure
            ]
        )

    def test_evaluate_smoke_expectations_accepts_same_second_post_preview_finalization(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "event_level_efficiency": {"violation_detected": False},
                    "post_preview_finalization": {
                        "report_available": True,
                        "materialize_completed_count": 3,
                        "finalization_completed_event_count": 1,
                        "finalization_completed_at": "2026-05-05T18:53:54+00:00",
                        "preview_to_finalization_completed_ms": 0.0,
                    },
                },
            },
            expectations={"require_post_preview_finalization_observed": True},
            provider_invocations=[],
        )

        self.assertFalse(
            [failure for failure in failures if "post_preview_finalization_observed" in failure]
        )

    def test_evaluate_smoke_expectations_honors_stage1_terminal_to_finalization_start_gate(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "post_preview_finalization": {
                        "report_available": True,
                        "stage1_terminal_to_first_finalization_start_ms": 45000.0,
                    }
                },
            },
            expectations={"max_stage1_terminal_to_finalization_start_ms": 30000},
            provider_invocations=[],
        )

        self.assertTrue(
            any(
                "post_preview_finalization.stage1_terminal_to_finalization_start_ms" in failure
                for failure in failures
            )
        )

    def test_stage1_terminal_to_finalization_start_uses_linkedin_stage1_boundary(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-13T13:07:30+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-13T13:06:02+00:00"},
            },
            job_events=[
                {
                    "created_at": "2026-05-13T13:07:41+00:00",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "started",
                        "reconcile_kind": "harvest_prefetch",
                    },
                }
            ],
            timings_ms={},
        )

        self.assertEqual(report["preview_to_first_finalization_start_ms"], 99000.0)
        self.assertEqual(report["stage1_terminal_to_first_finalization_start_ms"], 11000.0)
        self.assertEqual(report["finalization_start_gate_ms"], 11000.0)
        self.assertEqual(report["finalization_start_gate_source"], "linkedin_stage_1_completed_at")

    def test_finalization_start_gate_excludes_profile_provider_wait(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-14T01:52:01+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-14T01:52:07+00:00"},
            },
            job_events=[],
            timings_ms={},
            materialization_items=[
                {
                    "item_kind": "board_visible_delta_apply",
                    "status": "completed",
                    "created_at": "2026-05-14T01:55:43+00:00",
                    "completed_at": "2026-05-14T01:55:44+00:00",
                }
            ],
            agent_workers=[
                {
                    "worker_id": 501,
                    "status": "completed",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {"remote_completed_at": "2026-05-14T01:55:38+00:00"},
                }
            ],
        )

        self.assertEqual(report["stage1_terminal_to_first_finalization_start_ms"], 222000.0)
        self.assertEqual(report["profile_terminal_to_first_finalization_start_ms"], 5000.0)
        self.assertEqual(report["finalization_start_gate_ms"], 5000.0)
        self.assertEqual(report["finalization_start_gate_source"], "profile_terminal_at")
        self.assertEqual(report["profile_wait_excluded_from_finalization_gate_ms"], 217000.0)
        self.assertEqual(report["preview_to_finalization_completed_ms"], 217000.0)
        self.assertEqual(report["finalization_lag_evaluation_ms"], 5000.0)
        self.assertEqual(report["finalization_lag_evaluation_source"], "profile_terminal_at")
        self.assertTrue(report["raw_long_post_preview_finalization"])
        self.assertFalse(report["long_post_preview_finalization"])

        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"post_preview_finalization": report},
            },
            expectations={"max_stage1_terminal_to_finalization_start_ms": 30000},
            provider_invocations=[],
        )

        self.assertFalse(
            any("post_preview_finalization.stage1_terminal_to_finalization_start_ms" in failure for failure in failures)
        )

    def test_finalization_start_gate_recovers_stage_boundaries_from_job_events(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={},
            job_events=[
                {
                    "created_at": "2026-05-23T00:23:27+00:00",
                    "stage": "acquiring",
                    "status": "completed",
                    "detail": "LinkedIn Stage 1 acquisition completed.",
                    "payload": {
                        "status": "completed",
                        "candidate_doc_path": "/tmp/candidate_documents.json",
                    },
                },
                {
                    "created_at": "2026-05-23T00:23:29+00:00",
                    "stage": "acquiring",
                    "status": "completed",
                    "detail": "Stage 1 preview ready; continuing snapshot materialization.",
                    "payload": {
                        "analysis_stage": "stage_1_preview",
                        "preview_artifact_path": "/tmp/job.preview.json",
                    },
                },
            ],
            timings_ms={},
            workflow_commands=[
                {
                    "command_id": "cmd_local_apply",
                    "command_type": "linkedin.local_profile_delta.apply",
                    "status": "succeeded",
                    "owner": "profile_local_apply_owner",
                    "payload": {"item_id": "jlocal_apply"},
                    "result": {
                        "started_at": "2026-05-23T00:27:18+00:00",
                        "completed_at": "2026-05-23T00:27:18+00:00",
                    },
                }
            ],
            agent_workers=[
                {
                    "worker_id": 1,
                    "status": "completed",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {
                        "remote_provider_terminal_event": {
                            "remote_completed_at": "2026-05-23T00:27:01.698+00:00",
                        }
                    },
                }
            ],
        )

        self.assertEqual(report["linkedin_stage_1_completed_at"], "2026-05-23T00:23:27+00:00")
        self.assertEqual(report["stage_1_preview_completed_at"], "2026-05-23T00:23:29+00:00")
        self.assertEqual(report["finalization_start_gate_source"], "profile_terminal_at")
        self.assertEqual(report["finalization_start_gate_ms"], 16302.0)
        self.assertEqual(report["profile_wait_excluded_from_finalization_gate_ms"], 214698.0)
        self.assertEqual(report["finalization_lag_evaluation_ms"], 16302.0)

    def test_finalization_start_gate_prefers_typed_terminal_record_over_remote_worker_time(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-23T16:12:49+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-23T16:12:55+00:00"},
            },
            job_events=[],
            timings_ms={},
            workflow_commands=[
                {
                    "command_id": "cmd_terminal_record",
                    "command_type": "linkedin.profile_url_terminal.record",
                    "status": "succeeded",
                    "owner": "linkedin_profile_owner",
                    "updated_at": "2026-05-23T16:17:00+00:00",
                    "result": {
                        "recorded_count": 120,
                        "fetched_count": 120,
                        "completed_at": "2026-05-23T16:17:00+00:00",
                    },
                },
                {
                    "command_id": "cmd_local_apply",
                    "command_type": "linkedin.local_profile_delta.apply",
                    "status": "succeeded",
                    "owner": "profile_local_apply_owner",
                    "payload": {"item_id": "jlocal_apply"},
                    "result": {
                        "started_at": "2026-05-23T16:17:02+00:00",
                        "completed_at": "2026-05-23T16:17:02+00:00",
                    },
                },
            ],
            agent_workers=[
                {
                    "worker_id": 1,
                    "status": "completed",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {"remote_completed_at": "2026-05-23T16:16:29.729+00:00"},
                }
            ],
        )

        self.assertEqual(report["profile_terminal_at"], "2026-05-23T16:17:00+00:00")
        self.assertEqual(report["profile_terminal_source"], "profile_url_terminal_record_command_completed_at")
        self.assertEqual(report["profile_terminal_to_first_finalization_start_ms"], 2000.0)
        self.assertEqual(report["finalization_start_gate_ms"], 2000.0)
        self.assertEqual(
            report["finalization_start_gate_source"],
            "profile_url_terminal_record_command_completed_at",
        )
        self.assertFalse(report["long_post_preview_finalization"])

    def test_finalization_start_gate_excludes_profile_wait_when_finalization_started_early(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-15T10:32:21+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-15T10:32:28+00:00"},
            },
            job_events=[
                {
                    "created_at": "2026-05-15T10:36:11+00:00",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "started",
                        "reconcile_kind": "harvest_prefetch",
                    },
                }
            ],
            timings_ms={},
            agent_workers=[
                {
                    "worker_id": 1,
                    "status": "completed",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {"remote_completed_at": "2026-05-15T10:48:36+00:00"},
                }
            ],
        )

        self.assertEqual(report["stage1_terminal_to_first_finalization_start_ms"], 230000.0)
        self.assertEqual(report["profile_terminal_to_first_finalization_start_ms"], 0.0)
        self.assertEqual(report["finalization_start_gate_ms"], 0.0)
        self.assertEqual(report["finalization_start_gate_source"], "profile_terminal_at")
        self.assertEqual(report["profile_wait_excluded_from_finalization_gate_ms"], 975000.0)

        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"post_preview_finalization": report},
            },
            expectations={"max_stage1_terminal_to_finalization_start_ms": 30000},
            provider_invocations=[],
        )

        self.assertFalse(
            any("post_preview_finalization.stage1_terminal_to_finalization_start_ms" in failure for failure in failures)
        )

    def test_finalization_start_gate_reads_nested_remote_profile_terminal_event(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-14T06:31:50+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-14T06:31:55+00:00"},
            },
            job_events=[],
            timings_ms={},
            materialization_items=[
                {
                    "item_kind": "local_apply_closure",
                    "status": "completed",
                    "created_at": "2026-05-14T06:35:34+00:00",
                    "completed_at": "2026-05-14T06:35:45+00:00",
                }
            ],
            agent_workers=[
                {
                    "worker_id": 1,
                    "status": "completed",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {
                        "remote_provider_terminal_event": {
                            "remote_completed_at": "2026-05-14T06:35:24.767+00:00",
                        },
                    },
                    "updated_at": "2026-05-14T06:35:44+00:00",
                }
            ],
        )

        self.assertEqual(report["profile_terminal_at"], "2026-05-14T06:35:24.767000+00:00")
        self.assertEqual(report["finalization_start_gate_source"], "profile_terminal_at")
        self.assertEqual(report["finalization_start_gate_ms"], 9233.0)

    def test_post_preview_finalization_is_available_from_durable_items_without_preview_timestamp(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={},
            job_events=[],
            timings_ms={},
            materialization_items=[
                {
                    "item_kind": "board_visible_delta_apply",
                    "status": "completed",
                    "created_at": "2026-05-08T12:09:31+00:00",
                    "completed_at": "2026-05-08T12:09:32+00:00",
                }
            ],
        )

        self.assertTrue(report["report_available"])
        self.assertEqual(report["materialize_completed_count"], 1)
        self.assertEqual(report["finalization_completed_event_count"], 1)

    def test_post_preview_finalization_accepts_command_owned_snapshot_compaction(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-22T21:45:20+00:00"},
            },
            job_events=[],
            timings_ms={},
            workflow_commands=[
                {
                    "command_id": "cmd-snapshot-1",
                    "command_type": "snapshot.compaction.run",
                    "owner": "snapshot_materialization_owner",
                    "status": "succeeded",
                    "updated_at": "2026-05-22T21:52:21+00:00",
                    "payload": {"job_id": "job-google", "item_id": "snapshot-full-1"},
                    "result": {"status": "completed"},
                }
            ],
        )

        self.assertTrue(report["report_available"])
        self.assertEqual(report["materialize_completed_count"], 1)
        self.assertEqual(report["finalization_completed_event_count"], 1)
        self.assertEqual(report["first_finalization_started_at"], "2026-05-22T21:52:21+00:00")
        self.assertEqual(report["finalization_completed_at"], "2026-05-22T21:52:21+00:00")
        self.assertEqual(report["stage1_terminal_to_first_finalization_start_ms"], 421000.0)
        self.assertEqual(report["materialize_sync_scope_counts"], {"snapshot_full_materialization": 1})
        self.assertEqual(
            report["materialize_syncs"][0]["duration_semantics"],
            "workflow_command_terminal_timestamp_fallback",
        )

    def test_evaluate_smoke_expectations_requires_finalization_boundary_for_observed(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "event_level_efficiency": {"violation_detected": False},
                    "post_preview_finalization": {
                        "report_available": True,
                        "materialize_completed_count": 3,
                        "finalization_completed_event_count": 0,
                        "finalization_completed_at": "",
                        "preview_to_finalization_completed_ms": 0.0,
                    },
                },
            },
            expectations={"require_post_preview_finalization_observed": True},
            provider_invocations=[],
        )

        self.assertTrue(any("finalization_completed_event_count=0" in failure for failure in failures))

    def test_post_preview_finalization_uses_first_event_when_stage_preview_timestamp_is_late(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "stage_1_preview": {"completed_at": "2026-05-03T11:38:08+00:00"},
                "stage_2_final": {"completed_at": "2026-05-03T11:38:08+00:00"},
            },
            job_events=[
                {
                    "created_at": "2026-05-03T11:37:57+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_started",
                        "reconcile_kind": "harvest_prefetch",
                    },
                },
                {
                    "created_at": "2026-05-03T11:38:07+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_completed",
                        "reconcile_kind": "harvest_prefetch",
                    },
                },
            ],
            timings_ms={},
        )

        self.assertEqual(
            report["stage_1_preview_timestamp_source"],
            "first_finalization_event_when_stage_preview_timestamp_is_late",
        )
        self.assertEqual(report["preview_to_first_materialize_start_ms"], 0.0)
        self.assertEqual(report["preview_to_finalization_completed_ms"], 11000.0)

    def test_post_preview_finalization_counts_completed_durable_local_apply_items(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-03T12:00:01+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-03T12:00:00+00:00"},
                "stage_2_final": {"completed_at": "2026-05-03T12:00:30+00:00"},
            },
            job_events=[],
            timings_ms={},
            materialization_items=[
                {
                    "item_id": "closure-1",
                    "item_kind": "local_apply_closure",
                    "status": "completed",
                    "created_at": "2026-05-03T12:00:05+00:00",
                    "completed_at": "2026-05-03T12:00:10+00:00",
                }
            ],
        )

        self.assertEqual(report["materialize_completed_count"], 1)
        self.assertEqual(report["materialize_started_count"], 1)
        self.assertEqual(report["first_finalization_started_at"], "2026-05-03T12:00:05+00:00")
        self.assertEqual(report["preview_to_first_finalization_start_ms"], 5000.0)
        self.assertEqual(report["stage1_terminal_to_first_finalization_start_ms"], 4000.0)
        self.assertEqual(report["preview_to_last_materialize_completed_ms"], 10000.0)
        self.assertEqual(report["materialize_sync_duration_ms"]["max"], 5000.0)
        self.assertEqual(report["materialize_syncs"][0]["reconcile_kind"], "local_apply_closure")

    def test_post_preview_finalization_splits_candidate_source_and_profile_local_apply(self) -> None:
        report = _build_post_preview_finalization_report(
            stage_wall_clock={
                "linkedin_stage_1": {"completed_at": "2026-05-15T16:06:52+00:00"},
                "stage_1_preview": {"completed_at": "2026-05-15T16:06:52+00:00"},
                "stage_2_final": {"completed_at": "2026-05-15T16:09:53+00:00"},
            },
            job_events=[],
            timings_ms={},
            materialization_items=[
                {
                    "item_id": "roster-closure",
                    "item_kind": "local_apply_closure",
                    "status": "completed",
                    "source_worker_ids": [1],
                    "created_at": "2026-05-15T16:06:53+00:00",
                    "completed_at": "2026-05-15T16:09:04+00:00",
                    "metadata": {
                        "recovery_kind": "harvest_company_employees",
                        "worker_kind": "company_roster",
                        "inline_incremental_ingest": {
                            "sync_status": "deferred",
                            "sync_reason": "profile_prefetch_workers_still_inflight",
                        },
                    },
                },
                {
                    "item_id": "profile-closure",
                    "item_kind": "local_apply_closure",
                    "status": "completed",
                    "source_worker_ids": [14],
                    "created_at": "2026-05-15T16:09:45+00:00",
                    "completed_at": "2026-05-15T16:09:53+00:00",
                    "metadata": {"profile_url_count_for_budget": 50},
                },
            ],
            agent_workers=[
                {
                    "worker_id": 1,
                    "worker_key": "harvest_company_employees::lovable",
                    "metadata": {"recovery_kind": "harvest_company_employees"},
                },
                {
                    "worker_id": 14,
                    "worker_key": "harvest_profile_batch::lovable",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                },
            ],
        )

        self.assertEqual(report["materialize_sync_duration_ms"]["max"], 131000.0)
        self.assertEqual(report["candidate_source_closure_lifecycle_ms"]["max"], 131000.0)
        self.assertEqual(report["profile_batch_local_apply_duration_ms"]["max"], 8000.0)
        self.assertEqual(report["materialize_sync_scope_counts"]["candidate_source_closure_lifecycle"], 1)
        self.assertEqual(report["materialize_sync_scope_counts"]["profile_batch_local_apply"], 1)
        self.assertEqual(report["materialize_syncs"][0]["sync_scope"], "candidate_source_closure_lifecycle")
        self.assertTrue(report["materialize_syncs"][0]["duration_includes_deferred_wait"])
        self.assertEqual(report["materialize_syncs"][1]["sync_scope"], "profile_batch_local_apply")
        self.assertFalse(report["materialize_syncs"][1]["duration_includes_deferred_wait"])

    def test_evaluate_smoke_expectations_rejects_progress_contract_violations(self) -> None:
        progress_observability = {
            "sample_count": 2,
            "regression_detected": True,
            "counter_regressions": {"result_count": {"largest_drop": 290}},
            "stage1_regression_detected": True,
            "stage1_counter_regressions": {"current_search_returned_count": {"largest_drop": 83}},
            "result_view_lifecycle_regression_detected": True,
            "result_view_lifecycle_regressions": {"served_candidate_count": {"largest_drop": 1100}},
            "contract_violation_detected": True,
            "contract_violation_counts": {
                "raw_delta_only_result_view_served": 1,
                "stage1_profile_required_exceeds_deduped_population": 1,
            },
        }

        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {"violation_detected": False},
                    "progress_observability": progress_observability,
                },
            },
            expectations={"require_no_progress_contract_violation": True},
            provider_invocations=[],
        )

        self.assertTrue(any("Stage 1 counter regression" in failure for failure in failures))
        self.assertTrue(any("public counter regression" in failure for failure in failures))
        self.assertTrue(any("result_count" in failure for failure in failures))
        self.assertTrue(any("result-view lifecycle regression" in failure for failure in failures))
        self.assertTrue(any("raw_delta_only_result_view_served=1" in failure for failure in failures))

        clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "sample_count": 2,
                        "regression_detected": False,
                        "stage1_regression_detected": False,
                        "result_view_lifecycle_regression_detected": False,
                        "contract_violation_detected": False,
                    },
                },
            },
            expectations={"require_no_progress_contract_violation": True},
            provider_invocations=[],
        )

        self.assertFalse(clean_failures)

        already_promoted_clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "stage1_expected_candidate_count_violations": [],
                        "stage1_denominator_promotion_count": 0,
                        "stage1_denominator_promoted_observed": True,
                        "stage1_denominator_unpromoted_sample_count": 0,
                        "stage1_user_facing_denominator_violations": [],
                    },
                    "running_candidate_filter_probe": {
                        "sample_count": 1,
                        "display_ready_recall_bucket_observed": True,
                        "max_display_ready_count": 1,
                    },
                    "board": {"layering_status": "completed"},
                },
            },
            expectations={
                "require_stable_expected_candidate_count_during_stage1": True,
                "require_stable_user_facing_profile_card_denominator_during_stage1": True,
                "require_filter_returns_running_card_ready_recall_buckets": True,
                "require_layering_visible_after_results_within_slo": True,
            },
            provider_invocations=[],
        )

        self.assertFalse(already_promoted_clean_failures)

    def test_evaluate_smoke_expectations_rejects_workflow_causality_contract_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "workflow_causality_contract": {
                            "report_available": True,
                            "violation_detected": True,
                            "missing_envelope_count": 3,
                            "incomplete_envelope_count": 1,
                        }
                    }
                },
            },
            expectations={"require_workflow_causality_contract": True},
            provider_invocations=[],
        )

        self.assertTrue(any("workflow causality contract violation detected" in failure for failure in failures))
        self.assertTrue(any("missing_envelope_count=3" in failure for failure in failures))

        missing_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"service_metrics": {}},
            },
            expectations={"require_workflow_causality_contract": True},
            provider_invocations=[],
        )

        self.assertEqual(missing_failures, ["workflow causality contract: report missing"])

    def test_evaluate_smoke_expectations_requires_crm_public_web_queue_batch_command(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "target_candidate_public_web": {
                            "report_available": True,
                            "batch_count": 1,
                            "crm_storage_owner_batch_count": 1,
                            "legacy_storage_owner_batch_count": 0,
                            "execution_backend_counts": {"crm_public_web_v1": 1},
                            "execution_backend_bridge_count": 0,
                            "queue_batch_command_missing": True,
                            "queue_batch_command_count": 0,
                            "queue_batch_command_succeeded_count": 0,
                            "queue_batch_command_expected_owner_count": 0,
                            "queue_batch_command_invalid_owner_count": 0,
                            "queue_batch_command_incomplete_causality_count": 0,
                        }
                    }
                },
            },
            expectations={
                "require_crm_public_web_storage_owner": True,
                "require_crm_public_web_queue_batch_command": True,
                "require_public_web_execution_backend_report": True,
            },
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["target_candidate_public_web.queue_batch_command: crm.public_web.queue_batch command missing"],
        )

    def test_evaluate_smoke_expectations_rejects_post_profile_heuristic_slo_pairing(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "profile_file_visible_to_board_patch_visible": {
                                "heuristic_pairing_used": True,
                                "legacy_snapshot_pair_count": 2,
                            },
                        }
                    }
                },
            },
            expectations={"require_no_post_profile_heuristic_slo_pairing": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["post-profile SLO heuristic pairing used (legacy_snapshot_pair_count=2)"],
        )

    def test_evaluate_smoke_expectations_accepts_deferred_post_result_layering_with_complete_board(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board": {
                        "ready_nonempty": True,
                        "candidate_page_total_candidates": 597,
                        "layering_status": "deferred",
                        "board_runtime_state": {
                            "phase": "post_result_layering",
                            "publication_status": "complete",
                            "expected_candidate_count": 597,
                            "served_candidate_count": 597,
                            "display_ready_candidate_count": 597,
                        },
                    },
                },
            },
            expectations={"require_layering_visible_after_results_within_slo": True},
            provider_invocations=[],
        )

        self.assertFalse(failures)

        current_serving_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board": {
                        "ready_nonempty": True,
                        "candidate_page_total_candidates": 597,
                        "layering_status": "scheduled",
                        "board_runtime_state": {
                            "phase": "current_snapshot_serving",
                            "publication_status": "complete",
                            "expected_candidate_count": 597,
                            "served_candidate_count": 597,
                            "published_candidate_count": 597,
                        },
                    },
                },
            },
            expectations={"require_layering_visible_after_results_within_slo": True},
            provider_invocations=[],
        )

        self.assertFalse(current_serving_failures)

    def test_evaluate_smoke_expectations_rejects_board_runtime_endpoint_drift(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board_runtime_state_parity": {
                        "report_available": True,
                        "consistent": False,
                        "missing_sources": ["board_patches"],
                        "mismatched_fields": [
                            "display_ready_candidate_count",
                            "card_materialization_status_text",
                        ],
                    }
                },
            },
            expectations={"require_board_runtime_state_cross_endpoint_parity": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            [
                "board runtime parity: /progress, /dashboard, /candidates, /board-patches drift "
                "(missing=board_patches; mismatched=display_ready_candidate_count,card_materialization_status_text)"
            ],
        )

        clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board_runtime_state_parity": {
                        "report_available": True,
                        "consistent": True,
                        "missing_sources": [],
                        "mismatched_fields": [],
                    }
                },
            },
            expectations={"require_board_runtime_state_cross_endpoint_parity": True},
            provider_invocations=[],
        )

        self.assertEqual(clean_failures, [])

    def test_evaluate_smoke_expectations_rejects_terminal_partial_board_runtime(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board": {
                        "board_runtime_state": {
                            "phase": "partial_serving",
                            "publication_status": "partial",
                            "expected_candidate_count": 7384,
                            "served_candidate_count": 7352,
                            "published_candidate_count": 7321,
                            "row_hydration_target_count": 7321,
                        },
                    },
                },
            },
            expectations={"require_terminal_board_runtime_complete": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            [
                "terminal board runtime complete: expected complete current snapshot publication "
                "(phase=partial_serving, publication_status=partial, expected=7384, "
                "served=7352, published=7321, row_hydration=7321)"
            ],
        )

        clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "board": {
                        "board_runtime_state": {
                            "phase": "current_snapshot_serving",
                            "publication_status": "complete",
                            "expected_candidate_count": 7352,
                            "served_candidate_count": 7352,
                            "published_candidate_count": 7352,
                            "row_hydration_target_count": 7352,
                        },
                    },
                },
            },
            expectations={"require_terminal_board_runtime_complete": True},
            provider_invocations=[],
        )

        self.assertEqual(clean_failures, [])

    def test_provider_case_report_compares_board_runtime_state_across_public_endpoints(self) -> None:
        board_runtime_state = {
            "schema_version": 1,
            "job_id": "job-1",
            "result_mode": "asset_population",
            "phase": "current_snapshot_serving",
            "publication_status": "complete",
            "expected_candidate_count": 597,
            "served_candidate_count": 597,
            "published_candidate_count": 597,
            "display_ready_candidate_count": 597,
            "row_hydration_target_count": 597,
            "baseline_candidate_count": 300,
            "delta_profile_required_count": 297,
            "delta_profile_fetched_count": 297,
            "delta_profile_materialized_count": 297,
            "delta_profile_board_visible_count": 297,
            "delta_profile_denominator_promoted": True,
            "row_publication_sequence": 10,
            "row_publication_tier": "current_snapshot_serving",
            "row_publication_watermark": "snapshot|10|597|completed",
            "facet_summary_status": "complete",
            "facet_summary_scope": "global_full_population",
            "facet_summary_candidate_count": 597,
            "layering_status": "completed",
            "sync_status_text": "597/597",
            "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 297/297",
            "card_materialization_status_text": "卡片详情已合入看板 297/297",
            "filter_contract": {
                "source": "backend_board_runtime_state",
                "facet_count_scope": "global_full_population",
                "row_filter_scope": "backend_filtered_served_population",
                "backend_filtered_paging_supported": True,
            },
        }
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={"job": {"summary": {}}},
            dashboard_payload={"board_runtime_state": dict(board_runtime_state)},
            candidate_page_payload={"board_runtime_state": dict(board_runtime_state)},
            timings_ms={},
            progress_payload={"board_runtime_state": dict(board_runtime_state)},
            board_patches_payload={"board_runtime_state": dict(board_runtime_state)},
        )

        parity = report["board_runtime_state_parity"]
        self.assertTrue(parity["report_available"])
        self.assertTrue(parity["consistent"])
        self.assertEqual(parity["missing_sources"], [])
        self.assertEqual(parity["mismatches"], [])

        stale_candidates_state = {**board_runtime_state, "display_ready_candidate_count": 548}
        drift_report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={"job": {"summary": {}}},
            dashboard_payload={"board_runtime_state": dict(board_runtime_state)},
            candidate_page_payload={"board_runtime_state": stale_candidates_state},
            timings_ms={},
            progress_payload={"board_runtime_state": dict(board_runtime_state)},
            board_patches_payload={"board_runtime_state": dict(board_runtime_state)},
        )
        drift_parity = drift_report["board_runtime_state_parity"]
        self.assertFalse(drift_parity["consistent"])
        self.assertEqual(drift_parity["mismatched_fields"], ["display_ready_candidate_count"])
        self.assertEqual(drift_parity["mismatches"][0]["source"], "candidates")
        self.assertEqual(drift_parity["mismatches"][0]["expected"], 597)
        self.assertEqual(drift_parity["mismatches"][0]["actual"], 548)

        exact_projection_filter_state = {
            **board_runtime_state,
            "filter_contract": {
                **dict(board_runtime_state["filter_contract"]),
                "facet_count_scope": "exact_projection",
                "facet_summary_source": "serving_projection_public_facet_counts",
                "facet_summary_projection_id": "proj_filter_contract",
            },
        }
        filter_drift_report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={"job": {"summary": {}}},
            dashboard_payload={"board_runtime_state": dict(board_runtime_state)},
            candidate_page_payload={"board_runtime_state": dict(board_runtime_state)},
            timings_ms={},
            progress_payload={"board_runtime_state": dict(exact_projection_filter_state)},
            board_patches_payload={"board_runtime_state": dict(exact_projection_filter_state)},
        )
        filter_drift_parity = filter_drift_report["board_runtime_state_parity"]
        self.assertFalse(filter_drift_parity["consistent"])
        self.assertEqual(filter_drift_parity["mismatched_fields"], ["filter_contract"])
        self.assertEqual(filter_drift_parity["mismatches"][0]["source"], "progress")
        self.assertEqual(
            filter_drift_parity["mismatches"][0]["actual"]["facet_count_scope"],
            "exact_projection",
        )
        self.assertEqual(
            filter_drift_parity["mismatches"][0]["expected"]["facet_count_scope"],
            "global_full_population",
        )

    def test_projection_smoke_board_state_keeps_summary_scope_separate_from_count_scope(self) -> None:
        state = _projection_board_runtime_state_for_smoke(
            job_id="job-projection-scope",
            projection_payload={
                "projection": {
                    "projection_id": "proj_scope",
                    "source_run_id": "job-projection-scope",
                    "visible_member_count": 3,
                    "counts": {"candidate_count": 3, "count_scope": "exact_projection"},
                    "readiness": {"row_count": 3, "card_ready_count": 3},
                    "updated_at": "2026-05-23T00:00:00+08:00",
                }
            },
            candidate_page_payload={
                "total_candidates": 3,
                "facet_summary": {
                    "status": "complete",
                    "count_scope": "exact_projection",
                    "candidate_count": 3,
                    "layers": [{"id": "layer_0", "count": 3}],
                },
                "filter_contract": {
                    "source": "serving_projection_members",
                    "facet_count_scope": "exact_projection",
                    "facet_summary_source": "serving_projection_public_facet_counts",
                    "facet_summary_projection_id": "proj_scope",
                    "row_filter_scope": "projection_membership",
                    "backend_filtered_paging_supported": True,
                    "fallback_used": False,
                },
            },
            progress_payload={},
        )

        self.assertEqual(state["facet_summary_scope"], "global_full_population")
        self.assertEqual(state["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(
            state["filter_contract"]["facet_summary_source"],
            "serving_projection_public_facet_counts",
        )

    def test_build_progress_observability_report_tracks_stage1_denominator_stability(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "expected_candidate_count": 300,
                        "delta_profile_denominator_promoted": False,
                        "card_materialization_status_text": "卡片详情已合入看板 0 张",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 0 张",
                    },
                },
                {
                    "tick": 1,
                    "board_runtime_state": {
                        "expected_candidate_count": 425,
                        "delta_profile_denominator_promoted": False,
                        "card_materialization_status_text": "卡片详情已合入看板 0/125",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 10/125",
                    },
                },
                {
                    "tick": 2,
                    "board_runtime_state": {
                        "expected_candidate_count": 597,
                        "delta_profile_denominator_promoted": True,
                        "card_materialization_status_text": "卡片详情已合入看板 297/297",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 297/297",
                    },
                },
            ]
        )

        self.assertTrue(report["stage1_expected_candidate_count_violation_detected"])
        self.assertEqual(report["stage1_denominator_promotion_count"], 1)
        self.assertTrue(report["stage1_denominator_promoted_observed"])
        self.assertEqual(report["stage1_denominator_unpromoted_sample_count"], 2)
        self.assertTrue(report["stage1_user_facing_denominator_violation_detected"])

        clean_report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "expected_candidate_count": 300,
                        "delta_profile_denominator_promoted": False,
                        "card_materialization_status_text": "卡片详情已合入看板 0 张",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 0 张",
                    },
                },
                {
                    "tick": 1,
                    "board_runtime_state": {
                        "expected_candidate_count": 300,
                        "delta_profile_denominator_promoted": False,
                        "card_materialization_status_text": "卡片详情已合入看板 120 张",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 125 张",
                    },
                },
                {
                    "tick": 2,
                    "board_runtime_state": {
                        "expected_candidate_count": 597,
                        "delta_profile_denominator_promoted": True,
                        "card_materialization_status_text": "卡片详情已合入看板 297/297",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 297/297",
                    },
                },
            ]
        )

        self.assertFalse(clean_report["stage1_expected_candidate_count_violation_detected"])
        self.assertEqual(clean_report["stage1_denominator_promotion_count"], 1)
        self.assertTrue(clean_report["stage1_denominator_promoted_observed"])
        self.assertEqual(clean_report["stage1_denominator_unpromoted_sample_count"], 2)
        self.assertFalse(clean_report["stage1_user_facing_denominator_violation_detected"])

        already_promoted_report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "board_runtime_state": {
                        "expected_candidate_count": 597,
                        "delta_profile_denominator_promoted": True,
                        "card_materialization_status_text": "卡片详情已合入看板 297/297",
                        "profile_fetch_status_text": "新增 LinkedIn Profile 已取回 297/297",
                    },
                },
            ]
        )
        self.assertEqual(already_promoted_report["stage1_denominator_promotion_count"], 0)
        self.assertTrue(already_promoted_report["stage1_denominator_promoted_observed"])
        self.assertEqual(already_promoted_report["stage1_denominator_unpromoted_sample_count"], 0)

    def test_candidate_filter_probe_detects_running_display_ready_recall_bucket_rows(self) -> None:
        sample = _build_candidate_page_filter_probe_sample(
            payload={
                "returned_count": 1,
                "filtered_candidate_count": 1,
                "applied_filter": {"recall_buckets": ["Agent"]},
                "candidates": [
                    {
                        "candidate_id": "openai-agent-1",
                        "has_profile_detail": True,
                        "matched_keywords": ["Agent"],
                    }
                ],
            },
            tick=3,
            status="running",
            stage="acquiring",
            recall_bucket="Agent",
        )

        self.assertEqual(sample["display_ready_count"], 1)
        self.assertEqual(sample["display_ready_recall_bucket_count"], 1)

    def test_candidate_filter_probe_accepts_canonical_keyword_recall_bucket_echo(self) -> None:
        sample = _build_candidate_page_filter_probe_sample(
            payload={
                "returned_count": 1,
                "filtered_candidate_count": 1,
                "applied_filter": {"recall_buckets": ["keyword:agent"]},
                "candidates": [
                    {
                        "candidate_id": "openai-agent-1",
                        "has_profile_detail": True,
                    }
                ],
            },
            tick=3,
            status="running",
            stage="acquiring",
            recall_bucket="Agent",
        )

        self.assertEqual(sample["display_ready_count"], 1)
        self.assertEqual(sample["display_ready_recall_bucket_count"], 1)

    def test_evaluate_smoke_expectations_requires_progress_contract_observability(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={"final": {"raw_job_status": "completed"}, "provider_case_report": {}},
            expectations={"require_no_progress_contract_violation": True},
            provider_invocations=[],
        )

        self.assertEqual(failures, ["progress contract: progress_observability missing"])

    def test_evaluate_smoke_expectations_rejects_manual_streaming_symptom_gates(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "stage1_expected_candidate_count_violations": [{"tick": 1}],
                        "stage1_denominator_promotion_count": 2,
                        "stage1_denominator_promoted_observed": True,
                        "stage1_denominator_unpromoted_sample_count": 1,
                        "stage1_user_facing_denominator_violations": [
                            {"field": "card_materialization_status_text", "text": "0/125"}
                        ],
                    },
                    "running_candidate_filter_probe": {
                        "sample_count": 2,
                        "display_ready_recall_bucket_observed": False,
                        "max_display_ready_count": 0,
                    },
                    "board": {"layering_status": "running"},
                },
            },
            expectations={
                "require_stable_expected_candidate_count_during_stage1": True,
                "require_stable_user_facing_profile_card_denominator_during_stage1": True,
                "require_filter_returns_running_card_ready_recall_buckets": True,
                "require_layering_visible_after_results_within_slo": True,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("expected_candidate_count changed before Stage 1 promotion" in item for item in failures))
        self.assertTrue(any("user-facing status text exposed /N before promotion" in item for item in failures))
        self.assertFalse(any("recall_buckets=Agent" in item for item in failures))
        self.assertTrue(any("layering not visible" in item for item in failures))

        recall_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "running_candidate_filter_probe": {
                        "sample_count": 2,
                        "display_ready_recall_bucket_observed": False,
                        "max_display_ready_count": 3,
                    },
                },
            },
            expectations={"require_filter_returns_running_card_ready_recall_buckets": True},
            provider_invocations=[],
        )
        self.assertTrue(any("recall_buckets=Agent" in item for item in recall_failures))

        clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "progress_observability": {
                        "stage1_expected_candidate_count_violations": [],
                        "stage1_denominator_promotion_count": 1,
                        "stage1_denominator_promoted_observed": True,
                        "stage1_denominator_unpromoted_sample_count": 1,
                        "stage1_user_facing_denominator_violations": [],
                    },
                    "running_candidate_filter_probe": {
                        "sample_count": 2,
                        "display_ready_recall_bucket_observed": True,
                        "max_display_ready_count": 1,
                    },
                    "board": {"layering_status": "completed"},
                },
            },
            expectations={
                "require_stable_expected_candidate_count_during_stage1": True,
                "require_stable_user_facing_profile_card_denominator_during_stage1": True,
                "require_filter_returns_running_card_ready_recall_buckets": True,
                "require_layering_visible_after_results_within_slo": True,
            },
            provider_invocations=[],
        )

        self.assertFalse(clean_failures)

    def test_evaluate_smoke_expectations_rejects_full_reuse_progress_pollution(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "explain": {
                    "effective_acquisition_mode": "full_local_asset_reuse",
                    "planner_mode": "reuse_snapshot_only",
                    "dispatch_strategy": "reuse_snapshot",
                },
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "execution": {
                        "effective_acquisition_mode": "full_local_asset_reuse",
                        "planner_mode": "reuse_snapshot_only",
                        "dispatch_strategy": "reuse_snapshot",
                    },
                    "progress_observability": {
                        "max_payload_bytes": 125_000,
                        "payload_bytes": {"max": 125_000.0},
                        "latest_execution_phase_contract": {
                            "active_phase_label": "LinkedIn Stage 1",
                            "active_phase_detail": "需补取 LinkedIn Profile 48",
                            "profile_work_pending": True,
                        },
                        "latest_result_view_lifecycle": {
                            "delta_profile_progress_applicable": True,
                        },
                    },
                },
            },
            expectations={
                "max_progress_payload_bytes": 50_000,
                "require_no_active_stage1_for_full_local_reuse": True,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("progress_payload_bytes" in failure for failure in failures))
        self.assertTrue(any("profile_work_pending=true" in failure for failure in failures))
        self.assertTrue(any("active Stage 1 wording" in failure for failure in failures))
        self.assertTrue(any("delta_profile_progress_applicable=true" in failure for failure in failures))

    def test_evaluate_smoke_expectations_rejects_service_recovery_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "local_apply_backlog": {
                            "stale_applied_not_ingested_present": True,
                            "closure_retry_backlog_present": True,
                            "closure_stale_running_present": True,
                        },
                        "snapshot_full_materialization_queue": {
                            "retry_backlog_present": True,
                            "stale_running_present": True,
                        },
                        "search_seed_discovery_queue": {
                            "item_without_worker_owner_present": True,
                            "discovery_worker_without_item_present": True,
                            "discovery_worker_without_local_apply_present": True,
                            "retry_backlog_present": True,
                            "stale_provider_owned_present": True,
                            "exhausted_without_provider_retry_present": True,
                        },
                        "provider_search_retry_queue": {
                            "retry_backlog_present": True,
                            "stale_running_present": True,
                        },
                    },
                },
            },
            expectations={"require_no_service_recovery_violation": True},
            provider_invocations=[],
        )

        self.assertTrue(any("local_apply_backlog stale" in failure for failure in failures))
        self.assertTrue(any("local_apply_closure retry backlog" in failure for failure in failures))
        self.assertTrue(any("snapshot_full_materialization retry backlog" in failure for failure in failures))
        self.assertTrue(any("search_seed_discovery owner missing" in failure for failure in failures))
        self.assertTrue(any("search_seed_discovery worker without discovery item" in failure for failure in failures))
        self.assertTrue(
            any("search_seed_discovery worker without local_apply_closure item" in failure for failure in failures)
        )
        self.assertTrue(any("search_seed_discovery ready retry backlog" in failure for failure in failures))
        self.assertTrue(any("search_seed_discovery stale provider-owned item" in failure for failure in failures))
        self.assertTrue(any("search_seed_discovery exhausted without provider_retry report" in failure for failure in failures))
        self.assertTrue(any("provider_search_retry ready retry backlog" in failure for failure in failures))

    def test_evaluate_smoke_expectations_rejects_board_visible_projection_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "board_visible_projection": {
                            "report_available": True,
                            "projection_missing_for_visible_count": True,
                            "patch_log_missing_for_visible_count": True,
                            "patch_log_replay_lag": True,
                            "patch_log_required_missing": True,
                            "materialization_lag_violation": True,
                            "metadata_replay_dependency": True,
                            "patch_sequence_contiguous": False,
                            "patch_log_count": 0,
                            "delta_profile_board_visible_count": 1,
                            "fetched_to_board_visible_lag_count": 4,
                            "materialized_to_board_visible_lag_count": 3,
                            "patch_log_lag_count": 2,
                        },
                    },
                },
            },
            expectations={
                "require_no_board_visible_projection_violation": True,
                "min_board_visible_patch_count": 1,
                "min_delta_profile_board_visible_count": 2,
                "max_fetched_to_board_visible_lag_count": 3,
                "max_materialized_to_board_visible_lag_count": 2,
                "max_board_visible_patch_lag_count": 1,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("visible count without serving projection" in failure for failure in failures))
        self.assertTrue(any("visible count without patch log" in failure for failure in failures))
        self.assertTrue(any("patch log replay lag" in failure for failure in failures))
        self.assertTrue(any("non-contiguous patch sequence" in failure for failure in failures))
        self.assertTrue(any("board_visible_projection.patch_log_count" in failure for failure in failures))
        self.assertTrue(any("board_visible_projection.delta_profile_board_visible_count" in failure for failure in failures))
        self.assertTrue(any("board_visible_projection.fetched_to_board_visible_lag_count" in failure for failure in failures))
        self.assertTrue(any("board_visible_projection.materialized_to_board_visible_lag_count" in failure for failure in failures))
        self.assertTrue(any("board_visible_projection.patch_log_lag_count" in failure for failure in failures))

        missing_report_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"service_metrics": {"report_available": True}},
            },
            expectations={
                "require_no_board_visible_projection_violation": True,
                "require_board_visible_projection_report": True,
            },
            provider_invocations=[],
        )

        self.assertEqual(missing_report_failures, ["board-visible projection: report missing"])

    def test_evaluate_smoke_expectations_rejects_post_profile_slo_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "post_profile_completion": {
                            "report_available": True,
                            "url_terminal_state_recording": {
                                "terminal_queue_state_leak_count": 1,
                            },
                            "profile_file_visible_to_board_patch_visible": {
                                "elapsed_ms": {"count": 1, "max": 45000.0},
                            },
                            "all_profiles_fetched_to_all_cards_visible": {
                                "elapsed_ms": {"count": 1, "max": 47000.0},
                            },
                            "event_level_callback": {
                                "elapsed_ms": {"count": 1, "max": 31000.0},
                            },
                        },
                    },
                },
            },
            expectations={
                "max_post_profile_url_terminal_state_leak_count": 0,
                "max_profile_file_visible_to_board_patch_visible_ms": 30000,
                "max_all_profiles_fetched_to_all_cards_visible_ms": 30000,
                "max_event_level_materialization_callback_elapsed_ms": 30000,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("terminal_queue_state_leak_count" in failure for failure in failures))
        self.assertTrue(any("profile_file_visible_to_board_patch_visible" in failure for failure in failures))
        self.assertTrue(any("all_profiles_fetched_to_all_cards_visible" in failure for failure in failures))
        self.assertTrue(any("event_level_callback" in failure for failure in failures))

    def test_evaluate_smoke_expectations_rejects_recovery_phase_violations(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": True,
                            "failed_phase_present": True,
                            "slow_phase_present": True,
                            "unexpected_enabled_phase_present": True,
                            "recovery_tick_budget_exhausted_count": 2,
                            "elapsed_ms": {"count": 1, "max": 45000.0},
                            "total_elapsed_ms": {"count": 1, "max": 47000.0},
                        },
                    },
                },
            },
            expectations={
                "require_no_recovery_phase_violation": True,
                "max_recovery_phase_elapsed_ms": 30000,
                "max_recovery_total_elapsed_ms": 30000,
                "max_recovery_tick_budget_exhausted_count": 0,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("missing phase metrics" in failure for failure in failures))
        self.assertTrue(any("failed phase" in failure for failure in failures))
        self.assertTrue(any("slow phase" in failure for failure in failures))
        self.assertTrue(any("unexpected enabled phase" in failure for failure in failures))
        self.assertTrue(any("service_metrics.recovery_phase_metrics.elapsed_ms" in failure for failure in failures))
        self.assertTrue(any("service_metrics.recovery_phase_metrics.total_elapsed_ms" in failure for failure in failures))
        self.assertTrue(any("budget_yield_attention_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_allows_clean_recovery_budget_yield(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "recovery_tick_budget_exhausted_count": 1,
                            "cooperative_budget_yield_count": 1,
                            "cooperative_budget_yield_present": True,
                            "budget_yield_attention_required": False,
                            "elapsed_ms": {"count": 1, "max": 15000.0},
                            "total_elapsed_ms": {"count": 1, "max": 31000.0},
                        },
                    },
                },
            },
            expectations={"max_recovery_tick_budget_exhausted_count": 0},
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_rejects_remote_provider_event_driver_failure(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "remote_provider_event_driver": {
                    "events": [
                        {
                            "status": "provider_webhook_failed",
                            "reason": "parallel_provider_event_thread_timeout",
                        }
                    ]
                },
                "provider_case_report": {
                    "service_metrics": {"report_available": True},
                    "event_level_efficiency": {"report_available": True},
                },
            },
            expectations={"drive_remote_provider_duplicate_events": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["remote_provider_event_driver failed: provider_webhook_failed"],
        )

    def test_evaluate_smoke_expectations_requires_watcher_first_provider_late_driver_shape(self) -> None:
        valid_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "remote_provider_event_driver": {
                    "events": [
                        {
                            "status": "accepted",
                            "source": "local_provider_event_watcher",
                            "recovery_count": 0,
                            "recovery_dispatch_count": 0,
                            "shared_recovery_signal_count": 1,
                        },
                        {
                            "status": "accepted",
                            "source": "provider_webhook",
                            "reason": "matching_remote_provider_workers_not_recoverable",
                            "recovery_count": 0,
                        },
                    ]
                },
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "remote_provider_events": {
                            "report_available": True,
                            "source_counts": {
                                "local_provider_event_watcher": 1,
                                "provider_webhook": 1,
                            },
                            "status_counts": {
                                "received": 1,
                                "received_late": 1,
                            },
                        },
                    },
                    "event_level_efficiency": {"report_available": True},
                },
            },
            expectations={
                "drive_remote_provider_watcher_first_events": True,
                "min_remote_provider_event_source_counts": {
                    "local_provider_event_watcher": 1,
                    "provider_webhook": 1,
                },
                "min_remote_provider_event_status_counts": {
                    "received": 1,
                    "received_late": 1,
                },
            },
            provider_invocations=[],
        )
        invalid_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "remote_provider_event_driver": {
                    "events": [
                        {
                            "status": "accepted",
                            "source": "provider_webhook",
                            "recovery_count": 0,
                            "recovery_dispatch_count": 0,
                            "shared_recovery_signal_count": 1,
                        },
                        {
                            "status": "accepted",
                            "source": "local_provider_event_watcher",
                            "reason": "matching_remote_provider_workers_not_recoverable",
                            "recovery_count": 0,
                        },
                    ]
                },
                "provider_case_report": {
                    "service_metrics": {"report_available": True},
                    "event_level_efficiency": {"report_available": True},
                },
            },
            expectations={"drive_remote_provider_watcher_first_events": True},
            provider_invocations=[],
        )

        self.assertEqual(valid_failures, [])
        self.assertEqual(
            invalid_failures,
            [
                "remote_provider_event_driver watcher-first recovery was not observed",
                "remote_provider_event_driver provider-webhook late duplicate was not observed",
            ],
        )

    def test_evaluate_smoke_expectations_requires_remote_provider_event_source_and_status_counts(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "remote_provider_events": {
                            "report_available": True,
                            "source_counts": {"local_provider_event_watcher": 1},
                            "status_counts": {"received": 1},
                        },
                    },
                },
            },
            expectations={
                "min_remote_provider_event_source_counts": {
                    "local_provider_event_watcher": 1,
                    "provider_webhook": 1,
                },
                "min_remote_provider_event_status_counts": {
                    "received": 1,
                    "received_late": 1,
                },
            },
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            [
                "remote_provider_event_source_counts.provider_webhook: expected >= 1, actual=0",
                "remote_provider_event_status_counts.received_late: expected >= 1, actual=0",
            ],
        )

    def test_evaluate_smoke_expectations_enforces_service_slo_maxima(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 130000.0,
                    },
                    "event_level_efficiency": {
                        "report_available": True,
                        "remote_to_next_submit_start_ms": {"max": 12000.0},
                        "remote_to_local_marker_lag_ms": {"max": 9000.0},
                        "local_to_next_submit_start_ms": {"max": 1500.0},
                        "next_submit_attempt_elapsed_ms": {"max": 11000.0},
                        "thresholds_ms": {"remote_to_next_submit_start_hard": 30000.0},
                        "provider_io": {
                            "actor_run_duration_ms": {"max": 91000.0},
                            "dataset_download_duration_ms": {"max": 6500.0},
                        },
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_stage_1_preview_ms": 130000.0,
                            "job_to_board_visible_partial_ms": 11000.0,
                            "final_results_to_board_nonempty_ms": 6000.0,
                            "job_to_board_nonempty_ms": 12000.0,
                            "stage_1_preview_to_final_results_ms": 45000.0,
                        },
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "global_next_worker_start_gap_ms": {"max": 8000.0},
                            },
                        },
                        "remote_provider_events": {
                            "actionable_remote_to_local_event_lag_ms": {"max": 207000.0},
                        },
                        "serving_publication_gap": {
                            "report_available": True,
                            "gap_present": True,
                            "age_ms": 45000.0,
                            "served_snapshot_id": "baseline-snapshot",
                            "current_snapshot_id": "current-snapshot",
                        },
                    },
                },
            },
            expectations={
                "max_job_to_stage_1_preview_ms": 120000,
                "max_job_to_board_visible_partial_ms": 10000,
                "max_final_results_to_board_nonempty_ms": 5000,
                "max_job_to_board_nonempty_ms": 10000,
                "max_stage_1_preview_to_final_results_ms": 30000,
                "max_global_next_worker_start_gap_ms": 5000,
                "max_remote_provider_event_lag_ms": 30000,
                "max_serving_publication_gap_ms": 30000,
                "max_remote_to_next_submit_start_ms": 10000,
                "max_remote_to_local_marker_lag_ms": 8000,
                "max_local_to_next_submit_start_ms": 1000,
                "max_next_submit_attempt_elapsed_ms": 10000,
                "max_provider_io_actor_run_duration_ms": 90000,
                "max_provider_io_dataset_download_duration_ms": 5000,
                "require_no_serving_publication_gap": True,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("job_to_stage_1_preview" in failure for failure in failures))
        self.assertTrue(any("job_to_board_visible_partial_ms" in failure for failure in failures))
        self.assertTrue(any("final_results_to_board_nonempty_ms" in failure for failure in failures))
        self.assertTrue(any("job_to_board_nonempty_ms" in failure for failure in failures))
        self.assertFalse(any("stage_1_preview_to_final_results_ms" in failure for failure in failures))
        self.assertTrue(any("global_next_worker_start_gap_ms" in failure for failure in failures))
        self.assertTrue(
            any("remote_provider_events.actionable_remote_to_local_event_lag_ms" in failure for failure in failures)
        )
        self.assertTrue(any("event_level_efficiency.remote_to_local_marker_lag_ms" in failure for failure in failures))
        self.assertTrue(any("event_level_efficiency.local_to_next_submit_start_ms" in failure for failure in failures))
        self.assertTrue(
            any("event_level_efficiency.next_submit_provider_attempt_elapsed_ms" in failure for failure in failures)
        )
        self.assertTrue(any("event_level_efficiency.provider_io.actor_run_duration_ms" in failure for failure in failures))
        self.assertTrue(
            any("event_level_efficiency.provider_io.dataset_download_duration_ms" in failure for failure in failures)
        )
        self.assertTrue(any("serving publication gap detected" in failure for failure in failures))
        self.assertTrue(any("serving_publication_gap.age_ms" in failure for failure in failures))

    def test_evaluate_smoke_expectations_treats_remote_to_next_submit_soft_slo_as_diagnostic_until_hard_threshold(
        self,
    ) -> None:
        clean_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "remote_to_next_submit_start_ms": {"max": 14035.0},
                        "local_to_next_submit_start_ms": {"max": 0.0},
                        "next_submit_attempt_elapsed_ms": {"max": 1142.0},
                        "thresholds_ms": {"remote_to_next_submit_start_hard": 30000.0},
                    },
                },
            },
            expectations={
                "max_remote_to_next_submit_start_ms": 10000,
                "max_local_to_next_submit_start_ms": 1000,
                "max_next_submit_attempt_elapsed_ms": 10000,
            },
            provider_invocations=[],
        )

        self.assertFalse(clean_failures)

        hard_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "remote_to_next_submit_start_ms": {"max": 31000.0},
                        "local_to_next_submit_start_ms": {"max": 0.0},
                        "next_submit_attempt_elapsed_ms": {"max": 1142.0},
                        "thresholds_ms": {"remote_to_next_submit_start_hard": 30000.0},
                    },
                },
            },
            expectations={
                "max_remote_to_next_submit_start_ms": 10000,
                "max_local_to_next_submit_start_ms": 1000,
                "max_next_submit_attempt_elapsed_ms": 10000,
            },
            provider_invocations=[],
        )

        self.assertEqual(
            hard_failures,
            ["event_level_efficiency.remote_to_next_submit_start_ms: expected <= 30000.0, actual=31000.0"],
        )

    def test_evaluate_smoke_expectations_allows_missing_next_submit_metrics_when_not_applicable(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "next_submit_opportunity": {
                            "applicable": False,
                            "reason": "all_profile_urls_submitted_before_first_completion",
                            "metrics_required": False,
                        },
                        "provider_slot_to_remote_wait_started_ms": {"max": 0.0},
                    },
                },
            },
            expectations={
                "max_remote_to_next_submit_start_ms": 10000,
                "max_local_to_next_submit_start_ms": 1000,
                "max_next_submit_attempt_elapsed_ms": 10000,
                "max_provider_slot_to_remote_wait_started_ms": 5000,
            },
            provider_invocations=[],
        )

        self.assertFalse(failures)

    def test_evaluate_smoke_expectations_rejects_partial_board_visible_after_final_results(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_board_visible_partial": 12000.0,
                        "job_to_final_results": 10000.0,
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_visible_partial_ms": 12000.0,
                            "job_to_final_results_ms": 10000.0,
                        },
                    },
                },
            },
            expectations={"require_partial_board_visible_before_final_results": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["partial board visible: expected before final results (partial=12000.0, final=10000.0)"],
        )

    def test_evaluate_smoke_expectations_rejects_placeholder_to_final_board_jump(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_final_results": 10000.0,
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_final_results_ms": 10000.0,
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "patch_log_count": 1,
                            "patch_consumable_card_nonzero_count": 0,
                            "patch_consumable_card_distinct_count": 0,
                            "pure_shell_patch_count": 1,
                        },
                    },
                },
            },
            expectations={"require_no_placeholder_to_final_board_jump": True},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            [
                "placeholder-to-final board jump: no consumable running card patch observed",
                "placeholder-to-final board jump: consumable card progression did not advance before final",
                "placeholder-to-final board jump: no board-visible publication before final results",
            ],
        )

    def test_evaluate_smoke_expectations_accepts_consumable_card_progress_before_final(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_board_visible_partial": 3000.0,
                        "job_to_final_results": 10000.0,
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_visible_partial_ms": 3000.0,
                            "job_to_final_results_ms": 10000.0,
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "patch_log_count": 3,
                            "patch_consumable_card_nonzero_count": 2,
                            "patch_consumable_card_distinct_count": 2,
                        },
                    },
                },
            },
            expectations={
                "require_no_placeholder_to_final_board_jump": True,
                "require_partial_board_visible_before_final_results": True,
                "min_board_visible_patch_consumable_card_count": 1,
                "min_board_visible_consumable_card_progression_count": 2,
            },
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_accepts_single_complete_board_patch_before_final(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_board_visible_partial": 3000.0,
                        "job_to_final_results": 10000.0,
                    },
                    "board": {
                        "board_runtime_state": {
                            "expected_candidate_count": 140,
                        },
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_visible_partial_ms": 3000.0,
                            "job_to_final_results_ms": 10000.0,
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "patch_log_count": 1,
                            "patch_consumable_card_nonzero_count": 1,
                            "patch_consumable_card_distinct_count": 1,
                            "patch_consumable_card_count": {"max": 140},
                            "patch_display_ready_candidate_count": {"max": 140},
                        },
                    },
                },
            },
            expectations={
                "require_no_placeholder_to_final_board_jump": True,
                "require_partial_board_visible_before_final_results": True,
                "min_board_visible_patch_consumable_card_count": 1,
                "min_board_visible_consumable_card_progression_count": 1,
            },
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_accepts_replayed_complete_board_patch_before_final(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_board_visible_partial": 3000.0,
                        "job_to_final_results": 10000.0,
                    },
                    "board": {
                        "board_runtime_state": {
                            "expected_candidate_count": 140,
                        },
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_visible_partial_ms": 3000.0,
                            "job_to_final_results_ms": 10000.0,
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "patch_log_count": 2,
                            "patch_consumable_card_nonzero_count": 2,
                            "patch_consumable_card_distinct_count": 1,
                            "patch_consumable_card_count": {"max": 140},
                            "patch_display_ready_candidate_count": {"max": 140},
                        },
                    },
                },
            },
            expectations={
                "require_no_placeholder_to_final_board_jump": True,
                "require_partial_board_visible_before_final_results": True,
                "min_board_visible_patch_consumable_card_count": 1,
                "min_board_visible_consumable_card_progression_count": 1,
            },
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_accepts_delta_card_progress_when_baseline_total_is_flat(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": {
                        "job_to_board_visible_partial": 445000.0,
                        "job_to_final_results": 455000.0,
                    },
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_visible_partial_ms": 445000.0,
                            "job_to_final_results_ms": 455000.0,
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "patch_log_count": 2,
                            "patch_consumable_card_nonzero_count": 2,
                            "patch_consumable_card_distinct_count": 1,
                            "patch_display_ready_distinct_count": 2,
                            "patch_delta_card_visible_distinct_count": 2,
                        },
                    },
                },
            },
            expectations={
                "require_no_placeholder_to_final_board_jump": True,
                "require_partial_board_visible_before_final_results": True,
                "min_board_visible_consumable_card_progression_count": 2,
            },
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_requires_user_experience_slo_metric(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_nonempty_ms": 1000.0,
                        },
                    },
                },
            },
            expectations={"max_final_results_to_board_nonempty_ms": 5000},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["service_metrics.user_experience.final_results_to_board_nonempty_ms: metric missing"],
        )

    def test_evaluate_smoke_expectations_requires_worker_handoff_slo_metric(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "worker_timeline": {
                            "worker_count": 2,
                        },
                    },
                },
            },
            expectations={"max_global_next_worker_start_gap_ms": 5000},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["service_metrics.worker_timeline.global_next_worker_start_gap_ms: metric missing"],
        )

    def test_evaluate_smoke_expectations_prefers_profile_scheduler_handoff_gap(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "global_next_worker_start_gap_ms": {"max": 44279.0},
                                "profile_scheduler_next_worker_start_gap_ms": {"count": 0, "max": 0.0},
                            },
                        },
                    },
                },
            },
            expectations={"max_global_next_worker_start_gap_ms": 30000},
            provider_invocations=[],
        )

        assert failures == []

    def test_evaluate_smoke_expectations_fails_uncovered_profile_scheduler_handoff_gap(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "global_next_worker_start_gap_ms": {"max": 44279.0},
                                "profile_scheduler_next_worker_start_gap_ms": {"max": 44279.0},
                            },
                        },
                    },
                },
            },
            expectations={"max_global_next_worker_start_gap_ms": 30000},
            provider_invocations=[],
        )

        assert failures == [
            "service_metrics.worker_timeline.profile_scheduler_next_worker_start_gap_ms: "
            "expected <= 30000.0, actual=44279.0"
        ]

    def test_evaluate_smoke_expectations_allows_missing_remote_event_metric_without_provider_work(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                    },
                },
            },
            expectations={"max_remote_provider_event_lag_ms": 30000},
            provider_invocations=[],
        )

        self.assertEqual(failures, [])

    def test_evaluate_smoke_expectations_requires_remote_event_metric_for_provider_work(self) -> None:
        provider_backed_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                    },
                },
            },
            expectations={"max_remote_provider_event_lag_ms": 30000},
            provider_invocations=[{"logical_name": "harvest_profile_search"}],
        )
        expected_provider_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                    },
                },
            },
            expectations={
                "max_remote_provider_event_lag_ms": 30000,
                "min_provider_invocations_by_logical_name": {"harvest_profile_search": 1},
            },
            provider_invocations=[],
        )
        observed_remote_actor_failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "max_remote_actor_worker_count": 1,
                    },
                    "service_metrics": {
                        "report_available": True,
                    },
                },
            },
            expectations={"max_remote_provider_event_lag_ms": 30000},
            provider_invocations=[],
        )

        self.assertEqual(
            provider_backed_failures,
            ["service_metrics.remote_provider_events.actionable_remote_to_local_event_lag_ms: metric missing"],
        )
        self.assertIn(
            "service_metrics.remote_provider_events.actionable_remote_to_local_event_lag_ms: metric missing",
            expected_provider_failures,
        )
        self.assertEqual(
            observed_remote_actor_failures,
            ["service_metrics.remote_provider_events.actionable_remote_to_local_event_lag_ms: metric missing"],
        )

    def test_evaluate_smoke_expectations_requires_late_remote_duplicate_metrics(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "remote_provider_events": {
                            "report_available": True,
                            "event_count": 2,
                            "received_count": 1,
                            "late_duplicate_count": 0,
                            "in_flight_duplicate_count": 1,
                            "duplicate_event_count": 1,
                            "remote_to_local_event_lag_ms": {"max": 1000.0},
                        },
                    },
                },
            },
            expectations={
                "min_remote_provider_event_count": 2,
                "min_remote_provider_event_received_count": 1,
                "min_remote_provider_event_late_duplicate_count": 1,
                "max_remote_provider_event_in_flight_duplicate_count": 0,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("remote_provider_event_late_duplicate_count" in failure for failure in failures))
        self.assertTrue(any("remote_provider_event_in_flight_duplicate_count" in failure for failure in failures))

    def test_smoke_remote_provider_webhook_payload_uses_scripted_actor_duration(self) -> None:
        payload = _smoke_remote_provider_webhook_payload(
            {
                "worker_id": 44,
                "checkpoint": {
                    "run_id": "run-scripted",
                    "dataset_id": "dataset-scripted",
                    "scripted_remote_ready_epoch_ms": 1_000_000,
                    "provider_timings": {"actor_run_duration_ms": 22500},
                },
            }
        )

        event_data = payload["eventData"]
        self.assertEqual(event_data["finishedAt"], "1970-01-01T00:16:40+00:00")
        self.assertEqual(event_data["startedAt"], "1970-01-01T00:16:17.500000+00:00")

    def test_evaluate_smoke_expectations_can_gate_provider_anomalies_when_declared(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "provider_anomalies": {
                            "report_available": True,
                            "anomaly_count": 8,
                            "zero_result_retry_count": 3,
                            "zero_result_retry_exhausted_count": 1,
                            "zero_result_accepted_count": 1,
                            "empty_scale_count": 2,
                            "probe_total_drift_count": 1,
                            "empty_page_range_count": 2,
                            "single_page_retry_count": 4,
                        },
                    },
                },
            },
            expectations={
                "max_provider_anomaly_count": 0,
                "max_provider_zero_result_retry_count": 0,
                "max_provider_zero_result_retry_exhausted_count": 0,
                "max_provider_zero_result_accepted_count": 0,
                "max_provider_empty_scale_count": 0,
                "max_provider_probe_total_drift_count": 0,
                "max_provider_empty_page_range_count": 0,
                "max_provider_single_page_retry_count": 0,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("provider_anomalies.anomaly_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.zero_result_retry_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.zero_result_retry_exhausted_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.zero_result_accepted_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.empty_scale_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.probe_total_drift_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.empty_page_range_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.single_page_retry_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_requires_provider_anomaly_report_only_when_declared(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {"service_metrics": {"report_available": True}},
            },
            expectations={"max_provider_anomaly_count": 0},
            provider_invocations=[],
        )

        self.assertEqual(failures, ["provider_anomalies.anomaly_count: provider anomaly report unavailable"])

    def test_evaluate_smoke_expectations_can_gate_provider_anomaly_minimums_when_declared(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "provider_anomalies": {
                            "report_available": True,
                            "anomaly_count": 3,
                            "zero_result_retry_count": 2,
                            "zero_result_retry_exhausted_count": 0,
                            "empty_scale_count": 0,
                            "probe_total_drift_count": 0,
                            "empty_page_range_count": 0,
                            "single_page_retry_count": 1,
                        },
                    },
                },
            },
            expectations={
                "min_provider_anomaly_count": 4,
                "min_provider_zero_result_retry_count": 3,
                "min_provider_zero_result_retry_exhausted_count": 1,
                "min_provider_empty_scale_count": 1,
                "min_provider_probe_total_drift_count": 1,
                "min_provider_empty_page_range_count": 1,
                "min_provider_single_page_retry_count": 2,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("provider_anomalies.anomaly_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.zero_result_retry_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.zero_result_retry_exhausted_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.empty_scale_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.probe_total_drift_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.empty_page_range_count" in failure for failure in failures))
        self.assertTrue(any("provider_anomalies.single_page_retry_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_can_gate_target_public_web_guardrails(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "report_available": True,
                        "target_candidate_public_web": {
                            "report_available": True,
                            "remote_search_pending_run_count": 1,
                            "partial_failure_count": 1,
                            "completed_without_materialized_signals_count": 1,
                            "missing_phase_metric_count": 1,
                            "provider_or_fetch_failure_count": 2,
                            "local_processing_error_count": 1,
                            "crm_storage_owner_batch_count": 0,
                            "legacy_storage_owner_batch_count": 1,
                            "execution_backend_counts": {},
                            "duration_by_phase_ms_max": {
                                "document_fetch": 31_000.0,
                                "adjudication": 12_000.0,
                            },
                            "terminal_with_errors_count": 1,
                            "service_guardrail_violation_detected": True,
                            "latest_batch": {
                                "phase_metrics": {"service_guardrail_violation_detected": True},
                            },
                        },
                    },
                },
            },
            expectations={
                "require_no_target_public_web_guardrail_violation": True,
                "require_crm_public_web_storage_owner": True,
                "require_public_web_execution_backend_report": True,
                "max_target_public_web_legacy_storage_owner_batch_count": 0,
                "max_target_public_web_remote_pending_run_count": 0,
                "max_target_public_web_partial_failure_count": 0,
                "max_target_public_web_completed_without_materialized_signals_count": 0,
                "max_target_public_web_missing_phase_metric_count": 0,
                "max_target_public_web_provider_or_fetch_failure_count": 0,
                "max_target_public_web_local_processing_error_count": 0,
                "max_target_public_web_duration_by_phase_ms": {
                    "document_fetch": 30_000,
                    "adjudication": 10_000,
                },
            },
            provider_invocations=[],
        )

        self.assertTrue(any("target_candidate_public_web guardrail violation" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.remote_search_pending_run_count" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.partial_failure_count" in failure for failure in failures))
        self.assertTrue(
            any("target_candidate_public_web.completed_without_materialized_signals_count" in failure for failure in failures)
        )
        self.assertTrue(any("target_candidate_public_web.missing_phase_metric_count" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.provider_or_fetch_failure_count" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.local_processing_error_count" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.storage_owner" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.execution_backend" in failure for failure in failures))
        self.assertTrue(any("target_candidate_public_web.legacy_storage_owner_batch_count" in failure for failure in failures))
        self.assertTrue(
            any("target_candidate_public_web.duration_by_phase_ms_max.document_fetch" in failure for failure in failures)
        )
        self.assertTrue(
            any("target_candidate_public_web.duration_by_phase_ms_max.adjudication" in failure for failure in failures)
        )

    def test_workflow_wall_clock_uses_observed_final_when_stage_timestamp_is_zero(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-03T06:15:06+00:00",
                    "completed_at": "2026-05-03T06:15:06+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-03T06:15:06+00:00",
                    "completed_at": "2026-05-03T06:15:06+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 5000.0,
                "fetch_job_and_results": 100.0,
                "board_probe_wait": 250.0,
            },
            timeline=[],
        )

        self.assertEqual(report["job_to_final_results"], 5100.0)
        self.assertEqual(report["final_results_to_board_nonempty"], 250.0)
        self.assertEqual(report["job_to_board_nonempty"], 5350.0)

    def test_workflow_wall_clock_uses_board_specific_probe_wait_when_layering_probe_times_out(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "stage_2_final": {
                    "started_at": "2026-05-03T06:15:00+00:00",
                    "completed_at": "2026-05-03T06:15:00+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 5000.0,
                "fetch_job_and_results": 100.0,
                "board_probe_wait": 30000.0,
                "board_ready_wait": 125.0,
                "board_nonempty_wait": 150.0,
            },
            timeline=[],
        )

        self.assertEqual(report["final_results_to_board_ready"], 125.0)
        self.assertEqual(report["final_results_to_board_nonempty"], 150.0)

    def test_workflow_wall_clock_uses_effective_preview_when_stage_preview_timestamp_is_late(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-03T06:15:00+00:00",
                    "completed_at": "2026-05-03T06:15:55+00:00",
                },
                "stage_1_preview": {
                    "started_at": "2026-05-03T06:15:55+00:00",
                    "completed_at": "2026-05-03T06:17:02+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-03T06:17:02+00:00",
                    "completed_at": "2026-05-03T06:17:09+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "board_probe_wait": 1000.0,
                "board_ready_wait": 1000.0,
                "board_nonempty_wait": 1000.0,
            },
            timeline=[],
            post_preview_finalization={
                "effective_stage_1_preview_completed_at": "2026-05-03T06:16:09+00:00",
                "stage_1_preview_timestamp_source": "first_finalization_event_when_stage_preview_timestamp_is_late",
            },
        )

        self.assertEqual(report["job_to_stage_1_preview"], 69000.0)
        self.assertEqual(report["stage_1_preview_to_final_results"], 60000.0)

    def test_workflow_wall_clock_uses_backend_stage_lag_over_smoke_observation_lag(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-11T16:00:00+00:00",
                    "completed_at": "2026-05-11T16:00:45+00:00",
                },
                "stage_1_preview": {
                    "started_at": "2026-05-11T16:00:46+00:00",
                    "completed_at": "2026-05-11T16:00:52+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-11T16:00:46+00:00",
                    "completed_at": "2026-05-11T16:00:58+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 382000.0,
                "fetch_job_and_results": 792.0,
                "board_probe_wait": 1000.0,
            },
            timeline=[
                {
                    "status": "completed",
                    "stage": "acquiring",
                    "message": "Stage 1 preview ready; continuing snapshot materialization.",
                    "observed_at_ms": 235000.0,
                }
            ],
        )

        self.assertEqual(report["job_to_final_results"], 382792.0)
        self.assertEqual(report["stage_1_preview_to_final_results"], 6000.0)
        self.assertEqual(report["stage_1_preview_to_final_results_source"], "workflow_stage_summaries")

    def test_workflow_wall_clock_uses_durable_post_preview_evidence_when_stage2_summary_is_early(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-13T01:05:27+00:00",
                    "completed_at": "2026-05-13T01:06:05+00:00",
                },
                "stage_1_preview": {
                    "started_at": "2026-05-13T01:06:06+00:00",
                    "completed_at": "2026-05-13T01:06:11+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-13T01:06:06+00:00",
                    "completed_at": "2026-05-13T01:06:11+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 415000.0,
                "fetch_job_and_results": 700.0,
                "board_probe_wait": 1000.0,
            },
            timeline=[],
            post_preview_finalization={
                "effective_stage_1_preview_completed_at": "2026-05-13T01:06:11+00:00",
                "preview_to_finalization_completed_ms": 261000.0,
            },
        )

        self.assertEqual(report["stage_1_preview_to_final_results"], 261000.0)
        self.assertEqual(
            report["stage_1_preview_to_final_results_source"],
            "post_preview_finalization_durable_evidence",
        )

    def test_workflow_wall_clock_uses_stage1_preview_timeline_when_stage_summary_missing(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "explain": 5000.0,
                "plan": 4000.0,
                "review": 500.0,
                "start": 500.0,
                "wait_for_completion": 100000.0,
                "fetch_job_and_results": 1000.0,
            },
            timeline=[
                {
                    "status": "completed",
                    "stage": "acquiring",
                    "message": "Stage 1 preview ready; continuing snapshot materialization.",
                    "observed_at_ms": 55000.0,
                }
            ],
        )

        self.assertEqual(report["job_to_stage_1_preview"], 45000.0)

    def test_workflow_wall_clock_uses_running_stage1_preview_ready_timeline(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "explain": 5000.0,
                "plan": 4000.0,
                "review": 500.0,
                "start": 500.0,
                "wait_for_completion": 100000.0,
                "fetch_job_and_results": 1000.0,
            },
            timeline=[
                {
                    "status": "running",
                    "stage": "acquiring",
                    "message": "Stage 1 preview ready. Continuing snapshot materialization.",
                    "observed_at_ms": 70000.0,
                }
            ],
        )

        self.assertEqual(report["job_to_stage_1_preview"], 60000.0)

    def test_workflow_wall_clock_does_not_treat_stage1_building_as_preview_ready(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "explain": 5000.0,
                "plan": 4000.0,
                "review": 500.0,
                "start": 500.0,
                "wait_for_completion": 100000.0,
                "fetch_job_and_results": 1000.0,
            },
            timeline=[
                {
                    "status": "running",
                    "stage": "acquiring",
                    "message": "Building Stage 1 deterministic preview",
                    "observed_at_ms": 55000.0,
                }
            ],
        )

        self.assertNotIn("job_to_stage_1_preview", report)

    def test_workflow_wall_clock_uses_job_event_anchor_for_board_visible_patch_without_stage_summary(
        self,
    ) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 120000.0,
                "fetch_job_and_results": 500.0,
                "board_probe_wait": 1000.0,
            },
            timeline=[],
            job_events=[
                {
                    "created_at": "2026-05-08T13:10:38+00:00",
                    "stage": "planning",
                    "status": "queued",
                    "detail": "Workflow created from user request.",
                    "payload": {},
                }
            ],
            board_visible_patches=[
                {
                    "sequence_index": 1,
                    "patch_kind": "partial_delta_board_visible_patch",
                    "patch_phase": "board_visible_delta_applied",
                    "candidate_ids": ["c1"],
                    "cumulative_candidate_count": 1,
                    "served_candidate_count": 301,
                    "published_at": "2026-05-08T13:10:58+00:00",
                }
            ],
        )

        self.assertEqual(report["job_to_board_visible_partial"], 20000.0)
        self.assertEqual(report["job_to_board_nonempty"], 20000.0)
        self.assertEqual(report["job_to_board_ready"], 20000.0)
        self.assertEqual(report["job_to_stage_1_preview"], 20000.0)
        self.assertEqual(
            report["job_to_stage_1_preview_source"],
            "board_visible_publication_when_preview_anchor_missing",
        )

    def test_workflow_wall_clock_uses_row_shell_publication_for_board_nonempty(self) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={
                "ready": True,
                "ready_nonempty": True,
                "board_runtime_state": {
                    "published_candidate_count": 8831,
                    "served_candidate_count": 8831,
                    "expected_candidate_count": 8831,
                    "row_publication_started_at": "2026-05-08T13:10:45+00:00",
                    "row_publication_updated_at": "2026-05-08T13:15:00+00:00",
                },
            },
            timings_ms={
                "wait_for_completion": 120000.0,
                "fetch_job_and_results": 500.0,
                "board_probe_wait": 1000.0,
            },
            timeline=[],
            job_events=[
                {
                    "created_at": "2026-05-08T13:10:38+00:00",
                    "stage": "planning",
                    "status": "queued",
                    "detail": "Workflow created from user request.",
                    "payload": {},
                }
            ],
            board_visible_patches=[],
        )

        self.assertEqual(report["job_to_board_visible_partial"], 7000.0)
        self.assertEqual(report["job_to_board_nonempty"], 7000.0)
        self.assertEqual(report["job_to_board_ready"], 7000.0)

    def test_workflow_wall_clock_sets_final_to_board_zero_when_board_visible_before_final(
        self,
    ) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={},
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={
                "wait_for_completion": 120000.0,
                "fetch_job_and_results": 500.0,
                "board_probe_wait": 30000.0,
                "board_ready_wait": 30000.0,
                "board_nonempty_wait": 30000.0,
            },
            timeline=[],
            job_events=[
                {
                    "created_at": "2026-05-08T13:10:38+00:00",
                    "stage": "planning",
                    "status": "queued",
                    "detail": "Workflow created from user request.",
                    "payload": {},
                }
            ],
            board_visible_patches=[
                {
                    "sequence_index": 1,
                    "patch_kind": "partial_delta_board_visible_patch",
                    "patch_phase": "board_visible_delta_applied",
                    "candidate_ids": ["c1"],
                    "cumulative_candidate_count": 1,
                    "served_candidate_count": 301,
                    "published_at": "2026-05-08T13:10:55+00:00",
                }
            ],
        )

        self.assertEqual(report["job_to_final_results"], 120500.0)
        self.assertEqual(report["job_to_board_nonempty"], 17000.0)
        self.assertEqual(report["final_results_to_board_nonempty"], 0.0)

    def test_full_snapshot_board_visible_publication_counts_for_nonempty_not_partial_delta(
        self,
    ) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-05T05:49:34+00:00",
                    "completed_at": "2026-05-05T05:49:35+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-05T05:50:32+00:00",
                    "completed_at": "2026-05-05T05:50:34+00:00",
                },
            },
            board_probe={"ready": True, "ready_nonempty": True},
            timings_ms={"board_probe_wait": 1000.0},
            timeline=[],
            job_events=[
                {
                    "created_at": "2026-05-05T05:49:40+00:00",
                    "stage": "acquiring",
                    "status": "running",
                    "detail": "Inline workflow published full snapshot serving before final results.",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "board_visible_full_snapshot_serving",
                        "snapshot_id": "snap-lovable",
                        "board_visible_patch": {
                            "kind": "full_snapshot_board_visible_patch",
                            "patch_phase": "board_visible_full_snapshot_serving",
                            "served_candidate_count": 140,
                        },
                    },
                }
            ],
            board_visible_patches=[],
        )

        self.assertEqual(report["job_to_board_nonempty"], 6000.0)
        self.assertEqual(report["job_to_board_ready"], 6000.0)
        self.assertNotIn("job_to_board_visible_partial", report)

        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "workflow_wall_clock_ms": report,
                    "service_metrics": {
                        "report_available": True,
                        "user_experience": {
                            "job_to_board_nonempty_ms": report["job_to_board_nonempty"],
                            "job_to_final_results_ms": report["job_to_final_results"],
                        }
                    },
                },
            },
            expectations={
                "require_partial_board_visible_before_final_results": True,
                "max_job_to_board_nonempty_ms": 30000,
            },
            provider_invocations=[],
        )
        self.assertTrue(any("partial board visible" in failure for failure in failures))
        self.assertFalse(any("job_to_board_nonempty_ms" in failure for failure in failures))

    def test_workflow_wall_clock_uses_board_runtime_publication_for_partial_visibility(
        self,
    ) -> None:
        report = _build_workflow_wall_clock_report(
            stage_wall_clock={
                "linkedin_stage_1": {
                    "started_at": "2026-05-14T01:48:19+00:00",
                    "completed_at": "2026-05-14T01:52:01+00:00",
                },
                "stage_2_final": {
                    "started_at": "2026-05-14T01:55:43+00:00",
                    "completed_at": "2026-05-14T01:57:39+00:00",
                },
            },
            board_probe={
                "ready": True,
                "ready_nonempty": True,
                "board_runtime_state": {
                    "phase": "current_snapshot_serving",
                    "publication_status": "complete",
                    "served_candidate_count": 8831,
                    "expected_candidate_count": 8831,
                    "row_publication_updated_at": "2026-05-14T01:52:52+00:00",
                },
            },
            timings_ms={"board_probe_wait": 1000.0},
            timeline=[],
            board_visible_patches=[],
        )

        self.assertEqual(report["job_to_board_nonempty"], 273000.0)
        self.assertEqual(report["job_to_board_ready"], 273000.0)
        self.assertEqual(report["job_to_board_visible_partial"], 273000.0)

    def test_provider_case_report_uses_durable_board_patch_events_for_partial_visibility(self) -> None:
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={
                "workflow_stage_summaries": {
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "started_at": "2026-05-08T12:00:00+00:00",
                            "completed_at": "2026-05-08T12:00:05+00:00",
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-05-08T12:01:00+00:00",
                            "completed_at": "2026-05-08T12:01:10+00:00",
                        },
                    }
                },
                "job": {"summary": {}},
            },
            dashboard_payload={"asset_population": {"available": True, "candidate_count": 50}},
            candidate_page_payload={"total_candidates": 50, "candidates": [{"candidate_id": "c1"}]},
            timings_ms={"board_probe_wait": 500.0},
            job_events=[
                {
                    "created_at": "2026-05-08T12:00:15+00:00",
                    "stage": "acquiring",
                    "status": "running",
                    "detail": "Event-level materialization published board-visible delta.",
                    "payload_json": json.dumps(
                        {
                            "event_family": "workflow_materialization",
                            "phase": "board_visible_delta_applied",
                            "snapshot_id": "snap-openai",
                            "board_visible_patch": {
                                "kind": "partial_delta_board_visible_patch",
                                "patch_phase": "board_visible_delta_applied",
                                "candidate_ids": ["c1"],
                                "candidate_count": 1,
                                "cumulative_candidate_count": 1,
                                "published_at": "2026-05-08T12:00:15+00:00",
                            },
                        }
                    ),
                }
            ],
            board_visible_patches=[],
        )

        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_board_visible_partial"], 15000.0)
        self.assertEqual(report["service_metrics"]["user_experience"]["job_to_board_visible_partial_ms"], 15000.0)

    def test_provider_case_report_extracts_serving_publication_gap_from_dashboard(self) -> None:
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={"job": {"summary": {}}},
            dashboard_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 2,
                    "candidate_source": {
                        "result_view": {
                            "metadata": {
                                "serving_publication_gap": {
                                    "status": "pending_event_time_publication",
                                    "served_snapshot_id": "baseline-snapshot",
                                    "current_snapshot_id": "current-snapshot",
                                    "source_updated_at": "2026-04-30T16:00:00+00:00",
                                    "observed_at": "2026-04-30T16:01:00+00:00",
                                }
                            }
                        }
                    },
                }
            },
            candidate_page_payload={},
            timings_ms={},
        )

        gap = report["service_metrics"]["serving_publication_gap"]
        self.assertTrue(gap["gap_present"])
        self.assertTrue(gap["stale_gap_present"])
        self.assertEqual(gap["age_ms"], 60000.0)

    def test_provider_case_report_prefers_canonical_lifecycle_over_stale_publication_gap_metadata(self) -> None:
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={"job": {"summary": {}}},
            dashboard_payload={
                "result_view_lifecycle": {
                    "state": "current_snapshot_serving",
                    "phase": "current_snapshot_serving",
                    "serving_projection_phase": "current_snapshot_serving",
                    "current_snapshot_id": "current-snapshot",
                    "served_snapshot_id": "current-snapshot",
                },
                "asset_population": {
                    "available": True,
                    "candidate_count": 2,
                    "candidate_source": {
                        "result_view": {
                            "metadata": {
                                "serving_publication_gap": {
                                    "status": "pending_event_time_publication",
                                    "served_snapshot_id": "baseline-snapshot",
                                    "current_snapshot_id": "current-snapshot",
                                    "source_updated_at": "2026-04-30T16:00:00+00:00",
                                    "observed_at": "2026-04-30T16:01:00+00:00",
                                }
                            }
                        }
                    },
                },
            },
            candidate_page_payload={},
            timings_ms={},
        )

        gap = report["service_metrics"]["serving_publication_gap"]
        self.assertFalse(gap["report_available"])
        self.assertFalse(gap["gap_present"])

    def test_provider_case_report_counts_running_workflow_materialization_events(self) -> None:
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={
                "background_reconcile": {
                    "harvest_prefetch": {
                        "status": "completed",
                        "sync_result": {
                            "status": "completed",
                            "reason": "inline_background_harvest_prefetch_reconcile",
                        },
                    }
                }
            },
            results_payload={
                "workflow_stage_summaries": {
                    "summaries": {
                        "stage_1_preview": {
                            "stage": "stage_1_preview",
                            "status": "completed",
                            "completed_at": "2026-05-01T10:00:00+00:00",
                        },
                        "stage_2_final": {
                            "stage": "stage_2_final",
                            "status": "completed",
                            "completed_at": "2026-05-01T10:00:12+00:00",
                        },
                    },
                },
                "job": {"summary": {}},
            },
            dashboard_payload={},
            candidate_page_payload={},
            timings_ms={},
            job_events=[
                {
                    "created_at": "2026-05-01T10:00:03+00:00",
                    "stage": "acquiring",
                    "status": "running",
                    "detail": "Inline workflow materialization started candidate artifact sync.",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_started",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-running",
                        "worker_ids": [7001],
                    },
                },
                {
                    "created_at": "2026-05-01T10:00:09+00:00",
                    "stage": "acquiring",
                    "status": "completed",
                    "detail": "Inline workflow materialization completed candidate artifact sync.",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_completed",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-running",
                        "worker_ids": [7001],
                        "sync_result": {
                            "status": "completed",
                            "reason": "inline_background_harvest_prefetch_reconcile",
                            "materialization_streaming": {
                                "provider_response_count": 1,
                                "profile_url_count": 0,
                                "pending_delta_count": 1,
                                "active_writer_count": 0,
                                "queued_writer_count": 0,
                                "recommended_action": "dispatch_delta_writer",
                            },
                        },
                    },
                },
            ],
        )

        post_preview = report["post_preview_finalization"]
        self.assertTrue(post_preview["report_available"])
        self.assertEqual(post_preview["preview_to_first_materialize_start_ms"], 3000.0)
        self.assertEqual(post_preview["preview_to_last_materialize_completed_ms"], 9000.0)
        self.assertEqual(post_preview["materialize_completed_count"], 1)
        self.assertEqual(post_preview["materialize_sync_duration_ms"]["max"], 6000.0)
        self.assertEqual(report["event_level_efficiency"]["reconcile"]["materialize_completed_count"], 1)
        materialization_streaming = report["materialization_streaming"]
        self.assertTrue(materialization_streaming["report_available"])
        self.assertEqual(materialization_streaming["source"], "structured_materialization_events")
        self.assertEqual(materialization_streaming["event_sample_count"], 1)
        self.assertEqual(materialization_streaming["provider_response_count"], 1)
        self.assertEqual(materialization_streaming["pending_delta_count"], 1)
        self.assertEqual(materialization_streaming["budget"]["recommended_action"], "dispatch_delta_writer")

    def test_provider_case_report_rolls_up_materialization_streaming_from_structured_events(self) -> None:
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI"},
            job_summary={},
            results_payload={
                "workflow_stage_summaries": {
                    "summaries": {
                        "stage_1_preview": {
                            "stage": "stage_1_preview",
                            "status": "completed",
                            "completed_at": "2026-05-01T10:00:00+00:00",
                        },
                        "stage_2_final": {
                            "stage": "stage_2_final",
                            "status": "completed",
                            "completed_at": "2026-05-01T10:00:12+00:00",
                        },
                    },
                },
                "job": {"summary": {}},
            },
            dashboard_payload={},
            candidate_page_payload={},
            timings_ms={},
            job_events=[
                {
                    "created_at": "2026-05-01T10:00:03+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_deferred",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-running",
                        "worker_ids": [7001],
                        "sync_result": {
                            "status": "deferred",
                            "reason": "same_kind_background_workers_still_inflight",
                            "materialization_streaming": {
                                "provider_response_count": 1,
                                "profile_url_count": 0,
                                "pending_delta_count": 4,
                                "active_writer_count": 0,
                                "queued_writer_count": 3,
                                "coalescing_window_ms": 750,
                                "oldest_pending_delta_age_ms": 0.0,
                                "recommended_action": "coalesce_window",
                            },
                        },
                    },
                },
                {
                    "created_at": "2026-05-01T10:00:09+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "materialize_completed",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-running",
                        "worker_ids": [7001, 7002],
                        "sync_result": {
                            "status": "completed",
                            "reason": "inline_background_harvest_prefetch_reconcile",
                            "materialization_streaming": {
                                "provider_response_count": 2,
                                "profile_url_count": 0,
                                "pending_delta_count": 2,
                                "active_writer_count": 0,
                                "queued_writer_count": 0,
                                "recommended_action": "dispatch_delta_writer",
                            },
                        },
                    },
                },
            ],
        )

        materialization_streaming = report["materialization_streaming"]
        self.assertTrue(materialization_streaming["report_available"])
        self.assertEqual(materialization_streaming["source"], "structured_materialization_events")
        self.assertEqual(materialization_streaming["event_sample_count"], 2)
        self.assertEqual(materialization_streaming["provider_response_count"], 3)
        self.assertEqual(materialization_streaming["pending_delta_count"], 4)
        self.assertEqual(materialization_streaming["event_phase_counts"]["materialize_deferred"], 1)
        self.assertEqual(materialization_streaming["event_phase_counts"]["materialize_completed"], 1)
        self.assertEqual(materialization_streaming["event_budget_action_counts"]["coalesce_window"], 1)
        self.assertEqual(materialization_streaming["event_budget_action_counts"]["dispatch_delta_writer"], 1)
        self.assertEqual(materialization_streaming["budget"]["recommended_action"], "coalesce_window")

    def test_evaluate_smoke_expectations_uses_runtime_peak_remote_actor_count(self) -> None:
        record = {
            "final": {"raw_job_status": "completed"},
            "provider_case_report": {
                "behavior_guardrails": {
                    "duplicate_provider_dispatch": {"violation_detected": False},
                    "disabled_stage_violations": {"unexpected_public_web_stage": False},
                },
                "workflow_benchmark": {
                    "profile_url_total_count": 200,
                    "fetched_profile_count": 150,
                    "board_total_candidates": 150,
                },
                "event_level_efficiency": {
                    "violation_detected": False,
                    "provider_slots": {"remote_actor_worker_count": 0},
                },
                "progress_observability": {
                    "maxima": {"waiting_remote_harvest_count": 2},
                },
                "service_metrics": {
                    "worker_timeline": {"worker_count": 6},
                },
            },
        }
        failures = _evaluate_smoke_expectations(
            record=record,
            expectations={"min_remote_actor_worker_count": 2},
            provider_invocations=[],
        )

        self.assertFalse(failures)

    def test_evaluate_smoke_expectations_rejects_low_remote_actor_slot_occupancy(self) -> None:
        record = {
            "final": {"raw_job_status": "completed"},
            "provider_case_report": {
                "event_level_efficiency": {
                    "report_available": True,
                    "violation_detected": False,
                    "provider_slots": {"remote_actor_worker_count": 1},
                },
                "remote_actor_slot_observation": {
                    "report_available": True,
                    "harvest_profile_actor_global_inflight": 4,
                    "peak_waiting_remote_harvest_count": 1,
                    "remote_actor_slot_peak_occupancy_ratio": 0.25,
                },
            },
        }

        failures = _evaluate_smoke_expectations(
            record=record,
            expectations={"min_remote_actor_slot_occupancy_ratio": 0.75},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["remote_actor_slot_occupancy_ratio: expected >= 0.75, actual=0.25"],
        )

    def test_evaluate_smoke_expectations_checks_profile_prefetch_planned_worker_count(self) -> None:
        record = {
            "final": {"raw_job_status": "completed"},
            "provider_case_report": {
                "event_level_efficiency": {
                    "report_available": True,
                    "violation_detected": False,
                    "profile_prefetch_batch_plan": {
                        "plan_count": 2,
                        "planned_new_worker_count": 4,
                    },
                },
            },
        }

        failures = _evaluate_smoke_expectations(
            record=record,
            expectations={
                "min_profile_prefetch_batch_plan_count": 2,
                "min_profile_prefetch_planned_new_worker_count": 4,
            },
            provider_invocations=[],
        )

        self.assertFalse(failures)

    def test_evaluate_smoke_expectations_rejects_repeated_materialize_churn(self) -> None:
        record = {
            "final": {"raw_job_status": "completed"},
            "provider_case_report": {
                "behavior_guardrails": {
                    "duplicate_provider_dispatch": {"violation_detected": False},
                    "disabled_stage_violations": {"unexpected_public_web_stage": False},
                },
                "workflow_benchmark": {
                    "profile_url_total_count": 250,
                    "fetched_profile_count": 165,
                    "board_total_candidates": 1061,
                },
                "event_level_efficiency": {
                    "violation_detected": False,
                    "provider_slots": {"remote_actor_worker_count": 4},
                    "reconcile": {
                        "repeated_materialize_signature_count": 2,
                        "same_worker_reconcile_repeat_count": 1,
                    },
                },
                "service_metrics": {
                    "worker_timeline": {"worker_count": 7},
                },
            },
        }
        failures = _evaluate_smoke_expectations(
            record=record,
            expectations={
                "max_repeated_materialize_signature_count": 0,
                "max_same_worker_reconcile_repeat_count": 0,
            },
            provider_invocations=[],
        )

        self.assertTrue(any("repeated_materialize_signature_count" in failure for failure in failures))
        self.assertTrue(any("same_worker_reconcile_repeat_count" in failure for failure in failures))

    def test_evaluate_smoke_expectations_requires_completed_reconcile_lease_skip_observation(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "event_level_efficiency": {
                        "report_available": True,
                        "reconcile": {
                            "lease_skipped_count": 0,
                            "same_worker_reconcile_repeat_count": 0,
                            "repeated_materialize_signature_count": 0,
                        },
                    },
                    "service_metrics": {"report_available": True},
                },
            },
            expectations={"min_completed_reconcile_lease_skipped_count": 1},
            provider_invocations=[],
        )

        self.assertEqual(
            failures,
            ["completed_reconcile_lease_skipped_count: expected >= 1, actual=0"],
        )

    def test_evaluate_smoke_expectations_rejects_wrong_delta_dispatch_contract(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "explain": {
                    "dispatch_strategy": "new_job",
                    "planner_mode": "",
                    "requires_delta_acquisition": False,
                    "effective_acquisition_mode": "scoped_live_search",
                    "keywords": ["ChatGPT"],
                    "asset_reuse_baseline_snapshot_id": "baseline-old",
                    "dispatch_matched_snapshot_id": "dispatch-old",
                    "request_delta_baseline_snapshot_id": "request-old",
                },
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "workflow_benchmark": {},
                    "event_level_efficiency": {"violation_detected": False},
                },
            },
            expectations={
                "expect_explain_dispatch_strategy": "delta_from_snapshot",
                "expect_explain_requires_delta_acquisition": True,
                "expect_explain_effective_acquisition_mode": "baseline_reuse_with_delta",
                "expect_explain_keywords_include": ["ChatGPT"],
                "expect_explain_baseline_snapshot_id": "baseline-new",
                "expect_explain_dispatch_matched_snapshot_id": "dispatch-new",
                "expect_explain_request_delta_baseline_snapshot_id": "request-new",
            },
            provider_invocations=[],
        )

        self.assertTrue(any("explain.dispatch_strategy" in failure for failure in failures))
        self.assertTrue(any("explain.requires_delta_acquisition" in failure for failure in failures))
        self.assertTrue(any("explain.effective_acquisition_mode" in failure for failure in failures))
        self.assertTrue(any("explain.asset_reuse_baseline_snapshot_id" in failure for failure in failures))
        self.assertTrue(any("explain.dispatch_matched_snapshot_id" in failure for failure in failures))
        self.assertTrue(any("explain.request_delta_baseline_snapshot_id" in failure for failure in failures))

    def test_evaluate_smoke_expectations_can_gate_zero_provider_reuse(self) -> None:
        failures = _evaluate_smoke_expectations(
            record={
                "final": {"raw_job_status": "completed"},
                "provider_case_report": {
                    "behavior_guardrails": {
                        "duplicate_provider_dispatch": {"violation_detected": False},
                        "disabled_stage_violations": {"unexpected_public_web_stage": False},
                    },
                    "workflow_benchmark": {},
                    "event_level_efficiency": {"violation_detected": False},
                    "service_metrics": {"report_available": True},
                },
            },
            expectations={"max_provider_invocation_count": 0},
            provider_invocations=[
                {
                    "logical_name": "harvest_profile_search",
                    "dispatch_signature": "unexpected-provider-dispatch",
                }
            ],
        )

        self.assertEqual(failures, ["provider_invocation_count: expected <= 0, actual=1"])

    def test_effective_asset_population_candidate_count_prefers_final_results_canonical_total(self) -> None:
        count = _effective_asset_population_candidate_count(
            results_payload={"asset_population": {"candidate_count": 1020}},
            dashboard_payload={"asset_population": {"candidate_count": 1020}},
            candidate_page_payload={
                "result_mode": "asset_population",
                "returned_count": 2,
                "total_candidates": 2,
            },
        )

        self.assertEqual(count, 1020)

    def test_board_probe_uses_canonical_board_runtime_visible_count(self) -> None:
        report = _build_board_probe_report(
            dashboard_payload={},
            candidate_page_payload={
                "result_mode": "ranked_results",
                "returned_count": 0,
                "total_candidates": 0,
                "board_runtime_state": {
                    "phase": "current_snapshot_serving",
                    "publication_status": "complete",
                    "served_candidate_count": 8831,
                    "expected_candidate_count": 8831,
                    "row_hydration_target_count": 8831,
                },
            },
        )

        self.assertTrue(report["ready_nonempty"])
        self.assertEqual(report["candidate_page_total_candidates"], 8831)

    def test_effective_asset_population_candidate_count_prefers_board_runtime_when_page_total_is_zero(self) -> None:
        count = _effective_asset_population_candidate_count(
            results_payload={},
            dashboard_payload={},
            candidate_page_payload={
                "total_candidates": 0,
                "returned_count": 0,
                "board_runtime_state": {
                    "phase": "current_snapshot_serving",
                    "publication_status": "complete",
                    "served_candidate_count": 8831,
                    "expected_candidate_count": 8831,
                },
            },
        )

        self.assertEqual(count, 8831)

    def test_evaluate_smoke_record_completion_accepts_results_ready_nonterminal(self) -> None:
        ready, completion_state = _evaluate_smoke_record_completion(
            {"status": "running", "stage": "acquiring"},
            {
                "job": {"status": "running", "stage": "acquiring"},
                "manual_review_items": [{"candidate_id": "cand-1"}],
                "asset_population": {"available": True, "candidate_count": 40},
                "workflow_stage_summaries": {
                    "summaries": {
                        "stage_1_preview": {"status": "completed", "stage": "stage_1_preview"},
                        "stage_2_final": {"status": "completed", "stage": "stage_2_final"},
                    }
                },
            },
        )

        self.assertTrue(ready)
        self.assertEqual(completion_state, "results_ready_nonterminal")

    def test_run_hosted_smoke_matrix_uses_smoke_ready_instead_of_terminal_status_only(self) -> None:
        with mock.patch(
            "sourcing_agent.workflow_smoke.run_hosted_smoke_case",
            return_value={
                "case": "xai_full_roster",
                "query": "给我 xAI 的所有成员",
                "final": {
                    "job_status": "running",
                    "smoke_ready": True,
                    "smoke_completion_state": "results_ready_nonterminal",
                },
            },
        ):
            summaries, failures = run_hosted_smoke_matrix(
                client=mock.Mock(),
                cases=[{"case": "xai_full_roster", "payload": {"raw_user_request": "给我 xAI 的所有成员"}}],
                reviewer="simulate-smoke",
                poll_seconds=0.1,
                max_poll_seconds=10.0,
            )

        self.assertEqual(len(summaries), 1)
        self.assertFalse(failures)

    def test_build_provider_case_report_exposes_snapshot_full_materialization_queue(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "default_results_mode": "asset_population",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={"job": {"summary": {}}, "asset_population": {"candidate_count": 1112}},
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 1112, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 1112},
            timings_ms={"total": 1000.0},
            materialization_items=[
                {
                    "item_id": "job-openai|20260430T130700|snapshot_full_materialization",
                    "item_kind": "snapshot_full_materialization",
                    "status": "running",
                    "phase": "materializing",
                    "snapshot_id": "20260430T130700",
                    "lease_expires_at": "2026-04-22T10:00:00+00:00",
                },
                {
                    "item_id": "job-openai|20260430T140000|snapshot_full_materialization",
                    "item_kind": "snapshot_full_materialization",
                    "status": "failed_retryable",
                    "phase": "retry_wait",
                    "snapshot_id": "20260430T140000",
                    "last_error": "artifact generation timeout",
                },
                {
                    "item_id": "job-openai|delta|board-visible",
                    "item_kind": "board_visible_delta_apply",
                    "status": "queued",
                },
            ],
        )

        queue = report["service_metrics"]["snapshot_full_materialization_queue"]
        self.assertTrue(queue["report_available"])
        self.assertEqual(queue["item_count"], 2)
        self.assertEqual(queue["backlog_count"], 2)
        self.assertEqual(queue["retryable_count"], 1)
        self.assertEqual(queue["stale_running_count"], 1)
        self.assertEqual(queue["last_error_samples"], ["artifact generation timeout"])
        bottleneck_kinds = [
            item["kind"]
            for item in report["service_metrics"]["bottlenecks"]["top_bottlenecks"]
        ]
        self.assertIn("snapshot_full_materialization_retry_backlog", bottleneck_kinds)
        self.assertIn("snapshot_full_materialization_stale_running", bottleneck_kinds)

    def test_build_provider_case_report_prefers_explicit_board_visible_patches_over_event_fallback(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "default_results_mode": "asset_population",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={
                "job": {"summary": {}},
                "asset_population": {"candidate_count": 572},
            },
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 572, "candidates": [{"candidate_id": "c1"}]},
                "result_view_lifecycle": {
                    "state": "current_snapshot_materializing",
                    "served_candidate_count": 572,
                    "current_snapshot_id": "20260505T183438",
                    "served_snapshot_id": "baseline_delta_overlay",
                    "serving_projection_id": "overlay-1",
                    "serving_projection_phase": "baseline_delta_overlay_serving",
                    "delta_profile_required_count": 272,
                    "delta_profile_fetched_count": 272,
                    "delta_profile_materialized_count": 272,
                    "delta_profile_board_visible_count": 123,
                },
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 572},
            timings_ms={"total": 1000.0},
            job_events=[
                {
                    "created_at": "2026-05-05T10:34:46+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "board_visible_delta_applied",
                        "board_visible_patch": {
                            "sequence_index": 1,
                            "candidate_ids": ["c1"],
                            "cumulative_candidate_count": 1,
                            "served_candidate_count": 301,
                        },
                    },
                },
                {
                    "created_at": "2026-05-05T10:34:49+00:00",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "board_visible_delta_applied",
                        "board_visible_patch": {
                            "sequence_index": 3,
                            "candidate_ids": ["c3"],
                            "cumulative_candidate_count": 3,
                            "served_candidate_count": 303,
                        },
                    },
                },
            ],
            board_visible_patches=[
                {
                    "sequence_index": 1,
                    "candidate_ids": ["c1"],
                    "cumulative_candidate_count": 1,
                    "served_candidate_count": 301,
                    "patch_phase": "board_visible_delta_applied",
                },
                {
                    "sequence_index": 2,
                    "candidate_ids": ["c2"],
                    "cumulative_candidate_count": 2,
                    "served_candidate_count": 302,
                    "patch_phase": "board_visible_delta_applied",
                },
                {
                    "sequence_index": 3,
                    "candidate_ids": ["c3"],
                    "cumulative_candidate_count": 3,
                    "served_candidate_count": 303,
                    "patch_phase": "board_visible_delta_applied",
                },
            ],
        )

        projection = report["service_metrics"]["board_visible_projection"]
        self.assertEqual(projection["patch_sequence_values"], [1, 2, 3])
        self.assertTrue(projection["patch_sequence_contiguous"])
        self.assertEqual(projection["patch_log_count"], 3)

    def test_build_provider_case_report_exposes_target_public_web_batches(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "Example AI",
                "effective_acquisition_mode": "full_local_asset_reuse",
                "default_results_mode": "asset_population",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={"job": {"summary": {}}, "asset_population": {"candidate_count": 3}},
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 3, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 3, "total_candidates": 3},
            timings_ms={"total": 1000.0},
            target_candidate_public_web_batches=[
                {
                    "batch_id": "public-web-batch-live",
                    "status": "completed",
                    "updated_at": "2026-05-04T10:00:00+00:00",
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": "crm_public_web_v1",
                    "summary": {
                        "phase_metrics": {
                            "run_count": 2,
                            "metric_run_count": 1,
                            "completed_without_materialized_signals_count": 1,
                            "missing_phase_metric_count": 1,
                            "service_guardrail_violation_detected": True,
                        }
                    },
                }
            ],
        )

        public_web = report["service_metrics"]["target_candidate_public_web"]
        self.assertTrue(public_web["report_available"])
        self.assertEqual(public_web["batch_count"], 1)
        self.assertEqual(public_web["crm_storage_owner_batch_count"], 1)
        self.assertEqual(public_web["legacy_storage_owner_batch_count"], 0)
        self.assertEqual(public_web["execution_backend_counts"]["crm_public_web_v1"], 1)
        self.assertFalse(public_web["execution_backend_bridge_present"])
        self.assertEqual(public_web["execution_backend_bridge_count"], 0)
        self.assertEqual(public_web["run_count"], 2)
        self.assertEqual(public_web["completed_without_materialized_signals_count"], 1)
        self.assertEqual(public_web["missing_phase_metric_count"], 1)
        self.assertTrue(public_web["service_guardrail_violation_detected"])
        self.assertIn(
            "target_public_web_guardrail_violation",
            {item["kind"] for item in report["service_metrics"]["bottlenecks"]["top_bottlenecks"]},
        )

    def test_public_web_batches_for_smoke_window_filters_stale_batches(self) -> None:
        batches = _public_web_batches_for_smoke_window(
            [
                {
                    "batch_id": "stale-public-web-batch",
                    "updated_at": "2026-05-04T09:59:59+00:00",
                    "summary": {"phase_metrics": {"service_guardrail_violation_detected": True}},
                },
                {
                    "batch_id": "fresh-public-web-batch",
                    "updated_at": "2026-05-04T10:00:01+00:00",
                    "summary": {"phase_metrics": {"service_guardrail_violation_detected": False}},
                },
            ],
            started_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual([batch["batch_id"] for batch in batches], ["fresh-public-web-batch"])

    def test_build_provider_case_report_exposes_search_seed_discovery_queue(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "default_results_mode": "asset_population",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={"job": {"summary": {}}, "asset_population": {"candidate_count": 1112}},
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 1112, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 1112},
            timings_ms={"total": 1000.0},
            materialization_items=[
                {
                    "item_id": "search-seed-ownerless",
                    "item_kind": "search_seed_discovery_query",
                    "status": "running",
                    "phase": "provider_owned",
                    "updated_at": "2000-01-01 00:00:00",
                    "metadata": {
                        "query": "OpenAI Agent",
                        "employment_status": "current",
                        "provider_name": "dataforseo_google_organic",
                    },
                },
                {
                    "item_id": "search-seed-retry",
                    "item_kind": "search_seed_discovery_query",
                    "status": "failed_retryable",
                    "phase": "retry_wait",
                    "not_before_at": "2000-01-01 00:00:00",
                    "metadata": {
                        "query": "OpenAI ChatGPT",
                        "employment_status": "former",
                        "provider_name": "harvest_profile_search",
                    },
                },
                {
                    "item_id": "search-seed-exhausted",
                    "item_kind": "search_seed_discovery_query",
                    "status": "exhausted",
                    "phase": "exhausted",
                    "metadata": {"query": "OpenAI Health", "provider_name": "harvest_profile_search"},
                },
            ],
        )

        queue = report["service_metrics"]["search_seed_discovery_queue"]
        self.assertTrue(queue["report_available"])
        self.assertEqual(queue["item_count"], 3)
        self.assertEqual(queue["provider_owned_count"], 1)
        self.assertEqual(queue["ready_retry_count"], 1)
        self.assertEqual(queue["stale_provider_owned_count"], 1)
        self.assertEqual(queue["item_without_worker_owner_count"], 1)
        self.assertEqual(queue["exhausted_without_provider_retry_count"], 1)
        bottleneck_kinds = [
            item["kind"]
            for item in report["service_metrics"]["bottlenecks"]["top_bottlenecks"]
        ]
        self.assertIn("search_seed_discovery_owner_missing", bottleneck_kinds)
        self.assertIn("search_seed_discovery_retry_backlog", bottleneck_kinds)
        self.assertIn("search_seed_discovery_stale_provider_owned", bottleneck_kinds)
        self.assertIn("search_seed_discovery_exhausted_without_report", bottleneck_kinds)

    def test_build_provider_case_report_exposes_provider_roster_profile_and_board_metrics(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "analysis_stage_mode": "two_stage",
                "default_results_mode": "asset_population",
                "dispatch_strategy": "reuse_snapshot",
                "planner_mode": "baseline_reuse_delta",
                "runtime_tuning_profile": "fast_smoke",
            },
            job_summary={
                "latest_metrics": {
                    "query_count": 3,
                    "queued_query_count": 1,
                    "observed_company_candidate_count": 320,
                    "streaming_materialization": {
                        "provider_response_count": 2,
                        "profile_url_count": 80,
                        "pending_delta_count": 3,
                        "provider_response_to_first_materialization_ms": 450.0,
                        "runtime_tuning": {
                            "materialization_global_writer_budget": 1,
                            "materialization_coalescing_window_ms": 500,
                        },
                    },
                    "runtime_tuning": {
                        "runtime_tuning_profile": "fast_smoke",
                        "harvest_profile_actor_global_inflight": 2,
                    },
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": "snap-openai",
                        "asset_view": "canonical_merged",
                        "candidate_count": 47,
                    },
                },
                "background_reconcile": {
                    "search_seed": {
                        "status": "completed",
                        "entry_count": 26,
                        "added_entry_count": 7,
                        "queued_query_count": 1,
                        "applied_worker_count": 2,
                    },
                    "harvest_prefetch": {
                        "status": "completed",
                        "applied_worker_count": 1,
                        "provider_limiter": {
                            "limiter_key": "harvest_profile_scraper_actor",
                            "active_count": 2,
                            "budget": 2,
                            "wait_ms": 125.0,
                        },
                        "resume_result": {
                            "profile_completion_result": {
                                "status": "completed",
                                "result": {"fetched_profile_count": 5},
                                "artifact_result": {
                                    "summary": {
                                        "profile_detail_count": 5,
                                        "structured_experience_count": 5,
                                        "structured_education_count": 4,
                                    }
                                },
                            }
                        },
                    },
                },
            },
            results_payload={
                "job": {
                    "summary": {
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": "snap-openai",
                            "asset_view": "canonical_merged",
                            "candidate_count": 47,
                        }
                    }
                },
                "asset_population": {"candidate_count": 47},
                "effective_execution_semantics": {"default_results_mode": "asset_population"},
                "workflow_stage_summaries": {
                    "stage_order": [
                        "linkedin_stage_1",
                        "stage_1_preview",
                        "public_web_stage_2",
                        "stage_2_final",
                    ],
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:00+00:00",
                            "completed_at": "2026-04-22T10:00:02+00:00",
                            "candidate_count": 320,
                        },
                        "stage_1_preview": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:02+00:00",
                            "completed_at": "2026-04-22T10:00:03+00:00",
                            "returned_matches": 47,
                            "manual_review_queue_count": 5,
                        },
                        "public_web_stage_2": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:03+00:00",
                            "completed_at": "2026-04-22T10:00:04+00:00",
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:04+00:00",
                            "completed_at": "2026-04-22T10:00:05+00:00",
                            "candidate_source": {"candidate_count": 47},
                            "manual_review_queue_count": 5,
                        },
                    },
                },
            },
            dashboard_payload={
                "asset_population": {
                    "available": True,
                    "candidate_count": 47,
                    "profile_fetch_progress": {
                        "total_url_count": 9,
                        "fetched_url_count": 5,
                        "queued_url_count": 2,
                    },
                    "candidates": [{"candidate_id": "cand-1"}, {"candidate_id": "cand-2"}],
                },
                "ranked_result_count": 0,
                "results": [],
            },
            candidate_page_payload={
                "result_mode": "asset_population",
                "returned_count": 24,
                "total_candidates": 47,
                "has_more": True,
                "result_view_lifecycle": {
                    "phase": "current_snapshot_materializing",
                    "serving_projection_id": "/tmp/openai.partial.asset_population.json",
                    "serving_projection_phase": "partial_delta_overlay",
                    "served_candidate_count": 47,
                    "delta_profile_required_count": 9,
                    "delta_profile_fetched_count": 5,
                    "delta_profile_materialized_count": 2,
                    "delta_profile_board_visible_count": 2,
                },
            },
            timings_ms={
                "dashboard_fetch": 12.34,
                "candidate_page_fetch": 45.67,
                "total": 1234.56,
            },
            progress_observability={
                "sample_count": 5,
                "maxima": {"waiting_remote_harvest_count": 2, "queued_worker_count": 3},
                "counter_regressions": {},
                "regression_detected": False,
            },
            provider_invocations=[
                {
                    "provider_name": "scripted_harvest",
                    "dispatch_kind": "harvest.execute",
                    "dispatch_signature": "sig-harvest-1",
                    "logical_name": "harvest_company_employees",
                    "payload_hash": "payload-1",
                },
                {
                    "provider_name": "scripted_search",
                    "dispatch_kind": "search.batch_submit",
                    "dispatch_signature": "sig-search-1",
                    "query_text": "site:linkedin.com/in/ openai reasoning",
                    "task_key": "openai_reasoning_current",
                    "payload_hash": "payload-2",
                },
            ],
            job_events=[
                {
                    "created_at": "2026-04-22T10:00:02+00:00",
                    "stage": "remote_provider_event",
                    "status": "received",
                    "detail": "Received remote provider event for run run-1.",
                    "payload": {
                        "target_worker_ids": [701],
                        "event_metrics": {
                            "remote_completed_at": "2026-04-22T10:00:01+00:00",
                            "local_event_seen_at": "2026-04-22T10:00:02+00:00",
                            "remote_to_local_event_lag_ms": 1000,
                        },
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:02+00:00",
                    "stage": "acquiring",
                    "status": "running",
                    "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                    "payload": {
                        "worker_ids": [701],
                        "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                        "event_metrics": {
                            "local_event_apply_started_at": "2026-04-22T10:00:02+00:00",
                            "next_submit_attempt_started_at": "2026-04-22T10:00:02.050000+00:00",
                            "next_submit_attempt_finished_at": "2026-04-22T10:00:02.200000+00:00",
                            "post_ingest_prefetch_elapsed_ms": 150,
                            "post_ingest_prefetch_dispatched_url_count": 2,
                        },
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:06+00:00",
                    "stage": "completed",
                    "status": "running",
                    "detail": "Background snapshot materialization reconcile started candidate artifact sync.",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "materialize_started",
                        "reconcile_kind": "snapshot_materialization",
                        "snapshot_id": "snap-openai",
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:24+00:00",
                    "stage": "completed",
                    "status": "completed",
                    "detail": "Background snapshot materialization reconcile completed candidate artifact sync.",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "materialize_completed",
                        "reconcile_kind": "snapshot_materialization",
                        "snapshot_id": "snap-openai",
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:24.500000+00:00",
                    "stage": "acquiring",
                    "status": "running",
                    "detail": "Partial delta overlay made fetched profiles visible on the board.",
                    "payload": {
                        "event_family": "workflow_materialization",
                        "phase": "board_visible_delta_applied",
                        "worker_kind": "harvest_prefetch",
                        "snapshot_id": "snap-openai",
                        "worker_ids": [701],
                        "board_visible_patch": {
                            "patch_id": "job-openai|snap-openai|cand-1,cand-2",
                            "sequence_index": 1,
                            "candidate_ids": ["cand-1", "cand-2"],
                            "cumulative_candidate_ids": ["cand-1", "cand-2"],
                            "cumulative_candidate_count": 2,
                            "served_candidate_count": 47,
                            "overlay_path": "/tmp/openai.partial.asset_population.json",
                        },
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:25+00:00",
                    "stage": "completed",
                    "status": "running",
                    "detail": "Background harvest profile prefetch started candidate artifact sync.",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "materialize_started",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-openai",
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:34+00:00",
                    "stage": "completed",
                    "status": "completed",
                    "detail": "Background harvest profile prefetch completed candidate artifact sync.",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "materialize_completed",
                        "reconcile_kind": "harvest_prefetch",
                        "snapshot_id": "snap-openai",
                    },
                },
                {
                    "created_at": "2026-04-22T10:00:35+00:00",
                    "stage": "completed",
                    "status": "completed",
                    "detail": "Background outreach layering reconcile completed.",
                    "payload": {
                        "event_family": "completed_workflow_reconcile",
                        "phase": "completed",
                        "reconcile_kind": "outreach_layering",
                        "snapshot_id": "snap-openai",
                    },
                },
            ],
            agent_workers=[
                {
                    "worker_id": 701,
                    "span_id": 9001,
                    "status": "completed",
                    "lane_id": "search_seed",
                    "worker_key": "agent-current-probe",
                    "metadata": {"recovery_kind": "search_seed"},
                },
                {
                    "worker_id": 702,
                    "span_id": 9002,
                    "status": "running",
                    "lane_id": "enrichment_specialist",
                    "worker_key": "profile-batch-1",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "checkpoint": {
                        "stage": "waiting_remote_harvest",
                        "run_id": "run-active",
                        "provider_limiter_lease": {
                            "limiter_key": "harvest_profile_scraper_actor",
                            "lease_token": "lease-active",
                            "budget": 2,
                            "active_count": 1,
                        },
                    },
                },
                {
                    "worker_id": 703,
                    "span_id": 9003,
                    "status": "completed",
                    "lane_id": "enrichment_specialist",
                    "worker_key": "profile-batch-applied-not-ingested",
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                    "output": {
                        "inline_incremental_apply": {
                            "worker_kind": "harvest_prefetch",
                            "snapshot_id": "snap-openai",
                            "applied_at": "2000-01-01 00:00:00",
                            "apply_status": "applied",
                        }
                    },
                },
            ],
            agent_trace_spans=[
                {
                    "span_id": 9001,
                    "lane_id": "search_seed",
                    "started_at": "2026-04-22T10:00:00+00:00",
                    "completed_at": "2026-04-22T10:00:02+00:00",
                },
                {
                    "span_id": 9002,
                    "lane_id": "enrichment_specialist",
                    "started_at": "2026-04-22T10:00:08+00:00",
                    "completed_at": "2026-04-22T10:00:43+00:00",
                },
                {
                    "span_id": 9003,
                    "lane_id": "enrichment_specialist",
                    "started_at": "2026-04-22T10:00:44+00:00",
                    "completed_at": "2026-04-22T10:00:45+00:00",
                },
            ],
            materialization_items=[
                {
                    "item_id": "job-openai|snap-openai|snapshot_full_materialization",
                    "job_id": "job-openai",
                    "item_kind": "snapshot_full_materialization",
                    "status": "failed_retryable",
                    "phase": "retry_wait",
                    "snapshot_id": "snap-openai",
                    "updated_at": "2026-04-22T10:00:36+00:00",
                    "last_error": "artifact summary unavailable",
                }
            ],
            worker_recovery_runs=[
                {
                    "phase": "poll_auto_recovery",
                    "status": "completed",
                    "daemon_status": "completed",
                    "recovery_phase_metrics": {
                        "search_seed_discovery": {
                            "phase": "search_seed_discovery",
                            "owner": "search_seed_discovery_query_queue",
                            "max_sync_work": "no search-seed discovery work",
                            "elapsed_ms": 0,
                            "status": "skipped",
                            "reason": "search_seed_discovery_disabled_by_payload",
                            "counts": {},
                        },
                        "worker_recovery": {
                            "phase": "worker_recovery",
                            "owner": "worker_recovery_daemon",
                            "max_sync_work": "recover selected workers and invoke completion callbacks serially",
                            "elapsed_ms": 1200,
                            "status": "completed",
                            "reason": "",
                            "counts": {"executed_count": 1},
                        },
                    },
                }
            ],
        )

        self.assertEqual(report["execution"]["target_company"], "OpenAI")
        self.assertEqual(report["search"]["query_count"], 3)
        self.assertEqual(report["search"]["search_seed_added_entry_count"], 7)
        self.assertEqual(report["roster"]["returned_count"], 47)
        self.assertEqual(report["profile_completion"]["fetched_profile_count"], 5)
        self.assertTrue(report["board"]["ready"])
        self.assertTrue(report["board"]["ready_nonempty"])
        self.assertEqual(report["board"]["candidate_page_total_candidates"], 47)
        self.assertEqual(report["board"]["profile_fetch_progress"]["queued_url_count"], 2)
        self.assertEqual(report["counts"]["stage_1_preview_candidate_count"], 47)
        self.assertEqual(report["stage_wall_clock_ms"]["linkedin_stage_1"], 2000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_stage_1_preview"], 3000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_final_results"], 5000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["stage_1_preview_to_final_results"], 32000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_board_nonempty"], 5000.0)
        self.assertEqual(report["timings_ms"]["candidate_page_fetch"], 45.67)
        self.assertEqual(report["progress_observability"]["maxima"]["queued_worker_count"], 3)
        self.assertEqual(report["materialization_streaming"]["provider_response_count"], 2)
        self.assertEqual(report["materialization_streaming"]["pending_delta_count"], 3)
        self.assertEqual(
            report["materialization_streaming"]["budget"]["recommended_action"],
            "coalesce_window",
        )
        post_preview_finalization = report["post_preview_finalization"]
        self.assertTrue(post_preview_finalization["report_available"])
        self.assertEqual(post_preview_finalization["preview_to_first_materialize_start_ms"], 3000.0)
        self.assertEqual(post_preview_finalization["preview_to_last_materialize_completed_ms"], 31000.0)
        self.assertEqual(post_preview_finalization["preview_to_finalization_completed_ms"], 32000.0)
        self.assertEqual(post_preview_finalization["materialize_completed_count"], 2)
        self.assertTrue(post_preview_finalization["long_post_preview_finalization"])
        self.assertEqual(post_preview_finalization["materialize_sync_duration_ms"]["max"], 18000.0)
        self.assertTrue(report["provider_backpressure"]["limiter_exhausted"])
        self.assertEqual(report["provider_backpressure"]["recommended_action"], "throttle_provider_submit")
        remote_actor_slot_observation = report["remote_actor_slot_observation"]
        self.assertTrue(remote_actor_slot_observation["report_available"])
        self.assertEqual(remote_actor_slot_observation["harvest_profile_actor_global_inflight"], 2)
        self.assertEqual(remote_actor_slot_observation["peak_waiting_remote_harvest_count"], 2)
        self.assertEqual(remote_actor_slot_observation["remote_actor_slot_peak_occupancy_ratio"], 1.0)
        self.assertEqual(report["workflow_benchmark"]["search_returned_count"], 26)
        self.assertEqual(report["workflow_benchmark"]["roster_returned_count"], 47)
        self.assertEqual(report["workflow_benchmark"]["fetched_profile_count"], 5)
        self.assertEqual(report["workflow_benchmark"]["profile_url_queued_count"], 2)
        self.assertTrue(report["workflow_benchmark"]["provider_backpressure_detected"])
        self.assertEqual(report["event_level_efficiency"]["remote_to_local_event_lag_ms"]["max"], 1000)
        self.assertEqual(report["event_level_efficiency"]["remote_to_local_marker_lag_ms"]["max"], 1000)
        self.assertEqual(report["event_level_efficiency"]["local_to_next_submit_start_ms"]["max"], 50)
        self.assertEqual(
            report["event_level_efficiency"]["provider_slots"]["true_active_provider_slot_worker_count"],
            1,
        )
        service_metrics = report["service_metrics"]
        self.assertTrue(service_metrics["report_available"])
        remote_events = service_metrics["remote_provider_events"]
        self.assertTrue(remote_events["report_available"])
        self.assertEqual(remote_events["event_count"], 1)
        self.assertEqual(remote_events["received_count"], 1)
        self.assertEqual(remote_events["remote_to_local_event_lag_ms"]["max"], 1000.0)
        self.assertFalse(remote_events["lag_violation"])
        self.assertEqual(service_metrics["worker_timeline"]["worker_count"], 3)
        self.assertEqual(service_metrics["worker_timeline"]["trace_span_count"], 3)
        self.assertEqual(service_metrics["worker_timeline"]["duration_ms"]["max"], 35000.0)
        self.assertEqual(
            service_metrics["worker_timeline"]["handoff_gap_ms"]["global_next_worker_start_gap_ms"]["max"],
            7000.0,
        )
        board_visible_projection = service_metrics["board_visible_projection"]
        self.assertTrue(board_visible_projection["report_available"])
        self.assertEqual(board_visible_projection["patch_log_count"], 1)
        self.assertEqual(board_visible_projection["patch_log_latest_cumulative_count"], 2)
        self.assertEqual(board_visible_projection["fetched_to_board_visible_lag_count"], 3)
        self.assertFalse(board_visible_projection["projection_missing_for_visible_count"])
        self.assertFalse(board_visible_projection["patch_log_missing_for_visible_count"])
        local_apply_backlog = service_metrics["local_apply_backlog"]
        self.assertTrue(local_apply_backlog["report_available"])
        self.assertEqual(local_apply_backlog["applied_not_ingested_count"], 1)
        self.assertEqual(local_apply_backlog["stale_applied_not_ingested_count"], 1)
        self.assertTrue(local_apply_backlog["stale_applied_not_ingested_present"])
        snapshot_queue = service_metrics["snapshot_full_materialization_queue"]
        self.assertTrue(snapshot_queue["report_available"])
        self.assertEqual(snapshot_queue["item_count"], 1)
        self.assertEqual(snapshot_queue["backlog_count"], 1)
        self.assertEqual(snapshot_queue["retryable_count"], 1)
        self.assertTrue(snapshot_queue["retry_backlog_present"])
        recovery_phase_metrics = service_metrics["recovery_phase_metrics"]
        self.assertTrue(recovery_phase_metrics["report_available"])
        self.assertEqual(recovery_phase_metrics["run_count"], 1)
        self.assertGreaterEqual(recovery_phase_metrics["missing_phase_count"], 1)
        self.assertEqual(recovery_phase_metrics["phase_elapsed_ms_max"]["worker_recovery"], 1200.0)
        self.assertEqual(service_metrics["worker_timeline"]["slow_workers"][0]["worker_id"], 702)
        bottleneck_kinds = {item["kind"] for item in service_metrics["bottlenecks"]["top_bottlenecks"]}
        self.assertIn("slow_worker", bottleneck_kinds)
        self.assertIn("local_apply_backlog", bottleneck_kinds)
        self.assertFalse(report["behavior_guardrails"]["duplicate_provider_dispatch"]["violation_detected"])
        self.assertFalse(report["behavior_guardrails"]["disabled_stage_violations"]["violation_detected"])
        self.assertFalse(report["behavior_guardrails"]["streaming_materialization"]["violation_detected"])
        self.assertFalse(report["behavior_guardrails"]["event_level_efficiency"]["violation_detected"])

    def test_build_provider_case_report_flags_duplicate_dispatch_and_unexpected_public_web_stage(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "analysis_stage_mode": "single_stage",
                "default_results_mode": "asset_population",
                "dispatch_strategy": "delta_from_snapshot",
                "planner_mode": "baseline_reuse_delta",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={
                "job": {"summary": {}},
                "asset_population": {"candidate_count": 47},
                "workflow_stage_summaries": {
                    "stage_order": [
                        "linkedin_stage_1",
                        "stage_1_preview",
                        "public_web_stage_2",
                        "stage_2_final",
                    ],
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:00+00:00",
                            "completed_at": "2026-04-22T10:00:02+00:00",
                        },
                        "stage_1_preview": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:02+00:00",
                            "completed_at": "2026-04-22T10:00:03+00:00",
                            "returned_matches": 47,
                        },
                        "public_web_stage_2": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:03+00:00",
                            "completed_at": "2026-04-22T10:00:04+00:00",
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:09+00:00",
                            "completed_at": "2026-04-22T10:00:10+00:00",
                            "candidate_source": {"candidate_count": 47},
                        },
                    },
                },
            },
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 47, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 47},
            timings_ms={"total": 1000.0},
            progress_observability={},
            provider_invocations=[
                {
                    "provider_name": "scripted_harvest",
                    "dispatch_kind": "harvest.execute",
                    "dispatch_signature": "dup-1",
                    "logical_name": "harvest_company_employees",
                    "payload_hash": "payload-1",
                },
                {
                    "provider_name": "scripted_harvest",
                    "dispatch_kind": "harvest.execute",
                    "dispatch_signature": "dup-1",
                    "logical_name": "harvest_company_employees",
                    "payload_hash": "payload-1",
                },
            ],
        )

        behavior = dict(report.get("behavior_guardrails") or {})
        duplicate_report = dict(behavior.get("duplicate_provider_dispatch") or {})
        disabled_stage_report = dict(behavior.get("disabled_stage_violations") or {})
        prerequisite_gaps = dict(behavior.get("prerequisite_gaps") or {})
        self.assertTrue(duplicate_report.get("violation_detected"))
        self.assertEqual(duplicate_report.get("duplicate_signature_count"), 1)
        self.assertEqual(duplicate_report.get("redundant_dispatch_count"), 1)
        self.assertTrue(disabled_stage_report.get("violation_detected"))
        self.assertTrue(disabled_stage_report.get("unexpected_public_web_stage"))
        self.assertTrue(prerequisite_gaps.get("violation_detected"))
        self.assertEqual(
            dict(prerequisite_gaps.get("large_gap_metrics") or {}).get("stage_1_preview_to_stage_2_final_start"),
            6000.0,
        )

    def test_prerequisite_gap_is_diagnostic_not_hard_behavior_guardrail(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "baseline_reuse_with_delta",
                "analysis_stage_mode": "single_stage",
                "default_results_mode": "asset_population",
                "dispatch_strategy": "delta_from_snapshot",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={
                "job": {"summary": {}},
                "asset_population": {"candidate_count": 47},
                "workflow_stage_summaries": {
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:00+00:00",
                            "completed_at": "2026-04-22T10:00:01+00:00",
                        },
                        "stage_1_preview": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:01+00:00",
                            "completed_at": "2026-04-22T10:00:02+00:00",
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:12+00:00",
                            "completed_at": "2026-04-22T10:00:13+00:00",
                            "candidate_source": {"candidate_count": 47},
                        },
                    },
                },
            },
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 47, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 47},
            timings_ms={"total": 1000.0},
            progress_observability={},
            provider_invocations=[],
        )

        behavior = dict(report.get("behavior_guardrails") or {})
        self.assertFalse(behavior.get("violation_detected"))
        self.assertEqual(behavior.get("violation_count"), 0)
        self.assertTrue(behavior.get("diagnostic_violation_detected"))
        self.assertEqual(behavior.get("diagnostic_violation_count"), 1)

    def test_build_provider_case_report_ignores_implausibly_stale_stage_wall_clock(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "xAI",
                "effective_acquisition_mode": "full_local_asset_reuse",
                "default_results_mode": "asset_population",
                "dispatch_strategy": "reuse_completed",
                "planner_mode": "reuse_snapshot_only",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={
                "job": {"summary": {}},
                "asset_population": {"candidate_count": 2941},
                "workflow_stage_summaries": {
                    "stage_order": ["stage_2_final"],
                    "summaries": {
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-04-16T00:00:00+00:00",
                            "completed_at": "2026-04-22T00:00:00+00:00",
                            "candidate_source": {"candidate_count": 2941},
                        }
                    },
                },
            },
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 2941, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 2941},
            timings_ms={"total": 1234.0},
            progress_observability={},
        )

        self.assertNotIn("stage_2_final", report["stage_wall_clock_ms"])

    def test_build_provider_case_report_prefers_client_timeline_for_user_visible_latency(self) -> None:
        report = _build_provider_case_report(
            explain_payload={
                "target_company": "OpenAI",
                "effective_acquisition_mode": "full_local_asset_reuse",
                "default_results_mode": "asset_population",
                "dispatch_strategy": "reuse_snapshot",
                "planner_mode": "reuse_snapshot_only",
            },
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload={
                "job": {"summary": {}},
                "asset_population": {"candidate_count": 47},
                "workflow_stage_summaries": {
                    "stage_order": [
                        "linkedin_stage_1",
                        "stage_1_preview",
                        "public_web_stage_2",
                        "stage_2_final",
                    ],
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:00+00:00",
                            "completed_at": "2026-04-22T10:00:02+00:00",
                        },
                        "stage_1_preview": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:02+00:00",
                            "completed_at": "2026-04-22T10:00:03+00:00",
                            "returned_matches": 47,
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "started_at": "2026-04-22T10:00:04+00:00",
                            "completed_at": "2026-04-22T10:00:05+00:00",
                            "candidate_source": {"candidate_count": 47},
                        },
                    },
                },
            },
            dashboard_payload={
                "asset_population": {"available": True, "candidate_count": 47, "candidates": [{"candidate_id": "c1"}]},
                "results": [],
            },
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 47},
            timings_ms={"total": 1000.0},
            progress_observability={},
            timeline=[
                {
                    "message": "Stage 1 preview ready",
                    "stage": "acquiring",
                    "status": "completed",
                    "observed_at_ms": 9000.0,
                },
                {
                    "message": "Local asset population is ready",
                    "stage": "completed",
                    "status": "completed",
                    "observed_at_ms": 12000.0,
                },
            ],
        )

        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_stage_1_preview"], 9000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_final_results"], 12000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["stage_1_preview_to_final_results"], 2000.0)

    def test_smoke_report_refetches_runtime_results_details_for_wall_clock_anchors(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.paths: list[str] = []

            def get(self, path: str, timeout: float = 120.0) -> dict:
                del timeout
                self.paths.append(path)
                return {
                    "job": {"summary": {}},
                    "asset_population": {"candidate_count": 47, "available": True},
                    "workflow_stage_summaries": {
                        "stage_order": ["linkedin_stage_1", "stage_1_preview", "stage_2_final"],
                        "summaries": {
                            "linkedin_stage_1": {
                                "status": "completed",
                                "started_at": "2026-04-22T10:00:00+00:00",
                                "completed_at": "2026-04-22T10:00:02+00:00",
                            },
                            "stage_1_preview": {
                                "status": "completed",
                                "started_at": "2026-04-22T10:00:02+00:00",
                                "completed_at": "2026-04-22T10:00:05+00:00",
                                "returned_matches": 47,
                            },
                            "stage_2_final": {
                                "status": "completed",
                                "started_at": "2026-04-22T10:00:05+00:00",
                                "completed_at": "2026-04-22T10:00:08+00:00",
                                "candidate_source": {"candidate_count": 47},
                            },
                        },
                    },
                }

        timings_ms: dict[str, float] = {}
        client = FakeClient()
        refreshed = _ensure_results_runtime_details_for_smoke_report(
            client,
            job_id="job-openai",
            results_payload={"job": {"summary": {}}, "asset_population": {"candidate_count": 47}},
            timings_ms=timings_ms,
        )
        report = _build_provider_case_report(
            explain_payload={"target_company": "OpenAI", "default_results_mode": "asset_population"},
            job_summary={"latest_metrics": {}, "background_reconcile": {}},
            results_payload=refreshed,
            dashboard_payload={"asset_population": {"available": True, "candidate_count": 47}},
            candidate_page_payload={"result_mode": "asset_population", "returned_count": 24, "total_candidates": 47},
            timings_ms={"total": 1000.0},
            progress_observability={},
        )

        self.assertEqual(
            client.paths,
            ["/api/jobs/job-openai/results?include_runtime_details=1&include_candidates=0"],
        )
        self.assertGreaterEqual(timings_ms["fetch_runtime_results_details"], 0.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_stage_1_preview"], 5000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_final_results"], 8000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["stage_1_preview_to_final_results"], 3000.0)

    def test_smoke_report_keeps_existing_runtime_results_details_without_refetch(self) -> None:
        class FakeClient:
            def get(self, path: str, timeout: float = 120.0) -> dict:
                del path, timeout
                raise AssertionError("runtime details should not be refetched")

        timings_ms: dict[str, float] = {}
        payload = {
            "workflow_stage_summaries": {
                "summaries": {
                    "stage_1_preview": {
                        "started_at": "2026-04-22T10:00:00+00:00",
                        "completed_at": "2026-04-22T10:00:01+00:00",
                    },
                    "stage_2_final": {
                        "started_at": "2026-04-22T10:00:01+00:00",
                        "completed_at": "2026-04-22T10:00:02+00:00",
                    },
                }
            }
        }

        refreshed = _ensure_results_runtime_details_for_smoke_report(
            FakeClient(),
            job_id="job-openai",
            results_payload=payload,
            timings_ms=timings_ms,
        )

        self.assertIs(refreshed, payload)
        self.assertEqual(timings_ms["fetch_runtime_results_details"], 0.0)

    def test_build_progress_observability_report_surfaces_counter_regressions(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "counters": {
                        "result_count": 12,
                        "manual_review_count": 4,
                        "observed_company_candidate_count": 180,
                        "waiting_remote_harvest_count": 2,
                    },
                    "runtime_health": {"pending_worker_count": 3},
                },
                {
                    "counters": {
                        "result_count": 0,
                        "manual_review_count": 0,
                        "observed_company_candidate_count": 0,
                    },
                    "runtime_health": {"pending_worker_count": 1},
                },
            ]
        )

        self.assertTrue(report["regression_detected"])
        self.assertEqual(report["counter_regressions"]["result_count"]["largest_drop"], 12)
        self.assertNotIn("manual_review_count", report["counter_regressions"])
        self.assertEqual(report["backlog_reductions"]["manual_review_count"]["largest_reduction"], 4)
        self.assertEqual(report["maxima"]["waiting_remote_harvest_count"], 2)
        self.assertEqual(report["maxima"]["pending_worker_count"], 3)

    def test_build_progress_observability_report_allows_canonical_visible_count_normalization(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "counters": {"result_count": 8831},
                    "result_view_lifecycle": {
                        "served_candidate_count": 8831,
                        "expected_candidate_count": 8831,
                    },
                },
                {
                    "counters": {"result_count": 8830},
                    "result_view_lifecycle": {
                        "served_candidate_count": 8830,
                        "expected_candidate_count": 8830,
                        "metadata": {
                            "canonical_projection_public_count_normalization": {
                                "source": "serving_projection_members",
                                "visible_member_count": 8830,
                                "raw_expected_candidate_count": 8831,
                                "raw_served_candidate_count": 8831,
                            }
                        },
                    },
                },
            ]
        )

        self.assertFalse(report["regression_detected"])
        self.assertFalse(report["result_view_lifecycle_regression_detected"])
        self.assertEqual(report["canonical_projection_public_count_normalization_count"], 1)

    def test_build_progress_observability_report_rejects_unexplained_visible_count_drop(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "counters": {"result_count": 8831},
                    "result_view_lifecycle": {"served_candidate_count": 8831},
                },
                {
                    "counters": {"result_count": 7820},
                    "result_view_lifecycle": {
                        "served_candidate_count": 7820,
                        "metadata": {
                            "canonical_projection_public_count_normalization": {
                                "source": "serving_projection_members",
                                "visible_member_count": 8830,
                                "raw_expected_candidate_count": 8831,
                                "raw_served_candidate_count": 8831,
                            }
                        },
                    },
                },
            ]
        )

        self.assertTrue(report["regression_detected"])
        self.assertEqual(report["counter_regressions"]["result_count"]["largest_drop"], 1011)
        self.assertTrue(report["result_view_lifecycle_regression_detected"])
        self.assertEqual(
            report["result_view_lifecycle_regressions"]["served_candidate_count"]["largest_drop"],
            1011,
        )

    def test_build_progress_observability_report_tracks_payload_size_budget(self) -> None:
        report = _build_progress_observability_report(
            [
                {"tick": 0, "payload_bytes": 42},
                {"tick": 1, "payload_bytes": 125},
            ]
        )

        self.assertEqual(report["max_payload_bytes"], 125)
        self.assertEqual(report["payload_bytes"]["count"], 2)
        self.assertEqual(report["payload_bytes"]["max"], 125)

    def test_build_progress_observability_report_surfaces_stage1_lifecycle_contract_violations(self) -> None:
        report = _build_progress_observability_report(
            [
                {
                    "tick": 0,
                    "linkedin_stage_1_progress": {
                        "current_search_returned_count": 83,
                        "former_search_returned_count": 10,
                        "deduped_candidate_count": 93,
                        "deduped_profile_url_count": 89,
                        "profile_fetch_required_count": 89,
                        "profile_fetched_count": 54,
                    },
                    "result_view_lifecycle": {
                        "baseline_snapshot_id": "baseline",
                        "current_snapshot_id": "current",
                        "served_snapshot_id": "baseline",
                        "baseline_candidate_count": 1110,
                        "served_candidate_count": 1110,
                        "delta_profile_fetched_count": 54,
                        "delta_profile_materialized_count": 19,
                    },
                    "execution_phase_contract": {
                        "active_phase_label": "LinkedIn Stage 1",
                        "public_web_stage_applicable": False,
                    },
                },
                {
                    "tick": 1,
                    "linkedin_stage_1_progress": {
                        "current_search_returned_count": 0,
                        "former_search_returned_count": 10,
                        "deduped_candidate_count": 77,
                        "deduped_profile_url_count": 77,
                        "profile_fetch_required_count": 89,
                        "profile_fetched_count": 90,
                    },
                    "result_view_lifecycle": {
                        "baseline_snapshot_id": "baseline",
                        "current_snapshot_id": "current",
                        "served_snapshot_id": "current",
                        "baseline_candidate_count": 1110,
                        "served_candidate_count": 10,
                        "delta_profile_fetched_count": 54,
                        "delta_profile_materialized_count": 89,
                    },
                    "execution_phase_contract": {
                        "active_phase_label": "Public Web Stage 2",
                        "public_web_stage_applicable": False,
                    },
                },
            ]
        )

        self.assertTrue(report["stage1_regression_detected"])
        self.assertEqual(report["stage1_counter_regressions"]["current_search_returned_count"]["largest_drop"], 83)
        self.assertTrue(report["result_view_lifecycle_regression_detected"])
        self.assertEqual(report["result_view_lifecycle_regressions"]["served_candidate_count"]["largest_drop"], 1100)
        self.assertTrue(report["contract_violation_detected"])
        violation_counts = report["contract_violation_counts"]
        self.assertEqual(violation_counts["stage1_profile_required_exceeds_deduped_population"], 1)
        self.assertEqual(violation_counts["stage1_fetched_exceeds_required"], 1)
        self.assertEqual(violation_counts["lifecycle_materialized_exceeds_fetched"], 1)
        self.assertEqual(violation_counts["raw_delta_only_result_view_served"], 1)
        self.assertEqual(violation_counts["public_web_label_when_stage_not_applicable"], 1)

    def test_summarize_smoke_timings_aggregates_progress_contract_violations(self) -> None:
        progress_observability = {
            "sample_count": 2,
            "regression_detected": True,
            "stage1_regression_detected": True,
            "result_view_lifecycle_regression_detected": True,
            "contract_violation_detected": True,
            "contract_violation_counts": {
                "stage1_profile_required_exceeds_deduped_population": 1,
                "raw_delta_only_result_view_served": 1,
            },
        }

        summary = summarize_smoke_timings(
            [
                {
                    "timings_ms": {"total": 1000.0},
                    "provider_case_report": {"progress_observability": progress_observability},
                }
            ]
        )

        provider_summary = dict(summary.get("provider_case_report") or {})
        self.assertEqual(provider_summary["progress_sample_count"], 2)
        self.assertEqual(provider_summary["progress_regression_case_count"], 1)
        self.assertEqual(provider_summary["progress_stage1_regression_case_count"], 1)
        self.assertEqual(provider_summary["progress_lifecycle_regression_case_count"], 1)
        self.assertEqual(provider_summary["progress_contract_violation_case_count"], 1)
        self.assertEqual(
            provider_summary["progress_contract_violation_counts"]["raw_delta_only_result_view_served"],
            1,
        )

    def test_synthetic_terminal_sample_preserves_monotonic_counts(self) -> None:
        progress_samples = [
            {
                "tick": 0,
                "status": "running",
                "stage": "retrieving",
                "counters": {
                    "result_count": 12,
                    "manual_review_count": 4,
                    "observed_company_candidate_count": 180,
                    "event_count": 28,
                    "queued_worker_count": 3,
                },
                "runtime_health": {"pending_worker_count": 1},
            }
        ]
        progress_samples.append(
            _build_synthetic_terminal_progress_sample(
                progress_samples=progress_samples,
                job_payload={"progress": {"counters": {}}},
                raw_job_status="completed",
                raw_job_stage="completed",
            )
        )

        report = _build_progress_observability_report(progress_samples)

        self.assertFalse(report["regression_detected"])
        self.assertEqual(report["latest_counters"]["manual_review_count"], 4)
        self.assertEqual(report["latest_counters"]["result_count"], 12)
        self.assertEqual(report["latest_counters"]["event_count"], 28)
        self.assertNotIn("queued_worker_count", report["latest_counters"])

    def test_manual_review_count_can_drop_when_terminal_payload_reports_cleared_backlog(self) -> None:
        progress_samples = [
            {
                "tick": 0,
                "status": "running",
                "stage": "stage_1_preview",
                "counters": {
                    "result_count": 12,
                    "manual_review_count": 4,
                    "observed_company_candidate_count": 12,
                },
            }
        ]
        progress_samples.append(
            _build_synthetic_terminal_progress_sample(
                progress_samples=progress_samples,
                job_payload={"progress": {"counters": {"manual_review_count": 0, "result_count": 12}}},
                raw_job_status="completed",
                raw_job_stage="completed",
            )
        )

        report = _build_progress_observability_report(progress_samples)

        self.assertFalse(report["regression_detected"])
        self.assertTrue(report["backlog_reduction_detected"])
        self.assertEqual(report["backlog_reductions"]["manual_review_count"]["largest_reduction"], 4)
        self.assertNotIn("manual_review_count", report["latest_counters"])

    def test_build_case_level_smoke_exports_surfaces_stage_digest_and_timings(self) -> None:
        exports = _build_case_level_smoke_exports(
            results_payload={
                "workflow_stage_summaries": {
                    "summaries": {
                        "stage_1_preview": {
                            "status": "completed",
                            "stage": "stage_1_preview",
                            "candidate_count": 47,
                            "manual_review_count": 5,
                        }
                    }
                }
            },
            provider_case_report={
                "stage_wall_clock_ms": {"linkedin_stage_1": 2000.0},
                "workflow_wall_clock_ms": {"job_to_final_results": 5000.0},
                "event_level_efficiency": {
                    "report_available": True,
                    "remote_to_next_submit_start_ms": {"max": 9000.0},
                    "local_to_next_submit_start_ms": {"max": 0.0},
                    "next_submit_attempt_elapsed_ms": {"max": 250.0},
                    "provider_io": {
                        "report_available": True,
                        "actor_run_duration_ms": {"max": 50000.0},
                    },
                    "materialization_io": {
                        "report_available": True,
                        "sync_total_ms": {"max": 2165.0},
                    },
                },
                "service_metrics": {
                    "remote_provider_events": {
                        "report_available": True,
                        "event_count": 2,
                        "late_duplicate_count": 1,
                        "remote_to_local_event_lag_ms": {"max": 207000.0},
                    },
                    "provider_anomalies": {
                        "report_available": True,
                        "zero_result_retry_count": 2,
                    },
                },
            },
        )

        self.assertEqual(
            exports["stage_summary_digest"]["stage_1_preview"],
            {
                "status": "completed",
                "stage": "stage_1_preview",
                "candidate_count": 47,
                "manual_review_count": 5,
            },
        )
        self.assertEqual(exports["stage_wall_clock_ms"]["linkedin_stage_1"], 2000.0)
        self.assertEqual(exports["workflow_wall_clock_ms"]["job_to_final_results"], 5000.0)
        self.assertEqual(
            exports["event_level_efficiency"]["provider_io"]["actor_run_duration_ms"]["max"],
            50000.0,
        )
        self.assertEqual(
            exports["event_level_efficiency"]["materialization_io"]["sync_total_ms"]["max"],
            2165.0,
        )
        self.assertEqual(exports["event_level_efficiency"]["remote_to_next_submit_start_ms"]["max"], 9000.0)
        self.assertEqual(exports["event_level_efficiency"]["next_submit_attempt_elapsed_ms"]["max"], 250.0)
        self.assertEqual(exports["service_metrics"]["remote_provider_events"]["event_count"], 2)
        self.assertEqual(
            exports["service_metrics"]["remote_provider_events"]["remote_to_local_event_lag_ms"]["max"],
            207000.0,
        )
        self.assertEqual(exports["service_metrics"]["provider_anomalies"]["zero_result_retry_count"], 2)

    def test_stage_summary_digest_synthesizes_missing_public_web_stage_from_final_stage(self) -> None:
        digest = stage_summary_digest(
            {
                "workflow_stage_summaries": {
                    "stage_order": [
                        "linkedin_stage_1",
                        "stage_1_preview",
                        "public_web_stage_2",
                        "stage_2_final",
                    ],
                    "summaries": {
                        "linkedin_stage_1": {
                            "status": "completed",
                            "stage": "linkedin_stage_1",
                        },
                        "stage_1_preview": {
                            "status": "completed",
                            "stage": "stage_1_preview",
                        },
                        "stage_2_final": {
                            "status": "completed",
                            "stage": "stage_2_final",
                            "candidate_count": 47,
                            "manual_review_count": 5,
                        },
                    },
                }
            }
        )

        self.assertIn("public_web_stage_2", digest)
        self.assertEqual(
            digest["public_web_stage_2"],
            {
                "status": "completed",
                "stage": "public_web_stage_2",
                "candidate_count": 47,
                "manual_review_count": 5,
            },
        )

    def test_settle_board_probe_waits_for_first_nonempty_candidate_page(self) -> None:
        client = mock.Mock()
        client.get.side_effect = [
            {"asset_population": {"available": False, "candidate_count": 0}, "results": []},
            {"result_mode": "", "returned_count": 0, "total_candidates": 0, "has_more": False},
            {
                "asset_population": {
                    "available": True,
                    "candidate_count": 47,
                    "candidates": [{"candidate_id": "cand-1"}],
                },
                "results": [],
            },
            {"result_mode": "asset_population", "returned_count": 24, "total_candidates": 47, "has_more": True},
        ]

        dashboard_payload, candidate_page_payload, timings = _settle_board_probe(
            client,
            job_id="job-123",
            results_payload={
                "asset_population": {"available": True, "candidate_count": 47},
                "workflow_stage_summaries": {
                    "summaries": {
                        "stage_2_final": {
                            "status": "completed",
                            "candidate_source": {"candidate_count": 47},
                        }
                    }
                },
            },
            poll_seconds=0.01,
        )

        self.assertEqual(dashboard_payload["asset_population"]["candidate_count"], 47)
        self.assertEqual(candidate_page_payload["result_mode"], "asset_population")
        self.assertGreaterEqual(float(timings.get("attempt_count") or 0.0), 2.0)
        self.assertGreater(float(timings.get("wait_ms") or 0.0), 0.0)

    def test_summarize_smoke_timings_includes_provider_case_report_aggregates(self) -> None:
        summary = summarize_smoke_timings(
            [
                {
                    "timings_ms": {"total": 1000.0, "wait_for_completion": 800.0},
                    "provider_case_report": {
                        "execution": {
                            "effective_acquisition_mode": "full_local_asset_reuse",
                            "dispatch_strategy": "reuse_snapshot",
                        },
                        "board": {"ready": True, "ready_nonempty": True},
                        "behavior_guardrails": {
                            "duplicate_provider_dispatch": {
                                "violation_detected": False,
                                "duplicate_signature_count": 0,
                                "redundant_dispatch_count": 0,
                            },
                            "disabled_stage_violations": {"violation_detected": False},
                            "prerequisite_gaps": {
                                "violation_detected": False,
                                "metrics_ms": {"linkedin_stage_1_to_stage_1_preview_start": 0.0},
                            },
                            "final_results_board_consistency": {"violation_detected": False},
                        },
                        "stage_wall_clock_ms": {
                            "linkedin_stage_1": 2000.0,
                            "stage_1_preview": 500.0,
                        },
                        "workflow_wall_clock_ms": {
                            "job_to_stage_1_preview": 500.0,
                            "job_to_final_results": 1000.0,
                            "job_to_board_nonempty": 1000.0,
                        },
                        "event_level_efficiency": {
                            "report_available": True,
                            "provider_io": {
                                "actor_run_duration_ms": {"count": 1, "values": [45000.0], "max": 45000.0},
                                "dataset_download_duration_ms": {"count": 1, "values": [2600.0], "max": 2600.0},
                            },
                        },
                        "service_metrics": {
                            "report_available": True,
                            "worker_timeline": {
                                "worker_count": 2,
                                "trace_span_count": 2,
                                "duration_ms": {"count": 2, "values": [1000.0, 2000.0], "max": 2000.0},
                                "handoff_gap_ms": {
                                    "global_next_worker_start_gap_ms": {
                                        "count": 1,
                                        "values": [100.0],
                                        "max": 100.0,
                                    },
                                    "slow_gaps": [],
                                },
                                "slow_workers": [],
                            },
                            "local_apply_backlog": {
                                "report_available": False,
                                "applied_not_ingested_count": 0,
                                "stale_applied_not_ingested_count": 0,
                                "stale_applied_not_ingested_present": False,
                            },
                            "user_experience": {
                                "final_results_to_board_ready_ms": 100.0,
                                "final_results_to_board_nonempty_ms": 150.0,
                                "job_to_board_nonempty_ms": 1000.0,
                                "stage_1_preview_to_final_results_ms": 500.0,
                                "loading_feedback_required": False,
                                "board_readiness_violation": False,
                                "long_finalization_after_preview": False,
                            },
                            "board_visible_projection": {
                                "report_available": True,
                                "delta_profile_board_visible_count": 2,
                                "patch_log_count": 1,
                                "fetched_to_board_visible_lag_count": 3,
                                "patch_log_lag_count": 0,
                                "projection_missing_for_visible_count": False,
                                "patch_log_missing_for_visible_count": False,
                                "patch_log_replay_lag": False,
                                "materialization_lag_violation": False,
                            },
                            "snapshot_full_materialization_queue": {
                                "report_available": True,
                                "backlog_count": 0,
                                "retryable_count": 0,
                                "stale_running_count": 0,
                                "retry_backlog_present": False,
                                "stale_running_present": False,
                            },
                            "search_seed_discovery_queue": {
                                "report_available": True,
                                "item_count": 0,
                                "provider_owned_count": 0,
                                "retry_wait_count": 0,
                                "ready_retry_count": 0,
                                "exhausted_count": 0,
                                "stale_provider_owned_count": 0,
                                "item_without_worker_owner_count": 0,
                                "exhausted_without_provider_retry_count": 0,
                                "retry_backlog_present": False,
                                "stale_provider_owned_present": False,
                                "item_without_worker_owner_present": False,
                                "exhausted_without_provider_retry_present": False,
                            },
                            "provider_search_retry_queue": {
                                "report_available": False,
                                "backlog_count": 0,
                                "retryable_count": 0,
                                "terminal_failed_count": 0,
                                "stale_running_count": 0,
                                "retry_backlog_present": False,
                                "terminal_failure_present": False,
                                "stale_running_present": False,
                            },
                            "remote_provider_events": {
                                "report_available": False,
                                "event_count": 0,
                                "late_duplicate_count": 0,
                                "in_flight_duplicate_count": 0,
                                "remote_to_local_event_lag_ms": {"count": 0},
                                "slow_lag_count": 0,
                                "lag_violation": False,
                            },
                            "bottlenecks": {"bottleneck_count": 0, "top_bottlenecks": []},
                        },
                    },
                },
                {
                    "timings_ms": {"total": 2000.0, "wait_for_completion": 1200.0, "board_probe_wait": 150.0},
                    "provider_case_report": {
                        "execution": {
                            "effective_acquisition_mode": "baseline_reuse_with_delta",
                            "dispatch_strategy": "delta_from_snapshot",
                        },
                        "board": {"ready": True, "ready_nonempty": False},
                        "behavior_guardrails": {
                            "duplicate_provider_dispatch": {
                                "violation_detected": True,
                                "duplicate_signature_count": 1,
                                "redundant_dispatch_count": 1,
                            },
                            "disabled_stage_violations": {
                                "violation_detected": True,
                                "unexpected_public_web_stage": True,
                            },
                            "prerequisite_gaps": {
                                "violation_detected": True,
                                "metrics_ms": {"stage_1_preview_to_stage_2_final_start": 5000.0},
                            },
                            "final_results_board_consistency": {"violation_detected": True},
                        },
                        "stage_wall_clock_ms": {
                            "linkedin_stage_1": 4000.0,
                            "stage_1_preview": 1500.0,
                        },
                        "workflow_wall_clock_ms": {
                            "job_to_stage_1_preview": 1500.0,
                            "job_to_final_results": 2000.0,
                        },
                        "event_level_efficiency": {
                            "report_available": True,
                            "provider_io": {
                                "actor_run_duration_ms": {"count": 1, "values": [90000.0], "max": 90000.0},
                                "dataset_download_duration_ms": {"count": 1, "values": [4300.0], "max": 4300.0},
                            },
                        },
                        "service_metrics": {
                            "report_available": True,
                            "worker_timeline": {
                                "worker_count": 3,
                                "trace_span_count": 3,
                                "duration_ms": {"count": 3, "values": [1000.0, 45000.0], "max": 45000.0},
                                "handoff_gap_ms": {
                                    "global_next_worker_start_gap_ms": {
                                        "count": 1,
                                        "values": [6500.0],
                                        "max": 6500.0,
                                    },
                                    "slow_gaps": [{"gap_ms": 6500.0}],
                                },
                                "slow_workers": [{"worker_id": 22}],
                            },
                            "local_apply_backlog": {
                                "report_available": True,
                                "applied_not_ingested_count": 2,
                                "stale_applied_not_ingested_count": 1,
                                "stale_applied_not_ingested_present": True,
                                "age_ms": {"count": 2, "values": [1000.0, 45000.0], "max": 45000.0},
                                "closure_backlog_count": 3,
                                "closure_retryable_count": 2,
                                "closure_ready_retry_count": 1,
                                "closure_stale_running_count": 1,
                                "closure_retry_backlog_present": True,
                                "closure_stale_running_present": True,
                            },
                            "user_experience": {
                                "final_results_to_board_ready_ms": 3000.0,
                                "final_results_to_board_nonempty_ms": 8000.0,
                                "job_to_board_nonempty_ms": 10000.0,
                                "stage_1_preview_to_final_results_ms": 45000.0,
                                "loading_feedback_required": True,
                                "board_readiness_violation": True,
                                "long_finalization_after_preview": True,
                            },
                            "board_visible_projection": {
                                "report_available": True,
                                "delta_profile_board_visible_count": 5,
                                "patch_log_count": 0,
                                "fetched_to_board_visible_lag_count": 10,
                                "patch_log_lag_count": 5,
                                "projection_missing_for_visible_count": True,
                                "patch_log_missing_for_visible_count": True,
                                "patch_log_replay_lag": True,
                                "materialization_lag_violation": True,
                            },
                            "snapshot_full_materialization_queue": {
                                "report_available": True,
                                "backlog_count": 2,
                                "retryable_count": 1,
                                "stale_running_count": 1,
                                "retry_backlog_present": True,
                                "stale_running_present": True,
                            },
                            "search_seed_discovery_queue": {
                                "report_available": True,
                                "item_count": 4,
                                "provider_owned_count": 2,
                                "retry_wait_count": 1,
                                "ready_retry_count": 1,
                                "exhausted_count": 1,
                                "stale_provider_owned_count": 1,
                                "item_without_worker_owner_count": 1,
                                "exhausted_without_provider_retry_count": 1,
                                "retry_backlog_present": True,
                                "stale_provider_owned_present": True,
                                "item_without_worker_owner_present": True,
                                "exhausted_without_provider_retry_present": True,
                            },
                            "provider_search_retry_queue": {
                                "report_available": True,
                                "backlog_count": 2,
                                "retryable_count": 1,
                                "terminal_failed_count": 1,
                                "stale_running_count": 1,
                                "retry_backlog_present": True,
                                "terminal_failure_present": True,
                                "stale_running_present": True,
                            },
                            "provider_anomalies": {
                                "report_available": True,
                                "anomaly_count": 7,
                                "zero_result_retry_count": 3,
                                "zero_result_retry_exhausted_count": 1,
                                "zero_result_accepted_count": 1,
                                "empty_scale_count": 1,
                                "probe_total_drift_count": 1,
                                "empty_page_range_count": 1,
                                "single_page_retry_count": 2,
                            },
                            "target_candidate_public_web": {
                                "report_available": True,
                                "batch_count": 1,
                                "run_count": 2,
                                "metric_run_count": 1,
                                "crm_storage_owner_batch_count": 1,
                                "legacy_storage_owner_batch_count": 0,
                                "execution_backend_bridge_count": 0,
                                "execution_backend_counts": {"crm_public_web_v1": 1},
                                "queue_batch_command_count": 1,
                                "queue_batch_command_succeeded_count": 1,
                                "queue_batch_command_pending_count": 0,
                                "queue_batch_command_failed_count": 0,
                                "queue_batch_command_invalid_owner_count": 0,
                                "queue_batch_command_incomplete_causality_count": 0,
                                "queue_batch_command_contract_present": True,
                                "queue_batch_command_status_counts": {"succeeded": 1},
                                "queue_batch_command_owner_counts": {"crm_public_web_owner": 1},
                                "remote_search_pending_run_count": 1,
                                "partial_failure_count": 1,
                                "unmaterialized_signal_gap_count": 1,
                                "completed_without_materialized_signals_count": 1,
                                "missing_phase_metric_count": 1,
                                "provider_or_fetch_failure_count": 2,
                                "local_processing_error_count": 1,
                                "slowest_phase_counts": {"document_fetch": 1},
                                "duration_by_phase_ms_max": {
                                    "search_poll": 1000.0,
                                    "document_fetch": 31000.0,
                                    "adjudication": 2100.0,
                                },
                                "service_guardrail_violation_detected": True,
                                "risk_reason_counts": {
                                    "remote_search_pending": 1,
                                    "terminal_signal_materialization_gap": 1,
                                    "missing_phase_metrics": 1,
                                },
                            },
                            "remote_provider_events": {
                                "report_available": True,
                                "event_count": 2,
                                "late_duplicate_count": 1,
                                "in_flight_duplicate_count": 0,
                                "source_counts": {
                                    "local_provider_event_watcher": 1,
                                    "provider_webhook": 1,
                                },
                                "status_counts": {
                                    "received": 1,
                                    "received_late": 1,
                                },
                                "remote_to_local_event_lag_ms": {
                                    "count": 2,
                                    "values": [1000.0, 207000.0],
                                    "max": 207000.0,
                                },
                                "actionable_remote_to_local_event_lag_ms": {
                                    "count": 1,
                                    "values": [1000.0],
                                    "max": 1000.0,
                                },
                                "late_duplicate_remote_to_local_event_lag_ms": {
                                    "count": 1,
                                    "values": [207000.0],
                                    "max": 207000.0,
                                },
                                "slow_lag_count": 0,
                                "late_duplicate_slow_lag_count": 1,
                                "lag_violation": False,
                            },
                            "serving_publication_gap": {
                                "report_available": True,
                                "gap_present": True,
                                "stale_gap_present": True,
                                "age_ms": 45000.0,
                            },
                            "bottlenecks": {
                                "bottleneck_count": 2,
                                "top_bottlenecks": [
                                    {"kind": "slow_worker", "severity": "medium", "duration_ms": 45000.0},
                                    {"kind": "board_readiness_lag", "severity": "high", "duration_ms": 8000.0},
                                ],
                            },
                        },
                    },
                },
            ]
        )

        provider_summary = dict(summary.get("provider_case_report") or {})
        self.assertEqual(provider_summary.get("board_ready_count"), 2)
        self.assertEqual(provider_summary.get("board_ready_nonempty_count"), 1)
        self.assertEqual(provider_summary["stage_wall_clock_ms"]["linkedin_stage_1"]["avg"], 3000.0)
        self.assertEqual(provider_summary["stage_wall_clock_ms"]["stage_1_preview"]["max"], 1500.0)
        self.assertEqual(provider_summary["workflow_wall_clock_ms"]["job_to_final_results"]["avg"], 1500.0)
        self.assertEqual(provider_summary["behavior_guardrails"]["duplicate_provider_dispatch_case_count"], 1)
        self.assertEqual(provider_summary["behavior_guardrails"]["duplicate_provider_dispatch_signature_count"], 1)
        self.assertEqual(provider_summary["behavior_guardrails"]["redundant_provider_dispatch_count"], 1)
        self.assertEqual(provider_summary["behavior_guardrails"]["disabled_stage_violation_case_count"], 1)
        self.assertEqual(provider_summary["behavior_guardrails"]["unexpected_public_web_stage_case_count"], 1)
        self.assertEqual(
            provider_summary["behavior_guardrails"]["prerequisite_gap_ms"]["stage_1_preview_to_stage_2_final_start"][
                "max"
            ],
            5000.0,
        )
        self.assertEqual(
            provider_summary["strategy_rollups"]["effective_acquisition_mode"]["full_local_asset_reuse"]["timings_ms"][
                "job_to_board_nonempty"
            ]["max"],
            1000.0,
        )
        self.assertEqual(
            provider_summary["strategy_rollups"]["dispatch_strategy"]["delta_from_snapshot"]["timings_ms"][
                "board_probe_wait"
            ]["avg"],
            150.0,
        )
        service_metrics = dict(provider_summary.get("service_metrics") or {})
        self.assertEqual(service_metrics["report_count"], 2)
        self.assertEqual(service_metrics["worker_count"]["max"], 3.0)
        self.assertEqual(service_metrics["worker_duration_ms"]["max"], 45000.0)
        self.assertEqual(service_metrics["global_next_worker_start_gap_ms"]["max"], 6500.0)
        self.assertEqual(service_metrics["slow_worker_count"]["max"], 1.0)
        self.assertEqual(service_metrics["slow_gap_count"]["max"], 1.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["report_count"], 1)
        self.assertEqual(service_metrics["local_apply_backlog"]["applied_not_ingested_count"]["max"], 2.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["stale_applied_not_ingested_count"]["max"], 1.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["age_ms"]["max"], 45000.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["stale_case_count"], 1)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_backlog_count"]["max"], 3.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_retryable_count"]["max"], 2.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_ready_retry_count"]["max"], 1.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_stale_running_count"]["max"], 1.0)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_retry_backlog_case_count"], 1)
        self.assertEqual(service_metrics["local_apply_backlog"]["closure_stale_running_case_count"], 1)
        self.assertEqual(service_metrics["user_experience"]["board_readiness_violation_case_count"], 1)
        self.assertEqual(service_metrics["user_experience"]["long_finalization_case_count"], 1)
        self.assertEqual(service_metrics["board_visible_projection"]["report_count"], 2)
        self.assertEqual(service_metrics["board_visible_projection"]["board_visible_count"]["max"], 5.0)
        self.assertEqual(service_metrics["board_visible_projection"]["patch_log_count"]["max"], 1.0)
        self.assertEqual(
            service_metrics["board_visible_projection"]["fetched_to_board_visible_lag_count"]["max"],
            10.0,
        )
        self.assertEqual(service_metrics["board_visible_projection"]["projection_missing_case_count"], 1)
        self.assertEqual(service_metrics["board_visible_projection"]["patch_log_missing_case_count"], 1)
        self.assertEqual(service_metrics["board_visible_projection"]["patch_log_replay_lag_case_count"], 1)
        self.assertEqual(service_metrics["board_visible_projection"]["materialization_lag_case_count"], 1)
        snapshot_queue = service_metrics["snapshot_full_materialization_queue"]
        self.assertEqual(snapshot_queue["report_count"], 2)
        self.assertEqual(snapshot_queue["backlog_count"]["max"], 2.0)
        self.assertEqual(snapshot_queue["retryable_count"]["max"], 1.0)
        self.assertEqual(snapshot_queue["stale_running_count"]["max"], 1.0)
        self.assertEqual(snapshot_queue["retry_backlog_case_count"], 1)
        self.assertEqual(snapshot_queue["stale_running_case_count"], 1)
        search_seed_queue = service_metrics["search_seed_discovery_queue"]
        self.assertEqual(search_seed_queue["report_count"], 2)
        self.assertEqual(search_seed_queue["item_count"]["max"], 4.0)
        self.assertEqual(search_seed_queue["provider_owned_count"]["max"], 2.0)
        self.assertEqual(search_seed_queue["retry_wait_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["ready_retry_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["exhausted_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["stale_provider_owned_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["owner_missing_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["exhausted_without_report_count"]["max"], 1.0)
        self.assertEqual(search_seed_queue["retry_backlog_case_count"], 1)
        self.assertEqual(search_seed_queue["stale_provider_case_count"], 1)
        self.assertEqual(search_seed_queue["owner_missing_case_count"], 1)
        self.assertEqual(search_seed_queue["exhausted_without_report_case_count"], 1)
        provider_retry_queue = service_metrics["provider_search_retry_queue"]
        self.assertEqual(provider_retry_queue["report_count"], 1)
        self.assertEqual(provider_retry_queue["backlog_count"]["max"], 2.0)
        self.assertEqual(provider_retry_queue["retryable_count"]["max"], 1.0)
        self.assertEqual(provider_retry_queue["terminal_failed_count"]["max"], 1.0)
        self.assertEqual(provider_retry_queue["stale_running_count"]["max"], 1.0)
        self.assertEqual(provider_retry_queue["retry_backlog_case_count"], 1)
        self.assertEqual(provider_retry_queue["terminal_failure_case_count"], 1)
        self.assertEqual(provider_retry_queue["stale_running_case_count"], 1)
        provider_anomalies = service_metrics["provider_anomalies"]
        self.assertEqual(provider_anomalies["report_count"], 1)
        self.assertEqual(provider_anomalies["anomaly_count"]["max"], 7.0)
        self.assertEqual(provider_anomalies["zero_result_retry_count"]["max"], 3.0)
        self.assertEqual(provider_anomalies["zero_result_retry_exhausted_count"]["max"], 1.0)
        self.assertEqual(provider_anomalies["zero_result_accepted_count"]["max"], 1.0)
        self.assertEqual(provider_anomalies["empty_scale_count"]["max"], 1.0)
        self.assertEqual(provider_anomalies["probe_total_drift_count"]["max"], 1.0)
        self.assertEqual(provider_anomalies["empty_page_range_count"]["max"], 1.0)
        self.assertEqual(provider_anomalies["single_page_retry_count"]["max"], 2.0)
        target_public_web = service_metrics["target_candidate_public_web"]
        self.assertEqual(target_public_web["report_count"], 1)
        self.assertEqual(target_public_web["batch_count"]["max"], 1.0)
        self.assertEqual(target_public_web["run_count"]["max"], 2.0)
        self.assertEqual(target_public_web["crm_storage_owner_batch_count"]["max"], 1.0)
        self.assertEqual(target_public_web["legacy_storage_owner_batch_count"]["max"], 0.0)
        self.assertEqual(target_public_web["execution_backend_bridge_count"]["max"], 0.0)
        self.assertEqual(target_public_web["queue_batch_command_count"]["max"], 1.0)
        self.assertEqual(target_public_web["queue_batch_command_succeeded_count"]["max"], 1.0)
        self.assertEqual(target_public_web["queue_batch_command_pending_count"]["max"], 0.0)
        self.assertEqual(target_public_web["queue_batch_command_invalid_owner_count"]["max"], 0.0)
        self.assertEqual(target_public_web["queue_batch_command_incomplete_causality_count"]["max"], 0.0)
        self.assertEqual(target_public_web["queue_batch_command_contract_missing_case_count"], 0)
        self.assertEqual(target_public_web["queue_batch_command_status_counts"]["succeeded"], 1)
        self.assertEqual(target_public_web["queue_batch_command_owner_counts"]["crm_public_web_owner"], 1)
        self.assertEqual(
            target_public_web["execution_backend_counts"]["crm_public_web_v1"],
            1,
        )
        self.assertEqual(target_public_web["remote_search_pending_run_count"]["max"], 1.0)
        self.assertEqual(target_public_web["partial_failure_count"]["max"], 1.0)
        self.assertEqual(target_public_web["unmaterialized_signal_gap_count"]["max"], 1.0)
        self.assertEqual(target_public_web["completed_without_materialized_signals_count"]["max"], 1.0)
        self.assertEqual(target_public_web["missing_phase_metric_count"]["max"], 1.0)
        self.assertEqual(target_public_web["provider_or_fetch_failure_count"]["max"], 2.0)
        self.assertEqual(target_public_web["local_processing_error_count"]["max"], 1.0)
        self.assertEqual(target_public_web["duration_by_phase_ms_max"]["document_fetch"]["max"], 31000.0)
        self.assertEqual(target_public_web["guardrail_violation_case_count"], 1)
        self.assertEqual(target_public_web["risk_reason_counts"]["missing_phase_metrics"], 1)
        self.assertEqual(target_public_web["slowest_phase_counts"]["document_fetch"], 1)
        provider_io = provider_summary["event_level_efficiency"]["provider_io"]
        self.assertEqual(provider_io["actor_run_duration_ms"]["max"], 90000.0)
        self.assertEqual(provider_io["dataset_download_duration_ms"]["max"], 4300.0)
        remote_provider_events = service_metrics["remote_provider_events"]
        self.assertEqual(remote_provider_events["report_count"], 1)
        self.assertEqual(remote_provider_events["event_count"]["max"], 2.0)
        self.assertEqual(remote_provider_events["late_duplicate_count"]["max"], 1.0)
        self.assertEqual(remote_provider_events["remote_to_local_event_lag_ms"]["max"], 207000.0)
        self.assertEqual(remote_provider_events["actionable_remote_to_local_event_lag_ms"]["max"], 1000.0)
        self.assertEqual(remote_provider_events["late_duplicate_remote_to_local_event_lag_ms"]["max"], 207000.0)
        self.assertEqual(remote_provider_events["slow_lag_count"]["max"], 0.0)
        self.assertEqual(remote_provider_events["late_duplicate_slow_lag_count"]["max"], 1.0)
        self.assertEqual(remote_provider_events["lag_violation_case_count"], 0)
        self.assertEqual(
            remote_provider_events["source_counts"],
            {"local_provider_event_watcher": 1, "provider_webhook": 1},
        )
        self.assertEqual(remote_provider_events["status_counts"], {"received": 1, "received_late": 1})
        serving_publication_gap = service_metrics["serving_publication_gap"]
        self.assertEqual(serving_publication_gap["report_count"], 1)
        self.assertEqual(serving_publication_gap["gap_present_case_count"], 1)
        self.assertEqual(serving_publication_gap["stale_gap_case_count"], 1)
        self.assertEqual(serving_publication_gap["age_ms"]["max"], 45000.0)
        self.assertEqual(service_metrics["bottlenecks"]["case_count"], 1)
        self.assertEqual(service_metrics["bottlenecks"]["by_kind"]["slow_worker"], 1)
        self.assertEqual(service_metrics["bottlenecks"]["by_severity"]["high"], 1)

    def test_summarize_smoke_timings_includes_materialization_streaming_rollup(self) -> None:
        summary = summarize_smoke_timings(
            [
                {
                    "timings_ms": {"total": 1000.0},
                    "provider_case_report": {
                        "materialization_streaming": {
                            "report_available": True,
                            "provider_response_count": 2,
                            "pending_delta_count": 3,
                            "provider_response_to_first_materialization_ms": 450.0,
                        },
                        "post_preview_finalization": {
                            "report_available": True,
                            "preview_to_first_finalization_start_ms": 3000.0,
                            "preview_to_first_materialize_start_ms": 4000.0,
                            "preview_to_last_materialize_completed_ms": 22000.0,
                            "preview_to_finalization_completed_ms": 25000.0,
                            "materialize_completed_count": 2,
                            "materialize_sync_duration_ms": {"count": 2, "values": [8000.0, 18000.0]},
                            "profile_batch_local_apply_duration_ms": {"count": 1, "values": [5000.0]},
                            "candidate_source_closure_lifecycle_ms": {"count": 1, "values": [121000.0]},
                            "materialize_sync_scope_counts": {
                                "profile_batch_local_apply": 1,
                                "candidate_source_closure_lifecycle": 1,
                            },
                            "long_post_preview_finalization": False,
                        },
                        "provider_backpressure": {
                            "backpressure_detected": True,
                            "limiter_exhausted": True,
                            "observed_limiter_count": 1,
                            "observed_active_limiter_count": 2,
                            "max_provider_limiter_wait_ms": 125.0,
                        },
                        "remote_actor_slot_observation": {
                            "report_available": True,
                            "harvest_profile_actor_global_inflight": 4,
                            "peak_waiting_remote_harvest_count": 3,
                            "remote_actor_slot_peak_occupancy_ratio": 0.75,
                        },
                        "workflow_benchmark": {
                            "search_returned_count": 10,
                            "roster_returned_count": 40,
                            "fetched_profile_count": 5,
                            "profile_url_total_count": 9,
                            "board_total_candidates": 40,
                            "board_first_page_returned_count": 24,
                            "job_to_board_nonempty_ms": 1200.0,
                        },
                        "behavior_guardrails": {
                            "streaming_materialization": {"violation_detected": False},
                        },
                        "event_level_efficiency": {
                            "report_available": True,
                            "remote_to_local_event_lag_ms": {"count": 1, "avg": 1000.0, "values": [1000.0]},
                            "remote_to_local_marker_lag_ms": {"count": 1, "avg": 1400.0, "values": [1400.0]},
                            "local_to_next_submit_start_ms": {"count": 1, "avg": 100.0, "values": [100.0]},
                            "next_submit_attempt_elapsed_ms": {"count": 1, "avg": 150.0, "values": [150.0]},
                            "provider_slots": {
                                "true_active_provider_slot_worker_count": 2,
                                "pre_submit_provider_worker_count": 0,
                            },
                            "reconcile": {
                                "materialize_call_count": 1,
                                "same_worker_reconcile_repeat_count": 0,
                                "repeated_materialize_signature_count": 0,
                            },
                            "violation_detected": False,
                        },
                    },
                },
                {
                    "timings_ms": {"total": 2000.0},
                    "provider_case_report": {
                        "materialization_streaming": {
                            "report_available": True,
                            "provider_response_count": 1,
                            "pending_delta_count": 1,
                            "provider_response_to_first_materialization_ms": 6500.0,
                        },
                        "post_preview_finalization": {
                            "report_available": True,
                            "preview_to_first_finalization_start_ms": 16000.0,
                            "preview_to_first_materialize_start_ms": 17000.0,
                            "preview_to_last_materialize_completed_ms": 59000.0,
                            "preview_to_finalization_completed_ms": 62000.0,
                            "materialize_completed_count": 2,
                            "materialize_sync_duration_ms": {"count": 2, "values": [18000.0, 19000.0]},
                            "profile_batch_local_apply_duration_ms": {"count": 1, "values": [7000.0]},
                            "candidate_source_closure_lifecycle_ms": {},
                            "materialize_sync_scope_counts": {"profile_batch_local_apply": 1},
                            "long_post_preview_finalization": True,
                        },
                        "provider_backpressure": {
                            "backpressure_detected": False,
                            "limiter_exhausted": False,
                            "observed_limiter_count": 0,
                            "observed_active_limiter_count": 0,
                            "max_provider_limiter_wait_ms": 0.0,
                        },
                        "remote_actor_slot_observation": {
                            "report_available": True,
                            "harvest_profile_actor_global_inflight": 4,
                            "peak_waiting_remote_harvest_count": 4,
                            "remote_actor_slot_peak_occupancy_ratio": 1.0,
                        },
                        "workflow_benchmark": {
                            "search_returned_count": 4,
                            "roster_returned_count": 12,
                            "fetched_profile_count": 2,
                            "profile_url_total_count": 3,
                            "board_total_candidates": 12,
                            "board_first_page_returned_count": 12,
                            "job_to_board_nonempty_ms": 900.0,
                        },
                        "behavior_guardrails": {
                            "streaming_materialization": {"violation_detected": True},
                        },
                        "event_level_efficiency": {
                            "report_available": True,
                            "remote_to_local_event_lag_ms": {"count": 1, "avg": 2500.0, "values": [2500.0]},
                            "remote_to_local_marker_lag_ms": {"count": 1, "avg": 7000.0, "values": [7000.0]},
                            "local_to_next_submit_start_ms": {"count": 1, "avg": 1500.0, "values": [1500.0]},
                            "next_submit_attempt_elapsed_ms": {"count": 1, "avg": 900.0, "values": [900.0]},
                            "provider_slots": {
                                "true_active_provider_slot_worker_count": 1,
                                "pre_submit_provider_worker_count": 1,
                            },
                            "reconcile": {
                                "materialize_call_count": 2,
                                "same_worker_reconcile_repeat_count": 1,
                                "repeated_materialize_signature_count": 1,
                            },
                            "violation_detected": True,
                        },
                    },
                },
            ]
        )

        provider_summary = dict(summary.get("provider_case_report") or {})
        materialization = dict(provider_summary.get("materialization_streaming") or {})
        self.assertEqual(materialization["report_count"], 2)
        self.assertEqual(materialization["provider_response_count"]["max"], 2.0)
        self.assertEqual(materialization["pending_delta_count"]["avg"], 2.0)
        post_preview = dict(provider_summary.get("post_preview_finalization") or {})
        self.assertEqual(post_preview["report_count"], 2)
        self.assertEqual(post_preview["preview_to_first_materialize_start_ms"]["max"], 17000.0)
        self.assertEqual(post_preview["preview_to_finalization_completed_ms"]["max"], 62000.0)
        self.assertEqual(post_preview["materialize_completed_count"]["avg"], 2.0)
        self.assertEqual(post_preview["materialize_sync_duration_ms"]["max"], 19000.0)
        self.assertEqual(post_preview["profile_batch_local_apply_duration_ms"]["max"], 7000.0)
        self.assertEqual(post_preview["candidate_source_closure_lifecycle_ms"]["max"], 121000.0)
        self.assertEqual(post_preview["materialize_sync_scope_counts"]["profile_batch_local_apply"], 2)
        self.assertEqual(post_preview["materialize_sync_scope_counts"]["candidate_source_closure_lifecycle"], 1)
        self.assertEqual(post_preview["long_finalization_case_count"], 1)
        self.assertEqual(
            provider_summary["behavior_guardrails"]["streaming_materialization_violation_case_count"],
            1,
        )
        backpressure = dict(provider_summary.get("provider_backpressure") or {})
        self.assertEqual(backpressure["report_count"], 2)
        self.assertEqual(backpressure["backpressure_case_count"], 1)
        self.assertEqual(backpressure["limiter_exhausted_case_count"], 1)
        self.assertEqual(backpressure["observed_active_limiter_count"]["max"], 2.0)
        slot_observation = dict(provider_summary.get("remote_actor_slot_observation") or {})
        self.assertEqual(slot_observation["report_count"], 2)
        self.assertEqual(slot_observation["harvest_profile_actor_global_inflight"]["max"], 4.0)
        self.assertEqual(slot_observation["peak_waiting_remote_harvest_count"]["max"], 4.0)
        self.assertEqual(slot_observation["remote_actor_slot_peak_occupancy_ratio"]["min"], 0.75)
        self.assertEqual(slot_observation["remote_actor_slot_peak_occupancy_ratio"]["max"], 1.0)
        event_efficiency = dict(provider_summary.get("event_level_efficiency") or {})
        self.assertEqual(event_efficiency["report_count"], 2)
        self.assertEqual(event_efficiency["remote_to_local_marker_lag_ms"]["max"], 7000.0)
        self.assertEqual(event_efficiency["max_pre_submit_provider_worker_count"], 1)
        self.assertEqual(event_efficiency["same_worker_reconcile_repeat_count"], 1)
        benchmark = dict(provider_summary.get("workflow_benchmark") or {})
        self.assertEqual(benchmark["report_count"], 2)
        self.assertEqual(benchmark["metrics"]["search_returned_count"]["max"], 10.0)
        self.assertEqual(benchmark["metrics"]["roster_returned_count"]["avg"], 26.0)
        self.assertEqual(benchmark["metrics"]["fetched_profile_count"]["max"], 5.0)

    def test_build_workflow_benchmark_prefers_canonical_board_runtime_denominators(self) -> None:
        benchmark = _build_workflow_benchmark_report(
            search_report={"search_seed_entry_count": 0, "search_seed_added_entry_count": 0},
            roster_report={"returned_count": 7381},
            profile_completion={"fetched_profile_count": 2381, "profile_detail_count": 0},
            materialization_streaming={"profile_url_count": 2384},
            board_probe={
                "ready": True,
                "ready_nonempty": True,
                "candidate_page_total_candidates": 7381,
                "candidate_page_returned_count": 24,
                "profile_fetch_progress": {"total_url_count": 7381, "fetched_url_count": 2381},
                "board_runtime_state": {
                    "delta_profile_required_count": 2384,
                    "delta_profile_fetched_count": 2384,
                    "profile_fetch_required_count": 2384,
                    "profile_fetched_count": 2384,
                },
            },
            provider_backpressure={"backpressure_detected": False},
            workflow_wall_clock={"job_to_board_nonempty": 38000.0},
        )

        self.assertEqual(benchmark["profile_url_total_count"], 2384)
        self.assertEqual(benchmark["fetched_profile_count"], 2384)
        self.assertEqual(benchmark["board_total_candidates"], 7381)


if __name__ == "__main__":
    unittest.main()
