import unittest
from unittest import mock

from sourcing_agent.workflow_smoke import (
    _build_case_level_smoke_exports,
    _effective_asset_population_candidate_count,
    _build_provider_case_report,
    _build_progress_observability_report,
    _build_synthetic_terminal_progress_sample,
    _evaluate_smoke_record_completion,
    _settle_board_probe,
    stage_summary_digest,
    run_hosted_smoke_matrix,
    summarize_smoke_timings,
)


class WorkflowSmokeTest(unittest.TestCase):
    def test_effective_asset_population_candidate_count_prefers_board_visible_total(self) -> None:
        count = _effective_asset_population_candidate_count(
            results_payload={"asset_population": {"candidate_count": 1020}},
            dashboard_payload={"asset_population": {"candidate_count": 1020}},
            candidate_page_payload={
                "result_mode": "asset_population",
                "returned_count": 2,
                "total_candidates": 2,
            },
        )

        self.assertEqual(count, 2)

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
        )

        self.assertEqual(report["execution"]["target_company"], "OpenAI")
        self.assertEqual(report["search"]["query_count"], 3)
        self.assertEqual(report["search"]["search_seed_added_entry_count"], 7)
        self.assertEqual(report["profile_completion"]["fetched_profile_count"], 5)
        self.assertTrue(report["board"]["ready"])
        self.assertTrue(report["board"]["ready_nonempty"])
        self.assertEqual(report["board"]["candidate_page_total_candidates"], 47)
        self.assertEqual(report["counts"]["stage_1_preview_candidate_count"], 47)
        self.assertEqual(report["stage_wall_clock_ms"]["linkedin_stage_1"], 2000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_stage_1_preview"], 3000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_final_results"], 5000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["stage_1_preview_to_final_results"], 2000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_board_nonempty"], 5000.0)
        self.assertEqual(report["timings_ms"]["candidate_page_fetch"], 45.67)
        self.assertEqual(report["progress_observability"]["maxima"]["queued_worker_count"], 3)
        self.assertEqual(report["materialization_streaming"]["provider_response_count"], 2)
        self.assertEqual(report["materialization_streaming"]["pending_delta_count"], 3)
        self.assertEqual(
            report["materialization_streaming"]["budget"]["recommended_action"],
            "coalesce_window",
        )
        self.assertFalse(report["behavior_guardrails"]["duplicate_provider_dispatch"]["violation_detected"])
        self.assertFalse(report["behavior_guardrails"]["disabled_stage_violations"]["violation_detected"])
        self.assertFalse(report["behavior_guardrails"]["streaming_materialization"]["violation_detected"])

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

    def test_build_provider_case_report_prefers_backend_stage_timestamps_over_client_timeline(self) -> None:
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

        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_stage_1_preview"], 3000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["job_to_final_results"], 5000.0)
        self.assertEqual(report["workflow_wall_clock_ms"]["stage_1_preview_to_final_results"], 2000.0)

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
                        "behavior_guardrails": {
                            "streaming_materialization": {"violation_detected": False},
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
                        "behavior_guardrails": {
                            "streaming_materialization": {"violation_detected": True},
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
        self.assertEqual(
            provider_summary["behavior_guardrails"]["streaming_materialization_violation_case_count"],
            1,
        )


if __name__ == "__main__":
    unittest.main()
