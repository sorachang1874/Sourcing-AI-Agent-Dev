from sourcing_agent.durable_runtime import (
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
    workflow_command_display_contract,
)
from sourcing_agent.scripted_smoke_signoff import (
    build_scripted_smoke_signoff_report,
    render_scripted_smoke_signoff_markdown,
)


def _projection_export_control_policy() -> dict:
    return workflow_command_control_policy(
        "export.projection.generate",
        owner="projection_exporter",
    ).to_record()


def _projection_export_display_contract() -> dict:
    return workflow_command_display_contract(
        "export.projection.generate",
        owner="projection_exporter",
    ).to_record()


def _projection_export_activity_spine_policy() -> dict:
    return workflow_command_activity_spine_policy(
        "export.projection.generate",
        owner="projection_exporter",
    ).to_record()


def test_scripted_smoke_signoff_blocks_live_provider_and_expectation_failures() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "openai_agent_scoped_delta_streaming",
                "final": {"smoke_ready": False, "smoke_completion_state": "expectation_failed"},
                "expectation_failures": ["board runtime parity drift"],
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "live",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": False},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": True,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        }
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "blocked"
    assert {
        "smoke_case_not_ready",
        "smoke_expectation_failure",
        "provider_mode_not_scripted",
        "board_runtime_state_cross_endpoint_drift",
        "post_profile_completion_slo_violation",
    } <= finding_names


def test_scripted_smoke_signoff_blocks_terminal_profile_card_drift() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "lovable_live_roster_terminal_profile_card_drift",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 140},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "board": {
                        "board_runtime_state": {
                            "publication_status": "complete",
                            "baseline_candidate_count": 0,
                            "display_ready_candidate_count": 20,
                            "explicit_profile_capture_candidate_count": 20,
                            "profile_fetch_required_count": 140,
                            "profile_fetched_count": 140,
                            "delta_profile_required_count": 0,
                            "profile_fetch_status_text": "本次 LinkedIn Profile 已取回 140/140",
                            "card_materialization_status_text": "卡片详情已合入看板 20/140",
                        }
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "blocked"
    assert "board_visible_projection_terminal_profile_card_drift" in finding_names


def test_scripted_smoke_signoff_accepts_canonical_projection_when_legacy_materialize_checks_are_stale() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "openai_agent_scoped_delta_canonical_cutover",
                "final": {"smoke_ready": False, "smoke_completion_state": "expectation_failed"},
                "expectation_failures": [
                    "post_preview_finalization_observed: materialize_completed_count=0",
                    "service_metrics.post_profile_completion.profile_file_visible_to_board_patch_visible.elapsed_ms: metric missing",
                ],
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "projection_cutover": {
                        "report_available": True,
                        "run_projection_link_present": True,
                        "projection_missing": False,
                        "legacy_public_reader_fallback_used": False,
                        "legacy_endpoint_normal_path_used": False,
                    },
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "workflow_benchmark": {"fetched_profile_count": 140, "board_total_candidates": 140},
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "normal_path_write_count": 0,
                            "migration_adapter_write_count": 0,
                            "missing_contract_count": 0,
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "unexpected_enabled_phase_present": False,
                            "legacy_bridge_used_count": 0,
                            "legacy_bridge_used_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 0}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 20}},
                        },
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
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert "smoke_ready" in {item["gate"] for item in report["passed_gates"]}


def test_scripted_smoke_signoff_keeps_projection_membership_gap_blocking() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "lovable_projection_membership_gap",
                "final": {"smoke_ready": False, "smoke_completion_state": "expectation_failed"},
                "expectation_failures": [
                    "post_preview_finalization_observed: report unavailable",
                    "board_total_candidates: expected >= 100, actual=25",
                ],
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "projection_cutover": {
                        "report_available": True,
                        "run_projection_link_present": True,
                        "projection_missing": False,
                        "legacy_public_reader_fallback_used": False,
                        "legacy_endpoint_normal_path_used": False,
                    },
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "workflow_benchmark": {"fetched_profile_count": 140, "board_total_candidates": 25},
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "normal_path_write_count": 0,
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "legacy_bridge_used_count": 0,
                            "legacy_bridge_used_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 0}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 20}},
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "served_candidate_count": 25,
                            "expected_candidate_count": 140,
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "blocked"
    assert "smoke_case_not_ready" in finding_names
    assert "smoke_expectation_failure" in finding_names


def test_scripted_smoke_signoff_passes_clean_scripted_report_and_renders_markdown() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "lovable_small_company_live_roster",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 194},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 8000.0,
                        "job_to_final_results": 45000.0,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        summary={"case_count": 1},
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert {item["gate"] for item in report["passed_gates"]} >= {
        "provider_mode_no_cost",
        "smoke_ready",
        "board_runtime_state_parity",
        "post_profile_completion_slo",
        "profile_scheduler_contract",
    }
    markdown = render_scripted_smoke_signoff_markdown(report)
    assert "Pre-Manual Scripted Smoke Signoff" in markdown
    assert "Blocking Findings" in markdown


def test_scripted_smoke_signoff_treats_healthy_background_snapshot_compaction_as_passed_gate() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_gemini_pressure_background_compaction",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 120},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 8000.0,
                        "job_to_final_results": 45000.0,
                    },
                    "service_metrics": {
                        "snapshot_full_materialization_queue": {
                            "report_available": True,
                            "owner": "snapshot_materialization_owner",
                            "command_type": "snapshot.compaction.run",
                            "scope": "background_snapshot_compaction",
                            "manual_handoff_blocking": False,
                            "background_maintenance_pending": True,
                            "backlog_count": 1,
                            "queued_count": 0,
                            "running_count": 1,
                            "retry_backlog_present": False,
                            "stale_running_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert "background_maintenance_snapshot_compaction" in {
        item["gate"] for item in report["passed_gates"]
    }
    assert report["known_acceptable_warnings"] == []
    assert report["summary"]["background_maintenance_pending_count"] == 1


def test_scripted_smoke_signoff_blocks_unhealthy_background_snapshot_compaction() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_gemini_pressure_background_compaction_unhealthy",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 8000.0,
                        "job_to_final_results": 45000.0,
                    },
                    "service_metrics": {
                        "snapshot_full_materialization_queue": {
                            "report_available": True,
                            "backlog_count": 1,
                            "queued_count": 0,
                            "running_count": 0,
                            "retryable_count": 1,
                            "terminal_failed_count": 0,
                            "retry_backlog_present": True,
                            "stale_running_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "blocked"
    assert "snapshot_full_materialization_background_unhealthy" in {
        item["name"] for item in report["blocking_findings"]
    }


def test_scripted_smoke_signoff_uses_user_visible_wall_clock_for_post_preview_finalization() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "openai_agent_scoped_delta_streaming",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 66273.84,
                        "job_to_final_results": 83082.84,
                        "stage_1_preview_to_final_results": 16809.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 36000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                        "stage_1_preview_timestamp_source": "first_finalization_event_when_stage_preview_timestamp_is_late",
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    warning_names = {item["name"] for item in report["known_acceptable_warnings"]}
    assert "post_preview_finalization_timestamp_fallback_longer_than_user_visible_wall_clock" in warning_names


def test_scripted_smoke_signoff_respects_case_level_preview_to_final_slo() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "openai_agent_large_late_shard_profile_scheduler",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_stage_1_preview_to_final_results_ms": 120000,
                    "max_job_to_board_nonempty_ms": 30000,
                    "max_remote_to_next_submit_start_ms": 30000,
                    "max_local_to_next_submit_start_ms": 1000,
                    "max_next_submit_attempt_elapsed_ms": 10000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 690},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 66000.0,
                        "job_to_final_results": 119761.53,
                        "stage_1_preview_to_final_results": 53761.53,
                        "job_to_board_nonempty": 21000.0,
                        "job_to_board_visible_partial": 21000.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 35000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                        "stage_1_preview_timestamp_source": "first_finalization_event_when_stage_preview_timestamp_is_late",
                    },
                    "service_metrics": {
                        "user_experience": {
                            "long_finalization_after_preview": True,
                            "stage_1_preview_to_final_results_ms": 53761.53,
                            "thresholds_ms": {"stage_1_preview_to_final_results": 30000.0},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 0}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 0}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 0}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 862.0},
                        "remote_to_next_submit_start_ms": {"max": 760.0},
                        "local_completion_to_next_submit_start_ms": {"max": 0.0},
                        "next_submit_provider_attempt_elapsed_ms": {"max": 4632.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 1000.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    warning_names = {item["name"] for item in report["known_acceptable_warnings"]}
    assert "post_preview_finalization_timestamp_fallback_longer_than_user_visible_wall_clock" in warning_names
    assert "user_experience_finalization_exceeds_default_but_within_case_slo" in warning_names


def test_scripted_smoke_signoff_derives_job_to_final_slo_for_pressure_cases() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_vision_language_large_baseline_shard_real_asset",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_job_to_stage_1_preview_ms": 480000,
                    "max_stage_1_preview_to_final_results_ms": 180000,
                    "max_job_to_board_nonempty_ms": 60000,
                    "max_job_to_board_visible_partial_ms": 60000,
                    "max_remote_to_local_marker_lag_ms": 10000,
                    "max_remote_to_next_submit_start_ms": 30000,
                    "max_local_to_next_submit_start_ms": 1000,
                    "max_next_submit_attempt_elapsed_ms": 15000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 2384},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 334109.09,
                        "job_to_final_results": 367648.01,
                        "stage_1_preview_to_final_results": 33538.92,
                        "job_to_board_nonempty": 30000.0,
                        "job_to_board_visible_partial": 30000.0,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 0}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 0}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 0}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 7656.0},
                        "remote_to_next_submit_start_ms": {"max": 18416.0},
                        "local_completion_to_next_submit_start_ms": {"max": 0.0},
                        "next_submit_provider_attempt_elapsed_ms": {"max": 8328.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 2500.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "passed"
    assert "workflow_job_to_final_results_too_slow_for_manual_signoff" not in finding_names


def test_scripted_smoke_signoff_finalization_lag_is_not_manual_handoff_blocker() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "large_pressure_finalization_tail",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_stage_1_preview_to_final_results_ms": 30000,
                    "max_job_to_board_nonempty_ms": 30000,
                    "max_job_to_board_visible_partial_ms": 30000,
                    "max_remote_to_local_marker_lag_ms": 10000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 3000},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 65000.0,
                        "job_to_final_results": 110000.0,
                        "stage_1_preview_to_final_results": 45000.0,
                        "job_to_board_nonempty": 18000.0,
                        "job_to_board_visible_partial": 18000.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 45000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                    },
                    "service_metrics": {
                        "user_experience": {
                            "long_finalization_after_preview": True,
                            "stage_1_preview_to_final_results_ms": 45000.0,
                            "job_to_board_nonempty_ms": 18000.0,
                            "job_to_board_visible_partial_ms": 18000.0,
                            "thresholds_ms": {"stage_1_preview_to_final_results": 30000.0},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    manual_names = {item["name"] for item in report["manual_review_required_findings"]}
    assert {
        "post_preview_finalization_lag_exceeds_case_slo",
        "user_experience_finalization_lag_exceeds_case_slo",
    } <= manual_names
    assert report["gate_layers"]["optimization"]["status"] == "manual_review_required"
    assert report["gate_layers"]["manual_handoff"]["status"] == "passed"


def test_scripted_smoke_signoff_does_not_warn_when_profile_wait_explains_raw_finalization_lag() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "profile_provider_wait_after_stage1_preview",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_job_to_stage_1_preview_ms": 900000,
                    "max_job_to_final_results_ms": 900000,
                    "max_stage_1_preview_to_final_results_ms": 180000,
                    "max_stage1_terminal_to_finalization_start_ms": 30000,
                    "max_job_to_board_nonempty_ms": 300000,
                    "max_job_to_board_visible_partial_ms": 300000,
                    "max_remote_to_local_marker_lag_ms": 10000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 120},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 230000.0,
                        "job_to_final_results": 540000.0,
                        "stage_1_preview_to_final_results": 313000.0,
                        "job_to_board_nonempty": 234000.0,
                        "job_to_board_visible_partial": 234000.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 313000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                        "profile_wait_excluded_from_finalization_gate_ms": 213000.0,
                        "finalization_start_gate_ms": 10408.0,
                    },
                    "service_metrics": {
                        "user_experience": {
                            "long_finalization_after_preview": True,
                            "stage_1_preview_to_final_results_ms": 313000.0,
                            "job_to_board_nonempty_ms": 234000.0,
                            "job_to_board_visible_partial_ms": 234000.0,
                            "thresholds_ms": {"stage_1_preview_to_final_results": 30000.0},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 3000}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert report["manual_review_required_findings"] == []
    warning_names = {item["name"] for item in report["known_acceptable_warnings"]}
    assert "post_preview_finalization_lag_includes_profile_provider_wait" not in warning_names
    assert "user_experience_finalization_lag_includes_profile_provider_wait" not in warning_names


def test_scripted_smoke_signoff_treats_zero_finalization_start_gate_as_healthy() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "profile_wait_then_immediate_finalization",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_job_to_stage_1_preview_ms": 900000,
                    "max_job_to_final_results_ms": 900000,
                    "max_stage_1_preview_to_final_results_ms": 180000,
                    "max_stage1_terminal_to_finalization_start_ms": 30000,
                    "max_job_to_board_nonempty_ms": 300000,
                    "max_job_to_board_visible_partial_ms": 300000,
                    "max_remote_to_local_marker_lag_ms": 10000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 120},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 63000.0,
                        "job_to_final_results": 422549.94,
                        "stage_1_preview_to_final_results": 360000.0,
                        "job_to_board_nonempty": 175000.0,
                        "job_to_board_visible_partial": 175000.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 360000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                        "profile_wait_excluded_from_finalization_gate_ms": 206368.0,
                        "finalization_start_gate_ms": 0.0,
                    },
                    "service_metrics": {
                        "user_experience": {
                            "long_finalization_after_preview": True,
                            "stage_1_preview_to_final_results_ms": 360000.0,
                            "job_to_board_nonempty_ms": 175000.0,
                            "job_to_board_visible_partial_ms": 175000.0,
                            "thresholds_ms": {"stage_1_preview_to_final_results": 30000.0},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 3000}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert report["manual_review_required_findings"] == []
    warning_names = {item["name"] for item in report["known_acceptable_warnings"]}
    assert "post_preview_finalization_lag_includes_profile_provider_wait" not in warning_names
    assert "user_experience_finalization_lag_includes_profile_provider_wait" not in warning_names


def test_scripted_smoke_signoff_blocks_missing_default_workflow_wall_clock_metrics() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "missing_wall_clock",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "blocked"
    assert "workflow_job_to_stage_1_preview_too_slow_for_manual_signoff_missing_metric" in finding_names
    assert "workflow_job_to_final_results_too_slow_for_manual_signoff_missing_metric" in finding_names


def test_scripted_smoke_signoff_blocks_slow_pre_manual_ux_and_efficiency_metrics() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "openai_agent_scoped_delta_streaming",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_stage_1_preview_to_final_results_ms": 60000,
                    "max_job_to_board_nonempty_ms": 30000,
                    "max_job_to_board_visible_partial_ms": 30000,
                    "max_remote_to_local_marker_lag_ms": 10000,
                    "max_remote_to_next_submit_start_ms": 10000,
                    "max_local_to_next_submit_start_ms": 1000,
                    "max_next_submit_attempt_elapsed_ms": 10000,
                },
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 297},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 264433.19,
                        "job_to_final_results": 294891.75,
                        "stage_1_preview_to_final_results": 30458.56,
                        "job_to_board_nonempty": 18000.0,
                        "job_to_board_visible_partial": 18000.0,
                    },
                    "post_preview_finalization": {
                        "report_available": True,
                        "long_post_preview_finalization": True,
                        "preview_to_finalization_completed_ms": 192000.0,
                        "long_post_preview_finalization_threshold_ms": 30000.0,
                    },
                    "service_metrics": {
                        "user_experience": {
                            "long_finalization_after_preview": True,
                            "stage_1_preview_to_final_results_ms": 30458.56,
                            "thresholds_ms": {"stage_1_preview_to_final_results": 30000.0},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 41108.0},
                        "remote_to_next_submit_start_ms": {"max": 26956.0},
                        "local_completion_to_next_submit_start_ms": {"max": 0.0},
                        "next_submit_provider_attempt_elapsed_ms": {"max": 857.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 12000.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    warning_names = {item["name"] for item in report["known_acceptable_warnings"]}
    assert report["status"] == "blocked"
    assert {
        "workflow_job_to_stage_1_preview_too_slow_for_manual_signoff",
        "workflow_job_to_final_results_too_slow_for_manual_signoff",
        "event_efficiency_remote_to_local_marker_too_slow_for_manual_signoff",
        "event_efficiency_remote_to_next_submit_too_slow_for_manual_signoff",
        "event_efficiency_provider_slot_to_remote_wait_too_slow_for_manual_signoff",
    } <= finding_names
    assert "post_preview_finalization_timestamp_fallback_longer_than_user_visible_wall_clock" in warning_names
    assert "user_experience_finalization_exceeds_default_but_within_case_slo" in warning_names


def test_scripted_smoke_signoff_allows_missing_next_submit_metrics_when_not_applicable() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "lovable_small_company_live_roster",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed_job"},
                "expectations": {
                    "max_remote_to_next_submit_start_ms": 10000,
                    "max_local_to_next_submit_start_ms": 1000,
                    "max_next_submit_attempt_elapsed_ms": 10000,
                    "max_provider_slot_to_remote_wait_started_ms": 5000,
                },
                "provider_invocations": [
                    {
                        "logical_name": "harvest_profile_scraper_batch",
                        "provider_mode": "scripted",
                    }
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "event_level_efficiency": {
                        "report_available": True,
                        "next_submit_opportunity": {
                            "applicable": False,
                            "reason": "all_profile_urls_submitted_before_first_completion",
                            "metrics_required": False,
                        },
                        "provider_slot_to_remote_wait_started_ms": {"max": 0.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "event_efficiency_remote_to_next_submit_too_slow_for_manual_signoff_missing_metric" not in finding_names
    assert "event_efficiency_local_to_next_submit_too_slow_for_manual_signoff_missing_metric" not in finding_names
    assert "event_efficiency_next_submit_attempt_too_slow_for_manual_signoff_missing_metric" not in finding_names


def test_scripted_smoke_signoff_blocks_recovery_phase_contract_violations() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "review27_timeout_case",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                    },
                    "service_metrics": {
                        "user_experience": {
                            "workflow_wall_clock_ms": {
                                "job_to_stage_1_preview": 1000,
                                "job_to_final_results": 2000,
                            }
                        },
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "remote_to_local_marker_lag_ms": {"max": 0},
                                "provider_slot_to_remote_wait_started_ms": {"max": 0},
                            }
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": True,
                            "failed_phase_present": True,
                            "slow_phase_present": True,
                            "unexpected_enabled_phase_present": True,
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert {
        "recovery_phase_metrics_missing",
        "recovery_phase_failed",
        "recovery_phase_slow",
        "recovery_unexpected_enabled_phase",
    } <= finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_treats_clean_recovery_budget_yield_as_passed_gate() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "pressure_case_budget_yield",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                    },
                    "service_metrics": {
                        "user_experience": {
                            "workflow_wall_clock_ms": {
                                "job_to_stage_1_preview": 1000,
                                "job_to_final_results": 2000,
                            }
                        },
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "remote_to_local_marker_lag_ms": {"max": 0},
                                "provider_slot_to_remote_wait_started_ms": {"max": 0},
                            }
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "slow_total_phase_present": True,
                            "slow_total_phase_count": 1,
                            "recovery_tick_budget_exhausted_count": 1,
                            "budget_yield_next_tick_requested_count": 1,
                            "cooperative_budget_yield_count": 1,
                            "cooperative_budget_yield_present": True,
                            "budget_yield_attention_required": False,
                            "unexpected_enabled_phase_present": False,
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "recovery_phase_slow" not in {item["name"] for item in report["blocking_findings"]}
    assert "recovery_cooperative_budget_yield" in {
        item["gate"] for item in report["passed_gates"]
    }
    assert report["known_acceptable_warnings"] == []
    assert report["summary"]["cooperative_budget_yield_count"] == 1


def test_scripted_smoke_signoff_warns_on_dirty_recovery_budget_yield() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "pressure_case_dirty_budget_yield",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                    },
                    "service_metrics": {
                        "user_experience": {
                            "workflow_wall_clock_ms": {
                                "job_to_stage_1_preview": 1000,
                                "job_to_final_results": 2000,
                            }
                        },
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "remote_to_local_marker_lag_ms": {"max": 0},
                                "provider_slot_to_remote_wait_started_ms": {"max": 0},
                            }
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "slow_total_phase_present": True,
                            "slow_total_phase_count": 1,
                            "recovery_tick_budget_exhausted_count": 1,
                            "budget_yield_next_tick_requested_count": 0,
                            "cooperative_budget_yield_count": 0,
                            "cooperative_budget_yield_present": False,
                            "budget_yield_attention_required": True,
                            "unexpected_enabled_phase_present": False,
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "recovery_phase_slow" not in {item["name"] for item in report["blocking_findings"]}
    assert {item["name"] for item in report["known_acceptable_warnings"]} >= {
        "recovery_total_elapsed_slow",
        "recovery_tick_budget_exhausted",
    }


def test_scripted_smoke_signoff_treats_clean_recovery_handoff_yield_as_passed_gate() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "pressure_case_handoff_yield",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                    },
                    "service_metrics": {
                        "user_experience": {
                            "workflow_wall_clock_ms": {
                                "job_to_stage_1_preview": 1000,
                                "job_to_final_results": 2000,
                            }
                        },
                        "worker_timeline": {
                            "handoff_gap_ms": {
                                "remote_to_local_marker_lag_ms": {"max": 0},
                                "provider_slot_to_remote_wait_started_ms": {"max": 0},
                            }
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "slow_total_phase_present": False,
                            "recovery_tick_budget_exhausted_count": 0,
                            "durable_work_handoff_yield_count": 2,
                            "cooperative_handoff_yield_count": 2,
                            "unexpected_enabled_phase_present": False,
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "recovery_cooperative_handoff_yield" in {
        item["gate"] for item in report["passed_gates"]
    }
    assert report["known_acceptable_warnings"] == []
    assert report["summary"]["cooperative_handoff_yield_count"] == 2
    assert "recovery_durable_work_handoff_yield" not in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "passed"


def test_scripted_smoke_signoff_blocks_legacy_recovery_bridge_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_recovery_bridge_used",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "expectations": {"require_no_legacy_materialization_recovery_bridge": True},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                    "service_metrics": {
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "slow_total_phase_present": False,
                            "unexpected_enabled_phase_present": False,
                            "legacy_bridge_used_count": 1,
                            "legacy_bridge_used_present": True,
                            "legacy_bridge_used_phases": [
                                {
                                    "phase": "local_apply_backlog",
                                    "owner": "legacy_job_materialization_items",
                                }
                            ],
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "blocked"
    assert "legacy_materialization_recovery_bridge_used" in {
        item["name"] for item in report["blocking_findings"]
    }


def test_scripted_smoke_signoff_allows_no_legacy_recovery_bridge_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "typed_recovery_owner_only",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "expectations": {"require_no_legacy_materialization_recovery_bridge": True},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000,
                        "job_to_final_results": 2000,
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                    "service_metrics": {
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "missing_phase_present": False,
                            "failed_phase_present": False,
                            "slow_phase_present": False,
                            "slow_total_phase_present": False,
                            "unexpected_enabled_phase_present": False,
                            "legacy_bridge_used_count": 0,
                            "legacy_bridge_used_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "legacy_materialization_recovery_bridge_used" not in {
        item["name"] for item in report["blocking_findings"]
    }
    assert "recovery_phase_metrics" in {item["gate"] for item in report["passed_gates"]}


def test_scripted_smoke_signoff_blocks_recovery_without_phase_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "review19_missing_report_case",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "worker_recovery": [{"status": "completed"}],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "service_metrics": {
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "blocked"
    assert {
        item["name"] for item in report["blocking_findings"]
    } >= {"recovery_phase_metrics_report_missing"}


def test_scripted_smoke_signoff_does_not_treat_shared_signal_as_execution_evidence() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "review27_async_dispatch_without_metrics",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "remote_provider_event_driver": {
                    "events": [
                        {
                            "status": "accepted",
                            "source": "provider_webhook",
                            "recovery_count": 0,
                            "recovery_dispatch_count": 0,
                            "shared_recovery_signal_count": 1,
                        }
                    ]
                },
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "service_metrics": {
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "recovery_phase_metrics_report_missing" not in {
        item["name"] for item in report["blocking_findings"]
    }


def test_scripted_smoke_signoff_blocks_missing_parity_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "missing_parity",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_case_report": {
                    "service_metrics": {
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        }
                    }
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "board_runtime_state_parity_missing" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_profile_scheduler_violation_and_missing_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "scheduler_violation",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": True,
                            "small_normal_batch_without_reason_count": 1,
                        }
                    },
                },
            },
            {
                "case": "scheduler_missing",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                },
            },
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "profile_scheduler_contract_violation" in finding_names
    assert "profile_scheduler_contract_report_missing" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_partial_overlay_fast_path_fallback() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_pressure_overlay_fallback",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 2384},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                        "board_overlay_writes": {
                            "report_available": True,
                            "eligible_full_rebuild_fallback_present": True,
                            "eligible_full_rebuild_fallback_count": 1,
                            "fallback_reason_counts": {
                                "existing_overlay_record_unparseable": 1,
                            },
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "partial_overlay_fast_path_fallback" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_finalization_overlay_reuse_missed() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_pressure_finalization_rewrite",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 120},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                        "finalization_overlay": {
                            "report_available": True,
                            "reuse_eligible": True,
                            "reuse_used": False,
                            "eligible_full_rewrite_present": True,
                            "eligible_full_rewrite_count": 1,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "finalization_overlay_reuse_missed" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_warns_finalization_overlay_reuse_missed_when_canonical_cutover_is_clean() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_pressure_finalization_rewrite_legacy_cutover_clean",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "projection_cutover": {
                        "report_available": True,
                        "run_projection_link_present": True,
                        "projection_missing": False,
                        "legacy_public_reader_fallback_used": False,
                        "legacy_endpoint_normal_path_used": False,
                    },
                    "workflow_benchmark": {"fetched_profile_count": 120, "board_total_candidates": 8830},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "normal_path_write_count": 0,
                            "migration_adapter_write_count": 0,
                            "missing_contract_count": 0,
                        },
                        "recovery_phase_metrics": {
                            "report_available": True,
                            "legacy_bridge_used_count": 0,
                            "legacy_bridge_used_present": False,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "report_available": True,
                            "served_candidate_count": 8830,
                            "expected_candidate_count": 8830,
                            "delta_profile_required_count": 120,
                            "delta_profile_materialized_count": 120,
                            "delta_profile_board_visible_count": 120,
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                        "finalization_overlay": {
                            "report_available": True,
                            "reuse_eligible": True,
                            "reuse_used": False,
                            "eligible_full_rewrite_present": True,
                            "eligible_full_rewrite_count": 1,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "finalization_overlay_reuse_missed" not in {
        item["name"] for item in report["blocking_findings"]
    }
    assert "finalization_overlay_reuse_missed" in {
        item["name"] for item in report["known_acceptable_warnings"]
    }


def test_scripted_smoke_signoff_blocks_finalization_overlay_candidate_count_drift() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "google_pressure_finalization_count_drift",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 120},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                        "finalization_overlay": {
                            "report_available": True,
                            "reuse_eligible": True,
                            "reuse_used": True,
                            "eligible_full_rewrite_present": False,
                            "overlay_candidate_count": 7384,
                            "candidate_source_count": 7381,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "finalization_overlay_candidate_count_drift" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_missing_partial_overlay_write_mode_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_overlay_report_missing",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 10},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                            "overlay_write_mode_missing_present": True,
                            "overlay_write_mode_missing_count": 1,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "partial_overlay_write_mode_report_missing" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_incomplete_post_profile_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "post_profile_incomplete",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert "post_profile_completion_report_incomplete" in finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_projection_cutover_fallbacks() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "projection_cutover_fallback",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_projection_cutover_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "projection_cutover": {
                        "run_projection_link_present": False,
                        "projection_missing": True,
                        "legacy_public_reader_fallback_used": True,
                        "legacy_endpoint_normal_path_used": True,
                        "legacy_target_candidate_export_normal_path_used": True,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert {
        "legacy_public_reader_fallback_used",
        "canonical_projection_missing",
        "legacy_endpoint_normal_path_used",
        "legacy_target_candidate_export_normal_path_used",
    } <= finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_passes_clean_projection_cutover_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "projection_cutover_clean",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_projection_cutover_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "projection_cutover": {
                        "run_projection_link_present": True,
                        "projection_missing": False,
                        "legacy_public_reader_fallback_used": False,
                        "legacy_endpoint_normal_path_used": False,
                        "legacy_target_candidate_export_normal_path_used": False,
                    },
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert "projection_cutover" in {item["gate"] for item in report["passed_gates"]}
    assert report["summary"]["projection_cutover_report_count"] == 1


def test_scripted_smoke_signoff_blocks_legacy_materialization_normal_writes_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_materialization_normal_write",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_no_legacy_materialization_normal_writes": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "item_count": 2,
                            "normal_path_write_count": 1,
                            "migration_adapter_write_count": 1,
                            "missing_contract_count": 0,
                            "normal_path_kind_counts": {"board_visible_delta_apply": 1},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "legacy_materialization_normal_write_used" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_allows_migration_adapter_legacy_materialization_writes() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_materialization_migration_adapter",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {
                    "require_no_legacy_materialization_normal_writes": True,
                    "require_legacy_materialization_write_contract_report": True,
                },
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "item_count": 1,
                            "normal_path_write_count": 0,
                            "migration_adapter_write_count": 1,
                            "missing_contract_count": 0,
                            "migration_adapter_kind_counts": {"local_apply_closure": 1},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "legacy_materialization_write_contract" in {item["gate"] for item in report["passed_gates"]}


def test_scripted_smoke_signoff_blocks_workflow_causality_contract_violation_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "workflow_causality_missing",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_workflow_causality_contract": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "workflow_causality_contract": {
                            "report_available": True,
                            "command_count": 1,
                            "checked_command_count": 1,
                            "missing_envelope_count": 1,
                            "violation_detected": True,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "workflow_causality_contract_violation" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_durable_command_owner_contract_violation_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "durable_command_owner_drift",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_durable_command_owner_contracts": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "durable_command_owner_contracts": {
                            "report_available": True,
                            "checked_command_count": 1,
                            "invalid_owner_count": 1,
                            "incomplete_causality_count": 1,
                            "violation_detected": True,
                            "contracts": {
                                "projection_export_generate": {
                                    "command_type": "export.projection.generate",
                                    "expected_owner": "projection_exporter",
                                    "command_count": 1,
                                    "invalid_owner_count": 1,
                                    "incomplete_causality_count": 1,
                                }
                            },
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "durable_command_owner_contract_violation" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["summary"]["durable_command_owner_contract_report_count"] == 1
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_incomplete_durable_command_control_policy_when_required() -> None:
    policy = _projection_export_control_policy()
    policy["running_resume_blocked_reason"] = "owner_specific_resume_not_implemented"
    policy["running_resume_upgrade_requirements"] = []
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "durable_command_control_policy_drift",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_durable_command_owner_contracts": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "durable_command_owner_contracts": {
                            "report_available": True,
                            "checked_command_count": 1,
                            "invalid_owner_count": 0,
                            "incomplete_causality_count": 0,
                            "violation_detected": False,
                            "contracts": {
                                "projection_export_generate": {
                                    "command_type": "export.projection.generate",
                                    "expected_owner": "projection_exporter",
                                    "control_policy": policy,
                                    "display_contract": _projection_export_display_contract(),
                                    "activity_spine_policy": _projection_export_activity_spine_policy(),
                                    "command_count": 1,
                                    "expected_owner_count": 1,
                                    "invalid_owner_count": 0,
                                    "incomplete_causality_count": 0,
                                }
                            },
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "durable_command_control_policy_contract_violation" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_passes_durable_command_owner_contract_when_clean() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "durable_command_owner_clean",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_durable_command_owner_contracts": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "durable_command_owner_contracts": {
                            "report_available": True,
                            "checked_command_count": 1,
                            "invalid_owner_count": 0,
                            "incomplete_causality_count": 0,
                            "violation_detected": False,
                            "contracts": {
                                "projection_export_generate": {
                                    "command_type": "export.projection.generate",
                                    "expected_owner": "projection_exporter",
                                    "control_policy": _projection_export_control_policy(),
                                    "display_contract": _projection_export_display_contract(),
                                    "activity_spine_policy": _projection_export_activity_spine_policy(),
                                    "command_count": 1,
                                    "expected_owner_count": 1,
                                    "invalid_owner_count": 0,
                                    "incomplete_causality_count": 0,
                                }
                            },
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "durable_command_owner_contracts" in {item["gate"] for item in report["passed_gates"]}


def test_scripted_smoke_signoff_blocks_legacy_public_web_retirement_rows_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_public_web_rows",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_legacy_public_web_retirement_ready": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_public_web_retirement": {
                            "report_available": True,
                            "contract_version": "legacy_public_web_retirement_audit_v1",
                            "status": "blocked",
                            "legacy_rows_present": True,
                            "legacy_audit_limited": False,
                            "legacy_target_candidate_public_web_row_count": 1,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "legacy_public_web_rows_present" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["summary"]["legacy_public_web_retirement_report_count"] == 1
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_passes_legacy_public_web_retirement_when_clean() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_public_web_clean",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_legacy_public_web_retirement_ready": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_public_web_retirement": {
                            "report_available": True,
                            "contract_version": "legacy_public_web_retirement_audit_v1",
                            "status": "ready_for_physical_deletion",
                            "legacy_rows_present": False,
                            "legacy_audit_limited": False,
                            "legacy_target_candidate_public_web_row_count": 0,
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "legacy_public_web_retirement" in {item["gate"] for item in report["passed_gates"]}


def test_scripted_smoke_signoff_blocks_post_profile_heuristic_slo_pairing_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "post_profile_heuristic_pairing",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_no_post_profile_heuristic_slo_pairing": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {
                                "pairing_contract": "typed_causal_group_for_workflow_commands",
                                "heuristic_pairing_used": True,
                                "legacy_snapshot_pair_count": 1,
                                "elapsed_ms": {"max": 20},
                            },
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "post_profile_heuristic_slo_pairing_used" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_missing_legacy_materialization_contract_when_required() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_materialization_missing_contract",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_legacy_materialization_write_contract_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "legacy_materialization_write_contract": {
                            "report_available": True,
                            "item_count": 1,
                            "normal_path_write_count": 0,
                            "migration_adapter_write_count": 0,
                            "missing_contract_count": 1,
                            "missing_contract_kind_counts": {"snapshot_full_materialization": 1},
                        },
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "legacy_materialization_write_contract_missing" in {
        item["name"] for item in report["blocking_findings"]
    }
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_legacy_artifact_terminal_drift() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_artifact_terminal_drift",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_legacy_artifact_coherence_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "legacy_artifact_coherence": {
                        "report_available": True,
                        "canonical_job_status": "running",
                        "canonical_job_stage": "retrieving",
                        "progress_status": "running",
                        "progress_stage": "retrieving",
                        "legacy_artifact_status": "completed",
                        "legacy_artifact_stage": "completed",
                        "terminal_drift_detected": True,
                        "blocking_violation": True,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "blocked"
    assert "legacy_artifact_terminal_drift" in {item["name"] for item in report["blocking_findings"]}
    assert report["summary"]["legacy_artifact_coherence_report_count"] == 1


def test_scripted_smoke_signoff_passes_clean_legacy_artifact_coherence_report() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "legacy_artifact_coherence_clean",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_legacy_artifact_coherence_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "legacy_artifact_coherence": {
                        "report_available": True,
                        "canonical_job_status": "completed",
                        "canonical_job_stage": "completed",
                        "progress_status": "completed",
                        "progress_stage": "completed",
                        "legacy_artifact_status": "completed",
                        "legacy_artifact_stage": "completed",
                        "terminal_drift_detected": False,
                        "blocking_violation": False,
                    },
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        },
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert "legacy_artifact_coherence" in {item["gate"] for item in report["passed_gates"]}
    assert report["summary"]["legacy_artifact_coherence_report_count"] == 1


def test_scripted_smoke_signoff_passes_target_public_web_owner_backend_contract() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "crm_public_web_owned_execution",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "target_public_web_action": {
                    "status": "completed",
                    "search": {"batch_id": "crm-public-web-batch-1"},
                },
                "expectations": {
                    "require_crm_public_web_storage_owner": True,
                    "require_crm_public_web_queue_batch_command": True,
                    "require_public_web_execution_backend_report": True,
                    "max_target_public_web_legacy_storage_owner_batch_count": 0,
                },
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "target_candidate_public_web": {
                            "report_available": True,
                            "batch_count": 1,
                            "crm_storage_owner_batch_count": 1,
                            "legacy_storage_owner_batch_count": 0,
                            "execution_backend_bridge_present": False,
                            "execution_backend_counts": {"crm_public_web_v1": 1},
                            "queue_batch_command_count": 1,
                            "queue_batch_command_succeeded_count": 1,
                            "queue_batch_command_expected_owner_count": 1,
                            "queue_batch_command_invalid_owner_count": 0,
                            "queue_batch_command_incomplete_causality_count": 0,
                            "queue_batch_command_status_counts": {"succeeded": 1},
                            "queue_batch_command_owner_counts": {"crm_public_web_owner": 1},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert report["status"] == "passed"
    assert report["blocking_findings"] == []
    assert "target_public_web_contract" in {item["gate"] for item in report["passed_gates"]}
    assert report["summary"]["target_public_web_contract_report_count"] == 1
    assert report["known_acceptable_warnings"] == []


def test_scripted_smoke_signoff_blocks_target_public_web_legacy_owner_or_missing_backend() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "crm_public_web_contract_dirty",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "target_public_web_action": {
                    "status": "completed",
                    "search": {"batch_id": "legacy-public-web-batch-1"},
                },
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_wall_clock_ms": {
                        "job_to_stage_1_preview": 1000.0,
                        "job_to_final_results": 2000.0,
                    },
                    "service_metrics": {
                        "target_candidate_public_web": {
                            "report_available": True,
                            "batch_count": 1,
                            "crm_storage_owner_batch_count": 0,
                            "legacy_storage_owner_batch_count": 1,
                            "execution_backend_counts": {},
                            "queue_batch_command_count": 0,
                            "queue_batch_command_succeeded_count": 0,
                            "queue_batch_command_expected_owner_count": 0,
                            "queue_batch_command_invalid_owner_count": 0,
                            "queue_batch_command_incomplete_causality_count": 0,
                            "queue_batch_command_missing": True,
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "remote_to_local_marker_lag_ms": {"max": 1000.0},
                        "provider_slot_to_remote_wait_started_ms": {"max": 200.0},
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    finding_names = {item["name"] for item in report["blocking_findings"]}
    assert {
        "crm_public_web_storage_owner_missing",
        "legacy_public_web_storage_owner_used",
        "target_public_web_execution_backend_report_missing",
        "crm_public_web_queue_batch_command_missing",
        "crm_public_web_queue_batch_owner_missing",
    } <= finding_names
    assert report["status"] == "blocked"


def test_scripted_smoke_signoff_blocks_required_projection_cutover_report_missing() -> None:
    report = build_scripted_smoke_signoff_report(
        records=[
            {
                "case": "projection_cutover_missing",
                "final": {"smoke_ready": True, "smoke_completion_state": "completed"},
                "provider_invocations": [
                    {"logical_name": "harvest_profile_scraper_batch", "provider_mode": "scripted"}
                ],
                "expectations": {"require_projection_cutover_report": True},
                "provider_case_report": {
                    "board_runtime_state_parity": {"report_available": True, "consistent": True},
                    "workflow_benchmark": {"fetched_profile_count": 1},
                    "service_metrics": {
                        "post_profile_completion": {
                            "report_available": True,
                            "slo_violation_detected": False,
                            "url_terminal_state_recording": {"terminal_queue_state_leak_count": 0},
                            "event_level_callback": {"elapsed_ms": {"max": 10}},
                            "profile_file_visible_to_board_patch_visible": {"elapsed_ms": {"max": 20}},
                            "all_profiles_fetched_to_all_cards_visible": {"elapsed_ms": {"max": 30}},
                        },
                        "board_visible_projection": {
                            "projection_missing_for_visible_count": False,
                            "patch_log_replay_lag": False,
                        },
                    },
                    "event_level_efficiency": {
                        "profile_scheduler_contract": {
                            "report_available": True,
                            "violation_detected": False,
                        }
                    },
                },
            }
        ],
        expected_provider_mode="scripted",
    )

    assert "projection_cutover_report_missing" in {item["name"] for item in report["blocking_findings"]}
    assert report["status"] == "blocked"
