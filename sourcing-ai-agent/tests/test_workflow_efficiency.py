from datetime import datetime

from sourcing_agent.workflow_efficiency import (
    aggregate_event_level_efficiency_metrics,
    event_level_efficiency_runtime_subset,
    extract_event_level_efficiency_metrics,
)


def test_event_level_efficiency_tracks_remote_marker_next_submit_and_provider_slots() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:01+00:00",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:02.100000+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:02.250000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 150,
                        "post_ingest_prefetch_candidate_count": 10,
                        "post_ingest_prefetch_dispatched_url_count": 2,
                        "registry_cache_marker_count": 4,
                    },
                },
            },
        ],
        workers=[
            {
                "worker_id": 201,
                "status": "running",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-active",
                    "provider_limiter_lease": {
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_token": "lease-1",
                        "budget": 4,
                        "active_count": 2,
                    },
                },
            },
            {
                "worker_id": 202,
                "status": "running",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "submitting_remote_harvest",
                    "provider_limiter_lease": {
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_token": "lease-2",
                        "budget": 4,
                        "active_count": 2,
                    },
                },
            }
        ],
    )

    assert report["remote_provider_event_count"] == 1
    assert report["harvest_completion_event_count"] == 1
    assert report["remote_to_local_event_lag_ms"]["max"] == 1000
    assert report["remote_to_local_marker_lag_ms"]["max"] == 1000
    assert report["remote_to_next_submit_start_ms"]["max"] == 2100
    segments = report["remote_to_next_submit_segments_ms"]
    assert segments["remote_completed_to_event_seen_ms"]["max"] == 1000
    assert segments["event_seen_to_completion_marker_ms"]["max"] == 1000
    assert segments["completion_marker_to_next_submit_start_ms"]["max"] == 100
    assert segments["event_seen_to_next_submit_start_ms"]["max"] == 1100
    assert segments["event_seen_to_next_submit_finish_ms"]["max"] == 1250
    assert segments["next_submit_provider_attempt_elapsed_ms"]["max"] == 150
    assert report["local_completion_to_next_submit_start_ms"]["max"] == 100
    assert report["local_to_next_submit_start_ms"]["max"] == 100
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 150
    assert report["next_submit_attempt_elapsed_ms"]["max"] == 150
    assert report["post_ingest_prefetch_dispatched_url_count"] == 2
    assert report["registry_cache_marker_count"] == 4
    assert report["profile_batch_envelopes"]["envelope_count"] == 1
    assert report["profile_batch_envelopes"]["tiny_batch_count"] == 1
    assert report["profile_batch_envelopes"]["unexplained_tiny_batch_count"] == 0
    assert report["provider_slots"]["true_active_provider_slot_worker_count"] == 2
    assert report["provider_slots"]["remote_actor_worker_count"] == 1
    assert report["provider_slots"]["pre_submit_provider_worker_count"] == 0
    assert report["violation_detected"] is False
    assert report["diagnostic_violation_detected"] is False

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["remote_to_next_submit_start_ms"]["max"] == 2100
    aggregate_segments = aggregate["remote_to_next_submit_segments_ms"]
    assert aggregate_segments["event_seen_to_completion_marker_ms"]["max"] == 1000
    assert aggregate_segments["event_seen_to_next_submit_start_ms"]["max"] == 1100


def test_event_level_efficiency_treats_repeated_materialize_as_diagnostic_contract() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Snapshot materialization completed.",
                "payload": {
                    "snapshot_id": "snap-1",
                    "worker_ids": [301],
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Snapshot materialization completed.",
                "payload": {
                    "snapshot_id": "snap-1",
                    "worker_ids": [301],
                },
            },
        ],
        workers=[],
    )

    assert report["reconcile"]["repeated_materialize_signature_count"] == 1
    assert report["violation_detected"] is False

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["repeated_materialize_signature_count"] == 1
    assert aggregate["violation_detected"] is False


def test_event_level_efficiency_treats_same_worker_reconcile_repeat_as_diagnostic_contract() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed retrieval results after worker recovery.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301]},
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed retrieval results after worker recovery.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301]},
            },
        ],
        workers=[],
    )

    assert report["reconcile"]["same_worker_reconcile_repeat_count"] == 1
    assert report["violation_detected"] is False


def test_event_level_efficiency_splits_aggregate_and_provider_worker_batch_size() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 2384,
                        "requested_url_count": 2384,
                    }
                },
            }
        ],
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "worker_key": "harvest_profile_batch::a",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {"summary": {"requested_url_count": 529}},
            },
            {
                "worker_id": 2,
                "status": "completed",
                "worker_key": "harvest_profile_batch::b",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {"summary": {"requested_url_count": 619}},
            },
        ],
    )

    batch_report = report["profile_batch_envelopes"]
    assert batch_report["batch_size"]["max"] == 2384.0
    assert batch_report["provider_worker_batch_size"]["max"] == 619.0

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["profile_batch_envelopes"]["batch_size"]["max"] == 2384.0
    assert aggregate["profile_batch_envelopes"]["provider_worker_batch_size"]["max"] == 619.0


def test_event_level_efficiency_uses_refill_daemon_dispatch_for_signal_only_callbacks() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 5,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:03+00:00",
                    "finished_at": "2026-04-27T00:00:03.250000+00:00",
                    "elapsed_ms": 250,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "available_slot_count": 1,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 50,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 3000
    assert report["local_to_next_submit_start_ms"]["max"] == 1000
    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 250


def test_event_level_efficiency_uses_main_profile_refill_phase_as_handoff_dispatch() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:27+00:00",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 0,
                        "queued_worker_count": 0,
                        "profile_prefetch_queue": {"requested_url_count": 120},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch_refill_phase": "profile_prefetch_refill",
                    "started_at": "2026-04-27T00:00:03+00:00",
                    "finished_at": "2026-04-27T00:00:03.120000+00:00",
                    "elapsed_ms": 120,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "available_slot_count": 1,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 50,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 3000
    assert report["local_to_next_submit_start_ms"]["max"] == 1000
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 120
    assert dict(report.get("next_submit_opportunity") or {}).get("reason") == "next_submit_samples_observed"


def test_event_level_efficiency_uses_nested_profile_prefetch_dispatch_for_signal_only_callbacks() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "blocked",
                "detail": "Enrich LinkedIn profiles queued a background Harvest profile prefetch worker.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 40,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-27T00:00:03+00:00",
                            "prefetch_finished_at": "2026-04-27T00:00:03.100000+00:00",
                            "prefetch_elapsed_ms": 100,
                        },
                        "batch_plan": {
                            "available_slot_count": 2,
                            "planned_new_worker_count": 1,
                            "planned_dispatch_item_count": 40,
                        },
                    }
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 3000
    assert report["local_to_next_submit_start_ms"]["max"] == 1000
    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 100
    assert report["profile_prefetch_batch_plan"]["plan_count"] == 1
    assert report["profile_prefetch_refill"]["dispatched_url_count"] == 40


def test_event_level_efficiency_counts_phase_b_as_dispatch_not_completion_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background company-roster workers were incrementally applied to the live snapshot.",
                "payload": {
                    "pipeline_order": "company_roster_apply_to_profile_prefetch_before_materialization",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-27T00:00:03+00:00",
                            "prefetch_finished_at": "2026-04-27T00:00:03.125000+00:00",
                            "prefetch_elapsed_ms": 125,
                        },
                        "batch_plan": {
                            "requested_url_count": 50,
                            "available_slot_count": 1,
                            "planned_new_worker_count": 1,
                            "planned_dispatch_item_count": 50,
                        },
                        "profile_prefetch_queue": {"requested_url_count": 50},
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 125
    assert report["profile_prefetch_refill"]["dispatched_url_count"] == 50
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["metrics_required"] is False
    assert opportunity["reason"] == "all_profile_urls_submitted_by_non_handoff_dispatch"
    assert opportunity["legacy_reason"] == "all_profile_urls_submitted_by_discovery_append"


def test_event_level_efficiency_excludes_stage1_prefetch_elapsed_from_next_submit_slo() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background company-roster workers were incrementally applied to the live snapshot.",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-27T00:00:00+00:00",
                            "prefetch_finished_at": "2026-04-27T00:00:11.370000+00:00",
                            "prefetch_elapsed_ms": 11370,
                        },
                        "batch_plan": {
                            "available_slot_count": 2,
                            "planned_new_worker_count": 1,
                            "planned_dispatch_item_count": 50,
                        },
                    }
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile refill daemon dispatched the next ready batch.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "dispatched_url_count": 35,
                    "queued_worker_count": 1,
                    "elapsed_ms": 2036,
                    "batch_plan": {
                        "available_slot_count": 2,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 35,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 2036
    assert report["profile_batch_envelopes"]["envelope_count"] == 2
    assert report["profile_prefetch_refill"]["dispatched_url_count"] == 85


def test_event_level_efficiency_ignores_non_refill_prefetch_as_signal_only_submit_anchor() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:02+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background company-roster workers were incrementally applied to the live snapshot.",
                "payload": {
                    "profile_prefetch": {
                        "status": "completed",
                        "dispatched_url_count": 47,
                        "queued_worker_count": 0,
                        "metrics": {
                            "prefetch_started_at": "2026-04-27T00:00:01+00:00",
                            "prefetch_finished_at": "2026-04-27T00:01:06.537000+00:00",
                            "prefetch_elapsed_ms": 65537,
                        },
                    }
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:03+00:00",
                    "finished_at": "2026-04-27T00:00:03.250000+00:00",
                    "elapsed_ms": 250,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 3000
    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 250


def test_event_level_efficiency_uses_in_flight_remote_completion_as_handoff_anchor() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received local watcher event before remote terminal timestamp was available.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": None,
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "detail": "Received duplicate remote provider event while recovery already holds the worker lease.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:05+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:06+00:00",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:07+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:07+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:07+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "blocked",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-27T00:00:09+00:00",
                            "prefetch_finished_at": "2026-04-27T00:00:09.050000+00:00",
                            "prefetch_elapsed_ms": 50,
                        },
                        "batch_plan": {
                            "available_slot_count": 1,
                            "planned_new_worker_count": 1,
                            "planned_dispatch_item_count": 50,
                        },
                    }
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 4000
    assert report["remote_to_local_marker_lag_ms"]["max"] == 1000
    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 50


def test_event_level_efficiency_records_zero_start_lag_for_already_running_submit() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "detail": "Received duplicate remote provider event while matching worker recovery already holds an active lease.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:05+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:06+00:00",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:08+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:08+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:08+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:03+00:00",
                    "finished_at": "2026-04-27T00:00:06+00:00",
                    "elapsed_ms": 3000,
                    "provider_submit_elapsed_ms": 250,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 0
    assert report["remote_to_next_submit_finish_ms"]["max"] == 1000
    assert report["next_submit_attempt_elapsed_ms"]["max"] == 250
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 3000


def test_event_level_efficiency_skips_signal_only_completion_superseded_before_dispatch() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "First profile completion only signaled the refill daemon while peer work was still in flight.",
                "payload": {
                    "worker_ids": [201],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:00+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Second profile completion made the deferred tail actionable.",
                "payload": {
                    "worker_ids": [202],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:03+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:03+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:03+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile refill daemon dispatched the now-actionable tail batch.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:03.500000+00:00",
                    "finished_at": "2026-04-27T00:00:03.700000+00:00",
                    "elapsed_ms": 200,
                    "dispatched_url_count": 15,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "available_slot_count": 1,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 15,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["local_to_next_submit_start_ms"]["max"] == 500
    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 200
    assert report["violation_detected"] is False


def test_event_level_efficiency_does_not_pair_pre_worker_refill_after_new_remote_terminal_event() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [201],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [201],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [202],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:10+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:10+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch_refill_phase": "pre_worker_profile_prefetch_refill",
                    "status": "queued",
                    "started_at": "2026-04-27T00:03:43+00:00",
                    "finished_at": "2026-04-27T00:03:44+00:00",
                    "elapsed_ms": 1000,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "plan_reason": "ready_to_dispatch",
                        "planned_dispatch_item_count": 50,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 1000
    assert report["violation_detected"] is False


def test_event_level_efficiency_does_not_count_post_event_level_refill_as_completion_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [201],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [201],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch_refill_phase": "post_event_level_profile_prefetch_refill",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:03+00:00",
                    "finished_at": "2026-04-27T00:00:04+00:00",
                    "elapsed_ms": 1000,
                    "provider_submit_elapsed_ms": 75,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "plan_reason": "ready_to_dispatch",
                        "planned_dispatch_item_count": 50,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 75
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 1000
    assert report["violation_detected"] is False


def test_event_level_efficiency_pairs_signal_only_dispatch_that_started_before_local_apply() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:01+00:00",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-27T00:00:01.500000+00:00",
                    "finished_at": "2026-04-27T00:00:03+00:00",
                    "elapsed_ms": 1500,
                    "dispatched_url_count": 50,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "available_slot_count": 1,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 50,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:04+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:04+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:04+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                        "post_ingest_prefetch_elapsed_ms": 5,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 1500
    assert report["local_to_next_submit_start_ms"]["max"] == 0


def test_event_level_efficiency_uses_first_local_response_for_duplicate_worker_completion_events() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:01+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:01.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
            {
                "stage": "completed",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101, 202],
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:45+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:45+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:45.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_local_marker_lag_ms"]["max"] == 0
    assert report["remote_to_next_submit_start_ms"]["max"] == 1000
    assert report["violation_detected"] is False


def test_event_level_efficiency_uses_event_lag_when_seen_timestamp_loses_precision() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00.321+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 321,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:12+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:12+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:12.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_local_marker_lag_ms"]["max"] == 321
    assert report["remote_to_next_submit_segments_ms"]["event_seen_to_completion_marker_ms"]["max"] == 12000


def test_event_level_efficiency_ignores_in_flight_duplicate_remote_events_for_actionable_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": None,
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "detail": "Received remote provider event for run run-1 while worker was already in flight.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:01+00:00",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:01:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:01:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:01:00+00:00",
                        "post_ingest_prefetch_elapsed_ms": 0,
                        "next_submit_attempt_semantics": "refill_daemon_signal_not_provider_submit",
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_provider_event_count"] == 2
    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["local_to_next_submit_start_ms"]["max"] == 0
    assert report["violation_detected"] is False


def test_event_level_efficiency_separates_terminal_marker_from_completion_callback_lag() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:06+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:06+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:06.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_local_marker_lag_ms"]["max"] == 0
    assert report["remote_to_next_submit_segments_ms"]["event_seen_to_completion_marker_ms"]["max"] == 6000
    assert report["remote_to_next_submit_start_ms"]["max"] == 6000
    assert report["local_to_next_submit_start_ms"]["max"] == 0
    assert report["violation_detected"] is False
    assert report["diagnostic_violation_detected"] is False


def test_event_level_efficiency_dedupes_same_remote_run_marker_lag_across_sources() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received local watcher event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "local_provider_event_watcher",
                        "remote_completed_at": "2026-05-11T14:48:17.342+00:00",
                        "local_event_seen_at": "2026-05-11T14:48:17+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "detail": "Received duplicate webhook event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "provider_webhook",
                        "remote_completed_at": "2026-05-11T14:48:17.342000+00:00",
                        "local_event_seen_at": "2026-05-11T14:49:01+00:00",
                        "remote_to_local_event_lag_ms": 43658,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-05-11T14:48:20+00:00",
                        "next_submit_attempt_started_at": "2026-05-11T14:48:20+00:00",
                        "next_submit_attempt_finished_at": "2026-05-11T14:48:20.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_provider_event_count"] == 2
    assert report["remote_to_local_marker_lag_ms"]["max"] == 0
    assert report["remote_to_next_submit_start_ms"]["max"] == 2658


def test_event_level_efficiency_uses_first_terminal_marker_before_late_duplicates() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "event_id": 20,
                "stage": "remote_provider_event",
                "status": "received_late",
                "detail": "Received late duplicate terminal event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "local_provider_event_watcher",
                        "remote_completed_at": "2026-05-12T14:47:00.330+00:00",
                        "local_event_seen_at": "2026-05-12T14:47:15+00:00",
                        "remote_to_local_event_lag_ms": 14670,
                    },
                },
            },
            {
                "event_id": 10,
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "detail": "Received first duplicate terminal marker while recovery was in flight.",
                "payload": {
                    "target_worker_ids": [101],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "local_provider_event_watcher",
                        "remote_completed_at": "2026-05-12T14:47:00.330+00:00",
                        "local_event_seen_at": "2026-05-12T14:47:03+00:00",
                        "remote_to_local_event_lag_ms": 2670,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-05-12T14:47:10+00:00",
                        "next_submit_attempt_started_at": "2026-05-12T14:47:10+00:00",
                        "next_submit_attempt_finished_at": "2026-05-12T14:47:10.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_provider_event_count"] == 2
    assert report["late_remote_provider_event_count"] == 1
    assert report["remote_to_local_event_lag_ms"]["max"] == 14670
    assert report["remote_to_local_marker_lag_ms"]["max"] == 2670
    assert report["remote_to_next_submit_segments_ms"]["event_seen_to_completion_marker_ms"]["max"] == 7000
    assert report["diagnostic_violation_detected"] is False


def test_event_level_efficiency_flags_remote_to_next_submit_hard_violation() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-1.",
                "payload": {
                    "target_worker_ids": [101],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:00+00:00",
                        "local_event_seen_at": "2026-04-27T00:00:00+00:00",
                        "remote_to_local_event_lag_ms": 0,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:31+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:31+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:00:31.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 31000
    assert report["violation_detected"] is True
    assert report["violation_count"] == 1
    assert report["diagnostic_violation_detected"] is False


def test_event_level_efficiency_flags_batch_envelope_underuse_and_unexplained_tiny_batch() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "profile_refill_trigger": {
                        "kind": "profile_prefetch_refill_trigger",
                        "schema_version": 1,
                        "trigger_kind": "provider_completion",
                        "trigger_reason": "provider_completion",
                        "trigger_source": "worker_completion_callback",
                        "item_store": "linkedin_profile_registry",
                        "snapshot_id": "snapshot-openai-infra",
                        "status": "queued",
                        "requested_url_count": 89,
                        "dispatched_url_count": 3,
                        "queued_worker_count": 1,
                        "deferred_url_count": 54,
                        "available_slot_count": 2,
                        "planned_new_worker_count": 1,
                        "planned_dispatch_item_count": 35,
                        "planned_deferred_item_count": 54,
                        "unfilled_available_slot_count": 1,
                        "underfilled_with_deferred_items": True,
                        "refill_saturation": "underfilled_with_deferred_items",
                        "elapsed_ms": 100,
                    },
                    "profile_prefetch": {
                        "requested_url_count": 89,
                        "dispatched_url_count": 3,
                        "deferred_url_count": 54,
                        "tiny_batch_coalesced_count": 2,
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "plan_reason": "ready_to_dispatch",
                            "requested_url_count": 89,
                            "candidate_count": 89,
                            "queue_item_count": 89,
                            "planned_dispatch_worker_count": 1,
                            "planned_dispatch_item_count": 35,
                            "planned_deferred_item_count": 54,
                            "available_slot_count": 2,
                            "planned_new_worker_count": 1,
                            "unfilled_available_slot_count": 1,
                            "underfilled_with_deferred_items": True,
                            "refill_saturation": "underfilled_with_deferred_items",
                            "original_dispatch_chunk_count": 3,
                            "coalesced_dispatch_chunk_count": 2,
                            "tiny_batch_coalesced_count": 2,
                        },
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 89,
                            "cached_url_count": 10,
                            "ready_url_count": 79,
                            "queued_url_count": 3,
                            "deferred_url_count": 54,
                            "failed_url_count": 0,
                            "pending_url_count": 57,
                            "queue_quiescent": False,
                            "oldest_pending_item_age_ms": 2500,
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "batch_size": 3,
                                "dispatched_url_count": 3,
                                "requested_url_count": 89,
                                "deferred_url_count": 54,
                                "actor_budget": 4,
                                "submit_budget": 4,
                                "active_worker_count_before_dispatch": 0,
                                "queued_worker_count_after_dispatch": 1,
                                "is_tiny_batch": True,
                                "tiny_batch_allowed": False,
                                "small_batch_reason": "unexplained_tiny_batch",
                                "provider_slot_underuse_with_backlog": True,
                                "underuse_reason": "idle_actor_slots_with_deferred_profile_urls",
                            }
                        ],
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00.050000+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                        "post_ingest_prefetch_candidate_count": 89,
                        "post_ingest_prefetch_dispatched_url_count": 3,
                    },
                },
            }
        ],
        workers=[],
    )

    batch_report = report["profile_batch_envelopes"]
    assert batch_report["envelope_count"] == 1
    assert batch_report["batch_size"]["max"] == 3
    assert batch_report["tiny_batch_count"] == 1
    assert batch_report["unexplained_tiny_batch_count"] == 1
    assert batch_report["provider_slot_underuse_with_backlog_count"] == 1
    assert batch_report["tiny_batch_coalesced_count"] == 2
    assert batch_report["samples"][0]["underuse_reason"] == "idle_actor_slots_with_deferred_profile_urls"
    queue_report = report["profile_prefetch_queue"]
    assert queue_report["snapshot_count"] == 1
    assert queue_report["requested_url_count"] == 89
    assert queue_report["cached_url_count"] == 10
    assert queue_report["ready_url_count"] == 79
    assert queue_report["queued_url_count"] == 3
    assert queue_report["deferred_url_count"] == 54
    assert queue_report["pending_url_count"] == 57
    assert queue_report["non_quiescent_snapshot_count"] == 1
    assert queue_report["oldest_pending_item_age_ms"]["max"] == 2500
    plan_report = report["profile_prefetch_batch_plan"]
    assert plan_report["plan_count"] == 1
    assert plan_report["queue_item_count"] == 89
    assert plan_report["planned_dispatch_item_count"] == 35
    assert plan_report["planned_deferred_item_count"] == 54
    assert plan_report["available_slot_count"] == 2
    assert plan_report["planned_new_worker_count"] == 1
    assert plan_report["unfilled_available_slot_count"] == 1
    assert plan_report["underfilled_with_deferred_items_count"] == 1
    assert plan_report["samples"][0]["item_store"] == "linkedin_profile_registry"
    assert plan_report["samples"][0]["refill_saturation"] == "underfilled_with_deferred_items"
    refill_report = report["profile_prefetch_refill"]
    assert refill_report["trigger_count"] == 1
    assert refill_report["dispatched_url_count"] == 3
    assert refill_report["available_slot_count"] == 2
    assert refill_report["planned_new_worker_count"] == 1
    assert refill_report["planned_dispatch_item_count"] == 35
    assert refill_report["planned_deferred_item_count"] == 54
    assert refill_report["unfilled_available_slot_count"] == 1
    assert refill_report["underfilled_with_deferred_items_count"] == 1
    assert refill_report["samples"][0]["trigger_kind"] == "provider_completion"
    assert refill_report["samples"][0]["refill_saturation"] == "underfilled_with_deferred_items"
    assert report["violation_detected"] is True
    assert report["violation_count"] == 3
    assert report["profile_scheduler_contract"]["batch_size_contract_violation_count"] == 1
    assert report["profile_scheduler_contract"]["small_normal_batch_without_reason_count"] == 1
    assert report["profile_scheduler_contract"]["slot_refill_violation_count"] == 1

    aggregated = aggregate_event_level_efficiency_metrics([report])
    aggregate_batch_report = aggregated["profile_batch_envelopes"]
    assert aggregate_batch_report["envelope_count"] == 1
    assert aggregate_batch_report["tiny_batch_count"] == 1
    assert aggregate_batch_report["unexplained_tiny_batch_count"] == 1
    assert aggregate_batch_report["provider_slot_underuse_with_backlog_count"] == 1
    assert aggregate_batch_report["tiny_batch_coalesced_count"] == 2
    aggregate_queue_report = aggregated["profile_prefetch_queue"]
    assert aggregate_queue_report["snapshot_count"] == 1
    assert aggregate_queue_report["ready_url_count"] == 79
    assert aggregate_queue_report["deferred_url_count"] == 54
    assert aggregate_queue_report["oldest_pending_item_age_ms"]["max"] == 2500
    aggregate_plan_report = aggregated["profile_prefetch_batch_plan"]
    assert aggregate_plan_report["plan_count"] == 1
    assert aggregate_plan_report["queue_item_count"] == 89
    assert aggregate_plan_report["planned_dispatch_item_count"] == 35
    assert aggregate_plan_report["planned_deferred_item_count"] == 54
    assert aggregate_plan_report["available_slot_count"] == 2
    assert aggregate_plan_report["planned_new_worker_count"] == 1
    assert aggregate_plan_report["unfilled_available_slot_count"] == 1
    assert aggregate_plan_report["underfilled_with_deferred_items_count"] == 1
    aggregate_refill_report = aggregated["profile_prefetch_refill"]
    assert aggregate_refill_report["trigger_count"] == 1
    assert aggregate_refill_report["dispatched_url_count"] == 3
    assert aggregate_refill_report["available_slot_count"] == 2
    assert aggregate_refill_report["planned_new_worker_count"] == 1
    assert aggregate_refill_report["planned_dispatch_item_count"] == 35
    assert aggregate_refill_report["planned_deferred_item_count"] == 54
    assert aggregate_refill_report["unfilled_available_slot_count"] == 1
    assert aggregate_refill_report["underfilled_with_deferred_items_count"] == 1


def test_event_level_efficiency_preserves_retry_isolated_actor_slot_contract_fields() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "profile_prefetch": {
                        "requested_url_count": 1,
                        "dispatched_url_count": 1,
                        "deferred_url_count": 0,
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "refill_policy": "retry_wait_isolated_refill",
                            "plan_reason": "retry_wait_isolated_dispatch",
                            "refill_saturation": "filled_available_slots",
                            "requested_url_count": 1,
                            "candidate_count": 1,
                            "queue_item_count": 1,
                            "normal_queue_item_count": 0,
                            "retry_wait_item_count": 1,
                            "retry_isolation": True,
                            "planned_dispatch_worker_count": 1,
                            "planned_dispatch_item_count": 1,
                            "planned_deferred_item_count": 0,
                            "available_slot_count": 1,
                            "planned_new_worker_count": 1,
                            "unfilled_available_slot_count": 0,
                            "recommended_batch_size": 50,
                            "recommended_batch_count": 1,
                            "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            "batch_size_contract": "profile_actor_slot_ready_item_packing",
                            "batch_size_reason": "actor_slot_item_packing",
                            "actor_slot_url_target": 50,
                            "large_ready_set_max_batch_count": 8,
                            "large_ready_set_threshold_urls": 400,
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "batch_size": 1,
                                "dispatched_url_count": 1,
                                "requested_url_count": 1,
                                "is_tiny_batch": True,
                                "tiny_batch_allowed": True,
                                "small_batch_reason": "retry_isolation",
                                "flush_reason": "retry_isolation",
                            }
                        ],
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00.010000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 10,
                        "post_ingest_prefetch_candidate_count": 1,
                        "post_ingest_prefetch_dispatched_url_count": 1,
                    },
                },
            }
        ],
        workers=[],
    )

    assert report["harvest_completion_event_count"] == 1
    assert report["profile_batch_envelopes"]["tiny_batch_count"] == 1
    assert report["profile_batch_envelopes"]["unexplained_tiny_batch_count"] == 0
    plan_report = report["profile_prefetch_batch_plan"]
    assert plan_report["plan_count"] == 1
    sample = plan_report["samples"][0]
    assert sample["refill_policy"] == "retry_wait_isolated_refill"
    assert sample["plan_reason"] == "retry_wait_isolated_dispatch"
    assert sample["retry_isolation"] is True
    assert sample["normal_queue_item_count"] == 0
    assert sample["retry_wait_item_count"] == 1
    assert sample["batch_size_contract"] == "profile_actor_slot_ready_item_packing"
    assert sample["actor_slot_url_target"] == 50
    assert sample["large_ready_set_max_batch_count"] == 8
    assert report["violation_detected"] is False


def test_event_level_efficiency_accepts_durable_wave_batch_size_contract() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile refill drained a persisted durable wave.",
                "payload": {
                    "profile_prefetch": {
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "plan_reason": "ready_to_dispatch",
                            "refill_policy": "continuous_ready_item_refill",
                            "requested_url_count": 529,
                            "queue_item_count": 261,
                            "normal_queue_item_count": 261,
                            "retry_wait_item_count": 0,
                            "planned_dispatch_item_count": 261,
                            "planned_deferred_item_count": 0,
                            "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            "batch_size_contract": "profile_actor_slot_durable_wave_item_packing",
                            "batch_size_reason": "durable_refill_wave_batch_size",
                        },
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["batch_size_contract_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_event_level_efficiency_prefers_profile_prefetch_plan_arrays_over_legacy_latest_field() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "profile_prefetch": {
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "item_store": "linkedin_profile_registry",
                            "queue_item_count": 74,
                            "planned_dispatch_item_count": 24,
                            "planned_deferred_item_count": 50,
                        },
                        "batch_plans": [
                            {
                                "kind": "profile_prefetch_batch_plan",
                                "item_store": "linkedin_profile_registry",
                                "queue_item_count": 223,
                                "planned_dispatch_item_count": 200,
                                "planned_deferred_item_count": 23,
                            },
                            {
                                "kind": "profile_prefetch_batch_plan",
                                "item_store": "linkedin_profile_registry",
                                "queue_item_count": 74,
                                "planned_dispatch_item_count": 24,
                                "planned_deferred_item_count": 50,
                            },
                        ],
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "requested_url_count": 74,
                            "ready_url_count": 74,
                            "queued_url_count": 24,
                            "deferred_url_count": 50,
                        },
                        "profile_prefetch_queues": [
                            {
                                "kind": "linkedin_profile_prefetch_queue",
                                "requested_url_count": 223,
                                "ready_url_count": 223,
                                "queued_url_count": 200,
                                "deferred_url_count": 23,
                            },
                            {
                                "kind": "linkedin_profile_prefetch_queue",
                                "requested_url_count": 74,
                                "ready_url_count": 74,
                                "queued_url_count": 24,
                                "deferred_url_count": 50,
                            },
                        ],
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00.010000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 10,
                        "post_ingest_prefetch_candidate_count": 297,
                        "post_ingest_prefetch_dispatched_url_count": 224,
                    },
                },
            }
        ],
        workers=[],
    )

    plan_report = report["profile_prefetch_batch_plan"]
    assert plan_report["plan_count"] == 2
    assert plan_report["queue_item_count"] == 297
    assert plan_report["planned_dispatch_item_count"] == 224
    assert plan_report["planned_deferred_item_count"] == 73
    queue_report = report["profile_prefetch_queue"]
    assert queue_report["snapshot_count"] == 2
    assert queue_report["requested_url_count"] == 297
    assert queue_report["ready_url_count"] == 297
    assert queue_report["queued_url_count"] == 224
    assert queue_report["deferred_url_count"] == 73


def test_event_level_efficiency_does_not_treat_zero_dispatch_envelope_as_tiny_batch() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "profile_prefetch": {
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "batch_size": 1,
                                "dispatched_url_count": 0,
                                "requested_url_count": 77,
                                "deferred_url_count": 0,
                                "is_tiny_batch": False,
                                "tiny_batch_allowed": True,
                                "small_batch_reason": "",
                                "provider_slot_underuse_with_backlog": False,
                            }
                        ],
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 77,
                            "queued_url_count": 0,
                            "pending_url_count": 0,
                            "queue_quiescent": True,
                        },
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00.050000+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00.100000+00:00",
                        "post_ingest_prefetch_elapsed_ms": 100,
                        "post_ingest_prefetch_candidate_count": 77,
                        "post_ingest_prefetch_dispatched_url_count": 0,
                    },
                },
            }
        ],
        workers=[],
    )

    batch_report = report["profile_batch_envelopes"]
    assert batch_report["envelope_count"] == 1
    assert batch_report["tiny_batch_count"] == 0
    assert batch_report["unexplained_tiny_batch_count"] == 0
    assert report["violation_detected"] is False


def test_event_level_efficiency_reconstructs_profile_batch_envelopes_from_completed_workers() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "profile_prefetch": {
                        "status": "reused_local_raw_cache",
                        "dispatched_url_count": 0,
                        "requested_url_count": 38,
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-05-05T05:49:42+00:00",
                        "next_submit_attempt_started_at": "2026-05-05T05:49:42+00:00",
                        "next_submit_attempt_finished_at": "2026-05-05T05:49:42+00:00",
                        "post_ingest_prefetch_candidate_count": 38,
                        "post_ingest_prefetch_dispatched_url_count": 0,
                    },
                },
            }
        ],
        workers=[
            {
                "worker_id": 101,
                "status": "completed",
                "worker_key": "harvest_profile_batch::batch-101",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-{index}/" for index in range(38)]},
                "output": {"summary": {"requested_url_count": 38}},
                "budget": {"requested_url_count": 38},
            }
        ],
    )

    batch_report = report["profile_batch_envelopes"]
    assert batch_report["envelope_count"] == 1
    assert batch_report["batch_size"]["max"] == 38
    assert batch_report["tiny_batch_count"] == 0
    assert batch_report["unexplained_tiny_batch_count"] == 0
    assert batch_report["samples"][0]["source"] == "completed_worker_summary"
    assert report["violation_detected"] is False


def test_event_level_efficiency_flags_planned_dispatch_without_remote_owner() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00+00:00",
                    },
                    "profile_prefetch": {
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "schema_version": 1,
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 3,
                            "queued_url_count": 3,
                            "pending_url_count": 3,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {"planned_dispatch": 3},
                            "planned_dispatch_owner_missing_count": 2,
                            "planned_dispatch_remote_owner_count": 1,
                            "terminal_queue_state_leak_count": 0,
                        }
                    },
                },
            }
        ],
        workers=[],
    )

    queue_report = report["profile_prefetch_queue"]
    assert queue_report["planned_dispatch_owner_missing_count"] == 2
    assert queue_report["planned_dispatch_remote_owner_count"] == 1
    assert report["violation_detected"] is True
    assert report["violation_count"] == 2

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["profile_prefetch_queue"]["planned_dispatch_owner_missing_count"] == 2
    assert aggregate["profile_prefetch_queue"]["planned_dispatch_remote_owner_count"] == 1
    assert aggregate["violation_detected"] is True


def test_event_level_efficiency_flags_legacy_profile_coalescing_worker_timer() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[],
        workers=[
            {
                "worker_id": 701,
                "status": "running",
                "lane_id": "enrichment_specialist",
                "updated_at": "2026-05-02 00:00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "waiting_profile_coalescing",
                    "not_before_at": "2026-05-02 00:00:15",
                    "profile_urls": ["https://www.linkedin.com/in/tiny-tail/"],
                },
            }
        ],
        now=datetime.fromisoformat("2026-05-02T00:00:05+00:00"),
    )

    provider_slots = report["provider_slots"]
    assert provider_slots["coalescing_wait_worker_count"] == 1
    assert provider_slots["pre_submit_provider_worker_count"] == 0
    assert provider_slots["coalescing_wait_age_ms"]["max"] == 5000
    assert provider_slots["coalescing_until_ready_ms"]["max"] == 10000
    assert report["violation_detected"] is True
    assert report["violation_count"] == 1

    aggregated = aggregate_event_level_efficiency_metrics([report])
    assert aggregated["max_coalescing_wait_worker_count"] == 1
    assert aggregated["coalescing_wait_age_ms"]["max"] == 5000
    assert aggregated["coalescing_until_ready_ms"]["max"] == 10000
    assert aggregated["violation_detected"] is True


def test_event_level_efficiency_flags_duplicate_reconcile_materialize_and_phantom_slot() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={
            "background_reconcile": {
                "harvest_prefetch": {
                    "sync_result": {
                        "status": "completed",
                        "reason": "background_harvest_prefetch_reconcile",
                    }
                }
            }
        },
        events=[
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed retrieval results after worker recovery.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301]},
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed retrieval results after worker recovery.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301]},
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Snapshot materialization completed.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301], "reason": "candidate_artifact_rebuild"},
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Snapshot materialization completed.",
                "payload": {"snapshot_id": "snap-1", "worker_ids": [301], "reason": "candidate_artifact_rebuild"},
            },
        ],
        workers=[
            {
                "worker_id": 401,
                "status": "queued",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "waiting_remote_harvest"},
            }
        ],
    )

    assert report["provider_slots"]["pre_submit_provider_worker_count"] == 1
    assert report["reconcile"]["same_worker_reconcile_repeat_count"] == 1
    assert report["reconcile"]["repeated_materialize_signature_count"] == 1
    assert report["reconcile"]["materialize_call_count"] == 3
    assert report["violation_detected"] is True

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["max_pre_submit_provider_worker_count"] == 1
    assert aggregate["same_worker_reconcile_repeat_count"] == 1
    assert aggregate["repeated_materialize_signature_count"] == 1
    assert aggregate["violation_case_count"] == 1


def test_event_level_efficiency_splits_pure_handoff_from_provider_attempt_elapsed() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before candidate materialization.",
                "payload": {
                    "worker_ids": [910],
                    "pipeline_order": "provider_completed_to_local_ingest_to_next_submit_before_materialization",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-27T00:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-27T00:00:00.010000+00:00",
                        "next_submit_attempt_finished_at": "2026-04-27T00:03:00+00:00",
                        "post_ingest_prefetch_elapsed_ms": 180_000,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["local_completion_to_next_submit_start_ms"]["max"] == 10
    assert report["local_to_next_submit_start_ms"]["max"] == 10
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 180_000
    assert report["next_submit_attempt_elapsed_ms"]["max"] == 180_000

    aggregated = aggregate_event_level_efficiency_metrics([report])
    assert aggregated["local_completion_to_next_submit_start_ms"]["max"] == 10
    assert aggregated["local_to_next_submit_start_ms"]["max"] == 10
    assert aggregated["next_submit_provider_attempt_elapsed_ms"]["max"] == 180_000
    assert aggregated["next_submit_attempt_elapsed_ms"]["max"] == 180_000


def test_event_level_efficiency_prefers_structured_completed_reconcile_events() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "completed",
                "status": "skipped",
                "detail": "Completed workflow reconcile skipped because another runner holds the job lease.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "lease_skipped",
                    "reconcile_kind": "coordinator",
                    "lease_acquired": False,
                    "skip_reason": "completed_workflow_reconcile_inflight",
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Backfilled inline worker consumption markers from completed background reconcile state.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "marker_backfilled",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501, 502],
                    "marker_backfill_count": 2,
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Backfilled inline worker consumption markers from completed background reconcile state.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "marker_backfilled",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                    "marker_backfill_count": 1,
                },
            },
            {
                "stage": "completed",
                "status": "running",
                "detail": "Background harvest profile prefetch started candidate artifact sync.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "materialize_started",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                    "materialize_call": True,
                    "materialize_signature": "snap-structured|harvest_prefetch|same",
                },
            },
            {
                "stage": "completed",
                "status": "running",
                "detail": "Background harvest profile prefetch started candidate artifact sync.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "materialize_started",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                    "materialize_call": True,
                    "materialize_signature": "snap-structured|harvest_prefetch|same",
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background harvest profile prefetch completed candidate artifact sync.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "materialize_completed",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                    "sync_result": {"status": "completed", "reason": "background_harvest_prefetch_reconcile"},
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed results after harvest profile prefetch recovery.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "completed",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Background reconcile refreshed results after harvest profile prefetch recovery.",
                "payload": {
                    "event_family": "completed_workflow_reconcile",
                    "phase": "completed",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-structured",
                    "worker_ids": [501],
                },
            },
            {
                "stage": "completed",
                "status": "completed",
                "detail": "Snapshot materialization completed.",
                "payload": {"snapshot_id": "legacy-ignored", "reason": "candidate_artifact_rebuild"},
            },
        ],
        workers=[],
    )

    reconcile = report["reconcile"]
    assert reconcile["legacy_fallback_used"] is False
    assert reconcile["structured_event_count"] == 8
    assert reconcile["lease_skipped_count"] == 1
    assert reconcile["marker_backfill_count"] == 3
    assert reconcile["marker_backfill_repeat_count"] == 1
    assert reconcile["materialize_call_count"] == 2
    assert reconcile["materialize_started_count"] == 2
    assert reconcile["materialize_started_repeat_count"] == 1
    assert reconcile["materialize_started_signature_repeat_count"] == 1
    assert reconcile["materialize_started_worker_repeat_count"] == 1
    assert reconcile["materialize_completed_count"] == 1
    assert reconcile["repeated_materialize_signature_count"] == 1
    assert reconcile["same_worker_reconcile_repeat_count"] == 1

    aggregate = aggregate_event_level_efficiency_metrics([report])
    assert aggregate["marker_backfill_repeat_count"] == 1
    assert aggregate["materialize_started_repeat_count"] == 1


def test_event_level_efficiency_counts_running_workflow_materialization_events() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={
            "background_reconcile": {
                "harvest_prefetch": {
                    "sync_result": {
                        "status": "completed",
                        "reason": "inline_background_harvest_prefetch_reconcile",
                    }
                }
            }
        },
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Inline workflow materialization started candidate artifact sync.",
                "payload": {
                    "event_family": "workflow_materialization",
                    "phase": "materialize_started",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-running",
                    "worker_ids": [601, 602],
                    "materialize_call": True,
                    "materialize_signature": "snap-running|harvest_prefetch|601,602|inline",
                },
            },
            {
                "stage": "acquiring",
                "status": "completed",
                "detail": "Inline workflow materialization completed candidate artifact sync.",
                "payload": {
                    "event_family": "workflow_materialization",
                    "phase": "materialize_completed",
                    "reconcile_kind": "harvest_prefetch",
                    "snapshot_id": "snap-running",
                    "worker_ids": [601, 602],
                    "sync_result": {
                        "status": "completed",
                        "reason": "inline_background_harvest_prefetch_reconcile",
                        "timings_ms": {
                            "sync_total": 6100,
                            "view_write_total": 5200,
                            "state_upsert": 900,
                            "materialization_writer_wait": 75,
                        },
                        "delta_control_plane_sync": {
                            "status": "completed",
                            "timings_ms": {
                                "sync_total": 180,
                                "candidate_delta_control_plane_replace": 140,
                            },
                        },
                    },
                },
            },
        ],
        workers=[],
    )

    reconcile = report["reconcile"]
    assert reconcile["legacy_fallback_used"] is False
    assert reconcile["structured_event_count"] == 2
    assert reconcile["materialize_event_count"] == 1
    assert reconcile["materialize_started_count"] == 1
    assert reconcile["materialize_completed_count"] == 1
    assert reconcile["summary_materialize_result_count"] == 1
    assert reconcile["materialize_call_count"] == 2
    assert report["violation_detected"] is False
    materialization_io = report["materialization_io"]
    assert materialization_io["report_available"] is True
    assert materialization_io["sync_total_ms"]["max"] == 6100
    assert materialization_io["candidate_artifact_build_ms"]["max"] == 5200
    assert materialization_io["candidate_artifact_state_upsert_ms"]["max"] == 900
    assert materialization_io["candidate_delta_control_plane_replace_ms"]["max"] == 140
    assert materialization_io["materialization_writer_wait_ms"]["max"] == 75


def test_event_level_efficiency_aggregates_writer_lock_wait_metric() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background company-roster workers were incrementally applied to the live snapshot.",
                "payload": {
                    "snapshot_id": "snap-roster",
                    "worker_ids": [501],
                    "writer_lock": {
                        "scope": "inline_incremental:company_roster",
                        "writer_lock_wait_ms": 120,
                        "writer_lock_held_ms": 35,
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background search-seed workers were incrementally applied and profile prefetch was advanced.",
                "payload": {
                    "snapshot_id": "snap-seed",
                    "worker_ids": [502],
                    "writer_lock": {
                        "scope": "inline_incremental:search_seed",
                        "writer_lock_wait_ms": 5,
                        "writer_lock_held_ms": 12,
                    },
                },
            },
        ],
        workers=[],
    )

    writer_lock = dict(report.get("writer_lock") or {})
    assert writer_lock["writer_lock_event_count"] == 2
    wait_stats = dict(writer_lock.get("writer_lock_wait_ms") or {})
    assert int(wait_stats.get("max") or 0) == 120
    assert int(wait_stats.get("min") or -1) == 5
    held_stats = dict(writer_lock.get("writer_lock_held_ms") or {})
    assert int(held_stats.get("max") or 0) == 35
    by_scope = dict(writer_lock.get("writer_lock_wait_ms_by_scope") or {})
    assert "inline_incremental:company_roster" in by_scope
    assert "inline_incremental:search_seed" in by_scope


def test_aggregate_event_level_efficiency_metrics_includes_writer_lock() -> None:
    from sourcing_agent.workflow_efficiency import aggregate_event_level_efficiency_metrics

    aggregated = aggregate_event_level_efficiency_metrics(
        [
            {
                "report_available": True,
                "writer_lock": {
                    "writer_lock_wait_ms": {"max": 100, "min": 100, "values": [100]},
                    "writer_lock_held_ms": {"max": 20, "min": 20, "values": [20]},
                    "writer_lock_event_count": 1,
                },
            },
            {
                "report_available": True,
                "writer_lock": {
                    "writer_lock_wait_ms": {"max": 30, "min": 30, "values": [30]},
                    "writer_lock_held_ms": {"max": 7, "min": 7, "values": [7]},
                    "writer_lock_event_count": 2,
                },
            },
        ]
    )

    assert aggregated["writer_lock_event_count"] == 3
    wait_stats = dict(aggregated.get("writer_lock_wait_ms") or {})
    assert int(wait_stats.get("max") or 0) == 100


def test_event_level_efficiency_exposes_provider_io_timings() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "detail": "Received remote provider event for run run-io.",
                "payload": {
                    "target_worker_ids": [777],
                    "event_metrics": {
                        "remote_completed_at": "2026-05-04T10:00:00+00:00",
                        "local_event_seen_at": "2026-05-04T10:00:09+00:00",
                        "remote_to_local_event_lag_ms": 9000,
                        "actor_run_duration_ms": 22500,
                    },
                    "event": {
                        "run_id": "run-io",
                        "dataset_id": "dataset-io",
                    },
                },
            }
        ],
        workers=[
            {
                "worker_id": 777,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "completed",
                    "run_id": "run-io",
                    "dataset_id": "dataset-io",
                    "provider_timings": {
                        "dataset_download_duration_ms": 4300,
                        "actor_run_duration_ms": 22500,
                    },
                },
            }
        ],
    )

    provider_io = dict(report.get("provider_io") or {})
    assert provider_io["report_available"] is True
    actor_stats = dict(provider_io.get("actor_run_duration_ms") or {})
    assert actor_stats.get("count") == 1
    assert actor_stats.get("max") == 22500
    assert dict(provider_io.get("dataset_download_duration_ms") or {}).get("max") == 4300


def test_event_level_efficiency_surfaces_provider_slot_idle_window() -> None:
    """Workers in `waiting_remote_harvest` for several minutes with no progress are the
    documented "actor slot idle" failure mode. The new `remote_wait_age_ms` metric
    surfaces it without relying on Apify's UI."""

    from datetime import datetime, timezone

    reference_now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[],
        workers=[
            {
                "worker_id": 901,
                "status": "running",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-stuck",
                    "remote_wait_started_at": "2026-04-28T11:48:00+00:00",
                    "provider_limiter_lease": {
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_token": "lease-stuck",
                        "budget": 4,
                        "active_count": 1,
                        "created_at": "2026-04-28T11:47:55+00:00",
                    },
                },
            },
            {
                "worker_id": 902,
                "status": "running",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-recent",
                    "remote_wait_started_at": "2026-04-28T11:59:30+00:00",
                    "provider_limiter_lease": {
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_token": "lease-recent",
                        "budget": 4,
                        "active_count": 1,
                        "created_at": "2026-04-28T11:59:25+00:00",
                    },
                },
            },
        ],
        now=reference_now,
    )

    provider_slots = dict(report.get("provider_slots") or {})
    remote_wait_age = dict(provider_slots.get("remote_wait_age_ms") or {})
    # Stuck worker: 12 minutes = 720_000 ms; recent worker: 30 s = 30_000 ms.
    assert int(remote_wait_age.get("max") or 0) >= 12 * 60 * 1000
    assert int(remote_wait_age.get("min") or 0) <= 60 * 1000
    assert remote_wait_age.get("count") == 2
    lease_age = dict(provider_slots.get("provider_lease_age_ms") or {})
    # Lease created shortly before remote wait started — comparable order of magnitude.
    assert int(lease_age.get("max") or 0) >= 12 * 60 * 1000
    slot_to_remote_wait = dict(provider_slots.get("provider_slot_to_remote_wait_started_ms") or {})
    assert int(slot_to_remote_wait.get("max") or 0) == 5000
    assert dict(report.get("provider_slot_to_remote_wait_started_ms") or {}).get("max") == 5000


def test_event_level_efficiency_keeps_submit_hot_path_metric_for_completed_workers() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[],
        workers=[
            {
                "worker_id": 901,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "stage": "completed",
                    "remote_wait_started_at": "2026-04-28T11:59:30+00:00",
                    "provider_limiter_lease": {
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_token": "lease-completed",
                        "budget": 4,
                        "active_count": 1,
                        "created_at": "2026-04-28T11:59:25+00:00",
                    },
                },
            }
        ],
    )

    provider_slots = dict(report.get("provider_slots") or {})
    assert provider_slots["true_active_provider_slot_worker_count"] == 0
    assert dict(provider_slots.get("provider_slot_to_remote_wait_started_ms") or {}).get("max") == 5000
    assert dict(report.get("provider_slot_to_remote_wait_started_ms") or {}).get("max") == 5000


def test_event_level_efficiency_marks_next_submit_not_applicable_when_all_urls_pre_submitted() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "started_at": "2026-04-28T11:59:00+00:00",
                        "finished_at": "2026-04-28T11:59:01+00:00",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "batch_plan": {"requested_url_count": 50},
                        "profile_prefetch_queue": {"requested_url_count": 50},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    assert dict(report.get("next_submit_opportunity") or {}) == {
        "applicable": False,
        "reason": "all_profile_urls_submitted_before_first_completion",
        "metrics_required": False,
        "sample_count": 0,
        "dispatch_count": 1,
        "requested_url_count": 50,
        "dispatched_url_count": 50,
        "dispatched_before_first_completion_count": 50,
    }


def test_event_level_efficiency_counts_submit_anchor_without_nested_profile_prefetch_timestamps() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 120,
                        "queued_worker_count": 1,
                        "batch_plan": {"requested_url_count": 120},
                        "profile_prefetch_queue": {"requested_url_count": 120},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
        ],
        workers=[],
    )

    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["reason"] == "all_profile_urls_submitted_before_first_completion"
    assert opportunity["dispatched_url_count"] == 120
    assert opportunity["dispatched_before_first_completion_count"] == 120
    assert report["remote_to_next_submit_start_ms"] == {}


def test_event_level_efficiency_counts_initial_profile_prefetch_dispatch_as_submit_anchor() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "completed",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 25,
                        "queued_worker_count": 1,
                        "metrics": {"prefetch_started_at": "2026-04-28T11:59:00+00:00"},
                        "profile_prefetch_queue": {"requested_url_count": 140},
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:03+00:00",
                "payload": {
                    "kind": "profile_prefetch_phase_b_group",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 115,
                        "queued_worker_count": 3,
                        "metrics": {"prefetch_started_at": "2026-04-28T11:59:03+00:00"},
                        "batch_plan": {"requested_url_count": 140},
                        "profile_prefetch_queue": {"requested_url_count": 140},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
        ],
        workers=[],
    )

    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["dispatched_url_count"] == 140
    assert opportunity["dispatched_before_first_completion_count"] == 140


def test_event_level_efficiency_uses_worker_submit_proof_when_prefetch_events_are_compacted() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "started_at": "2026-04-28T11:59:00+00:00",
                        "finished_at": "2026-04-28T11:59:01+00:00",
                        "dispatched_url_count": 25,
                        "queued_worker_count": 1,
                        "batch_plan": {"requested_url_count": 25},
                        "profile_prefetch_queue": {"requested_url_count": 140},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [14],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [14],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
        ],
        workers=[
            {
                "worker_id": 12,
                "worker_key": "harvest_profile_batch::first",
                "status": "completed",
                "created_at": "2026-04-28 11:59:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:01+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-first-{index}/" for index in range(25)]},
                "output": {"summary": {"requested_url_count": 25}},
            },
            {
                "worker_id": 14,
                "worker_key": "harvest_profile_batch::second",
                "status": "completed",
                "created_at": "2026-04-28 11:59:18",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:18+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-second-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
            {
                "worker_id": 15,
                "worker_key": "harvest_profile_batch::third",
                "status": "completed",
                "created_at": "2026-04-28 11:59:19",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:19+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-third-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
            {
                "worker_id": 16,
                "worker_key": "harvest_profile_batch::tail",
                "status": "completed",
                "created_at": "2026-04-28 11:59:21",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:21+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-tail-{index}/" for index in range(15)]},
                "output": {"summary": {"requested_url_count": 15}},
            },
        ],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["metrics_required"] is False
    assert opportunity["reason"] == "all_profile_urls_submitted_before_first_completion"
    assert opportunity["event_dispatched_url_count"] == 25
    assert opportunity["worker_dispatched_url_count"] == 140
    assert opportunity["dispatched_url_count"] == 140
    assert opportunity["dispatched_before_first_completion_count"] == 140


def test_event_level_efficiency_ignores_late_tail_sample_when_initial_wave_pre_submitted() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:27+00:00",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 0,
                        "queued_worker_count": 0,
                        "profile_prefetch_queue": {"requested_url_count": 120},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [3],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [3],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T12:00:03+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch_refill_phase": "profile_prefetch_refill",
                    "started_at": "2026-04-28T12:00:03+00:00",
                    "finished_at": "2026-04-28T12:00:03.250000+00:00",
                    "elapsed_ms": 250,
                    "provider_submit_elapsed_ms": 25,
                    "dispatched_url_count": 20,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "plan_reason": "queue_quiescent_final_tail",
                        "requested_url_count": 20,
                        "planned_dispatch_item_count": 20,
                    },
                    "profile_prefetch_queue": {"requested_url_count": 20},
                },
            },
        ],
        workers=[
            {
                "worker_id": 3,
                "worker_key": "harvest_profile_batch::initial-a",
                "status": "completed",
                "created_at": "2026-04-28 11:59:27",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:28+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-a-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
            {
                "worker_id": 4,
                "worker_key": "harvest_profile_batch::initial-b",
                "status": "completed",
                "created_at": "2026-04-28 11:59:29",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:29+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-b-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
            {
                "worker_id": 5,
                "worker_key": "harvest_profile_batch::initial-c",
                "status": "completed",
                "created_at": "2026-04-28 11:59:30",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"stage": "completed", "remote_wait_started_at": "2026-04-28T11:59:30+00:00"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/lovable-c-{index}/" for index in range(20)]},
                "output": {"summary": {"requested_url_count": 20}},
            },
        ],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    ignored = dict(report.get("ignored_next_submit_slo_samples") or {})
    assert ignored["reason"] == "all_profile_urls_submitted_before_first_completion"
    assert ignored["remote_to_next_submit_start_ms"]["max"] == 3000
    assert ignored["local_completion_to_next_submit_start_ms"]["max"] == 2000
    assert ignored["next_submit_provider_attempt_elapsed_ms"]["max"] == 25
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["metrics_required"] is False
    assert opportunity["reason"] == "all_profile_urls_submitted_before_first_completion"
    assert opportunity["requested_url_count"] == 120
    assert opportunity["worker_dispatched_before_first_completion_count"] == 120


def test_event_level_efficiency_does_not_pair_discovery_append_submit_with_completion_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_phase_b_group",
                    "pipeline_order": "company_roster_apply_to_profile_prefetch_before_materialization",
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 120,
                        "queued_worker_count": 3,
                        "metrics": {
                            "prefetch_started_at": "2026-04-28T11:59:00+00:00",
                            "prefetch_finished_at": "2026-04-28T11:59:07+00:00",
                            "prefetch_elapsed_ms": 7000,
                        },
                        "profile_prefetch_queue": {"requested_url_count": 140},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "completed",
                "created_at": "2026-04-28T12:00:10+00:00",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "dispatched_url_count": 20,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-28T12:00:03+00:00",
                            "prefetch_finished_at": "2026-04-28T12:00:06+00:00",
                            "prefetch_elapsed_ms": 3000,
                        },
                        "profile_prefetch_queue": {"requested_url_count": 140},
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 7000
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["metrics_required"] is False
    assert opportunity["reason"] == "all_profile_urls_submitted_by_non_handoff_dispatch"
    assert opportunity["legacy_reason"] == "all_profile_urls_submitted_by_discovery_append"
    assert opportunity["dispatched_url_count"] == 140


def test_event_level_efficiency_does_not_pair_quiescent_tail_refill_with_completion_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T12:00:08+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "started_at": "2026-04-28T12:00:08+00:00",
                    "finished_at": "2026-04-28T12:00:08.500000+00:00",
                    "elapsed_ms": 500,
                    "dispatched_url_count": 20,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "plan_reason": "queue_quiescent_final_tail",
                        "requested_url_count": 20,
                        "planned_dispatch_item_count": 20,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"] == {}
    assert report["local_completion_to_next_submit_start_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 500
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["reason"] == "all_profile_urls_submitted_by_non_handoff_dispatch"
    assert opportunity["metrics_required"] is False
    assert opportunity["post_completion_non_handoff_dispatch_count"] == 1


def test_event_level_efficiency_pairs_pre_worker_final_tail_as_slot_release_handoff() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch_refill_phase": "pre_worker_profile_prefetch_refill",
                    "started_at": "2026-04-28T12:00:03+00:00",
                    "finished_at": "2026-04-28T12:00:03.250000+00:00",
                    "elapsed_ms": 250,
                    "provider_submit_elapsed_ms": 25,
                    "dispatched_url_count": 23,
                    "queued_worker_count": 1,
                    "batch_plan": {
                        "plan_reason": "queue_quiescent_final_tail",
                        "requested_url_count": 23,
                        "planned_dispatch_item_count": 23,
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["remote_to_next_submit_start_ms"]["max"] == 3000
    assert report["local_completion_to_next_submit_start_ms"]["max"] == 2000
    assert report["next_submit_provider_attempt_elapsed_ms"]["max"] == 25
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 250
    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["reason"] == "next_submit_samples_observed"


def test_event_level_efficiency_ignores_terminal_profile_prefetch_proof_as_submit_anchor() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "status": "queued",
                        "started_at": "2026-04-28T11:59:00+00:00",
                        "finished_at": "2026-04-28T11:59:00.500000+00:00",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-28T11:59:00+00:00",
                            "prefetch_finished_at": "2026-04-28T11:59:00.500000+00:00",
                            "prefetch_elapsed_ms": 500,
                        },
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "requested_url_count": 250,
                            "planned_dispatch_item_count": 50,
                        },
                        "profile_prefetch_queue": {"requested_url_count": 250},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "completed",
                "created_at": "2026-04-28T12:00:10+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "status": "completed",
                        "reason": "registry_all_requested_profiles_fetched",
                        "dispatched_url_count": 200,
                        "queued_worker_count": 1,
                        "metrics": {
                            "prefetch_started_at": "2026-04-28T11:51:52+00:00",
                            "prefetch_finished_at": "2026-04-28T12:00:00+00:00",
                            "prefetch_elapsed_ms": 487698,
                        },
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "requested_url_count": 250,
                            "planned_dispatch_item_count": 200,
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "status": "queued",
                                "dispatched_url_count": 200,
                            }
                        ],
                        "profile_prefetch_queue": {
                            "requested_url_count": 250,
                            "registry_all_requested_terminal": True,
                        },
                        "registry_terminal_summary": {"all_requested_terminal": True},
                    },
                },
            },
        ],
        workers=[],
    )

    assert report["next_submit_attempt_elapsed_ms"] == {}
    assert report["next_submit_provider_attempt_elapsed_ms"] == {}
    assert report["profile_refill_daemon_elapsed_ms"]["max"] == 500
    assert report["profile_prefetch_refill"]["dispatched_url_count"] == 50
    assert report["profile_prefetch_batch_plan"]["planned_dispatch_item_count"] == 50
    assert report["profile_batch_envelopes"]["envelope_count"] == 1
    assert report["profile_batch_envelopes"]["batch_size"]["max"] == 50
    assert dict(report.get("remote_to_next_submit_start_ms") or {}) == {}


def test_event_level_efficiency_marks_next_submit_applicable_when_post_completion_dispatch_exists() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T11:59:00+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "started_at": "2026-04-28T11:59:00+00:00",
                        "finished_at": "2026-04-28T11:59:01+00:00",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "batch_plan": {"requested_url_count": 100},
                        "profile_prefetch_queue": {"requested_url_count": 100},
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-28T12:00:00+00:00",
                        "local_event_seen_at": "2026-04-28T12:00:00+00:00",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "payload": {
                    "worker_ids": [901],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-28T12:00:01+00:00",
                        "next_submit_attempt_semantics": "refill_daemon_signal_only",
                    },
                },
            },
            {
                "stage": "acquiring",
                "status": "running",
                "created_at": "2026-04-28T12:00:03+00:00",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "profile_prefetch": {
                        "started_at": "2026-04-28T12:00:03+00:00",
                        "finished_at": "2026-04-28T12:00:04+00:00",
                        "dispatched_url_count": 50,
                        "queued_worker_count": 1,
                        "batch_plan": {"requested_url_count": 100},
                        "profile_prefetch_queue": {"requested_url_count": 100},
                    },
                },
            },
        ],
        workers=[],
    )

    opportunity = dict(report.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is True
    assert opportunity["metrics_required"] is True
    assert opportunity["reason"] == "next_submit_samples_observed"
    assert dict(report.get("remote_to_next_submit_start_ms") or {}).get("max") == 3000


def test_aggregate_event_level_efficiency_preserves_next_submit_not_applicable_contract() -> None:
    aggregated = aggregate_event_level_efficiency_metrics(
        [
            {
                "report_available": True,
                "remote_to_next_submit_start_ms": {},
                "local_to_next_submit_start_ms": {},
                "next_submit_attempt_elapsed_ms": {},
                "next_submit_opportunity": {
                    "applicable": False,
                    "reason": "all_profile_urls_submitted_before_first_completion",
                    "metrics_required": False,
                    "requested_url_count": 140,
                    "dispatched_url_count": 140,
                    "dispatched_before_first_completion_count": 140,
                },
            }
        ]
    )

    opportunity = dict(aggregated.get("next_submit_opportunity") or {})
    assert opportunity["applicable"] is False
    assert opportunity["metrics_required"] is False
    assert opportunity["reason"] == "all_reports_mark_next_submit_not_applicable"
    subset = event_level_efficiency_runtime_subset(aggregated)
    assert dict(subset.get("next_submit_opportunity") or {}).get("applicable") is False


def test_aggregate_event_level_efficiency_metrics_includes_provider_slot_idle() -> None:
    aggregated = aggregate_event_level_efficiency_metrics(
        [
            {
                "report_available": True,
                "provider_slots": {
                    "remote_wait_age_ms": {"max": 720_000, "min": 720_000, "values": [720_000]},
                    "provider_lease_age_ms": {"max": 720_005, "min": 720_005, "values": [720_005]},
                    "provider_slot_to_remote_wait_started_ms": {"max": 5, "min": 5, "values": [5]},
                },
            },
            {
                "report_available": True,
                "provider_slots": {
                    "remote_wait_age_ms": {"max": 30_000, "min": 30_000, "values": [30_000]},
                    "provider_lease_age_ms": {"max": 30_005, "min": 30_005, "values": [30_005]},
                    "provider_slot_to_remote_wait_started_ms": {"max": 5, "min": 5, "values": [5]},
                },
            },
        ]
    )

    remote_wait_age = dict(aggregated.get("remote_wait_age_ms") or {})
    assert int(remote_wait_age.get("max") or 0) == 720_000
    assert int(remote_wait_age.get("min") or 0) == 30_000
    provider_lease_age = dict(aggregated.get("provider_lease_age_ms") or {})
    assert int(provider_lease_age.get("max") or 0) == 720_005
    slot_to_remote_wait = dict(aggregated.get("provider_slot_to_remote_wait_started_ms") or {})
    assert int(slot_to_remote_wait.get("max") or 0) == 5


def test_profile_scheduler_contract_flags_underfilled_normal_batch_without_reason() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "schema_version": 1,
                    "status": "queued",
                    "profile_prefetch": {
                        "scheduler_lock": {
                            "required": True,
                            "kind": "pg_advisory_xact_lock",
                            "distributed": True,
                        },
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "item_store": "linkedin_profile_registry",
                            "plan_reason": "ready_to_dispatch",
                            "refill_policy": "continuous_ready_item_refill",
                            "queue_item_count": 74,
                            "normal_queue_item_count": 74,
                            "retry_wait_item_count": 0,
                            "planned_dispatch_item_count": 24,
                            "planned_deferred_item_count": 50,
                            "available_slot_count": 1,
                            "planned_new_worker_count": 1,
                            "recommended_batch_size": 50,
                            "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            "batch_size_contract": "profile_actor_slot_ready_item_packing",
                        },
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 74,
                            "ready_url_count": 74,
                            "queued_url_count": 24,
                            "deferred_url_count": 50,
                            "pending_url_count": 74,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {"planned_dispatch": 24, "deferred_budget": 50},
                            "planned_dispatch_owner_missing_count": 0,
                            "terminal_queue_state_leak_count": 0,
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "chunk_index": 1,
                                "status": "queued",
                                "batch_size": 24,
                                "dispatched_url_count": 24,
                                "requested_url_count": 74,
                                "deferred_url_count": 50,
                                "recommended_batch_size": 50,
                                "min_non_tail_batch_size": 50,
                                "small_batch_reason": "",
                                "flush_reason": "adaptive_prefetch_window",
                                "tiny_batch_allowed": True,
                            }
                        ],
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["report_available"] is True
    assert scheduler["small_normal_batch_without_reason_count"] == 1
    assert scheduler["violation_detected"] is True
    assert report["violation_detected"] is True


def test_profile_scheduler_contract_flags_deferred_urls_with_open_actor_slot() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background roster apply deferred provider submit to profile refill daemon.",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "reason": "provider_submit_deferred_to_refill_daemon",
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "status": "queued",
                            "reason": "provider_submit_deferred_to_refill_daemon",
                            "requested_url_count": 140,
                            "ready_url_count": 115,
                            "queued_url_count": 25,
                            "deferred_url_count": 115,
                            "ready_after_dispatch_url_count": 115,
                            "pending_url_count": 140,
                            "active_worker_count": 1,
                            "queued_worker_count": 0,
                            "actor_budget": 2,
                            "submit_budget": 2,
                            "available_new_worker_count": 1,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {
                                "planned_dispatch": 25,
                                "deferred_budget": 115,
                            },
                            "planned_dispatch_owner_missing_count": 0,
                            "terminal_queue_state_leak_count": 0,
                        },
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["report_available"] is True
    assert scheduler["queue_open_slot_deferred_violation_count"] == 1
    assert scheduler["slot_refill_violation_count"] == 1
    assert scheduler["violation_detected"] is True
    assert report["violation_detected"] is True


def test_profile_scheduler_contract_allows_sub_50_tail_coalescing_with_open_actor_slot() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Background roster apply queued one full batch and held a sub-50 tail.",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "status": "queued",
                            "requested_url_count": 74,
                            "ready_url_count": 74,
                            "queued_url_count": 50,
                            "deferred_url_count": 24,
                            "ready_after_dispatch_url_count": 24,
                            "pending_url_count": 74,
                            "active_worker_count": 0,
                            "queued_worker_count": 1,
                            "actor_budget": 4,
                            "submit_budget": 4,
                            "available_new_worker_count": 4,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {
                                "planned_dispatch": 50,
                                "deferred_coalescing": 24,
                            },
                            "planned_dispatch_owner_missing_count": 0,
                            "terminal_queue_state_leak_count": 0,
                        },
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["report_available"] is True
    assert scheduler["queue_open_slot_deferred_violation_count"] == 0
    assert scheduler["slot_refill_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_profile_scheduler_contract_uses_scheduler_reserved_slot_occupancy() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Concurrent prefetch observed reserved registry batches.",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "status": "queued",
                            "requested_url_count": 223,
                            "ready_url_count": 223,
                            "queued_url_count": 150,
                            "deferred_url_count": 50,
                            "ready_after_dispatch_url_count": 50,
                            "pending_url_count": 223,
                            "active_worker_count": 0,
                            "queued_worker_count": 3,
                            "scheduler_reserved_worker_count": 4,
                            "effective_active_worker_count": 4,
                            "slot_occupancy_count": 4,
                            "actor_budget": 4,
                            "submit_budget": 4,
                            "available_new_worker_count": 4,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {
                                "deferred_budget": 50,
                                "planned_dispatch": 150,
                            },
                            "planned_dispatch_owner_missing_count": 0,
                            "terminal_queue_state_leak_count": 0,
                        },
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["queue_open_slot_deferred_violation_count"] == 0
    assert scheduler["slot_refill_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_profile_scheduler_contract_allows_lease_contention_reserved_tail() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Refill tail was already reserved by provider-owned workers.",
                "payload": {
                    "profile_prefetch": {
                        "status": "queued",
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "status": "queued",
                            "reason": "profile_registry_lease_contention",
                            "requested_url_count": 25,
                            "ready_url_count": 20,
                            "deferred_url_count": 20,
                            "ready_after_dispatch_url_count": 20,
                            "pending_url_count": 20,
                            "active_worker_count": 1,
                            "queued_worker_count": 0,
                            "scheduler_reserved_worker_count": 2,
                            "effective_active_worker_count": 2,
                            "slot_occupancy_count": 2,
                            "actor_budget": 4,
                            "submit_budget": 4,
                            "available_new_worker_count": 2,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {
                                "deferred_budget": 20,
                            },
                            "planned_dispatch_owner_missing_count": 0,
                            "terminal_queue_state_leak_count": 0,
                        },
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["queue_open_slot_deferred_violation_count"] == 0
    assert scheduler["slot_refill_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_profile_scheduler_contract_does_not_apply_same_wave_ordinal_to_worker_fallback() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[],
        workers=[
            {
                "worker_id": 1,
                "worker_key": "harvest_profile_batch::first",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/current-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
            {
                "worker_id": 2,
                "worker_key": "harvest_profile_batch::second",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "input": {"profile_urls": [f"https://www.linkedin.com/in/former-{index}/" for index in range(50)]},
                "output": {"summary": {"requested_url_count": 50}},
            },
        ],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["same_wave_ordinal_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_profile_scheduler_contract_flags_overlapping_profile_url_dispatch_workers() -> None:
    duplicated_url = "https://www.linkedin.com/in/openai-agent-former-0053/"
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[],
        workers=[
            {
                "worker_id": 11,
                "worker_key": "harvest_profile_batch::first",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "input": {
                    "profile_urls": [
                        duplicated_url,
                        "https://www.linkedin.com/in/openai-agent-current-0213/",
                    ]
                },
                "output": {"summary": {"requested_url_count": 2}},
            },
            {
                "worker_id": 13,
                "worker_key": "harvest_profile_batch::duplicate",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "input": {
                    "profile_urls": [
                        duplicated_url,
                        "https://www.linkedin.com/in/openai-agent-former-0054/",
                    ]
                },
                "output": {"summary": {"requested_url_count": 2}},
            },
        ],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["url_overlap_violation_count"] == 1
    assert scheduler["violation_detected"] is True
    assert report["violation_detected"] is True
    assert scheduler["samples"]["url_overlaps"][0]["profile_url_key"] == duplicated_url.rstrip("/").lower()


def test_profile_scheduler_contract_resets_same_wave_ordinal_between_distinct_waves() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Profile prefetch refill daemon dispatched registry deferred items.",
                "payload": {
                    "kind": "profile_prefetch_refill_daemon_group",
                    "status": "queued",
                    "started_at": "2026-04-30T08:00:00+00:00",
                    "finished_at": "2026-04-30T08:00:00.100000+00:00",
                    "elapsed_ms": 100,
                    "dispatched_url_count": 250,
                    "queued_worker_count": 5,
                    "profile_prefetch": {
                        "scheduler_lock": {
                            "required": True,
                            "kind": "pg_advisory_xact_lock",
                            "distributed": True,
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "chunk_index": 1,
                                "status": "queued",
                                "dispatched_url_count": 50,
                                "requested_url_count": 223,
                                "recommended_batch_count": 5,
                                "recommended_max_workers": 4,
                                "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            },
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "chunk_index": 2,
                                "status": "queued",
                                "dispatched_url_count": 50,
                                "requested_url_count": 223,
                                "recommended_batch_count": 5,
                                "recommended_max_workers": 4,
                                "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            },
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "chunk_index": 1,
                                "status": "queued",
                                "dispatched_url_count": 50,
                                "requested_url_count": 74,
                                "recommended_batch_count": 2,
                                "recommended_max_workers": 2,
                                "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            },
                        ],
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["same_wave_ordinal_violation_count"] == 0
    assert scheduler["violation_detected"] is False


def test_profile_scheduler_contract_flags_retry_mixed_with_normal_and_missing_pg_lock() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "profile_prefetch": {
                        "scheduler_lock": {
                            "required": True,
                            "kind": "in_process_rlock",
                            "distributed": False,
                        },
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "item_store": "linkedin_profile_registry",
                            "plan_reason": "ready_to_dispatch",
                            "refill_policy": "continuous_ready_item_refill",
                            "queue_item_count": 51,
                            "normal_queue_item_count": 50,
                            "retry_wait_item_count": 1,
                            "retry_isolation": False,
                            "planned_dispatch_item_count": 50,
                            "planned_deferred_item_count": 1,
                            "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            "batch_size_contract": "profile_actor_slot_ready_item_packing",
                        },
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 51,
                            "ready_url_count": 51,
                            "queued_url_count": 50,
                            "deferred_url_count": 1,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {"retry_wait": 1, "deferred_budget": 50},
                        },
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00+00:00",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["retry_wave_isolation_violation_count"] == 2
    assert scheduler["advisory_lock_missing_count"] == 1
    assert scheduler["violation_detected"] is True


def test_profile_scheduler_contract_passes_retry_isolated_tail_with_pg_lock() -> None:
    report = extract_event_level_efficiency_metrics(
        job_summary={},
        events=[
            {
                "stage": "acquiring",
                "status": "running",
                "detail": "Harvest profile completion event advanced provider tail before local apply.",
                "payload": {
                    "worker_ids": [101],
                    "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
                    "profile_prefetch": {
                        "scheduler_lock": {
                            "required": True,
                            "kind": "pg_advisory_xact_lock",
                            "distributed": True,
                        },
                        "batch_plan": {
                            "kind": "profile_prefetch_batch_plan",
                            "item_store": "linkedin_profile_registry",
                            "plan_reason": "retry_wait_isolated_dispatch",
                            "refill_policy": "retry_wait_isolated_refill",
                            "queue_item_count": 1,
                            "normal_queue_item_count": 0,
                            "retry_wait_item_count": 1,
                            "retry_isolation": True,
                            "planned_dispatch_item_count": 1,
                            "planned_deferred_item_count": 0,
                            "dispatch_strategy": "actor_slot_item_packing_scripted_prefetch_window",
                            "batch_size_contract": "profile_actor_slot_ready_item_packing",
                        },
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "item_store": "linkedin_profile_registry",
                            "requested_url_count": 1,
                            "ready_url_count": 1,
                            "queued_url_count": 1,
                            "deferred_url_count": 0,
                            "queue_quiescent": False,
                            "refill_queue_state_counts": {"retry_wait": 1},
                        },
                        "batch_envelopes": [
                            {
                                "kind": "harvest_profile_scraper_batch",
                                "chunk_index": 0,
                                "status": "queued",
                                "batch_size": 1,
                                "dispatched_url_count": 1,
                                "requested_url_count": 1,
                                "deferred_url_count": 0,
                                "recommended_batch_size": 50,
                                "min_non_tail_batch_size": 50,
                                "small_batch_reason": "retry_isolation",
                                "flush_reason": "retry_isolation",
                                "tiny_batch_allowed": True,
                            }
                        ],
                    },
                    "event_metrics": {
                        "local_event_apply_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_started_at": "2026-04-30T08:00:00+00:00",
                        "next_submit_attempt_finished_at": "2026-04-30T08:00:00+00:00",
                        "post_ingest_prefetch_elapsed_ms": 0,
                    },
                },
            }
        ],
        workers=[],
    )

    scheduler = report["profile_scheduler_contract"]
    assert scheduler["report_available"] is True
    assert scheduler["violation_detected"] is False
    assert report["violation_detected"] is False
