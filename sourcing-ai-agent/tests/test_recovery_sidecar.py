from sourcing_agent.recovery_sidecar import (
    build_job_scoped_recovery_callback_payload,
    build_job_scoped_recovery_command,
    build_job_scoped_recovery_config,
    build_shared_recovery_config,
)


def test_shared_recovery_poll_default_is_a_backstop_within_stale_and_lease_windows() -> None:
    # Step 5c: with the event-signaled wakeup (5b) driving latency-sensitive
    # work sub-second, the shared poll is a backstop for the inherently
    # event-less conditions (crash/lease-expiry/stuck). The default must stay
    # comfortably INSIDE the stale_after and lease windows it has to cover, or a
    # crashed/expired worker would languish past its own TTL.
    config = build_shared_recovery_config({})
    poll_seconds = float(config["poll_seconds"])
    stale_after_seconds = int(config["stale_after_seconds"])
    lease_seconds = int(config["lease_seconds"])
    assert poll_seconds == 30.0
    # A worker is detected stale within at most stale_after + one poll; that must
    # remain well under the lease TTL so the lease is reclaimed in time.
    assert poll_seconds < stale_after_seconds
    assert stale_after_seconds + poll_seconds < lease_seconds


def test_shared_recovery_poll_seconds_override_is_honored() -> None:
    config = build_shared_recovery_config({"shared_recovery_poll_seconds": 5.0})
    assert float(config["poll_seconds"]) == 5.0


def test_job_scoped_recovery_accepts_cli_aliases_and_event_worker_scope() -> None:
    config = build_job_scoped_recovery_config(
        "job-event",
        {
            "service_name": "job-recovery-job-event",
            "poll_seconds": 0.5,
            "max_ticks": 4,
            "idle_stop_ticks": 2,
            "lease_seconds": 120,
            "stale_after_seconds": 0,
            "total_limit": 5,
            "explicit_worker_ids": [11, 11, 12],
            "remote_provider_event_worker_ids": [12, 13],
            "force_release_explicit_worker_leases": True,
            "profile_prefetch_nonblocking_submit": True,
            "profile_prefetch_refill_enabled": False,
            "profile_prefetch_refill_before_worker_recovery": True,
            "remote_event_followup_enabled": False,
            "search_seed_discovery_enabled": False,
            "snapshot_full_materialization_enabled": False,
            "projection_facet_layering_enabled": False,
            "excel_intake_recovery_enabled": False,
            "post_recovery_housekeeping_enabled": False,
            "workflow_auto_resume_enabled": False,
            "workflow_queue_auto_takeover_enabled": False,
            "workflow_resume_stale_after_seconds": 7,
            "workflow_queue_resume_stale_after_seconds": 0,
        },
    )

    assert config["poll_seconds"] == 0.5
    assert config["max_ticks"] == 4
    assert config["idle_stop_ticks"] == 2
    assert config["lease_seconds"] == 120
    assert config["stale_after_seconds"] == 0
    assert config["total_limit"] == 5
    assert config["explicit_worker_ids"] == [11, 12, 13]
    assert config["force_release_explicit_worker_leases"] is True
    assert config["profile_prefetch_nonblocking_submit"] is True
    assert config["profile_prefetch_refill_enabled"] is False
    assert config["profile_prefetch_refill_before_worker_recovery"] is True
    assert config["remote_event_followup_enabled"] is False
    assert config["search_seed_discovery_enabled"] is False
    assert config["snapshot_full_materialization_enabled"] is False
    assert config["projection_facet_layering_enabled"] is False
    assert config["excel_intake_recovery_enabled"] is False
    assert config["post_recovery_housekeeping_enabled"] is False
    assert config["workflow_auto_resume_enabled"] is False
    assert config["workflow_queue_auto_takeover_enabled"] is False
    assert config["workflow_resume_explicit_job"] is True
    assert config["workflow_resume_stale_after_seconds"] == 7
    assert config["workflow_queue_resume_stale_after_seconds"] == 0

    callback_payload = build_job_scoped_recovery_callback_payload("job-event", {}, config=config)
    assert callback_payload["explicit_worker_ids"] == [11, 12, 13]
    assert callback_payload["force_release_explicit_worker_leases"] is True
    assert callback_payload["profile_prefetch_nonblocking_submit"] is True
    assert callback_payload["profile_prefetch_refill_enabled"] is False
    assert callback_payload["profile_prefetch_refill_before_worker_recovery"] is True
    assert callback_payload["remote_event_followup_enabled"] is False
    assert callback_payload["search_seed_discovery_enabled"] is False
    assert callback_payload["snapshot_full_materialization_enabled"] is False
    assert callback_payload["projection_facet_layering_enabled"] is False
    assert callback_payload["excel_intake_recovery_enabled"] is False
    assert callback_payload["post_recovery_housekeeping_enabled"] is False
    assert callback_payload["workflow_auto_resume_enabled"] is False
    assert callback_payload["workflow_queue_auto_takeover_enabled"] is False
    assert callback_payload["workflow_resume_explicit_job"] is True
    assert callback_payload["job_recovery_idle_stop_ticks"] == 2
    assert callback_payload["explicit_job_followup_rounds"] == 0

    command = build_job_scoped_recovery_command(
        "job-event",
        config=config,
        python_executable="/usr/bin/python3",
    )
    assert command[0] == "/usr/bin/python3"
    assert command[command.index("--stale-after-seconds") + 1] == "0"
    assert command[command.index("--max-ticks") + 1] == "4"
    assert command[command.index("--idle-stop-ticks") + 1] == "2"
    assert command.count("--explicit-worker-id") == 3
    assert "--force-release-explicit-worker-leases" in command
    assert "--profile-prefetch-refill-before-worker-recovery" in command
    assert "--disable-remote-event-followup" in command
    assert "--disable-projection-facet-layering" in command


def test_job_scoped_recovery_defaults_remote_event_total_limit_to_bounded_burst(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT", "4")
    config = build_job_scoped_recovery_config(
        "job-event",
        {
            "remote_provider_event_worker_ids": [21, 22, 23, 24, 25],
            "force_release_explicit_worker_leases": True,
        },
    )

    assert config["explicit_worker_ids"] == [21, 22, 23, 24, 25]
    assert config["total_limit"] == 4
    command = build_job_scoped_recovery_command(
        "job-event",
        config=config,
        python_executable="/usr/bin/python3",
    )
    assert command[command.index("--total-limit") + 1] == "4"
    assert command.count("--explicit-worker-id") == 5
    assert "--profile-prefetch-nonblocking-submit" in command


def test_job_scoped_recovery_defaults_to_nonblocking_profile_prefetch_submit() -> None:
    config = build_job_scoped_recovery_config(
        "job-defaults",
        {
            "service_name": "job-recovery-job-defaults",
        },
    )

    assert config["profile_prefetch_nonblocking_submit"] is True
    assert config["explicit_job_followup_rounds"] == 0
    assert config["workflow_resume_explicit_job"] is True
    assert config["search_seed_discovery_enabled"] is False
    assert config["snapshot_full_materialization_enabled"] is False
    assert config["projection_facet_layering_enabled"] is False
    assert config["excel_intake_recovery_enabled"] is False
    assert config["post_recovery_housekeeping_enabled"] is False

    callback_payload = build_job_scoped_recovery_callback_payload("job-defaults", {}, config=config)
    assert callback_payload["profile_prefetch_nonblocking_submit"] is True
    assert callback_payload["explicit_job_followup_rounds"] == 0
    assert callback_payload["workflow_resume_explicit_job"] is True
    assert callback_payload["search_seed_discovery_enabled"] is False
    assert callback_payload["snapshot_full_materialization_enabled"] is False
    assert callback_payload["projection_facet_layering_enabled"] is False
    assert callback_payload["excel_intake_recovery_enabled"] is False
    assert callback_payload["post_recovery_housekeeping_enabled"] is False

    command = build_job_scoped_recovery_command(
        "job-defaults",
        config=config,
        python_executable="/usr/bin/python3",
    )
    assert "--profile-prefetch-nonblocking-submit" in command
    assert "--disable-search-seed-discovery" in command
    assert "--disable-snapshot-full-materialization" in command
    assert "--disable-projection-facet-layering" in command
    assert "--disable-excel-intake-recovery" in command
    assert "--disable-post-recovery-housekeeping" in command


def test_job_scoped_recovery_allows_explicit_resume_opt_out() -> None:
    config = build_job_scoped_recovery_config(
        "job-opt-out",
        {
            "workflow_resume_explicit_job": False,
        },
    )

    assert config["workflow_resume_explicit_job"] is False
    callback_payload = build_job_scoped_recovery_callback_payload("job-opt-out", {}, config=config)
    assert callback_payload["workflow_resume_explicit_job"] is False
    command = build_job_scoped_recovery_command(
        "job-opt-out",
        config=config,
        python_executable="/usr/bin/python3",
    )
    assert "--disable-workflow-explicit-job-resume" in command
