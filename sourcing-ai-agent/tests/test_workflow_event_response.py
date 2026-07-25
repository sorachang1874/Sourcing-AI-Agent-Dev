from sourcing_agent.workflow_event_response import (
    LIVE_ROSTER_DISCOVERY_LANE_ITEM_KIND,
    SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
    collect_remote_event_followup_targets,
    discovery_lane_has_non_terminal_worker,
    discovery_lane_has_remote_wait_worker,
    discovery_lane_worker_matches,
    linkedin_stage1_discovery_lane_contract,
    remote_event_lane_for_known_provider_worker,
    remote_event_lane_for_worker,
)


def test_remote_event_lane_identifies_linkedin_stage_1_harvest_wait_worker() -> None:
    worker = {
        "worker_id": 101,
        "job_id": "job-stage-1",
        "lane_id": "enrichment_specialist",
        "status": "queued",
        "checkpoint": {
            "stage": "waiting_remote_harvest",
            "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:00+00:00",
        },
        "metadata": {"recovery_kind": "harvest_profile_batch"},
    }

    assert remote_event_lane_for_worker(worker) == "linkedin_stage_1"
    targets = collect_remote_event_followup_targets([worker])
    assert targets["job_ids"] == ["job-stage-1"]
    assert targets["worker_ids"] == [101]
    assert targets["worker_ids_by_job"] == {"job-stage-1": [101]}
    assert targets["lane_counts"] == {"linkedin_stage_1": 1}
    assert targets["recovery_kind_counts"] == {"harvest_profile_batch": 1}


def test_remote_event_lane_can_use_effective_wait_stage_from_worker_projection() -> None:
    worker = {
        "worker_id": 102,
        "job_id": "job-stage-1",
        "lane_id": "acquisition_specialist",
        "status": "running",
        "checkpoint": {"run_id": "remote-run"},
        "effective_status": "waiting_remote_harvest",
        "metadata": {"recovery_kind": "harvest_company_employees"},
    }

    assert remote_event_lane_for_worker(worker) == "linkedin_stage_1"


def test_remote_event_lane_identifies_search_seed_discovery_wait_worker() -> None:
    worker = {
        "worker_id": 103,
        "job_id": "job-scoped-search",
        "lane_id": "search_planner",
        "status": "queued",
        "checkpoint": {
            "stage": "waiting_remote_search",
            "remote_provider_terminal_event": {"run_id": "run-search", "status": "SUCCEEDED"},
        },
        "metadata": {"recovery_kind": "search_seed_discovery"},
    }

    assert remote_event_lane_for_worker(worker) == "linkedin_stage_1"
    targets = collect_remote_event_followup_targets([worker])
    assert targets["job_ids"] == ["job-scoped-search"]
    assert targets["worker_ids"] == [103]
    assert targets["lane_counts"] == {"linkedin_stage_1": 1}
    assert targets["recovery_kind_counts"] == {"search_seed_discovery": 1}


def test_remote_event_lane_identifies_crm_public_web_wait_worker() -> None:
    worker = {
        "worker_id": 201,
        "job_id": "job-public-web",
        "lane_id": "exploration_specialist",
        "status": "running",
        "checkpoint": {
            "stage": "waiting_remote_search",
            "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:01+00:00",
        },
        "metadata": {"recovery_kind": "crm_public_web_search"},
    }

    assert remote_event_lane_for_worker(worker) == "crm_public_web_search"
    targets = collect_remote_event_followup_targets([worker])
    assert targets["job_ids"] == ["job-public-web"]
    assert targets["worker_ids"] == [201]
    assert targets["lane_counts"] == {"crm_public_web_search": 1}
    assert targets["recovery_kind_counts"] == {"crm_public_web_search": 1}


def test_legacy_target_candidate_public_web_lane_requires_explicit_followup_lane() -> None:
    worker = {
        "worker_id": 202,
        "job_id": "job-public-web-legacy",
        "lane_id": "exploration_specialist",
        "status": "running",
        "checkpoint": {
            "stage": "waiting_remote_search",
            "remote_provider_terminal_event_seen_at": "2026-05-08T00:00:01+00:00",
        },
        "metadata": {"recovery_kind": "target_candidate_public_web_search"},
    }

    assert remote_event_lane_for_worker(worker) == "target_candidate_public_web_search"
    assert collect_remote_event_followup_targets([worker])["worker_count"] == 0
    targets = collect_remote_event_followup_targets(
        [worker],
        enabled_lanes={"target_candidate_public_web_search"},
    )
    assert targets["job_ids"] == ["job-public-web-legacy"]
    assert targets["worker_ids"] == [202]
    assert targets["lane_counts"] == {"target_candidate_public_web_search": 1}


def test_remote_event_lane_ignores_non_remote_or_unknown_workers() -> None:
    assert (
        remote_event_lane_for_worker(
            {
                "worker_id": 201,
                "job_id": "job-unknown",
                "lane_id": "exploration_specialist",
                "status": "queued",
                "checkpoint": {"stage": "waiting_remote_search"},
                "metadata": {"recovery_kind": "unknown_public_web_like_worker"},
            }
        )
        == ""
    )
    assert collect_remote_event_followup_targets(
        [
            {
                "worker_id": 202,
                "job_id": "job-completed",
                "lane_id": "enrichment_specialist",
                "status": "completed",
                "checkpoint": {"stage": "waiting_remote_harvest"},
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }
        ]
    )["worker_count"] == 0


def test_remote_event_followup_targets_require_terminal_event_marker() -> None:
    worker = {
        "worker_id": 203,
        "job_id": "job-still-running-provider",
        "lane_id": "enrichment_specialist",
        "status": "queued",
        "checkpoint": {"stage": "waiting_remote_harvest"},
        "metadata": {"recovery_kind": "harvest_profile_batch"},
    }

    assert remote_event_lane_for_worker(worker) == "linkedin_stage_1"
    assert collect_remote_event_followup_targets([worker])["worker_count"] == 0


def test_remote_event_followup_does_not_treat_scripted_fetch_flag_as_terminal_event() -> None:
    worker = {
        "worker_id": 204,
        "job_id": "job-scripted-force-fetch",
        "lane_id": "enrichment_specialist",
        "status": "queued",
        "checkpoint": {
            "stage": "waiting_remote_harvest",
            "force_scripted_terminal_fetch": True,
        },
        "metadata": {"recovery_kind": "harvest_profile_batch"},
    }

    assert remote_event_lane_for_worker(worker) == "linkedin_stage_1"
    assert collect_remote_event_followup_targets([worker])["worker_count"] == 0


def test_known_provider_worker_lane_ignores_terminal_status_for_late_event_audit() -> None:
    worker = {
        "worker_id": 301,
        "job_id": "job-completed",
        "lane_id": "enrichment_specialist",
        "status": "completed",
        "checkpoint": {"stage": "completed", "run_id": "run-late"},
        "metadata": {"recovery_kind": "harvest_profile_batch"},
    }

    assert remote_event_lane_for_worker(worker) == ""
    assert remote_event_lane_for_known_provider_worker(worker) == "linkedin_stage_1"


def test_discovery_lane_contract_resolves_scoped_search_from_execution_preferences() -> None:
    contract = linkedin_stage1_discovery_lane_contract(
        {"delta_baseline_snapshot_id": "20260501T120000"}
    )

    assert contract.event_lane == "linkedin_stage_1"
    assert contract.lane_kind == "scoped_search"
    assert contract.durable_item_kind == SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND
    assert contract.require_any_durable_item is True
    assert contract.require_snapshot_apply_marker is False
    assert discovery_lane_worker_matches(
        {
            "lane_id": "search_planner",
            "metadata": {"recovery_kind": "search_seed_discovery"},
        },
        contract,
    )
    assert discovery_lane_has_remote_wait_worker(
        [
            {
                "lane_id": "search_planner",
                "status": "queued",
                "checkpoint": {"stage": "waiting_remote_search"},
                "metadata": {"recovery_kind": "search_seed_discovery"},
            }
        ],
        contract,
    )
    assert not discovery_lane_has_remote_wait_worker(
        [
            {
                "lane_id": "search_planner",
                "status": "completed",
                "checkpoint": {"stage": "waiting_remote_search"},
                "metadata": {"recovery_kind": "search_seed_discovery"},
            }
        ],
        contract,
    )


def test_discovery_lane_contract_resolves_live_roster_without_search_seed_preferences() -> None:
    contract = linkedin_stage1_discovery_lane_contract({})

    assert contract.event_lane == "linkedin_stage_1"
    assert contract.lane_kind == "live_roster"
    assert contract.durable_item_kind == LIVE_ROSTER_DISCOVERY_LANE_ITEM_KIND
    assert contract.require_any_durable_item is True
    assert discovery_lane_worker_matches(
        {
            "lane_id": "acquisition_specialist",
            "metadata": {"recovery_kind": "harvest_company_employees"},
        },
        contract,
    )


def test_discovery_lane_active_worker_covers_queued_before_remote_wait() -> None:
    contract = linkedin_stage1_discovery_lane_contract({})

    assert discovery_lane_has_non_terminal_worker(
        [
            {
                "lane_id": "acquisition_specialist",
                "status": "running",
                "checkpoint": {"stage": "submitting_remote_harvest"},
                "metadata": {"recovery_kind": "harvest_company_employees"},
            }
        ],
        contract,
    )
    assert not discovery_lane_has_non_terminal_worker(
        [
            {
                "lane_id": "acquisition_specialist",
                "status": "completed",
                "checkpoint": {"stage": "completed"},
                "metadata": {"recovery_kind": "harvest_company_employees"},
            }
        ],
        contract,
    )
    assert not discovery_lane_worker_matches(
        {
            "lane_id": "enrichment_specialist",
            "metadata": {"recovery_kind": "harvest_profile_batch"},
        },
        contract,
    )
