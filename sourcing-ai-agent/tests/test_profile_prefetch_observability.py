from sourcing_agent.acquisition import _merge_profile_prefetch_dispatch_summaries


def test_merge_profile_prefetch_dispatch_summaries_preserves_all_event_plans_and_queues() -> None:
    current_plan = {
        "kind": "profile_prefetch_batch_plan",
        "plan_reason": "ready_to_dispatch",
        "queue_item_count": 223,
        "planned_dispatch_item_count": 200,
        "planned_deferred_item_count": 23,
    }
    former_plan = {
        "kind": "profile_prefetch_batch_plan",
        "plan_reason": "ready_to_dispatch",
        "queue_item_count": 74,
        "planned_dispatch_item_count": 74,
        "planned_deferred_item_count": 0,
    }
    current_summary = {
        "status": "queued",
        "requested_url_count": 223,
        "dispatched_url_count": 200,
        "queued_worker_count": 4,
        "batch_plan": current_plan,
        "profile_prefetch_queue": {
            "kind": "linkedin_profile_prefetch_queue",
            "requested_url_count": 223,
            "queued_url_count": 200,
            "deferred_url_count": 23,
        },
    }
    first_merge = _merge_profile_prefetch_dispatch_summaries(
        [current_summary],
        requested_profile_urls={f"https://www.linkedin.com/in/current-{index}/" for index in range(223)},
    )

    merged = _merge_profile_prefetch_dispatch_summaries(
        [
            first_merge,
            {
                "status": "queued",
                "requested_url_count": 74,
                "dispatched_url_count": 24,
                "queued_worker_count": 1,
                "batch_plan": former_plan,
                "profile_prefetch_queue": {
                    "kind": "linkedin_profile_prefetch_queue",
                    "requested_url_count": 74,
                    "queued_url_count": 24,
                    "deferred_url_count": 50,
                },
            },
        ],
        requested_profile_urls={
            *{f"https://www.linkedin.com/in/current-{index}/" for index in range(223)},
            *{f"https://www.linkedin.com/in/former-{index}/" for index in range(74)},
        },
    )

    assert merged["requested_url_count"] == 297
    assert merged["dispatched_url_count"] == 224
    assert merged["dispatch_event_count"] == 2
    assert merged["profile_prefetch_event_count"] == 2
    assert merged["batch_plan_count"] == 2
    assert merged["profile_prefetch_queue_count"] == 2
    assert [plan["queue_item_count"] for plan in merged["batch_plans"]] == [223, 74]
    assert [event["requested_url_count"] for event in merged["profile_prefetch_events"]] == [223, 74]
    assert merged["latest_batch_plan"]["queue_item_count"] == 74
    assert merged["batch_plan"]["queue_item_count"] == 74


def test_merge_profile_prefetch_dispatch_summaries_clears_submit_anchor_for_terminal_proof() -> None:
    queued_summary = {
        "status": "queued",
        "requested_url_count": 250,
        "dispatched_url_count": 200,
        "queued_worker_count": 1,
        "active_worker_count": 3,
        "deferred_url_count": 50,
        "queued_urls": ["https://www.linkedin.com/in/queued/"],
        "summary_paths": ["/tmp/queued-summary.json"],
        "batch_plan": {
            "kind": "profile_prefetch_batch_plan",
            "queue_item_count": 250,
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
            "kind": "linkedin_profile_prefetch_queue",
            "requested_url_count": 250,
            "queued_url_count": 200,
            "deferred_url_count": 50,
            "pending_url_count": 250,
        },
    }
    terminal_summary = {
        **queued_summary,
        "status": "completed",
        "reason": "registry_all_requested_profiles_fetched",
        "metrics": {"prefetch_elapsed_ms": 420414},
        "registry_terminal_summary": {
            "all_requested_terminal": True,
            "terminal_url_count": 250,
            "fetched_url_count": 250,
            "unrecoverable_url_count": 0,
            "missing_url_count": 0,
            "open_url_count": 0,
        },
        "profile_prefetch_queue": {
            "kind": "linkedin_profile_prefetch_queue",
            "requested_url_count": 250,
            "queued_url_count": 200,
            "deferred_url_count": 50,
            "pending_url_count": 250,
            "registry_all_requested_terminal": True,
            "registry_terminal_url_count": 250,
            "registry_fetched_url_count": 250,
        },
    }

    merged = _merge_profile_prefetch_dispatch_summaries(
        [queued_summary, terminal_summary],
        requested_profile_urls={f"https://www.linkedin.com/in/item-{index}/" for index in range(250)},
    )

    assert merged["status"] == "completed"
    assert merged["reason"] == "registry_all_requested_profiles_fetched"
    assert merged["terminal_proof_only"] is True
    assert merged["submit_anchor"] is False
    assert merged["dispatched_url_count"] == 0
    assert merged["queued_worker_count"] == 0
    assert merged["active_worker_count"] == 0
    assert merged["deferred_url_count"] == 0
    assert merged["queued_urls"] == []
    assert merged["summary_paths"] == []
    assert merged["batch_envelopes"] == []
    assert merged["batch_plan"] == {}
    assert merged["batch_plans"] == []
    queue = merged["profile_prefetch_queue"]
    assert queue["registry_all_requested_terminal"] is True
    assert queue["queued_url_count"] == 0
    assert queue["deferred_url_count"] == 0
    assert queue["pending_url_count"] == 0
    assert queue["queue_quiescent"] is True


def test_merge_profile_prefetch_dispatch_summaries_fails_closed_on_blocked_input() -> None:
    # Decision #1 / invariant 7 / PROFILE_PREFETCH_SCHEDULER_CONTRACT.md:61:
    # a storeless/infrastructure-blocked summary (queued_worker_count=0,
    # dispatched=0) must NOT be merged into "completed" — blocked must win and
    # its two-signal shape must survive the merge, so an acquisition path can
    # never treat infrastructure-unavailable as "all profiles satisfied".
    blocked_urls = [f"https://www.linkedin.com/in/blocked-{index}/" for index in range(3)]
    blocked_summary = {
        "status": "blocked",
        "reason": "profile_refill_store_unavailable",
        "blocked": True,
        "blocked_url_count": len(blocked_urls),
        "blocked_urls": list(blocked_urls),
        "blocked_reason": "profile_refill_store_unavailable",
        "requested_url_count": 3,
        "dispatched_url_count": 0,
        "queued_worker_count": 0,
    }

    merged = _merge_profile_prefetch_dispatch_summaries(
        [blocked_summary],
        requested_profile_urls=set(blocked_urls),
    )

    assert merged["status"] == "blocked"
    assert merged["status"] != "completed"
    assert merged["reason"] == "profile_refill_store_unavailable"
    assert merged["blocked"] is True
    assert merged["blocked_url_count"] == 3
    assert sorted(merged["blocked_urls"]) == sorted(blocked_urls)
    assert merged["dispatched_url_count"] == 0
    assert merged["queued_worker_count"] == 0

    # Blocked must also win when mixed with an otherwise-completed sibling
    # summary (a healthy chunk does not mask an infrastructure failure).
    completed_summary = {
        "status": "completed",
        "requested_url_count": 5,
        "dispatched_url_count": 5,
        "queued_worker_count": 0,
    }
    mixed = _merge_profile_prefetch_dispatch_summaries(
        [completed_summary, blocked_summary],
        requested_profile_urls=set(blocked_urls),
    )
    assert mixed["status"] == "blocked"
    assert mixed["blocked_url_count"] == 3
