from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

_MAX_REASONABLE_EVENT_LAG_MS = 7 * 24 * 60 * 60 * 1000
_REMOTE_TO_LOCAL_MARKER_LAG_THRESHOLD_MS = 5000.0
_REMOTE_TO_NEXT_SUBMIT_HARD_THRESHOLD_MS = 30000.0
_NEXT_SUBMIT_LAG_THRESHOLD_MS = 1000.0
_TINY_BATCH_DEFAULT_THRESHOLD = 5
_PROFILE_SCHEDULER_ALLOWED_SMALL_NORMAL_REASONS = {
    "queue_quiescent_final_tail",
    "low_volume_company",
    "prior_batch_backpressure_ordinal_gate",
    "urgent_user_visible",
}
_PROFILE_SCHEDULER_ALLOWED_BATCH_SIZE_CONTRACTS = {
    "profile_actor_slot_ready_item_packing",
    "profile_actor_slot_durable_wave_item_packing",
}
_PROFILE_SCHEDULER_EXPECTED_LOCK_KINDS = {
    "pg_advisory_xact_lock",
    "pg_try_advisory_xact_lock",
}
_PROFILE_COMPLETION_HANDOFF_EXCLUDED_PLAN_REASONS = {
    "queue_quiescent_final_tail",
}
_PROFILE_COMPLETION_HANDOFF_REFILL_PHASES = {
    "pre_worker_profile_prefetch_refill",
    "profile_prefetch_refill",
    "post_followup_profile_prefetch_refill",
}
_PROFILE_COMPLETION_HANDOFF_TAIL_PHASES = {
    "pre_worker_profile_prefetch_refill",
    "profile_prefetch_refill",
}

_HARVEST_PROFILE_RECOVERY_KIND = "harvest_profile_batch"
_HARVEST_PROFILE_REMOTE_STAGES = {"waiting_remote_harvest"}
_HARVEST_PROFILE_COALESCING_STAGES = {"waiting_profile_coalescing"}
_ACTIVE_WORKER_STATUSES = {"queued", "running", "waiting_remote_harvest"}
_TERMINAL_WORKER_STATUSES = {"completed", "failed", "skipped", "cancelled", "canceled", "interrupted"}
_STRUCTURED_MATERIALIZATION_EVENT_FAMILIES = {"completed_workflow_reconcile", "workflow_materialization"}
_PROFILE_PREFETCH_PHASE_B_PIPELINE_ORDERS = {
    "company_roster_apply_to_profile_prefetch_before_materialization",
    "search_seed_apply_to_profile_prefetch_before_materialization",
}
_PROFILE_PREFETCH_TERMINAL_PROOF_REASONS = {
    "registry_all_requested_profiles_fetched",
    "registry_all_requested_profiles_terminal",
    "registry_terminal_after_lock_revalidation",
    "reused_local_raw_cache",
}


def extract_event_level_efficiency_metrics(
    *,
    job_summary: dict[str, Any] | None = None,
    events: list[dict[str, Any]] | None = None,
    workers: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build service-grade event-response metrics from persisted job state.

    The output intentionally uses persisted events/workers only. It is safe for
    scripted smoke, live smoke, and runtime health because it does not re-read
    provider payloads or materialized candidate artifacts.
    """

    normalized_events = [dict(item) for item in list(events or []) if isinstance(item, dict)]
    normalized_workers = [dict(item) for item in list(workers or []) if isinstance(item, dict)]
    summary = dict(job_summary or {})

    remote_event_lags_ms: list[float] = []
    remote_events_by_worker: dict[int, list[dict[str, Any]]] = {}
    remote_terminal_event_seen_times: list[tuple[int, datetime]] = []
    remote_provider_event_count = 0
    late_remote_provider_event_count = 0
    provider_actor_run_duration_values: list[float] = []
    provider_actor_run_duration_by_key: dict[str, float] = {}
    for event in normalized_events:
        stage = str(event.get("stage") or "").strip()
        payload = dict(event.get("payload") or {})
        if stage != "remote_provider_event" and "remote_provider_event" not in str(event.get("detail") or ""):
            continue
        remote_provider_event_count += 1
        if str(event.get("status") or "").strip() == "received_late":
            late_remote_provider_event_count += 1
        event_metrics = dict(payload.get("event_metrics") or {})
        lag = _safe_float(event_metrics.get("remote_to_local_event_lag_ms"))
        if lag >= 0.0:
            remote_event_lags_ms.append(lag)
        actor_duration = _safe_float(event_metrics.get("actor_run_duration_ms"))
        if actor_duration >= 0.0:
            actor_key = _provider_run_key_from_event_payload(payload)
            if actor_key:
                provider_actor_run_duration_by_key[actor_key] = actor_duration
            else:
                provider_actor_run_duration_values.append(actor_duration)
        worker_ids = _extract_worker_ids(payload)
        if not worker_ids:
            worker_ids = _extract_worker_ids(dict(payload.get("targets") or {}))
        provider_run_key = _provider_run_key_from_event_payload(payload)
        remote_event_record = {
            "event_id": str(event.get("event_id") or "").strip(),
            "remote_completed_at": str(event_metrics.get("remote_completed_at") or "").strip(),
            "local_event_seen_at": str(event_metrics.get("local_event_seen_at") or "").strip(),
            "remote_to_local_event_lag_ms": lag,
            "status": str(event.get("status") or ""),
            "provider_run_key": provider_run_key,
        }
        event_status = str(event.get("status") or "").strip().lower()
        if event_status == "received" or (
            event_status == "received_in_flight" and str(event_metrics.get("remote_completed_at") or "").strip()
        ):
            for worker_id in worker_ids:
                remote_events_by_worker.setdefault(worker_id, []).append(remote_event_record)
                local_seen = _parse_timestamp(str(event_metrics.get("local_event_seen_at") or "").strip())
                if local_seen is None:
                    local_seen = _parse_timestamp(str(event_metrics.get("remote_completed_at") or "").strip())
                if local_seen is not None:
                    remote_terminal_event_seen_times.append((worker_id, local_seen))

    remote_terminal_event_seen_times = []
    for worker_id, records in list(remote_events_by_worker.items()):
        deduped_records: dict[str, dict[str, Any]] = {}
        for index, record in enumerate(records):
            remote_completed_at = str(record.get("remote_completed_at") or "").strip()
            local_event_seen_at = str(record.get("local_event_seen_at") or "").strip()
            provider_run_key = str(record.get("provider_run_key") or "").strip()
            dedupe_key = (
                provider_run_key
                or _normalized_remote_event_timestamp_key(remote_completed_at)
                or _normalized_remote_event_timestamp_key(local_event_seen_at)
                or f"unkeyed:{index}"
            )
            existing = deduped_records.get(dedupe_key)
            if existing is None or _remote_event_record_marker_rank(record) < _remote_event_record_marker_rank(
                existing
            ):
                deduped_records[dedupe_key] = record
        sorted_records = sorted(deduped_records.values(), key=_remote_event_record_marker_rank)
        remote_events_by_worker[worker_id] = sorted_records
        for record in sorted_records:
            local_seen = _parse_timestamp(str(record.get("local_event_seen_at") or "").strip())
            if local_seen is None:
                local_seen = _parse_timestamp(str(record.get("remote_completed_at") or "").strip())
            if local_seen is not None:
                remote_terminal_event_seen_times.append((worker_id, local_seen))
    remote_terminal_event_seen_times.sort(key=lambda item: item[1])

    harvest_completion_events: list[dict[str, Any]] = []
    next_submit_elapsed_ms: list[float] = []
    profile_refill_daemon_elapsed_ms: list[float] = []
    local_completion_to_next_submit_start_ms: list[float] = []
    remote_to_completion_marker_ms: list[float] = []
    remote_to_next_submit_start_ms: list[float] = []
    remote_to_next_submit_finish_ms: list[float] = []
    event_seen_to_completion_marker_ms: list[float] = []
    event_seen_to_next_submit_start_ms: list[float] = []
    event_seen_to_next_submit_finish_ms: list[float] = []
    post_ingest_dispatched_url_count = 0
    post_ingest_candidate_count = 0
    registry_cache_marker_count = 0
    profile_batch_size_values: list[float] = []
    profile_batch_envelope_count = 0
    profile_tiny_batch_count = 0
    profile_unexplained_tiny_batch_count = 0
    provider_slot_underuse_with_backlog_count = 0
    profile_tiny_batch_coalesced_count = 0
    profile_worker_batch_size_values: list[float] = []
    profile_batch_envelope_samples: list[dict[str, Any]] = []
    profile_batch_envelope_worker_ids: set[int] = set()
    profile_queue_snapshot_count = 0
    profile_queue_requested_url_count = 0
    profile_queue_cached_url_count = 0
    profile_queue_ready_url_count = 0
    profile_queue_queued_url_count = 0
    profile_queue_deferred_url_count = 0
    profile_queue_failed_url_count = 0
    profile_queue_pending_url_count = 0
    profile_queue_non_quiescent_count = 0
    profile_queue_max_requested_url_count = 0
    profile_queue_planned_dispatch_owner_missing_count = 0
    profile_queue_planned_dispatch_remote_owner_count = 0
    profile_queue_terminal_state_leak_count = 0
    profile_queue_oldest_pending_age_values: list[float] = []
    profile_queue_refill_state_counts: Counter[str] = Counter()
    profile_queue_samples: list[dict[str, Any]] = []
    profile_batch_plan_count = 0
    profile_batch_plan_queue_item_count = 0
    profile_batch_plan_dispatch_item_count = 0
    profile_batch_plan_deferred_item_count = 0
    profile_batch_plan_available_slot_count = 0
    profile_batch_plan_new_worker_count = 0
    profile_batch_plan_unfilled_slot_count = 0
    profile_batch_plan_underfilled_with_deferred_count = 0
    profile_batch_plan_samples: list[dict[str, Any]] = []
    profile_refill_trigger_count = 0
    profile_refill_dispatched_url_count = 0
    profile_refill_available_slot_count = 0
    profile_refill_new_worker_count = 0
    profile_refill_dispatch_item_count = 0
    profile_refill_deferred_item_count = 0
    profile_refill_unfilled_slot_count = 0
    profile_refill_underfilled_with_deferred_count = 0
    profile_refill_samples: list[dict[str, Any]] = []
    profile_scheduler_lock_samples: list[dict[str, Any]] = []
    profile_scheduler_small_normal_batch_without_reason_count = 0
    profile_scheduler_batch_size_contract_violation_count = 0
    profile_scheduler_retry_wave_isolation_violation_count = 0
    profile_scheduler_same_wave_ordinal_violation_count = 0
    profile_scheduler_url_overlap_violation_count = 0
    profile_scheduler_url_overlap_samples: list[dict[str, Any]] = []
    profile_scheduler_url_owner_by_key: dict[str, str] = {}
    profile_dispatch_events: list[dict[str, Any]] = []
    profile_handoff_dispatch_events: list[dict[str, Any]] = []
    consumed_remote_completion_events: set[tuple[int, str]] = set()

    def _record_profile_batch_envelopes(
        batch_envelopes: list[dict[str, Any]],
        *,
        check_same_wave_ordinal: bool = True,
    ) -> None:
        nonlocal profile_batch_envelope_count
        nonlocal profile_tiny_batch_count
        nonlocal profile_unexplained_tiny_batch_count
        nonlocal provider_slot_underuse_with_backlog_count
        nonlocal profile_scheduler_small_normal_batch_without_reason_count
        nonlocal profile_scheduler_same_wave_ordinal_violation_count
        nonlocal profile_scheduler_url_overlap_violation_count
        profile_batch_envelope_count += len(batch_envelopes)
        if check_same_wave_ordinal:
            profile_scheduler_same_wave_ordinal_violation_count += (
                _profile_scheduler_same_wave_ordinal_violation_count(batch_envelopes)
            )
        for envelope in batch_envelopes:
            if str(envelope.get("source") or "").strip() == "completed_worker_summary":
                owner = str(envelope.get("worker_id") or envelope.get("worker_key") or "").strip()
                if not owner:
                    owner = f"envelope:{len(profile_scheduler_url_owner_by_key)}"
                for profile_url_key in _profile_scheduler_envelope_profile_url_keys(envelope):
                    previous_owner = profile_scheduler_url_owner_by_key.get(profile_url_key)
                    if previous_owner and previous_owner != owner:
                        profile_scheduler_url_overlap_violation_count += 1
                        if len(profile_scheduler_url_overlap_samples) < 20:
                            profile_scheduler_url_overlap_samples.append(
                                {
                                    "profile_url_key": profile_url_key,
                                    "first_owner": previous_owner,
                                    "duplicate_owner": owner,
                                }
                            )
                        continue
                    profile_scheduler_url_owner_by_key[profile_url_key] = owner
            if "dispatched_url_count" in envelope:
                batch_size = _safe_int(envelope.get("dispatched_url_count"))
            else:
                batch_size = _safe_int(envelope.get("batch_size") or envelope.get("requested_url_count"))
            if batch_size > 0:
                profile_batch_size_values.append(float(batch_size))
                if str(envelope.get("source") or "").strip() == "completed_worker_summary":
                    profile_worker_batch_size_values.append(float(batch_size))
            is_tiny_batch = bool(envelope.get("is_tiny_batch"))
            if not is_tiny_batch and 0 < batch_size <= _safe_int(
                envelope.get("small_batch_threshold") or _TINY_BATCH_DEFAULT_THRESHOLD
            ):
                is_tiny_batch = True
            if is_tiny_batch:
                profile_tiny_batch_count += 1
                if not bool(envelope.get("tiny_batch_allowed")):
                    profile_unexplained_tiny_batch_count += 1
            if bool(envelope.get("provider_slot_underuse_with_backlog")):
                provider_slot_underuse_with_backlog_count += 1
            if _profile_scheduler_small_normal_batch_without_reason(envelope):
                profile_scheduler_small_normal_batch_without_reason_count += 1
            if len(profile_batch_envelope_samples) < 20:
                profile_batch_envelope_samples.append(_compact_profile_batch_envelope(envelope))

    def _record_profile_prefetch_payload(profile_prefetch: dict[str, Any]) -> None:
        nonlocal profile_batch_plan_count
        nonlocal profile_batch_plan_queue_item_count
        nonlocal profile_batch_plan_dispatch_item_count
        nonlocal profile_batch_plan_deferred_item_count
        nonlocal profile_batch_plan_available_slot_count
        nonlocal profile_batch_plan_new_worker_count
        nonlocal profile_batch_plan_unfilled_slot_count
        nonlocal profile_batch_plan_underfilled_with_deferred_count
        nonlocal profile_queue_snapshot_count
        nonlocal profile_queue_requested_url_count
        nonlocal profile_queue_cached_url_count
        nonlocal profile_queue_ready_url_count
        nonlocal profile_queue_queued_url_count
        nonlocal profile_queue_deferred_url_count
        nonlocal profile_queue_failed_url_count
        nonlocal profile_queue_pending_url_count
        nonlocal profile_queue_non_quiescent_count
        nonlocal profile_queue_max_requested_url_count
        nonlocal profile_queue_planned_dispatch_owner_missing_count
        nonlocal profile_queue_planned_dispatch_remote_owner_count
        nonlocal profile_queue_terminal_state_leak_count
        nonlocal profile_tiny_batch_coalesced_count
        nonlocal profile_scheduler_batch_size_contract_violation_count
        nonlocal profile_scheduler_retry_wave_isolation_violation_count

        if not profile_prefetch:
            return
        profile_tiny_batch_coalesced_count += _safe_int(profile_prefetch.get("tiny_batch_coalesced_count"))
        scheduler_lock = dict(profile_prefetch.get("scheduler_lock") or {})
        if scheduler_lock and len(profile_scheduler_lock_samples) < 20:
            profile_scheduler_lock_samples.append(_compact_profile_scheduler_lock(scheduler_lock))
        terminal_proof_only = _is_profile_prefetch_terminal_proof_only(profile_prefetch)
        batch_plans = [] if terminal_proof_only else _extract_profile_prefetch_batch_plans(profile_prefetch)
        profile_batch_plan_count += len(batch_plans)
        for plan in batch_plans:
            profile_queue_max_requested_url_count = max(
                profile_queue_max_requested_url_count,
                _safe_int(plan.get("requested_url_count")),
            )
            profile_batch_plan_queue_item_count += _safe_int(plan.get("queue_item_count"))
            profile_batch_plan_dispatch_item_count += _safe_int(plan.get("planned_dispatch_item_count"))
            profile_batch_plan_deferred_item_count += _safe_int(plan.get("planned_deferred_item_count"))
            profile_batch_plan_available_slot_count += _safe_int(plan.get("available_slot_count"))
            profile_batch_plan_new_worker_count += _safe_int(plan.get("planned_new_worker_count"))
            profile_batch_plan_unfilled_slot_count += _safe_int(plan.get("unfilled_available_slot_count"))
            if bool(plan.get("underfilled_with_deferred_items")):
                profile_batch_plan_underfilled_with_deferred_count += 1
            if _profile_scheduler_batch_size_contract_violation(plan):
                profile_scheduler_batch_size_contract_violation_count += 1
            if _profile_scheduler_retry_wave_isolation_violation(plan):
                profile_scheduler_retry_wave_isolation_violation_count += 1
            if len(profile_batch_plan_samples) < 20:
                profile_batch_plan_samples.append(_compact_profile_prefetch_batch_plan(plan))
        batch_envelopes = [] if terminal_proof_only else _extract_profile_batch_envelopes(profile_prefetch, {})
        if batch_envelopes:
            _record_profile_batch_envelopes(batch_envelopes)
        queue_snapshots = _extract_profile_prefetch_queue_snapshots(profile_prefetch)
        profile_queue_snapshot_count += len(queue_snapshots)
        for snapshot in queue_snapshots:
            profile_queue_max_requested_url_count = max(
                profile_queue_max_requested_url_count,
                _safe_int(snapshot.get("requested_url_count")),
            )
            profile_queue_requested_url_count += _safe_int(snapshot.get("requested_url_count"))
            profile_queue_cached_url_count += _safe_int(snapshot.get("cached_url_count"))
            profile_queue_ready_url_count += _safe_int(snapshot.get("ready_url_count"))
            profile_queue_queued_url_count += _safe_int(snapshot.get("queued_url_count"))
            profile_queue_deferred_url_count += _safe_int(snapshot.get("deferred_url_count"))
            profile_queue_failed_url_count += _safe_int(snapshot.get("failed_url_count"))
            profile_queue_pending_url_count += _safe_int(snapshot.get("pending_url_count"))
            profile_queue_planned_dispatch_owner_missing_count += _safe_int(
                snapshot.get("planned_dispatch_owner_missing_count")
            )
            profile_queue_planned_dispatch_remote_owner_count += _safe_int(
                snapshot.get("planned_dispatch_remote_owner_count")
            )
            profile_queue_terminal_state_leak_count += _safe_int(snapshot.get("terminal_queue_state_leak_count"))
            if not bool(snapshot.get("queue_quiescent")):
                profile_queue_non_quiescent_count += 1
            oldest_age = _safe_float(snapshot.get("oldest_pending_item_age_ms"))
            if oldest_age >= 0.0:
                profile_queue_oldest_pending_age_values.append(oldest_age)
            profile_queue_refill_state_counts.update(_safe_counter(snapshot.get("refill_queue_state_counts")))
            if len(profile_queue_samples) < 20:
                profile_queue_samples.append(_compact_profile_prefetch_queue_snapshot(snapshot))
            if _profile_scheduler_queue_retry_wave_isolation_violation(snapshot):
                profile_scheduler_retry_wave_isolation_violation_count += 1

    def _numeric_field_if_present(payload: dict[str, Any], key: str) -> float | None:
        if key not in payload or payload.get(key) in (None, ""):
            return None
        try:
            return float(payload.get(key))
        except (TypeError, ValueError):
            return None

    for event in normalized_events:
        payload = dict(event.get("payload") or {})
        if not _is_profile_prefetch_dispatch_event(payload):
            continue
        if not _is_profile_dispatch_submit_anchor_event(payload):
            continue
        dispatch_payload = _profile_prefetch_dispatch_payload(payload)
        if (
            _safe_int(dispatch_payload.get("dispatched_url_count")) <= 0
            and _safe_int(dispatch_payload.get("queued_worker_count")) <= 0
        ):
            continue
        dispatch_metrics = dict(dispatch_payload.get("metrics") or {})
        provider_submit_elapsed = _numeric_field_if_present(
            dispatch_payload,
            "provider_submit_elapsed_ms",
        )
        is_refill_daemon_group = str(payload.get("kind") or "").strip() == "profile_prefetch_refill_daemon_group"
        dispatch_elapsed = provider_submit_elapsed
        if dispatch_elapsed is None and not is_refill_daemon_group:
            dispatch_elapsed = _safe_float(dispatch_payload.get("elapsed_ms") or dispatch_metrics.get("prefetch_elapsed_ms"))
        dispatch_event_record = {
            "started_at": str(
                dispatch_payload.get("provider_submit_started_at")
                or dispatch_payload.get("started_at")
                or dispatch_metrics.get("prefetch_started_at")
                or payload.get("started_at")
                or event.get("created_at")
                or ""
            ).strip(),
            "finished_at": str(
                dispatch_payload.get("provider_submit_finished_at")
                or dispatch_payload.get("finished_at")
                or dispatch_metrics.get("prefetch_finished_at")
                or payload.get("finished_at")
                or event.get("created_at")
                or ""
            ).strip(),
            "elapsed_ms": dispatch_elapsed if dispatch_elapsed is not None else -1.0,
            "refill_daemon_elapsed_ms": _safe_float(
                dispatch_payload.get("elapsed_ms") or dispatch_metrics.get("prefetch_elapsed_ms")
            ),
            "dispatched_url_count": _safe_int(dispatch_payload.get("dispatched_url_count")),
            "queued_worker_count": _safe_int(dispatch_payload.get("queued_worker_count")),
            "handoff_submit": _is_profile_completion_handoff_submit_event(payload),
            "refill_phase": str(
                payload.get("profile_prefetch_refill_phase")
                or dispatch_payload.get("profile_prefetch_refill_phase")
                or ""
            ).strip(),
        }
        profile_dispatch_events.append(dispatch_event_record)
        if bool(dispatch_event_record.get("handoff_submit")):
            profile_handoff_dispatch_events.append(dispatch_event_record)
    profile_dispatch_events.sort(
        key=lambda item: _parse_timestamp(str(item.get("started_at") or "")) or datetime.max.replace(tzinfo=timezone.utc)
    )
    profile_handoff_dispatch_events.sort(
        key=lambda item: _parse_timestamp(str(item.get("started_at") or "")) or datetime.max.replace(tzinfo=timezone.utc)
    )
    harvest_completion_anchor_times: list[datetime] = []
    for event in normalized_events:
        payload = dict(event.get("payload") or {})
        event_metrics = dict(payload.get("event_metrics") or {})
        if not _is_harvest_completion_event(payload, event_metrics):
            continue
        anchor_value = str(
            event_metrics.get("local_event_apply_started_at")
            or event_metrics.get("profile_completion_event_finished_at")
            or ""
        ).strip()
        anchor_dt = _parse_timestamp(anchor_value)
        if anchor_dt is not None:
            harvest_completion_anchor_times.append(anchor_dt)
    harvest_completion_anchor_times.sort()

    def _later_harvest_completion_before_dispatch(
        *,
        anchor_dt: datetime,
        dispatch_started_at: datetime,
    ) -> bool:
        return any(anchor_dt < candidate_dt <= dispatch_started_at for candidate_dt in harvest_completion_anchor_times)

    def _later_remote_terminal_event_seen_before_dispatch(
        *,
        anchor_dt: datetime,
        dispatch_started_at: datetime,
        current_worker_ids: list[int],
    ) -> bool:
        current_worker_id_set = {int(worker_id) for worker_id in current_worker_ids}
        return any(
            worker_id not in current_worker_id_set and anchor_dt < seen_dt <= dispatch_started_at
            for worker_id, seen_dt in remote_terminal_event_seen_times
        )

    for event in normalized_events:
        payload = dict(event.get("payload") or {})
        event_metrics = dict(payload.get("event_metrics") or {})
        worker_ids = _extract_worker_ids(payload)
        profile_prefetch = dict(payload.get("profile_prefetch") or {})
        if _is_profile_prefetch_dispatch_event(payload):
            dispatch_payload = _profile_prefetch_dispatch_payload(payload)
            _record_profile_prefetch_payload(dispatch_payload)
            if len(profile_refill_samples) < 20 and _is_profile_refill_submit_event(payload):
                profile_refill_samples.append(_compact_profile_prefetch_refill_trigger(payload))
            profile_refill_trigger_count += 1
            profile_refill_dispatched_url_count += _safe_int(dispatch_payload.get("dispatched_url_count"))
            batch_plan = dict(dispatch_payload.get("batch_plan") or {})
            profile_refill_available_slot_count += _safe_int(batch_plan.get("available_slot_count"))
            profile_refill_new_worker_count += _safe_int(batch_plan.get("planned_new_worker_count"))
            profile_refill_dispatch_item_count += _safe_int(batch_plan.get("planned_dispatch_item_count"))
            profile_refill_deferred_item_count += _safe_int(batch_plan.get("planned_deferred_item_count"))
            profile_refill_unfilled_slot_count += _safe_int(batch_plan.get("unfilled_available_slot_count"))
            if bool(batch_plan.get("underfilled_with_deferred_items")):
                profile_refill_underfilled_with_deferred_count += 1
            if (
                _safe_int(dispatch_payload.get("dispatched_url_count")) > 0
                or _safe_int(dispatch_payload.get("queued_worker_count")) > 0
            ):
                dispatch_metrics = dict(dispatch_payload.get("metrics") or {})
                provider_submit_elapsed = _numeric_field_if_present(
                    dispatch_payload,
                    "provider_submit_elapsed_ms",
                )
                elapsed = provider_submit_elapsed if provider_submit_elapsed is not None else -1.0
                is_refill_daemon_group = str(payload.get("kind") or "").strip() == "profile_prefetch_refill_daemon_group"
                if elapsed < 0.0 and not is_refill_daemon_group:
                    elapsed = _safe_float(
                        dispatch_payload.get("elapsed_ms") or dispatch_metrics.get("prefetch_elapsed_ms")
                    )
                if _is_profile_refill_submit_event(payload):
                    if is_refill_daemon_group:
                        refill_daemon_elapsed = _safe_float(
                            dispatch_payload.get("elapsed_ms") or dispatch_metrics.get("prefetch_elapsed_ms")
                        )
                        if refill_daemon_elapsed >= 0.0:
                            profile_refill_daemon_elapsed_ms.append(refill_daemon_elapsed)
                        if provider_submit_elapsed is not None and provider_submit_elapsed >= 0.0:
                            next_submit_elapsed_ms.append(provider_submit_elapsed)
                    elif elapsed >= 0.0:
                        next_submit_elapsed_ms.append(elapsed)
            continue
        is_harvest_completion_event = _is_harvest_completion_event(payload, event_metrics)
        if not is_harvest_completion_event:
            if profile_prefetch:
                _record_profile_prefetch_payload(profile_prefetch)
            continue
        local_apply_started_at = str(event_metrics.get("local_event_apply_started_at") or "").strip()
        next_submit_started_at = str(event_metrics.get("next_submit_attempt_started_at") or "").strip()
        next_submit_finished_at = str(event_metrics.get("next_submit_attempt_finished_at") or "").strip()
        next_submit_semantics = str(event_metrics.get("next_submit_attempt_semantics") or "").strip()
        if next_submit_semantics == "refill_daemon_signal_only":
            dispatch_after_completion_signal = None
            remote_completed_anchor_candidates: list[datetime] = []
            local_seen_anchor_candidates: list[datetime] = []
            for worker_id in worker_ids:
                for remote_event in remote_events_by_worker.get(worker_id, []):
                    remote_completed = _parse_timestamp(str(remote_event.get("remote_completed_at") or "").strip())
                    if remote_completed is not None:
                        remote_completed_anchor_candidates.append(remote_completed)
                        continue
                    local_seen = _parse_timestamp(str(remote_event.get("local_event_seen_at") or "").strip())
                    if local_seen is not None:
                        local_seen_anchor_candidates.append(local_seen)
            if remote_completed_anchor_candidates:
                anchor_dt = min(remote_completed_anchor_candidates)
            elif local_seen_anchor_candidates:
                anchor_dt = min(local_seen_anchor_candidates)
            else:
                local_apply_anchor = (
                    local_apply_started_at
                    or str(event_metrics.get("profile_completion_event_finished_at") or "").strip()
                )
                anchor_dt = _parse_timestamp(local_apply_anchor)
            if anchor_dt is not None:
                current_completion_dt = (
                    _parse_timestamp(
                        local_apply_started_at
                        or str(event_metrics.get("profile_completion_event_finished_at") or "").strip()
                    )
                    or anchor_dt
                )
                for dispatch_event in profile_handoff_dispatch_events:
                    dispatch_started_at = _parse_timestamp(str(dispatch_event.get("started_at") or "").strip())
                    dispatch_finished_at = _parse_timestamp(str(dispatch_event.get("finished_at") or "").strip())
                    if (
                        dispatch_started_at is not None
                        and _later_harvest_completion_before_dispatch(
                            anchor_dt=current_completion_dt,
                            dispatch_started_at=dispatch_started_at,
                        )
                    ):
                        continue
                    if (
                        dispatch_started_at is not None
                        and _later_remote_terminal_event_seen_before_dispatch(
                            anchor_dt=current_completion_dt,
                            dispatch_started_at=dispatch_started_at,
                            current_worker_ids=worker_ids,
                        )
                    ):
                        continue
                    if dispatch_started_at is not None and dispatch_started_at >= anchor_dt:
                        dispatch_after_completion_signal = dispatch_event
                        break
                    if (
                        dispatch_started_at is not None
                        and dispatch_finished_at is not None
                        and dispatch_started_at <= anchor_dt <= dispatch_finished_at
                    ):
                        dispatch_after_completion_signal = dispatch_event
                        break
            if dispatch_after_completion_signal is not None:
                next_submit_started_at = str(dispatch_after_completion_signal.get("started_at") or "").strip()
                next_submit_finished_at = str(dispatch_after_completion_signal.get("finished_at") or "").strip()
                dispatch_elapsed = _safe_float(dispatch_after_completion_signal.get("elapsed_ms"))
                if dispatch_elapsed >= 0.0:
                    next_submit_elapsed_ms.append(dispatch_elapsed)
                refill_daemon_elapsed = _safe_float(
                    dispatch_after_completion_signal.get("refill_daemon_elapsed_ms")
                )
                if refill_daemon_elapsed >= 0.0:
                    profile_refill_daemon_elapsed_ms.append(refill_daemon_elapsed)
            else:
                next_submit_started_at = ""
                next_submit_finished_at = ""
        completion_event = {
            "worker_ids": worker_ids,
            "source": str(payload.get("source") or ""),
            "snapshot_id": str(payload.get("snapshot_id") or ""),
            "local_event_apply_started_at": local_apply_started_at,
            "next_submit_attempt_started_at": next_submit_started_at,
            "next_submit_attempt_finished_at": next_submit_finished_at,
        }
        harvest_completion_events.append(completion_event)
        elapsed = _safe_float(event_metrics.get("post_ingest_prefetch_elapsed_ms"))
        if elapsed >= 0.0 and next_submit_semantics != "refill_daemon_signal_only":
            next_submit_elapsed_ms.append(elapsed)
        start_lag = _duration_ms(local_apply_started_at, next_submit_started_at)
        if start_lag is None and next_submit_semantics == "refill_daemon_signal_only":
            local_started_dt = _parse_timestamp(local_apply_started_at)
            next_started_dt = _parse_timestamp(next_submit_started_at)
            if local_started_dt is not None and next_started_dt is not None and next_started_dt < local_started_dt:
                start_lag = 0.0
        if start_lag is not None:
            local_completion_to_next_submit_start_ms.append(start_lag)
        post_ingest_dispatched_url_count += _safe_int(event_metrics.get("post_ingest_prefetch_dispatched_url_count"))
        post_ingest_candidate_count += _safe_int(event_metrics.get("post_ingest_prefetch_candidate_count"))
        registry_cache_marker_count += _safe_int(event_metrics.get("registry_cache_marker_count"))
        refill_trigger = dict(payload.get("profile_refill_trigger") or {})
        if refill_trigger:
            profile_refill_trigger_count += 1
            profile_refill_dispatched_url_count += _safe_int(refill_trigger.get("dispatched_url_count"))
            profile_refill_available_slot_count += _safe_int(refill_trigger.get("available_slot_count"))
            profile_refill_new_worker_count += _safe_int(refill_trigger.get("planned_new_worker_count"))
            profile_refill_dispatch_item_count += _safe_int(refill_trigger.get("planned_dispatch_item_count"))
            profile_refill_deferred_item_count += _safe_int(refill_trigger.get("planned_deferred_item_count"))
            profile_refill_unfilled_slot_count += _safe_int(refill_trigger.get("unfilled_available_slot_count"))
            if bool(refill_trigger.get("underfilled_with_deferred_items")):
                profile_refill_underfilled_with_deferred_count += 1
            if len(profile_refill_samples) < 20:
                profile_refill_samples.append(_compact_profile_prefetch_refill_trigger(refill_trigger))
        before_envelope_count = profile_batch_envelope_count
        _record_profile_prefetch_payload(profile_prefetch)
        batch_envelopes = _extract_profile_batch_envelopes(profile_prefetch, event_metrics)
        if batch_envelopes:
            profile_batch_envelope_worker_ids.update(worker_ids)
            if profile_batch_envelope_count == before_envelope_count:
                _record_profile_batch_envelopes(batch_envelopes)
        for worker_id in worker_ids:
            if next_submit_semantics == "refill_daemon_signal_not_provider_submit":
                continue
            for remote_event in remote_events_by_worker.get(worker_id, []):
                remote_completed_at = str(remote_event.get("remote_completed_at") or "").strip()
                if not remote_completed_at:
                    continue
                provider_run_key = str(remote_event.get("provider_run_key") or "").strip()
                remote_event_key = (
                    int(worker_id),
                    provider_run_key or _normalized_remote_event_timestamp_key(remote_completed_at),
                )
                if remote_event_key in consumed_remote_completion_events:
                    continue
                local_event_seen_at = str(remote_event.get("local_event_seen_at") or "").strip()
                event_marker_lag = _safe_float(remote_event.get("remote_to_local_event_lag_ms"))
                marker_lag = event_marker_lag if event_marker_lag >= 0.0 else None
                if marker_lag is None:
                    marker_lag = _duration_ms(remote_completed_at, local_event_seen_at)
                if marker_lag is None:
                    marker_lag = _duration_ms(remote_completed_at, local_apply_started_at)
                next_submit_start_lag_value = _duration_ms(remote_completed_at, next_submit_started_at)
                if next_submit_start_lag_value is None and _timestamp_within_interval(
                    remote_completed_at,
                    next_submit_started_at,
                    next_submit_finished_at,
                ):
                    next_submit_start_lag_value = 0.0
                next_submit_finish_lag_value = _duration_ms(remote_completed_at, next_submit_finished_at)
                if (
                    marker_lag is None
                    and next_submit_start_lag_value is None
                    and next_submit_finish_lag_value is None
                ):
                    continue
                consumed_remote_completion_events.add(remote_event_key)
                if marker_lag is not None:
                    remote_to_completion_marker_ms.append(marker_lag)
                if next_submit_start_lag_value is not None:
                    remote_to_next_submit_start_ms.append(next_submit_start_lag_value)
                if next_submit_finish_lag_value is not None:
                    remote_to_next_submit_finish_ms.append(next_submit_finish_lag_value)
                event_seen_marker_lag = _duration_ms(local_event_seen_at, local_apply_started_at)
                event_seen_submit_start_lag = _duration_ms(local_event_seen_at, next_submit_started_at)
                if event_seen_submit_start_lag is None and _timestamp_within_interval(
                    local_event_seen_at,
                    next_submit_started_at,
                    next_submit_finished_at,
                ):
                    event_seen_submit_start_lag = 0.0
                event_seen_submit_finish_lag = _duration_ms(local_event_seen_at, next_submit_finished_at)
                if event_seen_marker_lag is not None:
                    event_seen_to_completion_marker_ms.append(event_seen_marker_lag)
                if event_seen_submit_start_lag is not None:
                    event_seen_to_next_submit_start_ms.append(event_seen_submit_start_lag)
                if event_seen_submit_finish_lag is not None:
                    event_seen_to_next_submit_finish_ms.append(event_seen_submit_finish_lag)

    worker_batch_envelopes = _extract_profile_batch_envelopes_from_workers(
        normalized_workers,
        excluded_worker_ids=profile_batch_envelope_worker_ids,
    )
    _record_profile_batch_envelopes(worker_batch_envelopes, check_same_wave_ordinal=False)
    profile_worker_dispatch_events = _extract_profile_worker_dispatch_events(normalized_workers)
    next_submit_opportunity = _build_next_submit_opportunity_report(
        harvest_completion_anchor_times=harvest_completion_anchor_times,
        profile_dispatch_events=profile_dispatch_events,
        profile_worker_dispatch_events=profile_worker_dispatch_events,
        profile_handoff_dispatch_events=profile_handoff_dispatch_events,
        requested_url_count=profile_queue_max_requested_url_count,
        remote_to_next_submit_sample_count=len(remote_to_next_submit_start_ms),
        local_to_next_submit_sample_count=len(local_completion_to_next_submit_start_ms),
        next_submit_attempt_sample_count=len(next_submit_elapsed_ms),
    )
    next_submit_metric_not_applicable = next_submit_opportunity.get("applicable") is False
    ignored_next_submit_slo_samples = (
        {
            "reason": str(next_submit_opportunity.get("reason") or ""),
            "remote_to_next_submit_start_ms": _stats(remote_to_next_submit_start_ms),
            "remote_to_next_submit_finish_ms": _stats(remote_to_next_submit_finish_ms),
            "local_completion_to_next_submit_start_ms": _stats(local_completion_to_next_submit_start_ms),
            "event_seen_to_next_submit_start_ms": _stats(event_seen_to_next_submit_start_ms),
            "event_seen_to_next_submit_finish_ms": _stats(event_seen_to_next_submit_finish_ms),
            "next_submit_provider_attempt_elapsed_ms": _stats(next_submit_elapsed_ms),
        }
        if next_submit_metric_not_applicable
        and (
            remote_to_next_submit_start_ms
            or remote_to_next_submit_finish_ms
            or local_completion_to_next_submit_start_ms
            or event_seen_to_next_submit_start_ms
            or event_seen_to_next_submit_finish_ms
            or next_submit_elapsed_ms
        )
        else {}
    )
    if next_submit_metric_not_applicable:
        remote_to_next_submit_start_slo_ms: list[float] = []
        remote_to_next_submit_finish_slo_ms: list[float] = []
        local_completion_to_next_submit_start_slo_ms: list[float] = []
        event_seen_to_next_submit_start_slo_ms: list[float] = []
        event_seen_to_next_submit_finish_slo_ms: list[float] = []
        next_submit_elapsed_slo_ms: list[float] = []
    else:
        remote_to_next_submit_start_slo_ms = remote_to_next_submit_start_ms
        remote_to_next_submit_finish_slo_ms = remote_to_next_submit_finish_ms
        local_completion_to_next_submit_start_slo_ms = local_completion_to_next_submit_start_ms
        event_seen_to_next_submit_start_slo_ms = event_seen_to_next_submit_start_ms
        event_seen_to_next_submit_finish_slo_ms = event_seen_to_next_submit_finish_ms
        next_submit_elapsed_slo_ms = next_submit_elapsed_ms
    next_submit_provider_elapsed_report_ms = next_submit_elapsed_slo_ms
    if (
        next_submit_metric_not_applicable
        and str(next_submit_opportunity.get("reason") or "") == "all_profile_urls_submitted_by_non_handoff_dispatch"
    ):
        # The completion handoff SLO is not applicable, but the real non-handoff
        # profile dispatch cost remains useful for smoke/signoff observability.
        next_submit_provider_elapsed_report_ms = next_submit_elapsed_ms
    profile_scheduler_contract = _build_profile_scheduler_contract_report(
        batch_envelopes=profile_batch_envelope_samples,
        batch_plans=profile_batch_plan_samples,
        queue_snapshots=profile_queue_samples,
        refill_triggers=profile_refill_samples,
        scheduler_locks=profile_scheduler_lock_samples,
        envelope_count=profile_batch_envelope_count,
        batch_plan_count=profile_batch_plan_count,
        queue_snapshot_count=profile_queue_snapshot_count,
        refill_trigger_count=profile_refill_trigger_count,
        planned_dispatch_owner_missing_count=profile_queue_planned_dispatch_owner_missing_count,
        terminal_queue_state_leak_count=profile_queue_terminal_state_leak_count,
        underfilled_with_deferred_count=profile_batch_plan_underfilled_with_deferred_count,
        small_normal_batch_without_reason_count=profile_scheduler_small_normal_batch_without_reason_count,
        batch_size_contract_violation_count=profile_scheduler_batch_size_contract_violation_count,
        retry_wave_isolation_violation_count=profile_scheduler_retry_wave_isolation_violation_count,
        same_wave_ordinal_violation_count=profile_scheduler_same_wave_ordinal_violation_count,
        url_overlap_violation_count=profile_scheduler_url_overlap_violation_count,
        url_overlap_samples=profile_scheduler_url_overlap_samples,
    )

    worker_metrics = _extract_worker_efficiency_metrics(normalized_workers, now=now)
    provider_io_metrics = _extract_provider_io_metrics(
        workers=normalized_workers,
        actor_run_duration_values=provider_actor_run_duration_values,
        actor_run_duration_by_key=provider_actor_run_duration_by_key,
    )
    reconcile_metrics = _extract_reconcile_and_materialize_metrics(
        job_summary=summary,
        events=normalized_events,
        workers=normalized_workers,
    )
    writer_lock_wait_metrics = _extract_writer_lock_wait_metrics(normalized_events)
    materialization_io_metrics = _extract_materialization_io_metrics(normalized_events)
    remote_to_local_marker_lag = _stats(remote_to_completion_marker_ms)
    remote_to_next_submit_start_lag = _stats(remote_to_next_submit_start_slo_ms)
    next_submit_start_lag = _stats(local_completion_to_next_submit_start_slo_ms)
    remote_to_local_marker_slow = bool(
        remote_to_local_marker_lag
        and _safe_float(remote_to_local_marker_lag.get("max")) > _REMOTE_TO_LOCAL_MARKER_LAG_THRESHOLD_MS
    )
    remote_to_next_submit_hard_violation = bool(
        remote_to_next_submit_start_lag
        and _safe_float(remote_to_next_submit_start_lag.get("max")) > _REMOTE_TO_NEXT_SUBMIT_HARD_THRESHOLD_MS
    )
    violation_count = (
        _safe_int(worker_metrics.get("pre_submit_provider_worker_count"))
        + _safe_int(worker_metrics.get("coalescing_wait_worker_count"))
            + _safe_int(profile_scheduler_contract.get("violation_count"))
            + (
                1
            if remote_to_next_submit_hard_violation
            else 0
        )
        + (
            1
            if next_submit_start_lag
            and _safe_float(next_submit_start_lag.get("max")) > _NEXT_SUBMIT_LAG_THRESHOLD_MS
            else 0
        )
    )
    return {
        "report_available": bool(
            remote_provider_event_count
            or harvest_completion_events
            or normalized_workers
            or reconcile_metrics.get("completed_reconcile_event_count")
            or reconcile_metrics.get("materialize_call_count")
            or writer_lock_wait_metrics.get("writer_lock_event_count")
            or materialization_io_metrics.get("report_available")
            or profile_queue_snapshot_count
        ),
        "remote_provider_event_count": remote_provider_event_count,
        "late_remote_provider_event_count": late_remote_provider_event_count,
        "harvest_completion_event_count": len(harvest_completion_events),
        "remote_to_local_event_lag_ms": _stats(remote_event_lags_ms),
        "remote_to_local_marker_lag_ms": remote_to_local_marker_lag,
        "remote_to_next_submit_start_ms": remote_to_next_submit_start_lag,
        "remote_to_next_submit_finish_ms": _stats(remote_to_next_submit_finish_slo_ms),
        "remote_to_next_submit_segments_ms": {
            "remote_completed_to_event_seen_ms": _stats(remote_event_lags_ms),
            "event_seen_to_completion_marker_ms": _stats(event_seen_to_completion_marker_ms),
            "completion_marker_to_next_submit_start_ms": next_submit_start_lag,
            "event_seen_to_next_submit_start_ms": _stats(event_seen_to_next_submit_start_slo_ms),
            "event_seen_to_next_submit_finish_ms": _stats(event_seen_to_next_submit_finish_slo_ms),
            "next_submit_provider_attempt_elapsed_ms": _stats(next_submit_elapsed_slo_ms),
        },
        "local_completion_to_next_submit_start_ms": next_submit_start_lag,
        "local_to_next_submit_start_ms": next_submit_start_lag,
        "next_submit_provider_attempt_elapsed_ms": _stats(next_submit_provider_elapsed_report_ms),
        "next_submit_attempt_elapsed_ms": _stats(next_submit_elapsed_slo_ms),
        "profile_refill_daemon_elapsed_ms": _stats(profile_refill_daemon_elapsed_ms),
        "next_submit_opportunity": next_submit_opportunity,
        "ignored_next_submit_slo_samples": ignored_next_submit_slo_samples,
        "provider_slot_to_remote_wait_started_ms": dict(
            worker_metrics.get("provider_slot_to_remote_wait_started_ms") or {}
        ),
        "post_ingest_prefetch_dispatched_url_count": post_ingest_dispatched_url_count,
        "post_ingest_prefetch_candidate_count": post_ingest_candidate_count,
        "registry_cache_marker_count": registry_cache_marker_count,
        "profile_batch_envelopes": {
            "envelope_count": profile_batch_envelope_count,
            "batch_size": _stats(profile_batch_size_values),
            "provider_worker_batch_size": _stats(profile_worker_batch_size_values),
            "tiny_batch_count": profile_tiny_batch_count,
            "unexplained_tiny_batch_count": profile_unexplained_tiny_batch_count,
            "provider_slot_underuse_with_backlog_count": provider_slot_underuse_with_backlog_count,
            "tiny_batch_coalesced_count": profile_tiny_batch_coalesced_count,
            "samples": profile_batch_envelope_samples,
        },
        "profile_prefetch_queue": {
            "snapshot_count": profile_queue_snapshot_count,
            "requested_url_count": profile_queue_requested_url_count,
            "cached_url_count": profile_queue_cached_url_count,
            "ready_url_count": profile_queue_ready_url_count,
            "queued_url_count": profile_queue_queued_url_count,
            "deferred_url_count": profile_queue_deferred_url_count,
            "failed_url_count": profile_queue_failed_url_count,
            "pending_url_count": profile_queue_pending_url_count,
            "non_quiescent_snapshot_count": profile_queue_non_quiescent_count,
            "planned_dispatch_owner_missing_count": profile_queue_planned_dispatch_owner_missing_count,
            "planned_dispatch_remote_owner_count": profile_queue_planned_dispatch_remote_owner_count,
            "terminal_queue_state_leak_count": profile_queue_terminal_state_leak_count,
            "oldest_pending_item_age_ms": _stats(profile_queue_oldest_pending_age_values),
            "refill_queue_state_counts": dict(profile_queue_refill_state_counts),
            "samples": profile_queue_samples,
        },
        "profile_prefetch_batch_plan": {
            "plan_count": profile_batch_plan_count,
            "queue_item_count": profile_batch_plan_queue_item_count,
            "planned_dispatch_item_count": profile_batch_plan_dispatch_item_count,
            "planned_deferred_item_count": profile_batch_plan_deferred_item_count,
            "available_slot_count": profile_batch_plan_available_slot_count,
            "planned_new_worker_count": profile_batch_plan_new_worker_count,
            "unfilled_available_slot_count": profile_batch_plan_unfilled_slot_count,
            "underfilled_with_deferred_items_count": profile_batch_plan_underfilled_with_deferred_count,
            "samples": profile_batch_plan_samples,
        },
        "profile_prefetch_refill": {
            "trigger_count": profile_refill_trigger_count,
            "dispatched_url_count": profile_refill_dispatched_url_count,
            "available_slot_count": profile_refill_available_slot_count,
            "planned_new_worker_count": profile_refill_new_worker_count,
            "planned_dispatch_item_count": profile_refill_dispatch_item_count,
            "planned_deferred_item_count": profile_refill_deferred_item_count,
            "unfilled_available_slot_count": profile_refill_unfilled_slot_count,
            "underfilled_with_deferred_items_count": profile_refill_underfilled_with_deferred_count,
            "samples": profile_refill_samples,
        },
        "profile_scheduler_contract": profile_scheduler_contract,
        "provider_slots": worker_metrics,
        "provider_io": provider_io_metrics,
        "reconcile": reconcile_metrics,
        "writer_lock": writer_lock_wait_metrics,
        "materialization_io": materialization_io_metrics,
        "thresholds_ms": {
            "remote_to_local_marker_lag_diagnostic": _REMOTE_TO_LOCAL_MARKER_LAG_THRESHOLD_MS,
            "remote_to_next_submit_start_hard": _REMOTE_TO_NEXT_SUBMIT_HARD_THRESHOLD_MS,
            "local_to_next_submit_start": _NEXT_SUBMIT_LAG_THRESHOLD_MS,
        },
        "violation_count": violation_count,
        "violation_detected": violation_count > 0,
        "diagnostic_violation_count": 1 if remote_to_local_marker_slow else 0,
        "diagnostic_violation_detected": remote_to_local_marker_slow,
    }


def aggregate_event_level_efficiency_metrics(reports: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_reports = [dict(item) for item in list(reports or []) if isinstance(item, dict)]
    available_reports = [item for item in normalized_reports if bool(item.get("report_available"))]
    remote_event_lags: list[float] = []
    remote_marker_lags: list[float] = []
    remote_to_next_submit_start_lags: list[float] = []
    remote_to_next_submit_finish_lags: list[float] = []
    local_completion_to_next_submit_lags: list[float] = []
    event_seen_to_completion_marker_lags: list[float] = []
    event_seen_to_next_submit_start_lags: list[float] = []
    event_seen_to_next_submit_finish_lags: list[float] = []
    next_submit_elapsed: list[float] = []
    max_true_provider_slots = 0
    max_remote_actor_workers = 0
    max_pre_submit_workers = 0
    max_coalescing_wait_workers = 0
    materialize_call_count = 0
    duplicate_reconcile_count = 0
    repeated_materialize_signature_count = 0
    marker_backfill_count = 0
    marker_backfill_repeat_count = 0
    materialize_started_repeat_count = 0
    materialize_deferred_count = 0
    lease_skipped_count = 0
    writer_lock_wait_values: list[float] = []
    writer_lock_held_values: list[float] = []
    writer_lock_event_count = 0
    remote_wait_age_values: list[float] = []
    provider_lease_age_values: list[float] = []
    provider_slot_to_remote_wait_started_values: list[float] = []
    coalescing_wait_age_values: list[float] = []
    coalescing_until_ready_values: list[float] = []
    profile_batch_size_values: list[float] = []
    profile_batch_envelope_count = 0
    profile_tiny_batch_count = 0
    profile_unexplained_tiny_batch_count = 0
    provider_slot_underuse_with_backlog_count = 0
    profile_tiny_batch_coalesced_count = 0
    profile_worker_batch_size_values: list[float] = []
    profile_queue_snapshot_count = 0
    profile_queue_requested_url_count = 0
    profile_queue_cached_url_count = 0
    profile_queue_ready_url_count = 0
    profile_queue_queued_url_count = 0
    profile_queue_deferred_url_count = 0
    profile_queue_failed_url_count = 0
    profile_queue_pending_url_count = 0
    profile_queue_non_quiescent_count = 0
    profile_queue_planned_dispatch_owner_missing_count = 0
    profile_queue_planned_dispatch_remote_owner_count = 0
    profile_queue_terminal_state_leak_count = 0
    profile_queue_oldest_pending_age_values: list[float] = []
    profile_queue_refill_state_counts: Counter[str] = Counter()
    profile_batch_plan_count = 0
    profile_batch_plan_queue_item_count = 0
    profile_batch_plan_dispatch_item_count = 0
    profile_batch_plan_deferred_item_count = 0
    profile_batch_plan_available_slot_count = 0
    profile_batch_plan_new_worker_count = 0
    profile_batch_plan_unfilled_slot_count = 0
    profile_batch_plan_underfilled_with_deferred_count = 0
    profile_refill_trigger_count = 0
    profile_refill_dispatched_url_count = 0
    profile_refill_available_slot_count = 0
    profile_refill_new_worker_count = 0
    profile_refill_dispatch_item_count = 0
    profile_refill_deferred_item_count = 0
    profile_refill_unfilled_slot_count = 0
    profile_refill_underfilled_with_deferred_count = 0
    profile_scheduler_report_count = 0
    profile_scheduler_violation_case_count = 0
    profile_scheduler_violation_count = 0
    profile_scheduler_same_wave_ordinal_violation_count = 0
    profile_scheduler_url_overlap_violation_count = 0
    profile_scheduler_batch_size_contract_violation_count = 0
    profile_scheduler_small_normal_batch_without_reason_count = 0
    profile_scheduler_retry_wave_isolation_violation_count = 0
    profile_scheduler_slot_refill_violation_count = 0
    profile_scheduler_advisory_lock_missing_count = 0
    profile_scheduler_planned_dispatch_owner_missing_count = 0
    profile_scheduler_terminal_queue_state_leak_count = 0
    provider_io_report_count = 0
    provider_io_actor_run_duration_values: list[float] = []
    provider_io_dataset_download_duration_values: list[float] = []
    materialization_io_report_count = 0
    materialization_io_sync_total_values: list[float] = []
    materialization_io_delta_replace_values: list[float] = []
    materialization_io_artifact_build_values: list[float] = []
    materialization_io_state_upsert_values: list[float] = []
    materialization_io_writer_wait_values: list[float] = []
    profile_refill_daemon_elapsed_values: list[float] = []
    next_submit_opportunities: list[dict[str, Any]] = []
    violation_case_count = 0
    diagnostic_violation_case_count = 0
    for report in available_reports:
        remote_event_lags.extend(_stats_values(report.get("remote_to_local_event_lag_ms")))
        remote_marker_lags.extend(_stats_values(report.get("remote_to_local_marker_lag_ms")))
        remote_to_next_submit_start_lags.extend(_stats_values(report.get("remote_to_next_submit_start_ms")))
        remote_to_next_submit_finish_lags.extend(_stats_values(report.get("remote_to_next_submit_finish_ms")))
        local_completion_to_next_submit_lags.extend(
            _stats_values(
                report.get("local_completion_to_next_submit_start_ms")
                or report.get("local_to_next_submit_start_ms")
            )
        )
        segments = dict(report.get("remote_to_next_submit_segments_ms") or {})
        event_seen_to_completion_marker_lags.extend(
            _stats_values(segments.get("event_seen_to_completion_marker_ms"))
        )
        event_seen_to_next_submit_start_lags.extend(_stats_values(segments.get("event_seen_to_next_submit_start_ms")))
        event_seen_to_next_submit_finish_lags.extend(
            _stats_values(segments.get("event_seen_to_next_submit_finish_ms"))
        )
        next_submit_elapsed.extend(
            _stats_values(
                report.get("next_submit_provider_attempt_elapsed_ms")
                or report.get("next_submit_attempt_elapsed_ms")
            )
        )
        profile_refill_daemon_elapsed_values.extend(
            _stats_values(report.get("profile_refill_daemon_elapsed_ms"))
        )
        next_submit_opportunity = dict(report.get("next_submit_opportunity") or {})
        if next_submit_opportunity:
            next_submit_opportunities.append(next_submit_opportunity)
        provider_slots = dict(report.get("provider_slots") or {})
        max_true_provider_slots = max(
            max_true_provider_slots,
            _safe_int(provider_slots.get("true_active_provider_slot_worker_count")),
        )
        max_remote_actor_workers = max(
            max_remote_actor_workers,
            _safe_int(provider_slots.get("remote_actor_worker_count")),
        )
        max_pre_submit_workers = max(max_pre_submit_workers, _safe_int(provider_slots.get("pre_submit_provider_worker_count")))
        max_coalescing_wait_workers = max(
            max_coalescing_wait_workers,
            _safe_int(provider_slots.get("coalescing_wait_worker_count")),
        )
        remote_wait_age_values.extend(_stats_values(provider_slots.get("remote_wait_age_ms")))
        provider_lease_age_values.extend(_stats_values(provider_slots.get("provider_lease_age_ms")))
        provider_slot_to_remote_wait_started_values.extend(
            _stats_values(provider_slots.get("provider_slot_to_remote_wait_started_ms"))
        )
        coalescing_wait_age_values.extend(_stats_values(provider_slots.get("coalescing_wait_age_ms")))
        coalescing_until_ready_values.extend(_stats_values(provider_slots.get("coalescing_until_ready_ms")))
        reconcile = dict(report.get("reconcile") or {})
        materialize_call_count += _safe_int(reconcile.get("materialize_call_count"))
        duplicate_reconcile_count += _safe_int(reconcile.get("same_worker_reconcile_repeat_count"))
        repeated_materialize_signature_count += _safe_int(reconcile.get("repeated_materialize_signature_count"))
        marker_backfill_count += _safe_int(reconcile.get("marker_backfill_count"))
        marker_backfill_repeat_count += _safe_int(reconcile.get("marker_backfill_repeat_count"))
        materialize_started_repeat_count += _safe_int(reconcile.get("materialize_started_repeat_count"))
        materialize_deferred_count += _safe_int(reconcile.get("materialize_deferred_count"))
        lease_skipped_count += _safe_int(reconcile.get("lease_skipped_count"))
        writer_lock_report = dict(report.get("writer_lock") or {})
        writer_lock_wait_values.extend(_stats_values(writer_lock_report.get("writer_lock_wait_ms")))
        writer_lock_held_values.extend(_stats_values(writer_lock_report.get("writer_lock_held_ms")))
        writer_lock_event_count += _safe_int(writer_lock_report.get("writer_lock_event_count"))
        batch_report = dict(report.get("profile_batch_envelopes") or {})
        profile_batch_envelope_count += _safe_int(batch_report.get("envelope_count"))
        profile_tiny_batch_count += _safe_int(batch_report.get("tiny_batch_count"))
        profile_unexplained_tiny_batch_count += _safe_int(batch_report.get("unexplained_tiny_batch_count"))
        provider_slot_underuse_with_backlog_count += _safe_int(
            batch_report.get("provider_slot_underuse_with_backlog_count")
        )
        profile_tiny_batch_coalesced_count += _safe_int(batch_report.get("tiny_batch_coalesced_count"))
        profile_batch_size_values.extend(_stats_values(batch_report.get("batch_size")))
        profile_worker_batch_size_values.extend(_stats_values(batch_report.get("provider_worker_batch_size")))
        queue_report = dict(report.get("profile_prefetch_queue") or {})
        profile_queue_snapshot_count += _safe_int(queue_report.get("snapshot_count"))
        profile_queue_requested_url_count += _safe_int(queue_report.get("requested_url_count"))
        profile_queue_cached_url_count += _safe_int(queue_report.get("cached_url_count"))
        profile_queue_ready_url_count += _safe_int(queue_report.get("ready_url_count"))
        profile_queue_queued_url_count += _safe_int(queue_report.get("queued_url_count"))
        profile_queue_deferred_url_count += _safe_int(queue_report.get("deferred_url_count"))
        profile_queue_failed_url_count += _safe_int(queue_report.get("failed_url_count"))
        profile_queue_pending_url_count += _safe_int(queue_report.get("pending_url_count"))
        profile_queue_non_quiescent_count += _safe_int(queue_report.get("non_quiescent_snapshot_count"))
        profile_queue_planned_dispatch_owner_missing_count += _safe_int(
            queue_report.get("planned_dispatch_owner_missing_count")
        )
        profile_queue_planned_dispatch_remote_owner_count += _safe_int(
            queue_report.get("planned_dispatch_remote_owner_count")
        )
        profile_queue_terminal_state_leak_count += _safe_int(queue_report.get("terminal_queue_state_leak_count"))
        profile_queue_oldest_pending_age_values.extend(_stats_values(queue_report.get("oldest_pending_item_age_ms")))
        profile_queue_refill_state_counts.update(_safe_counter(queue_report.get("refill_queue_state_counts")))
        batch_plan_report = dict(report.get("profile_prefetch_batch_plan") or {})
        profile_batch_plan_count += _safe_int(batch_plan_report.get("plan_count"))
        profile_batch_plan_queue_item_count += _safe_int(batch_plan_report.get("queue_item_count"))
        profile_batch_plan_dispatch_item_count += _safe_int(batch_plan_report.get("planned_dispatch_item_count"))
        profile_batch_plan_deferred_item_count += _safe_int(batch_plan_report.get("planned_deferred_item_count"))
        profile_batch_plan_available_slot_count += _safe_int(batch_plan_report.get("available_slot_count"))
        profile_batch_plan_new_worker_count += _safe_int(batch_plan_report.get("planned_new_worker_count"))
        profile_batch_plan_unfilled_slot_count += _safe_int(batch_plan_report.get("unfilled_available_slot_count"))
        profile_batch_plan_underfilled_with_deferred_count += _safe_int(
            batch_plan_report.get("underfilled_with_deferred_items_count")
        )
        refill_report = dict(report.get("profile_prefetch_refill") or {})
        profile_refill_trigger_count += _safe_int(refill_report.get("trigger_count"))
        profile_refill_dispatched_url_count += _safe_int(refill_report.get("dispatched_url_count"))
        profile_refill_available_slot_count += _safe_int(refill_report.get("available_slot_count"))
        profile_refill_new_worker_count += _safe_int(refill_report.get("planned_new_worker_count"))
        profile_refill_dispatch_item_count += _safe_int(refill_report.get("planned_dispatch_item_count"))
        profile_refill_deferred_item_count += _safe_int(refill_report.get("planned_deferred_item_count"))
        profile_refill_unfilled_slot_count += _safe_int(refill_report.get("unfilled_available_slot_count"))
        profile_refill_underfilled_with_deferred_count += _safe_int(
            refill_report.get("underfilled_with_deferred_items_count")
        )
        scheduler_contract = dict(report.get("profile_scheduler_contract") or {})
        if bool(scheduler_contract.get("report_available")):
            profile_scheduler_report_count += 1
            profile_scheduler_violation_count += _safe_int(scheduler_contract.get("violation_count"))
            profile_scheduler_same_wave_ordinal_violation_count += _safe_int(
                scheduler_contract.get("same_wave_ordinal_violation_count")
            )
            profile_scheduler_url_overlap_violation_count += _safe_int(
                scheduler_contract.get("url_overlap_violation_count")
            )
            profile_scheduler_batch_size_contract_violation_count += _safe_int(
                scheduler_contract.get("batch_size_contract_violation_count")
            )
            profile_scheduler_small_normal_batch_without_reason_count += _safe_int(
                scheduler_contract.get("small_normal_batch_without_reason_count")
            )
            profile_scheduler_retry_wave_isolation_violation_count += _safe_int(
                scheduler_contract.get("retry_wave_isolation_violation_count")
            )
            profile_scheduler_slot_refill_violation_count += _safe_int(
                scheduler_contract.get("slot_refill_violation_count")
            )
            profile_scheduler_advisory_lock_missing_count += _safe_int(
                scheduler_contract.get("advisory_lock_missing_count")
            )
            profile_scheduler_planned_dispatch_owner_missing_count += _safe_int(
                scheduler_contract.get("planned_dispatch_owner_missing_count")
            )
            profile_scheduler_terminal_queue_state_leak_count += _safe_int(
                scheduler_contract.get("terminal_queue_state_leak_count")
            )
            if bool(scheduler_contract.get("violation_detected")):
                profile_scheduler_violation_case_count += 1
        provider_io_report = dict(report.get("provider_io") or {})
        if bool(provider_io_report.get("report_available")):
            provider_io_report_count += 1
        provider_io_actor_run_duration_values.extend(_stats_values(provider_io_report.get("actor_run_duration_ms")))
        provider_io_dataset_download_duration_values.extend(
            _stats_values(provider_io_report.get("dataset_download_duration_ms"))
        )
        materialization_io_report = dict(report.get("materialization_io") or {})
        if bool(materialization_io_report.get("report_available")):
            materialization_io_report_count += 1
        materialization_io_sync_total_values.extend(_stats_values(materialization_io_report.get("sync_total_ms")))
        materialization_io_delta_replace_values.extend(
            _stats_values(materialization_io_report.get("candidate_delta_control_plane_replace_ms"))
        )
        materialization_io_artifact_build_values.extend(
            _stats_values(materialization_io_report.get("candidate_artifact_build_ms"))
        )
        materialization_io_state_upsert_values.extend(
            _stats_values(materialization_io_report.get("candidate_artifact_state_upsert_ms"))
        )
        materialization_io_writer_wait_values.extend(
            _stats_values(materialization_io_report.get("materialization_writer_wait_ms"))
        )
        if bool(report.get("violation_detected")):
            violation_case_count += 1
        if bool(report.get("diagnostic_violation_detected")):
            diagnostic_violation_case_count += 1
    return {
        "report_count": len(available_reports),
        "remote_to_local_event_lag_ms": _stats(remote_event_lags),
        "remote_to_local_marker_lag_ms": _stats(remote_marker_lags),
        "remote_to_next_submit_start_ms": _stats(remote_to_next_submit_start_lags),
        "remote_to_next_submit_finish_ms": _stats(remote_to_next_submit_finish_lags),
        "remote_to_next_submit_segments_ms": {
            "remote_completed_to_event_seen_ms": _stats(remote_event_lags),
            "event_seen_to_completion_marker_ms": _stats(event_seen_to_completion_marker_lags),
            "completion_marker_to_next_submit_start_ms": _stats(local_completion_to_next_submit_lags),
            "event_seen_to_next_submit_start_ms": _stats(event_seen_to_next_submit_start_lags),
            "event_seen_to_next_submit_finish_ms": _stats(event_seen_to_next_submit_finish_lags),
            "next_submit_provider_attempt_elapsed_ms": _stats(next_submit_elapsed),
            "profile_refill_daemon_elapsed_ms": _stats(profile_refill_daemon_elapsed_values),
        },
        "local_completion_to_next_submit_start_ms": _stats(local_completion_to_next_submit_lags),
        "local_to_next_submit_start_ms": _stats(local_completion_to_next_submit_lags),
        "next_submit_provider_attempt_elapsed_ms": _stats(next_submit_elapsed),
        "next_submit_attempt_elapsed_ms": _stats(next_submit_elapsed),
        "profile_refill_daemon_elapsed_ms": _stats(profile_refill_daemon_elapsed_values),
        "next_submit_opportunity": _aggregate_next_submit_opportunity_reports(next_submit_opportunities),
        "max_true_active_provider_slot_worker_count": max_true_provider_slots,
        "max_remote_actor_worker_count": max_remote_actor_workers,
        "max_pre_submit_provider_worker_count": max_pre_submit_workers,
        "max_coalescing_wait_worker_count": max_coalescing_wait_workers,
        "remote_wait_age_ms": _stats(remote_wait_age_values),
        "provider_lease_age_ms": _stats(provider_lease_age_values),
        "provider_slot_to_remote_wait_started_ms": _stats(provider_slot_to_remote_wait_started_values),
        "coalescing_wait_age_ms": _stats(coalescing_wait_age_values),
        "coalescing_until_ready_ms": _stats(coalescing_until_ready_values),
        "materialize_call_count": materialize_call_count,
        "same_worker_reconcile_repeat_count": duplicate_reconcile_count,
        "repeated_materialize_signature_count": repeated_materialize_signature_count,
        "marker_backfill_count": marker_backfill_count,
        "marker_backfill_repeat_count": marker_backfill_repeat_count,
        "materialize_started_repeat_count": materialize_started_repeat_count,
        "materialize_deferred_count": materialize_deferred_count,
        "lease_skipped_count": lease_skipped_count,
        "writer_lock_wait_ms": _stats(writer_lock_wait_values),
        "writer_lock_held_ms": _stats(writer_lock_held_values),
        "writer_lock_event_count": writer_lock_event_count,
        "profile_batch_envelopes": {
            "envelope_count": profile_batch_envelope_count,
            "batch_size": _stats(profile_batch_size_values),
            "provider_worker_batch_size": _stats(profile_worker_batch_size_values),
            "tiny_batch_count": profile_tiny_batch_count,
            "unexplained_tiny_batch_count": profile_unexplained_tiny_batch_count,
            "provider_slot_underuse_with_backlog_count": provider_slot_underuse_with_backlog_count,
            "tiny_batch_coalesced_count": profile_tiny_batch_coalesced_count,
        },
        "profile_prefetch_queue": {
            "snapshot_count": profile_queue_snapshot_count,
            "requested_url_count": profile_queue_requested_url_count,
            "cached_url_count": profile_queue_cached_url_count,
            "ready_url_count": profile_queue_ready_url_count,
            "queued_url_count": profile_queue_queued_url_count,
            "deferred_url_count": profile_queue_deferred_url_count,
            "failed_url_count": profile_queue_failed_url_count,
            "pending_url_count": profile_queue_pending_url_count,
            "non_quiescent_snapshot_count": profile_queue_non_quiescent_count,
            "planned_dispatch_owner_missing_count": profile_queue_planned_dispatch_owner_missing_count,
            "planned_dispatch_remote_owner_count": profile_queue_planned_dispatch_remote_owner_count,
            "terminal_queue_state_leak_count": profile_queue_terminal_state_leak_count,
            "oldest_pending_item_age_ms": _stats(profile_queue_oldest_pending_age_values),
            "refill_queue_state_counts": dict(profile_queue_refill_state_counts),
        },
        "profile_prefetch_batch_plan": {
            "plan_count": profile_batch_plan_count,
            "queue_item_count": profile_batch_plan_queue_item_count,
            "planned_dispatch_item_count": profile_batch_plan_dispatch_item_count,
            "planned_deferred_item_count": profile_batch_plan_deferred_item_count,
            "available_slot_count": profile_batch_plan_available_slot_count,
            "planned_new_worker_count": profile_batch_plan_new_worker_count,
            "unfilled_available_slot_count": profile_batch_plan_unfilled_slot_count,
            "underfilled_with_deferred_items_count": profile_batch_plan_underfilled_with_deferred_count,
        },
        "profile_prefetch_refill": {
            "trigger_count": profile_refill_trigger_count,
            "dispatched_url_count": profile_refill_dispatched_url_count,
            "available_slot_count": profile_refill_available_slot_count,
            "planned_new_worker_count": profile_refill_new_worker_count,
            "planned_dispatch_item_count": profile_refill_dispatch_item_count,
            "planned_deferred_item_count": profile_refill_deferred_item_count,
            "unfilled_available_slot_count": profile_refill_unfilled_slot_count,
            "underfilled_with_deferred_items_count": profile_refill_underfilled_with_deferred_count,
        },
        "profile_scheduler_contract": {
            "report_count": profile_scheduler_report_count,
            "report_available": profile_scheduler_report_count > 0,
            "violation_case_count": profile_scheduler_violation_case_count,
            "violation_count": profile_scheduler_violation_count,
            "violation_detected": profile_scheduler_violation_case_count > 0 or profile_scheduler_violation_count > 0,
            "same_wave_ordinal_violation_count": profile_scheduler_same_wave_ordinal_violation_count,
            "url_overlap_violation_count": profile_scheduler_url_overlap_violation_count,
            "batch_size_contract_violation_count": profile_scheduler_batch_size_contract_violation_count,
            "small_normal_batch_without_reason_count": profile_scheduler_small_normal_batch_without_reason_count,
            "retry_wave_isolation_violation_count": profile_scheduler_retry_wave_isolation_violation_count,
            "slot_refill_violation_count": profile_scheduler_slot_refill_violation_count,
            "advisory_lock_missing_count": profile_scheduler_advisory_lock_missing_count,
            "planned_dispatch_owner_missing_count": profile_scheduler_planned_dispatch_owner_missing_count,
            "terminal_queue_state_leak_count": profile_scheduler_terminal_queue_state_leak_count,
        },
        "provider_io": {
            "report_count": provider_io_report_count,
            "report_available": provider_io_report_count > 0,
            "actor_run_duration_ms": _stats(provider_io_actor_run_duration_values),
            "dataset_download_duration_ms": _stats(provider_io_dataset_download_duration_values),
        },
        "materialization_io": {
            "report_count": materialization_io_report_count,
            "report_available": materialization_io_report_count > 0,
            "sync_total_ms": _stats(materialization_io_sync_total_values),
            "candidate_delta_control_plane_replace_ms": _stats(materialization_io_delta_replace_values),
            "candidate_artifact_build_ms": _stats(materialization_io_artifact_build_values),
            "candidate_artifact_state_upsert_ms": _stats(materialization_io_state_upsert_values),
            "materialization_writer_wait_ms": _stats(materialization_io_writer_wait_values),
        },
        "violation_case_count": violation_case_count,
        "violation_detected": violation_case_count > 0,
        "diagnostic_violation_case_count": diagnostic_violation_case_count,
        "diagnostic_violation_detected": diagnostic_violation_case_count > 0,
    }


def event_level_efficiency_runtime_subset(report: dict[str, Any]) -> dict[str, Any]:
    payload = dict(report or {})
    if "report_count" in payload and "remote_provider_event_count" not in payload:
        return {
            "report_count": _safe_int(payload.get("report_count")),
            "remote_to_local_event_lag_ms": dict(payload.get("remote_to_local_event_lag_ms") or {}),
            "remote_to_local_marker_lag_ms": dict(payload.get("remote_to_local_marker_lag_ms") or {}),
            "remote_to_next_submit_start_ms": dict(payload.get("remote_to_next_submit_start_ms") or {}),
            "remote_to_next_submit_finish_ms": dict(payload.get("remote_to_next_submit_finish_ms") or {}),
            "remote_to_next_submit_segments_ms": dict(payload.get("remote_to_next_submit_segments_ms") or {}),
            "local_completion_to_next_submit_start_ms": dict(
                payload.get("local_completion_to_next_submit_start_ms")
                or payload.get("local_to_next_submit_start_ms")
                or {}
            ),
            "local_to_next_submit_start_ms": dict(payload.get("local_to_next_submit_start_ms") or {}),
            "next_submit_provider_attempt_elapsed_ms": dict(
                payload.get("next_submit_provider_attempt_elapsed_ms")
                or payload.get("next_submit_attempt_elapsed_ms")
                or {}
            ),
            "next_submit_attempt_elapsed_ms": dict(payload.get("next_submit_attempt_elapsed_ms") or {}),
            "profile_refill_daemon_elapsed_ms": dict(payload.get("profile_refill_daemon_elapsed_ms") or {}),
            "next_submit_opportunity": dict(payload.get("next_submit_opportunity") or {}),
            "max_true_active_provider_slot_worker_count": _safe_int(
                payload.get("max_true_active_provider_slot_worker_count")
            ),
            "max_remote_actor_worker_count": _safe_int(payload.get("max_remote_actor_worker_count")),
            "max_pre_submit_provider_worker_count": _safe_int(payload.get("max_pre_submit_provider_worker_count")),
            "max_coalescing_wait_worker_count": _safe_int(payload.get("max_coalescing_wait_worker_count")),
            "remote_wait_age_ms": dict(payload.get("remote_wait_age_ms") or {}),
            "provider_lease_age_ms": dict(payload.get("provider_lease_age_ms") or {}),
            "provider_slot_to_remote_wait_started_ms": dict(
                payload.get("provider_slot_to_remote_wait_started_ms") or {}
            ),
            "coalescing_wait_age_ms": dict(payload.get("coalescing_wait_age_ms") or {}),
            "coalescing_until_ready_ms": dict(payload.get("coalescing_until_ready_ms") or {}),
            "materialize_call_count": _safe_int(payload.get("materialize_call_count")),
            "same_worker_reconcile_repeat_count": _safe_int(payload.get("same_worker_reconcile_repeat_count")),
            "repeated_materialize_signature_count": _safe_int(payload.get("repeated_materialize_signature_count")),
            "marker_backfill_count": _safe_int(payload.get("marker_backfill_count")),
            "marker_backfill_repeat_count": _safe_int(payload.get("marker_backfill_repeat_count")),
            "materialize_started_repeat_count": _safe_int(payload.get("materialize_started_repeat_count")),
            "materialize_deferred_count": _safe_int(payload.get("materialize_deferred_count")),
            "lease_skipped_count": _safe_int(payload.get("lease_skipped_count")),
            "writer_lock_wait_ms": dict(payload.get("writer_lock_wait_ms") or {}),
            "writer_lock_held_ms": dict(payload.get("writer_lock_held_ms") or {}),
            "writer_lock_event_count": _safe_int(payload.get("writer_lock_event_count")),
            "profile_batch_envelopes": dict(payload.get("profile_batch_envelopes") or {}),
            "profile_prefetch_queue": dict(payload.get("profile_prefetch_queue") or {}),
            "profile_prefetch_batch_plan": dict(payload.get("profile_prefetch_batch_plan") or {}),
            "profile_prefetch_refill": dict(payload.get("profile_prefetch_refill") or {}),
            "profile_scheduler_contract": dict(payload.get("profile_scheduler_contract") or {}),
            "provider_io": dict(payload.get("provider_io") or {}),
            "materialization_io": dict(payload.get("materialization_io") or {}),
            "violation_case_count": _safe_int(payload.get("violation_case_count")),
            "violation_detected": bool(payload.get("violation_detected")),
            "diagnostic_violation_case_count": _safe_int(payload.get("diagnostic_violation_case_count")),
            "diagnostic_violation_detected": bool(payload.get("diagnostic_violation_detected")),
        }
    provider_slots = dict(payload.get("provider_slots") or {})
    reconcile = dict(payload.get("reconcile") or {})
    writer_lock = dict(payload.get("writer_lock") or {})
    return {
        "report_available": bool(payload.get("report_available")),
        "remote_provider_event_count": _safe_int(payload.get("remote_provider_event_count")),
        "harvest_completion_event_count": _safe_int(payload.get("harvest_completion_event_count")),
        "remote_to_local_event_lag_ms": dict(payload.get("remote_to_local_event_lag_ms") or {}),
        "remote_to_local_marker_lag_ms": dict(payload.get("remote_to_local_marker_lag_ms") or {}),
        "remote_to_next_submit_start_ms": dict(payload.get("remote_to_next_submit_start_ms") or {}),
        "remote_to_next_submit_finish_ms": dict(payload.get("remote_to_next_submit_finish_ms") or {}),
        "remote_to_next_submit_segments_ms": dict(payload.get("remote_to_next_submit_segments_ms") or {}),
        "local_completion_to_next_submit_start_ms": dict(
            payload.get("local_completion_to_next_submit_start_ms")
            or payload.get("local_to_next_submit_start_ms")
            or {}
        ),
        "local_to_next_submit_start_ms": dict(payload.get("local_to_next_submit_start_ms") or {}),
        "next_submit_provider_attempt_elapsed_ms": dict(
            payload.get("next_submit_provider_attempt_elapsed_ms")
            or payload.get("next_submit_attempt_elapsed_ms")
            or {}
        ),
        "next_submit_attempt_elapsed_ms": dict(payload.get("next_submit_attempt_elapsed_ms") or {}),
        "profile_refill_daemon_elapsed_ms": dict(payload.get("profile_refill_daemon_elapsed_ms") or {}),
        "next_submit_opportunity": dict(payload.get("next_submit_opportunity") or {}),
        "true_active_provider_slot_worker_count": _safe_int(
            provider_slots.get("true_active_provider_slot_worker_count")
        ),
        "remote_actor_worker_count": _safe_int(provider_slots.get("remote_actor_worker_count")),
        "pre_submit_provider_worker_count": _safe_int(provider_slots.get("pre_submit_provider_worker_count")),
        "remote_wait_age_ms": dict(provider_slots.get("remote_wait_age_ms") or {}),
        "provider_lease_age_ms": dict(provider_slots.get("provider_lease_age_ms") or {}),
        "provider_slot_to_remote_wait_started_ms": dict(
            provider_slots.get("provider_slot_to_remote_wait_started_ms") or {}
        ),
        "coalescing_wait_worker_count": _safe_int(provider_slots.get("coalescing_wait_worker_count")),
        "coalescing_wait_age_ms": dict(provider_slots.get("coalescing_wait_age_ms") or {}),
        "coalescing_until_ready_ms": dict(provider_slots.get("coalescing_until_ready_ms") or {}),
        "materialize_call_count": _safe_int(reconcile.get("materialize_call_count")),
        "same_worker_reconcile_repeat_count": _safe_int(reconcile.get("same_worker_reconcile_repeat_count")),
        "repeated_materialize_signature_count": _safe_int(
            reconcile.get("repeated_materialize_signature_count")
        ),
        "marker_backfill_count": _safe_int(reconcile.get("marker_backfill_count")),
        "marker_backfill_repeat_count": _safe_int(reconcile.get("marker_backfill_repeat_count")),
        "materialize_started_repeat_count": _safe_int(reconcile.get("materialize_started_repeat_count")),
        "materialize_deferred_count": _safe_int(reconcile.get("materialize_deferred_count")),
        "lease_skipped_count": _safe_int(reconcile.get("lease_skipped_count")),
        "writer_lock_wait_ms": dict(writer_lock.get("writer_lock_wait_ms") or {}),
        "writer_lock_held_ms": dict(writer_lock.get("writer_lock_held_ms") or {}),
        "writer_lock_event_count": _safe_int(writer_lock.get("writer_lock_event_count")),
        "profile_batch_envelopes": dict(payload.get("profile_batch_envelopes") or {}),
        "profile_prefetch_queue": dict(payload.get("profile_prefetch_queue") or {}),
        "profile_prefetch_batch_plan": dict(payload.get("profile_prefetch_batch_plan") or {}),
        "profile_prefetch_refill": dict(payload.get("profile_prefetch_refill") or {}),
        "profile_scheduler_contract": dict(payload.get("profile_scheduler_contract") or {}),
        "provider_io": dict(payload.get("provider_io") or {}),
        "materialization_io": dict(payload.get("materialization_io") or {}),
        "violation_count": _safe_int(payload.get("violation_count")),
        "violation_detected": bool(payload.get("violation_detected")),
        "diagnostic_violation_count": _safe_int(payload.get("diagnostic_violation_count")),
        "diagnostic_violation_detected": bool(payload.get("diagnostic_violation_detected")),
    }


def _aggregate_next_submit_opportunity_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [dict(item) for item in list(reports or []) if isinstance(item, dict)]
    if not normalized:
        return {}
    applicable_reports = [item for item in normalized if item.get("applicable") is True]
    metrics_required_reports = [item for item in normalized if item.get("metrics_required") is True]
    requested_url_count = max(_safe_int(item.get("requested_url_count")) for item in normalized)
    dispatched_url_count = sum(_safe_int(item.get("dispatched_url_count")) for item in normalized)
    dispatched_before_first_completion_count = sum(
        _safe_int(item.get("dispatched_before_first_completion_count")) for item in normalized
    )
    if applicable_reports:
        return {
            "applicable": True,
            "reason": "one_or_more_reports_require_next_submit_metrics",
            "metrics_required": bool(metrics_required_reports),
            "report_count": len(normalized),
            "applicable_report_count": len(applicable_reports),
            "metrics_required_report_count": len(metrics_required_reports),
            "requested_url_count": requested_url_count,
            "dispatched_url_count": dispatched_url_count,
            "dispatched_before_first_completion_count": dispatched_before_first_completion_count,
            "reasons": sorted({str(item.get("reason") or "").strip() for item in applicable_reports if str(item.get("reason") or "").strip()}),
        }
    return {
        "applicable": False,
        "reason": "all_reports_mark_next_submit_not_applicable",
        "metrics_required": False,
        "report_count": len(normalized),
        "applicable_report_count": 0,
        "metrics_required_report_count": len(metrics_required_reports),
        "requested_url_count": requested_url_count,
        "dispatched_url_count": dispatched_url_count,
        "dispatched_before_first_completion_count": dispatched_before_first_completion_count,
        "reasons": sorted({str(item.get("reason") or "").strip() for item in normalized if str(item.get("reason") or "").strip()}),
    }


def _extract_writer_lock_wait_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate per-job inline writer-lock wait/hold timings.

    Diagnostic only. Event-level workflows record ``writer_lock`` on the
    inline reconcile event payload so dashboards can detect cases where a
    remote provider completion was blocked behind a peer's writer lock
    before the next-submit opportunity could fire.
    """

    wait_ms: list[float] = []
    held_ms: list[float] = []
    by_scope: dict[str, list[float]] = {}
    for event in events:
        payload = dict(event.get("payload") or {})
        writer_lock = dict(payload.get("writer_lock") or {})
        if not writer_lock:
            continue
        wait_value = _safe_float(writer_lock.get("writer_lock_wait_ms"))
        held_value = _safe_float(writer_lock.get("writer_lock_held_ms"))
        scope = str(writer_lock.get("scope") or "").strip()
        if wait_value >= 0.0:
            wait_ms.append(wait_value)
            if scope:
                by_scope.setdefault(scope, []).append(wait_value)
        if held_value >= 0.0:
            held_ms.append(held_value)
    return {
        "writer_lock_wait_ms": _stats(wait_ms),
        "writer_lock_held_ms": _stats(held_ms),
        "writer_lock_event_count": len(wait_ms),
        "writer_lock_wait_ms_by_scope": {
            key: _stats(value) for key, value in sorted(by_scope.items())
        },
    }


def _extract_materialization_io_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    sync_total_values: list[float] = []
    delta_replace_values: list[float] = []
    artifact_build_values: list[float] = []
    artifact_state_upsert_values: list[float] = []
    writer_wait_values: list[float] = []
    event_count = 0
    for event in events:
        payload = dict(event.get("payload") or {})
        if (
            str(payload.get("event_family") or "").strip()
            not in _STRUCTURED_MATERIALIZATION_EVENT_FAMILIES
        ):
            continue
        sync_result = dict(payload.get("sync_result") or {})
        sync_payloads = [sync_result]
        delta_sync = dict(sync_result.get("delta_control_plane_sync") or {})
        if delta_sync:
            sync_payloads.append(delta_sync)
        event_had_timing = False
        for sync_payload in sync_payloads:
            timings = dict(sync_payload.get("timings_ms") or {})
            if not timings:
                continue
            event_had_timing = True
            _append_positive(sync_total_values, timings.get("sync_total"))
            _append_positive(delta_replace_values, timings.get("candidate_delta_control_plane_replace"))
            _append_positive(
                artifact_build_values,
                timings.get("candidate_artifact_build")
                or timings.get("view_write_total")
                or timings.get("payload_build_total"),
            )
            _append_positive(artifact_state_upsert_values, timings.get("state_upsert"))
            _append_positive(writer_wait_values, timings.get("materialization_writer_wait"))
        writer_slot = dict(sync_result.get("writer_slot") or sync_result.get("materialization_writer_slot") or {})
        if writer_slot:
            event_had_timing = True
            _append_positive(writer_wait_values, writer_slot.get("wait_ms"))
        if event_had_timing:
            event_count += 1
    return {
        "report_available": event_count > 0,
        "event_count": event_count,
        "sync_total_ms": _stats(sync_total_values),
        "candidate_delta_control_plane_replace_ms": _stats(delta_replace_values),
        "candidate_artifact_build_ms": _stats(artifact_build_values),
        "candidate_artifact_state_upsert_ms": _stats(artifact_state_upsert_values),
        "materialization_writer_wait_ms": _stats(writer_wait_values),
    }


def _extract_provider_io_metrics(
    *,
    workers: list[dict[str, Any]],
    actor_run_duration_values: list[float],
    actor_run_duration_by_key: dict[str, float] | None = None,
) -> dict[str, Any]:
    actor_values = list(actor_run_duration_values or [])
    actor_values_by_key = dict(actor_run_duration_by_key or {})
    dataset_download_values: list[float] = []
    for worker in workers:
        checkpoint = dict(worker.get("checkpoint") or {})
        provider_timings = dict(checkpoint.get("provider_timings") or {})
        worker_actor_duration = _safe_float(provider_timings.get("actor_run_duration_ms"))
        if worker_actor_duration >= 0.0:
            actor_key = _provider_run_key_from_worker(worker)
            if actor_key:
                actor_values_by_key.setdefault(actor_key, worker_actor_duration)
            else:
                actor_values.append(worker_actor_duration)
        _append_positive(dataset_download_values, provider_timings.get("dataset_download_duration_ms"))
    actor_values.extend(actor_values_by_key.values())
    return {
        "report_available": bool(actor_values or dataset_download_values),
        "actor_run_duration_ms": _stats(actor_values),
        "dataset_download_duration_ms": _stats(dataset_download_values),
    }


def _provider_run_key_from_event_payload(payload: dict[str, Any]) -> str:
    event = dict(payload.get("event") or {})
    run_id = str(event.get("run_id") or "").strip()
    dataset_id = str(event.get("dataset_id") or "").strip()
    if run_id:
        return f"run:{run_id}"
    if dataset_id:
        return f"dataset:{dataset_id}"
    worker_ids = _extract_worker_ids(payload)
    if len(worker_ids) == 1:
        return f"worker:{worker_ids[0]}"
    return ""


def _normalized_remote_event_timestamp_key(value: str) -> str:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return str(value or "").strip()
    return f"remote_completed:{parsed.isoformat(timespec='microseconds')}"


def _remote_event_record_marker_rank(record: dict[str, Any]) -> tuple[int, datetime, float, int]:
    local_seen = _parse_timestamp(str(record.get("local_event_seen_at") or "").strip())
    remote_completed = _parse_timestamp(str(record.get("remote_completed_at") or "").strip())
    marker_at = local_seen or remote_completed or datetime.max.replace(tzinfo=timezone.utc)
    lag = _safe_float(record.get("remote_to_local_event_lag_ms"))
    if lag < 0.0:
        lag = _duration_ms(
            str(record.get("remote_completed_at") or "").strip(),
            str(record.get("local_event_seen_at") or "").strip(),
        )
    terminal_missing = 1 if remote_completed is None else 0
    return (
        terminal_missing,
        marker_at,
        lag if lag is not None and lag >= 0.0 else float("inf"),
        _safe_int(record.get("event_id")),
    )


def _provider_run_key_from_worker(worker: dict[str, Any]) -> str:
    checkpoint = dict(worker.get("checkpoint") or {})
    run_id = str(checkpoint.get("run_id") or "").strip()
    dataset_id = str(checkpoint.get("dataset_id") or checkpoint.get("default_dataset_id") or "").strip()
    if run_id:
        return f"run:{run_id}"
    if dataset_id:
        return f"dataset:{dataset_id}"
    worker_id = _safe_int(worker.get("worker_id"))
    if worker_id > 0:
        return f"worker:{worker_id}"
    return ""


def _append_positive(values: list[float], value: Any) -> None:
    numeric = _safe_float(value)
    if numeric >= 0.0:
        values.append(numeric)


def _extract_worker_efficiency_metrics(
    workers: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    true_active_provider_slot_worker_count = 0
    remote_actor_worker_count = 0
    pre_submit_provider_worker_count = 0
    waiting_remote_harvest_worker_count = 0
    coalescing_wait_worker_count = 0
    provider_limiter_keys: Counter[str] = Counter()
    provider_limiter_budgets: dict[str, int] = {}
    provider_limiter_reported_active: dict[str, int] = {}
    inline_marker_count = 0
    inline_marker_worker_kinds: Counter[str] = Counter()
    # Provider-slot idle window: for each worker that is currently in `waiting_remote_harvest`,
    # how long has it been since we handed it off to the provider? Surfaces the case where a
    # batch of workers has been remote-waiting for tens of minutes with no progress (the
    # documented "actor slot idle window" failure mode that previously could only be spotted
    # via Apify's UI).
    reference_now = now or datetime.now(timezone.utc)
    remote_wait_age_ms_values: list[float] = []
    lease_age_ms_values: list[float] = []
    provider_slot_to_remote_wait_started_ms_values: list[float] = []
    coalescing_wait_age_ms_values: list[float] = []
    coalescing_until_ready_ms_values: list[float] = []
    for worker in workers:
        checkpoint = dict(worker.get("checkpoint") or {})
        metadata = dict(worker.get("metadata") or {})
        output = dict(worker.get("output") or {})
        inline_marker = dict(output.get("inline_incremental_ingest") or {})
        if str(inline_marker.get("applied_at") or "").strip():
            inline_marker_count += 1
            worker_kind = str(inline_marker.get("worker_kind") or "").strip()
            if worker_kind:
                inline_marker_worker_kinds[worker_kind] += 1
        recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()
        if recovery_kind != _HARVEST_PROFILE_RECOVERY_KIND:
            continue
        status = str(worker.get("status") or "").strip().lower()
        checkpoint_stage = str(checkpoint.get("stage") or "").strip().lower()
        if checkpoint_stage in _HARVEST_PROFILE_REMOTE_STAGES or status in _HARVEST_PROFILE_REMOTE_STAGES:
            waiting_remote_harvest_worker_count += 1
            remote_wait_started_at = str(checkpoint.get("remote_wait_started_at") or "").strip()
            remote_wait_age = _age_ms(remote_wait_started_at, reference_now)
            if remote_wait_age is not None:
                remote_wait_age_ms_values.append(remote_wait_age)
        provider_limiter = dict(checkpoint.get("provider_limiter_lease") or {})
        limiter_key = str(provider_limiter.get("limiter_key") or "").strip()
        lease_token = str(provider_limiter.get("lease_token") or "").strip()
        lease_acquired_at = str(provider_limiter.get("created_at") or "").strip()
        remote_wait_started_at = str(checkpoint.get("remote_wait_started_at") or "").strip()
        has_true_provider_slot = bool(lease_token and limiter_key)
        # Completed workers no longer occupy provider capacity, but they still
        # define the submit hot-path latency from slot acquisition to remote wait.
        if has_true_provider_slot:
            slot_to_remote_wait = _duration_ms(lease_acquired_at, remote_wait_started_at)
            if slot_to_remote_wait is not None:
                provider_slot_to_remote_wait_started_ms_values.append(slot_to_remote_wait)
        if status in _TERMINAL_WORKER_STATUSES:
            continue
        if status not in _ACTIVE_WORKER_STATUSES and checkpoint_stage not in _HARVEST_PROFILE_REMOTE_STAGES:
            continue
        if checkpoint_stage in _HARVEST_PROFILE_COALESCING_STAGES:
            coalescing_wait_worker_count += 1
            wait_age = _age_ms(str(worker.get("updated_at") or ""), reference_now)
            if wait_age is not None:
                coalescing_wait_age_ms_values.append(wait_age)
            until_ready = _future_wait_ms(str(checkpoint.get("not_before_at") or ""), reference_now)
            if until_ready is not None:
                coalescing_until_ready_ms_values.append(until_ready)
            continue
        run_id = str(
            checkpoint.get("run_id")
            or checkpoint.get("actor_run_id")
            or dict(output.get("summary") or {}).get("run_id")
            or ""
        ).strip()
        dataset_id = str(
            checkpoint.get("dataset_id")
            or checkpoint.get("default_dataset_id")
            or dict(output.get("summary") or {}).get("dataset_id")
            or ""
        ).strip()
        if has_true_provider_slot:
            true_active_provider_slot_worker_count += 1
            if run_id or dataset_id or checkpoint_stage in _HARVEST_PROFILE_REMOTE_STAGES:
                remote_actor_worker_count += 1
            provider_limiter_keys[limiter_key] += 1
            provider_limiter_budgets[limiter_key] = max(
                int(provider_limiter_budgets.get(limiter_key) or 0),
                _safe_int(provider_limiter.get("budget")),
            )
            provider_limiter_reported_active[limiter_key] = max(
                int(provider_limiter_reported_active.get(limiter_key) or 0),
                _safe_int(provider_limiter.get("active_count")),
            )
            lease_age = _age_ms(lease_acquired_at, reference_now)
            if lease_age is not None:
                lease_age_ms_values.append(lease_age)
        elif status in _ACTIVE_WORKER_STATUSES or checkpoint_stage in _HARVEST_PROFILE_REMOTE_STAGES:
            pre_submit_provider_worker_count += 1
    max_budget = max(provider_limiter_budgets.values()) if provider_limiter_budgets else 0
    occupancy_ratio = (
        round(true_active_provider_slot_worker_count / max_budget, 4)
        if max_budget > 0
        else 0.0
    )
    return {
        "true_active_provider_slot_worker_count": true_active_provider_slot_worker_count,
        "remote_actor_worker_count": remote_actor_worker_count,
        "pre_submit_provider_worker_count": pre_submit_provider_worker_count,
        "waiting_remote_harvest_worker_count": waiting_remote_harvest_worker_count,
        "coalescing_wait_worker_count": coalescing_wait_worker_count,
        "inline_incremental_marker_count": inline_marker_count,
        "inline_incremental_marker_worker_kinds": {
            key: int(value) for key, value in sorted(inline_marker_worker_kinds.items())
        },
        "provider_limiter_keys": {key: int(value) for key, value in sorted(provider_limiter_keys.items())},
        "provider_limiter_budgets": provider_limiter_budgets,
        "provider_limiter_reported_active": provider_limiter_reported_active,
        "max_provider_limiter_budget": max_budget,
        "true_provider_slot_occupancy_ratio": occupancy_ratio,
        "remote_wait_age_ms": _stats(remote_wait_age_ms_values),
        "provider_lease_age_ms": _stats(lease_age_ms_values),
        "provider_slot_to_remote_wait_started_ms": _stats(provider_slot_to_remote_wait_started_ms_values),
        "coalescing_wait_age_ms": _stats(coalescing_wait_age_ms_values),
        "coalescing_until_ready_ms": _stats(coalescing_until_ready_ms_values),
    }


def _build_next_submit_opportunity_report(
    *,
    harvest_completion_anchor_times: list[datetime],
    profile_dispatch_events: list[dict[str, Any]],
    profile_worker_dispatch_events: list[dict[str, Any]] | None = None,
    profile_handoff_dispatch_events: list[dict[str, Any]] | None = None,
    requested_url_count: int,
    remote_to_next_submit_sample_count: int,
    local_to_next_submit_sample_count: int,
    next_submit_attempt_sample_count: int,
) -> dict[str, Any]:
    sample_count = (
        int(remote_to_next_submit_sample_count)
        + int(local_to_next_submit_sample_count)
    )
    if not harvest_completion_anchor_times:
        return {
            "applicable": False,
            "reason": "no_harvest_completion_event",
            "metrics_required": False,
            "sample_count": 0,
            "requested_url_count": int(requested_url_count),
        }
    first_completion_at = min(harvest_completion_anchor_times)
    dispatch_count = 0
    dispatched_url_count = 0
    dispatched_before_first_completion_count = 0
    post_completion_non_handoff_dispatch_count = 0
    for event in profile_dispatch_events:
        dispatch_started_at = _parse_timestamp(str(event.get("started_at") or "").strip())
        if dispatch_started_at is None:
            continue
        dispatched = _safe_int(event.get("dispatched_url_count"))
        if dispatched <= 0 and _safe_int(event.get("queued_worker_count")) > 0:
            dispatched = 1
        if dispatched <= 0:
            continue
        dispatch_count += 1
        dispatched_url_count += dispatched
        if dispatch_started_at <= first_completion_at:
            dispatched_before_first_completion_count += dispatched
        elif not bool(event.get("handoff_submit")):
            post_completion_non_handoff_dispatch_count += 1
    worker_dispatch_count = 0
    worker_dispatched_url_count = 0
    worker_dispatched_before_first_completion_count = 0
    for event in list(profile_worker_dispatch_events or []):
        dispatch_started_at = _parse_timestamp(str(event.get("started_at") or "").strip())
        if dispatch_started_at is None:
            continue
        dispatched = _safe_int(event.get("dispatched_url_count"))
        if dispatched <= 0:
            continue
        worker_dispatch_count += 1
        worker_dispatched_url_count += dispatched
        if dispatch_started_at <= first_completion_at:
            worker_dispatched_before_first_completion_count += dispatched
    effective_dispatch_count = max(dispatch_count, worker_dispatch_count)
    effective_dispatched_url_count = max(dispatched_url_count, worker_dispatched_url_count)
    effective_dispatched_before_first_completion_count = max(
        dispatched_before_first_completion_count,
        worker_dispatched_before_first_completion_count,
    )
    worker_dispatch_diagnostics = (
        {
            "event_dispatch_count": dispatch_count,
            "worker_dispatch_count": worker_dispatch_count,
            "event_dispatched_url_count": dispatched_url_count,
            "worker_dispatched_url_count": worker_dispatched_url_count,
            "event_dispatched_before_first_completion_count": dispatched_before_first_completion_count,
            "worker_dispatched_before_first_completion_count": worker_dispatched_before_first_completion_count,
        }
        if worker_dispatch_count > 0
        else {}
    )
    handoff_dispatch_count = 0
    for event in list(profile_handoff_dispatch_events or []):
        dispatch_started_at = _parse_timestamp(str(event.get("started_at") or "").strip())
        if dispatch_started_at is None or dispatch_started_at <= first_completion_at:
            continue
        dispatched = _safe_int(event.get("dispatched_url_count"))
        if dispatched <= 0 and _safe_int(event.get("queued_worker_count")) > 0:
            dispatched = 1
        if dispatched <= 0:
            continue
        handoff_dispatch_count += 1
    if requested_url_count > 0 and effective_dispatched_before_first_completion_count >= requested_url_count:
        return {
            "applicable": False,
            "reason": "all_profile_urls_submitted_before_first_completion",
            "metrics_required": False,
            "sample_count": 0,
            "dispatch_count": effective_dispatch_count,
            "requested_url_count": int(requested_url_count),
            "dispatched_url_count": effective_dispatched_url_count,
            "dispatched_before_first_completion_count": effective_dispatched_before_first_completion_count,
            **worker_dispatch_diagnostics,
        }
    if requested_url_count > 0 and effective_dispatched_url_count >= requested_url_count and post_completion_non_handoff_dispatch_count > 0:
        return {
            "applicable": False,
            "reason": "all_profile_urls_submitted_by_non_handoff_dispatch",
            "legacy_reason": "all_profile_urls_submitted_by_discovery_append",
            "metrics_required": False,
            "sample_count": 0,
            "dispatch_count": effective_dispatch_count,
            "post_completion_non_handoff_dispatch_count": post_completion_non_handoff_dispatch_count,
            "post_completion_discovery_dispatch_count": post_completion_non_handoff_dispatch_count,
            "requested_url_count": int(requested_url_count),
            "dispatched_url_count": effective_dispatched_url_count,
            "dispatched_before_first_completion_count": effective_dispatched_before_first_completion_count,
            **worker_dispatch_diagnostics,
        }
    if sample_count > 0:
        return {
            "applicable": True,
            "reason": "next_submit_samples_observed",
            "metrics_required": True,
            "sample_count": sample_count,
            "dispatch_count": effective_dispatch_count,
            "requested_url_count": int(requested_url_count),
            "dispatched_url_count": effective_dispatched_url_count,
            "dispatched_before_first_completion_count": effective_dispatched_before_first_completion_count,
            **worker_dispatch_diagnostics,
        }
    if handoff_dispatch_count > 0:
        return {
            "applicable": True,
            "reason": "post_completion_dispatch_observed_without_metric",
            "metrics_required": True,
            "sample_count": 0,
            "dispatch_count": effective_dispatch_count,
            "post_completion_dispatch_count": handoff_dispatch_count,
            "requested_url_count": int(requested_url_count),
            "dispatched_url_count": effective_dispatched_url_count,
            "dispatched_before_first_completion_count": effective_dispatched_before_first_completion_count,
            **worker_dispatch_diagnostics,
        }
    return {
        "applicable": True,
        "reason": "post_completion_backlog_unknown_without_next_submit_metric",
        "metrics_required": True,
        "sample_count": 0,
        "dispatch_count": effective_dispatch_count,
        "requested_url_count": int(requested_url_count),
        "dispatched_url_count": effective_dispatched_url_count,
        "dispatched_before_first_completion_count": effective_dispatched_before_first_completion_count,
        **worker_dispatch_diagnostics,
    }


def _age_ms(timestamp: str, reference_now: datetime) -> float | None:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    elapsed = (reference_now - parsed).total_seconds() * 1000
    if elapsed < 0:
        return None
    if elapsed > _MAX_REASONABLE_EVENT_LAG_MS:
        return None
    return round(elapsed, 2)


def _future_wait_ms(timestamp: str, reference_now: datetime) -> float | None:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    elapsed = (parsed - reference_now).total_seconds() * 1000
    if elapsed < 0:
        return 0.0
    if elapsed > _MAX_REASONABLE_EVENT_LAG_MS:
        return None
    return round(elapsed, 2)


def _extract_reconcile_and_materialize_metrics(
    *,
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    workers: list[dict[str, Any]],
) -> dict[str, Any]:
    structured_events = [
        event
        for event in events
        if (
            str(dict(event.get("payload") or {}).get("event_family") or "").strip()
            in _STRUCTURED_MATERIALIZATION_EVENT_FAMILIES
        )
    ]
    if structured_events:
        return _extract_structured_reconcile_and_materialize_metrics(
            job_summary=job_summary,
            events=structured_events,
            workers=workers,
        )
    return _extract_legacy_reconcile_and_materialize_metrics(
        job_summary=job_summary,
        events=events,
        workers=workers,
    )


def _extract_structured_reconcile_and_materialize_metrics(
    *,
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    workers: list[dict[str, Any]],
) -> dict[str, Any]:
    completed_reconcile_event_count = 0
    reconcile_worker_counts: Counter[int] = Counter()
    marker_backfill_worker_counts: Counter[int] = Counter()
    materialize_started_signatures: Counter[str] = Counter()
    materialize_started_worker_counts: Counter[int] = Counter()
    materialize_event_signatures: Counter[str] = Counter()
    materialize_event_count = 0
    marker_backfill_count = 0
    lease_acquired_count = 0
    lease_skipped_count = 0
    materialize_started_count = 0
    materialize_completed_count = 0
    materialize_deferred_count = 0
    for event in events:
        status = str(event.get("status") or "").strip().lower()
        payload = dict(event.get("payload") or {})
        phase = str(payload.get("phase") or "").strip().lower()
        worker_ids = _extract_worker_ids(payload)
        if phase == "lease_acquired":
            lease_acquired_count += 1
        elif phase == "lease_skipped":
            lease_skipped_count += 1
        if phase == "completed" and status == "completed":
            completed_reconcile_event_count += 1
            for worker_id in worker_ids:
                reconcile_worker_counts[worker_id] += 1
        if phase == "marker_backfilled":
            marker_backfill_count += (
                _safe_int(payload.get("marker_backfill_count"))
                or _safe_int(payload.get("marker_count"))
                or 1
            )
            for worker_id in worker_ids:
                marker_backfill_worker_counts[worker_id] += 1
        if phase == "materialize_started":
            materialize_started_count += 1
            for worker_id in worker_ids:
                materialize_started_worker_counts[worker_id] += 1
            signature = str(payload.get("materialize_signature") or "").strip()
            if not signature:
                signature = _materialize_signature(payload=payload, detail=phase)
            if signature:
                materialize_started_signatures[signature] += 1
        elif phase == "materialize_completed":
            materialize_completed_count += 1
        elif phase == "materialize_deferred":
            materialize_deferred_count += 1
        if bool(payload.get("materialize_call")):
            materialize_event_count += 1
            signature = str(payload.get("materialize_signature") or "").strip()
            if not signature:
                signature = _materialize_signature(payload=payload, detail=phase)
            if signature:
                materialize_event_signatures[signature] += 1
    inline_worker_counts: Counter[int] = Counter()
    for worker in workers:
        output = dict(worker.get("output") or {})
        marker = dict(output.get("inline_incremental_ingest") or {})
        if not marker:
            continue
        for worker_id in _extract_worker_ids(marker):
            inline_worker_counts[worker_id] += 1
    same_worker_reconcile_repeat_count = sum(max(0, count - 1) for count in reconcile_worker_counts.values())
    marker_backfill_repeat_count = sum(max(0, count - 1) for count in marker_backfill_worker_counts.values())
    materialize_started_signature_repeat_count = sum(
        max(0, count - 1) for count in materialize_started_signatures.values()
    )
    materialize_started_worker_repeat_count = sum(
        max(0, count - 1) for count in materialize_started_worker_counts.values()
    )
    materialize_started_repeat_count = max(
        materialize_started_signature_repeat_count,
        materialize_started_worker_repeat_count,
    )
    same_worker_inline_marker_repeat_count = sum(max(0, count - 1) for count in inline_worker_counts.values())
    repeated_materialize_signature_count = sum(
        max(0, count - 1) for count in materialize_event_signatures.values()
    )
    summary_materialize_result_count = _summary_materialize_result_count(job_summary)
    return {
        "completed_reconcile_event_count": completed_reconcile_event_count,
        "marker_backfill_count": marker_backfill_count,
        "marker_backfill_repeat_count": marker_backfill_repeat_count,
        "same_worker_reconcile_repeat_count": same_worker_reconcile_repeat_count,
        "same_worker_inline_marker_repeat_count": same_worker_inline_marker_repeat_count,
        "materialize_event_count": materialize_event_count,
        "materialize_started_count": materialize_started_count,
        "materialize_started_repeat_count": materialize_started_repeat_count,
        "materialize_started_signature_repeat_count": materialize_started_signature_repeat_count,
        "materialize_started_worker_repeat_count": materialize_started_worker_repeat_count,
        "materialize_completed_count": materialize_completed_count,
        "materialize_deferred_count": materialize_deferred_count,
        "summary_materialize_result_count": summary_materialize_result_count,
        "materialize_call_count": materialize_event_count + summary_materialize_result_count,
        "repeated_materialize_signature_count": repeated_materialize_signature_count,
        "lease_acquired_count": lease_acquired_count,
        "lease_skipped_count": lease_skipped_count,
        "structured_event_count": len(events),
        "legacy_fallback_used": False,
    }


def _extract_legacy_reconcile_and_materialize_metrics(
    *,
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    workers: list[dict[str, Any]],
) -> dict[str, Any]:
    completed_reconcile_event_count = 0
    reconcile_worker_counts: Counter[int] = Counter()
    marker_backfill_worker_counts: Counter[int] = Counter()
    materialize_event_signatures: Counter[str] = Counter()
    materialize_event_count = 0
    marker_backfill_count = 0
    for event in events:
        detail = str(event.get("detail") or "").strip().lower()
        status = str(event.get("status") or "").strip().lower()
        payload = dict(event.get("payload") or {})
        worker_ids = _extract_worker_ids(payload)
        is_reconcile_event = "reconcile" in detail
        if is_reconcile_event and status == "completed":
            completed_reconcile_event_count += 1
            for worker_id in worker_ids:
                reconcile_worker_counts[worker_id] += 1
        if "Backfilled inline worker consumption markers".lower() in detail:
            marker_backfill_count += _safe_int(payload.get("marker_count")) or 1
            for worker_id in worker_ids:
                marker_backfill_worker_counts[worker_id] += 1
        if _event_indicates_materialization(detail=detail, payload=payload):
            materialize_event_count += 1
            signature = _materialize_signature(payload=payload, detail=detail)
            if signature:
                materialize_event_signatures[signature] += 1
    inline_worker_counts: Counter[int] = Counter()
    for worker in workers:
        output = dict(worker.get("output") or {})
        marker = dict(output.get("inline_incremental_ingest") or {})
        if not marker:
            continue
        for worker_id in _extract_worker_ids(marker):
            inline_worker_counts[worker_id] += 1
    same_worker_reconcile_repeat_count = sum(max(0, count - 1) for count in reconcile_worker_counts.values())
    marker_backfill_repeat_count = sum(max(0, count - 1) for count in marker_backfill_worker_counts.values())
    same_worker_inline_marker_repeat_count = sum(max(0, count - 1) for count in inline_worker_counts.values())
    repeated_materialize_signature_count = sum(
        max(0, count - 1) for count in materialize_event_signatures.values()
    )
    summary_materialize_result_count = _summary_materialize_result_count(job_summary)
    return {
        "completed_reconcile_event_count": completed_reconcile_event_count,
        "marker_backfill_count": marker_backfill_count,
        "marker_backfill_repeat_count": marker_backfill_repeat_count,
        "same_worker_reconcile_repeat_count": same_worker_reconcile_repeat_count,
        "same_worker_inline_marker_repeat_count": same_worker_inline_marker_repeat_count,
        "materialize_event_count": materialize_event_count,
        "materialize_started_count": materialize_event_count,
        "materialize_started_repeat_count": repeated_materialize_signature_count,
        "materialize_started_signature_repeat_count": repeated_materialize_signature_count,
        "materialize_started_worker_repeat_count": 0,
        "materialize_completed_count": 0,
        "materialize_deferred_count": 0,
        "summary_materialize_result_count": summary_materialize_result_count,
        "materialize_call_count": materialize_event_count + summary_materialize_result_count,
        "repeated_materialize_signature_count": repeated_materialize_signature_count,
        "lease_acquired_count": 0,
        "lease_skipped_count": 0,
        "structured_event_count": 0,
        "legacy_fallback_used": True,
    }


def _summary_materialize_result_count(job_summary: dict[str, Any]) -> int:
    background_reconcile = dict(job_summary.get("background_reconcile") or {})
    count = 0
    for value in background_reconcile.values():
        if not isinstance(value, dict):
            continue
        payload = dict(value)
        sync_result = dict(payload.get("sync_result") or {})
        if not sync_result:
            sync_result = dict(dict(payload.get("resume_result") or {}).get("sync_result") or {})
        status = str(sync_result.get("status") or "").strip().lower()
        if status in {"completed", "deferred"}:
            count += 1
    return count


def _event_indicates_materialization(*, detail: str, payload: dict[str, Any]) -> bool:
    if "materializ" in detail or "candidate material" in detail:
        return True
    sync_result = dict(payload.get("sync_result") or {})
    reason = str(sync_result.get("reason") or payload.get("reason") or "").strip().lower()
    return bool("materializ" in reason or "candidate_document" in reason or "candidate_artifact" in reason)


def _materialize_signature(*, payload: dict[str, Any], detail: str) -> str:
    snapshot_id = str(payload.get("snapshot_id") or "").strip()
    worker_ids = ",".join(str(item) for item in sorted(_extract_worker_ids(payload)))
    sync_result = dict(payload.get("sync_result") or {})
    reason = str(sync_result.get("reason") or payload.get("reason") or detail).strip()
    if not snapshot_id and not worker_ids and not reason:
        return ""
    return f"{snapshot_id}|{worker_ids}|{reason}"


def _is_harvest_completion_event(payload: dict[str, Any], event_metrics: dict[str, Any]) -> bool:
    if str(payload.get("pipeline_order") or "").strip() in {
        "provider_completed_to_local_ingest_to_next_submit_before_materialization",
        "provider_completed_to_next_submit_before_local_apply",
    }:
        return True
    return "post_ingest_prefetch_elapsed_ms" in event_metrics


def _is_profile_prefetch_dispatch_event(payload: dict[str, Any]) -> bool:
    if str(payload.get("pipeline_order") or "").strip() in {
        "provider_completed_to_local_ingest_to_next_submit_before_materialization",
        "provider_completed_to_next_submit_before_local_apply",
    }:
        return False
    if "post_ingest_prefetch_elapsed_ms" in dict(payload.get("event_metrics") or {}):
        return False
    dispatch_payload = _profile_prefetch_dispatch_payload(payload)
    if _is_profile_prefetch_terminal_proof_only(dispatch_payload):
        return False
    kind = str(payload.get("kind") or "").strip()
    if kind in {"profile_prefetch_refill_daemon_group", "profile_prefetch_phase_b_group"}:
        return True
    if dispatch_payload and (
        _extract_profile_prefetch_batch_plans(dispatch_payload)
        or list(dispatch_payload.get("batch_envelopes") or [])
        or _safe_int(dispatch_payload.get("dispatched_url_count")) > 0
        or _safe_int(dispatch_payload.get("queued_worker_count")) > 0
    ):
        return True
    return False


def _is_profile_refill_submit_event(payload: dict[str, Any]) -> bool:
    kind = str(dict(payload or {}).get("kind") or "").strip()
    if kind in {"profile_prefetch_refill_daemon_group", "profile_prefetch_phase_b_group"}:
        return True
    pipeline_order = str(dict(payload or {}).get("pipeline_order") or "").strip()
    return pipeline_order in _PROFILE_PREFETCH_PHASE_B_PIPELINE_ORDERS


def _is_profile_completion_handoff_submit_event(payload: dict[str, Any]) -> bool:
    """True only for submit evidence owned by profile-completion slot refill.

    Discovery/roster/search-seed Phase B can submit real profile batches, but it
    is not evidence that a completed profile actor released a slot and refilled
    it. Keeping this distinction prevents the hard completion handoff SLO from
    pairing a profile completion with an unrelated later discovery append.
    """

    payload = dict(payload or {})
    kind = str(payload.get("kind") or "").strip()
    if kind == "profile_prefetch_refill_daemon_group":
        refill_phase = str(payload.get("profile_prefetch_refill_phase") or "").strip()
        if refill_phase and refill_phase not in _PROFILE_COMPLETION_HANDOFF_REFILL_PHASES:
            return False
        dispatch_payload = _profile_prefetch_dispatch_payload(payload)
        for plan in _extract_profile_prefetch_batch_plans(dispatch_payload) or [
            dict(payload.get("batch_plan") or {})
        ]:
            plan_reason = str(plan.get("plan_reason") or "").strip()
            if (
                plan_reason in _PROFILE_COMPLETION_HANDOFF_EXCLUDED_PLAN_REASONS
                and refill_phase not in _PROFILE_COMPLETION_HANDOFF_TAIL_PHASES
            ):
                return False
        return True
    return False


def _is_profile_dispatch_submit_anchor_event(payload: dict[str, Any]) -> bool:
    if not _is_profile_prefetch_dispatch_event(payload):
        return False
    dispatch_payload = _profile_prefetch_dispatch_payload(payload)
    if _is_profile_prefetch_terminal_proof_only(dispatch_payload):
        return False
    if not _is_profile_refill_submit_event(payload):
        dispatch_status = str(dispatch_payload.get("status") or "").strip().lower()
        if dispatch_status not in {"queued", "running"}:
            return False
    if (
        _safe_int(dispatch_payload.get("dispatched_url_count")) <= 0
        and _safe_int(dispatch_payload.get("queued_worker_count")) <= 0
    ):
        return False
    return True


def _profile_prefetch_dispatch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    profile_prefetch = dict(payload.get("profile_prefetch") or {})
    if profile_prefetch:
        return profile_prefetch
    return dict(payload or {})


def _is_profile_prefetch_terminal_proof_only(profile_prefetch: dict[str, Any]) -> bool:
    if not profile_prefetch:
        return False
    if bool(profile_prefetch.get("terminal_proof_only")):
        return True
    if profile_prefetch.get("submit_anchor") is False:
        return True
    status = str(profile_prefetch.get("status") or "").strip().lower()
    reason = str(profile_prefetch.get("reason") or "").strip().lower()
    terminal_summary = dict(profile_prefetch.get("registry_terminal_summary") or {})
    queue_snapshot = dict(
        profile_prefetch.get("latest_profile_prefetch_queue")
        or profile_prefetch.get("profile_prefetch_queue")
        or {}
    )
    all_requested_terminal = bool(
        terminal_summary.get("all_requested_terminal")
        or queue_snapshot.get("registry_all_requested_terminal")
    )
    if status == "completed" and reason in _PROFILE_PREFETCH_TERMINAL_PROOF_REASONS:
        return True
    return bool(
        status == "completed"
        and all_requested_terminal
        and reason.startswith("registry_all_requested_profiles_")
    )


def _extract_profile_prefetch_queue_snapshots(profile_prefetch: dict[str, Any]) -> list[dict[str, Any]]:
    snapshots = profile_prefetch.get("profile_prefetch_queues")
    if isinstance(snapshots, list):
        return [dict(item) for item in snapshots if isinstance(item, dict)]
    snapshot = profile_prefetch.get("latest_profile_prefetch_queue") or profile_prefetch.get("profile_prefetch_queue")
    if isinstance(snapshot, dict):
        return [dict(snapshot)]
    return []


def _extract_profile_prefetch_batch_plans(profile_prefetch: dict[str, Any]) -> list[dict[str, Any]]:
    plans = profile_prefetch.get("batch_plans") or profile_prefetch.get("profile_prefetch_batch_plans")
    if isinstance(plans, list):
        return [dict(item) for item in plans if isinstance(item, dict)]
    plan = (
        profile_prefetch.get("latest_batch_plan")
        or profile_prefetch.get("batch_plan")
        or profile_prefetch.get("profile_prefetch_batch_plan")
    )
    if isinstance(plan, dict):
        return [dict(plan)]
    return []


def _build_profile_scheduler_contract_report(
    *,
    batch_envelopes: list[dict[str, Any]],
    batch_plans: list[dict[str, Any]],
    queue_snapshots: list[dict[str, Any]],
    refill_triggers: list[dict[str, Any]],
    scheduler_locks: list[dict[str, Any]],
    envelope_count: int,
    batch_plan_count: int,
    queue_snapshot_count: int,
    refill_trigger_count: int,
    planned_dispatch_owner_missing_count: int,
    terminal_queue_state_leak_count: int,
    underfilled_with_deferred_count: int,
    small_normal_batch_without_reason_count: int,
    batch_size_contract_violation_count: int,
    retry_wave_isolation_violation_count: int,
    same_wave_ordinal_violation_count: int,
    url_overlap_violation_count: int = 0,
    url_overlap_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    report_available = bool(
        envelope_count
        or batch_plan_count
        or queue_snapshot_count
        or refill_trigger_count
        or scheduler_locks
    )
    advisory_lock_missing_count = sum(
        1
        for lock in list(scheduler_locks or [])
        if _profile_scheduler_advisory_lock_missing(lock)
    )
    queue_open_slot_deferred_violation_count = sum(
        1
        for snapshot in list(queue_snapshots or [])
        if _profile_scheduler_queue_snapshot_open_slot_deferred_violation(snapshot)
    )
    slot_refill_violation_count = int(underfilled_with_deferred_count or 0) + int(
        queue_open_slot_deferred_violation_count
    )
    violation_count = (
        int(same_wave_ordinal_violation_count or 0)
        + int(url_overlap_violation_count or 0)
        + int(batch_size_contract_violation_count or 0)
        + int(small_normal_batch_without_reason_count or 0)
        + int(retry_wave_isolation_violation_count or 0)
        + slot_refill_violation_count
        + advisory_lock_missing_count
        + int(planned_dispatch_owner_missing_count or 0)
        + int(terminal_queue_state_leak_count or 0)
    )
    return {
        "schema_version": 1,
        "report_available": report_available,
        "violation_detected": violation_count > 0,
        "violation_count": violation_count,
        "same_wave_ordinal_violation_count": int(same_wave_ordinal_violation_count or 0),
        "url_overlap_violation_count": int(url_overlap_violation_count or 0),
        "batch_size_contract_violation_count": int(batch_size_contract_violation_count or 0),
        "small_normal_batch_without_reason_count": int(small_normal_batch_without_reason_count or 0),
        "retry_wave_isolation_violation_count": int(retry_wave_isolation_violation_count or 0),
        "slot_refill_violation_count": slot_refill_violation_count,
        "queue_open_slot_deferred_violation_count": queue_open_slot_deferred_violation_count,
        "advisory_lock_missing_count": advisory_lock_missing_count,
        "planned_dispatch_owner_missing_count": int(planned_dispatch_owner_missing_count or 0),
        "terminal_queue_state_leak_count": int(terminal_queue_state_leak_count or 0),
        "observed": {
            "batch_envelope_count": int(envelope_count or 0),
            "batch_plan_count": int(batch_plan_count or 0),
            "queue_snapshot_count": int(queue_snapshot_count or 0),
            "refill_trigger_count": int(refill_trigger_count or 0),
            "scheduler_lock_count": len(list(scheduler_locks or [])),
        },
        "samples": {
            "batch_envelopes": list(batch_envelopes or [])[:10],
            "batch_plans": list(batch_plans or [])[:10],
            "queue_snapshots": list(queue_snapshots or [])[:10],
            "refill_triggers": list(refill_triggers or [])[:10],
            "scheduler_locks": list(scheduler_locks or [])[:10],
            "url_overlaps": list(url_overlap_samples or [])[:10],
        },
    }


def _profile_scheduler_small_normal_batch_without_reason(envelope: dict[str, Any]) -> bool:
    payload = dict(envelope or {})
    status = str(payload.get("status") or "").strip().lower()
    if status in {"backpressure", "deferred", "skipped"}:
        return False
    dispatched_count = _safe_int(payload.get("dispatched_url_count"))
    if dispatched_count <= 0:
        return False
    recommended_batch_size = _safe_int(payload.get("recommended_batch_size"))
    min_non_tail_batch_size = _safe_int(payload.get("min_non_tail_batch_size"))
    threshold = max(min_non_tail_batch_size, recommended_batch_size)
    if threshold <= 0:
        threshold = _TINY_BATCH_DEFAULT_THRESHOLD
    if dispatched_count >= threshold:
        return False
    if _safe_int(payload.get("deferred_url_count")) <= 0:
        return False
    reason = str(payload.get("small_batch_reason") or payload.get("flush_reason") or "").strip()
    if reason == "retry_isolation":
        return False
    return reason not in _PROFILE_SCHEDULER_ALLOWED_SMALL_NORMAL_REASONS


def _profile_scheduler_batch_size_contract_violation(plan: dict[str, Any]) -> bool:
    payload = dict(plan or {})
    if _safe_int(payload.get("planned_dispatch_item_count")) <= 0 and _safe_int(payload.get("queue_item_count")) <= 0:
        return False
    strategy = str(payload.get("dispatch_strategy") or payload.get("base_dispatch_strategy") or "").strip()
    if "prefetch_window" not in strategy and str(payload.get("item_store") or "") != "linkedin_profile_registry":
        return False
    return str(payload.get("batch_size_contract") or "").strip() not in _PROFILE_SCHEDULER_ALLOWED_BATCH_SIZE_CONTRACTS


def _profile_scheduler_retry_wave_isolation_violation(plan: dict[str, Any]) -> bool:
    payload = dict(plan or {})
    normal_count = _safe_int(payload.get("normal_queue_item_count"))
    retry_count = _safe_int(payload.get("retry_wait_item_count"))
    if retry_count <= 0:
        return False
    if normal_count > 0:
        return True
    if not bool(payload.get("retry_isolation")):
        return True
    return str(payload.get("refill_policy") or "").strip() not in {"retry_wait_isolated_refill", ""}


def _profile_scheduler_queue_retry_wave_isolation_violation(snapshot: dict[str, Any]) -> bool:
    state_counts = _safe_counter(dict(snapshot or {}).get("refill_queue_state_counts"))
    retry_count = _safe_int(state_counts.get("retry_wait"))
    if retry_count <= 0:
        return False
    normal_open_count = sum(
        _safe_int(state_counts.get(state))
        for state in ("deferred_budget", "deferred_coalescing", "dispatch_reserved", "dispatch_claimed")
    )
    planned_dispatch_count = _safe_int(state_counts.get("planned_dispatch"))
    return normal_open_count > 0 or planned_dispatch_count > 0


def _profile_scheduler_same_wave_ordinal_violation_count(envelopes: list[dict[str, Any]]) -> int:
    normalized = [dict(item) for item in list(envelopes or []) if isinstance(item, dict)]
    if len(normalized) <= 1:
        return 0
    violation_count = 0
    gate_seen_by_wave: set[tuple[Any, ...]] = set()
    last_chunk_index_by_wave: dict[tuple[Any, ...], int] = {}
    for envelope in normalized:
        wave_key = _profile_scheduler_envelope_wave_key(envelope)
        chunk_index = _safe_int(envelope.get("chunk_index"))
        last_chunk_index = last_chunk_index_by_wave.get(wave_key, -1)
        if chunk_index < last_chunk_index:
            violation_count += 1
        last_chunk_index_by_wave[wave_key] = max(last_chunk_index, chunk_index)
        flush_reason = str(envelope.get("flush_reason") or "").strip()
        status = str(envelope.get("status") or "").strip().lower()
        dispatched_count = _safe_int(envelope.get("dispatched_url_count"))
        if (
            wave_key in gate_seen_by_wave
            and dispatched_count > 0
            and status in {"queued", "running", "waiting_remote_harvest"}
        ):
            violation_count += 1
        if flush_reason == "ordinal_submit_gate":
            gate_seen_by_wave.add(wave_key)
    return violation_count


def _profile_scheduler_envelope_profile_url_keys(envelope: dict[str, Any]) -> list[str]:
    payload = dict(envelope or {})
    values = list(payload.get("profile_url_keys") or [])
    if not values:
        values = list(payload.get("profile_urls") or payload.get("requested_urls") or [])
    keys: list[str] = []
    for value in values:
        normalized = str(value or "").strip().lower().rstrip("/")
        if not normalized:
            continue
        if normalized.startswith("http://") or normalized.startswith("https://"):
            normalized = normalized.rstrip("/")
        if normalized and normalized not in keys:
            keys.append(normalized)
    return keys


def _profile_scheduler_envelope_wave_key(envelope: dict[str, Any]) -> tuple[Any, ...]:
    payload = dict(envelope or {})
    for key in (
        "dispatch_wave_id",
        "profile_prefetch_wave_id",
        "batch_plan_id",
        "plan_id",
        "refill_generation",
        "source_wave_id",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return ("explicit", key, value)
    return (
        "derived",
        str(payload.get("source_job") or "").strip(),
        str(payload.get("snapshot_id") or payload.get("snapshot_dir") or "").strip(),
        str(payload.get("lane_scope") or payload.get("source_lane") or payload.get("source_shard") or "").strip(),
        _safe_int(payload.get("requested_url_count")),
        _safe_int(payload.get("candidate_count")),
        _safe_int(payload.get("recommended_batch_count")),
        _safe_int(payload.get("recommended_max_workers")),
        str(payload.get("dispatch_strategy") or "").strip(),
        str(payload.get("small_batch_reason") or "").strip(),
        bool(payload.get("queue_quiescent")),
    )


def _profile_scheduler_advisory_lock_missing(lock: dict[str, Any]) -> bool:
    payload = dict(lock or {})
    if not bool(payload.get("required")):
        return False
    kind = str(payload.get("kind") or payload.get("lock_kind") or "").strip()
    if bool(payload.get("busy")) or payload.get("acquired") is False:
        return False
    return kind not in _PROFILE_SCHEDULER_EXPECTED_LOCK_KINDS or not bool(payload.get("distributed"))


def _profile_scheduler_queue_snapshot_open_slot_deferred_violation(snapshot: dict[str, Any]) -> bool:
    payload = dict(snapshot or {})
    state_counts = _safe_counter(payload.get("refill_queue_state_counts"))
    immediate_deferred_count = _safe_int(state_counts.get("deferred_budget"))
    if immediate_deferred_count <= 0:
        immediate_deferred_count = _safe_int(payload.get("failed_url_count"))
    if immediate_deferred_count <= 0:
        return False
    available_count = _safe_int(payload.get("available_new_worker_count"))
    if available_count <= 0:
        return False
    if str(payload.get("reason") or "").strip() in {"profile_registry_lease_contention", "provider_owned_planned_dispatch"}:
        return False
    active_count = _safe_int(payload.get("active_worker_count"))
    queued_count = _safe_int(payload.get("queued_worker_count"))
    scheduler_reserved_count = _safe_int(payload.get("scheduler_reserved_worker_count"))
    effective_active_count = _safe_int(payload.get("effective_active_worker_count"))
    slot_occupancy_count = _safe_int(payload.get("slot_occupancy_count"))
    actor_budget = _safe_int(payload.get("actor_budget"))
    submit_budget = _safe_int(payload.get("submit_budget"))
    effective_budget_values = [value for value in (actor_budget, submit_budget) if value > 0]
    effective_budget = min(effective_budget_values) if effective_budget_values else available_count
    if effective_budget <= 0:
        return False
    occupancy = max(
        slot_occupancy_count,
        active_count + queued_count,
        scheduler_reserved_count,
        effective_active_count,
    )
    return occupancy < effective_budget


def _compact_profile_scheduler_lock(lock: dict[str, Any]) -> dict[str, Any]:
    keys = ("required", "kind", "lock_kind", "distributed", "acquired", "busy", "source", "reason", "scope")
    return {key: lock.get(key) for key in keys if key in lock}


def _compact_profile_prefetch_batch_plan(plan: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "kind",
        "schema_version",
        "item_store",
        "refill_policy",
        "plan_reason",
        "refill_saturation",
        "requested_url_count",
        "candidate_count",
        "queue_item_count",
        "planned_dispatch_worker_count",
        "planned_dispatch_item_count",
        "planned_deferred_item_count",
        "available_slot_count",
        "planned_new_worker_count",
        "unfilled_available_slot_count",
        "underfilled_with_deferred_items",
        "original_dispatch_chunk_count",
        "coalesced_dispatch_chunk_count",
        "tiny_batch_coalesced_count",
        "recommended_batch_size",
        "recommended_batch_count",
        "recommended_max_workers",
        "dispatch_strategy",
        "base_dispatch_strategy",
        "batch_size_contract",
        "batch_size_reason",
        "actor_slot_url_target",
        "large_ready_set_max_batch_count",
        "large_ready_set_threshold_urls",
        "provider_envelope_max_urls",
        "retry_isolation",
        "normal_queue_item_count",
        "retry_wait_item_count",
        "active_worker_count",
        "scheduler_reserved_worker_count",
        "effective_active_worker_count",
        "submit_budget",
        "actor_budget",
        "available_new_worker_count",
        "scheduler_lock",
    )
    return {key: plan.get(key) for key in keys if key in plan}


def _compact_profile_prefetch_refill_trigger(trigger: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "kind",
        "schema_version",
        "trigger_kind",
        "trigger_reason",
        "trigger_source",
        "item_store",
        "snapshot_id",
        "profile_prefetch_refill_phase",
        "status",
        "requested_url_count",
        "dispatched_url_count",
        "queued_worker_count",
        "deferred_url_count",
        "available_slot_count",
        "planned_new_worker_count",
        "planned_dispatch_item_count",
        "planned_deferred_item_count",
        "unfilled_available_slot_count",
        "underfilled_with_deferred_items",
        "refill_saturation",
        "elapsed_ms",
    )
    return {key: trigger.get(key) for key in keys if key in trigger}


def _compact_profile_prefetch_queue_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "kind",
        "schema_version",
        "item_store",
        "status",
        "reason",
        "requested_url_count",
        "cached_url_count",
        "ready_url_count",
        "newly_queued_url_count",
        "already_queued_url_count",
        "queued_url_count",
        "deferred_url_count",
        "failed_url_count",
        "pending_url_count",
        "ready_after_dispatch_url_count",
        "active_worker_count",
        "queued_worker_count",
        "scheduler_reserved_worker_count",
        "effective_active_worker_count",
        "actor_budget",
        "submit_budget",
        "available_new_worker_count",
        "slot_occupancy_basis",
        "slot_occupancy_count",
        "local_queue_quiescent",
        "remote_queue_quiescent",
        "queue_quiescent",
        "oldest_pending_item_age_ms",
        "refill_queue_state_counts",
        "registry_status_counts",
        "planned_dispatch_owner_missing_count",
        "planned_dispatch_remote_owner_count",
        "terminal_queue_state_leak_count",
        "batch_envelope_count",
        "tiny_batch_count",
        "tiny_tail_flush_reasons",
        "scheduler_lock",
    )
    return {key: snapshot.get(key) for key in keys if key in snapshot}


def _extract_profile_batch_envelopes(
    profile_prefetch: dict[str, Any],
    event_metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    if _is_profile_prefetch_terminal_proof_only(profile_prefetch):
        return []
    envelopes = [
        dict(item)
        for item in list(profile_prefetch.get("batch_envelopes") or [])
        if isinstance(item, dict)
    ]
    if envelopes:
        return envelopes
    dispatched_count = _safe_int(
        profile_prefetch.get("dispatched_url_count")
        or event_metrics.get("post_ingest_prefetch_dispatched_url_count")
    )
    if dispatched_count <= 0:
        return []
    deferred_count = _safe_int(profile_prefetch.get("deferred_url_count"))
    requested_count = _safe_int(
        profile_prefetch.get("requested_url_count")
        or event_metrics.get("post_ingest_prefetch_candidate_count")
    )
    small_batch_threshold = _TINY_BATCH_DEFAULT_THRESHOLD
    is_tiny = 0 < dispatched_count <= small_batch_threshold
    small_batch_reason = ""
    if is_tiny:
        small_batch_reason = "legacy_envelope_missing_reason"
    return [
        {
            "kind": "harvest_profile_scraper_batch",
            "status": str(profile_prefetch.get("status") or "unknown"),
            "batch_size": dispatched_count,
            "dispatched_url_count": dispatched_count,
            "requested_url_count": requested_count,
            "deferred_url_count": deferred_count,
            "small_batch_threshold": small_batch_threshold,
            "is_tiny_batch": is_tiny,
            "tiny_batch_allowed": True,
            "small_batch_reason": small_batch_reason,
            "provider_slot_underuse_with_backlog": False,
            "underuse_reason": "",
        }
    ]


def _extract_profile_batch_envelopes_from_workers(
    workers: list[dict[str, Any]],
    *,
    excluded_worker_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    excluded = {int(item) for item in set(excluded_worker_ids or set()) if int(item or 0) > 0}
    envelopes: list[dict[str, Any]] = []
    for worker in list(workers or []):
        if not isinstance(worker, dict):
            continue
        worker_id = _safe_int(worker.get("worker_id"))
        if worker_id > 0 and worker_id in excluded:
            continue
        metadata = dict(worker.get("metadata") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()
        worker_key = str(worker.get("worker_key") or "").strip()
        if recovery_kind != _HARVEST_PROFILE_RECOVERY_KIND and not worker_key.startswith(
            f"{_HARVEST_PROFILE_RECOVERY_KIND}::"
        ):
            continue
        status = str(worker.get("status") or "").strip().lower()
        if status not in _TERMINAL_WORKER_STATUSES:
            continue
        output = dict(worker.get("output") or {})
        summary = dict(output.get("summary") or {})
        input_payload = dict(worker.get("input") or {})
        profile_urls = _dedupe_texts(
            summary.get("requested_urls")
            or summary.get("profile_urls")
            or input_payload.get("profile_urls")
            or metadata.get("profile_urls")
            or checkpoint.get("profile_urls")
            or []
        )
        profile_url_keys = _profile_scheduler_envelope_profile_url_keys(
            {"profile_urls": profile_urls}
        )
        requested_count = max(
            _safe_int(summary.get("requested_url_count")),
            _safe_int(summary.get("requested_count")),
            _safe_int(dict(worker.get("budget") or {}).get("requested_url_count")),
            len(profile_urls),
        )
        if requested_count <= 0:
            continue
        small_batch_threshold = _TINY_BATCH_DEFAULT_THRESHOLD
        is_tiny = requested_count <= small_batch_threshold
        envelopes.append(
            {
                "kind": "harvest_profile_scraper_batch",
                "source": "completed_worker_summary",
                "worker_id": worker_id,
                "worker_key": worker_key,
                "status": status,
                "batch_size": requested_count,
                "dispatched_url_count": requested_count,
                "requested_url_count": requested_count,
                "profile_url_count": len(profile_url_keys),
                "profile_url_keys": profile_url_keys,
                "profile_url_sample": profile_urls[:5],
                "deferred_url_count": 0,
                "small_batch_threshold": small_batch_threshold,
                "is_tiny_batch": is_tiny,
                "tiny_batch_allowed": True,
                "small_batch_reason": "worker_summary_reconstructed_envelope" if is_tiny else "",
                "provider_slot_underuse_with_backlog": False,
                "underuse_reason": "",
            }
        )
    return envelopes


def _extract_profile_worker_dispatch_events(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reconstruct profile submit coverage from durable worker rows.

    This is intentionally narrower than event-level handoff metrics. It only
    decides whether a next-submit opportunity existed at all when structured
    prefetch events were compacted or omitted from the smoke report.
    """

    dispatch_events: list[dict[str, Any]] = []
    for worker in list(workers or []):
        if not isinstance(worker, dict):
            continue
        metadata = dict(worker.get("metadata") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()
        worker_key = str(worker.get("worker_key") or "").strip()
        if recovery_kind != _HARVEST_PROFILE_RECOVERY_KIND and not worker_key.startswith(
            f"{_HARVEST_PROFILE_RECOVERY_KIND}::"
        ):
            continue
        dispatched_count = _profile_worker_requested_url_count(worker)
        if dispatched_count <= 0:
            continue
        started_at = _profile_worker_submit_started_at(worker)
        if not started_at:
            continue
        dispatch_events.append(
            {
                "source": "agent_worker_runs",
                "worker_id": _safe_int(worker.get("worker_id")),
                "worker_key": worker_key,
                "started_at": started_at,
                "dispatched_url_count": dispatched_count,
            }
        )
    dispatch_events.sort(
        key=lambda item: _parse_timestamp(str(item.get("started_at") or ""))
        or datetime.max.replace(tzinfo=timezone.utc)
    )
    return dispatch_events


def _profile_worker_requested_url_count(worker: dict[str, Any]) -> int:
    output = dict(worker.get("output") or {})
    summary = dict(output.get("summary") or {})
    input_payload = dict(worker.get("input") or {})
    metadata = dict(worker.get("metadata") or {})
    checkpoint = dict(worker.get("checkpoint") or {})
    profile_urls = _dedupe_texts(
        summary.get("requested_urls")
        or summary.get("profile_urls")
        or input_payload.get("profile_urls")
        or metadata.get("profile_urls")
        or checkpoint.get("profile_urls")
        or []
    )
    return max(
        _safe_int(summary.get("requested_url_count")),
        _safe_int(summary.get("requested_count")),
        _safe_int(dict(worker.get("budget") or {}).get("requested_url_count")),
        len(profile_urls),
    )


def _profile_worker_submit_started_at(worker: dict[str, Any]) -> str:
    checkpoint = dict(worker.get("checkpoint") or {})
    provider_limiter = dict(checkpoint.get("provider_limiter_lease") or {})
    candidates = [
        checkpoint.get("remote_wait_started_at"),
        checkpoint.get("provider_submit_started_at"),
        provider_limiter.get("created_at"),
        provider_limiter.get("updated_at"),
        worker.get("created_at"),
    ]
    parsed_candidates: list[datetime] = []
    for value in candidates:
        parsed = _parse_timestamp(value)
        if parsed is not None:
            parsed_candidates.append(parsed)
    if not parsed_candidates:
        return ""
    return min(parsed_candidates).isoformat()


def _compact_profile_batch_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "kind",
        "source",
        "worker_id",
        "worker_key",
        "chunk_index",
        "status",
        "flush_reason",
        "batch_size",
        "profile_url_count",
        "profile_url_sample",
        "dispatched_url_count",
        "requested_url_count",
        "deferred_url_count",
        "tail_coalescing_url_count",
        "worker_budget_deferred_url_count",
        "active_worker_count_before_dispatch",
        "queued_worker_count_after_dispatch",
        "actor_budget",
        "submit_budget",
        "recommended_batch_size",
        "recommended_batch_count",
        "recommended_max_workers",
        "dispatch_strategy",
        "is_tiny_batch",
        "small_batch_reason",
        "tiny_batch_allowed",
        "min_non_tail_batch_size",
        "idle_actor_slots_after_dispatch",
        "provider_slot_underuse_with_backlog",
        "underuse_reason",
        "queue_quiescent",
    )
    return {key: envelope.get(key) for key in keys if key in envelope}


def _extract_worker_ids(payload: dict[str, Any]) -> list[int]:
    values: list[Any] = []
    for key in ("worker_ids", "target_worker_ids", "applied_worker_ids"):
        values.extend(list(payload.get(key) or []))
    worker_id = payload.get("worker_id")
    if worker_id is not None:
        values.append(worker_id)
    deduped: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            worker_id_int = int(value or 0)
        except (TypeError, ValueError):
            continue
        if worker_id_int <= 0 or worker_id_int in seen:
            continue
        seen.add(worker_id_int)
        deduped.append(worker_id_int)
    return deduped


def _stats(values: list[float]) -> dict[str, Any]:
    normalized = [float(value) for value in list(values or []) if float(value) >= 0.0]
    if not normalized:
        return {}
    sorted_values = sorted(normalized)
    return {
        "count": len(sorted_values),
        "min": round(sorted_values[0], 2),
        "avg": round(sum(sorted_values) / len(sorted_values), 2),
        "max": round(sorted_values[-1], 2),
        "values": [round(value, 2) for value in sorted_values[:50]],
    }


def _stats_values(payload: Any) -> list[float]:
    if not isinstance(payload, dict):
        return []
    values = payload.get("values")
    if isinstance(values, list):
        return [float(value) for value in values if isinstance(value, (int, float))]
    count = _safe_int(payload.get("count"))
    if count <= 0:
        return []
    avg = _safe_float(payload.get("avg"))
    return [avg] * count if avg >= 0.0 else []


def _duration_ms(started_at: str, completed_at: str) -> float | None:
    started = _parse_timestamp(started_at)
    completed = _parse_timestamp(completed_at)
    if started is None or completed is None or completed < started:
        return None
    elapsed = (completed - started).total_seconds() * 1000
    if elapsed > _MAX_REASONABLE_EVENT_LAG_MS:
        return None
    return round(elapsed, 2)


def _timestamp_within_interval(timestamp_value: str, started_at: str, finished_at: str) -> bool:
    timestamp = _parse_timestamp(timestamp_value)
    started = _parse_timestamp(started_at)
    finished = _parse_timestamp(finished_at)
    if timestamp is None or started is None:
        return False
    if timestamp < started:
        return False
    if finished is None:
        return True
    return timestamp <= finished


def _parse_timestamp(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for candidate in (normalized, normalized.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S",):
        try:
            parsed = datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_counter(value: Any) -> Counter[str]:
    counter: Counter[str] = Counter()
    if not isinstance(value, dict):
        return counter
    for key, raw_count in value.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        counter[normalized_key] += _safe_int(raw_count)
    return counter


def _dedupe_texts(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    ordered: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered
