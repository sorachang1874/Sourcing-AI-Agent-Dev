from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .durable_runtime import (
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
    ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
    ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    CRM_NOTE_ADD_COMMAND_TYPE,
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_RECORD_UPDATE_COMMAND_TYPE,
    CRM_TASK_CREATE_COMMAND_TYPE,
    DEFAULT_COMMAND_TYPE_SPECS,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    TERMINAL_COMMAND_STATUSES,
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
    workflow_command_display_contract,
    workflow_command_metric_key,
)

_SLOW_WORKER_THRESHOLD_MS = 30_000.0
_SLOW_HANDOFF_GAP_THRESHOLD_MS = 5_000.0
_BOARD_VISIBLE_THRESHOLD_MS = 2_000.0
_BOARD_NONEMPTY_THRESHOLD_MS = 5_000.0
_LONG_FINALIZATION_THRESHOLD_MS = 30_000.0
_BOARD_VISIBLE_MATERIALIZATION_LAG_THRESHOLD = 5
_LOCAL_APPLY_BACKLOG_STALE_THRESHOLD_MS = 30_000.0
_REMOTE_PROVIDER_EVENT_LAG_THRESHOLD_MS = 30_000.0
_SERVING_PUBLICATION_GAP_THRESHOLD_MS = 30_000.0
_PROFILE_FILE_TO_BOARD_PATCH_THRESHOLD_MS = 30_000.0
_ALL_PROFILES_FETCHED_TO_ALL_CARDS_VISIBLE_THRESHOLD_MS = 30_000.0
_EVENT_LEVEL_CALLBACK_THRESHOLD_MS = 10_000.0
_RECOVERY_PHASE_DEFAULT_THRESHOLD_MS = 30_000.0
_RECOVERY_PHASE_TOTAL_THRESHOLD_MS = 30_000.0
_JOB_MATERIALIZATION_QUEUED_STATUSES = {"queued", "deferred", "waiting_prerequisite"}
_JOB_MATERIALIZATION_RUNNING_STATUSES = {"running", "applying"}
# Per-command-type metrics contract derived from the durable_runtime command
# type registry: metric_key and expected_owner are owned by
# DEFAULT_COMMAND_TYPE_SPECS. The explicit order tuple preserves the report
# ordering of the historical literal table.
_DURABLE_COMMAND_OWNER_CONTRACT_COMMAND_TYPE_ORDER = (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
    ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
    ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_RECORD_UPDATE_COMMAND_TYPE,
    CRM_NOTE_ADD_COMMAND_TYPE,
    CRM_TASK_CREATE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
)
if set(_DURABLE_COMMAND_OWNER_CONTRACT_COMMAND_TYPE_ORDER) != set(DEFAULT_COMMAND_TYPE_SPECS):
    raise RuntimeError(
        "workflow_service_metrics command-owner contract order tuple is out of sync "
        "with durable_runtime.DEFAULT_COMMAND_TYPE_SPECS"
    )
_DURABLE_COMMAND_OWNER_CONTRACTS = {
    command_type: {
        "metric_key": workflow_command_metric_key(command_type),
        "expected_owner": DEFAULT_COMMAND_TYPE_SPECS[command_type].owner,
    }
    for command_type in _DURABLE_COMMAND_OWNER_CONTRACT_COMMAND_TYPE_ORDER
}
_RECOVERY_PHASE_REQUIRED_NAMES = (
    "search_seed_discovery",
    "worker_recovery",
    "blocked_workflow_cleanup",
    "local_apply_backlog",
    "event_level_materialization_followup",
    "profile_prefetch_refill",
    "profile_refill_event_level_materialization_followup",
    "workflow_resume",
    "post_projection_workflow_resume",
    "post_completion_reconcile",
    "excel_intake_recovery",
    "board_visible_apply",
    "snapshot_full_materialization",
    "explicit_job_followup_rounds",
    "remote_event_followup",
    "post_recovery_housekeeping",
    "total",
)


def build_workflow_service_metrics(
    *,
    workers: list[dict[str, Any]] | None = None,
    trace_spans: list[dict[str, Any]] | None = None,
    workflow_wall_clock_ms: dict[str, Any] | None = None,
    stage_wall_clock_ms: dict[str, Any] | None = None,
    timings_ms: dict[str, Any] | None = None,
    timeline: list[dict[str, Any]] | None = None,
    job_events: list[dict[str, Any]] | None = None,
    final_summary: dict[str, Any] | None = None,
    result_view_lifecycle: dict[str, Any] | None = None,
    serving_publication_gap: dict[str, Any] | None = None,
    board_runtime_state: dict[str, Any] | None = None,
    board_visible_patches: list[dict[str, Any]] | None = None,
    materialization_items: list[dict[str, Any]] | None = None,
    workflow_commands: list[dict[str, Any]] | None = None,
    provider_anomaly_query_summaries: list[dict[str, Any]] | None = None,
    target_candidate_public_web_batches: list[dict[str, Any]] | None = None,
    legacy_public_web_retirement_audit: dict[str, Any] | None = None,
    company_public_web_runs: list[dict[str, Any]] | None = None,
    worker_recovery_runs: list[dict[str, Any]] | None = None,
    post_preview_finalization: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_workers = [dict(item) for item in list(workers or []) if isinstance(item, dict)]
    normalized_spans = [dict(item) for item in list(trace_spans or []) if isinstance(item, dict)]
    wall_clock = {str(key): _safe_float(value) for key, value in dict(workflow_wall_clock_ms or {}).items()}
    stage_clock = {str(key): _safe_float(value) for key, value in dict(stage_wall_clock_ms or {}).items()}
    timings = {str(key): _safe_float(value) for key, value in dict(timings_ms or {}).items()}
    normalized_final_summary = dict(final_summary or {})
    normalized_timeline = [dict(item) for item in list(timeline or []) if isinstance(item, dict)]
    normalized_job_events = [dict(item) for item in list(job_events or []) if isinstance(item, dict)]
    lifecycle = dict(result_view_lifecycle or {})
    publication_gap = dict(serving_publication_gap or {})
    runtime_state = dict(board_runtime_state or {})
    normalized_board_visible_patches = [
        dict(item) for item in list(board_visible_patches or []) if isinstance(item, dict)
    ]
    normalized_materialization_items = [
        dict(item) for item in list(materialization_items or []) if isinstance(item, dict)
    ]
    normalized_workflow_commands = [
        dict(item) for item in list(workflow_commands or []) if isinstance(item, dict)
    ]
    normalized_provider_anomaly_query_summaries = [
        dict(item) for item in list(provider_anomaly_query_summaries or []) if isinstance(item, dict)
    ]
    normalized_public_web_batches = [
        dict(item) for item in list(target_candidate_public_web_batches or []) if isinstance(item, dict)
    ]
    normalized_legacy_public_web_retirement_audit = dict(legacy_public_web_retirement_audit or {})
    normalized_company_public_web_runs = [
        dict(item) for item in list(company_public_web_runs or []) if isinstance(item, dict)
    ]
    normalized_worker_recovery_runs = [
        dict(item) for item in list(worker_recovery_runs or []) if isinstance(item, dict)
    ]
    normalized_post_preview_finalization = dict(post_preview_finalization or {})

    remote_completion_by_worker = _remote_provider_completion_by_worker(normalized_job_events)
    worker_timeline = _build_worker_timeline_metrics(
        normalized_workers,
        normalized_spans,
        remote_completion_by_worker=remote_completion_by_worker,
    )
    local_apply_backlog = _build_local_apply_backlog_metrics(
        workers=normalized_workers,
        materialization_items=normalized_materialization_items,
    )
    user_experience = _build_user_experience_metrics(
        workflow_wall_clock_ms=wall_clock,
        stage_wall_clock_ms=stage_clock,
        timings_ms=timings,
        timeline=normalized_timeline,
        post_preview_finalization=normalized_post_preview_finalization,
    )
    board_visible_projection = _build_board_visible_projection_metrics(
        lifecycle=lifecycle,
        board_visible_patches=normalized_board_visible_patches,
    )
    post_profile_completion = _build_post_profile_completion_metrics(
        workers=normalized_workers,
        job_events=normalized_job_events,
        materialization_items=normalized_materialization_items,
        workflow_commands=normalized_workflow_commands,
        lifecycle=lifecycle,
        board_runtime_state=runtime_state,
        board_visible_patches=normalized_board_visible_patches,
        board_visible_projection=board_visible_projection,
    )
    serving_publication_gap_metrics = _build_serving_publication_gap_metrics(publication_gap)
    snapshot_full_materialization_queue = _build_snapshot_full_materialization_queue_metrics(
        materialization_items=normalized_materialization_items,
        workflow_commands=normalized_workflow_commands,
    )
    search_seed_discovery_queue = _build_search_seed_discovery_queue_metrics(
        workers=normalized_workers,
        materialization_items=normalized_materialization_items,
        workflow_commands=normalized_workflow_commands,
    )
    provider_search_retry_queue = _build_provider_search_retry_queue_metrics(
        materialization_items=normalized_materialization_items,
    )
    legacy_materialization_write_contract = _build_legacy_materialization_write_contract_metrics(
        materialization_items=normalized_materialization_items,
    )
    workflow_causality_contract = _build_workflow_causality_contract_metrics(
        workflow_commands=normalized_workflow_commands,
    )
    durable_command_owner_contracts = _build_durable_command_owner_contract_metrics(
        workflow_commands=normalized_workflow_commands,
    )
    provider_anomalies = _build_provider_anomaly_metrics(
        materialization_items=normalized_materialization_items,
        workflow_commands=normalized_workflow_commands,
        provider_anomaly_query_summaries=normalized_provider_anomaly_query_summaries,
    )
    remote_provider_events = _build_remote_provider_event_metrics(job_events=normalized_job_events)
    target_candidate_public_web = _build_target_candidate_public_web_metrics(
        batches=normalized_public_web_batches,
        workflow_commands=normalized_workflow_commands,
    )
    legacy_public_web_retirement = _build_legacy_public_web_retirement_metrics(
        audit=normalized_legacy_public_web_retirement_audit,
    )
    company_public_web = _build_company_public_web_metrics(
        runs=normalized_company_public_web_runs,
    )
    recovery_phase_metrics = _build_recovery_phase_metrics(
        recovery_runs=normalized_worker_recovery_runs,
    )
    board_overlay_writes = _build_board_overlay_write_metrics(
        board_visible_patches=normalized_board_visible_patches,
    )
    finalization_overlay = _build_finalization_overlay_metrics(
        final_summary=normalized_final_summary,
        board_visible_projection=board_visible_projection,
    )
    bottlenecks = _build_bottleneck_report(
        worker_timeline=worker_timeline,
        local_apply_backlog=local_apply_backlog,
        user_experience=user_experience,
        board_visible_projection=board_visible_projection,
        serving_publication_gap=serving_publication_gap_metrics,
        snapshot_full_materialization_queue=snapshot_full_materialization_queue,
        search_seed_discovery_queue=search_seed_discovery_queue,
        provider_search_retry_queue=provider_search_retry_queue,
        remote_provider_events=remote_provider_events,
        target_candidate_public_web=target_candidate_public_web,
        legacy_public_web_retirement=legacy_public_web_retirement,
        company_public_web=company_public_web,
        recovery_phase_metrics=recovery_phase_metrics,
        board_overlay_writes=board_overlay_writes,
        finalization_overlay=finalization_overlay,
        legacy_materialization_write_contract=legacy_materialization_write_contract,
    )
    return {
        "report_available": bool(
            normalized_workers
            or normalized_spans
            or local_apply_backlog.get("report_available")
            or wall_clock
            or timings
            or lifecycle
            or publication_gap
            or normalized_board_visible_patches
            or normalized_materialization_items
            or normalized_workflow_commands
            or normalized_provider_anomaly_query_summaries
            or normalized_job_events
            or normalized_post_preview_finalization
            or finalization_overlay.get("report_available")
            or normalized_public_web_batches
            or legacy_public_web_retirement.get("report_available")
            or normalized_company_public_web_runs
            or normalized_worker_recovery_runs
            or post_profile_completion.get("report_available")
            or recovery_phase_metrics.get("report_available")
            or board_overlay_writes.get("report_available")
        ),
        "worker_timeline": worker_timeline,
        "local_apply_backlog": local_apply_backlog,
        "user_experience": user_experience,
        "board_visible_projection": board_visible_projection,
        "post_profile_completion": post_profile_completion,
        "serving_publication_gap": serving_publication_gap_metrics,
        "snapshot_full_materialization_queue": snapshot_full_materialization_queue,
        "search_seed_discovery_queue": search_seed_discovery_queue,
        "provider_search_retry_queue": provider_search_retry_queue,
        "legacy_materialization_write_contract": legacy_materialization_write_contract,
        "workflow_causality_contract": workflow_causality_contract,
        "durable_command_owner_contracts": durable_command_owner_contracts,
        "provider_anomalies": provider_anomalies,
        "remote_provider_events": remote_provider_events,
        "target_candidate_public_web": target_candidate_public_web,
        "legacy_public_web_retirement": legacy_public_web_retirement,
        "company_public_web": company_public_web,
        "recovery_phase_metrics": recovery_phase_metrics,
        "board_overlay_writes": board_overlay_writes,
        "finalization_overlay": finalization_overlay,
        "bottlenecks": bottlenecks,
    }


def _build_worker_timeline_metrics(
    workers: list[dict[str, Any]],
    trace_spans: list[dict[str, Any]],
    *,
    remote_completion_by_worker: dict[int, str] | None = None,
) -> dict[str, Any]:
    spans_by_id = {_safe_int(span.get("span_id")): span for span in trace_spans if _safe_int(span.get("span_id")) > 0}
    remote_completion_by_worker = dict(remote_completion_by_worker or {})
    rows: list[dict[str, Any]] = []
    lane_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    lane_durations: dict[str, list[float]] = defaultdict(list)
    for worker in workers:
        span = spans_by_id.get(_safe_int(worker.get("span_id")), {})
        started_at = _first_nonempty(span.get("started_at"), worker.get("created_at"))
        completed_at = _first_nonempty(span.get("completed_at"))
        if not completed_at and str(worker.get("status") or "").strip().lower() in {
            "completed",
            "failed",
            "skipped",
            "cancelled",
            "canceled",
            "interrupted",
        }:
            completed_at = _first_nonempty(worker.get("updated_at"))
        checkpoint = dict(worker.get("checkpoint") or {})
        remote_completed_at = str(remote_completion_by_worker.get(_safe_int(worker.get("worker_id"))) or "")
        provider_started_at = _provider_started_at_from_checkpoint(checkpoint)
        scripted_remote_ready_at = _scripted_remote_ready_at_from_checkpoint(checkpoint)
        handoff_started_at = provider_started_at or started_at
        handoff_completed_at = remote_completed_at or scripted_remote_ready_at or completed_at
        lane_id = str(worker.get("lane_id") or span.get("lane_id") or "").strip()
        status = str(worker.get("status") or span.get("status") or "").strip()
        duration_ms = _duration_ms(started_at, completed_at)
        lane_counts[lane_id] += 1
        status_counts[status] += 1
        if duration_ms is not None and duration_ms >= 0.0:
            lane_durations[lane_id].append(duration_ms)
        metadata = dict(worker.get("metadata") or {})
        prefetch_batch_context = _profile_prefetch_batch_context(worker)
        rows.append(
            {
                "worker_id": _safe_int(worker.get("worker_id")),
                "span_id": _safe_int(worker.get("span_id")),
                "lane_id": lane_id,
                "worker_key": str(worker.get("worker_key") or "").strip(),
                "status": status,
                "effective_status": str(worker.get("effective_status") or "").strip(),
                "wait_stage": str(worker.get("wait_stage") or "").strip(),
                "recovery_kind": str(metadata.get("recovery_kind") or "").strip(),
                "started_at": started_at,
                "provider_started_at": provider_started_at,
                "handoff_started_at": handoff_started_at,
                "completed_at": completed_at,
                "handoff_completed_at": handoff_completed_at,
                "remote_completed_at": remote_completed_at,
                "scripted_remote_ready_at": scripted_remote_ready_at,
                "duration_ms": round(duration_ms, 2) if duration_ms is not None else None,
                "next_worker_start_gap_ms": None,
                "same_lane_next_worker_start_gap_ms": None,
                "handoff_from_lane": str(span.get("handoff_from_lane") or "").strip(),
                "handoff_to_lane": str(span.get("handoff_to_lane") or "").strip(),
                "profile_prefetch_chunk_index": _safe_int(prefetch_batch_context.get("chunk_index")),
                "profile_prefetch_planned_dispatch_worker_count": _safe_int(
                    prefetch_batch_context.get("planned_dispatch_worker_count")
                ),
                "profile_prefetch_requested_url_count": _safe_int(
                    prefetch_batch_context.get("requested_url_count")
                ),
                "profile_prefetch_actual_requested_url_count": _safe_int(
                    checkpoint.get("request_context", {}).get("requested_url_count")
                    if isinstance(checkpoint.get("request_context"), dict)
                    else 0
                ),
            }
        )
    rows.sort(
        key=lambda item: (
            _parse_timestamp(str(item.get("handoff_started_at") or item.get("started_at") or ""))
            or datetime.max.replace(tzinfo=timezone.utc),
            _safe_int(item.get("worker_id")),
        )
    )
    gaps = _build_worker_gap_metrics(rows, trace_spans=trace_spans)
    out_of_order_completion = _build_worker_out_of_order_completion_metrics(rows)
    slow_workers = sorted(
        [row for row in rows if _safe_float(row.get("duration_ms")) >= _SLOW_WORKER_THRESHOLD_MS],
        key=lambda item: _safe_float(item.get("duration_ms")),
        reverse=True,
    )[:10]
    return {
        "worker_count": len(rows),
        "trace_span_count": len(trace_spans),
        "lane_counts": {key: int(value) for key, value in sorted(lane_counts.items()) if key},
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "duration_ms": _stats([_safe_float(row.get("duration_ms")) for row in rows if row.get("duration_ms") is not None]),
        "duration_by_lane_ms": {
            lane: _stats(values) for lane, values in sorted(lane_durations.items()) if lane and values
        },
        "handoff_gap_ms": gaps,
        "out_of_order_completion": out_of_order_completion,
        "slow_workers": [
            {
                "worker_id": row["worker_id"],
                "lane_id": row["lane_id"],
                "worker_key": row["worker_key"],
                "duration_ms": row["duration_ms"],
                "status": row["status"],
                "recovery_kind": row["recovery_kind"],
            }
            for row in slow_workers
        ],
        "timeline_sample": [
            {
                "worker_id": row["worker_id"],
                "lane_id": row["lane_id"],
                "worker_key": row["worker_key"],
                "status": row["status"],
                "started_at": row["started_at"],
                "provider_started_at": row["provider_started_at"],
                "handoff_started_at": row["handoff_started_at"],
                "completed_at": row["completed_at"],
                "handoff_completed_at": row["handoff_completed_at"],
                "remote_completed_at": row["remote_completed_at"],
                "scripted_remote_ready_at": row["scripted_remote_ready_at"],
                "duration_ms": row["duration_ms"],
                "next_worker_start_gap_ms": row["next_worker_start_gap_ms"],
                "same_lane_next_worker_start_gap_ms": row["same_lane_next_worker_start_gap_ms"],
                "wait_stage": row["wait_stage"],
                "recovery_kind": row["recovery_kind"],
            }
            for row in rows[:25]
        ],
    }


def _build_worker_out_of_order_completion_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    profile_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("recovery_kind") or "").strip() != "harvest_profile_batch":
            continue
        started_at = _parse_timestamp(str(row.get("provider_started_at") or "")) or _parse_timestamp(
            str(row.get("started_at") or "")
        )
        remote_completed_at = _parse_timestamp(str(row.get("remote_completed_at") or ""))
        scripted_remote_ready_at = _parse_timestamp(str(row.get("scripted_remote_ready_at") or ""))
        completed_at = remote_completed_at or scripted_remote_ready_at or _parse_timestamp(
            str(row.get("completed_at") or "")
        )
        if started_at is None or completed_at is None:
            continue
        if remote_completed_at is not None:
            completion_source = "remote_provider_event"
        elif scripted_remote_ready_at is not None:
            completion_source = "scripted_remote_ready"
        else:
            completion_source = "worker_terminal_marker"
        profile_rows.append(
            {
                **row,
                "_started_at": started_at,
                "_completed_at": completed_at,
                "_completion_source": completion_source,
            }
        )
    profile_rows.sort(key=lambda item: (item["_started_at"], _safe_int(item.get("worker_id"))))
    inversion_count = 0
    samples: list[dict[str, Any]] = []
    for index, earlier in enumerate(profile_rows):
        earlier_completed_at = earlier["_completed_at"]
        for later in profile_rows[index + 1 :]:
            same_start_time = earlier["_started_at"] == later["_started_at"]
            if earlier["_started_at"] > later["_started_at"]:
                continue
            if same_start_time and _safe_int(earlier.get("worker_id")) >= _safe_int(later.get("worker_id")):
                continue
            later_completed_at = later["_completed_at"]
            if earlier_completed_at <= later_completed_at:
                continue
            inversion_count += 1
            if len(samples) >= 10:
                continue
            samples.append(
                {
                    "earlier_worker_id": _safe_int(earlier.get("worker_id")),
                    "later_worker_id": _safe_int(later.get("worker_id")),
                    "earlier_worker_key": str(earlier.get("worker_key") or "").strip(),
                    "later_worker_key": str(later.get("worker_key") or "").strip(),
                    "earlier_completion_source": str(earlier.get("_completion_source") or ""),
                    "later_completion_source": str(later.get("_completion_source") or ""),
                    "start_gap_ms": round((later["_started_at"] - earlier["_started_at"]).total_seconds() * 1000, 2),
                    "completion_lead_ms": round(
                        (earlier_completed_at - later_completed_at).total_seconds() * 1000,
                        2,
                    ),
                }
            )
    return {
        "profile_batch_worker_count": len(profile_rows),
        "profile_batch_inversion_count": inversion_count,
        "profile_batch_observed": inversion_count > 0,
        "samples": samples,
    }


def _provider_started_at_from_checkpoint(checkpoint: dict[str, Any]) -> str:
    payload = dict(checkpoint or {})
    terminal_event = dict(payload.get("remote_provider_terminal_event") or {})
    for key in ("run_started_at", "remote_started_at", "started_at"):
        value = str(terminal_event.get(key) or "").strip()
        if value:
            return value
    for key in ("remote_wait_started_at", "provider_started_at", "scripted_remote_started_at"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    provider_limiter_lease = dict(payload.get("provider_limiter_lease") or {})
    value = str(provider_limiter_lease.get("created_at") or "").strip()
    if value:
        return value
    return ""


def _scripted_remote_ready_at_from_checkpoint(checkpoint: dict[str, Any]) -> str:
    payload = dict(checkpoint or {})
    provider_mode = str(payload.get("provider_mode") or "").strip().lower()
    if provider_mode != "scripted":
        return ""
    if not bool(payload.get("scripted_remote_wait_after_submit")):
        return ""
    ready_epoch_ms = _safe_float(payload.get("scripted_remote_ready_epoch_ms"))
    if ready_epoch_ms <= 0.0:
        return ""
    try:
        return datetime.fromtimestamp(ready_epoch_ms / 1000.0, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return ""


def _build_local_apply_backlog_metrics(
    *,
    workers: list[dict[str, Any]],
    materialization_items: list[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_worker_kind: Counter[str] = Counter()
    by_recovery_kind: Counter[str] = Counter()
    ages_ms: list[float] = []
    now = datetime.now(timezone.utc)
    for worker in workers:
        output = dict(worker.get("output") or {})
        apply_marker = dict(output.get("inline_incremental_apply") or {})
        if not apply_marker:
            continue
        ingest_marker = dict(output.get("inline_incremental_ingest") or {})
        if ingest_marker:
            continue
        applied_at = str(apply_marker.get("applied_at") or worker.get("updated_at") or "").strip()
        applied_at_dt = _parse_timestamp(applied_at)
        age_ms = max(0.0, (now - applied_at_dt).total_seconds() * 1000) if applied_at_dt is not None else 0.0
        if age_ms > 0.0:
            ages_ms.append(age_ms)
        worker_kind = str(apply_marker.get("worker_kind") or "").strip()
        recovery_kind = str(dict(worker.get("metadata") or {}).get("recovery_kind") or "").strip()
        if worker_kind:
            by_worker_kind[worker_kind] += 1
        if recovery_kind:
            by_recovery_kind[recovery_kind] += 1
        rows.append(
            {
                "worker_id": _safe_int(worker.get("worker_id")),
                "worker_kind": worker_kind,
                "recovery_kind": recovery_kind,
                "snapshot_id": str(apply_marker.get("snapshot_id") or "").strip(),
                "applied_at": applied_at,
                "age_ms": round(age_ms, 2),
                "apply_status": str(apply_marker.get("apply_status") or "").strip(),
            }
        )
    stale_rows = [
        row
        for row in rows
        if _safe_float(row.get("age_ms")) >= _LOCAL_APPLY_BACKLOG_STALE_THRESHOLD_MS
    ]
    closure_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "local_apply_closure"
    ]
    closure_status_counts: Counter[str] = Counter()
    closure_phase_counts: Counter[str] = Counter()
    closure_retryable_count = 0
    closure_ready_retry_count = 0
    closure_running_count = 0
    closure_stale_running_count = 0
    closure_terminal_failed_count = 0
    waiting_prerequisite_count = 0
    waiting_prerequisite_ages_ms: list[float] = []
    waiting_prerequisite_history_count = 0
    waiting_prerequisite_completed_count = 0
    waiting_prerequisite_reawakened_count = 0
    waiting_prerequisite_fallback_completed_count = 0
    closure_error_samples: list[str] = []
    now_iso_dt = now
    for item in closure_items:
        status = str(item.get("status") or "").strip().lower()
        phase = str(item.get("phase") or "").strip().lower()
        if status:
            closure_status_counts[status] += 1
        if phase:
            closure_phase_counts[phase] += 1
        if status == "failed_retryable":
            closure_retryable_count += 1
            not_before_at = str(item.get("not_before_at") or "").strip()
            parsed_not_before_at = _parse_timestamp(not_before_at)
            if parsed_not_before_at is None or parsed_not_before_at <= now_iso_dt:
                closure_ready_retry_count += 1
        if status == "waiting_prerequisite":
            waiting_prerequisite_count += 1
            updated_at = str(item.get("updated_at") or "").strip()
            parsed_updated_at = _parse_timestamp(updated_at)
            if parsed_updated_at is not None:
                age_ms = (now_iso_dt - parsed_updated_at).total_seconds() * 1000
                waiting_prerequisite_ages_ms.append(age_ms)
        metadata = dict(item.get("metadata") or {})
        if str(metadata.get("failure_reason") or "").strip() == "waiting_prerequisite_candidate_documents":
            waiting_prerequisite_history_count += 1
            if status == "completed":
                waiting_prerequisite_completed_count += 1
                if str(metadata.get("reawakened_by") or metadata.get("prerequisite_ready_source") or "").strip():
                    waiting_prerequisite_reawakened_count += 1
                else:
                    waiting_prerequisite_fallback_completed_count += 1
        if status in _JOB_MATERIALIZATION_RUNNING_STATUSES:
            closure_running_count += 1
            lease_expires_at = str(item.get("lease_expires_at") or "").strip()
            parsed_lease_expires_at = _parse_timestamp(lease_expires_at)
            if parsed_lease_expires_at is not None and parsed_lease_expires_at <= now_iso_dt:
                closure_stale_running_count += 1
        if status == "failed":
            closure_terminal_failed_count += 1
        last_error = str(item.get("last_error") or "").strip()
        if last_error and len(closure_error_samples) < 5:
            closure_error_samples.append(last_error)
    closure_backlog_count = sum(
        1
        for item in closure_items
        if (
            str(item.get("status") or "").strip().lower()
            in (_JOB_MATERIALIZATION_QUEUED_STATUSES | _JOB_MATERIALIZATION_RUNNING_STATUSES | {"failed_retryable"})
        )
    )
    return {
        "report_available": bool(rows or closure_items),
        "applied_not_ingested_count": len(rows),
        "stale_applied_not_ingested_count": len(stale_rows),
        "stale_applied_not_ingested_present": bool(stale_rows),
        "age_ms": _stats(ages_ms),
        "stale_threshold_ms": _LOCAL_APPLY_BACKLOG_STALE_THRESHOLD_MS,
        "by_worker_kind": {key: int(value) for key, value in sorted(by_worker_kind.items()) if key},
        "by_recovery_kind": {key: int(value) for key, value in sorted(by_recovery_kind.items()) if key},
        "closure_item_count": len(closure_items),
        "closure_backlog_count": closure_backlog_count,
        "closure_retryable_count": closure_retryable_count,
        "closure_ready_retry_count": closure_ready_retry_count,
        "closure_running_count": closure_running_count,
        "closure_stale_running_count": closure_stale_running_count,
        "closure_terminal_failed_count": closure_terminal_failed_count,
        "waiting_prerequisite_count": waiting_prerequisite_count,
        "waiting_prerequisite_age_max_ms": max(waiting_prerequisite_ages_ms) if waiting_prerequisite_ages_ms else 0,
        "waiting_prerequisite_age_mean_ms": (
            sum(waiting_prerequisite_ages_ms) / len(waiting_prerequisite_ages_ms) if waiting_prerequisite_ages_ms else 0
        ),
        "waiting_prerequisite_history_count": waiting_prerequisite_history_count,
        "waiting_prerequisite_completed_count": waiting_prerequisite_completed_count,
        "waiting_prerequisite_reawakened_count": waiting_prerequisite_reawakened_count,
        "waiting_prerequisite_fallback_completed_count": waiting_prerequisite_fallback_completed_count,
        "waiting_prerequisite_fallback_completed_present": waiting_prerequisite_fallback_completed_count > 0,
        "closure_status_counts": {key: int(value) for key, value in sorted(closure_status_counts.items()) if key},
        "closure_phase_counts": {key: int(value) for key, value in sorted(closure_phase_counts.items()) if key},
        "closure_error_samples": closure_error_samples,
        "closure_retry_backlog_present": closure_ready_retry_count > 0,
        "closure_stale_running_present": closure_stale_running_count > 0,
        "sample": sorted(rows, key=lambda item: _safe_float(item.get("age_ms")), reverse=True)[:10],
    }


def _build_worker_gap_metrics(
    rows: list[dict[str, Any]],
    *,
    trace_spans: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    global_gaps: list[float] = []
    profile_scheduler_uncovered_gaps: list[float] = []
    same_lane_gaps: dict[str, list[float]] = defaultdict(list)
    slow_gaps: list[dict[str, Any]] = []
    upstream_spans = _profile_scheduler_upstream_spans(list(trace_spans or []))
    previous_global: dict[str, Any] | None = None
    previous_by_lane: dict[str, dict[str, Any]] = {}
    for row in rows:
        start_dt = _parse_timestamp(str(row.get("handoff_started_at") or row.get("started_at") or ""))
        if start_dt is None:
            continue
        if previous_global is not None:
            gap = _gap_between(previous_global, row)
            if gap is not None:
                global_gaps.append(gap)
                previous_global["next_worker_start_gap_ms"] = round(gap, 2)
                gap_context = _profile_scheduler_gap_context(previous_global, row, upstream_spans=upstream_spans)
                if gap_context["profile_scheduler_gap"]:
                    previous_global["profile_scheduler_gap_covered_by_upstream"] = gap_context[
                        "covered_by_upstream"
                    ]
                    previous_global["profile_scheduler_gap_covering_span_id"] = gap_context["covering_span_id"]
                    if not gap_context["covered_by_upstream"]:
                        profile_scheduler_uncovered_gaps.append(gap)
                if gap >= _SLOW_HANDOFF_GAP_THRESHOLD_MS:
                    slow_gaps.append(_gap_record(previous_global, row, gap, scope="global", context=gap_context))
        lane = str(row.get("lane_id") or "").strip()
        previous_lane = previous_by_lane.get(lane)
        if lane and previous_lane is not None:
            gap = _gap_between(previous_lane, row)
            if gap is not None:
                same_lane_gaps[lane].append(gap)
                previous_lane["same_lane_next_worker_start_gap_ms"] = round(gap, 2)
                if gap >= _SLOW_HANDOFF_GAP_THRESHOLD_MS:
                    gap_context = _profile_scheduler_gap_context(previous_lane, row, upstream_spans=upstream_spans)
                    slow_gaps.append(_gap_record(previous_lane, row, gap, scope=f"lane:{lane}", context=gap_context))
        previous_global = row
        if lane:
            previous_by_lane[lane] = row
    return {
        "global_next_worker_start_gap_ms": _stats(global_gaps),
        "profile_scheduler_next_worker_start_gap_ms": _zeroable_stats(profile_scheduler_uncovered_gaps),
        "same_lane_next_worker_start_gap_ms": {
            lane: _stats(values) for lane, values in sorted(same_lane_gaps.items()) if values
        },
        "slow_gap_threshold_ms": _SLOW_HANDOFF_GAP_THRESHOLD_MS,
        "slow_gaps": sorted(slow_gaps, key=lambda item: _safe_float(item.get("gap_ms")), reverse=True)[:10],
    }


def _build_user_experience_metrics(
    *,
    workflow_wall_clock_ms: dict[str, float],
    stage_wall_clock_ms: dict[str, float],
    timings_ms: dict[str, float],
    timeline: list[dict[str, Any]],
    post_preview_finalization: dict[str, Any],
) -> dict[str, Any]:
    final_to_board_ready = _safe_float(workflow_wall_clock_ms.get("final_results_to_board_ready"))
    final_to_board_nonempty = _safe_float(workflow_wall_clock_ms.get("final_results_to_board_nonempty"))
    job_to_board_visible_partial = _safe_float(workflow_wall_clock_ms.get("job_to_board_visible_partial"))
    job_to_board_nonempty = _safe_float(workflow_wall_clock_ms.get("job_to_board_nonempty"))
    preview_to_final = _safe_float(workflow_wall_clock_ms.get("stage_1_preview_to_final_results"))
    profile_wait_excluded_ms = _safe_float(
        post_preview_finalization.get("profile_wait_excluded_from_finalization_gate_ms")
    )
    raw_finalization_start_gate_ms = post_preview_finalization.get("finalization_start_gate_ms")
    finalization_start_gate_observed = raw_finalization_start_gate_ms is not None
    finalization_start_gate_ms = _safe_float(raw_finalization_start_gate_ms)
    finalization_lag_evaluation_ms = preview_to_final
    finalization_lag_evaluation_source = "workflow_wall_clock_stage_1_preview_to_final_results"
    finalization_lag_excludes_profile_wait = False
    if profile_wait_excluded_ms > 0.0 and finalization_start_gate_observed:
        finalization_lag_evaluation_ms = finalization_start_gate_ms
        finalization_lag_evaluation_source = (
            str(post_preview_finalization.get("finalization_start_gate_source") or "").strip()
            or "profile_terminal_finalization_start_gate"
        )
        finalization_lag_excludes_profile_wait = True
    return {
        "first_progress_observed_ms": _first_positive_timeline_observed_ms(timeline),
        "timeline_event_count": len(timeline),
        "job_to_stage_1_preview_ms": _safe_float(workflow_wall_clock_ms.get("job_to_stage_1_preview")),
        "stage_1_preview_to_final_results_ms": preview_to_final,
        "raw_stage_1_preview_to_final_results_ms": preview_to_final,
        "finalization_lag_evaluation_ms": finalization_lag_evaluation_ms,
        "finalization_lag_evaluation_source": finalization_lag_evaluation_source,
        "finalization_lag_excludes_profile_provider_wait": finalization_lag_excludes_profile_wait,
        "profile_wait_excluded_from_finalization_gate_ms": profile_wait_excluded_ms,
        "finalization_start_gate_ms": finalization_start_gate_ms if finalization_start_gate_observed else None,
        "finalization_start_gate_source": str(
            post_preview_finalization.get("finalization_start_gate_source") or ""
        ).strip(),
        "job_to_final_results_ms": _safe_float(workflow_wall_clock_ms.get("job_to_final_results")),
        "final_results_to_board_ready_ms": final_to_board_ready,
        "final_results_to_board_nonempty_ms": final_to_board_nonempty,
        "job_to_board_visible_partial_ms": job_to_board_visible_partial,
        "job_to_board_nonempty_ms": job_to_board_nonempty,
        "dashboard_fetch_ms": _safe_float(timings_ms.get("dashboard_fetch")),
        "candidate_page_fetch_ms": _safe_float(timings_ms.get("candidate_page_fetch")),
        "board_probe_wait_ms": _safe_float(timings_ms.get("board_probe_wait")),
        "board_ready_wait_ms": _safe_float(timings_ms.get("board_ready_wait")),
        "board_nonempty_wait_ms": _safe_float(timings_ms.get("board_nonempty_wait")),
        "board_expected_total_wait_ms": _safe_float(timings_ms.get("board_expected_total_wait")),
        "layering_visible_wait_ms": _safe_float(timings_ms.get("layering_visible_wait")),
        "layering_visible_timed_out": bool(timings_ms.get("layering_visible_timed_out")),
        "stage_wall_clock_ms": {
            key: value for key, value in sorted(stage_wall_clock_ms.items()) if value > 0.0
        },
        "thresholds_ms": {
            "final_results_to_board_ready": _BOARD_VISIBLE_THRESHOLD_MS,
            "final_results_to_board_nonempty": _BOARD_NONEMPTY_THRESHOLD_MS,
            "job_to_board_visible_partial": _BOARD_VISIBLE_THRESHOLD_MS,
            "stage_1_preview_to_final_results": _LONG_FINALIZATION_THRESHOLD_MS,
        },
        "partial_board_visible_observed": job_to_board_visible_partial > 0.0,
        "loading_feedback_required": final_to_board_ready > _BOARD_VISIBLE_THRESHOLD_MS,
        "board_readiness_violation": final_to_board_nonempty > _BOARD_NONEMPTY_THRESHOLD_MS,
        "long_finalization_after_preview": finalization_lag_evaluation_ms > _LONG_FINALIZATION_THRESHOLD_MS,
    }


def _build_post_profile_completion_metrics(
    *,
    workers: list[dict[str, Any]],
    job_events: list[dict[str, Any]],
    materialization_items: list[dict[str, Any]],
    workflow_commands: list[dict[str, Any]],
    lifecycle: dict[str, Any],
    board_runtime_state: dict[str, Any],
    board_visible_patches: list[dict[str, Any]],
    board_visible_projection: dict[str, Any],
) -> dict[str, Any]:
    """Read-only post-profile SLO report built from smoke diagnostics."""

    profile_workers = [
        dict(worker)
        for worker in workers
        if str(dict(dict(worker).get("metadata") or {}).get("recovery_kind") or "").strip()
        == "harvest_profile_batch"
    ]
    completed_profile_workers = [
        worker
        for worker in profile_workers
        if str(worker.get("status") or "").strip().lower() in {"completed", "failed", "skipped", "cancelled"}
    ]
    profile_worker_ids = {
        _safe_int(worker.get("worker_id"))
        for worker in profile_workers
        if _safe_int(worker.get("worker_id")) > 0
    }
    completed_profile_times = [
        parsed
        for worker in completed_profile_workers
        if (
            parsed := _parse_timestamp(
                _first_nonempty(
                    dict(worker.get("checkpoint") or {}).get("remote_completed_at"),
                    worker.get("completed_at"),
                    worker.get("updated_at"),
                )
            )
        )
        is not None
    ]
    profile_terminal_at = max(completed_profile_times) if completed_profile_times else None

    queue_terminal_state_leak_count = 0
    queue_snapshot_count = 0
    refill_state_counts: Counter[str] = Counter()
    for snapshot in _profile_prefetch_queue_snapshots_from_events(job_events):
        queue_snapshot_count += 1
        snapshot_created_at = _parse_timestamp(str(snapshot.get("event_created_at") or ""))
        if profile_terminal_at is None or (
            snapshot_created_at is not None and snapshot_created_at >= profile_terminal_at
        ):
            queue_terminal_state_leak_count += _safe_int(snapshot.get("terminal_queue_state_leak_count"))
        refill_state_counts.update(_safe_counter(snapshot.get("refill_queue_state_counts")))

    callback_elapsed_values: list[float] = []
    callback_samples: list[dict[str, Any]] = []
    for event in job_events:
        payload = _event_payload(event)
        trigger = dict(
            payload.get("profile_refill_trigger")
            or dict(payload.get("profile_prefetch") or {}).get("profile_refill_trigger")
            or {}
        )
        if not bool(trigger.get("signal_only") or trigger.get("refill_daemon_signal_only")):
            continue
        elapsed_ms = _safe_float(trigger.get("elapsed_ms"))
        if elapsed_ms >= 0.0:
            callback_elapsed_values.append(elapsed_ms)
        if len(callback_samples) < 10:
            callback_samples.append(
                {
                    "source": str(trigger.get("trigger_source") or payload.get("source") or "").strip(),
                    "status": str(trigger.get("status") or "").strip(),
                    "elapsed_ms": round(elapsed_ms, 2),
                    "requested_url_count": _safe_int(trigger.get("requested_url_count")),
                    "dispatched_url_count": _safe_int(trigger.get("dispatched_url_count")),
                    "queued_worker_count": _safe_int(trigger.get("queued_worker_count")),
                    "created_at": _event_created_at(event),
                }
            )

    local_apply_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "local_apply_closure"
    ]
    typed_items = _post_profile_materialization_items_from_workflow_commands(workflow_commands)
    local_apply_items.extend(
        item for item in typed_items if str(item.get("item_kind") or "").strip() == "local_apply_closure"
    )
    completed_local_apply_items = [
        item for item in local_apply_items if str(item.get("status") or "").strip().lower() == "completed"
    ]
    completed_profile_local_apply_items = [
        item
        for item in completed_local_apply_items
        if _local_apply_item_belongs_to_profile_completion(item, profile_worker_ids=profile_worker_ids)
    ]
    board_visible_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "board_visible_delta_apply"
    ]
    board_visible_items.extend(
        item
        for item in typed_items
        if str(item.get("item_kind") or "").strip() == "board_visible_delta_apply"
    )
    completed_board_visible_items = [
        item for item in board_visible_items if str(item.get("status") or "").strip().lower() == "completed"
    ]

    board_visible_times_by_snapshot: dict[str, list[datetime]] = defaultdict(list)
    board_visible_times_by_causal_group: dict[str, list[datetime]] = defaultdict(list)
    for item in completed_board_visible_items:
        snapshot_id = str(item.get("snapshot_id") or "").strip()
        causal_group_id = str(item.get("causal_group_id") or "").strip()
        completed_at = _parse_timestamp(_first_nonempty(item.get("completed_at"), item.get("updated_at")))
        if snapshot_id and completed_at is not None:
            board_visible_times_by_snapshot[snapshot_id].append(completed_at)
        if causal_group_id and completed_at is not None:
            board_visible_times_by_causal_group[causal_group_id].append(completed_at)
    for patch in board_visible_patches:
        snapshot_id = str(patch.get("snapshot_id") or "").strip()
        published_at = _parse_timestamp(str(patch.get("published_at") or ""))
        if snapshot_id and published_at is not None:
            board_visible_times_by_snapshot[snapshot_id].append(published_at)

    profile_file_to_board_patch_values: list[float] = []
    unmatched_local_apply_count = 0
    typed_causal_group_pair_count = 0
    typed_missing_causal_group_count = 0
    legacy_snapshot_pair_count = 0
    legacy_snapshot_unmatched_count = 0
    for item in completed_profile_local_apply_items:
        if "candidate_count" in item and _safe_int(item.get("candidate_count")) <= 0:
            continue
        causal_group_id = str(item.get("causal_group_id") or "").strip()
        snapshot_id = str(item.get("snapshot_id") or "").strip()
        completed_at = _parse_timestamp(_first_nonempty(item.get("completed_at"), item.get("updated_at")))
        visible_window_started_at = _parse_timestamp(_first_nonempty(item.get("created_at"), item.get("updated_at")))
        if visible_window_started_at is None:
            visible_window_started_at = completed_at
        if completed_at is None or visible_window_started_at is None:
            continue
        if str(item.get("source") or "").strip() == "workflow_commands":
            if not causal_group_id:
                typed_missing_causal_group_count += 1
            visible_candidates = [
                visible_at
                for visible_at in board_visible_times_by_causal_group.get(causal_group_id, [])
                if visible_at >= visible_window_started_at
            ]
        elif snapshot_id:
            visible_candidates = [
                visible_at
                for visible_at in board_visible_times_by_snapshot.get(snapshot_id, [])
                if visible_at >= visible_window_started_at
            ]
        else:
            visible_candidates = []
        if not visible_candidates:
            unmatched_local_apply_count += 1
            if str(item.get("source") or "").strip() != "workflow_commands":
                legacy_snapshot_unmatched_count += 1
            continue
        if str(item.get("source") or "").strip() == "workflow_commands":
            typed_causal_group_pair_count += 1
        else:
            legacy_snapshot_pair_count += 1
        first_visible_at = min(visible_candidates)
        if first_visible_at <= completed_at:
            profile_file_to_board_patch_values.append(0.0)
            continue
        profile_file_to_board_patch_values.append(
            max(0.0, (first_visible_at - completed_at).total_seconds() * 1000)
        )

    delta_required = _safe_int(lifecycle.get("delta_profile_required_count"))
    delta_fetched = _safe_int(lifecycle.get("delta_profile_fetched_count"))
    delta_board_visible = _safe_int(board_visible_projection.get("delta_profile_board_visible_count"))
    served_candidate_count = _safe_int(board_visible_projection.get("served_candidate_count"))
    full_snapshot_serving = bool(board_visible_projection.get("full_snapshot_serving"))
    delta_progress_applicable = bool(lifecycle.get("delta_profile_progress_applicable") is not False)
    runtime_publication_status = str(board_runtime_state.get("publication_status") or "").strip().lower()
    runtime_expected_count = max(
        _safe_int(board_runtime_state.get("expected_candidate_count")),
        _safe_int(lifecycle.get("expected_candidate_count")),
        served_candidate_count,
    )
    runtime_visible_count = max(
        _safe_int(board_runtime_state.get("published_candidate_count")),
        _safe_int(board_runtime_state.get("display_ready_candidate_count")),
        served_candidate_count,
    )
    full_snapshot_completion_visible = bool(
        full_snapshot_serving
        or (not delta_progress_applicable and runtime_publication_status == "complete")
    )
    if full_snapshot_serving:
        full_snapshot_visible_count = served_candidate_count
    elif not delta_progress_applicable and runtime_publication_status == "complete":
        full_snapshot_visible_count = runtime_visible_count
    else:
        full_snapshot_visible_count = 0
    if not delta_progress_applicable:
        target_visible_count = runtime_expected_count
        visible_count = runtime_visible_count
    else:
        target_visible_count = max(delta_required, delta_fetched, delta_board_visible)
        visible_count = delta_board_visible
    all_cards_visible = bool(target_visible_count > 0 and visible_count >= target_visible_count)
    candidate_all_cards_visible_at = _all_cards_visible_timestamp(
        board_visible_patches=board_visible_patches,
        completed_board_visible_items=completed_board_visible_items,
        lifecycle=lifecycle,
        board_runtime_state=board_runtime_state,
        target_visible_count=target_visible_count,
        full_snapshot_serving=full_snapshot_completion_visible,
        delta_progress_applicable=delta_progress_applicable,
    )
    all_cards_visible_at = candidate_all_cards_visible_at if all_cards_visible else None
    all_profiles_to_cards_visible_ms = None
    if profile_terminal_at is not None and all_cards_visible_at is not None and all_cards_visible:
        all_profiles_to_cards_visible_ms = max(
            0.0,
            (all_cards_visible_at - profile_terminal_at).total_seconds() * 1000,
        )

    callback_elapsed_stats = _stats(callback_elapsed_values)
    profile_file_to_board_patch_stats = _stats(profile_file_to_board_patch_values)
    all_profiles_to_cards_values = (
        [all_profiles_to_cards_visible_ms] if all_profiles_to_cards_visible_ms is not None else []
    )
    all_profiles_to_cards_stats = _stats(all_profiles_to_cards_values)
    callback_violation = _safe_float(callback_elapsed_stats.get("max")) > _EVENT_LEVEL_CALLBACK_THRESHOLD_MS
    profile_file_violation = (
        _safe_float(profile_file_to_board_patch_stats.get("max")) > _PROFILE_FILE_TO_BOARD_PATCH_THRESHOLD_MS
    )
    all_profiles_violation = (
        _safe_float(all_profiles_to_cards_stats.get("max"))
        > _ALL_PROFILES_FETCHED_TO_ALL_CARDS_VISIBLE_THRESHOLD_MS
    )
    all_profiles_visibility_incomplete = bool(
        delta_progress_applicable
        and target_visible_count > 0
        and delta_fetched >= target_visible_count
        and not all_cards_visible
    )
    terminal_state_violation = queue_terminal_state_leak_count > 0
    return {
        "report_available": bool(
            profile_workers
            or queue_snapshot_count
            or callback_elapsed_values
            or local_apply_items
            or board_visible_items
            or board_visible_patches
            or target_visible_count > 0
        ),
        "profile_worker_count": len(profile_workers),
        "completed_profile_worker_count": len(completed_profile_workers),
        "profile_terminal_at": profile_terminal_at.isoformat() if profile_terminal_at else "",
        "url_terminal_state_recording": {
            "queue_snapshot_count": queue_snapshot_count,
            "terminal_queue_state_leak_count": queue_terminal_state_leak_count,
            "refill_queue_state_counts": dict(refill_state_counts),
            "slo_violation": terminal_state_violation,
        },
        "event_level_callback": {
            "event_count": len(callback_elapsed_values),
            "elapsed_ms": callback_elapsed_stats,
            "threshold_ms": _EVENT_LEVEL_CALLBACK_THRESHOLD_MS,
            "slo_violation": callback_violation,
            "samples": callback_samples,
        },
        "profile_file_visible_to_board_patch_visible": {
            "pairing_contract": "typed_causal_group_for_workflow_commands",
            "completed_local_apply_count": len(completed_profile_local_apply_items),
            "ignored_non_profile_local_apply_count": max(
                0,
                len(completed_local_apply_items) - len(completed_profile_local_apply_items),
            ),
            "completed_board_visible_item_count": len(completed_board_visible_items),
            "unmatched_local_apply_count": unmatched_local_apply_count,
            "typed_causal_group_pair_count": typed_causal_group_pair_count,
            "typed_missing_causal_group_count": typed_missing_causal_group_count,
            "legacy_snapshot_pair_count": legacy_snapshot_pair_count,
            "legacy_snapshot_unmatched_count": legacy_snapshot_unmatched_count,
            "heuristic_pairing_used": legacy_snapshot_pair_count > 0,
            "elapsed_ms": profile_file_to_board_patch_stats,
            "threshold_ms": _PROFILE_FILE_TO_BOARD_PATCH_THRESHOLD_MS,
            "slo_violation": profile_file_violation,
        },
        "all_profiles_fetched_to_all_cards_visible": {
            "delta_profile_progress_applicable": delta_progress_applicable,
            "delta_profile_required_count": delta_required,
            "delta_profile_fetched_count": delta_fetched,
            "delta_profile_board_visible_count": delta_board_visible,
            "full_snapshot_visible_count": full_snapshot_visible_count,
            "visible_count": visible_count,
            "target_visible_count": target_visible_count,
            "missing_visible_count": max(0, target_visible_count - visible_count),
            "all_cards_visible": all_cards_visible,
            "all_cards_visible_at": all_cards_visible_at.isoformat() if all_cards_visible_at else "",
            "elapsed_ms": all_profiles_to_cards_stats,
            "threshold_ms": _ALL_PROFILES_FETCHED_TO_ALL_CARDS_VISIBLE_THRESHOLD_MS,
            "slo_violation": bool(all_profiles_violation or all_profiles_visibility_incomplete),
        },
        "slo_violation_detected": bool(
            terminal_state_violation
            or callback_violation
            or profile_file_violation
            or all_profiles_violation
            or all_profiles_visibility_incomplete
        ),
    }


def _local_apply_item_belongs_to_profile_completion(
    item: dict[str, Any],
    *,
    profile_worker_ids: set[int],
) -> bool:
    metadata = dict(dict(item or {}).get("metadata") or {})
    if str(metadata.get("worker_kind") or "").strip() == "harvest_prefetch":
        return True
    if str(metadata.get("recovery_kind") or "").strip() == "harvest_profile_batch":
        return True
    provider_result = dict(metadata.get("provider_completion_result") or {})
    if str(provider_result.get("worker_kind") or "").strip() == "harvest_prefetch":
        return True
    if str(provider_result.get("recovery_kind") or "").strip() == "harvest_profile_batch":
        return True
    item_worker_ids = {
        _safe_int(worker_id)
        for worker_id in list(dict(item or {}).get("source_worker_ids") or [])
        if _safe_int(worker_id) > 0
    }
    if item_worker_ids:
        return bool(item_worker_ids & profile_worker_ids)
    # Older tests and snapshots may not carry source_worker_ids. In that legacy
    # shape a local_apply item in a profile-only report still represents profile
    # completion; newer rows are scoped by worker ids and metadata above.
    return bool(profile_worker_ids and not metadata)


def _post_profile_materialization_items_from_workflow_commands(
    workflow_commands: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expose typed post-profile commands through the old metric envelope.

    The W6 metrics contract must not depend on legacy `job_materialization_items`
    once normal-path work has moved to `workflow_commands`. This adapter is
    read-only reporting glue: it maps command-owner evidence into the same
    compact fields used by the latency/SLO reducers without reintroducing the
    legacy item queue as an execution path.
    """

    items: list[dict[str, Any]] = []
    command_type_to_item_kind = {
        LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE: "local_apply_closure",
        PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE: "board_visible_delta_apply",
    }
    for command in list(workflow_commands or []):
        command_payload = dict(command or {})
        command_type = str(command_payload.get("command_type") or "").strip()
        item_kind = command_type_to_item_kind.get(command_type, "")
        if not item_kind:
            continue
        payload = dict(command_payload.get("payload") or {})
        result = dict(command_payload.get("result") or {})
        status = str(command_payload.get("status") or "").strip().lower()
        if status == "succeeded":
            item_status = "completed"
        elif status in {"retry_wait", "failed_retryable"}:
            item_status = "failed_retryable"
        elif status in {"failed_terminal", "failed"}:
            item_status = "failed"
        elif status in {"partial", "running", "claimed", "queued", "waiting_prerequisite"}:
            item_status = status
        else:
            item_status = status or str(result.get("status") or "").strip().lower()
        snapshot_id = str(result.get("snapshot_id") or payload.get("snapshot_id") or "").strip()
        updated_at = _first_nonempty(
            result.get("completed_at"),
            command_payload.get("updated_at"),
            result.get("updated_at"),
        )
        source_worker_ids = [
            _safe_int(worker_id)
            for worker_id in list(payload.get("source_worker_ids") or payload.get("worker_ids") or [])
            if _safe_int(worker_id) > 0
        ]
        metadata = dict(payload.get("materialization_metadata") or {})
        if command_type == LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE:
            metadata.setdefault("worker_kind", str(payload.get("worker_kind") or "harvest_prefetch"))
            metadata.setdefault("recovery_kind", "harvest_profile_batch")
        produced_counts = dict(command_payload.get("produced_entity_counts") or {})
        produced_candidate_count = max(
            _safe_int(produced_counts.get("candidate")),
            _safe_int(produced_counts.get("candidate_count")),
        )
        items.append(
            {
                "item_id": str(payload.get("item_id") or command_payload.get("command_id") or "").strip(),
                "item_kind": item_kind,
                "status": item_status,
                "phase": str(payload.get("stage_key") or payload.get("phase") or command_payload.get("owner") or "").strip(),
                "snapshot_id": snapshot_id,
                "causal_group_id": str(command_payload.get("causal_group_id") or "").strip(),
                "parent_command_id": str(command_payload.get("parent_command_id") or "").strip(),
                "source_event_id": str(command_payload.get("source_event_id") or "").strip(),
                "readiness_effect": str(command_payload.get("readiness_effect") or "").strip(),
                "no_op_reason": str(command_payload.get("no_op_reason") or "").strip(),
                "candidate_count": max(
                    _safe_int(result.get("candidate_count")),
                    _safe_int(payload.get("candidate_count")),
                    produced_candidate_count,
                ),
                "source_worker_ids": source_worker_ids,
                "metadata": metadata,
                "created_at": str(command_payload.get("created_at") or ""),
                "updated_at": str(command_payload.get("updated_at") or ""),
                "completed_at": updated_at if item_status == "completed" else "",
                "published_at": updated_at if item_kind == "board_visible_delta_apply" else "",
                "source": "workflow_commands",
            }
        )
    return items


def _profile_prefetch_queue_snapshots_from_events(job_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []

    def append_if_queue(value: Any, *, event_created_at: str) -> None:
        if not isinstance(value, dict):
            return
        if str(value.get("kind") or "").strip() != "linkedin_profile_prefetch_queue":
            return
        snapshots.append({**dict(value), "event_created_at": event_created_at})

    for event in job_events:
        payload = _event_payload(dict(event or {}))
        event_created_at = _event_created_at(event)
        append_if_queue(payload.get("profile_prefetch_queue"), event_created_at=event_created_at)
        profile_prefetch = payload.get("profile_prefetch")
        if isinstance(profile_prefetch, dict):
            append_if_queue(profile_prefetch.get("profile_prefetch_queue"), event_created_at=event_created_at)
            append_if_queue(profile_prefetch.get("queue_snapshot"), event_created_at=event_created_at)
    return snapshots


def _all_cards_visible_timestamp(
    *,
    board_visible_patches: list[dict[str, Any]],
    completed_board_visible_items: list[dict[str, Any]],
    lifecycle: dict[str, Any],
    board_runtime_state: dict[str, Any],
    target_visible_count: int,
    full_snapshot_serving: bool,
    delta_progress_applicable: bool,
) -> datetime | None:
    if target_visible_count <= 0:
        return None
    candidates: list[datetime] = []
    for patch in sorted(
        board_visible_patches,
        key=lambda item: (
            _parse_timestamp(str(dict(item).get("published_at") or "")) or datetime.max.replace(tzinfo=timezone.utc),
            _safe_int(dict(item).get("sequence_index")),
        ),
    ):
        patch_payload = dict(patch or {})
        if delta_progress_applicable:
            visible_count = max(
                _safe_int(patch_payload.get("delta_profile_board_visible_count")),
                _safe_int(patch_payload.get("cumulative_candidate_count")),
                len(list(patch_payload.get("cumulative_candidate_ids") or []))
                if isinstance(patch_payload.get("cumulative_candidate_ids"), list)
                else 0,
                len(list(patch_payload.get("candidate_ids") or []))
                if isinstance(patch_payload.get("candidate_ids"), list)
                else 0,
            )
        else:
            visible_count = max(
                _safe_int(patch_payload.get("cumulative_candidate_count")),
                _safe_int(patch_payload.get("served_candidate_count")),
                len(list(patch_payload.get("cumulative_candidate_ids") or []))
                if isinstance(patch_payload.get("cumulative_candidate_ids"), list)
                else 0,
                len(list(patch_payload.get("candidate_ids") or []))
                if isinstance(patch_payload.get("candidate_ids"), list)
                else 0,
            )
        if visible_count < target_visible_count:
            continue
        published_at = _parse_timestamp(str(patch_payload.get("published_at") or ""))
        if published_at is not None:
            candidates.append(published_at)
    if delta_progress_applicable:
        for projection in (board_runtime_state, lifecycle):
            if _safe_int(dict(projection or {}).get("delta_profile_board_visible_count")) < target_visible_count:
                continue
            published_at = _parse_timestamp(
                _first_nonempty(
                    dict(projection or {}).get("row_publication_updated_at"),
                    dict(projection or {}).get("published_at"),
                    dict(projection or {}).get("updated_at"),
                    dict(projection or {}).get("completed_at"),
                )
            )
            if published_at is not None:
                candidates.append(published_at)
    elif not candidates and full_snapshot_serving:
        for item in completed_board_visible_items:
            completed_at = _parse_timestamp(_first_nonempty(item.get("completed_at"), item.get("updated_at")))
            if completed_at is not None:
                candidates.append(completed_at)
    return min(candidates) if candidates else None


def _patch_overlay_write_payload(patch: dict[str, Any]) -> dict[str, Any]:
    payload = dict(patch or {})
    metadata = dict(payload.get("metadata") or {})
    mirror = dict(metadata.get("result_view_metadata_mirror") or {})
    patch_payload = dict(metadata.get("partial_board_visible_patch") or {})
    asset_patch = dict(metadata.get("asset_population_patch") or {})
    return {
        "mode": str(
            payload.get("overlay_write_mode")
            or mirror.get("overlay_write_mode")
            or patch_payload.get("overlay_write_mode")
            or asset_patch.get("overlay_write_mode")
            or ""
        ).strip(),
        "fast_path_eligible": bool(
            payload.get("overlay_fast_path_eligible")
            or mirror.get("overlay_fast_path_eligible")
            or patch_payload.get("overlay_fast_path_eligible")
            or asset_patch.get("overlay_fast_path_eligible")
        ),
        "fast_path_used": bool(
            payload.get("overlay_fast_path_used")
            or mirror.get("overlay_fast_path_used")
            or patch_payload.get("overlay_fast_path_used")
            or asset_patch.get("overlay_fast_path_used")
        ),
        "fallback_reason": str(
            payload.get("overlay_fallback_reason")
            or mirror.get("overlay_fallback_reason")
            or patch_payload.get("overlay_fallback_reason")
            or asset_patch.get("overlay_fallback_reason")
            or ""
        ).strip(),
    }


def _build_board_overlay_write_metrics(
    *,
    board_visible_patches: list[dict[str, Any]],
) -> dict[str, Any]:
    mode_counts: Counter[str] = Counter()
    fallback_reason_counts: Counter[str] = Counter()
    sample_rows: list[dict[str, Any]] = []
    patch_count = 0
    fast_path_eligible_count = 0
    fast_path_used_count = 0
    full_rebuild_count = 0
    eligible_full_rebuild_fallback_count = 0
    for patch in list(board_visible_patches or []):
        payload = _patch_overlay_write_payload(dict(patch or {}))
        mode = str(payload.get("mode") or "").strip()
        if not mode:
            continue
        patch_count += 1
        mode_counts[mode] += 1
        fast_path_eligible = bool(payload.get("fast_path_eligible"))
        fast_path_used = bool(payload.get("fast_path_used"))
        fallback_reason = str(payload.get("fallback_reason") or "").strip()
        if fast_path_eligible:
            fast_path_eligible_count += 1
        if fast_path_used:
            fast_path_used_count += 1
        if mode == "full_rebuild":
            full_rebuild_count += 1
            if fast_path_eligible and (fallback_reason or not fast_path_used):
                eligible_full_rebuild_fallback_count += 1
        if fallback_reason:
            fallback_reason_counts[fallback_reason] += 1
        if len(sample_rows) < 10:
            sample_rows.append(
                {
                    "patch_id": str(dict(patch).get("patch_id") or "").strip(),
                    "sequence_index": _safe_int(dict(patch).get("sequence_index")),
                    "overlay_write_mode": mode,
                    "overlay_fast_path_eligible": fast_path_eligible,
                    "overlay_fast_path_used": fast_path_used,
                    "overlay_fallback_reason": fallback_reason,
                    "candidate_count": _safe_int(dict(patch).get("candidate_count")),
                    "cumulative_candidate_count": _safe_int(dict(patch).get("cumulative_candidate_count")),
                }
            )
    return {
        "report_available": patch_count > 0,
        "patch_count": patch_count,
        "mode_counts": dict(mode_counts),
        "full_rebuild_count": full_rebuild_count,
        "incremental_partial_overlay_count": int(mode_counts.get("incremental_partial_overlay") or 0),
        "fast_path_eligible_count": fast_path_eligible_count,
        "fast_path_used_count": fast_path_used_count,
        "eligible_full_rebuild_fallback_count": eligible_full_rebuild_fallback_count,
        "eligible_full_rebuild_fallback_present": eligible_full_rebuild_fallback_count > 0,
        "fallback_reason_counts": dict(fallback_reason_counts),
        "samples": sample_rows,
    }


def _build_finalization_overlay_metrics(
    *,
    final_summary: dict[str, Any],
    board_visible_projection: dict[str, Any],
) -> dict[str, Any]:
    summary = dict(final_summary or {})
    candidate_source = dict(summary.get("candidate_source") or {})
    overlay = dict(summary.get("asset_population_overlay") or {})
    reuse_payload = dict(candidate_source.get("asset_population_overlay_reuse") or {})
    overlay_write_metadata = dict(
        overlay.get("overlay_write_metadata")
        or reuse_payload.get("overlay_write_metadata")
        or candidate_source.get("overlay_write_metadata")
        or {}
    )
    overlay_path = str(overlay.get("path") or candidate_source.get("asset_population_overlay_path") or "").strip()
    analysis_stage = str(summary.get("analysis_stage") or "").strip()
    summary_provider = str(summary.get("summary_provider") or "").strip()
    default_results_mode = str(summary.get("default_results_mode") or "").strip()
    report_available = bool(
        summary_provider
        or candidate_source
        or overlay
        or overlay_path
        or analysis_stage
        or default_results_mode
    )
    if not report_available:
        return {"report_available": False}

    reuse_used = bool(overlay.get("reuse")) or str(reuse_payload.get("status") or "").strip().lower() == "reused"
    overlay_present = bool(overlay_path or overlay)
    finalization_performed = bool(
        summary_provider == "asset_population_fast_path"
        and analysis_stage != "stage_1_preview"
        and not bool(summary.get("deterministic_preview"))
    )
    projection = dict(board_visible_projection or {})
    projection_present = bool(projection.get("projection_present") or projection.get("serving_projection_id"))
    serving_projection_phase = str(projection.get("serving_projection_phase") or "").strip()
    board_projection_serving = bool(
        projection_present
        and serving_projection_phase
        in {
            "current_snapshot_row_shell_overlay",
            "current_snapshot_serving",
            "current_serving",
            "partial_delta_overlay",
        }
    )
    raw_expected_candidate_count = max(
        _safe_int(candidate_source.get("candidate_count")),
        _safe_int(candidate_source.get("unfiltered_candidate_count")),
        _safe_int(overlay.get("candidate_count")),
    )
    served_candidate_count = _safe_int(projection.get("served_candidate_count"))
    projection_public_terminal = bool(
        projection_present
        and served_candidate_count > 0
        and serving_projection_phase
        in {
            "current_snapshot_row_shell_overlay",
            "current_snapshot_serving",
            "current_serving",
        }
    )
    expected_candidate_count = (
        served_candidate_count
        if projection_public_terminal
        else max(raw_expected_candidate_count, served_candidate_count)
    )
    served_complete = bool(expected_candidate_count <= 0 or served_candidate_count >= expected_candidate_count)
    delta_required = _safe_int(projection.get("delta_profile_required_count"))
    delta_fetched = _safe_int(projection.get("delta_profile_fetched_count"))
    delta_materialized = _safe_int(projection.get("delta_profile_materialized_count"))
    delta_board_visible = _safe_int(projection.get("delta_profile_board_visible_count"))
    delta_complete = bool(
        delta_required <= 0
        or (
            delta_fetched >= delta_required
            and delta_materialized >= delta_required
            and delta_board_visible >= delta_required
        )
    )
    patch_log_complete = not bool(projection.get("patch_log_replay_lag") or projection.get("patch_log_required_missing"))
    reuse_eligible = bool(
        finalization_performed
        and overlay_present
        and board_projection_serving
        and served_complete
        and delta_complete
        and patch_log_complete
    )
    full_rewrite = bool(overlay_present and not reuse_used)
    eligible_full_rewrite = bool(reuse_eligible and full_rewrite)
    mode = str(overlay_write_metadata.get("overlay_write_mode") or "").strip()
    if not mode:
        mode = "finalization_serving_projection_reuse" if reuse_used else "full_rebuild"
    return {
        "report_available": True,
        "summary_provider": summary_provider,
        "analysis_stage": analysis_stage,
        "default_results_mode": default_results_mode,
        "finalization_performed": finalization_performed,
        "overlay_present": overlay_present,
        "overlay_path_present": bool(overlay_path),
        "overlay_candidate_count": _safe_int(overlay.get("candidate_count")),
        "candidate_source_count": _safe_int(candidate_source.get("candidate_count")),
        "raw_expected_candidate_count": raw_expected_candidate_count,
        "public_expected_count_source": (
            "serving_projection_visible_members" if projection_public_terminal else "final_summary_candidate_source"
        ),
        "reuse_used": reuse_used,
        "reuse_reason": str(overlay.get("reuse_reason") or reuse_payload.get("reason") or "").strip(),
        "reuse_eligible": reuse_eligible,
        "full_rewrite_count": 1 if full_rewrite else 0,
        "reuse_count": 1 if reuse_used else 0,
        "eligible_full_rewrite_count": 1 if eligible_full_rewrite else 0,
        "eligible_full_rewrite_present": eligible_full_rewrite,
        "overlay_write_mode": mode,
        "overlay_write_metadata": overlay_write_metadata,
        "board_projection_serving": board_projection_serving,
        "served_complete": served_complete,
        "delta_complete": delta_complete,
        "patch_log_complete": patch_log_complete,
        "expected_candidate_count": expected_candidate_count,
        "served_candidate_count": served_candidate_count,
        "delta_profile_required_count": delta_required,
        "delta_profile_fetched_count": delta_fetched,
        "delta_profile_materialized_count": delta_materialized,
        "delta_profile_board_visible_count": delta_board_visible,
        "serving_projection_phase": serving_projection_phase,
    }


def _build_bottleneck_report(
    *,
    worker_timeline: dict[str, Any],
    local_apply_backlog: dict[str, Any],
    user_experience: dict[str, Any],
    board_visible_projection: dict[str, Any],
    serving_publication_gap: dict[str, Any],
    snapshot_full_materialization_queue: dict[str, Any],
    search_seed_discovery_queue: dict[str, Any],
    provider_search_retry_queue: dict[str, Any],
    remote_provider_events: dict[str, Any],
    target_candidate_public_web: dict[str, Any],
    legacy_public_web_retirement: dict[str, Any],
    company_public_web: dict[str, Any],
    recovery_phase_metrics: dict[str, Any],
    board_overlay_writes: dict[str, Any],
    finalization_overlay: dict[str, Any],
    legacy_materialization_write_contract: dict[str, Any],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    max_worker_duration = _safe_float(dict(worker_timeline.get("duration_ms") or {}).get("max"))
    if max_worker_duration >= _SLOW_WORKER_THRESHOLD_MS:
        candidates.append(
            {
                "kind": "slow_worker",
                "severity": "high" if max_worker_duration >= 120_000.0 else "medium",
                "duration_ms": round(max_worker_duration, 2),
                "recommendation": "Inspect slow_workers; split local apply/materialize from provider submit or move heavy work behind writer budget.",
            }
        )
    global_gap = _safe_float(
        dict(dict(worker_timeline.get("handoff_gap_ms") or {}).get("global_next_worker_start_gap_ms") or {}).get("max")
    )
    if global_gap >= _SLOW_HANDOFF_GAP_THRESHOLD_MS:
        candidates.append(
            {
                "kind": "worker_handoff_gap",
                "severity": "high" if global_gap >= 30_000.0 else "medium",
                "duration_ms": round(global_gap, 2),
                "recommendation": "Find the preceding worker in slow_gaps; next-submit or recovery wakeup may still be blocked by sync work.",
            }
        )
    if bool(local_apply_backlog.get("stale_applied_not_ingested_present")):
        candidates.append(
            {
                "kind": "local_apply_backlog",
                "severity": "high",
                "duration_ms": round(
                    _safe_float(dict(local_apply_backlog.get("age_ms") or {}).get("max")),
                    2,
                ),
                "recommendation": "Recover workers with inline_incremental_apply but no inline_incremental_ingest; provider output was locally applied but downstream prefetch/materialization did not close.",
            }
        )
    if bool(local_apply_backlog.get("closure_retry_backlog_present")):
        candidates.append(
            {
                "kind": "local_apply_closure_retry_backlog",
                "severity": "high",
                "count": int(local_apply_backlog.get("closure_ready_retry_count") or 0),
                "recommendation": "Drain retryable local_apply_closure items; provider output was applied, but callback-only closure has not converged.",
            }
        )
    if bool(local_apply_backlog.get("closure_stale_running_present")):
        candidates.append(
            {
                "kind": "local_apply_closure_stale_running",
                "severity": "high",
                "count": int(local_apply_backlog.get("closure_stale_running_count") or 0),
                "recommendation": "Recover expired local_apply_closure leases; local apply closure may be blocked after a process crash.",
            }
        )
    final_board = _safe_float(user_experience.get("final_results_to_board_nonempty_ms"))
    if final_board >= _BOARD_NONEMPTY_THRESHOLD_MS:
        candidates.append(
            {
                "kind": "board_readiness_lag",
                "severity": "high",
                "duration_ms": round(final_board, 2),
                "recommendation": "Expose baseline/result-view lifecycle earlier and avoid gating board readiness on full artifact finalization.",
            }
        )
    preview_final = _safe_float(
        user_experience.get(
            "finalization_lag_evaluation_ms",
            user_experience.get("stage_1_preview_to_final_results_ms"),
        )
    )
    if preview_final >= _LONG_FINALIZATION_THRESHOLD_MS:
        candidates.append(
            {
                "kind": "post_preview_finalization_lag",
                "severity": "medium",
                "duration_ms": round(preview_final, 2),
                "raw_duration_ms": round(
                    _safe_float(user_experience.get("stage_1_preview_to_final_results_ms")), 2
                ),
                "evaluation_source": str(
                    user_experience.get("finalization_lag_evaluation_source") or ""
                ).strip(),
                "recommendation": "Inspect materialization/retrieval/layering timing; publish intermediate serving state before finalization completes.",
            }
        )
    if bool(board_visible_projection.get("projection_missing_for_visible_count")):
        candidates.append(
            {
                "kind": "board_visible_projection_missing",
                "severity": "high",
                "duration_ms": 0.0,
                "recommendation": "Do not advance board-visible counts without a serving projection id or full current-snapshot serving proof.",
            }
        )
    if bool(board_visible_projection.get("materialization_lag_violation")):
        candidates.append(
            {
                "kind": "board_visible_materialization_lag",
                "severity": "medium",
                "duration_ms": 0.0,
                "recommendation": "Inspect provider fetch to board-visible apply path; fetched profile rows are not becoming consumable quickly enough.",
            }
        )
    if bool(serving_publication_gap.get("gap_present")):
        candidates.append(
            {
                "kind": "serving_publication_gap",
                "severity": "high" if bool(serving_publication_gap.get("stale_gap_present")) else "medium",
                "duration_ms": round(_safe_float(serving_publication_gap.get("age_ms")), 2),
                "recommendation": "Drain the event-time board-visible/current-snapshot publisher; public reads may report this gap but must not publish the result view.",
            }
        )
    if bool(snapshot_full_materialization_queue.get("retry_backlog_present")):
        candidates.append(
            {
                "kind": "snapshot_full_materialization_retry_backlog",
                "severity": "medium",
                "duration_ms": 0.0,
                "recommendation": "Drain or inspect retryable snapshot_full_materialization items; full artifacts/retrieval/index are not converging.",
            }
        )
    if bool(snapshot_full_materialization_queue.get("stale_running_present")):
        candidates.append(
            {
                "kind": "snapshot_full_materialization_stale_running",
                "severity": "high",
                "duration_ms": 0.0,
                "recommendation": "Recover expired snapshot_full_materialization leases; a crashed compaction worker may be blocking final convergence.",
            }
        )
    if bool(search_seed_discovery_queue.get("item_without_worker_owner_present")):
        candidates.append(
            {
                "kind": "search_seed_discovery_owner_missing",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(search_seed_discovery_queue.get("item_without_worker_owner_count") or 0),
                "recommendation": "Provider-owned search-seed discovery items must carry a worker/provider owner; do not fall back to summary scans.",
            }
        )
    if bool(search_seed_discovery_queue.get("discovery_worker_owner_gap_present")):
        candidates.append(
            {
                "kind": "search_seed_discovery_worker_owner_gap",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(search_seed_discovery_queue.get("discovery_worker_owner_gap_count") or 0),
                "recommendation": "Completed search-seed workers must have both search_seed_discovery_query and local_apply_closure durable owners before worker-summary scans are considered retired.",
            }
        )
    if bool(search_seed_discovery_queue.get("stale_provider_owned_present")):
        candidates.append(
            {
                "kind": "search_seed_discovery_stale_provider_owned",
                "severity": "medium",
                "duration_ms": 0.0,
                "count": int(search_seed_discovery_queue.get("stale_provider_owned_count") or 0),
                "recommendation": "Inspect provider-owned search-seed discovery items whose worker envelope has not completed within the service window.",
            }
        )
    if bool(search_seed_discovery_queue.get("retry_backlog_present")):
        candidates.append(
            {
                "kind": "search_seed_discovery_retry_backlog",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(search_seed_discovery_queue.get("ready_retry_count") or 0),
                "recommendation": "Drain ready search_seed_discovery_query retry items; provider retry state must stay on the discovery owner.",
            }
        )
    if bool(search_seed_discovery_queue.get("exhausted_without_provider_retry_present")):
        candidates.append(
            {
                "kind": "search_seed_discovery_exhausted_without_report",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(search_seed_discovery_queue.get("exhausted_without_provider_retry_count") or 0),
                "recommendation": "Write the linked terminal provider_search_retry report for exhausted discovery queries; do not infer exhaustion only from summaries.",
            }
        )
    if bool(provider_search_retry_queue.get("retry_backlog_present")):
        candidates.append(
            {
                "kind": "provider_search_retry_backlog",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(provider_search_retry_queue.get("ready_retry_count") or 0),
                "recommendation": "Drain retryable provider_search_retry items; search-seed discovery is blocked on provider retry work.",
            }
        )
    if bool(provider_search_retry_queue.get("stale_running_present")):
        candidates.append(
            {
                "kind": "provider_search_retry_stale_running",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(provider_search_retry_queue.get("stale_running_count") or 0),
                "recommendation": "Recover expired provider_search_retry leases; discovery retry ownership may be stuck after a process crash.",
            }
        )
    if bool(provider_search_retry_queue.get("terminal_failure_present")):
        candidates.append(
            {
                "kind": "provider_search_retry_terminal_failure",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(provider_search_retry_queue.get("terminal_failed_count") or 0),
                "recommendation": "Inspect terminal provider_search_retry items; these represent exhausted provider discovery retries, not normal zero-result lanes.",
            }
        )
    if bool(remote_provider_events.get("lag_violation")):
        candidates.append(
            {
                "kind": "remote_provider_event_lag",
                "severity": "medium",
                "duration_ms": round(
                    _safe_float(
                        dict(remote_provider_events.get("actionable_remote_to_local_event_lag_ms") or {}).get("max")
                    ),
                    2,
                ),
                "count": int(remote_provider_events.get("slow_lag_count") or 0),
                "recommendation": "Inspect webhook relay and watcher timing; provider terminal events arrived after the service wakeup window.",
            }
        )
    if bool(target_candidate_public_web.get("service_guardrail_violation_detected")):
        candidates.append(
            {
                "kind": "target_public_web_guardrail_violation",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(target_candidate_public_web.get("service_guardrail_violation_count") or 0),
                "recommendation": "Inspect target-candidate Public Web batches; terminal runs must not expose completed state before phase metrics and signal materialization converge.",
            }
        )
    if bool(legacy_public_web_retirement.get("legacy_rows_present")):
        candidates.append(
            {
                "kind": "legacy_public_web_rows_present",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(legacy_public_web_retirement.get("legacy_target_candidate_public_web_row_count") or 0),
                "recommendation": (
                    "Before W7e physical deletion, migrate or cold-backup remaining "
                    "target_candidate_public_web_* rows. Normal Public Web execution must remain CRM-owned."
                ),
            }
        )
    if bool(legacy_public_web_retirement.get("legacy_audit_limited")):
        candidates.append(
            {
                "kind": "legacy_public_web_retirement_audit_limited",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(legacy_public_web_retirement.get("row_limit") or 0),
                "recommendation": "Rerun legacy Public Web retirement audit with a larger row_limit before deletion/signoff.",
            }
        )
    if bool(company_public_web.get("service_guardrail_violation_detected")):
        candidates.append(
            {
                "kind": "company_public_web_guardrail_violation",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(company_public_web.get("service_guardrail_violation_count") or 0),
                "recommendation": "Inspect company-level Public Web runs; collector/provider outputs must stay model-safe and report terminal failures explicitly.",
            }
        )
    if bool(recovery_phase_metrics.get("failed_phase_present")):
        candidates.append(
            {
                "kind": "recovery_phase_failure",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(recovery_phase_metrics.get("failed_phase_count") or 0),
                "recommendation": "Inspect recovery_phase_metrics.failed_phases; recovery/callback/daemon phase failures must not be hidden behind eventual smoke timeouts.",
            }
        )
    if bool(recovery_phase_metrics.get("missing_phase_present")):
        candidates.append(
            {
                "kind": "recovery_phase_metrics_missing",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(recovery_phase_metrics.get("missing_phase_count") or 0),
                "recommendation": "Every worker recovery run must expose phase-level owner/timing evidence before manual signoff.",
            }
        )
    if bool(recovery_phase_metrics.get("slow_phase_present")):
        candidates.append(
            {
                "kind": "recovery_phase_slow",
                "severity": "high",
                "duration_ms": round(_safe_float(dict(recovery_phase_metrics.get("elapsed_ms") or {}).get("max")), 2),
                "count": int(recovery_phase_metrics.get("slow_phase_count") or 0),
                "recommendation": "Split or bound slow recovery phases; callback/recovery/service-loop work must stay small-step and observable.",
            }
        )
    cooperative_budget_yield_count = _safe_int(
        recovery_phase_metrics.get("cooperative_budget_yield_count")
    )
    slow_total_phase_count = int(recovery_phase_metrics.get("slow_total_phase_count") or 0)
    if bool(recovery_phase_metrics.get("slow_total_phase_present")) and (
        slow_total_phase_count > cooperative_budget_yield_count
    ):
        candidates.append(
            {
                "kind": "recovery_total_elapsed_slow",
                "severity": "medium",
                "duration_ms": round(
                    _safe_float(dict(recovery_phase_metrics.get("total_elapsed_ms") or {}).get("max")),
                    2,
                ),
                "count": slow_total_phase_count,
                "recommendation": (
                    "Recovery tick total elapsed exceeded the default optimization threshold, but no single "
                    "bounded phase necessarily violated the synchronous-work contract. Inspect pressure-case "
                    "phase composition before turning this into a hard handoff gate."
                ),
            }
        )
    if _safe_int(recovery_phase_metrics.get("recovery_tick_budget_exhausted_count")) > 0 and not bool(
        recovery_phase_metrics.get("cooperative_budget_yield_present")
    ):
        candidates.append(
            {
                "kind": "recovery_tick_budget_exhausted",
                "severity": "medium",
                "duration_ms": round(
                    _safe_float(dict(recovery_phase_metrics.get("total_elapsed_ms") or {}).get("max")),
                    2,
                ),
                "count": int(recovery_phase_metrics.get("recovery_tick_budget_exhausted_count") or 0),
                "recommendation": (
                    "Recovery yielded after its total tick budget. This is expected under pressure only if the "
                    "job-scoped daemon immediately schedules the next tick and durable queues continue draining."
                ),
            }
        )
    if _safe_int(recovery_phase_metrics.get("durable_work_handoff_yield_count")) > 0 and not bool(
        recovery_phase_metrics.get("cooperative_handoff_yield_present")
    ):
        candidates.append(
            {
                "kind": "recovery_durable_work_handoff_yield",
                "severity": "info",
                "duration_ms": round(
                    _safe_float(dict(recovery_phase_metrics.get("total_elapsed_ms") or {}).get("max")),
                    2,
                ),
                "count": int(recovery_phase_metrics.get("durable_work_handoff_yield_count") or 0),
                "recommendation": (
                    "Recovery intentionally yielded after recording terminal/provider work so heavy local apply, "
                    "board-visible publication, or full compaction can run in a later bounded daemon tick."
                ),
            }
        )
    if bool(recovery_phase_metrics.get("unexpected_enabled_phase_present")):
        candidates.append(
            {
                "kind": "recovery_unexpected_enabled_phase",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(recovery_phase_metrics.get("unexpected_enabled_phase_count") or 0),
                "recommendation": "Remote-event/job-scoped recovery must not run full compaction, unrelated discovery, Excel recovery, or housekeeping phases.",
            }
        )
    if bool(recovery_phase_metrics.get("legacy_bridge_used_present")):
        candidates.append(
            {
                "kind": "legacy_materialization_recovery_bridge",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(recovery_phase_metrics.get("legacy_bridge_used_count") or 0),
                "recommendation": (
                    "Recovery must drain typed workflow_commands through their registered owners. "
                    "Legacy job_materialization_items bridges may remain only as explicit migration "
                    "adapter evidence before deletion."
                ),
            }
        )
    if bool(board_overlay_writes.get("eligible_full_rebuild_fallback_present")):
        candidates.append(
            {
                "kind": "partial_overlay_fast_path_fallback",
                "severity": "medium",
                "duration_ms": 0.0,
                "count": int(board_overlay_writes.get("eligible_full_rebuild_fallback_count") or 0),
                "recommendation": (
                    "Partial board-visible overlay fast path was eligible but fell back to full rebuild. "
                    "Inspect overlay_fallback_reasons before pressure/manual handoff."
                ),
            }
        )
    if bool(finalization_overlay.get("eligible_full_rewrite_present")):
        candidates.append(
            {
                "kind": "finalization_overlay_reuse_missed",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(finalization_overlay.get("eligible_full_rewrite_count") or 0),
                "recommendation": (
                    "Final asset-population publication rewrote the full overlay even though the "
                    "canonical board-visible projection was already complete. Reuse the serving projection "
                    "and keep full compaction/layering in background queues."
                ),
            }
        )
    if _safe_int(legacy_materialization_write_contract.get("normal_path_write_count")) > 0:
        candidates.append(
            {
                "kind": "legacy_materialization_normal_write",
                "severity": "high",
                "duration_ms": 0.0,
                "count": int(legacy_materialization_write_contract.get("normal_path_write_count") or 0),
                "recommendation": (
                    "New normal-path work must be planned as reducer-owned workflow_commands. "
                    "job_materialization_items may remain only as explicit migration adapter/backfill evidence."
                ),
            }
        )
    duration_by_phase = dict(target_candidate_public_web.get("duration_by_phase_ms_max") or {})
    if duration_by_phase:
        slowest_phase = str(
            max(duration_by_phase, key=lambda key: _safe_float(duration_by_phase.get(key)))
        ).strip()
        slowest_duration = _safe_float(duration_by_phase.get(slowest_phase)) if slowest_phase else 0.0
        if slowest_phase and slowest_duration >= _SLOW_WORKER_THRESHOLD_MS:
            candidates.append(
                {
                    "kind": "target_public_web_slowest_phase",
                    "severity": "medium" if slowest_duration < 120_000.0 else "high",
                    "duration_ms": round(slowest_duration, 2),
                    "phase": slowest_phase,
                    "recommendation": "Target-candidate Public Web is spending most time in this phase; split provider wait, fetch, adjudication, and materialization timing if this remains the bottleneck.",
                }
            )
    severity_rank = {"high": 2, "medium": 1, "low": 0}
    candidates.sort(key=lambda item: (severity_rank.get(str(item.get("severity")), 0), _safe_float(item.get("duration_ms"))), reverse=True)
    return {
        "bottleneck_count": len(candidates),
        "top_bottlenecks": candidates[:10],
        "optimization_ready": bool(candidates),
    }


def _build_legacy_materialization_write_contract_metrics(
    *,
    materialization_items: list[dict[str, Any]],
) -> dict[str, Any]:
    item_kind_counts: Counter[str] = Counter()
    normal_kind_counts: Counter[str] = Counter()
    migration_kind_counts: Counter[str] = Counter()
    missing_contract_kind_counts: Counter[str] = Counter()
    normal_path_samples: list[dict[str, Any]] = []
    missing_contract_samples: list[dict[str, Any]] = []
    normal_path_write_count = 0
    migration_adapter_write_count = 0
    missing_contract_count = 0
    for item in materialization_items:
        item_payload = dict(item or {})
        item_kind = str(item_payload.get("item_kind") or "unknown").strip() or "unknown"
        item_kind_counts[item_kind] += 1
        metadata = dict(item_payload.get("metadata") or {})
        contract = dict(metadata.get("legacy_materialization_write_contract") or {})
        if not contract:
            missing_contract_count += 1
            missing_contract_kind_counts[item_kind] += 1
            if len(missing_contract_samples) < 10:
                missing_contract_samples.append(
                    {
                        "item_id": str(item_payload.get("item_id") or ""),
                        "job_id": str(item_payload.get("job_id") or ""),
                        "item_kind": item_kind,
                        "status": str(item_payload.get("status") or ""),
                    }
                )
            continue
        if bool(contract.get("migration_adapter")):
            migration_adapter_write_count += 1
            migration_kind_counts[item_kind] += 1
        if bool(contract.get("normal_path")):
            normal_path_write_count += 1
            normal_kind_counts[item_kind] += 1
            if len(normal_path_samples) < 10:
                normal_path_samples.append(
                    {
                        "item_id": str(item_payload.get("item_id") or ""),
                        "job_id": str(item_payload.get("job_id") or ""),
                        "item_kind": item_kind,
                        "status": str(item_payload.get("status") or ""),
                        "target_runtime_table": str(contract.get("target_runtime_table") or ""),
                        "retirement_phase": str(contract.get("retirement_phase") or ""),
                    }
                )
    return {
        "report_available": True,
        "item_count": len(materialization_items),
        "normal_path_write_count": normal_path_write_count,
        "migration_adapter_write_count": migration_adapter_write_count,
        "missing_contract_count": missing_contract_count,
        "normal_path_write_present": normal_path_write_count > 0,
        "missing_contract_present": missing_contract_count > 0,
        "item_kind_counts": {key: int(value) for key, value in sorted(item_kind_counts.items()) if key},
        "normal_path_kind_counts": {key: int(value) for key, value in sorted(normal_kind_counts.items()) if key},
        "migration_adapter_kind_counts": {
            key: int(value) for key, value in sorted(migration_kind_counts.items()) if key
        },
        "missing_contract_kind_counts": {
            key: int(value) for key, value in sorted(missing_contract_kind_counts.items()) if key
        },
        "normal_path_samples": normal_path_samples,
        "missing_contract_samples": missing_contract_samples,
    }


def _build_workflow_causality_contract_metrics(
    *,
    workflow_commands: list[dict[str, Any]],
) -> dict[str, Any]:
    command_type_counts: Counter[str] = Counter()
    owner_counts: Counter[str] = Counter()
    missing_envelope_counts: Counter[str] = Counter()
    incomplete_envelope_counts: Counter[str] = Counter()
    no_op_contract_counts: Counter[str] = Counter()
    missing_envelope_samples: list[dict[str, Any]] = []
    incomplete_envelope_samples: list[dict[str, Any]] = []
    no_op_contract_samples: list[dict[str, Any]] = []
    causality_group_counts: Counter[str] = Counter()
    migration_adapter_command_count = 0
    checked_command_count = 0
    required_fields = (
        "workflow_run_id",
        "stage_id",
        "command_type",
        "owner",
        "causal_group_id",
        "source_event_id",
        "source_event_type",
        "idempotency_key",
        "readiness_effect",
        "schema_version",
    )

    for command in workflow_commands:
        command_payload = dict(command or {})
        command_type = str(command_payload.get("command_type") or "unknown").strip() or "unknown"
        owner = str(command_payload.get("owner") or "unknown_owner").strip() or "unknown_owner"
        command_type_counts[command_type] += 1
        owner_counts[owner] += 1
        if _is_migration_adapter_workflow_command(command_payload):
            migration_adapter_command_count += 1
            continue
        checked_command_count += 1
        causality = _workflow_command_physical_causality(command_payload)
        if not str(causality.get("causal_group_id") or "").strip() and not str(
            causality.get("source_event_id") or ""
        ).strip():
            missing_envelope_counts[command_type] += 1
            if len(missing_envelope_samples) < 10:
                missing_envelope_samples.append(_workflow_command_contract_sample(command_payload))
            continue
        missing_fields = [
            field_name
            for field_name in required_fields
            if not str(causality.get(field_name) or "").strip()
        ]
        if (
            str(causality.get("workflow_run_id") or "").strip()
            and str(causality.get("workflow_run_id") or "").strip()
            != str(command_payload.get("workflow_run_id") or "").strip()
        ):
            missing_fields.append("workflow_run_id_matches_command")
        if (
            str(causality.get("command_type") or "").strip()
            and str(causality.get("command_type") or "").strip() != command_type
        ):
            missing_fields.append("command_type_matches_command")
        if str(causality.get("owner") or "").strip() and str(causality.get("owner") or "").strip() != owner:
            missing_fields.append("owner_matches_command")
        group_id = str(causality.get("causal_group_id") or "").strip()
        if group_id:
            causality_group_counts[group_id] += 1
        produced_counts = _safe_dict(causality.get("produced_entity_counts"))
        no_op_reason = str(causality.get("no_op_reason") or "").strip()
        if not produced_counts and not no_op_reason:
            missing_fields.append("produced_entity_counts_or_no_op_reason")
        if missing_fields:
            incomplete_envelope_counts[command_type] += 1
            if len(incomplete_envelope_samples) < 10:
                incomplete_envelope_samples.append(
                    {
                        **_workflow_command_contract_sample(command_payload),
                        "missing_or_invalid_fields": missing_fields,
                    }
                )
        total_produced_count = sum(_safe_int(value) for value in produced_counts.values())
        if produced_counts and total_produced_count <= 0 and not no_op_reason:
            no_op_contract_counts[command_type] += 1
            if len(no_op_contract_samples) < 10:
                no_op_contract_samples.append(_workflow_command_contract_sample(command_payload))

    missing_envelope_count = sum(missing_envelope_counts.values())
    incomplete_envelope_count = sum(incomplete_envelope_counts.values())
    no_op_contract_violation_count = sum(no_op_contract_counts.values())
    violation_count = missing_envelope_count + incomplete_envelope_count + no_op_contract_violation_count
    return {
        "report_available": bool(workflow_commands),
        "command_count": len(workflow_commands),
        "checked_command_count": checked_command_count,
        "migration_adapter_command_count": migration_adapter_command_count,
        "missing_envelope_count": int(missing_envelope_count),
        "incomplete_envelope_count": int(incomplete_envelope_count),
        "no_op_contract_violation_count": int(no_op_contract_violation_count),
        "violation_count": int(violation_count),
        "violation_detected": violation_count > 0,
        "command_type_counts": {key: int(value) for key, value in sorted(command_type_counts.items()) if key},
        "owner_counts": {key: int(value) for key, value in sorted(owner_counts.items()) if key},
        "missing_envelope_command_type_counts": {
            key: int(value) for key, value in sorted(missing_envelope_counts.items()) if key
        },
        "incomplete_envelope_command_type_counts": {
            key: int(value) for key, value in sorted(incomplete_envelope_counts.items()) if key
        },
        "no_op_contract_command_type_counts": {
            key: int(value) for key, value in sorted(no_op_contract_counts.items()) if key
        },
        "causal_group_count": len(causality_group_counts),
        "causal_group_reuse_count": sum(1 for value in causality_group_counts.values() if int(value) > 1),
        "missing_envelope_samples": missing_envelope_samples,
        "incomplete_envelope_samples": incomplete_envelope_samples,
        "no_op_contract_samples": no_op_contract_samples,
    }


def _is_migration_adapter_workflow_command(command: dict[str, Any]) -> bool:
    payload = dict(command.get("payload") or {})
    metadata = dict(payload.get("metadata") or {})
    legacy_item = dict(payload.get("legacy_materialization_item") or {})
    causality = dict(payload.get("causality") or {})
    return bool(
        payload.get("migration_adapter")
        or metadata.get("migration_adapter")
        or legacy_item
        or str(causality.get("migration_phase") or "").strip().startswith("legacy_")
    )


def _workflow_command_physical_causality(command: dict[str, Any]) -> dict[str, Any]:
    return {
        "workflow_run_id": str(command.get("workflow_run_id") or "").strip(),
        "operation_id": str(command.get("operation_id") or "").strip(),
        "stage_id": str(command.get("stage_id") or "").strip(),
        "command_type": str(command.get("command_type") or "").strip(),
        "owner": str(command.get("owner") or "").strip(),
        "causal_group_id": str(command.get("causal_group_id") or "").strip(),
        "parent_command_id": str(command.get("parent_command_id") or "").strip(),
        "source_event_id": str(command.get("source_event_id") or "").strip(),
        "source_event_type": str(command.get("source_event_type") or "").strip(),
        "idempotency_key": str(command.get("idempotency_key") or "").strip(),
        "input_artifact_refs": list(command.get("input_artifact_refs") or []),
        "output_artifact_refs": list(command.get("output_artifact_refs") or []),
        "produced_entity_counts": dict(command.get("produced_entity_counts") or {}),
        "no_op_reason": str(command.get("no_op_reason") or "").strip(),
        "readiness_effect": str(command.get("readiness_effect") or "").strip(),
        "downstream_command_ids": list(command.get("downstream_command_ids") or []),
        "schema_version": str(command.get("causality_schema_version") or "").strip(),
    }


def _workflow_command_contract_sample(command: dict[str, Any]) -> dict[str, Any]:
    return {
        "command_id": str(command.get("command_id") or ""),
        "workflow_run_id": str(command.get("workflow_run_id") or ""),
        "operation_id": str(command.get("operation_id") or ""),
        "command_type": str(command.get("command_type") or ""),
        "owner": str(command.get("owner") or ""),
        "status": str(command.get("status") or ""),
        "idempotency_key": str(command.get("idempotency_key") or ""),
    }


def _workflow_command_causality_missing_fields(
    command: dict[str, Any],
    *,
    expected_command_type: str,
    expected_owner: str,
) -> list[str]:
    causality = _workflow_command_physical_causality(command)
    missing_fields = [
        field_name
        for field_name in (
            "workflow_run_id",
            "stage_id",
            "command_type",
            "owner",
            "causal_group_id",
            "source_event_id",
            "source_event_type",
            "idempotency_key",
            "readiness_effect",
            "schema_version",
        )
        if not str(causality.get(field_name) or "").strip()
    ]
    if str(causality.get("command_type") or "").strip() != expected_command_type:
        missing_fields.append("command_type_matches_contract")
    if str(causality.get("owner") or "").strip() != expected_owner:
        missing_fields.append("owner_matches_contract")
    if not _safe_dict(causality.get("produced_entity_counts")) and not str(
        causality.get("no_op_reason") or ""
    ).strip():
        missing_fields.append("produced_entity_counts_or_no_op_reason")
    return missing_fields


def _workflow_command_control_policy_missing_fields(
    policy: dict[str, Any],
    *,
    expected_command_type: str,
    expected_owner: str,
) -> list[str]:
    missing_fields: list[str] = []
    required_text_fields = (
        "schema_version",
        "command_type",
        "owner",
        "generic_control_contract",
        "running_control_category",
        "running_control_maturity",
        "running_control_gap_status",
        "running_control_surface",
        "running_cancel_contract",
        "running_resume_contract",
        "control_source_of_truth",
        "agent_callable_surface",
        "fallback_status",
    )
    for field_name in required_text_fields:
        if not str(policy.get(field_name) or "").strip():
            missing_fields.append(field_name)
    if str(policy.get("command_type") or "").strip() != expected_command_type:
        missing_fields.append("command_type_matches_contract")
    if str(policy.get("owner") or "").strip() != expected_owner:
        missing_fields.append("owner_matches_contract")
    if str(policy.get("fallback_status") or "").strip() != "fail_closed":
        missing_fields.append("fallback_status_fail_closed")
    if (
        str(policy.get("control_source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_control_policy"
    ):
        missing_fields.append("control_source_of_truth")
    if str(policy.get("agent_callable_surface") or "").strip() != "workflow_command_control_api":
        missing_fields.append("agent_callable_surface")
    if str(policy.get("running_control_surface") or "").strip() != "workflow_command_control_api_only":
        missing_fields.append("running_control_surface")
    if str(policy.get("running_control_maturity") or "").strip() not in {
        "owner_specific_cancel_resume",
        "owner_specific_cancel_only",
        "owner_specific_resume_only",
        "fail_closed_with_upgrade_requirements",
    }:
        missing_fields.append("running_control_maturity")
    if str(policy.get("running_control_gap_status") or "").strip() not in {
        "closed",
        "partial_resume_gap_reported",
        "partial_cancel_gap_reported",
        "accepted_fail_closed_pending_owner_specific_control",
    }:
        missing_fields.append("running_control_gap_status")
    for field_name in ("generic_cancel_statuses", "generic_retry_statuses", "generic_resume_statuses"):
        if not _non_empty_text_list(policy.get(field_name)):
            missing_fields.append(field_name)
    categories = policy.get("running_control_categories")
    if not _non_empty_text_list(categories):
        missing_fields.append("running_control_categories")
    elif len(list(categories or [])) != 1:
        missing_fields.append("running_control_categories_exactly_one")
    if str(policy.get("running_control_category") or "").strip() not in {
        str(category or "").strip() for category in list(categories or [])
    }:
        missing_fields.append("running_control_category_matches_categories")
    missing_fields.extend(
        _running_command_control_policy_missing_fields(
            policy,
            action="cancel",
            unsupported_reason="running_command_requires_owner_specific_cancel",
            placeholder_reason="owner_specific_interrupt_not_implemented",
        )
    )
    missing_fields.extend(
        _running_command_control_policy_missing_fields(
            policy,
            action="resume",
            unsupported_reason="running_command_requires_owner_specific_resume",
            placeholder_reason="owner_specific_resume_not_implemented",
        )
    )
    return missing_fields


def _running_command_control_policy_missing_fields(
    policy: dict[str, Any],
    *,
    action: str,
    unsupported_reason: str,
    placeholder_reason: str,
) -> list[str]:
    missing_fields: list[str] = []
    supported_field = f"running_{action}_supported"
    statuses_field = f"running_{action}_statuses"
    owner_field = f"running_{action}_owner"
    delegate_field = f"running_{action}_delegate"
    blocked_reason_field = f"running_{action}_blocked_reason"
    upgrade_requirements_field = f"running_{action}_upgrade_requirements"
    unsupported_reason_field = f"unsupported_running_{action}_reason"
    if supported_field not in policy or not isinstance(policy.get(supported_field), bool):
        missing_fields.append(supported_field)
    if str(policy.get(unsupported_reason_field) or "").strip() != unsupported_reason:
        missing_fields.append(unsupported_reason_field)
    if bool(policy.get(supported_field)):
        if not _non_empty_text_list(policy.get(statuses_field)):
            missing_fields.append(statuses_field)
        if not str(policy.get(owner_field) or "").strip():
            missing_fields.append(owner_field)
        if not str(policy.get(delegate_field) or "").strip():
            missing_fields.append(delegate_field)
        return missing_fields

    blocked_reason = str(policy.get(blocked_reason_field) or "").strip()
    if not blocked_reason:
        missing_fields.append(blocked_reason_field)
    elif blocked_reason == placeholder_reason:
        missing_fields.append(f"{blocked_reason_field}_not_placeholder")
    if not _non_empty_text_list(policy.get(upgrade_requirements_field)):
        missing_fields.append(upgrade_requirements_field)
    return missing_fields


def _non_empty_text_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return any(str(item or "").strip() for item in value)


def _workflow_command_display_contract_missing_fields(
    display_contract: dict[str, Any],
    *,
    expected_command_type: str,
    expected_owner: str,
) -> list[str]:
    missing_fields: list[str] = []
    for field_name in (
        "schema_version",
        "command_type",
        "owner",
        "display_label",
        "display_category",
        "description",
        "source_of_truth",
        "fallback_status",
    ):
        if not str(display_contract.get(field_name) or "").strip():
            missing_fields.append(field_name)
    if str(display_contract.get("command_type") or "").strip() != expected_command_type:
        missing_fields.append("command_type_matches_contract")
    if str(display_contract.get("owner") or "").strip() != expected_owner:
        missing_fields.append("owner_matches_contract")
    if (
        str(display_contract.get("source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_display_contract"
    ):
        missing_fields.append("source_of_truth")
    if str(display_contract.get("fallback_status") or "").strip() != "fail_closed":
        missing_fields.append("fallback_status_fail_closed")
    return missing_fields


def _workflow_command_activity_spine_policy_missing_fields(
    activity_spine_policy: dict[str, Any],
    *,
    expected_command_type: str,
    expected_owner: str,
) -> list[str]:
    missing_fields: list[str] = []
    for field_name in (
        "schema_version",
        "command_type",
        "owner",
        "requirement",
        "activity_table",
        "attempt_table",
        "entity_delta_table",
        "source_of_truth",
        "agent_callable_surface",
        "fallback_status",
    ):
        if not str(activity_spine_policy.get(field_name) or "").strip():
            missing_fields.append(field_name)
    if str(activity_spine_policy.get("command_type") or "").strip() != expected_command_type:
        missing_fields.append("command_type_matches_contract")
    if str(activity_spine_policy.get("owner") or "").strip() != expected_owner:
        missing_fields.append("owner_matches_contract")
    if (
        str(activity_spine_policy.get("source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_activity_spine_policy"
    ):
        missing_fields.append("source_of_truth")
    if str(activity_spine_policy.get("agent_callable_surface") or "").strip() != (
        "operation_command_activity_api"
    ):
        missing_fields.append("agent_callable_surface")
    if str(activity_spine_policy.get("fallback_status") or "").strip() != "fail_closed":
        missing_fields.append("fallback_status_fail_closed")
    if str(activity_spine_policy.get("requirement") or "").strip() == "legacy_internal_pending_activity_spine":
        missing_fields.append("requirement_not_legacy_internal")
    for field_name in (
        "must_write_activity_run",
        "must_write_activity_attempt",
        "must_write_entity_delta",
        "downstream_activity_required",
        "agent_callable",
    ):
        if not isinstance(activity_spine_policy.get(field_name), bool):
            missing_fields.append(field_name)
    return missing_fields


def _build_durable_command_owner_contract_metrics(
    *,
    workflow_commands: list[dict[str, Any]],
) -> dict[str, Any]:
    owner_contracts: dict[str, dict[str, Any]] = {}
    total_command_count = 0
    total_invalid_owner_count = 0
    total_incomplete_causality_count = 0
    total_missing_causality_count = 0
    total_control_policy_violation_count = 0
    total_display_contract_violation_count = 0
    total_activity_spine_policy_violation_count = 0
    running_control_maturity_counts: Counter[str] = Counter()
    running_control_gap_status_counts: Counter[str] = Counter()
    for command_type, contract in _DURABLE_COMMAND_OWNER_CONTRACTS.items():
        expected_owner = str(contract.get("expected_owner") or "").strip()
        metric_key = str(contract.get("metric_key") or command_type).strip()
        commands = [
            dict(command)
            for command in list(workflow_commands or [])
            if str(dict(command).get("command_type") or "").strip() == command_type
        ]
        status_counts: Counter[str] = Counter()
        owner_counts: Counter[str] = Counter()
        incomplete_samples: list[dict[str, Any]] = []
        succeeded_count = 0
        terminal_count = 0
        failed_count = 0
        pending_count = 0
        invalid_owner_count = 0
        incomplete_causality_count = 0
        missing_causality_count = 0
        for command in commands:
            status = str(command.get("status") or "").strip() or "unknown"
            owner = str(command.get("owner") or "").strip() or "unknown_owner"
            status_counts[status] += 1
            owner_counts[owner] += 1
            if status == "succeeded":
                succeeded_count += 1
            if status in TERMINAL_COMMAND_STATUSES:
                terminal_count += 1
            else:
                pending_count += 1
            if status in {"failed", "failed_terminal", "cancelled", "canceled", "superseded"}:
                failed_count += 1
            if owner != expected_owner:
                invalid_owner_count += 1
            missing_fields = _workflow_command_causality_missing_fields(
                command,
                expected_command_type=command_type,
                expected_owner=expected_owner,
            )
            if missing_fields:
                incomplete_causality_count += 1
                if "causal_group_id" in missing_fields and "source_event_id" in missing_fields:
                    missing_causality_count += 1
                if len(incomplete_samples) < 5:
                    incomplete_samples.append(
                        {
                            **_workflow_command_contract_sample(command),
                            "missing_or_invalid_fields": missing_fields,
                        }
                    )
        total_command_count += len(commands)
        total_invalid_owner_count += invalid_owner_count
        total_incomplete_causality_count += incomplete_causality_count
        total_missing_causality_count += missing_causality_count
        control_policy = workflow_command_control_policy(
            command_type,
            owner=expected_owner,
        ).to_record()
        running_control_maturity = str(control_policy.get("running_control_maturity") or "unknown").strip() or "unknown"
        running_control_gap_status = str(control_policy.get("running_control_gap_status") or "unknown").strip() or "unknown"
        running_control_maturity_counts[running_control_maturity] += 1
        running_control_gap_status_counts[running_control_gap_status] += 1
        control_policy_missing_fields = _workflow_command_control_policy_missing_fields(
            control_policy,
            expected_command_type=command_type,
            expected_owner=expected_owner,
        )
        control_policy_violation_count = int(bool(commands and control_policy_missing_fields))
        total_control_policy_violation_count += control_policy_violation_count
        display_contract = workflow_command_display_contract(
            command_type,
            owner=expected_owner,
        ).to_record()
        display_contract_missing_fields = _workflow_command_display_contract_missing_fields(
            display_contract,
            expected_command_type=command_type,
            expected_owner=expected_owner,
        )
        display_contract_violation_count = int(bool(commands and display_contract_missing_fields))
        total_display_contract_violation_count += display_contract_violation_count
        activity_spine_policy = workflow_command_activity_spine_policy(
            command_type,
            owner=expected_owner,
        ).to_record()
        activity_spine_policy_missing_fields = _workflow_command_activity_spine_policy_missing_fields(
            activity_spine_policy,
            expected_command_type=command_type,
            expected_owner=expected_owner,
        )
        activity_spine_policy_violation_count = int(bool(commands and activity_spine_policy_missing_fields))
        total_activity_spine_policy_violation_count += activity_spine_policy_violation_count
        owner_contracts[metric_key] = {
            "command_type": command_type,
            "expected_owner": expected_owner,
            "control_policy": control_policy,
            "running_control_maturity": running_control_maturity,
            "running_control_gap_status": running_control_gap_status,
            "display_contract": display_contract,
            "activity_spine_policy": activity_spine_policy,
            "command_count": len(commands),
            "succeeded_count": succeeded_count,
            "terminal_count": terminal_count,
            "pending_count": pending_count,
            "failed_count": failed_count,
            "expected_owner_count": int(owner_counts.get(expected_owner, 0)),
            "invalid_owner_count": invalid_owner_count,
            "incomplete_causality_count": incomplete_causality_count,
            "missing_causality_count": missing_causality_count,
            "control_policy_violation_count": control_policy_violation_count,
            "control_policy_missing_or_invalid_fields": control_policy_missing_fields,
            "display_contract_violation_count": display_contract_violation_count,
            "display_contract_missing_or_invalid_fields": display_contract_missing_fields,
            "activity_spine_policy_violation_count": activity_spine_policy_violation_count,
            "activity_spine_policy_missing_or_invalid_fields": activity_spine_policy_missing_fields,
            "contract_present": bool(
                commands
                and invalid_owner_count <= 0
                and incomplete_causality_count <= 0
                and control_policy_violation_count <= 0
                and display_contract_violation_count <= 0
                and activity_spine_policy_violation_count <= 0
            ),
            "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
            "owner_counts": {key: int(value) for key, value in sorted(owner_counts.items()) if key},
            "incomplete_causality_samples": incomplete_samples,
        }
    return {
        "report_available": total_command_count > 0,
        "checked_command_type_count": len(_DURABLE_COMMAND_OWNER_CONTRACTS),
        "checked_command_count": total_command_count,
        "invalid_owner_count": total_invalid_owner_count,
        "incomplete_causality_count": total_incomplete_causality_count,
        "missing_causality_count": total_missing_causality_count,
        "control_policy_violation_count": total_control_policy_violation_count,
        "display_contract_violation_count": total_display_contract_violation_count,
        "activity_spine_policy_violation_count": total_activity_spine_policy_violation_count,
        "running_control_maturity_counts": {
            key: int(value) for key, value in sorted(running_control_maturity_counts.items()) if key
        },
        "running_control_gap_status_counts": {
            key: int(value) for key, value in sorted(running_control_gap_status_counts.items()) if key
        },
        "violation_detected": bool(
            total_invalid_owner_count
            or total_incomplete_causality_count
            or total_control_policy_violation_count
            or total_display_contract_violation_count
            or total_activity_spine_policy_violation_count
        ),
        "contracts": owner_contracts,
    }


def _build_target_candidate_public_web_metrics(
    *,
    batches: list[dict[str, Any]],
    workflow_commands: list[dict[str, Any]],
) -> dict[str, Any]:
    queue_batch_commands = [
        dict(command)
        for command in list(workflow_commands or [])
        if str(dict(command).get("command_type") or "").strip() == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE
    ]
    if not batches and not queue_batch_commands:
        return {"report_available": False}
    status_counts: Counter[str] = Counter()
    risk_reason_counts: Counter[str] = Counter()
    slowest_phase_counts: Counter[str] = Counter()
    storage_owner_counts: Counter[str] = Counter()
    execution_backend_counts: Counter[str] = Counter()
    queue_command_status_counts: Counter[str] = Counter()
    queue_command_owner_counts: Counter[str] = Counter()
    queue_command_incomplete_causality_samples: list[dict[str, Any]] = []
    numeric_totals: Counter[str] = Counter()
    duration_by_phase_ms_max: dict[str, float] = {}
    latest_batch: dict[str, Any] = {}
    latest_timestamp = ""
    service_guardrail_violation_count = 0
    queue_batch_command_succeeded_count = 0
    queue_batch_command_invalid_owner_count = 0
    queue_batch_command_incomplete_causality_count = 0
    queue_batch_command_missing_causality_count = 0
    for command in queue_batch_commands:
        status = str(command.get("status") or "").strip() or "unknown"
        owner = str(command.get("owner") or "").strip() or "unknown_owner"
        queue_command_status_counts[status] += 1
        queue_command_owner_counts[owner] += 1
        if status == "succeeded":
            queue_batch_command_succeeded_count += 1
        if owner != CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER:
            queue_batch_command_invalid_owner_count += 1
        missing_fields = _workflow_command_causality_missing_fields(
            command,
            expected_command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            expected_owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
        )
        if missing_fields:
            queue_batch_command_incomplete_causality_count += 1
            if "causal_group_id" in missing_fields and "source_event_id" in missing_fields:
                queue_batch_command_missing_causality_count += 1
            if len(queue_command_incomplete_causality_samples) < 5:
                queue_command_incomplete_causality_samples.append(
                    {
                        **_workflow_command_contract_sample(command),
                        "missing_or_invalid_fields": missing_fields,
                    }
                )
    for batch in batches:
        summary = dict(batch.get("summary") or {})
        metrics = dict(summary.get("phase_metrics") or batch.get("phase_metrics") or {})
        status = str(batch.get("status") or summary.get("status") or "").strip() or "unknown"
        status_counts[status] += 1
        storage_owner = str(
            batch.get("public_web_storage_owner")
            or summary.get("public_web_storage_owner")
            or dict(batch.get("metadata") or {}).get("owner")
            or summary.get("owner")
            or ""
        ).strip()
        if not storage_owner:
            storage_owner = "legacy_target_candidate_public_web_v1"
        storage_owner_counts[storage_owner] += 1
        execution_backend = str(
            batch.get("public_web_execution_backend")
            or batch.get("execution_backend")
            or summary.get("public_web_execution_backend")
            or summary.get("execution_backend")
            or dict(batch.get("metadata") or {}).get("execution_backend")
            or ""
        ).strip()
        if not execution_backend:
            execution_backend = "unknown"
        execution_backend_counts[execution_backend] += 1
        updated_at = str(batch.get("updated_at") or batch.get("created_at") or summary.get("updated_at") or "")
        if not latest_batch or updated_at >= latest_timestamp:
            latest_timestamp = updated_at
            latest_batch = {
                "batch_id": str(batch.get("batch_id") or batch.get("id") or ""),
                "status": status,
                "updated_at": updated_at,
                "public_web_storage_owner": storage_owner,
                "public_web_execution_backend": execution_backend,
                "phase_metrics": {
                    key: metrics.get(key)
                    for key in (
                        "slowest_phase",
                        "duration_by_phase_ms_max",
                        "remote_search_pending_run_count",
                        "unmaterialized_signal_gap_count",
                        "completed_without_materialized_signals_count",
                        "terminal_with_errors_count",
                        "partial_failure_count",
                        "runs_with_errors_count",
                        "runs_with_metric_errors_count",
                        "missing_phase_metric_count",
                        "provider_or_fetch_failure_count",
                        "local_processing_error_count",
                        "service_guardrail_violation_detected",
                    )
                    if key in metrics
                },
            }
        for key in (
            "run_count",
            "metric_run_count",
            "remote_search_pending_run_count",
            "unmaterialized_signal_gap_count",
            "completed_without_materialized_signals_count",
            "terminal_with_errors_count",
            "partial_failure_count",
            "runs_with_errors_count",
            "runs_with_metric_errors_count",
            "missing_phase_metric_count",
            "provider_or_fetch_failure_count",
            "local_processing_error_count",
        ):
            numeric_totals[key] += _safe_int(metrics.get(key) if key in metrics else summary.get(key))
        slowest_phase = str(metrics.get("slowest_phase") or "").strip()
        if slowest_phase:
            slowest_phase_counts[slowest_phase] += 1
        for phase, value in dict(metrics.get("duration_by_phase_ms_max") or {}).items():
            normalized_phase = str(phase or "").strip()
            if not normalized_phase:
                continue
            duration_by_phase_ms_max[normalized_phase] = max(
                _safe_float(value),
                duration_by_phase_ms_max.get(normalized_phase, 0.0),
            )
        for reason in list(metrics.get("phase_lag_risk_reasons") or []):
            normalized_reason = str(reason or "").strip()
            if normalized_reason:
                risk_reason_counts[normalized_reason] += 1
        if bool(metrics.get("service_guardrail_violation_detected")):
            service_guardrail_violation_count += 1
    return {
        "report_available": True,
        "batch_count": len(batches),
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "storage_owner_counts": {
            key: int(value) for key, value in sorted(storage_owner_counts.items()) if key
        },
        "execution_backend_counts": {
            key: int(value) for key, value in sorted(execution_backend_counts.items()) if key
        },
        "crm_storage_owner_batch_count": int(storage_owner_counts.get("crm_public_web_v1", 0)),
        "legacy_storage_owner_batch_count": int(
            sum(
                count
                for owner, count in storage_owner_counts.items()
                if owner and owner != "crm_public_web_v1"
            )
        ),
        "execution_backend_bridge_count": int(
            execution_backend_counts.get("target_candidate_public_web_v1", 0)
        ),
        "execution_backend_bridge_present": bool(
            execution_backend_counts.get("target_candidate_public_web_v1", 0) > 0
        ),
        "execution_backend_retired": not bool(execution_backend_counts.get("target_candidate_public_web_v1", 0) > 0),
        "queue_batch_command_count": len(queue_batch_commands),
        "queue_batch_command_succeeded_count": queue_batch_command_succeeded_count,
        "queue_batch_command_pending_count": sum(
            count
            for status, count in queue_command_status_counts.items()
            if status not in {"succeeded", "failed_terminal", "cancelled", "canceled", "superseded"}
        ),
        "queue_batch_command_failed_count": sum(
            count
            for status, count in queue_command_status_counts.items()
            if status in {"failed", "failed_terminal", "cancelled", "canceled", "superseded"}
        ),
        "queue_batch_command_missing": bool(batches and not queue_batch_commands),
        "queue_batch_command_contract_present": bool(
            queue_batch_commands
            and queue_batch_command_succeeded_count > 0
            and queue_batch_command_invalid_owner_count <= 0
            and queue_batch_command_incomplete_causality_count <= 0
        ),
        "queue_batch_command_status_counts": {
            key: int(value) for key, value in sorted(queue_command_status_counts.items()) if key
        },
        "queue_batch_command_owner_counts": {
            key: int(value) for key, value in sorted(queue_command_owner_counts.items()) if key
        },
        "queue_batch_command_expected_owner_count": int(
            queue_command_owner_counts.get(CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER, 0)
        ),
        "queue_batch_command_invalid_owner_count": queue_batch_command_invalid_owner_count,
        "queue_batch_command_incomplete_causality_count": queue_batch_command_incomplete_causality_count,
        "queue_batch_command_missing_causality_count": queue_batch_command_missing_causality_count,
        "queue_batch_command_incomplete_causality_samples": queue_command_incomplete_causality_samples,
        "risk_reason_counts": {key: int(value) for key, value in sorted(risk_reason_counts.items()) if key},
        "slowest_phase_counts": {key: int(value) for key, value in sorted(slowest_phase_counts.items()) if key},
        "duration_by_phase_ms_max": {key: round(value, 2) for key, value in sorted(duration_by_phase_ms_max.items())},
        "latest_batch": latest_batch,
        **{key: int(value) for key, value in sorted(numeric_totals.items())},
        "service_guardrail_violation_count": service_guardrail_violation_count,
        "service_guardrail_violation_detected": service_guardrail_violation_count > 0,
    }


def _build_legacy_public_web_retirement_metrics(
    *,
    audit: dict[str, Any],
) -> dict[str, Any]:
    if not audit:
        return {"report_available": False}
    summary = dict(audit.get("summary") or {})
    deletion_gate = dict(audit.get("deletion_gate") or {})
    blockers = [dict(item) for item in list(audit.get("deletion_blockers") or []) if isinstance(item, dict)]
    legacy_row_count = _safe_int(
        summary.get("legacy_target_candidate_public_web_row_count")
        if "legacy_target_candidate_public_web_row_count" in summary
        else deletion_gate.get("legacy_target_candidate_public_web_row_count")
    )
    legacy_limited = bool(
        summary.get("legacy_target_candidate_public_web_limited")
        or deletion_gate.get("legacy_audit_limited")
    )
    status = str(audit.get("status") or "").strip() or "unknown"
    return {
        "report_available": True,
        "contract_version": str(audit.get("contract_version") or "").strip(),
        "status": status,
        "read_only": bool(audit.get("read_only", True)),
        "deletion_allowed": bool(audit.get("deletion_allowed")),
        "legacy_rows_present": legacy_row_count > 0,
        "legacy_audit_limited": legacy_limited,
        "legacy_target_candidate_public_web_row_count": legacy_row_count,
        "legacy_target_candidate_public_web_batch_count": _safe_int(
            summary.get("legacy_target_candidate_public_web_batch_count")
        ),
        "legacy_target_candidate_public_web_run_count": _safe_int(
            summary.get("legacy_target_candidate_public_web_run_count")
        ),
        "legacy_target_candidate_public_web_promotion_count": _safe_int(
            summary.get("legacy_target_candidate_public_web_promotion_count")
        ),
        "crm_public_web_batch_count": _safe_int(summary.get("crm_public_web_batch_count")),
        "crm_public_web_run_count": _safe_int(summary.get("crm_public_web_run_count")),
        "crm_public_web_promotion_count": _safe_int(summary.get("crm_public_web_promotion_count")),
        "blocker_count": len(blockers),
        "blocker_names": [
            str(item.get("blocker") or "").strip()
            for item in blockers
            if str(item.get("blocker") or "").strip()
        ],
        "row_limit": _safe_int(audit.get("row_limit")),
        "deletion_gate": {
            "normal_path_owner": str(deletion_gate.get("normal_path_owner") or "crm_public_web_v1"),
            "legacy_owner": str(deletion_gate.get("legacy_owner") or "target_candidate_public_web_v1"),
            "requires_migration_or_cold_backup": bool(
                deletion_gate.get("requires_migration_or_cold_backup")
                or legacy_row_count > 0
                or legacy_limited
            ),
            "company_asset_overview_required_before_deletion": bool(
                deletion_gate.get("company_asset_overview_required_before_deletion")
            ),
        },
    }


def _build_recovery_phase_metrics(*, recovery_runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not recovery_runs:
        return {"report_available": False}
    run_count = len(recovery_runs)
    runs_with_phase_metrics = 0
    missing_phase_count = 0
    failed_phase_count = 0
    slow_phase_count = 0
    slow_total_phase_count = 0
    unexpected_enabled_phase_count = 0
    legacy_bridge_used_count = 0
    recovery_tick_budget_exhausted_count = 0
    durable_work_handoff_yield_count = 0
    budget_yield_next_tick_requested_count = 0
    cooperative_budget_yield_count = 0
    phase_elapsed_values: list[float] = []
    total_elapsed_values: list[float] = []
    phase_candidate_values: list[float] = []
    local_apply_candidate_per_second_values: list[float] = []
    status_counts: Counter[str] = Counter()
    owner_counts: Counter[str] = Counter()
    phase_elapsed_max: dict[str, float] = {}
    phase_candidate_count_max: dict[str, int] = {}
    missing_phases: list[dict[str, Any]] = []
    failed_phases: list[dict[str, Any]] = []
    slow_phases: list[dict[str, Any]] = []
    slow_total_phases: list[dict[str, Any]] = []
    unexpected_enabled_phases: list[dict[str, Any]] = []
    legacy_bridge_used_phases: list[dict[str, Any]] = []
    run_samples: list[dict[str, Any]] = []
    for run_index, run in enumerate(recovery_runs):
        phase_metrics = {
            str(key): dict(value)
            for key, value in dict(run.get("recovery_phase_metrics") or {}).items()
            if str(key).strip() and isinstance(value, dict)
        }
        phase_names = sorted(phase_metrics)
        if not phase_metrics:
            missing_phase_count += len(_RECOVERY_PHASE_REQUIRED_NAMES)
            missing_phases.append(
                {
                    "run_index": run_index,
                    "phase": "<all>",
                    "reason": "recovery_phase_metrics_missing",
                    "run_phase": str(run.get("phase") or ""),
                    "daemon_status": str(run.get("daemon_status") or ""),
                }
            )
            continue
        runs_with_phase_metrics += 1
        missing_for_run = [name for name in _RECOVERY_PHASE_REQUIRED_NAMES if name not in phase_metrics]
        missing_phase_count += len(missing_for_run)
        run_missing_phase_count = len(missing_for_run)
        run_failed_phase_count = 0
        run_slow_phase_count = 0
        run_unexpected_enabled_phase_count = 0
        run_legacy_bridge_used_count = 0
        run_budget_exhausted = False
        run_next_tick_requested = bool(run.get("next_tick_requested"))
        run_durable_work_handoff_yield = bool(run.get("durable_work_handoff_yield"))
        for phase_name in missing_for_run[:10]:
            if len(missing_phases) >= 20:
                break
            missing_phases.append(
                {
                    "run_index": run_index,
                    "phase": phase_name,
                    "reason": "required_phase_missing",
                    "run_phase": str(run.get("phase") or ""),
                    "daemon_status": str(run.get("daemon_status") or ""),
                }
            )
        for phase_name, phase_payload in phase_metrics.items():
            status = str(phase_payload.get("status") or "").strip().lower() or "unknown"
            owner = str(phase_payload.get("owner") or "").strip() or "unknown"
            elapsed_ms = _safe_float(phase_payload.get("elapsed_ms"))
            counts = dict(phase_payload.get("counts") or {})
            candidate_count = _safe_int(counts.get("candidate_count"))
            status_counts[status] += 1
            owner_counts[owner] += 1
            phase_elapsed_max[phase_name] = max(elapsed_ms, phase_elapsed_max.get(phase_name, 0.0))
            if bool(phase_payload.get("budget_exhausted")):
                run_budget_exhausted = True
            if phase_name == "total":
                run_durable_work_handoff_yield = run_durable_work_handoff_yield or bool(
                    phase_payload.get("durable_work_handoff_yield")
                ) or _safe_int(dict(phase_payload.get("counts") or {}).get("durable_work_handoff_yield_count")) > 0
            if candidate_count > 0:
                phase_candidate_values.append(float(candidate_count))
                phase_candidate_count_max[phase_name] = max(
                    candidate_count,
                    phase_candidate_count_max.get(phase_name, 0),
                )
                if phase_name in {
                    "local_apply_backlog",
                    "event_level_materialization_followup",
                    "profile_refill_event_level_materialization_followup",
                    "post_followup_event_level_materialization_followup",
                    "board_visible_apply",
                } and elapsed_ms > 0:
                    local_apply_candidate_per_second_values.append(candidate_count / (elapsed_ms / 1000.0))
            if phase_name == "total":
                total_elapsed_values.append(elapsed_ms)
            if status == "failed":
                failed_phase_count += 1
                run_failed_phase_count += 1
                if len(failed_phases) < 20:
                    failed_phases.append(
                        {
                            "run_index": run_index,
                            "phase": phase_name,
                            "status": status,
                            "reason": str(phase_payload.get("reason") or ""),
                            "elapsed_ms": round(elapsed_ms, 2),
                            "owner": owner,
                        }
                    )
            if phase_name == "total":
                threshold_ms = _RECOVERY_PHASE_TOTAL_THRESHOLD_MS
                run_next_tick_requested = run_next_tick_requested or bool(
                    phase_payload.get("next_tick_requested")
                )
                if elapsed_ms > threshold_ms:
                    slow_total_phase_count += 1
                    if len(slow_total_phases) < 20:
                        slow_total_phases.append(
                            {
                                "run_index": run_index,
                                "phase": phase_name,
                                "status": status,
                                "elapsed_ms": round(elapsed_ms, 2),
                                "threshold_ms": threshold_ms,
                                "owner": owner,
                            }
                        )
                continue
            phase_elapsed_values.append(elapsed_ms)
            threshold_ms = _RECOVERY_PHASE_DEFAULT_THRESHOLD_MS
            if elapsed_ms > threshold_ms:
                slow_phase_count += 1
                run_slow_phase_count += 1
                if len(slow_phases) < 20:
                    slow_phases.append(
                        {
                            "run_index": run_index,
                            "phase": phase_name,
                            "status": status,
                            "elapsed_ms": round(elapsed_ms, 2),
                            "threshold_ms": threshold_ms,
                            "owner": owner,
                        }
                    )
            if _recovery_phase_unexpectedly_enabled_for_job_scoped_event(run, phase_name, phase_payload):
                unexpected_enabled_phase_count += 1
                run_unexpected_enabled_phase_count += 1
                if len(unexpected_enabled_phases) < 20:
                    unexpected_enabled_phases.append(
                        {
                            "run_index": run_index,
                            "phase": phase_name,
                            "status": status,
                            "reason": str(phase_payload.get("reason") or ""),
                            "elapsed_ms": round(elapsed_ms, 2),
                            "owner": owner,
                        }
                    )
            if bool(phase_payload.get("legacy_bridge_used")):
                legacy_bridge_used_count += 1
                run_legacy_bridge_used_count += 1
                if len(legacy_bridge_used_phases) < 20:
                    legacy_bridge_used_phases.append(
                        {
                            "run_index": run_index,
                            "phase": phase_name,
                            "status": status,
                            "reason": str(phase_payload.get("reason") or ""),
                            "elapsed_ms": round(elapsed_ms, 2),
                            "owner": owner,
                        }
                    )
        if run_budget_exhausted:
            recovery_tick_budget_exhausted_count += 1
            if run_next_tick_requested:
                budget_yield_next_tick_requested_count += 1
            if (
                run_next_tick_requested
                and not run_missing_phase_count
                and not run_failed_phase_count
                and not run_slow_phase_count
                and not run_unexpected_enabled_phase_count
                and not run_legacy_bridge_used_count
            ):
                cooperative_budget_yield_count += 1
        if run_durable_work_handoff_yield:
            durable_work_handoff_yield_count += 1
        if len(run_samples) < 10:
            run_samples.append(
                {
                    "run_index": run_index,
                    "phase": str(run.get("phase") or ""),
                    "status": str(run.get("status") or ""),
                    "daemon_status": str(run.get("daemon_status") or ""),
                    "phase_count": len(phase_metrics),
                    "phase_names": phase_names[:32],
                    "durable_work_handoff_yield": run_durable_work_handoff_yield,
                    "budget_exhausted": run_budget_exhausted,
                    "next_tick_requested": run_next_tick_requested,
                }
            )
    recovery_contract_clean = not bool(
        missing_phase_count
        or failed_phase_count
        or slow_phase_count
        or unexpected_enabled_phase_count
        or legacy_bridge_used_count
        or recovery_tick_budget_exhausted_count
    )
    cooperative_handoff_yield_count = (
        durable_work_handoff_yield_count if recovery_contract_clean else 0
    )
    return {
        "report_available": True,
        "run_count": run_count,
        "runs_with_phase_metrics_count": runs_with_phase_metrics,
        "missing_phase_count": missing_phase_count,
        "failed_phase_count": failed_phase_count,
        "slow_phase_count": slow_phase_count,
        "slow_total_phase_count": slow_total_phase_count,
        "unexpected_enabled_phase_count": unexpected_enabled_phase_count,
        "legacy_bridge_used_count": legacy_bridge_used_count,
        "recovery_tick_budget_exhausted_count": recovery_tick_budget_exhausted_count,
        "budget_yield_next_tick_requested_count": budget_yield_next_tick_requested_count,
        "cooperative_budget_yield_count": cooperative_budget_yield_count,
        "budget_yield_contract": "cooperative_recovery_budget_yield",
        "budget_yield_manual_handoff_blocking": False,
        "durable_work_handoff_yield_count": durable_work_handoff_yield_count,
        "cooperative_handoff_yield_count": cooperative_handoff_yield_count,
        "handoff_yield_contract": "cooperative_scheduling_yield",
        "handoff_yield_manual_handoff_blocking": False,
        "missing_phase_present": missing_phase_count > 0,
        "failed_phase_present": failed_phase_count > 0,
        "slow_phase_present": slow_phase_count > 0,
        "slow_total_phase_present": slow_total_phase_count > 0,
        "unexpected_enabled_phase_present": unexpected_enabled_phase_count > 0,
        "legacy_bridge_used_present": legacy_bridge_used_count > 0,
        "recovery_tick_budget_exhausted_present": recovery_tick_budget_exhausted_count > 0,
        "cooperative_budget_yield_present": cooperative_budget_yield_count > 0,
        "budget_yield_attention_required": (
            recovery_tick_budget_exhausted_count > cooperative_budget_yield_count
        ),
        "budget_yield_next_tick_missing_count": max(
            0,
            recovery_tick_budget_exhausted_count - budget_yield_next_tick_requested_count,
        ),
        "durable_work_handoff_yield_present": durable_work_handoff_yield_count > 0,
        "cooperative_handoff_yield_present": cooperative_handoff_yield_count > 0,
        "handoff_yield_attention_required": (
            durable_work_handoff_yield_count > 0 and cooperative_handoff_yield_count <= 0
        ),
        "elapsed_ms": _stats(phase_elapsed_values),
        "total_elapsed_ms": _stats(total_elapsed_values),
        "phase_candidate_count": _stats(phase_candidate_values),
        "local_apply_candidate_per_second": _stats(local_apply_candidate_per_second_values),
        "phase_elapsed_ms_max": {key: round(value, 2) for key, value in sorted(phase_elapsed_max.items())},
        "phase_candidate_count_max": {key: int(value) for key, value in sorted(phase_candidate_count_max.items())},
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "owner_counts": {key: int(value) for key, value in sorted(owner_counts.items()) if key},
        "required_phase_names": list(_RECOVERY_PHASE_REQUIRED_NAMES),
        "missing_phases": missing_phases[:20],
        "failed_phases": failed_phases[:20],
        "slow_phases": slow_phases[:20],
        "slow_total_phases": slow_total_phases[:20],
        "unexpected_enabled_phases": unexpected_enabled_phases[:20],
        "legacy_bridge_used_phases": legacy_bridge_used_phases[:20],
        "samples": run_samples,
        "slo_violation_detected": bool(
            missing_phase_count
            or failed_phase_count
            or slow_phase_count
            or unexpected_enabled_phase_count
        ),
        "optimization_attention_detected": slow_total_phase_count > cooperative_budget_yield_count,
    }


def _recovery_phase_unexpectedly_enabled_for_job_scoped_event(
    run: dict[str, Any],
    phase_name: str,
    phase_payload: dict[str, Any],
) -> bool:
    normalized_run_phase = str(run.get("phase") or "").strip().lower()
    if normalized_run_phase not in {
        "poll_auto_recovery",
        "post_terminal",
        "target_public_web_action",
        "remote_provider_event",
        "job_scoped_recovery",
    }:
        return False
    if phase_name not in {
        "search_seed_discovery",
        "snapshot_full_materialization",
        "excel_intake_recovery",
        "post_recovery_housekeeping",
        "blocked_workflow_cleanup",
    }:
        return False
    if (
        phase_name == "snapshot_full_materialization"
        and normalized_run_phase == "post_terminal"
        and bool(run.get("allow_background_snapshot_full_materialization"))
    ):
        return False
    status = str(phase_payload.get("status") or "").strip().lower()
    if status in {"", "skipped"}:
        return False
    counts = dict(phase_payload.get("counts") or {})
    active_count = sum(_safe_int(value) for value in counts.values())
    if active_count > 0:
        return True
    return status not in {"completed", "idle", "noop", "no_op"}


def _build_company_public_web_metrics(*, runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        return {"report_available": False}
    status_counts: Counter[str] = Counter()
    collection_mode_counts: Counter[str] = Counter()
    collector_type_counts: Counter[str] = Counter()
    asset_count = 0
    collector_record_count = 0
    provider_result_count = 0
    failed_run_count = 0
    raw_assets_included_count = 0
    collector_manifest_missing_count = 0
    collector_source_count = 0
    collector_document_fetch_count = 0
    collector_document_fetch_failure_count = 0
    collector_fetch_duration_ms_max = 0.0
    service_guardrail_violation_count = 0
    latest_run: dict[str, Any] = {}
    latest_timestamp = ""
    for run in runs:
        summary = dict(run.get("summary") or {})
        metadata = dict(run.get("metadata") or {})
        artifact_paths = dict(metadata.get("artifact_paths") or {})
        status = str(run.get("status") or summary.get("status") or "").strip() or "unknown"
        status_counts[status] += 1
        if status == "failed":
            failed_run_count += 1
        updated_at = str(run.get("updated_at") or run.get("completed_at") or run.get("started_at") or "")
        if not latest_run or updated_at >= latest_timestamp:
            latest_timestamp = updated_at
            latest_run = {
                "run_id": str(run.get("run_id") or ""),
                "status": status,
                "phase": str(run.get("phase") or ""),
                "updated_at": updated_at,
                "summary": {
                    "asset_count": _safe_int(summary.get("asset_count")),
                    "collector_record_count": _safe_int(summary.get("collector_record_count")),
                    "provider_result_count": _safe_int(summary.get("provider_result_count")),
                    "raw_assets_included": bool(summary.get("raw_assets_included")),
                    "collection_mode_counts": dict(summary.get("collection_mode_counts") or {}),
                    "collector_type_counts": dict(summary.get("collector_type_counts") or {}),
                    "collector_source_count": _safe_int(summary.get("collector_source_count")),
                    "collector_document_fetch_count": _safe_int(summary.get("collector_document_fetch_count")),
                    "collector_document_fetch_failure_count": _safe_int(
                        summary.get("collector_document_fetch_failure_count")
                    ),
                    "collector_fetch_duration_ms_max": _safe_float(summary.get("collector_fetch_duration_ms_max")),
                },
            }
        asset_count += _safe_int(summary.get("asset_count"))
        collector_record_count += _safe_int(summary.get("collector_record_count"))
        provider_result_count += _safe_int(summary.get("provider_result_count"))
        collector_source_count += _safe_int(summary.get("collector_source_count"))
        collector_document_fetch_count += _safe_int(summary.get("collector_document_fetch_count"))
        collector_document_fetch_failure_count += _safe_int(summary.get("collector_document_fetch_failure_count"))
        collector_fetch_duration_ms_max = max(
            collector_fetch_duration_ms_max,
            _safe_float(summary.get("collector_fetch_duration_ms_max")),
        )
        if bool(summary.get("raw_assets_included")):
            raw_assets_included_count += 1
        for mode, count in dict(summary.get("collection_mode_counts") or {}).items():
            normalized_mode = str(mode or "").strip()
            if normalized_mode:
                collection_mode_counts[normalized_mode] += _safe_int(count)
        for collector_type, count in dict(summary.get("collector_type_counts") or {}).items():
            normalized_type = str(collector_type or "").strip()
            if normalized_type:
                collector_type_counts[normalized_type] += _safe_int(count)
        run_collector_manifest_missing = False
        if _safe_int(dict(summary.get("collection_mode_counts") or {}).get("collector_bundle")) > 0:
            if not str(artifact_paths.get("collector_manifest") or "").strip():
                collector_manifest_missing_count += 1
                run_collector_manifest_missing = True
        if (
            status == "failed"
            or bool(summary.get("raw_assets_included"))
            or run_collector_manifest_missing
            or _safe_int(summary.get("collector_document_fetch_failure_count")) > 0
        ):
            service_guardrail_violation_count += 1
    return {
        "report_available": True,
        "run_count": len(runs),
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "collection_mode_counts": {key: int(value) for key, value in sorted(collection_mode_counts.items()) if key},
        "collector_type_counts": {key: int(value) for key, value in sorted(collector_type_counts.items()) if key},
        "asset_count": asset_count,
        "collector_record_count": collector_record_count,
        "provider_result_count": provider_result_count,
        "collector_source_count": collector_source_count,
        "collector_document_fetch_count": collector_document_fetch_count,
        "collector_document_fetch_failure_count": collector_document_fetch_failure_count,
        "collector_fetch_duration_ms_max": round(collector_fetch_duration_ms_max, 2),
        "failed_run_count": failed_run_count,
        "raw_assets_included_count": raw_assets_included_count,
        "collector_manifest_missing_count": collector_manifest_missing_count,
        "latest_run": latest_run,
        "service_guardrail_violation_count": service_guardrail_violation_count,
        "service_guardrail_violation_detected": service_guardrail_violation_count > 0,
    }


def _build_remote_provider_event_metrics(
    *,
    job_events: list[dict[str, Any]],
) -> dict[str, Any]:
    remote_events = [
        dict(event)
        for event in list(job_events or [])
        if str(dict(event).get("stage") or "").strip() == "remote_provider_event"
    ]
    status_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    target_worker_count_values: list[float] = []
    lag_values: list[float] = []
    actionable_lag_values: list[float] = []
    late_duplicate_lag_values: list[float] = []
    received_count = 0
    late_duplicate_count = 0
    in_flight_duplicate_count = 0
    slow_lag_count = 0
    late_duplicate_slow_lag_count = 0
    samples: list[dict[str, Any]] = []
    seen_terminal_event_keys: set[tuple[str, str, tuple[int, ...]]] = set()
    inferred_received_duplicate_count = 0
    for event in sorted(
        remote_events,
        key=lambda item: (str(dict(item).get("created_at") or ""), _safe_int(dict(item).get("event_id"))),
    ):
        status = str(event.get("status") or "").strip().lower()
        payload = _event_payload(event)
        event_metrics = dict(payload.get("event_metrics") or {})
        source = str(event_metrics.get("source") or payload.get("source") or "").strip() or "unknown"
        target_worker_ids = [
            _safe_int(worker_id)
            for worker_id in list(payload.get("target_worker_ids") or [])
            if _safe_int(worker_id) > 0
        ]
        lag_metric_present = "remote_to_local_event_lag_ms" in event_metrics
        lag_ms = _safe_float(event_metrics.get("remote_to_local_event_lag_ms"))
        provider_event = dict(payload.get("event") or {})
        event_key = (
            str(provider_event.get("run_id") or "").strip(),
            str(provider_event.get("dataset_id") or "").strip(),
            tuple(sorted(target_worker_ids)),
        )
        has_event_identity = bool(event_key[0] or event_key[1]) and bool(event_key[2])
        if status == "received" and has_event_identity and event_key in seen_terminal_event_keys:
            status = "received_late"
            inferred_received_duplicate_count += 1
        status_counts[status] += 1
        source_counts[source] += 1
        if target_worker_ids:
            target_worker_count_values.append(float(len(target_worker_ids)))
        if lag_metric_present and lag_ms >= 0.0:
            lag_values.append(lag_ms)
            if status == "received":
                actionable_lag_values.append(lag_ms)
                if lag_ms >= _REMOTE_PROVIDER_EVENT_LAG_THRESHOLD_MS:
                    slow_lag_count += 1
            elif status == "received_late":
                late_duplicate_lag_values.append(lag_ms)
                if lag_ms >= _REMOTE_PROVIDER_EVENT_LAG_THRESHOLD_MS:
                    late_duplicate_slow_lag_count += 1
        if status == "received_late":
            late_duplicate_count += 1
        elif status == "received_in_flight":
            in_flight_duplicate_count += 1
        elif status == "received":
            received_count += 1
            if has_event_identity:
                seen_terminal_event_keys.add(event_key)
        if len(samples) < 10:
            samples.append(
                {
                    "status": status,
                    "source": source,
                    "remote_to_local_event_lag_ms": round(lag_ms, 2),
                    "run_id": str(provider_event.get("run_id") or "").strip(),
                    "dataset_id": str(provider_event.get("dataset_id") or "").strip(),
                    "target_worker_ids": target_worker_ids,
                    "created_at": str(event.get("created_at") or ""),
                }
            )
    duplicate_event_count = late_duplicate_count + in_flight_duplicate_count
    return {
        "report_available": bool(remote_events),
        "event_count": len(remote_events),
        "received_count": received_count,
        "late_duplicate_count": late_duplicate_count,
        "in_flight_duplicate_count": in_flight_duplicate_count,
        "inferred_received_duplicate_count": inferred_received_duplicate_count,
        "duplicate_event_count": duplicate_event_count,
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "source_counts": {key: int(value) for key, value in sorted(source_counts.items()) if key},
        "target_worker_count": _stats(target_worker_count_values),
        "remote_to_local_event_lag_ms": _stats(lag_values),
        "actionable_remote_to_local_event_lag_ms": _stats(actionable_lag_values),
        "late_duplicate_remote_to_local_event_lag_ms": _stats(late_duplicate_lag_values),
        "lag_threshold_ms": _REMOTE_PROVIDER_EVENT_LAG_THRESHOLD_MS,
        "slow_lag_count": slow_lag_count,
        "late_duplicate_slow_lag_count": late_duplicate_slow_lag_count,
        "lag_violation": slow_lag_count > 0,
        "samples": samples,
    }


def _remote_provider_completion_by_worker(job_events: list[dict[str, Any]]) -> dict[int, str]:
    completions: dict[int, str] = {}
    for event in list(job_events or []):
        if str(dict(event).get("stage") or "").strip() != "remote_provider_event":
            continue
        payload = _event_payload(event)
        event_metrics = dict(payload.get("event_metrics") or {})
        remote_completed_at = str(event_metrics.get("remote_completed_at") or "").strip()
        if not remote_completed_at:
            continue
        for worker_id in [
            _safe_int(worker_id)
            for worker_id in list(payload.get("target_worker_ids") or [])
            if _safe_int(worker_id) > 0
        ]:
            existing = completions.get(worker_id)
            if not existing or remote_completed_at < existing:
                completions[worker_id] = remote_completed_at
    return completions


def _build_snapshot_full_materialization_queue_metrics(
    *,
    materialization_items: list[dict[str, Any]],
    workflow_commands: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    snapshot_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "snapshot_full_materialization"
    ]
    snapshot_commands = [
        dict(command)
        for command in list(workflow_commands or [])
        if str(dict(command).get("command_type") or "").strip() == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE
    ]
    status_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    command_status_counts: Counter[str] = Counter()
    retryable_count = 0
    terminal_failed_count = 0
    running_count = 0
    queued_count = 0
    waiting_prerequisite_count = 0
    completed_count = 0
    stale_running_count = 0
    now = datetime.now(timezone.utc)
    ready_retry_count = 0
    latest_updated_at = ""
    last_error_samples: list[str] = []
    for item in snapshot_items:
        status = str(item.get("status") or "").strip().lower()
        phase = str(item.get("phase") or "").strip().lower()
        status_counts[status] += 1
        phase_counts[phase] += 1
        latest_updated_at = max(latest_updated_at, str(item.get("updated_at") or ""))
        if status in _JOB_MATERIALIZATION_QUEUED_STATUSES:
            queued_count += 1
        if status == "waiting_prerequisite":
            waiting_prerequisite_count += 1
        if status in _JOB_MATERIALIZATION_RUNNING_STATUSES:
            running_count += 1
            lease_expires_at = _parse_timestamp(str(item.get("lease_expires_at") or ""))
            if lease_expires_at is not None and lease_expires_at <= now:
                stale_running_count += 1
        if status == "failed_retryable":
            retryable_count += 1
            not_before_at = _parse_timestamp(str(item.get("not_before_at") or ""))
            if not_before_at is None or not_before_at <= now:
                ready_retry_count += 1
        if status == "failed":
            terminal_failed_count += 1
        if status == "completed":
            completed_count += 1
        last_error = str(item.get("last_error") or "").strip()
        if last_error and len(last_error_samples) < 5:
            last_error_samples.append(last_error)
    command_queued_count = 0
    command_running_count = 0
    command_retryable_count = 0
    command_ready_retry_count = 0
    command_completed_count = 0
    command_terminal_failed_count = 0
    command_waiting_prerequisite_count = 0
    command_stale_running_count = 0
    for command in snapshot_commands:
        status = str(command.get("status") or "").strip().lower()
        result = dict(command.get("result") or {})
        command_status_counts[status] += 1
        latest_updated_at = max(latest_updated_at, str(command.get("updated_at") or ""))
        if status == "queued":
            command_queued_count += 1
        if status == "retry_wait":
            command_retryable_count += 1
            not_before_at = _parse_timestamp(str(command.get("not_before_at") or ""))
            if not_before_at is None or not_before_at <= now:
                command_ready_retry_count += 1
            if str(result.get("status") or "").strip().lower() == "waiting_prerequisite":
                command_waiting_prerequisite_count += 1
        if status in {"claimed", "running"}:
            command_running_count += 1
            lease_expires_at = _parse_timestamp(str(command.get("lease_expires_at") or ""))
            if lease_expires_at is not None and lease_expires_at <= now:
                command_stale_running_count += 1
        if status == "succeeded":
            command_completed_count += 1
        if status == "failed_terminal":
            command_terminal_failed_count += 1
        last_error = str(command.get("last_error") or "").strip()
        if last_error and len(last_error_samples) < 5:
            last_error_samples.append(last_error)
    queued_count += command_queued_count + command_retryable_count
    running_count += command_running_count
    retryable_count += command_retryable_count
    ready_retry_count += command_ready_retry_count
    completed_count += command_completed_count
    terminal_failed_count += command_terminal_failed_count
    waiting_prerequisite_count += command_waiting_prerequisite_count
    stale_running_count += command_stale_running_count
    backlog_count = queued_count + running_count + retryable_count
    unhealthy_count = ready_retry_count + stale_running_count + terminal_failed_count
    background_maintenance_pending = backlog_count > 0 and unhealthy_count <= 0
    return {
        "report_available": bool(snapshot_items or snapshot_commands),
        "owner": "snapshot_materialization_owner",
        "command_type": SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
        "scope": "background_snapshot_compaction",
        "readiness_effect": "background_artifact_compaction",
        "manual_handoff_blocking": False,
        "background_maintenance_pending": background_maintenance_pending,
        "healthy_background_backlog": background_maintenance_pending,
        "unhealthy_count": unhealthy_count,
        "item_count": len(snapshot_items),
        "command_count": len(snapshot_commands),
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "phase_counts": {key: int(value) for key, value in sorted(phase_counts.items()) if key},
        "command_status_counts": {
            key: int(value) for key, value in sorted(command_status_counts.items()) if key
        },
        "queued_count": queued_count,
        "waiting_prerequisite_count": waiting_prerequisite_count,
        "running_count": running_count,
        "completed_count": completed_count,
        "retryable_count": retryable_count,
        "ready_retry_count": ready_retry_count,
        "terminal_failed_count": terminal_failed_count,
        "backlog_count": backlog_count,
        "retry_backlog_present": retryable_count > 0,
        "stale_running_count": stale_running_count,
        "stale_running_present": stale_running_count > 0,
        "latest_updated_at": latest_updated_at,
        "last_error_samples": last_error_samples,
    }


def _build_provider_search_retry_queue_metrics(
    *,
    materialization_items: list[dict[str, Any]],
) -> dict[str, Any]:
    retry_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "provider_search_retry"
    ]
    status_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    retry_type_counts: Counter[str] = Counter()
    provider_counts: Counter[str] = Counter()
    queued_count = 0
    running_count = 0
    completed_count = 0
    retryable_count = 0
    ready_retry_count = 0
    terminal_failed_count = 0
    stale_running_count = 0
    latest_updated_at = ""
    last_error_samples: list[str] = []
    query_samples: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for item in retry_items:
        status = str(item.get("status") or "").strip().lower()
        phase = str(item.get("phase") or "").strip().lower()
        metadata = dict(item.get("metadata") or {})
        retry_item = dict(metadata.get("provider_retry_item") or {})
        provider_retry_type = str(
            retry_item.get("provider_retry_type") or item.get("reason") or ""
        ).strip()
        provider = str(retry_item.get("provider") or "").strip()
        status_counts[status] += 1
        phase_counts[phase] += 1
        if provider_retry_type:
            retry_type_counts[provider_retry_type] += 1
        if provider:
            provider_counts[provider] += 1
        latest_updated_at = max(latest_updated_at, str(item.get("updated_at") or ""))
        if status in _JOB_MATERIALIZATION_QUEUED_STATUSES:
            queued_count += 1
        if status in _JOB_MATERIALIZATION_RUNNING_STATUSES:
            running_count += 1
            lease_expires_at = _parse_timestamp(str(item.get("lease_expires_at") or ""))
            if lease_expires_at is not None and lease_expires_at <= now:
                stale_running_count += 1
        if status == "failed_retryable":
            retryable_count += 1
            not_before_at = _parse_timestamp(str(item.get("not_before_at") or ""))
            if not_before_at is None or not_before_at <= now:
                ready_retry_count += 1
        if status == "failed":
            terminal_failed_count += 1
        if status == "completed":
            completed_count += 1
        last_error = str(item.get("last_error") or "").strip()
        if last_error and len(last_error_samples) < 5:
            last_error_samples.append(last_error)
        if len(query_samples) < 10:
            query_samples.append(
                {
                    "item_id": str(item.get("item_id") or ""),
                    "status": status,
                    "phase": phase,
                    "provider_retry_type": provider_retry_type,
                    "provider": provider,
                    "query": str(retry_item.get("query") or ""),
                    "employment_status": str(retry_item.get("employment_status") or ""),
                    "last_error": last_error,
                }
            )
    backlog_count = queued_count + running_count + retryable_count
    return {
        "report_available": bool(retry_items),
        "item_count": len(retry_items),
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "phase_counts": {key: int(value) for key, value in sorted(phase_counts.items()) if key},
        "provider_retry_type_counts": {
            key: int(value) for key, value in sorted(retry_type_counts.items()) if key
        },
        "provider_counts": {key: int(value) for key, value in sorted(provider_counts.items()) if key},
        "queued_count": queued_count,
        "running_count": running_count,
        "completed_count": completed_count,
        "retryable_count": retryable_count,
        "ready_retry_count": ready_retry_count,
        "terminal_failed_count": terminal_failed_count,
        "backlog_count": backlog_count,
        "retry_backlog_present": ready_retry_count > 0,
        "stale_running_count": stale_running_count,
        "stale_running_present": stale_running_count > 0,
        "terminal_failure_present": terminal_failed_count > 0,
        "latest_updated_at": latest_updated_at,
        "last_error_samples": last_error_samples,
        "query_samples": query_samples,
    }


def _build_provider_anomaly_metrics(
    *,
    materialization_items: list[dict[str, Any]],
    workflow_commands: list[dict[str, Any]] | None = None,
    provider_anomaly_query_summaries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    query_summaries = _provider_anomaly_query_summaries(
        materialization_items,
        workflow_commands=list(workflow_commands or []),
        provider_anomaly_query_summaries=list(provider_anomaly_query_summaries or []),
    )
    provider_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    degraded_reason_counts: Counter[str] = Counter()
    incomplete_reason_counts: Counter[str] = Counter()
    zero_result_retry_count = 0
    zero_result_retry_exhausted_count = 0
    zero_result_accepted_count = 0
    empty_scale_count = 0
    probe_total_drift_count = 0
    chunked_scale_fallback_count = 0
    empty_page_range_count = 0
    single_page_retry_count = 0
    samples: list[dict[str, Any]] = []
    for summary in query_summaries:
        provider = str(summary.get("mode") or summary.get("provider") or summary.get("account_id") or "").strip()
        status = str(summary.get("status") or "completed").strip().lower()
        degraded_reason = str(summary.get("degraded_reason") or "").strip()
        incomplete_reason = str(summary.get("incomplete_reason") or "").strip()
        if provider:
            provider_counts[provider] += 1
        if status:
            status_counts[status] += 1
        if degraded_reason:
            degraded_reason_counts[degraded_reason] += 1
        if incomplete_reason:
            incomplete_reason_counts[incomplete_reason] += 1
        zero_retry = dict(summary.get("zero_result_retry") or {})
        retry_count = _zero_result_retry_count(zero_retry)
        zero_result_retry_count += retry_count
        if _zero_result_retry_exhausted(zero_retry) or incomplete_reason == "provider_zero_results_after_retry":
            zero_result_retry_exhausted_count += 1
        if bool(summary.get("zero_result_accepted")):
            zero_result_accepted_count += 1
        if str(summary.get("fallback_reason") or "") == "scaled_harvest_profile_search_returned_no_rows":
            empty_scale_count += 1
        if degraded_reason == "provider_reported_variable_or_unreliable_page_coverage_after_probe":
            probe_total_drift_count += 1
        chunked = dict(summary.get("chunked_scale_fallback") or {})
        if chunked:
            chunked_scale_fallback_count += 1
            empty_page_range_count += len([item for item in list(chunked.get("empty_page_ranges") or []) if isinstance(item, dict)])
            single_page_retry_count += _safe_int(chunked.get("single_page_retry_count"))
        if len(samples) < 10 and (
            retry_count
            or bool(summary.get("zero_result_accepted"))
            or degraded_reason
            or incomplete_reason
            or chunked
            or str(summary.get("fallback_reason") or "")
        ):
            samples.append(
                {
                    "query": str(summary.get("query") or ""),
                    "provider": provider,
                    "status": status,
                    "employment_status": str(summary.get("employment_status") or summary.get("employment_scope") or ""),
                    "zero_result_retry_count": retry_count,
                    "zero_result_accepted": bool(summary.get("zero_result_accepted")),
                    "fallback_reason": str(summary.get("fallback_reason") or ""),
                    "degraded_reason": degraded_reason,
                    "incomplete_reason": incomplete_reason,
                    "empty_page_range_count": len(
                        [item for item in list(chunked.get("empty_page_ranges") or []) if isinstance(item, dict)]
                    ),
                    "single_page_retry_count": _safe_int(chunked.get("single_page_retry_count")),
                }
            )
    anomaly_count = (
        zero_result_retry_count
        + zero_result_retry_exhausted_count
        + zero_result_accepted_count
        + empty_scale_count
        + probe_total_drift_count
        + empty_page_range_count
        + single_page_retry_count
    )
    return {
        "report_available": bool(query_summaries),
        "query_summary_count": len(query_summaries),
        "anomaly_count": anomaly_count,
        "zero_result_retry_count": zero_result_retry_count,
        "zero_result_retry_exhausted_count": zero_result_retry_exhausted_count,
        "zero_result_accepted_count": zero_result_accepted_count,
        "empty_scale_count": empty_scale_count,
        "probe_total_drift_count": probe_total_drift_count,
        "chunked_scale_fallback_count": chunked_scale_fallback_count,
        "empty_page_range_count": empty_page_range_count,
        "single_page_retry_count": single_page_retry_count,
        "provider_counts": {key: int(value) for key, value in sorted(provider_counts.items()) if key},
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "degraded_reason_counts": {
            key: int(value) for key, value in sorted(degraded_reason_counts.items()) if key
        },
        "incomplete_reason_counts": {
            key: int(value) for key, value in sorted(incomplete_reason_counts.items()) if key
        },
        "samples": samples,
    }


def _provider_anomaly_query_summaries(
    materialization_items: list[dict[str, Any]],
    *,
    workflow_commands: list[dict[str, Any]] | None = None,
    provider_anomaly_query_summaries: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in list(provider_anomaly_query_summaries or []):
        if isinstance(candidate, dict):
            _append_provider_anomaly_query_summary(summaries, seen, candidate)
    for item in materialization_items:
        metadata = dict(dict(item).get("metadata") or {})
        for candidate in (
            metadata.get("query_summary"),
            metadata.get("summary"),
            dict(metadata.get("provider_retry_item") or {}).get("query_summary"),
        ):
            if isinstance(candidate, dict):
                _append_provider_anomaly_query_summary(summaries, seen, candidate)
        for candidate in list(metadata.get("query_summaries") or []):
            if isinstance(candidate, dict):
                _append_provider_anomaly_query_summary(summaries, seen, candidate)
        retry_item = dict(metadata.get("provider_retry_item") or {})
        if retry_item and not isinstance(retry_item.get("query_summary"), dict):
            synthetic = {
                "query": retry_item.get("query"),
                "mode": retry_item.get("provider"),
                "employment_status": retry_item.get("employment_status"),
                "status": retry_item.get("status") or dict(item).get("status"),
                "incomplete_reason": dict(item).get("last_error"),
                "zero_result_retry": retry_item.get("zero_result_retry"),
            }
            _append_provider_anomaly_query_summary(summaries, seen, synthetic)
    for command in list(workflow_commands or []):
        payload = dict(dict(command).get("payload") or {})
        result = dict(dict(command).get("result") or {})
        metadata = dict(payload.get("materialization_metadata") or {})
        for candidate in (
            result.get("query_summary"),
            result.get("summary"),
            metadata.get("query_summary"),
            metadata.get("summary"),
        ):
            if isinstance(candidate, dict):
                _append_provider_anomaly_query_summary(summaries, seen, candidate)
        for container in (result, metadata):
            for candidate in list(dict(container).get("query_summaries") or []):
                if isinstance(candidate, dict):
                    _append_provider_anomaly_query_summary(summaries, seen, candidate)
    return summaries


def _append_provider_anomaly_query_summary(
    summaries: list[dict[str, Any]],
    seen: set[str],
    summary: dict[str, Any],
) -> None:
    normalized = dict(summary or {})
    key = json.dumps(
        {
            "query": str(normalized.get("query") or ""),
            "provider": str(normalized.get("mode") or normalized.get("provider") or normalized.get("account_id") or ""),
            "status": str(normalized.get("status") or ""),
            "reason": str(normalized.get("incomplete_reason") or normalized.get("degraded_reason") or ""),
            "item": str(normalized.get("search_seed_discovery_query_item_id") or normalized.get("discovery_query_item_id") or ""),
        },
        sort_keys=True,
    )
    if key in seen:
        return
    seen.add(key)
    summaries.append(normalized)


def _zero_result_retry_count(zero_retry: dict[str, Any]) -> int:
    retry_count = 0
    for value in list(zero_retry.values()):
        if isinstance(value, dict):
            if "retry_count" in value:
                retry_count += _safe_int(value.get("retry_count"))
            else:
                retry_count += _safe_int(value.get("attempts"))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    if "retry_count" in item:
                        retry_count += _safe_int(item.get("retry_count"))
                    else:
                        retry_count += _safe_int(item.get("attempts"))
    return retry_count


def _zero_result_retry_exhausted(zero_retry: dict[str, Any]) -> bool:
    for value in list(zero_retry.values()):
        if isinstance(value, dict) and bool(value.get("exhausted")):
            return True
        if isinstance(value, list) and any(isinstance(item, dict) and bool(item.get("exhausted")) for item in value):
            return True
    return False


def _build_search_seed_discovery_queue_metrics(
    *,
    workers: list[dict[str, Any]],
    materialization_items: list[dict[str, Any]],
    workflow_commands: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    discovery_items = [
        dict(item)
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "search_seed_discovery_query"
    ]
    discovery_commands = [
        dict(command)
        for command in list(workflow_commands or [])
        if str(dict(command).get("command_type") or "").strip() == LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE
    ]
    provider_retry_owner_item_ids = {
        str(
            dict(dict(item).get("metadata") or {})
            .get("provider_retry_item", {})
            .get("owner_item_id")
            or dict(dict(item).get("metadata") or {})
            .get("provider_retry_item", {})
            .get("search_seed_discovery_query_item_id")
            or ""
        ).strip()
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "provider_search_retry"
    }
    provider_retry_owner_item_ids.discard("")
    discovery_worker_owner_ids = {
        worker_id
        for item in discovery_items
        for worker_id in _item_source_worker_ids(item)
    }
    local_apply_worker_owner_ids = {
        worker_id
        for item in materialization_items
        if str(dict(item).get("item_kind") or "").strip() == "local_apply_closure"
        and str(dict(dict(item).get("metadata") or {}).get("worker_kind") or "").strip() == "search_seed"
        for worker_id in _item_source_worker_ids(dict(item))
    }
    completed_search_seed_worker_ids = [
        _safe_int(worker.get("worker_id"))
        for worker in workers
        if _is_completed_search_seed_discovery_worker(worker)
    ]
    completed_search_seed_worker_ids = [
        worker_id for worker_id in completed_search_seed_worker_ids if worker_id > 0
    ]
    discovery_worker_without_item_ids = [
        worker_id
        for worker_id in completed_search_seed_worker_ids
        if worker_id not in discovery_worker_owner_ids
    ]
    discovery_worker_without_local_apply_ids = [
        worker_id
        for worker_id in completed_search_seed_worker_ids
        if worker_id not in local_apply_worker_owner_ids
    ]
    status_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    command_status_counts: Counter[str] = Counter()
    provider_counts: Counter[str] = Counter()
    employment_scope_counts: Counter[str] = Counter()
    queued_count = 0
    dispatch_claimed_count = 0
    provider_owned_count = 0
    retry_wait_count = 0
    ready_retry_count = 0
    completed_count = 0
    failed_count = 0
    exhausted_count = 0
    exhausted_without_provider_retry_count = 0
    item_without_worker_owner_count = 0
    stale_provider_owned_count = 0
    latest_updated_at = ""
    query_samples: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    stale_threshold_ms = 120_000.0
    for item in discovery_items:
        status = str(item.get("status") or "").strip().lower()
        phase = str(item.get("phase") or "").strip().lower()
        metadata = dict(item.get("metadata") or {})
        item_id = str(item.get("item_id") or "")
        worker_id = _safe_int(metadata.get("worker_id"))
        status_counts[status] += 1
        phase_counts[phase] += 1
        provider = str(metadata.get("provider_name") or metadata.get("provider") or "").strip()
        employment_status = str(metadata.get("employment_status") or "").strip()
        if provider:
            provider_counts[provider] += 1
        if employment_status:
            employment_scope_counts[employment_status] += 1
        latest_updated_at = max(latest_updated_at, str(item.get("updated_at") or ""))
        if status in {"queued", "deferred"}:
            queued_count += 1
        if phase == "dispatch_claimed":
            dispatch_claimed_count += 1
        if phase == "provider_owned":
            provider_owned_count += 1
            source_worker_ids = [
                _safe_int(value)
                for value in list(item.get("source_worker_ids") or [])
                if _safe_int(value) > 0
            ]
            provider_owner_present = bool(
                source_worker_ids
                or worker_id > 0
                or str(metadata.get("provider_run_id") or metadata.get("provider_dataset_id") or "").strip()
                or str(dict(metadata.get("search_state") or {}).get("task_id") or "").strip()
            )
            if not provider_owner_present:
                item_without_worker_owner_count += 1
            updated_at = _parse_timestamp(str(item.get("updated_at") or ""))
            if updated_at is not None and (now - updated_at).total_seconds() * 1000 >= stale_threshold_ms:
                stale_provider_owned_count += 1
        if phase == "retry_wait" or status == "failed_retryable":
            retry_wait_count += 1
            not_before_at = _parse_timestamp(str(item.get("not_before_at") or ""))
            if not_before_at is None or not_before_at <= now:
                ready_retry_count += 1
        if status == "completed":
            completed_count += 1
        if status == "failed":
            failed_count += 1
        if status == "exhausted" or phase == "exhausted":
            exhausted_count += 1
            if item_id not in provider_retry_owner_item_ids:
                exhausted_without_provider_retry_count += 1
        if len(query_samples) < 10:
            query_samples.append(
                {
                    "item_id": item_id,
                    "status": status,
                    "phase": phase,
                    "query": str(metadata.get("query") or ""),
                    "employment_status": employment_status,
                    "provider": provider,
                    "worker_id": worker_id,
                }
            )
    for command in discovery_commands:
        status = str(command.get("status") or "").strip().lower()
        result = dict(command.get("result") or {})
        payload = dict(command.get("payload") or {})
        metadata = dict(payload.get("materialization_metadata") or {})
        command_status_counts[status] += 1
        latest_updated_at = max(latest_updated_at, str(command.get("updated_at") or ""))
        if status == "queued":
            queued_count += 1
        if status == "retry_wait":
            retry_wait_count += 1
            not_before_at = _parse_timestamp(str(command.get("not_before_at") or ""))
            if not_before_at is None or not_before_at <= now:
                ready_retry_count += 1
        if status in {"claimed", "running"}:
            provider_owned_count += 1
            lease_expires_at = _parse_timestamp(str(command.get("lease_expires_at") or ""))
            if lease_expires_at is not None and (now - lease_expires_at).total_seconds() * 1000 >= stale_threshold_ms:
                stale_provider_owned_count += 1
        if status == "succeeded":
            if str(result.get("status") or "").strip().lower() == "exhausted":
                exhausted_count += 1
            else:
                completed_count += 1
        if status == "failed_terminal":
            failed_count += 1
        if len(query_samples) < 10:
            query_samples.append(
                {
                    "item_id": str(payload.get("item_id") or ""),
                    "status": status,
                    "phase": "workflow_command",
                    "query": str(payload.get("query") or metadata.get("query") or ""),
                    "employment_status": str(payload.get("employment_status") or metadata.get("employment_status") or ""),
                    "provider": str(metadata.get("provider_name") or metadata.get("provider") or ""),
                    "workflow_command_id": str(command.get("command_id") or ""),
                }
            )
    backlog_count = queued_count + dispatch_claimed_count + provider_owned_count + retry_wait_count
    return {
        "report_available": bool(discovery_items or discovery_commands or completed_search_seed_worker_ids),
        "item_count": len(discovery_items),
        "command_count": len(discovery_commands),
        "completed_search_seed_worker_count": len(completed_search_seed_worker_ids),
        "discovery_worker_without_item_count": len(discovery_worker_without_item_ids),
        "discovery_worker_without_item_present": bool(discovery_worker_without_item_ids),
        "discovery_worker_without_local_apply_count": len(discovery_worker_without_local_apply_ids),
        "discovery_worker_without_local_apply_present": bool(discovery_worker_without_local_apply_ids),
        "discovery_worker_owner_gap_count": len(
            set(discovery_worker_without_item_ids) | set(discovery_worker_without_local_apply_ids)
        ),
        "discovery_worker_owner_gap_present": bool(
            discovery_worker_without_item_ids or discovery_worker_without_local_apply_ids
        ),
        "discovery_worker_owner_gap_worker_ids": sorted(
            set(discovery_worker_without_item_ids) | set(discovery_worker_without_local_apply_ids)
        )[:20],
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items()) if key},
        "phase_counts": {key: int(value) for key, value in sorted(phase_counts.items()) if key},
        "command_status_counts": {
            key: int(value) for key, value in sorted(command_status_counts.items()) if key
        },
        "provider_counts": {key: int(value) for key, value in sorted(provider_counts.items()) if key},
        "employment_scope_counts": {
            key: int(value) for key, value in sorted(employment_scope_counts.items()) if key
        },
        "queued_count": queued_count,
        "dispatch_claimed_count": dispatch_claimed_count,
        "provider_owned_count": provider_owned_count,
        "retry_wait_count": retry_wait_count,
        "ready_retry_count": ready_retry_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "exhausted_count": exhausted_count,
        "exhausted_without_provider_retry_count": exhausted_without_provider_retry_count,
        "exhausted_without_provider_retry_present": exhausted_without_provider_retry_count > 0,
        "backlog_count": backlog_count,
        "retry_backlog_present": ready_retry_count > 0,
        "item_without_worker_owner_count": item_without_worker_owner_count,
        "item_without_worker_owner_present": item_without_worker_owner_count > 0,
        "stale_provider_owned_count": stale_provider_owned_count,
        "stale_provider_owned_present": stale_provider_owned_count > 0,
        "latest_updated_at": latest_updated_at,
        "query_samples": query_samples,
    }


def _item_source_worker_ids(item: dict[str, Any]) -> list[int]:
    worker_ids: list[int] = []
    seen: set[int] = set()
    for value in list(dict(item).get("source_worker_ids") or []):
        worker_id = _safe_int(value)
        if worker_id <= 0 or worker_id in seen:
            continue
        seen.add(worker_id)
        worker_ids.append(worker_id)
    return worker_ids


def _is_completed_search_seed_discovery_worker(worker: dict[str, Any]) -> bool:
    payload = dict(worker or {})
    if str(payload.get("status") or "").strip().lower() != "completed":
        return False
    lane_id = str(payload.get("lane_id") or "").strip()
    if lane_id not in {"search_planner", "public_media_specialist"}:
        return False
    metadata = dict(payload.get("metadata") or {})
    checkpoint = dict(payload.get("checkpoint") or {})
    output = dict(payload.get("output") or {})
    recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip().lower()
    if recovery_kind == "search_seed_discovery":
        return True
    if recovery_kind:
        return False
    summary = dict(output.get("summary") or {})
    input_payload = dict(payload.get("input") or {})
    return bool(
        summary.get("query")
        or input_payload.get("query")
        or dict(input_payload.get("query_spec") or {}).get("query")
        or output.get("entries")
    )


def _build_board_visible_projection_metrics(
    *,
    lifecycle: dict[str, Any],
    board_visible_patches: list[dict[str, Any]],
) -> dict[str, Any]:
    delta_required = _safe_int(lifecycle.get("delta_profile_required_count"))
    delta_fetched = _safe_int(lifecycle.get("delta_profile_fetched_count"))
    delta_materialized = _safe_int(lifecycle.get("delta_profile_materialized_count"))
    raw_delta_board_visible = _safe_int(lifecycle.get("delta_profile_board_visible_count"))
    served_candidate_count = _safe_int(lifecycle.get("served_candidate_count"))
    raw_expected_candidate_count = _safe_int(lifecycle.get("expected_candidate_count"))
    current_snapshot_id = str(lifecycle.get("current_snapshot_id") or "").strip()
    served_snapshot_id = str(lifecycle.get("served_snapshot_id") or "").strip()
    serving_projection_id = str(lifecycle.get("serving_projection_id") or "").strip()
    serving_projection_phase = str(lifecycle.get("serving_projection_phase") or "").strip()
    phase = str(lifecycle.get("phase") or lifecycle.get("state") or "").strip()
    full_snapshot_serving = bool(
        serving_projection_phase == "current_snapshot_serving"
        or (
            phase in {"current_snapshot_serving", "current_serving"}
            and current_snapshot_id
            and served_snapshot_id == current_snapshot_id
        )
    )
    board_visible_inferred_from_full_snapshot = bool(
        full_snapshot_serving
        and max(delta_fetched, delta_materialized) > 0
        and served_candidate_count > 0
        and raw_delta_board_visible < max(delta_fetched, delta_materialized)
    )
    delta_board_visible = (
        max(raw_delta_board_visible, delta_fetched, delta_materialized)
        if board_visible_inferred_from_full_snapshot
        else raw_delta_board_visible
    )
    ordered_patches = sorted(
        board_visible_patches,
        key=lambda item: (
            _safe_int(item.get("sequence_index")),
            str(item.get("published_at") or ""),
            str(item.get("patch_id") or ""),
        ),
    )
    patch_candidate_ids: list[str] = []
    patch_sequence_values: list[int] = []
    patch_served_counts: list[float] = []
    patch_display_ready_counts: list[float] = []
    patch_consumable_card_counts: list[float] = []
    patch_delta_card_visible_counts: list[float] = []
    pure_shell_patch_count = 0
    patch_card_quality_missing_count = 0
    partial_delta_patch_count = 0
    overlay_write_mode_missing_count = 0
    cumulative_replay_ids: list[str] = []
    for patch in ordered_patches:
        patch_sequence_values.append(_safe_int(patch.get("sequence_index")))
        patch_phase = str(patch.get("patch_phase") or "").strip()
        patch_kind = str(patch.get("patch_kind") or patch.get("kind") or "").strip()
        if patch_phase == "board_visible_delta_applied" or patch_kind == "partial_delta_board_visible_patch":
            partial_delta_patch_count += 1
            if not str(_patch_overlay_write_payload(patch).get("mode") or "").strip():
                overlay_write_mode_missing_count += 1
        candidate_ids = patch.get("candidate_ids")
        if not isinstance(candidate_ids, list):
            candidate_ids = []
        deduped_candidate_ids = _dedupe_texts(candidate_ids)
        patch_candidate_ids.extend(deduped_candidate_ids)
        cumulative_replay_ids = _dedupe_texts([*cumulative_replay_ids, *deduped_candidate_ids])
        served_count = _safe_int(patch.get("served_candidate_count"))
        if served_count > 0:
            patch_served_counts.append(float(served_count))
        card_summary = _patch_card_materialization_summary(patch)
        if card_summary:
            display_ready_count = _safe_int(card_summary.get("display_ready_candidate_count"))
            needs_profile_completion_count = _safe_int(card_summary.get("needs_profile_completion_candidate_count"))
            # `low_profile_richness` is a display-ready sub-class, not a
            # disjoint bucket. Consumable cards are the union of display-ready
            # rows and explicit profile-completion placeholders.
            consumable_card_count = display_ready_count + needs_profile_completion_count
            candidate_count = _safe_int(card_summary.get("candidate_count"))
            if candidate_count > 0:
                consumable_card_count = min(candidate_count, consumable_card_count)
            patch_display_ready_counts.append(float(display_ready_count))
            patch_consumable_card_counts.append(float(consumable_card_count))
            if consumable_card_count > 0:
                cumulative_candidate_ids = patch.get("cumulative_candidate_ids")
                if not isinstance(cumulative_candidate_ids, list):
                    cumulative_candidate_ids = []
                cumulative_delta_count = _safe_int(patch.get("cumulative_candidate_count")) or len(
                    _dedupe_texts(cumulative_candidate_ids)
                )
                if cumulative_delta_count <= 0:
                    cumulative_delta_count = len(cumulative_replay_ids)
                if cumulative_delta_count > 0:
                    patch_delta_card_visible_counts.append(float(cumulative_delta_count))
            preview_count = _safe_int(card_summary.get("preview_candidate_count"))
            candidate_count = max(
                _safe_int(card_summary.get("candidate_count")),
                served_count,
                _safe_int(patch.get("cumulative_candidate_count")),
                _safe_int(patch.get("candidate_count")),
            )
            quality_fields_available = bool(card_summary.get("quality_fields_available")) or any(
                key in card_summary
                for key in (
                    "display_ready_candidate_count",
                    "needs_profile_completion_candidate_count",
                    "low_profile_richness_candidate_count",
                )
            )
            if quality_fields_available and consumable_card_count <= 0 and max(preview_count, candidate_count) > 0:
                pure_shell_patch_count += 1
        else:
            patch_card_quality_missing_count += 1
    replay_candidate_ids = _dedupe_texts(patch_candidate_ids)
    replay_candidate_count = len(replay_candidate_ids)
    latest_patch = ordered_patches[-1] if ordered_patches else {}
    latest_cumulative_candidate_ids = latest_patch.get("cumulative_candidate_ids")
    if not isinstance(latest_cumulative_candidate_ids, list):
        latest_cumulative_candidate_ids = []
    latest_cumulative_count = _safe_int(latest_patch.get("cumulative_candidate_count")) or len(
        _dedupe_texts(latest_cumulative_candidate_ids)
    )
    if latest_cumulative_count <= 0:
        latest_cumulative_count = replay_candidate_count
    patch_log_required = bool(delta_board_visible > 0 and not full_snapshot_serving)
    expected_candidate_count = (
        served_candidate_count
        if full_snapshot_serving and served_candidate_count > 0
        else max(raw_expected_candidate_count, served_candidate_count)
    )
    projection_present = bool(serving_projection_id or full_snapshot_serving)
    visible_without_projection = delta_board_visible > 0 and not projection_present
    visible_without_patch_log = patch_log_required and not ordered_patches
    patch_log_lag_count = (
        max(0, delta_board_visible - latest_cumulative_count)
        if patch_log_required
        else 0
    )
    fetched_visible_lag = max(0, delta_fetched - delta_board_visible)
    materialized_visible_lag = max(0, delta_materialized - delta_board_visible)
    return {
        "report_available": bool(lifecycle or ordered_patches),
        "phase": phase,
        "current_snapshot_id": current_snapshot_id,
        "served_snapshot_id": served_snapshot_id,
        "serving_projection_id": serving_projection_id,
        "serving_projection_phase": serving_projection_phase,
        "full_snapshot_serving": full_snapshot_serving,
        "patch_log_required": patch_log_required,
        "board_visible_inferred_from_full_snapshot": board_visible_inferred_from_full_snapshot,
        "expected_candidate_count": expected_candidate_count,
        "raw_expected_candidate_count": raw_expected_candidate_count,
        "public_expected_count_source": (
            "serving_projection_visible_members" if full_snapshot_serving and served_candidate_count > 0 else "lifecycle"
        ),
        "served_candidate_count": served_candidate_count,
        "delta_profile_required_count": delta_required,
        "delta_profile_fetched_count": delta_fetched,
        "delta_profile_materialized_count": delta_materialized,
        "raw_delta_profile_board_visible_count": raw_delta_board_visible,
        "delta_profile_board_visible_count": delta_board_visible,
        "fetched_to_board_visible_lag_count": fetched_visible_lag,
        "materialized_to_board_visible_lag_count": materialized_visible_lag,
        "patch_log_count": len(ordered_patches),
        "patch_log_candidate_replay_count": replay_candidate_count,
        "patch_log_latest_cumulative_count": latest_cumulative_count,
        "patch_log_lag_count": patch_log_lag_count,
        "patch_sequence_values": patch_sequence_values[:200],
        "patch_sequence_contiguous": _sequence_is_contiguous(patch_sequence_values),
        "patch_served_candidate_count": _stats(patch_served_counts),
        "patch_display_ready_candidate_count": _stats(patch_display_ready_counts),
        "patch_display_ready_distinct_count": len({int(value) for value in patch_display_ready_counts if value > 0}),
        "patch_display_ready_nonzero_count": sum(1 for value in patch_display_ready_counts if value > 0),
        "patch_consumable_card_count": _stats(patch_consumable_card_counts),
        "patch_consumable_card_distinct_count": len(
            {int(value) for value in patch_consumable_card_counts if value > 0}
        ),
        "patch_consumable_card_nonzero_count": sum(1 for value in patch_consumable_card_counts if value > 0),
        "patch_delta_card_visible_count": _stats(patch_delta_card_visible_counts),
        "patch_delta_card_visible_distinct_count": len(
            {int(value) for value in patch_delta_card_visible_counts if value > 0}
        ),
        "patch_delta_card_visible_nonzero_count": sum(1 for value in patch_delta_card_visible_counts if value > 0),
        "pure_shell_patch_count": pure_shell_patch_count,
        "patch_card_quality_missing_count": patch_card_quality_missing_count,
        "partial_delta_patch_count": partial_delta_patch_count,
        "overlay_write_mode_missing_count": overlay_write_mode_missing_count,
        "overlay_write_mode_missing_present": overlay_write_mode_missing_count > 0,
        "projection_present": projection_present,
        "projection_missing_for_visible_count": visible_without_projection,
        "patch_log_missing_for_visible_count": visible_without_patch_log,
        "patch_log_replay_lag": patch_log_lag_count > 0,
        "metadata_replay_dependency": False,
        "patch_log_required_missing": visible_without_patch_log,
        "materialization_lag_threshold_count": _BOARD_VISIBLE_MATERIALIZATION_LAG_THRESHOLD,
        "materialization_lag_violation": fetched_visible_lag > _BOARD_VISIBLE_MATERIALIZATION_LAG_THRESHOLD,
    }


def _patch_card_materialization_summary(patch: dict[str, Any]) -> dict[str, Any]:
    payload = dict(patch or {})
    metadata = dict(payload.get("metadata") or {})
    candidates = (
        metadata.get("card_materialization_summary"),
        dict(metadata.get("result_view_metadata_mirror") or {}).get("card_materialization_summary"),
        metadata.get("partial_board_visible_patch"),
        payload.get("card_materialization_summary"),
        payload,
    )
    known_keys = {
        "candidate_count",
        "display_ready_candidate_count",
        "preview_candidate_count",
        "needs_profile_completion_candidate_count",
        "low_profile_richness_candidate_count",
        "quality_fields_available",
    }
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if any(key in candidate for key in known_keys):
            return dict(candidate)
    return {}


def _build_serving_publication_gap_metrics(gap: dict[str, Any]) -> dict[str, Any]:
    payload = dict(gap or {})
    status = str(payload.get("status") or "").strip()
    served_snapshot_id = str(payload.get("served_snapshot_id") or "").strip()
    current_snapshot_id = str(payload.get("current_snapshot_id") or "").strip()
    gap_present = bool(
        payload
        and status in {"", "pending_event_time_publication"}
        and current_snapshot_id
        and served_snapshot_id
        and current_snapshot_id != served_snapshot_id
    )
    source_updated_at = _first_nonempty(
        payload.get("source_updated_at"),
        payload.get("current_snapshot_updated_at"),
        payload.get("completed_at"),
        payload.get("updated_at"),
    )
    observed_at = _first_nonempty(payload.get("observed_at"), datetime.now(timezone.utc).isoformat())
    age_ms = _duration_ms(source_updated_at, observed_at) if source_updated_at else None
    age_unknown = gap_present and age_ms is None
    stale_gap_present = bool(
        gap_present
        and (
            age_unknown
            or (age_ms is not None and age_ms >= _SERVING_PUBLICATION_GAP_THRESHOLD_MS)
        )
    )
    return {
        "report_available": bool(payload),
        "gap_present": gap_present,
        "stale_gap_present": stale_gap_present,
        "age_unknown": age_unknown,
        "age_ms": round(age_ms, 2) if age_ms is not None else 0.0,
        "threshold_ms": _SERVING_PUBLICATION_GAP_THRESHOLD_MS,
        "status": status,
        "reason": str(payload.get("reason") or "").strip(),
        "job_id": str(payload.get("job_id") or "").strip(),
        "target_company": str(payload.get("target_company") or "").strip(),
        "served_snapshot_id": served_snapshot_id,
        "current_snapshot_id": current_snapshot_id,
        "source_kind": str(payload.get("source_kind") or "").strip(),
        "source_updated_at": source_updated_at,
        "observed_at": observed_at,
    }


def _sequence_is_contiguous(values: list[int]) -> bool:
    cleaned = [int(value) for value in values if int(value or 0) > 0]
    if not cleaned:
        return True
    return cleaned == list(range(cleaned[0], cleaned[0] + len(cleaned)))


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


def _gap_between(previous: dict[str, Any], current: dict[str, Any]) -> float | None:
    previous_end = _parse_timestamp(str(previous.get("handoff_completed_at") or previous.get("completed_at") or ""))
    current_start = _parse_timestamp(str(current.get("handoff_started_at") or current.get("started_at") or ""))
    if previous_end is None or current_start is None:
        return None
    return max(0.0, (current_start - previous_end).total_seconds() * 1000)


def _gap_record(
    previous: dict[str, Any],
    current: dict[str, Any],
    gap: float,
    *,
    scope: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = {
        "scope": scope,
        "gap_ms": round(gap, 2),
        "previous_worker_id": _safe_int(previous.get("worker_id")),
        "previous_lane_id": str(previous.get("lane_id") or ""),
        "previous_worker_key": str(previous.get("worker_key") or ""),
        "next_worker_id": _safe_int(current.get("worker_id")),
        "next_lane_id": str(current.get("lane_id") or ""),
        "next_worker_key": str(current.get("worker_key") or ""),
    }
    normalized_context = dict(context or {})
    if normalized_context:
        record.update(
            {
                "profile_scheduler_gap": bool(normalized_context.get("profile_scheduler_gap")),
                "covered_by_upstream": bool(normalized_context.get("covered_by_upstream")),
                "covering_span_id": _safe_int(normalized_context.get("covering_span_id")),
                "covering_lane_id": str(normalized_context.get("covering_lane_id") or ""),
                "reason": str(normalized_context.get("reason") or "").strip(),
            }
        )
    return record


def _profile_scheduler_gap_context(
    previous: dict[str, Any],
    current: dict[str, Any],
    *,
    upstream_spans: list[dict[str, Any]],
) -> dict[str, Any]:
    if str(previous.get("recovery_kind") or "").strip() != "harvest_profile_batch":
        return {"profile_scheduler_gap": False, "covered_by_upstream": False}
    if str(current.get("recovery_kind") or "").strip() != "harvest_profile_batch":
        return {"profile_scheduler_gap": False, "covered_by_upstream": False}
    if _profile_prefetch_plan_exhausted_before_next_wave(previous, current):
        return {
            "profile_scheduler_gap": False,
            "covered_by_upstream": False,
            "reason": "profile_prefetch_dispatch_plan_exhausted",
        }
    previous_end = _parse_timestamp(str(previous.get("handoff_completed_at") or previous.get("completed_at") or ""))
    current_start = _parse_timestamp(str(current.get("handoff_started_at") or current.get("started_at") or ""))
    covering_span = _covering_profile_scheduler_upstream_span(
        previous_end=previous_end,
        current_start=current_start,
        upstream_spans=upstream_spans,
    )
    return {
        "profile_scheduler_gap": True,
        "covered_by_upstream": bool(covering_span),
        "covering_span_id": _safe_int(dict(covering_span or {}).get("span_id")),
        "covering_lane_id": str(dict(covering_span or {}).get("lane_id") or ""),
        "reason": "covered_by_upstream_acquisition" if covering_span else "same_wave_profile_scheduler_gap",
    }


def _profile_prefetch_batch_context(worker: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(worker.get("metadata") or {})
    checkpoint = dict(worker.get("checkpoint") or {})
    for source in (
        checkpoint.get("prefetch_batch_context"),
        metadata.get("prefetch_batch_context"),
        checkpoint,
        metadata,
    ):
        if isinstance(source, dict) and (
            "chunk_index" in source or "planned_dispatch_worker_count" in source
        ):
            return dict(source)
    return {}


def _profile_prefetch_plan_exhausted_before_next_wave(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    previous_chunk_index = _safe_int(previous.get("profile_prefetch_chunk_index"))
    previous_planned_workers = _safe_int(previous.get("profile_prefetch_planned_dispatch_worker_count"))
    current_chunk_index = _safe_int(current.get("profile_prefetch_chunk_index"))
    if previous_chunk_index <= 0 or previous_planned_workers <= 0 or current_chunk_index <= 0:
        return False
    if previous_chunk_index < previous_planned_workers:
        return False
    return current_chunk_index <= previous_chunk_index


def _profile_scheduler_upstream_spans(trace_spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    for span in trace_spans:
        lane_id = str(span.get("lane_id") or "").strip()
        handoff_to = str(span.get("handoff_to_lane") or "").strip()
        metadata = dict(span.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "").strip()
        if recovery_kind == "harvest_profile_batch":
            continue
        if lane_id not in {"acquisition_specialist", "search_seed", "search_seed_discovery"} and handoff_to != (
            "acquisition_specialist"
        ):
            continue
        started_at = _parse_timestamp(str(span.get("started_at") or ""))
        completed_at = _parse_timestamp(str(span.get("completed_at") or ""))
        if started_at is None or completed_at is None:
            continue
        spans.append({**span, "_started_at": started_at, "_completed_at": completed_at})
    return spans


def _covering_profile_scheduler_upstream_span(
    *,
    previous_end: datetime | None,
    current_start: datetime | None,
    upstream_spans: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if previous_end is None or current_start is None:
        return None
    for span in upstream_spans:
        started_at = span.get("_started_at")
        completed_at = span.get("_completed_at")
        if not isinstance(started_at, datetime) or not isinstance(completed_at, datetime):
            continue
        if started_at <= previous_end and completed_at >= current_start:
            return span
    return None


def _first_positive_timeline_observed_ms(timeline: list[dict[str, Any]]) -> float:
    for item in timeline:
        value = _safe_float(item.get("observed_at_ms"))
        if value > 0.0:
            return round(value, 2)
    return 0.0


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    payload = event.get("event_payload")
    if isinstance(payload, dict):
        return dict(payload)
    raw_payload = event.get("payload_json") or event.get("event_payload_json")
    if isinstance(raw_payload, str) and raw_payload.strip():
        try:
            decoded = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _event_created_at(event: dict[str, Any]) -> str:
    return str(event.get("created_at") or event.get("timestamp") or event.get("updated_at") or "").strip()


def _stats(values: list[float]) -> dict[str, Any]:
    cleaned = sorted(float(value) for value in values if float(value) >= 0.0)
    if not cleaned:
        return {"count": 0}
    return {
        "count": len(cleaned),
        "min": round(cleaned[0], 2),
        "max": round(cleaned[-1], 2),
        "avg": round(sum(cleaned) / len(cleaned), 2),
        "p50": round(_percentile(cleaned, 0.50), 2),
        "p95": round(_percentile(cleaned, 0.95), 2),
        "values": [round(value, 2) for value in cleaned[:200]],
    }


def _zeroable_stats(values: list[float]) -> dict[str, Any]:
    stats = _stats(values)
    if _safe_int(stats.get("count")) == 0:
        return {"count": 0, "max": 0.0}
    return stats


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    position = max(0.0, min(1.0, q)) * (len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    fraction = position - lower
    return values[lower] * (1 - fraction) + values[upper] * fraction


def _duration_ms(started_at: str, completed_at: str) -> float | None:
    start = _parse_timestamp(started_at)
    end = _parse_timestamp(completed_at)
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds() * 1000)


def _parse_timestamp(value: str) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    candidates = [normalized, normalized.replace("Z", "+00:00")]
    if "T" not in normalized and " " in normalized:
        candidates.append(normalized.replace(" ", "T") + "+00:00")
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
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


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _safe_counter(value: Any) -> Counter[str]:
    if not isinstance(value, dict):
        return Counter()
    return Counter({str(key): _safe_int(raw_value) for key, raw_value in value.items() if str(key).strip()})


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""
