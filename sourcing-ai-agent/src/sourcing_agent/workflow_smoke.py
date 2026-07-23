from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib.parse import parse_qs, quote, urlparse

from .durable_runtime import (
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
)
from .remote_provider_events import shared_recovery_signal_count
from .runtime_tuning import (
    build_materialization_streaming_budget_report,
    build_provider_backpressure_budget_report,
    resolved_harvest_profile_actor_global_inflight,
)
from .scripted_provider_scenario import load_scripted_provider_invocations
from .smoke_expectation_contract import validate_smoke_expectations
from .workflow_efficiency import (
    aggregate_event_level_efficiency_metrics,
    event_level_efficiency_runtime_subset,
    extract_event_level_efficiency_metrics,
)
from .workflow_service_metrics import build_workflow_service_metrics

_POST_TERMINAL_MATERIALIZATION_PENDING_STATUSES = {
    "queued",
    "deferred",
    "failed_retryable",
    "waiting_prerequisite",
    "running",
    "applying",
}
_POST_TERMINAL_MATERIALIZATION_RUNNING_STATUSES = {"running", "applying"}
_POST_TERMINAL_COMMAND_PENDING_STATUSES = {
    "queued",
    "retry_wait",
    "claimed",
    "running",
}
_POST_TERMINAL_COMMAND_RUNNING_STATUSES = {"claimed", "running"}
_POST_TERMINAL_COMMAND_ITEM_KINDS = {
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE: "local_apply_closure",
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE: "board_visible_delta_apply",
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE: "projection_person_search_index_build",
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE: "projection_facet_layering_build",
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE: "snapshot_full_materialization",
}

# Product-shared smoke surface moved to hosted_smoke_surface (2026-07-22
# god-file split wave 1); re-imported for the internal runners and for
# backward-compatible consumers of this module path.
from .hosted_smoke_surface import (  # noqa: E402,F401 — module-path compat re-exports
    DEFAULT_SMOKE_CASES,
    HostedWorkflowSmokeClient,
    load_smoke_cases,
)

TERMINAL_WORKFLOW_STATUSES = {"completed", "failed"}
TERMINAL_WORKER_STATUSES = {"completed", "failed", "skipped", "cancelled", "canceled"}

_AUTO_RECOVERY_RUNTIME_HEALTH_CLASSIFICATIONS = {
    "blocked_on_acquisition_workers",
    "blocked_ready_for_resume",
    "queued_waiting_for_runner",
    "runner_failed_to_start",
    "runner_not_alive",
    "runner_takeover_pending",
}

_TIMING_SUMMARY_KEYS = (
    "explain",
    "plan",
    "review",
    "start",
    "wait_for_completion",
    "fetch_job_and_results",
    "dashboard_fetch",
    "candidate_page_fetch",
    "board_probe_wait",
    "total",
)

_PROVIDER_STAGE_ORDER = (
    "linkedin_stage_1",
    "stage_1_preview",
    "public_web_stage_2",
    "stage_2_final",
)
_WORKFLOW_WALL_CLOCK_KEYS = (
    "job_to_stage_1_preview",
    "job_to_final_results",
    "stage_1_preview_to_final_results",
    "final_results_to_board_ready",
    "job_to_board_ready",
    "job_to_board_visible_partial",
    "final_results_to_board_nonempty",
    "job_to_board_nonempty",
)

_MAX_REASONABLE_STAGE_WALL_CLOCK_MS = 6 * 60 * 60 * 1000
# Backend stage timestamps are currently persisted at second precision. Keep
# this threshold above truncation noise while still catching product-visible
# idle gaps such as minute-scale waits before materialization or board readiness.
_PREREQUISITE_GAP_VIOLATION_MS = 5000.0
_FINAL_RESULTS_BOARD_LAG_VIOLATION_MS = 5000.0
_STREAMING_MATERIALIZATION_GAP_VIOLATION_MS = 5000.0
_POST_PREVIEW_FINALIZATION_LAG_MS = 30_000.0
_MONOTONIC_PROGRESS_COUNTER_KEYS = (
    "result_count",
    "observed_company_candidate_count",
)
_STAGE1_MONOTONIC_PROGRESS_KEYS = (
    "current_search_returned_count",
    "former_search_returned_count",
    "all_search_returned_count",
    "deduped_candidate_count",
    "deduped_profile_url_count",
    "profile_fetch_required_count",
    "profile_fetched_count",
)
_RESULT_LIFECYCLE_MONOTONIC_PROGRESS_KEYS = (
    "served_candidate_count",
    "delta_profile_required_count",
    "delta_profile_fetched_count",
    "delta_profile_materialized_count",
    "delta_profile_board_visible_count",
)
_BACKLOG_PROGRESS_COUNTER_KEYS = ("manual_review_count",)
_PROGRESS_MAX_COUNTER_KEYS = (
    "queued_worker_count",
    "blocked_worker_count",
    "waiting_remote_harvest_count",
    "waiting_remote_search_count",
    "queued_harvest_worker_count",
    "queued_exploration_count",
    "candidate_count",
    "evidence_count",
    "pending_worker_count",
    "active_worker_count",
)
_SMOKE_PROVIDER_WEBHOOK_PATH = "/api/providers/apify/webhook"
_BOARD_RUNTIME_PARITY_ENDPOINTS = ("progress", "dashboard", "candidates", "board_patches")
_BOARD_RUNTIME_PARITY_FIELDS = (
    "schema_version",
    "job_id",
    "result_mode",
    "phase",
    "publication_status",
    "expected_candidate_count",
    "served_candidate_count",
    "published_candidate_count",
    "display_ready_candidate_count",
    "row_hydration_target_count",
    "baseline_candidate_count",
    "delta_profile_required_count",
    "delta_profile_fetched_count",
    "delta_profile_materialized_count",
    "delta_profile_board_visible_count",
    "delta_profile_denominator_promoted",
    "row_publication_sequence",
    "row_publication_tier",
    "row_publication_watermark",
    "facet_summary_status",
    "facet_summary_scope",
    "facet_summary_candidate_count",
    "layering_status",
    "sync_status_text",
    "profile_fetch_status_text",
    "card_materialization_status_text",
    "filter_contract",
)


def _progress_runtime_health(snapshot: dict[str, Any]) -> dict[str, Any]:
    return dict(dict(snapshot.get("progress") or {}).get("runtime_health") or {})


def _remote_actor_slot_observation_report(
    *,
    progress_observability: dict[str, Any],
    runtime_tuning_context: dict[str, Any],
) -> dict[str, Any]:
    actor_budget = resolved_harvest_profile_actor_global_inflight(runtime_tuning_context)
    progress_maxima = dict(progress_observability.get("maxima") or {})
    peak_waiting_remote_harvest_count = _safe_int(progress_maxima.get("waiting_remote_harvest_count"))
    occupancy_ratio = round(min(peak_waiting_remote_harvest_count, actor_budget) / actor_budget, 4)
    return {
        "report_available": actor_budget > 0,
        "budget_source": "runtime_tuning_context",
        "harvest_profile_actor_global_inflight": actor_budget,
        "peak_waiting_remote_harvest_count": peak_waiting_remote_harvest_count,
        "remote_actor_slot_peak_occupancy_ratio": occupancy_ratio,
    }


def _should_auto_run_worker_recovery(snapshot: dict[str, Any]) -> bool:
    status = str(snapshot.get("status") or "").strip().lower()
    stage = str(snapshot.get("stage") or "").strip().lower()
    if stage != "acquiring" or status not in {"blocked", "running"}:
        return False
    runtime_health = _progress_runtime_health(snapshot)
    classification = str(runtime_health.get("classification") or "").strip().lower()
    if classification in _AUTO_RECOVERY_RUNTIME_HEALTH_CLASSIFICATIONS:
        return True
    counters = dict(dict(snapshot.get("progress") or {}).get("counters") or {})
    # Service-level simulations must keep draining provider tails after the UI
    # moves from blocked back to running; otherwise ready remote actors can hold
    # limiter leases until the smoke timeout while no real service loop is alive.
    return any(
        int(counters.get(key) or 0) > 0
        for key in (
            "blocked_worker_count",
            "queued_worker_count",
            "waiting_remote_harvest_count",
            "waiting_remote_search_count",
        )
    )


def _should_drive_smoke_remote_provider_events(snapshot: dict[str, Any]) -> bool:
    status = str(snapshot.get("status") or "").strip().lower()
    stage = str(snapshot.get("stage") or "").strip().lower()
    if status in TERMINAL_WORKFLOW_STATUSES:
        return False
    return stage in {"acquiring", "retrieving"} and status in {"blocked", "running", "queued"}


def _worker_has_inline_incremental_ingest(worker: dict[str, Any]) -> bool:
    output = dict(dict(worker or {}).get("output") or {})
    inline_ingest = dict(output.get("inline_incremental_ingest") or {})
    return bool(str(inline_ingest.get("applied_at") or "").strip())


def _worker_has_smoke_remote_terminal_marker(worker: dict[str, Any]) -> bool:
    checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    if str(checkpoint.get("remote_provider_terminal_event_seen_at") or "").strip():
        return True
    terminal_event = dict(checkpoint.get("remote_provider_terminal_event") or {})
    if bool(terminal_event.get("is_terminal")):
        return True
    status = str(terminal_event.get("status") or "").strip().lower()
    event_type = str(terminal_event.get("event_type") or terminal_event.get("eventType") or "").strip().lower()
    return status in {"succeeded", "completed", "failed", "timed-out", "timed_out"} or "succeeded" in event_type


def _worker_needs_smoke_recovery(worker: dict[str, Any]) -> bool:
    payload = dict(worker or {})
    metadata = dict(payload.get("metadata") or {})
    recovery_kind = str(metadata.get("recovery_kind") or "").strip()
    if recovery_kind in {"target_candidate_public_web_search", "crm_public_web_search"}:
        status = str(payload.get("status") or "").strip().lower()
        return status not in TERMINAL_WORKER_STATUSES
    if recovery_kind not in {"harvest_company_employees", "harvest_profile_batch"}:
        return False
    status = str(payload.get("status") or "").strip().lower()
    if status not in TERMINAL_WORKER_STATUSES:
        return True
    if status == "completed" and not _worker_has_inline_incremental_ingest(payload):
        return True
    return False


def _worker_smoke_remote_identity(worker: dict[str, Any]) -> tuple[str, str]:
    payload = dict(worker or {})
    checkpoint = dict(payload.get("checkpoint") or {})
    output = dict(payload.get("output") or {})
    summary = dict(output.get("summary") or {})
    run_id = str(
        checkpoint.get("run_id")
        or checkpoint.get("actor_run_id")
        or summary.get("run_id")
        or summary.get("actor_run_id")
        or ""
    ).strip()
    dataset_id = str(
        checkpoint.get("dataset_id")
        or checkpoint.get("default_dataset_id")
        or summary.get("dataset_id")
        or summary.get("default_dataset_id")
        or ""
    ).strip()
    return run_id, dataset_id


def _worker_is_pending_smoke_provider_webhook(
    worker: dict[str, Any],
    *,
    allow_terminal_marker_duplicate: bool = False,
) -> tuple[bool, str]:
    payload = dict(worker or {})
    if not _worker_needs_smoke_recovery(payload):
        return False, "worker_does_not_need_remote_recovery"
    status = str(payload.get("status") or "").strip().lower()
    if status in TERMINAL_WORKER_STATUSES:
        return False, "terminal_worker"
    if not allow_terminal_marker_duplicate and _worker_has_smoke_remote_terminal_marker(payload):
        return False, "remote_provider_terminal_event_already_seen"
    run_id, dataset_id = _worker_smoke_remote_identity(payload)
    if not run_id and not dataset_id:
        return False, "missing_remote_identity"
    checkpoint = dict(payload.get("checkpoint") or {})
    ready_epoch_ms = _safe_float(checkpoint.get("scripted_remote_ready_epoch_ms"))
    if ready_epoch_ms > 0.0 and time.time() * 1000 < ready_epoch_ms:
        return True, "remote_result_not_ready"
    return True, ""


def _worker_can_receive_smoke_provider_webhook(worker: dict[str, Any]) -> bool:
    payload = dict(worker or {})
    pending, reason = _worker_is_pending_smoke_provider_webhook(payload)
    return bool(pending and not reason)


def _smoke_shared_recovery_signal_payload() -> dict[str, Any]:
    """Return the only payload allowed across the API-to-daemon signal boundary."""

    return {}


def _smoke_provider_webhook_headers() -> dict[str, str]:
    token = (
        str(os.getenv("SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN") or "").strip()
        or str(os.getenv("SOURCING_PROVIDER_WEBHOOK_TOKEN") or "").strip()
        or str(os.getenv("APIFY_WEBHOOK_TOKEN") or "").strip()
    )
    if not token:
        return {}
    return {"X-Sourcing-Provider-Webhook-Token": token}


def _smoke_remote_finished_at_for_worker(worker: dict[str, Any]) -> str:
    checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    ready_epoch_ms = _safe_float(checkpoint.get("scripted_remote_ready_epoch_ms"))
    if ready_epoch_ms > 0.0:
        try:
            return datetime.fromtimestamp(ready_epoch_ms / 1000.0, tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            pass
    return datetime.now(timezone.utc).isoformat()


def _smoke_remote_started_at_for_worker(worker: dict[str, Any], finished_at: str) -> str:
    checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    provider_timings = dict(checkpoint.get("provider_timings") or {})
    actor_run_duration_ms = _safe_float(provider_timings.get("actor_run_duration_ms"))
    if actor_run_duration_ms <= 0.0:
        remote_wait_seconds = _safe_float(checkpoint.get("scripted_remote_wait_seconds"))
        if remote_wait_seconds > 0.0:
            actor_run_duration_ms = remote_wait_seconds * 1000.0
    finished = _parse_timestamp(finished_at)
    if finished is None or actor_run_duration_ms <= 0.0:
        return ""
    return (finished - timedelta(milliseconds=actor_run_duration_ms)).isoformat()


def _smoke_remote_provider_webhook_payload(
    worker: dict[str, Any],
    *,
    source: str = "provider_webhook",
    event_sequence: int = 1,
) -> dict[str, Any]:
    payload = dict(worker or {})
    run_id, dataset_id = _worker_smoke_remote_identity(payload)
    worker_id = _safe_int(payload.get("worker_id") or payload.get("workerId"))
    normalized_sequence = max(1, _safe_int(event_sequence))
    finished_at = _smoke_remote_finished_at_for_worker(payload)
    started_at = _smoke_remote_started_at_for_worker(payload, finished_at)
    event_data = {
        "actorRunId": run_id,
        "defaultDatasetId": dataset_id,
        "status": "SUCCEEDED",
        "finishedAt": finished_at,
    }
    if started_at:
        event_data["startedAt"] = started_at
    return {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "eventData": event_data,
        "owner_id": f"scripted-smoke-webhook-{run_id or dataset_id or worker_id or 'worker'}-{normalized_sequence}",
        "worker_scan_limit": 500,
        "total_limit": 4,
        "explicit_job_followup_rounds": 1,
        "source": source,
        "worker_id": worker_id,
    }


def _drive_smoke_remote_provider_webhook_recovery_once(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    event_sequence: int,
    include_watcher_duplicate: bool = False,
    primary_source: str = "provider_webhook",
    already_accepted_remote_keys: set[tuple[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    workers_payload = client.get(f"/api/jobs/{job_id}/workers")
    pending_workers: list[tuple[dict[str, Any], str]] = []
    target_workers: list[dict[str, Any]] = []
    accepted_remote_keys = set(already_accepted_remote_keys or set())
    for worker in list(workers_payload.get("agent_workers") or []):
        if not isinstance(worker, dict):
            continue
        normalized_worker = dict(worker)
        pending, reason = _worker_is_pending_smoke_provider_webhook(
            normalized_worker,
            allow_terminal_marker_duplicate=bool(include_watcher_duplicate),
        )
        if not pending:
            continue
        if reason:
            pending_workers.append((normalized_worker, reason))
            continue
        remote_key = _worker_smoke_remote_identity(normalized_worker)
        if remote_key in accepted_remote_keys:
            continue
        target_workers.append(normalized_worker)
    if not target_workers:
        deferred_events: list[dict[str, Any]] = []
        for worker, reason in pending_workers[:4]:
            deferred_events.append(
                {
                    "status": "provider_webhook_deferred",
                    "reason": reason,
                    "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                    "event_sequence": event_sequence,
                    "recovery_count": 0,
                    "webhook_to_response_ms": 0.0,
                }
            )
        return deferred_events, []
    headers = _smoke_provider_webhook_headers()
    events: list[dict[str, Any]] = []
    accepted_workers: list[dict[str, Any]] = []

    def _provider_event_durable_handoff_accepted(event: dict[str, Any]) -> bool:
        status = str(event.get("status") or "").strip()
        reason = str(event.get("reason") or "").strip()
        if status != "accepted":
            return False
        if (
            _safe_int(event.get("recovery_count")) > 0
            or _safe_int(event.get("recovery_dispatch_count")) > 0
            or shared_recovery_signal_count(dict(event.get("shared_recovery_signal") or {})) > 0
        ):
            return True
        if reason == "remote_provider_event_recovery_already_in_flight":
            return True
        if reason in {"no_matching_remote_wait_workers", "matching_remote_provider_workers_not_recoverable"}:
            return False
        # HTTP 202 proves the terminal checkpoint/lease handoff was accepted even
        # if the best-effort nudge failed; the daemon poll remains the recovery
        # backstop. This boolean is deliberately not shared-signal evidence.
        mode = str(event.get("mode") or "").strip()
        if mode == "shared_recovery_signal":
            return True
        return mode in {"", "async_recovery", "job_scoped_recovery"} and not reason

    def _post_event(worker: dict[str, Any], *, source: str, sequence: int) -> dict[str, Any]:
        request_started_at = time.perf_counter()
        try:
            result = client.post(
                _SMOKE_PROVIDER_WEBHOOK_PATH,
                _smoke_remote_provider_webhook_payload(
                    worker,
                    source=source,
                    event_sequence=sequence,
                ),
                headers=headers,
            )
            elapsed_ms = round((time.perf_counter() - request_started_at) * 1000, 2)
            shared_recovery_signal = dict(result.get("shared_recovery_signal") or {})
            return {
                "status": str(result.get("status") or ""),
                "reason": str(result.get("reason") or ""),
                "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                "event_sequence": sequence,
                "source": source,
                "mode": str(result.get("mode") or ""),
                "recovery_count": _safe_int(result.get("recovery_count")),
                "recovery_dispatch_count": _safe_int(result.get("recovery_dispatch_count")),
                "shared_recovery_signal_count": shared_recovery_signal_count(shared_recovery_signal),
                "shared_recovery_signal": shared_recovery_signal,
                "webhook_to_response_ms": elapsed_ms,
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - request_started_at) * 1000, 2)
            return {
                "status": "provider_webhook_failed",
                "reason": str(exc),
                "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                "event_sequence": sequence,
                "source": source,
                "recovery_count": 0,
                "webhook_to_response_ms": elapsed_ms,
            }

    def _post_provider_and_watcher_in_parallel(worker: dict[str, Any]) -> list[dict[str, Any]]:
        event_specs = (
            ("provider_webhook", event_sequence),
            ("local_provider_event_watcher", event_sequence + 1),
        )
        barrier = threading.Barrier(len(event_specs))
        results: list[dict[str, Any] | None] = [None for _ in event_specs]

        def _run(index: int, source: str, sequence: int) -> None:
            try:
                barrier.wait(timeout=2.0)
            except threading.BrokenBarrierError:
                pass
            results[index] = _post_event(worker, source=source, sequence=sequence)

        threads = [
            threading.Thread(
                target=_run,
                args=(index, source, sequence),
                name=f"smoke-provider-event-{source}-{index}",
                daemon=True,
            )
            for index, (source, sequence) in enumerate(event_specs)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60.0)
        normalized_results: list[dict[str, Any]] = []
        for index, result in enumerate(results):
            if result is None:
                source, sequence = event_specs[index]
                result = {
                    "status": "provider_webhook_failed",
                    "reason": "parallel_provider_event_thread_timeout",
                    "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                    "event_sequence": sequence,
                    "source": source,
                    "recovery_count": 0,
                    "recovery_dispatch_count": 0,
                    "shared_recovery_signal_count": 0,
                    "webhook_to_response_ms": 0.0,
                }
            normalized_results.append(result)
        return normalized_results

    normalized_primary_source = str(primary_source or "").strip() or "provider_webhook"

    def _post_ready_workers_in_parallel(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not workers:
            return []
        if include_watcher_duplicate and normalized_primary_source == "provider_webhook":
            worker_events: list[dict[str, Any]] = []
            for worker in workers:
                worker_events.extend(_post_provider_and_watcher_in_parallel(worker))
            return worker_events

        results: list[dict[str, Any] | None] = [None for _ in workers]
        barrier = threading.Barrier(len(workers)) if len(workers) > 1 else None

        def _run(index: int, worker: dict[str, Any]) -> None:
            if barrier is not None:
                try:
                    barrier.wait(timeout=2.0)
                except threading.BrokenBarrierError:
                    pass
            results[index] = _post_event(worker, source=normalized_primary_source, sequence=event_sequence + index)

        threads = [
            threading.Thread(
                target=_run,
                args=(index, worker),
                name=f"smoke-provider-event-worker-{_safe_int(worker.get('worker_id') or worker.get('workerId'))}",
                daemon=True,
            )
            for index, worker in enumerate(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60.0)
        normalized_results: list[dict[str, Any]] = []
        for index, result in enumerate(results):
            if result is None:
                worker = workers[index]
                result = {
                    "status": "provider_webhook_failed",
                    "reason": "parallel_provider_event_thread_timeout",
                    "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                    "event_sequence": event_sequence + index,
                    "source": normalized_primary_source,
                    "recovery_count": 0,
                    "recovery_dispatch_count": 0,
                    "shared_recovery_signal_count": 0,
                    "webhook_to_response_ms": 0.0,
                }
            normalized_results.append(result)
        return normalized_results

    accepted_worker_keys: set[int] = set()
    worker_events = _post_ready_workers_in_parallel(target_workers[:4])
    events.extend(worker_events)
    for worker in target_workers[:4]:
        worker_id = _safe_int(worker.get("worker_id") or worker.get("workerId"))
        accepted = any(
            _provider_event_durable_handoff_accepted(dict(event))
            for event in worker_events
            if _safe_int(event.get("worker_id")) == worker_id
        )
        if accepted and worker_id not in accepted_worker_keys:
            accepted_worker_keys.add(worker_id)
            accepted_workers.append(dict(worker))
            accepted_remote_keys.add(_worker_smoke_remote_identity(worker))
    return events, accepted_workers


def _drive_smoke_remote_provider_late_watcher_duplicates(
    client: HostedWorkflowSmokeClient,
    *,
    accepted_workers: list[dict[str, Any]],
    start_sequence: int = 2,
    duplicate_source: str = "local_provider_event_watcher",
) -> list[dict[str, Any]]:
    headers = _smoke_provider_webhook_headers()
    events: list[dict[str, Any]] = []
    seen_remote_keys: set[tuple[str, str]] = set()
    normalized_duplicate_source = str(duplicate_source or "").strip() or "local_provider_event_watcher"
    for index, worker in enumerate(list(accepted_workers or []), start=max(2, _safe_int(start_sequence))):
        checkpoint = dict(worker.get("checkpoint") or {})
        output = dict(worker.get("output") or {})
        summary = dict(output.get("summary") or {})
        remote_key = (
            str(checkpoint.get("run_id") or checkpoint.get("actor_run_id") or summary.get("run_id") or "").strip(),
            str(
                checkpoint.get("dataset_id")
                or checkpoint.get("default_dataset_id")
                or summary.get("dataset_id")
                or summary.get("default_dataset_id")
                or ""
            ).strip(),
        )
        if remote_key in seen_remote_keys:
            continue
        seen_remote_keys.add(remote_key)
        request_started_at = time.perf_counter()
        try:
            result = client.post(
                _SMOKE_PROVIDER_WEBHOOK_PATH,
                _smoke_remote_provider_webhook_payload(
                    dict(worker),
                    source=normalized_duplicate_source,
                    event_sequence=index,
                ),
                headers=headers,
            )
            elapsed_ms = round((time.perf_counter() - request_started_at) * 1000, 2)
            shared_recovery_signal = dict(result.get("shared_recovery_signal") or {})
            events.append(
                {
                    "status": str(result.get("status") or ""),
                    "reason": str(result.get("reason") or ""),
                    "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                    "event_sequence": index,
                    "source": normalized_duplicate_source,
                    "mode": str(result.get("mode") or ""),
                    "recovery_count": _safe_int(result.get("recovery_count")),
                    "recovery_dispatch_count": _safe_int(result.get("recovery_dispatch_count")),
                    "shared_recovery_signal_count": shared_recovery_signal_count(shared_recovery_signal),
                    "shared_recovery_signal": shared_recovery_signal,
                    "webhook_to_response_ms": elapsed_ms,
                }
            )
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - request_started_at) * 1000, 2)
            events.append(
                {
                    "status": "provider_watcher_late_failed",
                    "reason": str(exc),
                    "worker_id": _safe_int(worker.get("worker_id") or worker.get("workerId")),
                    "event_sequence": index,
                    "source": normalized_duplicate_source,
                    "recovery_count": 0,
                    "recovery_dispatch_count": 0,
                    "shared_recovery_signal_count": 0,
                    "webhook_to_response_ms": elapsed_ms,
                }
            )
    return events


def _smoke_shared_recovery_signal_record(
    response: dict[str, Any],
    *,
    tick: int | None = None,
    round_index: int | None = None,
    phase: str = "",
    recoverable_workers: list[dict[str, Any]] | None = None,
    recoverable_worker_ids: list[int] | None = None,
    materialization_wait_state: dict[str, Any] | None = None,
    allow_background_snapshot_full_materialization: bool = False,
    service_status_payload: dict[str, Any] | None = None,
    progress_observed: bool | None = None,
) -> dict[str, Any]:
    signal = dict(response.get("shared_recovery_signal") or {})
    signal_status = str(signal.get("status") or "").strip()
    record: dict[str, Any] = {
        "event_type": "shared_recovery_signal",
        "status": response.get("status"),
        "mode": str(response.get("mode") or "shared_recovery_signal"),
        "reason": str(response.get("reason") or ""),
        "signal_status": signal_status,
        "signal_reason": str(signal.get("reason") or ""),
        "signal_service_name": str(signal.get("service_name") or ""),
        "shared_recovery_signal_count": shared_recovery_signal_count(signal),
        "progress_observation_source": "worker_and_recovery_service_status",
    }
    normalized_phase = str(phase or "").strip()
    if normalized_phase:
        record["phase"] = normalized_phase
    if tick is not None:
        record["tick"] = int(tick)
    if round_index is not None:
        record["post_terminal_round"] = int(round_index) + 1
    if recoverable_workers is not None:
        record["recoverable_worker_count"] = len(recoverable_workers)
    normalized_worker_ids = [
        _safe_int(worker_id) for worker_id in list(recoverable_worker_ids or []) if _safe_int(worker_id) > 0
    ]
    if normalized_worker_ids:
        record["recoverable_worker_ids"] = normalized_worker_ids
    if materialization_wait_state is not None:
        record["materialization_wait_state"] = dict(materialization_wait_state or {})
    if allow_background_snapshot_full_materialization:
        record["allow_background_snapshot_full_materialization"] = True
    service_status_report = _smoke_recovery_service_status_report(service_status_payload)
    if service_status_report.get("service_count"):
        record["recovery_service_status"] = service_status_report
    if progress_observed is not None:
        record["progress_observed"] = bool(progress_observed)
    return record


def _fetch_smoke_recovery_service_status(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
) -> dict[str, Any]:
    try:
        return client.get(f"/api/workers/daemon/status?job_id={quote(str(job_id or ''))}&include_details=1")
    except Exception:
        return {}


def _smoke_recovery_observation_fingerprint(
    *,
    workers: list[dict[str, Any]],
    materialization_wait_state: dict[str, Any],
    service_status_payload: dict[str, Any],
) -> tuple[Any, ...]:
    worker_state = tuple(
        sorted(
            (
                _safe_int(worker.get("worker_id") or worker.get("workerId")),
                str(worker.get("status") or "").strip().lower(),
                str(worker.get("updated_at") or "").strip(),
            )
            for worker in workers
        )
    )
    item_state = tuple(
        sorted(
            (
                str(item.get("item_id") or item.get("command_id") or "").strip(),
                str(item.get("status") or "").strip().lower(),
                str(item.get("phase") or "").strip().lower(),
            )
            for key in (
                "handoff_items",
                "background_snapshot_full_materialization_items",
                "projection_facet_layering_items",
                "projection_person_search_index_items",
            )
            for item in list(materialization_wait_state.get(key) or [])
            if isinstance(item, dict)
        )
    )
    service_report = _smoke_recovery_service_status_report(service_status_payload)
    service_state = tuple(
        sorted(
            (
                str(role),
                str(status.get("status") or "").strip().lower(),
                _safe_int(status.get("tick")),
            )
            for role, status in dict(service_report.get("services") or {}).items()
            if isinstance(status, dict)
        )
    )
    return worker_state, item_state, service_state


def _recovery_runs_from_service_status_payload(
    payload: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Expose daemon-owned recovery ticks to smoke service metrics.

    Smoke can only signal the shared daemon. Daemon-owned ticks from the status
    endpoint are therefore the execution evidence used for service SLOs; a
    successful signal response is never treated as a completed recovery tick.
    """

    root = dict(payload or {})
    services: list[tuple[str, dict[str, Any]]] = []
    recovery_services = dict(root.get("recovery_services") or {})
    for role, status in recovery_services.items():
        if isinstance(status, dict):
            services.append((str(role or "").strip() or "unknown", dict(status)))
    if not services and (
        root.get("last_summary") or root.get("last_nonempty_summary") or root.get("historical_last_nonempty_summary")
    ):
        services.append(("worker_recovery", root))

    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for role, status in services:
        service_name = str(status.get("service_name") or role).strip() or role
        phase = (
            "job_scoped_recovery"
            if role == "job_scoped" or service_name.startswith("job-recovery-")
            else "service_daemon_status"
        )
        for summary_key in (
            "last_nonempty_summary",
            "historical_last_nonempty_summary",
            "last_summary",
            "activity_summary",
            "current_activity_summary",
        ):
            summary = dict(status.get(summary_key) or {})
            phase_metrics = {
                str(key): dict(value)
                for key, value in dict(summary.get("recovery_phase_metrics") or {}).items()
                if str(key).strip() and isinstance(value, dict)
            }
            if not phase_metrics:
                continue
            dedupe_key = (
                service_name,
                summary_key,
                json.dumps(phase_metrics, sort_keys=True, ensure_ascii=False),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            record: dict[str, Any] = {
                "phase": phase,
                "status": str(summary.get("status") or status.get("status") or "").strip(),
                "daemon_status": str(status.get("status") or "").strip(),
                "service_name": service_name,
                "service_role": role,
                "service_summary_source": summary_key,
                "service_tick": _safe_int(status.get("tick")),
                "service_updated_at": str(status.get("updated_at") or "").strip(),
                "recovery_phase_metrics": phase_metrics,
            }
            if bool(summary.get("durable_work_handoff_yield")):
                record["durable_work_handoff_yield"] = True
            if bool(summary.get("next_tick_requested")):
                record["next_tick_requested"] = True
            records.append(record)
    return records


def _smoke_recovery_service_status_report(payload: dict[str, Any] | None) -> dict[str, Any]:
    root = dict(payload or {})
    recovery_services = dict(root.get("recovery_services") or {})
    services: dict[str, dict[str, Any]] = {}
    for role, status in recovery_services.items():
        if not isinstance(status, dict):
            continue
        services[str(role or "").strip() or "unknown"] = {
            "service_name": str(status.get("service_name") or "").strip(),
            "status": str(status.get("status") or "").strip(),
            "tick": _safe_int(status.get("tick")),
            "updated_at": str(status.get("updated_at") or "").strip(),
            "has_last_nonempty_summary": bool(
                dict(status.get("last_nonempty_summary") or {}).get("recovery_phase_metrics")
            ),
            "has_historical_last_nonempty_summary": bool(
                dict(status.get("historical_last_nonempty_summary") or {}).get("recovery_phase_metrics")
            ),
        }
    return {
        "status": str(root.get("status") or "").strip(),
        "service_count": len(services),
        "services": services,
    }


def _post_terminal_materialization_wait_state(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return {"pending_count": 0, "seconds_until_next_ready": 0.0, "items": []}
    try:
        payload = client.get(f"/api/jobs/{normalized_job_id}/materialization-items")
    except Exception:
        return {"pending_count": 0, "seconds_until_next_ready": 0.0, "items": []}
    now = datetime.now(timezone.utc)
    pending_items: list[dict[str, Any]] = []
    handoff_pending_items: list[dict[str, Any]] = []
    background_snapshot_items: list[dict[str, Any]] = []
    projection_facet_layering_items: list[dict[str, Any]] = []
    projection_person_search_index_items: list[dict[str, Any]] = []
    seconds_until_ready: list[float] = []

    def _append_pending_item(
        *,
        item_id: str,
        item_kind: str,
        status: str,
        phase: str = "",
        not_before_at: str = "",
        last_error: str = "",
        source: str = "job_materialization_items",
    ) -> None:
        normalized_kind = str(item_kind or "").strip()
        normalized_status = str(status or "").strip().lower()
        if normalized_kind not in {
            "local_apply_closure",
            "board_visible_delta_apply",
            "snapshot_full_materialization",
            "projection_facet_layering_build",
            "projection_person_search_index_build",
        }:
            return
        if not normalized_status:
            return
        parsed_not_before = _parse_timestamp(not_before_at)
        wait_seconds = 0.0
        if parsed_not_before is not None and parsed_not_before > now:
            wait_seconds = round((parsed_not_before - now).total_seconds(), 3)
            seconds_until_ready.append(wait_seconds)
        pending_item = {
            "item_id": str(item_id or "").strip(),
            "item_kind": normalized_kind,
            "status": normalized_status,
            "phase": str(phase or "").strip(),
            "not_before_at": str(not_before_at or "").strip(),
            "seconds_until_ready": wait_seconds,
            "last_error": str(last_error or "").strip(),
            "source": str(source or "").strip() or "job_materialization_items",
        }
        pending_items.append(pending_item)
        if normalized_kind == "snapshot_full_materialization":
            background_snapshot_items.append(pending_item)
        elif normalized_kind == "projection_facet_layering_build":
            projection_facet_layering_items.append(pending_item)
        elif normalized_kind == "projection_person_search_index_build":
            projection_person_search_index_items.append(pending_item)
        else:
            handoff_pending_items.append(pending_item)

    for item in list(dict(payload or {}).get("job_materialization_items") or []):
        if not isinstance(item, dict):
            continue
        item_kind = str(item.get("item_kind") or "").strip()
        status = str(item.get("status") or "").strip().lower()
        if status not in _POST_TERMINAL_MATERIALIZATION_PENDING_STATUSES:
            continue
        _append_pending_item(
            item_id=str(item.get("item_id") or "").strip(),
            item_kind=item_kind,
            status=status,
            phase=str(item.get("phase") or "").strip(),
            not_before_at=str(item.get("not_before_at") or "").strip(),
            last_error=str(item.get("last_error") or "").strip(),
            source="job_materialization_items",
        )
    for command in list(dict(payload or {}).get("workflow_commands") or []):
        if not isinstance(command, dict):
            continue
        command_type = str(command.get("command_type") or "").strip()
        item_kind = _POST_TERMINAL_COMMAND_ITEM_KINDS.get(command_type, "")
        status = str(command.get("status") or "").strip().lower()
        if status not in _POST_TERMINAL_COMMAND_PENDING_STATUSES:
            continue
        command_payload = dict(command.get("payload") or {})
        _append_pending_item(
            item_id=str(command_payload.get("item_id") or command.get("command_id") or "").strip(),
            item_kind=item_kind,
            status=("running" if status in _POST_TERMINAL_COMMAND_RUNNING_STATUSES else status),
            phase=str(command_payload.get("stage_key") or command.get("owner") or "").strip(),
            not_before_at=str(command.get("not_before_at") or "").strip(),
            last_error=str(command.get("last_error") or "").strip(),
            source="workflow_commands",
        )
    return {
        "pending_count": len(pending_items),
        "handoff_pending_count": len(handoff_pending_items),
        "background_snapshot_full_materialization_pending_count": len(background_snapshot_items),
        "projection_facet_layering_pending_count": len(projection_facet_layering_items),
        "projection_person_search_index_pending_count": len(projection_person_search_index_items),
        "retry_wait_count": sum(1 for item in pending_items if item["status"] == "failed_retryable"),
        "handoff_retry_wait_count": sum(1 for item in handoff_pending_items if item["status"] == "failed_retryable"),
        "running_count": sum(
            1 for item in pending_items if item["status"] in _POST_TERMINAL_MATERIALIZATION_RUNNING_STATUSES
        ),
        "handoff_running_count": sum(
            1 for item in handoff_pending_items if item["status"] in _POST_TERMINAL_MATERIALIZATION_RUNNING_STATUSES
        ),
        "snapshot_full_materialization_pending_count": sum(
            1 for item in pending_items if item["item_kind"] == "snapshot_full_materialization"
        ),
        "seconds_until_next_ready": min(seconds_until_ready) if seconds_until_ready else 0.0,
        "items": pending_items[:10],
        "handoff_items": handoff_pending_items[:10],
        "background_snapshot_full_materialization_items": background_snapshot_items[:10],
        "projection_facet_layering_items": projection_facet_layering_items[:10],
        "projection_person_search_index_items": projection_person_search_index_items[:10],
    }


def _post_terminal_background_snapshot_item_records(
    materialization_items: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in list(materialization_items or []):
        if not isinstance(item, dict):
            continue
        item_kind = str(item.get("item_kind") or "").strip()
        if item_kind != "snapshot_full_materialization":
            continue
        status = str(item.get("status") or "").strip().lower()
        if status not in _POST_TERMINAL_MATERIALIZATION_PENDING_STATUSES:
            continue
        records.append(
            {
                "item_id": str(item.get("item_id") or "").strip(),
                "item_kind": item_kind,
                "status": status,
                "phase": str(item.get("phase") or "").strip(),
                "not_before_at": str(item.get("not_before_at") or "").strip(),
                "seconds_until_ready": 0.0,
                "last_error": str(item.get("last_error") or "").strip(),
            }
        )
    return records


def _synchronize_post_terminal_recovery_with_service_metrics(
    recovery_state: dict[str, Any],
    *,
    service_metrics: dict[str, Any],
    materialization_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not recovery_state:
        return {}
    synchronized = dict(recovery_state)
    snapshot_queue = dict(service_metrics.get("snapshot_full_materialization_queue") or {})
    if not bool(snapshot_queue.get("report_available")):
        return synchronized
    backlog_count = _safe_int(snapshot_queue.get("backlog_count"))
    existing_count = _safe_int(synchronized.get("background_snapshot_full_materialization_pending_count"))
    if backlog_count <= existing_count:
        return synchronized

    synchronized["background_snapshot_full_materialization_pending_count"] = backlog_count
    synchronized["background_snapshot_full_materialization_backlog_count"] = backlog_count
    synchronized["background_snapshot_full_materialization_queue"] = {
        key: snapshot_queue.get(key)
        for key in (
            "status_counts",
            "phase_counts",
            "queued_count",
            "running_count",
            "retryable_count",
            "waiting_prerequisite_count",
            "backlog_count",
            "retry_backlog_present",
            "stale_running_count",
            "stale_running_present",
        )
        if key in snapshot_queue
    }
    synchronized["background_snapshot_full_materialization_count_source"] = "service_metrics"
    item_records = _post_terminal_background_snapshot_item_records(materialization_items)
    if item_records:
        synchronized["background_snapshot_full_materialization_items"] = item_records[:10]
    if bool(synchronized.get("background_snapshot_full_materialization_drain_requested")) and backlog_count > 0:
        synchronized["settled"] = False
        synchronized["max_rounds_exhausted"] = bool(
            _safe_int(synchronized.get("round_count")) >= _safe_int(synchronized.get("max_rounds"))
        )
    return synchronized


def _settle_post_terminal_worker_recovery(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    poll_seconds: float,
    max_rounds: int = 8,
    drain_background_snapshot_full_materialization: bool = False,
    drain_projection_facet_layering: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], float, dict[str, Any]]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return (
            {},
            {},
            [],
            0.0,
            {
                "settled": True,
                "reason": "missing_job_id",
                "remaining_recoverable_worker_count": 0,
                "remaining_recoverable_worker_ids": [],
                "round_count": 0,
                "max_rounds": max(0, int(max_rounds or 0)),
                "max_rounds_exhausted": False,
                "no_progress_rounds": 0,
            },
        )
    signal_runs: list[dict[str, Any]] = []
    timings_ms = 0.0
    latest_job_payload: dict[str, Any] = {}
    latest_results_payload: dict[str, Any] = {}
    no_progress_rounds = 0
    normalized_max_rounds = max(0, int(max_rounds or 0))
    remaining_workers: list[dict[str, Any]] = []
    materialization_wait_state: dict[str, Any] = {}
    for round_index in range(normalized_max_rounds):
        workers_payload = client.get(f"/api/jobs/{normalized_job_id}/workers")
        workers = [dict(item) for item in list(workers_payload.get("agent_workers") or []) if isinstance(item, dict)]
        recoverable_workers = [worker for worker in workers if _worker_needs_smoke_recovery(worker)]
        materialization_wait_state = _post_terminal_materialization_wait_state(client, job_id=normalized_job_id)
        materialization_settle_pending_count = _safe_int(materialization_wait_state.get("handoff_pending_count"))
        if drain_background_snapshot_full_materialization:
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("snapshot_full_materialization_pending_count")
            )
        if drain_projection_facet_layering:
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("projection_facet_layering_pending_count")
            )
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("projection_person_search_index_pending_count")
            )
        if not recoverable_workers and materialization_settle_pending_count <= 0:
            remaining_workers = []
            break
        recoverable_worker_ids = [
            _safe_int(worker.get("worker_id"))
            for worker in recoverable_workers
            if _safe_int(worker.get("worker_id")) > 0
        ]
        service_status_before = _fetch_smoke_recovery_service_status(client, job_id=normalized_job_id)
        observation_before = _smoke_recovery_observation_fingerprint(
            workers=recoverable_workers,
            materialization_wait_state=materialization_wait_state,
            service_status_payload=service_status_before,
        )
        started_at = time.perf_counter()
        signal_response = client.post(
            "/api/workers/daemon/run-once",
            _smoke_shared_recovery_signal_payload(),
        )
        timings_ms += (time.perf_counter() - started_at) * 1000
        latest_job_payload = client.get(f"/api/jobs/{normalized_job_id}")
        latest_results_payload = _fetch_smoke_results_payload(
            client,
            job_id=normalized_job_id,
            job_payload=latest_job_payload,
            include_runtime_details=False,
            include_candidates=False,
        )
        remaining_workers_payload = client.get(f"/api/jobs/{normalized_job_id}/workers")
        remaining_workers = [
            dict(item)
            for item in list(remaining_workers_payload.get("agent_workers") or [])
            if isinstance(item, dict) and _worker_needs_smoke_recovery(dict(item))
        ]
        materialization_wait_state = _post_terminal_materialization_wait_state(client, job_id=normalized_job_id)
        service_status_after = _fetch_smoke_recovery_service_status(client, job_id=normalized_job_id)
        observation_after = _smoke_recovery_observation_fingerprint(
            workers=remaining_workers,
            materialization_wait_state=materialization_wait_state,
            service_status_payload=service_status_after,
        )
        progress_observed = observation_after != observation_before
        signal_runs.append(
            _smoke_shared_recovery_signal_record(
                signal_response,
                round_index=round_index,
                phase="post_terminal_signal",
                recoverable_workers=recoverable_workers,
                recoverable_worker_ids=recoverable_worker_ids,
                materialization_wait_state=materialization_wait_state,
                allow_background_snapshot_full_materialization=bool(drain_background_snapshot_full_materialization),
                service_status_payload=service_status_after,
                progress_observed=progress_observed,
            )
        )
        materialization_settle_pending_count = _safe_int(materialization_wait_state.get("handoff_pending_count"))
        if drain_background_snapshot_full_materialization:
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("snapshot_full_materialization_pending_count")
            )
        if drain_projection_facet_layering:
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("projection_facet_layering_pending_count")
            )
            materialization_settle_pending_count += _safe_int(
                materialization_wait_state.get("projection_person_search_index_pending_count")
            )
        if not remaining_workers and materialization_settle_pending_count <= 0:
            break
        if progress_observed:
            no_progress_rounds = 0
        else:
            wait_seconds = _safe_float(materialization_wait_state.get("seconds_until_next_ready"))
            if wait_seconds > 0.0:
                no_progress_rounds = 0
                time.sleep(min(5.0, max(0.05, wait_seconds, poll_seconds)))
                continue
            running_wait_count = _safe_int(materialization_wait_state.get("handoff_running_count"))
            if drain_background_snapshot_full_materialization:
                running_wait_count = _safe_int(materialization_wait_state.get("running_count"))
            if drain_projection_facet_layering:
                running_wait_count = max(
                    running_wait_count,
                    sum(
                        1
                        for item in list(materialization_wait_state.get("projection_facet_layering_items") or [])
                        if str(dict(item).get("status") or "").strip().lower() == "running"
                    ),
                    sum(
                        1
                        for item in list(materialization_wait_state.get("projection_person_search_index_items") or [])
                        if str(dict(item).get("status") or "").strip().lower() == "running"
                    ),
                )
            if running_wait_count > 0:
                no_progress_rounds = 0
                time.sleep(max(0.05, poll_seconds))
                continue
            no_progress_rounds += 1
            if no_progress_rounds >= 3:
                break
        time.sleep(max(0.05, poll_seconds))
    else:
        workers_payload = client.get(f"/api/jobs/{normalized_job_id}/workers")
        remaining_workers = [
            dict(item)
            for item in list(workers_payload.get("agent_workers") or [])
            if isinstance(item, dict) and _worker_needs_smoke_recovery(dict(item))
        ]
    remaining_worker_ids = [
        _safe_int(worker.get("worker_id")) for worker in remaining_workers if _safe_int(worker.get("worker_id")) > 0
    ]
    remaining_materialization_item_count = _safe_int(materialization_wait_state.get("handoff_pending_count"))
    background_snapshot_full_materialization_item_count = _safe_int(
        materialization_wait_state.get("background_snapshot_full_materialization_pending_count")
    )
    settled_materialization_item_count = remaining_materialization_item_count
    if drain_background_snapshot_full_materialization:
        settled_materialization_item_count += background_snapshot_full_materialization_item_count
    projection_facet_layering_item_count = _safe_int(
        materialization_wait_state.get("projection_facet_layering_pending_count")
    )
    projection_person_search_index_item_count = _safe_int(
        materialization_wait_state.get("projection_person_search_index_pending_count")
    )
    if drain_projection_facet_layering:
        settled_materialization_item_count += projection_facet_layering_item_count
        settled_materialization_item_count += projection_person_search_index_item_count
    recovery_state = {
        "settled": not remaining_worker_ids and settled_materialization_item_count <= 0,
        "remaining_recoverable_worker_count": len(remaining_worker_ids),
        "remaining_recoverable_worker_ids": remaining_worker_ids,
        "remaining_materialization_item_count": remaining_materialization_item_count,
        "remaining_materialization_items": list(materialization_wait_state.get("handoff_items") or []),
        "background_snapshot_full_materialization_pending_count": background_snapshot_full_materialization_item_count,
        "background_snapshot_full_materialization_items": list(
            materialization_wait_state.get("background_snapshot_full_materialization_items") or []
        ),
        "projection_facet_layering_pending_count": projection_facet_layering_item_count,
        "projection_facet_layering_items": list(
            materialization_wait_state.get("projection_facet_layering_items") or []
        ),
        "projection_person_search_index_pending_count": projection_person_search_index_item_count,
        "projection_person_search_index_items": list(
            materialization_wait_state.get("projection_person_search_index_items") or []
        ),
        "round_count": len(signal_runs),
        "max_rounds": normalized_max_rounds,
        "max_rounds_exhausted": bool(
            (remaining_worker_ids or settled_materialization_item_count > 0)
            and len(signal_runs) >= normalized_max_rounds
        ),
        "no_progress_rounds": no_progress_rounds,
        "background_snapshot_full_materialization_drain_requested": bool(
            drain_background_snapshot_full_materialization
        ),
        "projection_facet_layering_drain_requested": bool(drain_projection_facet_layering),
    }
    return latest_job_payload, latest_results_payload, signal_runs, round(timings_ms, 2), recovery_state


def _target_public_web_action_enabled(action: dict[str, Any] | None) -> bool:
    return bool(isinstance(action, dict) and action.get("enabled") is True)


def _company_public_web_action_enabled(action: dict[str, Any] | None) -> bool:
    return bool(isinstance(action, dict) and action.get("enabled") is True)


def _run_company_public_web_smoke_action(
    client: HostedWorkflowSmokeClient,
    *,
    action: dict[str, Any],
    case_name: str,
    explain: dict[str, Any],
) -> tuple[dict[str, Any], float]:
    timings_ms = 0.0
    request_preview = dict(explain.get("request_preview") or {})
    target_company = str(action.get("target_company") or request_preview.get("target_company") or "").strip()
    if not target_company:
        return {"status": "skipped", "reason": "missing_target_company"}, timings_ms
    payload = {
        "target_company": target_company,
        "source_families": list(action.get("source_families") or []),
        "seed_urls": list(action.get("seed_urls") or []),
        "collector_inputs": dict(action.get("collector_inputs") or {}),
        "collector_documents": list(action.get("collector_documents") or []),
        "collector_sources": list(action.get("collector_sources") or []),
        "options": {
            "collection_mode": str(action.get("collection_mode") or "collector_bundle").strip() or "collector_bundle",
            **dict(action.get("options") or {}),
        },
        "force_refresh": bool(action.get("force_refresh", True)),
        "refresh_nonce": str(action.get("refresh_nonce") or f"{case_name}-{target_company}").strip(),
        "requested_by": str(action.get("requested_by") or "hosted-smoke").strip() or "hosted-smoke",
    }
    started_at = time.perf_counter()
    refresh_result = client.post("/api/company-assets/public-web", payload)
    timings_ms += (time.perf_counter() - started_at) * 1000
    started_at = time.perf_counter()
    list_result = client.get(f"/api/company-assets/public-web?target_company={quote(target_company)}&limit=1000")
    timings_ms += (time.perf_counter() - started_at) * 1000
    return (
        {
            "status": refresh_result.get("status"),
            "target_company": target_company,
            "refresh": refresh_result,
            "list": list_result,
        },
        round(timings_ms, 2),
    )


def _latest_public_web_batch_from_poll(payload: dict[str, Any], *, batch_id: str) -> dict[str, Any]:
    normalized_batch_id = str(batch_id or "").strip()
    for item in list(dict(payload or {}).get("batches") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("batch_id") or "").strip() == normalized_batch_id:
            return dict(item)
    return {}


def _target_public_web_batch_terminal(batch: dict[str, Any]) -> bool:
    status = str(dict(batch or {}).get("status") or "").strip().lower()
    return status in {"completed", "completed_with_errors", "needs_review", "failed", "cancelled", "canceled"}


def _workflow_commands_from_target_public_web_action(action: dict[str, Any] | None) -> list[dict[str, Any]]:
    commands: list[dict[str, Any]] = []
    seen_command_ids: set[str] = set()

    def _append(value: Any) -> None:
        if not isinstance(value, dict):
            return
        command = dict(value)
        if str(command.get("command_type") or "").strip() != CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE:
            return
        command_id = str(command.get("command_id") or "").strip()
        dedupe_key = command_id or str(command.get("idempotency_key") or "").strip()
        if dedupe_key and dedupe_key in seen_command_ids:
            return
        if dedupe_key:
            seen_command_ids.add(dedupe_key)
        commands.append(command)

    payload = dict(action or {})
    _append(payload.get("workflow_command"))
    search = dict(payload.get("search") or {})
    _append(search.get("workflow_command"))
    worker_summary = dict(payload.get("worker_summary") or search.get("worker_summary") or {})
    _append(worker_summary.get("workflow_command"))
    for key in ("items", "results", "command_results"):
        for item in list(worker_summary.get(key) or []):
            if isinstance(item, dict):
                _append(dict(item).get("workflow_command"))
    return commands


def _run_target_public_web_smoke_action(
    client: HostedWorkflowSmokeClient,
    *,
    source_job_id: str,
    action: dict[str, Any],
    poll_seconds: float,
    case_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], float]:
    """Run a real CRM-owned Public Web action after a workflow result exists."""

    normalized_job_id = str(source_job_id or "").strip()
    timings_ms = 0.0
    if not normalized_job_id:
        return {"status": "skipped", "reason": "missing_source_job_id"}, [], timings_ms
    import_limit = max(1, _safe_int(action.get("import_limit")) or _safe_int(action.get("record_limit")) or 1)
    record_limit = max(1, _safe_int(action.get("record_limit")) or import_limit)
    max_recovery_rounds = max(1, _safe_int(action.get("max_recovery_rounds")) or 12)
    requested_by = str(action.get("requested_by") or "hosted-smoke").strip() or "hosted-smoke"

    started_at = time.perf_counter()
    projection_link = client.get(f"/api/runs/{quote(normalized_job_id)}/projection-link")
    timings_ms += (time.perf_counter() - started_at) * 1000
    projection_id = str(projection_link.get("projection_id") or "").strip()
    if not projection_id:
        return (
            {
                "status": "invalid",
                "reason": "run_projection_link_missing",
                "projection_link": projection_link,
            },
            [],
            round(timings_ms, 2),
        )

    started_at = time.perf_counter()
    projection_page = client.get(
        f"/api/projections/{quote(projection_id)}/candidates?offset=0&limit={min(import_limit, record_limit)}"
    )
    timings_ms += (time.perf_counter() - started_at) * 1000
    projection_candidates = [
        dict(item) for item in list(projection_page.get("candidates") or []) if isinstance(item, dict)
    ]
    candidate_identity_keys = [
        str(record.get("candidate_identity_key") or "").strip()
        for record in projection_candidates[:record_limit]
        if str(record.get("candidate_identity_key") or "").strip()
    ]
    if not candidate_identity_keys:
        return (
            {
                "status": "invalid",
                "reason": "no_projection_candidates_available_for_crm_public_web",
                "projection_id": projection_id,
                "projection_page": {
                    "status": projection_page.get("status"),
                    "filtered_candidate_count": projection_page.get("filtered_candidate_count"),
                    "candidate_count": len(projection_candidates),
                },
            },
            [],
            round(timings_ms, 2),
        )

    crm_records: list[dict[str, Any]] = []
    for index, candidate_identity_key in enumerate(candidate_identity_keys, start=1):
        started_at = time.perf_counter()
        add_result = client.post(
            "/api/crm/records",
            {
                "projection_id": projection_id,
                "candidate_identity_key": candidate_identity_key,
                "idempotency_key": (
                    str(action.get("crm_idempotency_prefix") or "").strip() or f"smoke:{case_name}:{normalized_job_id}"
                )
                + f":{index}:{candidate_identity_key}",
                "stage": str(action.get("crm_stage") or "researching").strip() or "researching",
                "source_reason": "target_public_web_smoke_action",
            },
        )
        timings_ms += (time.perf_counter() - started_at) * 1000
        crm_record = dict(add_result.get("crm_record") or {})
        crm_record_id = str(crm_record.get("crm_record_id") or crm_record.get("record_id") or "").strip()
        if crm_record_id:
            crm_records.append(crm_record)
    record_ids = [
        str(record.get("crm_record_id") or record.get("record_id") or "").strip()
        for record in crm_records[:record_limit]
        if str(record.get("crm_record_id") or record.get("record_id") or "").strip()
    ]
    if not record_ids:
        return (
            {
                "status": "invalid",
                "reason": "no_crm_records_created_for_public_web_action",
                "projection_id": projection_id,
                "candidate_identity_keys": candidate_identity_keys,
            },
            [],
            round(timings_ms, 2),
        )

    search_payload = {
        "crm_record_ids": record_ids,
        "requested_by": requested_by,
        "force_refresh": bool(action.get("force_refresh", True)),
        "refresh_nonce": str(action.get("refresh_nonce") or f"{case_name}-{normalized_job_id}").strip(),
        "options": dict(action.get("options") or {}),
    }
    if "metadata" in action and isinstance(action.get("metadata"), dict):
        search_payload["metadata"] = dict(action.get("metadata") or {})
    started_at = time.perf_counter()
    search_result = client.post("/api/crm/records/public-web-search", search_payload)
    timings_ms += (time.perf_counter() - started_at) * 1000
    batch = dict(search_result.get("batch") or {})
    batch_id = str(batch.get("batch_id") or "").strip()
    public_web_job_id = str(dict(search_result.get("job") or {}).get("job_id") or "").strip()
    signal_runs: list[dict[str, Any]] = []
    latest_poll: dict[str, Any] = {}
    for round_index in range(max_recovery_rounds):
        if batch_id:
            started_at = time.perf_counter()
            latest_poll = client.post("/api/crm/records/public-web-search/poll", {"batch_id": batch_id})
            timings_ms += (time.perf_counter() - started_at) * 1000
            latest_batch = _latest_public_web_batch_from_poll(latest_poll, batch_id=batch_id)
            if latest_batch and _target_public_web_batch_terminal(latest_batch):
                batch = latest_batch
                break
        if not public_web_job_id:
            break
        workers_payload = client.get(f"/api/jobs/{public_web_job_id}/workers")
        recoverable_workers = [
            dict(item)
            for item in list(workers_payload.get("agent_workers") or [])
            if isinstance(item, dict) and _worker_needs_smoke_recovery(dict(item))
        ]
        if not recoverable_workers:
            if batch_id:
                latest_poll = client.post("/api/crm/records/public-web-search/poll", {"batch_id": batch_id})
                latest_batch = _latest_public_web_batch_from_poll(latest_poll, batch_id=batch_id)
                if latest_batch:
                    batch = latest_batch
            break
        worker_ids = [
            _safe_int(worker.get("worker_id"))
            for worker in recoverable_workers
            if _safe_int(worker.get("worker_id")) > 0
        ]
        started_at = time.perf_counter()
        signal_response = client.post(
            "/api/workers/daemon/run-once",
            _smoke_shared_recovery_signal_payload(),
        )
        timings_ms += (time.perf_counter() - started_at) * 1000
        service_status = _fetch_smoke_recovery_service_status(client, job_id=public_web_job_id)
        signal_runs.append(
            _smoke_shared_recovery_signal_record(
                signal_response,
                round_index=round_index,
                phase="target_public_web_action_signal",
                recoverable_workers=recoverable_workers,
                recoverable_worker_ids=worker_ids,
                service_status_payload=service_status,
            )
        )
        time.sleep(max(0.05, poll_seconds))
    if batch_id:
        latest_poll = client.post("/api/crm/records/public-web-search/poll", {"batch_id": batch_id})
        latest_batch = _latest_public_web_batch_from_poll(latest_poll, batch_id=batch_id)
        if latest_batch:
            batch = latest_batch
    return (
        {
            "status": "completed" if _target_public_web_batch_terminal(batch) else "incomplete",
            "source_job_id": normalized_job_id,
            "import": {
                "status": "crm_records_created",
                "source_projection_id": projection_id,
                "imported_count": len(crm_records),
                "eligible_count": len(candidate_identity_keys),
            },
            "search": {
                "status": search_result.get("status"),
                "batch_id": batch_id,
                "job_id": public_web_job_id,
                "crm_record_ids": record_ids,
                "record_ids": record_ids,
                "workflow_command": dict(search_result.get("workflow_command") or {}),
                "worker_summary": dict(search_result.get("worker_summary") or {}),
            },
            "workflow_command": dict(search_result.get("workflow_command") or {}),
            "worker_summary": dict(search_result.get("worker_summary") or {}),
            "latest_batch": batch,
            "latest_poll": latest_poll,
            "recovery": {
                "round_count": len(signal_runs),
                "max_rounds": max_recovery_rounds,
                "settled": _target_public_web_batch_terminal(batch),
            },
        },
        signal_runs,
        round(timings_ms, 2),
    )


def _explain_full_roster_task_metadata(explain: dict[str, Any]) -> dict[str, Any]:
    plan = dict(explain.get("plan") or {})
    for task in list(plan.get("acquisition_tasks") or []):
        if not isinstance(task, dict):
            continue
        if str(task.get("task_type") or "").strip() != "acquire_full_roster":
            continue
        return dict(task.get("metadata") or {})
    return {}


def _explain_task_metadata(explain: dict[str, Any], task_type: str) -> dict[str, Any]:
    plan = dict(explain.get("plan") or {})
    for task in list(plan.get("acquisition_tasks") or []):
        if not isinstance(task, dict):
            continue
        if str(task.get("task_type") or "").strip() != task_type:
            continue
        return dict(task.get("metadata") or {})
    return {}


def _extract_query_bundle_queries(values: Any) -> list[str]:
    queries: list[str] = []
    for bundle in list(values or []):
        if not isinstance(bundle, dict):
            continue
        for query in list(bundle.get("queries") or []):
            text = " ".join(str(query or "").split()).strip()
            if text:
                queries.append(text)
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        lowered = query.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(query)
    return deduped


def _build_smoke_explain_digest(
    *,
    explain: dict[str, Any],
    effective_payload: dict[str, Any],
) -> dict[str, Any]:
    explain_full_roster_task_metadata = _explain_full_roster_task_metadata(explain)
    explain_full_roster_shard_policy = dict(
        explain_full_roster_task_metadata.get("company_employee_shard_policy") or {}
    )
    explain_former_task_metadata = _explain_task_metadata(explain, "acquire_former_search_seed")
    explain_current_filter_hints = dict(explain_full_roster_task_metadata.get("filter_hints") or {})
    explain_former_filter_hints = dict(explain_former_task_metadata.get("filter_hints") or {})
    explain_dispatch_preview = dict(explain.get("dispatch_preview") or {})
    explain_matched_job = dict(explain_dispatch_preview.get("matched_job") or {})
    explain_asset_reuse_plan = dict(explain.get("asset_reuse_plan") or {})
    explain_request_after_dispatch = dict(explain_dispatch_preview.get("request_after_dispatch_hints") or {})
    explain_request_after_dispatch_preferences = dict(explain_request_after_dispatch.get("execution_preferences") or {})
    explain_dispatch_explanation = dict(explain_dispatch_preview.get("request_family_match_explanation") or {})
    return {
        "status": explain.get("status"),
        "target_company": ((explain.get("request_preview") or {}).get("target_company")),
        "target_scope": ((explain.get("request_preview") or {}).get("target_scope")),
        "keywords": ((explain.get("request_preview") or {}).get("keywords")),
        "analysis_stage_mode": (
            explain.get("analysis_stage_mode") or effective_payload.get("analysis_stage_mode") or "single_stage"
        ),
        "org_scale_band": ((explain.get("organization_execution_profile") or {}).get("org_scale_band")),
        "default_acquisition_mode": (
            (explain.get("organization_execution_profile") or {}).get("default_acquisition_mode")
        ),
        "planner_mode": explain_asset_reuse_plan.get("planner_mode"),
        "requires_delta_acquisition": explain_asset_reuse_plan.get("requires_delta_acquisition"),
        "dispatch_strategy": explain_dispatch_preview.get("strategy"),
        "reuse_basis": str(
            explain_dispatch_preview.get("reuse_basis") or explain_dispatch_explanation.get("reuse_basis") or ""
        ),
        "dispatch_matched_job_id": explain_matched_job.get("job_id"),
        "dispatch_matched_job_status": explain_matched_job.get("status"),
        "asset_reuse_baseline_snapshot_id": str(explain_asset_reuse_plan.get("baseline_snapshot_id") or ""),
        "dispatch_matched_snapshot_id": str(explain_dispatch_preview.get("matched_snapshot_id") or ""),
        "request_delta_baseline_snapshot_id": str(
            explain_request_after_dispatch_preferences.get("delta_baseline_snapshot_id") or ""
        ),
        "current_lane": (((explain.get("lane_preview") or {}).get("current") or {}).get("planned_behavior")),
        "former_lane": (((explain.get("lane_preview") or {}).get("former") or {}).get("planned_behavior")),
        "runtime_tuning_profile": (
            (((explain.get("request_preview") or {}).get("intent_axes") or {}).get("fallback_policy") or {}).get(
                "runtime_tuning_profile"
            )
        ),
        "harvest_profile_actor_global_inflight": dict(effective_payload.get("execution_preferences") or {}).get(
            "harvest_profile_actor_global_inflight"
        ),
        "harvest_profile_batch_submit_global_inflight": dict(effective_payload.get("execution_preferences") or {}).get(
            "harvest_profile_batch_submit_global_inflight"
        ),
        "effective_acquisition_mode": (
            (explain.get("effective_execution_semantics") or {}).get("effective_acquisition_mode")
        ),
        "default_results_mode": ((explain.get("effective_execution_semantics") or {}).get("default_results_mode")),
        "baseline_directional_local_reuse_eligible": bool(
            explain_asset_reuse_plan.get("baseline_directional_local_reuse_eligible")
        ),
        "plan_primary_strategy_type": (
            ((explain.get("plan") or {}).get("acquisition_strategy") or {}).get("strategy_type")
        ),
        "plan_company_employee_shard_strategy": explain_full_roster_task_metadata.get(
            "company_employee_shard_strategy"
        ),
        "plan_company_employee_shard_policy_allow_overflow_partial": bool(
            explain_full_roster_shard_policy.get("allow_overflow_partial")
        ),
        "plan_current_task_strategy_type": explain_full_roster_task_metadata.get("strategy_type"),
        "plan_current_search_seed_queries": explain_full_roster_task_metadata.get("search_seed_queries"),
        "plan_current_filter_keywords": explain_current_filter_hints.get("keywords"),
        "plan_current_query_bundle_queries": _extract_query_bundle_queries(
            explain_full_roster_task_metadata.get("search_query_bundles") or []
        ),
        "plan_former_task_strategy_type": explain_former_task_metadata.get("strategy_type"),
        "plan_former_search_seed_queries": explain_former_task_metadata.get("search_seed_queries"),
        "plan_former_filter_keywords": explain_former_filter_hints.get("keywords"),
        "plan_former_query_bundle_queries": _extract_query_bundle_queries(
            explain_former_task_metadata.get("search_query_bundles") or []
        ),
        "timings_ms": explain.get("timings_ms"),
        "timing_breakdown_ms": explain.get("timing_breakdown_ms"),
    }


def _http_error_payload(exc: urllib_error.HTTPError) -> dict[str, Any]:
    try:
        raw_body = exc.read().decode("utf-8")
    except Exception:
        raw_body = ""
    if not raw_body.strip():
        return {}
    try:
        decoded = json.loads(raw_body)
    except json.JSONDecodeError:
        return {"raw_error_body": raw_body}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _projection_id_from_legacy_cutover_payload(payload: dict[str, Any]) -> str:
    source = dict(payload or {})
    if str(source.get("reason") or "").strip() != "legacy_job_result_endpoint_retired":
        return ""
    projection_id = str(source.get("projection_id") or "").strip()
    if projection_id:
        return projection_id
    retirement = dict(source.get("retirement") or {})
    return str(retirement.get("projection_id") or "").strip()


def _projection_link_for_smoke(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    legacy_error_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    error_payload = dict(legacy_error_payload or {})
    projection_id = _projection_id_from_legacy_cutover_payload(error_payload)
    if projection_id:
        return {
            "status": "ready",
            "run_id": normalized_job_id,
            "projection_id": projection_id,
            "source": "legacy_cutover_payload",
            "legacy_cutover": error_payload,
        }
    if not normalized_job_id:
        return {"status": "invalid", "reason": "job_id_required"}
    return client.get(f"/api/runs/{quote(normalized_job_id)}/projection-link")


def _projection_payloads_for_smoke(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    legacy_error_payload: dict[str, Any] | None = None,
    offset: int = 0,
    limit: int = 24,
    candidate_query_suffix: str = "",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    link_payload = _projection_link_for_smoke(
        client,
        job_id=job_id,
        legacy_error_payload=legacy_error_payload,
    )
    projection_id = str(link_payload.get("projection_id") or "").strip()
    if not projection_id:
        return link_payload, {}, {}
    projection_payload = client.get(f"/api/projections/{quote(projection_id)}")
    query_suffix = str(candidate_query_suffix or "").strip()
    if query_suffix and not query_suffix.startswith("&"):
        query_suffix = "&" + query_suffix.lstrip("?")
    candidate_page = client.get(
        f"/api/projections/{quote(projection_id)}/candidates"
        f"?offset={max(0, int(offset or 0))}&limit={max(1, min(250, int(limit or 24)))}{query_suffix}"
    )
    return link_payload, projection_payload, candidate_page


def _flatten_projection_candidate_for_smoke(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    public_summary = dict(payload.get("public_summary") or {})
    projection_metrics = dict(payload.get("projection_metrics") or {})
    candidate_id = str(payload.get("candidate_id") or public_summary.get("candidate_id") or "").strip()
    candidate_identity_key = str(payload.get("candidate_identity_key") or "").strip()
    employment_status = str(
        payload.get("employment_scope") or public_summary.get("employment_status") or public_summary.get("status") or ""
    ).strip()
    flattened = {
        **public_summary,
        "candidate_id": candidate_id or candidate_identity_key,
        "id": candidate_id or candidate_identity_key,
        "candidate_identity_key": candidate_identity_key,
        "person_identity_key": str(payload.get("person_identity_key") or "").strip(),
        "profile_url_key": str(payload.get("profile_url_key") or "").strip(),
        "projection_id": str(payload.get("projection_id") or "").strip(),
        "rank_index": _safe_int(payload.get("rank_index")),
        "rank_key": str(payload.get("rank_key") or "").strip(),
        "lane": str(payload.get("lane") or "").strip(),
        "source_dataset": str(payload.get("source_shard_key") or payload.get("lane") or "").strip(),
        "source_shard_key": str(payload.get("source_shard_key") or "").strip(),
        "source_run_id": str(payload.get("source_run_id") or "").strip(),
        "employment_status": employment_status,
        "row_readiness": str(payload.get("row_readiness") or "").strip(),
        "profile_readiness": str(payload.get("profile_readiness") or "").strip(),
        "card_readiness": str(payload.get("card_readiness") or "").strip(),
        "visibility_state": str(payload.get("visibility_state") or "").strip(),
        "crm_overlay_summary": dict(payload.get("crm_overlay_summary") or {}),
        "projection_metrics": projection_metrics,
    }
    if "has_profile_detail" not in flattened:
        flattened["has_profile_detail"] = bool(
            projection_metrics.get("has_profile_detail")
            or str(payload.get("profile_readiness") or "").strip().lower() in {"ready", "complete", "completed"}
        )
    if "needs_profile_completion" not in flattened:
        flattened["needs_profile_completion"] = bool(
            projection_metrics.get("needs_profile_completion")
            or str(payload.get("profile_readiness") or "").strip().lower() not in {"ready", "complete", "completed"}
        )
    return flattened


def _projection_profile_progress_for_smoke(projection: dict[str, Any]) -> dict[str, Any]:
    counts = dict(projection.get("counts") or {})
    readiness = dict(projection.get("readiness") or {})
    total_count = max(
        _safe_int(counts.get("profile_fetch_required_count")),
        _safe_int(readiness.get("profile_required_count")),
    )
    fetched_count = max(
        _safe_int(counts.get("profile_fetched_count")),
        _safe_int(readiness.get("profile_ready_count")),
    )
    return {
        "total_url_count": total_count,
        "fetched_url_count": fetched_count,
        "queued_url_count": max(0, total_count - fetched_count),
        "failed_retryable_url_count": 0,
        "deferred_url_count": 0,
        "count_scope": str(counts.get("count_scope") or readiness.get("count_scope") or "exact_projection"),
    }


def _projection_board_runtime_state_for_smoke(
    *,
    job_id: str,
    projection_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
    progress_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    progress_board = dict(dict(progress_payload or {}).get("board_runtime_state") or {})
    projection = dict(projection_payload.get("projection") or {})
    counts = dict(projection.get("counts") or {})
    readiness = dict(projection.get("readiness") or {})
    expected_count = max(
        _safe_int(projection.get("visible_member_count")),
        _safe_int(counts.get("result_count")),
        _safe_int(counts.get("candidate_count")),
        _safe_int(candidate_page_payload.get("total_candidates")),
    )
    row_count = max(_safe_int(readiness.get("row_count")), expected_count)
    profile_required = max(
        _safe_int(counts.get("profile_fetch_required_count")),
        _safe_int(readiness.get("profile_required_count")),
    )
    profile_ready = max(
        _safe_int(counts.get("profile_fetched_count")),
        _safe_int(readiness.get("profile_ready_count")),
    )
    card_ready = max(
        _safe_int(counts.get("card_materialized_count")),
        _safe_int(readiness.get("card_ready_count")),
        profile_ready,
    )
    facet_summary = dict(candidate_page_payload.get("facet_summary") or {})
    facet_summary_status = str(facet_summary.get("status") or "unavailable").strip()
    candidate_filter_contract = dict(candidate_page_payload.get("filter_contract") or {})
    facet_count_scope = str(
        candidate_filter_contract.get("facet_count_scope")
        or facet_summary.get("count_scope")
        or ("exact_projection" if facet_summary_status == "complete" else "unavailable")
    ).strip()
    facet_summary_scope = str(candidate_page_payload.get("facet_summary_scope") or "").strip()
    if not facet_summary_scope:
        facet_summary_scope = "global_full_population" if facet_summary_status == "complete" else "unavailable"
    facet_summary_candidate_count = expected_count if facet_summary_status == "complete" else 0
    layer_count = sum(
        _safe_int(dict(item).get("count")) for item in list(facet_summary.get("layers") or []) if isinstance(item, dict)
    )
    layering_status = str(
        readiness.get("layering_status")
        or counts.get("layering_status")
        or ("completed" if facet_summary_status == "complete" and layer_count > 0 else "")
    ).strip()
    if not layering_status and facet_summary_status == "complete":
        layering_status = "completed"
    row_publication_watermark = str(projection.get("updated_at") or projection.get("published_at") or "").strip()
    if progress_board:
        watermark_parts = str(progress_board.get("row_publication_watermark") or "").split("|")
        if len(watermark_parts) >= 4:
            watermark_parts[-1] = layering_status or str(progress_board.get("layering_status") or "").strip()
            row_publication_watermark = "|".join(watermark_parts)
        elif str(progress_board.get("row_publication_watermark") or "").strip():
            row_publication_watermark = str(progress_board.get("row_publication_watermark") or "").strip()
    filter_contract = {
        **dict(progress_board.get("filter_contract") or {}),
        **candidate_filter_contract,
    }
    if facet_count_scope:
        filter_contract["facet_count_scope"] = facet_count_scope
    if "source" not in filter_contract:
        filter_contract["source"] = "backend_board_runtime_state"
    filter_contract.setdefault("row_filter_scope", "backend_filtered_served_population")
    filter_contract.setdefault("backend_filtered_paging_supported", True)
    projection_state = {
        "schema_version": "projection_smoke_v1",
        "job_id": str(job_id or projection.get("source_run_id") or "").strip(),
        "result_mode": "asset_population",
        "phase": "current_snapshot_serving" if row_count >= expected_count else "projection_serving",
        "publication_status": "complete" if row_count >= expected_count else "partial",
        "expected_candidate_count": expected_count,
        "served_candidate_count": row_count,
        "published_candidate_count": row_count,
        "display_ready_candidate_count": max(card_ready, row_count if profile_required <= 0 else 0),
        "row_hydration_target_count": expected_count,
        "baseline_candidate_count": _safe_int(counts.get("baseline_candidate_count")),
        "delta_profile_required_count": profile_required,
        "delta_profile_fetched_count": profile_ready,
        "delta_profile_materialized_count": card_ready,
        "delta_profile_board_visible_count": card_ready,
        "delta_profile_denominator_promoted": profile_required > 0,
        "row_publication_sequence": "projection",
        "row_publication_tier": "canonical_projection",
        "row_publication_watermark": row_publication_watermark,
        "facet_summary_status": facet_summary_status,
        "facet_summary_scope": facet_summary_scope,
        "facet_summary_candidate_count": facet_summary_candidate_count,
        "layering_status": layering_status,
        "sync_status_text": f"候选人发现 {row_count}/{expected_count}",
        "profile_fetch_status_text": f"新增 LinkedIn Profile 已取回 {profile_ready}/{profile_required}",
        "card_materialization_status_text": f"卡片详情已合入看板 {card_ready}/{profile_required}",
        "filter_contract": filter_contract,
    }
    if not progress_board:
        return projection_state
    # Projection routes do not expose every legacy board-runtime field yet.
    # Use /progress as a shape template, but let the projection/candidate page
    # remain authoritative for global facet/layering/filter readiness so smoke
    # parity cannot be polluted by a stale progress sample captured during
    # migration-era endpoint synthesis.
    return {
        **progress_board,
        "facet_summary_status": projection_state["facet_summary_status"],
        "facet_summary_scope": projection_state["facet_summary_scope"],
        "facet_summary_candidate_count": projection_state["facet_summary_candidate_count"],
        "layering_status": projection_state["layering_status"],
        "row_publication_watermark": projection_state["row_publication_watermark"],
        "filter_contract": projection_state["filter_contract"],
    }


def _build_projection_smoke_payloads(
    *,
    job_id: str,
    job_payload: dict[str, Any] | None,
    projection_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
    progress_payload: dict[str, Any] | None = None,
    include_candidates: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    projection = dict(projection_payload.get("projection") or {})
    rows = [
        _flatten_projection_candidate_for_smoke(dict(item))
        for item in list(candidate_page_payload.get("candidates") or [])
        if isinstance(item, dict)
    ]
    total_candidates = max(
        _safe_int(candidate_page_payload.get("total_candidates")),
        _safe_int(projection.get("visible_member_count")),
        _safe_int(dict(projection.get("counts") or {}).get("result_count")),
    )
    profile_progress = _projection_profile_progress_for_smoke(projection)
    board_runtime_state = _projection_board_runtime_state_for_smoke(
        job_id=job_id,
        projection_payload=projection_payload,
        candidate_page_payload=candidate_page_payload,
        progress_payload=progress_payload,
    )
    lifecycle = dict(dict(progress_payload or {}).get("result_view_lifecycle") or {})
    result_job = dict(dict(job_payload or {}).get("job") or job_payload or {})
    result_job.setdefault("job_id", str(job_id or projection.get("source_run_id") or "").strip())
    asset_population = {
        "available": True,
        "source_kind": "serving_projection",
        "source_projection_id": str(projection.get("projection_id") or "").strip(),
        "target_company": str(dict(projection.get("scope_spec") or {}).get("target_company") or "").strip(),
        "snapshot_id": str(dict(projection.get("scope_spec") or {}).get("snapshot_id") or "").strip(),
        "asset_view": str(dict(projection.get("scope_spec") or {}).get("asset_view") or "canonical_projection").strip(),
        "candidate_count": total_candidates,
        "profile_fetch_progress": profile_progress,
        "card_materialization_summary": {
            "card_ready_count": _safe_int(board_runtime_state.get("delta_profile_board_visible_count")),
            "profile_required_count": _safe_int(board_runtime_state.get("delta_profile_required_count")),
        },
        "facet_summary": dict(candidate_page_payload.get("facet_summary") or {}),
        "facet_summary_scope": str(
            dict(candidate_page_payload.get("facet_summary") or {}).get("count_scope") or "unavailable"
        ),
        "result_view_lifecycle": lifecycle,
        "candidates": rows if include_candidates else [],
    }
    candidate_page = {
        **dict(candidate_page_payload),
        "result_mode": "asset_population",
        "total_candidates": total_candidates,
        "returned_count": len(rows),
        "candidates": rows,
        "profile_fetch_progress": profile_progress,
        "board_runtime_state": board_runtime_state,
        "result_view_lifecycle": lifecycle,
        "asset_population": asset_population,
        "read_contract": dict(candidate_page_payload.get("read_contract") or {})
        or {
            "source": "serving_projection_members",
            "fallback_used": False,
            "fail_closed": True,
        },
    }
    progress_status = str(dict(progress_payload or {}).get("status") or "").strip()
    progress_stage = str(dict(progress_payload or {}).get("stage") or "").strip()
    if progress_status:
        result_job["status"] = progress_status
    if progress_stage:
        result_job["stage"] = progress_stage
    dashboard = {
        "status": "ok",
        "job": result_job,
        "result_mode": "asset_population",
        "ranked_result_count": 0,
        "results": [],
        "ranked_results": [],
        "asset_population": {**asset_population, "candidates": []},
        "board_runtime_state": board_runtime_state,
        "result_view_lifecycle": lifecycle,
        "projection": projection,
        "read_contract": {
            "source": "serving_projection_members",
            "fallback_used": False,
            "fail_closed": True,
        },
    }
    results_payload = {
        "status": "ready",
        "job": result_job,
        "result_mode": "asset_population",
        "results": [],
        "ranked_results": [],
        "asset_population": asset_population,
        "board_runtime_state": board_runtime_state,
        "result_view_lifecycle": lifecycle,
        "projection": projection,
        "read_contract": {
            "source": "serving_projection_members",
            "fallback_used": False,
            "fail_closed": True,
            "legacy_endpoint": "retired",
        },
    }
    return results_payload, dashboard, candidate_page


def _build_projection_cutover_report(
    *,
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> dict[str, Any]:
    """Summarize whether final public reads are owned by canonical projections.

    This report is intentionally based on the final public payloads observed by
    the smoke runner. It gives signoff a stable contract gate for the
    migration-era period where legacy endpoints still exist but normal reads
    must not silently compose results from legacy artifacts.
    """

    payloads = {
        "results": dict(results_payload or {}),
        "dashboard": dict(dashboard_payload or {}),
        "candidates": dict(candidate_page_payload or {}),
    }
    projection_ids: set[str] = set()
    fallback_endpoints: list[str] = []
    legacy_endpoints: list[str] = []
    retired_legacy_endpoints: list[str] = []
    sources: dict[str, str] = {}
    endpoint_details: dict[str, dict[str, Any]] = {}

    for endpoint, payload in payloads.items():
        read_contract = dict(payload.get("read_contract") or {})
        projection = dict(payload.get("projection") or {})
        asset_population = dict(payload.get("asset_population") or {})
        projection_id = str(
            projection.get("projection_id")
            or asset_population.get("source_projection_id")
            or payload.get("projection_id")
            or ""
        ).strip()
        if projection_id:
            projection_ids.add(projection_id)
        source = str(read_contract.get("source") or "").strip()
        sources[endpoint] = source
        if bool(read_contract.get("fallback_used")):
            fallback_endpoints.append(endpoint)
        if str(read_contract.get("legacy_endpoint") or "").strip() == "retired":
            retired_legacy_endpoints.append(endpoint)
        elif source and "serving_projection" not in source and "projection" not in source:
            legacy_endpoints.append(endpoint)
        endpoint_details[endpoint] = {
            "source": source,
            "fallback_used": bool(read_contract.get("fallback_used")),
            "legacy_endpoint": str(read_contract.get("legacy_endpoint") or "").strip(),
            "projection_id": projection_id,
        }

    return {
        "report_available": True,
        "run_projection_link_present": bool(projection_ids),
        "projection_missing": not bool(projection_ids),
        "projection_id_count": len(projection_ids),
        "projection_ids": sorted(projection_ids),
        "legacy_public_reader_fallback_used": bool(fallback_endpoints),
        "legacy_public_reader_fallback_endpoints": fallback_endpoints,
        "legacy_endpoint_normal_path_used": bool(legacy_endpoints),
        "legacy_endpoint_normal_path_endpoints": legacy_endpoints,
        "retired_legacy_endpoint_count": len(retired_legacy_endpoints),
        "retired_legacy_endpoint_reads": retired_legacy_endpoints,
        "read_contract_sources": sources,
        "endpoint_details": endpoint_details,
    }


def _workflow_status_from_payload(payload: dict[str, Any]) -> str:
    source = dict(payload or {})
    job = dict(source.get("job") or {})
    return str(job.get("status") or source.get("status") or "").strip().lower()


def _workflow_stage_from_payload(payload: dict[str, Any]) -> str:
    source = dict(payload or {})
    job = dict(source.get("job") or {})
    return str(job.get("stage") or source.get("stage") or "").strip().lower()


def _build_legacy_artifact_coherence_report(
    *,
    job_payload: dict[str, Any],
    results_payload: dict[str, Any],
    progress_payload: dict[str, Any],
) -> dict[str, Any]:
    """Report whether legacy job artifacts disagree with canonical workflow state.

    During migration, legacy job artifact JSON can still exist for operator
    evidence. It must not claim terminal completion while the canonical PG job,
    lifecycle/progress, or projection-backed result payload remains nonterminal.
    """

    canonical_status = _workflow_status_from_payload(job_payload)
    canonical_stage = _workflow_stage_from_payload(job_payload)
    progress_status = _workflow_status_from_payload(progress_payload)
    progress_stage = _workflow_stage_from_payload(progress_payload)
    artifact_status = _workflow_status_from_payload(results_payload)
    artifact_stage = _workflow_stage_from_payload(results_payload)

    canonical_terminal = canonical_status in TERMINAL_WORKFLOW_STATUSES
    progress_terminal = (not progress_status) or progress_status in TERMINAL_WORKFLOW_STATUSES
    artifact_terminal = artifact_status in TERMINAL_WORKFLOW_STATUSES
    terminal_drift = bool(
        artifact_terminal
        and artifact_status == "completed"
        and ((canonical_status and not canonical_terminal) or (progress_status and not progress_terminal))
    )
    terminal_coherent = not terminal_drift

    return {
        "report_available": True,
        "canonical_job_status": canonical_status,
        "canonical_job_stage": canonical_stage,
        "progress_status": progress_status,
        "progress_stage": progress_stage,
        "legacy_artifact_status": artifact_status,
        "legacy_artifact_stage": artifact_stage,
        "canonical_terminal": canonical_terminal,
        "progress_terminal": progress_terminal,
        "legacy_artifact_terminal": artifact_terminal,
        "terminal_coherent": terminal_coherent,
        "terminal_drift_detected": terminal_drift,
        "blocking_violation": terminal_drift,
    }


def _fetch_smoke_projection_fallback_payloads(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    legacy_error_payload: dict[str, Any],
    job_payload: dict[str, Any] | None = None,
    include_candidates: bool = False,
    offset: int = 0,
    limit: int = 24,
    candidate_query_suffix: str = "",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    progress_payload: dict[str, Any] = {}
    try:
        progress_payload = client.get(f"/api/jobs/{quote(str(job_id or ''))}/progress")
    except Exception:
        progress_payload = {}
    link_payload, projection_payload, candidate_page_payload = _projection_payloads_for_smoke(
        client,
        job_id=job_id,
        legacy_error_payload=legacy_error_payload,
        offset=offset,
        limit=limit,
        candidate_query_suffix=candidate_query_suffix,
    )
    if str(projection_payload.get("status") or "") != "ready":
        not_ready = {
            "status": str(projection_payload.get("status") or link_payload.get("status") or "not_ready"),
            "reason": str(projection_payload.get("reason") or link_payload.get("reason") or "projection_not_ready"),
            "legacy_cutover": dict(legacy_error_payload or {}),
            "projection_link": link_payload,
        }
        return not_ready, not_ready, not_ready
    return _build_projection_smoke_payloads(
        job_id=job_id,
        job_payload=job_payload,
        projection_payload=projection_payload,
        candidate_page_payload=candidate_page_payload,
        progress_payload=progress_payload,
        include_candidates=include_candidates,
    )


def _fetch_smoke_projection_payloads_if_ready(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    job_payload: dict[str, Any] | None = None,
    include_candidates: bool = False,
    offset: int = 0,
    limit: int = 24,
    candidate_query_suffix: str = "",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return None
    try:
        link_payload = _projection_link_for_smoke(client, job_id=normalized_job_id)
    except urllib_error.HTTPError as exc:
        if exc.code in {400, 404, 409}:
            return None
        raise
    except Exception:
        return None
    if str(link_payload.get("status") or "").strip() != "ready":
        return None
    projection_id = str(link_payload.get("projection_id") or "").strip()
    if not projection_id:
        return None
    query_suffix = str(candidate_query_suffix or "").strip()
    if query_suffix and not query_suffix.startswith("&"):
        query_suffix = "&" + query_suffix.lstrip("?")
    try:
        projection_payload = client.get(f"/api/projections/{quote(projection_id)}")
    except urllib_error.HTTPError as exc:
        if exc.code in {400, 404, 409}:
            return None
        raise
    if str(projection_payload.get("status") or "").strip() != "ready":
        return None
    try:
        candidate_page_payload = client.get(
            f"/api/projections/{quote(projection_id)}/candidates"
            f"?offset={max(0, int(offset or 0))}&limit={max(1, min(250, int(limit or 24)))}{query_suffix}"
        )
    except urllib_error.HTTPError as exc:
        if exc.code in {400, 404, 409}:
            return None
        raise
    progress_payload: dict[str, Any] = {}
    try:
        progress_payload = client.get(f"/api/jobs/{quote(normalized_job_id)}/progress")
    except Exception:
        progress_payload = {}
    return _build_projection_smoke_payloads(
        job_id=normalized_job_id,
        job_payload=job_payload,
        projection_payload=projection_payload,
        candidate_page_payload=candidate_page_payload,
        progress_payload=progress_payload,
        include_candidates=include_candidates,
    )


def _fetch_smoke_results_payload(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    job_payload: dict[str, Any] | None = None,
    include_runtime_details: bool = False,
    include_candidates: bool = False,
) -> dict[str, Any]:
    query = (
        f"include_runtime_details={1 if include_runtime_details else 0}"
        f"&include_candidates={1 if include_candidates else 0}"
    )
    try:
        return client.get(f"/api/jobs/{quote(str(job_id or ''))}/results?{query}")
    except urllib_error.HTTPError as exc:
        payload = _http_error_payload(exc)
        if exc.code != 410 or not _projection_id_from_legacy_cutover_payload(payload):
            raise
        results_payload, _, _ = _fetch_smoke_projection_fallback_payloads(
            client,
            job_id=job_id,
            legacy_error_payload=payload,
            job_payload=job_payload,
            include_candidates=include_candidates,
        )
        return results_payload


def _fetch_smoke_dashboard_payload(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    job_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    projection_payloads = _fetch_smoke_projection_payloads_if_ready(
        client,
        job_id=job_id,
        job_payload=job_payload,
    )
    if projection_payloads is not None:
        _, dashboard_payload, _ = projection_payloads
        return dashboard_payload
    try:
        return client.get(f"/api/jobs/{quote(str(job_id or ''))}/dashboard?include_candidates=0")
    except urllib_error.HTTPError as exc:
        payload = _http_error_payload(exc)
        if exc.code != 410 or not _projection_id_from_legacy_cutover_payload(payload):
            raise
        _, dashboard_payload, _ = _fetch_smoke_projection_fallback_payloads(
            client,
            job_id=job_id,
            legacy_error_payload=payload,
            job_payload=job_payload,
        )
        return dashboard_payload


def _candidate_query_window_from_path(path: str) -> tuple[int, int, str]:
    parsed = urlparse(path)
    query = parse_qs(parsed.query)
    offset = _safe_int((query.get("offset") or ["0"])[0])
    limit = _safe_int((query.get("limit") or ["24"])[0]) or 24
    passthrough: list[str] = []
    for key, values in sorted(query.items()):
        if key in {"offset", "limit", "lightweight"}:
            continue
        for value in values:
            passthrough.append(f"{quote(str(key))}={quote(str(value))}")
    return offset, limit, "&".join(passthrough)


def _fetch_smoke_candidate_page_payload(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    path: str | None = None,
    job_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_path = path or f"/api/jobs/{quote(str(job_id or ''))}/candidates?offset=0&limit=24&lightweight=1"
    offset, limit, query_suffix = _candidate_query_window_from_path(candidate_path)
    projection_payloads = _fetch_smoke_projection_payloads_if_ready(
        client,
        job_id=job_id,
        job_payload=job_payload,
        offset=offset,
        limit=limit,
        candidate_query_suffix=query_suffix,
    )
    if projection_payloads is not None:
        _, _, candidate_page_payload = projection_payloads
        return candidate_page_payload
    try:
        return client.get(candidate_path)
    except urllib_error.HTTPError as exc:
        payload = _http_error_payload(exc)
        if exc.code != 410 or not _projection_id_from_legacy_cutover_payload(payload):
            raise
        _, _, candidate_page_payload = _fetch_smoke_projection_fallback_payloads(
            client,
            job_id=job_id,
            legacy_error_payload=payload,
            job_payload=job_payload,
            offset=offset,
            limit=limit,
            candidate_query_suffix=query_suffix,
        )
        return candidate_page_payload


def stage_summary_digest(results_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    root = results_payload.get("workflow_stage_summaries") or {}
    summaries = root.get("summaries") or {}
    raw_digest: dict[str, dict[str, Any]] = {}
    if not isinstance(summaries, dict):
        return raw_digest
    for key, value in summaries.items():
        if not isinstance(value, dict):
            continue
        raw_digest[str(key)] = {
            "status": value.get("status"),
            "stage": value.get("stage"),
            "candidate_count": value.get("candidate_count"),
            "manual_review_count": value.get("manual_review_count"),
        }
    stage_order: list[str] = []
    seen_stage_names: set[str] = set()
    for item in list(_PROVIDER_STAGE_ORDER) + list(root.get("stage_order") or []):
        stage_name = str(item).strip()
        if not stage_name or stage_name in seen_stage_names:
            continue
        seen_stage_names.add(stage_name)
        stage_order.append(stage_name)
    if not stage_order:
        return raw_digest
    digest: dict[str, dict[str, Any]] = {}
    for index, stage_name in enumerate(stage_order):
        entry = dict(raw_digest.get(stage_name) or {})
        if not entry:
            for later_stage_name in stage_order[index + 1 :]:
                later_entry = dict(raw_digest.get(later_stage_name) or {})
                later_status = str(later_entry.get("status") or "").strip().lower()
                if later_status not in {"completed", "skipped"}:
                    continue
                entry = {
                    "status": later_entry.get("status"),
                    "stage": stage_name,
                    "candidate_count": later_entry.get("candidate_count"),
                    "manual_review_count": later_entry.get("manual_review_count"),
                }
                break
        if entry:
            entry.setdefault("stage", stage_name)
            digest[stage_name] = entry
    for stage_name, entry in raw_digest.items():
        if stage_name not in digest:
            digest[stage_name] = dict(entry)
    return digest


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


def _text_contains_count_denominator(value: Any) -> bool:
    text = str(value or "")
    if "/" not in text:
        return False
    return bool(re.search(r"\d+\s*/\s*\d+", text))


def _first_count_fraction(value: Any) -> tuple[int, int] | None:
    text = str(value or "")
    match = re.search(r"(\d+)\s*/\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


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


def _elapsed_ms(started_at: Any, completed_at: Any) -> float | None:
    started = _parse_timestamp(started_at)
    completed = _parse_timestamp(completed_at)
    if started is None or completed is None or completed < started:
        return None
    return round((completed - started).total_seconds() * 1000, 2)


def _public_web_batches_for_smoke_window(
    batches: list[dict[str, Any]],
    *,
    started_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    normalized_started_at = started_at.astimezone(timezone.utc)
    for batch in batches:
        if not isinstance(batch, dict):
            continue
        observed_at = _parse_timestamp(batch.get("updated_at") or batch.get("created_at"))
        if observed_at is None or observed_at < normalized_started_at:
            continue
        rows.append(dict(batch))
    return rows


def _reasonable_elapsed_ms(started_at: Any, completed_at: Any) -> float | None:
    elapsed = _elapsed_ms(started_at, completed_at)
    if elapsed is None or elapsed > _MAX_REASONABLE_STAGE_WALL_CLOCK_MS:
        return None
    return elapsed


def _raw_stage_summaries(results_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    workflow_stage_summaries = dict(results_payload.get("workflow_stage_summaries") or {})
    return {
        str(key): dict(value or {})
        for key, value in dict(workflow_stage_summaries.get("summaries") or {}).items()
        if str(key).strip() and isinstance(value, dict)
    }


def _build_duplicate_provider_dispatch_report(provider_invocations: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in list(provider_invocations or []):
        if not isinstance(item, dict):
            continue
        signature = str(item.get("dispatch_signature") or "").strip()
        if not signature:
            continue
        entry = grouped.setdefault(
            signature,
            {
                "dispatch_signature": signature,
                "provider_name": str(item.get("provider_name") or ""),
                "dispatch_kind": str(item.get("dispatch_kind") or ""),
                "logical_name": str(item.get("logical_name") or ""),
                "query_text": str(item.get("query_text") or ""),
                "task_key": str(item.get("task_key") or ""),
                "payload_hash": str(item.get("payload_hash") or ""),
                "first_recorded_at": str(item.get("recorded_at") or ""),
                "count": 0,
            },
        )
        entry["count"] = int(entry.get("count") or 0) + 1
    duplicates = sorted(
        (entry for entry in grouped.values() if int(entry.get("count") or 0) > 1),
        key=lambda item: (
            -int(item.get("count") or 0),
            str(item.get("dispatch_kind") or ""),
            str(item.get("logical_name") or ""),
        ),
    )
    redundant_dispatch_count = sum(max(0, int(item.get("count") or 0) - 1) for item in duplicates)
    return {
        "invocation_count": len([item for item in provider_invocations if isinstance(item, dict)]),
        "signature_count": len(grouped),
        "duplicate_signature_count": len(duplicates),
        "redundant_dispatch_count": redundant_dispatch_count,
        "duplicate_signatures": duplicates,
        "violation_count": len(duplicates),
        "violation_detected": bool(duplicates),
        "log_available": bool(os.getenv("SOURCING_RUNTIME_DIR")),
    }


def _build_disabled_stage_violation_report(
    *,
    explain_payload: dict[str, Any],
    results_payload: dict[str, Any],
) -> dict[str, Any]:
    analysis_stage_mode = (
        str(
            explain_payload.get("analysis_stage_mode")
            or dict(dict(results_payload.get("job") or {}).get("summary") or {}).get("analysis_stage_mode")
            or "single_stage"
        )
        .strip()
        .lower()
        or "single_stage"
    )
    raw_stage_summaries = _raw_stage_summaries(results_payload)
    public_web_summary = dict(raw_stage_summaries.get("public_web_stage_2") or {})
    public_web_stage_present = bool(public_web_summary) and str(
        public_web_summary.get("status") or ""
    ).strip().lower() not in {
        "",
        "not_started",
    }
    unexpected_public_web_stage = analysis_stage_mode != "two_stage" and public_web_stage_present
    return {
        "analysis_stage_mode": analysis_stage_mode,
        "public_web_stage_present": public_web_stage_present,
        "public_web_stage_status": str(public_web_summary.get("status") or ""),
        "unexpected_public_web_stage": unexpected_public_web_stage,
        "violation_count": 1 if unexpected_public_web_stage else 0,
        "violation_detected": unexpected_public_web_stage,
    }


def _build_prerequisite_gap_report(stage_wall_clock: dict[str, dict[str, Any]]) -> dict[str, Any]:
    metrics_ms: dict[str, float] = {}
    for metric_name, start_stage, end_stage, anchor in (
        ("linkedin_stage_1_to_stage_1_preview_start", "linkedin_stage_1", "stage_1_preview", "started_at"),
        ("stage_1_preview_to_stage_2_final_start", "stage_1_preview", "stage_2_final", "started_at"),
        ("public_web_stage_2_to_stage_2_final_start", "public_web_stage_2", "stage_2_final", "started_at"),
    ):
        start_value = str(dict(stage_wall_clock.get(start_stage) or {}).get("completed_at") or "").strip()
        end_value = str(dict(stage_wall_clock.get(end_stage) or {}).get(anchor) or "").strip()
        elapsed = _reasonable_elapsed_ms(start_value, end_value)
        if elapsed is not None:
            metrics_ms[metric_name] = elapsed
    large_gap_metrics = {
        key: value for key, value in metrics_ms.items() if float(value) > _PREREQUISITE_GAP_VIOLATION_MS
    }
    return {
        "metrics_ms": metrics_ms,
        "large_gap_threshold_ms": _PREREQUISITE_GAP_VIOLATION_MS,
        "large_gap_metrics": large_gap_metrics,
        "max_gap_ms": round(max(metrics_ms.values()), 2) if metrics_ms else 0.0,
        "violation_count": len(large_gap_metrics),
        "violation_detected": bool(large_gap_metrics),
    }


def _build_final_results_board_consistency_report(
    *,
    results_payload: dict[str, Any],
    board_probe: dict[str, Any],
    workflow_wall_clock: dict[str, float],
) -> dict[str, Any]:
    expected_candidate_count = _expected_board_candidate_count(results_payload)
    final_to_board_ready_ms = _safe_float(workflow_wall_clock.get("final_results_to_board_ready"))
    final_to_board_nonempty_ms = _safe_float(workflow_wall_clock.get("final_results_to_board_nonempty"))
    missing_board_after_final = expected_candidate_count > 0 and not bool(board_probe.get("ready_nonempty"))
    delayed_board_after_final = (
        expected_candidate_count > 0
        and bool(board_probe.get("ready_nonempty"))
        and final_to_board_nonempty_ms > _FINAL_RESULTS_BOARD_LAG_VIOLATION_MS
    )
    violation_count = int(missing_board_after_final) + int(delayed_board_after_final)
    return {
        "expected_candidate_count": expected_candidate_count,
        "board_ready": bool(board_probe.get("ready")),
        "board_ready_nonempty": bool(board_probe.get("ready_nonempty")),
        "final_results_to_board_ready_ms": final_to_board_ready_ms,
        "final_results_to_board_nonempty_ms": final_to_board_nonempty_ms,
        "missing_board_after_final": missing_board_after_final,
        "delayed_board_after_final": delayed_board_after_final,
        "lag_violation_threshold_ms": _FINAL_RESULTS_BOARD_LAG_VIOLATION_MS,
        "violation_count": violation_count,
        "violation_detected": violation_count > 0,
    }


def _result_view_lifecycle_from_payloads(
    *,
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> dict[str, Any]:
    for source in (
        candidate_page_payload,
        dashboard_payload,
        results_payload,
        dict(candidate_page_payload.get("asset_population") or {}),
        dict(results_payload.get("asset_population") or {}),
        dict(dashboard_payload.get("asset_population") or {}),
    ):
        if not isinstance(source, dict):
            continue
        payload = dict(source.get("result_view_lifecycle") or {})
        if payload:
            return payload
    result_view = dict(results_payload.get("result_view") or dashboard_payload.get("result_view") or {})
    metadata = dict(result_view.get("metadata") or {})
    return dict(metadata.get("result_view_lifecycle") or {})


def _lifecycle_supersedes_serving_publication_gap(lifecycle: dict[str, Any]) -> bool:
    payload = dict(lifecycle or {})
    if not payload:
        return False
    current_snapshot_id = str(payload.get("current_snapshot_id") or "").strip()
    served_snapshot_id = str(payload.get("served_snapshot_id") or "").strip()
    if current_snapshot_id and served_snapshot_id and current_snapshot_id == served_snapshot_id:
        return True
    serving_phase = (
        str(payload.get("serving_projection_phase") or payload.get("phase") or payload.get("state") or "")
        .strip()
        .lower()
    )
    return bool(
        current_snapshot_id
        and serving_phase in {"current_snapshot_serving", "current_serving"}
        and (not served_snapshot_id or served_snapshot_id == current_snapshot_id)
    )


def _serving_publication_gap_from_payloads(
    *,
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = _result_view_lifecycle_from_payloads(
        results_payload=results_payload,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
    )
    if _lifecycle_supersedes_serving_publication_gap(lifecycle):
        return {}
    for source in (
        dict(dashboard_payload.get("asset_population") or {}),
        dict(candidate_page_payload.get("asset_population") or {}),
        dict(results_payload.get("asset_population") or {}),
        dashboard_payload,
        candidate_page_payload,
        results_payload,
    ):
        if not isinstance(source, dict):
            continue
        candidate_source = dict(source.get("candidate_source") or {})
        result_view = dict(candidate_source.get("result_view") or source.get("result_view") or {})
        metadata_candidates = (
            dict(result_view.get("metadata") or {}),
            dict(candidate_source.get("result_view_metadata") or {}),
            dict(source.get("result_view_metadata") or {}),
        )
        for metadata in metadata_candidates:
            gap = dict(metadata.get("serving_publication_gap") or {})
            if gap:
                return gap
    return {}


def _board_visible_patches_from_events(job_events: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    patches: list[dict[str, Any]] = []
    accepted_phases = {"board_visible_delta_applied", "board_visible_full_snapshot_serving"}
    for event in list(job_events or []):
        payload = _event_payload(dict(event or {}))
        if str(payload.get("event_family") or "").strip() != "workflow_materialization":
            continue
        phase = str(payload.get("phase") or "").strip()
        if phase not in accepted_phases:
            continue
        patch = dict(payload.get("board_visible_patch") or {})
        if not patch:
            continue
        patch.setdefault("patch_phase", phase)
        if "snapshot_id" not in patch and str(payload.get("snapshot_id") or "").strip():
            patch["snapshot_id"] = str(payload.get("snapshot_id") or "").strip()
        if "published_at" not in patch:
            patch["published_at"] = _event_created_at(dict(event or {}))
        patches.append(patch)
    return patches


def _materialization_streaming_events(job_events: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for event in list(job_events or []):
        payload = _event_payload(dict(event or {}))
        event_family = str(payload.get("event_family") or "").strip()
        if event_family not in {"completed_workflow_reconcile", "workflow_materialization"}:
            continue
        phase = str(payload.get("phase") or "").strip().lower()
        if not phase.startswith("materialize_"):
            continue
        sync_result = dict(payload.get("sync_result") or {})
        materialization_streaming = dict(sync_result.get("materialization_streaming") or {})
        if not materialization_streaming:
            continue
        sample = dict(materialization_streaming)
        sample["phase"] = phase
        sample["event_family"] = event_family
        sample["created_at"] = _event_created_at(dict(event or {}))
        sample["snapshot_id"] = str(payload.get("snapshot_id") or "").strip()
        sample["reconcile_kind"] = str(payload.get("reconcile_kind") or payload.get("worker_kind") or "").strip()
        sample["worker_ids"] = list(payload.get("worker_ids") or [])
        samples.append(sample)
    return samples


def _build_materialization_streaming_report(
    job_summary: dict[str, Any],
    *,
    job_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    latest_metrics = dict(job_summary.get("latest_metrics") or {})
    background_reconcile = dict(
        job_summary.get("background_reconcile") or latest_metrics.get("background_reconcile") or {}
    )
    raw_streaming = dict(
        latest_metrics.get("streaming_materialization")
        or latest_metrics.get("materialization_streaming")
        or latest_metrics.get("incremental_materialization")
        or {}
    )
    raw_writer = dict(
        latest_metrics.get("materialization_writer_budget")
        or latest_metrics.get("materialization_writer")
        or raw_streaming.get("writer_budget")
        or {}
    )
    search_seed_reconcile = dict(background_reconcile.get("search_seed") or {})
    harvest_prefetch_reconcile = dict(background_reconcile.get("harvest_prefetch") or {})
    company_roster_reconcile = dict(background_reconcile.get("company_roster") or {})
    profile_completion_result = dict(
        dict(harvest_prefetch_reconcile.get("resume_result") or {}).get("profile_completion_result") or {}
    )
    profile_completion_result_payload = dict(profile_completion_result.get("result") or {})
    provider_response_count = _safe_int(
        raw_streaming.get("provider_response_count")
        or raw_streaming.get("provider_request_completed_count")
        or raw_streaming.get("completed_provider_request_count")
    )
    profile_url_count = _safe_int(
        raw_streaming.get("profile_url_count")
        or raw_streaming.get("fetched_profile_count")
        or profile_completion_result_payload.get("fetched_profile_count")
    )
    inline_worker_count = sum(
        _safe_int(value)
        for value in (
            search_seed_reconcile.get("applied_worker_count"),
            harvest_prefetch_reconcile.get("applied_worker_count"),
            company_roster_reconcile.get("applied_worker_count"),
            latest_metrics.get("inline_company_roster_worker_count"),
            latest_metrics.get("inline_harvest_profile_worker_count"),
        )
    )
    pending_delta_count = _safe_int(
        raw_streaming.get("pending_delta_count")
        or raw_streaming.get("delta_batch_count")
        or raw_streaming.get("applied_delta_count")
        or inline_worker_count
    )
    active_writer_count = _safe_int(raw_writer.get("active_writer_count") or raw_streaming.get("active_writer_count"))
    queued_writer_count = _safe_int(raw_writer.get("queued_writer_count") or raw_streaming.get("queued_writer_count"))
    oldest_pending_delta_age_ms = _safe_float(
        raw_writer.get("oldest_pending_delta_age_ms") or raw_streaming.get("oldest_pending_delta_age_ms")
    )
    request_context = dict(
        latest_metrics.get("runtime_tuning")
        or latest_metrics.get("runtime_timing_overrides")
        or raw_streaming.get("runtime_tuning")
        or {}
    )
    provider_to_materialization_ms = _safe_float(
        raw_streaming.get("provider_response_to_first_materialization_ms")
        or raw_streaming.get("provider_result_to_first_materialization_ms")
    )
    event_samples = _materialization_streaming_events(job_events)
    event_provider_response_count = sum(_safe_int(item.get("provider_response_count")) for item in event_samples)
    event_profile_url_count = sum(_safe_int(item.get("profile_url_count")) for item in event_samples)
    event_pending_delta_count = max((_safe_int(item.get("pending_delta_count")) for item in event_samples), default=0)
    event_active_writer_count = max((_safe_int(item.get("active_writer_count")) for item in event_samples), default=0)
    event_queued_writer_count = max((_safe_int(item.get("queued_writer_count")) for item in event_samples), default=0)
    event_oldest_pending_delta_age_ms = max(
        (_safe_float(item.get("oldest_pending_delta_age_ms")) for item in event_samples),
        default=0.0,
    )
    if provider_response_count <= 0 and event_provider_response_count > 0:
        provider_response_count = event_provider_response_count
    if profile_url_count <= 0 and event_profile_url_count > 0:
        profile_url_count = event_profile_url_count
    if pending_delta_count <= 0 and event_pending_delta_count > 0:
        pending_delta_count = event_pending_delta_count
    if active_writer_count <= 0 and event_active_writer_count > 0:
        active_writer_count = event_active_writer_count
    if queued_writer_count <= 0 and event_queued_writer_count > 0:
        queued_writer_count = event_queued_writer_count
    if oldest_pending_delta_age_ms <= 0.0 and event_oldest_pending_delta_age_ms > 0.0:
        oldest_pending_delta_age_ms = event_oldest_pending_delta_age_ms
    if provider_to_materialization_ms <= 0.0 and event_samples:
        first_started_at = min(
            (
                parsed_started
                for item in event_samples
                if str(item.get("phase") or "") == "materialize_started"
                and (parsed_started := _parse_timestamp(str(item.get("created_at") or ""))) is not None
            ),
            default=None,
        )
        first_completed_at = min(
            (
                parsed_completed
                for item in event_samples
                if str(item.get("phase") or "") == "materialize_completed"
                and (parsed_completed := _parse_timestamp(str(item.get("created_at") or ""))) is not None
            ),
            default=None,
        )
        if first_started_at is not None and first_completed_at is not None and first_completed_at >= first_started_at:
            provider_to_materialization_ms = (first_completed_at - first_started_at).total_seconds() * 1000
        elif first_started_at is not None:
            provider_to_materialization_ms = 0.01
    report_available = bool(
        raw_streaming
        or raw_writer
        or inline_worker_count
        or provider_response_count
        or profile_url_count
        or event_samples
    )
    budget_report = build_materialization_streaming_budget_report(
        request_context,
        provider_response_count=provider_response_count,
        profile_url_count=profile_url_count,
        pending_delta_count=pending_delta_count,
        active_writer_count=active_writer_count,
        queued_writer_count=queued_writer_count,
        oldest_pending_delta_age_ms=oldest_pending_delta_age_ms,
    )
    event_budget_actions: Counter[str] = Counter()
    for item in event_samples:
        budget_action = str(
            item.get("recommended_action") or dict(item.get("budget") or {}).get("recommended_action") or ""
        ).strip()
        if budget_action:
            event_budget_actions[budget_action] += 1
    report = {
        "report_available": report_available,
        "provider_response_count": provider_response_count,
        "profile_url_count": profile_url_count,
        "inline_incremental_worker_count": inline_worker_count,
        "pending_delta_count": pending_delta_count,
        "provider_response_to_first_materialization_ms": round(provider_to_materialization_ms, 2),
        "budget": budget_report,
    }
    if event_samples:
        report["source"] = "structured_materialization_events" if not raw_streaming else "summary_and_events"
        report["event_sample_count"] = len(event_samples)
        report["event_phase_counts"] = dict(Counter(str(item.get("phase") or "") for item in event_samples))
        report["event_budget_action_counts"] = dict(event_budget_actions)
        report["event_samples"] = event_samples[:10]
    return report


def _collect_provider_limiter_slots(payload: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []

    def _visit(value: Any) -> None:
        if len(slots) >= limit:
            return
        if isinstance(value, dict):
            if str(value.get("limiter_key") or "").strip() and (
                "budget" in value or "active_count" in value or "wait_ms" in value
            ):
                slots.append(
                    {
                        "limiter_key": str(value.get("limiter_key") or "").strip(),
                        "budget": _safe_int(value.get("budget")),
                        "active_count": _safe_int(value.get("active_count")),
                        "wait_ms": _safe_float(value.get("wait_ms")),
                        "db_limiter_enabled": bool(value.get("db_limiter_enabled", True)),
                    }
                )
            for nested_value in value.values():
                _visit(nested_value)
            return
        if isinstance(value, list):
            for item in value:
                _visit(item)

    _visit(payload)
    return slots


def _runtime_tuning_context_for_smoke_report(
    *,
    explain_payload: dict[str, Any],
    latest_metrics: dict[str, Any],
    materialization_streaming: dict[str, Any],
) -> dict[str, Any]:
    context = dict(
        latest_metrics.get("runtime_tuning")
        or latest_metrics.get("runtime_timing_overrides")
        or dict(materialization_streaming.get("budget") or {}).get("runtime_tuning")
        or {}
    )
    runtime_profile = str(explain_payload.get("runtime_tuning_profile") or "").strip()
    if runtime_profile and not str(context.get("runtime_tuning_profile") or "").strip():
        context["runtime_tuning_profile"] = runtime_profile
    for key in (
        "harvest_global_inflight_budget",
        "harvest_profile_actor_global_inflight",
        "harvest_profile_batch_submit_global_inflight",
        "harvest_profile_scrape_global_inflight",
        "harvest_people_search_global_inflight",
        "harvest_company_roster_global_inflight",
    ):
        if key in explain_payload and key not in context:
            context[key] = explain_payload[key]
    return context


def _build_provider_backpressure_report(
    *,
    explain_payload: dict[str, Any],
    job_summary: dict[str, Any],
    latest_metrics: dict[str, Any],
    materialization_streaming: dict[str, Any],
    progress_observability: dict[str, Any] | None,
) -> dict[str, Any]:
    progress_maxima = dict(dict(progress_observability or {}).get("maxima") or {})
    return build_provider_backpressure_budget_report(
        _runtime_tuning_context_for_smoke_report(
            explain_payload=explain_payload,
            latest_metrics=latest_metrics,
            materialization_streaming=materialization_streaming,
        ),
        observed_limiter_slots=_collect_provider_limiter_slots(job_summary),
        active_provider_worker_count=_safe_int(progress_maxima.get("active_worker_count")),
        queued_provider_worker_count=_safe_int(progress_maxima.get("queued_worker_count")),
        waiting_remote_harvest_count=_safe_int(progress_maxima.get("waiting_remote_harvest_count")),
    )


def _build_streaming_materialization_guardrail_report(materialization_streaming: dict[str, Any]) -> dict[str, Any]:
    payload = dict(materialization_streaming or {})
    budget = dict(payload.get("budget") or {})
    provider_to_materialization_ms = _safe_float(payload.get("provider_response_to_first_materialization_ms"))
    slow_first_materialization = provider_to_materialization_ms > _STREAMING_MATERIALIZATION_GAP_VIOLATION_MS and bool(
        payload.get("report_available")
    )
    writer_budget_exhausted = (
        bool(payload.get("report_available"))
        and bool(budget.get("delta_materialization_ready"))
        and not bool(budget.get("writer_slot_available"))
    )
    # Exhausted writer budget is an observability signal, not automatically a
    # workflow violation. It becomes actionable when paired with a slow first
    # materialization gap.
    violation_count = 1 if slow_first_materialization else 0
    return {
        "report_available": bool(payload.get("report_available")),
        "provider_response_to_first_materialization_ms": round(provider_to_materialization_ms, 2),
        "slow_first_materialization": slow_first_materialization,
        "writer_budget_exhausted": writer_budget_exhausted,
        "recommended_action": str(budget.get("recommended_action") or ""),
        "gap_violation_threshold_ms": _STREAMING_MATERIALIZATION_GAP_VIOLATION_MS,
        "violation_count": violation_count,
        "violation_detected": violation_count > 0,
    }


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


def _stage_boundary_completed_at_from_events(job_events: list[dict[str, Any]], *, boundary: str) -> str:
    normalized_boundary = str(boundary or "").strip().lower()
    candidates: list[datetime] = []
    for event in list(job_events or []):
        payload = _event_payload(dict(event or {}))
        detail = str(dict(event or {}).get("detail") or payload.get("detail") or "").strip().lower()
        event_stage = str(dict(event or {}).get("stage") or payload.get("stage") or "").strip().lower()
        event_status = str(dict(event or {}).get("status") or payload.get("status") or "").strip().lower()
        analysis_stage = str(payload.get("analysis_stage") or "").strip().lower()
        is_match = False
        if normalized_boundary == "linkedin_stage_1":
            is_match = (
                event_stage == "acquiring"
                and event_status == "completed"
                and (
                    "linkedin stage 1 acquisition completed" in detail
                    or str(payload.get("status") or "").strip().lower() == "completed"
                    and bool(payload.get("candidate_doc_path") or payload.get("stage_candidate_doc_path"))
                )
            )
        elif normalized_boundary == "stage_1_preview":
            is_match = (
                event_stage == "acquiring"
                and event_status == "completed"
                and (
                    analysis_stage == "stage_1_preview"
                    or "stage 1 preview ready" in detail
                    or bool(payload.get("preview_artifact_path"))
                )
            )
        if not is_match:
            continue
        created_at = _parse_timestamp(_event_created_at(dict(event or {})))
        if created_at is not None:
            candidates.append(created_at)
    if not candidates:
        return ""
    return min(candidates).isoformat()


def _jsonish_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _jsonish_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return list(decoded) if isinstance(decoded, list) else []
    return []


def _materialization_item_metadata(item: dict[str, Any]) -> dict[str, Any]:
    payload = dict(item or {})
    for key in ("metadata", "metadata_json"):
        metadata = _jsonish_dict(payload.get(key))
        if metadata:
            return metadata
    return {}


def _materialization_item_source_worker_ids(item: dict[str, Any]) -> list[int]:
    payload = dict(item or {})
    raw_values: list[Any] = []
    for key in ("source_worker_ids", "source_worker_ids_json"):
        values = _jsonish_list(payload.get(key))
        if values:
            raw_values.extend(values)
    metadata = _materialization_item_metadata(payload)
    for key in ("source_worker_ids", "applied_worker_ids"):
        values = _jsonish_list(metadata.get(key))
        if values:
            raw_values.extend(values)
    normalized: list[int] = []
    seen: set[int] = set()
    for value in raw_values:
        worker_id = _safe_int(value)
        if worker_id <= 0 or worker_id in seen:
            continue
        seen.add(worker_id)
        normalized.append(worker_id)
    return normalized


def _materialization_worker_lookup(agent_workers: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    lookup: dict[int, dict[str, Any]] = {}
    for worker in list(agent_workers or []):
        payload = dict(worker or {})
        worker_id = _safe_int(payload.get("worker_id") or payload.get("workerId"))
        if worker_id > 0:
            lookup[worker_id] = payload
    return lookup


def _worker_recovery_kind(worker: dict[str, Any]) -> str:
    payload = dict(worker or {})
    metadata = dict(payload.get("metadata") or {})
    return str(metadata.get("recovery_kind") or payload.get("recovery_kind") or "").strip()


def _materialization_sync_scope(
    *,
    item_kind: str,
    item: dict[str, Any],
    worker_by_id: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    if item_kind != "local_apply_closure":
        return {
            "sync_scope": item_kind,
            "duration_semantics": "durable_item_lifecycle",
            "duration_includes_deferred_wait": False,
        }

    payload = dict(item or {})
    metadata = _materialization_item_metadata(payload)
    source_worker_ids = _materialization_item_source_worker_ids(payload)
    source_workers = [dict(worker_by_id.get(worker_id) or {}) for worker_id in source_worker_ids]
    source_recovery_kinds = [kind for kind in (_worker_recovery_kind(worker) for worker in source_workers) if kind]
    metadata_recovery_kind = str(metadata.get("recovery_kind") or "").strip()
    if metadata_recovery_kind:
        source_recovery_kinds.append(metadata_recovery_kind)
    source_worker_keys = [
        str(worker.get("worker_key") or worker.get("workerKey") or "").strip()
        for worker in source_workers
        if str(worker.get("worker_key") or worker.get("workerKey") or "").strip()
    ]
    worker_kind = str(metadata.get("worker_kind") or "").strip()
    profile_url_count_for_budget = _safe_int(
        metadata.get("profile_url_count_for_budget") or payload.get("profile_url_count_for_budget")
    )
    is_profile_batch = (
        profile_url_count_for_budget > 0
        or any(kind == "harvest_profile_batch" for kind in source_recovery_kinds)
        or any(key.startswith("harvest_profile_batch::") for key in source_worker_keys)
    )
    is_candidate_source = not is_profile_batch and (
        worker_kind in {"company_roster", "search_seed", "search_seed_discovery"}
        or any(
            kind
            in {
                "harvest_company_employees",
                "harvest_company_people",
                "search_seed_discovery",
                "harvest_profile_search",
            }
            for kind in source_recovery_kinds
        )
    )
    inline_ingest = _jsonish_dict(metadata.get("inline_incremental_ingest"))
    sync_status = str(inline_ingest.get("sync_status") or "").strip()
    sync_reason = str(inline_ingest.get("sync_reason") or "").strip()
    duration_includes_deferred_wait = sync_status in {
        "deferred",
        "waiting",
        "waiting_prerequisite",
    } or bool(sync_reason)
    if is_profile_batch:
        scope = "profile_batch_local_apply"
    elif is_candidate_source:
        scope = "candidate_source_closure_lifecycle"
    else:
        scope = "local_apply_closure_lifecycle"
    return {
        "sync_scope": scope,
        "duration_semantics": "durable_item_lifecycle",
        "duration_includes_deferred_wait": duration_includes_deferred_wait,
        "deferred_sync_status": sync_status,
        "deferred_sync_reason": sync_reason,
        "source_worker_ids": source_worker_ids,
        "source_worker_recovery_kinds": sorted(set(source_recovery_kinds)),
        "source_worker_keys": source_worker_keys[:5],
        "worker_kind": worker_kind,
        "profile_url_count_for_budget": profile_url_count_for_budget,
    }


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _build_post_preview_finalization_report(
    *,
    stage_wall_clock: dict[str, dict[str, Any]],
    job_events: list[dict[str, Any]],
    timings_ms: dict[str, float],
    materialization_items: list[dict[str, Any]] | None = None,
    workflow_commands: list[dict[str, Any]] | None = None,
    agent_workers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    linkedin_stage_1_completed_at = str(
        dict(stage_wall_clock.get("linkedin_stage_1") or {}).get("completed_at") or ""
    ).strip()
    preview_completed_at = str(dict(stage_wall_clock.get("stage_1_preview") or {}).get("completed_at") or "").strip()
    if not linkedin_stage_1_completed_at:
        linkedin_stage_1_completed_at = _stage_boundary_completed_at_from_events(
            list(job_events or []),
            boundary="linkedin_stage_1",
        )
    if not preview_completed_at:
        preview_completed_at = _stage_boundary_completed_at_from_events(
            list(job_events or []),
            boundary="stage_1_preview",
        )
    final_stage = dict(stage_wall_clock.get("stage_2_final") or {})
    stage_2_final_completed_at = str(final_stage.get("completed_at") or "").strip()
    materialize_started_at: list[datetime] = []
    materialize_completed_at: list[datetime] = []
    finalization_started_at: list[datetime] = []
    finalization_completed_at: list[datetime] = []
    materialize_starts_by_kind: dict[str, list[datetime]] = {}
    materialize_sync_durations: list[float] = []
    materialize_sync_durations_by_scope: dict[str, list[float]] = defaultdict(list)
    materialize_sync_scope_counts: Counter[str] = Counter()
    materialize_syncs: list[dict[str, Any]] = []
    worker_by_id = _materialization_worker_lookup(list(agent_workers or []))

    for event in list(job_events or []):
        payload = _event_payload(dict(event or {}))
        phase = str(payload.get("phase") or "").strip().lower()
        reconcile_kind = str(payload.get("reconcile_kind") or "").strip()
        created_at = _parse_timestamp(_event_created_at(dict(event or {})))
        if created_at is None:
            continue
        event_family = str(payload.get("event_family") or "").strip()
        materialization_event = event_family in {"completed_workflow_reconcile", "workflow_materialization"}
        if (
            phase in {"started", "materialize_started"}
            and reconcile_kind
            in {
                "harvest_prefetch",
                "snapshot_materialization",
                "outreach_layering",
            }
            and (materialization_event or not event_family)
        ):
            finalization_started_at.append(created_at)
        if (
            phase in {"completed", "materialize_completed"}
            and reconcile_kind
            in {
                "harvest_prefetch",
                "snapshot_materialization",
                "outreach_layering",
            }
            and (materialization_event or not event_family)
        ):
            finalization_completed_at.append(created_at)
        if phase == "materialize_started" and (materialization_event or not event_family):
            materialize_started_at.append(created_at)
            materialize_starts_by_kind.setdefault(reconcile_kind or "unknown", []).append(created_at)
        elif phase == "materialize_completed" and (materialization_event or not event_family):
            materialize_completed_at.append(created_at)
            start_candidates = materialize_starts_by_kind.get(reconcile_kind or "unknown") or []
            started_at = start_candidates.pop(0) if start_candidates else None
            if started_at is not None:
                elapsed_ms = max(0.0, (created_at - started_at).total_seconds() * 1000)
                materialize_sync_durations.append(elapsed_ms)
                event_scope = reconcile_kind or "unknown"
                materialize_sync_durations_by_scope[event_scope].append(elapsed_ms)
                materialize_sync_scope_counts[event_scope] += 1
                materialize_syncs.append(
                    {
                        "reconcile_kind": reconcile_kind or "unknown",
                        "sync_scope": event_scope,
                        "duration_semantics": "event_started_to_completed",
                        "duration_includes_deferred_wait": False,
                        "started_at": started_at.isoformat(),
                        "completed_at": created_at.isoformat(),
                        "duration_ms": round(elapsed_ms, 2),
                    }
                )

    for item in list(materialization_items or []):
        payload = dict(item or {})
        if str(payload.get("status") or "").strip().lower() != "completed":
            continue
        item_kind = str(payload.get("item_kind") or "").strip()
        if item_kind not in {"local_apply_closure", "board_visible_delta_apply", "snapshot_full_materialization"}:
            continue
        completed_at = _parse_timestamp(str(payload.get("completed_at") or payload.get("updated_at") or "").strip())
        if completed_at is None:
            continue
        materialize_completed_at.append(completed_at)
        finalization_completed_at.append(completed_at)
        started_at = _parse_timestamp(str(payload.get("started_at") or payload.get("created_at") or "").strip())
        if started_at is not None:
            materialize_started_at.append(started_at)
            finalization_started_at.append(started_at)
        if started_at is not None and completed_at >= started_at:
            elapsed_ms = (completed_at - started_at).total_seconds() * 1000
            materialize_sync_durations.append(elapsed_ms)
            sync_scope = _materialization_sync_scope(
                item_kind=item_kind,
                item=payload,
                worker_by_id=worker_by_id,
            )
            scope_name = str(sync_scope.get("sync_scope") or item_kind)
            materialize_sync_durations_by_scope[scope_name].append(elapsed_ms)
            materialize_sync_scope_counts[scope_name] += 1
            item_metadata = _materialization_item_metadata(payload)
            materialize_syncs.append(
                {
                    "item_id": str(payload.get("item_id") or "").strip(),
                    "reconcile_kind": item_kind,
                    **sync_scope,
                    "candidate_count": _safe_int(payload.get("candidate_count")),
                    "metadata_candidate_count": _safe_int(item_metadata.get("candidate_count")),
                    "started_at": started_at.isoformat(),
                    "completed_at": completed_at.isoformat(),
                    "duration_ms": round(elapsed_ms, 2),
                }
            )

    for command in list(workflow_commands or []):
        payload = dict(command or {})
        if str(payload.get("status") or "").strip().lower() != "succeeded":
            continue
        command_type = str(payload.get("command_type") or "").strip()
        item_kind = _POST_TERMINAL_COMMAND_ITEM_KINDS.get(command_type, "")
        if not item_kind:
            continue
        command_payload = dict(payload.get("payload") or {})
        command_result = dict(payload.get("result") or {})
        completed_at = _parse_timestamp(
            _first_nonempty(
                command_result.get("completed_at"),
                command_result.get("applied_at"),
                payload.get("completed_at"),
                payload.get("updated_at"),
            )
        )
        if completed_at is None:
            continue
        started_at = _parse_timestamp(
            _first_nonempty(
                command_result.get("started_at"),
                command_result.get("claimed_at"),
                payload.get("started_at"),
                payload.get("claimed_at"),
            )
        )
        duration_semantics = "workflow_command_lifecycle"
        if started_at is None:
            started_at = completed_at
            duration_semantics = "workflow_command_terminal_timestamp_fallback"
        materialize_completed_at.append(completed_at)
        finalization_completed_at.append(completed_at)
        materialize_started_at.append(started_at)
        finalization_started_at.append(started_at)
        elapsed_ms = max(0.0, (completed_at - started_at).total_seconds() * 1000)
        sync_scope_label = item_kind
        if item_kind == "snapshot_full_materialization":
            sync_scope_label = "snapshot_full_materialization"
        elif item_kind == "board_visible_delta_apply":
            sync_scope_label = "board_visible_delta_apply"
        elif item_kind == "local_apply_closure":
            sync_scope_label = "local_apply_closure_lifecycle"
        materialize_sync_durations.append(elapsed_ms)
        materialize_sync_durations_by_scope[sync_scope_label].append(elapsed_ms)
        materialize_sync_scope_counts[sync_scope_label] += 1
        materialize_syncs.append(
            {
                "command_id": str(payload.get("command_id") or "").strip(),
                "command_type": command_type,
                "item_id": str(command_payload.get("item_id") or payload.get("command_id") or "").strip(),
                "reconcile_kind": item_kind,
                "sync_scope": sync_scope_label,
                "duration_semantics": duration_semantics,
                "duration_includes_deferred_wait": False,
                "owner": str(payload.get("owner") or "").strip(),
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": round(elapsed_ms, 2),
            }
        )

    stage_2_final_completed = _parse_timestamp(stage_2_final_completed_at)
    if stage_2_final_completed is not None:
        finalization_completed_at.append(stage_2_final_completed)

    first_finalization_start = min(finalization_started_at) if finalization_started_at else None
    first_materialize_start = min(materialize_started_at) if materialize_started_at else None
    last_materialize_completed = max(materialize_completed_at) if materialize_completed_at else None
    last_finalization_completed = max(finalization_completed_at) if finalization_completed_at else None
    raw_preview_completed = _parse_timestamp(preview_completed_at)
    effective_preview_completed = raw_preview_completed
    preview_timestamp_source = "stage_1_preview_completed_at" if raw_preview_completed is not None else ""
    finalization_boundaries = [
        item
        for item in (
            first_finalization_start,
            first_materialize_start,
            last_materialize_completed,
            last_finalization_completed,
        )
        if item is not None
    ]
    earliest_finalization_boundary = min(finalization_boundaries) if finalization_boundaries else None
    if (
        raw_preview_completed is not None
        and earliest_finalization_boundary is not None
        and raw_preview_completed > earliest_finalization_boundary
    ):
        effective_preview_completed = earliest_finalization_boundary
        preview_timestamp_source = "first_finalization_event_when_stage_preview_timestamp_is_late"
    effective_preview_completed_at = (
        effective_preview_completed.isoformat() if effective_preview_completed is not None else preview_completed_at
    )

    preview_to_first_finalization_start = (
        _reasonable_elapsed_ms(effective_preview_completed_at, first_finalization_start.isoformat())
        if first_finalization_start is not None
        else None
    )
    stage1_terminal_to_first_finalization_start = (
        _reasonable_elapsed_ms(linkedin_stage_1_completed_at, first_finalization_start.isoformat())
        if first_finalization_start is not None and linkedin_stage_1_completed_at
        else None
    )
    profile_terminal_at, profile_terminal_source = _profile_terminal_at_from_commands_or_workers(
        workflow_commands=list(workflow_commands or []),
        agent_workers=list(agent_workers or []),
    )
    stage1_terminal_at = _parse_timestamp(linkedin_stage_1_completed_at)
    finalization_start_gate_source = "linkedin_stage_1_completed_at" if stage1_terminal_at else ""
    finalization_start_gate_boundary = stage1_terminal_at
    profile_wait_excluded_ms = 0.0
    if (
        profile_terminal_at is not None
        and stage1_terminal_at is not None
        and first_finalization_start is not None
        and profile_terminal_at > stage1_terminal_at
    ):
        finalization_start_gate_boundary = profile_terminal_at
        finalization_start_gate_source = profile_terminal_source or "profile_terminal_at"
        profile_wait_excluded_ms = max(0.0, (profile_terminal_at - stage1_terminal_at).total_seconds() * 1000)
    finalization_start_gate_ms: float | None
    if finalization_start_gate_boundary is not None and first_finalization_start is not None:
        if first_finalization_start <= finalization_start_gate_boundary:
            finalization_start_gate_ms = 0.0
        else:
            finalization_start_gate_ms = _reasonable_elapsed_ms(
                finalization_start_gate_boundary.isoformat(),
                first_finalization_start.isoformat(),
            )
    else:
        finalization_start_gate_ms = None
    profile_terminal_to_first_finalization_start = (
        _reasonable_elapsed_ms(profile_terminal_at.isoformat(), first_finalization_start.isoformat())
        if profile_terminal_at is not None and first_finalization_start is not None
        else None
    )
    preview_to_first_materialize_start = (
        _reasonable_elapsed_ms(effective_preview_completed_at, first_materialize_start.isoformat())
        if first_materialize_start is not None
        else None
    )
    preview_to_last_materialize_completed = (
        _reasonable_elapsed_ms(effective_preview_completed_at, last_materialize_completed.isoformat())
        if last_materialize_completed is not None
        else None
    )
    preview_to_finalization_completed = (
        _reasonable_elapsed_ms(effective_preview_completed_at, last_finalization_completed.isoformat())
        if last_finalization_completed is not None
        else None
    )
    raw_long_post_preview_finalization = (preview_to_finalization_completed or 0.0) > _POST_PREVIEW_FINALIZATION_LAG_MS
    finalization_lag_evaluation_ms = preview_to_finalization_completed or 0.0
    finalization_lag_evaluation_source = "preview_to_finalization_completed_ms"
    if profile_wait_excluded_ms > 0.0 and finalization_start_gate_ms is not None:
        finalization_lag_evaluation_ms = finalization_start_gate_ms
        finalization_lag_evaluation_source = (
            finalization_start_gate_source or "profile_terminal_finalization_start_gate"
        )
    return {
        "report_available": bool(
            (preview_completed_at and (job_events or stage_2_final_completed_at))
            or materialize_completed_at
            or finalization_completed_at
        ),
        "linkedin_stage_1_completed_at": linkedin_stage_1_completed_at,
        "stage_1_preview_completed_at": preview_completed_at,
        "effective_stage_1_preview_completed_at": effective_preview_completed_at,
        "stage_1_preview_timestamp_source": preview_timestamp_source,
        "first_finalization_started_at": first_finalization_start.isoformat() if first_finalization_start else "",
        "first_materialize_started_at": first_materialize_start.isoformat() if first_materialize_start else "",
        "last_materialize_completed_at": last_materialize_completed.isoformat() if last_materialize_completed else "",
        "finalization_completed_at": last_finalization_completed.isoformat() if last_finalization_completed else "",
        "stage_2_final_completed_at": stage_2_final_completed_at,
        "profile_terminal_at": profile_terminal_at.isoformat() if profile_terminal_at else "",
        "profile_terminal_source": profile_terminal_source,
        "preview_to_first_finalization_start_ms": round(preview_to_first_finalization_start or 0.0, 2),
        "stage1_terminal_to_first_finalization_start_ms": round(
            stage1_terminal_to_first_finalization_start or 0.0,
            2,
        ),
        "profile_terminal_to_first_finalization_start_ms": round(
            profile_terminal_to_first_finalization_start or 0.0,
            2,
        ),
        "finalization_start_gate_ms": round(finalization_start_gate_ms or 0.0, 2),
        "finalization_start_gate_source": finalization_start_gate_source,
        "profile_wait_excluded_from_finalization_gate_ms": round(profile_wait_excluded_ms, 2),
        "finalization_lag_evaluation_ms": round(finalization_lag_evaluation_ms, 2),
        "finalization_lag_evaluation_source": finalization_lag_evaluation_source,
        "raw_long_post_preview_finalization": raw_long_post_preview_finalization,
        "preview_to_first_materialize_start_ms": round(preview_to_first_materialize_start or 0.0, 2),
        "preview_to_last_materialize_completed_ms": round(preview_to_last_materialize_completed or 0.0, 2),
        "preview_to_finalization_completed_ms": round(preview_to_finalization_completed or 0.0, 2),
        "post_terminal_worker_recovery_ms": round(float(timings_ms.get("post_terminal_worker_recovery") or 0.0), 2),
        "materialize_started_count": len(materialize_started_at),
        "materialize_completed_count": len(materialize_completed_at),
        "finalization_started_event_count": len(finalization_started_at),
        "finalization_completed_event_count": len(finalization_completed_at),
        "materialize_sync_duration_ms": _numeric_summary(materialize_sync_durations),
        "materialize_sync_duration_by_scope_ms": {
            scope: _numeric_summary(values) for scope, values in sorted(materialize_sync_durations_by_scope.items())
        },
        "materialize_sync_scope_counts": dict(materialize_sync_scope_counts),
        "profile_batch_local_apply_duration_ms": _numeric_summary(
            materialize_sync_durations_by_scope.get("profile_batch_local_apply", [])
        ),
        "candidate_source_closure_lifecycle_ms": _numeric_summary(
            materialize_sync_durations_by_scope.get("candidate_source_closure_lifecycle", [])
        ),
        "local_apply_closure_lifecycle_ms": _numeric_summary(
            materialize_sync_durations_by_scope.get("local_apply_closure_lifecycle", [])
        ),
        "materialize_syncs": materialize_syncs[:10],
        "long_post_preview_finalization": finalization_lag_evaluation_ms > _POST_PREVIEW_FINALIZATION_LAG_MS,
        "long_post_preview_finalization_threshold_ms": _POST_PREVIEW_FINALIZATION_LAG_MS,
    }


def _profile_terminal_at_from_commands_or_workers(
    *,
    workflow_commands: list[dict[str, Any]],
    agent_workers: list[dict[str, Any]],
) -> tuple[datetime | None, str]:
    command_terminal_at = _profile_terminal_at_from_terminal_record_commands(workflow_commands)
    if command_terminal_at is not None:
        return command_terminal_at, "profile_url_terminal_record_command_completed_at"
    worker_terminal_at = _profile_terminal_at_from_workers(agent_workers)
    if worker_terminal_at is not None:
        return worker_terminal_at, "profile_terminal_at"
    return None, ""


def _profile_terminal_at_from_terminal_record_commands(workflow_commands: list[dict[str, Any]]) -> datetime | None:
    terminal_times: list[datetime] = []
    for command in list(workflow_commands or []):
        payload = dict(command or {})
        if str(payload.get("command_type") or "").strip() != LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE:
            continue
        if str(payload.get("status") or "").strip().lower() != "succeeded":
            continue
        result = dict(payload.get("result") or {})
        terminal_at = _parse_timestamp(
            _first_nonempty(
                result.get("completed_at"),
                result.get("recorded_at"),
                result.get("applied_at"),
                payload.get("completed_at"),
                payload.get("updated_at"),
            )
        )
        if terminal_at is not None:
            terminal_times.append(terminal_at)
    return max(terminal_times) if terminal_times else None


def _profile_terminal_at_from_workers(agent_workers: list[dict[str, Any]]) -> datetime | None:
    terminal_times: list[datetime] = []
    for worker in list(agent_workers or []):
        payload = dict(worker or {})
        metadata = dict(payload.get("metadata") or {})
        if str(metadata.get("recovery_kind") or "").strip() != "harvest_profile_batch":
            continue
        if str(payload.get("status") or "").strip().lower() not in {
            "completed",
            "failed",
            "skipped",
            "cancelled",
            "canceled",
        }:
            continue
        checkpoint = dict(payload.get("checkpoint") or {})
        remote_event = dict(checkpoint.get("remote_provider_terminal_event") or {})
        remote_event_metrics = dict(checkpoint.get("remote_provider_terminal_event_metrics") or {})
        terminal_at = _parse_timestamp(
            _first_nonempty(
                checkpoint.get("remote_completed_at"),
                checkpoint.get("scripted_remote_completed_at"),
                remote_event.get("remote_completed_at"),
                remote_event.get("event_created_at"),
                remote_event_metrics.get("remote_completed_at"),
                checkpoint.get("remote_provider_terminal_event_seen_at"),
                payload.get("completed_at"),
                payload.get("updated_at"),
            )
        )
        if terminal_at is not None:
            terminal_times.append(terminal_at)
    return max(terminal_times) if terminal_times else None


def _build_behavior_guardrails_report(
    *,
    explain_payload: dict[str, Any],
    results_payload: dict[str, Any],
    stage_wall_clock: dict[str, dict[str, Any]],
    board_probe: dict[str, Any],
    workflow_wall_clock: dict[str, float],
    provider_invocations: list[dict[str, Any]] | None = None,
    materialization_streaming: dict[str, Any] | None = None,
    event_level_efficiency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    duplicate_provider_dispatch = _build_duplicate_provider_dispatch_report(list(provider_invocations or []))
    disabled_stage_violations = _build_disabled_stage_violation_report(
        explain_payload=explain_payload,
        results_payload=results_payload,
    )
    prerequisite_gaps = _build_prerequisite_gap_report(stage_wall_clock)
    final_results_board_consistency = _build_final_results_board_consistency_report(
        results_payload=results_payload,
        board_probe=board_probe,
        workflow_wall_clock=workflow_wall_clock,
    )
    streaming_materialization = _build_streaming_materialization_guardrail_report(dict(materialization_streaming or {}))
    event_efficiency = dict(event_level_efficiency or {})
    violation_count = (
        int(duplicate_provider_dispatch.get("violation_count") or 0)
        + int(disabled_stage_violations.get("violation_count") or 0)
        + int(final_results_board_consistency.get("violation_count") or 0)
        + int(streaming_materialization.get("violation_count") or 0)
        + int(event_efficiency.get("violation_count") or 0)
    )
    diagnostic_violation_count = int(prerequisite_gaps.get("violation_count") or 0)
    return {
        "duplicate_provider_dispatch": duplicate_provider_dispatch,
        "disabled_stage_violations": disabled_stage_violations,
        "prerequisite_gaps": prerequisite_gaps,
        "final_results_board_consistency": final_results_board_consistency,
        "streaming_materialization": streaming_materialization,
        "event_level_efficiency": event_efficiency,
        "violation_count": violation_count,
        "violation_detected": violation_count > 0,
        "diagnostic_violation_count": diagnostic_violation_count,
        "diagnostic_violation_detected": diagnostic_violation_count > 0,
    }


def _stage_candidate_count(summary: dict[str, Any]) -> int:
    candidate_source = dict(summary.get("candidate_source") or {})
    return max(
        _safe_int(summary.get("candidate_count")),
        _safe_int(summary.get("returned_matches")),
        _safe_int(summary.get("total_matches")),
        _safe_int(candidate_source.get("candidate_count")),
    )


def _stage_manual_review_count(summary: dict[str, Any]) -> int:
    return max(
        _safe_int(summary.get("manual_review_count")),
        _safe_int(summary.get("manual_review_queue_count")),
    )


def _build_stage_wall_clock_report(results_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    workflow_stage_summaries = dict(results_payload.get("workflow_stage_summaries") or {})
    summaries = {
        str(key): dict(value or {})
        for key, value in dict(workflow_stage_summaries.get("summaries") or {}).items()
        if str(key).strip() and isinstance(value, dict)
    }
    stage_order: list[str] = []
    seen_stage_names: set[str] = set()
    for item in list(_PROVIDER_STAGE_ORDER) + list(workflow_stage_summaries.get("stage_order") or []):
        stage_name = str(item).strip()
        if not stage_name or stage_name in seen_stage_names:
            continue
        seen_stage_names.add(stage_name)
        stage_order.append(stage_name)
    report: dict[str, dict[str, Any]] = {}
    for stage_name in stage_order:
        summary = dict(summaries.get(stage_name) or {})
        if not summary:
            continue
        started_at = str(summary.get("started_at") or "").strip()
        completed_at = str(summary.get("completed_at") or summary.get("saved_at") or "").strip()
        stage_report = {
            "status": str(summary.get("status") or ""),
            "candidate_count": _stage_candidate_count(summary),
            "manual_review_count": _stage_manual_review_count(summary),
        }
        elapsed = _elapsed_ms(started_at, completed_at)
        if started_at:
            stage_report["started_at"] = started_at
        if completed_at:
            stage_report["completed_at"] = completed_at
        if elapsed is not None and elapsed <= _MAX_REASONABLE_STAGE_WALL_CLOCK_MS:
            stage_report["wall_clock_ms"] = elapsed
        report[stage_name] = stage_report
    return report


def _earliest_stage_started_at(stage_wall_clock: dict[str, dict[str, Any]]) -> str:
    earliest: datetime | None = None
    earliest_value = ""
    for stage_payload in stage_wall_clock.values():
        started_at = str(dict(stage_payload).get("started_at") or "").strip()
        parsed = _parse_timestamp(started_at)
        if parsed is None:
            continue
        if earliest is None or parsed < earliest:
            earliest = parsed
            earliest_value = started_at
    return earliest_value


def _workflow_wall_clock_anchor_started_at(
    *,
    stage_wall_clock: dict[str, dict[str, Any]],
    job_events: list[dict[str, Any]] | None,
) -> str:
    stage_anchor = _earliest_stage_started_at(stage_wall_clock)
    if stage_anchor:
        return stage_anchor
    earliest: datetime | None = None
    earliest_value = ""
    for event in list(job_events or []):
        created_at = str(dict(event or {}).get("created_at") or "").strip()
        parsed = _parse_timestamp(created_at)
        if parsed is None:
            continue
        if earliest is None or parsed < earliest:
            earliest = parsed
            earliest_value = created_at
    return earliest_value


def _build_workflow_wall_clock_report(
    *,
    stage_wall_clock: dict[str, dict[str, Any]],
    board_probe: dict[str, Any],
    timings_ms: dict[str, float],
    timeline: list[dict[str, Any]] | None = None,
    job_events: list[dict[str, Any]] | None = None,
    board_visible_patches: list[dict[str, Any]] | None = None,
    post_preview_finalization: dict[str, Any] | None = None,
) -> dict[str, float]:
    anchor_started_at = _workflow_wall_clock_anchor_started_at(
        stage_wall_clock=stage_wall_clock,
        job_events=job_events,
    )
    preview_completed_at = str(dict(stage_wall_clock.get("stage_1_preview") or {}).get("completed_at") or "").strip()
    post_preview = dict(post_preview_finalization or {})
    effective_preview_completed_at = str(post_preview.get("effective_stage_1_preview_completed_at") or "").strip()
    raw_preview_completed = _parse_timestamp(preview_completed_at)
    effective_preview_completed = _parse_timestamp(effective_preview_completed_at)
    if effective_preview_completed is not None and (
        raw_preview_completed is None or effective_preview_completed < raw_preview_completed
    ):
        preview_completed_at = effective_preview_completed_at
    final_stage = dict(stage_wall_clock.get("stage_2_final") or stage_wall_clock.get("public_web_stage_2") or {})
    final_completed_at = str(final_stage.get("completed_at") or "").strip()
    report: dict[str, Any] = {}
    timeline = list(timeline or [])
    job_submit_offset_ms = round(
        sum(float(timings_ms.get(key) or 0.0) for key in ("explain", "plan", "review", "start")),
        2,
    )

    def _timeline_milestone_observed_ms(
        *needles: str, require_stage: str = "", require_status: str = ""
    ) -> float | None:
        normalized_needles = tuple(str(item).strip().lower() for item in needles if str(item).strip())
        for item in timeline:
            message = str(item.get("message") or "").strip().lower()
            stage = str(item.get("stage") or "").strip().lower()
            status = str(item.get("status") or "").strip().lower()
            if require_stage and stage != require_stage:
                continue
            if require_status and status != require_status:
                continue
            if normalized_needles and not any(needle in message for needle in normalized_needles):
                continue
            observed_at_ms = float(item.get("observed_at_ms") or 0.0)
            if observed_at_ms <= 0.0:
                continue
            return round(max(0.0, observed_at_ms - job_submit_offset_ms), 2)
        return None

    def _board_visible_elapsed_ms(*, include_full_snapshot: bool) -> float | None:
        candidates: list[float] = []
        for patch in list(board_visible_patches or []):
            patch_payload = dict(patch or {})
            patch_phase = str(patch_payload.get("patch_phase") or "").strip()
            patch_kind = str(patch_payload.get("kind") or "").strip()
            is_full_snapshot = bool(
                patch_phase == "board_visible_full_snapshot_serving"
                or patch_kind == "full_snapshot_board_visible_patch"
            )
            if is_full_snapshot and not include_full_snapshot:
                continue
            patch_visible_count = max(
                _safe_int(patch_payload.get("served_candidate_count")),
                _safe_int(patch_payload.get("cumulative_candidate_count")),
                len(list(patch_payload.get("candidate_ids") or []))
                if isinstance(patch_payload.get("candidate_ids"), list)
                else 0,
            )
            if patch_visible_count <= 0:
                continue
            elapsed = _reasonable_elapsed_ms(anchor_started_at, str(patch_payload.get("published_at") or ""))
            if elapsed is not None:
                candidates.append(elapsed)
        for event in list(job_events or []):
            payload = dict(event.get("payload") or {})
            board_patch = dict(payload.get("board_visible_patch") or {})
            phase = str(payload.get("phase") or "").strip()
            detail = str(event.get("detail") or "").strip().lower()
            is_full_snapshot = bool(
                phase == "board_visible_full_snapshot_serving"
                or str(board_patch.get("kind") or "").strip() == "full_snapshot_board_visible_patch"
            )
            if is_full_snapshot and not include_full_snapshot:
                continue
            if not board_patch and "partial delta overlay" not in detail:
                continue
            patch_visible_count = max(
                _safe_int(board_patch.get("served_candidate_count")),
                _safe_int(board_patch.get("cumulative_candidate_count")),
                len(list(board_patch.get("candidate_ids") or []))
                if isinstance(board_patch.get("candidate_ids"), list)
                else 0,
            )
            if patch_visible_count <= 0:
                continue
            elapsed = _reasonable_elapsed_ms(anchor_started_at, str(event.get("created_at") or ""))
            if elapsed is not None:
                candidates.append(elapsed)
        if not candidates:
            return None
        return round(min(value for value in candidates if value >= 0.0), 2)

    def _partial_board_visible_elapsed_ms() -> float | None:
        return _board_visible_elapsed_ms(include_full_snapshot=False)

    def _board_nonempty_publication_elapsed_ms() -> float | None:
        return _board_visible_elapsed_ms(include_full_snapshot=True)

    def _board_runtime_publication_elapsed_ms() -> float | None:
        runtime_state = dict(board_probe.get("board_runtime_state") or {})
        if _board_runtime_visible_candidate_count(runtime_state) <= 0:
            return None
        timestamp = _first_nonempty(
            runtime_state.get("row_publication_started_at"),
            runtime_state.get("row_publication_updated_at"),
            runtime_state.get("published_at"),
            runtime_state.get("updated_at"),
            runtime_state.get("completed_at"),
        )
        if not timestamp:
            return None
        return _reasonable_elapsed_ms(anchor_started_at, timestamp)

    job_to_preview = _reasonable_elapsed_ms(anchor_started_at, preview_completed_at)
    preview_observed_ms = _timeline_milestone_observed_ms(
        "stage 1 preview ready",
        "stage 1 preview 已生成",
        "stage 1 preview is ready",
    )
    if preview_observed_ms is not None:
        report["job_to_stage_1_preview"] = preview_observed_ms
    elif job_to_preview is not None and job_to_preview > 0.0:
        report["job_to_stage_1_preview"] = job_to_preview
    elif job_to_preview is not None:
        report["job_to_stage_1_preview"] = job_to_preview
    else:
        board_visible_preview_ms = _partial_board_visible_elapsed_ms()
        if board_visible_preview_ms is None:
            board_visible_preview_ms = _board_runtime_publication_elapsed_ms()
        if board_visible_preview_ms is not None:
            report["job_to_stage_1_preview"] = board_visible_preview_ms
            report["job_to_stage_1_preview_source"] = "board_visible_publication_when_preview_anchor_missing"

    job_to_final = _reasonable_elapsed_ms(anchor_started_at, final_completed_at)
    final_results_observed_ms = _timeline_milestone_observed_ms(
        "local asset population is ready",
        "public web stage 2 acquisition completed",
        "finalizing asset population",
        require_status="completed",
    )
    if final_results_observed_ms is None:
        inferred_final_ms = round(
            float(timings_ms.get("wait_for_completion") or 0.0) + float(timings_ms.get("fetch_job_and_results") or 0.0),
            2,
        )
        if inferred_final_ms > 0.0:
            final_results_observed_ms = inferred_final_ms
    if final_results_observed_ms is not None:
        report["job_to_final_results"] = final_results_observed_ms
    elif job_to_final is not None and job_to_final > 0.0:
        report["job_to_final_results"] = job_to_final
    elif job_to_final is not None:
        report["job_to_final_results"] = job_to_final

    preview_to_final = _reasonable_elapsed_ms(preview_completed_at, final_completed_at)
    preview_anchor_ms = float(report.get("job_to_stage_1_preview") or 0.0)
    final_anchor_ms = float(report.get("job_to_final_results") or 0.0)
    post_preview_finalization_ms = _safe_float(post_preview.get("preview_to_finalization_completed_ms"))
    if (
        post_preview_finalization_ms > 0.0
        and preview_to_final is not None
        and post_preview_finalization_ms > preview_to_final
    ):
        report["stage_1_preview_to_final_results"] = post_preview_finalization_ms
        report["stage_1_preview_to_final_results_source"] = "post_preview_finalization_durable_evidence"
    elif preview_to_final is not None:
        report["stage_1_preview_to_final_results"] = preview_to_final
        report["stage_1_preview_to_final_results_source"] = "workflow_stage_summaries"
    elif preview_anchor_ms > 0.0 and final_anchor_ms >= preview_anchor_ms:
        report["stage_1_preview_to_final_results"] = round(final_anchor_ms - preview_anchor_ms, 2)
        report["stage_1_preview_to_final_results_source"] = "smoke_observed_wall_clock"

    board_probe_wait_ms = round(float(timings_ms.get("board_probe_wait") or 0.0), 2)
    board_ready_wait_ms = round(
        float(timings_ms.get("board_ready_wait") or timings_ms.get("board_ready_wait_ms") or board_probe_wait_ms),
        2,
    )
    board_nonempty_wait_ms = round(
        float(timings_ms.get("board_nonempty_wait") or timings_ms.get("board_nonempty_wait_ms") or board_probe_wait_ms),
        2,
    )
    if bool(board_probe.get("ready")) and board_ready_wait_ms >= 0.0:
        report["final_results_to_board_ready"] = board_ready_wait_ms
        if final_anchor_ms > 0.0:
            report["job_to_board_ready"] = round(final_anchor_ms + board_ready_wait_ms, 2)
    if bool(board_probe.get("ready_nonempty")) and board_nonempty_wait_ms >= 0.0:
        report["final_results_to_board_nonempty"] = board_nonempty_wait_ms
        if final_anchor_ms > 0.0:
            report["job_to_board_nonempty"] = round(final_anchor_ms + board_nonempty_wait_ms, 2)

    def _apply_board_nonempty_elapsed_ms(elapsed_ms: float) -> None:
        if "job_to_board_ready" in report:
            report["job_to_board_ready"] = round(min(float(report["job_to_board_ready"]), elapsed_ms), 2)
        else:
            report["job_to_board_ready"] = elapsed_ms
        if "job_to_board_nonempty" in report:
            report["job_to_board_nonempty"] = round(min(float(report["job_to_board_nonempty"]), elapsed_ms), 2)
        else:
            report["job_to_board_nonempty"] = elapsed_ms
        if final_anchor_ms > 0.0:
            final_to_board_ms = round(max(0.0, elapsed_ms - final_anchor_ms), 2)
            if "final_results_to_board_ready" in report:
                report["final_results_to_board_ready"] = round(
                    min(float(report["final_results_to_board_ready"]), final_to_board_ms),
                    2,
                )
            else:
                report["final_results_to_board_ready"] = final_to_board_ms
            if "final_results_to_board_nonempty" in report:
                report["final_results_to_board_nonempty"] = round(
                    min(float(report["final_results_to_board_nonempty"]), final_to_board_ms),
                    2,
                )
            else:
                report["final_results_to_board_nonempty"] = final_to_board_ms

    def _apply_partial_board_visible_elapsed_ms(elapsed_ms: float) -> None:
        if "job_to_board_visible_partial" in report:
            report["job_to_board_visible_partial"] = round(
                min(float(report["job_to_board_visible_partial"]), elapsed_ms),
                2,
            )
        else:
            report["job_to_board_visible_partial"] = elapsed_ms
        _apply_board_nonempty_elapsed_ms(elapsed_ms)

    partial_board_visible_ms = _partial_board_visible_elapsed_ms()
    if partial_board_visible_ms is not None:
        _apply_partial_board_visible_elapsed_ms(partial_board_visible_ms)
    board_nonempty_publication_ms = _board_nonempty_publication_elapsed_ms()
    if board_nonempty_publication_ms is not None:
        _apply_board_nonempty_elapsed_ms(board_nonempty_publication_ms)
    board_runtime_publication_ms = _board_runtime_publication_elapsed_ms()
    if board_runtime_publication_ms is not None:
        runtime_state = dict(board_probe.get("board_runtime_state") or {})
        runtime_phase = str(runtime_state.get("phase") or "").strip()
        runtime_publication_status = str(runtime_state.get("publication_status") or "").strip()
        if runtime_phase == "current_snapshot_serving" or runtime_publication_status == "complete":
            _apply_board_nonempty_elapsed_ms(board_runtime_publication_ms)
            _apply_partial_board_visible_elapsed_ms(board_runtime_publication_ms)
        else:
            _apply_partial_board_visible_elapsed_ms(board_runtime_publication_ms)
    return report


def _build_search_report(
    *,
    latest_metrics: dict[str, Any],
    background_reconcile: dict[str, Any],
) -> dict[str, Any]:
    search_seed = dict(background_reconcile.get("search_seed") or {})
    refresh_metrics = dict(latest_metrics.get("refresh_metrics") or {})
    return {
        "query_count": _safe_int(latest_metrics.get("query_count")),
        "queued_query_count": _safe_int(
            latest_metrics.get("queued_query_count") or search_seed.get("queued_query_count")
        ),
        "observed_company_candidate_count": _safe_int(latest_metrics.get("observed_company_candidate_count")),
        "search_seed_entry_count": _safe_int(search_seed.get("entry_count")),
        "search_seed_added_entry_count": _safe_int(
            search_seed.get("added_entry_count") or refresh_metrics.get("background_search_seed_added_entry_count")
        ),
        "search_seed_worker_count": _safe_int(
            search_seed.get("applied_worker_count") or refresh_metrics.get("background_search_seed_worker_count")
        ),
        "search_seed_status": str(search_seed.get("status") or ""),
    }


def _build_roster_report(
    *,
    latest_metrics: dict[str, Any],
    background_reconcile: dict[str, Any],
    candidate_source: dict[str, Any],
) -> dict[str, Any]:
    company_roster = dict(background_reconcile.get("company_roster") or {})
    roster_entry_count = _safe_int(
        company_roster.get("entry_count")
        or latest_metrics.get("company_roster_entry_count")
        or latest_metrics.get("roster_entry_count")
    )
    roster_added_entry_count = _safe_int(
        company_roster.get("added_entry_count")
        or latest_metrics.get("company_roster_added_entry_count")
        or latest_metrics.get("roster_added_entry_count")
    )
    candidate_source_count = _safe_int(candidate_source.get("candidate_count"))
    observed_company_candidate_count = _safe_int(latest_metrics.get("observed_company_candidate_count"))
    return {
        "status": str(company_roster.get("status") or ""),
        "entry_count": roster_entry_count,
        "added_entry_count": roster_added_entry_count,
        "candidate_source_count": candidate_source_count,
        "observed_company_candidate_count": observed_company_candidate_count,
        "returned_count": max(roster_entry_count, roster_added_entry_count, candidate_source_count),
        "available_shard_count": _safe_int(company_roster.get("available_shard_count")),
        "expected_shard_count": _safe_int(company_roster.get("expected_shard_count")),
        "completion_status": str(company_roster.get("completion_status") or ""),
    }


def _provider_anomaly_query_summaries_from_search_seed_artifacts(
    *,
    job_summary: dict[str, Any],
    result_job_summary: dict[str, Any],
    candidate_source: dict[str, Any],
) -> list[dict[str, Any]]:
    """Read provider-quality query summaries from the snapshot search artifact.

    Provider-quality anomaly metrics used to come from legacy
    job_materialization_items. Normal execution no longer writes those rows, so
    smoke reporting must consume the canonical search-seed summary artifact
    emitted by LinkedIn acquisition instead of relying on migration-era rows.
    """

    summary_paths: list[Path] = []
    seen_paths: set[str] = set()

    def _append_summary_path(value: Any) -> None:
        raw_path = str(value or "").strip()
        if not raw_path:
            return
        path = Path(raw_path).expanduser()
        candidates: list[Path] = []
        if path.name == "summary.json" and path.parent.name == "search_seed_discovery":
            candidates.append(path)
        if path.name.startswith("candidate_documents") and path.suffix == ".json":
            candidates.append(path.parent / "search_seed_discovery" / "summary.json")
        for candidate in candidates:
            key = str(candidate)
            if key in seen_paths:
                continue
            seen_paths.add(key)
            summary_paths.append(candidate)

    def _walk_paths(payload: Any, *, depth: int = 0) -> None:
        if depth > 8:
            return
        if isinstance(payload, dict):
            for key, value in payload.items():
                normalized_key = str(key or "").strip().lower()
                if normalized_key in {
                    "candidate_doc_path",
                    "candidate_documents_path",
                    "source_path",
                    "stage_candidate_doc_path",
                    "summary_path",
                    "search_seed_summary_path",
                }:
                    _append_summary_path(value)
                _walk_paths(value, depth=depth + 1)
            return
        if isinstance(payload, list):
            for item in payload[:50]:
                _walk_paths(item, depth=depth + 1)

    _walk_paths(candidate_source)
    _walk_paths(result_job_summary)
    _walk_paths(job_summary)

    summaries: list[dict[str, Any]] = []
    seen_summaries: set[str] = set()
    for summary_path in summary_paths:
        if not summary_path.exists():
            continue
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for summary in list(dict(payload).get("query_summaries") or []):
            if not isinstance(summary, dict):
                continue
            key = json.dumps(
                {
                    "path": str(summary_path),
                    "query": str(summary.get("query") or ""),
                    "mode": str(summary.get("mode") or summary.get("provider") or ""),
                    "employment_status": str(summary.get("employment_status") or summary.get("employment_scope") or ""),
                    "status": str(summary.get("status") or ""),
                    "reason": str(summary.get("incomplete_reason") or summary.get("degraded_reason") or ""),
                },
                sort_keys=True,
            )
            if key in seen_summaries:
                continue
            seen_summaries.add(key)
            summaries.append(dict(summary))
    return summaries


def _build_profile_completion_report(background_reconcile: dict[str, Any]) -> dict[str, Any]:
    harvest_prefetch = dict(background_reconcile.get("harvest_prefetch") or {})
    resume_result = dict(harvest_prefetch.get("resume_result") or {})
    profile_completion_result = dict(resume_result.get("profile_completion_result") or {})
    completion_metrics = dict(profile_completion_result.get("result") or {})
    artifact_summary = dict(dict(profile_completion_result.get("artifact_result") or {}).get("summary") or {})
    return {
        "status": str(profile_completion_result.get("status") or ""),
        "harvest_prefetch_status": str(harvest_prefetch.get("status") or ""),
        "applied_worker_count": _safe_int(harvest_prefetch.get("applied_worker_count")),
        "fetched_profile_count": _safe_int(completion_metrics.get("fetched_profile_count")),
        "profile_detail_count": _safe_int(artifact_summary.get("profile_detail_count")),
        "structured_experience_count": _safe_int(artifact_summary.get("structured_experience_count")),
        "structured_education_count": _safe_int(artifact_summary.get("structured_education_count")),
    }


def _build_board_probe_report(
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> dict[str, Any]:
    asset_population = dict(dashboard_payload.get("asset_population") or {})
    dashboard_board_runtime = dict(dashboard_payload.get("board_runtime_state") or {})
    candidate_page_board_runtime = dict(candidate_page_payload.get("board_runtime_state") or {})
    board_runtime_state = {**dashboard_board_runtime, **candidate_page_board_runtime}
    dashboard_profile_progress = dict(asset_population.get("profile_fetch_progress") or {})
    page_profile_progress = dict(candidate_page_payload.get("profile_fetch_progress") or {})
    profile_fetch_progress = {**dashboard_profile_progress, **page_profile_progress}
    dashboard_preview_candidates = list(asset_population.get("candidates") or [])
    ranked_preview = list(dashboard_payload.get("results") or dashboard_payload.get("ranked_results") or [])
    preview_count = len(dashboard_preview_candidates) if dashboard_preview_candidates else len(ranked_preview)
    runtime_visible_candidate_count = _board_runtime_visible_candidate_count(board_runtime_state)
    result_mode = str(candidate_page_payload.get("result_mode") or board_runtime_state.get("result_mode") or "").strip()
    total_candidates = max(
        runtime_visible_candidate_count,
        _safe_int(
            candidate_page_payload.get("total_candidates")
            or asset_population.get("candidate_count")
            or dashboard_payload.get("ranked_result_count")
        ),
    )
    returned_count = _safe_int(candidate_page_payload.get("returned_count"))
    ready = bool(result_mode) or runtime_visible_candidate_count > 0
    return {
        "ready": ready,
        "ready_nonempty": ready and max(total_candidates, returned_count) > 0,
        "result_mode": result_mode,
        "asset_population_available": bool(asset_population.get("available")),
        "dashboard_candidate_count": _safe_int(
            asset_population.get("candidate_count") or dashboard_payload.get("ranked_result_count")
        ),
        "dashboard_preview_returned_count": preview_count,
        "candidate_page_total_candidates": total_candidates,
        "candidate_page_returned_count": returned_count,
        "has_more": bool(candidate_page_payload.get("has_more")),
        "layering_status": str(
            board_runtime_state.get("layering_status")
            or board_runtime_state.get("outreach_layering_status")
            or dict(dashboard_payload.get("result_view_lifecycle") or {}).get("outreach_layering_status")
            or dict(candidate_page_payload.get("result_view_lifecycle") or {}).get("outreach_layering_status")
            or ""
        ).strip(),
        "board_runtime_state": board_runtime_state,
        "profile_fetch_progress": {
            "total_url_count": _safe_int(profile_fetch_progress.get("total_url_count")),
            "fetched_url_count": _safe_int(profile_fetch_progress.get("fetched_url_count")),
            "queued_url_count": _safe_int(profile_fetch_progress.get("queued_url_count")),
            "failed_retryable_url_count": _safe_int(profile_fetch_progress.get("failed_retryable_url_count")),
            "deferred_url_count": _safe_int(profile_fetch_progress.get("deferred_url_count")),
        },
    }


def _board_runtime_visible_candidate_count(board_runtime_state: dict[str, Any]) -> int:
    payload = dict(board_runtime_state or {})
    visible_count = max(
        _safe_int(payload.get("row_hydration_target_count")),
        _safe_int(payload.get("served_candidate_count")),
        _safe_int(payload.get("published_candidate_count")),
    )
    if visible_count > 0:
        return visible_count
    publication_status = str(payload.get("publication_status") or "").strip().lower()
    phase = str(payload.get("phase") or "").strip().lower()
    if publication_status in {"complete", "completed", "serving"} or phase in {
        "current_snapshot_serving",
        "baseline_serving",
    }:
        return _safe_int(payload.get("expected_candidate_count"))
    return 0


def _post_result_layering_deferred_with_complete_board(board_report: dict[str, Any]) -> bool:
    """True when layering is explicitly a post-result tail, not a board-serving blocker."""

    layering_status = str(board_report.get("layering_status") or "").strip().lower()
    if layering_status not in {"deferred", "scheduled"}:
        return False
    board_runtime_state = dict(board_report.get("board_runtime_state") or {})
    phase = str(board_runtime_state.get("phase") or "").strip().lower()
    publication_status = str(board_runtime_state.get("publication_status") or "").strip().lower()
    expected_candidate_count = max(
        _safe_int(board_runtime_state.get("expected_candidate_count")),
        _safe_int(board_report.get("candidate_page_total_candidates")),
    )
    served_candidate_count = max(
        _safe_int(board_runtime_state.get("served_candidate_count")),
        _safe_int(board_runtime_state.get("published_candidate_count")),
        _safe_int(board_runtime_state.get("display_ready_candidate_count")),
        _safe_int(board_report.get("candidate_page_total_candidates")),
    )
    return (
        phase in {"post_result_layering", "current_snapshot_serving", "current_serving"}
        and publication_status == "complete"
        and expected_candidate_count > 0
        and served_candidate_count >= expected_candidate_count
        and bool(board_report.get("ready_nonempty"))
    )


def _normalize_board_runtime_parity_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dict):
        return {
            str(key): _normalize_board_runtime_parity_value(item)
            for key, item in sorted(value.items())
            if str(key).strip()
        }
    if isinstance(value, list):
        return [_normalize_board_runtime_parity_value(item) for item in value]
    return str(value or "").strip()


def _comparable_board_runtime_state(board_runtime_state: dict[str, Any]) -> dict[str, Any]:
    payload = dict(board_runtime_state or {})
    return {field: _normalize_board_runtime_parity_value(payload.get(field)) for field in _BOARD_RUNTIME_PARITY_FIELDS}


def _build_board_runtime_state_endpoint_parity_report(
    *,
    progress_payload: dict[str, Any] | None = None,
    dashboard_payload: dict[str, Any] | None = None,
    candidate_page_payload: dict[str, Any] | None = None,
    board_patches_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_states = {
        "progress": dict(dict(progress_payload or {}).get("board_runtime_state") or {}),
        "dashboard": dict(dict(dashboard_payload or {}).get("board_runtime_state") or {}),
        "candidates": dict(dict(candidate_page_payload or {}).get("board_runtime_state") or {}),
        "board_patches": dict(dict(board_patches_payload or {}).get("board_runtime_state") or {}),
    }
    present_sources = [source for source, state in raw_states.items() if state]
    missing_sources = [source for source, state in raw_states.items() if not state]
    comparable = {source: _comparable_board_runtime_state(state) for source, state in raw_states.items() if state}
    reference_source = "dashboard" if "dashboard" in comparable else (present_sources[0] if present_sources else "")
    reference = dict(comparable.get(reference_source) or {})
    mismatches: list[dict[str, Any]] = []
    if reference:
        for source, summary in comparable.items():
            for field in _BOARD_RUNTIME_PARITY_FIELDS:
                actual = summary.get(field)
                expected = reference.get(field)
                if actual == expected:
                    continue
                mismatches.append(
                    {
                        "source": source,
                        "field": field,
                        "actual": actual,
                        "expected": expected,
                    }
                )
    return {
        "report_available": bool(reference),
        "consistent": bool(reference) and not missing_sources and not mismatches,
        "reference_source": reference_source,
        "checked_endpoint_count": len(_BOARD_RUNTIME_PARITY_ENDPOINTS),
        "present_endpoint_count": len(present_sources),
        "missing_sources": missing_sources,
        "mismatch_count": len(mismatches),
        "mismatched_fields": sorted(
            {str(item.get("field") or "") for item in mismatches if str(item.get("field") or "")}
        ),
        "mismatches": mismatches[:25],
        "summaries": comparable,
    }


def _candidate_page_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("candidates", "results", "ranked_results"):
        records = [dict(item) for item in list(dict(payload or {}).get(key) or []) if isinstance(item, dict)]
        if records:
            return records
    return []


def _candidate_record_display_ready(record: dict[str, Any]) -> bool:
    payload = dict(record or {})
    if any(bool(payload.get(key)) for key in ("display_ready", "card_ready", "has_profile_detail")):
        return True
    if bool(payload.get("needs_profile_completion")):
        return False
    if payload.get("experience_lines") or payload.get("education_lines"):
        return True
    metadata = dict(payload.get("metadata") or {})
    return bool(
        payload.get("headline")
        or payload.get("summary")
        or metadata.get("headline")
        or metadata.get("summary")
        or metadata.get("profile_capture_kind")
    )


def _candidate_record_contains_recall_bucket(record: dict[str, Any], bucket: str) -> bool:
    normalized_bucket = str(bucket or "").strip().lower()
    if not normalized_bucket:
        return False
    payload = dict(record or {})
    direct_values: list[Any] = []
    for key in ("recall_buckets", "matched_keywords", "function_ids"):
        direct_values.extend(list(payload.get(key) or []))
    for source_match in list(payload.get("source_matches") or []):
        if not isinstance(source_match, dict):
            continue
        direct_values.extend(
            [
                source_match.get("keyword"),
                source_match.get("matched_on"),
                source_match.get("bucket"),
                source_match.get("label"),
            ]
        )
    return any(normalized_bucket in str(value or "").strip().lower() for value in direct_values)


def _build_candidate_page_filter_probe_sample(
    *,
    payload: dict[str, Any],
    tick: int,
    status: str,
    stage: str,
    recall_bucket: str,
) -> dict[str, Any]:
    candidates = _candidate_page_records(payload)
    display_ready_candidates = [record for record in candidates if _candidate_record_display_ready(record)]
    applied_filter = dict(payload.get("applied_filter") or {})
    applied_recall_buckets = [str(item or "").strip() for item in list(applied_filter.get("recall_buckets") or [])]
    normalized_recall_bucket = str(recall_bucket or "").strip().lower()
    endpoint_filter_confirms_bucket = any(
        str(item or "").strip().lower() in {normalized_recall_bucket, f"keyword:{normalized_recall_bucket}"}
        for item in applied_recall_buckets
    )
    display_ready_recall_candidates = [
        record
        for record in display_ready_candidates
        if endpoint_filter_confirms_bucket or _candidate_record_contains_recall_bucket(record, recall_bucket)
    ]
    return {
        "tick": tick,
        "status": str(status or ""),
        "stage": str(stage or ""),
        "recall_bucket": str(recall_bucket or ""),
        "returned_count": _safe_int(payload.get("returned_count") or len(candidates)),
        "filtered_candidate_count": _safe_int(
            payload.get("filtered_candidate_count") or payload.get("total_candidates")
        ),
        "display_ready_count": len(display_ready_candidates),
        "display_ready_recall_bucket_count": len(display_ready_recall_candidates),
        "applied_recall_buckets": applied_recall_buckets,
    }


def _build_running_candidate_filter_probe_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_samples = [dict(item) for item in list(samples or []) if isinstance(item, dict)]
    return {
        "sample_count": len(normalized_samples),
        "display_ready_recall_bucket_observed": any(
            _safe_int(item.get("display_ready_recall_bucket_count")) > 0 for item in normalized_samples
        ),
        "max_returned_count": max([_safe_int(item.get("returned_count")) for item in normalized_samples] or [0]),
        "max_display_ready_count": max(
            [_safe_int(item.get("display_ready_count")) for item in normalized_samples] or [0]
        ),
        "max_display_ready_recall_bucket_count": max(
            [_safe_int(item.get("display_ready_recall_bucket_count")) for item in normalized_samples] or [0]
        ),
        "samples": normalized_samples[:25],
    }


def _build_workflow_benchmark_report(
    *,
    search_report: dict[str, Any],
    roster_report: dict[str, Any],
    profile_completion: dict[str, Any],
    materialization_streaming: dict[str, Any],
    board_probe: dict[str, Any],
    provider_backpressure: dict[str, Any],
    workflow_wall_clock: dict[str, float],
) -> dict[str, Any]:
    profile_progress = dict(board_probe.get("profile_fetch_progress") or {})
    board_runtime_state = dict(board_probe.get("board_runtime_state") or {})
    runtime_profile_required_count = max(
        _safe_int(board_runtime_state.get("delta_profile_required_count")),
        _safe_int(board_runtime_state.get("profile_fetch_required_count")),
    )
    runtime_profile_fetched_count = max(
        _safe_int(board_runtime_state.get("delta_profile_fetched_count")),
        _safe_int(board_runtime_state.get("profile_fetched_count")),
    )
    fetched_profile_count = max(
        _safe_int(profile_completion.get("fetched_profile_count")),
        _safe_int(profile_completion.get("profile_detail_count")),
        runtime_profile_fetched_count,
        _safe_int(profile_progress.get("fetched_url_count")) if runtime_profile_fetched_count <= 0 else 0,
    )
    if runtime_profile_required_count > 0:
        profile_url_total_count = max(
            runtime_profile_required_count,
            _safe_int(materialization_streaming.get("profile_url_count")),
        )
    else:
        profile_url_total_count = max(
            _safe_int(materialization_streaming.get("profile_url_count")),
            _safe_int(profile_progress.get("total_url_count")),
        )
    return {
        "search_returned_count": max(
            _safe_int(search_report.get("search_seed_entry_count")),
            _safe_int(search_report.get("search_seed_added_entry_count")),
        ),
        "roster_returned_count": _safe_int(roster_report.get("returned_count")),
        "fetched_profile_count": fetched_profile_count,
        "profile_url_total_count": profile_url_total_count,
        "profile_url_queued_count": _safe_int(profile_progress.get("queued_url_count")),
        "profile_url_retryable_count": _safe_int(profile_progress.get("failed_retryable_url_count")),
        "board_ready": bool(board_probe.get("ready")),
        "board_ready_nonempty": bool(board_probe.get("ready_nonempty")),
        "board_total_candidates": _safe_int(board_probe.get("candidate_page_total_candidates")),
        "board_first_page_returned_count": _safe_int(board_probe.get("candidate_page_returned_count")),
        "job_to_board_nonempty_ms": _safe_float(workflow_wall_clock.get("job_to_board_nonempty")),
        "provider_backpressure_detected": bool(provider_backpressure.get("backpressure_detected")),
        "provider_limiter_exhausted": bool(provider_backpressure.get("limiter_exhausted")),
        "provider_limiter_observed_count": _safe_int(provider_backpressure.get("observed_limiter_count")),
        "provider_active_limiter_count": _safe_int(provider_backpressure.get("observed_active_limiter_count")),
        "provider_limiter_max_wait_ms": _safe_float(provider_backpressure.get("max_provider_limiter_wait_ms")),
    }


def _effective_asset_population_candidate_count(
    *,
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> int:
    runtime_visible_count = max(
        _board_runtime_visible_candidate_count(dict(results_payload.get("board_runtime_state") or {})),
        _board_runtime_visible_candidate_count(dict(dashboard_payload.get("board_runtime_state") or {})),
        _board_runtime_visible_candidate_count(dict(candidate_page_payload.get("board_runtime_state") or {})),
    )
    if runtime_visible_count > 0:
        return runtime_visible_count
    result_total = _safe_int(dict(results_payload.get("asset_population") or {}).get("candidate_count"))
    if result_total > 0:
        return result_total
    dashboard_total = _safe_int(
        dict(dashboard_payload.get("asset_population") or {}).get("candidate_count")
        or dashboard_payload.get("ranked_result_count")
    )
    if dashboard_total > 0:
        return dashboard_total
    return _safe_int(candidate_page_payload.get("total_candidates") or candidate_page_payload.get("returned_count"))


def _build_progress_observability_report(progress_samples: list[dict[str, Any]]) -> dict[str, Any]:
    maxima: dict[str, int] = {key: 0 for key in _PROGRESS_MAX_COUNTER_KEYS}
    peak_counter_values: dict[str, int] = {}
    peak_backlog_values: dict[str, int] = {}
    peak_stage1_values: dict[str, int] = {}
    peak_lifecycle_values: dict[str, int] = {}
    regressions: dict[str, dict[str, Any]] = {}
    backlog_reductions: dict[str, dict[str, Any]] = {}
    stage1_regressions: dict[str, dict[str, Any]] = {}
    lifecycle_regressions: dict[str, dict[str, Any]] = {}
    contract_violations: list[dict[str, Any]] = []
    latest_counters: dict[str, int] = {}
    latest_stage1_progress: dict[str, Any] = {}
    latest_result_view_lifecycle: dict[str, Any] = {}
    latest_execution_phase_contract: dict[str, Any] = {}
    latest_board_runtime_state: dict[str, Any] = {}
    latest_runtime_health: dict[str, Any] = {}
    payload_byte_values: list[float] = []
    synthetic_terminal_sample_count = 0
    stage1_expected_count_violations: list[dict[str, Any]] = []
    user_facing_denominator_violations: list[dict[str, Any]] = []
    first_unpromoted_expected_candidate_count: int | None = None
    previous_denominator_promoted: bool | None = None
    denominator_promotion_count = 0
    denominator_promoted_observed = False
    denominator_unpromoted_sample_count = 0
    denominator_promotion_regressions: list[dict[str, Any]] = []
    canonical_projection_public_count_normalization_count = 0

    def _canonical_projection_normalization_allows_drop(
        *,
        key: str,
        current_value: int,
        previous_peak: int,
        lifecycle: dict[str, Any],
    ) -> bool:
        if key not in {"result_count", "served_candidate_count"}:
            return False
        metadata_payload = dict(dict(lifecycle or {}).get("metadata") or {})
        normalization_payload = dict(metadata_payload.get("canonical_projection_public_count_normalization") or {})
        visible_count = _safe_int(normalization_payload.get("visible_member_count"))
        if visible_count <= 0 or current_value != visible_count or previous_peak <= current_value:
            return False
        raw_counts = {
            _safe_int(normalization_payload.get("raw_expected_candidate_count")),
            _safe_int(normalization_payload.get("raw_served_candidate_count")),
        }
        return previous_peak in raw_counts

    for sample in list(progress_samples or []):
        payload_bytes = _safe_int(sample.get("payload_bytes"))
        if payload_bytes > 0:
            payload_byte_values.append(float(payload_bytes))
        counters = {
            str(key): _safe_int(value) for key, value in dict(sample.get("counters") or {}).items() if str(key).strip()
        }
        stage1_progress = {
            str(key): value
            for key, value in dict(sample.get("linkedin_stage_1_progress") or {}).items()
            if str(key).strip()
        }
        result_view_lifecycle = {
            str(key): value
            for key, value in dict(sample.get("result_view_lifecycle") or {}).items()
            if str(key).strip()
        }
        execution_phase_contract = {
            str(key): value
            for key, value in dict(sample.get("execution_phase_contract") or {}).items()
            if str(key).strip()
        }
        board_runtime_state = {
            str(key): value for key, value in dict(sample.get("board_runtime_state") or {}).items() if str(key).strip()
        }
        runtime_health = {
            str(key): value for key, value in dict(sample.get("runtime_health") or {}).items() if str(key).strip()
        }
        if dict(dict(result_view_lifecycle or {}).get("metadata") or {}).get(
            "canonical_projection_public_count_normalization"
        ):
            canonical_projection_public_count_normalization_count += 1
        if bool(sample.get("synthetic_terminal_sample")):
            synthetic_terminal_sample_count += 1
        latest_runtime_health = runtime_health
        latest_counters = counters
        if stage1_progress:
            latest_stage1_progress = stage1_progress
        if result_view_lifecycle:
            latest_result_view_lifecycle = result_view_lifecycle
        if execution_phase_contract:
            latest_execution_phase_contract = execution_phase_contract
        if board_runtime_state:
            latest_board_runtime_state = board_runtime_state

        for key in _PROGRESS_MAX_COUNTER_KEYS:
            candidate_value = counters.get(key)
            if candidate_value is None:
                candidate_value = _safe_int(runtime_health.get(key))
            maxima[key] = max(int(maxima.get(key) or 0), _safe_int(candidate_value))

        for key in _MONOTONIC_PROGRESS_COUNTER_KEYS:
            current_value = _safe_int(counters.get(key))
            previous_peak = int(peak_counter_values.get(key) or 0)
            if current_value < previous_peak:
                if _canonical_projection_normalization_allows_drop(
                    key=key,
                    current_value=current_value,
                    previous_peak=previous_peak,
                    lifecycle=result_view_lifecycle,
                ):
                    peak_counter_values[key] = current_value
                    continue
                else:
                    drop = previous_peak - current_value
                    existing = dict(regressions.get(key) or {})
                    regressions[key] = {
                        "regression_count": int(existing.get("regression_count") or 0) + 1,
                        "largest_drop": max(int(existing.get("largest_drop") or 0), drop),
                        "peak_before_drop": max(int(existing.get("peak_before_drop") or 0), previous_peak),
                        "latest_value": current_value,
                    }
            peak_counter_values[key] = max(previous_peak, current_value)

        if stage1_progress:
            for key in _STAGE1_MONOTONIC_PROGRESS_KEYS:
                current_value = _safe_int(stage1_progress.get(key))
                previous_peak = int(peak_stage1_values.get(key) or 0)
                if current_value < previous_peak:
                    drop = previous_peak - current_value
                    existing = dict(stage1_regressions.get(key) or {})
                    stage1_regressions[key] = {
                        "regression_count": int(existing.get("regression_count") or 0) + 1,
                        "largest_drop": max(int(existing.get("largest_drop") or 0), drop),
                        "peak_before_drop": max(int(existing.get("peak_before_drop") or 0), previous_peak),
                        "latest_value": current_value,
                    }
                peak_stage1_values[key] = max(previous_peak, current_value)

        if result_view_lifecycle:
            for key in _RESULT_LIFECYCLE_MONOTONIC_PROGRESS_KEYS:
                current_value = _safe_int(result_view_lifecycle.get(key))
                previous_peak = int(peak_lifecycle_values.get(key) or 0)
                if current_value < previous_peak:
                    if _canonical_projection_normalization_allows_drop(
                        key=key,
                        current_value=current_value,
                        previous_peak=previous_peak,
                        lifecycle=result_view_lifecycle,
                    ):
                        peak_lifecycle_values[key] = current_value
                        continue
                    else:
                        drop = previous_peak - current_value
                        existing = dict(lifecycle_regressions.get(key) or {})
                        lifecycle_regressions[key] = {
                            "regression_count": int(existing.get("regression_count") or 0) + 1,
                            "largest_drop": max(int(existing.get("largest_drop") or 0), drop),
                            "peak_before_drop": max(int(existing.get("peak_before_drop") or 0), previous_peak),
                            "latest_value": current_value,
                        }
                peak_lifecycle_values[key] = max(previous_peak, current_value)

        for key in _BACKLOG_PROGRESS_COUNTER_KEYS:
            current_value = _safe_int(counters.get(key))
            previous_peak = int(peak_backlog_values.get(key) or 0)
            if current_value < previous_peak:
                reduction = previous_peak - current_value
                existing = dict(backlog_reductions.get(key) or {})
                backlog_reductions[key] = {
                    "reduction_count": int(existing.get("reduction_count") or 0) + 1,
                    "largest_reduction": max(int(existing.get("largest_reduction") or 0), reduction),
                    "peak_before_reduction": max(int(existing.get("peak_before_reduction") or 0), previous_peak),
                    "latest_value": current_value,
                }
            peak_backlog_values[key] = max(previous_peak, current_value)

        if stage1_progress:
            required_count = _safe_int(stage1_progress.get("profile_fetch_required_count"))
            fetched_count = _safe_int(stage1_progress.get("profile_fetched_count"))
            current_returned_count = _safe_int(stage1_progress.get("current_search_returned_count"))
            former_returned_count = _safe_int(stage1_progress.get("former_search_returned_count"))
            all_returned_count = _safe_int(stage1_progress.get("all_search_returned_count"))
            deduped_candidate_count = _safe_int(stage1_progress.get("deduped_candidate_count"))
            deduped_profile_url_count = _safe_int(stage1_progress.get("deduped_profile_url_count"))
            returned_population_count = current_returned_count + former_returned_count + all_returned_count
            if returned_population_count > 0:
                if deduped_candidate_count > returned_population_count:
                    contract_violations.append(
                        {
                            "kind": "stage1_deduped_exceeds_lane_returned_population",
                            "tick": _safe_int(sample.get("tick")),
                            "current_search_returned_count": current_returned_count,
                            "former_search_returned_count": former_returned_count,
                            "all_search_returned_count": all_returned_count,
                            "deduped_candidate_count": deduped_candidate_count,
                        }
                    )
                if deduped_profile_url_count > returned_population_count:
                    contract_violations.append(
                        {
                            "kind": "stage1_deduped_profile_urls_exceed_lane_returned_population",
                            "tick": _safe_int(sample.get("tick")),
                            "current_search_returned_count": current_returned_count,
                            "former_search_returned_count": former_returned_count,
                            "all_search_returned_count": all_returned_count,
                            "deduped_profile_url_count": deduped_profile_url_count,
                        }
                    )
            if required_count > max(deduped_candidate_count, deduped_profile_url_count):
                contract_violations.append(
                    {
                        "kind": "stage1_profile_required_exceeds_deduped_population",
                        "tick": _safe_int(sample.get("tick")),
                        "profile_fetch_required_count": required_count,
                        "deduped_candidate_count": deduped_candidate_count,
                        "deduped_profile_url_count": deduped_profile_url_count,
                    }
                )
            if fetched_count > required_count and required_count >= 0:
                contract_violations.append(
                    {
                        "kind": "stage1_fetched_exceeds_required",
                        "tick": _safe_int(sample.get("tick")),
                        "profile_fetched_count": fetched_count,
                        "profile_fetch_required_count": required_count,
                    }
                )
        if result_view_lifecycle:
            delta_fetched = _safe_int(result_view_lifecycle.get("delta_profile_fetched_count"))
            delta_materialized = max(
                _safe_int(result_view_lifecycle.get("delta_profile_materialized_count")),
                _safe_int(result_view_lifecycle.get("delta_profile_board_visible_count")),
            )
            if delta_materialized > delta_fetched:
                contract_violations.append(
                    {
                        "kind": "lifecycle_materialized_exceeds_fetched",
                        "tick": _safe_int(sample.get("tick")),
                        "delta_profile_materialized_count": delta_materialized,
                        "delta_profile_fetched_count": delta_fetched,
                    }
                )
            baseline_snapshot_id = str(result_view_lifecycle.get("baseline_snapshot_id") or "").strip()
            current_snapshot_id = str(result_view_lifecycle.get("current_snapshot_id") or "").strip()
            served_snapshot_id = str(result_view_lifecycle.get("served_snapshot_id") or "").strip()
            baseline_candidate_count = _safe_int(result_view_lifecycle.get("baseline_candidate_count"))
            served_candidate_count = _safe_int(result_view_lifecycle.get("served_candidate_count"))
            if (
                baseline_snapshot_id
                and current_snapshot_id
                and served_snapshot_id == current_snapshot_id
                and baseline_snapshot_id != current_snapshot_id
                and served_candidate_count < baseline_candidate_count
            ):
                contract_violations.append(
                    {
                        "kind": "raw_delta_only_result_view_served",
                        "tick": _safe_int(sample.get("tick")),
                        "baseline_snapshot_id": baseline_snapshot_id,
                        "current_snapshot_id": current_snapshot_id,
                        "served_snapshot_id": served_snapshot_id,
                        "baseline_candidate_count": baseline_candidate_count,
                        "served_candidate_count": served_candidate_count,
                    }
                )
        if execution_phase_contract:
            phase_text = " ".join(
                str(execution_phase_contract.get(key) or "")
                for key in ("active_phase_label", "active_phase_detail", "active_stage_label", "active_stage_detail")
            )
            if (
                phase_text
                and "public web" in phase_text.lower()
                and not bool(execution_phase_contract.get("public_web_stage_applicable"))
            ):
                contract_violations.append(
                    {
                        "kind": "public_web_label_when_stage_not_applicable",
                        "tick": _safe_int(sample.get("tick")),
                        "active_phase_label": str(execution_phase_contract.get("active_phase_label") or ""),
                        "active_stage_label": str(execution_phase_contract.get("active_stage_label") or ""),
                    }
                )

        if board_runtime_state:
            denominator_promoted = bool(board_runtime_state.get("delta_profile_denominator_promoted"))
            expected_candidate_count = _safe_int(board_runtime_state.get("expected_candidate_count"))
            display_ready_count = _safe_int(board_runtime_state.get("display_ready_candidate_count"))
            delta_required_count = _safe_int(board_runtime_state.get("delta_profile_required_count"))
            profile_required_count = _safe_int(board_runtime_state.get("profile_fetch_required_count"))
            card_status_fraction = _first_count_fraction(board_runtime_state.get("card_materialization_status_text"))
            if card_status_fraction:
                text_ready_count, text_required_count = card_status_fraction
                allowed_required_counts = {
                    expected_candidate_count,
                    *(
                        count
                        for count in (delta_required_count, profile_required_count)
                        if 0 < count < expected_candidate_count
                    ),
                }
                allowed_required_counts = {count for count in allowed_required_counts if count > 0}
                card_ready_count = display_ready_count
                card_required_count = (
                    text_required_count if text_required_count in allowed_required_counts else expected_candidate_count
                )
                if (
                    text_ready_count > card_ready_count
                    or expected_candidate_count <= 0
                    or text_required_count not in allowed_required_counts
                    or text_ready_count > text_required_count
                    or text_required_count > expected_candidate_count
                ):
                    contract_violations.append(
                        {
                            "kind": "board_runtime_card_text_exceeds_card_readiness",
                            "tick": _safe_int(sample.get("tick")),
                            "card_materialization_status_text": str(
                                board_runtime_state.get("card_materialization_status_text") or ""
                            ),
                            "text_ready_count": text_ready_count,
                            "text_required_count": text_required_count,
                            "card_ready_count": card_ready_count,
                            "card_required_count": card_required_count,
                            "display_ready_candidate_count": display_ready_count,
                        }
                    )
            if denominator_promoted:
                denominator_promoted_observed = True
            else:
                denominator_unpromoted_sample_count += 1
            if previous_denominator_promoted is False and denominator_promoted:
                denominator_promotion_count += 1
            if previous_denominator_promoted is True and not denominator_promoted:
                denominator_promotion_regressions.append(
                    {
                        "tick": _safe_int(sample.get("tick")),
                        "expected_candidate_count": expected_candidate_count,
                    }
                )
            previous_denominator_promoted = denominator_promoted
            if not denominator_promoted and expected_candidate_count > 0:
                if first_unpromoted_expected_candidate_count is None:
                    first_unpromoted_expected_candidate_count = expected_candidate_count
                elif expected_candidate_count != first_unpromoted_expected_candidate_count:
                    stage1_expected_count_violations.append(
                        {
                            "tick": _safe_int(sample.get("tick")),
                            "first_expected_candidate_count": first_unpromoted_expected_candidate_count,
                            "expected_candidate_count": expected_candidate_count,
                        }
                    )
                for field_name in ("card_materialization_status_text", "profile_fetch_status_text"):
                    text = str(board_runtime_state.get(field_name) or "")
                    if _text_contains_count_denominator(text):
                        user_facing_denominator_violations.append(
                            {
                                "tick": _safe_int(sample.get("tick")),
                                "field": field_name,
                                "text": text,
                            }
                        )

    filtered_maxima = {key: value for key, value in maxima.items() if int(value or 0) > 0}
    contract_violation_counts = Counter(str(item.get("kind") or "") for item in contract_violations)
    return {
        "sample_count": len(list(progress_samples or [])),
        "maxima": filtered_maxima,
        "counter_regressions": regressions,
        "regression_detected": bool(regressions),
        "stage1_counter_regressions": stage1_regressions,
        "stage1_regression_detected": bool(stage1_regressions),
        "result_view_lifecycle_regressions": lifecycle_regressions,
        "result_view_lifecycle_regression_detected": bool(lifecycle_regressions),
        "contract_violations": contract_violations[:25],
        "contract_violation_counts": {
            key: int(value) for key, value in sorted(contract_violation_counts.items()) if key
        },
        "contract_violation_detected": bool(contract_violations),
        "stage1_expected_candidate_count_violations": stage1_expected_count_violations[:25],
        "stage1_expected_candidate_count_violation_detected": bool(stage1_expected_count_violations),
        "stage1_denominator_promotion_count": denominator_promotion_count,
        "stage1_denominator_promoted_observed": denominator_promoted_observed,
        "stage1_denominator_unpromoted_sample_count": denominator_unpromoted_sample_count,
        "stage1_denominator_promotion_regressions": denominator_promotion_regressions[:25],
        "stage1_denominator_promotion_regression_detected": bool(denominator_promotion_regressions),
        "stage1_user_facing_denominator_violations": user_facing_denominator_violations[:25],
        "stage1_user_facing_denominator_violation_detected": bool(user_facing_denominator_violations),
        "canonical_projection_public_count_normalization_count": canonical_projection_public_count_normalization_count,
        "backlog_reductions": backlog_reductions,
        "backlog_reduction_detected": bool(backlog_reductions),
        "synthetic_terminal_sample_count": synthetic_terminal_sample_count,
        "terminal_progress_lag_detected": synthetic_terminal_sample_count > 0,
        "payload_bytes": _numeric_summary(payload_byte_values),
        "max_payload_bytes": int(max(payload_byte_values)) if payload_byte_values else 0,
        "latest_counters": {key: value for key, value in latest_counters.items() if int(value or 0) > 0},
        "latest_linkedin_stage_1_progress": latest_stage1_progress,
        "latest_result_view_lifecycle": latest_result_view_lifecycle,
        "latest_execution_phase_contract": latest_execution_phase_contract,
        "latest_board_runtime_state": latest_board_runtime_state,
        "latest_runtime_health": latest_runtime_health,
    }


def _build_synthetic_terminal_progress_sample(
    *,
    progress_samples: list[dict[str, Any]],
    job_payload: dict[str, Any],
    raw_job_status: str,
    raw_job_stage: str,
) -> dict[str, Any]:
    previous_counters = {
        str(key): _safe_int(value)
        for key, value in dict((progress_samples[-1] or {}).get("counters") or {}).items()
        if str(key).strip()
    }
    job_counters = {
        str(key): _safe_int(value)
        for key, value in dict(dict(job_payload.get("progress") or {}).get("counters") or {}).items()
        if str(key).strip()
    }
    merged_counters: dict[str, int] = {}
    monotonic_terminal_keys = set(_MONOTONIC_PROGRESS_COUNTER_KEYS) | {"event_count"}
    carry_forward_when_missing_keys = set(_BACKLOG_PROGRESS_COUNTER_KEYS)
    for key in set(previous_counters) | set(job_counters):
        if key in monotonic_terminal_keys:
            merged_counters[key] = max(_safe_int(previous_counters.get(key)), _safe_int(job_counters.get(key)))
            continue
        if key in carry_forward_when_missing_keys and key not in job_counters:
            merged_counters[key] = _safe_int(previous_counters.get(key))
            continue
        if key in job_counters:
            merged_counters[key] = _safe_int(job_counters.get(key))
            continue
        merged_counters[key] = 0
    return {
        "tick": len(progress_samples),
        "status": raw_job_status,
        "stage": raw_job_stage or raw_job_status,
        "counters": {key: value for key, value in merged_counters.items() if int(value or 0) > 0},
        "runtime_health": {
            "state": "terminal",
            "classification": str(raw_job_status or "").strip().lower(),
            "detail": f"Job {str(raw_job_status or '').strip().lower()}.",
        },
        "synthetic_terminal_sample": True,
    }


def _progress_snapshot_to_observability_sample(snapshot: dict[str, Any], *, tick: int) -> dict[str, Any]:
    snapshot_progress = dict(snapshot.get("progress") or {})
    return {
        "tick": tick,
        "status": str(snapshot.get("status") or ""),
        "stage": str(snapshot.get("stage") or ""),
        "payload_bytes": len(json.dumps(snapshot, ensure_ascii=False)),
        "counters": dict(snapshot_progress.get("counters") or {}),
        "runtime_health": dict(snapshot_progress.get("runtime_health") or {}),
        "linkedin_stage_1_progress": dict(snapshot.get("linkedin_stage_1_progress") or {}),
        "result_view_lifecycle": dict(snapshot.get("result_view_lifecycle") or {}),
        "board_runtime_state": dict(snapshot.get("board_runtime_state") or {}),
        "execution_phase_contract": dict(snapshot.get("execution_phase_contract") or {}),
    }


def _build_case_level_smoke_exports(
    *,
    results_payload: dict[str, Any],
    provider_case_report: dict[str, Any],
) -> dict[str, Any]:
    exports = {
        "stage_summary_digest": stage_summary_digest(results_payload),
        "stage_wall_clock_ms": dict(provider_case_report.get("stage_wall_clock_ms") or {}),
        "workflow_wall_clock_ms": dict(provider_case_report.get("workflow_wall_clock_ms") or {}),
        "behavior_guardrails": dict(provider_case_report.get("behavior_guardrails") or {}),
    }
    service_metrics = dict(provider_case_report.get("service_metrics") or {})
    event_level_efficiency = dict(provider_case_report.get("event_level_efficiency") or {})
    exported_event_level_efficiency = event_level_efficiency_runtime_subset(event_level_efficiency)
    profile_scheduler_contract = dict(event_level_efficiency.get("profile_scheduler_contract") or {})
    if exported_event_level_efficiency:
        exports["event_level_efficiency"] = exported_event_level_efficiency
    remote_provider_events = dict(service_metrics.get("remote_provider_events") or {})
    serving_publication_gap = dict(service_metrics.get("serving_publication_gap") or {})
    post_profile_completion = dict(service_metrics.get("post_profile_completion") or {})
    recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
    board_overlay_writes = dict(service_metrics.get("board_overlay_writes") or {})
    finalization_overlay = dict(service_metrics.get("finalization_overlay") or {})
    provider_anomalies = dict(service_metrics.get("provider_anomalies") or {})
    company_public_web = dict(service_metrics.get("company_public_web") or {})
    legacy_materialization_write_contract = dict(service_metrics.get("legacy_materialization_write_contract") or {})
    workflow_causality_contract = dict(service_metrics.get("workflow_causality_contract") or {})
    exported_service_metrics = {}
    if remote_provider_events:
        exported_service_metrics["remote_provider_events"] = remote_provider_events
    if serving_publication_gap:
        exported_service_metrics["serving_publication_gap"] = serving_publication_gap
    if post_profile_completion:
        exported_service_metrics["post_profile_completion"] = post_profile_completion
    if recovery_phase_metrics:
        exported_service_metrics["recovery_phase_metrics"] = recovery_phase_metrics
    if board_overlay_writes:
        exported_service_metrics["board_overlay_writes"] = board_overlay_writes
    if finalization_overlay:
        exported_service_metrics["finalization_overlay"] = finalization_overlay
    if provider_anomalies:
        exported_service_metrics["provider_anomalies"] = provider_anomalies
    if company_public_web:
        exported_service_metrics["company_public_web"] = company_public_web
    if legacy_materialization_write_contract:
        exported_service_metrics["legacy_materialization_write_contract"] = legacy_materialization_write_contract
    if workflow_causality_contract:
        exported_service_metrics["workflow_causality_contract"] = workflow_causality_contract
    if profile_scheduler_contract:
        exported_service_metrics["profile_scheduler_contract"] = profile_scheduler_contract
    if exported_service_metrics:
        exports["service_metrics"] = exported_service_metrics
    board_runtime_state_parity = dict(provider_case_report.get("board_runtime_state_parity") or {})
    if board_runtime_state_parity:
        exports["board_runtime_state_parity"] = board_runtime_state_parity
    legacy_artifact_coherence = dict(provider_case_report.get("legacy_artifact_coherence") or {})
    if legacy_artifact_coherence:
        exports["legacy_artifact_coherence"] = legacy_artifact_coherence
    return exports


def _expected_board_candidate_count(results_payload: dict[str, Any]) -> int:
    asset_population = dict(results_payload.get("asset_population") or {})
    if bool(asset_population.get("available")):
        candidate_count = _safe_int(asset_population.get("candidate_count"))
        if candidate_count > 0:
            return candidate_count
    stage_summaries = dict(dict(results_payload.get("workflow_stage_summaries") or {}).get("summaries") or {})
    stage_2_final = dict(stage_summaries.get("stage_2_final") or {})
    candidate_source = dict(stage_2_final.get("candidate_source") or {})
    return _safe_int(candidate_source.get("candidate_count") or stage_2_final.get("candidate_count"))


def _results_payload_has_stage_wall_clock_anchors(results_payload: dict[str, Any]) -> bool:
    summaries = dict(dict(results_payload.get("workflow_stage_summaries") or {}).get("summaries") or {})
    if not summaries:
        return False
    required_stage_names = ("stage_1_preview", "stage_2_final")
    for stage_name in required_stage_names:
        stage_payload = dict(summaries.get(stage_name) or {})
        if not str(stage_payload.get("started_at") or "").strip():
            return False
        if not str(stage_payload.get("completed_at") or stage_payload.get("saved_at") or "").strip():
            return False
    return True


def _ensure_results_runtime_details_for_smoke_report(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    results_payload: dict[str, Any],
    timings_ms: dict[str, float],
) -> dict[str, Any]:
    if not str(job_id or "").strip() or _results_payload_has_stage_wall_clock_anchors(results_payload):
        timings_ms.setdefault("fetch_runtime_results_details", 0.0)
        return results_payload
    details_started_at = time.perf_counter()
    try:
        refreshed_payload = _fetch_smoke_results_payload(
            client,
            job_id=job_id,
            job_payload=dict(results_payload.get("job") or {}),
            include_runtime_details=True,
            include_candidates=False,
        )
    except Exception:
        timings_ms["fetch_runtime_results_details"] = round((time.perf_counter() - details_started_at) * 1000, 2)
        return results_payload
    timings_ms["fetch_runtime_results_details"] = round((time.perf_counter() - details_started_at) * 1000, 2)
    if refreshed_payload and _results_payload_has_stage_wall_clock_anchors(dict(refreshed_payload or {})):
        return dict(refreshed_payload)
    return results_payload


def _settle_board_probe(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    results_payload: dict[str, Any],
    poll_seconds: float,
    wait_for_layering_visible: bool = False,
    layering_visible_timeout_seconds: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
    expected_candidate_count = _expected_board_candidate_count(results_payload)
    default_timeout_seconds = 5.0 if wait_for_layering_visible else 2.0
    if wait_for_layering_visible and layering_visible_timeout_seconds is not None:
        default_timeout_seconds = max(0.0, float(layering_visible_timeout_seconds))
    board_probe_timeout_seconds = max(default_timeout_seconds, poll_seconds * 20)
    timings = {
        "dashboard_fetch_ms": 0.0,
        "candidate_page_fetch_ms": 0.0,
        "wait_ms": 0.0,
        "board_ready_wait_ms": 0.0,
        "board_nonempty_wait_ms": 0.0,
        "board_expected_total_wait_ms": 0.0,
        "attempt_count": 0.0,
        "layering_ready": 0.0,
        "layering_visible_wait_ms": 0.0,
        "layering_visible_timed_out": 0.0,
        "layering_visible_timeout_ms": board_probe_timeout_seconds * 1000.0,
    }
    dashboard_payload: dict[str, Any] = {}
    candidate_page_payload: dict[str, Any] = {}
    board_ready_wait_ms: float | None = None
    board_nonempty_wait_ms: float | None = None
    board_expected_total_wait_ms: float | None = None
    layering_visible_wait_ms: float | None = None
    started_at = time.perf_counter()
    deadline = time.monotonic() + board_probe_timeout_seconds
    while True:
        timings["attempt_count"] += 1.0
        fetch_started_at = time.perf_counter()
        dashboard_payload = _fetch_smoke_dashboard_payload(client, job_id=job_id)
        timings["dashboard_fetch_ms"] += (time.perf_counter() - fetch_started_at) * 1000
        fetch_started_at = time.perf_counter()
        candidate_page_payload = _fetch_smoke_candidate_page_payload(
            client,
            job_id=job_id,
            path=f"/api/jobs/{job_id}/candidates?offset=0&limit=24&lightweight=1",
        )
        timings["candidate_page_fetch_ms"] += (time.perf_counter() - fetch_started_at) * 1000
        probe = _build_board_probe_report(dashboard_payload, candidate_page_payload)
        total_candidates = _safe_int(probe.get("candidate_page_total_candidates"))
        returned_count = _safe_int(probe.get("candidate_page_returned_count"))
        expected_total_ready = expected_candidate_count > 0 and total_candidates >= expected_candidate_count
        fallback_ready = expected_candidate_count <= 0 and (probe.get("ready_nonempty") or returned_count > 0)
        layering_status = str(probe.get("layering_status") or "").strip().lower()
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        if board_ready_wait_ms is None and bool(probe.get("ready")):
            board_ready_wait_ms = elapsed_ms
        if board_nonempty_wait_ms is None and bool(probe.get("ready_nonempty")):
            board_nonempty_wait_ms = elapsed_ms
        if board_expected_total_wait_ms is None and (expected_total_ready or fallback_ready):
            board_expected_total_wait_ms = elapsed_ms
        if layering_visible_wait_ms is None and layering_status in {"completed", "ready", "available"}:
            layering_visible_wait_ms = elapsed_ms
        layering_ready = (not wait_for_layering_visible) or layering_visible_wait_ms is not None
        timings["layering_ready"] = 1.0 if layering_ready else 0.0
        if (expected_total_ready or fallback_ready) and layering_ready:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.05, min(0.25, poll_seconds)))
    timings["wait_ms"] = (time.perf_counter() - started_at) * 1000
    timings["board_ready_wait_ms"] = float(board_ready_wait_ms or 0.0)
    timings["board_nonempty_wait_ms"] = float(board_nonempty_wait_ms or 0.0)
    timings["board_expected_total_wait_ms"] = float(board_expected_total_wait_ms or 0.0)
    timings["layering_visible_wait_ms"] = float(layering_visible_wait_ms or 0.0)
    timings["layering_visible_timed_out"] = (
        1.0 if wait_for_layering_visible and layering_visible_wait_ms is None else 0.0
    )
    return dashboard_payload, candidate_page_payload, timings


def _build_provider_case_report(
    *,
    explain_payload: dict[str, Any],
    job_payload: dict[str, Any] | None = None,
    job_summary: dict[str, Any],
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
    timings_ms: dict[str, float],
    progress_payload: dict[str, Any] | None = None,
    board_patches_payload: dict[str, Any] | None = None,
    progress_observability: dict[str, Any] | None = None,
    timeline: list[dict[str, Any]] | None = None,
    provider_invocations: list[dict[str, Any]] | None = None,
    job_events: list[dict[str, Any]] | None = None,
    agent_workers: list[dict[str, Any]] | None = None,
    agent_trace_spans: list[dict[str, Any]] | None = None,
    board_visible_patches: list[dict[str, Any]] | None = None,
    materialization_items: list[dict[str, Any]] | None = None,
    workflow_commands: list[dict[str, Any]] | None = None,
    target_candidate_public_web_batches: list[dict[str, Any]] | None = None,
    legacy_public_web_retirement_audit: dict[str, Any] | None = None,
    company_public_web_runs: list[dict[str, Any]] | None = None,
    worker_recovery_runs: list[dict[str, Any]] | None = None,
    running_candidate_filter_probe_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    latest_metrics = dict(job_summary.get("latest_metrics") or {})
    result_job_summary = dict(dict(results_payload.get("job") or {}).get("summary") or {})
    candidate_source = dict(
        result_job_summary.get("candidate_source")
        or latest_metrics.get("candidate_source")
        or dict(dict(results_payload.get("asset_population") or {}).get("candidate_source") or {})
    )
    background_reconcile = dict(
        job_summary.get("background_reconcile") or latest_metrics.get("background_reconcile") or {}
    )
    stage_wall_clock = _build_stage_wall_clock_report(results_payload)
    stage1_preview = dict(stage_wall_clock.get("stage_1_preview") or {})
    stage2_final = dict(stage_wall_clock.get("stage_2_final") or {})
    board_probe = _build_board_probe_report(dashboard_payload, candidate_page_payload)
    result_view_lifecycle = _result_view_lifecycle_from_payloads(
        results_payload=results_payload,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
    )
    serving_publication_gap = _serving_publication_gap_from_payloads(
        results_payload=results_payload,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
    )
    materialization_streaming = _build_materialization_streaming_report(
        job_summary,
        job_events=list(job_events or []),
    )
    runtime_tuning_context = _runtime_tuning_context_for_smoke_report(
        explain_payload=explain_payload,
        latest_metrics=latest_metrics,
        materialization_streaming=materialization_streaming,
    )
    event_level_efficiency = extract_event_level_efficiency_metrics(
        job_summary=job_summary,
        events=list(job_events or []),
        workers=list(agent_workers or []),
    )
    post_preview_finalization = _build_post_preview_finalization_report(
        stage_wall_clock=stage_wall_clock,
        job_events=list(job_events or []),
        timings_ms=timings_ms,
        materialization_items=list(materialization_items or []),
        workflow_commands=list(workflow_commands or []),
        agent_workers=list(agent_workers or []),
    )
    board_visible_patch_records = list(board_visible_patches or []) or _board_visible_patches_from_events(
        list(job_events or [])
    )
    workflow_wall_clock = _build_workflow_wall_clock_report(
        stage_wall_clock=stage_wall_clock,
        board_probe=board_probe,
        timings_ms=timings_ms,
        timeline=timeline,
        job_events=list(job_events or []),
        board_visible_patches=board_visible_patch_records,
        post_preview_finalization=post_preview_finalization,
    )
    behavior_guardrails = _build_behavior_guardrails_report(
        explain_payload=explain_payload,
        results_payload=results_payload,
        stage_wall_clock=stage_wall_clock,
        board_probe=board_probe,
        workflow_wall_clock=workflow_wall_clock,
        provider_invocations=provider_invocations,
        materialization_streaming=materialization_streaming,
        event_level_efficiency=event_level_efficiency,
    )
    search_report = _build_search_report(latest_metrics=latest_metrics, background_reconcile=background_reconcile)
    roster_report = _build_roster_report(
        latest_metrics=latest_metrics,
        background_reconcile=background_reconcile,
        candidate_source=candidate_source,
    )
    profile_completion_report = _build_profile_completion_report(background_reconcile)
    provider_backpressure = _build_provider_backpressure_report(
        explain_payload=explain_payload,
        job_summary=job_summary,
        latest_metrics=latest_metrics,
        materialization_streaming=materialization_streaming,
        progress_observability=progress_observability,
    )
    remote_actor_slot_observation = _remote_actor_slot_observation_report(
        progress_observability=dict(progress_observability or {}),
        runtime_tuning_context=runtime_tuning_context,
    )
    board_runtime_state_parity = _build_board_runtime_state_endpoint_parity_report(
        progress_payload=dict(progress_payload or {}),
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
        board_patches_payload=dict(board_patches_payload or {}),
    )
    workflow_benchmark = _build_workflow_benchmark_report(
        search_report=search_report,
        roster_report=roster_report,
        profile_completion=profile_completion_report,
        materialization_streaming=materialization_streaming,
        board_probe=board_probe,
        provider_backpressure=provider_backpressure,
        workflow_wall_clock=workflow_wall_clock,
    )
    service_metrics = build_workflow_service_metrics(
        workers=list(agent_workers or []),
        trace_spans=list(agent_trace_spans or []),
        workflow_wall_clock_ms=workflow_wall_clock,
        stage_wall_clock_ms={
            stage_name: float(dict(stage_payload).get("wall_clock_ms") or 0.0)
            for stage_name, stage_payload in stage_wall_clock.items()
            if float(dict(stage_payload).get("wall_clock_ms") or 0.0) > 0.0
        },
        timings_ms=timings_ms,
        timeline=list(timeline or []),
        job_events=list(job_events or []),
        final_summary=result_job_summary,
        result_view_lifecycle=result_view_lifecycle,
        serving_publication_gap=serving_publication_gap,
        board_runtime_state=dict(board_probe.get("board_runtime_state") or {}),
        board_visible_patches=board_visible_patch_records,
        materialization_items=list(materialization_items or []),
        workflow_commands=list(workflow_commands or []),
        provider_anomaly_query_summaries=_provider_anomaly_query_summaries_from_search_seed_artifacts(
            job_summary=job_summary,
            result_job_summary=result_job_summary,
            candidate_source=candidate_source,
        ),
        target_candidate_public_web_batches=list(target_candidate_public_web_batches or []),
        legacy_public_web_retirement_audit=dict(legacy_public_web_retirement_audit or {}),
        company_public_web_runs=list(company_public_web_runs or []),
        worker_recovery_runs=list(worker_recovery_runs or []),
        post_preview_finalization=post_preview_finalization,
    )
    return {
        "execution": {
            "target_company": str(explain_payload.get("target_company") or ""),
            "effective_acquisition_mode": str(explain_payload.get("effective_acquisition_mode") or ""),
            "analysis_stage_mode": str(explain_payload.get("analysis_stage_mode") or "single_stage"),
            "default_results_mode": str(
                (results_payload.get("effective_execution_semantics") or {}).get("default_results_mode")
                or explain_payload.get("default_results_mode")
                or ""
            ),
            "dispatch_strategy": str(explain_payload.get("dispatch_strategy") or ""),
            "planner_mode": str(explain_payload.get("planner_mode") or ""),
            "runtime_tuning_profile": str(explain_payload.get("runtime_tuning_profile") or ""),
        },
        "candidate_source": {
            "source_kind": str(candidate_source.get("source_kind") or ""),
            "snapshot_id": str(candidate_source.get("snapshot_id") or ""),
            "asset_view": str(candidate_source.get("asset_view") or ""),
            "candidate_count": _safe_int(
                candidate_source.get("candidate_count")
                or stage2_final.get("candidate_count")
                or stage1_preview.get("candidate_count")
            ),
        },
        "search": search_report,
        "roster": roster_report,
        "profile_completion": profile_completion_report,
        "provider_backpressure": provider_backpressure,
        "remote_actor_slot_observation": remote_actor_slot_observation,
        "materialization_streaming": materialization_streaming,
        "post_preview_finalization": post_preview_finalization,
        "event_level_efficiency": event_level_efficiency,
        "service_metrics": service_metrics,
        "workflow_benchmark": workflow_benchmark,
        "board": board_probe,
        "board_runtime_state_parity": board_runtime_state_parity,
        "legacy_artifact_coherence": _build_legacy_artifact_coherence_report(
            job_payload=dict(job_payload or dict(results_payload.get("job") or {})),
            results_payload=results_payload,
            progress_payload=dict(progress_payload or {}),
        ),
        "projection_cutover": _build_projection_cutover_report(
            results_payload=results_payload,
            dashboard_payload=dashboard_payload,
            candidate_page_payload=candidate_page_payload,
        ),
        "running_candidate_filter_probe": _build_running_candidate_filter_probe_report(
            list(running_candidate_filter_probe_samples or [])
        ),
        "progress_observability": dict(progress_observability or {}),
        "behavior_guardrails": behavior_guardrails,
        "stage_wall_clock_ms": {
            stage_name: float(dict(stage_payload).get("wall_clock_ms") or 0.0)
            for stage_name, stage_payload in stage_wall_clock.items()
            if float(dict(stage_payload).get("wall_clock_ms") or 0.0) > 0.0
        },
        "workflow_wall_clock_ms": workflow_wall_clock,
        "counts": {
            "stage_1_preview_candidate_count": _safe_int(stage1_preview.get("candidate_count")),
            "stage_1_preview_manual_review_count": _safe_int(stage1_preview.get("manual_review_count")),
            "final_candidate_count": _safe_int(
                stage2_final.get("candidate_count")
                or (results_payload.get("asset_population") or {}).get("candidate_count")
            ),
            "final_manual_review_count": _safe_int(stage2_final.get("manual_review_count")),
            "board_total_candidates": _safe_int(board_probe.get("candidate_page_total_candidates")),
            "board_first_page_returned_count": _safe_int(board_probe.get("candidate_page_returned_count")),
        },
        "timings_ms": {
            "dashboard_fetch": round(float(timings_ms.get("dashboard_fetch") or 0.0), 2),
            "candidate_page_fetch": round(float(timings_ms.get("candidate_page_fetch") or 0.0), 2),
            "total": round(float(timings_ms.get("total") or 0.0), 2),
        },
    }


def _evaluate_smoke_record_completion(
    job_payload: dict[str, Any],
    results_payload: dict[str, Any],
) -> tuple[bool, str]:
    results_job = dict(results_payload.get("job") or {})
    final_job = results_job or dict(job_payload or {})
    final_status = str(final_job.get("status") or "").strip().lower()
    if final_status == "completed":
        return True, "completed_job"
    if final_status == "failed":
        return False, "failed_job"
    stage_summaries = stage_summary_digest(results_payload)
    stage_statuses = [
        str(dict(value).get("status") or "").strip().lower()
        for value in stage_summaries.values()
        if isinstance(value, dict)
    ]
    all_reported_stages_completed = bool(stage_statuses) and all(status == "completed" for status in stage_statuses)
    asset_population = dict(results_payload.get("asset_population") or {})
    has_usable_output = bool(results_payload.get("results") or []) or bool(
        results_payload.get("manual_review_items") or []
    )
    if not has_usable_output:
        has_usable_output = (
            bool(asset_population.get("available")) and int(asset_population.get("candidate_count") or 0) > 0
        )
    if has_usable_output and all_reported_stages_completed:
        return True, "results_ready_nonterminal"
    return False, "incomplete"


def _case_allows_early_results_ready(expectations: dict[str, Any] | None) -> bool:
    payload = dict(expectations or {})
    if bool(payload.get("require_terminal_job")):
        return False
    if "allow_results_ready_nonterminal" in payload:
        return bool(payload.get("allow_results_ready_nonterminal"))
    long_tail_keys = {
        "min_profile_url_total_count",
        "min_fetched_profile_count",
        "min_board_total_candidates",
        "min_agent_worker_count",
        "min_provider_invocation_count",
        "min_provider_invocations_by_logical_name",
    }
    return not any(key in payload for key in long_tail_keys)


def _expectation_minimum_failure(
    *,
    name: str,
    actual: int | float,
    expected: int | float,
) -> str:
    return f"{name}: expected >= {expected}, actual={actual}"


def _expectation_maximum_failure(
    *,
    name: str,
    actual: int | float,
    expected: int | float,
) -> str:
    return f"{name}: expected <= {expected}, actual={actual}"


def _expectation_equal_failure(
    *,
    name: str,
    actual: Any,
    expected: Any,
) -> str:
    return f"{name}: expected {expected!r}, actual={actual!r}"


def _canonical_projection_completion_proof(
    *,
    provider_report: dict[str, Any],
    service_metrics: dict[str, Any],
) -> dict[str, Any]:
    projection_cutover = dict(provider_report.get("projection_cutover") or {})
    board_projection = dict(service_metrics.get("board_visible_projection") or {})
    post_profile = dict(service_metrics.get("post_profile_completion") or {})
    finalization_overlay = dict(service_metrics.get("finalization_overlay") or {})
    legacy_write_contract = dict(service_metrics.get("legacy_materialization_write_contract") or {})
    recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})

    expected_count = max(
        _safe_int(board_projection.get("expected_candidate_count")),
        _safe_int(board_projection.get("served_candidate_count")),
        _safe_int(finalization_overlay.get("expected_candidate_count")),
        _safe_int(finalization_overlay.get("served_candidate_count")),
    )
    served_count = max(
        _safe_int(board_projection.get("served_candidate_count")),
        _safe_int(finalization_overlay.get("served_candidate_count")),
    )
    delta_required = max(
        _safe_int(board_projection.get("delta_profile_required_count")),
        _safe_int(finalization_overlay.get("delta_profile_required_count")),
    )
    delta_board_visible = max(
        _safe_int(board_projection.get("delta_profile_board_visible_count")),
        _safe_int(finalization_overlay.get("delta_profile_board_visible_count")),
    )
    delta_materialized = max(
        _safe_int(board_projection.get("delta_profile_materialized_count")),
        _safe_int(finalization_overlay.get("delta_profile_materialized_count")),
    )
    canonical_reader_clean = bool(
        projection_cutover.get("report_available")
        and projection_cutover.get("run_projection_link_present", True)
        and not projection_cutover.get("projection_missing")
        and not projection_cutover.get("legacy_public_reader_fallback_used")
        and not projection_cutover.get("legacy_endpoint_normal_path_used")
    )
    board_projection_clean = bool(
        board_projection.get("report_available")
        and not board_projection.get("projection_missing_for_visible_count")
        and not board_projection.get("patch_log_replay_lag")
        and not board_projection.get("patch_log_required_missing")
        and not board_projection.get("materialization_lag_violation")
        and (
            not board_projection.get("patch_sequence_values") or board_projection.get("patch_sequence_contiguous", True)
        )
        and served_count > 0
        and (expected_count <= 0 or served_count >= expected_count)
        and (delta_required <= 0 or min(delta_board_visible, delta_materialized) >= delta_required)
    )
    finalization_clean = bool(
        not finalization_overlay
        or (
            finalization_overlay.get("report_available")
            and not finalization_overlay.get("eligible_full_rewrite_present")
            and (
                not finalization_overlay.get("reuse_eligible")
                or finalization_overlay.get("reuse_used")
                or finalization_overlay.get("served_complete")
            )
        )
    )
    post_profile_clean = bool(
        post_profile.get("report_available")
        and not post_profile.get("slo_violation_detected")
        and _safe_int(
            dict(post_profile.get("url_terminal_state_recording") or {}).get("terminal_queue_state_leak_count")
        )
        == 0
        and bool(dict(post_profile.get("all_profiles_fetched_to_all_cards_visible") or {}).get("elapsed_ms"))
    )
    legacy_clean = bool(
        legacy_write_contract.get("report_available")
        and _safe_int(legacy_write_contract.get("normal_path_write_count")) == 0
        and recovery_phase_metrics.get("report_available")
        and _safe_int(recovery_phase_metrics.get("legacy_bridge_used_count")) == 0
        and not recovery_phase_metrics.get("legacy_bridge_used_present")
    )
    serving_complete = bool(canonical_reader_clean and board_projection_clean and post_profile_clean and legacy_clean)
    return {
        "complete": serving_complete,
        "serving_complete": serving_complete,
        "canonical_reader_clean": canonical_reader_clean,
        "board_projection_clean": board_projection_clean,
        "finalization_clean": finalization_clean,
        "post_profile_clean": post_profile_clean,
        "legacy_clean": legacy_clean,
        "expected_candidate_count": expected_count,
        "served_candidate_count": served_count,
        "delta_profile_required_count": delta_required,
        "delta_profile_board_visible_count": delta_board_visible,
        "delta_profile_materialized_count": delta_materialized,
    }


def _append_service_recovery_violation_failures(
    *,
    failures: list[str],
    service_metrics: dict[str, Any],
) -> None:
    if not bool(service_metrics.get("report_available")):
        failures.append("service metrics recovery: service_metrics missing")
        return
    local_apply_backlog = dict(service_metrics.get("local_apply_backlog") or {})
    local_apply_checks = (
        ("stale_applied_not_ingested_present", "local_apply_backlog stale applied-not-ingested work"),
        ("closure_retry_backlog_present", "local_apply_closure retry backlog"),
        ("closure_stale_running_present", "local_apply_closure stale running item"),
    )
    for flag_key, label in local_apply_checks:
        if bool(local_apply_backlog.get(flag_key)):
            failures.append(f"service metrics recovery: {label} detected")
    snapshot_full_materialization_queue = dict(service_metrics.get("snapshot_full_materialization_queue") or {})
    snapshot_checks = (
        ("retry_backlog_present", "snapshot_full_materialization retry backlog"),
        ("stale_running_present", "snapshot_full_materialization stale running item"),
    )
    for flag_key, label in snapshot_checks:
        if bool(snapshot_full_materialization_queue.get(flag_key)):
            failures.append(f"service metrics recovery: {label} detected")
    search_seed_discovery_queue = dict(service_metrics.get("search_seed_discovery_queue") or {})
    discovery_checks = (
        ("item_without_worker_owner_present", "search_seed_discovery owner missing"),
        ("discovery_worker_without_item_present", "search_seed_discovery worker without discovery item"),
        (
            "discovery_worker_without_local_apply_present",
            "search_seed_discovery worker without local_apply_closure item",
        ),
        ("retry_backlog_present", "search_seed_discovery ready retry backlog"),
        ("stale_provider_owned_present", "search_seed_discovery stale provider-owned item"),
        ("exhausted_without_provider_retry_present", "search_seed_discovery exhausted without provider_retry report"),
    )
    for flag_key, label in discovery_checks:
        if bool(search_seed_discovery_queue.get(flag_key)):
            failures.append(f"service metrics recovery: {label} detected")
    provider_search_retry_queue = dict(service_metrics.get("provider_search_retry_queue") or {})
    provider_retry_checks = (
        ("retry_backlog_present", "provider_search_retry ready retry backlog"),
        ("stale_running_present", "provider_search_retry stale running item"),
    )
    for flag_key, label in provider_retry_checks:
        if bool(provider_search_retry_queue.get(flag_key)):
            failures.append(f"service metrics recovery: {label} detected")
    recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
    if bool(recovery_phase_metrics.get("report_available")):
        recovery_checks = (
            ("missing_phase_present", "recovery phase metrics missing"),
            ("failed_phase_present", "recovery phase failure"),
            ("slow_phase_present", "recovery phase slow"),
            ("unexpected_enabled_phase_present", "unexpected heavy phase enabled in job-scoped recovery"),
        )
        for flag_key, label in recovery_checks:
            if bool(recovery_phase_metrics.get(flag_key)):
                failures.append(f"service metrics recovery: {label} detected")


def _append_board_visible_projection_violation_failures(
    *,
    failures: list[str],
    service_metrics: dict[str, Any],
    require_report: bool,
) -> None:
    board_visible_projection = dict(service_metrics.get("board_visible_projection") or {})
    if not bool(board_visible_projection.get("report_available")):
        if require_report:
            failures.append("board-visible projection: report missing")
        return
    board_projection_checks = (
        ("projection_missing_for_visible_count", "visible count without serving projection"),
        ("patch_log_missing_for_visible_count", "visible count without patch log"),
        ("patch_log_replay_lag", "patch log replay lag"),
        ("patch_log_required_missing", "required patch log missing"),
        ("materialization_lag_violation", "fetched-to-board-visible materialization lag"),
        ("metadata_replay_dependency", "metadata replay dependency"),
    )
    for flag_key, label in board_projection_checks:
        if bool(board_visible_projection.get(flag_key)):
            failures.append(f"board-visible projection: {label} detected")
    if not bool(board_visible_projection.get("patch_sequence_contiguous", True)):
        failures.append("board-visible projection: non-contiguous patch sequence detected")


def _evaluate_smoke_expectations(
    *,
    record: dict[str, Any],
    expectations: dict[str, Any] | None,
    provider_invocations: list[dict[str, Any]],
) -> list[str]:
    payload = dict(expectations or {})
    if not payload:
        return []
    failures: list[str] = validate_smoke_expectations(payload)
    final = dict(record.get("final") or {})
    provider_report = dict(record.get("provider_case_report") or {})
    behavior_guardrails = dict(provider_report.get("behavior_guardrails") or {})
    workflow_benchmark = dict(provider_report.get("workflow_benchmark") or {})
    event_efficiency = dict(provider_report.get("event_level_efficiency") or {})
    service_metrics = dict(provider_report.get("service_metrics") or {})
    worker_timeline_metrics = dict(service_metrics.get("worker_timeline") or {})
    board_visible_projection_metrics = dict(service_metrics.get("board_visible_projection") or {})
    post_profile_completion_metrics = dict(service_metrics.get("post_profile_completion") or {})
    workflow_causality_contract_metrics = dict(service_metrics.get("workflow_causality_contract") or {})
    recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
    target_public_web_metrics = dict(service_metrics.get("target_candidate_public_web") or {})
    company_public_web_metrics = dict(service_metrics.get("company_public_web") or {})
    out_of_order_completion = dict(worker_timeline_metrics.get("out_of_order_completion") or {})
    post_preview_finalization = dict(provider_report.get("post_preview_finalization") or {})
    progress_observability = dict(provider_report.get("progress_observability") or {})
    progress_maxima = dict(progress_observability.get("maxima") or {})
    provider_slots = dict(event_efficiency.get("provider_slots") or {})
    profile_scheduler_contract = dict(event_efficiency.get("profile_scheduler_contract") or {})
    remote_actor_slot_observation = dict(provider_report.get("remote_actor_slot_observation") or {})
    reconcile_metrics = dict(event_efficiency.get("reconcile") or {})
    profile_batch_envelope_metrics = dict(event_efficiency.get("profile_batch_envelopes") or {})
    profile_worker_batch_size_metrics = dict(
        profile_batch_envelope_metrics.get("provider_worker_batch_size")
        or profile_batch_envelope_metrics.get("batch_size")
        or {}
    )
    profile_prefetch_batch_plan_metrics = dict(event_efficiency.get("profile_prefetch_batch_plan") or {})
    profile_scraper_invocation_count = sum(
        1
        for item in list(provider_invocations or [])
        if str(dict(item or {}).get("logical_name") or "").strip() == "harvest_profile_scraper_batch"
    )
    observed_remote_actor_worker_count = max(
        _safe_int(event_efficiency.get("max_remote_actor_worker_count")),
        _safe_int(provider_slots.get("remote_actor_worker_count")),
        _safe_int(progress_maxima.get("waiting_remote_harvest_count")),
        _safe_int(progress_maxima.get("waiting_remote_search_count")),
    )
    canonical_projection_proof = _canonical_projection_completion_proof(
        provider_report=provider_report,
        service_metrics=service_metrics,
    )

    if (
        bool(payload.get("require_terminal_job"))
        and str(final.get("raw_job_status") or "").strip().lower() != "completed"
    ):
        failures.append(f"require_terminal_job: raw_job_status={final.get('raw_job_status') or ''}")
    remote_provider_event_driver = dict(record.get("remote_provider_event_driver") or {})
    if remote_provider_event_driver and (
        bool(payload.get("drive_remote_provider_events"))
        or bool(payload.get("drive_remote_provider_duplicate_events"))
        or bool(payload.get("drive_remote_provider_watcher_first_events"))
        or bool(payload.get("require_no_remote_provider_event_driver_failure"))
    ):
        failed_driver_events = [
            dict(item)
            for item in list(remote_provider_event_driver.get("events") or [])
            if str(dict(item).get("status") or "").strip().endswith("_failed")
        ]
        if failed_driver_events:
            failures.append(
                "remote_provider_event_driver failed: "
                + ", ".join(str(item.get("status") or "unknown") for item in failed_driver_events[:3])
            )
    if bool(payload.get("drive_remote_provider_watcher_first_events")):
        driver_events = [dict(item) for item in list(remote_provider_event_driver.get("events") or [])]
        watcher_recovery = any(
            str(item.get("source") or "").strip() == "local_provider_event_watcher"
            and (
                _safe_int(item.get("recovery_count")) > 0
                or _safe_int(item.get("recovery_dispatch_count")) > 0
                or shared_recovery_signal_count(dict(item.get("shared_recovery_signal") or {})) > 0
            )
            for item in driver_events
        )
        provider_late = any(
            str(item.get("source") or "").strip() == "provider_webhook"
            and str(item.get("reason") or "").strip() == "matching_remote_provider_workers_not_recoverable"
            and _safe_int(item.get("recovery_count")) == 0
            for item in driver_events
        )
        if not watcher_recovery:
            failures.append("remote_provider_event_driver watcher-first recovery was not observed")
        if not provider_late:
            failures.append("remote_provider_event_driver provider-webhook late duplicate was not observed")
    if bool(payload.get("require_no_duplicate_provider_dispatch")):
        duplicate_report = dict(behavior_guardrails.get("duplicate_provider_dispatch") or {})
        if bool(duplicate_report.get("violation_detected")):
            failures.append("duplicate_provider_dispatch violation detected")
    if bool(payload.get("require_no_unexpected_public_web_stage")):
        disabled_stage_report = dict(behavior_guardrails.get("disabled_stage_violations") or {})
        if bool(disabled_stage_report.get("unexpected_public_web_stage")):
            failures.append("unexpected public-web stage detected")
    if bool(payload.get("require_no_event_level_efficiency_violation")) and bool(
        event_efficiency.get("violation_detected")
    ):
        failures.append("event-level efficiency violation detected")
    if bool(payload.get("require_no_profile_scheduler_contract_violation")):
        if not bool(profile_scheduler_contract.get("report_available")):
            failures.append("profile scheduler contract: report missing")
        elif bool(profile_scheduler_contract.get("violation_detected")):
            scheduler_violation_details = {
                key: _safe_int(profile_scheduler_contract.get(key))
                for key in (
                    "same_wave_ordinal_violation_count",
                    "batch_size_contract_violation_count",
                    "small_normal_batch_without_reason_count",
                    "retry_wave_isolation_violation_count",
                    "slot_refill_violation_count",
                    "advisory_lock_missing_count",
                    "planned_dispatch_owner_missing_count",
                    "terminal_queue_state_leak_count",
                )
                if _safe_int(profile_scheduler_contract.get(key)) > 0
            }
            detail_text = ", ".join(f"{key}={value}" for key, value in sorted(scheduler_violation_details.items()))
            failures.append(
                "profile scheduler contract violation detected" + (f" ({detail_text})" if detail_text else "")
            )
    if bool(payload.get("require_workflow_causality_contract")):
        if not bool(workflow_causality_contract_metrics.get("report_available")):
            failures.append("workflow causality contract: report missing")
        elif bool(workflow_causality_contract_metrics.get("violation_detected")):
            causality_violation_details = {
                key: _safe_int(workflow_causality_contract_metrics.get(key))
                for key in (
                    "missing_envelope_count",
                    "incomplete_envelope_count",
                    "no_op_contract_violation_count",
                    "migration_adapter_command_count",
                )
                if _safe_int(workflow_causality_contract_metrics.get(key)) > 0
            }
            detail_text = ", ".join(f"{key}={value}" for key, value in sorted(causality_violation_details.items()))
            failures.append(
                "workflow causality contract violation detected" + (f" ({detail_text})" if detail_text else "")
            )
    if bool(payload.get("require_no_post_profile_heuristic_slo_pairing")):
        profile_file_to_board_patch = dict(
            post_profile_completion_metrics.get("profile_file_visible_to_board_patch_visible") or {}
        )
        if not bool(post_profile_completion_metrics.get("report_available")):
            failures.append("post-profile completion report unavailable")
        elif bool(profile_file_to_board_patch.get("heuristic_pairing_used")):
            failures.append(
                "post-profile SLO heuristic pairing used "
                f"(legacy_snapshot_pair_count={_safe_int(profile_file_to_board_patch.get('legacy_snapshot_pair_count'))})"
            )
    if bool(payload.get("require_post_terminal_recovery_settled")):
        recovery_state = dict(record.get("post_terminal_recovery") or {})
        if not recovery_state:
            failures.append("post_terminal_recovery_settled: missing recovery state")
        elif not bool(recovery_state.get("settled")):
            failures.append(
                "post_terminal_recovery_settled: "
                f"remaining_recoverable_worker_count={_safe_int(recovery_state.get('remaining_recoverable_worker_count'))}, "
                f"remaining_materialization_item_count={_safe_int(recovery_state.get('remaining_materialization_item_count'))}, "
                "background_snapshot_full_materialization_pending_count="
                f"{_safe_int(recovery_state.get('background_snapshot_full_materialization_pending_count'))}, "
                "projection_facet_layering_pending_count="
                f"{_safe_int(recovery_state.get('projection_facet_layering_pending_count'))}"
            )
    if bool(payload.get("require_no_recovery_phase_violation")):
        if not bool(recovery_phase_metrics.get("report_available")):
            failures.append("recovery_phase_metrics: report missing")
        else:
            recovery_phase_checks = (
                ("missing_phase_present", "missing phase metrics"),
                ("failed_phase_present", "failed phase"),
                ("slow_phase_present", "slow phase"),
                ("unexpected_enabled_phase_present", "unexpected enabled phase"),
            )
            for flag_key, label in recovery_phase_checks:
                if bool(recovery_phase_metrics.get(flag_key)):
                    failures.append(f"recovery_phase_metrics: {label} detected")
    if bool(payload.get("require_post_preview_finalization_observed")):
        if canonical_projection_proof["complete"]:
            pass
        elif not bool(post_preview_finalization.get("report_available")):
            failures.append("post_preview_finalization_observed: report unavailable")
        elif _safe_int(post_preview_finalization.get("materialize_completed_count")) <= 0:
            failures.append("post_preview_finalization_observed: materialize_completed_count=0")
        elif (
            _safe_int(post_preview_finalization.get("finalization_completed_event_count")) <= 0
            and not str(post_preview_finalization.get("finalization_completed_at") or "").strip()
        ):
            failures.append("post_preview_finalization_observed: finalization_completed_event_count=0")
    if "max_stage1_terminal_to_finalization_start_ms" in payload:
        if not bool(post_preview_finalization.get("report_available")):
            failures.append("post_preview_finalization.stage1_terminal_to_finalization_start_ms: report unavailable")
        else:
            gate_value = post_preview_finalization.get("finalization_start_gate_ms")
            actual_value = _safe_float(
                gate_value
                if gate_value is not None
                else post_preview_finalization.get("stage1_terminal_to_first_finalization_start_ms")
            )
            expected_value = _safe_float(payload.get("max_stage1_terminal_to_finalization_start_ms"))
            if actual_value > expected_value:
                failures.append(
                    _expectation_maximum_failure(
                        name="post_preview_finalization.stage1_terminal_to_finalization_start_ms",
                        actual=actual_value,
                        expected=expected_value,
                    )
                )
    if bool(payload.get("require_no_progress_contract_violation")):
        if not progress_observability:
            failures.append("progress contract: progress_observability missing")
        if bool(progress_observability.get("regression_detected")):
            regression_keys = sorted(
                str(key) for key in dict(progress_observability.get("counter_regressions") or {}) if str(key).strip()
            )
            failures.append(
                "progress contract: public counter regression detected"
                + (f" ({', '.join(regression_keys)})" if regression_keys else "")
            )
        if bool(progress_observability.get("stage1_regression_detected")):
            regression_keys = sorted(
                str(key)
                for key in dict(progress_observability.get("stage1_counter_regressions") or {})
                if str(key).strip()
            )
            failures.append(
                "progress contract: Stage 1 counter regression detected"
                + (f" ({', '.join(regression_keys)})" if regression_keys else "")
            )
        if bool(progress_observability.get("result_view_lifecycle_regression_detected")):
            regression_keys = sorted(
                str(key)
                for key in dict(progress_observability.get("result_view_lifecycle_regressions") or {})
                if str(key).strip()
            )
            failures.append(
                "progress contract: result-view lifecycle regression detected"
                + (f" ({', '.join(regression_keys)})" if regression_keys else "")
            )
        if bool(progress_observability.get("contract_violation_detected")):
            violation_counts = {
                str(key): _safe_int(value)
                for key, value in dict(progress_observability.get("contract_violation_counts") or {}).items()
                if str(key).strip() and _safe_int(value) > 0
            }
            detail = ", ".join(f"{key}={value}" for key, value in sorted(violation_counts.items()))
            failures.append("progress contract: invariant violation detected" + (f" ({detail})" if detail else ""))
    if bool(payload.get("require_board_runtime_state_cross_endpoint_parity")):
        parity_report = dict(provider_report.get("board_runtime_state_parity") or {})
        if not bool(parity_report.get("report_available")):
            failures.append("board runtime parity: report unavailable")
        elif not bool(parity_report.get("consistent")):
            missing_sources = [
                str(item or "").strip()
                for item in list(parity_report.get("missing_sources") or [])
                if str(item or "").strip()
            ]
            mismatched_fields = [
                str(item or "").strip()
                for item in list(parity_report.get("mismatched_fields") or [])
                if str(item or "").strip()
            ]
            details: list[str] = []
            if missing_sources:
                details.append("missing=" + ",".join(missing_sources))
            if mismatched_fields:
                details.append("mismatched=" + ",".join(mismatched_fields))
            failures.append(
                "board runtime parity: /progress, /dashboard, /candidates, /board-patches drift"
                + (f" ({'; '.join(details)})" if details else "")
            )
    if bool(payload.get("require_legacy_artifact_coherence_report")):
        artifact_coherence = dict(provider_report.get("legacy_artifact_coherence") or {})
        if not bool(artifact_coherence.get("report_available")):
            failures.append("legacy artifact coherence: report unavailable")
        elif bool(artifact_coherence.get("terminal_drift_detected")):
            failures.append(
                "legacy artifact coherence: legacy artifact completed while canonical job/progress was nonterminal "
                f"(canonical_job_status={artifact_coherence.get('canonical_job_status') or 'missing'}, "
                f"progress_status={artifact_coherence.get('progress_status') or 'missing'}, "
                f"legacy_artifact_status={artifact_coherence.get('legacy_artifact_status') or 'missing'})"
            )
    if bool(payload.get("require_terminal_board_runtime_complete")):
        board_report = dict(provider_report.get("board") or {})
        board_runtime_state = dict(board_report.get("board_runtime_state") or {})
        if not board_runtime_state:
            board_runtime_state = dict(progress_observability.get("latest_board_runtime_state") or {})
        if not board_runtime_state:
            failures.append("terminal board runtime complete: board_runtime_state missing")
        else:
            phase = str(board_runtime_state.get("phase") or "").strip().lower()
            publication_status = str(board_runtime_state.get("publication_status") or "").strip().lower()
            expected_count = _safe_int(board_runtime_state.get("expected_candidate_count"))
            served_count = _safe_int(board_runtime_state.get("served_candidate_count"))
            published_count = _safe_int(board_runtime_state.get("published_candidate_count"))
            row_hydration_target_count = _safe_int(board_runtime_state.get("row_hydration_target_count"))
            complete_phase = phase in {"current_snapshot_serving", "current_serving", "post_result_layering"}
            complete_publication = (
                publication_status == "complete"
                and expected_count > 0
                and max(served_count, published_count, row_hydration_target_count) >= expected_count
            )
            if not complete_phase or not complete_publication:
                failures.append(
                    "terminal board runtime complete: expected complete current snapshot publication "
                    f"(phase={phase or 'missing'}, publication_status={publication_status or 'missing'}, "
                    f"expected={expected_count}, served={served_count}, published={published_count}, "
                    f"row_hydration={row_hydration_target_count})"
                )
    if bool(payload.get("require_no_service_recovery_violation")):
        _append_service_recovery_violation_failures(
            failures=failures,
            service_metrics=service_metrics,
        )
    if bool(payload.get("require_no_target_public_web_guardrail_violation")):
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append("target_candidate_public_web: service metrics unavailable")
        elif bool(target_public_web_metrics.get("service_guardrail_violation_detected")):
            latest_metrics = dict(dict(target_public_web_metrics.get("latest_batch") or {}).get("phase_metrics") or {})
            failures.append(
                "target_candidate_public_web guardrail violation detected "
                f"(pending={_safe_int(target_public_web_metrics.get('remote_search_pending_run_count'))}, "
                f"gaps={_safe_int(target_public_web_metrics.get('unmaterialized_signal_gap_count'))}, "
                f"terminal_errors={_safe_int(target_public_web_metrics.get('terminal_with_errors_count'))}, "
                f"missing_metrics={_safe_int(target_public_web_metrics.get('missing_phase_metric_count'))}, "
                f"latest_guardrail={bool(latest_metrics.get('service_guardrail_violation_detected'))})"
            )
    if bool(payload.get("require_crm_public_web_storage_owner")):
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append("target_candidate_public_web.storage_owner: service metrics unavailable")
        elif _safe_int(target_public_web_metrics.get("crm_storage_owner_batch_count")) <= 0:
            failures.append("target_candidate_public_web.storage_owner: crm_public_web_v1 owner batch not observed")
        elif _safe_int(target_public_web_metrics.get("legacy_storage_owner_batch_count")) > 0:
            failures.append(
                _expectation_maximum_failure(
                    name="target_candidate_public_web.legacy_storage_owner_batch_count",
                    actual=_safe_int(target_public_web_metrics.get("legacy_storage_owner_batch_count")),
                    expected=0,
                )
            )
    if bool(payload.get("require_public_web_execution_backend_report")):
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append("target_candidate_public_web.execution_backend: service metrics unavailable")
        elif not dict(target_public_web_metrics.get("execution_backend_counts") or {}):
            failures.append("target_candidate_public_web.execution_backend: backend counts missing")
        elif _safe_int(target_public_web_metrics.get("execution_backend_bridge_count")) > 0:
            failures.append(
                _expectation_maximum_failure(
                    name="target_candidate_public_web.execution_backend_bridge_count",
                    actual=_safe_int(target_public_web_metrics.get("execution_backend_bridge_count")),
                    expected=0,
                )
            )
    if bool(payload.get("require_crm_public_web_queue_batch_command")):
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append("target_candidate_public_web.queue_batch_command: service metrics unavailable")
        elif bool(target_public_web_metrics.get("queue_batch_command_missing")):
            failures.append(
                "target_candidate_public_web.queue_batch_command: crm.public_web.queue_batch command missing"
            )
        elif _safe_int(target_public_web_metrics.get("queue_batch_command_succeeded_count")) <= 0:
            failures.append(
                "target_candidate_public_web.queue_batch_command: crm.public_web.queue_batch command did not succeed"
            )
        elif _safe_int(target_public_web_metrics.get("queue_batch_command_expected_owner_count")) <= 0:
            failures.append(
                "target_candidate_public_web.queue_batch_command: crm_public_web_owner command not observed"
            )
        elif _safe_int(target_public_web_metrics.get("queue_batch_command_invalid_owner_count")) > 0:
            failures.append(
                _expectation_maximum_failure(
                    name="target_candidate_public_web.queue_batch_command_invalid_owner_count",
                    actual=_safe_int(target_public_web_metrics.get("queue_batch_command_invalid_owner_count")),
                    expected=0,
                )
            )
        elif _safe_int(target_public_web_metrics.get("queue_batch_command_incomplete_causality_count")) > 0:
            failures.append(
                _expectation_maximum_failure(
                    name="target_candidate_public_web.queue_batch_command_incomplete_causality_count",
                    actual=_safe_int(target_public_web_metrics.get("queue_batch_command_incomplete_causality_count")),
                    expected=0,
                )
            )
    if bool(payload.get("require_no_company_public_web_guardrail_violation")):
        if not bool(company_public_web_metrics.get("report_available")):
            failures.append("company_public_web: service metrics unavailable")
        elif bool(company_public_web_metrics.get("service_guardrail_violation_detected")):
            failures.append(
                "company_public_web guardrail violation detected "
                f"(failed={_safe_int(company_public_web_metrics.get('failed_run_count'))}, "
                f"raw={_safe_int(company_public_web_metrics.get('raw_assets_included_count'))}, "
                f"missing_collector_manifest={_safe_int(company_public_web_metrics.get('collector_manifest_missing_count'))})"
            )
    if bool(payload.get("require_no_board_visible_projection_violation")):
        _append_board_visible_projection_violation_failures(
            failures=failures,
            service_metrics=service_metrics,
            require_report=bool(payload.get("require_board_visible_projection_report")),
        )
    if bool(payload.get("require_no_placeholder_to_final_board_jump")):
        if not bool(board_visible_projection_metrics.get("report_available")):
            failures.append("placeholder-to-final board jump: board-visible projection report unavailable")
        else:
            consumable_patch_count = _safe_int(
                board_visible_projection_metrics.get("patch_consumable_card_nonzero_count")
            )
            visible_card_progression_count = max(
                _safe_int(board_visible_projection_metrics.get("patch_consumable_card_distinct_count")),
                _safe_int(board_visible_projection_metrics.get("patch_display_ready_distinct_count")),
                _safe_int(board_visible_projection_metrics.get("patch_delta_card_visible_distinct_count")),
            )
            board_runtime_state = dict(dict(provider_report.get("board") or {}).get("board_runtime_state") or {})
            expected_board_count = max(
                _safe_int(board_runtime_state.get("expected_candidate_count")),
                _safe_int(board_visible_projection_metrics.get("served_candidate_count")),
            )

            def _metric_max(name: str) -> float:
                return _safe_float(dict(board_visible_projection_metrics.get(name) or {}).get("max"))

            max_visible_patch_count = max(
                _metric_max("patch_consumable_card_count"),
                _metric_max("patch_display_ready_candidate_count"),
                _metric_max("patch_delta_card_visible_count"),
            )
            final_results_ms = _safe_float(
                dict(service_metrics.get("user_experience") or {}).get("job_to_final_results_ms")
                or dict(provider_report.get("workflow_wall_clock_ms") or {}).get("job_to_final_results")
            )
            partial_visible_ms = _safe_float(
                dict(service_metrics.get("user_experience") or {}).get("job_to_board_visible_partial_ms")
                or dict(provider_report.get("workflow_wall_clock_ms") or {}).get("job_to_board_visible_partial")
            )
            complete_patch_before_final = bool(
                expected_board_count > 0
                and max_visible_patch_count >= float(expected_board_count)
                and partial_visible_ms > 0.0
                and (final_results_ms <= 0.0 or partial_visible_ms < final_results_ms)
            )
            if consumable_patch_count <= 0:
                failures.append("placeholder-to-final board jump: no consumable running card patch observed")
            if visible_card_progression_count < 2 and not complete_patch_before_final:
                failures.append(
                    "placeholder-to-final board jump: consumable card progression did not advance before final"
                )
            if partial_visible_ms <= 0.0 or (final_results_ms > 0.0 and partial_visible_ms >= final_results_ms):
                failures.append("placeholder-to-final board jump: no board-visible publication before final results")
    if bool(payload.get("require_partial_board_visible_before_final_results")):
        user_experience_metrics = dict(service_metrics.get("user_experience") or {})
        workflow_wall_clock = dict(provider_report.get("workflow_wall_clock_ms") or {})
        partial_visible_ms = _safe_float(
            user_experience_metrics.get("job_to_board_visible_partial_ms")
            or workflow_wall_clock.get("job_to_board_visible_partial")
        )
        final_results_ms = _safe_float(
            user_experience_metrics.get("job_to_final_results_ms") or workflow_wall_clock.get("job_to_final_results")
        )
        if partial_visible_ms <= 0.0:
            failures.append("partial board visible: job_to_board_visible_partial_ms missing")
        elif final_results_ms > 0.0 and partial_visible_ms >= final_results_ms:
            failures.append(
                "partial board visible: expected before final results "
                f"(partial={partial_visible_ms}, final={final_results_ms})"
            )
    serving_publication_gap_metrics = dict(service_metrics.get("serving_publication_gap") or {})
    if bool(payload.get("require_no_serving_publication_gap")):
        if not bool(service_metrics.get("report_available")):
            failures.append("serving publication gap: service_metrics missing")
        elif bool(serving_publication_gap_metrics.get("gap_present")):
            failures.append(
                "serving publication gap detected "
                f"(served={serving_publication_gap_metrics.get('served_snapshot_id') or ''}, "
                f"current={serving_publication_gap_metrics.get('current_snapshot_id') or ''})"
            )
    if bool(payload.get("require_no_local_apply_candidate_documents_retry_storm")):
        # Asserts the event-level reawaken contract: candidate_documents_missing
        # closure items must move to `waiting_prerequisite` (never burn
        # `failed_retryable` budget) and the prerequisite-writer event must
        # reawaken them within the 30 s safety bound. Longer means a real
        # prerequisite is missing or the event hook didn't fire.
        local_apply_backlog = dict(service_metrics.get("local_apply_backlog") or {})
        if not bool(service_metrics.get("report_available")) and not bool(local_apply_backlog.get("report_available")):
            failures.append("local_apply_backlog: service_metrics missing")
        else:
            error_samples = list(local_apply_backlog.get("closure_error_samples") or [])
            doc_missing_failures = [
                sample
                for sample in error_samples
                if isinstance(sample, str) and "candidate_documents_missing" in sample
            ]
            if doc_missing_failures:
                failures.append(
                    "local_apply candidate_documents_missing retry storm: "
                    f"{len(doc_missing_failures)} retryable failures observed"
                )
            waiting_age_max_ms = _safe_float(local_apply_backlog.get("waiting_prerequisite_age_max_ms"))
            if waiting_age_max_ms > 30000:
                failures.append(f"local_apply waiting_prerequisite age exceeds 30 s (age_max_ms={waiting_age_max_ms})")
    if bool(payload.get("require_stable_expected_candidate_count_during_stage1")):
        if not progress_observability:
            failures.append("stable expected candidate count: progress_observability missing")
        else:
            expected_violations = list(progress_observability.get("stage1_expected_candidate_count_violations") or [])
            if expected_violations:
                failures.append(
                    "stable expected candidate count: expected_candidate_count changed before Stage 1 promotion "
                    f"({len(expected_violations)} samples)"
                )
            promotion_regressions = list(progress_observability.get("stage1_denominator_promotion_regressions") or [])
            if promotion_regressions:
                failures.append(
                    "stable expected candidate count: Stage 1 denominator promotion regressed "
                    f"({len(promotion_regressions)} samples)"
                )
            promotion_count = _safe_int(progress_observability.get("stage1_denominator_promotion_count"))
            promoted_observed = bool(progress_observability.get("stage1_denominator_promoted_observed"))
            unpromoted_sample_count = _safe_int(
                progress_observability.get("stage1_denominator_unpromoted_sample_count")
            )
            if promotion_count > 1:
                failures.append(
                    "stable expected candidate count: expected at most one Stage 1 denominator promotion "
                    f"(observed={promotion_count})"
                )
            if not promoted_observed:
                failures.append("stable expected candidate count: Stage 1 denominator promotion was never observed")
            elif unpromoted_sample_count > 0 and promotion_count == 0:
                failures.append(
                    "stable expected candidate count: observed unpromoted Stage 1 samples but no promotion transition"
                )
    if bool(payload.get("require_stable_user_facing_profile_card_denominator_during_stage1")):
        if not progress_observability:
            failures.append("stable profile/card denominator: progress_observability missing")
        else:
            denominator_violations = list(progress_observability.get("stage1_user_facing_denominator_violations") or [])
            if denominator_violations:
                fields = sorted(
                    {str(dict(item).get("field") or "") for item in denominator_violations if isinstance(item, dict)}
                )
                failures.append(
                    "stable profile/card denominator: user-facing status text exposed /N before promotion"
                    + (f" ({', '.join(field for field in fields if field)})" if fields else "")
                )
    if bool(payload.get("require_filter_returns_running_card_ready_recall_buckets")):
        running_filter_probe = dict(provider_report.get("running_candidate_filter_probe") or {})
        if not running_filter_probe:
            failures.append("running recall filter: probe report missing")
        elif _safe_int(running_filter_probe.get("max_display_ready_count")) <= 0:
            pass
        elif not bool(running_filter_probe.get("display_ready_recall_bucket_observed")):
            failures.append(
                "running recall filter: recall_buckets=Agent did not return running display-ready rows "
                f"(samples={_safe_int(running_filter_probe.get('sample_count'))}, "
                f"max_display_ready={_safe_int(running_filter_probe.get('max_display_ready_count'))})"
            )
    if bool(payload.get("require_layering_visible_after_results_within_slo")):
        board_report = dict(provider_report.get("board") or {})
        layering_status = str(board_report.get("layering_status") or "").strip().lower()
        layering_visible = layering_status in {"completed", "ready", "available"}
        layering_deferred_with_complete_board = _post_result_layering_deferred_with_complete_board(board_report)
        if not layering_visible and not layering_deferred_with_complete_board:
            failures.append(
                "layering visible after results: layering not visible or explicitly deferred behind a complete board "
                f"(status={layering_status or 'missing'})"
            )
    if "max_progress_payload_bytes" in payload:
        if not progress_observability:
            failures.append("progress payload bytes: progress_observability missing")
        else:
            actual_payload_bytes = max(
                _safe_int(progress_observability.get("max_payload_bytes")),
                _safe_int(dict(progress_observability.get("payload_bytes") or {}).get("max")),
            )
            expected_payload_bytes = _safe_int(payload.get("max_progress_payload_bytes"))
            if actual_payload_bytes > expected_payload_bytes:
                failures.append(
                    _expectation_maximum_failure(
                        name="progress_payload_bytes",
                        actual=actual_payload_bytes,
                        expected=expected_payload_bytes,
                    )
                )

    explain_payload = dict(record.get("explain") or {})
    execution_payload = dict(provider_report.get("execution") or {})
    if bool(payload.get("require_no_active_stage1_for_full_local_reuse")):
        effective_acquisition_mode = str(
            execution_payload.get("effective_acquisition_mode")
            or explain_payload.get("effective_acquisition_mode")
            or ""
        ).strip()
        planner_mode = str(execution_payload.get("planner_mode") or explain_payload.get("planner_mode") or "").strip()
        dispatch_strategy = str(
            execution_payload.get("dispatch_strategy") or explain_payload.get("dispatch_strategy") or ""
        ).strip()
        full_local_reuse = (
            effective_acquisition_mode == "full_local_asset_reuse"
            or planner_mode == "reuse_snapshot_only"
            or dispatch_strategy in {"reuse_snapshot", "reuse_completed"}
        )
        if full_local_reuse:
            if not progress_observability:
                failures.append("full local reuse progress: progress_observability missing")
            latest_contract = dict(progress_observability.get("latest_execution_phase_contract") or {})
            latest_lifecycle = dict(progress_observability.get("latest_result_view_lifecycle") or {})
            active_text = " ".join(
                str(latest_contract.get(key) or "")
                for key in (
                    "active_phase_id",
                    "active_phase_label",
                    "active_phase_detail",
                    "active_stage_id",
                    "active_stage_label",
                    "active_stage_detail",
                )
            ).strip()
            active_text_lower = active_text.lower()
            if bool(latest_contract.get("profile_work_pending")):
                failures.append("full local reuse progress: profile_work_pending=true")
            stage1_markers = (
                "新发现在职候选人",
                "新发现离职候选人",
                "新取回在职候选人",
                "新取回离职候选人",
                "需补取 LinkedIn Profile",
            )
            if "linkedin stage 1" in active_text_lower or any(marker in active_text for marker in stage1_markers):
                failures.append(f"full local reuse progress: active Stage 1 wording detected ({active_text})")
            if bool(latest_lifecycle.get("delta_profile_progress_applicable")):
                failures.append("full local reuse progress: delta_profile_progress_applicable=true")
    string_expectations = (
        (
            "expect_explain_dispatch_strategy",
            "explain.dispatch_strategy",
            str(explain_payload.get("dispatch_strategy") or execution_payload.get("dispatch_strategy") or ""),
        ),
        (
            "expect_explain_planner_mode",
            "explain.planner_mode",
            str(explain_payload.get("planner_mode") or execution_payload.get("planner_mode") or ""),
        ),
        (
            "expect_explain_effective_acquisition_mode",
            "explain.effective_acquisition_mode",
            str(
                explain_payload.get("effective_acquisition_mode")
                or execution_payload.get("effective_acquisition_mode")
                or ""
            ),
        ),
        (
            "expect_explain_baseline_snapshot_id",
            "explain.asset_reuse_baseline_snapshot_id",
            str(explain_payload.get("asset_reuse_baseline_snapshot_id") or ""),
        ),
        (
            "expect_explain_dispatch_matched_snapshot_id",
            "explain.dispatch_matched_snapshot_id",
            str(explain_payload.get("dispatch_matched_snapshot_id") or ""),
        ),
        (
            "expect_explain_request_delta_baseline_snapshot_id",
            "explain.request_delta_baseline_snapshot_id",
            str(explain_payload.get("request_delta_baseline_snapshot_id") or ""),
        ),
    )
    for expectation_key, metric_name, actual_text in string_expectations:
        if expectation_key not in payload:
            continue
        expected_text = str(payload.get(expectation_key) or "")
        if actual_text != expected_text:
            failures.append(
                _expectation_equal_failure(
                    name=metric_name,
                    actual=actual_text,
                    expected=expected_text,
                )
            )
    if "expect_explain_requires_delta_acquisition" in payload:
        actual_requires_delta = bool(explain_payload.get("requires_delta_acquisition"))
        expected_requires_delta = bool(payload.get("expect_explain_requires_delta_acquisition"))
        if actual_requires_delta != expected_requires_delta:
            failures.append(
                _expectation_equal_failure(
                    name="explain.requires_delta_acquisition",
                    actual=actual_requires_delta,
                    expected=expected_requires_delta,
                )
            )
    expected_keywords = [
        str(item or "").strip()
        for item in list(payload.get("expect_explain_keywords_include") or [])
        if str(item or "").strip()
    ]
    if expected_keywords:
        actual_keywords = {
            str(item or "").strip() for item in list(explain_payload.get("keywords") or []) if str(item or "").strip()
        }
        missing_keywords = [keyword for keyword in expected_keywords if keyword not in actual_keywords]
        if missing_keywords:
            failures.append(
                _expectation_equal_failure(
                    name="explain.keywords",
                    actual=sorted(actual_keywords),
                    expected=f"include {missing_keywords}",
                )
            )

    latest_stage1_progress = dict(progress_observability.get("latest_linkedin_stage_1_progress") or {})
    latest_stage1_expectations = (
        (
            "expect_latest_stage1_current_search_returned_count",
            "latest_stage1.current_search_returned_count",
            _safe_int(latest_stage1_progress.get("current_search_returned_count")),
        ),
        (
            "expect_latest_stage1_former_search_returned_count",
            "latest_stage1.former_search_returned_count",
            _safe_int(latest_stage1_progress.get("former_search_returned_count")),
        ),
        (
            "expect_latest_stage1_all_search_returned_count",
            "latest_stage1.all_search_returned_count",
            _safe_int(latest_stage1_progress.get("all_search_returned_count")),
        ),
        (
            "expect_latest_stage1_deduped_candidate_count",
            "latest_stage1.deduped_candidate_count",
            _safe_int(latest_stage1_progress.get("deduped_candidate_count")),
        ),
        (
            "expect_latest_stage1_deduped_profile_url_count",
            "latest_stage1.deduped_profile_url_count",
            _safe_int(latest_stage1_progress.get("deduped_profile_url_count")),
        ),
        (
            "expect_latest_stage1_profile_fetch_required_count",
            "latest_stage1.profile_fetch_required_count",
            _safe_int(latest_stage1_progress.get("profile_fetch_required_count")),
        ),
        (
            "expect_latest_stage1_profile_fetched_count",
            "latest_stage1.profile_fetched_count",
            _safe_int(latest_stage1_progress.get("profile_fetched_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in latest_stage1_expectations:
        if expectation_key not in payload:
            continue
        if not latest_stage1_progress:
            failures.append(f"{metric_name}: latest LinkedIn Stage 1 progress missing")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value != expected_value:
            failures.append(
                _expectation_equal_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    latest_lifecycle = dict(progress_observability.get("latest_result_view_lifecycle") or {})
    lifecycle_minimum_expectations = (
        (
            "min_latest_lifecycle_baseline_candidate_count",
            "latest_lifecycle.baseline_candidate_count",
            _safe_int(latest_lifecycle.get("baseline_candidate_count")),
        ),
        (
            "min_latest_lifecycle_served_candidate_count",
            "latest_lifecycle.served_candidate_count",
            _safe_int(latest_lifecycle.get("served_candidate_count")),
        ),
        (
            "min_latest_lifecycle_delta_profile_materialized_count",
            "latest_lifecycle.delta_profile_materialized_count",
            _safe_int(latest_lifecycle.get("delta_profile_materialized_count")),
        ),
        (
            "min_latest_lifecycle_delta_profile_board_visible_count",
            "latest_lifecycle.delta_profile_board_visible_count",
            _safe_int(latest_lifecycle.get("delta_profile_board_visible_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in lifecycle_minimum_expectations:
        if expectation_key not in payload:
            continue
        if not latest_lifecycle:
            failures.append(f"{metric_name}: latest result-view lifecycle missing")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    lifecycle_exact_expectations = (
        (
            "expect_latest_lifecycle_baseline_candidate_count",
            "latest_lifecycle.baseline_candidate_count",
            _safe_int(latest_lifecycle.get("baseline_candidate_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in lifecycle_exact_expectations:
        if expectation_key not in payload:
            continue
        if not latest_lifecycle:
            failures.append(f"{metric_name}: latest result-view lifecycle missing")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value != expected_value:
            failures.append(
                _expectation_equal_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    if bool(payload.get("require_latest_lifecycle_served_snapshot_not_baseline")):
        if not latest_lifecycle:
            failures.append("latest_lifecycle.served_snapshot_id: latest result-view lifecycle missing")
        else:
            baseline_snapshot_id = str(latest_lifecycle.get("baseline_snapshot_id") or "").strip()
            served_snapshot_id = str(latest_lifecycle.get("served_snapshot_id") or "").strip()
            if not baseline_snapshot_id or not served_snapshot_id or served_snapshot_id == baseline_snapshot_id:
                failures.append(
                    "latest_lifecycle.served_snapshot_id: "
                    f"served snapshot must differ from baseline (served={served_snapshot_id}, "
                    f"baseline={baseline_snapshot_id})"
                )

    board_visible_minimum_expectations = (
        (
            "min_board_visible_patch_count",
            "board_visible_projection.patch_log_count",
            _safe_int(board_visible_projection_metrics.get("patch_log_count")),
        ),
        (
            "min_delta_profile_board_visible_count",
            "board_visible_projection.delta_profile_board_visible_count",
            _safe_int(board_visible_projection_metrics.get("delta_profile_board_visible_count")),
        ),
        (
            "min_board_visible_patch_display_ready_count",
            "board_visible_projection.patch_display_ready_nonzero_count",
            _safe_int(board_visible_projection_metrics.get("patch_display_ready_nonzero_count")),
        ),
        (
            "min_board_visible_patch_consumable_card_count",
            "board_visible_projection.patch_consumable_card_nonzero_count",
            _safe_int(board_visible_projection_metrics.get("patch_consumable_card_nonzero_count")),
        ),
        (
            "min_board_visible_display_ready_progression_count",
            "board_visible_projection.patch_display_ready_distinct_count",
            _safe_int(board_visible_projection_metrics.get("patch_display_ready_distinct_count")),
        ),
        (
            "min_board_visible_consumable_card_progression_count",
            "board_visible_projection.patch_visible_card_progression_count",
            max(
                _safe_int(board_visible_projection_metrics.get("patch_consumable_card_distinct_count")),
                _safe_int(board_visible_projection_metrics.get("patch_display_ready_distinct_count")),
                _safe_int(board_visible_projection_metrics.get("patch_delta_card_visible_distinct_count")),
            ),
        ),
    )
    for expectation_key, metric_name, actual_value in board_visible_minimum_expectations:
        if expectation_key not in payload:
            continue
        if not bool(board_visible_projection_metrics.get("report_available")):
            failures.append(f"{metric_name}: board-visible projection report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    minimum_checks = (
        ("min_provider_invocation_count", "provider_invocation_count", len(provider_invocations)),
        (
            "min_profile_url_total_count",
            "profile_url_total_count",
            _safe_int(workflow_benchmark.get("profile_url_total_count")),
        ),
        (
            "min_fetched_profile_count",
            "fetched_profile_count",
            _safe_int(workflow_benchmark.get("fetched_profile_count")),
        ),
        (
            "min_board_total_candidates",
            "board_total_candidates",
            _safe_int(workflow_benchmark.get("board_total_candidates")),
        ),
        (
            "min_agent_worker_count",
            "agent_worker_count",
            _safe_int(
                dict(service_metrics.get("worker_timeline") or {}).get("worker_count")
                or dict(service_metrics.get("worker_count") or {}).get("max")
            ),
        ),
        (
            "min_remote_actor_worker_count",
            "remote_actor_worker_count",
            max(observed_remote_actor_worker_count, profile_scraper_invocation_count),
        ),
        (
            "min_out_of_order_profile_completion_count",
            "out_of_order_profile_completion_count",
            _safe_int(out_of_order_completion.get("profile_batch_inversion_count")),
        ),
        (
            "min_profile_batch_envelope_count",
            "profile_batch_envelope_count",
            _safe_int(profile_batch_envelope_metrics.get("envelope_count")),
        ),
        (
            "min_profile_batch_size_max",
            "profile_batch_size_max",
            _safe_float(profile_worker_batch_size_metrics.get("max")),
        ),
        (
            "min_profile_tiny_batch_coalesced_count",
            "profile_tiny_batch_coalesced_count",
            _safe_int(profile_batch_envelope_metrics.get("tiny_batch_coalesced_count")),
        ),
        (
            "min_profile_prefetch_batch_plan_count",
            "profile_prefetch_batch_plan_count",
            _safe_int(profile_prefetch_batch_plan_metrics.get("plan_count")),
        ),
        (
            "min_profile_prefetch_planned_new_worker_count",
            "profile_prefetch_planned_new_worker_count",
            _safe_int(profile_prefetch_batch_plan_metrics.get("planned_new_worker_count")),
        ),
        (
            "min_completed_reconcile_lease_skipped_count",
            "completed_reconcile_lease_skipped_count",
            _safe_int(reconcile_metrics.get("lease_skipped_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in minimum_checks:
        if expectation_key not in payload:
            continue
        if expectation_key.startswith("min_profile_") and not bool(event_efficiency.get("report_available")):
            failures.append(f"{metric_name}: event-level efficiency report unavailable")
            continue
        if expectation_key.startswith("min_completed_reconcile_") and not bool(
            event_efficiency.get("report_available")
        ):
            failures.append(f"{metric_name}: event-level efficiency report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    if "min_materialize_completed_count" in payload and not canonical_projection_proof["complete"]:
        actual_value = _safe_int(post_preview_finalization.get("materialize_completed_count"))
        expected_value = _safe_int(payload.get("min_materialize_completed_count"))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name="materialize_completed_count",
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    if "min_remote_actor_slot_occupancy_ratio" in payload:
        if not bool(remote_actor_slot_observation.get("report_available")):
            failures.append("remote_actor_slot_occupancy_ratio: remote actor slot observation missing")
        else:
            actual_ratio = _safe_float(remote_actor_slot_observation.get("remote_actor_slot_peak_occupancy_ratio"))
            expected_ratio = _safe_float(payload.get("min_remote_actor_slot_occupancy_ratio"))
            if actual_ratio < expected_ratio:
                failures.append(
                    _expectation_minimum_failure(
                        name="remote_actor_slot_occupancy_ratio",
                        actual=actual_ratio,
                        expected=expected_ratio,
                    )
                )

    maximum_checks = (
        ("max_provider_invocation_count", "provider_invocation_count", len(provider_invocations)),
        (
            "max_repeated_materialize_signature_count",
            "repeated_materialize_signature_count",
            _safe_int(reconcile_metrics.get("repeated_materialize_signature_count")),
        ),
        (
            "max_materialize_started_repeat_count",
            "materialize_started_repeat_count",
            _safe_int(reconcile_metrics.get("materialize_started_repeat_count")),
        ),
        (
            "max_marker_backfill_repeat_count",
            "marker_backfill_repeat_count",
            _safe_int(reconcile_metrics.get("marker_backfill_repeat_count")),
        ),
        (
            "max_same_worker_reconcile_repeat_count",
            "same_worker_reconcile_repeat_count",
            _safe_int(reconcile_metrics.get("same_worker_reconcile_repeat_count")),
        ),
        (
            "max_profile_unexplained_tiny_batch_count",
            "profile_unexplained_tiny_batch_count",
            _safe_int(profile_batch_envelope_metrics.get("unexplained_tiny_batch_count")),
        ),
        (
            "max_profile_batch_size_max",
            "profile_batch_size_max",
            _safe_float(profile_worker_batch_size_metrics.get("max")),
        ),
        (
            "max_provider_slot_underuse_with_backlog_count",
            "provider_slot_underuse_with_backlog_count",
            _safe_int(profile_batch_envelope_metrics.get("provider_slot_underuse_with_backlog_count")),
        ),
        (
            "max_profile_prefetch_underfilled_with_deferred_count",
            "profile_prefetch_underfilled_with_deferred_count",
            _safe_int(profile_prefetch_batch_plan_metrics.get("underfilled_with_deferred_items_count")),
        ),
        (
            "max_post_profile_url_terminal_state_leak_count",
            "post_profile_completion.url_terminal_state_recording.terminal_queue_state_leak_count",
            _safe_int(
                dict(post_profile_completion_metrics.get("url_terminal_state_recording") or {}).get(
                    "terminal_queue_state_leak_count"
                )
            ),
        ),
    )
    for expectation_key, metric_name, actual_value in maximum_checks:
        if expectation_key not in payload:
            continue
        if expectation_key.startswith(("max_profile_", "max_provider_slot_")) and not bool(
            event_efficiency.get("report_available")
        ):
            failures.append(f"{metric_name}: event-level efficiency report unavailable")
            continue
        if expectation_key.startswith("max_post_profile_") and not bool(
            post_profile_completion_metrics.get("report_available")
        ):
            failures.append(f"{metric_name}: post-profile completion report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    event_efficiency_slo_checks = (
        (
            "max_remote_to_local_marker_lag_ms",
            "event_level_efficiency.remote_to_local_marker_lag_ms",
            dict(event_efficiency.get("remote_to_local_marker_lag_ms") or {}),
        ),
        (
            "max_local_to_next_submit_start_ms",
            "event_level_efficiency.local_to_next_submit_start_ms",
            dict(
                event_efficiency.get("local_to_next_submit_start_ms")
                or event_efficiency.get("local_completion_to_next_submit_start_ms")
                or {}
            ),
        ),
        (
            "max_provider_io_actor_run_duration_ms",
            "event_level_efficiency.provider_io.actor_run_duration_ms",
            dict(dict(event_efficiency.get("provider_io") or {}).get("actor_run_duration_ms") or {}),
        ),
        (
            "max_provider_io_dataset_download_duration_ms",
            "event_level_efficiency.provider_io.dataset_download_duration_ms",
            dict(dict(event_efficiency.get("provider_io") or {}).get("dataset_download_duration_ms") or {}),
        ),
        (
            "max_next_submit_attempt_elapsed_ms",
            "event_level_efficiency.next_submit_provider_attempt_elapsed_ms",
            dict(
                event_efficiency.get("next_submit_provider_attempt_elapsed_ms")
                or event_efficiency.get("next_submit_attempt_elapsed_ms")
                or {}
            ),
        ),
        (
            "max_provider_slot_to_remote_wait_started_ms",
            "event_level_efficiency.provider_slot_to_remote_wait_started_ms",
            dict(event_efficiency.get("provider_slot_to_remote_wait_started_ms") or {}),
        ),
    )
    next_submit_opportunity = dict(event_efficiency.get("next_submit_opportunity") or {})
    next_submit_metric_not_applicable = next_submit_opportunity.get("applicable") is False
    for expectation_key, metric_name, metric_payload in event_efficiency_slo_checks:
        if expectation_key not in payload:
            continue
        if not bool(event_efficiency.get("report_available")):
            failures.append(f"{metric_name}: event-level efficiency report unavailable")
            continue
        if next_submit_metric_not_applicable and metric_name in {
            "event_level_efficiency.local_to_next_submit_start_ms",
            "event_level_efficiency.next_submit_provider_attempt_elapsed_ms",
        }:
            continue
        if "max" not in metric_payload:
            failures.append(f"{metric_name}: metric missing")
            continue
        actual_value = _safe_float(metric_payload.get("max"))
        expected_value = _safe_float(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    if "max_remote_to_next_submit_start_ms" in payload:
        if not bool(event_efficiency.get("report_available")):
            failures.append(
                "event_level_efficiency.remote_to_next_submit_start_ms: event-level efficiency report unavailable"
            )
        else:
            remote_to_next_submit_metrics = dict(event_efficiency.get("remote_to_next_submit_start_ms") or {})
            if "max" not in remote_to_next_submit_metrics:
                if not next_submit_metric_not_applicable:
                    failures.append("event_level_efficiency.remote_to_next_submit_start_ms: metric missing")
            else:
                actual_value = _safe_float(remote_to_next_submit_metrics.get("max"))
                requested_value = _safe_float(payload.get("max_remote_to_next_submit_start_ms"))
                hard_threshold = _safe_float(
                    dict(event_efficiency.get("thresholds_ms") or {}).get("remote_to_next_submit_start_hard")
                )
                expected_value = max(requested_value, hard_threshold) if hard_threshold > 0 else requested_value
                if actual_value > expected_value:
                    failures.append(
                        _expectation_maximum_failure(
                            name="event_level_efficiency.remote_to_next_submit_start_ms",
                            actual=actual_value,
                            expected=expected_value,
                        )
                    )

    user_experience_metrics = dict(service_metrics.get("user_experience") or {})
    workflow_wall_clock = dict(provider_report.get("workflow_wall_clock_ms") or {})
    remote_provider_event_metrics = dict(service_metrics.get("remote_provider_events") or {})
    provider_anomaly_metrics = dict(service_metrics.get("provider_anomalies") or {})
    global_handoff_gap_metrics = dict(
        dict(worker_timeline_metrics.get("handoff_gap_ms") or {}).get("global_next_worker_start_gap_ms") or {}
    )
    profile_scheduler_handoff_gap_metrics = dict(
        dict(worker_timeline_metrics.get("handoff_gap_ms") or {}).get("profile_scheduler_next_worker_start_gap_ms")
        or {}
    )
    expectation_handoff_gap_metrics = (
        profile_scheduler_handoff_gap_metrics if profile_scheduler_handoff_gap_metrics else global_handoff_gap_metrics
    )
    expectation_handoff_gap_metric_name = (
        "service_metrics.worker_timeline.profile_scheduler_next_worker_start_gap_ms"
        if profile_scheduler_handoff_gap_metrics
        else "service_metrics.worker_timeline.global_next_worker_start_gap_ms"
    )
    remote_provider_event_lag_metrics = dict(
        remote_provider_event_metrics.get("actionable_remote_to_local_event_lag_ms")
        or remote_provider_event_metrics.get("remote_to_local_event_lag_ms")
        or {}
    )
    remote_provider_event_minimum_checks = (
        (
            "min_remote_provider_event_count",
            "remote_provider_event_count",
            _safe_int(remote_provider_event_metrics.get("event_count")),
        ),
        (
            "min_remote_provider_event_received_count",
            "remote_provider_event_received_count",
            _safe_int(remote_provider_event_metrics.get("received_count")),
        ),
        (
            "min_remote_provider_event_late_duplicate_count",
            "remote_provider_event_late_duplicate_count",
            _safe_int(remote_provider_event_metrics.get("late_duplicate_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in remote_provider_event_minimum_checks:
        if expectation_key not in payload:
            continue
        if not bool(remote_provider_event_metrics.get("report_available")):
            failures.append(f"{metric_name}: remote provider event report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    for expectation_key, metric_key, metric_name in (
        (
            "min_remote_provider_event_source_counts",
            "source_counts",
            "remote_provider_event_source_counts",
        ),
        (
            "min_remote_provider_event_status_counts",
            "status_counts",
            "remote_provider_event_status_counts",
        ),
    ):
        if expectation_key not in payload:
            continue
        if not bool(remote_provider_event_metrics.get("report_available")):
            failures.append(f"{metric_name}: remote provider event report unavailable")
            continue
        expected_counts = dict(payload.get(expectation_key) or {})
        actual_counts = {
            str(key): _safe_int(value)
            for key, value in dict(remote_provider_event_metrics.get(metric_key) or {}).items()
            if str(key).strip()
        }
        for expected_key, expected_value in sorted(expected_counts.items()):
            normalized_key = str(expected_key or "").strip()
            if not normalized_key:
                continue
            actual_value = _safe_int(actual_counts.get(normalized_key))
            minimum_value = _safe_int(expected_value)
            if actual_value < minimum_value:
                failures.append(
                    _expectation_minimum_failure(
                        name=f"{metric_name}.{normalized_key}",
                        actual=actual_value,
                        expected=minimum_value,
                    )
                )
    remote_provider_event_maximum_checks = (
        (
            "max_remote_provider_event_in_flight_duplicate_count",
            "remote_provider_event_in_flight_duplicate_count",
            _safe_int(remote_provider_event_metrics.get("in_flight_duplicate_count")),
        ),
        (
            "max_remote_provider_event_duplicate_count",
            "remote_provider_event_duplicate_count",
            _safe_int(remote_provider_event_metrics.get("duplicate_event_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in remote_provider_event_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(remote_provider_event_metrics.get("report_available")):
            failures.append(f"{metric_name}: remote provider event report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    provider_anomaly_minimum_checks = (
        ("min_provider_anomaly_count", "provider_anomalies.anomaly_count", "anomaly_count"),
        (
            "min_provider_zero_result_retry_count",
            "provider_anomalies.zero_result_retry_count",
            "zero_result_retry_count",
        ),
        (
            "min_provider_zero_result_retry_exhausted_count",
            "provider_anomalies.zero_result_retry_exhausted_count",
            "zero_result_retry_exhausted_count",
        ),
        ("min_provider_empty_scale_count", "provider_anomalies.empty_scale_count", "empty_scale_count"),
        (
            "min_provider_probe_total_drift_count",
            "provider_anomalies.probe_total_drift_count",
            "probe_total_drift_count",
        ),
        (
            "min_provider_empty_page_range_count",
            "provider_anomalies.empty_page_range_count",
            "empty_page_range_count",
        ),
        (
            "min_provider_single_page_retry_count",
            "provider_anomalies.single_page_retry_count",
            "single_page_retry_count",
        ),
    )
    for expectation_key, metric_name, metric_key in provider_anomaly_minimum_checks:
        if expectation_key not in payload:
            continue
        if not bool(provider_anomaly_metrics.get("report_available")):
            failures.append(f"{metric_name}: provider anomaly report unavailable")
            continue
        actual_value = _safe_int(provider_anomaly_metrics.get(metric_key))
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    provider_anomaly_maximum_checks = (
        ("max_provider_anomaly_count", "provider_anomalies.anomaly_count", "anomaly_count"),
        (
            "max_provider_zero_result_retry_count",
            "provider_anomalies.zero_result_retry_count",
            "zero_result_retry_count",
        ),
        (
            "max_provider_zero_result_retry_exhausted_count",
            "provider_anomalies.zero_result_retry_exhausted_count",
            "zero_result_retry_exhausted_count",
        ),
        (
            "max_provider_zero_result_accepted_count",
            "provider_anomalies.zero_result_accepted_count",
            "zero_result_accepted_count",
        ),
        ("max_provider_empty_scale_count", "provider_anomalies.empty_scale_count", "empty_scale_count"),
        (
            "max_provider_probe_total_drift_count",
            "provider_anomalies.probe_total_drift_count",
            "probe_total_drift_count",
        ),
        (
            "max_provider_empty_page_range_count",
            "provider_anomalies.empty_page_range_count",
            "empty_page_range_count",
        ),
        (
            "max_provider_single_page_retry_count",
            "provider_anomalies.single_page_retry_count",
            "single_page_retry_count",
        ),
    )
    for expectation_key, metric_name, metric_key in provider_anomaly_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(provider_anomaly_metrics.get("report_available")):
            failures.append(f"{metric_name}: provider anomaly report unavailable")
            continue
        actual_value = _safe_int(provider_anomaly_metrics.get(metric_key))
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    target_public_web_maximum_checks = (
        (
            "max_target_public_web_legacy_storage_owner_batch_count",
            "target_candidate_public_web.legacy_storage_owner_batch_count",
            "legacy_storage_owner_batch_count",
        ),
        (
            "max_target_public_web_execution_backend_bridge_count",
            "target_candidate_public_web.execution_backend_bridge_count",
            "execution_backend_bridge_count",
        ),
        (
            "max_target_public_web_queue_batch_command_pending_count",
            "target_candidate_public_web.queue_batch_command_pending_count",
            "queue_batch_command_pending_count",
        ),
        (
            "max_target_public_web_queue_batch_command_failed_count",
            "target_candidate_public_web.queue_batch_command_failed_count",
            "queue_batch_command_failed_count",
        ),
        (
            "max_target_public_web_queue_batch_command_invalid_owner_count",
            "target_candidate_public_web.queue_batch_command_invalid_owner_count",
            "queue_batch_command_invalid_owner_count",
        ),
        (
            "max_target_public_web_queue_batch_command_incomplete_causality_count",
            "target_candidate_public_web.queue_batch_command_incomplete_causality_count",
            "queue_batch_command_incomplete_causality_count",
        ),
        (
            "max_target_public_web_remote_pending_run_count",
            "target_candidate_public_web.remote_search_pending_run_count",
            "remote_search_pending_run_count",
        ),
        (
            "max_target_public_web_partial_failure_count",
            "target_candidate_public_web.partial_failure_count",
            "partial_failure_count",
        ),
        (
            "max_target_public_web_completed_without_materialized_signals_count",
            "target_candidate_public_web.completed_without_materialized_signals_count",
            "completed_without_materialized_signals_count",
        ),
        (
            "max_target_public_web_missing_phase_metric_count",
            "target_candidate_public_web.missing_phase_metric_count",
            "missing_phase_metric_count",
        ),
        (
            "max_target_public_web_provider_or_fetch_failure_count",
            "target_candidate_public_web.provider_or_fetch_failure_count",
            "provider_or_fetch_failure_count",
        ),
        (
            "max_target_public_web_local_processing_error_count",
            "target_candidate_public_web.local_processing_error_count",
            "local_processing_error_count",
        ),
    )
    for expectation_key, metric_name, metric_key in target_public_web_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append(f"{metric_name}: target-candidate public web report unavailable")
            continue
        actual_value = _safe_int(target_public_web_metrics.get(metric_key))
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    if "max_target_public_web_duration_by_phase_ms" in payload:
        if not bool(target_public_web_metrics.get("report_available")):
            failures.append(
                "target_candidate_public_web.duration_by_phase_ms_max: target-candidate public web report unavailable"
            )
        else:
            expected_phase_durations = dict(payload.get("max_target_public_web_duration_by_phase_ms") or {})
            actual_phase_durations = dict(target_public_web_metrics.get("duration_by_phase_ms_max") or {})
            for phase, expected_duration in sorted(expected_phase_durations.items()):
                normalized_phase = str(phase or "").strip()
                if not normalized_phase:
                    continue
                actual_duration = _safe_float(actual_phase_durations.get(normalized_phase))
                expected_value = _safe_float(expected_duration)
                if actual_duration > expected_value:
                    failures.append(
                        _expectation_maximum_failure(
                            name=f"target_candidate_public_web.duration_by_phase_ms_max.{normalized_phase}",
                            actual=actual_duration,
                            expected=expected_value,
                        )
                    )
    company_public_web_minimum_checks = (
        (
            "min_company_public_web_asset_count",
            "company_public_web.asset_count",
            "asset_count",
        ),
        (
            "min_company_public_web_collector_record_count",
            "company_public_web.collector_record_count",
            "collector_record_count",
        ),
        (
            "min_company_public_web_collector_source_count",
            "company_public_web.collector_source_count",
            "collector_source_count",
        ),
        (
            "min_company_public_web_collector_document_fetch_count",
            "company_public_web.collector_document_fetch_count",
            "collector_document_fetch_count",
        ),
    )
    for expectation_key, metric_name, metric_key in company_public_web_minimum_checks:
        if expectation_key not in payload:
            continue
        if not bool(company_public_web_metrics.get("report_available")):
            failures.append(f"{metric_name}: company public web report unavailable")
            continue
        actual_value = _safe_int(company_public_web_metrics.get(metric_key))
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value < expected_value:
            failures.append(
                _expectation_minimum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    company_public_web_maximum_checks = (
        (
            "max_company_public_web_failed_run_count",
            "company_public_web.failed_run_count",
            "failed_run_count",
        ),
        (
            "max_company_public_web_raw_assets_included_count",
            "company_public_web.raw_assets_included_count",
            "raw_assets_included_count",
        ),
        (
            "max_company_public_web_collector_manifest_missing_count",
            "company_public_web.collector_manifest_missing_count",
            "collector_manifest_missing_count",
        ),
        (
            "max_company_public_web_collector_document_fetch_failure_count",
            "company_public_web.collector_document_fetch_failure_count",
            "collector_document_fetch_failure_count",
        ),
    )
    for expectation_key, metric_name, metric_key in company_public_web_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(company_public_web_metrics.get("report_available")):
            failures.append(f"{metric_name}: company public web report unavailable")
            continue
        actual_value = _safe_int(company_public_web_metrics.get(metric_key))
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )
    expected_provider_invocation_minimums = dict(payload.get("min_provider_invocations_by_logical_name") or {})
    provider_work_expected = (
        bool(provider_invocations)
        or observed_remote_actor_worker_count > 0
        or _safe_int(remote_provider_event_metrics.get("event_count")) > 0
        or _safe_int(remote_provider_event_metrics.get("target_worker_count")) > 0
        or _safe_int(payload.get("min_remote_actor_worker_count")) > 0
    )
    if not provider_work_expected:
        provider_work_expected = any(_safe_int(value) > 0 for value in expected_provider_invocation_minimums.values())
    profile_file_visible_to_board_patch_visible = dict(
        post_profile_completion_metrics.get("profile_file_visible_to_board_patch_visible") or {}
    )
    profile_file_visible_elapsed = dict(profile_file_visible_to_board_patch_visible.get("elapsed_ms") or {})
    require_profile_file_visible_metric = bool(
        post_profile_completion_metrics.get("report_available") and not canonical_projection_proof["complete"]
    )
    service_slo_maximum_checks = (
        (
            "max_job_to_stage_1_preview_ms",
            "workflow_wall_clock_ms.job_to_stage_1_preview",
            _safe_float(workflow_wall_clock.get("job_to_stage_1_preview")),
            workflow_wall_clock,
            "job_to_stage_1_preview",
            True,
        ),
        (
            "max_job_to_board_visible_partial_ms",
            "service_metrics.user_experience.job_to_board_visible_partial_ms",
            _safe_float(user_experience_metrics.get("job_to_board_visible_partial_ms")),
            user_experience_metrics,
            "job_to_board_visible_partial_ms",
            True,
        ),
        (
            "max_final_results_to_board_nonempty_ms",
            "service_metrics.user_experience.final_results_to_board_nonempty_ms",
            _safe_float(user_experience_metrics.get("final_results_to_board_nonempty_ms")),
            user_experience_metrics,
            "final_results_to_board_nonempty_ms",
            True,
        ),
        (
            "max_job_to_board_nonempty_ms",
            "service_metrics.user_experience.job_to_board_nonempty_ms",
            _safe_float(user_experience_metrics.get("job_to_board_nonempty_ms")),
            user_experience_metrics,
            "job_to_board_nonempty_ms",
            True,
        ),
        (
            "max_global_next_worker_start_gap_ms",
            expectation_handoff_gap_metric_name,
            _safe_float(expectation_handoff_gap_metrics.get("max")),
            expectation_handoff_gap_metrics,
            "max",
            True,
        ),
        (
            "max_remote_provider_event_lag_ms",
            "service_metrics.remote_provider_events.actionable_remote_to_local_event_lag_ms",
            _safe_float(remote_provider_event_lag_metrics.get("max")),
            remote_provider_event_lag_metrics,
            "max",
            provider_work_expected,
        ),
        (
            "max_serving_publication_gap_ms",
            "service_metrics.serving_publication_gap.age_ms",
            _safe_float(serving_publication_gap_metrics.get("age_ms")),
            serving_publication_gap_metrics,
            "age_ms",
            bool(serving_publication_gap_metrics.get("gap_present")),
        ),
        (
            "max_profile_file_visible_to_board_patch_visible_ms",
            "service_metrics.post_profile_completion.profile_file_visible_to_board_patch_visible.elapsed_ms",
            _safe_float(profile_file_visible_elapsed.get("max")),
            profile_file_visible_elapsed,
            "max",
            require_profile_file_visible_metric,
        ),
        (
            "max_all_profiles_fetched_to_all_cards_visible_ms",
            "service_metrics.post_profile_completion.all_profiles_fetched_to_all_cards_visible.elapsed_ms",
            _safe_float(
                dict(
                    dict(post_profile_completion_metrics.get("all_profiles_fetched_to_all_cards_visible") or {}).get(
                        "elapsed_ms"
                    )
                    or {}
                ).get("max")
            ),
            dict(
                dict(post_profile_completion_metrics.get("all_profiles_fetched_to_all_cards_visible") or {}).get(
                    "elapsed_ms"
                )
                or {}
            ),
            "max",
            bool(post_profile_completion_metrics.get("report_available")),
        ),
        (
            "max_event_level_materialization_callback_elapsed_ms",
            "service_metrics.post_profile_completion.event_level_callback.elapsed_ms",
            _safe_float(
                dict(
                    dict(post_profile_completion_metrics.get("event_level_callback") or {}).get("elapsed_ms") or {}
                ).get("max")
            ),
            dict(dict(post_profile_completion_metrics.get("event_level_callback") or {}).get("elapsed_ms") or {}),
            "max",
            bool(post_profile_completion_metrics.get("report_available")),
        ),
        (
            "max_recovery_phase_elapsed_ms",
            "service_metrics.recovery_phase_metrics.elapsed_ms",
            _safe_float(dict(recovery_phase_metrics.get("elapsed_ms") or {}).get("max")),
            dict(recovery_phase_metrics.get("elapsed_ms") or {}),
            "max",
            bool(recovery_phase_metrics.get("report_available")),
        ),
        (
            "max_recovery_total_elapsed_ms",
            "service_metrics.recovery_phase_metrics.total_elapsed_ms",
            _safe_float(dict(recovery_phase_metrics.get("total_elapsed_ms") or {}).get("max")),
            dict(recovery_phase_metrics.get("total_elapsed_ms") or {}),
            "max",
            bool(recovery_phase_metrics.get("report_available")),
        ),
        (
            "max_recovery_tick_budget_exhausted_count",
            "service_metrics.recovery_phase_metrics.budget_yield_attention_count",
            _safe_float(
                max(
                    0,
                    _safe_int(recovery_phase_metrics.get("recovery_tick_budget_exhausted_count"))
                    - _safe_int(recovery_phase_metrics.get("cooperative_budget_yield_count")),
                )
            ),
            recovery_phase_metrics,
            "recovery_tick_budget_exhausted_count",
            bool(recovery_phase_metrics.get("report_available")),
        ),
    )
    for (
        expectation_key,
        metric_name,
        actual_value,
        metric_payload,
        value_key,
        require_metric,
    ) in service_slo_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(service_metrics.get("report_available")):
            failures.append(f"{metric_name}: service_metrics missing")
            continue
        if require_metric and value_key not in metric_payload:
            failures.append(f"{metric_name}: metric missing")
            continue
        expected_value = _safe_float(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    board_visible_maximum_checks = (
        (
            "max_fetched_to_board_visible_lag_count",
            "board_visible_projection.fetched_to_board_visible_lag_count",
            _safe_int(board_visible_projection_metrics.get("fetched_to_board_visible_lag_count")),
        ),
        (
            "max_materialized_to_board_visible_lag_count",
            "board_visible_projection.materialized_to_board_visible_lag_count",
            _safe_int(board_visible_projection_metrics.get("materialized_to_board_visible_lag_count")),
        ),
        (
            "max_board_visible_patch_lag_count",
            "board_visible_projection.patch_log_lag_count",
            _safe_int(board_visible_projection_metrics.get("patch_log_lag_count")),
        ),
    )
    for expectation_key, metric_name, actual_value in board_visible_maximum_checks:
        if expectation_key not in payload:
            continue
        if not bool(board_visible_projection_metrics.get("report_available")):
            failures.append(f"{metric_name}: board-visible projection report unavailable")
            continue
        expected_value = _safe_int(payload.get(expectation_key))
        if actual_value > expected_value:
            failures.append(
                _expectation_maximum_failure(
                    name=metric_name,
                    actual=actual_value,
                    expected=expected_value,
                )
            )

    expected_by_logical_name = payload.get("min_provider_invocations_by_logical_name")
    if isinstance(expected_by_logical_name, dict):
        counts_by_logical_name = Counter(
            str(item.get("logical_name") or "").strip()
            for item in provider_invocations
            if str(item.get("logical_name") or "").strip()
        )
        for logical_name, expected_value in expected_by_logical_name.items():
            normalized_name = str(logical_name or "").strip()
            if not normalized_name:
                continue
            actual_value = int(counts_by_logical_name.get(normalized_name) or 0)
            expected_count = _safe_int(expected_value)
            if actual_value < expected_count:
                failures.append(
                    _expectation_minimum_failure(
                        name=f"provider_invocations.{normalized_name}",
                        actual=actual_value,
                        expected=expected_count,
                    )
                )
    if "max_company_public_web_collector_fetch_duration_ms" in payload:
        if not bool(company_public_web_metrics.get("report_available")):
            failures.append("company_public_web.collector_fetch_duration_ms_max: company public web report unavailable")
        else:
            actual_duration = _safe_float(company_public_web_metrics.get("collector_fetch_duration_ms_max"))
            expected_duration = _safe_float(payload.get("max_company_public_web_collector_fetch_duration_ms"))
            if actual_duration > expected_duration:
                failures.append(
                    _expectation_maximum_failure(
                        name="company_public_web.collector_fetch_duration_ms_max",
                        actual=actual_duration,
                        expected=expected_duration,
                    )
                )
    return failures


def run_hosted_smoke_case(
    client: HostedWorkflowSmokeClient,
    *,
    case_name: str,
    payload: dict[str, Any],
    reviewer: str,
    poll_seconds: float,
    max_poll_seconds: float,
    auto_continue_stage2: bool = True,
    runtime_tuning_profile: str = "",
    review_decision: dict[str, Any] | None = None,
    expectations: dict[str, Any] | None = None,
    target_public_web_action: dict[str, Any] | None = None,
    company_public_web_action: dict[str, Any] | None = None,
) -> dict[str, Any]:
    case_started_at = time.perf_counter()
    case_wall_started_at = datetime.now(timezone.utc)
    effective_payload = dict(payload or {})
    normalized_expectations = dict(expectations or {})
    normalized_target_public_web_action = dict(target_public_web_action or {})
    normalized_company_public_web_action = dict(company_public_web_action or {})
    allow_early_results_ready = _case_allows_early_results_ready(normalized_expectations)
    drive_remote_provider_events = bool(
        normalized_expectations.get("drive_remote_provider_events")
        or effective_payload.get("drive_remote_provider_events")
        or "max_remote_provider_event_lag_ms" in normalized_expectations
    )
    drive_remote_provider_duplicate_events = bool(
        normalized_expectations.get("drive_remote_provider_duplicate_events")
        or effective_payload.get("drive_remote_provider_duplicate_events")
    )
    drive_remote_provider_watcher_first_events = bool(
        normalized_expectations.get("drive_remote_provider_watcher_first_events")
        or effective_payload.get("drive_remote_provider_watcher_first_events")
    )
    normalized_runtime_tuning_profile = str(runtime_tuning_profile or "").strip().lower()
    if normalized_runtime_tuning_profile:
        execution_preferences = dict(effective_payload.get("execution_preferences") or {})
        execution_preferences["runtime_tuning_profile"] = normalized_runtime_tuning_profile
        effective_payload["execution_preferences"] = execution_preferences
    record: dict[str, Any] = {"case": case_name, "query": effective_payload.get("raw_user_request")}
    timings_ms: dict[str, float] = {}
    explain_started_at = time.perf_counter()
    explain = client.post("/api/workflows/explain", effective_payload)
    timings_ms["explain"] = round((time.perf_counter() - explain_started_at) * 1000, 2)
    record["explain"] = _build_smoke_explain_digest(explain=explain, effective_payload=effective_payload)
    plan_started_at = time.perf_counter()
    # C1 (substrate-unify): the synchronous /api/plan route was deleted. Plan
    # compile is the async submit path the frontend uses — POST /api/plan/submit
    # returns pending, a worker compiles, and the result lands on the frontend
    # history link. Submit, then poll the history recovery until the plan is
    # hydrated (review_id present) or generation terminates. This smoke-tests the
    # real async plan path instead of a removed sync shortcut.
    submit = client.post("/api/plan/submit", effective_payload)
    plan = dict(submit)
    plan_history_id = str(submit.get("history_id") or "").strip()
    if plan_history_id:
        plan_poll_ticks = max(1, int(max_poll_seconds / max(0.1, poll_seconds)))
        for _plan_tick in range(plan_poll_ticks + 1):
            recovered = client.get(f"/api/frontend-history/{plan_history_id}")
            recovery = dict(recovered.get("recovery") or {})
            plan_generation = dict(dict(recovery.get("metadata") or {}).get("plan_generation") or {})
            generation_status = str(plan_generation.get("status") or "").strip().lower()
            recovered_review_id = int(recovery.get("review_id") or 0)
            if recovered_review_id > 0 or generation_status in {"completed", "failed"}:
                plan = {
                    "status": "needs_plan_review" if recovered_review_id > 0 else (generation_status or "pending"),
                    "history_id": plan_history_id,
                    "request": dict(recovery.get("request") or {}),
                    "plan": dict(recovery.get("plan") or {}),
                    "plan_review_gate": dict(recovery.get("plan_review_gate") or {}),
                    "plan_review_session": dict(recovery.get("plan_review_session") or {}),
                    "error_message": str(recovery.get("error_message") or ""),
                }
                break
            time.sleep(max(0.05, poll_seconds))
    timings_ms["plan"] = round((time.perf_counter() - plan_started_at) * 1000, 2)
    review_id = (plan.get("plan_review_session") or {}).get("review_id")
    record["plan_review"] = {
        "review_id": review_id,
        "gate_status": ((plan.get("plan_review_gate") or {}).get("status")),
        "risk_level": ((plan.get("plan_review_gate") or {}).get("risk_level")),
    }
    if review_id:
        review_started_at = time.perf_counter()
        review = client.post(
            "/api/plan/review",
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": reviewer,
                "decision": dict(review_decision or {}),
            },
        )
        timings_ms["review"] = round((time.perf_counter() - review_started_at) * 1000, 2)
        record["review"] = {"status": review.get("status"), "review_id": review_id}
        start_payload: dict[str, Any] = {"plan_review_id": review_id}
        for key in ("runtime_execution_mode", "workflow_runner_mode"):
            value = str(effective_payload.get(key) or "").strip()
            if value:
                start_payload[key] = value
        if "auto_job_daemon" in effective_payload:
            start_payload["auto_job_daemon"] = bool(effective_payload.get("auto_job_daemon"))
    else:
        timings_ms["review"] = 0.0
        record["review"] = {"status": "skipped", "review_id": None}
        start_payload = dict(effective_payload)
    start_started_at = time.perf_counter()
    start = client.post("/api/workflows", start_payload)
    timings_ms["start"] = round((time.perf_counter() - start_started_at) * 1000, 2)
    job_id = start.get("job_id")
    record["start"] = {key: start.get(key) for key in ("job_id", "status", "stage")}
    if job_id:
        record["job_id"] = str(job_id)
    timeline: list[dict[str, Any]] = []
    progress_samples: list[dict[str, Any]] = []
    results: dict[str, Any] = {}
    job_payload: dict[str, Any] = {}
    stage2_continue_requested = False
    worker_recovery_runs: list[dict[str, Any]] = []
    shared_recovery_signal_runs: list[dict[str, Any]] = []
    remote_provider_event_driver_runs: list[dict[str, Any]] = []
    remote_provider_event_driver_accepted_workers: list[dict[str, Any]] = []
    remote_provider_event_driver_accepted_keys: set[tuple[str, str]] = set()
    running_results_probe_errors: list[dict[str, Any]] = []
    poll_auto_recovery_errors: list[dict[str, Any]] = []
    remote_event_recovery_grace_until = 0.0
    running_candidate_filter_probe_samples: list[dict[str, Any]] = []
    remote_provider_event_sequence = 1
    last_worker_recovery_at = 0.0
    worker_recovery_cooldown_seconds = max(0.25, poll_seconds * 5)
    running_filter_probe_enabled = bool(
        normalized_expectations.get("require_filter_returns_running_card_ready_recall_buckets")
    )
    last_running_filter_probe_at = 0.0
    running_filter_probe_cooldown_seconds = max(0.5, poll_seconds * 2)
    last_results_probe_at = 0.0
    results_probe_cooldown_seconds = max(2.0, poll_seconds * 10)
    if job_id:
        wait_started_at = time.perf_counter()
        seen_marker: tuple[Any, Any, Any, Any] | None = None
        max_ticks = max(1, int(max_poll_seconds / max(0.1, poll_seconds)))
        for tick in range(max_ticks + 1):
            snapshot = client.get(f"/api/jobs/{job_id}/progress")
            progress_samples.append(_progress_snapshot_to_observability_sample(snapshot, tick=tick))
            marker = (
                snapshot.get("status"),
                snapshot.get("stage"),
                snapshot.get("current_message"),
                snapshot.get("awaiting_user_action"),
            )
            if marker != seen_marker:
                timeline.append(
                    {
                        "tick": tick,
                        "status": snapshot.get("status"),
                        "stage": snapshot.get("stage"),
                        "message": snapshot.get("current_message"),
                        "awaiting_user_action": snapshot.get("awaiting_user_action"),
                        "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                    }
                )
                seen_marker = marker
            if running_filter_probe_enabled and snapshot.get("status") not in TERMINAL_WORKFLOW_STATUSES:
                now = time.monotonic()
                if (now - last_running_filter_probe_at) >= running_filter_probe_cooldown_seconds:
                    try:
                        candidate_filter_payload = _fetch_smoke_candidate_page_payload(
                            client,
                            job_id=str(job_id or ""),
                            path=(
                                f"/api/jobs/{job_id}/candidates?offset=0&limit=24&lightweight=1&recall_buckets=Agent"
                            ),
                        )
                        running_candidate_filter_probe_samples.append(
                            _build_candidate_page_filter_probe_sample(
                                payload=candidate_filter_payload,
                                tick=tick,
                                status=str(snapshot.get("status") or ""),
                                stage=str(snapshot.get("stage") or ""),
                                recall_bucket="Agent",
                            )
                        )
                    except Exception as exc:
                        running_candidate_filter_probe_samples.append(
                            {
                                "tick": tick,
                                "status": str(snapshot.get("status") or ""),
                                "stage": str(snapshot.get("stage") or ""),
                                "recall_bucket": "Agent",
                                "error": str(exc),
                            }
                        )
                    last_running_filter_probe_at = now
            if (
                auto_continue_stage2
                and not stage2_continue_requested
                and snapshot.get("status") == "blocked"
                and snapshot.get("stage") == "retrieving"
                and snapshot.get("awaiting_user_action") == "continue_stage2"
            ):
                continuation = client.post(
                    f"/api/workflows/{job_id}/continue-stage2",
                    {},
                )
                record["stage2_continue"] = {
                    "status": continuation.get("status"),
                    "job_id": continuation.get("job_id"),
                    "analysis_stage": continuation.get("analysis_stage"),
                }
                timeline.append(
                    {
                        "tick": tick,
                        "status": continuation.get("status"),
                        "stage": "retrieving",
                        "message": "Smoke runner auto-requested stage 2 continuation.",
                        "awaiting_user_action": "",
                        "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                    }
                )
                stage2_continue_requested = True
            should_auto_run_recovery = _should_auto_run_worker_recovery(snapshot)
            should_drive_provider_events = (
                drive_remote_provider_events
                or drive_remote_provider_duplicate_events
                or drive_remote_provider_watcher_first_events
            ) and _should_drive_smoke_remote_provider_events(snapshot)
            if should_auto_run_recovery or should_drive_provider_events:
                now = time.monotonic()
                if (now - last_worker_recovery_at) >= worker_recovery_cooldown_seconds:
                    drove_provider_event = False
                    if (
                        drive_remote_provider_events
                        or drive_remote_provider_duplicate_events
                        or drive_remote_provider_watcher_first_events
                    ):
                        try:
                            provider_events, accepted_workers = _drive_smoke_remote_provider_webhook_recovery_once(
                                client,
                                job_id=str(job_id or ""),
                                event_sequence=remote_provider_event_sequence,
                                include_watcher_duplicate=drive_remote_provider_duplicate_events,
                                primary_source=(
                                    "local_provider_event_watcher"
                                    if drive_remote_provider_watcher_first_events
                                    else "provider_webhook"
                                ),
                                already_accepted_remote_keys=remote_provider_event_driver_accepted_keys,
                            )
                        except Exception as exc:
                            provider_events = [
                                {
                                    "status": "provider_webhook_driver_failed",
                                    "reason": str(exc),
                                    "event_sequence": remote_provider_event_sequence,
                                    "recovery_count": 0,
                                }
                            ]
                            accepted_workers = []
                        if provider_events:
                            remote_provider_event_sequence += len(provider_events)
                            remote_provider_event_driver_runs.extend(
                                {"tick": tick, "phase": "webhook_recovery", **dict(item)} for item in provider_events
                            )
                            if accepted_workers:
                                drove_provider_event = True
                                remote_event_recovery_grace_until = max(
                                    remote_event_recovery_grace_until,
                                    time.monotonic() + max(5.0, poll_seconds * 20),
                                )
                                remote_provider_event_driver_accepted_workers.extend(accepted_workers)
                                for worker in accepted_workers:
                                    remote_provider_event_driver_accepted_keys.add(
                                        _worker_smoke_remote_identity(dict(worker))
                                    )
                                timeline.append(
                                    {
                                        "tick": tick,
                                        "status": snapshot.get("status"),
                                        "stage": snapshot.get("stage"),
                                        "message": "Smoke runner drove remote provider webhook recovery for blocked acquisition.",
                                        "awaiting_user_action": "",
                                        "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                                    }
                                )
                    if (
                        should_auto_run_recovery
                        and not drove_provider_event
                        and time.monotonic() >= remote_event_recovery_grace_until
                    ):
                        try:
                            signal_response = client.post(
                                "/api/workers/daemon/run-once",
                                _smoke_shared_recovery_signal_payload(),
                            )
                            service_status = _fetch_smoke_recovery_service_status(
                                client,
                                job_id=str(job_id or ""),
                            )
                            shared_recovery_signal_runs.append(
                                _smoke_shared_recovery_signal_record(
                                    signal_response,
                                    tick=tick,
                                    phase="poll_auto_recovery_signal",
                                    service_status_payload=service_status,
                                )
                            )
                            timeline.append(
                                {
                                    "tick": tick,
                                    "status": snapshot.get("status"),
                                    "stage": snapshot.get("stage"),
                                    "message": "Smoke runner signaled the shared recovery daemon for blocked acquisition.",
                                    "awaiting_user_action": "",
                                    "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                                }
                            )
                        except Exception as exc:
                            error_record = {
                                "tick": tick,
                                "status": snapshot.get("status"),
                                "stage": snapshot.get("stage"),
                                "phase": "poll_auto_recovery",
                                "error": repr(exc),
                                "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                            }
                            poll_auto_recovery_errors.append(error_record)
                            shared_recovery_signal_runs.append(
                                {
                                    **error_record,
                                    "status": "failed",
                                    "event_type": "shared_recovery_signal",
                                    "reason": "poll_auto_recovery_signal_failed",
                                }
                            )
                    last_worker_recovery_at = now
            if snapshot.get("status") in TERMINAL_WORKFLOW_STATUSES:
                break
            if tick > 0 and str(snapshot.get("stage") or "").strip().lower() in {
                "acquiring",
                "retrieving",
                "completed",
            }:
                now = time.monotonic()
                if (now - last_results_probe_at) >= results_probe_cooldown_seconds:
                    probe_started_at = time.perf_counter()
                    try:
                        probed_job_payload = client.get(f"/api/jobs/{job_id}")
                        probed_results = _fetch_smoke_results_payload(
                            client,
                            job_id=str(job_id or ""),
                            job_payload=probed_job_payload,
                            include_runtime_details=False,
                            include_candidates=False,
                        )
                    except Exception as exc:
                        elapsed_ms = round((time.perf_counter() - probe_started_at) * 1000, 2)
                        timings_ms["results_probe"] = timings_ms.get("results_probe", 0.0) + elapsed_ms
                        running_results_probe_errors.append(
                            {
                                "tick": tick,
                                "status": snapshot.get("status"),
                                "stage": snapshot.get("stage"),
                                "error": repr(exc),
                                "elapsed_ms": elapsed_ms,
                            }
                        )
                        last_results_probe_at = now
                        continue
                    timings_ms["results_probe"] = timings_ms.get("results_probe", 0.0) + round(
                        (time.perf_counter() - probe_started_at) * 1000,
                        2,
                    )
                    probe_ready, probe_completion_state = _evaluate_smoke_record_completion(
                        probed_job_payload,
                        probed_results,
                    )
                    last_results_probe_at = now
                    if probe_ready and (
                        probe_completion_state != "results_ready_nonterminal" or allow_early_results_ready
                    ):
                        settled_job_payload = probed_job_payload
                        settled_results = probed_results
                        settle_deadline = time.monotonic() + max(2.0, poll_seconds * 20)
                        while time.monotonic() < settle_deadline:
                            settle_snapshot = client.get(f"/api/jobs/{job_id}/progress")
                            if str(settle_snapshot.get("status") or "").strip().lower() in TERMINAL_WORKFLOW_STATUSES:
                                settled_job_payload = client.get(f"/api/jobs/{job_id}")
                                settled_results = _fetch_smoke_results_payload(
                                    client,
                                    job_id=str(job_id or ""),
                                    job_payload=settled_job_payload,
                                    include_runtime_details=False,
                                    include_candidates=False,
                                )
                                probe_ready, probe_completion_state = _evaluate_smoke_record_completion(
                                    settled_job_payload,
                                    settled_results,
                                )
                                break
                            time.sleep(max(0.05, min(0.25, poll_seconds)))
                        job_payload = settled_job_payload
                        results = settled_results
                        record["early_results_probe"] = {
                            "tick": tick,
                            "smoke_completion_state": probe_completion_state,
                        }
                        break
            time.sleep(max(0.05, poll_seconds))
        timings_ms["wait_for_completion"] = round((time.perf_counter() - wait_started_at) * 1000, 2)
        if not job_payload or not results:
            results_started_at = time.perf_counter()
            job_payload = client.get(f"/api/jobs/{job_id}")
            results = _fetch_smoke_results_payload(
                client,
                job_id=str(job_id or ""),
                job_payload=job_payload,
                include_runtime_details=False,
                include_candidates=False,
            )
            timings_ms["fetch_job_and_results"] = round((time.perf_counter() - results_started_at) * 1000, 2)
        else:
            timings_ms["fetch_job_and_results"] = 0.0
        start_status = str((record.get("start") or {}).get("status") or "").strip().lower()
        if (
            str(((job_payload or {}).get("status") or "").strip().lower()) == "completed"
            and start_status != "reused_completed_job"
        ):
            (
                post_terminal_job_payload,
                post_terminal_results,
                post_terminal_signal_runs,
                post_terminal_recovery_ms,
                post_terminal_recovery_state,
            ) = _settle_post_terminal_worker_recovery(
                client,
                job_id=str(job_id or ""),
                poll_seconds=poll_seconds,
                max_rounds=max(1, _safe_int(normalized_expectations.get("post_terminal_recovery_rounds")) or 8),
                drain_background_snapshot_full_materialization=bool(
                    normalized_expectations.get("drain_background_snapshot_full_materialization_post_terminal")
                ),
                drain_projection_facet_layering=bool(
                    normalized_expectations.get("drain_projection_facet_layering_post_terminal")
                    or normalized_expectations.get("require_board_runtime_state_cross_endpoint_parity")
                ),
            )
            record["post_terminal_recovery"] = post_terminal_recovery_state
            if post_terminal_signal_runs:
                shared_recovery_signal_runs.extend(post_terminal_signal_runs)
                post_terminal_tick = (
                    max(
                        [
                            int(item.get("tick") or 0)
                            for item in timeline
                            if isinstance(item, dict) and str(item.get("tick") or "").isdigit()
                        ]
                        or [0]
                    )
                    + 1
                )
                timeline.append(
                    {
                        "tick": post_terminal_tick,
                        "status": "completed",
                        "stage": "completed",
                        "message": "Smoke runner settled post-terminal background worker recovery.",
                        "awaiting_user_action": "",
                        "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                    }
                )
                timings_ms["post_terminal_worker_recovery"] = post_terminal_recovery_ms
                if post_terminal_job_payload:
                    job_payload = post_terminal_job_payload
                if post_terminal_results:
                    results = post_terminal_results
            else:
                timings_ms["post_terminal_worker_recovery"] = 0.0
    else:
        timings_ms["wait_for_completion"] = 0.0
        timings_ms["fetch_job_and_results"] = 0.0
        timings_ms["post_terminal_worker_recovery"] = 0.0
    record["timeline"] = timeline
    timings_ms["timeline_event_count"] = float(len(timeline))
    timings_ms["total"] = round((time.perf_counter() - case_started_at) * 1000, 2)
    record["timings_ms"] = timings_ms
    if job_id:
        try:
            recovery_service_status_payload = client.get(
                f"/api/workers/daemon/status?job_id={quote(str(job_id or ''))}&include_details=1"
            )
        except Exception:
            recovery_service_status_payload = {}
        service_recovery_runs = _recovery_runs_from_service_status_payload(recovery_service_status_payload)
        if service_recovery_runs:
            worker_recovery_runs.extend(service_recovery_runs)
        service_status_report = _smoke_recovery_service_status_report(recovery_service_status_payload)
        if service_status_report.get("service_count"):
            record["recovery_service_status"] = service_status_report
    if worker_recovery_runs:
        record["worker_recovery"] = worker_recovery_runs
    if shared_recovery_signal_runs:
        record["shared_recovery_signals"] = shared_recovery_signal_runs
    if running_results_probe_errors:
        record["running_results_probe_errors"] = running_results_probe_errors
    if poll_auto_recovery_errors:
        record["poll_auto_recovery_errors"] = poll_auto_recovery_errors
    if job_id:
        results = _ensure_results_runtime_details_for_smoke_report(
            client,
            job_id=job_id,
            results_payload=results,
            timings_ms=timings_ms,
        )
    summary_payload = dict(job_payload.get("summary") or {})
    record["job_summary"] = {
        "background_reconcile": dict(summary_payload.get("background_reconcile") or {}),
        "latest_metrics": dict(summary_payload.get("latest_metrics") or {}),
    }
    dashboard_payload: dict[str, Any] = {}
    candidate_page_payload: dict[str, Any] = {}
    final_progress_payload: dict[str, Any] = {}
    board_patches_payload: dict[str, Any] = {}
    job_events: list[dict[str, Any]] = []
    agent_workers: list[dict[str, Any]] = []
    agent_trace_spans: list[dict[str, Any]] = []
    materialization_items: list[dict[str, Any]] = []
    workflow_commands: list[dict[str, Any]] = []
    board_visible_patches: list[dict[str, Any]] = []
    target_candidate_public_web_batches: list[dict[str, Any]] = []
    legacy_public_web_retirement_audit: dict[str, Any] = {}
    target_public_web_action_batch_id = ""
    company_public_web_runs: list[dict[str, Any]] = []
    if job_id:
        dashboard_payload, candidate_page_payload, board_probe_timings = _settle_board_probe(
            client,
            job_id=job_id,
            results_payload=results,
            poll_seconds=poll_seconds,
            wait_for_layering_visible=bool(
                normalized_expectations.get("require_layering_visible_after_results_within_slo")
            ),
            layering_visible_timeout_seconds=(
                float(normalized_expectations["max_final_results_to_layering_visible_ms"]) / 1000.0
                if "max_final_results_to_layering_visible_ms" in normalized_expectations
                else None
            ),
        )
        timings_ms["dashboard_fetch"] = round(float(board_probe_timings.get("dashboard_fetch_ms") or 0.0), 2)
        timings_ms["candidate_page_fetch"] = round(
            float(board_probe_timings.get("candidate_page_fetch_ms") or 0.0),
            2,
        )
        timings_ms["board_probe_wait"] = round(float(board_probe_timings.get("wait_ms") or 0.0), 2)
        timings_ms["board_ready_wait"] = round(float(board_probe_timings.get("board_ready_wait_ms") or 0.0), 2)
        timings_ms["board_nonempty_wait"] = round(float(board_probe_timings.get("board_nonempty_wait_ms") or 0.0), 2)
        timings_ms["board_expected_total_wait"] = round(
            float(board_probe_timings.get("board_expected_total_wait_ms") or 0.0),
            2,
        )
        timings_ms["layering_visible_wait"] = round(
            float(board_probe_timings.get("layering_visible_wait_ms") or 0.0),
            2,
        )
        timings_ms["layering_visible_timed_out"] = round(
            float(board_probe_timings.get("layering_visible_timed_out") or 0.0),
            2,
        )
        timings_ms["layering_visible_probe_timeout"] = round(
            float(board_probe_timings.get("layering_visible_timeout_ms") or 0.0),
            2,
        )
        if _target_public_web_action_enabled(normalized_target_public_web_action):
            public_web_action, public_web_action_signal_runs, public_web_action_ms = (
                _run_target_public_web_smoke_action(
                    client,
                    source_job_id=job_id,
                    action=normalized_target_public_web_action,
                    poll_seconds=poll_seconds,
                    case_name=case_name,
                )
            )
            timings_ms["target_public_web_action"] = round(public_web_action_ms, 2)
            record["target_public_web_action"] = public_web_action
            if public_web_action_signal_runs:
                record["target_public_web_action_recovery_signals"] = public_web_action_signal_runs
            workflow_commands.extend(_workflow_commands_from_target_public_web_action(public_web_action))
            target_public_web_action_batch_id = str(
                dict(public_web_action.get("search") or {}).get("batch_id") or ""
            ).strip()
            if target_public_web_action_batch_id:
                diagnostics_started_at = time.perf_counter()
                try:
                    public_web_payload = client.post(
                        "/api/crm/records/public-web-search/poll",
                        {"batch_id": target_public_web_action_batch_id, "limit": 1000},
                    )
                    target_candidate_public_web_batches = _public_web_batches_for_smoke_window(
                        [
                            dict(item)
                            for item in list(public_web_payload.get("batches") or [])
                            if isinstance(item, dict)
                        ],
                        started_at=case_wall_started_at,
                    )
                except Exception:
                    target_candidate_public_web_batches = []
                timings_ms["target_public_web_action_diagnostics_fetch"] = round(
                    (time.perf_counter() - diagnostics_started_at) * 1000,
                    2,
                )
        if _company_public_web_action_enabled(normalized_company_public_web_action):
            company_public_web_action_result, company_public_web_action_ms = _run_company_public_web_smoke_action(
                client,
                action=normalized_company_public_web_action,
                case_name=case_name,
                explain=explain,
            )
            timings_ms["company_public_web_action"] = round(company_public_web_action_ms, 2)
            record["company_public_web_action"] = company_public_web_action_result
            company_public_web_runs = [
                dict(item)
                for item in list(dict(company_public_web_action_result.get("list") or {}).get("runs") or [])
                if isinstance(item, dict)
            ]
        if (
            drive_remote_provider_duplicate_events or drive_remote_provider_watcher_first_events
        ) and remote_provider_event_driver_accepted_workers:
            duplicate_started_at = time.perf_counter()
            late_events = _drive_smoke_remote_provider_late_watcher_duplicates(
                client,
                accepted_workers=remote_provider_event_driver_accepted_workers,
                start_sequence=remote_provider_event_sequence,
                duplicate_source=(
                    "provider_webhook" if drive_remote_provider_watcher_first_events else "local_provider_event_watcher"
                ),
            )
            if late_events:
                late_duplicate_phase = (
                    "provider_webhook_late_duplicate"
                    if drive_remote_provider_watcher_first_events
                    else "watcher_late_duplicate"
                )
                remote_provider_event_driver_runs.extend(
                    {"tick": len(progress_samples), "phase": late_duplicate_phase, **dict(item)} for item in late_events
                )
                timeline.append(
                    {
                        "tick": len(progress_samples),
                        "status": str(dict(results.get("job") or {}).get("status") or ""),
                        "stage": str(dict(results.get("job") or {}).get("stage") or ""),
                        "message": "Smoke runner injected late duplicate remote provider events after board probe.",
                        "awaiting_user_action": "",
                        "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                    }
                )
            timings_ms["remote_provider_duplicate_driver"] = round(
                (time.perf_counter() - duplicate_started_at) * 1000,
                2,
            )
        diagnostics_started_at = time.perf_counter()
        try:
            detailed_job_payload = client.get(f"/api/jobs/{job_id}?include_details=1")
            job_events = [
                dict(item) for item in list(detailed_job_payload.get("events") or []) if isinstance(item, dict)
            ]
        except Exception:
            job_events = []
        try:
            worker_payload = client.get(f"/api/jobs/{job_id}/workers")
            agent_workers = [
                dict(item) for item in list(worker_payload.get("agent_workers") or []) if isinstance(item, dict)
            ]
        except Exception:
            agent_workers = []
        try:
            trace_payload = client.get(f"/api/jobs/{job_id}/trace")
            agent_trace_spans = [
                dict(item) for item in list(trace_payload.get("agent_trace_spans") or []) if isinstance(item, dict)
            ]
            if not agent_workers:
                agent_workers = [
                    dict(item) for item in list(trace_payload.get("agent_workers") or []) if isinstance(item, dict)
                ]
        except Exception:
            agent_trace_spans = []
        try:
            materialization_payload = client.get(f"/api/jobs/{job_id}/materialization-items")
            materialization_items = [
                dict(item)
                for item in list(materialization_payload.get("job_materialization_items") or [])
                if isinstance(item, dict)
            ]
            workflow_commands = [
                dict(item)
                for item in list(materialization_payload.get("workflow_commands") or [])
                if isinstance(item, dict)
            ]
            workflow_commands.extend(
                _workflow_commands_from_target_public_web_action(record.get("target_public_web_action"))
            )
            board_visible_patches = [
                dict(item)
                for item in list(materialization_payload.get("job_board_visible_patches") or [])
                if isinstance(item, dict)
            ]
        except Exception:
            materialization_items = []
            workflow_commands = []
            board_visible_patches = []
        try:
            board_patches_payload = client.get(f"/api/jobs/{job_id}/board-patches")
        except Exception:
            board_patches_payload = {}
        if target_public_web_action_batch_id:
            try:
                public_web_payload = client.post(
                    "/api/crm/records/public-web-search/poll",
                    {"batch_id": target_public_web_action_batch_id, "limit": 1000},
                )
                target_candidate_public_web_batches = _public_web_batches_for_smoke_window(
                    [dict(item) for item in list(public_web_payload.get("batches") or []) if isinstance(item, dict)],
                    started_at=case_wall_started_at,
                )
            except Exception:
                target_candidate_public_web_batches = []
        if bool(normalized_expectations.get("require_legacy_public_web_retirement_ready")):
            try:
                legacy_public_web_retirement_audit = client.get(
                    "/api/migrations/legacy-public-web?row_limit=10000&sample_limit=10"
                )
            except Exception as exc:
                legacy_public_web_retirement_audit = {
                    "status": "unavailable",
                    "reason": "legacy_public_web_retirement_audit_fetch_failed",
                    "error": repr(exc),
                }
        if _company_public_web_action_enabled(normalized_company_public_web_action):
            target_company = str(
                normalized_company_public_web_action.get("target_company")
                or dict(record.get("explain") or {}).get("target_company")
                or ""
            ).strip()
            if target_company:
                try:
                    company_public_web_payload = client.get(
                        f"/api/company-assets/public-web?target_company={quote(target_company)}&limit=1000"
                    )
                    company_public_web_runs = [
                        dict(item)
                        for item in list(company_public_web_payload.get("runs") or [])
                        if isinstance(item, dict)
                    ]
                except Exception:
                    company_public_web_runs = []
        timings_ms["event_efficiency_diagnostics_fetch"] = round(
            (time.perf_counter() - diagnostics_started_at) * 1000,
            2,
        )
        try:
            final_progress_snapshot = client.get(f"/api/jobs/{job_id}/progress")
            final_progress_payload = dict(final_progress_snapshot or {})
            progress_samples.append(
                _progress_snapshot_to_observability_sample(
                    final_progress_snapshot,
                    tick=len(progress_samples),
                )
            )
        except Exception:
            pass
        final_terminal_refetch_started_at = time.perf_counter()
        try:
            refreshed_job_payload = client.get(f"/api/jobs/{job_id}")
            refreshed_results = _fetch_smoke_results_payload(
                client,
                job_id=str(job_id or ""),
                job_payload=refreshed_job_payload,
                include_runtime_details=False,
                include_candidates=False,
            )
            refreshed_job_record = dict(refreshed_job_payload.get("job") or refreshed_job_payload or {})
            refreshed_results_job = dict(refreshed_results.get("job") or {})
            refreshed_job_status = (
                str(
                    refreshed_job_record.get("status")
                    or refreshed_results_job.get("status")
                    or final_progress_payload.get("status")
                    or ""
                )
                .strip()
                .lower()
            )
            if refreshed_job_status in TERMINAL_WORKFLOW_STATUSES:
                refreshed_results_job.update(
                    {
                        key: value
                        for key, value in {
                            "job_id": refreshed_job_record.get("job_id") or refreshed_job_record.get("id") or job_id,
                            "status": refreshed_job_record.get("status") or refreshed_results_job.get("status"),
                            "stage": refreshed_job_record.get("stage") or refreshed_results_job.get("stage"),
                            "summary": refreshed_job_record.get("summary") or refreshed_results_job.get("summary"),
                            "progress": refreshed_job_record.get("progress") or refreshed_results_job.get("progress"),
                        }.items()
                        if value is not None
                    }
                )
                refreshed_results = {**dict(refreshed_results or {}), "job": refreshed_results_job}
                job_payload = refreshed_job_payload
                results = refreshed_results
                if (
                    refreshed_job_status == "completed"
                    and start_status != "reused_completed_job"
                    and "post_terminal_recovery" not in record
                ):
                    (
                        post_terminal_job_payload,
                        post_terminal_results,
                        post_terminal_signal_runs,
                        post_terminal_recovery_ms,
                        post_terminal_recovery_state,
                    ) = _settle_post_terminal_worker_recovery(
                        client,
                        job_id=str(job_id or ""),
                        poll_seconds=poll_seconds,
                        max_rounds=max(1, _safe_int(normalized_expectations.get("post_terminal_recovery_rounds")) or 8),
                        drain_background_snapshot_full_materialization=bool(
                            normalized_expectations.get("drain_background_snapshot_full_materialization_post_terminal")
                        ),
                        drain_projection_facet_layering=bool(
                            normalized_expectations.get("drain_projection_facet_layering_post_terminal")
                            or normalized_expectations.get("require_board_runtime_state_cross_endpoint_parity")
                        ),
                    )
                    record["post_terminal_recovery"] = post_terminal_recovery_state
                    timings_ms["post_terminal_worker_recovery"] = round(
                        float(timings_ms.get("post_terminal_worker_recovery") or 0.0)
                        + float(post_terminal_recovery_ms or 0.0),
                        2,
                    )
                    if post_terminal_signal_runs:
                        shared_recovery_signal_runs.extend(post_terminal_signal_runs)
                        post_terminal_tick = (
                            max(
                                [
                                    int(item.get("tick") or 0)
                                    for item in timeline
                                    if isinstance(item, dict) and str(item.get("tick") or "").isdigit()
                                ]
                                or [0]
                            )
                            + 1
                        )
                        timeline.append(
                            {
                                "tick": post_terminal_tick,
                                "status": "completed",
                                "stage": "completed",
                                "message": "Smoke runner settled post-terminal background worker recovery after final refetch.",
                                "awaiting_user_action": "",
                                "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
                            }
                        )
                    if post_terminal_job_payload:
                        job_payload = post_terminal_job_payload
                    if post_terminal_results:
                        results = post_terminal_results
        except Exception as exc:
            record["final_terminal_refetch_error"] = str(exc)
        timings_ms["final_terminal_refetch"] = round(
            (time.perf_counter() - final_terminal_refetch_started_at) * 1000,
            2,
        )
        final_endpoint_refetch_started_at = time.perf_counter()
        try:
            final_progress_snapshot = client.get(f"/api/jobs/{job_id}/progress")
            final_progress_payload = dict(final_progress_snapshot or {})
            progress_samples.append(
                _progress_snapshot_to_observability_sample(
                    final_progress_snapshot,
                    tick=len(progress_samples),
                )
            )
        except Exception:
            pass
        try:
            dashboard_payload = _fetch_smoke_dashboard_payload(
                client,
                job_id=str(job_id or ""),
                job_payload=job_payload,
            )
        except Exception:
            pass
        try:
            candidate_page_payload = _fetch_smoke_candidate_page_payload(
                client,
                job_id=str(job_id or ""),
                path=f"/api/jobs/{job_id}/candidates?offset=0&limit=24&lightweight=1",
                job_payload=job_payload,
            )
        except Exception:
            pass
        try:
            board_patches_payload = client.get(f"/api/jobs/{job_id}/board-patches")
        except Exception:
            pass
        timings_ms["final_public_endpoint_refetch"] = round(
            (time.perf_counter() - final_endpoint_refetch_started_at) * 1000,
            2,
        )
    else:
        timings_ms["dashboard_fetch"] = 0.0
        timings_ms["candidate_page_fetch"] = 0.0
        timings_ms["board_probe_wait"] = 0.0
        timings_ms["event_efficiency_diagnostics_fetch"] = 0.0
    if remote_provider_event_driver_runs:
        record["remote_provider_event_driver"] = {
            "event_count": len(remote_provider_event_driver_runs),
            "recovery_count": sum(_safe_int(item.get("recovery_count")) for item in remote_provider_event_driver_runs),
            "recovery_dispatch_count": sum(
                _safe_int(item.get("recovery_dispatch_count")) for item in remote_provider_event_driver_runs
            ),
            "shared_recovery_signal_count": sum(
                shared_recovery_signal_count(dict(item.get("shared_recovery_signal") or {}))
                for item in remote_provider_event_driver_runs
            ),
            "late_duplicate_count": sum(
                1
                for item in remote_provider_event_driver_runs
                if str(item.get("reason") or "").strip() == "matching_remote_provider_workers_not_recoverable"
            ),
            "events": remote_provider_event_driver_runs,
        }
    smoke_ready, smoke_completion_state = _evaluate_smoke_record_completion(job_payload, results)
    raw_job_status = str(((results.get("job") or {}).get("status")) or "")
    raw_job_stage = str(((results.get("job") or {}).get("stage")) or "")
    final_status_lower = raw_job_status.strip().lower()
    latest_progress_status = (
        str((progress_samples[-1] or {}).get("status") or "").strip().lower() if progress_samples else ""
    )
    if final_status_lower in TERMINAL_WORKFLOW_STATUSES and latest_progress_status != final_status_lower:
        progress_samples.append(
            _build_synthetic_terminal_progress_sample(
                progress_samples=progress_samples,
                job_payload=job_payload,
                raw_job_status=raw_job_status,
                raw_job_stage=raw_job_stage,
            )
        )
    normalized_job_status = raw_job_status
    normalized_job_stage = raw_job_stage
    if smoke_ready and raw_job_status.strip().lower() != "completed":
        normalized_job_status = "completed"
        if not normalized_job_stage or normalized_job_stage.strip().lower() != "completed":
            normalized_job_stage = "completed"
    effective_asset_population_candidate_count = _effective_asset_population_candidate_count(
        results_payload=results,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
    )
    if not dict(results.get("workflow_stage_summaries") or {}):
        # Post legacy-results cutover (410 -> projection route) the results
        # payload no longer carries workflow_stage_summaries; the progress
        # surface owns them. Backfill once here so every downstream report
        # (stage digest, raw summaries, wall-clock) reads one shape.
        progress_stage_summaries = dict(final_progress_payload.get("workflow_stage_summaries") or {})
        if progress_stage_summaries:
            results = {**results, "workflow_stage_summaries": progress_stage_summaries}
    result_asset_population = dict(results.get("asset_population") or {})
    default_results_mode = str((results.get("effective_execution_semantics") or {}).get("default_results_mode") or "")
    asset_population_available = bool(result_asset_population.get("available")) or (
        default_results_mode.strip().lower() == "asset_population" and effective_asset_population_candidate_count > 0
    )
    record["final"] = {
        "job_id": str(job_id or ""),
        "job_status": normalized_job_status,
        "job_stage": normalized_job_stage,
        "raw_job_status": raw_job_status,
        "raw_job_stage": raw_job_stage,
        "results_count": len(results.get("results") or []),
        "manual_review_count": len(results.get("manual_review_items") or []),
        "default_results_mode": ((results.get("effective_execution_semantics") or {}).get("default_results_mode")),
        "asset_population_available": asset_population_available,
        "asset_population_candidate_count": effective_asset_population_candidate_count,
        "stage_summaries": stage_summary_digest(results),
        "background_reconcile": dict(summary_payload.get("background_reconcile") or {}),
        "smoke_ready": smoke_ready,
        "smoke_completion_state": smoke_completion_state,
    }
    record["status"] = normalized_job_status
    record["stage"] = normalized_job_stage
    progress_observability = _build_progress_observability_report(progress_samples)
    record["progress_observability"] = progress_observability
    case_wall_completed_at = datetime.now(timezone.utc)
    provider_invocations = load_scripted_provider_invocations(
        started_at=case_wall_started_at,
        completed_at=case_wall_completed_at,
    )
    if provider_invocations:
        record["provider_invocations"] = provider_invocations
    record["provider_case_report"] = _build_provider_case_report(
        explain_payload=dict(record.get("explain") or {}),
        job_payload=job_payload,
        job_summary=dict(record.get("job_summary") or {}),
        results_payload=results,
        progress_payload=final_progress_payload,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
        board_patches_payload=board_patches_payload,
        timings_ms=timings_ms,
        progress_observability=progress_observability,
        timeline=timeline,
        provider_invocations=provider_invocations,
        job_events=job_events,
        agent_workers=agent_workers,
        agent_trace_spans=agent_trace_spans,
        board_visible_patches=board_visible_patches,
        materialization_items=materialization_items,
        workflow_commands=workflow_commands,
        target_candidate_public_web_batches=target_candidate_public_web_batches,
        legacy_public_web_retirement_audit=legacy_public_web_retirement_audit,
        company_public_web_runs=company_public_web_runs,
        worker_recovery_runs=worker_recovery_runs,
        running_candidate_filter_probe_samples=running_candidate_filter_probe_samples,
    )
    if isinstance(record.get("post_terminal_recovery"), dict):
        record["post_terminal_recovery"] = _synchronize_post_terminal_recovery_with_service_metrics(
            dict(record.get("post_terminal_recovery") or {}),
            service_metrics=dict(dict(record.get("provider_case_report") or {}).get("service_metrics") or {}),
            materialization_items=materialization_items,
        )
    expectation_failures = _evaluate_smoke_expectations(
        record=record,
        expectations=normalized_expectations,
        provider_invocations=provider_invocations,
    )
    if normalized_expectations:
        record["expectations"] = normalized_expectations
        record["expectation_failures"] = expectation_failures
    if expectation_failures:
        final_payload = dict(record.get("final") or {})
        final_payload["smoke_ready"] = False
        final_payload["smoke_completion_state"] = "expectation_failed"
        final_payload["expectation_failures"] = expectation_failures
        record["final"] = final_payload
    record.update(
        _build_case_level_smoke_exports(
            results_payload=results,
            provider_case_report=dict(record.get("provider_case_report") or {}),
        )
    )
    return record


def run_hosted_smoke_matrix(
    client: HostedWorkflowSmokeClient,
    *,
    cases: list[dict[str, Any]],
    reviewer: str,
    poll_seconds: float,
    max_poll_seconds: float,
    auto_continue_stage2: bool = True,
    runtime_tuning_profile: str = "",
) -> tuple[list[dict[str, Any]], list[str]]:
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for item in cases:
        case_name = str(item.get("case") or "").strip()
        payload = dict(item.get("payload") or {})
        expectations = dict(item.get("expectations") or {}) if isinstance(item.get("expectations"), dict) else {}
        review_decision = (
            dict(item.get("review_decision") or {}) if isinstance(item.get("review_decision"), dict) else {}
        )
        target_public_web_action = (
            dict(item.get("target_public_web_action") or {})
            if isinstance(item.get("target_public_web_action"), dict)
            else {}
        )
        company_public_web_action = (
            dict(item.get("company_public_web_action") or {})
            if isinstance(item.get("company_public_web_action"), dict)
            else {}
        )
        coverage_tags = [str(tag).strip() for tag in list(item.get("coverage_tags") or []) if str(tag).strip()]
        scripted_scenario = str(item.get("scripted_scenario") or "").strip()
        if not case_name or not payload:
            continue
        case_max_poll_seconds = _safe_float(item.get("max_poll_seconds"))
        if case_max_poll_seconds <= 0.0:
            case_max_poll_seconds = max_poll_seconds
        try:
            record = run_hosted_smoke_case(
                client=client,
                case_name=case_name,
                payload=payload,
                reviewer=reviewer,
                poll_seconds=poll_seconds,
                max_poll_seconds=case_max_poll_seconds,
                auto_continue_stage2=auto_continue_stage2,
                runtime_tuning_profile=runtime_tuning_profile,
                review_decision=review_decision,
                expectations=expectations,
                target_public_web_action=target_public_web_action,
                company_public_web_action=company_public_web_action,
            )
        except urllib_error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            record = {
                "case": case_name,
                "query": payload.get("raw_user_request"),
                "error": f"HTTPError {exc.code}: {body}",
            }
        except Exception as exc:  # pragma: no cover - operational fallback
            record = {
                "case": case_name,
                "query": payload.get("raw_user_request"),
                "error": repr(exc),
            }
        if coverage_tags:
            record["coverage_tags"] = coverage_tags
        if scripted_scenario:
            record["scripted_scenario"] = scripted_scenario
        summaries.append(record)
        smoke_ready = bool((record.get("final") or {}).get("smoke_ready"))
        if record.get("error") or not smoke_ready:
            failures.append(case_name)
    return summaries, failures


def summarize_smoke_timings(records: list[dict[str, Any]]) -> dict[str, Any]:
    aggregates: dict[str, list[float]] = {key: [] for key in _TIMING_SUMMARY_KEYS}
    provider_stage_aggregates: dict[str, list[float]] = {key: [] for key in _PROVIDER_STAGE_ORDER}
    workflow_wall_clock_aggregates: dict[str, list[float]] = {key: [] for key in _WORKFLOW_WALL_CLOCK_KEYS}
    board_ready_count = 0
    board_ready_nonempty_count = 0
    board_runtime_state_parity_report_count = 0
    board_runtime_state_parity_violation_count = 0
    board_runtime_state_parity_missing_counts: Counter[str] = Counter()
    board_runtime_state_parity_mismatch_counts: Counter[str] = Counter()
    progress_regression_case_count = 0
    progress_stage1_regression_case_count = 0
    progress_lifecycle_regression_case_count = 0
    progress_contract_violation_case_count = 0
    progress_contract_violation_counts: Counter[str] = Counter()
    progress_sample_count = 0
    progress_maxima: dict[str, int] = {}
    progress_payload_byte_values: list[float] = []
    prerequisite_gap_aggregates: dict[str, list[float]] = {}
    materialization_provider_response_counts: list[float] = []
    materialization_pending_delta_counts: list[float] = []
    materialization_first_delta_gaps: list[float] = []
    streaming_materialization_violation_case_count = 0
    streaming_materialization_report_count = 0
    post_preview_finalization_report_count = 0
    post_preview_finalization_complete_ms: list[float] = []
    post_preview_first_finalization_start_ms: list[float] = []
    post_preview_first_materialize_start_ms: list[float] = []
    post_preview_last_materialize_completed_ms: list[float] = []
    post_preview_materialize_completed_counts: list[float] = []
    post_preview_materialize_sync_duration_ms: list[float] = []
    post_preview_profile_batch_local_apply_duration_ms: list[float] = []
    post_preview_candidate_source_closure_lifecycle_ms: list[float] = []
    post_preview_materialize_sync_scope_counts: Counter[str] = Counter()
    post_preview_long_finalization_case_count = 0
    workflow_benchmark_report_count = 0
    workflow_benchmark_aggregates: dict[str, list[float]] = {
        "search_returned_count": [],
        "roster_returned_count": [],
        "fetched_profile_count": [],
        "profile_url_total_count": [],
        "profile_url_queued_count": [],
        "board_total_candidates": [],
        "board_first_page_returned_count": [],
        "job_to_board_nonempty_ms": [],
    }
    provider_backpressure_report_count = 0
    provider_backpressure_case_count = 0
    provider_limiter_exhausted_case_count = 0
    provider_limiter_observed_case_count = 0
    provider_limiter_active_counts: list[float] = []
    provider_limiter_wait_ms: list[float] = []
    remote_actor_slot_observation_report_count = 0
    remote_actor_slot_budget_values: list[float] = []
    remote_actor_slot_peak_values: list[float] = []
    remote_actor_slot_occupancy_values: list[float] = []
    event_level_efficiency_reports: list[dict[str, Any]] = []
    service_metrics_report_count = 0
    service_worker_counts: list[float] = []
    service_trace_span_counts: list[float] = []
    service_worker_duration_ms: list[float] = []
    service_global_handoff_gap_ms: list[float] = []
    service_slow_worker_counts: list[float] = []
    service_slow_gap_counts: list[float] = []
    service_out_of_order_profile_completion_values: list[float] = []
    service_local_apply_backlog_report_count = 0
    service_local_apply_backlog_values: list[float] = []
    service_local_apply_backlog_stale_values: list[float] = []
    service_local_apply_backlog_age_ms: list[float] = []
    service_local_apply_closure_backlog_values: list[float] = []
    service_local_apply_closure_retryable_values: list[float] = []
    service_local_apply_closure_ready_retry_values: list[float] = []
    service_local_apply_closure_stale_running_values: list[float] = []
    service_local_apply_backlog_stale_case_count = 0
    service_local_apply_closure_retry_backlog_case_count = 0
    service_local_apply_closure_stale_running_case_count = 0
    service_final_results_to_board_ready_ms: list[float] = []
    service_final_results_to_board_nonempty_ms: list[float] = []
    service_job_to_board_visible_partial_ms: list[float] = []
    service_job_to_board_nonempty_ms: list[float] = []
    service_stage_1_preview_to_final_results_ms: list[float] = []
    service_loading_feedback_required_count = 0
    service_board_readiness_violation_count = 0
    service_long_finalization_count = 0
    service_board_visible_projection_report_count = 0
    service_board_visible_projection_missing_count = 0
    service_board_visible_patch_log_missing_count = 0
    service_board_visible_patch_log_lag_count = 0
    service_board_visible_materialization_lag_count = 0
    service_board_visible_count_values: list[float] = []
    service_board_visible_patch_count_values: list[float] = []
    service_board_visible_fetched_lag_values: list[float] = []
    service_board_visible_patch_lag_values: list[float] = []
    service_board_visible_consumable_patch_values: list[float] = []
    service_board_visible_consumable_progression_values: list[float] = []
    service_board_visible_pure_shell_patch_values: list[float] = []
    service_post_profile_report_count = 0
    service_post_profile_terminal_leak_values: list[float] = []
    service_post_profile_callback_elapsed_values: list[float] = []
    service_post_profile_file_to_patch_values: list[float] = []
    service_post_profile_all_profiles_to_cards_values: list[float] = []
    service_post_profile_violation_case_count = 0
    service_recovery_phase_report_count = 0
    service_recovery_phase_missing_values: list[float] = []
    service_recovery_phase_failed_values: list[float] = []
    service_recovery_phase_slow_values: list[float] = []
    service_recovery_phase_unexpected_values: list[float] = []
    service_recovery_tick_budget_exhausted_values: list[float] = []
    service_recovery_budget_yield_attention_values: list[float] = []
    service_recovery_cooperative_budget_yield_values: list[float] = []
    service_recovery_phase_elapsed_values: list[float] = []
    service_recovery_phase_total_elapsed_values: list[float] = []
    service_recovery_phase_candidate_values: list[float] = []
    service_recovery_local_apply_candidate_per_second_values: list[float] = []
    service_recovery_phase_violation_case_count = 0
    service_snapshot_full_materialization_queue_report_count = 0
    service_snapshot_full_materialization_queue_backlog_values: list[float] = []
    service_snapshot_full_materialization_queue_retryable_values: list[float] = []
    service_snapshot_full_materialization_queue_stale_running_values: list[float] = []
    service_snapshot_full_materialization_queue_retry_backlog_case_count = 0
    service_snapshot_full_materialization_queue_stale_running_case_count = 0
    service_search_seed_discovery_queue_report_count = 0
    service_search_seed_discovery_queue_item_values: list[float] = []
    service_search_seed_discovery_queue_provider_owned_values: list[float] = []
    service_search_seed_discovery_queue_retry_wait_values: list[float] = []
    service_search_seed_discovery_queue_ready_retry_values: list[float] = []
    service_search_seed_discovery_queue_exhausted_values: list[float] = []
    service_search_seed_discovery_queue_stale_provider_values: list[float] = []
    service_search_seed_discovery_queue_owner_missing_values: list[float] = []
    service_search_seed_discovery_worker_without_item_values: list[float] = []
    service_search_seed_discovery_worker_without_local_apply_values: list[float] = []
    service_search_seed_discovery_queue_exhausted_without_report_values: list[float] = []
    service_search_seed_discovery_queue_retry_backlog_case_count = 0
    service_search_seed_discovery_queue_stale_provider_case_count = 0
    service_search_seed_discovery_queue_owner_missing_case_count = 0
    service_search_seed_discovery_worker_without_item_case_count = 0
    service_search_seed_discovery_worker_without_local_apply_case_count = 0
    service_search_seed_discovery_queue_exhausted_without_report_case_count = 0
    service_provider_search_retry_queue_report_count = 0
    service_provider_search_retry_queue_backlog_values: list[float] = []
    service_provider_search_retry_queue_retryable_values: list[float] = []
    service_provider_search_retry_queue_terminal_failed_values: list[float] = []
    service_provider_search_retry_queue_stale_running_values: list[float] = []
    service_provider_search_retry_queue_retry_backlog_case_count = 0
    service_provider_search_retry_queue_terminal_failure_case_count = 0
    service_provider_search_retry_queue_stale_running_case_count = 0
    service_remote_provider_event_report_count = 0
    service_remote_provider_event_count_values: list[float] = []
    service_remote_provider_event_late_duplicate_values: list[float] = []
    service_remote_provider_event_in_flight_duplicate_values: list[float] = []
    service_remote_provider_event_lag_values: list[float] = []
    service_remote_provider_event_actionable_lag_values: list[float] = []
    service_remote_provider_event_late_duplicate_lag_values: list[float] = []
    service_remote_provider_event_slow_lag_values: list[float] = []
    service_remote_provider_event_late_duplicate_slow_lag_values: list[float] = []
    service_remote_provider_event_lag_violation_case_count = 0
    service_remote_provider_event_source_counts: Counter[str] = Counter()
    service_remote_provider_event_status_counts: Counter[str] = Counter()
    service_provider_anomaly_report_count = 0
    service_provider_anomaly_count_values: list[float] = []
    service_provider_zero_retry_values: list[float] = []
    service_provider_zero_exhausted_values: list[float] = []
    service_provider_zero_accepted_values: list[float] = []
    service_provider_empty_scale_values: list[float] = []
    service_provider_probe_total_drift_values: list[float] = []
    service_provider_empty_page_range_values: list[float] = []
    service_provider_single_page_retry_values: list[float] = []
    service_target_public_web_report_count = 0
    service_target_public_web_batch_values: list[float] = []
    service_target_public_web_run_values: list[float] = []
    service_target_public_web_metric_run_values: list[float] = []
    service_target_public_web_remote_pending_values: list[float] = []
    service_target_public_web_partial_failure_values: list[float] = []
    service_target_public_web_unmaterialized_signal_gap_values: list[float] = []
    service_target_public_web_completed_without_materialized_values: list[float] = []
    service_target_public_web_missing_metric_values: list[float] = []
    service_target_public_web_provider_failure_values: list[float] = []
    service_target_public_web_local_error_values: list[float] = []
    service_target_public_web_crm_owner_values: list[float] = []
    service_target_public_web_legacy_owner_values: list[float] = []
    service_target_public_web_execution_bridge_values: list[float] = []
    service_target_public_web_queue_command_values: list[float] = []
    service_target_public_web_queue_command_succeeded_values: list[float] = []
    service_target_public_web_queue_command_pending_values: list[float] = []
    service_target_public_web_queue_command_failed_values: list[float] = []
    service_target_public_web_queue_command_invalid_owner_values: list[float] = []
    service_target_public_web_queue_command_incomplete_causality_values: list[float] = []
    service_target_public_web_queue_command_missing_case_count = 0
    service_target_public_web_queue_command_contract_missing_case_count = 0
    service_target_public_web_guardrail_violation_case_count = 0
    service_target_public_web_storage_owner_counts: Counter[str] = Counter()
    service_target_public_web_execution_backend_counts: Counter[str] = Counter()
    service_target_public_web_queue_command_status_counts: Counter[str] = Counter()
    service_target_public_web_queue_command_owner_counts: Counter[str] = Counter()
    service_target_public_web_risk_reason_counts: Counter[str] = Counter()
    service_target_public_web_slowest_phase_counts: Counter[str] = Counter()
    service_target_public_web_phase_duration_values: dict[str, list[float]] = defaultdict(list)
    service_serving_publication_gap_report_count = 0
    service_serving_publication_gap_present_count = 0
    service_serving_publication_gap_stale_count = 0
    service_serving_publication_gap_age_values: list[float] = []
    service_board_overlay_write_report_count = 0
    service_board_overlay_write_full_rebuild_values: list[float] = []
    service_board_overlay_write_incremental_values: list[float] = []
    service_board_overlay_write_fast_path_eligible_values: list[float] = []
    service_board_overlay_write_fast_path_used_values: list[float] = []
    service_board_overlay_write_eligible_fallback_values: list[float] = []
    service_board_overlay_write_eligible_fallback_case_count = 0
    service_finalization_overlay_report_count = 0
    service_finalization_overlay_full_rewrite_values: list[float] = []
    service_finalization_overlay_reuse_values: list[float] = []
    service_finalization_overlay_eligible_full_rewrite_values: list[float] = []
    service_finalization_overlay_eligible_full_rewrite_case_count = 0
    service_legacy_materialization_write_contract_report_count = 0
    service_legacy_materialization_item_values: list[float] = []
    service_legacy_materialization_normal_write_values: list[float] = []
    service_legacy_materialization_migration_adapter_values: list[float] = []
    service_legacy_materialization_missing_contract_values: list[float] = []
    service_legacy_materialization_normal_write_case_count = 0
    service_legacy_materialization_missing_contract_case_count = 0
    service_legacy_materialization_item_kind_counts: Counter[str] = Counter()
    service_legacy_materialization_normal_kind_counts: Counter[str] = Counter()
    service_legacy_materialization_migration_kind_counts: Counter[str] = Counter()
    service_legacy_materialization_missing_kind_counts: Counter[str] = Counter()
    service_bottleneck_case_count = 0
    service_bottleneck_total_count = 0
    service_bottleneck_kind_counts: Counter[str] = Counter()
    service_bottleneck_severity_counts: Counter[str] = Counter()
    duplicate_provider_dispatch_case_count = 0
    duplicate_provider_dispatch_signature_count = 0
    redundant_provider_dispatch_count = 0
    disabled_stage_violation_case_count = 0
    unexpected_public_web_stage_case_count = 0
    prerequisite_gap_case_count = 0
    final_results_board_violation_case_count = 0
    delayed_board_after_final_case_count = 0
    acquisition_mode_rollups: dict[str, dict[str, Any]] = {}
    dispatch_strategy_rollups: dict[str, dict[str, Any]] = {}
    for record in records:
        timings = dict(record.get("timings_ms") or {})
        for key in _TIMING_SUMMARY_KEYS:
            value = timings.get(key)
            if isinstance(value, (int, float)):
                aggregates[key].append(float(value))
        provider_case_report = dict(record.get("provider_case_report") or {})
        workflow_wall_clock_ms = dict(provider_case_report.get("workflow_wall_clock_ms") or {})
        for metric_name in _WORKFLOW_WALL_CLOCK_KEYS:
            value = workflow_wall_clock_ms.get(metric_name)
            if isinstance(value, (int, float)) and float(value) >= 0.0:
                workflow_wall_clock_aggregates[metric_name].append(float(value))
        stage_wall_clock_ms = dict(provider_case_report.get("stage_wall_clock_ms") or {})
        for stage_name in _PROVIDER_STAGE_ORDER:
            value = stage_wall_clock_ms.get(stage_name)
            if isinstance(value, (int, float)) and float(value) > 0.0:
                provider_stage_aggregates[stage_name].append(float(value))
        execution = dict(provider_case_report.get("execution") or {})
        for rollups, key in (
            (acquisition_mode_rollups, str(execution.get("effective_acquisition_mode") or "").strip()),
            (dispatch_strategy_rollups, str(execution.get("dispatch_strategy") or "").strip()),
        ):
            if not key:
                continue
            bucket = rollups.setdefault(
                key,
                {
                    "case_count": 0,
                    "total": [],
                    "wait_for_completion": [],
                    "board_probe_wait": [],
                    "job_to_stage_1_preview": [],
                    "job_to_final_results": [],
                    "job_to_board_nonempty": [],
                },
            )
            bucket["case_count"] = int(bucket.get("case_count") or 0) + 1
            for metric_name in ("total", "wait_for_completion", "board_probe_wait"):
                value = timings.get(metric_name)
                if isinstance(value, (int, float)):
                    cast_values = list(bucket.get(metric_name) or [])
                    cast_values.append(float(value))
                    bucket[metric_name] = cast_values
            for metric_name in ("job_to_stage_1_preview", "job_to_final_results", "job_to_board_nonempty"):
                value = workflow_wall_clock_ms.get(metric_name)
                if isinstance(value, (int, float)):
                    cast_values = list(bucket.get(metric_name) or [])
                    cast_values.append(float(value))
                    bucket[metric_name] = cast_values
        board = dict(provider_case_report.get("board") or {})
        if bool(board.get("ready")):
            board_ready_count += 1
        if bool(board.get("ready_nonempty")):
            board_ready_nonempty_count += 1
        board_runtime_state_parity = dict(provider_case_report.get("board_runtime_state_parity") or {})
        if bool(board_runtime_state_parity.get("report_available")):
            board_runtime_state_parity_report_count += 1
            if not bool(board_runtime_state_parity.get("consistent")):
                board_runtime_state_parity_violation_count += 1
            board_runtime_state_parity_missing_counts.update(
                str(item or "").strip()
                for item in list(board_runtime_state_parity.get("missing_sources") or [])
                if str(item or "").strip()
            )
            board_runtime_state_parity_mismatch_counts.update(
                str(item or "").strip()
                for item in list(board_runtime_state_parity.get("mismatched_fields") or [])
                if str(item or "").strip()
            )
        progress_observability = dict(provider_case_report.get("progress_observability") or {})
        if bool(progress_observability.get("regression_detected")):
            progress_regression_case_count += 1
        if bool(progress_observability.get("stage1_regression_detected")):
            progress_stage1_regression_case_count += 1
        if bool(progress_observability.get("result_view_lifecycle_regression_detected")):
            progress_lifecycle_regression_case_count += 1
        if bool(progress_observability.get("contract_violation_detected")):
            progress_contract_violation_case_count += 1
        progress_contract_violation_counts.update(
            {
                str(key): _safe_int(value)
                for key, value in dict(progress_observability.get("contract_violation_counts") or {}).items()
                if str(key).strip()
            }
        )
        progress_sample_count += _safe_int(progress_observability.get("sample_count"))
        _extend_summary_values(progress_payload_byte_values, dict(progress_observability.get("payload_bytes") or {}))
        for key, value in dict(progress_observability.get("maxima") or {}).items():
            progress_maxima[str(key)] = max(int(progress_maxima.get(str(key)) or 0), _safe_int(value))
        materialization_streaming = dict(provider_case_report.get("materialization_streaming") or {})
        if bool(materialization_streaming.get("report_available")):
            streaming_materialization_report_count += 1
        for values, key in (
            (materialization_provider_response_counts, "provider_response_count"),
            (materialization_pending_delta_counts, "pending_delta_count"),
            (materialization_first_delta_gaps, "provider_response_to_first_materialization_ms"),
        ):
            value = materialization_streaming.get(key)
            if isinstance(value, (int, float)) and float(value) > 0.0:
                values.append(float(value))
        post_preview_finalization = dict(provider_case_report.get("post_preview_finalization") or {})
        if bool(post_preview_finalization.get("report_available")):
            post_preview_finalization_report_count += 1
        for values, key in (
            (post_preview_finalization_complete_ms, "preview_to_finalization_completed_ms"),
            (post_preview_first_finalization_start_ms, "preview_to_first_finalization_start_ms"),
            (post_preview_first_materialize_start_ms, "preview_to_first_materialize_start_ms"),
            (post_preview_last_materialize_completed_ms, "preview_to_last_materialize_completed_ms"),
        ):
            value = post_preview_finalization.get(key)
            if isinstance(value, (int, float)) and float(value) > 0.0:
                values.append(float(value))
        materialize_completed_count = post_preview_finalization.get("materialize_completed_count")
        if isinstance(materialize_completed_count, (int, float)):
            post_preview_materialize_completed_counts.append(float(materialize_completed_count))
        materialize_sync_values_added = False
        for item in list(post_preview_finalization.get("materialize_syncs") or []):
            if not isinstance(item, dict):
                continue
            duration_ms = item.get("duration_ms")
            if isinstance(duration_ms, (int, float)) and float(duration_ms) >= 0.0:
                post_preview_materialize_sync_duration_ms.append(float(duration_ms))
                materialize_sync_values_added = True
        if not materialize_sync_values_added:
            _extend_summary_values(
                post_preview_materialize_sync_duration_ms,
                dict(post_preview_finalization.get("materialize_sync_duration_ms") or {}),
            )
        _extend_summary_values(
            post_preview_profile_batch_local_apply_duration_ms,
            dict(post_preview_finalization.get("profile_batch_local_apply_duration_ms") or {}),
        )
        _extend_summary_values(
            post_preview_candidate_source_closure_lifecycle_ms,
            dict(post_preview_finalization.get("candidate_source_closure_lifecycle_ms") or {}),
        )
        post_preview_materialize_sync_scope_counts.update(
            {
                str(key): _safe_int(value)
                for key, value in dict(post_preview_finalization.get("materialize_sync_scope_counts") or {}).items()
                if str(key).strip()
            }
        )
        if bool(post_preview_finalization.get("long_post_preview_finalization")):
            post_preview_long_finalization_case_count += 1
        behavior_guardrails = dict(provider_case_report.get("behavior_guardrails") or {})
        duplicate_provider_dispatch = dict(behavior_guardrails.get("duplicate_provider_dispatch") or {})
        if bool(duplicate_provider_dispatch.get("violation_detected")):
            duplicate_provider_dispatch_case_count += 1
        duplicate_provider_dispatch_signature_count += _safe_int(
            duplicate_provider_dispatch.get("duplicate_signature_count")
        )
        redundant_provider_dispatch_count += _safe_int(duplicate_provider_dispatch.get("redundant_dispatch_count"))
        disabled_stage_violations = dict(behavior_guardrails.get("disabled_stage_violations") or {})
        if bool(disabled_stage_violations.get("violation_detected")):
            disabled_stage_violation_case_count += 1
        if bool(disabled_stage_violations.get("unexpected_public_web_stage")):
            unexpected_public_web_stage_case_count += 1
        prerequisite_gaps = dict(behavior_guardrails.get("prerequisite_gaps") or {})
        if bool(prerequisite_gaps.get("violation_detected")):
            prerequisite_gap_case_count += 1
        for key, value in dict(prerequisite_gaps.get("metrics_ms") or {}).items():
            if isinstance(value, (int, float)):
                prerequisite_gap_aggregates.setdefault(str(key), []).append(float(value))
        final_results_board_consistency = dict(behavior_guardrails.get("final_results_board_consistency") or {})
        if bool(final_results_board_consistency.get("violation_detected")):
            final_results_board_violation_case_count += 1
        if bool(final_results_board_consistency.get("delayed_board_after_final")):
            delayed_board_after_final_case_count += 1
        streaming_materialization = dict(behavior_guardrails.get("streaming_materialization") or {})
        if bool(streaming_materialization.get("violation_detected")):
            streaming_materialization_violation_case_count += 1
        workflow_benchmark = dict(provider_case_report.get("workflow_benchmark") or {})
        if workflow_benchmark:
            workflow_benchmark_report_count += 1
            for metric_name, values in workflow_benchmark_aggregates.items():
                value = workflow_benchmark.get(metric_name)
                if isinstance(value, (int, float)):
                    values.append(float(value))
        provider_backpressure = dict(provider_case_report.get("provider_backpressure") or {})
        if provider_backpressure:
            provider_backpressure_report_count += 1
            if bool(provider_backpressure.get("backpressure_detected")):
                provider_backpressure_case_count += 1
            if bool(provider_backpressure.get("limiter_exhausted")):
                provider_limiter_exhausted_case_count += 1
            if _safe_int(provider_backpressure.get("observed_limiter_count")) > 0:
                provider_limiter_observed_case_count += 1
            provider_limiter_active_counts.append(
                float(_safe_int(provider_backpressure.get("observed_active_limiter_count")))
            )
            provider_limiter_wait_ms.append(_safe_float(provider_backpressure.get("max_provider_limiter_wait_ms")))
        remote_actor_slot_observation = dict(provider_case_report.get("remote_actor_slot_observation") or {})
        if bool(remote_actor_slot_observation.get("report_available")):
            remote_actor_slot_observation_report_count += 1
            remote_actor_slot_budget_values.append(
                float(_safe_int(remote_actor_slot_observation.get("harvest_profile_actor_global_inflight")))
            )
            remote_actor_slot_peak_values.append(
                float(_safe_int(remote_actor_slot_observation.get("peak_waiting_remote_harvest_count")))
            )
            remote_actor_slot_occupancy_values.append(
                _safe_float(remote_actor_slot_observation.get("remote_actor_slot_peak_occupancy_ratio"))
            )
        event_level_efficiency = dict(provider_case_report.get("event_level_efficiency") or {})
        if bool(event_level_efficiency.get("report_available")):
            event_level_efficiency_reports.append(event_level_efficiency)
        service_metrics = dict(provider_case_report.get("service_metrics") or {})
        if bool(service_metrics.get("report_available")):
            service_metrics_report_count += 1
            worker_timeline = dict(service_metrics.get("worker_timeline") or {})
            service_worker_counts.append(float(_safe_int(worker_timeline.get("worker_count"))))
            service_trace_span_counts.append(float(_safe_int(worker_timeline.get("trace_span_count"))))
            _extend_summary_values(service_worker_duration_ms, dict(worker_timeline.get("duration_ms") or {}))
            handoff_gap = dict(worker_timeline.get("handoff_gap_ms") or {})
            _extend_summary_values(
                service_global_handoff_gap_ms,
                dict(handoff_gap.get("global_next_worker_start_gap_ms") or {}),
            )
            service_slow_worker_counts.append(float(len(list(worker_timeline.get("slow_workers") or []))))
            service_slow_gap_counts.append(float(len(list(handoff_gap.get("slow_gaps") or []))))
            out_of_order_completion = dict(worker_timeline.get("out_of_order_completion") or {})
            service_out_of_order_profile_completion_values.append(
                float(_safe_int(out_of_order_completion.get("profile_batch_inversion_count")))
            )
            local_apply_backlog = dict(service_metrics.get("local_apply_backlog") or {})
            if bool(local_apply_backlog.get("report_available")):
                service_local_apply_backlog_report_count += 1
                for values, key in (
                    (service_local_apply_backlog_values, "applied_not_ingested_count"),
                    (service_local_apply_backlog_stale_values, "stale_applied_not_ingested_count"),
                    (service_local_apply_closure_backlog_values, "closure_backlog_count"),
                    (service_local_apply_closure_retryable_values, "closure_retryable_count"),
                    (service_local_apply_closure_ready_retry_values, "closure_ready_retry_count"),
                    (service_local_apply_closure_stale_running_values, "closure_stale_running_count"),
                ):
                    value = local_apply_backlog.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                _extend_summary_values(
                    service_local_apply_backlog_age_ms, dict(local_apply_backlog.get("age_ms") or {})
                )
                if bool(local_apply_backlog.get("stale_applied_not_ingested_present")):
                    service_local_apply_backlog_stale_case_count += 1
                if bool(local_apply_backlog.get("closure_retry_backlog_present")):
                    service_local_apply_closure_retry_backlog_case_count += 1
                if bool(local_apply_backlog.get("closure_stale_running_present")):
                    service_local_apply_closure_stale_running_case_count += 1
            user_experience = dict(service_metrics.get("user_experience") or {})
            for values, key in (
                (service_final_results_to_board_ready_ms, "final_results_to_board_ready_ms"),
                (service_final_results_to_board_nonempty_ms, "final_results_to_board_nonempty_ms"),
                (service_job_to_board_visible_partial_ms, "job_to_board_visible_partial_ms"),
                (service_job_to_board_nonempty_ms, "job_to_board_nonempty_ms"),
                (service_stage_1_preview_to_final_results_ms, "stage_1_preview_to_final_results_ms"),
            ):
                value = user_experience.get(key)
                if isinstance(value, (int, float)) and float(value) >= 0.0:
                    values.append(float(value))
            if bool(user_experience.get("loading_feedback_required")):
                service_loading_feedback_required_count += 1
            if bool(user_experience.get("board_readiness_violation")):
                service_board_readiness_violation_count += 1
            if bool(user_experience.get("long_finalization_after_preview")):
                service_long_finalization_count += 1
            board_visible_projection = dict(service_metrics.get("board_visible_projection") or {})
            if bool(board_visible_projection.get("report_available")):
                service_board_visible_projection_report_count += 1
                for values, key in (
                    (service_board_visible_count_values, "delta_profile_board_visible_count"),
                    (service_board_visible_patch_count_values, "patch_log_count"),
                    (service_board_visible_fetched_lag_values, "fetched_to_board_visible_lag_count"),
                    (service_board_visible_patch_lag_values, "patch_log_lag_count"),
                    (service_board_visible_consumable_patch_values, "patch_consumable_card_nonzero_count"),
                    (service_board_visible_consumable_progression_values, "patch_consumable_card_distinct_count"),
                    (service_board_visible_pure_shell_patch_values, "pure_shell_patch_count"),
                ):
                    value = board_visible_projection.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(board_visible_projection.get("projection_missing_for_visible_count")):
                    service_board_visible_projection_missing_count += 1
                if bool(board_visible_projection.get("patch_log_missing_for_visible_count")):
                    service_board_visible_patch_log_missing_count += 1
                if bool(board_visible_projection.get("patch_log_replay_lag")):
                    service_board_visible_patch_log_lag_count += 1
                if bool(board_visible_projection.get("materialization_lag_violation")):
                    service_board_visible_materialization_lag_count += 1
            post_profile_completion = dict(service_metrics.get("post_profile_completion") or {})
            if bool(post_profile_completion.get("report_available")):
                service_post_profile_report_count += 1
                terminal_state = dict(post_profile_completion.get("url_terminal_state_recording") or {})
                terminal_leak_count = terminal_state.get("terminal_queue_state_leak_count")
                if isinstance(terminal_leak_count, (int, float)) and float(terminal_leak_count) >= 0.0:
                    service_post_profile_terminal_leak_values.append(float(terminal_leak_count))
                _extend_summary_values(
                    service_post_profile_callback_elapsed_values,
                    dict(dict(post_profile_completion.get("event_level_callback") or {}).get("elapsed_ms") or {}),
                )
                _extend_summary_values(
                    service_post_profile_file_to_patch_values,
                    dict(
                        dict(post_profile_completion.get("profile_file_visible_to_board_patch_visible") or {}).get(
                            "elapsed_ms"
                        )
                        or {}
                    ),
                )
                _extend_summary_values(
                    service_post_profile_all_profiles_to_cards_values,
                    dict(
                        dict(post_profile_completion.get("all_profiles_fetched_to_all_cards_visible") or {}).get(
                            "elapsed_ms"
                        )
                        or {}
                    ),
                )
                if bool(post_profile_completion.get("slo_violation_detected")):
                    service_post_profile_violation_case_count += 1
            recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
            if bool(recovery_phase_metrics.get("report_available")):
                service_recovery_phase_report_count += 1
                for values, key in (
                    (service_recovery_phase_missing_values, "missing_phase_count"),
                    (service_recovery_phase_failed_values, "failed_phase_count"),
                    (service_recovery_phase_slow_values, "slow_phase_count"),
                    (service_recovery_phase_unexpected_values, "unexpected_enabled_phase_count"),
                    (service_recovery_tick_budget_exhausted_values, "recovery_tick_budget_exhausted_count"),
                    (service_recovery_cooperative_budget_yield_values, "cooperative_budget_yield_count"),
                ):
                    value = recovery_phase_metrics.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                budget_yield_attention_count = max(
                    0,
                    _safe_int(recovery_phase_metrics.get("recovery_tick_budget_exhausted_count"))
                    - _safe_int(recovery_phase_metrics.get("cooperative_budget_yield_count")),
                )
                service_recovery_budget_yield_attention_values.append(float(budget_yield_attention_count))
                _extend_summary_values(
                    service_recovery_phase_elapsed_values,
                    dict(recovery_phase_metrics.get("elapsed_ms") or {}),
                )
                _extend_summary_values(
                    service_recovery_phase_total_elapsed_values,
                    dict(recovery_phase_metrics.get("total_elapsed_ms") or {}),
                )
                _extend_summary_values(
                    service_recovery_phase_candidate_values,
                    dict(recovery_phase_metrics.get("phase_candidate_count") or {}),
                )
                _extend_summary_values(
                    service_recovery_local_apply_candidate_per_second_values,
                    dict(recovery_phase_metrics.get("local_apply_candidate_per_second") or {}),
                )
                if bool(recovery_phase_metrics.get("slo_violation_detected")):
                    service_recovery_phase_violation_case_count += 1
            snapshot_full_materialization_queue = dict(service_metrics.get("snapshot_full_materialization_queue") or {})
            if bool(snapshot_full_materialization_queue.get("report_available")):
                service_snapshot_full_materialization_queue_report_count += 1
                for values, key in (
                    (service_snapshot_full_materialization_queue_backlog_values, "backlog_count"),
                    (service_snapshot_full_materialization_queue_retryable_values, "retryable_count"),
                    (service_snapshot_full_materialization_queue_stale_running_values, "stale_running_count"),
                ):
                    value = snapshot_full_materialization_queue.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(snapshot_full_materialization_queue.get("retry_backlog_present")):
                    service_snapshot_full_materialization_queue_retry_backlog_case_count += 1
                if bool(snapshot_full_materialization_queue.get("stale_running_present")):
                    service_snapshot_full_materialization_queue_stale_running_case_count += 1
            search_seed_discovery_queue = dict(service_metrics.get("search_seed_discovery_queue") or {})
            if bool(search_seed_discovery_queue.get("report_available")):
                service_search_seed_discovery_queue_report_count += 1
                for values, key in (
                    (service_search_seed_discovery_queue_item_values, "item_count"),
                    (service_search_seed_discovery_queue_provider_owned_values, "provider_owned_count"),
                    (service_search_seed_discovery_queue_retry_wait_values, "retry_wait_count"),
                    (service_search_seed_discovery_queue_ready_retry_values, "ready_retry_count"),
                    (service_search_seed_discovery_queue_exhausted_values, "exhausted_count"),
                    (service_search_seed_discovery_queue_stale_provider_values, "stale_provider_owned_count"),
                    (service_search_seed_discovery_queue_owner_missing_values, "item_without_worker_owner_count"),
                    (
                        service_search_seed_discovery_worker_without_item_values,
                        "discovery_worker_without_item_count",
                    ),
                    (
                        service_search_seed_discovery_worker_without_local_apply_values,
                        "discovery_worker_without_local_apply_count",
                    ),
                    (
                        service_search_seed_discovery_queue_exhausted_without_report_values,
                        "exhausted_without_provider_retry_count",
                    ),
                ):
                    value = search_seed_discovery_queue.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(search_seed_discovery_queue.get("retry_backlog_present")):
                    service_search_seed_discovery_queue_retry_backlog_case_count += 1
                if bool(search_seed_discovery_queue.get("stale_provider_owned_present")):
                    service_search_seed_discovery_queue_stale_provider_case_count += 1
                if bool(search_seed_discovery_queue.get("item_without_worker_owner_present")):
                    service_search_seed_discovery_queue_owner_missing_case_count += 1
                if bool(search_seed_discovery_queue.get("discovery_worker_without_item_present")):
                    service_search_seed_discovery_worker_without_item_case_count += 1
                if bool(search_seed_discovery_queue.get("discovery_worker_without_local_apply_present")):
                    service_search_seed_discovery_worker_without_local_apply_case_count += 1
                if bool(search_seed_discovery_queue.get("exhausted_without_provider_retry_present")):
                    service_search_seed_discovery_queue_exhausted_without_report_case_count += 1
            provider_search_retry_queue = dict(service_metrics.get("provider_search_retry_queue") or {})
            if bool(provider_search_retry_queue.get("report_available")):
                service_provider_search_retry_queue_report_count += 1
                for values, key in (
                    (service_provider_search_retry_queue_backlog_values, "backlog_count"),
                    (service_provider_search_retry_queue_retryable_values, "retryable_count"),
                    (service_provider_search_retry_queue_terminal_failed_values, "terminal_failed_count"),
                    (service_provider_search_retry_queue_stale_running_values, "stale_running_count"),
                ):
                    value = provider_search_retry_queue.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(provider_search_retry_queue.get("retry_backlog_present")):
                    service_provider_search_retry_queue_retry_backlog_case_count += 1
                if bool(provider_search_retry_queue.get("terminal_failure_present")):
                    service_provider_search_retry_queue_terminal_failure_case_count += 1
                if bool(provider_search_retry_queue.get("stale_running_present")):
                    service_provider_search_retry_queue_stale_running_case_count += 1
            remote_provider_events = dict(service_metrics.get("remote_provider_events") or {})
            if bool(remote_provider_events.get("report_available")):
                service_remote_provider_event_report_count += 1
                for values, key in (
                    (service_remote_provider_event_count_values, "event_count"),
                    (service_remote_provider_event_late_duplicate_values, "late_duplicate_count"),
                    (service_remote_provider_event_in_flight_duplicate_values, "in_flight_duplicate_count"),
                    (service_remote_provider_event_slow_lag_values, "slow_lag_count"),
                    (
                        service_remote_provider_event_late_duplicate_slow_lag_values,
                        "late_duplicate_slow_lag_count",
                    ),
                ):
                    value = remote_provider_events.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                _extend_summary_values(
                    service_remote_provider_event_lag_values,
                    dict(remote_provider_events.get("remote_to_local_event_lag_ms") or {}),
                )
                _extend_summary_values(
                    service_remote_provider_event_actionable_lag_values,
                    dict(remote_provider_events.get("actionable_remote_to_local_event_lag_ms") or {}),
                )
                _extend_summary_values(
                    service_remote_provider_event_late_duplicate_lag_values,
                    dict(remote_provider_events.get("late_duplicate_remote_to_local_event_lag_ms") or {}),
                )
                service_remote_provider_event_source_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(remote_provider_events.get("source_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                service_remote_provider_event_status_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(remote_provider_events.get("status_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                if bool(remote_provider_events.get("lag_violation")):
                    service_remote_provider_event_lag_violation_case_count += 1
            provider_anomalies = dict(service_metrics.get("provider_anomalies") or {})
            if bool(provider_anomalies.get("report_available")):
                service_provider_anomaly_report_count += 1
                for values, key in (
                    (service_provider_anomaly_count_values, "anomaly_count"),
                    (service_provider_zero_retry_values, "zero_result_retry_count"),
                    (service_provider_zero_exhausted_values, "zero_result_retry_exhausted_count"),
                    (service_provider_zero_accepted_values, "zero_result_accepted_count"),
                    (service_provider_empty_scale_values, "empty_scale_count"),
                    (service_provider_probe_total_drift_values, "probe_total_drift_count"),
                    (service_provider_empty_page_range_values, "empty_page_range_count"),
                    (service_provider_single_page_retry_values, "single_page_retry_count"),
                ):
                    value = provider_anomalies.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
            target_public_web = dict(service_metrics.get("target_candidate_public_web") or {})
            if bool(target_public_web.get("report_available")):
                service_target_public_web_report_count += 1
                for values, key in (
                    (service_target_public_web_batch_values, "batch_count"),
                    (service_target_public_web_run_values, "run_count"),
                    (service_target_public_web_metric_run_values, "metric_run_count"),
                    (service_target_public_web_remote_pending_values, "remote_search_pending_run_count"),
                    (service_target_public_web_partial_failure_values, "partial_failure_count"),
                    (
                        service_target_public_web_unmaterialized_signal_gap_values,
                        "unmaterialized_signal_gap_count",
                    ),
                    (
                        service_target_public_web_completed_without_materialized_values,
                        "completed_without_materialized_signals_count",
                    ),
                    (service_target_public_web_missing_metric_values, "missing_phase_metric_count"),
                    (service_target_public_web_provider_failure_values, "provider_or_fetch_failure_count"),
                    (service_target_public_web_local_error_values, "local_processing_error_count"),
                    (service_target_public_web_crm_owner_values, "crm_storage_owner_batch_count"),
                    (service_target_public_web_legacy_owner_values, "legacy_storage_owner_batch_count"),
                    (service_target_public_web_execution_bridge_values, "execution_backend_bridge_count"),
                    (service_target_public_web_queue_command_values, "queue_batch_command_count"),
                    (
                        service_target_public_web_queue_command_succeeded_values,
                        "queue_batch_command_succeeded_count",
                    ),
                    (service_target_public_web_queue_command_pending_values, "queue_batch_command_pending_count"),
                    (service_target_public_web_queue_command_failed_values, "queue_batch_command_failed_count"),
                    (
                        service_target_public_web_queue_command_invalid_owner_values,
                        "queue_batch_command_invalid_owner_count",
                    ),
                    (
                        service_target_public_web_queue_command_incomplete_causality_values,
                        "queue_batch_command_incomplete_causality_count",
                    ),
                ):
                    value = target_public_web.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(target_public_web.get("queue_batch_command_missing")):
                    service_target_public_web_queue_command_missing_case_count += 1
                if not bool(target_public_web.get("queue_batch_command_contract_present")):
                    service_target_public_web_queue_command_contract_missing_case_count += 1
                service_target_public_web_storage_owner_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("storage_owner_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                service_target_public_web_execution_backend_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("execution_backend_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                service_target_public_web_queue_command_status_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("queue_batch_command_status_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                service_target_public_web_queue_command_owner_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("queue_batch_command_owner_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                if bool(target_public_web.get("service_guardrail_violation_detected")):
                    service_target_public_web_guardrail_violation_case_count += 1
                service_target_public_web_risk_reason_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("risk_reason_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                service_target_public_web_slowest_phase_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(target_public_web.get("slowest_phase_counts") or {}).items()
                        if str(key).strip()
                    }
                )
                for phase, duration_ms in dict(target_public_web.get("duration_by_phase_ms_max") or {}).items():
                    normalized_phase = str(phase or "").strip()
                    if normalized_phase:
                        service_target_public_web_phase_duration_values[normalized_phase].append(
                            _safe_float(duration_ms)
                        )
            serving_publication_gap = dict(service_metrics.get("serving_publication_gap") or {})
            if bool(serving_publication_gap.get("report_available")):
                service_serving_publication_gap_report_count += 1
                age_ms = serving_publication_gap.get("age_ms")
                if isinstance(age_ms, (int, float)) and float(age_ms) >= 0.0:
                    service_serving_publication_gap_age_values.append(float(age_ms))
                if bool(serving_publication_gap.get("gap_present")):
                    service_serving_publication_gap_present_count += 1
                if bool(serving_publication_gap.get("stale_gap_present")):
                    service_serving_publication_gap_stale_count += 1
            board_overlay_writes = dict(service_metrics.get("board_overlay_writes") or {})
            if bool(board_overlay_writes.get("report_available")):
                service_board_overlay_write_report_count += 1
                for values, key in (
                    (service_board_overlay_write_full_rebuild_values, "full_rebuild_count"),
                    (service_board_overlay_write_incremental_values, "incremental_partial_overlay_count"),
                    (service_board_overlay_write_fast_path_eligible_values, "fast_path_eligible_count"),
                    (service_board_overlay_write_fast_path_used_values, "fast_path_used_count"),
                    (
                        service_board_overlay_write_eligible_fallback_values,
                        "eligible_full_rebuild_fallback_count",
                    ),
                ):
                    value = board_overlay_writes.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
            if bool(board_overlay_writes.get("eligible_full_rebuild_fallback_present")):
                service_board_overlay_write_eligible_fallback_case_count += 1
            finalization_overlay = dict(service_metrics.get("finalization_overlay") or {})
            if bool(finalization_overlay.get("report_available")):
                service_finalization_overlay_report_count += 1
                for values, key in (
                    (service_finalization_overlay_full_rewrite_values, "full_rewrite_count"),
                    (service_finalization_overlay_reuse_values, "reuse_count"),
                    (
                        service_finalization_overlay_eligible_full_rewrite_values,
                        "eligible_full_rewrite_count",
                    ),
                ):
                    value = finalization_overlay.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(finalization_overlay.get("eligible_full_rewrite_present")):
                    service_finalization_overlay_eligible_full_rewrite_case_count += 1
            legacy_materialization_write_contract = dict(
                service_metrics.get("legacy_materialization_write_contract") or {}
            )
            if bool(legacy_materialization_write_contract.get("report_available")):
                service_legacy_materialization_write_contract_report_count += 1
                for values, key in (
                    (service_legacy_materialization_item_values, "item_count"),
                    (service_legacy_materialization_normal_write_values, "normal_path_write_count"),
                    (service_legacy_materialization_migration_adapter_values, "migration_adapter_write_count"),
                    (service_legacy_materialization_missing_contract_values, "missing_contract_count"),
                ):
                    value = legacy_materialization_write_contract.get(key)
                    if isinstance(value, (int, float)) and float(value) >= 0.0:
                        values.append(float(value))
                if bool(legacy_materialization_write_contract.get("normal_path_write_present")):
                    service_legacy_materialization_normal_write_case_count += 1
                if bool(legacy_materialization_write_contract.get("missing_contract_present")):
                    service_legacy_materialization_missing_contract_case_count += 1
                service_legacy_materialization_item_kind_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(
                            legacy_materialization_write_contract.get("item_kind_counts") or {}
                        ).items()
                        if str(key).strip()
                    }
                )
                service_legacy_materialization_normal_kind_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(
                            legacy_materialization_write_contract.get("normal_path_kind_counts") or {}
                        ).items()
                        if str(key).strip()
                    }
                )
                service_legacy_materialization_migration_kind_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(
                            legacy_materialization_write_contract.get("migration_adapter_kind_counts") or {}
                        ).items()
                        if str(key).strip()
                    }
                )
                service_legacy_materialization_missing_kind_counts.update(
                    {
                        str(key): _safe_int(value)
                        for key, value in dict(
                            legacy_materialization_write_contract.get("missing_contract_kind_counts") or {}
                        ).items()
                        if str(key).strip()
                    }
                )
            bottlenecks = dict(service_metrics.get("bottlenecks") or {})
            top_bottlenecks = [
                dict(item) for item in list(bottlenecks.get("top_bottlenecks") or []) if isinstance(item, dict)
            ]
            bottleneck_count = max(_safe_int(bottlenecks.get("bottleneck_count")), len(top_bottlenecks))
            if bottleneck_count > 0:
                service_bottleneck_case_count += 1
            service_bottleneck_total_count += bottleneck_count
            for item in top_bottlenecks:
                kind = str(item.get("kind") or "").strip()
                severity = str(item.get("severity") or "").strip()
                if kind:
                    service_bottleneck_kind_counts[kind] += 1
                if severity:
                    service_bottleneck_severity_counts[severity] += 1
    event_level_efficiency_summary = aggregate_event_level_efficiency_metrics(event_level_efficiency_reports)
    summary: dict[str, Any] = {
        "case_count": len(records),
        "timings_ms": {},
    }
    for key, values in aggregates.items():
        if not values:
            continue
        sorted_values = sorted(values)
        summary["timings_ms"][key] = {
            "count": len(values),
            "min": round(sorted_values[0], 2),
            "avg": round(sum(sorted_values) / len(sorted_values), 2),
            "p95": round(_percentile(sorted_values, 0.95), 2),
            "max": round(sorted_values[-1], 2),
        }
    provider_summary: dict[str, Any] = {
        "board_ready_count": board_ready_count,
        "board_ready_nonempty_count": board_ready_nonempty_count,
        "board_runtime_state_parity": {
            "report_count": board_runtime_state_parity_report_count,
            "violation_case_count": board_runtime_state_parity_violation_count,
            "missing_source_counts": dict(sorted(board_runtime_state_parity_missing_counts.items())),
            "mismatched_field_counts": dict(sorted(board_runtime_state_parity_mismatch_counts.items())),
        },
        "progress_regression_case_count": progress_regression_case_count,
        "progress_stage1_regression_case_count": progress_stage1_regression_case_count,
        "progress_lifecycle_regression_case_count": progress_lifecycle_regression_case_count,
        "progress_contract_violation_case_count": progress_contract_violation_case_count,
        "progress_contract_violation_counts": dict(sorted(progress_contract_violation_counts.items())),
        "progress_sample_count": progress_sample_count,
        "progress_maxima": {key: value for key, value in progress_maxima.items() if int(value or 0) > 0},
        "progress_payload_bytes": _numeric_summary(progress_payload_byte_values),
        "stage_wall_clock_ms": {},
        "workflow_wall_clock_ms": {},
        "behavior_guardrails": {
            "duplicate_provider_dispatch_case_count": duplicate_provider_dispatch_case_count,
            "duplicate_provider_dispatch_signature_count": duplicate_provider_dispatch_signature_count,
            "redundant_provider_dispatch_count": redundant_provider_dispatch_count,
            "disabled_stage_violation_case_count": disabled_stage_violation_case_count,
            "unexpected_public_web_stage_case_count": unexpected_public_web_stage_case_count,
            "prerequisite_gap_case_count": prerequisite_gap_case_count,
            "final_results_board_violation_case_count": final_results_board_violation_case_count,
            "delayed_board_after_final_case_count": delayed_board_after_final_case_count,
            "streaming_materialization_violation_case_count": streaming_materialization_violation_case_count,
            "prerequisite_gap_ms": {},
        },
        "materialization_streaming": {
            "report_count": streaming_materialization_report_count,
            "provider_response_count": _numeric_summary(materialization_provider_response_counts),
            "pending_delta_count": _numeric_summary(materialization_pending_delta_counts),
            "provider_response_to_first_materialization_ms": _numeric_summary(materialization_first_delta_gaps),
        },
        "post_preview_finalization": {
            "report_count": post_preview_finalization_report_count,
            "preview_to_first_finalization_start_ms": _numeric_summary(post_preview_first_finalization_start_ms),
            "preview_to_first_materialize_start_ms": _numeric_summary(post_preview_first_materialize_start_ms),
            "preview_to_last_materialize_completed_ms": _numeric_summary(post_preview_last_materialize_completed_ms),
            "preview_to_finalization_completed_ms": _numeric_summary(post_preview_finalization_complete_ms),
            "materialize_completed_count": _numeric_summary(post_preview_materialize_completed_counts),
            "materialize_sync_duration_ms": _numeric_summary(post_preview_materialize_sync_duration_ms),
            "profile_batch_local_apply_duration_ms": _numeric_summary(
                post_preview_profile_batch_local_apply_duration_ms
            ),
            "candidate_source_closure_lifecycle_ms": _numeric_summary(
                post_preview_candidate_source_closure_lifecycle_ms
            ),
            "materialize_sync_scope_counts": dict(post_preview_materialize_sync_scope_counts),
            "long_finalization_case_count": post_preview_long_finalization_case_count,
        },
        "provider_backpressure": {
            "report_count": provider_backpressure_report_count,
            "backpressure_case_count": provider_backpressure_case_count,
            "limiter_exhausted_case_count": provider_limiter_exhausted_case_count,
            "limiter_observed_case_count": provider_limiter_observed_case_count,
            "observed_active_limiter_count": _numeric_summary(provider_limiter_active_counts),
            "max_provider_limiter_wait_ms": _numeric_summary(provider_limiter_wait_ms),
        },
        "remote_actor_slot_observation": {
            "report_count": remote_actor_slot_observation_report_count,
            "harvest_profile_actor_global_inflight": _numeric_summary(remote_actor_slot_budget_values),
            "peak_waiting_remote_harvest_count": _numeric_summary(remote_actor_slot_peak_values),
            "remote_actor_slot_peak_occupancy_ratio": _numeric_summary(remote_actor_slot_occupancy_values),
        },
        "event_level_efficiency": event_level_efficiency_summary,
        "service_metrics": {
            "report_count": service_metrics_report_count,
            "worker_count": _numeric_summary(service_worker_counts),
            "trace_span_count": _numeric_summary(service_trace_span_counts),
            "worker_duration_ms": _numeric_summary(service_worker_duration_ms),
            "global_next_worker_start_gap_ms": _numeric_summary(service_global_handoff_gap_ms),
            "slow_worker_count": _numeric_summary(service_slow_worker_counts),
            "slow_gap_count": _numeric_summary(service_slow_gap_counts),
            "out_of_order_profile_completion_count": _numeric_summary(service_out_of_order_profile_completion_values),
            "local_apply_backlog": {
                "report_count": service_local_apply_backlog_report_count,
                "applied_not_ingested_count": _numeric_summary(service_local_apply_backlog_values),
                "stale_applied_not_ingested_count": _numeric_summary(service_local_apply_backlog_stale_values),
                "age_ms": _numeric_summary(service_local_apply_backlog_age_ms),
                "stale_case_count": service_local_apply_backlog_stale_case_count,
                "closure_backlog_count": _numeric_summary(service_local_apply_closure_backlog_values),
                "closure_retryable_count": _numeric_summary(service_local_apply_closure_retryable_values),
                "closure_ready_retry_count": _numeric_summary(service_local_apply_closure_ready_retry_values),
                "closure_stale_running_count": _numeric_summary(service_local_apply_closure_stale_running_values),
                "closure_retry_backlog_case_count": service_local_apply_closure_retry_backlog_case_count,
                "closure_stale_running_case_count": service_local_apply_closure_stale_running_case_count,
            },
            "user_experience": {
                "final_results_to_board_ready_ms": _numeric_summary(service_final_results_to_board_ready_ms),
                "final_results_to_board_nonempty_ms": _numeric_summary(service_final_results_to_board_nonempty_ms),
                "job_to_board_visible_partial_ms": _numeric_summary(service_job_to_board_visible_partial_ms),
                "job_to_board_nonempty_ms": _numeric_summary(service_job_to_board_nonempty_ms),
                "stage_1_preview_to_final_results_ms": _numeric_summary(service_stage_1_preview_to_final_results_ms),
                "loading_feedback_required_case_count": service_loading_feedback_required_count,
                "board_readiness_violation_case_count": service_board_readiness_violation_count,
                "long_finalization_case_count": service_long_finalization_count,
            },
            "board_visible_projection": {
                "report_count": service_board_visible_projection_report_count,
                "board_visible_count": _numeric_summary(service_board_visible_count_values),
                "patch_log_count": _numeric_summary(service_board_visible_patch_count_values),
                "fetched_to_board_visible_lag_count": _numeric_summary(service_board_visible_fetched_lag_values),
                "patch_log_lag_count": _numeric_summary(service_board_visible_patch_lag_values),
                "patch_consumable_card_nonzero_count": _numeric_summary(service_board_visible_consumable_patch_values),
                "patch_consumable_card_distinct_count": _numeric_summary(
                    service_board_visible_consumable_progression_values
                ),
                "pure_shell_patch_count": _numeric_summary(service_board_visible_pure_shell_patch_values),
                "projection_missing_case_count": service_board_visible_projection_missing_count,
                "patch_log_missing_case_count": service_board_visible_patch_log_missing_count,
                "patch_log_replay_lag_case_count": service_board_visible_patch_log_lag_count,
                "materialization_lag_case_count": service_board_visible_materialization_lag_count,
            },
            "post_profile_completion": {
                "report_count": service_post_profile_report_count,
                "url_terminal_state_leak_count": _numeric_summary(service_post_profile_terminal_leak_values),
                "event_level_callback_elapsed_ms": _numeric_summary(service_post_profile_callback_elapsed_values),
                "profile_file_visible_to_board_patch_visible_ms": _numeric_summary(
                    service_post_profile_file_to_patch_values
                ),
                "all_profiles_fetched_to_all_cards_visible_ms": _numeric_summary(
                    service_post_profile_all_profiles_to_cards_values
                ),
                "slo_violation_case_count": service_post_profile_violation_case_count,
            },
            "recovery_phase_metrics": {
                "report_count": service_recovery_phase_report_count,
                "missing_phase_count": _numeric_summary(service_recovery_phase_missing_values),
                "failed_phase_count": _numeric_summary(service_recovery_phase_failed_values),
                "slow_phase_count": _numeric_summary(service_recovery_phase_slow_values),
                "unexpected_enabled_phase_count": _numeric_summary(service_recovery_phase_unexpected_values),
                "recovery_tick_budget_exhausted_count": _numeric_summary(service_recovery_tick_budget_exhausted_values),
                "cooperative_budget_yield_count": _numeric_summary(service_recovery_cooperative_budget_yield_values),
                "budget_yield_attention_count": _numeric_summary(service_recovery_budget_yield_attention_values),
                "elapsed_ms": _numeric_summary(service_recovery_phase_elapsed_values),
                "total_elapsed_ms": _numeric_summary(service_recovery_phase_total_elapsed_values),
                "phase_candidate_count": _numeric_summary(service_recovery_phase_candidate_values),
                "local_apply_candidate_per_second": _numeric_summary(
                    service_recovery_local_apply_candidate_per_second_values
                ),
                "slo_violation_case_count": service_recovery_phase_violation_case_count,
            },
            "snapshot_full_materialization_queue": {
                "report_count": service_snapshot_full_materialization_queue_report_count,
                "backlog_count": _numeric_summary(service_snapshot_full_materialization_queue_backlog_values),
                "retryable_count": _numeric_summary(service_snapshot_full_materialization_queue_retryable_values),
                "stale_running_count": _numeric_summary(
                    service_snapshot_full_materialization_queue_stale_running_values
                ),
                "retry_backlog_case_count": service_snapshot_full_materialization_queue_retry_backlog_case_count,
                "stale_running_case_count": service_snapshot_full_materialization_queue_stale_running_case_count,
            },
            "search_seed_discovery_queue": {
                "report_count": service_search_seed_discovery_queue_report_count,
                "item_count": _numeric_summary(service_search_seed_discovery_queue_item_values),
                "provider_owned_count": _numeric_summary(service_search_seed_discovery_queue_provider_owned_values),
                "retry_wait_count": _numeric_summary(service_search_seed_discovery_queue_retry_wait_values),
                "ready_retry_count": _numeric_summary(service_search_seed_discovery_queue_ready_retry_values),
                "exhausted_count": _numeric_summary(service_search_seed_discovery_queue_exhausted_values),
                "stale_provider_owned_count": _numeric_summary(
                    service_search_seed_discovery_queue_stale_provider_values
                ),
                "owner_missing_count": _numeric_summary(service_search_seed_discovery_queue_owner_missing_values),
                "worker_without_item_count": _numeric_summary(service_search_seed_discovery_worker_without_item_values),
                "worker_without_local_apply_count": _numeric_summary(
                    service_search_seed_discovery_worker_without_local_apply_values
                ),
                "exhausted_without_report_count": _numeric_summary(
                    service_search_seed_discovery_queue_exhausted_without_report_values
                ),
                "retry_backlog_case_count": service_search_seed_discovery_queue_retry_backlog_case_count,
                "stale_provider_case_count": service_search_seed_discovery_queue_stale_provider_case_count,
                "owner_missing_case_count": service_search_seed_discovery_queue_owner_missing_case_count,
                "worker_without_item_case_count": service_search_seed_discovery_worker_without_item_case_count,
                "worker_without_local_apply_case_count": (
                    service_search_seed_discovery_worker_without_local_apply_case_count
                ),
                "exhausted_without_report_case_count": (
                    service_search_seed_discovery_queue_exhausted_without_report_case_count
                ),
            },
            "provider_search_retry_queue": {
                "report_count": service_provider_search_retry_queue_report_count,
                "backlog_count": _numeric_summary(service_provider_search_retry_queue_backlog_values),
                "retryable_count": _numeric_summary(service_provider_search_retry_queue_retryable_values),
                "terminal_failed_count": _numeric_summary(service_provider_search_retry_queue_terminal_failed_values),
                "stale_running_count": _numeric_summary(service_provider_search_retry_queue_stale_running_values),
                "retry_backlog_case_count": service_provider_search_retry_queue_retry_backlog_case_count,
                "terminal_failure_case_count": service_provider_search_retry_queue_terminal_failure_case_count,
                "stale_running_case_count": service_provider_search_retry_queue_stale_running_case_count,
            },
            "remote_provider_events": {
                "report_count": service_remote_provider_event_report_count,
                "event_count": _numeric_summary(service_remote_provider_event_count_values),
                "late_duplicate_count": _numeric_summary(service_remote_provider_event_late_duplicate_values),
                "in_flight_duplicate_count": _numeric_summary(service_remote_provider_event_in_flight_duplicate_values),
                "remote_to_local_event_lag_ms": _numeric_summary(service_remote_provider_event_lag_values),
                "actionable_remote_to_local_event_lag_ms": _numeric_summary(
                    service_remote_provider_event_actionable_lag_values
                ),
                "late_duplicate_remote_to_local_event_lag_ms": _numeric_summary(
                    service_remote_provider_event_late_duplicate_lag_values
                ),
                "slow_lag_count": _numeric_summary(service_remote_provider_event_slow_lag_values),
                "late_duplicate_slow_lag_count": _numeric_summary(
                    service_remote_provider_event_late_duplicate_slow_lag_values
                ),
                "lag_violation_case_count": service_remote_provider_event_lag_violation_case_count,
                "source_counts": dict(sorted(service_remote_provider_event_source_counts.items())),
                "status_counts": dict(sorted(service_remote_provider_event_status_counts.items())),
            },
            "provider_anomalies": {
                "report_count": service_provider_anomaly_report_count,
                "anomaly_count": _numeric_summary(service_provider_anomaly_count_values),
                "zero_result_retry_count": _numeric_summary(service_provider_zero_retry_values),
                "zero_result_retry_exhausted_count": _numeric_summary(service_provider_zero_exhausted_values),
                "zero_result_accepted_count": _numeric_summary(service_provider_zero_accepted_values),
                "empty_scale_count": _numeric_summary(service_provider_empty_scale_values),
                "probe_total_drift_count": _numeric_summary(service_provider_probe_total_drift_values),
                "empty_page_range_count": _numeric_summary(service_provider_empty_page_range_values),
                "single_page_retry_count": _numeric_summary(service_provider_single_page_retry_values),
            },
            "target_candidate_public_web": {
                "report_count": service_target_public_web_report_count,
                "batch_count": _numeric_summary(service_target_public_web_batch_values),
                "run_count": _numeric_summary(service_target_public_web_run_values),
                "metric_run_count": _numeric_summary(service_target_public_web_metric_run_values),
                "remote_search_pending_run_count": _numeric_summary(service_target_public_web_remote_pending_values),
                "partial_failure_count": _numeric_summary(service_target_public_web_partial_failure_values),
                "unmaterialized_signal_gap_count": _numeric_summary(
                    service_target_public_web_unmaterialized_signal_gap_values
                ),
                "completed_without_materialized_signals_count": _numeric_summary(
                    service_target_public_web_completed_without_materialized_values
                ),
                "missing_phase_metric_count": _numeric_summary(service_target_public_web_missing_metric_values),
                "provider_or_fetch_failure_count": _numeric_summary(service_target_public_web_provider_failure_values),
                "local_processing_error_count": _numeric_summary(service_target_public_web_local_error_values),
                "crm_storage_owner_batch_count": _numeric_summary(service_target_public_web_crm_owner_values),
                "legacy_storage_owner_batch_count": _numeric_summary(service_target_public_web_legacy_owner_values),
                "execution_backend_bridge_count": _numeric_summary(service_target_public_web_execution_bridge_values),
                "queue_batch_command_count": _numeric_summary(service_target_public_web_queue_command_values),
                "queue_batch_command_succeeded_count": _numeric_summary(
                    service_target_public_web_queue_command_succeeded_values
                ),
                "queue_batch_command_pending_count": _numeric_summary(
                    service_target_public_web_queue_command_pending_values
                ),
                "queue_batch_command_failed_count": _numeric_summary(
                    service_target_public_web_queue_command_failed_values
                ),
                "queue_batch_command_invalid_owner_count": _numeric_summary(
                    service_target_public_web_queue_command_invalid_owner_values
                ),
                "queue_batch_command_incomplete_causality_count": _numeric_summary(
                    service_target_public_web_queue_command_incomplete_causality_values
                ),
                "queue_batch_command_missing_case_count": service_target_public_web_queue_command_missing_case_count,
                "queue_batch_command_contract_missing_case_count": (
                    service_target_public_web_queue_command_contract_missing_case_count
                ),
                "queue_batch_command_status_counts": dict(
                    sorted(service_target_public_web_queue_command_status_counts.items())
                ),
                "queue_batch_command_owner_counts": dict(
                    sorted(service_target_public_web_queue_command_owner_counts.items())
                ),
                "storage_owner_counts": dict(sorted(service_target_public_web_storage_owner_counts.items())),
                "execution_backend_counts": dict(sorted(service_target_public_web_execution_backend_counts.items())),
                "duration_by_phase_ms_max": {
                    key: _numeric_summary(values)
                    for key, values in sorted(service_target_public_web_phase_duration_values.items())
                    if values
                },
                "guardrail_violation_case_count": service_target_public_web_guardrail_violation_case_count,
                "risk_reason_counts": dict(sorted(service_target_public_web_risk_reason_counts.items())),
                "slowest_phase_counts": dict(sorted(service_target_public_web_slowest_phase_counts.items())),
            },
            "serving_publication_gap": {
                "report_count": service_serving_publication_gap_report_count,
                "gap_present_case_count": service_serving_publication_gap_present_count,
                "stale_gap_case_count": service_serving_publication_gap_stale_count,
                "age_ms": _numeric_summary(service_serving_publication_gap_age_values),
            },
            "board_overlay_writes": {
                "report_count": service_board_overlay_write_report_count,
                "full_rebuild_count": _numeric_summary(service_board_overlay_write_full_rebuild_values),
                "incremental_partial_overlay_count": _numeric_summary(service_board_overlay_write_incremental_values),
                "fast_path_eligible_count": _numeric_summary(service_board_overlay_write_fast_path_eligible_values),
                "fast_path_used_count": _numeric_summary(service_board_overlay_write_fast_path_used_values),
                "eligible_full_rebuild_fallback_count": _numeric_summary(
                    service_board_overlay_write_eligible_fallback_values
                ),
                "eligible_full_rebuild_fallback_case_count": (service_board_overlay_write_eligible_fallback_case_count),
            },
            "finalization_overlay": {
                "report_count": service_finalization_overlay_report_count,
                "full_rewrite_count": _numeric_summary(service_finalization_overlay_full_rewrite_values),
                "reuse_count": _numeric_summary(service_finalization_overlay_reuse_values),
                "eligible_full_rewrite_count": _numeric_summary(
                    service_finalization_overlay_eligible_full_rewrite_values
                ),
                "eligible_full_rewrite_case_count": (service_finalization_overlay_eligible_full_rewrite_case_count),
            },
            "legacy_materialization_write_contract": {
                "report_count": service_legacy_materialization_write_contract_report_count,
                "item_count": _numeric_summary(service_legacy_materialization_item_values),
                "normal_path_write_count": _numeric_summary(service_legacy_materialization_normal_write_values),
                "migration_adapter_write_count": _numeric_summary(
                    service_legacy_materialization_migration_adapter_values
                ),
                "missing_contract_count": _numeric_summary(service_legacy_materialization_missing_contract_values),
                "normal_path_write_case_count": service_legacy_materialization_normal_write_case_count,
                "missing_contract_case_count": service_legacy_materialization_missing_contract_case_count,
                "item_kind_counts": dict(sorted(service_legacy_materialization_item_kind_counts.items())),
                "normal_path_kind_counts": dict(sorted(service_legacy_materialization_normal_kind_counts.items())),
                "migration_adapter_kind_counts": dict(
                    sorted(service_legacy_materialization_migration_kind_counts.items())
                ),
                "missing_contract_kind_counts": dict(
                    sorted(service_legacy_materialization_missing_kind_counts.items())
                ),
            },
            "bottlenecks": {
                "case_count": service_bottleneck_case_count,
                "total_count": service_bottleneck_total_count,
                "by_kind": dict(sorted(service_bottleneck_kind_counts.items())),
                "by_severity": dict(sorted(service_bottleneck_severity_counts.items())),
            },
        },
        "workflow_benchmark": {
            "report_count": workflow_benchmark_report_count,
            "metrics": {
                key: _numeric_summary(values) for key, values in workflow_benchmark_aggregates.items() if values
            },
        },
    }
    for stage_name, values in provider_stage_aggregates.items():
        if not values:
            continue
        sorted_values = sorted(values)
        provider_summary["stage_wall_clock_ms"][stage_name] = {
            "count": len(values),
            "min": round(sorted_values[0], 2),
            "avg": round(sum(sorted_values) / len(sorted_values), 2),
            "p95": round(_percentile(sorted_values, 0.95), 2),
            "max": round(sorted_values[-1], 2),
        }
    for metric_name, values in workflow_wall_clock_aggregates.items():
        if not values:
            continue
        sorted_values = sorted(values)
        provider_summary["workflow_wall_clock_ms"][metric_name] = {
            "count": len(values),
            "min": round(sorted_values[0], 2),
            "avg": round(sum(sorted_values) / len(sorted_values), 2),
            "p95": round(_percentile(sorted_values, 0.95), 2),
            "max": round(sorted_values[-1], 2),
        }
    for metric_name, values in sorted(prerequisite_gap_aggregates.items()):
        if not values:
            continue
        sorted_values = sorted(values)
        provider_summary["behavior_guardrails"]["prerequisite_gap_ms"][metric_name] = {
            "count": len(values),
            "min": round(sorted_values[0], 2),
            "avg": round(sum(sorted_values) / len(sorted_values), 2),
            "p95": round(_percentile(sorted_values, 0.95), 2),
            "max": round(sorted_values[-1], 2),
        }
    strategy_rollups: dict[str, Any] = {}
    for group_name, rollups in (
        ("effective_acquisition_mode", acquisition_mode_rollups),
        ("dispatch_strategy", dispatch_strategy_rollups),
    ):
        if not rollups:
            continue
        strategy_rollups[group_name] = {}
        for key, bucket in sorted(rollups.items()):
            bucket_summary: dict[str, Any] = {"case_count": int(bucket.get("case_count") or 0), "timings_ms": {}}
            for metric_name in (
                "total",
                "wait_for_completion",
                "board_probe_wait",
                "job_to_stage_1_preview",
                "job_to_final_results",
                "job_to_board_nonempty",
            ):
                values = [
                    float(value) for value in list(bucket.get(metric_name) or []) if isinstance(value, (int, float))
                ]
                if not values:
                    continue
                sorted_values = sorted(values)
                bucket_summary["timings_ms"][metric_name] = {
                    "count": len(values),
                    "min": round(sorted_values[0], 2),
                    "avg": round(sum(sorted_values) / len(sorted_values), 2),
                    "p95": round(_percentile(sorted_values, 0.95), 2),
                    "max": round(sorted_values[-1], 2),
                }
            strategy_rollups[group_name][key] = bucket_summary
    if strategy_rollups:
        provider_summary["strategy_rollups"] = strategy_rollups
    if (
        provider_summary["stage_wall_clock_ms"]
        or provider_summary["workflow_wall_clock_ms"]
        or board_ready_count
        or board_ready_nonempty_count
        or board_runtime_state_parity_report_count
        or board_runtime_state_parity_violation_count
        or progress_sample_count
        or progress_regression_case_count
        or progress_stage1_regression_case_count
        or progress_lifecycle_regression_case_count
        or progress_contract_violation_case_count
        or duplicate_provider_dispatch_case_count
        or duplicate_provider_dispatch_signature_count
        or disabled_stage_violation_case_count
        or final_results_board_violation_case_count
        or streaming_materialization_report_count
        or streaming_materialization_violation_case_count
        or workflow_benchmark_report_count
        or provider_backpressure_report_count
        or int(event_level_efficiency_summary.get("report_count") or 0) > 0
        or service_metrics_report_count
        or provider_summary["behavior_guardrails"]["prerequisite_gap_ms"]
    ):
        summary["provider_case_report"] = provider_summary
    return summary


def _numeric_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {}
    sorted_values = sorted(values)
    return {
        "count": len(values),
        "min": round(sorted_values[0], 2),
        "avg": round(sum(sorted_values) / len(sorted_values), 2),
        "p95": round(_percentile(sorted_values, 0.95), 2),
        "max": round(sorted_values[-1], 2),
    }


def _extend_summary_values(target: list[float], summary: dict[str, Any]) -> None:
    values = summary.get("values")
    if isinstance(values, list):
        target.extend(float(value) for value in values if isinstance(value, (int, float)) and float(value) >= 0.0)
        return
    avg = summary.get("avg")
    if isinstance(avg, (int, float)) and float(avg) >= 0.0:
        target.append(float(avg))


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    clamped = min(1.0, max(0.0, float(percentile)))
    index = clamped * (len(sorted_values) - 1)
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    weight = index - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight
