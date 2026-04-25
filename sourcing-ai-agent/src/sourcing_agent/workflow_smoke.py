from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from .runtime_tuning import build_materialization_streaming_budget_report
from .scripted_provider_scenario import load_scripted_provider_invocations

DEFAULT_SMOKE_CASES: list[dict[str, Any]] = [
    {
        "case": "skild_pretrain",
        "payload": {
            "raw_user_request": "请帮我找Skild AI里做Pre-train方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "humansand_coding",
        "payload": {
            "raw_user_request": "我想了解Humans&里偏Coding agents方向的研究成员",
            "top_k": 10,
        },
    },
    {
        "case": "anthropic_pretraining",
        "payload": {
            "raw_user_request": "帮我找Anthropic里做Pre-training方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "xai_full_roster",
        "payload": {
            "raw_user_request": "给我 xAI 的所有成员",
            "top_k": 10,
        },
    },
    {
        "case": "xai_coding_all_members_scoped",
        "payload": {
            "raw_user_request": "我要 xAI 做 Coding 方向的全部成员",
            "top_k": 10,
        },
    },
    {
        "case": "openai_reasoning",
        "payload": {
            "raw_user_request": "我想要OpenAI里做Reasoning方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "google_multimodal_pretrain",
        "payload": {
            "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
            "top_k": 10,
        },
    },
]


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
_MONOTONIC_PROGRESS_COUNTER_KEYS = (
    "result_count",
    "observed_company_candidate_count",
)
_BACKLOG_PROGRESS_COUNTER_KEYS = (
    "manual_review_count",
)
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


def _progress_runtime_health(snapshot: dict[str, Any]) -> dict[str, Any]:
    return dict(dict(snapshot.get("progress") or {}).get("runtime_health") or {})


def _should_auto_run_worker_recovery(snapshot: dict[str, Any]) -> bool:
    status = str(snapshot.get("status") or "").strip().lower()
    stage = str(snapshot.get("stage") or "").strip().lower()
    if status != "blocked" or stage != "acquiring":
        return False
    runtime_health = _progress_runtime_health(snapshot)
    classification = str(runtime_health.get("classification") or "").strip().lower()
    if classification in _AUTO_RECOVERY_RUNTIME_HEALTH_CLASSIFICATIONS:
        return True
    counters = dict(dict(snapshot.get("progress") or {}).get("counters") or {})
    return any(
        int(counters.get(key) or 0) > 0
        for key in (
            "blocked_worker_count",
            "queued_worker_count",
            "waiting_remote_harvest_count",
            "waiting_remote_search_count",
        )
    )


def _worker_has_inline_incremental_ingest(worker: dict[str, Any]) -> bool:
    output = dict(dict(worker or {}).get("output") or {})
    inline_ingest = dict(output.get("inline_incremental_ingest") or {})
    return bool(str(inline_ingest.get("applied_at") or "").strip())


def _worker_needs_smoke_recovery(worker: dict[str, Any]) -> bool:
    payload = dict(worker or {})
    metadata = dict(payload.get("metadata") or {})
    recovery_kind = str(metadata.get("recovery_kind") or "").strip()
    if recovery_kind not in {"harvest_company_employees", "harvest_profile_batch"}:
        return False
    status = str(payload.get("status") or "").strip().lower()
    if status not in TERMINAL_WORKER_STATUSES:
        return True
    if status == "completed" and not _worker_has_inline_incremental_ingest(payload):
        return True
    return False


def _smoke_worker_recovery_payload(job_id: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "workflow_stale_scope_job_id": job_id,
        "workflow_resume_explicit_job": True,
        "workflow_auto_resume_enabled": True,
        "workflow_resume_stale_after_seconds": 0,
        "workflow_resume_limit": 1,
        "workflow_queue_auto_takeover_enabled": True,
        "workflow_queue_resume_stale_after_seconds": 0,
        "workflow_queue_resume_limit": 1,
        "explicit_job_followup_rounds": 6,
        "stale_after_seconds": 0,
        "runtime_heartbeat_source": "hosted_smoke_poll",
        "runtime_heartbeat_interval_seconds": 0,
    }


def _worker_recovery_result_made_progress(recovery: dict[str, Any]) -> bool:
    for item in list(recovery.get("post_completion_reconcile") or []):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        if status and status not in {"skipped", "noop", "no_op"}:
            return True
    for item in list(recovery.get("workflow_resume") or []):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        if status and status not in {"skipped", "noop", "no_op"}:
            return True
    daemon_status = str(dict(recovery.get("daemon") or {}).get("status") or "").strip().lower()
    return bool(daemon_status and daemon_status not in {"completed", "idle", "skipped", "noop", "no_op"})


def _settle_post_terminal_worker_recovery(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    poll_seconds: float,
    max_rounds: int = 8,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], float]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return {}, {}, [], 0.0
    recovery_runs: list[dict[str, Any]] = []
    timings_ms = 0.0
    latest_job_payload: dict[str, Any] = {}
    latest_results_payload: dict[str, Any] = {}
    for round_index in range(max(0, int(max_rounds or 0))):
        workers_payload = client.get(f"/api/jobs/{normalized_job_id}/workers")
        workers = [dict(item) for item in list(workers_payload.get("agent_workers") or []) if isinstance(item, dict)]
        recoverable_workers = [worker for worker in workers if _worker_needs_smoke_recovery(worker)]
        if not recoverable_workers:
            break
        started_at = time.perf_counter()
        recovery = client.post("/api/workers/daemon/run-once", _smoke_worker_recovery_payload(normalized_job_id))
        timings_ms += (time.perf_counter() - started_at) * 1000
        recovery_runs.append(
            {
                "post_terminal_round": round_index + 1,
                "status": recovery.get("status"),
                "daemon_status": dict(recovery.get("daemon") or {}).get("status"),
                "worker_count": dict(recovery.get("daemon") or {}).get("worker_count"),
                "recoverable_worker_count": len(recoverable_workers),
                "workflow_resume": list(recovery.get("workflow_resume") or []),
                "post_completion_reconcile": list(recovery.get("post_completion_reconcile") or []),
            }
        )
        latest_job_payload = client.get(f"/api/jobs/{normalized_job_id}")
        latest_results_payload = client.get(f"/api/jobs/{normalized_job_id}/results")
        remaining_workers_payload = client.get(f"/api/jobs/{normalized_job_id}/workers")
        remaining_workers = [
            dict(item)
            for item in list(remaining_workers_payload.get("agent_workers") or [])
            if isinstance(item, dict) and _worker_needs_smoke_recovery(dict(item))
        ]
        if not remaining_workers or not _worker_recovery_result_made_progress(recovery):
            break
        time.sleep(max(0.05, poll_seconds))
    return latest_job_payload, latest_results_payload, recovery_runs, round(timings_ms, 2)


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


def load_smoke_cases(matrix_file: str = "", selected_cases: set[str] | None = None) -> list[dict[str, Any]]:
    selected = {str(item).strip() for item in list(selected_cases or set()) if str(item).strip()}
    cases = DEFAULT_SMOKE_CASES
    if matrix_file:
        payload = json.loads(Path(matrix_file).read_text(encoding="utf-8"))
        loaded_cases = payload.get("cases")
        if not isinstance(loaded_cases, list):
            raise ValueError("matrix file must contain a top-level `cases` list")
        cases = loaded_cases
    normalized: list[dict[str, Any]] = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        case_name = str(case.get("case") or "").strip()
        if not case_name:
            continue
        if selected and case_name not in selected:
            continue
        payload = case.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"case `{case_name}` is missing a dict payload")
        normalized.append({"case": case_name, "payload": payload})
    if selected and not normalized:
        missing = ", ".join(sorted(selected))
        raise ValueError(f"requested cases not found in matrix: {missing}")
    return normalized


class HostedWorkflowSmokeClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))

    def post(self, path: str, payload: dict[str, Any], timeout: float = 120.0) -> dict[str, Any]:
        request = urllib_request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def get(self, path: str, timeout: float = 120.0) -> dict[str, Any]:
        request = urllib_request.Request(f"{self.base_url}{path}", method="GET")
        with self.opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))


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
        key=lambda item: (-int(item.get("count") or 0), str(item.get("dispatch_kind") or ""), str(item.get("logical_name") or "")),
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
    analysis_stage_mode = str(
        explain_payload.get("analysis_stage_mode")
        or dict(dict(results_payload.get("job") or {}).get("summary") or {}).get("analysis_stage_mode")
        or "single_stage"
    ).strip().lower() or "single_stage"
    raw_stage_summaries = _raw_stage_summaries(results_payload)
    public_web_summary = dict(raw_stage_summaries.get("public_web_stage_2") or {})
    public_web_stage_present = bool(public_web_summary) and str(public_web_summary.get("status") or "").strip().lower() not in {
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


def _build_materialization_streaming_report(job_summary: dict[str, Any]) -> dict[str, Any]:
    latest_metrics = dict(job_summary.get("latest_metrics") or {})
    background_reconcile = dict(job_summary.get("background_reconcile") or latest_metrics.get("background_reconcile") or {})
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
    budget_report = build_materialization_streaming_budget_report(
        request_context,
        provider_response_count=provider_response_count,
        profile_url_count=profile_url_count,
        pending_delta_count=pending_delta_count,
        active_writer_count=active_writer_count,
        queued_writer_count=queued_writer_count,
        oldest_pending_delta_age_ms=oldest_pending_delta_age_ms,
    )
    provider_to_materialization_ms = _safe_float(
        raw_streaming.get("provider_response_to_first_materialization_ms")
        or raw_streaming.get("provider_result_to_first_materialization_ms")
    )
    report_available = bool(raw_streaming or raw_writer or inline_worker_count or provider_response_count or profile_url_count)
    return {
        "report_available": report_available,
        "provider_response_count": provider_response_count,
        "profile_url_count": profile_url_count,
        "inline_incremental_worker_count": inline_worker_count,
        "pending_delta_count": pending_delta_count,
        "provider_response_to_first_materialization_ms": round(provider_to_materialization_ms, 2),
        "budget": budget_report,
    }


def _build_streaming_materialization_guardrail_report(materialization_streaming: dict[str, Any]) -> dict[str, Any]:
    payload = dict(materialization_streaming or {})
    budget = dict(payload.get("budget") or {})
    provider_to_materialization_ms = _safe_float(payload.get("provider_response_to_first_materialization_ms"))
    slow_first_materialization = (
        provider_to_materialization_ms > _STREAMING_MATERIALIZATION_GAP_VIOLATION_MS
        and bool(payload.get("report_available"))
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


def _build_behavior_guardrails_report(
    *,
    explain_payload: dict[str, Any],
    results_payload: dict[str, Any],
    stage_wall_clock: dict[str, dict[str, Any]],
    board_probe: dict[str, Any],
    workflow_wall_clock: dict[str, float],
    provider_invocations: list[dict[str, Any]] | None = None,
    materialization_streaming: dict[str, Any] | None = None,
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
    streaming_materialization = _build_streaming_materialization_guardrail_report(
        dict(materialization_streaming or {})
    )
    violation_count = (
        int(duplicate_provider_dispatch.get("violation_count") or 0)
        + int(disabled_stage_violations.get("violation_count") or 0)
        + int(prerequisite_gaps.get("violation_count") or 0)
        + int(final_results_board_consistency.get("violation_count") or 0)
        + int(streaming_materialization.get("violation_count") or 0)
    )
    return {
        "duplicate_provider_dispatch": duplicate_provider_dispatch,
        "disabled_stage_violations": disabled_stage_violations,
        "prerequisite_gaps": prerequisite_gaps,
        "final_results_board_consistency": final_results_board_consistency,
        "streaming_materialization": streaming_materialization,
        "violation_count": violation_count,
        "violation_detected": violation_count > 0,
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


def _build_workflow_wall_clock_report(
    *,
    stage_wall_clock: dict[str, dict[str, Any]],
    board_probe: dict[str, Any],
    timings_ms: dict[str, float],
    timeline: list[dict[str, Any]] | None = None,
) -> dict[str, float]:
    anchor_started_at = _earliest_stage_started_at(stage_wall_clock)
    preview_completed_at = str(dict(stage_wall_clock.get("stage_1_preview") or {}).get("completed_at") or "").strip()
    final_stage = dict(stage_wall_clock.get("stage_2_final") or stage_wall_clock.get("public_web_stage_2") or {})
    final_completed_at = str(final_stage.get("completed_at") or "").strip()
    report: dict[str, float] = {}
    timeline = list(timeline or [])
    job_submit_offset_ms = round(
        sum(float(timings_ms.get(key) or 0.0) for key in ("explain", "plan", "review", "start")),
        2,
    )

    def _timeline_milestone_observed_ms(*needles: str, require_stage: str = "", require_status: str = "") -> float | None:
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

    job_to_preview = _reasonable_elapsed_ms(anchor_started_at, preview_completed_at)
    preview_observed_ms = _timeline_milestone_observed_ms(
        "stage 1 preview ready",
        "interim linkedin stage 1 candidate documents",
        "stage 1 preview 已生成",
        "当前返回",
    )
    if job_to_preview is not None:
        report["job_to_stage_1_preview"] = job_to_preview
    elif preview_observed_ms is not None:
        report["job_to_stage_1_preview"] = preview_observed_ms

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
    if job_to_final is not None:
        report["job_to_final_results"] = job_to_final
    elif final_results_observed_ms is not None:
        report["job_to_final_results"] = final_results_observed_ms

    preview_to_final = _reasonable_elapsed_ms(preview_completed_at, final_completed_at)
    preview_anchor_ms = float(report.get("job_to_stage_1_preview") or 0.0)
    final_anchor_ms = float(report.get("job_to_final_results") or 0.0)
    if preview_anchor_ms > 0.0 and final_anchor_ms >= preview_anchor_ms:
        report["stage_1_preview_to_final_results"] = round(final_anchor_ms - preview_anchor_ms, 2)
    elif preview_to_final is not None:
        report["stage_1_preview_to_final_results"] = preview_to_final

    board_wait_ms = round(float(timings_ms.get("board_probe_wait") or 0.0), 2)
    if bool(board_probe.get("ready")) and board_wait_ms >= 0.0:
        report["final_results_to_board_ready"] = board_wait_ms
        if final_anchor_ms > 0.0:
            report["job_to_board_ready"] = round(final_anchor_ms + board_wait_ms, 2)
    if bool(board_probe.get("ready_nonempty")) and board_wait_ms >= 0.0:
        report["final_results_to_board_nonempty"] = board_wait_ms
        if final_anchor_ms > 0.0:
            report["job_to_board_nonempty"] = round(final_anchor_ms + board_wait_ms, 2)
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
    dashboard_preview_candidates = list(asset_population.get("candidates") or [])
    ranked_preview = list(dashboard_payload.get("results") or dashboard_payload.get("ranked_results") or [])
    preview_count = len(dashboard_preview_candidates) if dashboard_preview_candidates else len(ranked_preview)
    result_mode = str(candidate_page_payload.get("result_mode") or "").strip()
    total_candidates = _safe_int(
        candidate_page_payload.get("total_candidates")
        or asset_population.get("candidate_count")
        or dashboard_payload.get("ranked_result_count")
    )
    returned_count = _safe_int(candidate_page_payload.get("returned_count"))
    ready = bool(result_mode)
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
    }


def _effective_asset_population_candidate_count(
    *,
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
) -> int:
    board_total = _safe_int(
        candidate_page_payload.get("total_candidates") or candidate_page_payload.get("returned_count")
    )
    if board_total > 0:
        return board_total
    dashboard_total = _safe_int(
        dict(dashboard_payload.get("asset_population") or {}).get("candidate_count")
        or dashboard_payload.get("ranked_result_count")
    )
    if dashboard_total > 0:
        return dashboard_total
    return _safe_int(dict(results_payload.get("asset_population") or {}).get("candidate_count"))


def _build_progress_observability_report(progress_samples: list[dict[str, Any]]) -> dict[str, Any]:
    maxima: dict[str, int] = {key: 0 for key in _PROGRESS_MAX_COUNTER_KEYS}
    peak_counter_values: dict[str, int] = {}
    peak_backlog_values: dict[str, int] = {}
    regressions: dict[str, dict[str, Any]] = {}
    backlog_reductions: dict[str, dict[str, Any]] = {}
    latest_counters: dict[str, int] = {}
    latest_runtime_health: dict[str, Any] = {}
    synthetic_terminal_sample_count = 0

    for sample in list(progress_samples or []):
        counters = {
            str(key): _safe_int(value)
            for key, value in dict(sample.get("counters") or {}).items()
            if str(key).strip()
        }
        runtime_health = {
            str(key): value
            for key, value in dict(sample.get("runtime_health") or {}).items()
            if str(key).strip()
        }
        if bool(sample.get("synthetic_terminal_sample")):
            synthetic_terminal_sample_count += 1
        latest_runtime_health = runtime_health
        latest_counters = counters

        for key in _PROGRESS_MAX_COUNTER_KEYS:
            candidate_value = counters.get(key)
            if candidate_value is None:
                candidate_value = _safe_int(runtime_health.get(key))
            maxima[key] = max(int(maxima.get(key) or 0), _safe_int(candidate_value))

        for key in _MONOTONIC_PROGRESS_COUNTER_KEYS:
            current_value = _safe_int(counters.get(key))
            previous_peak = int(peak_counter_values.get(key) or 0)
            if current_value < previous_peak:
                drop = previous_peak - current_value
                existing = dict(regressions.get(key) or {})
                regressions[key] = {
                    "regression_count": int(existing.get("regression_count") or 0) + 1,
                    "largest_drop": max(int(existing.get("largest_drop") or 0), drop),
                    "peak_before_drop": max(int(existing.get("peak_before_drop") or 0), previous_peak),
                    "latest_value": current_value,
                }
            peak_counter_values[key] = max(previous_peak, current_value)

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

    filtered_maxima = {key: value for key, value in maxima.items() if int(value or 0) > 0}
    return {
        "sample_count": len(list(progress_samples or [])),
        "maxima": filtered_maxima,
        "counter_regressions": regressions,
        "regression_detected": bool(regressions),
        "backlog_reductions": backlog_reductions,
        "backlog_reduction_detected": bool(backlog_reductions),
        "synthetic_terminal_sample_count": synthetic_terminal_sample_count,
        "terminal_progress_lag_detected": synthetic_terminal_sample_count > 0,
        "latest_counters": {key: value for key, value in latest_counters.items() if int(value or 0) > 0},
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


def _build_case_level_smoke_exports(
    *,
    results_payload: dict[str, Any],
    provider_case_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "stage_summary_digest": stage_summary_digest(results_payload),
        "stage_wall_clock_ms": dict(provider_case_report.get("stage_wall_clock_ms") or {}),
        "workflow_wall_clock_ms": dict(provider_case_report.get("workflow_wall_clock_ms") or {}),
        "behavior_guardrails": dict(provider_case_report.get("behavior_guardrails") or {}),
    }


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


def _settle_board_probe(
    client: HostedWorkflowSmokeClient,
    *,
    job_id: str,
    results_payload: dict[str, Any],
    poll_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
    expected_candidate_count = _expected_board_candidate_count(results_payload)
    timings = {
        "dashboard_fetch_ms": 0.0,
        "candidate_page_fetch_ms": 0.0,
        "wait_ms": 0.0,
        "attempt_count": 0.0,
    }
    dashboard_payload: dict[str, Any] = {}
    candidate_page_payload: dict[str, Any] = {}
    started_at = time.perf_counter()
    deadline = time.monotonic() + max(2.0, poll_seconds * 20)
    while True:
        timings["attempt_count"] += 1.0
        fetch_started_at = time.perf_counter()
        dashboard_payload = client.get(f"/api/jobs/{job_id}/dashboard")
        timings["dashboard_fetch_ms"] += (time.perf_counter() - fetch_started_at) * 1000
        fetch_started_at = time.perf_counter()
        candidate_page_payload = client.get(f"/api/jobs/{job_id}/candidates?offset=0&limit=24&lightweight=1")
        timings["candidate_page_fetch_ms"] += (time.perf_counter() - fetch_started_at) * 1000
        probe = _build_board_probe_report(dashboard_payload, candidate_page_payload)
        total_candidates = _safe_int(probe.get("candidate_page_total_candidates"))
        returned_count = _safe_int(probe.get("candidate_page_returned_count"))
        if (
            expected_candidate_count <= 0
            or probe.get("ready_nonempty")
            or returned_count > 0
            or total_candidates >= expected_candidate_count > 0
        ):
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.05, min(0.25, poll_seconds)))
    timings["wait_ms"] = (time.perf_counter() - started_at) * 1000
    return dashboard_payload, candidate_page_payload, timings


def _build_provider_case_report(
    *,
    explain_payload: dict[str, Any],
    job_summary: dict[str, Any],
    results_payload: dict[str, Any],
    dashboard_payload: dict[str, Any],
    candidate_page_payload: dict[str, Any],
    timings_ms: dict[str, float],
    progress_observability: dict[str, Any] | None = None,
    timeline: list[dict[str, Any]] | None = None,
    provider_invocations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    latest_metrics = dict(job_summary.get("latest_metrics") or {})
    result_job_summary = dict(dict(results_payload.get("job") or {}).get("summary") or {})
    candidate_source = dict(
        result_job_summary.get("candidate_source")
        or latest_metrics.get("candidate_source")
        or dict(dict(results_payload.get("asset_population") or {}).get("candidate_source") or {})
    )
    background_reconcile = dict(job_summary.get("background_reconcile") or latest_metrics.get("background_reconcile") or {})
    stage_wall_clock = _build_stage_wall_clock_report(results_payload)
    stage1_preview = dict(stage_wall_clock.get("stage_1_preview") or {})
    stage2_final = dict(stage_wall_clock.get("stage_2_final") or {})
    board_probe = _build_board_probe_report(dashboard_payload, candidate_page_payload)
    materialization_streaming = _build_materialization_streaming_report(job_summary)
    workflow_wall_clock = _build_workflow_wall_clock_report(
        stage_wall_clock=stage_wall_clock,
        board_probe=board_probe,
        timings_ms=timings_ms,
        timeline=timeline,
    )
    behavior_guardrails = _build_behavior_guardrails_report(
        explain_payload=explain_payload,
        results_payload=results_payload,
        stage_wall_clock=stage_wall_clock,
        board_probe=board_probe,
        workflow_wall_clock=workflow_wall_clock,
        provider_invocations=provider_invocations,
        materialization_streaming=materialization_streaming,
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
        "search": _build_search_report(latest_metrics=latest_metrics, background_reconcile=background_reconcile),
        "profile_completion": _build_profile_completion_report(background_reconcile),
        "materialization_streaming": materialization_streaming,
        "board": board_probe,
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
) -> dict[str, Any]:
    case_started_at = time.perf_counter()
    case_wall_started_at = datetime.now(timezone.utc)
    effective_payload = dict(payload or {})
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
    explain_full_roster_task_metadata = _explain_full_roster_task_metadata(explain)
    explain_full_roster_shard_policy = dict(
        explain_full_roster_task_metadata.get("company_employee_shard_policy") or {}
    )
    explain_former_task_metadata = _explain_task_metadata(explain, "acquire_former_search_seed")
    explain_current_filter_hints = dict(explain_full_roster_task_metadata.get("filter_hints") or {})
    explain_former_filter_hints = dict(explain_former_task_metadata.get("filter_hints") or {})
    explain_matched_job = dict((explain.get("dispatch_preview") or {}).get("matched_job") or {})
    record["explain"] = {
        "status": explain.get("status"),
        "target_company": ((explain.get("request_preview") or {}).get("target_company")),
        "target_scope": ((explain.get("request_preview") or {}).get("target_scope")),
        "keywords": ((explain.get("request_preview") or {}).get("keywords")),
        "analysis_stage_mode": (
            explain.get("analysis_stage_mode")
            or effective_payload.get("analysis_stage_mode")
            or "single_stage"
        ),
        "org_scale_band": ((explain.get("organization_execution_profile") or {}).get("org_scale_band")),
        "default_acquisition_mode": (
            (explain.get("organization_execution_profile") or {}).get("default_acquisition_mode")
        ),
        "planner_mode": ((explain.get("asset_reuse_plan") or {}).get("planner_mode")),
        "requires_delta_acquisition": ((explain.get("asset_reuse_plan") or {}).get("requires_delta_acquisition")),
        "dispatch_strategy": ((explain.get("dispatch_preview") or {}).get("strategy")),
        "dispatch_matched_job_id": explain_matched_job.get("job_id"),
        "dispatch_matched_job_status": explain_matched_job.get("status"),
        "current_lane": (((explain.get("lane_preview") or {}).get("current") or {}).get("planned_behavior")),
        "former_lane": (((explain.get("lane_preview") or {}).get("former") or {}).get("planned_behavior")),
        "runtime_tuning_profile": (
            (((explain.get("request_preview") or {}).get("intent_axes") or {}).get("fallback_policy") or {}).get(
                "runtime_tuning_profile"
            )
        ),
        "effective_acquisition_mode": (
            (explain.get("effective_execution_semantics") or {}).get("effective_acquisition_mode")
        ),
        "default_results_mode": ((explain.get("effective_execution_semantics") or {}).get("default_results_mode")),
        "baseline_directional_local_reuse_eligible": bool(
            ((explain.get("asset_reuse_plan") or {}).get("baseline_directional_local_reuse_eligible"))
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
    plan_started_at = time.perf_counter()
    plan = client.post("/api/plan", effective_payload)
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
                "decision": {},
            },
        )
        timings_ms["review"] = round((time.perf_counter() - review_started_at) * 1000, 2)
        record["review"] = {"status": review.get("status"), "review_id": review_id}
        start_payload: dict[str, Any] = {"plan_review_id": review_id}
    else:
        timings_ms["review"] = 0.0
        record["review"] = {"status": "skipped", "review_id": None}
        start_payload = dict(effective_payload)
    start_started_at = time.perf_counter()
    start = client.post("/api/workflows", start_payload)
    timings_ms["start"] = round((time.perf_counter() - start_started_at) * 1000, 2)
    job_id = start.get("job_id")
    record["start"] = {key: start.get(key) for key in ("job_id", "status", "stage")}
    timeline: list[dict[str, Any]] = []
    progress_samples: list[dict[str, Any]] = []
    results: dict[str, Any] = {}
    job_payload: dict[str, Any] = {}
    stage2_continue_requested = False
    worker_recovery_runs: list[dict[str, Any]] = []
    last_worker_recovery_at = 0.0
    worker_recovery_cooldown_seconds = max(0.25, poll_seconds * 5)
    last_results_probe_at = 0.0
    results_probe_cooldown_seconds = max(2.0, poll_seconds * 10)
    if job_id:
        wait_started_at = time.perf_counter()
        seen_marker: tuple[Any, Any, Any, Any] | None = None
        max_ticks = max(1, int(max_poll_seconds / max(0.1, poll_seconds)))
        for tick in range(max_ticks + 1):
            snapshot = client.get(f"/api/jobs/{job_id}/progress")
            snapshot_progress = dict(snapshot.get("progress") or {})
            progress_samples.append(
                {
                    "tick": tick,
                    "status": str(snapshot.get("status") or ""),
                    "stage": str(snapshot.get("stage") or ""),
                    "counters": dict(snapshot_progress.get("counters") or {}),
                    "runtime_health": dict(snapshot_progress.get("runtime_health") or {}),
                }
            )
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
            if _should_auto_run_worker_recovery(snapshot):
                now = time.monotonic()
                if (now - last_worker_recovery_at) >= worker_recovery_cooldown_seconds:
                    recovery = client.post(
                        "/api/workers/daemon/run-once",
                        _smoke_worker_recovery_payload(str(job_id or "")),
                    )
                    daemon = dict(recovery.get("daemon") or {})
                    workflow_resume = list(recovery.get("workflow_resume") or [])
                    worker_recovery_runs.append(
                        {
                            "tick": tick,
                            "status": recovery.get("status"),
                            "daemon_status": daemon.get("status"),
                            "worker_count": daemon.get("worker_count"),
                            "workflow_resume": workflow_resume,
                        }
                    )
                    timeline.append(
                        {
                            "tick": tick,
                            "status": snapshot.get("status"),
                            "stage": snapshot.get("stage"),
                            "message": "Smoke runner triggered worker recovery daemon once for blocked acquisition.",
                            "awaiting_user_action": "",
                            "observed_at_ms": round((time.perf_counter() - case_started_at) * 1000, 2),
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
                    probed_job_payload = client.get(f"/api/jobs/{job_id}")
                    probed_results = client.get(f"/api/jobs/{job_id}/results")
                    timings_ms["results_probe"] = timings_ms.get("results_probe", 0.0) + round(
                        (time.perf_counter() - probe_started_at) * 1000,
                        2,
                    )
                    probe_ready, probe_completion_state = _evaluate_smoke_record_completion(
                        probed_job_payload,
                        probed_results,
                    )
                    last_results_probe_at = now
                    if probe_ready:
                        settled_job_payload = probed_job_payload
                        settled_results = probed_results
                        settle_deadline = time.monotonic() + max(2.0, poll_seconds * 20)
                        while time.monotonic() < settle_deadline:
                            settle_snapshot = client.get(f"/api/jobs/{job_id}/progress")
                            if str(settle_snapshot.get("status") or "").strip().lower() in TERMINAL_WORKFLOW_STATUSES:
                                settled_job_payload = client.get(f"/api/jobs/{job_id}")
                                settled_results = client.get(f"/api/jobs/{job_id}/results")
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
            results = client.get(f"/api/jobs/{job_id}/results")
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
                post_terminal_recovery_runs,
                post_terminal_recovery_ms,
            ) = _settle_post_terminal_worker_recovery(
                client,
                job_id=str(job_id or ""),
                poll_seconds=poll_seconds,
            )
            if post_terminal_recovery_runs:
                worker_recovery_runs.extend(post_terminal_recovery_runs)
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
    if worker_recovery_runs:
        record["worker_recovery"] = worker_recovery_runs
    summary_payload = dict(job_payload.get("summary") or {})
    record["job_summary"] = {
        "background_reconcile": dict(summary_payload.get("background_reconcile") or {}),
        "latest_metrics": dict(summary_payload.get("latest_metrics") or {}),
    }
    dashboard_payload: dict[str, Any] = {}
    candidate_page_payload: dict[str, Any] = {}
    if job_id:
        dashboard_payload, candidate_page_payload, board_probe_timings = _settle_board_probe(
            client,
            job_id=job_id,
            results_payload=results,
            poll_seconds=poll_seconds,
        )
        timings_ms["dashboard_fetch"] = round(float(board_probe_timings.get("dashboard_fetch_ms") or 0.0), 2)
        timings_ms["candidate_page_fetch"] = round(
            float(board_probe_timings.get("candidate_page_fetch_ms") or 0.0),
            2,
        )
        timings_ms["board_probe_wait"] = round(float(board_probe_timings.get("wait_ms") or 0.0), 2)
    else:
        timings_ms["dashboard_fetch"] = 0.0
        timings_ms["candidate_page_fetch"] = 0.0
        timings_ms["board_probe_wait"] = 0.0
    smoke_ready, smoke_completion_state = _evaluate_smoke_record_completion(job_payload, results)
    raw_job_status = str(((results.get("job") or {}).get("status")) or "")
    raw_job_stage = str(((results.get("job") or {}).get("stage")) or "")
    final_status_lower = raw_job_status.strip().lower()
    latest_progress_status = str((progress_samples[-1] or {}).get("status") or "").strip().lower() if progress_samples else ""
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
    result_asset_population = dict(results.get("asset_population") or {})
    default_results_mode = str((results.get("effective_execution_semantics") or {}).get("default_results_mode") or "")
    asset_population_available = bool(result_asset_population.get("available")) or (
        default_results_mode.strip().lower() == "asset_population"
        and effective_asset_population_candidate_count > 0
    )
    record["final"] = {
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
    progress_observability = _build_progress_observability_report(progress_samples)
    record["progress_observability"] = progress_observability
    case_wall_completed_at = datetime.now(timezone.utc)
    provider_invocations = load_scripted_provider_invocations(
        started_at=case_wall_started_at,
        completed_at=case_wall_completed_at,
    )
    record["provider_case_report"] = _build_provider_case_report(
        explain_payload=dict(record.get("explain") or {}),
        job_summary=dict(record.get("job_summary") or {}),
        results_payload=results,
        dashboard_payload=dashboard_payload,
        candidate_page_payload=candidate_page_payload,
        timings_ms=timings_ms,
        progress_observability=progress_observability,
        timeline=timeline,
        provider_invocations=provider_invocations,
    )
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
        if not case_name or not payload:
            continue
        try:
            record = run_hosted_smoke_case(
                client=client,
                case_name=case_name,
                payload=payload,
                reviewer=reviewer,
                poll_seconds=poll_seconds,
                max_poll_seconds=max_poll_seconds,
                auto_continue_stage2=auto_continue_stage2,
                runtime_tuning_profile=runtime_tuning_profile,
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
    progress_regression_case_count = 0
    progress_sample_count = 0
    progress_maxima: dict[str, int] = {}
    prerequisite_gap_aggregates: dict[str, list[float]] = {}
    materialization_provider_response_counts: list[float] = []
    materialization_pending_delta_counts: list[float] = []
    materialization_first_delta_gaps: list[float] = []
    streaming_materialization_violation_case_count = 0
    streaming_materialization_report_count = 0
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
        progress_observability = dict(provider_case_report.get("progress_observability") or {})
        if bool(progress_observability.get("regression_detected")):
            progress_regression_case_count += 1
        progress_sample_count += _safe_int(progress_observability.get("sample_count"))
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
        "progress_regression_case_count": progress_regression_case_count,
        "progress_sample_count": progress_sample_count,
        "progress_maxima": {key: value for key, value in progress_maxima.items() if int(value or 0) > 0},
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
                values = [float(value) for value in list(bucket.get(metric_name) or []) if isinstance(value, (int, float))]
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
        or duplicate_provider_dispatch_case_count
        or duplicate_provider_dispatch_signature_count
        or disabled_stage_violation_case_count
        or final_results_board_violation_case_count
        or streaming_materialization_report_count
        or streaming_materialization_violation_case_count
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
