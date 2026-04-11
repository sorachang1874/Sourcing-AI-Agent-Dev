from __future__ import annotations

from pathlib import Path
from typing import Any

from .domain import normalize_name_token


def extract_progress_metrics(
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    results: list[dict[str, Any]],
    manual_review_items: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    interesting_keys = {
        "snapshot_id",
        "entry_count",
        "query_count",
        "queued_query_count",
        "queued_harvest_worker_count",
        "queued_exploration_count",
        "candidate_count",
        "evidence_count",
        "observed_company_candidate_count",
        "stop_reason",
    }
    for event in events:
        payload = dict(event.get("payload") or {})
        for key in interesting_keys:
            value = payload.get(key)
            if value is None or value == "" or value == []:
                continue
            metrics[key] = value
        if isinstance(payload.get("artifact_paths"), dict) and payload.get("artifact_paths"):
            metrics["artifact_paths"] = dict(payload.get("artifact_paths") or {})
    candidate_source = dict(job_summary.get("candidate_source") or {})
    if candidate_source:
        metrics["candidate_source"] = candidate_source
    refresh_metrics = extract_refresh_metrics(job_summary)
    if refresh_metrics:
        metrics["refresh_metrics"] = refresh_metrics
    pre_retrieval_refresh = dict(job_summary.get("pre_retrieval_refresh") or {})
    if pre_retrieval_refresh:
        metrics["pre_retrieval_refresh"] = pre_retrieval_refresh
    background_reconcile = dict(job_summary.get("background_reconcile") or {})
    if background_reconcile:
        metrics["background_reconcile"] = background_reconcile
    metrics["result_count"] = len(results)
    metrics["manual_review_count"] = len(manual_review_items)
    if "manual_review_queue_count" in job_summary:
        metrics["manual_review_queue_count"] = int(job_summary.get("manual_review_queue_count") or 0)
    if "semantic_hit_count" in job_summary:
        metrics["semantic_hit_count"] = int(job_summary.get("semantic_hit_count") or 0)
    outreach_layering = dict(job_summary.get("outreach_layering") or {})
    if outreach_layering:
        metrics["outreach_layering"] = outreach_layering
    stage1_preview = dict(job_summary.get("stage1_preview") or {})
    if stage1_preview:
        metrics["stage1_preview"] = stage1_preview
    return metrics


def extract_refresh_metrics(job_summary: dict[str, Any]) -> dict[str, Any]:
    pre_retrieval_refresh = dict(job_summary.get("pre_retrieval_refresh") or {})
    background_reconcile = dict(job_summary.get("background_reconcile") or {})
    search_seed_reconcile = dict(background_reconcile.get("search_seed") or {})
    harvest_prefetch_reconcile = dict(background_reconcile.get("harvest_prefetch") or {})
    exploration_reconcile = (
        dict(background_reconcile)
        if background_reconcile
        and "search_seed" not in background_reconcile
        and "harvest_prefetch" not in background_reconcile
        else {}
    )
    metrics: dict[str, Any] = {}
    if pre_retrieval_refresh:
        metrics["pre_retrieval_refresh_count"] = 1
        metrics["pre_retrieval_refresh_status"] = str(pre_retrieval_refresh.get("status") or "")
        metrics["inline_search_seed_worker_count"] = int(pre_retrieval_refresh.get("search_seed_worker_count") or 0)
        metrics["inline_harvest_prefetch_worker_count"] = int(
            pre_retrieval_refresh.get("harvest_prefetch_worker_count") or 0
        )
        metrics["pre_retrieval_refresh_snapshot_id"] = str(pre_retrieval_refresh.get("snapshot_id") or "")
    background_reconcile_count = 0
    if search_seed_reconcile:
        background_reconcile_count += 1
        metrics["background_search_seed_reconcile_count"] = 1
        metrics["background_search_seed_reconcile_status"] = str(search_seed_reconcile.get("status") or "")
        metrics["background_search_seed_worker_count"] = int(search_seed_reconcile.get("applied_worker_count") or 0)
        metrics["background_search_seed_added_entry_count"] = int(search_seed_reconcile.get("added_entry_count") or 0)
    if harvest_prefetch_reconcile:
        background_reconcile_count += 1
        metrics["background_harvest_prefetch_reconcile_count"] = 1
        metrics["background_harvest_prefetch_reconcile_status"] = str(harvest_prefetch_reconcile.get("status") or "")
        metrics["background_harvest_prefetch_worker_count"] = int(
            harvest_prefetch_reconcile.get("applied_worker_count") or 0
        )
    if exploration_reconcile:
        background_reconcile_count += 1
        metrics["background_exploration_reconcile_count"] = 1
        metrics["background_exploration_reconcile_status"] = str(exploration_reconcile.get("status") or "")
        metrics["background_exploration_worker_count"] = int(exploration_reconcile.get("applied_worker_count") or 0)
    if background_reconcile_count > 0:
        metrics["background_reconcile_count"] = background_reconcile_count
    return metrics


def runtime_refresh_metric_subset(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "pre_retrieval_refresh_job_count": int(metrics.get("pre_retrieval_refresh_job_count") or 0),
        "inline_search_seed_worker_count": int(metrics.get("inline_search_seed_worker_count") or 0),
        "inline_harvest_prefetch_worker_count": int(metrics.get("inline_harvest_prefetch_worker_count") or 0),
        "background_reconcile_job_count": int(metrics.get("background_reconcile_job_count") or 0),
        "background_search_seed_reconcile_job_count": int(
            metrics.get("background_search_seed_reconcile_job_count") or 0
        ),
        "background_harvest_prefetch_reconcile_job_count": int(
            metrics.get("background_harvest_prefetch_reconcile_job_count") or 0
        ),
    }


def worker_blocks_acquisition_resume(worker: dict[str, Any], *, blocked_task: str) -> bool:
    if _worker_is_terminal_for_acquisition_resume(worker):
        return False
    lane_id = str(worker.get("lane_id") or "").strip()
    if lane_id == "exploration_specialist":
        return False
    if str(blocked_task or "").strip() == "enrich_profiles_multisource":
        if lane_id != "enrichment_specialist":
            return False
        metadata = dict(worker.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "").strip().lower()
        worker_key = str(worker.get("worker_key") or "").strip().lower()
        return recovery_kind == "harvest_profile_batch" or worker_key.startswith("harvest_profile_batch::")
    return True


def worker_has_background_candidate_output(worker: dict[str, Any]) -> bool:
    output = dict(worker.get("output") or {})
    if isinstance(output.get("candidate"), dict) and output.get("candidate"):
        return True
    evidence = output.get("evidence")
    return isinstance(evidence, list) and any(isinstance(item, dict) for item in evidence)


def worker_has_completed_background_harvest_prefetch(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") != "enrichment_specialist":
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    metadata = dict(worker.get("metadata") or {})
    return str(metadata.get("recovery_kind") or "") == "harvest_profile_batch"


def worker_has_completed_background_search_output(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") not in {"search_planner", "public_media_specialist"}:
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    output = dict(worker.get("output") or {})
    if dict(output.get("summary") or {}):
        return True
    return any(isinstance(item, dict) for item in list(output.get("entries") or []))


def resolve_reconcile_snapshot_id(job_summary: dict[str, Any], workers: list[dict[str, Any]]) -> str:
    candidate_source = dict(job_summary.get("candidate_source") or {})
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    reconcile_state = dict(job_summary.get("background_reconcile") or {})
    snapshot_id = str(reconcile_state.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    for worker in workers:
        snapshot_dir = Path(str(dict(worker.get("metadata") or {}).get("snapshot_dir") or "")).expanduser()
        if snapshot_dir.name:
            return snapshot_dir.name
    return ""


def resolve_reconcile_snapshot_dir(
    runtime_dir: Path,
    target_company: str,
    job_summary: dict[str, Any],
    workers: list[dict[str, Any]],
) -> Path | None:
    for worker in workers:
        snapshot_dir = Path(str(dict(worker.get("metadata") or {}).get("snapshot_dir") or "")).expanduser()
        if snapshot_dir.exists():
            return snapshot_dir
    snapshot_id = resolve_reconcile_snapshot_id(job_summary, workers)
    if not snapshot_id:
        return None
    company_key = normalize_name_token(target_company)
    if not company_key:
        return None
    candidate = runtime_dir / "company_assets" / company_key / snapshot_id
    return candidate if candidate.exists() else None


def _worker_is_terminal_for_acquisition_resume(worker: dict[str, Any]) -> bool:
    status = str(worker.get("status") or "").strip().lower()
    return status in {"completed", "failed", "interrupted", "cancelled", "canceled", "superseded"}
