from __future__ import annotations

from pathlib import Path
from typing import Any

from .asset_paths import resolve_company_snapshot_dir, resolve_snapshot_dir_from_source_path
from .domain import normalize_name_token


def extract_progress_metrics(
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    *,
    result_count: int,
    manual_review_count: int,
    precomputed_event_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metrics: dict[str, Any] = dict(precomputed_event_metrics or {})
    if not metrics:
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
    metrics["result_count"] = max(0, int(result_count or 0))
    metrics["manual_review_count"] = max(0, int(manual_review_count or 0))
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
    company_roster_reconcile = dict(background_reconcile.get("company_roster") or {})
    search_seed_reconcile = dict(background_reconcile.get("search_seed") or {})
    harvest_prefetch_reconcile = dict(background_reconcile.get("harvest_prefetch") or {})
    exploration_reconcile = (
        dict(background_reconcile)
        if background_reconcile
        and "company_roster" not in background_reconcile
        and "search_seed" not in background_reconcile
        and "harvest_prefetch" not in background_reconcile
        else {}
    )
    metrics: dict[str, Any] = {}
    if pre_retrieval_refresh:
        metrics["pre_retrieval_refresh_count"] = 1
        metrics["pre_retrieval_refresh_status"] = str(pre_retrieval_refresh.get("status") or "")
        metrics["inline_company_roster_worker_count"] = int(
            pre_retrieval_refresh.get("company_roster_worker_count") or 0
        )
        metrics["inline_search_seed_worker_count"] = int(pre_retrieval_refresh.get("search_seed_worker_count") or 0)
        metrics["inline_harvest_prefetch_worker_count"] = int(
            pre_retrieval_refresh.get("harvest_prefetch_worker_count") or 0
        )
        metrics["pre_retrieval_refresh_snapshot_id"] = str(pre_retrieval_refresh.get("snapshot_id") or "")
    background_reconcile_count = 0
    if company_roster_reconcile:
        background_reconcile_count += 1
        metrics["background_company_roster_reconcile_count"] = 1
        metrics["background_company_roster_reconcile_status"] = str(company_roster_reconcile.get("status") or "")
        metrics["background_company_roster_worker_count"] = int(
            company_roster_reconcile.get("applied_worker_count") or 0
        )
        metrics["background_company_roster_added_entry_count"] = int(
            company_roster_reconcile.get("added_entry_count") or 0
        )
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
        "inline_company_roster_worker_count": int(metrics.get("inline_company_roster_worker_count") or 0),
        "inline_search_seed_worker_count": int(metrics.get("inline_search_seed_worker_count") or 0),
        "inline_harvest_prefetch_worker_count": int(metrics.get("inline_harvest_prefetch_worker_count") or 0),
        "background_reconcile_job_count": int(metrics.get("background_reconcile_job_count") or 0),
        "background_company_roster_reconcile_job_count": int(
            metrics.get("background_company_roster_reconcile_job_count") or 0
        ),
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


def worker_has_inline_incremental_ingest_output(worker: dict[str, Any]) -> bool:
    output = dict(worker.get("output") or {})
    inline_ingest = dict(output.get("inline_incremental_ingest") or {})
    return bool(str(inline_ingest.get("applied_at") or "").strip())


def worker_has_completed_background_harvest_prefetch(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") != "enrichment_specialist":
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    if worker_has_inline_incremental_ingest_output(worker):
        return False
    metadata = dict(worker.get("metadata") or {})
    return str(metadata.get("recovery_kind") or "") == "harvest_profile_batch"


def worker_has_completed_background_company_roster_output(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") != "acquisition_specialist":
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    if worker_has_inline_incremental_ingest_output(worker):
        return False
    metadata = dict(worker.get("metadata") or {})
    if str(metadata.get("recovery_kind") or "").strip() != "harvest_company_employees":
        return False
    output = dict(worker.get("output") or {})
    summary = dict(output.get("summary") or {})
    return bool(summary)


def worker_has_completed_background_search_output(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") not in {"search_planner", "public_media_specialist"}:
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    output = dict(worker.get("output") or {})
    if dict(output.get("summary") or {}):
        return True
    return any(isinstance(item, dict) for item in list(output.get("entries") or []))


def _worker_snapshot_dir_candidates(worker: dict[str, Any]) -> list[Path]:
    output = dict(worker.get("output") or {})
    summary = dict(output.get("summary") or {})
    metadata = dict(worker.get("metadata") or {})
    path_values = [
        str(summary.get("root_snapshot_dir") or "").strip(),
        str(metadata.get("root_snapshot_dir") or "").strip(),
        str(summary.get("snapshot_dir") or "").strip(),
        str(metadata.get("snapshot_dir") or "").strip(),
    ]
    candidates: list[Path] = []
    seen: set[str] = set()
    for value in path_values:
        if not value:
            continue
        normalized = str(Path(value).expanduser())
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(Path(normalized))
    return candidates


def resolve_reconcile_snapshot_id(
    job_summary: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    job_result_view: dict[str, Any] | None = None,
) -> str:
    result_view = dict(job_result_view or {})
    snapshot_id = str(result_view.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    candidate_source = dict(job_summary.get("candidate_source") or {})
    result_view = dict(candidate_source.get("result_view") or {})
    snapshot_id = str(result_view.get("snapshot_id") or candidate_source.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    reconcile_state = dict(job_summary.get("background_reconcile") or {})
    snapshot_id = str(reconcile_state.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    for worker in workers:
        for snapshot_dir in _worker_snapshot_dir_candidates(worker):
            if snapshot_dir.name:
                return snapshot_dir.name
    return ""


def resolve_reconcile_snapshot_dir(
    runtime_dir: Path,
    target_company: str,
    job_summary: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    job_result_view: dict[str, Any] | None = None,
) -> Path | None:
    for worker in workers:
        for snapshot_dir in _worker_snapshot_dir_candidates(worker):
            if snapshot_dir.exists():
                return snapshot_dir
    result_view = dict(job_result_view or {})
    source_path = str(result_view.get("source_path") or "").strip()
    if source_path:
        snapshot_dir = resolve_snapshot_dir_from_source_path(
            source_path,
            runtime_dir=runtime_dir,
        )
        if snapshot_dir is not None and snapshot_dir.exists():
            return snapshot_dir
    snapshot_id = resolve_reconcile_snapshot_id(
        job_summary,
        workers,
        job_result_view=result_view,
    )
    if not snapshot_id:
        return None
    company_key = normalize_name_token(target_company)
    if not company_key:
        return None
    candidate = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=company_key,
        snapshot_id=snapshot_id,
    )
    return candidate if candidate is not None and candidate.exists() else None


def _worker_is_terminal_for_acquisition_resume(worker: dict[str, Any]) -> bool:
    status = str(worker.get("status") or "").strip().lower()
    return status in {"completed", "failed", "interrupted", "cancelled", "canceled", "superseded"}
