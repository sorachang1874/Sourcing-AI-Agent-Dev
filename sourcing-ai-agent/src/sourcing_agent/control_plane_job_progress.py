from __future__ import annotations

from typing import Any

_PROGRESS_EVENT_METRIC_KEYS = {
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


def merge_progress_event_metrics(
    metrics: dict[str, Any] | None,
    *,
    event_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(metrics or {})
    payload = dict(event_payload or {})
    for key in _PROGRESS_EVENT_METRIC_KEYS:
        value = payload.get(key)
        if value is None or value == "" or value == []:
            continue
        merged[key] = value
    artifact_paths = payload.get("artifact_paths")
    if isinstance(artifact_paths, dict) and artifact_paths:
        merged["artifact_paths"] = dict(artifact_paths)
    return merged


def build_job_progress_event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    stage_sequence: list[str] = []
    stage_stats: dict[str, dict[str, Any]] = {}
    latest_event: dict[str, Any] = {}
    latest_metrics: dict[str, Any] = {}
    for event in events:
        latest_event = {
            "event_id": int(event.get("event_id") or 0),
            "stage": str(event.get("stage") or ""),
            "status": str(event.get("status") or ""),
            "detail": str(event.get("detail") or ""),
            "payload": dict(event.get("payload") or {}),
            "created_at": str(event.get("created_at") or ""),
        }
        latest_metrics = merge_progress_event_metrics(latest_metrics, event_payload=latest_event.get("payload"))
        stage = str(event.get("stage") or "").strip()
        if not stage or stage in {"runtime_control", "runtime_heartbeat"}:
            continue
        if stage not in stage_sequence:
            stage_sequence.append(stage)
        stats = stage_stats.setdefault(
            stage,
            {
                "event_count": 0,
                "started_at": str(event.get("created_at") or ""),
                "latest_at": "",
                "latest_status": "",
                "latest_detail": "",
                "latest_payload": {},
                "completed_at": "",
            },
        )
        stats["event_count"] = int(stats.get("event_count") or 0) + 1
        stats["latest_at"] = str(event.get("created_at") or "")
        stats["latest_status"] = str(event.get("status") or "")
        stats["latest_detail"] = str(event.get("detail") or "")
        stats["latest_payload"] = dict(event.get("payload") or {})
        if str(event.get("status") or "") in {"completed", "failed"}:
            stats["completed_at"] = str(event.get("created_at") or "")
    return {
        "event_count": len(events),
        "latest_event": latest_event,
        "stage_sequence": stage_sequence,
        "stage_stats": stage_stats,
        "latest_metrics": latest_metrics,
    }


def update_job_progress_event_summary(
    summary: dict[str, Any] | None,
    *,
    event: dict[str, Any],
) -> dict[str, Any]:
    updated = {
        "event_count": int(dict(summary or {}).get("event_count") or 0),
        "latest_event": dict(dict(summary or {}).get("latest_event") or {}),
        "stage_sequence": [
            str(item).strip() for item in list(dict(summary or {}).get("stage_sequence") or []) if str(item).strip()
        ],
        "stage_stats": dict(dict(summary or {}).get("stage_stats") or {}),
        "latest_metrics": dict(dict(summary or {}).get("latest_metrics") or {}),
    }
    updated["event_count"] = int(updated.get("event_count") or 0) + 1
    updated["latest_event"] = {
        "event_id": int(event.get("event_id") or 0),
        "stage": str(event.get("stage") or ""),
        "status": str(event.get("status") or ""),
        "detail": str(event.get("detail") or ""),
        "payload": dict(event.get("payload") or {}),
        "created_at": str(event.get("created_at") or ""),
    }
    updated["latest_metrics"] = merge_progress_event_metrics(
        dict(updated.get("latest_metrics") or {}),
        event_payload=updated["latest_event"].get("payload"),
    )
    stage = str(event.get("stage") or "").strip()
    if stage and stage not in {"runtime_control", "runtime_heartbeat"}:
        stage_sequence = list(updated.get("stage_sequence") or [])
        if stage not in stage_sequence:
            stage_sequence.append(stage)
        updated["stage_sequence"] = stage_sequence
        stage_stats = dict(updated.get("stage_stats") or {})
        stats = dict(stage_stats.get(stage) or {})
        if not str(stats.get("started_at") or "").strip():
            stats["started_at"] = str(event.get("created_at") or "")
        stats["event_count"] = int(stats.get("event_count") or 0) + 1
        stats["latest_at"] = str(event.get("created_at") or "")
        stats["latest_status"] = str(event.get("status") or "")
        stats["latest_detail"] = str(event.get("detail") or "")
        stats["latest_payload"] = dict(event.get("payload") or {})
        if str(event.get("status") or "") in {"completed", "failed"}:
            stats["completed_at"] = str(event.get("created_at") or "")
        else:
            stats.setdefault("completed_at", str(stats.get("completed_at") or ""))
        stage_stats[stage] = stats
        updated["stage_stats"] = stage_stats
    return updated
