from __future__ import annotations

from typing import Any

TERMINAL_APIFY_EVENT_TYPES = {
    "ACTOR.RUN.SUCCEEDED",
    "ACTOR.RUN.FAILED",
    "ACTOR.RUN.TIMED_OUT",
    "ACTOR.RUN.ABORTED",
}


def normalize_remote_provider_event(payload: dict[str, Any] | None, *, provider: str = "apify") -> dict[str, Any]:
    """Normalize provider callback payloads into the runtime event contract."""
    raw_payload = dict(payload or {})
    event_data = _first_mapping(
        raw_payload.get("eventData"),
        raw_payload.get("event_data"),
        raw_payload.get("resource"),
        raw_payload.get("data"),
        raw_payload.get("actorRun"),
        raw_payload.get("actor_run"),
    )
    merged = {**event_data, **raw_payload}
    run_payload = _first_mapping(
        merged.get("actorRun"),
        merged.get("actor_run"),
        merged.get("run"),
        merged.get("resource"),
        merged.get("data"),
    )
    merged = {**run_payload, **event_data, **raw_payload}
    event_type = _first_string(merged, "eventType", "event_type", "type")
    run_id = _first_string(merged, "actorRunId", "actor_run_id", "runId", "run_id", "id")
    actor_id = _first_string(merged, "actorId", "actor_id", "actId", "act_id")
    dataset_id = _first_string(
        merged,
        "defaultDatasetId",
        "default_dataset_id",
        "datasetId",
        "dataset_id",
    )
    status = _first_string(merged, "status", "runStatus", "run_status").upper()
    if not status:
        status = _status_from_apify_event_type(event_type)
    event_created_at = _first_string(
        merged,
        "eventCreatedAt",
        "event_created_at",
        "createdAt",
        "created_at",
    )
    run_started_at = _first_string(merged, "startedAt", "started_at")
    run_finished_at = _first_string(
        merged,
        "finishedAt",
        "finished_at",
        "endedAt",
        "ended_at",
    )
    return {
        "provider": str(provider or "apify").strip().lower() or "apify",
        "event_type": event_type,
        "run_id": run_id,
        "actor_id": actor_id,
        "dataset_id": dataset_id,
        "status": status,
        "is_terminal": _event_is_terminal(event_type=event_type, status=status),
        "event_created_at": event_created_at,
        "run_started_at": run_started_at,
        "run_finished_at": run_finished_at,
        "remote_completed_at": run_finished_at or event_created_at,
        "raw_payload": raw_payload,
    }


def remote_provider_event_matches_worker(event: dict[str, Any], worker: dict[str, Any]) -> bool:
    checkpoint = dict(worker.get("checkpoint") or {})
    event_run_id = str(event.get("run_id") or "").strip()
    event_dataset_id = str(event.get("dataset_id") or "").strip()
    worker_run_id = str(checkpoint.get("run_id") or "").strip()
    worker_dataset_id = str(checkpoint.get("dataset_id") or checkpoint.get("default_dataset_id") or "").strip()
    if event_run_id and worker_run_id and event_run_id == worker_run_id:
        return True
    return bool(event_dataset_id and worker_dataset_id and event_dataset_id == worker_dataset_id)


def collect_remote_provider_event_targets(
    workers: list[dict[str, Any]],
    event: dict[str, Any],
    *,
    enabled_lanes: set[str] | None = None,
    lane_resolver,
) -> dict[str, Any]:
    enabled = set(enabled_lanes or {"linkedin_stage_1"})
    job_ids: list[str] = []
    worker_ids: list[int] = []
    lane_counts: dict[str, int] = {}
    for worker in list(workers or []):
        if not remote_provider_event_matches_worker(event, dict(worker)):
            continue
        event_lane = str(lane_resolver(dict(worker)) or "").strip()
        if not event_lane or event_lane not in enabled:
            continue
        job_id = str(worker.get("job_id") or "").strip()
        worker_id = int(worker.get("worker_id") or 0)
        if job_id and job_id not in job_ids:
            job_ids.append(job_id)
        if worker_id > 0:
            worker_ids.append(worker_id)
        lane_counts[event_lane] = int(lane_counts.get(event_lane, 0)) + 1
    return {
        "job_ids": job_ids,
        "worker_ids": worker_ids,
        "worker_count": len(worker_ids),
        "lane_counts": lane_counts,
    }


def _first_mapping(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict):
            return dict(value)
    return {}


def _first_string(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return ""


def _status_from_apify_event_type(event_type: str) -> str:
    normalized = str(event_type or "").strip().upper()
    if normalized == "ACTOR.RUN.SUCCEEDED":
        return "SUCCEEDED"
    if normalized == "ACTOR.RUN.FAILED":
        return "FAILED"
    if normalized == "ACTOR.RUN.TIMED_OUT":
        return "TIMED-OUT"
    if normalized == "ACTOR.RUN.ABORTED":
        return "ABORTED"
    return ""


def _event_is_terminal(*, event_type: str, status: str) -> bool:
    normalized_event_type = str(event_type or "").strip().upper()
    normalized_status = str(status or "").strip().upper()
    return normalized_event_type in TERMINAL_APIFY_EVENT_TYPES or normalized_status in {
        "SUCCEEDED",
        "FAILED",
        "TIMED-OUT",
        "TIMED_OUT",
        "ABORTED",
    }
