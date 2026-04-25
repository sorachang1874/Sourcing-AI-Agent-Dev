from __future__ import annotations

from collections import Counter
from typing import Any

from .runtime_tuning import (
    resolved_lane_budget_caps,
    resolved_parallel_exploration_workers,
    resolved_parallel_search_workers,
)


LANE_PRIORITY = {
    "public_media_specialist": 90,
    "search_planner": 80,
    "exploration_specialist": 70,
    "acquisition_specialist": 60,
    "enrichment_specialist": 50,
    "retrieval_specialist": 40,
    "review_specialist": 30,
    "triage_planner": 20,
}

RESUME_PRIORITY = {
    "resume_from_checkpoint": 40,
    "reuse_checkpoint": 30,
    "retry_after_failure": 20,
    "fresh_start": 10,
    "already_running": 0,
}

REMOTE_WAIT_STAGES = {"waiting_remote_search", "waiting_remote_harvest"}


def _plan_runtime_tuning_context(plan_payload: dict[str, Any]) -> dict[str, Any]:
    task_preferences: list[dict[str, Any]] = []
    for task in list(plan_payload.get("acquisition_tasks") or []):
        if not isinstance(task, dict):
            continue
        metadata = dict(task.get("metadata") or {})
        intent_view = dict(metadata.get("intent_view") or {})
        execution_preferences = dict(intent_view.get("execution_preferences") or {})
        if execution_preferences:
            task_preferences.append(execution_preferences)
    merged: dict[str, Any] = {}
    for candidate in (
        dict(plan_payload.get("execution_preferences") or {}),
        dict(plan_payload.get("request_intent_view") or {}).get("execution_preferences") or {},
        dict(plan_payload.get("intent_view") or {}).get("execution_preferences") or {},
        *(task_preferences),
    ):
        if not isinstance(candidate, dict):
            continue
        for key, value in candidate.items():
            if key not in merged and value not in (None, "", []):
                merged[key] = value
    return merged


def lane_limits_from_plan(plan_payload: dict[str, Any]) -> dict[str, int]:
    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
    runtime_context = _plan_runtime_tuning_context(plan_payload)
    search_limit = resolved_parallel_search_workers(
        runtime_context,
        cost_policy=cost_policy,
        default=3,
    )
    exploration_limit = resolved_parallel_exploration_workers(
        runtime_context,
        cost_policy=cost_policy,
        default=2,
    )
    return {
        "search_planner": search_limit,
        "public_media_specialist": search_limit,
        "exploration_specialist": exploration_limit,
    }


def lane_budget_caps_from_plan(plan_payload: dict[str, Any]) -> dict[str, int]:
    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
    runtime_context = _plan_runtime_tuning_context(plan_payload)
    return resolved_lane_budget_caps(runtime_context, cost_policy=cost_policy)


def summarize_scheduler(
    *,
    plan_payload: dict[str, Any],
    workers: list[dict[str, Any]],
) -> dict[str, Any]:
    limits = lane_limits_from_plan(plan_payload)
    budget_caps = lane_budget_caps_from_plan(plan_payload)
    lane_counter: dict[str, Counter[str]] = {}
    resumable: list[dict[str, Any]] = []
    for worker in workers:
        lane_id = str(worker.get("lane_id") or "")
        status = effective_worker_status(worker)
        lane_counter.setdefault(lane_id, Counter()).update([status or "unknown"])
        resume_mode = infer_resume_mode(worker)
        if resume_mode != "already_running":
            resumable.append(
                {
                    "worker_id": int(worker.get("worker_id") or 0),
                    "lane_id": lane_id,
                    "worker_key": str(worker.get("worker_key") or ""),
                    "status": status,
                    "raw_status": str(worker.get("status") or ""),
                    "wait_stage": wait_stage(worker),
                    "resume_mode": resume_mode,
                    "checkpoint_keys": sorted(list(dict(worker.get("checkpoint") or {}).keys())),
                    "priority_score": _priority_score(lane_id, resume_mode, int(worker.get("worker_id") or 0)),
                }
            )
    resumable.sort(key=lambda item: (-item["priority_score"], item["worker_id"]))
    lane_summary = []
    for lane_id in sorted(lane_counter):
        counter = lane_counter[lane_id]
        lane_summary.append(
            {
                "lane_id": lane_id,
                "limit": int(limits.get(lane_id, 1)),
                "budget_cap": int(budget_caps.get(lane_id, 1)),
                "queued": int(counter.get("queued", 0))
                + int(counter.get("waiting_remote_search", 0))
                + int(counter.get("waiting_remote_harvest", 0)),
                "running": int(counter.get("running", 0)),
                "interrupted": int(counter.get("interrupted", 0)),
                "failed": int(counter.get("failed", 0)),
                "completed": int(counter.get("completed", 0)),
                "waiting_remote_search": int(counter.get("waiting_remote_search", 0)),
                "waiting_remote_harvest": int(counter.get("waiting_remote_harvest", 0)),
            }
        )
    return {
        "lane_limits": limits,
        "lane_budget_caps": budget_caps,
        "lane_summary": lane_summary,
        "resumable_workers": resumable,
    }


def schedule_work_specs(
    specs: list[dict[str, Any]],
    *,
    existing_workers: list[dict[str, Any]],
    lane_limits: dict[str, int],
    total_limit: int,
) -> dict[str, Any]:
    existing_map = {
        (str(item.get("lane_id") or ""), str(item.get("worker_key") or "")): item for item in existing_workers
    }
    annotated: list[dict[str, Any]] = []
    for spec in specs:
        lane_id = str(spec.get("lane_id") or "")
        worker_key = str(spec.get("worker_key") or "")
        existing = existing_map.get((lane_id, worker_key))
        resume_mode = infer_resume_mode(existing)
        annotated.append(
            {
                **spec,
                "existing_worker_id": int((existing or {}).get("worker_id") or 0),
                "existing_status": str((existing or {}).get("status") or ""),
                "resume_mode": resume_mode,
                "checkpoint_keys": sorted(list(dict((existing or {}).get("checkpoint") or {}).keys())),
                "priority_score": _priority_score(lane_id, resume_mode, int(spec.get("index") or 0)),
                "is_runnable": resume_mode != "already_running",
            }
        )
    runnable = sorted(
        [item for item in annotated if item["is_runnable"]],
        key=lambda item: (-item["priority_score"], int(item.get("index") or 0)),
    )
    selected: list[dict[str, Any]] = []
    lane_counts: Counter[str] = Counter()
    for item in runnable:
        lane_id = str(item.get("lane_id") or "")
        if len(selected) >= total_limit:
            break
        if lane_counts[lane_id] >= int(lane_limits.get(lane_id, total_limit) or total_limit):
            continue
        selected.append(item)
        lane_counts[lane_id] += 1
    selected_keys = {(str(item.get("lane_id") or ""), str(item.get("worker_key") or "")) for item in selected}
    backlog = [
        item
        for item in runnable
        if (str(item.get("lane_id") or ""), str(item.get("worker_key") or "")) not in selected_keys
    ]
    skipped = [item for item in annotated if not item["is_runnable"]]
    return {
        "selected": selected,
        "backlog": backlog,
        "skipped": skipped,
        "lane_limits": lane_limits,
    }


def infer_resume_mode(worker: dict[str, Any] | None) -> str:
    if not worker:
        return "fresh_start"
    status = str(worker.get("status") or "")
    if status == "running" and not is_remote_waiting_worker(worker):
        return "already_running"
    checkpoint = dict(worker.get("checkpoint") or {})
    if status == "interrupted":
        return "resume_from_checkpoint"
    if status == "failed":
        return "retry_after_failure"
    if status == "completed":
        return "reuse_checkpoint" if checkpoint else "fresh_start"
    if checkpoint:
        return "resume_from_checkpoint"
    return "fresh_start"


def wait_stage(worker: dict[str, Any] | None) -> str:
    checkpoint = dict((worker or {}).get("checkpoint") or {})
    stage = str(checkpoint.get("stage") or "").strip()
    if stage in REMOTE_WAIT_STAGES:
        return stage
    return ""


def is_remote_waiting_worker(worker: dict[str, Any] | None) -> bool:
    return bool(wait_stage(worker))


def effective_worker_status(worker: dict[str, Any] | None) -> str:
    if not worker:
        return "unknown"
    if is_remote_waiting_worker(worker):
        return wait_stage(worker)
    return str(worker.get("status") or "unknown")


def _priority_score(lane_id: str, resume_mode: str, ordinal: int) -> int:
    lane_priority = int(LANE_PRIORITY.get(lane_id, 10))
    resume_priority = int(RESUME_PRIORITY.get(resume_mode, 0))
    order_bonus = max(0, 1000 - ordinal)
    return lane_priority * 10000 + resume_priority * 100 + order_bonus
