from __future__ import annotations

from typing import Any


MANAGED_WORKFLOW_RUNTIME_MODES = frozenset(
    {"managed_subprocess", "runner_managed", "detached_sidecar"}
)


def normalize_workflow_runtime_mode(value: Any, *, default: str = "hosted") -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "hosted":
        return "hosted"
    if normalized in MANAGED_WORKFLOW_RUNTIME_MODES:
        return normalized
    return str(default or "hosted").strip().lower() or "hosted"


def workflow_runtime_uses_managed_runner(value: Any) -> bool:
    return normalize_workflow_runtime_mode(value) in MANAGED_WORKFLOW_RUNTIME_MODES


def normalize_workflow_submission_payload(
    payload: dict[str, Any] | None,
    *,
    default_runtime_execution_mode: str = "hosted",
    hosted_auto_job_daemon: bool = False,
    managed_auto_job_daemon: bool = True,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    runtime_execution_mode = normalize_workflow_runtime_mode(
        normalized.get("runtime_execution_mode") or normalized.get("workflow_runner_mode"),
        default=default_runtime_execution_mode,
    )
    normalized["runtime_execution_mode"] = runtime_execution_mode
    normalized.setdefault("analysis_stage_mode", "single_stage")
    if workflow_runtime_uses_managed_runner(runtime_execution_mode):
        normalized["auto_job_daemon"] = bool(
            normalized.get("auto_job_daemon", managed_auto_job_daemon)
        )
    else:
        normalized["auto_job_daemon"] = bool(
            normalized.get("auto_job_daemon", hosted_auto_job_daemon)
        )
    return normalized
