from __future__ import annotations

import os
import uuid
from typing import Any


def build_job_scoped_recovery_config(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    def _resolve_int(*keys: str, default: int) -> int:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None or value == "":
                continue
            return int(value)
        return int(default)

    service_name = str(payload.get("job_recovery_service_name") or f"job-recovery-{job_id}").strip() or f"job-recovery-{job_id}"
    return {
        "service_name": service_name,
        "poll_seconds": max(0.5, float(payload.get("job_recovery_poll_seconds") or 2.0)),
        "max_ticks": max(0, int(payload.get("job_recovery_max_ticks") or 0)),
        "lease_seconds": int(payload.get("job_recovery_lease_seconds") or 300),
        "stale_after_seconds": int(payload.get("job_recovery_stale_after_seconds") or 90),
        "total_limit": int(payload.get("job_recovery_total_limit") or 8),
        "startup_timeout_seconds": max(0.2, float(payload.get("job_recovery_startup_timeout_seconds") or 1.0)),
        "startup_poll_seconds": max(0.05, float(payload.get("job_recovery_startup_poll_seconds") or 0.1)),
        "workflow_resume_stale_after_seconds": max(
            0,
            _resolve_int(
                "job_recovery_workflow_resume_stale_after_seconds",
                "workflow_resume_stale_after_seconds",
                default=180,
            ),
        ),
        "workflow_queue_resume_stale_after_seconds": max(
            0,
            _resolve_int(
                "job_recovery_workflow_queue_resume_stale_after_seconds",
                "workflow_queue_resume_stale_after_seconds",
                default=180,
            ),
        ),
    }


def build_shared_recovery_config(payload: dict[str, Any]) -> dict[str, Any]:
    def _resolve_int(*keys: str, default: int) -> int:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None or value == "":
                continue
            return int(value)
        return int(default)

    return {
        "service_name": str(payload.get("shared_recovery_service_name") or "worker-recovery-daemon").strip()
        or "worker-recovery-daemon",
        "poll_seconds": max(0.5, float(payload.get("shared_recovery_poll_seconds") or 5.0)),
        "max_ticks": max(0, int(payload.get("shared_recovery_max_ticks") or 0)),
        "lease_seconds": int(payload.get("shared_recovery_lease_seconds") or 300),
        "stale_after_seconds": int(payload.get("shared_recovery_stale_after_seconds") or 90),
        "total_limit": int(payload.get("shared_recovery_total_limit") or 8),
        "startup_timeout_seconds": max(0.2, float(payload.get("shared_recovery_startup_timeout_seconds") or 1.5)),
        "startup_poll_seconds": max(0.05, float(payload.get("shared_recovery_startup_poll_seconds") or 0.1)),
        "workflow_resume_stale_after_seconds": max(
            0,
            _resolve_int(
                "shared_recovery_workflow_resume_stale_after_seconds",
                "workflow_resume_stale_after_seconds",
                default=180,
            ),
        ),
        "workflow_queue_resume_stale_after_seconds": max(
            0,
            _resolve_int(
                "shared_recovery_workflow_queue_resume_stale_after_seconds",
                "workflow_queue_resume_stale_after_seconds",
                default=180,
            ),
        ),
        "workflow_resume_limit": max(1, int(payload.get("shared_recovery_workflow_resume_limit") or 10)),
        "workflow_queue_resume_limit": max(1, int(payload.get("shared_recovery_workflow_queue_resume_limit") or 10)),
    }


def build_hosted_runtime_watchdog_config(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "service_name": str(payload.get("hosted_runtime_watchdog_service_name") or "server-runtime-watchdog").strip()
        or "server-runtime-watchdog",
        "shared_service_name": str(payload.get("shared_service_name") or "worker-recovery-daemon").strip()
        or "worker-recovery-daemon",
        "poll_seconds": max(
            1.0,
            float(
                payload.get("hosted_runtime_watchdog_poll_seconds")
                or payload.get("runtime_watchdog_poll_seconds")
                or 15.0
            ),
        ),
        "max_ticks": max(0, int(payload.get("hosted_runtime_watchdog_max_ticks") or 0)),
        "startup_timeout_seconds": max(
            0.2,
            float(payload.get("hosted_runtime_watchdog_startup_timeout_seconds") or 1.5),
        ),
        "startup_poll_seconds": max(
            0.05,
            float(payload.get("hosted_runtime_watchdog_startup_poll_seconds") or 0.1),
        ),
    }


def build_job_scoped_recovery_callback_payload(
    job_id: str,
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = dict(config or build_job_scoped_recovery_config(job_id, payload))
    return {
        "job_id": job_id,
        "workflow_auto_resume_enabled": True,
        "workflow_resume_explicit_job": False,
        "workflow_stale_scope_job_id": job_id,
        "workflow_resume_stale_after_seconds": int(resolved["workflow_resume_stale_after_seconds"]),
        "workflow_resume_limit": max(1, int(payload.get("job_recovery_workflow_resume_limit") or 1)),
        "workflow_queue_auto_takeover_enabled": True,
        "workflow_queue_resume_stale_after_seconds": int(resolved["workflow_queue_resume_stale_after_seconds"]),
        "workflow_queue_resume_limit": max(1, int(payload.get("job_recovery_workflow_queue_resume_limit") or 1)),
        "runtime_heartbeat_source": "job_recovery_daemon",
        "runtime_heartbeat_interval_seconds": int(
            payload.get("runtime_heartbeat_interval_seconds")
            or _env_int("WORKFLOW_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60)
        ),
        "runtime_heartbeat_service_name": str(resolved.get("service_name") or f"job-recovery-{job_id}"),
    }


def build_shared_recovery_callback_payload(
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = dict(config or build_shared_recovery_config(payload))
    return {
        "workflow_auto_resume_enabled": True,
        "workflow_resume_explicit_job": False,
        "workflow_resume_stale_after_seconds": int(resolved["workflow_resume_stale_after_seconds"]),
        "workflow_resume_limit": int(resolved["workflow_resume_limit"]),
        "workflow_queue_auto_takeover_enabled": True,
        "workflow_queue_resume_stale_after_seconds": int(resolved["workflow_queue_resume_stale_after_seconds"]),
        "workflow_queue_resume_limit": int(resolved["workflow_queue_resume_limit"]),
        "runtime_heartbeat_source": "shared_recovery_daemon",
        "runtime_heartbeat_interval_seconds": int(
            payload.get("runtime_heartbeat_interval_seconds")
            or _env_int("WORKFLOW_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60)
        ),
        "runtime_heartbeat_service_name": str(resolved.get("service_name") or "worker-recovery-daemon"),
    }


def build_recovery_bootstrap_payload(
    payload: dict[str, Any],
    *,
    config: dict[str, Any],
    scope: str,
    job_id: str = "",
) -> dict[str, Any]:
    bootstrap_payload = {
        "owner_id": f"{scope}-bootstrap-{uuid.uuid4().hex[:8]}",
        "lease_seconds": int(config["lease_seconds"]),
        "stale_after_seconds": int(config["stale_after_seconds"]),
        "total_limit": int(config["total_limit"]),
    }
    if scope == "job_scoped" and job_id:
        bootstrap_payload.update(build_job_scoped_recovery_callback_payload(job_id, payload, config=config))
    else:
        bootstrap_payload.update(build_shared_recovery_callback_payload(payload, config=config))
    return bootstrap_payload


def build_job_scoped_recovery_command(job_id: str, *, config: dict[str, Any], python_executable: str) -> list[str]:
    return [
        python_executable,
        "-m",
        "sourcing_agent.cli",
        "run-worker-daemon-service",
        "--service-name",
        str(config["service_name"]),
        "--job-id",
        job_id,
        "--job-scoped",
        "--poll-seconds",
        f"{float(config['poll_seconds']):g}",
        "--lease-seconds",
        str(int(config["lease_seconds"])),
        "--stale-after-seconds",
        str(int(config["stale_after_seconds"])),
        "--total-limit",
        str(int(config["total_limit"])),
        "--max-ticks",
        str(int(config["max_ticks"])),
        "--workflow-auto-resume-stale-after-seconds",
        str(int(config["workflow_resume_stale_after_seconds"])),
        "--workflow-queue-auto-takeover-stale-after-seconds",
        str(int(config["workflow_queue_resume_stale_after_seconds"])),
    ]


def build_shared_recovery_command(*, config: dict[str, Any], python_executable: str) -> list[str]:
    return [
        python_executable,
        "-m",
        "sourcing_agent.cli",
        "run-worker-daemon-service",
        "--service-name",
        str(config["service_name"]),
        "--poll-seconds",
        f"{float(config['poll_seconds']):g}",
        "--lease-seconds",
        str(int(config["lease_seconds"])),
        "--stale-after-seconds",
        str(int(config["stale_after_seconds"])),
        "--total-limit",
        str(int(config["total_limit"])),
        "--max-ticks",
        str(int(config["max_ticks"])),
        "--workflow-auto-resume-stale-after-seconds",
        str(int(config["workflow_resume_stale_after_seconds"])),
        "--workflow-queue-auto-takeover-stale-after-seconds",
        str(int(config["workflow_queue_resume_stale_after_seconds"])),
    ]


def build_hosted_runtime_watchdog_command(*, config: dict[str, Any], python_executable: str) -> list[str]:
    return [
        python_executable,
        "-m",
        "sourcing_agent.cli",
        "run-server-runtime-watchdog-service",
        "--service-name",
        str(config["service_name"]),
        "--shared-service-name",
        str(config["shared_service_name"]),
        "--poll-seconds",
        f"{float(config['poll_seconds']):g}",
        "--max-ticks",
        str(int(config["max_ticks"])),
    ]


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default
