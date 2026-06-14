from __future__ import annotations

import os
import uuid
from typing import Any

from .recovery_contract import remote_provider_event_recovery_total_limit


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_worker_ids(value: Any) -> list[int]:
    raw_values = value if isinstance(value, list) else [value]
    worker_ids: list[int] = []
    for item in raw_values:
        try:
            worker_id = int(item or 0)
        except (TypeError, ValueError):
            continue
        if worker_id > 0 and worker_id not in worker_ids:
            worker_ids.append(worker_id)
    return worker_ids


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

    def _resolve_float(*keys: str, default: float) -> float:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None or value == "":
                continue
            return float(value)
        return float(default)

    service_name = (
        str(payload.get("job_recovery_service_name") or payload.get("service_name") or f"job-recovery-{job_id}").strip()
        or f"job-recovery-{job_id}"
    )
    explicit_worker_ids = _normalize_worker_ids(payload.get("explicit_worker_ids"))
    for worker_id in _normalize_worker_ids(payload.get("remote_provider_event_worker_ids")):
        if worker_id not in explicit_worker_ids:
            explicit_worker_ids.append(worker_id)
    remote_event_worker_ids = _normalize_worker_ids(payload.get("remote_provider_event_worker_ids"))
    explicit_worker_limit_default = (
        min(
            max(1, len(explicit_worker_ids)),
            remote_provider_event_recovery_total_limit(),
        )
        if remote_event_worker_ids
        else (max(1, len(explicit_worker_ids)) if explicit_worker_ids else 8)
    )
    return {
        "service_name": service_name,
        "poll_seconds": max(0.5, _resolve_float("job_recovery_poll_seconds", "poll_seconds", default=2.0)),
        "max_ticks": max(0, _resolve_int("job_recovery_max_ticks", "max_ticks", default=0)),
        "idle_stop_ticks": max(
            0,
            _resolve_int("job_recovery_idle_stop_ticks", "idle_stop_ticks", default=0),
        ),
        "lease_seconds": _resolve_int("job_recovery_lease_seconds", "lease_seconds", default=300),
        "stale_after_seconds": _resolve_int("job_recovery_stale_after_seconds", "stale_after_seconds", default=90),
        "total_limit": _resolve_int(
            "job_recovery_total_limit",
            "total_limit",
            default=explicit_worker_limit_default,
        ),
        "explicit_job_followup_rounds": max(
            0,
            _resolve_int(
                "job_recovery_explicit_job_followup_rounds",
                "explicit_job_followup_rounds",
                default=0,
            ),
        ),
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
        "workflow_resume_explicit_job": _coerce_bool(
            payload.get("job_recovery_workflow_resume_explicit_job", payload.get("workflow_resume_explicit_job")),
            True,
        ),
        "workflow_queue_resume_stale_after_seconds": max(
            0,
            _resolve_int(
                "job_recovery_workflow_queue_resume_stale_after_seconds",
                "workflow_queue_resume_stale_after_seconds",
                default=180,
            ),
        ),
        "explicit_worker_ids": explicit_worker_ids,
        "force_release_explicit_worker_leases": _coerce_bool(
            payload.get("force_release_explicit_worker_leases"),
            False,
        ),
        "profile_prefetch_nonblocking_submit": _coerce_bool(
            payload.get("profile_prefetch_nonblocking_submit"),
            True,
        ),
        "profile_prefetch_refill_enabled": _coerce_bool(
            payload.get("profile_prefetch_refill_enabled"),
            True,
        ),
        "profile_prefetch_refill_before_worker_recovery": _coerce_bool(
            payload.get("profile_prefetch_refill_before_worker_recovery"),
            False,
        ),
        "remote_event_followup_enabled": _coerce_bool(
            payload.get("remote_event_followup_enabled"),
            True,
        ),
        "search_seed_discovery_enabled": _coerce_bool(
            payload.get("search_seed_discovery_enabled"),
            False,
        ),
        "snapshot_full_materialization_enabled": _coerce_bool(
            payload.get("snapshot_full_materialization_enabled"),
            False,
        ),
        "projection_facet_layering_enabled": _coerce_bool(
            payload.get("projection_facet_layering_enabled"),
            False,
        ),
        "excel_intake_recovery_enabled": _coerce_bool(
            payload.get("excel_intake_recovery_enabled"),
            False,
        ),
        "post_recovery_housekeeping_enabled": _coerce_bool(
            payload.get("post_recovery_housekeeping_enabled"),
            False,
        ),
        "workflow_auto_resume_enabled": _coerce_bool(
            payload.get("workflow_auto_resume_enabled"),
            True,
        ),
        "workflow_queue_auto_takeover_enabled": _coerce_bool(
            payload.get("workflow_queue_auto_takeover_enabled"),
            True,
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
        # Step 5c: the shared recovery poll is now a BACKSTOP, not the primary
        # driver. Step 5b's event-signaled wakeup (request_service_wakeup on
        # durable commits) handles the latency-sensitive work sub-second, so the
        # poll only needs to catch the inherently-event-less conditions
        # (crash recovery, lease expiry, stuck state). 30s is comfortably inside
        # the stale_after (90s) and lease (300s) windows it must cover, while
        # cutting idle control-plane scan load ~6x vs the old 5s cadence.
        # Override via shared_recovery_poll_seconds for a tighter backstop.
        "poll_seconds": max(0.5, float(payload.get("shared_recovery_poll_seconds") or 30.0)),
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
    callback_payload = {
        "job_id": job_id,
        "workflow_auto_resume_enabled": True,
        "workflow_resume_explicit_job": _coerce_bool(resolved.get("workflow_resume_explicit_job"), True),
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
        "explicit_job_followup_rounds": int(resolved.get("explicit_job_followup_rounds") or 0),
    }
    idle_stop_ticks = int(resolved.get("idle_stop_ticks") or 0)
    if idle_stop_ticks > 0:
        callback_payload["job_recovery_idle_stop_ticks"] = idle_stop_ticks
    explicit_worker_ids = _normalize_worker_ids(resolved.get("explicit_worker_ids"))
    if explicit_worker_ids:
        callback_payload["explicit_worker_ids"] = explicit_worker_ids
        callback_payload["force_release_explicit_worker_leases"] = _coerce_bool(
            resolved.get("force_release_explicit_worker_leases"),
            False,
        )
    if _coerce_bool(resolved.get("profile_prefetch_nonblocking_submit"), False):
        callback_payload["profile_prefetch_nonblocking_submit"] = True
    callback_payload["profile_prefetch_refill_enabled"] = _coerce_bool(
        resolved.get("profile_prefetch_refill_enabled"),
        True,
    )
    callback_payload["profile_prefetch_refill_before_worker_recovery"] = _coerce_bool(
        resolved.get("profile_prefetch_refill_before_worker_recovery"),
        False,
    )
    callback_payload["remote_event_followup_enabled"] = _coerce_bool(
        resolved.get("remote_event_followup_enabled"),
        True,
    )
    callback_payload["search_seed_discovery_enabled"] = _coerce_bool(
        resolved.get("search_seed_discovery_enabled"),
        False,
    )
    callback_payload["snapshot_full_materialization_enabled"] = _coerce_bool(
        resolved.get("snapshot_full_materialization_enabled"),
        False,
    )
    callback_payload["projection_facet_layering_enabled"] = _coerce_bool(
        resolved.get("projection_facet_layering_enabled"),
        False,
    )
    callback_payload["excel_intake_recovery_enabled"] = _coerce_bool(
        resolved.get("excel_intake_recovery_enabled"),
        False,
    )
    callback_payload["post_recovery_housekeeping_enabled"] = _coerce_bool(
        resolved.get("post_recovery_housekeeping_enabled"),
        False,
    )
    callback_payload["workflow_auto_resume_enabled"] = _coerce_bool(
        resolved.get("workflow_auto_resume_enabled"),
        True,
    )
    callback_payload["workflow_queue_auto_takeover_enabled"] = _coerce_bool(
        resolved.get("workflow_queue_auto_takeover_enabled"),
        True,
    )
    return callback_payload


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
    command = [
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
        "--idle-stop-ticks",
        str(int(config.get("idle_stop_ticks") or 0)),
        "--workflow-auto-resume-stale-after-seconds",
        str(int(config["workflow_resume_stale_after_seconds"])),
        "--workflow-queue-auto-takeover-stale-after-seconds",
        str(int(config["workflow_queue_resume_stale_after_seconds"])),
    ]
    for worker_id in _normalize_worker_ids(config.get("explicit_worker_ids")):
        command.extend(["--explicit-worker-id", str(worker_id)])
    if _coerce_bool(config.get("force_release_explicit_worker_leases"), False):
        command.append("--force-release-explicit-worker-leases")
    if _coerce_bool(config.get("profile_prefetch_nonblocking_submit"), False):
        command.append("--profile-prefetch-nonblocking-submit")
    if not _coerce_bool(config.get("profile_prefetch_refill_enabled"), True):
        command.append("--disable-profile-prefetch-refill")
    if _coerce_bool(config.get("profile_prefetch_refill_before_worker_recovery"), False):
        command.append("--profile-prefetch-refill-before-worker-recovery")
    if not _coerce_bool(config.get("remote_event_followup_enabled"), True):
        command.append("--disable-remote-event-followup")
    if not _coerce_bool(config.get("search_seed_discovery_enabled"), True):
        command.append("--disable-search-seed-discovery")
    if not _coerce_bool(config.get("snapshot_full_materialization_enabled"), True):
        command.append("--disable-snapshot-full-materialization")
    if not _coerce_bool(config.get("projection_facet_layering_enabled"), True):
        command.append("--disable-projection-facet-layering")
    if not _coerce_bool(config.get("excel_intake_recovery_enabled"), True):
        command.append("--disable-excel-intake-recovery")
    if not _coerce_bool(config.get("post_recovery_housekeeping_enabled"), True):
        command.append("--disable-post-recovery-housekeeping")
    if not _coerce_bool(config.get("workflow_auto_resume_enabled"), True):
        command.append("--disable-workflow-auto-resume")
    if not _coerce_bool(config.get("workflow_resume_explicit_job"), True):
        command.append("--disable-workflow-explicit-job-resume")
    if not _coerce_bool(config.get("workflow_queue_auto_takeover_enabled"), True):
        command.append("--disable-workflow-queue-auto-takeover")
    return command


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
