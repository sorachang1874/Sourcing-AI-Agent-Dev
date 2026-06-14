from __future__ import annotations

import fcntl
import getpass
import json
import os
import signal
import socket
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from .recovery_contract import coerce_positive_int, remote_provider_event_recovery_total_limit

ServiceCallback = Callable[[dict[str, Any]], dict[str, Any]]
_COMPACT_SERVICE_LIST_LIMIT = 20
_COMPACT_SERVICE_DETAIL_LIST_LIMIT = 10


def _json_safe_payload(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_payload(item) for item in value]
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        try:
            return _json_safe_payload(to_record())
        except Exception:
            return str(value)
    return str(value)


def _compact_mapping(
    payload: dict[str, Any] | None,
    *,
    keys: tuple[str, ...],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    source = dict(payload or {})
    compact: dict[str, Any] = {}
    for key in keys:
        value = source.get(key)
        if value not in (None, "", [], {}):
            compact[key] = value
    return compact


def _compact_record_list(
    values: Any,
    *,
    keys: tuple[str, ...],
    limit: int = _COMPACT_SERVICE_LIST_LIMIT,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in list(values or []):
        if not isinstance(item, dict):
            continue
        compact = _compact_mapping(item, keys=keys)
        if compact:
            records.append(compact)
        if len(records) >= limit:
            break
    return records


def _compact_recovery_phase_metrics(value: Any) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if not isinstance(value, dict):
        return metrics
    for phase_name, phase_payload in dict(value or {}).items():
        normalized_phase = str(phase_name or "").strip()
        if not normalized_phase or not isinstance(phase_payload, dict):
            continue
        compact = _compact_mapping(
            phase_payload,
            keys=(
                "phase",
                "owner",
                "status",
                "reason",
                "elapsed_ms",
                "budget_exhausted",
                "tick_elapsed_ms",
                "tick_total_budget_ms",
                "durable_work_handoff_yield",
            ),
        )
        counts = dict(phase_payload.get("counts") or {})
        if counts:
            compact["counts"] = _compact_mapping(
                counts,
                keys=(
                    "claimed_count",
                    "completed_count",
                    "failed_count",
                    "skipped_count",
                    "candidate_count",
                    "dispatched_url_count",
                    "queued_worker_count",
                    "command_count",
                    "executed_command_count",
                    "planned_command_count",
                    "planned_worker_count",
                "planned_url_count",
                "inspected_count",
                "converted_count",
                "already_converted_count",
                "unsupported_count",
                "invalid_scope_count",
                "open_unconverted_count",
                "recoverable_count",
                "executed_count",
                "open_work_count",
                "daemon_owned_open_work_count",
                    "workflow_open_count",
                    "durable_work_handoff_yield_count",
                ),
            )
        if compact:
            metrics[normalized_phase] = compact
    return metrics


def _compact_service_status_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    """Return the bounded daemon status summary persisted to status.json.

    Recovery callbacks can return rich diagnostic payloads, but service status is
    a heartbeat/control-plane artifact. Persisting raw candidate rows, profile
    URLs, or materialization results makes heartbeat writes and public health
    reads scale with workflow size. This compact projection keeps the state
    machine counters and SLO phase metrics while dropping large replay payloads.
    """

    payload = dict(summary or {})
    compact = _compact_mapping(
        payload,
        keys=(
            "status",
            "reason",
            "next_tick_requested",
            "recovery_tick_budget_exhausted",
            "phase_budget_exhausted",
            "durable_work_handoff_yield",
            "recovery_tick_total_budget_ms",
        ),
    )
    for key, keys in {
        "daemon": (
            "status",
            "reason",
            "recoverable_count",
            "explicit_worker_count",
            "worker_count",
            "claimed_count",
            "executed_count",
            "candidate_count",
            "phase_budget_ms",
            "candidate_limit",
            "elapsed_ms",
            "elapsed_budget_exhausted",
            "candidate_budget_exhausted",
        ),
        "profile_prefetch_refill": (
            "status",
            "reason",
            "group_count",
            "retry_wait_blocked_group_count",
            "planned_command_count",
            "planned_worker_count",
            "planned_url_count",
            "dispatched_url_count",
            "queued_worker_count",
            "inspected_group_count",
            "active_group_count",
            "deferred_url_count",
        ),
        "profile_refill_command_owner": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "dispatched_url_count",
            "queued_worker_count",
            "deferred_url_count",
        ),
        "profile_url_terminal_record_command_owner": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "recorded_count",
            "fetched_count",
            "failed_count",
        ),
        "legacy_materialization_adapter": (
            "status",
            "reason",
            "migration_phase",
            "migration_bridge",
            "job_id",
            "inspected_count",
            "converted_count",
            "already_converted_count",
            "unsupported_count",
            "invalid_scope_count",
            "failed_count",
            "skipped_count",
            "open_unconverted_count",
        ),
        "local_apply_backlog": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "claimed_count",
            "completed_count",
            "failed_count",
            "partial_count",
            "waiting_prerequisite_count",
            "skipped_count",
            "candidate_count",
            "legacy_bridge_used",
        ),
        "board_visible_apply": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "claimed_count",
            "completed_count",
            "failed_count",
            "waiting_prerequisite_count",
            "skipped_count",
            "candidate_count",
            "legacy_bridge_used",
        ),
        "run_scope_projection_finalize": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "claimed_count",
            "completed_count",
            "failed_count",
            "candidate_count",
            "legacy_bridge_used",
        ),
        "projection_person_search_index": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "claimed_count",
            "completed_count",
            "failed_count",
            "partial_count",
            "waiting_prerequisite_count",
            "candidate_count",
            "indexed_count",
            "legacy_bridge_used",
        ),
        "snapshot_full_materialization": (
            "status",
            "reason",
            "claimed_count",
            "completed_count",
            "failed_count",
            "skipped_count",
            "candidate_count",
        ),
        "projection_facet_layering": (
            "status",
            "reason",
            "claimed_count",
            "completed_count",
            "failed_count",
            "skipped_count",
            "candidate_count",
        ),
        "collection_authoritative_merge": (
            "status",
            "reason",
            "workflow_run_id",
            "command_count",
            "executed_command_count",
            "claimed_count",
            "completed_count",
            "failed_count",
            "skipped_count",
            "candidate_count",
            "legacy_bridge_used",
        ),
        "excel_intake_recovery": (
            "status",
            "reason",
            "recovered_count",
            "claimed_count",
            "completed_count",
        ),
        "job_recovery_open_work": (
            "status",
            "reason",
            "job_id",
            "job_status",
            "job_stage",
            "job_terminal",
            "open_work_count",
            "daemon_owned_open_work_count",
            "non_daemon_open_work_count",
            "workflow_open_count",
            "workflow_lease_alive",
            "workflow_resume_actionable",
            "pending_worker_count",
            "profile_refill_open_item_count",
            "materialization_open_item_count",
        ),
    }.items():
        nested = _compact_mapping(dict(payload.get(key) or {}), keys=keys)
        if nested:
            compact[key] = nested
    for key in (
        "workflow_resume",
        "post_projection_workflow_resume",
        "post_completion_reconcile",
        "post_followup_workflow_resume",
        "post_followup_post_completion_reconcile",
    ):
        records = _compact_record_list(
            payload.get(key),
            keys=("status", "reason", "job_id", "stage", "resumed", "completed"),
        )
        if records:
            compact[key] = records
    phase_metrics = _compact_recovery_phase_metrics(payload.get("recovery_phase_metrics"))
    if phase_metrics:
        compact["recovery_phase_metrics"] = phase_metrics
    for key in (
        "profile_refill_event_level_materialization_followup",
        "event_level_materialization_followup",
        "post_followup_event_level_materialization_followup",
    ):
        followup = dict(payload.get(key) or {})
        followup_compact = _compact_mapping(followup, keys=("status", "reason"))
        local_apply = _compact_mapping(
            dict(followup.get("local_apply") or {}),
            keys=("status", "reason", "claimed_count", "completed_count", "failed_count", "candidate_count"),
        )
        board_visible = _compact_mapping(
            dict(followup.get("board_visible_apply") or {}),
            keys=("status", "reason", "claimed_count", "completed_count", "failed_count", "candidate_count"),
        )
        if local_apply:
            followup_compact["local_apply"] = local_apply
        if board_visible:
            followup_compact["board_visible_apply"] = board_visible
        if followup_compact:
            compact[key] = followup_compact
    return compact


def service_state_dir(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return Path(runtime_dir) / "services" / service_name


def service_stop_request_path(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return service_state_dir(runtime_dir, service_name) / "stop_request.json"


def service_wakeup_request_path(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return service_state_dir(runtime_dir, service_name) / "wake_request.json"


def read_service_stop_request(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> dict[str, Any]:
    request_path = service_stop_request_path(runtime_dir, service_name)
    if not request_path.exists():
        return {
            "status": "not_requested",
            "service_name": service_name,
            "path": str(request_path),
        }
    try:
        payload = json.loads(request_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "status": "corrupted",
            "service_name": service_name,
            "path": str(request_path),
        }
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("status", "requested")
    payload.setdefault("service_name", service_name)
    payload.setdefault("path", str(request_path))
    return payload


def request_service_stop(
    runtime_dir: str | Path,
    service_name: str = "worker-recovery-daemon",
    *,
    reason: str = "",
    requested_by: str = "",
    target_status: dict[str, Any] | None = None,
    target_scope: str = "",
) -> dict[str, Any]:
    normalized_service_name = str(service_name or "worker-recovery-daemon").strip() or "worker-recovery-daemon"
    current_status = dict(target_status or read_service_status(runtime_dir, normalized_service_name))
    request_path = service_stop_request_path(runtime_dir, normalized_service_name)
    request_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": "requested",
        "service_name": normalized_service_name,
        "requested_at": _utc_now(),
        "requested_by": str(requested_by or "").strip() or "operator",
        "reason": str(reason or "").strip() or "cooperative_shutdown_requested",
        "target_pid": _coerce_pid(current_status.get("pid")),
        "target_owner_id": str(current_status.get("owner_id") or "").strip(),
        "target_started_at": str(current_status.get("started_at") or "").strip(),
        "target_status": str(current_status.get("status") or "").strip(),
        "target_scope": str(target_scope or "").strip(),
        "path": str(request_path),
    }
    request_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def clear_service_stop_request(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> bool:
    request_path = service_stop_request_path(runtime_dir, service_name)
    try:
        request_path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def _normalize_callback_worker_ids(value: Any) -> list[int]:
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


def _merge_service_callback_payload(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Merge one-shot wakeup scope into the service callback payload.

    Wakeup payloads are used to narrow an already-running daemon's next tick to
    specific remote-provider workers. Scalar fields from the latest wakeup are
    authoritative; worker id fields are unioned so concurrent wakeups do not
    drop terminal events. Remote-provider events are allowed to raise
    ``total_limit`` to the configured bounded-burst limit, while generic wakeups
    keep their scalar limit unchanged.
    """

    merged = dict(base or {})
    incoming = dict(overlay or {})
    for key, value in incoming.items():
        if key in {"explicit_worker_ids", "remote_provider_event_worker_ids"}:
            existing_ids = _normalize_callback_worker_ids(merged.get(key))
            for worker_id in _normalize_callback_worker_ids(value):
                if worker_id not in existing_ids:
                    existing_ids.append(worker_id)
            merged[key] = existing_ids
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**dict(merged.get(key) or {}), **value}
            continue
        if value is not None:
            merged[key] = value
    remote_event_worker_ids = _normalize_callback_worker_ids(merged.get("remote_provider_event_worker_ids"))
    if remote_event_worker_ids:
        explicit_worker_ids = _normalize_callback_worker_ids(merged.get("explicit_worker_ids"))
        for worker_id in remote_event_worker_ids:
            if worker_id not in explicit_worker_ids:
                explicit_worker_ids.append(worker_id)
        merged["explicit_worker_ids"] = explicit_worker_ids
        burst_limit = remote_provider_event_recovery_total_limit()
        target_count = max(len(explicit_worker_ids), len(remote_event_worker_ids))
        current_limit = coerce_positive_int(merged.get("total_limit"), 1)
        merged["total_limit"] = max(current_limit, min(target_count, burst_limit))
    return merged


def _merge_pending_wakeup_callback_payload(base: dict[str, Any], pending: dict[str, Any]) -> dict[str, Any]:
    """Apply one-shot wakeup scope to the base service callback payload.

    The service's base payload may contain stale bootstrap ``explicit_worker_ids``
    from the first daemon start. A pending remote-event wakeup is more specific,
    so worker id fields from the pending wakeup replace the base worker scope
    for the next tick while scalar flags are still merged normally.
    """

    merged = _merge_service_callback_payload(base, pending)
    pending_explicit_ids = _normalize_callback_worker_ids(pending.get("explicit_worker_ids"))
    pending_remote_ids = _normalize_callback_worker_ids(pending.get("remote_provider_event_worker_ids"))
    if pending_explicit_ids or pending_remote_ids:
        explicit_ids = list(pending_explicit_ids)
        for worker_id in pending_remote_ids:
            if worker_id not in explicit_ids:
                explicit_ids.append(worker_id)
        merged["explicit_worker_ids"] = explicit_ids
        if pending_remote_ids:
            merged["remote_provider_event_worker_ids"] = pending_remote_ids
        else:
            merged.pop("remote_provider_event_worker_ids", None)
    return merged


def _split_callback_payload_for_tick(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the bounded payload for one tick plus retained wakeup work.

    This only splits explicit worker scopes. Non-worker callback payloads pass
    through unchanged. Retained payload keeps the same scalar recovery settings
    and the not-yet-delivered worker ids for the next service tick.
    """

    explicit_ids = _normalize_callback_worker_ids(payload.get("explicit_worker_ids"))
    remote_ids = _normalize_callback_worker_ids(payload.get("remote_provider_event_worker_ids"))
    target_ids: list[int] = []
    for worker_id in [*explicit_ids, *remote_ids]:
        if worker_id not in target_ids:
            target_ids.append(worker_id)
    if not target_ids:
        return dict(payload), {}
    try:
        total_limit = int(payload.get("total_limit") or 1)
    except (TypeError, ValueError):
        total_limit = 1
    total_limit = max(1, total_limit)
    if len(target_ids) <= total_limit:
        return dict(payload), {}

    selected_ids = target_ids[:total_limit]
    retained_ids = target_ids[total_limit:]
    selected_set = set(selected_ids)
    retained_set = set(retained_ids)

    tick_payload = dict(payload)
    tick_payload["explicit_worker_ids"] = selected_ids
    if remote_ids:
        selected_remote_ids = [worker_id for worker_id in remote_ids if worker_id in selected_set]
        if selected_remote_ids:
            tick_payload["remote_provider_event_worker_ids"] = selected_remote_ids
        else:
            tick_payload.pop("remote_provider_event_worker_ids", None)

    retained_payload = dict(payload)
    retained_payload["explicit_worker_ids"] = retained_ids
    if remote_ids:
        retained_remote_ids = [worker_id for worker_id in remote_ids if worker_id in retained_set]
        if retained_remote_ids:
            retained_payload["remote_provider_event_worker_ids"] = retained_remote_ids
        else:
            retained_payload.pop("remote_provider_event_worker_ids", None)
    else:
        retained_payload.pop("remote_provider_event_worker_ids", None)
    return tick_payload, retained_payload


def request_service_wakeup(
    runtime_dir: str | Path,
    service_name: str = "worker-recovery-daemon",
    *,
    reason: str = "",
    requested_by: str = "",
    callback_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_service_name = str(service_name or "worker-recovery-daemon").strip() or "worker-recovery-daemon"
    request_path = service_wakeup_request_path(runtime_dir, normalized_service_name)
    request_path.parent.mkdir(parents=True, exist_ok=True)
    existing_payload: dict[str, Any] = {}
    if request_path.exists():
        try:
            existing = json.loads(request_path.read_text())
            if isinstance(existing, dict):
                existing_payload = dict(existing.get("callback_payload") or {})
        except (OSError, json.JSONDecodeError):
            existing_payload = {}
    merged_callback_payload = _merge_service_callback_payload(
        existing_payload,
        dict(callback_payload or {}),
    )
    payload = {
        "status": "requested",
        "service_name": normalized_service_name,
        "requested_at": _utc_now(),
        "requested_by": str(requested_by or "").strip() or "operator",
        "reason": str(reason or "").strip() or "service_wakeup_requested",
        "path": str(request_path),
    }
    if merged_callback_payload:
        payload["callback_payload"] = merged_callback_payload
    # Atomic write (temp + os.replace): Step 5b makes this a hot path — every
    # durable-event commit signals here, not just rare provider events — so a
    # concurrent daemon reader must never observe a half-written request file.
    # The read side already degrades to "corrupted" + poll backstop, but the
    # rename keeps the fast path clean.
    tmp_path = request_path.with_name(f"{request_path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    os.replace(tmp_path, request_path)
    return payload


def read_service_wakeup_request(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> dict[str, Any]:
    request_path = service_wakeup_request_path(runtime_dir, service_name)
    if not request_path.exists():
        return {
            "status": "not_requested",
            "service_name": service_name,
            "path": str(request_path),
        }
    try:
        payload = json.loads(request_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "status": "corrupted",
            "service_name": service_name,
            "path": str(request_path),
        }
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("status", "requested")
    payload.setdefault("service_name", service_name)
    payload.setdefault("path", str(request_path))
    return payload


def clear_service_wakeup_request(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> bool:
    request_path = service_wakeup_request_path(runtime_dir, service_name)
    try:
        request_path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def read_service_status(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> dict[str, Any]:
    status_path = service_state_dir(runtime_dir, service_name) / "status.json"
    if not status_path.exists():
        payload = {
            "service_name": service_name,
            "status": "not_started",
            "status_path": str(status_path),
        }
        stop_request = read_service_stop_request(runtime_dir, service_name)
        if str(stop_request.get("status") or "") != "not_requested":
            payload["stop_request"] = stop_request
            payload["stop_requested"] = True
        else:
            payload["stop_requested"] = False
        return payload
    try:
        payload = json.loads(status_path.read_text())
    except (OSError, json.JSONDecodeError):
        payload = {
            "service_name": service_name,
            "status": "corrupted",
            "status_path": str(status_path),
        }
        stop_request = read_service_stop_request(runtime_dir, service_name)
        if str(stop_request.get("status") or "") != "not_requested":
            payload["stop_request"] = stop_request
            payload["stop_requested"] = True
        else:
            payload["stop_requested"] = False
        return payload
    lock_path = Path(str(payload.get("lock_path") or service_state_dir(runtime_dir, service_name) / "service.lock"))
    lock_status = _probe_service_lock_status(lock_path)
    payload["lock_status"] = lock_status
    pid = _coerce_pid(payload.get("pid"))
    payload["pid"] = pid
    payload["pid_alive"] = _pid_is_alive(pid)
    heartbeat_age_seconds = _heartbeat_age_seconds(payload.get("updated_at"))
    if heartbeat_age_seconds is not None:
        payload["heartbeat_age_seconds"] = heartbeat_age_seconds
    heartbeat_timeout_seconds = _heartbeat_timeout_seconds(payload.get("poll_seconds"))
    payload["heartbeat_timeout_seconds"] = heartbeat_timeout_seconds
    stop_request = read_service_stop_request(runtime_dir, service_name)
    if str(stop_request.get("status") or "") != "not_requested":
        payload["stop_request"] = stop_request
        payload["stop_requested"] = True
    else:
        payload["stop_requested"] = False
    payload.setdefault("service_name", service_name)
    payload.setdefault("status_path", str(status_path))
    payload = _decorate_service_activity_status(payload)
    if str(payload.get("status") or "") in {"starting", "running", "stopping"} and (
        lock_status != "locked"
        or (heartbeat_age_seconds is not None and heartbeat_age_seconds > heartbeat_timeout_seconds)
    ):
        payload["reported_status"] = str(payload.get("status") or "")
        payload["status"] = "stale"
        if heartbeat_age_seconds is not None and heartbeat_age_seconds > heartbeat_timeout_seconds:
            payload["stale_reason"] = "heartbeat_expired"
        else:
            payload["stale_reason"] = "process_not_alive" if pid <= 0 or not bool(payload.get("pid_alive")) else "lock_not_held"
    return payload


def compact_service_status(status: dict[str, Any] | None) -> dict[str, Any]:
    """Return the public health projection for a service status payload.

    The raw service status file intentionally keeps detailed callback summaries for
    local diagnostics. Public health endpoints should expose only counters and
    readiness fields so historical workflow payloads cannot bloat polling responses.
    """

    payload = dict(status or {})
    compact: dict[str, Any] = {}
    for key in (
        "service_name",
        "status",
        "reported_status",
        "status_path",
        "pid",
        "pid_alive",
        "lock_status",
        "heartbeat_age_seconds",
        "heartbeat_timeout_seconds",
        "stale_reason",
        "started_at",
        "updated_at",
        "owner_id",
        "poll_seconds",
        "tick",
        "idle_stop_ticks",
        "idle_tick_count",
        "stop_requested",
        "current_activity_has_work",
        "activity_summary_source",
        "activity_summary_window_seconds",
        "last_activity_age_seconds",
        "last_nonempty_summary_is_historical",
    ):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            compact[key] = value
    if isinstance(payload.get("stop_request"), dict):
        compact["stop_request"] = _compact_stop_request(dict(payload.get("stop_request") or {}))
    for key in (
        "last_summary",
        "current_activity_summary",
        "activity_summary",
        "last_nonempty_summary",
        "historical_last_nonempty_summary",
    ):
        summary = _summary_dict(payload.get(key))
        if summary:
            compact[key] = _summarize_service_log_payload(summary)
    cumulative_summary = _summary_dict(payload.get("cumulative_summary"))
    if cumulative_summary:
        compact["cumulative_summary"] = {
            key: value
            for key, value in cumulative_summary.items()
            if key != "job_totals"
        }
        job_totals = dict(cumulative_summary.get("job_totals") or {})
        if job_totals:
            compact["cumulative_summary"]["job_total_count"] = len(job_totals)
    return compact


def _compact_stop_request(payload: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in (
        "status",
        "service_name",
        "requested_at",
        "requested_by",
        "reason",
        "target_pid",
        "target_owner_id",
        "target_started_at",
        "target_status",
    ):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            compact[key] = value
    return compact


def render_systemd_unit(
    *,
    project_root: str | Path,
    service_name: str = "sourcing-agent-worker-daemon",
    poll_seconds: float = 5.0,
    lease_seconds: int = 300,
    stale_after_seconds: int = 180,
    total_limit: int = 4,
    python_bin: str = "/usr/bin/env python3",
    user_name: str = "",
) -> str:
    root = Path(project_root).expanduser()
    account = user_name.strip() or getpass.getuser()
    postgres_dsn = str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    postgres_schema = str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or "public").strip() or "public"
    sqlite_shadow_backend = str(os.getenv("SOURCING_PG_ONLY_SQLITE_BACKEND") or "shared_memory").strip() or "shared_memory"
    exec_start = (
        f"{python_bin} -m sourcing_agent.cli run-worker-daemon-service "
        f"--service-name {service_name} "
        f"--poll-seconds {float(poll_seconds):g} "
        f"--lease-seconds {int(lease_seconds)} "
        f"--stale-after-seconds {int(stale_after_seconds)} "
        f"--total-limit {int(total_limit)}"
    )
    return "\n".join(
        [
            "[Unit]",
            "Description=Sourcing AI Agent Worker Recovery Daemon",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            f"User={account}",
            f"WorkingDirectory={root}",
            "Environment=PYTHONPATH=src",
            "Environment=PYTHONUNBUFFERED=1",
            *(
                [
                    f"Environment=SOURCING_CONTROL_PLANE_POSTGRES_DSN={postgres_dsn}",
                    "Environment=SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                    f"Environment=SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA={postgres_schema}",
                    "Environment=SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1",
                    f"Environment=SOURCING_PG_ONLY_SQLITE_BACKEND={sqlite_shadow_backend}",
                ]
                if postgres_dsn
                else []
            ),
            f"ExecStart={exec_start}",
            "Restart=always",
            "RestartSec=5",
            "KillSignal=SIGTERM",
            "TimeoutStopSec=30",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )


class SingleInstanceError(RuntimeError):
    pass


class WorkerDaemonService:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        recovery_callback: ServiceCallback,
        service_name: str = "worker-recovery-daemon",
        owner_id: str = "",
        callback_payload: dict[str, Any] | None = None,
        poll_seconds: float = 5.0,
        lease_seconds: int = 300,
        stale_after_seconds: int = 180,
        total_limit: int = 4,
        idle_stop_ticks: int = 0,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.service_name = service_name.strip() or "worker-recovery-daemon"
        self.recovery_callback = recovery_callback
        self.owner_id = owner_id.strip() or f"{self.service_name}-{socket.gethostname()}-{os.getpid()}"
        self.callback_payload = dict(callback_payload or {})
        self.poll_seconds = max(0.1, float(poll_seconds or 5.0))
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(0, 180 if stale_after_seconds in {None, ""} else int(stale_after_seconds))
        self.total_limit = max(1, int(total_limit or 4))
        self.idle_stop_ticks = max(0, int(idle_stop_ticks or 0))
        self.root_dir = service_state_dir(self.runtime_dir, self.service_name)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.root_dir / "status.json"
        self.lock_path = self.root_dir / "service.lock"
        self._stop_event = threading.Event()
        self._lock_handle = None
        self._started_at = _utc_now()
        self._status_lock = threading.Lock()
        self._last_wakeup_mtime_ns = 0
        self._pending_wakeup_callback_payload: dict[str, Any] = {}

    def run_forever(self, *, max_ticks: int = 0) -> dict[str, Any]:
        self._install_signal_handlers()
        with self._acquire_lock():
            tick = 0
            last_summary: dict[str, Any] = {}
            last_nonempty_summary: dict[str, Any] = {}
            last_nonempty_tick = 0
            last_nonempty_at = ""
            callback_payload: dict[str, Any] = {}
            cumulative_summary = _empty_cumulative_service_summary()
            self._write_status(
                "starting",
                tick=tick,
                last_summary=last_summary,
                last_nonempty_summary=last_nonempty_summary,
                last_nonempty_tick=last_nonempty_tick,
                last_nonempty_at=last_nonempty_at,
                cumulative_summary=cumulative_summary,
            )
            self._emit_log(
                event="service_start",
                tick=tick,
                max_ticks=int(max_ticks or 0),
                idle_stop_ticks=self.idle_stop_ticks,
                callback_payload=self.callback_payload,
            )
            try:
                while not self._stop_event.is_set():
                    self._consume_wakeup_request()
                    stop_request = self._matching_stop_request()
                    if stop_request:
                        self._emit_log(
                            event="service_stop_requested",
                            tick=tick,
                            reason=str(stop_request.get("reason") or ""),
                            requested_by=str(stop_request.get("requested_by") or ""),
                        )
                        self._stop_event.set()
                        break
                    tick += 1
                    callback_payload = self._build_callback_payload()
                    if self._pending_wakeup_callback_payload:
                        callback_payload = _merge_pending_wakeup_callback_payload(
                            callback_payload,
                            self._pending_wakeup_callback_payload,
                        )
                        self._pending_wakeup_callback_payload = {}
                    callback_payload, retained_wakeup_callback_payload = _split_callback_payload_for_tick(
                        callback_payload
                    )
                    if retained_wakeup_callback_payload:
                        self._pending_wakeup_callback_payload = _merge_service_callback_payload(
                            self._pending_wakeup_callback_payload,
                            retained_wakeup_callback_payload,
                        )
                    self._write_status(
                        "running",
                        tick=tick,
                        last_summary=last_summary,
                        last_nonempty_summary=last_nonempty_summary,
                        last_nonempty_tick=last_nonempty_tick,
                        last_nonempty_at=last_nonempty_at,
                        cumulative_summary=cumulative_summary,
                        cycle_state="running_callback",
                        callback_payload=callback_payload,
                    )
                    heartbeat_stop = threading.Event()
                    heartbeat_thread = self._start_callback_heartbeat(
                        stop_event=heartbeat_stop,
                        tick=tick,
                        last_summary=last_summary,
                        last_nonempty_summary=last_nonempty_summary,
                        last_nonempty_tick=last_nonempty_tick,
                        last_nonempty_at=last_nonempty_at,
                        cumulative_summary=cumulative_summary,
                        callback_payload=callback_payload,
                    )
                    try:
                        last_summary = self.recovery_callback(dict(callback_payload))
                    finally:
                        heartbeat_stop.set()
                        if heartbeat_thread is not None:
                            heartbeat_thread.join(timeout=max(0.1, min(1.0, self.poll_seconds) + 0.1))
                    cumulative_summary = _accumulate_cumulative_service_summary(
                        cumulative_summary,
                        summary=last_summary,
                        tick=tick,
                    )
                    summary_has_activity = _service_summary_has_activity(last_summary)
                    summary_keeps_service_alive = _service_summary_keeps_service_alive(last_summary)
                    if tick == 1 or summary_has_activity:
                        self._emit_log(
                            event="service_tick",
                            tick=tick,
                            cycle_state="callback_completed",
                            summary=_summarize_service_log_payload(last_summary),
                        )
                    if summary_has_activity:
                        last_nonempty_summary = dict(last_summary)
                        last_nonempty_tick = tick
                        last_nonempty_at = _utc_now()
                    idle_tick_count = _service_idle_tick_count(
                        tick=tick,
                        last_nonempty_tick=last_nonempty_tick,
                        summary_keeps_service_alive=summary_keeps_service_alive,
                    )
                    self._write_status(
                        "running",
                        tick=tick,
                        last_summary=last_summary,
                        last_nonempty_summary=last_nonempty_summary,
                        last_nonempty_tick=last_nonempty_tick,
                        last_nonempty_at=last_nonempty_at,
                        cumulative_summary=cumulative_summary,
                        cycle_state="idle",
                        callback_payload=callback_payload,
                        idle_tick_count=idle_tick_count,
                    )
                    if self.idle_stop_ticks > 0 and idle_tick_count >= self.idle_stop_ticks:
                        self._emit_log(
                            event="service_idle_stop",
                            tick=tick,
                            idle_tick_count=idle_tick_count,
                            idle_stop_ticks=self.idle_stop_ticks,
                        )
                        break
                    if max_ticks > 0 and tick >= max_ticks:
                        break
                    stop_request = self._matching_stop_request()
                    if stop_request:
                        self._emit_log(
                            event="service_stop_requested",
                            tick=tick,
                            reason=str(stop_request.get("reason") or ""),
                            requested_by=str(stop_request.get("requested_by") or ""),
                        )
                        self._stop_event.set()
                        break
                    if summary_has_activity:
                        continue
                    if self._sleep_until_next_tick():
                        break
                final_status = "stopped"
                self._write_status(
                    final_status,
                    tick=tick,
                    last_summary=last_summary,
                    last_nonempty_summary=last_nonempty_summary,
                    last_nonempty_tick=last_nonempty_tick,
                    last_nonempty_at=last_nonempty_at,
                    cumulative_summary=cumulative_summary,
                    cycle_state="stopped" if self._stop_event.is_set() else "idle",
                    callback_payload=callback_payload,
                    idle_tick_count=_service_idle_tick_count(
                        tick=tick,
                        last_nonempty_tick=last_nonempty_tick,
                        summary_keeps_service_alive=_service_summary_keeps_service_alive(last_summary),
                    ),
                )
                self._emit_log(
                    event="service_stop",
                    tick=tick,
                    final_status=final_status,
                    summary=_summarize_service_log_payload(last_summary),
                )
                final_stop_request = read_service_stop_request(self.runtime_dir, self.service_name)
                if str(final_stop_request.get("target_scope") or "").strip() != "service_shutdown_fence":
                    clear_service_stop_request(self.runtime_dir, self.service_name)
                return read_service_status(self.runtime_dir, self.service_name)
            except Exception as exc:
                self._write_status(
                    "failed",
                    tick=tick,
                    last_summary=last_summary,
                    last_nonempty_summary=last_nonempty_summary,
                    last_nonempty_tick=last_nonempty_tick,
                    last_nonempty_at=last_nonempty_at,
                    cumulative_summary=cumulative_summary,
                    error=str(exc),
                    cycle_state="failed",
                )
                self._emit_log(
                    event="service_failed",
                    tick=tick,
                    error=str(exc),
                    traceback=traceback.format_exc(limit=10),
                )
                raise

    def request_stop(self) -> None:
        self._stop_event.set()

    def write_systemd_unit(
        self,
        *,
        project_root: str | Path,
        output_path: str | Path,
        python_bin: str = "/usr/bin/env python3",
        user_name: str = "",
    ) -> Path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        unit_text = render_systemd_unit(
            project_root=project_root,
            service_name=self.service_name,
            poll_seconds=self.poll_seconds,
            lease_seconds=self.lease_seconds,
            stale_after_seconds=self.stale_after_seconds,
            total_limit=self.total_limit,
            python_bin=python_bin,
            user_name=user_name,
        )
        target.write_text(unit_text)
        return target

    @contextmanager
    def _acquire_lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            current = read_service_status(self.runtime_dir, self.service_name)
            raise SingleInstanceError(
                f"Service {self.service_name} already running: {current.get('status_path', str(self.status_path))}"
            ) from exc
        self._lock_handle = handle
        try:
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
                self._lock_handle = None

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Any) -> None:  # noqa: ARG002
        self._emit_log(event="service_signal", signal=int(signum))
        self._stop_event.set()

    def _sleep_until_next_tick(self) -> bool:
        deadline = time.time() + self.poll_seconds
        while time.time() < deadline:
            if self._matching_stop_request():
                self._stop_event.set()
                return True
            if self._consume_wakeup_request():
                return False
            if self._stop_event.wait(timeout=min(0.25, max(0.01, deadline - time.time()))):
                return True
        return False

    def _current_wakeup_mtime_ns(self) -> int:
        request_path = service_wakeup_request_path(self.runtime_dir, self.service_name)
        try:
            return int(request_path.stat().st_mtime_ns)
        except OSError:
            return 0

    def _consume_wakeup_request(self) -> bool:
        current_mtime_ns = self._current_wakeup_mtime_ns()
        if current_mtime_ns <= 0 or current_mtime_ns <= int(self._last_wakeup_mtime_ns or 0):
            return False
        self._last_wakeup_mtime_ns = current_mtime_ns
        wakeup = read_service_wakeup_request(self.runtime_dir, self.service_name)
        wakeup_callback_payload = dict(wakeup.get("callback_payload") or {})
        if wakeup_callback_payload:
            self._pending_wakeup_callback_payload = _merge_service_callback_payload(
                self._pending_wakeup_callback_payload,
                wakeup_callback_payload,
            )
        clear_service_wakeup_request(self.runtime_dir, self.service_name)
        return True

    def _matching_stop_request(self) -> dict[str, Any]:
        stop_request = read_service_stop_request(self.runtime_dir, self.service_name)
        if str(stop_request.get("status") or "") != "requested":
            return {}
        target_pid = _coerce_pid(stop_request.get("target_pid"))
        target_owner_id = str(stop_request.get("target_owner_id") or "").strip()
        requested_at = _parse_datetime(stop_request.get("requested_at"))
        started_at = _parse_datetime(self._started_at)
        if str(stop_request.get("target_scope") or "").strip() == "service_shutdown_fence":
            if target_pid > 0 and target_pid != os.getpid():
                clear_service_stop_request(self.runtime_dir, self.service_name)
                return {}
            if target_owner_id and target_owner_id != self.owner_id:
                clear_service_stop_request(self.runtime_dir, self.service_name)
                return {}
            return stop_request
        if target_pid > 0:
            return stop_request if target_pid == os.getpid() else {}
        if target_owner_id:
            return stop_request if target_owner_id == self.owner_id else {}
        if requested_at is not None and started_at is not None:
            return stop_request if requested_at >= started_at else {}
        return stop_request

    def _build_callback_payload(self) -> dict[str, Any]:
        payload = {
            "owner_id": self.owner_id,
            "lease_seconds": self.lease_seconds,
            "stale_after_seconds": self.stale_after_seconds,
            "total_limit": self.total_limit,
        }
        payload.update(self.callback_payload)
        return payload

    def _start_callback_heartbeat(
        self,
        *,
        stop_event: threading.Event,
        tick: int,
        last_summary: dict[str, Any],
        last_nonempty_summary: dict[str, Any],
        last_nonempty_tick: int,
        last_nonempty_at: str,
        cumulative_summary: dict[str, Any],
        callback_payload: dict[str, Any],
    ) -> threading.Thread | None:
        interval = max(0.25, min(5.0, self.poll_seconds))
        if interval <= 0:
            return None

        def _heartbeat_loop() -> None:
            while not stop_event.wait(interval):
                try:
                    self._write_status(
                        "running",
                        tick=tick,
                        last_summary=last_summary,
                        last_nonempty_summary=last_nonempty_summary,
                        last_nonempty_tick=last_nonempty_tick,
                        last_nonempty_at=last_nonempty_at,
                        cumulative_summary=cumulative_summary,
                        cycle_state="running_callback",
                        callback_payload=callback_payload,
                    )
                except Exception:
                    return

        thread = threading.Thread(
            target=_heartbeat_loop,
            name=f"{self.service_name}-heartbeat",
            daemon=True,
        )
        thread.start()
        return thread

    def _write_status(
        self,
        status: str,
        *,
        tick: int,
        last_summary: dict[str, Any],
        last_nonempty_summary: dict[str, Any],
        last_nonempty_tick: int,
        last_nonempty_at: str,
        cumulative_summary: dict[str, Any],
        error: str = "",
        cycle_state: str = "",
        callback_payload: dict[str, Any] | None = None,
        idle_tick_count: int = 0,
    ) -> None:
        compact_last_summary = _compact_service_status_summary(last_summary)
        compact_last_nonempty_summary = _compact_service_status_summary(last_nonempty_summary)
        payload = {
            "service_name": self.service_name,
            "status": status,
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": self._started_at,
            "updated_at": _utc_now(),
            "tick": int(tick),
            "poll_seconds": self.poll_seconds,
            "lease_seconds": self.lease_seconds,
            "stale_after_seconds": self.stale_after_seconds,
            "total_limit": self.total_limit,
            "idle_stop_ticks": self.idle_stop_ticks,
            "idle_tick_count": max(0, int(idle_tick_count or 0)),
            "runtime_dir": str(self.runtime_dir),
            "status_path": str(self.status_path),
            "lock_path": str(self.lock_path),
            "last_summary": compact_last_summary,
            "last_summary_projection": "compact_v1",
            "last_nonempty_summary": compact_last_nonempty_summary,
            "last_nonempty_summary_projection": "compact_v1",
            "last_nonempty_tick": int(last_nonempty_tick),
            "last_nonempty_at": last_nonempty_at,
            "cumulative_summary": cumulative_summary,
        }
        if cycle_state:
            payload["cycle_state"] = cycle_state
        if callback_payload:
            payload["callback_payload"] = dict(callback_payload)
        if error:
            payload["error"] = error
        with self._status_lock:
            self.status_path.write_text(json.dumps(_json_safe_payload(payload), ensure_ascii=False, indent=2))

    def _emit_log(self, *, event: str, **fields: Any) -> None:
        payload = {
            "service_name": self.service_name,
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "event": str(event or "").strip() or "service_event",
            "observed_at": _utc_now(),
        }
        for key, value in fields.items():
            if value in (None, "", [], {}):
                continue
            payload[str(key)] = value
        try:
            print(json.dumps(_json_safe_payload(payload), ensure_ascii=False), file=sys.stderr, flush=True)
        except Exception:
            return


def _empty_cumulative_service_summary() -> dict[str, Any]:
    return {
        "tick_count": 0,
        "active_tick_count": 0,
        "total_claimed_count": 0,
        "total_executed_count": 0,
        "max_recoverable_count": 0,
        "workflow_resume_status_counts": {},
        "job_totals": {},
    }


def _job_recovery_open_work_has_service_activity(job_recovery_open_work: dict[str, Any]) -> bool:
    daemon_owned_open_work_count = int(job_recovery_open_work.get("daemon_owned_open_work_count") or 0)
    if daemon_owned_open_work_count > 0:
        return True
    if int(job_recovery_open_work.get("profile_refill_ready_item_count") or 0) > 0:
        return True
    if _job_recovery_open_work_keeps_service_alive(job_recovery_open_work):
        return False
    if "daemon_owned_open_work_count" in job_recovery_open_work or "workflow_open_count" in job_recovery_open_work:
        return False
    return int(job_recovery_open_work.get("open_work_count") or 0) > 0


def _job_recovery_open_work_keeps_service_alive(job_recovery_open_work: dict[str, Any]) -> bool:
    workflow_open_count = int(job_recovery_open_work.get("workflow_open_count") or 0)
    job_status = str(job_recovery_open_work.get("job_status") or "").strip().lower()
    job_terminal = bool(job_recovery_open_work.get("job_terminal")) or job_status in {
        "completed",
        "failed",
        "cancelled",
        "canceled",
    }
    if workflow_open_count <= 0 or job_terminal:
        return False
    if "workflow_resume_actionable" in job_recovery_open_work:
        return bool(job_recovery_open_work.get("workflow_resume_actionable"))
    return True


def _service_summary_keeps_service_alive(summary: dict[str, Any]) -> bool:
    job_recovery_open_work = dict(summary.get("job_recovery_open_work") or {})
    return _job_recovery_open_work_keeps_service_alive(job_recovery_open_work)


def _service_idle_tick_count(
    *,
    tick: int,
    last_nonempty_tick: int,
    summary_keeps_service_alive: bool,
) -> int:
    if summary_keeps_service_alive:
        return 0
    return max(0, tick - last_nonempty_tick) if last_nonempty_tick else tick


def _service_summary_has_activity(summary: dict[str, Any]) -> bool:
    if bool(summary.get("next_tick_requested")):
        return True
    daemon = dict(summary.get("daemon") or {})
    if int(daemon.get("claimed_count") or 0) > 0 or int(daemon.get("executed_count") or 0) > 0:
        return True
    profile_refill = dict(summary.get("profile_prefetch_refill") or {})
    if (
        int(profile_refill.get("dispatched_url_count") or 0) > 0
        or int(profile_refill.get("queued_worker_count") or 0) > 0
        or int(profile_refill.get("planned_command_count") or 0) > 0
        or int(profile_refill.get("planned_worker_count") or 0) > 0
        or int(profile_refill.get("planned_url_count") or 0) > 0
    ):
        return True
    profile_refill_command_owner = dict(summary.get("profile_refill_command_owner") or {})
    if (
        int(profile_refill_command_owner.get("executed_command_count") or 0) > 0
        or int(profile_refill_command_owner.get("queued_worker_count") or 0) > 0
        or int(profile_refill_command_owner.get("dispatched_url_count") or 0) > 0
    ):
        return True
    profile_url_terminal_record_command_owner = dict(
        summary.get("profile_url_terminal_record_command_owner") or {}
    )
    if (
        int(profile_url_terminal_record_command_owner.get("executed_command_count") or 0) > 0
        or int(profile_url_terminal_record_command_owner.get("recorded_count") or 0) > 0
    ):
        return True
    legacy_materialization_adapter = dict(summary.get("legacy_materialization_adapter") or {})
    if int(legacy_materialization_adapter.get("converted_count") or 0) > 0:
        return True
    local_apply_backlog = dict(summary.get("local_apply_backlog") or {})
    if (
        int(local_apply_backlog.get("claimed_count") or 0) > 0
        or int(local_apply_backlog.get("completed_count") or 0) > 0
        or int(local_apply_backlog.get("executed_command_count") or 0) > 0
    ):
        return True
    board_visible_apply = dict(summary.get("board_visible_apply") or {})
    if (
        int(board_visible_apply.get("claimed_count") or 0) > 0
        or int(board_visible_apply.get("completed_count") or 0) > 0
        or int(board_visible_apply.get("executed_command_count") or 0) > 0
    ):
        return True
    run_scope_projection_finalize = dict(summary.get("run_scope_projection_finalize") or {})
    if (
        int(run_scope_projection_finalize.get("claimed_count") or 0) > 0
        or int(run_scope_projection_finalize.get("completed_count") or 0) > 0
        or int(run_scope_projection_finalize.get("executed_command_count") or 0) > 0
    ):
        return True
    projection_person_search_index = dict(summary.get("projection_person_search_index") or {})
    if (
        int(projection_person_search_index.get("claimed_count") or 0) > 0
        or int(projection_person_search_index.get("completed_count") or 0) > 0
        or int(projection_person_search_index.get("partial_count") or 0) > 0
        or int(projection_person_search_index.get("executed_command_count") or 0) > 0
    ):
        return True
    snapshot_full_materialization = dict(summary.get("snapshot_full_materialization") or {})
    if (
        int(snapshot_full_materialization.get("claimed_count") or 0) > 0
        or int(snapshot_full_materialization.get("completed_count") or 0) > 0
    ):
        return True
    collection_authoritative_merge = dict(summary.get("collection_authoritative_merge") or {})
    if (
        int(collection_authoritative_merge.get("claimed_count") or 0) > 0
        or int(collection_authoritative_merge.get("completed_count") or 0) > 0
        or int(collection_authoritative_merge.get("executed_command_count") or 0) > 0
    ):
        return True
    excel_intake_recovery = dict(summary.get("excel_intake_recovery") or {})
    if int(excel_intake_recovery.get("recovered_count") or 0) > 0:
        return True
    job_recovery_open_work = dict(summary.get("job_recovery_open_work") or {})
    if _job_recovery_open_work_has_service_activity(job_recovery_open_work):
        return True
    workflow_resume = list(summary.get("workflow_resume") or [])
    return any(str(item.get("status") or "") in {"resumed", "failed"} for item in workflow_resume)


def _accumulate_cumulative_service_summary(
    existing: dict[str, Any],
    *,
    summary: dict[str, Any],
    tick: int,
) -> dict[str, Any]:
    daemon = dict(summary.get("daemon") or {})
    workflow_resume = list(summary.get("workflow_resume") or [])
    job_totals: dict[str, Any] = {
        str(job_id): dict(payload)
        for job_id, payload in dict(existing.get("job_totals") or {}).items()
        if str(job_id).strip()
    }
    for job in list(daemon.get("jobs") or []):
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            continue
        prior = dict(job_totals.get(job_id) or {})
        job_totals[job_id] = {
            "claimed_count": int(prior.get("claimed_count") or 0) + int(job.get("claimed_count") or 0),
            "executed_count": int(prior.get("executed_count") or 0) + int(job.get("executed_count") or 0),
            "max_backlog_count": max(int(prior.get("max_backlog_count") or 0), int(job.get("backlog_count") or 0)),
        }

    workflow_resume_status_counts: dict[str, int] = {
        str(status): int(count)
        for status, count in dict(existing.get("workflow_resume_status_counts") or {}).items()
        if str(status).strip()
    }
    for item in workflow_resume:
        status = str(item.get("status") or "").strip()
        if not status:
            continue
        workflow_resume_status_counts[status] = int(workflow_resume_status_counts.get(status) or 0) + 1

    profile_refill = dict(summary.get("profile_prefetch_refill") or {})
    profile_refill_command_owner = dict(summary.get("profile_refill_command_owner") or {})
    profile_url_terminal_record_command_owner = dict(
        summary.get("profile_url_terminal_record_command_owner") or {}
    )
    legacy_materialization_adapter = dict(summary.get("legacy_materialization_adapter") or {})
    local_apply_backlog = dict(summary.get("local_apply_backlog") or {})
    board_visible_apply = dict(summary.get("board_visible_apply") or {})
    run_scope_projection_finalize = dict(summary.get("run_scope_projection_finalize") or {})
    projection_person_search_index = dict(summary.get("projection_person_search_index") or {})
    snapshot_full_materialization = dict(summary.get("snapshot_full_materialization") or {})
    collection_authoritative_merge = dict(summary.get("collection_authoritative_merge") or {})
    excel_intake_recovery = dict(summary.get("excel_intake_recovery") or {})
    job_recovery_open_work = dict(summary.get("job_recovery_open_work") or {})
    has_activity = _service_summary_has_activity(summary)
    return {
        "tick_count": int(tick),
        "active_tick_count": int(existing.get("active_tick_count") or 0) + (1 if has_activity else 0),
        "total_claimed_count": int(existing.get("total_claimed_count") or 0) + int(daemon.get("claimed_count") or 0),
        "total_executed_count": int(existing.get("total_executed_count") or 0) + int(daemon.get("executed_count") or 0),
        "total_profile_refill_dispatched_url_count": int(
            existing.get("total_profile_refill_dispatched_url_count") or 0
        )
        + int(profile_refill.get("dispatched_url_count") or 0),
        "total_profile_refill_queued_worker_count": int(
            existing.get("total_profile_refill_queued_worker_count") or 0
        )
        + int(profile_refill.get("queued_worker_count") or 0),
        "total_profile_refill_planned_command_count": int(
            existing.get("total_profile_refill_planned_command_count") or 0
        )
        + int(profile_refill.get("planned_command_count") or 0),
        "total_profile_refill_command_owner_executed_count": int(
            existing.get("total_profile_refill_command_owner_executed_count") or 0
        )
        + int(profile_refill_command_owner.get("executed_command_count") or 0),
        "total_profile_url_terminal_record_command_owner_executed_count": int(
            existing.get("total_profile_url_terminal_record_command_owner_executed_count") or 0
        )
        + int(profile_url_terminal_record_command_owner.get("executed_command_count") or 0),
        "total_profile_url_terminal_recorded_count": int(
            existing.get("total_profile_url_terminal_recorded_count") or 0
        )
        + int(profile_url_terminal_record_command_owner.get("recorded_count") or 0),
        "total_legacy_materialization_adapter_inspected_count": int(
            existing.get("total_legacy_materialization_adapter_inspected_count") or 0
        )
        + int(legacy_materialization_adapter.get("inspected_count") or 0),
        "total_legacy_materialization_adapter_converted_count": int(
            existing.get("total_legacy_materialization_adapter_converted_count") or 0
        )
        + int(legacy_materialization_adapter.get("converted_count") or 0),
        "total_legacy_materialization_adapter_open_unconverted_count": int(
            existing.get("total_legacy_materialization_adapter_open_unconverted_count") or 0
        )
        + int(legacy_materialization_adapter.get("open_unconverted_count") or 0),
        "total_local_apply_backlog_claimed_count": int(
            existing.get("total_local_apply_backlog_claimed_count") or 0
        )
        + int(local_apply_backlog.get("claimed_count") or 0),
        "total_local_apply_backlog_completed_count": int(
            existing.get("total_local_apply_backlog_completed_count") or 0
        )
        + int(local_apply_backlog.get("completed_count") or 0),
        "total_local_profile_delta_apply_command_owner_executed_count": int(
            existing.get("total_local_profile_delta_apply_command_owner_executed_count") or 0
        )
        + int(local_apply_backlog.get("executed_command_count") or 0),
        "total_board_visible_apply_claimed_count": int(
            existing.get("total_board_visible_apply_claimed_count") or 0
        )
        + int(board_visible_apply.get("claimed_count") or 0),
        "total_board_visible_apply_completed_count": int(
            existing.get("total_board_visible_apply_completed_count") or 0
        )
        + int(board_visible_apply.get("completed_count") or 0),
        "total_board_visible_patch_publish_command_owner_executed_count": int(
            existing.get("total_board_visible_patch_publish_command_owner_executed_count") or 0
        )
        + int(board_visible_apply.get("executed_command_count") or 0),
        "total_run_scope_projection_finalize_claimed_count": int(
            existing.get("total_run_scope_projection_finalize_claimed_count") or 0
        )
        + int(run_scope_projection_finalize.get("claimed_count") or 0),
        "total_run_scope_projection_finalize_completed_count": int(
            existing.get("total_run_scope_projection_finalize_completed_count") or 0
        )
        + int(run_scope_projection_finalize.get("completed_count") or 0),
        "total_run_scope_projection_finalize_command_owner_executed_count": int(
            existing.get("total_run_scope_projection_finalize_command_owner_executed_count") or 0
        )
        + int(run_scope_projection_finalize.get("executed_command_count") or 0),
        "total_run_scope_projection_finalize_candidate_count": int(
            existing.get("total_run_scope_projection_finalize_candidate_count") or 0
        )
        + int(run_scope_projection_finalize.get("candidate_count") or 0),
        "total_projection_person_search_index_claimed_count": int(
            existing.get("total_projection_person_search_index_claimed_count") or 0
        )
        + int(projection_person_search_index.get("claimed_count") or 0),
        "total_projection_person_search_index_completed_count": int(
            existing.get("total_projection_person_search_index_completed_count") or 0
        )
        + int(projection_person_search_index.get("completed_count") or 0),
        "total_projection_person_search_index_partial_count": int(
            existing.get("total_projection_person_search_index_partial_count") or 0
        )
        + int(projection_person_search_index.get("partial_count") or 0),
        "total_projection_person_search_index_build_command_owner_executed_count": int(
            existing.get("total_projection_person_search_index_build_command_owner_executed_count") or 0
        )
        + int(projection_person_search_index.get("executed_command_count") or 0),
        "total_projection_person_search_index_indexed_count": int(
            existing.get("total_projection_person_search_index_indexed_count") or 0
        )
        + int(projection_person_search_index.get("indexed_count") or 0),
        "total_snapshot_full_materialization_claimed_count": int(
            existing.get("total_snapshot_full_materialization_claimed_count") or 0
        )
        + int(snapshot_full_materialization.get("claimed_count") or 0),
        "total_snapshot_full_materialization_completed_count": int(
            existing.get("total_snapshot_full_materialization_completed_count") or 0
        )
        + int(snapshot_full_materialization.get("completed_count") or 0),
        "total_collection_authoritative_merge_claimed_count": int(
            existing.get("total_collection_authoritative_merge_claimed_count") or 0
        )
        + int(collection_authoritative_merge.get("claimed_count") or 0),
        "total_collection_authoritative_merge_completed_count": int(
            existing.get("total_collection_authoritative_merge_completed_count") or 0
        )
        + int(collection_authoritative_merge.get("completed_count") or 0),
        "total_collection_authoritative_merge_command_owner_executed_count": int(
            existing.get("total_collection_authoritative_merge_command_owner_executed_count") or 0
        )
        + int(collection_authoritative_merge.get("executed_command_count") or 0),
        "total_excel_intake_recovered_count": int(existing.get("total_excel_intake_recovered_count") or 0)
        + int(excel_intake_recovery.get("recovered_count") or 0),
        "max_job_recovery_open_work_count": max(
            int(existing.get("max_job_recovery_open_work_count") or 0),
            int(job_recovery_open_work.get("open_work_count") or 0),
        ),
        "max_recoverable_count": max(
            int(existing.get("max_recoverable_count") or 0),
            int(daemon.get("recoverable_count") or 0),
        ),
        "workflow_resume_status_counts": workflow_resume_status_counts,
        "job_totals": job_totals,
    }


def _probe_service_lock_status(lock_path: Path) -> str:
    if not lock_path.exists():
        return "missing"
    handle = None
    locked_here = False
    try:
        handle = lock_path.open("a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked_here = True
            return "free"
        except BlockingIOError:
            return "locked"
    except OSError:
        return "unknown"
    finally:
        if handle is not None:
            try:
                if locked_here:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_pid(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _pid_is_alive(pid: int) -> bool:
    normalized_pid = max(0, int(pid or 0))
    if normalized_pid <= 0:
        return False
    try:
        os.kill(normalized_pid, 0)
    except OSError:
        return False
    return True


def _heartbeat_age_seconds(updated_at: Any) -> float | None:
    observed_at = _parse_datetime(updated_at)
    if observed_at is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds())


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        observed_at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc)


def _heartbeat_timeout_seconds(poll_seconds: Any) -> float:
    try:
        normalized_poll = max(0.1, float(poll_seconds or 5.0))
    except (TypeError, ValueError):
        normalized_poll = 5.0
    return max(5.0, normalized_poll * 3.0 + 2.0)


def _summarize_service_log_payload(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(summary or {})
    daemon = dict(payload.get("daemon") or {})
    workflow_resume = list(payload.get("workflow_resume") or [])
    post_completion_reconcile = list(payload.get("post_completion_reconcile") or [])
    profile_refill = dict(payload.get("profile_prefetch_refill") or {})
    profile_refill_command_owner = dict(payload.get("profile_refill_command_owner") or {})
    profile_url_terminal_record_command_owner = dict(
        payload.get("profile_url_terminal_record_command_owner") or {}
    )
    legacy_materialization_adapter = dict(payload.get("legacy_materialization_adapter") or {})
    profile_refill_followup = dict(payload.get("profile_refill_event_level_materialization_followup") or {})
    profile_refill_local_apply = dict(profile_refill_followup.get("local_apply") or {})
    profile_refill_board_visible = dict(profile_refill_followup.get("board_visible_apply") or {})
    local_apply_backlog = dict(payload.get("local_apply_backlog") or {})
    board_visible_apply = dict(payload.get("board_visible_apply") or {})
    run_scope_projection_finalize = dict(payload.get("run_scope_projection_finalize") or {})
    projection_person_search_index = dict(payload.get("projection_person_search_index") or {})
    snapshot_full_materialization = dict(payload.get("snapshot_full_materialization") or {})
    projection_facet_layering = dict(payload.get("projection_facet_layering") or {})
    collection_authoritative_merge = dict(payload.get("collection_authoritative_merge") or {})
    excel_intake_recovery = dict(payload.get("excel_intake_recovery") or {})
    job_recovery_open_work = dict(payload.get("job_recovery_open_work") or {})
    return {
        "status": str(payload.get("status") or ""),
        "next_tick_requested": bool(payload.get("next_tick_requested")),
        "recovery_tick_budget_exhausted": bool(payload.get("recovery_tick_budget_exhausted")),
        "daemon_recoverable_count": int(daemon.get("recoverable_count") or 0),
        "daemon_claimed_count": int(daemon.get("claimed_count") or 0),
        "daemon_executed_count": int(daemon.get("executed_count") or 0),
        "workflow_resume_count": len(workflow_resume),
        "post_completion_reconcile_count": len(post_completion_reconcile),
        "profile_refill_dispatched_url_count": int(profile_refill.get("dispatched_url_count") or 0),
        "profile_refill_queued_worker_count": int(profile_refill.get("queued_worker_count") or 0),
        "profile_refill_planned_command_count": int(profile_refill.get("planned_command_count") or 0),
        "profile_refill_command_owner_executed_count": int(
            profile_refill_command_owner.get("executed_command_count") or 0
        ),
        "profile_url_terminal_record_command_owner_executed_count": int(
            profile_url_terminal_record_command_owner.get("executed_command_count") or 0
        ),
        "profile_url_terminal_recorded_count": int(
            profile_url_terminal_record_command_owner.get("recorded_count") or 0
        ),
        "legacy_materialization_adapter_inspected_count": int(
            legacy_materialization_adapter.get("inspected_count") or 0
        ),
        "legacy_materialization_adapter_converted_count": int(
            legacy_materialization_adapter.get("converted_count") or 0
        ),
        "legacy_materialization_adapter_open_unconverted_count": int(
            legacy_materialization_adapter.get("open_unconverted_count") or 0
        ),
        "local_apply_backlog_claimed_count": int(local_apply_backlog.get("claimed_count") or 0),
        "local_apply_backlog_completed_count": int(local_apply_backlog.get("completed_count") or 0),
        "local_profile_delta_apply_command_owner_executed_count": int(
            local_apply_backlog.get("executed_command_count") or 0
        ),
        "profile_refill_event_local_apply_claimed_count": int(profile_refill_local_apply.get("claimed_count") or 0),
        "profile_refill_event_local_apply_completed_count": int(profile_refill_local_apply.get("completed_count") or 0),
        "board_visible_apply_claimed_count": int(board_visible_apply.get("claimed_count") or 0),
        "board_visible_apply_completed_count": int(board_visible_apply.get("completed_count") or 0),
        "board_visible_patch_publish_command_owner_executed_count": int(
            board_visible_apply.get("executed_command_count") or 0
        ),
        "run_scope_projection_finalize_claimed_count": int(run_scope_projection_finalize.get("claimed_count") or 0),
        "run_scope_projection_finalize_completed_count": int(run_scope_projection_finalize.get("completed_count") or 0),
        "run_scope_projection_finalize_command_owner_executed_count": int(
            run_scope_projection_finalize.get("executed_command_count") or 0
        ),
        "run_scope_projection_finalize_candidate_count": int(
            run_scope_projection_finalize.get("candidate_count") or 0
        ),
        "projection_person_search_index_claimed_count": int(
            projection_person_search_index.get("claimed_count") or 0
        ),
        "projection_person_search_index_completed_count": int(
            projection_person_search_index.get("completed_count") or 0
        ),
        "projection_person_search_index_partial_count": int(
            projection_person_search_index.get("partial_count") or 0
        ),
        "projection_person_search_index_build_command_owner_executed_count": int(
            projection_person_search_index.get("executed_command_count") or 0
        ),
        "projection_person_search_index_indexed_count": int(
            projection_person_search_index.get("indexed_count") or 0
        ),
        "profile_refill_event_board_visible_claimed_count": int(profile_refill_board_visible.get("claimed_count") or 0),
        "profile_refill_event_board_visible_completed_count": int(profile_refill_board_visible.get("completed_count") or 0),
        "snapshot_full_materialization_claimed_count": int(
            snapshot_full_materialization.get("claimed_count") or 0
        ),
        "snapshot_full_materialization_completed_count": int(
            snapshot_full_materialization.get("completed_count") or 0
        ),
        "projection_facet_layering_claimed_count": int(projection_facet_layering.get("claimed_count") or 0),
        "projection_facet_layering_completed_count": int(projection_facet_layering.get("completed_count") or 0),
        "collection_authoritative_merge_claimed_count": int(collection_authoritative_merge.get("claimed_count") or 0),
        "collection_authoritative_merge_completed_count": int(collection_authoritative_merge.get("completed_count") or 0),
        "collection_authoritative_merge_command_owner_executed_count": int(
            collection_authoritative_merge.get("executed_command_count") or 0
        ),
        "excel_intake_recovered_count": int(excel_intake_recovery.get("recovered_count") or 0),
        "job_recovery_open_work_count": int(job_recovery_open_work.get("open_work_count") or 0),
        "job_recovery_daemon_owned_open_work_count": int(
            job_recovery_open_work.get("daemon_owned_open_work_count") or 0
        ),
        "job_recovery_workflow_open_count": int(job_recovery_open_work.get("workflow_open_count") or 0),
        "job_recovery_pending_worker_count": int(job_recovery_open_work.get("pending_worker_count") or 0),
        "job_recovery_profile_refill_open_item_count": int(
            job_recovery_open_work.get("profile_refill_open_item_count") or 0
        ),
        "job_recovery_profile_refill_ready_item_count": int(
            job_recovery_open_work.get("profile_refill_ready_item_count") or 0
        ),
        "job_recovery_materialization_open_item_count": int(
            job_recovery_open_work.get("materialization_open_item_count") or 0
        ),
    }


def _decorate_service_activity_status(payload: dict[str, Any]) -> dict[str, Any]:
    decorated = dict(payload or {})
    last_summary = _summary_dict(decorated.get("last_summary"))
    raw_last_nonempty_summary = _summary_dict(decorated.get("last_nonempty_summary"))
    last_nonempty_tick = int(decorated.get("last_nonempty_tick") or 0)
    last_nonempty_at = str(decorated.get("last_nonempty_at") or "")
    current_has_activity = _service_summary_has_activity(last_summary)
    last_activity_age_seconds = _heartbeat_age_seconds(last_nonempty_at)
    last_activity_window_seconds = _activity_summary_recency_window_seconds(decorated.get("poll_seconds"))
    historical_last_activity = (
        bool(raw_last_nonempty_summary)
        and not current_has_activity
        and (
            last_activity_age_seconds is None
            or last_activity_age_seconds > last_activity_window_seconds
        )
    )

    decorated["current_activity_summary"] = last_summary if current_has_activity else {}
    decorated["current_activity_has_work"] = current_has_activity
    decorated["activity_summary_window_seconds"] = last_activity_window_seconds
    if last_activity_age_seconds is not None:
        decorated["last_activity_age_seconds"] = last_activity_age_seconds

    if historical_last_activity:
        decorated["historical_last_nonempty_summary"] = raw_last_nonempty_summary
        decorated["historical_last_nonempty_tick"] = last_nonempty_tick
        decorated["historical_last_nonempty_at"] = last_nonempty_at
        decorated["last_nonempty_summary"] = {}
        decorated["last_nonempty_tick"] = 0
        decorated["last_nonempty_at"] = ""
        decorated["last_nonempty_summary_is_historical"] = True
        decorated["activity_summary"] = {}
        decorated["activity_summary_source"] = "none"
    else:
        decorated.setdefault("historical_last_nonempty_summary", {})
        decorated.setdefault("historical_last_nonempty_tick", 0)
        decorated.setdefault("historical_last_nonempty_at", "")
        decorated["last_nonempty_summary"] = raw_last_nonempty_summary
        decorated["last_nonempty_tick"] = last_nonempty_tick
        decorated["last_nonempty_at"] = last_nonempty_at
        decorated["last_nonempty_summary_is_historical"] = False
        if current_has_activity:
            decorated["activity_summary"] = last_summary
            decorated["activity_summary_source"] = "current"
        elif raw_last_nonempty_summary:
            decorated["activity_summary"] = raw_last_nonempty_summary
            decorated["activity_summary_source"] = "recent"
        else:
            decorated["activity_summary"] = {}
            decorated["activity_summary_source"] = "none"
    return decorated


def _summary_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _activity_summary_recency_window_seconds(poll_seconds: Any) -> float:
    return max(60.0, _heartbeat_timeout_seconds(poll_seconds) * 3.0)
