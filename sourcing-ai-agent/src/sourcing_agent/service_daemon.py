from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import getpass
import json
import os
from pathlib import Path
import signal
import socket
import sys
import threading
import time
import traceback
from typing import Any, Callable, Iterator


ServiceCallback = Callable[[dict[str, Any]], dict[str, Any]]


def service_state_dir(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return Path(runtime_dir) / "services" / service_name


def service_stop_request_path(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return service_state_dir(runtime_dir, service_name) / "stop_request.json"


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
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.service_name = service_name.strip() or "worker-recovery-daemon"
        self.recovery_callback = recovery_callback
        self.owner_id = owner_id.strip() or f"{self.service_name}-{socket.gethostname()}-{os.getpid()}"
        self.callback_payload = dict(callback_payload or {})
        self.poll_seconds = max(0.1, float(poll_seconds or 5.0))
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(1, int(stale_after_seconds or 180))
        self.total_limit = max(1, int(total_limit or 4))
        self.root_dir = service_state_dir(self.runtime_dir, self.service_name)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.root_dir / "status.json"
        self.lock_path = self.root_dir / "service.lock"
        self._stop_event = threading.Event()
        self._lock_handle = None
        self._started_at = _utc_now()
        self._status_lock = threading.Lock()

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
                callback_payload=self.callback_payload,
            )
            try:
                while not self._stop_event.is_set():
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
                    if tick == 1 or _service_summary_has_activity(last_summary):
                        self._emit_log(
                            event="service_tick",
                            tick=tick,
                            cycle_state="callback_completed",
                            summary=_summarize_service_log_payload(last_summary),
                        )
                    if _service_summary_has_activity(last_summary):
                        last_nonempty_summary = dict(last_summary)
                        last_nonempty_tick = tick
                        last_nonempty_at = _utc_now()
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
                    )
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
                    if _service_summary_has_activity(last_summary):
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
                )
                self._emit_log(
                    event="service_stop",
                    tick=tick,
                    final_status=final_status,
                    summary=_summarize_service_log_payload(last_summary),
                )
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
            if self._stop_event.wait(timeout=min(0.25, max(0.01, deadline - time.time()))):
                return True
        return False

    def _matching_stop_request(self) -> dict[str, Any]:
        stop_request = read_service_stop_request(self.runtime_dir, self.service_name)
        if str(stop_request.get("status") or "") != "requested":
            return {}
        target_pid = _coerce_pid(stop_request.get("target_pid"))
        target_owner_id = str(stop_request.get("target_owner_id") or "").strip()
        if target_pid > 0:
            return stop_request if target_pid == os.getpid() else {}
        if target_owner_id:
            return stop_request if target_owner_id == self.owner_id else {}
        requested_at = _parse_datetime(stop_request.get("requested_at"))
        started_at = _parse_datetime(self._started_at)
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
    ) -> None:
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
            "runtime_dir": str(self.runtime_dir),
            "status_path": str(self.status_path),
            "lock_path": str(self.lock_path),
            "last_summary": last_summary,
            "last_nonempty_summary": last_nonempty_summary,
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
            self.status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

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
            print(json.dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)
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


def _service_summary_has_activity(summary: dict[str, Any]) -> bool:
    daemon = dict(summary.get("daemon") or {})
    if int(daemon.get("claimed_count") or 0) > 0 or int(daemon.get("executed_count") or 0) > 0:
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

    has_activity = _service_summary_has_activity(summary)
    return {
        "tick_count": int(tick),
        "active_tick_count": int(existing.get("active_tick_count") or 0) + (1 if has_activity else 0),
        "total_claimed_count": int(existing.get("total_claimed_count") or 0) + int(daemon.get("claimed_count") or 0),
        "total_executed_count": int(existing.get("total_executed_count") or 0) + int(daemon.get("executed_count") or 0),
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
    return {
        "status": str(payload.get("status") or ""),
        "daemon_recoverable_count": int(daemon.get("recoverable_count") or 0),
        "daemon_claimed_count": int(daemon.get("claimed_count") or 0),
        "daemon_executed_count": int(daemon.get("executed_count") or 0),
        "workflow_resume_count": len(workflow_resume),
        "post_completion_reconcile_count": len(post_completion_reconcile),
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
