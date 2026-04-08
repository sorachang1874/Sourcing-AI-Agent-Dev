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
import threading
import time
from typing import Any, Callable, Iterator


ServiceCallback = Callable[[dict[str, Any]], dict[str, Any]]


def service_state_dir(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> Path:
    return Path(runtime_dir) / "services" / service_name


def read_service_status(runtime_dir: str | Path, service_name: str = "worker-recovery-daemon") -> dict[str, Any]:
    status_path = service_state_dir(runtime_dir, service_name) / "status.json"
    if not status_path.exists():
        return {
            "service_name": service_name,
            "status": "not_started",
            "status_path": str(status_path),
        }
    try:
        return json.loads(status_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "service_name": service_name,
            "status": "corrupted",
            "status_path": str(status_path),
        }


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
    root = Path(project_root).expanduser().resolve()
    account = user_name.strip() or getpass.getuser()
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
        poll_seconds: float = 5.0,
        lease_seconds: int = 300,
        stale_after_seconds: int = 180,
        total_limit: int = 4,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.service_name = service_name.strip() or "worker-recovery-daemon"
        self.recovery_callback = recovery_callback
        self.owner_id = owner_id.strip() or f"{self.service_name}-{socket.gethostname()}-{os.getpid()}"
        self.poll_seconds = max(0.1, float(poll_seconds or 5.0))
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(30, int(stale_after_seconds or 180))
        self.total_limit = max(1, int(total_limit or 4))
        self.root_dir = service_state_dir(self.runtime_dir, self.service_name)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.root_dir / "status.json"
        self.lock_path = self.root_dir / "service.lock"
        self._stop_event = threading.Event()
        self._lock_handle = None
        self._started_at = _utc_now()

    def run_forever(self, *, max_ticks: int = 0) -> dict[str, Any]:
        self._install_signal_handlers()
        with self._acquire_lock():
            tick = 0
            last_summary: dict[str, Any] = {}
            last_nonempty_summary: dict[str, Any] = {}
            last_nonempty_tick = 0
            last_nonempty_at = ""
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
            try:
                while not self._stop_event.is_set():
                    tick += 1
                    last_summary = self.recovery_callback(
                        {
                            "owner_id": self.owner_id,
                            "lease_seconds": self.lease_seconds,
                            "stale_after_seconds": self.stale_after_seconds,
                            "total_limit": self.total_limit,
                        }
                    )
                    cumulative_summary = _accumulate_cumulative_service_summary(
                        cumulative_summary,
                        summary=last_summary,
                        tick=tick,
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
                    )
                    if max_ticks > 0 and tick >= max_ticks:
                        break
                    if self._sleep_until_next_tick():
                        break
                final_status = "stopped" if not self._stop_event.is_set() else "stopping"
                self._write_status(
                    final_status,
                    tick=tick,
                    last_summary=last_summary,
                    last_nonempty_summary=last_nonempty_summary,
                    last_nonempty_tick=last_nonempty_tick,
                    last_nonempty_at=last_nonempty_at,
                    cumulative_summary=cumulative_summary,
                )
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
        self._stop_event.set()

    def _sleep_until_next_tick(self) -> bool:
        deadline = time.time() + self.poll_seconds
        while time.time() < deadline:
            if self._stop_event.wait(timeout=min(0.25, max(0.01, deadline - time.time()))):
                return True
        return False

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
        if error:
            payload["error"] = error
        self.status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
