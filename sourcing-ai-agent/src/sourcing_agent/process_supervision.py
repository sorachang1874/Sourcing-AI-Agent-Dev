from __future__ import annotations

import os
from pathlib import Path
import subprocess
import time
from typing import Any, Callable


def build_subprocess_env(project_root: Path, *, base_env: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(base_env or os.environ)
    src_path = str((project_root / "src").resolve())
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = os.pathsep.join([src_path, existing_pythonpath]) if existing_pythonpath else src_path
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def read_text_tail(path: Path, *, max_bytes: int = 2000) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            return handle.read().decode("utf-8", errors="replace").strip()
    except OSError:
        return ""


def spawn_detached_process(
    *,
    command: list[str],
    cwd: Path,
    log_path: Path,
    env: dict[str, str],
    startup_wait_seconds: float = 0.15,
) -> dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("ab")
    try:
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        log_handle.close()
        return {
            "status": "failed_to_start",
            "pid": 0,
            "log_path": str(log_path),
            "command": command,
            "error": str(exc),
        }
    finally:
        try:
            log_handle.close()
        except Exception:
            pass

    time.sleep(max(0.0, float(startup_wait_seconds or 0.0)))
    poll = getattr(process, "poll", None)
    exit_code = poll() if callable(poll) else None
    if exit_code is not None:
        return {
            "status": "failed_to_start",
            "pid": process.pid,
            "log_path": str(log_path),
            "command": command,
            "exit_code": int(exit_code),
            "log_tail": read_text_tail(log_path),
        }
    return {
        "status": "started",
        "pid": process.pid,
        "log_path": str(log_path),
        "command": command,
    }


def process_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def service_status_is_ready(payload: dict[str, Any] | None) -> bool:
    status = str((payload or {}).get("status") or "").strip()
    lock_status = str((payload or {}).get("lock_status") or "").strip()
    return status in {"starting", "running"} and lock_status == "locked"


def resolve_timeout(
    value: float | None,
    *,
    env_key: str,
    default: float,
    minimum: float = 0.2,
) -> float:
    if value is not None:
        try:
            return max(minimum, float(value))
        except (TypeError, ValueError):
            return default
    raw = str(os.getenv(env_key) or "").strip()
    try:
        return max(minimum, float(raw or default))
    except ValueError:
        return default


def wait_for_service_ready(
    *,
    read_status: Callable[[], dict[str, Any]],
    pid: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.2, float(timeout_seconds or 0.0))
    while time.monotonic() < deadline:
        service_status = read_status()
        if service_status_is_ready(service_status):
            return {
                "status": "ready",
                "service_status": service_status,
            }
        if not process_alive(pid):
            return {
                "status": "process_exited_before_ready",
                "service_status": service_status,
                "pid": int(pid or 0),
            }
        time.sleep(max(0.05, float(poll_seconds or 0.1)))
    final_status = read_status()
    if service_status_is_ready(final_status):
        return {
            "status": "ready",
            "service_status": final_status,
        }
    if process_alive(pid):
        return {
            "status": "timeout_process_alive",
            "service_status": final_status,
            "pid": int(pid or 0),
        }
    return {
        "status": "timeout_process_exited",
        "service_status": final_status,
        "pid": int(pid or 0),
    }


def wait_for_job_status_transition(
    *,
    get_job: Callable[[], dict[str, Any]],
    pid: int,
    timeout_seconds: float,
    poll_seconds: float,
    waiting_statuses: set[str] | None = None,
) -> dict[str, Any]:
    waiting = {str(item).strip().lower() for item in list(waiting_statuses or {"queued"}) if str(item).strip()}
    deadline = time.monotonic() + max(0.2, float(timeout_seconds or 0.0))
    while time.monotonic() < deadline:
        job = get_job()
        job_status = str(job.get("status") or "").strip().lower()
        job_stage = str(job.get("stage") or "").strip().lower()
        if job_status and job_status not in waiting:
            return {
                "status": "advanced",
                "job_status": job_status,
                "job_stage": job_stage,
            }
        if not process_alive(pid):
            return {
                "status": "runner_exited_before_advance",
                "job_status": job_status,
                "job_stage": job_stage,
                "pid": int(pid or 0),
            }
        time.sleep(max(0.05, float(poll_seconds or 0.1)))
    final_job = get_job()
    final_status = str(final_job.get("status") or "").strip().lower()
    final_stage = str(final_job.get("stage") or "").strip().lower()
    if final_status and final_status not in waiting:
        return {
            "status": "advanced",
            "job_status": final_status,
            "job_stage": final_stage,
        }
    if process_alive(pid):
        return {
            "status": "timeout_runner_alive",
            "job_status": final_status,
            "job_stage": final_stage,
            "pid": int(pid or 0),
        }
    return {
        "status": "timeout_runner_exited",
        "job_status": final_status,
        "job_stage": final_stage,
        "pid": int(pid or 0),
    }
