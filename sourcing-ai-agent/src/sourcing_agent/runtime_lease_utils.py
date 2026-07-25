from __future__ import annotations

import os
import socket

_LOCAL_PROCESS_LEASE_PREFIXES = (
    "job-recovery",
    "recovery-daemon",
    "worker-recovery-daemon",
    "server-runtime-watchdog",
)


def workflow_job_lease_owner_is_dead_local_process(lease_owner: str) -> bool:
    """Return True when a workflow job lease owner points at a dead local pid."""
    normalized_owner = str(lease_owner or "").strip()
    if not normalized_owner:
        return False
    hostname, separator, remainder = normalized_owner.partition(":")
    if not separator:
        return worker_lease_owner_is_dead_local_process(normalized_owner)
    raw_pid, pid_separator, _thread_id = remainder.partition(":")
    if not pid_separator or not raw_pid.isdigit():
        return False
    if hostname != socket.gethostname():
        return False
    return _pid_is_dead_local_process(int(raw_pid))


def worker_lease_owner_is_dead_local_process(lease_owner: str) -> bool:
    """Return True when a service-style lease owner points at a dead local pid."""
    normalized_owner = str(lease_owner or "").strip()
    if not normalized_owner:
        return False
    owner_prefix, separator, raw_pid = normalized_owner.rpartition("-")
    if not separator or not raw_pid.isdigit():
        return False
    pid = int(raw_pid)
    if pid <= 0:
        return False
    hostname = socket.gethostname()
    hostname_suffix = f"-{hostname}"
    if not owner_prefix.endswith(hostname_suffix):
        return False
    service_name = owner_prefix[: -len(hostname_suffix)]
    if not any(service_name == prefix or service_name.startswith(f"{prefix}-") for prefix in _LOCAL_PROCESS_LEASE_PREFIXES):
        return False
    return _pid_is_dead_local_process(pid)


def _pid_is_dead_local_process(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    except OSError:
        return False
    return False
