"""Docker-backed local control-plane Postgres (macOS-first path).

The Linux/WSL-only ``.local-postgres/{extract,data}`` layout managed by
``sourcing_agent.local_postgres`` cannot run on macOS (the extracted Debian
postgresql-16 binaries are Linux ELF). This module provisions the same
control-plane contract from a persistent Docker container instead:

- container ``sourcing-local-postgres`` from ``postgres:16-alpine``
- host port ``127.0.0.1:55432`` -> container ``5432``
- named volume ``sourcing-local-postgres-data`` for durable cluster data
- ``POSTGRES_USER=sourcing`` / ``POSTGRES_DB=sourcing_agent``
- loopback ``trust`` auth, matching the existing password-less DSN convention
  ``postgresql://sourcing@127.0.0.1:55432/sourcing_agent``

The DSN is recorded in the same ``.local-postgres.env`` file that
``resolve_control_plane_postgres_dsn`` already discovers, so existing
consumers need zero changes.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

DOCKER_LOCAL_POSTGRES_CONTAINER_NAME = "sourcing-local-postgres"
DOCKER_LOCAL_POSTGRES_VOLUME_NAME = "sourcing-local-postgres-data"
DOCKER_LOCAL_POSTGRES_IMAGE = "postgres:16-alpine"
DOCKER_LOCAL_POSTGRES_HOST = "127.0.0.1"

_CONTAINER_POSTGRES_PORT = 5432
_DEFAULT_HOST_PORT = 55432
_DEFAULT_USER = "sourcing"
_DEFAULT_DATABASE = "sourcing_agent"
_ENV_FILE_MANAGED_MARKER = "# managed-by: sourcing_agent.local_postgres_docker"
_ENV_FILE_PRESERVED_PREFIX = "# previous-unmanaged: "
_LOCAL_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
# Container-engine binary search dirs. Docker was retired in favor of Podman
# (2026-07-24); Podman installs to the Homebrew bin, and the dead
# /Applications/Docker.app path is gone.
_DOCKER_BIN_DIRS = (
    "/opt/homebrew/bin",
    "/usr/local/bin",
)
# Engine resolution: honor an explicit SOURCING_CONTAINER_ENGINE override,
# otherwise prefer podman and fall back to docker so a host that still has
# Docker keeps working. Podman's CLI is Docker-compatible for the run/exec/
# volume/info verbs this module uses.
_CONTAINER_ENGINE_ENV = "SOURCING_CONTAINER_ENGINE"
_PREFERRED_CONTAINER_ENGINES = ("podman", "docker")
_DOCKER_DAEMON_NEGATIVE_CACHE_SECONDS = 15.0
_docker_daemon_unreachable_until = 0.0


def _strip(value: Any) -> str:
    return str(value or "").strip()


def _docker_environment() -> dict[str, str]:
    env = dict(os.environ)
    current_path = env.get("PATH", "")
    prefix_dirs = [directory for directory in _DOCKER_BIN_DIRS if os.path.isdir(directory)]
    if prefix_dirs:
        env["PATH"] = os.pathsep.join([*prefix_dirs, current_path]) if current_path else os.pathsep.join(prefix_dirs)
    return env


def _docker_cli_path() -> str:
    """Resolve the container-engine binary (podman-preferred, docker fallback)."""
    env = _docker_environment()
    path = env.get("PATH", "")
    explicit = _strip(os.getenv(_CONTAINER_ENGINE_ENV))
    if explicit:
        return _strip(shutil.which(explicit, path=path)) or explicit
    for engine in _PREFERRED_CONTAINER_ENGINES:
        found = _strip(shutil.which(engine, path=path))
        if found:
            return found
    return ""


def _run_docker(args: list[str], *, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
    cli = _docker_cli_path()
    if not cli:
        return subprocess.CompletedProcess(
            ["podman", *args], 127, "", "container engine (podman/docker) not found"
        )
    command = [cli, *args]
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_docker_environment(),
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(command, 124, "", f"docker command timed out after {timeout}s")
    except Exception as exc:  # pragma: no cover - depends on host docker installation.
        return subprocess.CompletedProcess(command, 1, "", f"{type(exc).__name__}: {exc}")


def docker_runtime_available(*, force_refresh: bool = False) -> bool:
    """Whether the docker CLI exists and the daemon answers ``docker info``."""

    global _docker_daemon_unreachable_until
    if not force_refresh and time.monotonic() < _docker_daemon_unreachable_until:
        return False
    if not _docker_cli_path():
        _docker_daemon_unreachable_until = time.monotonic() + _DOCKER_DAEMON_NEGATIVE_CACHE_SECONDS
        return False
    # Cross-engine liveness probe: `version --format {{.Server.Version}}` works
    # on both docker and podman, whereas docker's `info --format
    # {{.ServerVersion}}` errors under podman (different info schema).
    result = _run_docker(["version", "--format", "{{.Server.Version}}"], timeout=10.0)
    if result.returncode == 0 and _strip(result.stdout):
        _docker_daemon_unreachable_until = 0.0
        return True
    _docker_daemon_unreachable_until = time.monotonic() + _DOCKER_DAEMON_NEGATIVE_CACHE_SECONDS
    return False


def resolve_docker_local_postgres_env_file_path(base_dir: str | Path | None = None) -> Path:
    explicit = _strip(os.getenv("SOURCING_LOCAL_POSTGRES_ENV_FILE"))
    if explicit:
        return Path(explicit).expanduser()
    anchor = Path(base_dir).expanduser().resolve() if base_dir is not None else Path.cwd().expanduser().resolve()
    return anchor / ".local-postgres.env"


def resolve_docker_local_postgres_settings(base_dir: str | Path | None = None) -> dict[str, Any]:
    container = _strip(os.getenv("SOURCING_LOCAL_POSTGRES_DOCKER_CONTAINER")) or DOCKER_LOCAL_POSTGRES_CONTAINER_NAME
    volume = _strip(os.getenv("SOURCING_LOCAL_POSTGRES_DOCKER_VOLUME")) or DOCKER_LOCAL_POSTGRES_VOLUME_NAME
    image = _strip(os.getenv("SOURCING_LOCAL_POSTGRES_DOCKER_IMAGE")) or DOCKER_LOCAL_POSTGRES_IMAGE
    raw_port = _strip(os.getenv("SOURCING_LOCAL_POSTGRES_DOCKER_PORT")) or _strip(os.getenv("LOCAL_PG_PORT"))
    try:
        port = int(raw_port) if raw_port else _DEFAULT_HOST_PORT
    except ValueError:
        port = _DEFAULT_HOST_PORT
    user = _strip(os.getenv("LOCAL_PG_USER")) or _DEFAULT_USER
    database = _strip(os.getenv("LOCAL_PG_DB")) or _DEFAULT_DATABASE
    dsn = f"postgresql://{user}@{DOCKER_LOCAL_POSTGRES_HOST}:{port}/{database}"
    return {
        "container": container,
        "volume": volume,
        "image": image,
        "port": port,
        "user": user,
        "database": database,
        "dsn": dsn,
        "env_file": str(resolve_docker_local_postgres_env_file_path(base_dir)),
    }


def docker_local_postgres_dsn(base_dir: str | Path | None = None) -> str:
    return str(resolve_docker_local_postgres_settings(base_dir)["dsn"])


def dsn_targets_docker_local_postgres(dsn: str, base_dir: str | Path | None = None) -> bool:
    normalized = _strip(dsn)
    if not normalized:
        return False
    try:
        parsed = urlsplit(normalized)
    except Exception:
        return False
    if _strip(parsed.scheme).lower() not in {"postgres", "postgresql"}:
        return False
    if _strip(parsed.hostname).lower() not in _LOCAL_LOOPBACK_HOSTS:
        return False
    try:
        port = int(parsed.port or 0)
    except Exception:
        return False
    settings = resolve_docker_local_postgres_settings(base_dir)
    return port == int(settings["port"])


def _host_port_open(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        return sock.connect_ex((DOCKER_LOCAL_POSTGRES_HOST, int(port))) == 0
    finally:
        sock.close()


def _psycopg_probe_error(dsn: str) -> str:
    """Return '' when a SELECT 1 probe succeeds (or psycopg is unavailable)."""

    try:
        import psycopg
    except Exception:
        return ""
    from sourcing_agent.local_postgres import normalize_control_plane_postgres_connect_dsn

    try:
        with psycopg.connect(
            normalize_control_plane_postgres_connect_dsn(dsn),
            connect_timeout=3,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return ""
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def _container_state(container: str) -> str:
    result = _run_docker(["inspect", "--format", "{{.State.Status}}", container], timeout=10.0)
    if result.returncode != 0:
        return ""
    return _strip(result.stdout)


def _render_env_file(settings: dict[str, Any], *, preserved_lines: list[str]) -> str:
    lines = [
        _ENV_FILE_MANAGED_MARKER,
        "# Docker-backed local control-plane Postgres (macOS path).",
        f"# Container: {settings['container']}  Volume: {settings['volume']}  Image: {settings['image']}",
        "# Refresh: make local-pg-up    Stop: make local-pg-down    Status: make local-pg-status",
        f"SOURCING_CONTROL_PLANE_POSTGRES_DSN={settings['dsn']}",
        "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
        f"LOCAL_PG_PORT={settings['port']}",
        f"LOCAL_PG_USER={settings['user']}",
        f"LOCAL_PG_DB={settings['database']}",
    ]
    if preserved_lines:
        lines.append("")
        lines.extend(preserved_lines)
    return "\n".join(lines) + "\n"


def write_docker_local_postgres_env_file(
    base_dir: str | Path | None = None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Write/refresh the ``.local-postgres.env`` DSN file for the Docker path.

    The file shape matches what ``sourcing_agent.local_postgres`` and
    ``scripts/dev_postgres_env.sh`` already parse. An existing file that was
    not written by this module is left untouched unless ``force=True``; with
    ``force=True`` its previous lines are preserved as comments.
    """

    settings = resolve_docker_local_postgres_settings(base_dir)
    path = resolve_docker_local_postgres_env_file_path(base_dir)
    previous_text = ""
    if path.exists() and path.is_file():
        previous_text = path.read_text(encoding="utf-8")
    managed = _ENV_FILE_MANAGED_MARKER in previous_text
    if previous_text and not managed and not force:
        return {"status": "kept_unmanaged", "path": str(path), "dsn": ""}
    preserved_lines: list[str] = []
    if previous_text and managed:
        preserved_lines = [
            line for line in previous_text.splitlines() if line.startswith(_ENV_FILE_PRESERVED_PREFIX)
        ]
    elif previous_text and force:
        preserved_lines = [
            f"{_ENV_FILE_PRESERVED_PREFIX}{line}" for line in previous_text.splitlines() if line.strip()
        ]
    content = _render_env_file(settings, preserved_lines=preserved_lines)
    if previous_text == content:
        return {"status": "unchanged", "path": str(path), "dsn": str(settings["dsn"])}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return {"status": "written", "path": str(path), "dsn": str(settings["dsn"])}


def _wait_for_docker_postgres_ready(settings: dict[str, Any], *, timeout_seconds: float) -> str:
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    container = str(settings["container"])
    last_detail = ""
    while time.monotonic() < deadline:
        state = _container_state(container)
        if state and state not in {"created", "restarting", "running"}:
            logs = _run_docker(["logs", "--tail", "25", container], timeout=10.0)
            tail = _strip(logs.stderr) or _strip(logs.stdout)
            return f"container entered state {state}: {tail}"
        ready = _run_docker(
            [
                "exec",
                container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-p",
                str(_CONTAINER_POSTGRES_PORT),
                "-U",
                str(settings["user"]),
                "-d",
                str(settings["database"]),
            ],
            timeout=10.0,
        )
        if ready.returncode == 0 and _host_port_open(int(settings["port"])):
            probe_error = _psycopg_probe_error(str(settings["dsn"]))
            if not probe_error:
                return ""
            last_detail = probe_error
        else:
            last_detail = _strip(f"{ready.stdout} {ready.stderr}") or last_detail
        time.sleep(0.5)
    return f"timed out after {timeout_seconds}s waiting for postgres readiness: {last_detail}"


def ensure_docker_local_postgres_started(
    base_dir: str | Path | None = None,
    *,
    wait_timeout_seconds: float = 90.0,
    write_env_file: bool = True,
    force_env_file: bool = False,
) -> dict[str, Any]:
    """Ensure the persistent Docker local control-plane Postgres is running.

    Status values mirror ``ensure_local_postgres_started``: ``running``,
    ``started``, ``missing_docker``, ``failed``.
    """

    settings = resolve_docker_local_postgres_settings(base_dir)
    summary: dict[str, Any] = {
        "dsn": str(settings["dsn"]),
        "container": str(settings["container"]),
        "image": str(settings["image"]),
        "port": int(settings["port"]),
        "env_file": "",
        "env_file_status": "skipped",
    }

    def _finalize(status: str, **extra: Any) -> dict[str, Any]:
        if write_env_file and status in {"running", "started"}:
            env_result = write_docker_local_postgres_env_file(base_dir, force=force_env_file)
            summary["env_file"] = str(env_result.get("path") or "")
            summary["env_file_status"] = str(env_result.get("status") or "")
        result = dict(summary)
        result["status"] = status
        result.update(extra)
        return result

    if _host_port_open(int(settings["port"])) and not _psycopg_probe_error(str(settings["dsn"])):
        return _finalize("running")

    if not docker_runtime_available(force_refresh=True):
        return _finalize("missing_docker", dsn="")

    state = _container_state(str(settings["container"]))
    created = False
    if state != "running":
        if state:
            start_result = _run_docker(["start", str(settings["container"])], timeout=60.0)
            if start_result.returncode != 0:
                return _finalize("failed", detail=_strip(start_result.stderr) or _strip(start_result.stdout))
        else:
            run_result = _run_docker(
                [
                    "run",
                    "-d",
                    "--name",
                    str(settings["container"]),
                    "--restart",
                    "unless-stopped",
                    "-p",
                    f"{DOCKER_LOCAL_POSTGRES_HOST}:{int(settings['port'])}:{_CONTAINER_POSTGRES_PORT}",
                    "-v",
                    f"{settings['volume']}:/var/lib/postgresql/data",
                    "-e",
                    f"POSTGRES_USER={settings['user']}",
                    "-e",
                    f"POSTGRES_DB={settings['database']}",
                    "-e",
                    "POSTGRES_HOST_AUTH_METHOD=trust",
                    "-e",
                    "POSTGRES_INITDB_ARGS=--encoding=UTF8",
                    str(settings["image"]),
                ],
                timeout=120.0,
            )
            if run_result.returncode != 0:
                return _finalize("failed", detail=_strip(run_result.stderr) or _strip(run_result.stdout))
            created = True
    wait_error = _wait_for_docker_postgres_ready(settings, timeout_seconds=wait_timeout_seconds)
    if wait_error:
        return _finalize("failed", detail=wait_error)
    return _finalize("started" if (created or state != "running") else "running")


def stop_docker_local_postgres(
    base_dir: str | Path | None = None,
    *,
    remove_container: bool = False,
    remove_volume: bool = False,
) -> dict[str, Any]:
    settings = resolve_docker_local_postgres_settings(base_dir)
    container = str(settings["container"])
    summary: dict[str, Any] = {"container": container, "volume": str(settings["volume"])}
    if not docker_runtime_available(force_refresh=True):
        return {**summary, "status": "missing_docker"}
    state = _container_state(container)
    if not state:
        return {**summary, "status": "not_found"}
    status = "stopped"
    if state == "running":
        stop_result = _run_docker(["stop", container], timeout=60.0)
        if stop_result.returncode != 0:
            return {
                **summary,
                "status": "failed",
                "detail": _strip(stop_result.stderr) or _strip(stop_result.stdout),
            }
    if remove_container:
        remove_result = _run_docker(["rm", container], timeout=60.0)
        if remove_result.returncode != 0:
            return {
                **summary,
                "status": "failed",
                "detail": _strip(remove_result.stderr) or _strip(remove_result.stdout),
            }
        status = "removed"
        if remove_volume:
            _run_docker(["volume", "rm", str(settings["volume"])], timeout=60.0)
    return {**summary, "status": status}


def docker_local_postgres_status(base_dir: str | Path | None = None) -> dict[str, Any]:
    settings = resolve_docker_local_postgres_settings(base_dir)
    available = docker_runtime_available(force_refresh=True)
    container = str(settings["container"])
    state = _container_state(container) if available else ""
    running = state == "running"
    ready = False
    if available and running:
        probe = _run_docker(
            [
                "exec",
                container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-p",
                str(_CONTAINER_POSTGRES_PORT),
                "-U",
                str(settings["user"]),
                "-d",
                str(settings["database"]),
            ],
            timeout=10.0,
        )
        ready = probe.returncode == 0
    env_path = Path(str(settings["env_file"]))
    env_text = ""
    if env_path.exists() and env_path.is_file():
        env_text = env_path.read_text(encoding="utf-8")
    env_dsn_matches = any(
        line.strip() == f"SOURCING_CONTROL_PLANE_POSTGRES_DSN={settings['dsn']}"
        for line in env_text.splitlines()
    )
    return {
        "docker_available": available,
        "docker_cli": _docker_cli_path(),
        "container": container,
        "volume": str(settings["volume"]),
        "image": str(settings["image"]),
        "container_exists": bool(state),
        "container_state": state,
        "running": running,
        "ready": ready,
        "port": int(settings["port"]),
        "port_open": _host_port_open(int(settings["port"])),
        "dsn": str(settings["dsn"]),
        "env_file": str(env_path),
        "env_file_exists": bool(env_text),
        "env_file_managed": _ENV_FILE_MANAGED_MARKER in env_text,
        "env_file_dsn_matches": env_dsn_matches,
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="sourcing_agent.local_postgres_docker",
        description="Manage the Docker-backed local control-plane Postgres (macOS path).",
    )
    parser.add_argument("command", choices=("up", "down", "status"))
    parser.add_argument("--base-dir", default="", help="Anchor directory for the .local-postgres.env file (default: cwd).")
    parser.add_argument("--no-env-file", action="store_true", help="Do not write/refresh .local-postgres.env on up.")
    parser.add_argument("--remove-container", action="store_true", help="Also remove the container on down.")
    parser.add_argument("--remove-volume", action="store_true", help="Also remove the data volume on down (requires --remove-container).")
    args = parser.parse_args(argv)
    base_dir = args.base_dir or None
    if args.command == "up":
        result = ensure_docker_local_postgres_started(
            base_dir,
            write_env_file=not args.no_env_file,
            force_env_file=not args.no_env_file,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if str(result.get("status") or "") in {"running", "started"} else 1
    if args.command == "down":
        result = stop_docker_local_postgres(
            base_dir,
            remove_container=bool(args.remove_container),
            remove_volume=bool(args.remove_volume),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if str(result.get("status") or "") in {"stopped", "removed", "not_found"} else 1
    result = docker_local_postgres_status(base_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))
