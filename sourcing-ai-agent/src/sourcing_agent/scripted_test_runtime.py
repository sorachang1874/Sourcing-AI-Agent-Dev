from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .api import create_server
from .cli import build_orchestrator
from .runtime_environment import normalize_provider_mode
from .smoke_runtime_seed import seed_reference_smoke_runtime

_LOCAL_POSTGRES_ENV_KEYS = (
    "SOURCING_CONTROL_PLANE_POSTGRES_DSN",
    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE",
    "SOURCING_LOCAL_POSTGRES_ROOT",
    "LOCAL_PG_ROOT",
    "LOCAL_PG_EXTRACT",
    "LOCAL_PG_DATA",
    "LOCAL_PG_RUN",
    "LOCAL_PG_PORT",
    "LOCAL_PG_USER",
    "LOCAL_PG_DB",
)

FAST_HOSTED_TEST_ENV: dict[str, str] = {
    "WEB_SEARCH_READY_COOLDOWN_SECONDS": "0",
    "WEB_SEARCH_FETCH_COOLDOWN_SECONDS": "0",
    "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "0",
    "SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS": "0",
    "EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "EXPLORATION_FETCH_MIN_INTERVAL_SECONDS": "0",
    "DATAFORSEO_TASK_GET_BATCH_WORKERS": "4",
}


@dataclass
class HostedScriptTestRuntime:
    runtime_dir: Path
    runtime_env_file: Path
    base_url: str
    provider_mode: str
    orchestrator: Any
    server: Any
    thread: threading.Thread
    seed_result: dict[str, Any]


def isolated_runtime_state_paths(runtime_dir: str | Path) -> dict[str, Path]:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    return {
        "runtime_dir": runtime_root,
        "jobs_dir": runtime_root / "jobs",
        "company_assets_dir": runtime_root / "company_assets",
        "canonical_assets_dir": runtime_root / "company_assets",
        "hot_cache_assets_dir": runtime_root / "hot_cache_company_assets",
        "db_path": runtime_root / "sourcing_agent.db",
        "secrets_file": runtime_root / "secrets" / "providers.local.json",
        "object_storage_dir": runtime_root / "object_store",
        "provider_cache_dir": runtime_root / "provider_cache",
        "object_sync_dir": runtime_root / "object_sync",
    }


def ensure_isolated_runtime_env_file(runtime_dir: str | Path, runtime_env_file: str | Path = "") -> Path:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    if str(runtime_env_file or "").strip():
        resolved = Path(str(runtime_env_file)).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"runtime env file not found: {resolved}")
        return resolved
    sentinel = runtime_root / ".isolated-local-postgres.env"
    if not sentinel.exists():
        sentinel.write_text(
            "# Explicit empty local-postgres env for isolated scripted runtime.\n"
            "# This blocks fallback to repo-level .local-postgres.env while keeping the runtime deterministic.\n",
            encoding="utf-8",
        )
    return sentinel


def build_isolated_runtime_env(
    *,
    runtime_dir: str | Path,
    runtime_env_file: str | Path = "",
    provider_mode: str = "simulate",
    scripted_scenario: str = "",
    extra_env: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], Path]:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    env_file = ensure_isolated_runtime_env_file(runtime_root, runtime_env_file)
    state_paths = isolated_runtime_state_paths(runtime_root)
    normalized_provider_mode = normalize_provider_mode(provider_mode or "simulate")
    runtime_environment = (
        str(dict(extra_env or {}).get("SOURCING_RUNTIME_ENVIRONMENT") or "").strip()
        or ("test" if normalized_provider_mode == "live" else normalized_provider_mode)
    )
    env_payload = {key: "" for key in _LOCAL_POSTGRES_ENV_KEYS}
    env_payload.update(
        {
            "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(env_file),
            "SOURCING_RUNTIME_DIR": str(runtime_root),
            "SOURCING_RUNTIME_ENVIRONMENT": runtime_environment,
            "SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_root),
            "SOURCING_JOBS_DIR": str(state_paths["jobs_dir"]),
            "SOURCING_COMPANY_ASSETS_DIR": str(state_paths["company_assets_dir"]),
            "SOURCING_CANONICAL_ASSETS_DIR": str(state_paths["canonical_assets_dir"]),
            "SOURCING_HOT_CACHE_ASSETS_DIR": str(state_paths["hot_cache_assets_dir"]),
            "SOURCING_DB_PATH": str(state_paths["db_path"]),
            "SOURCING_SECRETS_FILE": str(state_paths["secrets_file"]),
            "OBJECT_STORAGE_LOCAL_DIR": str(state_paths["object_storage_dir"]),
            "SOURCING_EXTERNAL_PROVIDER_MODE": normalized_provider_mode,
        }
    )
    if str(scripted_scenario or "").strip():
        env_payload["SOURCING_SCRIPTED_PROVIDER_SCENARIO"] = str(Path(scripted_scenario).expanduser().resolve())
    else:
        env_payload["SOURCING_SCRIPTED_PROVIDER_SCENARIO"] = ""
    for key, value in dict(extra_env or {}).items():
        env_payload[str(key)] = str(value)
    return env_payload, env_file


@contextmanager
def patched_environment(env_payload: Mapping[str, str]) -> Iterator[None]:
    original: dict[str, str | None] = {key: os.environ.get(key) for key in env_payload}
    try:
        for key, value in env_payload.items():
            os.environ[str(key)] = str(value)
        yield
    finally:
        for key, original_value in original.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


def _close_store_connection(orchestrator: Any) -> None:
    store = getattr(orchestrator, "store", None)
    connection = getattr(store, "_connection", None)
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        return


def _join_runtime_threads() -> None:
    runtime_thread_prefixes = (
        "hosted-workflow-",
        "progress-auto-takeover-",
        "workflow-job-lease-",
    )
    for _ in range(20):
        active_runtime_threads = [
            item
            for item in threading.enumerate()
            if item.is_alive() and any(item.name.startswith(prefix) for prefix in runtime_thread_prefixes)
        ]
        if not active_runtime_threads:
            return
        for active_thread in active_runtime_threads:
            active_thread.join(timeout=0.1)
        time.sleep(0.05)


@contextmanager
def isolated_hosted_test_runtime(
    *,
    runtime_dir: str | Path,
    runtime_env_file: str | Path = "",
    provider_mode: str = "simulate",
    scripted_scenario: str = "",
    seed_reference_runtime: bool = False,
    extra_env: Mapping[str, str] | None = None,
) -> Iterator[HostedScriptTestRuntime]:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    env_payload, resolved_env_file = build_isolated_runtime_env(
        runtime_dir=runtime_root,
        runtime_env_file=runtime_env_file,
        provider_mode=provider_mode,
        scripted_scenario=scripted_scenario,
        extra_env=extra_env,
    )
    seed_result: dict[str, Any] = {}
    server = None
    thread = None
    orchestrator = None
    with patched_environment(env_payload):
        try:
            if seed_reference_runtime:
                seeded = seed_reference_smoke_runtime(runtime_dir=runtime_root)
                seed_result = dict(seeded if isinstance(seeded, dict) else {})
            orchestrator = build_orchestrator()
            server = create_server(orchestrator, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            yield HostedScriptTestRuntime(
                runtime_dir=runtime_root,
                runtime_env_file=resolved_env_file,
                base_url=f"http://{host}:{port}",
                provider_mode=str(provider_mode or "simulate").strip() or "simulate",
                orchestrator=orchestrator,
                server=server,
                thread=thread,
                seed_result=seed_result,
            )
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
            if thread is not None:
                thread.join(timeout=5)
            _join_runtime_threads()
            if orchestrator is not None:
                _close_store_connection(orchestrator)
