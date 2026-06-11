from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .api import create_server
from .cli import build_orchestrator
from .local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)
from .process_supervision import process_alive
from .runtime_environment import (
    LIVE_PROVIDER_ACCESS_DISABLED_ENV,
    LIVE_PROVIDER_SECRET_ENV_KEYS,
    NON_LIVE_PROVIDER_MODES,
    normalize_provider_mode,
    provider_isolation_env_overrides,
)
from .service_daemon import (
    clear_service_stop_request,
    read_service_status,
    request_service_stop,
)
from .smoke_runtime_seed import seed_reference_smoke_runtime

_LOCAL_POSTGRES_ENV_KEYS = (
    "SOURCING_CONTROL_PLANE_POSTGRES_DSN",
    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE",
    "SOURCING_LOCAL_POSTGRES_ROOT",
    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA",
    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES",
    "SOURCING_PG_ONLY_SQLITE_BACKEND",
    "LOCAL_PG_ROOT",
    "LOCAL_PG_EXTRACT",
    "LOCAL_PG_DATA",
    "LOCAL_PG_RUN",
    "LOCAL_PG_PORT",
    "LOCAL_PG_USER",
    "LOCAL_PG_DB",
)

_LOGGER = logging.getLogger(__name__)

_LOCAL_SCRIPTED_PROVIDER_WEBHOOK_TOKEN = "local-scripted-provider-webhook-token"
_WORKFLOW_CONFIDENCE_ENV_FILE_NAME = ".scripted-local-postgres.env"
EPHEMERAL_TEST_ENV_MARKER_NAME = ".ephemeral-test-env.json"
_WORKFLOW_CONFIDENCE_SCHEMA_PREFIXES = (
    "sourcing_scripted",
    "sourcing_simulate",
    "sourcing_replay",
    "sourcing_test",
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
    "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0.1",
    "SOURCING_ALLOW_UNSIGNED_PROVIDER_WEBHOOKS": "1",
    "SOURCING_PROVIDER_WEBHOOK_SOURCE_OVERRIDE_ENABLED": "1",
}


def _runtime_env_file_assignments(env_file: Path) -> dict[str, str]:
    assignments: dict[str, str] = {}
    try:
        lines = env_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return assignments
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        normalized_value = value.strip()
        if (
            len(normalized_value) >= 2
            and normalized_value[0] == normalized_value[-1]
            and normalized_value[0] in {"'", '"'}
        ):
            normalized_value = normalized_value[1:-1]
        assignments[normalized_key] = normalized_value
    return assignments


def _shell_env_quote(value: str) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _default_isolated_postgres_schema(runtime_dir: Path, *, provider_mode: str) -> str:
    normalized_provider_mode = normalize_provider_mode(provider_mode or "simulate")
    resolved_runtime_dir = Path(runtime_dir).expanduser().resolve()
    runtime_label = normalize_control_plane_postgres_schema(resolved_runtime_dir.name or "runtime")
    runtime_label = runtime_label[:24].strip("_") or "runtime"
    runtime_digest = hashlib.sha1(str(resolved_runtime_dir).encode("utf-8")).hexdigest()[:8]
    raw_schema = f"sourcing_{normalized_provider_mode}_{runtime_label}_{runtime_digest}"
    return normalize_control_plane_postgres_schema(
        raw_schema,
        default=f"sourcing_{normalized_provider_mode}_{runtime_digest}",
    )


def _write_runtime_scoped_postgres_env_file(
    runtime_dir: Path,
    *,
    provider_mode: str,
) -> Path:
    """Create the PG-only control-plane env file for workflow confidence runtimes.

    This intentionally fails closed when no Postgres DSN can be resolved. Scripted
    smoke/manual browser confidence must exercise the same PG queue/lock behavior
    as production; SQLite remains a unit-test compatibility backend only.
    """

    runtime_dir.mkdir(parents=True, exist_ok=True)
    state_paths = isolated_runtime_state_paths(runtime_dir)
    dsn = resolve_control_plane_postgres_dsn(Path.cwd())
    if not str(dsn or "").strip():
        raise RuntimeError(
            "PG-only workflow confidence runtime requires SOURCING_CONTROL_PLANE_POSTGRES_DSN "
            "or a local .local-postgres.env. SQLite fallback is not allowed for scripted/manual smoke."
        )
    normalized_provider_mode = normalize_provider_mode(provider_mode or "simulate")
    schema = _default_isolated_postgres_schema(runtime_dir, provider_mode=normalized_provider_mode)
    env_file = runtime_dir / _WORKFLOW_CONFIDENCE_ENV_FILE_NAME
    env_file.write_text(
        "\n".join(
            [
                "# Generated by scripted_test_runtime.py.",
                "# Workflow confidence runtimes are PG-only; SQLite is not a smoke/manual control plane.",
                f"export SOURCING_CONTROL_PLANE_POSTGRES_DSN={_shell_env_quote(str(dsn).strip())}",
                "export SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                f"export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA={_shell_env_quote(schema)}",
                "export SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1",
                "export SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory",
                f"export SOURCING_RUNTIME_DIR={_shell_env_quote(str(runtime_dir))}",
                f"export SOURCING_COMPANY_REGISTRY_RUNTIME_DIR={_shell_env_quote(str(runtime_dir))}",
                f"export SOURCING_JOBS_DIR={_shell_env_quote(str(state_paths['jobs_dir']))}",
                f"export SOURCING_COMPANY_ASSETS_DIR={_shell_env_quote(str(state_paths['company_assets_dir']))}",
                f"export SOURCING_CANONICAL_ASSETS_DIR={_shell_env_quote(str(state_paths['canonical_assets_dir']))}",
                f"export SOURCING_HOT_CACHE_ASSETS_DIR={_shell_env_quote(str(state_paths['hot_cache_assets_dir']))}",
                f"export SOURCING_DB_PATH={_shell_env_quote(str(state_paths['db_path']))}",
                f"export SOURCING_SECRETS_FILE={_shell_env_quote(str(state_paths['secrets_file']))}",
                f"export OBJECT_STORAGE_LOCAL_DIR={_shell_env_quote(str(state_paths['object_storage_dir']))}",
                f"export SOURCING_RUNTIME_ENVIRONMENT={_shell_env_quote('test' if normalized_provider_mode == 'live' else normalized_provider_mode)}",
                f"export SOURCING_EXTERNAL_PROVIDER_MODE={_shell_env_quote(normalized_provider_mode)}",
                "export SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1",
                *(
                    ["export SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=1"]
                    if normalized_provider_mode == "scripted"
                    else []
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    return env_file


def prepare_workflow_confidence_postgres_schema(env_payload: Mapping[str, str]) -> dict[str, Any]:
    """Fail closed unless the isolated workflow runtime can use a PG-only schema."""

    raw_dsn = str(env_payload.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    schema = normalize_control_plane_postgres_schema(
        env_payload.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or ""
    )
    if not raw_dsn:
        raise RuntimeError("PG-only workflow confidence runtime requires a non-empty Postgres DSN")
    if not schema or not schema.startswith(_WORKFLOW_CONFIDENCE_SCHEMA_PREFIXES):
        raise RuntimeError(
            "PG-only workflow confidence runtime refuses unsafe Postgres schema: "
            f"{schema or '<empty>'}"
        )
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("PG-only workflow confidence runtime requires psycopg") from exc
    dsn = normalize_control_plane_postgres_connect_dsn(raw_dsn)
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                # Ownership check BEFORE CREATE SCHEMA IF NOT EXISTS: when the
                # schema already exists (e.g. supplied via --runtime-env-file),
                # this runtime did not create it and teardown must not drop it.
                cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))
                pre_existing = cursor.fetchone() is not None
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_control_plane_postgres_identifier(schema)}")
    except Exception as exc:
        raise RuntimeError(
            "PG-only workflow confidence runtime could not connect to or prepare Postgres schema "
            f"{schema}: {type(exc).__name__}: {exc}"
        ) from exc
    return {
        "status": "ready",
        "schema": schema,
        "dsn_configured": True,
        "pre_existing": pre_existing,
    }


def write_ephemeral_test_env_marker(
    runtime_dir: str | Path,
    *,
    schema: str,
    provider_mode: str = "simulate",
    extra: Mapping[str, Any] | None = None,
) -> Path | None:
    """Pair the runtime dir with its per-runtime PG schema for the janitor.

    Writes `.ephemeral-test-env.json` into the runtime dir so
    `scripts/prune_test_schemas.py` can tell referenced schemas from orphans.
    Fail-soft: marker write failures must never break the runtime itself.
    """

    runtime_root = Path(runtime_dir).expanduser().resolve()
    payload: dict[str, Any] = {
        "schema": normalize_control_plane_postgres_schema(schema),
        "runtime_dir": str(runtime_root),
        "provider_mode": normalize_provider_mode(provider_mode or "simulate"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "sourcing_agent.scripted_test_runtime",
    }
    payload.update({str(key): value for key, value in dict(extra or {}).items()})
    marker_path = runtime_root / EPHEMERAL_TEST_ENV_MARKER_NAME
    try:
        runtime_root.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        _LOGGER.warning(
            "could not write ephemeral test env marker %s: %s: %s",
            marker_path,
            type(exc).__name__,
            exc,
        )
        return None
    return marker_path


def drop_workflow_confidence_postgres_schema(env_payload: Mapping[str, str]) -> dict[str, Any]:
    """Teardown twin of prepare_workflow_confidence_postgres_schema.

    Drops the per-runtime PG schema so isolated workflow-confidence runtimes are
    truly ephemeral (paired schema + runtime dir). Fail-soft by contract: this is
    cleanup-path code and must never raise; failures are logged and reported in
    the returned payload. Refuses schemas outside the workflow-confidence
    prefixes so it can never drop a shared/production schema.
    """

    raw_dsn = str(dict(env_payload or {}).get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    schema = normalize_control_plane_postgres_schema(
        dict(env_payload or {}).get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or "",
        default="",
    )
    if not schema or not schema.startswith(_WORKFLOW_CONFIDENCE_SCHEMA_PREFIXES):
        return {"status": "skipped", "schema": schema, "reason": "unsafe_schema"}
    if not raw_dsn:
        return {"status": "skipped", "schema": schema, "reason": "postgres_dsn_missing"}
    try:
        import psycopg
    except ImportError as exc:
        _LOGGER.warning("could not drop ephemeral test schema %s: psycopg unavailable: %s", schema, exc)
        return {"status": "failed", "schema": schema, "reason": f"psycopg_unavailable:{exc}"}
    dsn = normalize_control_plane_postgres_connect_dsn(raw_dsn)
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(schema)} CASCADE"
                )
    except Exception as exc:
        _LOGGER.warning(
            "could not drop ephemeral test schema %s: %s: %s",
            schema,
            type(exc).__name__,
            exc,
        )
        return {"status": "failed", "schema": schema, "reason": f"{type(exc).__name__}: {exc}"}
    return {"status": "dropped", "schema": schema}


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
    postgres_prepare_result: dict[str, Any]


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


def ensure_isolated_runtime_env_file(
    runtime_dir: str | Path,
    runtime_env_file: str | Path = "",
    *,
    provider_mode: str = "simulate",
) -> Path:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    if str(runtime_env_file or "").strip():
        resolved = Path(str(runtime_env_file)).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"runtime env file not found: {resolved}")
        return resolved
    return _write_runtime_scoped_postgres_env_file(runtime_root, provider_mode=provider_mode)


def build_isolated_runtime_env(
    *,
    runtime_dir: str | Path,
    runtime_env_file: str | Path = "",
    provider_mode: str = "simulate",
    scripted_scenario: str = "",
    extra_env: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], Path]:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    env_file = (
        ensure_isolated_runtime_env_file(
            runtime_root,
            runtime_env_file,
            provider_mode=provider_mode,
        )
        if str(runtime_env_file or "").strip()
        else _write_runtime_scoped_postgres_env_file(runtime_root, provider_mode=provider_mode)
    )
    state_paths = isolated_runtime_state_paths(runtime_root)
    normalized_provider_mode = normalize_provider_mode(provider_mode or "simulate")
    runtime_environment = (
        str(dict(extra_env or {}).get("SOURCING_RUNTIME_ENVIRONMENT") or "").strip()
        or ("test" if normalized_provider_mode == "live" else normalized_provider_mode)
    )
    env_payload = {key: "" for key in _LOCAL_POSTGRES_ENV_KEYS}
    env_file_assignments = _runtime_env_file_assignments(env_file)
    env_payload.update(
        {
            "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(env_file),
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN": str(
                env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or ""
            ),
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": str(
                env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE") or "postgres_only"
            ),
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": str(
                env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA")
                or _default_isolated_postgres_schema(runtime_root, provider_mode=normalized_provider_mode)
            ),
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": str(
                env_file_assignments.get("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES") or "1"
            ),
            "SOURCING_PG_ONLY_SQLITE_BACKEND": str(
                env_file_assignments.get("SOURCING_PG_ONLY_SQLITE_BACKEND") or "shared_memory"
            ),
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
    # Workflow confidence control-plane semantics are not case-tunable.
    env_payload["SOURCING_LOCAL_POSTGRES_ENV_FILE"] = str(env_file)
    env_payload["SOURCING_CONTROL_PLANE_POSTGRES_DSN"] = str(
        env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or ""
    )
    env_payload["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = "postgres_only"
    env_payload["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"] = str(
        env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA")
        or _default_isolated_postgres_schema(runtime_root, provider_mode=normalized_provider_mode)
    )
    env_payload["SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES"] = "1"
    env_payload["SOURCING_PG_ONLY_SQLITE_BACKEND"] = "shared_memory"
    env_payload.update(
        provider_isolation_env_overrides(
            provider_mode=normalized_provider_mode,
            runtime_environment=runtime_environment,
            runtime_dir=runtime_root,
            environ=env_payload,
        )
    )
    if normalized_provider_mode in {"simulate", "scripted", "replay"}:
        webhook_token = (
            str(env_payload.get("SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN") or "").strip()
            or str(env_payload.get("SOURCING_PROVIDER_WEBHOOK_TOKEN") or "").strip()
            or _LOCAL_SCRIPTED_PROVIDER_WEBHOOK_TOKEN
        )
        env_payload["SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN"] = webhook_token
        env_payload["SOURCING_PROVIDER_WEBHOOK_TOKEN"] = webhook_token
        env_payload.setdefault("SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN", "0")
    if normalized_provider_mode == "scripted":
        env_payload.setdefault("SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED", "1")
    validation = validate_isolated_runtime_env_contract(
        env_payload=env_payload,
        runtime_dir=runtime_root,
        runtime_env_file=env_file,
        provider_mode=normalized_provider_mode,
    )
    if str(validation.get("status") or "") != "ok":
        raise RuntimeError(
            "isolated runtime env contract violation: "
            + "; ".join(str(item) for item in list(validation.get("violations") or []))
        )
    return env_payload, env_file


def validate_isolated_runtime_env_contract(
    *,
    env_payload: Mapping[str, str],
    runtime_dir: str | Path,
    runtime_env_file: str | Path,
    provider_mode: str,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser().resolve()
    env_file = Path(runtime_env_file).expanduser()
    normalized_provider_mode = normalize_provider_mode(provider_mode)
    env = {str(key): str(value) for key, value in dict(env_payload or {}).items()}
    violations: list[str] = []
    if not env_file.exists() or not env_file.is_file():
        violations.append(f"runtime_env_file_missing:{env_file}")
    env_file_assignments = _runtime_env_file_assignments(env_file) if env_file.exists() else {}
    runtime_dir_value = str(env.get("SOURCING_RUNTIME_DIR") or "").strip()
    if not runtime_dir_value:
        violations.append("SOURCING_RUNTIME_DIR_missing")
    else:
        try:
            if Path(runtime_dir_value).expanduser().resolve() != runtime_root:
                violations.append(f"SOURCING_RUNTIME_DIR_mismatch:{runtime_dir_value}")
        except OSError:
            violations.append(f"SOURCING_RUNTIME_DIR_unresolvable:{runtime_dir_value}")
    effective_provider_mode = normalize_provider_mode(env.get("SOURCING_EXTERNAL_PROVIDER_MODE"))
    if effective_provider_mode != normalized_provider_mode:
        violations.append(
            f"SOURCING_EXTERNAL_PROVIDER_MODE_mismatch:{effective_provider_mode}!={normalized_provider_mode}"
        )
    if str(env.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip() == "":
        violations.append("SOURCING_CONTROL_PLANE_POSTGRES_DSN_missing")
    if str(env.get("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE") or "").strip().lower() != "postgres_only":
        violations.append("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE_not_postgres_only")
    if str(env.get("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES") or "").strip() != "1":
        violations.append("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES_not_enabled")
    if str(env.get("SOURCING_PG_ONLY_SQLITE_BACKEND") or "").strip().lower() != "shared_memory":
        violations.append("SOURCING_PG_ONLY_SQLITE_BACKEND_not_shared_memory")
    if str(env.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or "").strip() == "":
        violations.append("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA_missing")
    env_file_dsn = str(env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    if not env_file_dsn:
        violations.append("runtime_env_file_postgres_dsn_missing")
    env_file_live_mode = str(env_file_assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE") or "").strip().lower()
    if env_file_live_mode != "postgres_only":
        violations.append("runtime_env_file_postgres_live_mode_not_postgres_only")
    env_file_required_pg = str(env_file_assignments.get("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES") or "").strip()
    if env_file_required_pg != "1":
        violations.append("runtime_env_file_require_postgres_not_enabled")
    env_file_shadow_backend = str(env_file_assignments.get("SOURCING_PG_ONLY_SQLITE_BACKEND") or "").strip().lower()
    if env_file_shadow_backend != "shared_memory":
        violations.append("runtime_env_file_pg_only_sqlite_backend_not_shared_memory")
    if normalized_provider_mode in NON_LIVE_PROVIDER_MODES:
        if str(env.get(LIVE_PROVIDER_ACCESS_DISABLED_ENV) or "") != "1":
            violations.append(f"{LIVE_PROVIDER_ACCESS_DISABLED_ENV}_missing")
        for secret_key in LIVE_PROVIDER_SECRET_ENV_KEYS:
            if str(env.get(secret_key) or "").strip():
                violations.append(f"live_provider_secret_not_blank:{secret_key}")
            if str(env_file_assignments.get(secret_key) or "").strip():
                violations.append(f"runtime_env_file_live_provider_secret:{secret_key}")
        env_file_provider_mode = str(env_file_assignments.get("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip()
        if env_file_provider_mode and normalize_provider_mode(env_file_provider_mode) == "live":
            violations.append("runtime_env_file_provider_mode_live")
        env_file_runtime_environment = str(
            env_file_assignments.get("SOURCING_RUNTIME_ENVIRONMENT") or ""
        ).strip().lower()
        if env_file_runtime_environment and env_file_runtime_environment not in {
            "test",
            "simulate",
            "scripted",
            "replay",
            "ci",
        }:
            violations.append(f"runtime_env_file_environment_not_isolated:{env_file_runtime_environment}")
        env_file_live_disabled = str(env_file_assignments.get(LIVE_PROVIDER_ACCESS_DISABLED_ENV) or "").strip()
        if env_file_live_disabled and env_file_live_disabled != "1":
            violations.append(f"runtime_env_file_{LIVE_PROVIDER_ACCESS_DISABLED_ENV}_not_disabled")
        env_file_runtime_dir = str(env_file_assignments.get("SOURCING_RUNTIME_DIR") or "").strip()
        if env_file_runtime_dir:
            try:
                if Path(env_file_runtime_dir).expanduser().resolve() != runtime_root:
                    violations.append(f"runtime_env_file_SOURCING_RUNTIME_DIR_mismatch:{env_file_runtime_dir}")
            except OSError:
                violations.append(f"runtime_env_file_SOURCING_RUNTIME_DIR_unresolvable:{env_file_runtime_dir}")
        if normalized_provider_mode == "scripted":
            scripted_watch = str(env.get("SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED") or "").strip()
            if scripted_watch != "1":
                violations.append("SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_not_enabled")
        runtime_environment = str(env.get("SOURCING_RUNTIME_ENVIRONMENT") or "").strip().lower()
        if runtime_environment not in {"test", "simulate", "scripted", "replay", "ci"}:
            violations.append(f"SOURCING_RUNTIME_ENVIRONMENT_not_isolated:{runtime_environment or '<empty>'}")
    return {
        "status": "failed" if violations else "ok",
        "violations": violations,
        "runtime_dir": str(runtime_root),
        "runtime_env_file": str(env_file),
        "provider_mode": normalized_provider_mode,
    }


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
    if store is None:
        return
    # Prefer the store-level close(): it disposes the live PG adapter pool in
    # addition to the sqlite compatibility connection (multi-runtime processes
    # would otherwise accumulate one pool per runtime).
    close = getattr(store, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass
        return
    # Legacy fallback for stores without close(): sqlite connection only.
    connection = getattr(store, "_connection", None)
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        return


def _join_runtime_threads() -> None:
    runtime_thread_prefixes = (
        "provider-webhook-event",
        "hosted-workflow-",
        "hosted-stage2-",
        "harvest-profile-run-watch-",
        "progress-auto-takeover-",
        "workflow-job-lease-",
        "workflow-runtime-controls-",
        "job-recovery-",
        "worker-recovery-daemon-thread",
        "shared-recovery-deferred-",
        "hosted-runtime-watchdog-deferred-",
        "profile-completion-refill-",
        "background-snapshot-materialization-",
        "background-outreach-layering-",
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


def _runtime_sidecar_service_names(runtime_root: Path) -> list[str]:
    services_root = Path(runtime_root) / "services"
    if not services_root.exists():
        return []
    names: list[str] = []
    for status_path in sorted(services_root.glob("*/status.json")):
        service_name = str(status_path.parent.name or "").strip()
        if service_name:
            names.append(service_name)
    return names


def _paths_refer_to_same_location(left: str | Path, right: str | Path) -> bool:
    left_path = Path(left).expanduser()
    right_path = Path(right).expanduser()
    try:
        return left_path.resolve().samefile(right_path.resolve())
    except (OSError, RuntimeError, ValueError):
        try:
            return left_path.resolve() == right_path.resolve()
        except OSError:
            return str(left_path) == str(right_path)


def _cleanup_runtime_sidecar_processes(runtime_root: Path, *, grace_seconds: float = 2.0) -> dict[str, Any]:
    normalized_runtime_root = Path(runtime_root).expanduser().resolve()
    findings: list[dict[str, Any]] = []
    for service_name in _runtime_sidecar_service_names(normalized_runtime_root):
        status = read_service_status(normalized_runtime_root, service_name)
        status_runtime_dir = str(status.get("runtime_dir") or "").strip()
        if not status_runtime_dir:
            continue
        if not _paths_refer_to_same_location(status_runtime_dir, normalized_runtime_root):
            continue
        pid = int(status.get("pid") or 0)
        if pid <= 0 or pid == os.getpid() or not process_alive(pid):
            continue
        stop_request = request_service_stop(
            normalized_runtime_root,
            service_name,
            reason="isolated_hosted_test_runtime_cleanup",
            requested_by="scripted_test_runtime",
            target_status=status,
            target_scope="service_shutdown_fence",
        )
        deadline = time.monotonic() + max(0.1, float(grace_seconds or 0.0))
        while time.monotonic() < deadline and process_alive(pid):
            time.sleep(0.05)
        if process_alive(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
            kill_deadline = time.monotonic() + 1.0
            while time.monotonic() < kill_deadline and process_alive(pid):
                time.sleep(0.05)
        if process_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        if not process_alive(pid):
            clear_service_stop_request(normalized_runtime_root, service_name)
        findings.append(
            {
                "service_name": service_name,
                "pid": pid,
                "stop_request": stop_request,
                "terminated": not process_alive(pid),
            }
        )
    return {
        "status": "completed",
        "runtime_dir": str(normalized_runtime_root),
        "cleaned_count": len(findings),
        "findings": findings,
    }


@contextmanager
def isolated_hosted_test_runtime(
    *,
    runtime_dir: str | Path,
    runtime_env_file: str | Path = "",
    provider_mode: str = "simulate",
    scripted_scenario: str = "",
    seed_reference_runtime: bool = False,
    extra_env: Mapping[str, str] | None = None,
    keep_schema: bool = False,
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
    postgres_prepare_result = prepare_workflow_confidence_postgres_schema(env_payload)
    schema_pre_existing = bool(postgres_prepare_result.get("pre_existing"))
    write_ephemeral_test_env_marker(
        runtime_root,
        schema=str(env_payload.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or ""),
        provider_mode=provider_mode,
        extra={
            "runtime_env_file": str(resolved_env_file),
            "keep_schema": bool(keep_schema),
            "pre_existing": schema_pre_existing,
        },
    )
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
                postgres_prepare_result=postgres_prepare_result,
            )
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
            if thread is not None:
                thread.join(timeout=5)
            _cleanup_runtime_sidecar_processes(runtime_root)
            _join_runtime_threads()
            if orchestrator is not None:
                repair = getattr(orchestrator, "repair_dead_local_recovery_leases", None)
                if callable(repair):
                    repair({"source": "isolated_hosted_test_runtime_cleanup", "runtime_dir": str(runtime_root)})
            if orchestrator is not None:
                _close_store_connection(orchestrator)
            if keep_schema:
                _LOGGER.info(
                    "keeping ephemeral test schema %s for debugging (keep_schema=True); "
                    "reclaim later via scripts/prune_test_schemas.py",
                    str(env_payload.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or ""),
                )
            elif schema_pre_existing:
                # Ownership boundary: the schema existed before this runtime
                # prepared it (typically named via a user-supplied
                # --runtime-env-file). We did not create it, so teardown must
                # not drop it.
                _LOGGER.info(
                    "keeping pre-existing schema %s: it was not created by this runtime",
                    str(env_payload.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA") or ""),
                )
            else:
                # Ephemeral test-environment contract v2: the per-runtime PG schema is
                # paired with the runtime dir and must not outlive the runtime.
                # Fail-soft: cleanup never raises (drop helper logs failures).
                drop_workflow_confidence_postgres_schema(env_payload)
