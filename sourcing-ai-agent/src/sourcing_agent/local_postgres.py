from __future__ import annotations

import os
import re
import socket
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_DEFAULT_LOCAL_PG_PORT = 55432
_DEFAULT_LOCAL_PG_USER = "sourcing"
_DEFAULT_LOCAL_PG_DB = "sourcing_agent"
_DEFAULT_LOCAL_PG_SCHEMA = "public"
_LOCAL_LOOPBACK_POSTGRES_HOSTS = {"127.0.0.1", "localhost", "::1"}
_LOCAL_POSTGRES_ENV_KEYS = (
    "SOURCING_CONTROL_PLANE_POSTGRES_DSN",
    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE",
    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA",
    "SOURCING_LOCAL_POSTGRES_ROOT",
    "LOCAL_PG_ROOT",
    "LOCAL_PG_EXTRACT",
    "LOCAL_PG_DATA",
    "LOCAL_PG_RUN",
    "LOCAL_PG_PORT",
    "LOCAL_PG_USER",
    "LOCAL_PG_DB",
    "LOCAL_PG_SCHEMA",
)
_ISOLATED_RUNTIME_ENVIRONMENTS = {"test", "simulate", "scripted", "replay", "ci"}


def _normalize_anchor(base_dir: str | Path | None = None) -> Path:
    if base_dir is None:
        return Path.cwd().expanduser().resolve()
    return Path(base_dir).expanduser().resolve()


def _candidate_local_postgres_env_files(anchor: Path) -> list[Path]:
    candidates: list[Path] = []
    explicit_env_file = str(os.getenv("SOURCING_LOCAL_POSTGRES_ENV_FILE") or "").strip()
    if explicit_env_file:
        candidates.append(Path(explicit_env_file).expanduser())
    candidates.append(anchor / ".local-postgres.env")
    candidates.append(anchor / ".local-postgres" / "connection.env")
    for parent in anchor.parents:
        candidates.append(parent / ".local-postgres.env")
        candidates.append(parent / ".local-postgres" / "connection.env")
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate.expanduser())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def _strip_wrapping_quotes(value: str) -> str:
    normalized = str(value).strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {"'", '"'}:
        return normalized[1:-1]
    return normalized


def _load_local_postgres_env(anchor: Path) -> tuple[dict[str, str], Path | None]:
    for candidate in _candidate_local_postgres_env_files(anchor):
        if not candidate.exists() or not candidate.is_file():
            continue
        payload: dict[str, str] = {}
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            normalized_key = str(key or "").strip()
            if normalized_key not in _LOCAL_POSTGRES_ENV_KEYS:
                continue
            payload[normalized_key] = _strip_wrapping_quotes(raw_value)
        return payload, candidate.resolve()
    return {}, None


def _resolved_local_postgres_env(base_dir: str | Path | None = None) -> tuple[dict[str, str], Path | None]:
    anchor = _normalize_anchor(base_dir)
    file_payload, env_file = _load_local_postgres_env(anchor)
    merged = dict(file_payload)
    for key in _LOCAL_POSTGRES_ENV_KEYS:
        explicit = str(os.getenv(key) or "").strip()
        if explicit:
            merged[key] = explicit
    return merged, env_file


def _resolve_path_like_value(raw_value: str, *, relative_to: Path | None = None) -> Path:
    path = Path(str(raw_value).strip()).expanduser()
    if not path.is_absolute() and relative_to is not None:
        path = (relative_to / path).resolve()
    return path


def _candidate_local_postgres_roots(
    anchor: Path,
    *,
    env_values: dict[str, str] | None = None,
    env_file: Path | None = None,
) -> list[Path]:
    candidates: list[Path] = []
    env_payload = env_values or {}
    explicit_root = str(
        env_payload.get("SOURCING_LOCAL_POSTGRES_ROOT") or env_payload.get("LOCAL_PG_ROOT") or ""
    ).strip()
    if explicit_root:
        candidates.append(_resolve_path_like_value(explicit_root, relative_to=env_file.parent if env_file else anchor))
    candidates.append(anchor / ".local-postgres")
    for parent in anchor.parents:
        candidates.append(parent / ".local-postgres")
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate.expanduser())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def discover_local_postgres_root(base_dir: str | Path | None = None) -> Path | None:
    anchor = _normalize_anchor(base_dir)
    env_values, env_file = _resolved_local_postgres_env(anchor)
    for candidate in _candidate_local_postgres_roots(anchor, env_values=env_values, env_file=env_file):
        if candidate.exists():
            return candidate
    return None


def resolve_local_postgres_settings(base_dir: str | Path | None = None) -> dict[str, Any]:
    anchor = _normalize_anchor(base_dir)
    env_values, env_file = _resolved_local_postgres_env(anchor)
    root = discover_local_postgres_root(anchor)
    port = int(str(env_values.get("LOCAL_PG_PORT") or _DEFAULT_LOCAL_PG_PORT).strip() or _DEFAULT_LOCAL_PG_PORT)
    user = str(env_values.get("LOCAL_PG_USER") or _DEFAULT_LOCAL_PG_USER).strip() or _DEFAULT_LOCAL_PG_USER
    database = str(env_values.get("LOCAL_PG_DB") or _DEFAULT_LOCAL_PG_DB).strip() or _DEFAULT_LOCAL_PG_DB
    configured_dsn = str(env_values.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    if root is None:
        return {
            "available": False,
            "root": None,
            "extract_dir": None,
            "data_dir": None,
            "run_dir": None,
            "port": port,
            "user": user,
            "database": database,
            "dsn": configured_dsn,
            "pg_ctl_path": "",
            "env_file": str(env_file) if env_file is not None else "",
            "configured_dsn": configured_dsn,
        }
    relative_base = env_file.parent if env_file is not None else anchor
    extract_dir = _resolve_path_like_value(str(env_values.get("LOCAL_PG_EXTRACT") or root / "extract"), relative_to=relative_base)
    data_dir = _resolve_path_like_value(str(env_values.get("LOCAL_PG_DATA") or root / "data"), relative_to=relative_base)
    run_dir = _resolve_path_like_value(str(env_values.get("LOCAL_PG_RUN") or root / "run"), relative_to=relative_base)
    pg_ctl_path = extract_dir / "usr/lib/postgresql/16/bin/pg_ctl"
    available = extract_dir.exists() and data_dir.exists()
    default_dsn = f"postgresql://{user}@127.0.0.1:{port}/{database}" if available else ""
    return {
        "available": bool(available),
        "root": root,
        "extract_dir": extract_dir,
        "data_dir": data_dir,
        "run_dir": run_dir,
        "port": port,
        "user": user,
        "database": database,
        "dsn": configured_dsn or default_dsn,
        "pg_ctl_path": str(pg_ctl_path) if pg_ctl_path.exists() else "",
        "env_file": str(env_file) if env_file is not None else "",
        "configured_dsn": configured_dsn,
    }


def resolve_control_plane_postgres_dsn(base_dir: str | Path | None = None) -> str:
    env_values, _ = _resolved_local_postgres_env(base_dir)
    explicit = str(env_values.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip()
    if explicit:
        return explicit
    settings = resolve_local_postgres_settings(base_dir)
    return str(settings.get("dsn") or "").strip()


def normalize_control_plane_postgres_schema(value: Any, *, default: str = _DEFAULT_LOCAL_PG_SCHEMA) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    if not raw:
        raw = str(default or _DEFAULT_LOCAL_PG_SCHEMA).strip().lower() or _DEFAULT_LOCAL_PG_SCHEMA
    if raw and raw[0].isdigit():
        raw = f"s_{raw}"
    return raw[:63] or _DEFAULT_LOCAL_PG_SCHEMA


def resolve_control_plane_postgres_schema(
    base_dir: str | Path | None = None,
    *,
    runtime_environment: str | None = None,
) -> str:
    env_values, _ = _resolved_local_postgres_env(base_dir)
    explicit = str(
        env_values.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA")
        or env_values.get("LOCAL_PG_SCHEMA")
        or ""
    ).strip()
    if explicit:
        return normalize_control_plane_postgres_schema(explicit)
    runtime_env = str(runtime_environment or os.getenv("SOURCING_RUNTIME_ENVIRONMENT") or "").strip().lower()
    runtime_env = runtime_env.replace("-", "_")
    if runtime_env in _ISOLATED_RUNTIME_ENVIRONMENTS:
        return normalize_control_plane_postgres_schema(f"sourcing_{runtime_env}")
    return _DEFAULT_LOCAL_PG_SCHEMA


def quote_control_plane_postgres_identifier(value: Any) -> str:
    normalized = normalize_control_plane_postgres_schema(value)
    return '"' + normalized.replace('"', '""') + '"'


def configure_control_plane_postgres_session(
    connection: Any,
    *,
    schema: str | None = None,
    create_schema: bool = True,
) -> Any:
    resolved_schema = normalize_control_plane_postgres_schema(
        schema or resolve_control_plane_postgres_schema()
    )
    cursor_factory = getattr(connection, "cursor", None)
    if not callable(cursor_factory):
        return connection
    with cursor_factory() as cursor:
        try:
            cursor.execute("SET client_encoding TO 'UTF8'")
        except Exception:
            pass
        if resolved_schema and resolved_schema != _DEFAULT_LOCAL_PG_SCHEMA:
            quoted_schema = quote_control_plane_postgres_identifier(resolved_schema)
            if create_schema:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
            cursor.execute(f"SET search_path TO {quoted_schema}, public")
    return connection


def normalize_control_plane_postgres_connect_dsn(dsn: str) -> str:
    normalized = str(dsn or "").strip()
    if not normalized:
        return ""
    try:
        parsed = urlsplit(normalized)
    except Exception:
        return normalized
    if str(parsed.scheme or "").strip().lower() not in {"postgres", "postgresql"}:
        return normalized
    hostname = str(parsed.hostname or "").strip().lower()
    if hostname not in _LOCAL_LOOPBACK_POSTGRES_HOSTS:
        return normalized
    query_items = list(parse_qsl(parsed.query, keep_blank_values=True))
    existing_keys = {str(key or "").strip().lower() for key, _ in query_items}
    if "gssencmode" not in existing_keys:
        query_items.append(("gssencmode", "disable"))
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query_items),
            parsed.fragment,
        )
    )


def resolve_default_control_plane_db_path(
    runtime_dir: str | Path,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    anchor = base_dir if base_dir is not None else runtime_root
    if resolve_control_plane_postgres_dsn(anchor):
        return runtime_root / "control_plane.shadow.db"
    return runtime_root / "sourcing_agent.db"


def resolve_default_control_plane_postgres_live_mode(
    explicit_value: Any = "",
    *,
    base_dir: str | Path | None = None,
) -> str:
    env_values, _ = _resolved_local_postgres_env(base_dir)
    normalized = str(
        explicit_value or env_values.get("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE") or ""
    ).strip().lower()
    if normalized in {"mirror", "prefer_postgres", "postgres_only"}:
        return normalized
    return "postgres_only" if resolve_control_plane_postgres_dsn(base_dir) else "disabled"


def local_postgres_is_running(base_dir: str | Path | None = None) -> bool:
    settings = resolve_local_postgres_settings(base_dir)
    if not bool(settings.get("available")):
        return False
    port = int(settings.get("port") or _DEFAULT_LOCAL_PG_PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        return sock.connect_ex(("127.0.0.1", port)) == 0
    finally:
        sock.close()


def ensure_local_postgres_started(base_dir: str | Path | None = None) -> dict[str, Any]:
    settings = resolve_local_postgres_settings(base_dir)
    if not bool(settings.get("available")):
        return {
            "status": "missing_assets",
            "dsn": "",
            "root": str(settings.get("root") or ""),
        }
    if local_postgres_is_running(base_dir):
        return {
            "status": "running",
            "dsn": str(settings.get("dsn") or ""),
            "root": str(settings.get("root") or ""),
        }
    pg_ctl_path = str(settings.get("pg_ctl_path") or "").strip()
    if not pg_ctl_path:
        return {
            "status": "missing_pg_ctl",
            "dsn": str(settings.get("dsn") or ""),
            "root": str(settings.get("root") or ""),
        }
    run_dir = Path(str(settings.get("run_dir") or "")).expanduser()
    data_dir = Path(str(settings.get("data_dir") or "")).expanduser()
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "postgres.log"
    command = [
        pg_ctl_path,
        "-D",
        str(data_dir),
        "-l",
        str(log_path),
        "-o",
        f"-k {run_dir} -p {int(settings.get('port') or _DEFAULT_LOCAL_PG_PORT)} -h 127.0.0.1",
        "start",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if local_postgres_is_running(base_dir):
        return {
            "status": "started",
            "dsn": str(settings.get("dsn") or ""),
            "root": str(settings.get("root") or ""),
            "stdout": str(result.stdout or "").strip(),
            "stderr": str(result.stderr or "").strip(),
        }
    return {
        "status": "failed",
        "dsn": str(settings.get("dsn") or ""),
        "root": str(settings.get("root") or ""),
        "stdout": str(result.stdout or "").strip(),
        "stderr": str(result.stderr or "").strip(),
    }


def inspect_local_postgres_encoding(base_dir: str | Path | None = None) -> dict[str, Any]:
    settings = resolve_local_postgres_settings(base_dir)
    dsn = str(settings.get("dsn") or "").strip()
    if not bool(settings.get("available")) or not dsn:
        return {
            "available": False,
            "running": False,
            "status": "missing",
        }
    running = local_postgres_is_running(base_dir)
    if not running:
        return {
            "available": True,
            "running": False,
            "status": "stopped",
        }
    try:
        import psycopg
    except ImportError:
        return {
            "available": True,
            "running": True,
            "status": "missing_psycopg",
        }
    try:
        with psycopg.connect(dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW server_encoding")
                server_encoding = str((cursor.fetchone() or [""])[0] or "").strip()
                cursor.execute("SHOW client_encoding")
                client_encoding = str((cursor.fetchone() or [""])[0] or "").strip()
                cursor.execute(
                    """
                    SELECT datname, pg_encoding_to_char(encoding), datcollate, datctype
                    FROM pg_database
                    WHERE datname = current_database()
                    """
                )
                row = cursor.fetchone() or ("", "", "", "")
        database_name = str(row[0] or "").strip()
        database_encoding = str(row[1] or "").strip()
        lc_collate = str(row[2] or "").strip()
        lc_ctype = str(row[3] or "").strip()
        encoding_warning = server_encoding.upper() == "SQL_ASCII" or database_encoding.upper() == "SQL_ASCII"
        return {
            "available": True,
            "running": True,
            "status": "ok",
            "database": database_name,
            "server_encoding": server_encoding,
            "client_encoding": client_encoding,
            "database_encoding": database_encoding,
            "lc_collate": lc_collate,
            "lc_ctype": lc_ctype,
            "encoding_warning": encoding_warning,
        }
    except Exception as exc:
        return {
            "available": True,
            "running": True,
            "status": "probe_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def describe_control_plane_runtime(
    *,
    base_dir: str | Path | None = None,
    runtime_dir: str | Path | None = None,
) -> dict[str, Any]:
    anchor = _normalize_anchor(base_dir if base_dir is not None else runtime_dir)
    resolved_runtime_dir = (
        Path(runtime_dir).expanduser().resolve()
        if runtime_dir is not None
        else None
    )
    local_settings = resolve_local_postgres_settings(anchor)
    local_postgres_summary = {
        "available": bool(local_settings.get("available")),
        "root": str(local_settings.get("root") or ""),
        "extract_dir": str(local_settings.get("extract_dir") or ""),
        "data_dir": str(local_settings.get("data_dir") or ""),
        "run_dir": str(local_settings.get("run_dir") or ""),
        "env_file": str(local_settings.get("env_file") or ""),
        "port": int(local_settings.get("port") or _DEFAULT_LOCAL_PG_PORT),
        "user": str(local_settings.get("user") or _DEFAULT_LOCAL_PG_USER),
        "database": str(local_settings.get("database") or _DEFAULT_LOCAL_PG_DB),
        "dsn": str(local_settings.get("dsn") or ""),
        "configured_dsn": str(local_settings.get("configured_dsn") or ""),
        "pg_ctl_path": str(local_settings.get("pg_ctl_path") or ""),
        "running": local_postgres_is_running(anchor) if bool(local_settings.get("available")) else False,
        "encoding_probe": inspect_local_postgres_encoding(anchor),
    }
    summary: dict[str, Any] = {
        "anchor": str(anchor),
        "resolved_control_plane_postgres_dsn": resolve_control_plane_postgres_dsn(anchor),
        "resolved_control_plane_postgres_live_mode": resolve_default_control_plane_postgres_live_mode(base_dir=anchor),
        "resolved_control_plane_postgres_schema": resolve_control_plane_postgres_schema(anchor),
        "local_postgres": local_postgres_summary,
    }
    if resolved_runtime_dir is not None:
        default_db_path = resolve_default_control_plane_db_path(resolved_runtime_dir, base_dir=anchor)
        summary.update(
            {
                "runtime_dir": str(resolved_runtime_dir),
                "default_db_path": str(default_db_path),
                "default_db_path_exists": default_db_path.exists(),
            }
        )
    return summary
