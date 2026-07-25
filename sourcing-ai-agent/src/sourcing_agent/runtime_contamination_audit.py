from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import Any, Iterable

from .control_plane_postgres import _import_psycopg
from .local_postgres import (
    configure_control_plane_postgres_session,
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    resolve_control_plane_postgres_dsn,
    resolve_control_plane_postgres_schema,
)
from .runtime_environment import synthetic_provider_input_markers

_CONTROL_PLANE_TABLES_TO_AUDIT = (
    "jobs",
    "agent_worker_runs",
    "job_materialization_items",
    "job_result_views",
    "job_result_lifecycle",
    "job_board_visible_patches",
    "linkedin_profile_registry",
    "linkedin_profile_registry_events",
    "linkedin_profile_registry_leases",
)
_TEXTUAL_POSTGRES_TYPES = {
    "character varying",
    "text",
    "json",
    "jsonb",
    "uuid",
}
_AUDIT_RELEVANT_COLUMN_EXACT = {
    "artifact_path",
    "candidate_documents_path",
    "checkpoint_json",
    "last_snapshot_dir",
    "metadata_json",
    "payload_json",
    "request_json",
    "result_view_json",
    "snapshot_dir",
    "source_path",
    "stage_summary_json",
    "status_json",
    "summary_path",
}
_AUDIT_IDENTIFIER_COLUMNS = (
    "id",
    "job_id",
    "worker_id",
    "item_id",
    "snapshot_id",
    "profile_url",
    "candidate_id",
    "status",
    "stage",
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RUNTIME_DAEMON_PIDFILE_HINTS = (
    "dev-worker-daemon",
    "worker-recovery-daemon",
    "server-runtime-watchdog",
    "workflow-runtime-controls",
)
_RUNTIME_DAEMON_PROCESS_MARKERS = (
    "-m sourcing_agent.cli run-worker-daemon-service",
    "-m sourcing_agent.cli serve",
    "sourcing_agent.cli run-worker-daemon-service",
    "sourcing_agent.cli serve",
)
_DEFAULT_QUARANTINE_ROOT = "runtime/quarantine/runtime_contamination"


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(str(value or "")):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'


def _sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _safe_json_loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _read_jsonish_file(path: Path) -> Any:
    return _safe_json_loads(path.read_text(encoding="utf-8", errors="replace"))


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    with suppress(ProcessLookupError, PermissionError):
        os.kill(pid, 0)
        return True
    return False


def _process_command_for_pid(pid: int) -> str:
    if pid <= 0:
        return ""
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception:
        return ""
    return " ".join(str(completed.stdout or "").strip().split())


def _is_runtime_daemon_command(command: str) -> bool:
    normalized_command = " ".join(str(command or "").split())
    return bool(normalized_command and any(marker in normalized_command for marker in _RUNTIME_DAEMON_PROCESS_MARKERS))


def _runtime_search_terms(target_runtime_dir: str | Path | None) -> list[str]:
    if target_runtime_dir is None or not str(target_runtime_dir).strip():
        return ["runtime/test_env/", "runtime/test_env_"]
    raw_path = Path(str(target_runtime_dir)).expanduser()
    terms: list[str] = [str(raw_path)]
    try:
        resolved = raw_path.resolve()
        terms.append(str(resolved))
        parts = list(resolved.parts)
    except OSError:
        parts = list(raw_path.parts)
    normalized_parts = [str(part) for part in parts]
    if "runtime" in normalized_parts:
        runtime_index = normalized_parts.index("runtime")
        relative_runtime_path = "/".join(normalized_parts[runtime_index:])
        # Only use a relative runtime term when it identifies a nested test namespace.
        # A bare output/.../runtime directory would otherwise add "runtime", which
        # matches almost every historical control-plane row and makes preflight
        # fail on unrelated root/runtime paths.
        if relative_runtime_path not in {"runtime", "./runtime"}:
            terms.append(relative_runtime_path)
    if "test_env" in normalized_parts:
        test_env_index = normalized_parts.index("test_env")
        terms.append("/".join(normalized_parts[test_env_index:]))
    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        normalized = str(term or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def outer_runtime_root_for_path(runtime_dir: str | Path | None) -> Path:
    raw_path = Path(str(runtime_dir or os.getenv("SOURCING_RUNTIME_DIR") or "runtime")).expanduser()
    try:
        path = raw_path.resolve()
    except OSError:
        path = raw_path
    parts = list(path.parts)
    if "runtime" in parts:
        runtime_index = parts.index("runtime")
        return Path(*parts[: runtime_index + 1])
    return path


def audit_active_runtime_daemons(*, runtime_root: str | Path) -> dict[str, Any]:
    root = Path(str(runtime_root)).expanduser()
    service_logs = root / "service_logs"
    services_root = root / "services"
    findings: list[dict[str, Any]] = []
    if service_logs.exists():
        for pid_file in sorted(service_logs.glob("*.pid")):
            name = pid_file.stem
            if not any(hint in name for hint in _RUNTIME_DAEMON_PIDFILE_HINTS):
                continue
            try:
                pid = int("".join(character for character in pid_file.read_text(encoding="utf-8") if character.isdigit()))
            except Exception:
                continue
            command = _process_command_for_pid(pid) if _pid_is_running(pid) else ""
            if _is_runtime_daemon_command(command):
                findings.append(
                    {
                        "kind": "pid_file",
                        "path": str(pid_file),
                        "pid": pid,
                        "name": name,
                        "command": command[:500],
                    }
                )
    if services_root.exists():
        for status_file in sorted(services_root.glob("*/*.json")):
            if not any(hint in status_file.parent.name for hint in _RUNTIME_DAEMON_PIDFILE_HINTS):
                continue
            try:
                payload = _read_jsonish_file(status_file)
            except Exception:
                continue
            pid = 0
            if isinstance(payload, dict):
                pid = int(payload.get("pid") or 0)
            command = _process_command_for_pid(pid) if _pid_is_running(pid) else ""
            if _is_runtime_daemon_command(command):
                findings.append(
                    {
                        "kind": "status_file",
                        "path": str(status_file),
                        "pid": pid,
                        "name": status_file.parent.name,
                        "command": command[:500],
                    }
                )
    findings.extend(_active_runtime_daemon_process_findings())
    return {
        "status": "blocked" if findings else "ok",
        "runtime_root": str(root),
        "finding_count": len(findings),
        "findings": findings,
    }


def _active_runtime_daemon_process_findings() -> list[dict[str, Any]]:
    """Detect live runtime daemons even when stale pid/status files point elsewhere."""

    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception:
        return []
    findings: list[dict[str, Any]] = []
    current_pid = os.getpid()
    seen_pids: set[int] = set()
    for raw_line in str(completed.stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid_text, _, command = line.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid <= 0 or pid == current_pid or pid in seen_pids:
            continue
        normalized_command = " ".join(str(command or "").split())
        if not _is_runtime_daemon_command(normalized_command):
            continue
        seen_pids.add(pid)
        findings.append(
            {
                "kind": "process_scan",
                "pid": pid,
                "name": "sourcing_agent.cli",
                "command": normalized_command[:500],
            }
        )
    return findings


def scan_live_provider_cache_for_synthetic_fixtures(
    *,
    runtime_root: str | Path = "runtime",
    sample_limit: int = 25,
) -> dict[str, Any]:
    root = Path(str(runtime_root)).expanduser()
    provider_cache_root = root / "provider_cache"
    findings: list[dict[str, Any]] = []
    scanned_count = 0
    finding_count = 0
    if not provider_cache_root.exists():
        return {
            "status": "not_found",
            "provider_cache_root": str(provider_cache_root),
            "scanned_file_count": 0,
            "finding_count": 0,
            "findings": [],
        }
    for path in sorted(provider_cache_root.glob("* /live/**/*.request.json".replace(" ", ""))):
        if not path.is_file():
            continue
        scanned_count += 1
        try:
            payload = _read_jsonish_file(path)
        except OSError as exc:
            findings.append(
                {
                    "path": str(path),
                    "reason": "provider_cache_read_error",
                    "error": str(exc),
                    "markers": [],
                }
            )
            continue
        markers = synthetic_provider_input_markers(payload)
        if not markers:
            continue
        finding_count += 1
        if len(findings) < max(1, int(sample_limit or 25)):
            findings.append(
                {
                    "path": str(path),
                    "reason": "synthetic_fixture_in_live_provider_cache",
                    "markers": markers[:10],
                }
            )
    return {
        "status": "ok",
        "provider_cache_root": str(provider_cache_root),
        "scanned_file_count": scanned_count,
        "finding_count": finding_count,
        "sample_count": len(findings),
        "findings": findings,
    }


def contaminated_live_provider_cache_request_paths(*, runtime_root: str | Path = "runtime") -> list[Path]:
    root = Path(str(runtime_root)).expanduser()
    provider_cache_root = root / "provider_cache"
    paths: list[Path] = []
    if not provider_cache_root.exists():
        return []
    for path in sorted(provider_cache_root.glob("* /live/**/*.request.json".replace(" ", ""))):
        if not path.is_file():
            continue
        try:
            payload = _read_jsonish_file(path)
        except OSError:
            continue
        if synthetic_provider_input_markers(payload):
            paths.append(path)
    return paths


def contaminated_live_provider_cache_quarantine_paths(*, runtime_root: str | Path = "runtime") -> list[Path]:
    """Return contaminated request manifests plus exact same-stem cache payloads.

    Moving only `*.request.json` makes the audit green but can leave the
    provider response cache (`<hash>.json`) available to future live cache reads.
    Quarantine treats the request manifest and its companion payload as one
    artifact unit.
    """

    paths: list[Path] = []
    seen: set[Path] = set()
    for request_path in contaminated_live_provider_cache_request_paths(runtime_root=runtime_root):
        request_name = request_path.name
        base_name = request_name[: -len(".request.json")] if request_name.endswith(".request.json") else request_path.stem
        candidates = [
            request_path,
            request_path.with_name(f"{base_name}.json"),
            request_path.with_name(f"{base_name}.summary.json"),
            request_path.with_name(f"{base_name}.response.json"),
            request_path.with_name(f"{base_name}.raw.json"),
        ]
        for candidate in candidates:
            if not candidate.exists() or not candidate.is_file():
                continue
            try:
                resolved = candidate.resolve()
            except OSError:
                resolved = candidate
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(candidate)
    return paths


def _postgres_text_columns(cursor: Any, *, schema: str, table_name: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    columns: list[str] = []
    for column_name, data_type in cursor.fetchall():
        normalized_column = str(column_name or "").strip()
        if str(data_type or "").strip().lower() not in _TEXTUAL_POSTGRES_TYPES:
            continue
        if _postgres_column_is_audit_relevant(normalized_column):
            columns.append(normalized_column)
    return columns


def _postgres_column_names(cursor: Any, *, schema: str, table_name: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _postgres_column_is_audit_relevant(column_name: str) -> bool:
    normalized = str(column_name or "").strip().lower()
    if normalized in _AUDIT_RELEVANT_COLUMN_EXACT:
        return True
    return normalized.endswith(("_path", "_dir", "_json"))


def _postgres_table_exists(cursor: Any, *, schema: str, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table_name),
    )
    row = cursor.fetchone()
    return bool(row and row[0])


def _row_to_dict(cursor: Any, row: Any) -> dict[str, Any]:
    names = [str(item.name if hasattr(item, "name") else item[0]) for item in cursor.description]
    return {name: value for name, value in zip(names, row)}


def _compact_row_sample(row: dict[str, Any], *, search_terms: Iterable[str]) -> dict[str, Any]:
    identifiers: dict[str, Any] = {}
    for key in ("id", "job_id", "worker_id", "item_id", "snapshot_id", "profile_url", "candidate_id", "status", "stage"):
        if key in row and row[key] not in (None, ""):
            identifiers[key] = row[key]
    matched_columns: dict[str, str] = {}
    terms = [str(item) for item in search_terms if str(item or "").strip()]
    for key, value in row.items():
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
        if any(term in text for term in terms):
            matched_columns[str(key)] = text[:500]
    return {
        "identifiers": identifiers,
        "matched_columns": matched_columns,
    }


def _postgres_runtime_contamination_predicate(columns: list[str], search_terms: Iterable[str]) -> str:
    predicates: list[str] = []
    for column_name in columns:
        quoted_column = _quote_identifier(column_name)
        for term in search_terms:
            normalized_term = str(term or "").strip()
            if not normalized_term:
                continue
            predicates.append(f"{quoted_column}::text LIKE {_sql_literal('%' + normalized_term + '%')}")
    return " OR ".join(predicates) or "FALSE"


def _sample_select_columns(columns: list[str]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for column_name in [*_AUDIT_IDENTIFIER_COLUMNS, *columns]:
        if column_name in seen:
            continue
        seen.add(column_name)
        selected.append(column_name)
    return selected


def audit_control_plane_runtime_contamination(
    *,
    dsn: str = "",
    schema: str = "",
    target_runtime_dir: str | Path | None = None,
    sample_limit: int = 25,
) -> dict[str, Any]:
    resolved_dsn = str(dsn or resolve_control_plane_postgres_dsn(Path.cwd()) or "").strip()
    resolved_schema = normalize_control_plane_postgres_schema(
        schema or resolve_control_plane_postgres_schema(Path.cwd())
    )
    if not resolved_dsn:
        return {
            "status": "skipped",
            "reason": "postgres_dsn_missing",
            "schema": resolved_schema,
            "finding_count": 0,
            "tables": [],
        }
    terms = _runtime_search_terms(target_runtime_dir)
    tables: list[dict[str, Any]] = []
    finding_count = 0
    try:
        psycopg = _import_psycopg()
        effective_dsn = normalize_control_plane_postgres_connect_dsn(resolved_dsn)
        connection = psycopg.connect(effective_dsn, client_encoding="utf8", connect_timeout=3)
        with configure_control_plane_postgres_session(
            connection,
            schema=resolved_schema,
            create_schema=False,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout TO '5000ms'")
                for table_name in _CONTROL_PLANE_TABLES_TO_AUDIT:
                    if not _postgres_table_exists(cursor, schema=resolved_schema, table_name=table_name):
                        continue
                    columns = _postgres_text_columns(cursor, schema=resolved_schema, table_name=table_name)
                    if not columns:
                        continue
                    all_columns = set(_postgres_column_names(cursor, schema=resolved_schema, table_name=table_name))
                    sample_columns = [
                        column
                        for column in _sample_select_columns(columns)
                        if column in all_columns
                    ]
                    predicates: list[str] = []
                    params: list[str] = []
                    for column_name in columns:
                        quoted_column = _quote_identifier(column_name)
                        for term in terms:
                            predicates.append(f"{quoted_column}::text LIKE %s")
                            params.append(f"%{term}%")
                    where_clause = " OR ".join(predicates)
                    cursor.execute(
                        f"SELECT count(*) FROM {_quote_identifier(table_name)} WHERE {where_clause}",
                        tuple(params),
                    )
                    count_row = cursor.fetchone()
                    table_finding_count = int(count_row[0] if count_row else 0)
                    select_columns = ", ".join(_quote_identifier(column) for column in sample_columns)
                    sql = f"SELECT {select_columns} FROM {_quote_identifier(table_name)} WHERE {where_clause} LIMIT %s"
                    cursor.execute(sql, (*params, max(1, int(sample_limit or 25))))
                    rows = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
                    finding_count += table_finding_count
                    tables.append(
                        {
                            "table": table_name,
                            "text_columns": columns,
                            "text_column_count": len(columns),
                            "finding_count": table_finding_count,
                            "samples": [
                                _compact_row_sample(row, search_terms=terms)
                                for row in rows[: max(1, int(sample_limit or 25))]
                            ],
                        }
                    )
    except Exception as exc:
        return {
            "status": "error",
            "reason": "postgres_audit_failed",
            "schema": resolved_schema,
            "error": str(exc),
            "finding_count": finding_count,
            "tables": tables,
        }
    return {
        "status": "ok",
        "schema": resolved_schema,
        "search_terms": terms,
        "finding_count": finding_count,
        "tables": tables,
    }


def build_runtime_contamination_report(
    *,
    workspace_root: str | Path = ".",
    target_runtime_dir: str | Path | None = None,
    provider_cache_runtime_root: str | Path | None = None,
    dsn: str = "",
    schema: str = "",
    include_postgres: bool = True,
    sample_limit: int = 25,
) -> dict[str, Any]:
    workspace = Path(str(workspace_root)).expanduser()
    runtime_root = outer_runtime_root_for_path(target_runtime_dir or workspace / "runtime")
    provider_cache_root = (
        Path(str(provider_cache_runtime_root)).expanduser()
        if provider_cache_runtime_root is not None and str(provider_cache_runtime_root).strip()
        else runtime_root
    )
    provider_cache = scan_live_provider_cache_for_synthetic_fixtures(
        runtime_root=provider_cache_root,
        sample_limit=sample_limit,
    )
    postgres = (
        audit_control_plane_runtime_contamination(
            dsn=dsn,
            schema=schema,
            target_runtime_dir=target_runtime_dir,
            sample_limit=sample_limit,
        )
        if include_postgres
        else {"status": "skipped", "reason": "postgres_disabled", "finding_count": 0, "tables": []}
    )
    finding_count = int(provider_cache.get("finding_count") or 0) + int(postgres.get("finding_count") or 0)
    return {
        "status": "contaminated" if finding_count else "clean",
        "finding_count": finding_count,
        "workspace_root": str(workspace),
        "target_runtime_dir": str(target_runtime_dir or ""),
        "runtime_root": str(runtime_root),
        "provider_cache_runtime_root": str(provider_cache_root),
        "provider_cache": provider_cache,
        "postgres": postgres,
    }


def build_runtime_contamination_quarantine_plan(
    *,
    workspace_root: str | Path = ".",
    target_runtime_dir: str | Path | None = None,
    dsn: str = "",
    schema: str = "",
    quarantine_root: str | Path = _DEFAULT_QUARANTINE_ROOT,
    sample_limit: int = 25,
) -> dict[str, Any]:
    report = build_runtime_contamination_report(
        workspace_root=workspace_root,
        target_runtime_dir=target_runtime_dir,
        dsn=dsn,
        schema=schema,
        include_postgres=True,
        sample_limit=sample_limit,
    )
    runtime_root = Path(str(report.get("runtime_root") or outer_runtime_root_for_path(target_runtime_dir))).expanduser()
    quarantine_dir = Path(str(quarantine_root)).expanduser()
    if not quarantine_dir.is_absolute():
        quarantine_dir = runtime_root / quarantine_dir.relative_to("runtime") if str(quarantine_dir).startswith("runtime/") else Path(str(workspace_root)).expanduser() / quarantine_dir
    cache_paths = contaminated_live_provider_cache_request_paths(runtime_root=runtime_root)
    quarantine_paths = contaminated_live_provider_cache_quarantine_paths(runtime_root=runtime_root)
    file_moves: list[dict[str, str]] = []
    for source_path in quarantine_paths:
        try:
            relative = source_path.relative_to(runtime_root)
        except ValueError:
            relative = Path(source_path.name)
        destination = quarantine_dir / "provider_cache" / relative
        file_moves.append(
            {
                "source": str(source_path),
                "destination": str(destination),
                "shell": f"mkdir -p {_shell_quote(str(destination.parent))} && mv -- {_shell_quote(str(source_path))} {_shell_quote(str(destination))}",
            }
        )
    postgres_plan: list[dict[str, Any]] = []
    postgres_report = dict(report.get("postgres") or {})
    schema_name = str(postgres_report.get("schema") or schema or "public")
    search_terms = [str(item) for item in list(postgres_report.get("search_terms") or _runtime_search_terms(target_runtime_dir))]
    for table in list(postgres_report.get("tables") or []):
        table_payload = dict(table or {})
        if int(table_payload.get("finding_count") or 0) <= 0:
            continue
        table_name = str(table_payload.get("table") or "").strip()
        columns = [str(item) for item in list(table_payload.get("text_columns") or []) if str(item or "").strip()]
        if not table_name or not columns:
            continue
        where_clause = _postgres_runtime_contamination_predicate(columns, search_terms)
        quoted_table = _quote_identifier(table_name)
        quarantine_table = _quote_identifier(f"quarantine_{table_name}_20260507_runtime_contamination"[:63])
        postgres_plan.append(
            {
                "table": table_name,
                "finding_count": int(table_payload.get("finding_count") or 0),
                "text_columns": columns,
                "count_sql": f"SELECT count(*) FROM {quoted_table} WHERE {where_clause};",
                "quarantine_sql": (
                    f"CREATE TABLE IF NOT EXISTS {quarantine_table} AS SELECT * FROM {quoted_table} WHERE FALSE;\n"
                    f"INSERT INTO {quarantine_table} SELECT * FROM {quoted_table} WHERE {where_clause};"
                ),
                "delete_sql_requires_approval": f"DELETE FROM {quoted_table} WHERE {where_clause};",
                "samples": list(table_payload.get("samples") or [])[: max(1, int(sample_limit or 25))],
            }
        )
    return {
        "status": "dry_run_only",
        "mutates_state": False,
        "requires_user_approval_before_apply": True,
        "report_status": report.get("status"),
        "finding_count": report.get("finding_count"),
        "target_runtime_dir": str(target_runtime_dir or ""),
        "runtime_root": str(runtime_root),
        "quarantine_root": str(quarantine_dir),
        "provider_cache": {
            "contaminated_request_manifest_count": len(cache_paths),
            "file_move_count": len(file_moves),
            "file_moves": file_moves[: max(1, int(sample_limit or 25))],
            "full_file_move_manifest_included": len(file_moves) <= max(1, int(sample_limit or 25)),
        },
        "postgres": {
            "schema": schema_name,
            "status": postgres_report.get("status"),
            "finding_count": postgres_report.get("finding_count"),
            "dry_run_sql": postgres_plan,
            "apply_wrapper": "BEGIN; -- review quarantine_sql and delete_sql_requires_approval statements; ROLLBACK;",
        },
        "audit_report": report,
    }


def apply_runtime_contamination_quarantine_plan(
    *,
    workspace_root: str | Path = ".",
    target_runtime_dir: str | Path | None = None,
    dsn: str = "",
    schema: str = "",
    quarantine_root: str | Path = _DEFAULT_QUARANTINE_ROOT,
    sample_limit: int = 25,
    include_postgres: bool = True,
) -> dict[str, Any]:
    """Apply the explicit quarantine plan.

    This moves contaminated live provider-cache artifacts to the quarantine
    directory, copies matching PG rows into quarantine tables, then removes those
    rows from the active schema. Callers must have already reviewed the dry-run
    report; this function is intentionally exposed only through an explicit CLI
    flag.
    """

    plan = build_runtime_contamination_quarantine_plan(
        workspace_root=workspace_root,
        target_runtime_dir=target_runtime_dir,
        dsn=dsn,
        schema=schema,
        quarantine_root=quarantine_root,
        sample_limit=sample_limit,
    )
    runtime_root = Path(str(plan.get("runtime_root") or outer_runtime_root_for_path(target_runtime_dir))).expanduser()
    quarantine_dir = Path(str(plan.get("quarantine_root") or quarantine_root)).expanduser()
    source_paths = contaminated_live_provider_cache_quarantine_paths(runtime_root=runtime_root)
    file_results: list[dict[str, Any]] = []
    for source_path in source_paths:
        try:
            relative = source_path.relative_to(runtime_root)
        except ValueError:
            relative = Path(source_path.name)
        destination = quarantine_dir / "provider_cache" / relative
        result: dict[str, Any] = {
            "source": str(source_path),
            "destination": str(destination),
        }
        if not source_path.exists():
            result["status"] = "missing"
            file_results.append(result)
            continue
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                destination = destination.with_name(f"{destination.name}.duplicate")
                result["destination"] = str(destination)
            shutil.move(str(source_path), str(destination))
            result["status"] = "moved"
        except Exception as exc:
            result["status"] = "failed"
            result["error"] = f"{type(exc).__name__}: {exc}"
        file_results.append(result)

    postgres_result: dict[str, Any]
    if not include_postgres:
        postgres_result = {"status": "skipped", "reason": "postgres_disabled", "tables": []}
    else:
        postgres_result = _apply_postgres_quarantine_plan(plan=plan, dsn=dsn, schema=schema)

    file_failure_count = sum(1 for item in file_results if str(item.get("status") or "") == "failed")
    pg_failed = str(postgres_result.get("status") or "").strip().lower() == "failed"
    post_report = build_runtime_contamination_report(
        workspace_root=workspace_root,
        target_runtime_dir=target_runtime_dir,
        dsn=dsn,
        schema=schema,
        include_postgres=include_postgres,
        sample_limit=sample_limit,
    )
    return {
        "status": "failed" if file_failure_count or pg_failed else "applied",
        "mutates_state": True,
        "target_runtime_dir": str(target_runtime_dir or ""),
        "runtime_root": str(runtime_root),
        "quarantine_root": str(quarantine_dir),
        "pre_apply_finding_count": plan.get("finding_count"),
        "provider_cache": {
            "requested_manifest_count": dict(plan.get("provider_cache") or {}).get(
                "contaminated_request_manifest_count"
            ),
            "file_move_count": len(file_results),
            "moved_count": sum(1 for item in file_results if str(item.get("status") or "") == "moved"),
            "failed_count": file_failure_count,
            "sample_results": file_results[: max(1, int(sample_limit or 25))],
        },
        "postgres": postgres_result,
        "post_apply_report": post_report,
    }


def _apply_postgres_quarantine_plan(*, plan: dict[str, Any], dsn: str = "", schema: str = "") -> dict[str, Any]:
    postgres_plan = list(dict(plan.get("postgres") or {}).get("dry_run_sql") or [])
    if not postgres_plan:
        return {"status": "skipped", "reason": "no_postgres_findings", "tables": []}
    resolved_dsn = str(dsn or resolve_control_plane_postgres_dsn(Path.cwd()) or "").strip()
    resolved_schema = normalize_control_plane_postgres_schema(
        schema or str(dict(plan.get("postgres") or {}).get("schema") or "") or resolve_control_plane_postgres_schema(Path.cwd())
    )
    if not resolved_dsn:
        return {"status": "skipped", "reason": "postgres_dsn_missing", "schema": resolved_schema, "tables": []}
    table_results: list[dict[str, Any]] = []
    try:
        psycopg = _import_psycopg()
        effective_dsn = normalize_control_plane_postgres_connect_dsn(resolved_dsn)
        connection = psycopg.connect(effective_dsn, client_encoding="utf8", connect_timeout=3)
        with configure_control_plane_postgres_session(
            connection,
            schema=resolved_schema,
            create_schema=False,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout TO '10000ms'")
                for item in postgres_plan:
                    payload = dict(item or {})
                    table_name = str(payload.get("table") or "").strip()
                    count_sql = str(payload.get("count_sql") or "").strip()
                    quarantine_sql = str(payload.get("quarantine_sql") or "").strip()
                    delete_sql = str(payload.get("delete_sql_requires_approval") or "").strip()
                    if not table_name or not count_sql or not quarantine_sql or not delete_sql:
                        continue
                    cursor.execute(count_sql)
                    before_row = cursor.fetchone()
                    before_count = int(before_row[0] if before_row else 0)
                    if before_count > 0:
                        for statement in [part.strip() for part in quarantine_sql.split(";") if part.strip()]:
                            cursor.execute(statement)
                        cursor.execute(delete_sql)
                    cursor.execute(count_sql)
                    after_row = cursor.fetchone()
                    table_results.append(
                        {
                            "table": table_name,
                            "before_count": before_count,
                            "after_count": int(after_row[0] if after_row else 0),
                        }
                    )
                connection.commit()
    except Exception as exc:
        return {
            "status": "failed",
            "schema": resolved_schema,
            "error": f"{type(exc).__name__}: {exc}",
            "tables": table_results,
        }
    return {
        "status": "applied",
        "schema": resolved_schema,
        "tables": table_results,
        "before_count": sum(int(item.get("before_count") or 0) for item in table_results),
        "after_count": sum(int(item.get("after_count") or 0) for item in table_results),
    }


def _shell_quote(value: str) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"
