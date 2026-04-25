from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from datetime import date, datetime, timezone
from datetime import time as datetime_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, TextIO

from .local_postgres import (
    configure_control_plane_postgres_session,
    ensure_local_postgres_started,
    normalize_control_plane_postgres_connect_dsn,
    resolve_control_plane_postgres_dsn,
    resolve_default_control_plane_db_path,
)

DEFAULT_CONTROL_PLANE_TABLES = [
    "candidates",
    "evidence",
    "jobs",
    "job_results",
    "job_events",
    "job_progress_event_summaries",
    "job_result_views",
    "plan_review_sessions",
    "manual_review_items",
    "candidate_review_registry",
    "target_candidates",
    "asset_default_pointers",
    "asset_default_pointer_history",
    "frontend_history_links",
    "agent_runtime_sessions",
    "agent_trace_spans",
    "agent_worker_runs",
    "workflow_job_leases",
    "query_dispatches",
    "confidence_policy_runs",
    "confidence_policy_controls",
    "criteria_feedback",
    "criteria_patterns",
    "criteria_versions",
    "criteria_compiler_runs",
    "criteria_result_diffs",
    "criteria_pattern_suggestions",
    "organization_asset_registry",
    "organization_execution_profiles",
    "acquisition_shard_registry",
    "cloud_asset_operation_ledger",
    "asset_materialization_generations",
    "asset_membership_index",
    "candidate_materialization_state",
    "snapshot_materialization_runs",
    "generation_index_entries",
    "linkedin_profile_registry",
    "linkedin_profile_registry_aliases",
    "linkedin_profile_registry_leases",
    "linkedin_profile_registry_events",
    "linkedin_profile_registry_backfill_runs",
]
ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE = "acquisition_shard_registry"
ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE = "acquisition_shard_registry_current"
ACQUISITION_SHARD_REGISTRY_FORMER_TABLE = "acquisition_shard_registry_former"
ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES = (
    ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
    ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
)
ACQUISITION_SHARD_REGISTRY_COLUMNS = [
    {"name": "shard_key", "type": "TEXT", "notnull": 1, "default": None, "pk_position": 1},
    {"name": "target_company", "type": "TEXT", "notnull": 1, "default": None, "pk_position": 0},
    {"name": "company_key", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "snapshot_id", "type": "TEXT", "notnull": 1, "default": None, "pk_position": 0},
    {"name": "asset_view", "type": "TEXT", "notnull": 1, "default": "'canonical_merged'", "pk_position": 0},
    {"name": "lane", "type": "TEXT", "notnull": 1, "default": None, "pk_position": 0},
    {"name": "status", "type": "TEXT", "notnull": 1, "default": "'completed'", "pk_position": 0},
    {"name": "employment_scope", "type": "TEXT", "notnull": 1, "default": "'all'", "pk_position": 0},
    {"name": "strategy_type", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "shard_id", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "shard_title", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "search_query", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "query_signature", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "company_scope_json", "type": "TEXT", "notnull": 1, "default": "'[]'", "pk_position": 0},
    {"name": "locations_json", "type": "TEXT", "notnull": 1, "default": "'[]'", "pk_position": 0},
    {"name": "function_ids_json", "type": "TEXT", "notnull": 1, "default": "'[]'", "pk_position": 0},
    {"name": "result_count", "type": "BIGINT", "notnull": 1, "default": "0", "pk_position": 0},
    {"name": "estimated_total_count", "type": "BIGINT", "notnull": 1, "default": "0", "pk_position": 0},
    {"name": "provider_cap_hit", "type": "BOOLEAN", "notnull": 1, "default": "FALSE", "pk_position": 0},
    {"name": "source_path", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "source_job_id", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "metadata_json", "type": "TEXT", "notnull": 1, "default": "'{}'", "pk_position": 0},
    {"name": "first_seen_at", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "last_completed_at", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "materialization_generation_key", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "materialization_generation_sequence", "type": "BIGINT", "notnull": 1, "default": "0", "pk_position": 0},
    {"name": "materialization_watermark", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
    {"name": "created_at", "type": "TEXT", "notnull": 0, "default": "CURRENT_TIMESTAMP", "pk_position": 0},
    {"name": "updated_at", "type": "TEXT", "notnull": 0, "default": "CURRENT_TIMESTAMP", "pk_position": 0},
]
_ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES = tuple(
    str(column.get("name") or "").strip()
    for column in ACQUISITION_SHARD_REGISTRY_COLUMNS
    if str(column.get("name") or "").strip()
)
DEFAULT_CONTROL_PLANE_POSTGRES_SYNC_MIN_INTERVAL_SECONDS = 60
DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE = 250
DEFAULT_SQLITE_POSTGRES_DIRECT_COMMIT_EVERY_CHUNKS = 20
DEFAULT_SQLITE_POSTGRES_DIRECT_PROGRESS_EVERY_CHUNKS = 50
DEFAULT_CONTROL_PLANE_SNAPSHOT_EXPORT_BATCH_SIZE = 1000


def acquisition_shard_registry_split_table_name(employment_scope: Any) -> str:
    normalized_scope = str(employment_scope or "").strip().lower()
    if normalized_scope == "former":
        return ACQUISITION_SHARD_REGISTRY_FORMER_TABLE
    return ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE


def _acquisition_shard_registry_other_split_table(table_name: str) -> str:
    return (
        ACQUISITION_SHARD_REGISTRY_FORMER_TABLE
        if str(table_name or "").strip() == ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE
        else ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE
    )


def _acquisition_shard_registry_union_sql() -> str:
    select_columns = ", ".join(_quote_identifier(column_name) for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES)
    return (
        f"SELECT {select_columns} FROM {_quote_identifier(ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE)} "
        f"UNION ALL SELECT {select_columns} FROM {_quote_identifier(ACQUISITION_SHARD_REGISTRY_FORMER_TABLE)}"
    )


def _normalize_acquisition_shard_registry_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    normalized_text = str(value or "").strip().lower()
    if not normalized_text or normalized_text in {"0", "false", "f", "no", "n", "off", "null", "none"}:
        return False
    return True


def _legacy_acquisition_shard_registry_select_columns_sql() -> str:
    select_expressions: list[str] = []
    for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES:
        quoted_column = _quote_identifier(column_name)
        if column_name == "provider_cap_hit":
            select_expressions.append(
                " ".join(
                    (
                        "CASE",
                        f"WHEN {quoted_column} IS NULL THEN FALSE",
                        f"WHEN lower(trim({quoted_column}::text)) IN ('', '0', 'false', 'f', 'no', 'n', 'off', 'null', 'none') THEN FALSE",
                        "ELSE TRUE",
                        f"END AS {quoted_column}",
                    )
                )
            )
            continue
        select_expressions.append(quoted_column)
    return ", ".join(select_expressions)


def _build_acquisition_shard_registry_split_table_sql(table_name: str) -> str:
    column_sql: list[str] = []
    def _column_int(column: dict[str, Any], key: str) -> int:
        raw_value = column.get(key)
        try:
            return int(raw_value or 0)
        except (TypeError, ValueError):
            return 0
    primary_key_columns = [
        dict(item) for item in ACQUISITION_SHARD_REGISTRY_COLUMNS if _column_int(dict(item), "pk_position") > 0
    ]
    for column in ACQUISITION_SHARD_REGISTRY_COLUMNS:
        column_name = str(column.get("name") or "").strip()
        declared_type = str(column.get("type") or "TEXT").strip() or "TEXT"
        if not column_name:
            continue
        parts = [_quote_identifier(column_name), declared_type]
        if _column_int(column, "notnull") > 0:
            parts.append("NOT NULL")
        default_value = column.get("default")
        if default_value not in {None, ""}:
            parts.append(f"DEFAULT {default_value}")
        column_sql.append(" ".join(parts))
    if primary_key_columns:
        sorted_primary_keys = sorted(primary_key_columns, key=lambda item: _column_int(item, "pk_position"))
        column_sql.append(
            f"PRIMARY KEY ({', '.join(_quote_identifier(str(item.get('name') or '')) for item in sorted_primary_keys)})"
        )
    return f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} ({', '.join(column_sql)})"


def _build_acquisition_shard_registry_upsert_sql(table_name: str) -> str:
    quoted_table_name = _quote_identifier(table_name)
    quoted_columns = [_quote_identifier(column_name) for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES]
    update_columns = [column_name for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES if column_name != "shard_key"]
    return (
        f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) "
        f"VALUES ({', '.join(['%s'] * len(quoted_columns))}) "
        f"ON CONFLICT ({_quote_identifier('shard_key')}) DO UPDATE SET "
        + ", ".join(
            f"{_quote_identifier(column_name)} = EXCLUDED.{_quote_identifier(column_name)}"
            for column_name in update_columns
        )
    )


def _postgres_relation_kind(cursor: Any, relation_name: str) -> str:
    cursor.execute(
        """
        SELECT class_rel.relkind
        FROM pg_class AS class_rel
        JOIN pg_namespace AS namespace_rel
          ON namespace_rel.oid = class_rel.relnamespace
        WHERE namespace_rel.nspname = 'public'
          AND class_rel.relname = %s
        LIMIT 1
        """,
        (str(relation_name or "").strip(),),
    )
    row = cursor.fetchone()
    if isinstance(row, dict):
        return str(row.get("relkind") or "").strip()
    if hasattr(row, "_mapping"):
        return str(getattr(row, "_mapping").get("relkind") or "").strip()
    if isinstance(row, (list, tuple)) and row:
        return str(row[0] or "").strip()
    return str(getattr(row, "relkind", "") or "").strip()


def read_acquisition_shard_registry_snapshot(cursor: Any) -> dict[str, Any] | None:
    logical_relation_kind = _postgres_relation_kind(cursor, ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)
    if logical_relation_kind in {"r", "p", "v", "m"}:
        columns = _postgres_table_columns(cursor, ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE) or list(
            ACQUISITION_SHARD_REGISTRY_COLUMNS
        )
        cursor.execute(f"SELECT * FROM {_quote_identifier(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)}")
        rows = _fetch_cursor_rows_as_dicts(cursor)
        return {
            "columns": columns,
            "row_count": len(rows),
            "rows": rows,
        }
    current_kind = _postgres_relation_kind(cursor, ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE)
    former_kind = _postgres_relation_kind(cursor, ACQUISITION_SHARD_REGISTRY_FORMER_TABLE)
    if current_kind not in {"r", "p"} and former_kind not in {"r", "p"}:
        return None
    cursor.execute(_acquisition_shard_registry_union_sql())
    rows = _fetch_cursor_rows_as_dicts(cursor)
    return {
        "columns": list(ACQUISITION_SHARD_REGISTRY_COLUMNS),
        "row_count": len(rows),
        "rows": rows,
    }


def ensure_acquisition_shard_registry_split_schema(cursor: Any) -> None:
    for table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES:
        cursor.execute(_build_acquisition_shard_registry_split_table_sql(table_name))
    logical_relation_kind = _postgres_relation_kind(cursor, ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)
    if logical_relation_kind in {"r", "p"}:
        insert_columns = ", ".join(
            _quote_identifier(column_name) for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES
        )
        select_columns = _legacy_acquisition_shard_registry_select_columns_sql()
        for table_name, scope_predicate in (
            (
                ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
                "lower(coalesce(employment_scope, '')) = 'former'",
            ),
            (
                ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
                "lower(coalesce(employment_scope, '')) <> 'former'",
            ),
        ):
            cursor.execute(
                (
                    f"INSERT INTO {_quote_identifier(table_name)} ({insert_columns}) "
                    f"SELECT {select_columns} FROM {_quote_identifier(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)} "
                    f"WHERE {scope_predicate} "
                    f"ON CONFLICT ({_quote_identifier('shard_key')}) DO UPDATE SET "
                    + ", ".join(
                        f"{_quote_identifier(column_name)} = EXCLUDED.{_quote_identifier(column_name)}"
                        for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES
                        if column_name != "shard_key"
                    )
                )
            )
        cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)} CASCADE")
    elif logical_relation_kind == "v":
        cursor.execute(f"DROP VIEW IF EXISTS {_quote_identifier(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)}")
    cursor.execute(
        f"CREATE OR REPLACE VIEW {_quote_identifier(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)} AS "
        f"{_acquisition_shard_registry_union_sql()}"
    )
    for table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES:
        cursor.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{table_name}_shard_key')} "
            f"ON {_quote_identifier(table_name)} ({_quote_identifier('shard_key')})"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{table_name}_company')} "
            f"ON {_quote_identifier(table_name)} "
            f"({_quote_identifier('target_company')}, {_quote_identifier('snapshot_id')}, "
            f"{_quote_identifier('lane')}, {_quote_identifier('employment_scope')}, {_quote_identifier('updated_at')})"
        )


def upsert_acquisition_shard_registry_rows(
    cursor: Any,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    ensure_schema: bool = True,
) -> dict[str, Any]:
    if ensure_schema:
        ensure_acquisition_shard_registry_split_schema(cursor)
    grouped_rows: dict[str, list[dict[str, Any]]] = {
        ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE: [],
        ACQUISITION_SHARD_REGISTRY_FORMER_TABLE: [],
    }
    nul_text_replacements = 0
    for raw_row in list(rows or []):
        payload = dict(raw_row or {})
        shard_key = str(payload.get("shard_key") or "").strip()
        if not shard_key:
            continue
        grouped_rows[acquisition_shard_registry_split_table_name(payload.get("employment_scope"))].append(payload)
    for table_name, payload_rows in grouped_rows.items():
        if not payload_rows:
            continue
        other_table_name = _acquisition_shard_registry_other_split_table(table_name)
        shard_keys = [str(dict(item).get("shard_key") or "").strip() for item in payload_rows if str(dict(item).get("shard_key") or "").strip()]
        if shard_keys:
            placeholders = ", ".join(["%s"] * len(shard_keys))
            cursor.execute(
                f"DELETE FROM {_quote_identifier(other_table_name)} WHERE {_quote_identifier('shard_key')} IN ({placeholders})",
                tuple(shard_keys),
            )
        insert_sql = _build_acquisition_shard_registry_upsert_sql(table_name)
        values: list[tuple[Any, ...]] = []
        for payload in payload_rows:
            prepared_payload = {
                column_name: payload.get(column_name)
                for column_name in _ACQUISITION_SHARD_REGISTRY_COLUMN_NAMES
            }
            prepared_payload["provider_cap_hit"] = _normalize_acquisition_shard_registry_bool(
                prepared_payload.get("provider_cap_hit")
            )
            prepared_values, replacement_count = _prepare_postgres_row_values(
                prepared_payload,
                ACQUISITION_SHARD_REGISTRY_COLUMNS,
            )
            values.append(prepared_values)
            nul_text_replacements += replacement_count
        cursor.executemany(insert_sql, values)
    return {
        "row_count": sum(len(payload_rows) for payload_rows in grouped_rows.values()),
        "nul_text_replacements": nul_text_replacements,
    }


def count_acquisition_shard_registry_rows(cursor: Any) -> int:
    snapshot = read_acquisition_shard_registry_snapshot(cursor)
    return int(snapshot.get("row_count") or 0) if isinstance(snapshot, dict) else 0


def _resolve_sqlite_connect_target(
    *,
    runtime_dir: Path,
    sqlite_path: str | Path | None = None,
) -> str:
    if sqlite_path is None:
        return str(resolve_default_control_plane_db_path(runtime_dir, base_dir=runtime_dir).expanduser())
    return str(sqlite_path).strip() or str(resolve_default_control_plane_db_path(runtime_dir, base_dir=runtime_dir).expanduser())


def _sqlite_connect_uses_uri(connect_target: str) -> bool:
    return str(connect_target or "").strip().startswith("file:")


def _connect_sqlite(connect_target: str) -> sqlite3.Connection:
    return sqlite3.connect(connect_target, uri=_sqlite_connect_uses_uri(connect_target))


def _sqlite_export_source_exists(connect_target: str) -> bool:
    if _sqlite_connect_uses_uri(connect_target):
        return True
    resolved_path = Path(str(connect_target or "")).expanduser()
    if not resolved_path.exists():
        return False
    try:
        return resolved_path.read_bytes()[:16] == b"SQLite format 3\x00"
    except OSError:
        return False


def export_control_plane_snapshot(
    *,
    runtime_dir: str | Path,
    output_path: str | Path,
    sqlite_path: str | Path | None = None,
    tables: list[str] | None = None,
    include_all_sqlite_tables: bool = False,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    db_path = _resolve_sqlite_connect_target(runtime_dir=runtime_root, sqlite_path=sqlite_path)
    source_backend = "sqlite"
    resolved_output_path = Path(output_path).expanduser()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    table_summaries: dict[str, Any] = {}
    if _sqlite_export_source_exists(db_path):
        connection = _connect_sqlite(db_path)
        connection.row_factory = sqlite3.Row
        try:
            selected_tables = _resolve_export_table_names(
                connection,
                runtime_dir=runtime_root,
                tables=tables,
                include_all_sqlite_tables=include_all_sqlite_tables,
            )
            table_summaries = _write_control_plane_snapshot_json(
                output_path=resolved_output_path,
                runtime_root=runtime_root,
                sqlite_path=str(db_path),
                source_backend=source_backend,
                include_all_sqlite_tables=include_all_sqlite_tables,
                requested_tables=selected_tables,
                table_writer=lambda handle, table_name: _write_sqlite_snapshot_table_json(
                    handle,
                    connection=connection,
                    runtime_dir=runtime_root,
                    table_name=table_name,
                ),
            )
        finally:
            connection.close()
    else:
        effective_dsn = resolve_control_plane_postgres_dsn(runtime_root)
        if not effective_dsn:
            raise FileNotFoundError(f"Control-plane source is unavailable: {db_path}")
        if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
            try:
                ensure_local_postgres_started(runtime_root)
            except Exception:
                pass
        source_backend = "postgres"
        psycopg = _import_psycopg()
        with _connect_postgres(effective_dsn, psycopg=psycopg) as connection:
            with connection.cursor() as cursor:
                selected_tables = _resolve_export_table_names_postgres(
                    cursor,
                    runtime_dir=runtime_root,
                    tables=tables,
                    include_all_sqlite_tables=include_all_sqlite_tables,
                )
                table_summaries = _write_control_plane_snapshot_json(
                    output_path=resolved_output_path,
                    runtime_root=runtime_root,
                    sqlite_path=str(db_path),
                    source_backend=source_backend,
                    include_all_sqlite_tables=include_all_sqlite_tables,
                    requested_tables=selected_tables,
                    table_writer=lambda handle, table_name: _write_postgres_snapshot_table_json(
                        handle,
                        cursor=cursor,
                        runtime_dir=runtime_root,
                        table_name=table_name,
                    ),
                )
    return {
        "status": "exported",
        "source_backend": source_backend,
        "output_path": str(resolved_output_path),
        "table_count": len(table_summaries),
        "tables": dict(table_summaries),
    }


def sync_control_plane_snapshot_to_postgres(
    *,
    snapshot_path: str | Path,
    dsn: str = "",
    tables: list[str] | None = None,
    truncate_first: bool = False,
    validate_postgres: bool = False,
) -> dict[str, Any]:
    resolved_snapshot_path = Path(snapshot_path).expanduser()
    snapshot_payload = json.loads(resolved_snapshot_path.read_text(encoding="utf-8"))
    tables_payload = dict(snapshot_payload.get("tables") or {})
    selected_tables = _resolve_snapshot_table_names(snapshot_payload=snapshot_payload, tables=tables)
    effective_dsn = str(dsn or resolve_control_plane_postgres_dsn(resolved_snapshot_path)).strip()
    if not effective_dsn:
        raise RuntimeError("Postgres DSN is required. Set --dsn or SOURCING_CONTROL_PLANE_POSTGRES_DSN.")
    if not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
        try:
            ensure_local_postgres_started(resolved_snapshot_path)
        except Exception:
            pass

    psycopg = _import_psycopg()
    synced_tables: dict[str, Any] = {}
    validation_tables: dict[str, Any] = {}
    validation_errors: list[str] = []
    with _connect_postgres(effective_dsn, psycopg=psycopg) as connection:
        with connection.cursor() as cursor:
            for table_name in selected_tables:
                table_payload = dict(tables_payload.get(table_name) or {})
                if not table_payload:
                    continue
                columns = [dict(item) for item in list(table_payload.get("columns") or []) if isinstance(item, dict)]
                rows = [dict(item) for item in list(table_payload.get("rows") or []) if isinstance(item, dict)]
                if not columns:
                    continue
                if table_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
                    ensure_acquisition_shard_registry_split_schema(cursor)
                    if truncate_first:
                        for split_table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES:
                            cursor.execute(f"TRUNCATE TABLE {_quote_identifier(split_table_name)}")
                    import_summary = upsert_acquisition_shard_registry_rows(cursor, rows, ensure_schema=False)
                    actual_row_count = None
                    validation_mode = "disabled"
                    if validate_postgres:
                        actual_row_count = count_acquisition_shard_registry_rows(cursor)
                        validation_mode = "exact" if truncate_first else "at_least"
                        validation_ok = actual_row_count == len(rows) if truncate_first else actual_row_count >= len(rows)
                        validation_tables[table_name] = {
                            "mode": validation_mode,
                            "expected_row_count": len(rows),
                            "actual_row_count": actual_row_count,
                            "ok": bool(validation_ok),
                        }
                        if not validation_ok:
                            comparator = "==" if truncate_first else ">="
                            validation_errors.append(
                                f"{table_name} expected {comparator} {len(rows)} rows, got {actual_row_count}"
                            )
                    synced_tables[table_name] = {
                        "row_count": int(import_summary.get("row_count") or len(rows)),
                        "column_count": len(columns),
                        "primary_key": [
                            str(item.get("name") or "") for item in columns if int(item.get("pk_position") or 0) > 0
                        ],
                    }
                    continue
                cursor.execute(_build_create_table_sql(table_name, columns))
                if truncate_first:
                    cursor.execute(f"TRUNCATE TABLE {_quote_identifier(table_name)}")
                if rows:
                    insert_sql = _build_upsert_sql(table_name, columns)
                    values = [tuple(row.get(column["name"]) for column in columns) for row in rows]
                    cursor.executemany(insert_sql, values)
                actual_row_count = None
                validation_mode = "disabled"
                if validate_postgres:
                    actual_row_count = _count_postgres_table_rows(cursor, table_name)
                    validation_mode = "exact" if truncate_first else "at_least"
                    validation_ok = actual_row_count == len(rows) if truncate_first else actual_row_count >= len(rows)
                    validation_tables[table_name] = {
                        "mode": validation_mode,
                        "expected_row_count": len(rows),
                        "actual_row_count": actual_row_count,
                        "ok": bool(validation_ok),
                    }
                    if not validation_ok:
                        comparator = "==" if truncate_first else ">="
                        validation_errors.append(
                            f"{table_name} expected {comparator} {len(rows)} rows, got {actual_row_count}"
                        )
                synced_tables[table_name] = {
                    "row_count": len(rows),
                    "column_count": len(columns),
                    "primary_key": [
                        str(item.get("name") or "") for item in columns if int(item.get("pk_position") or 0) > 0
                    ],
                }
        connection.commit()
    if validation_errors:
        raise RuntimeError("Postgres validation failed: " + "; ".join(validation_errors))
    return {
        "status": "synced",
        "snapshot_path": str(resolved_snapshot_path),
        "table_count": len(synced_tables),
        "tables": synced_tables,
        "validated_postgres": bool(validate_postgres),
        "validation": {
            "mode": "exact" if truncate_first else "at_least",
            "table_count": len(validation_tables),
            "tables": validation_tables,
        }
        if validate_postgres
        else {"mode": "disabled", "table_count": 0, "tables": {}},
    }


def restore_control_plane_snapshot_to_sqlite(
    *,
    snapshot_path: str | Path,
    runtime_dir: str | Path,
    sqlite_path: str | Path | None = None,
    tables: list[str] | None = None,
) -> dict[str, Any]:
    resolved_snapshot_path = Path(snapshot_path).expanduser()
    snapshot_payload = json.loads(resolved_snapshot_path.read_text(encoding="utf-8"))
    runtime_root = Path(runtime_dir).expanduser()
    target_sqlite_path = Path(
        _resolve_sqlite_connect_target(runtime_dir=runtime_root, sqlite_path=sqlite_path)
    ).expanduser()
    target_sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    selected_tables = _resolve_snapshot_table_names(snapshot_payload=snapshot_payload, tables=tables)
    tables_payload = dict(snapshot_payload.get("tables") or {})
    restored_tables: dict[str, Any] = {}
    connection = _connect_sqlite(str(target_sqlite_path))
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        for table_name in selected_tables:
            table_payload = dict(tables_payload.get(table_name) or {})
            if not table_payload:
                continue
            if table_name == "generation_index_entries":
                _restore_generation_index_entries(runtime_root, table_payload)
                restored_tables[table_name] = {
                    "row_count": len(
                        [dict(item) for item in list(table_payload.get("rows") or []) if isinstance(item, dict)]
                    ),
                    "column_count": len(
                        [dict(item) for item in list(table_payload.get("columns") or []) if isinstance(item, dict)]
                    ),
                    "target": str(runtime_root / "object_sync" / "generation_index.json"),
                }
                continue
            columns = [dict(item) for item in list(table_payload.get("columns") or []) if isinstance(item, dict)]
            rows = [dict(item) for item in list(table_payload.get("rows") or []) if isinstance(item, dict)]
            if not columns:
                continue
            connection.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")
            connection.execute(_build_create_table_sqlite(table_name, columns))
            if rows:
                insert_sql = _build_insert_sqlite_sql(table_name, columns)
                values = [tuple(row.get(str(column.get('name') or '')) for column in columns) for row in rows]
                connection.executemany(insert_sql, values)
            restored_tables[table_name] = {
                "row_count": len(rows),
                "column_count": len(columns),
            }
        connection.commit()
    finally:
        connection.close()
    return {
        "status": "restored",
        "snapshot_path": str(resolved_snapshot_path),
        "sqlite_path": str(target_sqlite_path),
        "table_count": len(restored_tables),
        "tables": restored_tables,
    }


def control_plane_snapshot_output_path(runtime_dir: str | Path) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    return runtime_root / "object_sync" / "control_plane" / "control_plane_snapshot.json"


def control_plane_postgres_sync_state_path(runtime_dir: str | Path) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    return runtime_root / "object_sync" / "control_plane" / "postgres_sync_state.json"


def load_control_plane_postgres_sync_state(runtime_dir: str | Path) -> dict[str, Any]:
    state_path = control_plane_postgres_sync_state_path(runtime_dir)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def build_control_plane_source_fingerprint(
    *,
    runtime_dir: str | Path,
    sqlite_path: str | Path | None = None,
    tables: list[str] | None = None,
    include_all_sqlite_tables: bool = False,
) -> str:
    runtime_root = Path(runtime_dir).expanduser()
    db_path = _resolve_sqlite_connect_target(runtime_dir=runtime_root, sqlite_path=sqlite_path)
    generation_index_path = runtime_root / "object_sync" / "generation_index.json"
    if include_all_sqlite_tables:
        connection = _connect_sqlite(db_path)
        connection.row_factory = sqlite3.Row
        try:
            selected_tables = _resolve_export_table_names(
                connection,
                runtime_dir=runtime_root,
                tables=tables,
                include_all_sqlite_tables=include_all_sqlite_tables,
            )
        finally:
            connection.close()
    else:
        selected_tables = [
            str(item).strip() for item in list(tables or DEFAULT_CONTROL_PLANE_TABLES) if str(item).strip()
        ]
    fingerprint_payload = {
        "runtime_dir": str(runtime_root),
        "sqlite_path": str(db_path),
        "tables": selected_tables,
        "sqlite": _path_signature(db_path),
        "generation_index": _path_signature(generation_index_path),
    }
    return hashlib.sha256(
        json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def sync_runtime_control_plane_to_postgres(
    *,
    runtime_dir: str | Path,
    sqlite_path: str | Path | None = None,
    dsn: str = "",
    tables: list[str] | None = None,
    truncate_first: bool = False,
    snapshot_path: str | Path | None = None,
    state_path: str | Path | None = None,
    min_interval_seconds: float = 0.0,
    force: bool = False,
    include_all_sqlite_tables: bool = False,
    validate_postgres: bool = False,
    direct_stream: bool = False,
    chunk_size: int = DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE,
    commit_every_chunks: int = DEFAULT_SQLITE_POSTGRES_DIRECT_COMMIT_EVERY_CHUNKS,
    progress_every_chunks: int = DEFAULT_SQLITE_POSTGRES_DIRECT_PROGRESS_EVERY_CHUNKS,
    chunk_pause_seconds: float = 0.0,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    resolved_sqlite_path = _resolve_sqlite_connect_target(runtime_dir=runtime_root, sqlite_path=sqlite_path)
    resolved_snapshot_path = (
        Path(snapshot_path).expanduser() if snapshot_path else control_plane_snapshot_output_path(runtime_root)
    )
    resolved_state_path = (
        Path(state_path).expanduser() if state_path else control_plane_postgres_sync_state_path(runtime_root)
    )
    connection = _connect_sqlite(resolved_sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        selected_tables = _resolve_export_table_names(
            connection,
            runtime_dir=runtime_root,
            tables=tables,
            include_all_sqlite_tables=include_all_sqlite_tables,
        )
    finally:
        connection.close()
    effective_dsn = str(dsn or resolve_control_plane_postgres_dsn(runtime_root)).strip()
    if effective_dsn and not str(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN") or "").strip():
        try:
            ensure_local_postgres_started(runtime_root)
        except Exception:
            pass
    effective_min_interval_seconds = max(
        0.0,
        float(
            min_interval_seconds
            or os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_SYNC_MIN_INTERVAL_SECONDS")
            or DEFAULT_CONTROL_PLANE_POSTGRES_SYNC_MIN_INTERVAL_SECONDS
        ),
    )
    source_fingerprint = build_control_plane_source_fingerprint(
        runtime_dir=runtime_root,
        sqlite_path=resolved_sqlite_path,
        tables=selected_tables,
        include_all_sqlite_tables=include_all_sqlite_tables,
    )
    previous_state = load_control_plane_postgres_sync_state(runtime_root)
    now_epoch = time.time()
    last_started_epoch = _timestamp_to_epoch(str(previous_state.get("last_started_at") or ""))

    summary_base = {
        "runtime_dir": str(runtime_root),
        "sqlite_path": str(resolved_sqlite_path),
        "snapshot_path": str(resolved_snapshot_path),
        "state_path": str(resolved_state_path),
        "table_count_requested": len(selected_tables),
        "tables": selected_tables,
        "truncate_first": bool(truncate_first),
        "force": bool(force),
        "min_interval_seconds": effective_min_interval_seconds,
        "source_fingerprint": source_fingerprint,
        "include_all_sqlite_tables": bool(include_all_sqlite_tables),
        "validate_postgres": bool(validate_postgres),
        "direct_stream": bool(direct_stream),
        "chunk_size": max(1, int(chunk_size or DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE)),
        "commit_every_chunks": max(1, int(commit_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_COMMIT_EVERY_CHUNKS)),
        "progress_every_chunks": max(
            1,
            int(progress_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_PROGRESS_EVERY_CHUNKS),
        ),
        "chunk_pause_seconds": max(0.0, float(chunk_pause_seconds or 0.0)),
    }
    if not effective_dsn:
        summary = {
            **summary_base,
            "status": "disabled",
            "reason": "missing_dsn",
        }
        _write_control_plane_sync_state(
            resolved_state_path,
            previous_state=previous_state,
            summary=summary,
            preserve_last_success=True,
        )
        return summary

    previous_fingerprint = str(previous_state.get("last_synced_fingerprint") or "").strip()
    previous_status = str(previous_state.get("last_sync_status") or "").strip().lower()
    if (
        not force
        and previous_fingerprint
        and previous_fingerprint == source_fingerprint
        and previous_status == "synced"
    ):
        summary = {
            **summary_base,
            "status": "skipped",
            "reason": "unchanged_source",
            "last_synced_at": str(previous_state.get("last_completed_at") or ""),
        }
        _write_control_plane_sync_state(
            resolved_state_path,
            previous_state=previous_state,
            summary=summary,
            preserve_last_success=True,
        )
        return summary

    if (
        not force
        and effective_min_interval_seconds > 0
        and last_started_epoch > 0
        and (now_epoch - last_started_epoch) < effective_min_interval_seconds
    ):
        summary = {
            **summary_base,
            "status": "skipped",
            "reason": "throttled",
            "seconds_since_last_start": round(now_epoch - last_started_epoch, 3),
            "last_started_at": str(previous_state.get("last_started_at") or ""),
        }
        _write_control_plane_sync_state(
            resolved_state_path,
            previous_state=previous_state,
            summary=summary,
            preserve_last_success=True,
        )
        return summary

    started_at = _utc_now_iso()
    _write_control_plane_sync_state(
        resolved_state_path,
        previous_state=previous_state,
        summary={
            **summary_base,
            "status": "running",
            "started_at": started_at,
        },
        started_at=started_at,
        preserve_last_success=True,
    )

    try:
        if direct_stream:
            export_summary = {
                "status": "skipped",
                "reason": "direct_stream",
                "table_count": len(selected_tables),
            }
            postgres_summary = _sync_runtime_sqlite_to_postgres_direct(
                runtime_dir=runtime_root,
                sqlite_path=resolved_sqlite_path,
                dsn=effective_dsn,
                tables=selected_tables,
                truncate_first=truncate_first,
                validate_postgres=validate_postgres,
                state_path=resolved_state_path,
                previous_state=previous_state,
                summary_base=summary_base,
                started_at=started_at,
                source_fingerprint=source_fingerprint,
                chunk_size=max(1, int(chunk_size or DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE)),
                commit_every_chunks=max(
                    1,
                    int(commit_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_COMMIT_EVERY_CHUNKS),
                ),
                progress_every_chunks=max(
                    1,
                    int(progress_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_PROGRESS_EVERY_CHUNKS),
                ),
                chunk_pause_seconds=max(0.0, float(chunk_pause_seconds or 0.0)),
            )
        else:
            export_summary = export_control_plane_snapshot(
                runtime_dir=runtime_root,
                output_path=resolved_snapshot_path,
                sqlite_path=resolved_sqlite_path,
                tables=selected_tables,
                include_all_sqlite_tables=include_all_sqlite_tables,
            )
            postgres_summary = sync_control_plane_snapshot_to_postgres(
                snapshot_path=resolved_snapshot_path,
                dsn=effective_dsn,
                tables=selected_tables,
                truncate_first=truncate_first,
                validate_postgres=validate_postgres,
            )
    except Exception as exc:
        summary = {
            **summary_base,
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "started_at": started_at,
        }
        _write_control_plane_sync_state(
            resolved_state_path,
            previous_state=previous_state,
            summary=summary,
            started_at=started_at,
            preserve_last_success=True,
        )
        return summary

    summary = {
        **summary_base,
        "status": "synced",
        "started_at": started_at,
        "export": export_summary,
        "postgres": postgres_summary,
        "table_count_synced": int(postgres_summary.get("table_count") or 0),
    }
    _write_control_plane_sync_state(
        resolved_state_path,
        previous_state=previous_state,
        summary=summary,
        started_at=started_at,
        last_synced_fingerprint=source_fingerprint,
    )
    return summary


def _export_sqlite_table(connection: sqlite3.Connection, table_name: str) -> dict[str, Any] | None:
    if not _sqlite_table_exists(connection, table_name):
        return None
    columns = _sqlite_table_columns(connection, table_name)
    rows = [dict(row) for row in connection.execute(f"SELECT * FROM {_quote_identifier(table_name)}")]
    return {
        "columns": columns,
        "row_count": len(rows),
        "rows": rows,
    }


def _write_control_plane_snapshot_json(
    *,
    output_path: Path,
    runtime_root: Path,
    sqlite_path: str,
    source_backend: str,
    include_all_sqlite_tables: bool,
    requested_tables: list[str],
    table_writer: Callable[[TextIO, str], dict[str, Any] | None],
) -> dict[str, dict[str, int]]:
    temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    table_summaries: dict[str, dict[str, int]] = {}
    header_fields = [
        ("schema_version", 1),
        ("created_at", _utc_now_iso()),
        ("runtime_dir", str(runtime_root)),
        ("sqlite_path", sqlite_path),
        ("source_backend", source_backend),
        ("include_all_sqlite_tables", bool(include_all_sqlite_tables)),
        ("requested_tables", list(requested_tables)),
    ]
    with temp_path.open("w", encoding="utf-8") as handle:
        handle.write("{\n")
        for key, value in header_fields:
            handle.write(f"  {json.dumps(key)}: {json.dumps(value, ensure_ascii=False)},\n")
        handle.write('  "tables": {')
        first_table = True
        for table_name in requested_tables:
            table_buffer = io.StringIO()
            table_summary = table_writer(table_buffer, table_name)
            if table_summary is None:
                continue
            if first_table:
                handle.write("\n")
            else:
                handle.write(",\n")
            handle.write(table_buffer.getvalue())
            first_table = False
            table_summaries[table_name] = {
                "row_count": int(table_summary.get("row_count") or 0),
                "column_count": int(table_summary.get("column_count") or 0),
            }
        if not first_table:
            handle.write("\n")
        handle.write("  }\n}\n")
    temp_path.replace(output_path)
    return table_summaries


def _write_sqlite_snapshot_table_json(
    handle: TextIO,
    *,
    connection: sqlite3.Connection,
    runtime_dir: Path,
    table_name: str,
) -> dict[str, Any] | None:
    if table_name == "generation_index_entries":
        payload = _export_generation_index_entries(runtime_dir)
        if payload is None:
            return None
        return _write_snapshot_table_payload_json(handle, table_name=table_name, payload=payload)
    if not _sqlite_table_exists(connection, table_name):
        return None
    columns = _sqlite_table_columns(connection, table_name)
    if not columns:
        return None
    row_count = _count_sqlite_table_rows(connection, table_name)
    cursor = connection.execute(f"SELECT * FROM {_quote_identifier(table_name)}")
    return _write_snapshot_table_json(
        handle,
        table_name=table_name,
        columns=columns,
        row_count=row_count,
        rows=_iter_cursor_rows_as_dicts(cursor),
    )


def _write_postgres_snapshot_table_json(
    handle: TextIO,
    *,
    cursor: Any,
    runtime_dir: Path,
    table_name: str,
) -> dict[str, Any] | None:
    if table_name == "generation_index_entries":
        payload = _export_generation_index_entries(runtime_dir)
        if payload is None:
            return None
        return _write_snapshot_table_payload_json(handle, table_name=table_name, payload=payload)
    if table_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
        payload = read_acquisition_shard_registry_snapshot(cursor)
        if payload is None:
            return None
        return _write_snapshot_table_payload_json(handle, table_name=table_name, payload=payload)
    if not _postgres_table_exists(cursor, table_name):
        return None
    columns = _postgres_table_columns(cursor, table_name)
    if not columns:
        return None
    row_count = _count_postgres_table_rows(cursor, table_name)
    cursor.execute(f"SELECT * FROM {_quote_identifier(table_name)}")
    return _write_snapshot_table_json(
        handle,
        table_name=table_name,
        columns=columns,
        row_count=row_count,
        rows=_iter_cursor_rows_as_dicts(cursor),
    )


def _write_snapshot_table_payload_json(
    handle: TextIO,
    *,
    table_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return _write_snapshot_table_json(
        handle,
        table_name=table_name,
        columns=list(payload.get("columns") or []),
        row_count=int(payload.get("row_count") or 0),
        rows=iter(list(payload.get("rows") or [])),
    )


def _write_snapshot_table_json(
    handle: TextIO,
    *,
    table_name: str,
    columns: list[dict[str, Any]],
    row_count: int,
    rows: Iterator[dict[str, Any]],
) -> dict[str, Any]:
    handle.write(f"    {json.dumps(table_name)}: {{\n")
    handle.write(f'      "columns": {json.dumps(columns, ensure_ascii=False)},\n')
    handle.write(f'      "row_count": {int(row_count)},\n')
    handle.write('      "rows": [')
    first_row = True
    for row in rows:
        if first_row:
            handle.write("\n")
        else:
            handle.write(",\n")
        first_row = False
        handle.write(f"        {json.dumps(row, ensure_ascii=False)}")
    if not first_row:
        handle.write("\n")
    handle.write("      ]\n    }")
    return {
        "row_count": int(row_count),
        "column_count": len(columns),
    }


def _export_control_plane_snapshot_from_postgres(
    *,
    runtime_dir: Path,
    dsn: str,
    tables: list[str] | None = None,
    include_all_sqlite_tables: bool = False,
) -> tuple[list[str], dict[str, Any]]:
    psycopg = _import_psycopg()
    exported_tables: dict[str, Any] = {}
    with _connect_postgres(dsn, psycopg=psycopg) as connection:
        with connection.cursor() as cursor:
            selected_tables = _resolve_export_table_names_postgres(
                cursor,
                runtime_dir=runtime_dir,
                tables=tables,
                include_all_sqlite_tables=include_all_sqlite_tables,
            )
            for table_name in selected_tables:
                if table_name == "generation_index_entries":
                    exported_table = _export_generation_index_entries(runtime_dir)
                else:
                    exported_table = _export_postgres_table(cursor, table_name)
                if exported_table is None:
                    continue
                exported_tables[table_name] = exported_table
    return selected_tables, exported_tables


def _export_postgres_table(cursor: Any, table_name: str) -> dict[str, Any] | None:
    if table_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
        return read_acquisition_shard_registry_snapshot(cursor)
    if not _postgres_table_exists(cursor, table_name):
        return None
    columns = _postgres_table_columns(cursor, table_name)
    if not columns:
        return None
    cursor.execute(f"SELECT * FROM {_quote_identifier(table_name)}")
    rows = _fetch_cursor_rows_as_dicts(cursor)
    return {
        "columns": columns,
        "row_count": len(rows),
        "rows": rows,
    }


def _export_generation_index_entries(runtime_dir: Path) -> dict[str, Any] | None:
    generation_index_path = runtime_dir / "object_sync" / "generation_index.json"
    if not generation_index_path.exists():
        return None
    payload = json.loads(generation_index_path.read_text(encoding="utf-8"))
    rows = [
        dict(item)
        for item in list(payload.get("generations") or [])
        if isinstance(item, dict) and str(item.get("generation_key") or "").strip()
    ]
    return {
        "columns": [
            {"name": "generation_key", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 1},
            {"name": "generation_sequence", "type": "INTEGER", "notnull": 1, "default": "0", "pk_position": 0},
            {"name": "generation_watermark", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "target_company", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "company_key", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "snapshot_id", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "asset_view", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "remote_prefix", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "generation_manifest_key", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "generation_manifest_path", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "latest_run_id", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "last_uploaded_at", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "last_hydrated_at", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "hot_cache_snapshot_dir", "type": "TEXT", "notnull": 0, "default": None, "pk_position": 0},
            {"name": "hydrated_file_count", "type": "INTEGER", "notnull": 1, "default": "0", "pk_position": 0},
        ],
        "row_count": len(rows),
        "rows": rows,
    }


def _restore_generation_index_entries(runtime_dir: Path, table_payload: dict[str, Any]) -> None:
    generation_index_path = runtime_dir / "object_sync" / "generation_index.json"
    generation_index_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [dict(item) for item in list(table_payload.get("rows") or []) if isinstance(item, dict)]
    generation_index_path.write_text(
        json.dumps(
            {
                "updated_at": _utc_now_iso(),
                "generations": rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _sync_runtime_sqlite_to_postgres_direct(
    *,
    runtime_dir: Path,
    sqlite_path: str | Path,
    dsn: str,
    tables: list[str],
    truncate_first: bool,
    validate_postgres: bool,
    state_path: Path,
    previous_state: dict[str, Any],
    summary_base: dict[str, Any],
    started_at: str,
    source_fingerprint: str,
    chunk_size: int,
    commit_every_chunks: int,
    progress_every_chunks: int,
    chunk_pause_seconds: float,
) -> dict[str, Any]:
    sqlite_connection = _connect_sqlite(str(sqlite_path))
    sqlite_connection.row_factory = sqlite3.Row
    try:
        psycopg = _import_psycopg()
        previous_table_progress = _load_table_progress(previous_state, source_fingerprint=source_fingerprint)
        selected_table_names = set(tables)
        table_progress: dict[str, Any] = {
            table_name: dict(progress)
            for table_name, progress in previous_table_progress.items()
            if table_name in selected_table_names
        }
        synced_tables: dict[str, Any] = {}
        validation_tables: dict[str, Any] = {}
        skipped_tables: list[str] = []
        with _connect_postgres(dsn, psycopg=psycopg) as postgres_connection:
            with postgres_connection.cursor() as postgres_cursor:
                for table_name in tables:
                    previous_progress = dict(table_progress.get(table_name) or {})
                    if str(previous_progress.get("status") or "").strip().lower() == "synced":
                        skipped_tables.append(table_name)
                        synced_tables[table_name] = previous_progress
                        if validate_postgres and isinstance(previous_progress.get("validation"), dict):
                            validation_tables[table_name] = dict(previous_progress.get("validation") or {})
                        continue
                    _write_table_progress_state(
                        state_path=state_path,
                        previous_state=previous_state,
                        summary={
                            **summary_base,
                            "status": "running",
                            "started_at": started_at,
                            "current_table": table_name,
                            "table_progress": {
                                **table_progress,
                                table_name: {
                                    **previous_progress,
                                    "status": "running",
                                },
                            },
                        },
                        started_at=started_at,
                    )
                    def _progress_callback(progress: dict[str, Any]) -> None:
                        current_progress = {
                            **previous_progress,
                            **progress,
                            "status": "running",
                        }
                        table_progress[table_name] = current_progress
                        _write_table_progress_state(
                            state_path=state_path,
                            previous_state=previous_state,
                            summary={
                                **summary_base,
                                "status": "running",
                                "started_at": started_at,
                                "current_table": table_name,
                                "table_progress": table_progress,
                            },
                            started_at=started_at,
                        )
                    table_summary = _copy_sqlite_table_to_postgres(
                        sqlite_connection=sqlite_connection,
                        postgres_connection=postgres_connection,
                        postgres_cursor=postgres_cursor,
                        runtime_dir=runtime_dir,
                        table_name=table_name,
                        truncate_first=truncate_first,
                        validate_postgres=validate_postgres,
                        chunk_size=chunk_size,
                        commit_every_chunks=commit_every_chunks,
                        progress_every_chunks=progress_every_chunks,
                        chunk_pause_seconds=chunk_pause_seconds,
                        progress_callback=_progress_callback,
                    )
                    postgres_connection.commit()
                    table_progress[table_name] = table_summary
                    synced_tables[table_name] = table_summary
                    if validate_postgres:
                        validation_tables[table_name] = dict(table_summary.get("validation") or {})
                    _write_table_progress_state(
                        state_path=state_path,
                        previous_state=previous_state,
                        summary={
                            **summary_base,
                            "status": "running",
                            "started_at": started_at,
                            "current_table": table_name,
                            "table_progress": table_progress,
                        },
                        started_at=started_at,
                    )
    finally:
        sqlite_connection.close()
    return {
        "status": "synced",
        "mode": "direct_stream",
        "table_count": len(synced_tables),
        "chunk_size": max(1, int(chunk_size or DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE)),
        "commit_every_chunks": max(1, int(commit_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_COMMIT_EVERY_CHUNKS)),
        "progress_every_chunks": max(
            1,
            int(progress_every_chunks or DEFAULT_SQLITE_POSTGRES_DIRECT_PROGRESS_EVERY_CHUNKS),
        ),
        "chunk_pause_seconds": max(0.0, float(chunk_pause_seconds or 0.0)),
        "skipped_tables": skipped_tables,
        "tables": synced_tables,
        "validated_postgres": bool(validate_postgres),
        "validation": {
            "mode": "exact" if truncate_first else "at_least",
            "table_count": len(validation_tables),
            "tables": validation_tables,
        }
        if validate_postgres
        else {"mode": "disabled", "table_count": 0, "tables": {}},
    }


def _copy_sqlite_table_to_postgres(
    *,
    sqlite_connection: sqlite3.Connection,
    postgres_connection: Any,
    postgres_cursor: Any,
    runtime_dir: Path,
    table_name: str,
    truncate_first: bool,
    validate_postgres: bool,
    chunk_size: int,
    commit_every_chunks: int,
    progress_every_chunks: int,
    chunk_pause_seconds: float,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if table_name == "generation_index_entries":
        exported = _export_generation_index_entries(runtime_dir)
        if not exported:
            return {
                "status": "missing",
                "row_count": 0,
                "column_count": 0,
                "chunk_count": 0,
            }
        columns = [dict(item) for item in list(exported.get("columns") or []) if isinstance(item, dict)]
        rows = [dict(item) for item in list(exported.get("rows") or []) if isinstance(item, dict)]
        postgres_cursor.execute(_build_create_table_sql(table_name, columns))
        if truncate_first:
            postgres_cursor.execute(f"TRUNCATE TABLE {_quote_identifier(table_name)}")
        chunk_count = 0
        nul_text_replacements = 0
        if rows:
            insert_sql = _build_upsert_sql(table_name, columns)
            for batch in _chunked_rows(rows, chunk_size=max(1, int(chunk_size or 1))):
                values: list[tuple[Any, ...]] = []
                for row in batch:
                    prepared_row, replacement_count = _prepare_postgres_row_values(row, columns)
                    values.append(prepared_row)
                    nul_text_replacements += replacement_count
                postgres_cursor.executemany(insert_sql, values)
                chunk_count += 1
                _flush_direct_chunk_progress(
                    postgres_connection=postgres_connection,
                    chunk_count=chunk_count,
                    row_count=min(len(rows), chunk_count * max(1, int(chunk_size or 1))),
                    expected_row_count=len(rows),
                    column_count=len(columns),
                    commit_every_chunks=commit_every_chunks,
                    progress_every_chunks=progress_every_chunks,
                    chunk_pause_seconds=chunk_pause_seconds,
                    progress_callback=progress_callback,
                )
        validation = _validate_postgres_table(
            postgres_cursor=postgres_cursor,
            table_name=table_name,
            expected_row_count=len(rows),
            truncate_first=truncate_first,
            validate_postgres=validate_postgres,
        )
        return {
            "status": "synced",
            "row_count": len(rows),
            "column_count": len(columns),
            "chunk_count": chunk_count,
            "nul_text_replacements": nul_text_replacements,
            "primary_key": [str(item.get("name") or "") for item in columns if int(item.get("pk_position") or 0) > 0],
            "validation": validation,
        }
    if table_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
        if not _sqlite_table_exists(sqlite_connection, table_name):
            return {
                "status": "missing",
                "row_count": 0,
                "column_count": 0,
                "chunk_count": 0,
            }
        columns = _sqlite_table_columns(sqlite_connection, table_name)
        if not columns:
            return {
                "status": "missing_columns",
                "row_count": 0,
                "column_count": 0,
                "chunk_count": 0,
            }
        expected_row_count = int(
            sqlite_connection.execute(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}").fetchone()[0] or 0
        )
        ensure_acquisition_shard_registry_split_schema(postgres_cursor)
        if truncate_first:
            for split_table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES:
                postgres_cursor.execute(f"TRUNCATE TABLE {_quote_identifier(split_table_name)}")
        sqlite_cursor = sqlite_connection.execute(f"SELECT * FROM {_quote_identifier(table_name)}")
        row_count = 0
        chunk_count = 0
        nul_text_replacements = 0
        fetch_size = max(1, int(chunk_size or DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE))
        while True:
            batch = sqlite_cursor.fetchmany(fetch_size)
            if not batch:
                break
            import_summary = upsert_acquisition_shard_registry_rows(
                postgres_cursor,
                [dict(row) for row in batch],
                ensure_schema=False,
            )
            row_count += int(import_summary.get("row_count") or 0)
            nul_text_replacements += int(import_summary.get("nul_text_replacements") or 0)
            chunk_count += 1
            _flush_direct_chunk_progress(
                postgres_connection=postgres_connection,
                chunk_count=chunk_count,
                row_count=row_count,
                expected_row_count=expected_row_count,
                column_count=len(columns),
                commit_every_chunks=commit_every_chunks,
                progress_every_chunks=progress_every_chunks,
                chunk_pause_seconds=chunk_pause_seconds,
                progress_callback=progress_callback,
            )
        validation = {
            "mode": "disabled",
            "expected_row_count": expected_row_count,
            "actual_row_count": 0,
            "ok": True,
        }
        if validate_postgres:
            actual_row_count = count_acquisition_shard_registry_rows(postgres_cursor)
            mode = "exact" if truncate_first else "at_least"
            ok = actual_row_count == expected_row_count if truncate_first else actual_row_count >= expected_row_count
            if not ok:
                comparator = "==" if truncate_first else ">="
                raise RuntimeError(
                    f"Postgres validation failed for {table_name}: expected {comparator} {expected_row_count} rows, got {actual_row_count}"
                )
            validation = {
                "mode": mode,
                "expected_row_count": expected_row_count,
                "actual_row_count": actual_row_count,
                "ok": True,
            }
        return {
            "status": "synced",
            "row_count": row_count,
            "column_count": len(columns),
            "chunk_count": chunk_count,
            "nul_text_replacements": nul_text_replacements,
            "primary_key": [str(item.get("name") or "") for item in columns if int(item.get("pk_position") or 0) > 0],
            "validation": validation,
        }

    if not _sqlite_table_exists(sqlite_connection, table_name):
        return {
            "status": "missing",
            "row_count": 0,
            "column_count": 0,
            "chunk_count": 0,
        }
    columns = _sqlite_table_columns(sqlite_connection, table_name)
    if not columns:
        return {
            "status": "missing_columns",
            "row_count": 0,
            "column_count": 0,
            "chunk_count": 0,
        }
    expected_row_count = int(
        sqlite_connection.execute(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}").fetchone()[0] or 0
    )
    postgres_cursor.execute(_build_create_table_sql(table_name, columns))
    if truncate_first:
        postgres_cursor.execute(f"TRUNCATE TABLE {_quote_identifier(table_name)}")
    insert_sql = _build_upsert_sql(table_name, columns)
    sqlite_cursor = sqlite_connection.execute(f"SELECT * FROM {_quote_identifier(table_name)}")
    row_count = 0
    chunk_count = 0
    nul_text_replacements = 0
    fetch_size = max(1, int(chunk_size or DEFAULT_SQLITE_POSTGRES_DIRECT_CHUNK_SIZE))
    while True:
        batch = sqlite_cursor.fetchmany(fetch_size)
        if not batch:
            break
        batch_values: list[tuple[Any, ...]] = []
        for row in batch:
            prepared_row, replacement_count = _prepare_postgres_row_values(dict(row), columns)
            batch_values.append(prepared_row)
            nul_text_replacements += replacement_count
        postgres_cursor.executemany(insert_sql, batch_values)
        row_count += len(batch_values)
        chunk_count += 1
        _flush_direct_chunk_progress(
            postgres_connection=postgres_connection,
            chunk_count=chunk_count,
            row_count=row_count,
            expected_row_count=expected_row_count,
            column_count=len(columns),
            commit_every_chunks=commit_every_chunks,
            progress_every_chunks=progress_every_chunks,
            chunk_pause_seconds=chunk_pause_seconds,
            progress_callback=progress_callback,
        )
    validation = _validate_postgres_table(
        postgres_cursor=postgres_cursor,
        table_name=table_name,
        expected_row_count=expected_row_count,
        truncate_first=truncate_first,
        validate_postgres=validate_postgres,
    )
    return {
        "status": "synced",
        "row_count": row_count,
        "column_count": len(columns),
        "chunk_count": chunk_count,
        "nul_text_replacements": nul_text_replacements,
        "primary_key": [str(item.get("name") or "") for item in columns if int(item.get("pk_position") or 0) > 0],
        "validation": validation,
    }


def _prepare_postgres_row_values(row: dict[str, Any], columns: list[dict[str, Any]]) -> tuple[tuple[Any, ...], int]:
    values: list[Any] = []
    nul_text_replacements = 0
    for column in columns:
        value, replacement_count = _normalize_postgres_value(row.get(column["name"]))
        values.append(value)
        nul_text_replacements += replacement_count
    return tuple(values), nul_text_replacements


def _normalize_postgres_value(value: Any) -> tuple[Any, int]:
    if isinstance(value, str):
        nul_count = value.count("\x00")
        if nul_count <= 0:
            return value, 0
        return value.replace("\x00", "\\u0000"), nul_count
    if isinstance(value, bytearray):
        return bytes(value), 0
    if isinstance(value, memoryview):
        return bytes(value), 0
    return value, 0


def _flush_direct_chunk_progress(
    *,
    postgres_connection: Any,
    chunk_count: int,
    row_count: int,
    expected_row_count: int,
    column_count: int,
    commit_every_chunks: int,
    progress_every_chunks: int,
    chunk_pause_seconds: float,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> None:
    if commit_every_chunks > 0 and (chunk_count % commit_every_chunks) == 0:
        postgres_connection.commit()
    if progress_callback and progress_every_chunks > 0 and (chunk_count % progress_every_chunks) == 0:
        progress_callback(
            {
                "row_count": row_count,
                "expected_row_count": expected_row_count,
                "column_count": column_count,
                "chunk_count": chunk_count,
            }
        )
    if chunk_pause_seconds > 0:
        time.sleep(chunk_pause_seconds)


def _validate_postgres_table(
    *,
    postgres_cursor: Any,
    table_name: str,
    expected_row_count: int,
    truncate_first: bool,
    validate_postgres: bool,
) -> dict[str, Any]:
    if not validate_postgres:
        return {"mode": "disabled", "expected_row_count": expected_row_count, "actual_row_count": 0, "ok": True}
    actual_row_count = _count_postgres_table_rows(postgres_cursor, table_name)
    mode = "exact" if truncate_first else "at_least"
    ok = actual_row_count == expected_row_count if truncate_first else actual_row_count >= expected_row_count
    if not ok:
        comparator = "==" if truncate_first else ">="
        raise RuntimeError(
            f"Postgres validation failed for {table_name}: expected {comparator} {expected_row_count} rows, got {actual_row_count}"
        )
    return {
        "mode": mode,
        "expected_row_count": expected_row_count,
        "actual_row_count": actual_row_count,
        "ok": True,
    }


def _chunked_rows(rows: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    size = max(1, int(chunk_size or 1))
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def _load_table_progress(previous_state: dict[str, Any], *, source_fingerprint: str) -> dict[str, Any]:
    previous_fingerprint = str(previous_state.get("last_synced_fingerprint") or previous_state.get("source_fingerprint") or "").strip()
    if not previous_fingerprint or previous_fingerprint != str(source_fingerprint or "").strip():
        return {}
    previous_summary = dict(previous_state.get("last_summary") or {})
    table_progress = dict(previous_summary.get("table_progress") or {})
    return {str(table_name): dict(progress or {}) for table_name, progress in table_progress.items()}


def _write_table_progress_state(
    *,
    state_path: Path,
    previous_state: dict[str, Any],
    summary: dict[str, Any],
    started_at: str,
) -> None:
    _write_control_plane_sync_state(
        state_path,
        previous_state=previous_state,
        summary=summary,
        started_at=started_at,
        preserve_last_success=True,
    )


def _resolve_export_table_names(
    connection: sqlite3.Connection,
    *,
    runtime_dir: Path,
    tables: list[str] | None = None,
    include_all_sqlite_tables: bool = False,
) -> list[str]:
    explicit_tables = [str(item).strip() for item in list(tables or []) if str(item).strip()]
    if include_all_sqlite_tables:
        resolved = _list_sqlite_tables(connection)
        if _generation_index_exists(runtime_dir):
            resolved.append("generation_index_entries")
        if explicit_tables:
            resolved.extend(explicit_tables)
        return _dedupe_table_names(resolved)
    if explicit_tables:
        return _dedupe_table_names(explicit_tables)
    return _dedupe_table_names(DEFAULT_CONTROL_PLANE_TABLES)


def _resolve_snapshot_table_names(
    *,
    snapshot_payload: dict[str, Any],
    tables: list[str] | None = None,
) -> list[str]:
    explicit_tables = [str(item).strip() for item in list(tables or []) if str(item).strip()]
    if explicit_tables:
        return _dedupe_table_names(explicit_tables)
    return _dedupe_table_names(list(dict(snapshot_payload.get("tables") or {}).keys()))


def _dedupe_table_names(table_names: list[str] | tuple[str, ...]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw_name in table_names:
        name = str(raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _generation_index_exists(runtime_dir: Path) -> bool:
    return (runtime_dir / "object_sync" / "generation_index.json").exists()


def _list_sqlite_tables(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row["name"] or "").strip() for row in rows if str(row["name"] or "").strip()]


def _sqlite_table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def _sqlite_table_columns(connection: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    rows = connection.execute(f"PRAGMA table_info({_quote_identifier(table_name)})").fetchall()
    return [
        {
            "name": str(row["name"] or ""),
            "type": str(row["type"] or "TEXT"),
            "notnull": int(row["notnull"] or 0),
            "default": row["dflt_value"],
            "pk_position": int(row["pk"] or 0),
        }
        for row in rows
        if str(row["name"] or "").strip()
    ]


def _count_sqlite_table_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(table_name)}").fetchone()
    if isinstance(row, sqlite3.Row):
        return int(row["row_count"] or 0)
    if isinstance(row, (list, tuple)):
        return int((row[0] if row else 0) or 0)
    return int(getattr(row, "row_count", 0) or 0)


def _resolve_export_table_names_postgres(
    cursor: Any,
    *,
    runtime_dir: Path,
    tables: list[str] | None = None,
    include_all_sqlite_tables: bool = False,
) -> list[str]:
    explicit_tables = [str(item).strip() for item in list(tables or []) if str(item).strip()]
    if include_all_sqlite_tables:
        resolved = _list_postgres_tables(cursor)
        if _generation_index_exists(runtime_dir):
            resolved.append("generation_index_entries")
        if explicit_tables:
            resolved.extend(explicit_tables)
        return _dedupe_table_names(resolved)
    if explicit_tables:
        return _dedupe_table_names(explicit_tables)
    return _dedupe_table_names(DEFAULT_CONTROL_PLANE_TABLES)


def _list_postgres_tables(cursor: Any) -> list[str]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    resolved_tables = [
        str(value or "").strip()
        for value in _extract_cursor_column_values(cursor, "table_name")
        if str(value or "").strip()
        and str(value or "").strip() not in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES
    ]
    logical_relation_kind = _postgres_relation_kind(cursor, ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)
    if logical_relation_kind in {"r", "p", "v", "m"} or any(
        _postgres_relation_kind(cursor, split_table_name) in {"r", "p"}
        for split_table_name in ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES
    ):
        resolved_tables.append(ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE)
    return resolved_tables


def _postgres_table_exists(cursor: Any, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = %s
        LIMIT 1
        """,
        (str(table_name or "").strip(),),
    )
    return cursor.fetchone() is not None


def _postgres_table_columns(cursor: Any, table_name: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            attribute.attname AS name,
            format_type(attribute.atttypid, attribute.atttypmod) AS type,
            CASE WHEN attribute.attnotnull THEN 1 ELSE 0 END AS notnull,
            pg_get_expr(attrdef.adbin, attrdef.adrelid) AS default,
            COALESCE(
                CASE
                    WHEN index_meta.indisprimary THEN array_position(index_meta.indkey::smallint[], attribute.attnum::smallint)
                    ELSE NULL
                END,
                0
            ) AS pk_position
        FROM pg_attribute AS attribute
        JOIN pg_class AS class_rel
          ON class_rel.oid = attribute.attrelid
        JOIN pg_namespace AS namespace_rel
          ON namespace_rel.oid = class_rel.relnamespace
        LEFT JOIN pg_attrdef AS attrdef
          ON attrdef.adrelid = attribute.attrelid
         AND attrdef.adnum = attribute.attnum
        LEFT JOIN pg_index AS index_meta
          ON index_meta.indrelid = class_rel.oid
         AND index_meta.indisprimary
        WHERE namespace_rel.nspname = 'public'
          AND class_rel.relname = %s
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY attribute.attnum
        """,
        (str(table_name or "").strip(),),
    )
    return [
        {
            "name": str(row.get("name") or ""),
            "type": str(row.get("type") or "TEXT"),
            "notnull": int(row.get("notnull") or 0),
            "default": row.get("default"),
            "pk_position": int(row.get("pk_position") or 0),
        }
        for row in _fetch_cursor_rows_as_dicts(cursor)
        if str(row.get("name") or "").strip()
    ]


def _build_create_table_sql(table_name: str, columns: list[dict[str, Any]]) -> str:
    column_sql: list[str] = []
    primary_key_columns = [dict(item) for item in columns if int(item.get("pk_position") or 0) > 0]
    for column in columns:
        parts = [
            _quote_identifier(str(column.get("name") or "")),
            _sqlite_decl_to_postgres_type(str(column.get("type") or "TEXT")),
        ]
        if int(column.get("notnull") or 0) > 0:
            parts.append("NOT NULL")
        column_sql.append(" ".join(parts))
    if primary_key_columns:
        sorted_primary_keys = sorted(primary_key_columns, key=lambda item: int(item.get("pk_position") or 0))
        column_sql.append(
            f"PRIMARY KEY ({', '.join(_quote_identifier(str(item.get('name') or '')) for item in sorted_primary_keys)})"
        )
    return f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} ({', '.join(column_sql)})"


def _build_create_table_sqlite(table_name: str, columns: list[dict[str, Any]]) -> str:
    column_sql: list[str] = []
    primary_key_columns = [dict(item) for item in columns if int(item.get("pk_position") or 0) > 0]
    for column in columns:
        declared_type = str(column.get("type") or "TEXT").strip() or "TEXT"
        parts = [
            _quote_identifier(str(column.get("name") or "")),
            declared_type,
        ]
        if int(column.get("notnull") or 0) > 0:
            parts.append("NOT NULL")
        column_sql.append(" ".join(parts))
    if primary_key_columns:
        sorted_primary_keys = sorted(primary_key_columns, key=lambda item: int(item.get("pk_position") or 0))
        column_sql.append(
            f"PRIMARY KEY ({', '.join(_quote_identifier(str(item.get('name') or '')) for item in sorted_primary_keys)})"
        )
    return f"CREATE TABLE {_quote_identifier(table_name)} ({', '.join(column_sql)})"


def _build_upsert_sql(table_name: str, columns: list[dict[str, Any]]) -> str:
    quoted_table_name = _quote_identifier(table_name)
    quoted_columns = [_quote_identifier(str(column.get("name") or "")) for column in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    primary_key_columns = [
        _quote_identifier(str(item.get("name") or ""))
        for item in sorted(columns, key=lambda item: int(item.get("pk_position") or 0))
        if int(item.get("pk_position") or 0) > 0
    ]
    if not primary_key_columns:
        return f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
    update_columns = [column for column in quoted_columns if column not in primary_key_columns]
    if not update_columns:
        return (
            f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(primary_key_columns)}) DO NOTHING"
        )
    return (
        f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET "
        + ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    )


def _build_insert_sqlite_sql(table_name: str, columns: list[dict[str, Any]]) -> str:
    quoted_table_name = _quote_identifier(table_name)
    quoted_columns = [_quote_identifier(str(column.get("name") or "")) for column in columns]
    placeholders = ", ".join(["?"] * len(columns))
    return f"INSERT INTO {quoted_table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"


def _sqlite_decl_to_postgres_type(sqlite_decl: str) -> str:
    normalized = str(sqlite_decl or "TEXT").strip().upper()
    if not normalized:
        return "TEXT"
    if "INT" in normalized:
        return "BIGINT"
    if any(token in normalized for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if "BOOL" in normalized:
        return "BOOLEAN"
    if "BLOB" in normalized:
        return "BYTEA"
    return "TEXT"


def _count_postgres_table_rows(cursor: Any, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(table_name)}")
    row = cursor.fetchone()
    if isinstance(row, dict):
        value = row.get("row_count")
    elif isinstance(row, (list, tuple)):
        value = row[0] if row else 0
    else:
        value = getattr(row, "row_count", None) if row is not None else 0
    return int(value or 0)


def _cursor_column_names(cursor: Any) -> list[str]:
    description = list(getattr(cursor, "description", None) or [])
    names: list[str] = []
    for column in description:
        if isinstance(column, (list, tuple)):
            names.append(str(column[0] or ""))
            continue
        names.append(str(getattr(column, "name", "") or ""))
    return names


def _fetch_cursor_rows_as_dicts(cursor: Any) -> list[dict[str, Any]]:
    column_names = _cursor_column_names(cursor)
    return [dict(row) for row in _iter_cursor_rows_as_dicts(cursor, column_names=column_names)]


def _iter_cursor_rows_as_dicts(
    cursor: Any,
    *,
    column_names: list[str] | None = None,
    batch_size: int = DEFAULT_CONTROL_PLANE_SNAPSHOT_EXPORT_BATCH_SIZE,
) -> Iterator[dict[str, Any]]:
    resolved_column_names = list(column_names or _cursor_column_names(cursor))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield _cursor_row_to_dict(row, resolved_column_names)


def _cursor_row_to_dict(row: Any, column_names: list[str]) -> dict[str, Any]:
    if isinstance(row, dict):
        return {str(key): _json_safe_snapshot_value(value) for key, value in row.items()}
    if hasattr(row, "_mapping"):
        return {
            str(key): _json_safe_snapshot_value(value)
            for key, value in dict(getattr(row, "_mapping")).items()
        }
    if hasattr(row, "_asdict"):
        return {str(key): _json_safe_snapshot_value(value) for key, value in row._asdict().items()}
    if isinstance(row, sqlite3.Row):
        return {
            str(key): _json_safe_snapshot_value(row[key])
            for key in row.keys()
            if str(key or "").strip()
        }
    if isinstance(row, (list, tuple)):
        return {
            column_names[index]: _json_safe_snapshot_value(value)
            for index, value in enumerate(row)
            if index < len(column_names) and column_names[index]
        }
    return {}


def _extract_cursor_column_values(cursor: Any, column_name: str) -> list[Any]:
    normalized_column = str(column_name or "").strip()
    values: list[Any] = []
    for row in _fetch_cursor_rows_as_dicts(cursor):
        if normalized_column in row:
            values.append(row[normalized_column])
    return values


def _json_safe_snapshot_value(value: Any) -> Any:
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(key): _json_safe_snapshot_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_snapshot_value(item) for item in value]
    return value


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier or "").replace('"', '""')
    return f'"{escaped}"'


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _path_signature(path: str | Path) -> dict[str, Any]:
    raw_path = str(path or "").strip()
    if _sqlite_connect_uses_uri(raw_path):
        return {
            "path": raw_path,
            "exists": False,
            "size_bytes": 0,
            "mtime_ns": 0,
            "uri": True,
        }
    resolved_path = Path(raw_path).expanduser()
    if not resolved_path.exists():
        return {
            "path": str(resolved_path),
            "exists": False,
            "size_bytes": 0,
            "mtime_ns": 0,
        }
    stat_result = resolved_path.stat()
    return {
        "path": str(resolved_path),
        "exists": True,
        "size_bytes": int(stat_result.st_size),
        "mtime_ns": int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))),
    }


def _write_control_plane_sync_state(
    path: Path,
    *,
    previous_state: dict[str, Any],
    summary: dict[str, Any],
    started_at: str = "",
    last_synced_fingerprint: str = "",
    preserve_last_success: bool = False,
) -> None:
    resolved_path = Path(path).expanduser()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    completed_at = _utc_now_iso()
    prior_success_summary = dict(previous_state.get("last_success_summary") or {})
    status = str(summary.get("status") or "")
    if status == "synced":
        prior_success_summary = dict(summary)
    payload = {
        "status": status,
        "runtime_dir": str(summary.get("runtime_dir") or previous_state.get("runtime_dir") or ""),
        "sqlite_path": str(summary.get("sqlite_path") or previous_state.get("sqlite_path") or ""),
        "snapshot_path": str(summary.get("snapshot_path") or previous_state.get("snapshot_path") or ""),
        "table_count_requested": int(
            summary.get("table_count_requested") or previous_state.get("table_count_requested") or 0
        ),
        "tables": list(summary.get("tables") or previous_state.get("tables") or []),
        "truncate_first": bool(summary.get("truncate_first") or previous_state.get("truncate_first")),
        "force": bool(summary.get("force") or False),
        "min_interval_seconds": float(
            summary.get("min_interval_seconds") or previous_state.get("min_interval_seconds") or 0.0
        ),
        "source_fingerprint": str(summary.get("source_fingerprint") or ""),
        "last_checked_at": completed_at,
        "last_started_at": str(started_at or summary.get("started_at") or previous_state.get("last_started_at") or ""),
        "last_completed_at": completed_at,
        "last_sync_status": status,
        "last_synced_fingerprint": str(last_synced_fingerprint or previous_state.get("last_synced_fingerprint") or ""),
        "last_error": str(summary.get("error") or ""),
        "last_summary": summary,
        "last_success_summary": prior_success_summary if preserve_last_success or status == "synced" else {},
    }
    resolved_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _timestamp_to_epoch(value: str) -> float:
    normalized = str(value or "").strip()
    if not normalized:
        return 0.0
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _import_psycopg() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised via caller
        raise RuntimeError(
            "psycopg is required for Postgres control-plane sync. Install psycopg[binary] in the active environment."
        ) from exc
    return psycopg


def _configure_postgres_connection_utf8(connection: Any) -> Any:
    try:
        return configure_control_plane_postgres_session(connection)
    except Exception:
        pass
    set_client_encoding = getattr(connection, "set_client_encoding", None)
    if callable(set_client_encoding):
        try:
            set_client_encoding("UTF8")
            return connection
        except Exception:
            pass
    cursor_factory = getattr(connection, "cursor", None)
    if callable(cursor_factory):
        try:
            with cursor_factory() as cursor:
                cursor.execute("SET client_encoding TO 'UTF8'")
            return connection
        except Exception:
            pass
    return connection


def _connect_postgres(dsn: str, *, psycopg: Any | None = None) -> Any:
    if psycopg is None:
        psycopg = _import_psycopg()
    effective_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    try:
        return configure_control_plane_postgres_session(psycopg.connect(effective_dsn, client_encoding="utf8"))
    except TypeError:
        return _configure_postgres_connection_utf8(psycopg.connect(effective_dsn))
