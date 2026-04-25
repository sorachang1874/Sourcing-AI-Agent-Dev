#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from typing import Any

from sourcing_agent.control_plane_live_postgres import _normalize_postgres_payload
from sourcing_agent.control_plane_postgres import (
    ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
    ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
    ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
    DEFAULT_CONTROL_PLANE_TABLES,
    _build_create_table_sql,
    _build_upsert_sql,
    _connect_postgres,
    _postgres_table_columns,
    _postgres_table_exists,
    _quote_identifier,
    count_acquisition_shard_registry_rows,
    ensure_acquisition_shard_registry_split_schema,
    read_acquisition_shard_registry_snapshot,
    upsert_acquisition_shard_registry_rows,
)


def _row_to_dict(row: Any, column_names: list[str]) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "_mapping"):
        return dict(getattr(row, "_mapping"))
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if isinstance(row, (list, tuple)):
        return {
            column_names[index]: row[index]
            for index in range(min(len(column_names), len(row)))
            if column_names[index]
        }
    return {}


def migrate_table(
    *,
    old_connection: Any,
    new_connection: Any,
    table_name: str,
    batch_size: int,
    truncate_first: bool,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    if table_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE:
        with old_connection.cursor() as old_cursor:
            exported = read_acquisition_shard_registry_snapshot(old_cursor)
            if exported is None:
                return {"table": table_name, "status": "missing_source", "row_count": 0}
            rows = [dict(item) for item in list(exported.get("rows") or []) if isinstance(item, dict)]
            columns = [dict(item) for item in list(exported.get("columns") or []) if isinstance(item, dict)]
        with new_connection.cursor() as new_cursor:
            ensure_acquisition_shard_registry_split_schema(new_cursor)
            if truncate_first:
                for split_table_name in (
                    ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
                    ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
                ):
                    new_cursor.execute(f"TRUNCATE TABLE {_quote_identifier(split_table_name)}")
            upsert_acquisition_shard_registry_rows(new_cursor, rows, ensure_schema=False)
        new_connection.commit()
        with new_connection.cursor() as new_cursor:
            migrated_count = count_acquisition_shard_registry_rows(new_cursor)
        return {
            "table": table_name,
            "status": "migrated",
            "row_count": migrated_count,
            "column_count": len(columns),
            "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
        }
    with old_connection.cursor() as old_cursor:
        if not _postgres_table_exists(old_cursor, table_name):
            return {"table": table_name, "status": "missing_source", "row_count": 0}
        columns = _postgres_table_columns(old_cursor, table_name)
        if not columns:
            return {"table": table_name, "status": "missing_columns", "row_count": 0}
    column_names = [str(column.get("name") or "").strip() for column in columns if str(column.get("name") or "").strip()]
    insert_sql = _build_upsert_sql(table_name, columns)
    quoted_table_name = _quote_identifier(table_name)

    with new_connection.cursor() as new_cursor:
        new_cursor.execute(_build_create_table_sql(table_name, columns))
        if truncate_first:
            new_cursor.execute(f"TRUNCATE TABLE {quoted_table_name}")
    new_connection.commit()

    migrated_count = 0
    with old_connection.cursor(name=f"migrate_{table_name}") as read_cursor:
        read_cursor.itersize = max(1, int(batch_size))
        read_cursor.execute(f"SELECT * FROM {quoted_table_name}")
        while True:
            rows = read_cursor.fetchmany(max(1, int(batch_size)))
            if not rows:
                break
            payloads: list[tuple[Any, ...]] = []
            for row in rows:
                mapped = _row_to_dict(row, column_names)
                normalized_row = {
                    column_name: _normalize_postgres_payload(mapped.get(column_name))
                    for column_name in column_names
                }
                payloads.append(tuple(normalized_row.get(column_name) for column_name in column_names))
            with new_connection.cursor() as new_cursor:
                new_cursor.executemany(insert_sql, payloads)
            new_connection.commit()
            migrated_count += len(payloads)
            print(f"{table_name}\t{migrated_count}", flush=True)

    return {
        "table": table_name,
        "status": "migrated",
        "row_count": migrated_count,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate control-plane tables between Postgres DSNs with text normalization.")
    parser.add_argument("--old-dsn", required=True, help="Source Postgres DSN")
    parser.add_argument("--new-dsn", required=True, help="Destination Postgres DSN")
    parser.add_argument("--table", action="append", default=[], help="Optional table override; repeatable")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per batch")
    parser.add_argument("--no-truncate", action="store_true", help="Do not truncate destination tables before import")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tables = [str(item).strip() for item in list(args.table or DEFAULT_CONTROL_PLANE_TABLES) if str(item).strip()]
    with _connect_postgres(args.old_dsn) as old_connection, _connect_postgres(args.new_dsn) as new_connection:
        for table_name in tables:
            summary = migrate_table(
                old_connection=old_connection,
                new_connection=new_connection,
                table_name=table_name,
                batch_size=max(1, int(args.batch_size or 200)),
                truncate_first=not bool(args.no_truncate),
            )
            print(summary, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
