"""Migration-only readers for retired target-candidate Public Web tables.

Normal CRM/Public Web code must use `crm_public_web_*` storage. These helpers
exist only for W7e deletion audits and reviewed historical migration paths.
They fail closed to empty lists when legacy tables no longer exist.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LEGACY_PUBLIC_WEB_STORAGE_PHASE = "W7e_legacy_target_public_web_migration_only"
LEGACY_TARGET_PUBLIC_WEB_TABLES = (
    "target_candidate_public_web_batches",
    "target_candidate_public_web_runs",
    "target_candidate_public_web_promotions",
)
LEGACY_PUBLIC_WEB_ARCHIVE_CONTRACT_VERSION = "legacy_public_web_archive_v1"
LEGACY_PUBLIC_WEB_DROP_CONTRACT_VERSION = "legacy_public_web_drop_v1"


def seed_legacy_target_public_web_batch(store: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Write a retired target-candidate Public Web batch for migration tests.

    Normal runtime code must not call this helper. It exists only to seed or
    preserve historical rows before the legacy tables are physically deleted.
    """

    with _migration_write_context(store):
        return store.upsert_target_candidate_public_web_batch(_migration_payload(payload))


def seed_legacy_target_public_web_run(store: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Write a retired target-candidate Public Web run for migration tests."""

    with _migration_write_context(store):
        return store.upsert_target_candidate_public_web_run(_migration_payload(payload))


def update_legacy_target_public_web_run(
    store: Any,
    run_id: str,
    patch: dict[str, Any],
) -> dict[str, Any] | None:
    """Update a retired target-candidate Public Web run for migration tests."""

    with _migration_write_context(store):
        return store.update_target_candidate_public_web_run(run_id, dict(patch or {}))


def seed_legacy_target_public_web_promotion(store: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Write a retired target-candidate Public Web promotion for migration tests."""

    with _migration_write_context(store):
        return store.upsert_target_candidate_public_web_promotion(_migration_payload(payload))


def list_legacy_target_public_web_batches(
    store: Any,
    *,
    status: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_status = str(status or "").strip().lower()
    return _list_rows(
        store,
        table_name="target_candidate_public_web_batches",
        row_builder_name="_target_candidate_public_web_batch_from_row",
        where_columns={"status": normalized_status} if normalized_status else {},
        order_by_sql="updated_at DESC, created_at DESC, batch_id DESC",
        limit=limit,
    )


def list_legacy_target_public_web_runs(
    store: Any,
    *,
    batch_id: str = "",
    record_id: str = "",
    status: str = "",
    limit: int = 500,
) -> list[dict[str, Any]]:
    where_columns: dict[str, str] = {}
    for column_name, value in (
        ("batch_id", batch_id),
        ("record_id", record_id),
        ("status", str(status or "").strip().lower()),
    ):
        normalized = str(value or "").strip()
        if normalized:
            where_columns[column_name] = normalized
    return _list_rows(
        store,
        table_name="target_candidate_public_web_runs",
        row_builder_name="_target_candidate_public_web_run_from_row",
        where_columns=where_columns,
        order_by_sql="updated_at DESC, created_at DESC, run_id DESC",
        limit=limit,
    )


def list_legacy_target_public_web_promotions(
    store: Any,
    *,
    record_id: str = "",
    signal_id: str = "",
    run_id: str = "",
    action: str = "",
    limit: int = 500,
) -> list[dict[str, Any]]:
    where_columns: dict[str, str] = {}
    for column_name, value in (
        ("record_id", record_id),
        ("signal_id", signal_id),
        ("run_id", run_id),
        ("action", str(action or "").strip().lower()),
    ):
        normalized = str(value or "").strip()
        if normalized:
            where_columns[column_name] = normalized
    return _list_rows(
        store,
        table_name="target_candidate_public_web_promotions",
        row_builder_name="_target_candidate_public_web_promotion_from_row",
        where_columns=where_columns,
        order_by_sql="updated_at DESC, created_at DESC, promotion_id DESC",
        limit=limit,
    )


def archive_legacy_target_public_web_tables(
    store: Any,
    output_path: str | Path,
    *,
    row_limit: int = 10000,
    sample_limit: int = 25,
) -> dict[str, Any]:
    """Write a cold archive manifest for retired target-candidate Public Web rows."""

    normalized_output_path = Path(output_path).expanduser()
    normalized_output_path.parent.mkdir(parents=True, exist_ok=True)
    archive = build_legacy_target_public_web_archive_manifest(
        store,
        row_limit=row_limit,
        sample_limit=sample_limit,
        include_rows=True,
    )
    normalized_output_path.write_text(
        json.dumps(archive, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "status": "archived",
        "contract_version": LEGACY_PUBLIC_WEB_ARCHIVE_CONTRACT_VERSION,
        "archive_path": str(normalized_output_path),
        "row_count": int(archive.get("row_count") or 0),
        "limited": bool(archive.get("limited")),
        "tables": {
            table_name: {
                "row_count": int(dict(table_payload).get("row_count") or 0),
                "limited": bool(dict(table_payload).get("limited")),
            }
            for table_name, table_payload in dict(archive.get("tables") or {}).items()
            if isinstance(table_payload, dict)
        },
    }


def build_legacy_target_public_web_archive_manifest(
    store: Any,
    *,
    row_limit: int = 10000,
    sample_limit: int = 25,
    include_rows: bool = False,
) -> dict[str, Any]:
    """Return a cold archive manifest for retired target-candidate Public Web rows."""

    normalized_row_limit = max(1, int(row_limit or 10000))
    normalized_sample_limit = max(1, int(sample_limit or 25))
    table_rows = {
        "target_candidate_public_web_batches": list_legacy_target_public_web_batches(
            store,
            limit=normalized_row_limit,
        ),
        "target_candidate_public_web_runs": list_legacy_target_public_web_runs(
            store,
            limit=normalized_row_limit,
        ),
        "target_candidate_public_web_promotions": list_legacy_target_public_web_promotions(
            store,
            limit=normalized_row_limit,
        ),
    }
    tables: dict[str, Any] = {}
    total_rows = 0
    limited = False
    for table_name in LEGACY_TARGET_PUBLIC_WEB_TABLES:
        rows = [dict(row) for row in list(table_rows.get(table_name) or []) if isinstance(row, dict)]
        row_count = len(rows)
        table_limited = row_count >= normalized_row_limit
        total_rows += row_count
        limited = limited or table_limited
        payload: dict[str, Any] = {
            "row_count": row_count,
            "limited": table_limited,
            "sample": rows[:normalized_sample_limit],
        }
        if include_rows:
            payload["rows"] = rows
        tables[table_name] = payload
    return {
        "contract_version": LEGACY_PUBLIC_WEB_ARCHIVE_CONTRACT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "migration_phase": LEGACY_PUBLIC_WEB_STORAGE_PHASE,
        "row_limit": normalized_row_limit,
        "sample_limit": normalized_sample_limit,
        "row_count": total_rows,
        "limited": limited,
        "tables": tables,
    }


def drop_legacy_target_public_web_tables(
    store: Any,
    *,
    archive_path: str | Path | None = None,
    row_limit: int = 10000,
    sample_limit: int = 25,
    allow_non_empty_without_archive: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    """Drop retired target-candidate Public Web tables after optional cold archive.

    Non-empty legacy tables require an archive path unless the caller explicitly
    opts out. This makes physical deletion deliberate while keeping the normal
    runtime free of legacy table bootstrap.
    """

    pre_drop = build_legacy_target_public_web_archive_manifest(
        store,
        row_limit=row_limit,
        sample_limit=sample_limit,
        include_rows=False,
    )
    legacy_row_count = int(pre_drop.get("row_count") or 0)
    if bool(pre_drop.get("limited")):
        return {
            "status": "blocked",
            "contract_version": LEGACY_PUBLIC_WEB_DROP_CONTRACT_VERSION,
            "reason": "legacy_public_web_archive_limited",
            "pre_drop": pre_drop,
        }
    archive_result: dict[str, Any] = {}
    if archive_path:
        archive_result = archive_legacy_target_public_web_tables(
            store,
            archive_path,
            row_limit=row_limit,
            sample_limit=sample_limit,
        )
        if bool(archive_result.get("limited")):
            return {
                "status": "blocked",
                "contract_version": LEGACY_PUBLIC_WEB_DROP_CONTRACT_VERSION,
                "reason": "legacy_public_web_archive_limited",
                "archive": archive_result,
                "pre_drop": pre_drop,
            }
    elif legacy_row_count > 0 and not allow_non_empty_without_archive:
        return {
            "status": "blocked",
            "contract_version": LEGACY_PUBLIC_WEB_DROP_CONTRACT_VERSION,
            "reason": "archive_required_before_non_empty_legacy_public_web_drop",
            "pre_drop": pre_drop,
        }
    postgres_drop = _drop_postgres_legacy_tables(store, reason=reason)
    return {
        "status": "dropped",
        "contract_version": LEGACY_PUBLIC_WEB_DROP_CONTRACT_VERSION,
        "dropped_at": datetime.now(timezone.utc).isoformat(),
        "reason": str(reason or "").strip() or LEGACY_PUBLIC_WEB_STORAGE_PHASE,
        "archive": archive_result,
        "pre_drop": pre_drop,
        "postgres": postgres_drop,
    }


def _list_rows(
    store: Any,
    *,
    table_name: str,
    row_builder_name: str,
    where_columns: dict[str, str] | None = None,
    order_by_sql: str,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit or 1))
    where_columns = dict(where_columns or {})
    row_builder = getattr(store, row_builder_name, None)
    if row_builder is None:
        row_builder = lambda row: dict(row or {})

    return _list_postgres_rows(
        store,
        table_name=table_name,
        row_builder=row_builder,
        where_columns=where_columns,
        order_by_sql=order_by_sql,
        limit=normalized_limit,
    )


def _list_postgres_rows(
    store: Any,
    *,
    table_name: str,
    row_builder: Any,
    where_columns: dict[str, str],
    order_by_sql: str,
    limit: int,
) -> list[dict[str, Any]]:
    select_rows = getattr(store, "_select_control_plane_rows", None)
    if select_rows is None:
        return []
    where_sql = " AND ".join(f"{column_name} = %s" for column_name in where_columns)
    context_factory = getattr(store, "legacy_target_public_web_migration_read_context", None)
    context = (
        context_factory(LEGACY_PUBLIC_WEB_STORAGE_PHASE)
        if callable(context_factory)
        else _NullMigrationWriteContext()
    )
    try:
        with context:
            rows = select_rows(
                table_name,
                row_builder=row_builder,
                where_sql=where_sql,
                params=list(where_columns.values()),
                order_by_sql=order_by_sql,
                limit=limit,
            )
    except Exception:
        return []
    return [dict(row) for row in list(rows or []) if isinstance(row, dict)]


def _drop_postgres_legacy_tables(store: Any, *, reason: str = "") -> dict[str, Any]:
    adapter = getattr(store, "_control_plane_postgres", None)
    if adapter is None or not bool(getattr(adapter, "enabled", False)):
        return {"status": "skipped", "reason": "postgres_adapter_disabled", "tables": []}
    connect = getattr(adapter, "_connect", None)
    context_factory = getattr(adapter, "legacy_target_public_web_migration_table_context", None)
    if not callable(connect):
        return {"status": "skipped", "reason": "postgres_connect_unavailable", "tables": []}
    context = (
        context_factory(str(reason or "").strip() or LEGACY_PUBLIC_WEB_STORAGE_PHASE)
        if callable(context_factory)
        else _NullMigrationWriteContext()
    )
    dropped: list[str] = []
    with context:
        with connect() as connection:
            with connection.cursor() as cursor:
                for table_name in LEGACY_TARGET_PUBLIC_WEB_TABLES:
                    cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")
                    dropped.append(table_name)
            connection.commit()
    return {"status": "dropped", "tables": dropped, "table_count": len(dropped)}


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _migration_payload(payload: dict[str, Any]) -> dict[str, Any]:
    copied = dict(payload or {})
    metadata = dict(copied.get("metadata") or {})
    metadata.setdefault("migration_only", True)
    metadata.setdefault("migration_phase", LEGACY_PUBLIC_WEB_STORAGE_PHASE)
    copied["metadata"] = metadata
    return copied


def _migration_write_context(store: Any):
    context_factory = getattr(store, "legacy_target_public_web_migration_write_context", None)
    if context_factory is not None:
        return context_factory(LEGACY_PUBLIC_WEB_STORAGE_PHASE)
    return _NullMigrationWriteContext()


class _NullMigrationWriteContext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False
