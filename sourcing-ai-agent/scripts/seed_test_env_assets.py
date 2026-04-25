#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sourcing_agent.asset_reuse_planning import backfill_organization_asset_registry_for_company
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn, resolve_default_control_plane_db_path
from sourcing_agent.storage import SQLiteStore


_ACQUISITION_SHARD_REGISTRY_COLUMNS = (
    "shard_key",
    "target_company",
    "company_key",
    "snapshot_id",
    "asset_view",
    "lane",
    "status",
    "employment_scope",
    "strategy_type",
    "shard_id",
    "shard_title",
    "search_query",
    "query_signature",
    "company_scope_json",
    "locations_json",
    "function_ids_json",
    "result_count",
    "estimated_total_count",
    "provider_cap_hit",
    "source_path",
    "source_job_id",
    "metadata_json",
    "first_seen_at",
    "last_completed_at",
    "materialization_generation_key",
    "materialization_generation_sequence",
    "materialization_watermark",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed an isolated test runtime with authoritative company snapshots from the default runtime.",
    )
    parser.add_argument(
        "--source-runtime-dir",
        default="runtime",
        help="Source runtime root that already contains company_assets and, when PG is disabled, a control-plane sqlite db.",
    )
    parser.add_argument(
        "--target-runtime-dir",
        default="runtime/test_env",
        help="Target isolated runtime root that should receive linked/copied company assets.",
    )
    parser.add_argument(
        "--source-db-path",
        default="",
        help="Optional source sqlite override. Defaults to <source-runtime-dir>/sourcing_agent.db.",
    )
    parser.add_argument(
        "--target-db-path",
        default="",
        help="Optional target sqlite/shadow override. Defaults to the runtime's resolved control-plane db path.",
    )
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company key or canonical company name; repeatable.",
    )
    parser.add_argument(
        "--asset-view",
        choices=["canonical_merged", "strict_roster_only"],
        default="canonical_merged",
        help="Asset view to backfill into the isolated runtime registry.",
    )
    parser.add_argument(
        "--link-mode",
        choices=["symlink", "copy"],
        default="symlink",
        help="Use lightweight symlinks by default; copy only when strict isolation is required.",
    )
    return parser.parse_args()


def _load_authoritative_snapshot_row(
    *,
    source_runtime_dir: Path,
    source_db_path: Path,
    company: str,
) -> dict[str, Any]:
    normalized_key = normalize_company_key(company)
    postgres_dsn = resolve_control_plane_postgres_dsn(source_runtime_dir)
    if postgres_dsn:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - depends on env packaging
            raise SystemExit(
                "Postgres control plane is configured but psycopg is unavailable for test-env seeding."
            ) from exc
        with psycopg.connect(postgres_dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      target_company,
                      company_key,
                      snapshot_id,
                      selected_snapshot_ids_json,
                      source_snapshot_selection_json
                    FROM organization_asset_registry
                    WHERE authoritative = 1
                      AND asset_view = 'canonical_merged'
                      AND (
                        lower(target_company) = lower(%s)
                        OR company_key = %s
                      )
                    ORDER BY updated_at DESC, registry_id DESC
                    LIMIT 1
                    """,
                    (str(company or "").strip(), normalized_key),
                )
                row = cursor.fetchone()
                if row is not None:
                    columns = [str(item[0] or "").strip() for item in list(cursor.description or [])]
                    payload = {column: value for column, value in zip(columns, row)}
                    payload["company_key"] = str(payload.get("company_key") or normalized_key)
                    payload["target_company"] = str(payload.get("target_company") or company).strip()
                    payload["snapshot_id"] = str(payload.get("snapshot_id") or "").strip()
                    if not payload["snapshot_id"]:
                        raise SystemExit(
                            f"Authoritative organization_asset_registry row for `{company}` is missing snapshot_id."
                        )
                    return payload
        raise SystemExit(
            f"No authoritative organization_asset_registry row found for `{company}` in Postgres control plane."
        )
    if not source_db_path.exists():
        raise SystemExit(f"Source sqlite database not found: {source_db_path}")
    with sqlite3.connect(str(source_db_path)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT
              target_company,
              company_key,
              snapshot_id,
              selected_snapshot_ids_json,
              source_snapshot_selection_json
            FROM organization_asset_registry
            WHERE authoritative = 1
              AND asset_view = 'canonical_merged'
              AND (
                lower(target_company) = lower(?)
                OR company_key = ?
              )
            ORDER BY updated_at DESC, registry_id DESC
            LIMIT 1
            """,
            (str(company or "").strip(), normalized_key),
        ).fetchone()
    if row is None:
        raise SystemExit(f"No authoritative organization_asset_registry row found for `{company}` in {source_db_path}.")
    payload = dict(row)
    payload["company_key"] = str(payload.get("company_key") or normalized_key)
    payload["target_company"] = str(payload.get("target_company") or company).strip()
    payload["snapshot_id"] = str(payload.get("snapshot_id") or "").strip()
    if not payload["snapshot_id"]:
        raise SystemExit(f"Authoritative organization_asset_registry row for `{company}` is missing snapshot_id.")
    return payload


def _decode_json_payload(value: Any, *, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _selected_snapshot_ids(row: dict[str, Any]) -> list[str]:
    decoded = _decode_json_payload(row.get("selected_snapshot_ids_json"), default=[])
    snapshot_ids = [str(item or "").strip() for item in list(decoded or []) if str(item or "").strip()]
    snapshot_id = str(row.get("snapshot_id") or "").strip()
    if snapshot_id and snapshot_id not in snapshot_ids:
        snapshot_ids.append(snapshot_id)
    return snapshot_ids


def _query_source_acquisition_shard_rows(
    *,
    source_runtime_dir: Path,
    source_db_path: Path,
    target_company: str,
    company_key: str,
    snapshot_ids: list[str],
) -> list[dict[str, Any]]:
    normalized_snapshot_ids = [str(item or "").strip() for item in list(snapshot_ids or []) if str(item or "").strip()]
    if not normalized_snapshot_ids:
        return []
    column_sql = ", ".join(_ACQUISITION_SHARD_REGISTRY_COLUMNS)
    postgres_dsn = resolve_control_plane_postgres_dsn(source_runtime_dir)
    if postgres_dsn:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - depends on env packaging
            raise SystemExit(
                "Postgres control plane is configured but psycopg is unavailable for test-env shard import."
            ) from exc
        with psycopg.connect(postgres_dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {column_sql}
                    FROM acquisition_shard_registry
                    WHERE (
                      lower(target_company) = lower(%s)
                      OR company_key = %s
                    )
                      AND snapshot_id = ANY(%s)
                    ORDER BY updated_at DESC, created_at DESC, shard_key ASC
                    """,
                    (target_company, company_key, normalized_snapshot_ids),
                )
                columns = [str(item[0] or "").strip() for item in list(cursor.description or [])]
                rows = [
                    {column: value for column, value in zip(columns, raw_row)}
                    for raw_row in list(cursor.fetchall() or [])
                ]
    else:
        if not source_db_path.exists():
            return []
        placeholder_sql = ", ".join("?" for _ in normalized_snapshot_ids)
        with sqlite3.connect(str(source_db_path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = [
                dict(row)
                for row in connection.execute(
                    f"""
                    SELECT {column_sql}
                    FROM acquisition_shard_registry
                    WHERE (
                      lower(target_company) = lower(?)
                      OR company_key = ?
                    )
                      AND snapshot_id IN ({placeholder_sql})
                    ORDER BY updated_at DESC, created_at DESC, shard_key ASC
                    """,
                    (target_company, company_key, *normalized_snapshot_ids),
                ).fetchall()
            ]
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        shard_key = str(row.get("shard_key") or "").strip()
        if not shard_key or shard_key in deduped:
            continue
        deduped[shard_key] = row
    return list(deduped.values())


def _seed_acquisition_shard_rows(
    *,
    store: SQLiteStore,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    imported_count = 0
    snapshot_ids: set[str] = set()
    lanes: set[str] = set()
    employment_scopes: set[str] = set()
    for row in list(rows or []):
        payload = dict(row or {})
        payload["company_scope"] = _decode_json_payload(payload.pop("company_scope_json", "[]"), default=[])
        payload["locations"] = _decode_json_payload(payload.pop("locations_json", "[]"), default=[])
        payload["function_ids"] = _decode_json_payload(payload.pop("function_ids_json", "[]"), default=[])
        payload["metadata"] = _decode_json_payload(payload.pop("metadata_json", "{}"), default={})
        persisted = store.upsert_acquisition_shard_registry(payload)
        if not persisted:
            continue
        imported_count += 1
        snapshot_id = str(persisted.get("snapshot_id") or payload.get("snapshot_id") or "").strip()
        if snapshot_id:
            snapshot_ids.add(snapshot_id)
        lane = str(persisted.get("lane") or payload.get("lane") or "").strip()
        if lane:
            lanes.add(lane)
        employment_scope = str(persisted.get("employment_scope") or payload.get("employment_scope") or "").strip()
        if employment_scope:
            employment_scopes.add(employment_scope)
    return {
        "imported_row_count": imported_count,
        "snapshot_id_count": len(snapshot_ids),
        "lane_count": len(lanes),
        "employment_scope_count": len(employment_scopes),
        "snapshot_ids": sorted(snapshot_ids),
        "lanes": sorted(lanes),
        "employment_scopes": sorted(employment_scopes),
    }


@contextmanager
def _disabled_live_postgres() -> Any:
    original = os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE")
    os.environ["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = "disabled"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE", None)
        else:
            os.environ["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = original


def _build_isolated_target_store(target_db_path: Path) -> SQLiteStore:
    with _disabled_live_postgres():
        return SQLiteStore(str(target_db_path))


def _load_company_identity(
    *,
    source_company_dir: Path,
    source_snapshot_dir: Path,
    target_company: str,
    company_key: str,
) -> dict[str, Any]:
    latest_snapshot_path = source_company_dir / "latest_snapshot.json"
    if latest_snapshot_path.exists():
        try:
            latest_payload = json.loads(latest_snapshot_path.read_text(encoding="utf-8"))
            identity = dict(latest_payload.get("company_identity") or {})
            if identity:
                return identity
        except Exception:
            pass
    identity_path = source_snapshot_dir / "identity.json"
    if identity_path.exists():
        try:
            return json.loads(identity_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "requested_name": target_company,
        "canonical_name": target_company,
        "company_key": company_key,
    }


def _materialize_snapshot_link(
    *,
    source_snapshot_dir: Path,
    target_snapshot_dir: Path,
    link_mode: str,
) -> str:
    if target_snapshot_dir.exists():
        if target_snapshot_dir.is_symlink() and target_snapshot_dir.resolve() == source_snapshot_dir.resolve():
            return "reused"
        raise SystemExit(
            f"Target snapshot path already exists and is not the expected linked snapshot: {target_snapshot_dir}"
        )
    target_snapshot_dir.parent.mkdir(parents=True, exist_ok=True)
    if link_mode == "copy":
        shutil.copytree(source_snapshot_dir, target_snapshot_dir)
        return "copied"
    target_snapshot_dir.symlink_to(source_snapshot_dir.resolve(), target_is_directory=True)
    return "linked"


def main() -> int:
    args = parse_args()
    companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
    if not companies:
        raise SystemExit("At least one --company is required.")

    source_runtime_dir = Path(args.source_runtime_dir).expanduser().resolve()
    target_runtime_dir = Path(args.target_runtime_dir).expanduser().resolve()
    source_db_path = (
        Path(args.source_db_path).expanduser().resolve()
        if str(args.source_db_path or "").strip()
        else (source_runtime_dir / "sourcing_agent.db").resolve()
    )
    target_db_path = (
        Path(args.target_db_path).expanduser().resolve()
        if str(args.target_db_path or "").strip()
        else resolve_default_control_plane_db_path(target_runtime_dir, base_dir=target_runtime_dir).resolve()
    )

    if not source_runtime_dir.exists():
        raise SystemExit(f"Source runtime dir not found: {source_runtime_dir}")

    target_runtime_dir.mkdir(parents=True, exist_ok=True)
    (target_runtime_dir / "company_assets").mkdir(parents=True, exist_ok=True)
    store = _build_isolated_target_store(target_db_path)

    results: list[dict[str, Any]] = []
    for company in companies:
        row = _load_authoritative_snapshot_row(
            source_runtime_dir=source_runtime_dir,
            source_db_path=source_db_path,
            company=company,
        )
        target_company = str(row.get("target_company") or company).strip()
        company_key = str(row.get("company_key") or normalize_company_key(target_company)).strip()
        snapshot_id = str(row.get("snapshot_id") or "").strip()
        source_company_dir = source_runtime_dir / "company_assets" / company_key
        source_snapshot_dir = source_company_dir / snapshot_id
        if not source_snapshot_dir.exists():
            raise SystemExit(f"Source snapshot directory not found for `{target_company}`: {source_snapshot_dir}")

        target_company_dir = target_runtime_dir / "company_assets" / company_key
        target_snapshot_dir = target_company_dir / snapshot_id
        link_status = _materialize_snapshot_link(
            source_snapshot_dir=source_snapshot_dir,
            target_snapshot_dir=target_snapshot_dir,
            link_mode=str(args.link_mode or "symlink"),
        )
        company_identity = _load_company_identity(
            source_company_dir=source_company_dir,
            source_snapshot_dir=source_snapshot_dir,
            target_company=target_company,
            company_key=company_key,
        )
        target_company_dir.mkdir(parents=True, exist_ok=True)
        (target_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "company_identity": company_identity,
                    "snapshot_id": snapshot_id,
                    "snapshot_dir": str(target_snapshot_dir),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        selected_snapshot_ids = _selected_snapshot_ids(row)
        shard_rows = _query_source_acquisition_shard_rows(
            source_runtime_dir=source_runtime_dir,
            source_db_path=source_db_path,
            target_company=target_company,
            company_key=company_key,
            snapshot_ids=selected_snapshot_ids,
        )
        shard_seed_result = _seed_acquisition_shard_rows(store=store, rows=shard_rows)
        backfill_result = backfill_organization_asset_registry_for_company(
            runtime_dir=target_runtime_dir,
            store=store,
            target_company=target_company,
            asset_view=str(args.asset_view or "canonical_merged"),
        )
        results.append(
            {
                "target_company": target_company,
                "company_key": company_key,
                "snapshot_id": snapshot_id,
                "source_snapshot_dir": str(source_snapshot_dir),
                "target_snapshot_dir": str(target_snapshot_dir),
                "materialization": link_status,
                "selected_snapshot_ids": selected_snapshot_ids,
                "acquisition_shard_seed": shard_seed_result,
                "registry_backfill": backfill_result,
            }
        )

    print(
        json.dumps(
            {
                "source_runtime_dir": str(source_runtime_dir),
                "source_db_path": str(source_db_path),
                "target_runtime_dir": str(target_runtime_dir),
                "target_db_path": str(target_db_path),
                "asset_view": str(args.asset_view or "canonical_merged"),
                "link_mode": str(args.link_mode or "symlink"),
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
