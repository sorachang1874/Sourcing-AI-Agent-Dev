#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if sys.version_info < (3, 10):
    raise SystemExit(
        "backfill_acquisition_shard_query_families.py requires Python 3.10+. "
        "Use .venv/bin/python or .venv-tests/bin/python."
    )
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sourcing_agent.asset_reuse_planning import (
    ensure_acquisition_shard_registry_for_snapshot,
    refresh_acquisition_shard_registry_query_family_metadata,
)
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import ControlPlaneStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill explicit query-family metadata onto acquisition_shard_registry rows and, when requested, "
            "rebuild missing rows from on-disk search-seed snapshot summaries first."
        )
    )
    parser.add_argument("--project-root", default=str(REPO_ROOT), help="Repo root containing runtime/ and src/.")
    parser.add_argument(
        "--runtime-dir",
        default="",
        help="Optional runtime root override. Defaults to the runtime resolved from settings/env.",
    )
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Target company filter; repeatable. Required for targeted backfills.",
    )
    parser.add_argument(
        "--snapshot-id",
        action="append",
        default=[],
        help="Optional snapshot id filter; repeatable.",
    )
    parser.add_argument(
        "--rebuild-from-assets",
        action="store_true",
        help="Re-run snapshot->registry ingestion before metadata backfill so missing rows are recovered from assets.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report planned updates without writing rows.")
    parser.add_argument("--limit", type=int, default=5000, help="Per-company row fetch limit.")
    return parser.parse_args()


def _normalized_values(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        normalized = str(item or "").strip()
        if not normalized:
            continue
        signature = normalized.lower()
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(normalized)
    return deduped


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()
    if args.runtime_dir:
        os.environ["SOURCING_RUNTIME_DIR"] = str(Path(args.runtime_dir).expanduser())
    settings = load_settings(project_root)
    store = ControlPlaneStore(settings.db_path)

    companies = _normalized_values(list(args.company or []))
    snapshot_ids = _normalized_values(list(args.snapshot_id or []))
    if not companies:
        raise SystemExit("At least one --company is required.")
    if bool(args.rebuild_from_assets) and not snapshot_ids:
        raise SystemExit("--rebuild-from-assets requires at least one --snapshot-id.")

    rebuild_results: list[dict[str, Any]] = []
    if bool(args.rebuild_from_assets):
        for company in companies:
            for snapshot_id in snapshot_ids:
                rebuild_results.append(
                    {
                        "target_company": company,
                        "snapshot_id": snapshot_id,
                        **ensure_acquisition_shard_registry_for_snapshot(
                            runtime_dir=settings.runtime_dir,
                            store=store,
                            target_company=company,
                            snapshot_id=snapshot_id,
                        ),
                    }
                )

    seen_shard_keys: set[str] = set()
    touched_rows: list[dict[str, Any]] = []
    changed_rows = 0
    family_counts: dict[str, int] = {}
    for company in companies:
        rows = store.list_acquisition_shard_registry(
            target_company=company,
            snapshot_ids=snapshot_ids or None,
            limit=max(1, int(args.limit or 0)),
        )
        for row in rows:
            shard_key = str(row.get("shard_key") or "").strip()
            if not shard_key or shard_key in seen_shard_keys:
                continue
            seen_shard_keys.add(shard_key)
            refreshed, changed = refresh_acquisition_shard_registry_query_family_metadata(row)
            family_payload = dict(dict(refreshed.get("metadata") or {}).get("query_family") or {})
            canonical_family = str(family_payload.get("canonical_label") or "").strip()
            if canonical_family:
                family_counts[canonical_family] = family_counts.get(canonical_family, 0) + 1
            if changed:
                changed_rows += 1
                if not bool(args.dry_run):
                    store.upsert_acquisition_shard_registry(refreshed)
            touched_rows.append(
                {
                    "shard_key": shard_key,
                    "target_company": str(refreshed.get("target_company") or company),
                    "snapshot_id": str(refreshed.get("snapshot_id") or ""),
                    "employment_scope": str(refreshed.get("employment_scope") or ""),
                    "search_query": str(refreshed.get("search_query") or ""),
                    "canonical_query_family": canonical_family,
                    "query_family_signatures": list(family_payload.get("signatures") or []),
                    "changed": changed,
                }
            )

    print(
        json.dumps(
            {
                "status": "completed",
                "project_root": str(project_root),
                "runtime_dir": str(settings.runtime_dir),
                "db_path": str(settings.db_path),
                "rebuild_from_assets": bool(args.rebuild_from_assets),
                "dry_run": bool(args.dry_run),
                "rebuild_results": rebuild_results,
                "row_count": len(touched_rows),
                "changed_rows": changed_rows,
                "family_counts": dict(sorted(family_counts.items())),
                "rows": touched_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
