#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from sourcing_agent.candidate_artifacts import CandidateArtifactError
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import ControlPlaneStore


def _load_snapshot_identity(snapshot_dir: Path) -> dict[str, object]:
    for candidate in (
        snapshot_dir / "identity.json",
        snapshot_dir / "manifest.json",
        snapshot_dir / "candidate_documents.json",
    ):
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if candidate.name == "manifest.json":
            identity = dict(payload.get("company_identity") or {})
        elif candidate.name == "candidate_documents.json":
            identity = dict(dict(payload.get("snapshot") or {}).get("company_identity") or {})
        else:
            identity = dict(payload or {})
        if identity:
            return identity
    return {}


def _iter_authoritative_rows(
    store: ControlPlaneStore,
    *,
    companies: set[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    distinct_companies = [
        str(row[0] or "").strip()
        for row in store._connection.execute(
            "SELECT DISTINCT target_company FROM organization_asset_registry WHERE asset_view = 'canonical_merged'"
        ).fetchall()
        if str(row[0] or "").strip()
    ]
    for target_company in sorted(distinct_companies, key=str.lower):
        normalized_company = normalize_company_key(target_company)
        if companies and normalized_company not in companies:
            continue
        authoritative = store.get_authoritative_organization_asset_registry(
            target_company=target_company,
            asset_view="canonical_merged",
        )
        if not authoritative:
            continue
        rows.append(
            {
                "target_company": str(authoritative.get("target_company") or target_company).strip(),
                "snapshot_id": str(authoritative.get("snapshot_id") or "").strip(),
                "asset_view": str(authoritative.get("asset_view") or "canonical_merged").strip(),
                "source_path": str(authoritative.get("source_path") or "").strip(),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync runtime/company_assets/*/latest_snapshot.json from authoritative organization_asset_registry rows.",
    )
    parser.add_argument("--project-root", default=".", help="Repo root containing runtime/")
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Optional company key or canonical company name filter; repeatable",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report planned updates without writing files")
    args = parser.parse_args()

    settings = load_settings(Path(args.project_root).resolve())
    store = ControlPlaneStore(settings.db_path)
    company_filters = {
        normalize_company_key(str(item or "").strip())
        for item in list(args.company or [])
        if str(item or "").strip()
    }

    results: list[dict[str, object]] = []
    for row in _iter_authoritative_rows(store, companies=company_filters):
        target_company = str(row.get("target_company") or "").strip()
        snapshot_id = str(row.get("snapshot_id") or "").strip()
        company_key = normalize_company_key(target_company)
        if not target_company or not snapshot_id or not company_key:
            results.append(
                {
                    **row,
                    "status": "skipped_invalid_row",
                }
            )
            continue
        company_dir = settings.company_assets_dir / company_key
        snapshot_dir = company_dir / snapshot_id
        latest_path = company_dir / "latest_snapshot.json"
        if not snapshot_dir.exists():
            results.append(
                {
                    **row,
                    "status": "missing_snapshot_dir",
                    "snapshot_dir": str(snapshot_dir),
                }
            )
            continue
        identity = _load_snapshot_identity(snapshot_dir)
        if not identity:
            results.append(
                {
                    **row,
                    "status": "missing_snapshot_identity",
                    "snapshot_dir": str(snapshot_dir),
                }
            )
            continue
        payload = {
            "company_identity": identity,
            "snapshot_id": snapshot_id,
            "snapshot_dir": str(snapshot_dir),
        }
        existing_payload = {}
        if latest_path.exists():
            try:
                existing_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                existing_payload = {}
        status = "unchanged" if existing_payload == payload else "updated"
        if status == "updated" and not bool(args.dry_run):
            latest_path.parent.mkdir(parents=True, exist_ok=True)
            latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        results.append(
            {
                **row,
                "status": status if not bool(args.dry_run) else f"dry_run_{status}",
                "latest_snapshot_path": str(latest_path),
            }
        )

    print(
        json.dumps(
            {
                "status": "completed",
                "runtime_dir": str(settings.runtime_dir),
                "db_path": str(settings.db_path),
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
