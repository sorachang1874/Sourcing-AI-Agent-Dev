#!/usr/bin/env python3
"""Targeted, guard-protected promotion of ONE company snapshot (registry + pointer).

Why this exists (2026-07-22): promoting a snapshot through
``rebuild-company-serving-view`` triggers a cross-snapshot canonical_merged
sweep — it discovered, registered, and re-materialized EVERY historical
snapshot of the company (30+ April dirs, ~5 min each), merged stale and even
simulate-placeholder sources into one 12k-candidate view, and after 75 minutes
had still not flipped the authoritative row. The actual promotion need is two
O(1) operations on an already-verified snapshot:

1. Upsert a snapshot-scoped organization_asset_registry row (docs-derived
   summary, same field semantics as the incumbent rows) through
   ``upsert_organization_asset_registry_with_guard`` — the stale-sequence /
   coverage-regression / promotion-evaluation guards all stay in force.
2. If (and only if) the row landed authoritative, advance
   ``latest_snapshot.json`` via the committed pointer-sync path.

Dry-run by default; ``--apply`` writes. ``--provenance-only`` registers the
snapshot with authoritative=False (no pointer move) for salvage/audit lineage.
No provider calls; local files + control-plane Postgres only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from hashlib import sha1
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.asset_reuse_planning import (  # noqa: E402
    build_organization_asset_registry_record,
    evaluate_organization_asset_registry_promotion,
    upsert_organization_asset_registry_with_guard,
)
from sourcing_agent.organization_assets import load_company_snapshot_registry_summary  # noqa: E402
from sourcing_agent.settings import load_settings  # noqa: E402
from sourcing_agent.storage import ControlPlaneStore  # noqa: E402


def _mint_new_lineage_generation(summary: dict, *, company_key: str, snapshot_id: str, asset_view: str) -> dict:
    """Deterministic new-lineage generation identity for a promoted snapshot.

    Content-derived like the product's asset-materialization keys
    (sha1 over identity + docs signature, 32 hex): reproducible and auditable,
    and a NEW key means the storage guard's same-lineage stale-sequence rule
    can never confuse it with the incumbent lineage."""
    payload_signature = sha1(
        json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:24]
    key = sha1(
        "|".join([company_key, snapshot_id, asset_view, "registry_promotion", payload_signature]).encode("utf-8")
    ).hexdigest()[:32]
    return {
        "materialization_generation_key": key,
        "materialization_generation_sequence": 1,
        "materialization_watermark": f"1:{key[:12]}",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company", required=True, help="Company key or canonical name")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--project-root", default=str(REPO_ROOT))
    parser.add_argument("--asset-view", default="canonical_merged")
    parser.add_argument(
        "--include-selected-snapshot-id",
        action="append",
        default=[],
        help=(
            "Extra snapshot ids to record in selected_snapshot_ids (e.g. the incumbent baseline the new "
            "snapshot supersedes) — keeps the coverage guard protecting against later subset replays"
        ),
    )
    parser.add_argument(
        "--provenance-only",
        action="store_true",
        help="Register authoritative=False (no promotion evaluation, no pointer move)",
    )
    parser.add_argument("--apply", action="store_true", help="Write; default is dry-run")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    runtime_dir = project_root / "runtime"

    summary_result = load_company_snapshot_registry_summary(
        runtime_dir=runtime_dir,
        target_company=args.company,
        snapshot_id=args.snapshot_id,
        asset_view=args.asset_view,
    )
    summary = dict(summary_result.get("summary") or {})
    if not summary:
        print(f"ERROR: no registry summary derivable for {args.company} {args.snapshot_id}")
        return 2
    identity = dict(summary_result.get("identity_payload") or {})
    company_key = str(summary_result.get("company_key") or identity.get("company_key") or "").strip()
    target_company = str(identity.get("canonical_name") or args.company).strip()

    if not str(summary.get("materialization_generation_key") or "").strip():
        summary.update(
            _mint_new_lineage_generation(
                summary, company_key=company_key, snapshot_id=args.snapshot_id, asset_view=args.asset_view
            )
        )

    extra_selected = [str(v).strip() for v in args.include_selected_snapshot_id if str(v).strip()]
    if extra_selected:
        selection = dict(summary.get("source_snapshot_selection") or {})
        selected = [
            *[v for v in (selection.get("selected_snapshot_ids") or summary.get("selected_snapshot_ids") or [])],
            *extra_selected,
            args.snapshot_id,
        ]
        deduped = list(dict.fromkeys(str(v).strip() for v in selected if str(v).strip()))
        selection["selected_snapshot_ids"] = deduped
        summary["source_snapshot_selection"] = selection
        summary["selected_snapshot_ids"] = deduped

    record = build_organization_asset_registry_record(
        target_company=target_company,
        company_key=company_key,
        snapshot_id=args.snapshot_id,
        asset_view=args.asset_view,
        summary=summary,
        source_path=str(summary_result.get("source_path") or ""),
        authoritative=not args.provenance_only,
    )

    store = ControlPlaneStore(load_settings(project_root).db_path)
    incumbent = dict(
        store.get_authoritative_organization_asset_registry(
            target_company=target_company, asset_view=args.asset_view
        )
        or {}
    )
    decision = evaluate_organization_asset_registry_promotion(
        existing_authoritative=incumbent or None,
        candidate_record=record,
    )

    print(json.dumps(
        {
            "mode": "provenance_only" if args.provenance_only else "promotion",
            "target_company": target_company,
            "company_key": company_key,
            "snapshot_id": args.snapshot_id,
            "candidate_count": record.get("candidate_count"),
            "profile_detail_count": record.get("profile_detail_count"),
            "generation": {
                "key": record.get("materialization_generation_key"),
                "sequence": record.get("materialization_generation_sequence"),
            },
            "selected_snapshot_ids": record.get("selected_snapshot_ids"),
            "incumbent": {
                "snapshot_id": incumbent.get("snapshot_id"),
                "candidate_count": incumbent.get("candidate_count"),
                "generation_key": incumbent.get("materialization_generation_key"),
                "selected_snapshot_ids": incumbent.get("selected_snapshot_ids"),
            },
            "promotion_guard_prediction": decision,
        },
        ensure_ascii=False,
        indent=1,
        default=str,
    ))

    if not args.apply:
        print("dry-run only; pass --apply to write")
        return 0

    if args.provenance_only:
        persisted = store.upsert_organization_asset_registry(record, authoritative=False)
    else:
        persisted = upsert_organization_asset_registry_with_guard(store=store, candidate_record=record)

    outcome = {
        "persisted_snapshot_id": persisted.get("snapshot_id"),
        "authoritative": persisted.get("authoritative"),
        "authoritative_promotion_refused": persisted.get("authoritative_promotion_refused"),
    }
    print(json.dumps({"apply_outcome": outcome}, ensure_ascii=False, indent=1, default=str))

    if not args.provenance_only:
        if not bool(persisted.get("authoritative")) or persisted.get("authoritative_promotion_refused"):
            print("PROMOTION DID NOT LAND AUTHORITATIVE — pointer NOT advanced (row kept as provenance).")
            return 1
        sync = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "sync_latest_snapshot_from_registry.py"),
                "--project-root",
                str(project_root),
                "--company",
                company_key,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        print(sync.stdout.strip())
        if sync.returncode != 0:
            print(f"POINTER SYNC FAILED (exit {sync.returncode}): {sync.stderr.strip()}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
