#!/usr/bin/env python3
"""Reconcile candidate_documents profile_fetched flags with on-disk envelopes.

Salvage merges (R-033, 2026-07-22) wrote profile envelopes into authoritative
snapshots' harvest_profiles/ without flipping the candidate documents'
`profile_fetched` flags or stamping `profile_mode`. The flag is the
paid-refetch dedupe authority (HARVESTAPI_PLAYBOOK: driver dedupe uses the
document flag, never url hashes), so an understated flag risks paying to
re-fetch profiles that already exist locally.

Matching is evidence-only: a row is reconciled when its envelope exists in
the snapshot's harvest_profiles/ keyed by candidate_id or
sha1(linkedin_url)[:16] (the two schemes used by the salvage merge). Mode is
stamped as full_details_no_email only when the envelope item carries no
emails; rows without on-disk evidence are never touched.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SALVAGE_MERGE_PATH_MARKER = "salvage_hash_joined"


def _candidate_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("candidates", "documents"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return rows
    raise SystemExit("unsupported candidate_documents.json container shape")


def _envelope_key(row: dict[str, Any], profiles: dict[str, Path]) -> str:
    for key in (
        str(row.get("candidate_id") or "").strip(),
        hashlib.sha1(str(row.get("linkedin_url") or "").encode()).hexdigest()[:16],
    ):
        if key and key in profiles:
            return key
    return ""


def _envelope_has_emails(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return False
    return bool(dict(payload.get("item") or {}).get("emails"))


def reconcile_candidate_documents(
    snapshot_dir: Path,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    docs_path = snapshot_dir / "candidate_documents.json"
    profiles_dir = snapshot_dir / "harvest_profiles"
    if not docs_path.exists():
        return {"status": "skipped", "reason": "missing_candidate_documents"}
    if not profiles_dir.exists():
        return {"status": "skipped", "reason": "missing_harvest_profiles"}
    payload = json.loads(docs_path.read_text())
    rows = _candidate_rows(payload)
    profiles = {p.stem: p for p in profiles_dir.glob("*.json")}

    flag_flips: list[str] = []
    mode_stamps: list[str] = []
    for row in rows:
        key = _envelope_key(row, profiles)
        if not key:
            continue
        display = str(row.get("display_name") or row.get("candidate_id") or key)
        if not row.get("profile_fetched"):
            flag_flips.append(display)
            if not dry_run:
                row["profile_fetched"] = True
                if not str(row.get("profile_merge_path") or ""):
                    row["profile_merge_path"] = SALVAGE_MERGE_PATH_MARKER
        if not str(row.get("profile_mode") or "") and not _envelope_has_emails(profiles[key]):
            mode_stamps.append(display)
            if not dry_run:
                row["profile_mode"] = "full_details_no_email"

    changed = bool(flag_flips or mode_stamps)
    if changed and not dry_run:
        backup = docs_path.with_suffix(".json.pre_flag_backfill_bak")
        if not backup.exists():
            shutil.copy2(docs_path, backup)
        docs_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return {
        "status": "completed",
        "snapshot_dir": str(snapshot_dir),
        "rows": len(rows),
        "profile_files": len(profiles),
        "flag_flips": len(flag_flips),
        "flag_flip_names": flag_flips,
        "mode_stamps": len(mode_stamps),
        "written": changed and not dry_run,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--company", required=True, help="Target company display name.")
    parser.add_argument("--snapshot-id", required=True, help="Snapshot id to reconcile.")
    parser.add_argument("--refresh-ledger", action="store_true", help="Refresh the completeness ledger after writing.")
    parser.add_argument("--dry-run", action="store_true", help="Report planned updates without writing.")
    args = parser.parse_args()

    from sourcing_agent.candidate_artifacts import _resolve_company_snapshot
    from sourcing_agent.settings import load_settings

    settings = load_settings(REPO_ROOT)
    _company_key, snapshot_dir, _identity = _resolve_company_snapshot(
        settings.runtime_dir, args.company, snapshot_id=args.snapshot_id
    )
    result = reconcile_candidate_documents(snapshot_dir, dry_run=bool(args.dry_run))
    if bool(args.refresh_ledger) and not bool(args.dry_run) and result.get("written"):
        from sourcing_agent.organization_assets import ensure_organization_completeness_ledger
        from sourcing_agent.storage import ControlPlaneStore

        store = ControlPlaneStore(settings.db_path)
        ledger = ensure_organization_completeness_ledger(
            runtime_dir=settings.runtime_dir,
            store=store,
            target_company=args.company,
            snapshot_id=args.snapshot_id,
        )
        result["ledger_refresh"] = {
            "status": str(ledger.get("status") or ""),
            "readiness": str(ledger.get("readiness") or ""),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
