#!/usr/bin/env python3
"""Merge cached profile payloads into candidate documents (offline, $0).

Layering signals (layer_2/layer_3 region & language experience) read
top-level ``headline/location/experience/education/languages`` on candidate
documents — the OpenAI salvage snapshot's shape.  Salvage/workflow snapshots
whose docs are seed-level get those fields back-filled here from the
snapshot's ``harvest_profiles/<sha1(url)>.json`` cache (connector envelope
contract), so the deterministic layering sees the full paid profile content.

Idempotent: already-enriched docs are skipped unless --force; a .bak backup
is written before the first rewrite.  No provider calls.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from live_xfirst_seed_build import ProfileEnvelopeIndex  # noqa: E402
from sourcing_agent.harvest_connectors import _profile_cache_key  # noqa: E402

ENRICHED_FIELDS = ("headline", "location", "experience", "education", "languages", "skills", "about")


def _location_text(item: dict[str, Any]) -> str:
    loc = item.get("location")
    if isinstance(loc, dict):
        parsed = loc.get("parsed") if isinstance(loc.get("parsed"), dict) else {}
        return str(loc.get("linkedinText") or parsed.get("text") or "").strip()
    return str(loc or "").strip()


def _profile_envelope(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None  # batch payload files are list-shaped; queue artifacts are not envelopes
    item = payload.get("item")
    return item if isinstance(item, dict) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    snapshot_dir: Path = args.snapshot_dir.resolve()
    doc_path = snapshot_dir / "candidate_documents.json"
    payload = json.loads(doc_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates") if isinstance(payload, dict) else payload
    if not isinstance(candidates, list):
        print("candidate_documents.json has no candidates list", file=sys.stderr)
        return 2

    harvest_dir = snapshot_dir / "harvest_profiles"
    envelope_index = ProfileEnvelopeIndex(snapshot_dir)
    enriched = skipped_has_profile = missing_profile = already = 0
    for doc in candidates:
        if not isinstance(doc, dict):
            continue
        url = str(doc.get("linkedin_url") or "").strip()
        if not url:
            continue
        if not args.force and any(str(doc.get(f) or "").strip() for f in ("experience", "education", "languages")):
            already += 1
            continue
        item = envelope_index.load(url) or _profile_envelope(harvest_dir / f"{_profile_cache_key(url)}.json")
        if item is None:
            missing_profile += 1
            continue
        headline = str(item.get("headline") or "").strip()
        location = _location_text(item)
        # --force re-derives envelope-backed fields even when the doc carries a
        # value: pass-through docs can arrive with repr-stringified copies (the
        # 2026-07-22 salvage defect) that a fill-only merge would preserve.
        if headline and (args.force or not doc.get("headline")):
            doc["headline"] = headline
        if location and (args.force or not doc.get("location")):
            doc["location"] = location
        for field, item_key in (
            ("experience", "experience"),
            ("education", "education"),
            ("languages", "languages"),
            ("skills", "skills"),
            ("about", "about"),
        ):
            value = item.get(item_key)
            if value and (args.force or not doc.get(field)):
                doc[field] = value
        enriched += 1
        skipped_has_profile += 0

    if isinstance(payload, dict):
        summary = dict(payload.get("enrichment_summary") or {})
        summary.update(
            {
                "profiles_merged": enriched,
                "merged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source": "scripts/live_candidate_profile_enrich.py (harvest_profiles cache)",
            }
        )
        payload["enrichment_summary"] = summary

    if enriched or already == 0:
        backup = doc_path.with_suffix(".json.bak")
        if not backup.exists():
            shutil.copy2(doc_path, backup)
        out = payload if isinstance(payload, dict) else candidates
        doc_path.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    print(f"enriched={enriched} already_enriched={already} missing_profile={missing_profile} total={len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
