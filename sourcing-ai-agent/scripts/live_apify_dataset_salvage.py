#!/usr/bin/env python3
"""Adopt already-paid Apify datasets into a company-assets snapshot (salvage path).

This is the committed salvage adapter for live operations.  It exists so that
paid provider data is NEVER re-dispatched just because local materialization is
missing (GDM incident, 2026-07-20): download the dataset once, then build the
canonical snapshot locally with product-faithful dedupe and profile caching.

What it does (offline, zero provider calls):

1. Maps a raw Harvest company-employees dataset (current roster, Short mode)
   into product candidate documents (same shape the workflow materializer
   emits, see candidate_materialization.candidate_from_payload).
2. Passes through an existing product-shaped former candidate_documents.json.
3. Union-dedupes current+former through the product's own
   ``ingest_materialized_candidate`` / ``consolidate_materialized_duplicates``
   (linkedin-identity keys; current membership outranks former on conflicts).
4. Writes a NEW snapshot dir (identity.json copied from a verified source,
   candidate_documents.json, asset_registry.json, salvage_manifest.json).
5. Adopts paid profile datasets into ``harvest_profiles/<sha1(url)>.json``
   cache envelopes (connector contract: ``_profile_cache_key`` +
   ``_match_harvest_profile_items_to_requested_urls``) so any later
   ``fetch_profiles_by_urls`` run cache-hits them instead of re-paying.

Usage (see --help): all inputs are local files; --apply writes, default is
dry-run that only prints the plan and counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.candidate_materialization import (  # noqa: E402
    candidate_from_payload,
    consolidate_materialized_duplicates,
    ingest_materialized_candidate,
)
from sourcing_agent.harvest_connectors import (  # noqa: E402
    _match_harvest_profile_items_to_requested_urls,
    _profile_cache_key,
)


def _sha1_16(value: str) -> str:
    return hashlib.sha1(value.strip().encode("utf-8")).hexdigest()[:16]


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("candidates", "documents", "items"):
            if isinstance(payload.get(key), list):
                return [dict(item) for item in payload[key] if isinstance(item, dict)]
    raise ValueError(f"unsupported JSON shape in {path}")


def _roster_salvage_note(employment_status: str, function_tag: str) -> str:
    lane = "current roster" if employment_status == "current" else "former roster"
    if function_tag:
        return (
            f"{lane} salvage from function-sharded query (function {function_tag}); "
            "function attribution preserved from the shard lane"
        )
    return f"{lane} salvage (merged/unsharded query); function attribution untagged"


def _roster_item_to_candidate_payload(
    item: dict[str, Any],
    *,
    target_company: str,
    organization: str,
    dataset_id: str,
    run_id: str,
    source_path: Path,
    employment_status: str = "current",
    source_dataset: str = "apify_salvage_current_roster",
    function_tag: str = "",
) -> dict[str, Any] | None:
    linkedin_url = str(item.get("linkedinUrl") or item.get("profileUrl") or item.get("profile_url") or item.get("url") or "").strip()
    if not linkedin_url:
        return None
    first = str(item.get("firstName") or "").strip()
    last = str(item.get("lastName") or "").strip()
    name = f"{first} {last}".strip() or str(item.get("full_name") or item.get("fullName") or "").strip()
    if not name:
        return None
    location = item.get("location")
    if isinstance(location, dict):
        location_text = str(location.get("linkedinText") or location.get("text") or "").strip()
    else:
        location_text = str(location or item.get("location_normalized", {}).get("raw_text") if isinstance(item.get("location_normalized"), dict) else location or "").strip()
    role = ""
    positions = item.get("currentPositions") or item.get("currentPosition") or []
    if isinstance(positions, dict):
        positions = [positions]
    if isinstance(positions, list) and positions:
        head = dict(positions[0] or {})
        role = str(head.get("title") or head.get("position") or head.get("role") or "").strip()
    summary = str(item.get("summary") or item.get("headline") or "").strip()
    if not role and summary:
        role = summary  # profile-search shape: headline carries the role context
    slug = linkedin_url.rstrip("/").rsplit("/", 1)[-1]
    return {
        "candidate_id": _sha1_16(linkedin_url),
        "name_en": name,
        "display_name": name,
        "category": "employee" if employment_status == "current" else "former_employee",
        "target_company": target_company,
        "organization": organization,
        "employment_status": employment_status,
        "role": role,
        "focus_areas": role,
        "notes": (
            f"Salvaged from paid Apify dataset {dataset_id} (run {run_id}, {employment_status} roster). "
            f"Location: {location_text}."
        ).strip(),
        "linkedin_url": linkedin_url,
        "source_dataset": source_dataset,
        "source_path": str(source_path),
        "metadata": {
            "seed_slug": slug,
            "profile_location": location_text,
            "summary": summary,
            "salvage_dataset_id": dataset_id,
            "salvage_run_id": run_id,
            "salvage_note": _roster_salvage_note(employment_status, function_tag),
            **({"salvage_function_id": function_tag} if function_tag else {}),
        },
    }


PROFILE_SHAPE_FIELDS = ("headline", "location", "experience", "education", "languages", "skills", "about")


def _reattach_profile_shape_fields(
    records: list[dict[str, Any]], originals: list[dict[str, Any]]
) -> int:
    """Preserve structured top-level profile fields across the domain round-trip.

    candidate_from_payload/to_record only carry domain-record fields, so a
    pass-through doc's structured experience/education/... (the enrichment
    contract's top-level shape) would be silently dropped (experience) or
    repr-stringified (education) — the 2026-07-22 openai/tml salvage defect.
    Re-attach the original values wherever the round-tripped record lost them."""
    by_id: dict[str, dict[str, Any]] = {}
    by_url: dict[str, dict[str, Any]] = {}
    for doc in originals:
        cid = str(doc.get("candidate_id") or "").strip()
        url = str(doc.get("linkedin_url") or "").strip().rstrip("/").lower()
        if cid and cid not in by_id:
            by_id[cid] = doc
        if url and url not in by_url:
            by_url[url] = doc
    attached = 0
    for record in records:
        source = by_id.get(str(record.get("candidate_id") or "").strip()) or by_url.get(
            str(record.get("linkedin_url") or "").strip().rstrip("/").lower()
        )
        if not source:
            continue
        for field in PROFILE_SHAPE_FIELDS:
            value = source.get(field)
            if value and not record.get(field):
                record[field] = value
                attached += 1
    return attached


def _asset_entry(root: Path, path: Path, asset_type: str, source_kind: str) -> dict[str, Any]:
    rel = path.relative_to(root)
    return {
        "asset_id": _sha1_16(str(rel)),
        "relative_path": str(rel),
        "absolute_path": str(path),
        "asset_type": asset_type,
        "source_kind": source_kind,
        "content_type": "application/json",
        "size_bytes": path.stat().st_size,
        "is_raw_asset": False,
        "model_safe": True,
        "metadata": {},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company-assets-root", required=True, type=Path)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--identity-from", required=True, type=Path)
    parser.add_argument("--current-roster-dataset", type=Path, action="append", default=[])
    parser.add_argument(
        "--current-roster-function",
        action="append",
        default=[],
        help=(
            "function tag paired positionally with the Nth --current-roster-dataset; "
            "preserves shard-level function attribution instead of collapsing it on merge"
        ),
    )
    parser.add_argument(
        "--current-roster-run-id",
        action="append",
        default=[],
        help="Apify run id paired positionally with the Nth --current-roster-dataset (falls back to --current-run-id)",
    )
    parser.add_argument("--former-roster-dataset", type=Path, action="append", default=[])
    parser.add_argument("--current-dataset-id", default="")
    parser.add_argument("--current-run-id", default="")
    parser.add_argument("--former-candidate-documents", type=Path, default=None)
    parser.add_argument("--profile-dataset", type=Path, action="append", default=[])
    parser.add_argument("--extra-profile-cache-dir", type=Path, action="append", default=[])
    parser.add_argument("--target-company", default="")
    parser.add_argument("--organization", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    root: Path = args.company_assets_root.resolve()  # absolute: connector internals re-anchor relative paths
    snapshot_dir = root / args.snapshot_id
    identity = json.loads(args.identity_from.read_text(encoding="utf-8"))
    target_company = args.target_company or str(identity.get("canonical_name") or identity.get("requested_name") or "").strip()
    organization = args.organization or target_company

    candidates: list[dict[str, Any]] = []
    skipped = 0
    if args.former_candidate_documents:
        for doc in _load_json_list(args.former_candidate_documents):
            doc["_salvage_passthrough_origin"] = str(args.former_candidate_documents)
            candidates.append(doc)
    current_shards: list[dict[str, Any]] = []
    for idx, roster_path in enumerate(args.current_roster_dataset):
        current_shards.append(
            {
                "path": roster_path,
                "dataset_id": args.current_dataset_id or roster_path.stem,
                "run_id": (
                    args.current_roster_run_id[idx]
                    if idx < len(args.current_roster_run_id)
                    else args.current_run_id
                ).strip(),
                "function": (
                    args.current_roster_function[idx]
                    if idx < len(args.current_roster_function)
                    else ""
                ).strip(),
            }
        )
    roster_specs = [
        *[
            (shard["path"], "current", "apify_salvage_current_roster", shard["dataset_id"], shard["run_id"], shard["function"])
            for shard in current_shards
        ],
        *[
            (p, "former", "apify_salvage_former_roster", args.current_dataset_id or p.stem, args.current_run_id, "")
            for p in args.former_roster_dataset
        ],
    ]
    for roster_path, status, source_name, dataset_id, run_id, function_tag in roster_specs:
        for item in _load_json_list(roster_path):
            payload = _roster_item_to_candidate_payload(
                item,
                target_company=target_company,
                organization=organization,
                dataset_id=dataset_id,
                run_id=run_id,
                source_path=roster_path,
                employment_status=status,
                source_dataset=source_name,
                function_tag=function_tag,
            )
            if payload is None:
                skipped += 1
                continue
            candidates.append(payload)

    merged: dict[str, Any] = {}
    aliases: dict[str, str] = {}
    index: dict[str, str] = {}
    invalid = 0
    passthrough_preserved: list[dict[str, Any]] = []
    for payload in candidates:
        origin = str(payload.pop("_salvage_passthrough_origin", "") or "")
        candidate = candidate_from_payload(payload)
        if candidate is None:
            if origin:
                # Paid coverage already materialized upstream (e.g. empty-name
                # former rows pending a name resolve) must survive the rebuild:
                # preserve verbatim instead of silently shrinking the union.
                kept = dict(payload)
                metadata = dict(kept.get("metadata") or {})
                metadata["salvage_passthrough_invalid"] = True
                metadata["salvage_passthrough_origin"] = origin
                kept["metadata"] = metadata
                passthrough_preserved.append(kept)
            else:
                invalid += 1
            continue
        ingest_materialized_candidate(
            candidate,
            merged_candidates=merged,
            candidate_aliases=aliases,
            identity_index=index,
            prefer_incoming=False,
        )
    merged, _ = consolidate_materialized_duplicates(merged, {})
    records = [candidate.to_record() for candidate in merged.values()]
    _reattach_profile_shape_fields(records, candidates)
    merged_urls = {
        str(record.get("linkedin_url") or "").strip().rstrip("/").lower()
        for record in records
        if str(record.get("linkedin_url") or "").strip()
    }
    for kept in passthrough_preserved:
        kept_url = str(kept.get("linkedin_url") or "").strip().rstrip("/").lower()
        if kept_url and kept_url in merged_urls:
            continue
        records.append(kept)
    status_counts: dict[str, int] = {}
    for record in records:
        key = str(record.get("employment_status") or "").strip() or "unknown"
        status_counts[key] = status_counts.get(key, 0) + 1

    profile_urls = [
        str(record.get("linkedin_url") or "").strip() for record in records if str(record.get("linkedin_url") or "").strip()
    ]
    profile_urls = list(dict.fromkeys(profile_urls))

    matched_profiles: dict[str, dict[str, Any]] = {}
    for dataset_path in args.profile_dataset:
        items = _load_json_list(dataset_path)
        remaining = [u for u in profile_urls if u not in matched_profiles]
        matched, _unmatched = _match_harvest_profile_items_to_requested_urls(remaining, items)
        matched_profiles.update(matched)

    print(f"candidates_in={len(candidates)} skipped_no_url={skipped} invalid={invalid}")
    print(f"union={len(records)} by_status={status_counts} passthrough_preserved={len(passthrough_preserved)}")
    print(f"profile_urls={len(profile_urls)} salvaged_profiles_matched={len(matched_profiles)}")
    extra_cache = [p for d in args.extra_profile_cache_dir for p in sorted(Path(d).glob('*.json'))]
    print(f"extra_profile_cache_files={len(extra_cache)}")

    if not args.apply:
        print("dry-run only; pass --apply to write the snapshot")
        return 0

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "identity.json").write_text(json.dumps(identity, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    # Envelope mirrors the OpenAI salvage snapshot (the proven precedent):
    # dict with candidates/evidence + provenance keys, never a bare list —
    # candidate_materialization.load_company_candidate_snapshot requires it.
    documents_payload = {
        "snapshot": args.snapshot_id,
        "target_company": target_company,
        "candidate_count": len(records),
        "candidates": records,
        "evidence": [],
        "salvage_note": (
            "Built from adopted paid Apify datasets ("
            + (
                ", ".join(
                    f"{shard['dataset_id']}" + (f" fn{shard['function']}" if shard["function"] else "")
                    for shard in current_shards
                )
                or f"{args.current_dataset_id} via run {args.current_run_id}"
            )
            + ") + reused former/prior candidate documents (operator-approved); "
            "no re-fetch, no new provider dispatches."
        ),
        "acquisition_sources": {
            "current_roster_salvage": {
                "dataset_id": args.current_dataset_id,
                "run_id": args.current_run_id,
                "note": (
                    "function-sharded rosters adopted with per-shard attribution (see shards)"
                    if any(shard["function"] for shard in current_shards)
                    else "current roster salvage (merged/unsharded query); function attribution untagged"
                ),
                "shards": [
                    {
                        "file": str(shard["path"]),
                        "dataset_id": shard["dataset_id"],
                        "run_id": shard["run_id"],
                        "function": shard["function"],
                    }
                    for shard in current_shards
                ],
            },
            "former_candidate_documents": {
                "reused_from": str(args.former_candidate_documents or ""),
            },
            "profile_datasets_adopted": [str(p) for p in args.profile_dataset],
        },
        "enrichment_summary": {
            "profiles_merged": 0,
            "merged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "salvage adapter scripts/live_apify_dataset_salvage.py",
        },
    }
    (snapshot_dir / "candidate_documents.json").write_text(
        json.dumps(documents_payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )
    harvest_dir = snapshot_dir / "harvest_profiles"
    harvest_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for url, item in matched_profiles.items():
        envelope = {"_harvest_request": {"kind": "url", "value": url, "profile_url": url}, "item": item}
        (harvest_dir / f"{_profile_cache_key(url)}.json").write_text(
            json.dumps(envelope, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        written += 1
    copied = 0
    for path in extra_cache:
        target = harvest_dir / path.name
        if target.exists():
            continue
        target.write_bytes(path.read_bytes())
        copied += 1

    manifest = {
        "snapshot_id": args.snapshot_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "salvage_note": "Built from adopted paid Apify datasets (no new provider dispatches).",
        "inputs": {
            "identity_from": str(args.identity_from),
            "current_roster_dataset": str(args.current_roster_dataset or ""),
            "current_roster_functions": list(args.current_roster_function or []),
            "current_roster_run_ids": list(args.current_roster_run_id or []),
            "current_dataset_id": args.current_dataset_id,
            "current_run_id": args.current_run_id,
            "former_candidate_documents": str(args.former_candidate_documents or ""),
            "profile_datasets": [str(p) for p in args.profile_dataset],
            "extra_profile_cache_dirs": [str(p) for p in args.extra_profile_cache_dir],
        },
        "counts": {
            "candidates_in": len(candidates),
            "union_candidates": len(records),
            "by_status": status_counts,
            "passthrough_preserved_invalid": len(passthrough_preserved),
            "salvaged_profiles_written": written,
            "extra_profile_cache_copied": copied,
        },
    }
    (snapshot_dir / "salvage_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    assets = [
        _asset_entry(snapshot_dir, snapshot_dir / "identity.json", "company_identity", "salvage_adopt"),
        _asset_entry(snapshot_dir, snapshot_dir / "candidate_documents.json", "candidate_documents", "salvage_adopt"),
        _asset_entry(snapshot_dir, snapshot_dir / "salvage_manifest.json", "salvage_manifest", "salvage_adopt"),
    ]
    registry = {
        "root_dir": str(snapshot_dir),
        "assets": assets,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (snapshot_dir / "asset_registry.json").write_text(json.dumps(registry, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote snapshot {snapshot_dir} (profiles written={written}, cache copied={copied})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
