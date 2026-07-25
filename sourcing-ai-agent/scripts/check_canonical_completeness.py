#!/usr/bin/env python3
"""Canonical completeness check for a company's asset tree (offline, read-only).

The first committed completeness gate for ANY lab (previously BOARD prose only).
Checks, for --company-key under --assets-root:

  1. latest_snapshot.json resolves BY snapshot_id (the pointer contract): the
     snapshot directory must exist under the company dir with a
     candidate_documents.json; the pointer's absolute snapshot_dir is treated
     as provenance only and never trusted.
  2. candidate_documents.json contains no linkedin_url+name duplicates that are
     not explained by a committed identity alias map (--alias-map).
  3. every candidate id is format-valid (12 or 16 lowercase hex).
  4. when an alias map is supplied, its three id derivations are recomputed and
     must match for every entry (fail-closed on drift).

Exit 0 = complete; exit 1 = violations (each printed as one JSON line);
exit 2 = usage/environment error. No Postgres, no network, no writes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.seed_discovery import normalize_name_token  # noqa: E402

_ID_PATTERN = re.compile(r"^[0-9a-f]{12}([0-9a-f]{4})?$")


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _load_json(path: Path) -> dict | list:
    return json.loads(path.read_text(encoding="utf-8"))


def check_alias_map(alias_map: dict) -> list[dict]:
    violations: list[dict] = []
    company = str(alias_map.get("company_key") or "")
    for entry in list(alias_map.get("entries") or []):
        canonical = str(entry.get("canonical_id") or "")
        name = str(entry.get("name") or "")
        url = str(entry.get("linkedin_url") or "")
        aliases = dict(entry.get("aliases") or {})
        expected = {
            "canonical_id": _sha1("|".join([normalize_name_token(company), normalize_name_token(name), url]))[:16],
            "harvest_url_hash_16hex": _sha1(url)[:16],
            "v1_seed_name_hash_12hex": _sha1(name)[:12],
        }
        actual = {
            "canonical_id": canonical,
            "harvest_url_hash_16hex": str(aliases.get("harvest_url_hash_16hex") or ""),
            "v1_seed_name_hash_12hex": str(aliases.get("v1_seed_name_hash_12hex") or ""),
        }
        for key, want in expected.items():
            if actual[key] and actual[key] != want:
                violations.append(
                    {
                        "check": "alias_map_hash_drift",
                        "canonical_id": canonical,
                        "field": key,
                        "expected": want,
                        "actual": actual[key],
                    }
                )
    return violations


def _alias_known_ids(alias_map: dict | None) -> set[str]:
    known: set[str] = set()
    for entry in list((alias_map or {}).get("entries") or []):
        known.add(str(entry.get("canonical_id") or ""))
        for value in dict(entry.get("aliases") or {}).values():
            known.add(str(value or ""))
    known.discard("")
    return known


def check_documents(documents: list[dict], alias_map: dict | None) -> list[dict]:
    violations: list[dict] = []
    known_alias_ids = _alias_known_ids(alias_map)
    seen: dict[tuple[str, str], str] = {}
    for doc in documents:
        candidate_id = str(doc.get("candidate_id") or "")
        name = str(doc.get("name_en") or doc.get("name") or "").strip()
        url = str(doc.get("linkedin_url") or "").strip()
        if not _ID_PATTERN.match(candidate_id):
            violations.append({"check": "candidate_id_format", "candidate_id": candidate_id, "name": name})
        if not url or not name:
            continue
        key = (url, name)
        if key not in seen:
            seen[key] = candidate_id
            continue
        first_id = seen[key]
        if first_id in known_alias_ids and candidate_id in known_alias_ids:
            continue  # documented merge pair — allowed to coexist in raw history
        violations.append(
            {
                "check": "duplicate_identity",
                "linkedin_url": url,
                "name": name,
                "candidate_ids": [first_id, candidate_id],
            }
        )
    return violations


def check_pointer(company_dir: Path) -> tuple[list[dict], Path | None]:
    pointer_path = company_dir / "latest_snapshot.json"
    if not pointer_path.is_file():
        return [{"check": "pointer_missing", "path": str(pointer_path)}], None
    pointer = _load_json(pointer_path)
    snapshot_id = str(pointer.get("snapshot_id") or "").strip() if isinstance(pointer, dict) else ""
    if not snapshot_id:
        return [{"check": "pointer_no_snapshot_id", "path": str(pointer_path)}], None
    snapshot_dir = company_dir / snapshot_id
    if not snapshot_dir.is_dir():
        return [
            {
                "check": "pointer_dangling",
                "snapshot_id": snapshot_id,
                "expected_dir": str(snapshot_dir),
                "provenance_snapshot_dir": str(pointer.get("snapshot_dir") or ""),
            }
        ], None
    documents_path = snapshot_dir / "candidate_documents.json"
    if not documents_path.is_file():
        return [{"check": "snapshot_without_documents", "snapshot_dir": str(snapshot_dir)}], None
    return [], documents_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company-key", required=True)
    parser.add_argument("--assets-root", required=True, type=Path)
    parser.add_argument("--alias-map", type=Path, default=None)
    parser.add_argument("--documents", type=Path, default=None, help="check this documents file instead of the pointer target")
    args = parser.parse_args()

    company_dir = args.assets_root / args.company_key
    if not company_dir.is_dir():
        print(json.dumps({"check": "company_dir_missing", "path": str(company_dir)}))
        return 2

    alias_map: dict | None = None
    violations: list[dict] = []
    if args.alias_map is not None:
        alias_map = _load_json(args.alias_map)  # type: ignore[assignment]
        violations.extend(check_alias_map(alias_map or {}))

    documents_path = args.documents
    if documents_path is None:
        pointer_violations, documents_path = check_pointer(company_dir)
        violations.extend(pointer_violations)
    if documents_path is not None:
        payload = _load_json(documents_path)
        documents = payload if isinstance(payload, list) else list(payload.get("documents") or payload.get("candidates") or [])
        violations.extend(check_documents(documents, alias_map))

    for violation in violations:
        print(json.dumps(violation, ensure_ascii=False))
    print(
        json.dumps(
            {
                "company_key": args.company_key,
                "documents_path": str(documents_path or ""),
                "violation_count": len(violations),
                "status": "complete" if not violations else "violations",
            },
            ensure_ascii=False,
        )
    )
    return 0 if not violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
