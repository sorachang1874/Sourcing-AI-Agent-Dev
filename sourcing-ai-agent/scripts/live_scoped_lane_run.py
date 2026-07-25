#!/usr/bin/env python3
"""Scoped keyword lane acquisition driver (live, paid, library path).

Executes per-status × per-function scoped profile-search queries — the
operator's contract for keyword-scoped team recall (e.g. Meta TBD): one
query per (employment_status, function id) cell, each carrying the keyword
as searchQuery, the company URL (current/pastCompanies by status), locations,
and a SINGLE functionIds value.  Never a merged multi-function query, never
a dropped keyword (the two dry-run failure modes of the planned flow).

Payload per cell (printed before dispatch): {searchQuery: <kw>,
(current|past)Companies: [url], locations: [...], functionIds: [<id>]}.

Requires APIFY_API_TOKEN + the live triple; refuses otherwise.  Entries are
written as salvage-shaped JSONs (--out-dir, one per cell) for
scripts/live_apify_dataset_salvage.py (--current/--former-roster-dataset).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.harvest_connectors import (  # noqa: E402
    HarvestProfileSearchConnector,
    _apply_harvest_search_filters,
    _runtime_scoped_provider_mode,
)
from sourcing_agent.settings import HarvestActorSettings  # noqa: E402

PROFILE_SEARCH_ACTOR_ID = "M2FMdjRVeF1HPGFcc"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company-url", required=True)
    parser.add_argument("--query", required=True, help="scoped keyword, e.g. TBD")
    parser.add_argument("--locations", default="United States")
    parser.add_argument("--function-ids", default="8,24")
    parser.add_argument("--statuses", default="current,former")
    parser.add_argument("--discovery-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    locations = [p.strip() for p in args.locations.split(",") if p.strip()]
    function_ids = [p.strip() for p in args.function_ids.split(",") if p.strip()]
    statuses = [p.strip() for p in args.statuses.split(",") if p.strip()]

    cells: list[dict[str, Any]] = []
    for status in statuses:
        company_key = "current_companies" if status == "current" else "past_companies"
        for fid in function_ids:
            hints = {company_key: [args.company_url], "function_ids": [fid]}
            if locations:
                hints["locations"] = locations
            payload: dict[str, Any] = {
                "profileScraperMode": "Short", "maxItems": 2500, "startPage": 1, "takePages": 100,
                "searchQuery": args.query,
            }
            _apply_harvest_search_filters(payload, hints, status)
            cells.append({"status": status, "function_id": fid, "filter_hints": hints, "payload": payload})

    for cell in cells:
        print(f"{cell['status']}_function_{cell['function_id']}: {json.dumps(cell['payload'], ensure_ascii=False)}")
    if args.dry_run:
        return 0

    discovery_dir = args.discovery_dir.resolve()
    out_dir = args.out_dir.resolve()
    if _runtime_scoped_provider_mode(base_path=discovery_dir) != "live":
        print("REFUSING: provider mode is not live (export the live contract)", file=sys.stderr)
        return 2
    for flag in ("SOURCING_LIVE_PROVIDER_CONFIRM", "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS"):
        if os.environ.get(flag, "").strip() != "1":
            print(f"REFUSING: {flag}=1 required", file=sys.stderr)
            return 2
    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        print("APIFY_API_TOKEN is required", file=sys.stderr)
        return 2

    settings = HarvestActorSettings(
        enabled=True,
        api_token=token,
        actor_id=PROFILE_SEARCH_ACTOR_ID,
        timeout_seconds=3600,
        max_total_charge_usd=15.0,
        max_paid_items=2500,
        default_mode="short",
        collect_email=False,
    )
    connector = HarvestProfileSearchConnector(settings)
    discovery_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: list[dict[str, Any]] = []
    for cell in cells:
        lane = f"{cell['status']}_function_{cell['function_id']}"
        print(f"--- executing {lane} ...", flush=True)
        result = connector.search_profiles(
            query_text=args.query,
            filter_hints=dict(cell["filter_hints"]),
            employment_status=cell["status"],
            discovery_dir=discovery_dir,
            limit=25,
            pages=100,
            allow_shared_provider_cache=True,
        ) or {}
        rows = result.get("rows") or result.get("entries") or []
        out_path = out_dir / f"{lane}.json"
        out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        summary.append({"lane": lane, "rows": len(rows), "out": str(out_path), "status": str(result.get("status") or "")})
        print(f"    {lane}: rows={len(rows)}", flush=True)
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
