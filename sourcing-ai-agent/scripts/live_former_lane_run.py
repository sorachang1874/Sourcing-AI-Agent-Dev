#!/usr/bin/env python3
"""Former-lane per-function acquisition driver (live, paid, library path).

Executes the operator's former-lane contract directly (bypasses the
daemon-dependent workflow path — use it when the workflow runtime is
degraded): ONE profile-search shard per selected function id, planned by the
product's own `build_request_scoped_former_search_shard_plan` (per-function,
never merged, plan marker flows so the guardrail keeps the single
functionIds), each executed through `HarvestProfileSearchConnector.search_profiles`
with probe auto-expansion to the provider total.

Payload per shard (verified before dispatch, printed): {pastCompanies:
[<company url>], locations: [...], functionIds: [<single id>]} — no keywords,
no searchQuery.

Requires APIFY_API_TOKEN + the live triple (SOURCING_EXTERNAL_PROVIDER_MODE=live,
SOURCING_LIVE_PROVIDER_CONFIRM=1, SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS=1);
the driver refuses otherwise.  Entries are written as salvage-shaped JSONs
(--out-dir) for scripts/live_apify_dataset_salvage.py --former-roster-dataset.
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

from sourcing_agent.company_shard_planning import build_request_scoped_former_search_shard_plan  # noqa: E402
from sourcing_agent.harvest_connectors import (  # noqa: E402
    HarvestProfileSearchConnector,
    _apply_harvest_search_filters,
    _runtime_scoped_provider_mode,
)
from sourcing_agent.settings import HarvestActorSettings  # noqa: E402

PROFILE_SEARCH_ACTOR_ID = "M2FMdjRVeF1HPGFcc"


def _preview_payload(shard: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {"profileScraperMode": "Short", "maxItems": 2500, "startPage": 1, "takePages": 100}
    _apply_harvest_search_filters(payload, dict(shard.get("filter_hints") or {}), "former")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company-url", required=True, help="LinkedIn company URL, e.g. https://www.linkedin.com/company/anthropicresearch/")
    parser.add_argument("--locations", default="United States", help="comma-separated (single-writer; '' opts out)")
    parser.add_argument("--function-ids", default="8,24", help="comma-separated; one shard per id")
    parser.add_argument("--discovery-dir", required=True, type=Path, help="snapshot search_seed_discovery dir (receipts land here)")
    parser.add_argument("--out-dir", required=True, type=Path, help="salvage-shaped output dir for salvage script")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    locations = [part.strip() for part in args.locations.split(",") if part.strip()]
    function_ids = [part.strip() for part in args.function_ids.split(",") if part.strip()]
    plan = build_request_scoped_former_search_shard_plan(
        function_ids=function_ids,
        past_companies=[args.company_url],
        locations=locations or None,
    )
    print(f"strategy={plan['strategy_id']} shards={[s['shard_id'] for s in plan['shards']]}")
    for shard in plan["shards"]:
        print(f"  {shard['shard_id']}: {json.dumps(_preview_payload(shard), ensure_ascii=False)}")
    if args.dry_run:
        return 0

    if _runtime_scoped_provider_mode(base_path=args.discovery_dir.resolve()) != "live":
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
        max_total_charge_usd=25.0,
        max_paid_items=2500,
        default_mode="short",
        collect_email=False,
    )
    connector = HarvestProfileSearchConnector(settings)
    args.discovery_dir.resolve().mkdir(parents=True, exist_ok=True)
    args.out_dir.resolve().mkdir(parents=True, exist_ok=True)
    discovery_dir = args.discovery_dir.resolve()  # absolute: connector internals re-anchor relative paths
    out_dir = args.out_dir.resolve()

    summary: list[dict[str, Any]] = []
    for shard in plan["shards"]:
        shard_id = shard["shard_id"]
        print(f"--- executing {shard_id} ...", flush=True)
        result = connector.search_profiles(
            query_text="",
            filter_hints=dict(shard.get("filter_hints") or {}),
            employment_status="former",
            discovery_dir=discovery_dir,
            limit=25,
            pages=100,
            allow_shared_provider_cache=True,
        ) or {}
        entries = result.get("entries") or result.get("seed_entries") or []
        out_path = out_dir / f"former_{shard_id}.json"
        out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        summary.append(
            {
                "shard_id": shard_id,
                "entries": len(entries),
                "out": str(out_path),
                "status": str(result.get("status") or ""),
                "run_ids": list(result.get("run_ids") or []),
            }
        )
        print(f"    {shard_id}: entries={len(entries)} status={result.get('status')}", flush=True)
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
