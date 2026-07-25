#!/usr/bin/env python3
"""Concurrent slot-fill profile fetch driver (live ops, paid).

Fetches ONLY the profiles that are missing from a snapshot's
``harvest_profiles/`` cache, in a small number of concurrent batches
(operator rule: 4-8 batches, fire as soon as a slot is free — not dozens of
tiny batches, not serial).

- Dedupe happens BEFORE dispatch: urls come from the snapshot's
  ``candidate_documents.json`` minus cache-hit urls (``_profile_cache_key``),
  so anything already salvaged/paid is never re-fetched.
- Each batch runs through the product connector
  (``HarvestProfileConnector.fetch_profiles_by_urls``), which itself
  re-checks cache + shared provider cache before submitting — a second
  line of defence against duplicate paid fetches.
- Mode is fixed: ``Profile details no email ($4 per 1k)`` (collect_email off),
  matching the TML/OpenAI profile fetches.  No keywords/parameters beyond
  the url list ever reach the provider.

Requires APIFY_API_TOKEN in the environment.  --dry-run prints the dispatch
plan (counts, chunking, charge caps) without any provider call.

Note: concurrent batches write per-url envelope files (unique names, safe);
the informational asset_registry side-writes may race cosmetically — rebuild
the registry afterwards if exact bookkeeping is needed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.harvest_connectors import (  # noqa: E402
    HarvestProfileConnector,
    _profile_cache_key,
)
from sourcing_agent.settings import HarvestActorSettings  # noqa: E402

PROFILE_SCRAPER_ACTOR_ID = "LpVuK3Zozwuipa5bp"
PROFILE_MODE_PRICE_PER_ITEM_USD = 0.004  # "Profile details no email ($4 per 1k)"


def _missing_profile_urls(snapshot_dir: Path) -> list[str]:
    payload = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
    candidates = payload.get("candidates") if isinstance(payload, dict) else payload
    urls: list[str] = []
    for doc in list(candidates or []):
        url = str(dict(doc).get("linkedin_url") or "").strip()
        if url and url not in urls:
            urls.append(url)
    harvest_dir = snapshot_dir / "harvest_profiles"
    cached = {path.stem for path in harvest_dir.glob("*.json")} if harvest_dir.is_dir() else set()
    return [url for url in urls if _profile_cache_key(url) not in cached]


def _chunk(values: list[str], parts: int) -> list[list[str]]:
    parts = max(1, min(parts, len(values) or 1))
    chunks: list[list[str]] = [[] for _ in range(parts)]
    for index, value in enumerate(values):
        chunks[index % parts].append(value)
    return [chunk for chunk in chunks if chunk]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--batch-count", type=int, default=6)
    parser.add_argument("--charge-cap-per-batch-usd", type=float, default=3.0)
    parser.add_argument("--timeout-seconds", type=int, default=1200)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    snapshot_dir: Path = args.snapshot_dir.resolve()  # absolute: connector internals re-anchor relative paths
    missing = _missing_profile_urls(snapshot_dir)
    chunks = _chunk(missing, args.batch_count)
    estimated = len(missing) * PROFILE_MODE_PRICE_PER_ITEM_USD
    print(f"missing_urls={len(missing)} batches={len(chunks)} sizes={[len(c) for c in chunks]}")
    print(
        f"mode='Profile details no email ($4 per 1k)' findEmail=false "
        f"est_cost=${estimated:.2f} cap_per_batch=${args.charge_cap_per_batch_usd:.2f} "
        f"cap_total=${args.charge_cap_per_batch_usd * len(chunks):.2f}"
    )
    if args.dry_run or not missing:
        return 0

    from sourcing_agent.harvest_connectors import _runtime_scoped_provider_mode

    provider_mode = _runtime_scoped_provider_mode(base_path=snapshot_dir)
    if provider_mode != "live":
        print(
            f"REFUSING dispatch: provider mode is {provider_mode!r}, not 'live' — a paid-intent "
            "fetch must fail closed rather than silently simulate. Export "
            "SOURCING_EXTERNAL_PROVIDER_MODE=live (and APIFY_API_TOKEN) to run for real.",
            file=sys.stderr,
        )
        return 2
    for flag in ("SOURCING_LIVE_PROVIDER_CONFIRM", "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS"):
        if os.environ.get(flag, "").strip() != "1":
            print(
                f"REFUSING dispatch: {flag}=1 is required for isolated live provider access "
                "(same gate as `make test-env-backend-live`).",
                file=sys.stderr,
            )
            return 2

    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        print("APIFY_API_TOKEN is required", file=sys.stderr)
        return 2

    settings = HarvestActorSettings(
        enabled=True,
        api_token=token,
        actor_id=PROFILE_SCRAPER_ACTOR_ID,
        timeout_seconds=args.timeout_seconds,
        max_total_charge_usd=args.charge_cap_per_batch_usd,
        max_paid_items=max(len(c) for c in chunks) + 25,
        default_mode="full",  # maps to "Profile details no email ($4 per 1k)"
        collect_email=False,
    )

    print_lock = threading.Lock()
    run_records: list[dict[str, Any]] = []

    def _on_batch_result(chunk_index: int):
        def _callback(record: dict[str, Any]) -> None:
            with print_lock:
                run_records.append({"chunk_index": chunk_index, **{k: record.get(k) for k in ("run_id", "dataset_id", "status", "paid_item_count", "charge_usd")}})
        return _callback

    def _run_chunk(chunk_index: int, urls: list[str]) -> dict[str, Any]:
        connector = HarvestProfileConnector(settings)
        results = connector.fetch_profiles_by_urls(
            urls,
            snapshot_dir,
            runtime_timing_overrides=None,
            on_batch_result=_on_batch_result(chunk_index),
        )
        unresolved = [url for url in urls if url not in results]
        return {"chunk_index": chunk_index, "requested": len(urls), "resolved": len(results), "unresolved": len(unresolved)}

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(chunks), thread_name_prefix="profile-slot-fill") as pool:
        futures = {pool.submit(_run_chunk, index, chunk): index for index, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            outcome = future.result()
            outcomes.append(outcome)
            with print_lock:
                print(f"chunk {outcome['chunk_index']}: requested={outcome['requested']} resolved={outcome['resolved']} unresolved={outcome['unresolved']}")

    still_missing = _missing_profile_urls(snapshot_dir)
    summary = {
        "requested": len(missing),
        "resolved_total": sum(o["resolved"] for o in outcomes),
        "still_missing": len(still_missing),
        "runs": sorted(run_records, key=lambda r: r["chunk_index"]),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
