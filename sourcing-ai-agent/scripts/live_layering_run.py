#!/usr/bin/env python3
"""Live-ops driver for the outreach (华人线索) layering stage.

Thin wrapper over the product's own
``outreach_layering.analyze_company_outreach_layers`` with
``allow_candidate_documents_source=True`` — required for salvage-built
snapshots (scripts/live_apify_dataset_salvage.py), whose candidate source is
a plain ``candidate_documents.json`` envelope and which carry no pre-built
canonical artifact dir (the CLI flag does not exist; see cli.py
``segment-company-outreach-layers`` — it only works for snapshots whose
canonical artifacts were built through the store path).

Env (mirrors the live backend): SOURCING_RUNTIME_DIR + MODEL_PROVIDER_*
(API_KEY/BASE_URL/MODEL/NAME/API_STYLE/MIN_MAX_TOKENS/TIMEOUT_SECONDS).
Pass --no-ai for a deterministic-only run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from sourcing_agent.asset_catalog import AssetCatalog  # noqa: E402
from sourcing_agent.model_provider import OpenAICompatibleChatModelClient  # noqa: E402
from sourcing_agent.outreach_layering import analyze_company_outreach_layers  # noqa: E402
from sourcing_agent.settings import load_settings  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--asset-view", default="canonical_merged")
    parser.add_argument("--query", default="")
    parser.add_argument("--max-ai-verifications", type=int, default=80)
    parser.add_argument("--ai-workers", type=int, default=8)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--no-ai", action="store_true")
    args = parser.parse_args()

    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    model_client = None
    if not args.no_ai:
        if not settings.model_provider.enabled:
            print("model_provider is not enabled (check MODEL_PROVIDER_* env)", file=sys.stderr)
            return 2
        model_client = OpenAICompatibleChatModelClient(settings.model_provider)

    result = analyze_company_outreach_layers(
        runtime_dir=os.environ.get("SOURCING_RUNTIME_DIR") or settings.runtime_dir,
        target_company=args.company,
        snapshot_id=args.snapshot_id,
        view=args.asset_view,
        query=args.query,
        model_client=model_client,
        max_ai_verifications=max(0, int(args.max_ai_verifications)),
        ai_workers=max(1, int(args.ai_workers)),
        output_dir=args.output_dir or None,
        allow_candidate_documents_source=True,
    )
    summary = result.get("analysis_summary") or result.get("summary") or {}
    print(json.dumps({
        "status": result.get("status"),
        "target_company": result.get("target_company"),
        "snapshot_id": result.get("snapshot_id"),
        "candidate_count": result.get("candidate_count"),
        "layer_counts": summary.get("layer_counts") or summary.get("layers") or summary,
        "output_dir": result.get("output_dir") or result.get("analysis_dir"),
    }, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
