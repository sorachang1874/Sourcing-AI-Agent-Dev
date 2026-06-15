#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.asset_consolidation_repair_proposal import (  # noqa: E402
    build_asset_consolidation_repair_proposal,
    render_asset_consolidation_repair_proposal_markdown,
)
from sourcing_agent.settings import load_settings  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only W5c.2 authoritative-source repair proposal from a W5c plan JSON. "
            "It verifies local payload-backed candidates and does not mutate registry pointers or files."
        )
    )
    parser.add_argument("--plan-json", required=True, help="Path to asset consolidation plan JSON.")
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument("--candidate-limit-per-company", type=int, default=5)
    parser.add_argument("--identity-candidate-limit", type=int, default=50000)
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when no verified payload-backed candidate exists for a missing-reference company.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    plan_path = Path(str(args.plan_json)).expanduser()
    if not plan_path.is_absolute():
        plan_path = (PROJECT_ROOT / plan_path).resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    proposal = build_asset_consolidation_repair_proposal(
        plan=plan,
        runtime_dir=runtime_dir,
        candidate_limit_per_company=max(1, int(args.candidate_limit_per_company or 5)),
        identity_candidate_limit=max(1, int(args.identity_candidate_limit or 50000)),
    )
    payload = json.dumps(proposal, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_asset_consolidation_repair_proposal_markdown(proposal), encoding="utf-8")
    if args.strict and str(proposal.get("status") or "").startswith("blocked"):
        return 1
    return 0


def _resolve_runtime_dir(raw_value: str) -> Path:
    if str(raw_value or "").strip():
        candidate = Path(str(raw_value)).expanduser()
        if not candidate.is_absolute():
            candidate = (PROJECT_ROOT / candidate).resolve()
        os.environ["SOURCING_RUNTIME_DIR"] = str(candidate)
        return candidate
    return load_settings(PROJECT_ROOT).runtime_dir


if __name__ == "__main__":
    raise SystemExit(main())
