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

from sourcing_agent.asset_consolidation_repair_apply import (  # noqa: E402
    apply_asset_consolidation_repair,
    render_asset_consolidation_repair_apply_markdown,
)
from sourcing_agent.settings import load_settings  # noqa: E402
from sourcing_agent.storage import ControlPlaneStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "W5c.3 authoritative-source repair executor. Default mode is dry-run. "
            "Only --apply --reviewed mutates canonical collection projection/pointer records."
        )
    )
    parser.add_argument("--proposal-json", required=True, help="Path to W5c.2 repair proposal JSON.")
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument(
        "--selection",
        action="append",
        default=[],
        help="Explicit company=snapshot selection. Repeatable. Preferred for multi-company repairs.",
    )
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company key/name to repair. Repeatable. Must be paired with --candidate-snapshot-id.",
    )
    parser.add_argument(
        "--candidate-snapshot-id",
        action="append",
        default=[],
        help="Candidate snapshot id to publish for the matching --company. Repeatable.",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument("--apply", action="store_true", help="Publish canonical projection/pointer records.")
    parser.add_argument(
        "--reviewed",
        action="store_true",
        help="Required with --apply to record manual acceptance of the reviewed repair candidate.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the report is blocked or --apply did not apply all requested companies.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    proposal_path = Path(str(args.proposal_json)).expanduser()
    if not proposal_path.is_absolute():
        proposal_path = (PROJECT_ROOT / proposal_path).resolve()
    proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    selections = _parse_selections(args)
    store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
    report = apply_asset_consolidation_repair(
        proposal=proposal,
        runtime_dir=runtime_dir,
        store=store,
        selections=selections,
        apply=bool(args.apply),
        manual_review_accepted=bool(args.reviewed),
        source_proposal_path=str(proposal_path),
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_asset_consolidation_repair_apply_markdown(report), encoding="utf-8")
    if args.strict and str(report.get("status") or "") != ("applied" if args.apply else "dry_run_ready"):
        return 1
    return 0


def _parse_selections(args: argparse.Namespace) -> dict[str, str]:
    selections: dict[str, str] = {}
    for raw_selection in list(args.selection or []):
        if "=" not in str(raw_selection):
            raise SystemExit(f"Invalid --selection value: {raw_selection!r}; expected company=snapshot")
        company, snapshot_id = str(raw_selection).split("=", 1)
        if not company.strip() or not snapshot_id.strip():
            raise SystemExit(f"Invalid --selection value: {raw_selection!r}; expected non-empty company and snapshot")
        selections[company.strip()] = snapshot_id.strip()
    companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
    snapshot_ids = [str(item or "").strip() for item in list(args.candidate_snapshot_id or []) if str(item or "").strip()]
    if companies or snapshot_ids:
        if len(companies) != len(snapshot_ids):
            raise SystemExit("--company and --candidate-snapshot-id must be provided the same number of times")
        for company, snapshot_id in zip(companies, snapshot_ids, strict=False):
            selections[company] = snapshot_id
    if not selections:
        raise SystemExit("At least one explicit --selection company=snapshot or --company/--candidate-snapshot-id pair is required")
    return selections


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
