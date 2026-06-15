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

from sourcing_agent.asset_consolidation_plan import (  # noqa: E402
    build_asset_consolidation_plan,
    render_asset_consolidation_plan_markdown,
)
from sourcing_agent.settings import load_settings  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only W5c asset consolidation repair/archive plan from a W5 audit JSON report. "
            "The plan does not modify registry pointers, projections, or local asset files."
        )
    )
    parser.add_argument("--audit-json", required=True, help="Path to asset consolidation audit JSON.")
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the plan is blocked or requires hard actions.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    audit_path = Path(str(args.audit_json)).expanduser()
    if not audit_path.is_absolute():
        audit_path = (PROJECT_ROOT / audit_path).resolve()
    audit_report = json.loads(audit_path.read_text(encoding="utf-8"))
    plan = build_asset_consolidation_plan(audit_report=audit_report, runtime_dir=runtime_dir)
    payload = json.dumps(plan, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_asset_consolidation_plan_markdown(plan), encoding="utf-8")
    if args.strict and str(plan.get("status") or "").startswith("blocked"):
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
