#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sourcing_agent.scripted_smoke_signoff import (
    build_scripted_smoke_signoff_report,
    load_optional_json,
    load_scripted_smoke_records,
    render_scripted_smoke_signoff_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review a scripted smoke report before handing the workflow to manual browser testing. "
            "Blocking findings return a non-zero exit code."
        )
    )
    parser.add_argument("--report-json", required=True, help="Per-case smoke report JSON from run_simulate_smoke_matrix.py.")
    parser.add_argument("--summary-json", default="", help="Optional aggregate smoke summary JSON.")
    parser.add_argument("--output-json", default="", help="Optional path to write the signoff JSON report.")
    parser.add_argument("--output-md", default="", help="Optional path to write the signoff Markdown report.")
    parser.add_argument(
        "--expected-provider-mode",
        default="scripted",
        help="Expected no-cost provider mode for all provider invocations. Defaults to scripted.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    records = load_scripted_smoke_records(args.report_json)
    summary = load_optional_json(args.summary_json)
    report = build_scripted_smoke_signoff_report(
        records=records,
        summary=summary,
        expected_provider_mode=str(args.expected_provider_mode or "scripted"),
    )
    output_json = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_json, encoding="utf-8")
    else:
        sys.stdout.write(output_json)
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_scripted_smoke_signoff_markdown(report), encoding="utf-8")
    return 1 if report.get("blocking_findings") else 0


if __name__ == "__main__":
    raise SystemExit(main())
