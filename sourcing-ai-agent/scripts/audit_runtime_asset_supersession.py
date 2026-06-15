#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.runtime_asset_supersession_audit import (  # noqa: E402
    build_runtime_asset_supersession_report,
    dumps_report,
    render_runtime_asset_supersession_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only supersession audit for high-proof runtime/output artifacts. "
            "It identifies older signoff/phase artifacts that may be superseded by a newer family reference, "
            "but never deletes, moves, or excludes anything from reuse."
        )
    )
    parser.add_argument("--retention-json", required=True, help="Runtime retention audit JSON.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    retention_path = Path(str(args.retention_json)).expanduser()
    if not retention_path.is_absolute():
        retention_path = (PROJECT_ROOT / retention_path).resolve()
    retention_report = json.loads(retention_path.read_text(encoding="utf-8"))
    report = build_runtime_asset_supersession_report(retention_report=retention_report)
    payload = dumps_report(report)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        if not output_path.is_absolute():
            output_path = (PROJECT_ROOT / output_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        if not output_path.is_absolute():
            output_path = (PROJECT_ROOT / output_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_runtime_asset_supersession_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

