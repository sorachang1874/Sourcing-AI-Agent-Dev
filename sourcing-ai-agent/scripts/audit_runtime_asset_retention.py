#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.runtime_asset_retention_audit import (  # noqa: E402
    DEFAULT_RETENTION_NAME_MARKERS,
    DEFAULT_RETENTION_SCAN_ROOTS,
    build_runtime_asset_retention_report,
    dumps_report,
    render_runtime_asset_retention_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only M0.5 runtime/output asset retention inventory. It reports large historical "
            "test/signoff artifacts and never deletes, moves, or excludes files from reuse."
        )
    )
    parser.add_argument("--workspace-root", default=".", help="Repository root. Defaults to current directory.")
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        help=f"Root directory to scan. Repeatable. Defaults to {', '.join(DEFAULT_RETENTION_SCAN_ROOTS)}.",
    )
    parser.add_argument(
        "--include-name",
        action="append",
        default=[],
        help=(
            "Directory-name marker to include, for example google or nightly. Repeatable. "
            f"Defaults to {', '.join(DEFAULT_RETENTION_NAME_MARKERS)}."
        ),
    )
    parser.add_argument("--min-size-mb", type=float, default=0.0, help="Skip directories smaller than this size.")
    parser.add_argument("--max-depth", type=int, default=1, help="Directory depth below each root to scan.")
    parser.add_argument("--sample-limit", type=int, default=250, help="Maximum directory rows to return.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_runtime_asset_retention_report(
        workspace_root=Path(str(args.workspace_root)).expanduser(),
        scan_roots=list(args.root or []) or DEFAULT_RETENTION_SCAN_ROOTS,
        include_name_markers=list(args.include_name or []) or DEFAULT_RETENTION_NAME_MARKERS,
        min_size_bytes=int(float(args.min_size_mb or 0.0) * 1024 * 1024),
        max_depth=max(1, int(args.max_depth or 1)),
        sample_limit=max(1, int(args.sample_limit or 250)),
    )
    payload = dumps_report(report)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_runtime_asset_retention_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
