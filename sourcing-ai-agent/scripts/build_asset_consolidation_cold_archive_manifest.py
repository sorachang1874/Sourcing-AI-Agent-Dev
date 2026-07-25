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

from sourcing_agent.asset_consolidation_cold_archive_manifest import (  # noqa: E402
    build_asset_consolidation_cold_archive_manifest,
    render_asset_consolidation_cold_archive_manifest_markdown,
)
from sourcing_agent.settings import load_settings  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only W5c cold-backup/archive manifest from an asset consolidation plan. "
            "This does not move, delete, or mark any snapshot excluded from reuse."
        )
    )
    parser.add_argument("--plan-json", required=True, help="Path to W5c asset consolidation plan JSON.")
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument(
        "--no-file-sha256",
        action="store_true",
        help="Skip per-file sha256 hashes. File paths/sizes are still listed.",
    )
    parser.add_argument(
        "--max-files-per-snapshot",
        type=int,
        default=10_000,
        help="Fail closed for a snapshot when more files than this are discovered.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when any archive candidate is blocked from the manifest.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    plan_path = Path(str(args.plan_json)).expanduser()
    if not plan_path.is_absolute():
        plan_path = (PROJECT_ROOT / plan_path).resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    manifest = build_asset_consolidation_cold_archive_manifest(
        plan=plan,
        runtime_dir=runtime_dir,
        include_file_sha256=not bool(args.no_file_sha256),
        max_files_per_snapshot=int(args.max_files_per_snapshot or 10_000),
    )
    payload = json.dumps(manifest, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_asset_consolidation_cold_archive_manifest_markdown(manifest), encoding="utf-8")
    if args.strict and int(dict(manifest.get("summary") or {}).get("blocked_snapshot_count") or 0) > 0:
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
