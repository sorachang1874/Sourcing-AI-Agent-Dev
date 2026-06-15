#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.runtime_asset_supersession_cold_manifest import (  # noqa: E402
    build_runtime_asset_supersession_cold_manifest,
    dumps_manifest,
    render_runtime_asset_supersession_cold_manifest_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only cold-storage planning/proof manifest from a runtime supersession audit. "
            "This does not move, delete, compress, or exclude any runtime/output path from reuse."
        )
    )
    parser.add_argument("--supersession-json", required=True, help="Path to runtime_asset_supersession_audit_v1 JSON.")
    parser.add_argument("--workspace-root", default=".", help="Workspace root. Defaults to current repository root.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument(
        "--no-file-sha256",
        action="store_true",
        help="Skip per-file sha256. The result remains planning-only and is not proof-ready for local removal.",
    )
    parser.add_argument(
        "--max-files-per-artifact",
        type=int,
        default=50_000,
        help="Fail closed for an artifact when more files than this must be listed.",
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        default=0,
        help="Maximum number of supersession candidates to scan. 0 means no entry limit.",
    )
    parser.add_argument(
        "--target-bytes",
        type=int,
        default=0,
        help="Approximate source-size budget for selected candidates. 0 means no byte limit.",
    )
    parser.add_argument(
        "--target-gib",
        type=float,
        default=0.0,
        help="Convenience source-size budget in GiB. Ignored when --target-bytes is set.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless every selected artifact is proof-ready and no candidate is blocked.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    supersession_path = _resolve_path(args.supersession_json)
    workspace_root = _resolve_path(args.workspace_root)
    target_bytes = int(args.target_bytes or 0)
    if target_bytes <= 0 and float(args.target_gib or 0.0) > 0:
        target_bytes = int(float(args.target_gib) * 1024 * 1024 * 1024)
    report = json.loads(supersession_path.read_text(encoding="utf-8"))
    manifest = build_runtime_asset_supersession_cold_manifest(
        supersession_report=report,
        workspace_root=workspace_root,
        include_file_sha256=not bool(args.no_file_sha256),
        max_files_per_artifact=int(args.max_files_per_artifact or 50_000),
        max_entries=int(args.max_entries or 0),
        target_bytes=target_bytes,
    )
    payload = dumps_manifest(manifest)
    if str(args.output_json or "").strip():
        output_path = _resolve_output_path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if str(args.output_md or "").strip():
        output_path = _resolve_output_path(args.output_md)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_runtime_asset_supersession_cold_manifest_markdown(manifest), encoding="utf-8")
    summary = dict(manifest.get("summary") or {})
    if args.strict and (
        int(summary.get("proof_ready_artifact_count") or 0) != int(summary.get("selected_artifact_count") or 0)
        or int(summary.get("blocked_artifact_count") or 0) > 0
        or int(summary.get("deferred_artifact_count") or 0) > 0
    ):
        return 1
    return 0


def _resolve_path(value: str) -> Path:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _resolve_output_path(value: str) -> Path:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


if __name__ == "__main__":
    raise SystemExit(main())
