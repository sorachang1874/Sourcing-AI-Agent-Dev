#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.runtime_asset_supersession_cold_bundle import (  # noqa: E402
    build_runtime_asset_supersession_cold_bundle_manifest,
    dumps_manifest,
    render_runtime_asset_supersession_cold_bundle_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a local compressed cold-bundle manifest from a proof-ready runtime supersession cold manifest. "
            "This may create archive files, but it never deletes source runtime/output directories."
        )
    )
    parser.add_argument("--proof-manifest", required=True, help="Path to runtime_asset_supersession_cold_manifest_v1 JSON.")
    parser.add_argument("--workspace-root", default=".", help="Workspace root. Defaults to current repository root.")
    parser.add_argument("--archive-root", required=True, help="Directory where cold bundle archives are written.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument("--create-archives", action="store_true", help="Create and verify tar archives.")
    parser.add_argument("--compression", default="zstd", choices=["zstd", "gzip"], help="Archive compression.")
    parser.add_argument("--compression-level", type=int, default=3, help="Compression level.")
    parser.add_argument("--max-entries", type=int, default=0, help="Maximum selected artifacts to bundle. 0 means all.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless all selected archives are ready and the manifest is apply-consumable.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    proof_path = _resolve_path(args.proof_manifest)
    workspace_root = _resolve_path(args.workspace_root)
    proof_manifest = json.loads(proof_path.read_text(encoding="utf-8"))
    manifest = build_runtime_asset_supersession_cold_bundle_manifest(
        proof_manifest=proof_manifest,
        workspace_root=workspace_root,
        archive_root=args.archive_root,
        create_archives=bool(args.create_archives),
        compression=str(args.compression or "zstd"),
        compression_level=int(args.compression_level or 3),
        max_entries=int(args.max_entries or 0),
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
        output_path.write_text(render_runtime_asset_supersession_cold_bundle_markdown(manifest), encoding="utf-8")
    if args.strict and str(manifest.get("status") or "") != "ready_for_prune_cold_copy_manifest":
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
