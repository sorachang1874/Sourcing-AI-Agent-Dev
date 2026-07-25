#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.runtime_asset_retention_prune import (  # noqa: E402
    TTL_LOCAL_REBUILDABLE_PRUNE_ROOT,
    apply_runtime_asset_prune_plan,
    build_runtime_asset_prune_plan,
    build_runtime_asset_prune_plan_from_cold_bundle_manifest,
    build_ttl_local_rebuildable_prune_plan,
    dumps_report,
    render_runtime_asset_prune_apply_markdown,
    render_runtime_asset_prune_plan_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build or apply a reviewed runtime/output retention prune manifest. "
            "Default mode is dry-run and never deletes files."
        )
    )
    parser.add_argument("--workspace-root", default=".", help="Repository root. Defaults to current directory.")
    parser.add_argument("--audit-json", default="", help="Read-only runtime retention audit JSON to convert to a plan.")
    parser.add_argument(
        "--cold-bundle-manifest",
        default="",
        help="Verified runtime supersession cold bundle manifest to convert to an exact prune plan.",
    )
    parser.add_argument("--plan-json", default="", help="Existing prune plan JSON to dry-run/apply.")
    parser.add_argument(
        "--policy",
        default="",
        choices=["", "ttl-local-rebuildable"],
        help=(
            "Optional prune policy. 'ttl-local-rebuildable' scans top-level per-run directories under "
            "runtime/test_env older than --min-age-days and waives the review-artifact gate for that "
            "root only. Dry-run remains the default; --apply and --reviewed are still required."
        ),
    )
    parser.add_argument(
        "--prune-root",
        default=TTL_LOCAL_REBUILDABLE_PRUNE_ROOT,
        help="Prune root for --policy ttl-local-rebuildable. Must be runtime/test_env; other roots are refused.",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown output path.")
    parser.add_argument("--min-age-days", type=int, default=10, help="Skip directories newer than this age.")
    parser.add_argument("--target-free-gb", type=float, default=0.0, help="Optional planned free-space target in GiB.")
    parser.add_argument("--max-entries", type=int, default=0, help="Optional maximum number of selected directories.")
    parser.add_argument(
        "--allowed-retention-class",
        action="append",
        default=[],
        help="Optional retention class to include in the plan. Repeatable.",
    )
    parser.add_argument("--review-artifact", default="", help="Expected independent review artifact path for a real apply.")
    parser.add_argument("--review-title", default="", help="Expected independent review title recorded in the artifact.")
    parser.add_argument(
        "--review-required-file",
        action="append",
        default=[],
        help="File path that must appear in the review artifact scope. Repeatable.",
    )
    parser.add_argument("--cold-copy-manifest", default="", help="Optional cold-copy/package manifest path.")
    parser.add_argument(
        "--accepted-retention-exception",
        default="",
        help="Explicit operator/user exception when local disk pressure requires pruning without cold copy.",
    )
    parser.add_argument(
        "--reuse-index-effect",
        default="none_runtime_output_only",
        help="How this prune affects reuse indexes. Defaults to no normal reuse index effect.",
    )
    parser.add_argument("--apply", action="store_true", help="Remove selected local runtime/output directories.")
    parser.add_argument("--reviewed", action="store_true", help="Required with --apply after independent review/user acceptance.")
    parser.add_argument(
        "--skip-process-check",
        action="store_true",
        help="Bypass active runtime process detection for dry-run diagnostics only. Real --apply rejects this flag.",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on blocked/partial apply.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    exit_code = 0
    workspace_root = Path(str(args.workspace_root)).expanduser()
    if not workspace_root.is_absolute():
        workspace_root = (Path.cwd() / workspace_root).resolve()
    input_count = sum(bool(value) for value in (args.audit_json, args.cold_bundle_manifest, args.plan_json))
    if args.policy == "ttl-local-rebuildable":
        if input_count != 0:
            raise SystemExit("--policy ttl-local-rebuildable does not accept --audit-json, --cold-bundle-manifest, or --plan-json")
        normalized_prune_root = Path(str(args.prune_root or "").strip()).as_posix().strip("/")
        if normalized_prune_root != TTL_LOCAL_REBUILDABLE_PRUNE_ROOT:
            raise SystemExit(f"--policy ttl-local-rebuildable only allows --prune-root {TTL_LOCAL_REBUILDABLE_PRUNE_ROOT}")
        plan = build_ttl_local_rebuildable_prune_plan(
            workspace_root=workspace_root,
            prune_root=normalized_prune_root,
            min_age_days=max(0, int(args.min_age_days or 0)),
            target_free_bytes=int(max(0.0, float(args.target_free_gb or 0.0)) * 1024 * 1024 * 1024),
            max_entries=max(0, int(args.max_entries or 0)),
        )
        if args.apply:
            report = apply_runtime_asset_prune_plan(
                plan=plan,
                workspace_root=workspace_root,
                apply=True,
                reviewed=bool(args.reviewed),
                skip_process_check=bool(args.skip_process_check),
            )
            markdown = render_runtime_asset_prune_apply_markdown(report)
            strict_failure = str(report.get("status") or "") != "applied"
            if strict_failure:
                exit_code = 1
        else:
            report = plan
            markdown = render_runtime_asset_prune_plan_markdown(report)
            strict_failure = str(report.get("status") or "") != "ready_for_review"
    elif input_count != 1:
        raise SystemExit("Provide exactly one of --audit-json, --cold-bundle-manifest, or --plan-json")
    elif args.audit_json:
        audit_path = _resolve_input_path(args.audit_json, workspace_root)
        audit_report = json.loads(audit_path.read_text(encoding="utf-8"))
        report = build_runtime_asset_prune_plan(
            audit_report=audit_report,
            workspace_root=workspace_root,
            min_age_days=max(0, int(args.min_age_days or 0)),
            target_free_bytes=int(max(0.0, float(args.target_free_gb or 0.0)) * 1024 * 1024 * 1024),
            max_entries=max(0, int(args.max_entries or 0)),
            review_artifact=str(args.review_artifact or ""),
            review_title=str(args.review_title or ""),
            review_required_files=[str(item) for item in list(args.review_required_file or [])],
            accepted_retention_exception=str(args.accepted_retention_exception or ""),
            cold_copy_manifest=str(args.cold_copy_manifest or ""),
            reuse_index_effect=str(args.reuse_index_effect or ""),
            allowed_retention_classes=[str(item) for item in list(args.allowed_retention_class or [])],
        )
        markdown = render_runtime_asset_prune_plan_markdown(report)
        strict_failure = str(report.get("status") or "") != "ready_for_review"
    elif args.cold_bundle_manifest:
        bundle_path = _resolve_input_path(args.cold_bundle_manifest, workspace_root)
        bundle_manifest = json.loads(bundle_path.read_text(encoding="utf-8"))
        report = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
            bundle_manifest=bundle_manifest,
            workspace_root=workspace_root,
            cold_copy_manifest=str(bundle_path.relative_to(workspace_root) if _is_relative_to(bundle_path, workspace_root) else bundle_path),
            review_artifact=str(args.review_artifact or ""),
            review_title=str(args.review_title or ""),
            review_required_files=[str(item) for item in list(args.review_required_file or [])],
            review_plan_artifact=_workspace_relative_output_path(str(args.output_json or ""), workspace_root),
            reuse_index_effect=str(args.reuse_index_effect or ""),
        )
        markdown = render_runtime_asset_prune_plan_markdown(report)
        strict_failure = str(report.get("status") or "") != "ready_for_review"
    else:
        plan_path = _resolve_input_path(args.plan_json, workspace_root)
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        report = apply_runtime_asset_prune_plan(
            plan=plan,
            workspace_root=workspace_root,
            apply=bool(args.apply),
            reviewed=bool(args.reviewed),
            skip_process_check=bool(args.skip_process_check),
        )
        markdown = render_runtime_asset_prune_apply_markdown(report)
        expected_status = "applied" if args.apply else "dry_run_ready"
        strict_failure = str(report.get("status") or "") != expected_status
        if args.apply and strict_failure:
            exit_code = 1
    payload = dumps_report(report)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        if not output_path.is_absolute():
            output_path = workspace_root / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if str(args.output_md or "").strip():
        output_path = Path(str(args.output_md)).expanduser()
        if not output_path.is_absolute():
            output_path = workspace_root / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown, encoding="utf-8")
    if args.strict and strict_failure:
        return 1
    return exit_code


def _resolve_input_path(raw_path: str, workspace_root: Path) -> Path:
    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute():
        path = workspace_root / path
    return path.resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _workspace_relative_output_path(raw_path: str, workspace_root: Path) -> str:
    text = str(raw_path or "").strip()
    if not text:
        return ""
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = workspace_root / path
    try:
        return path.resolve().relative_to(workspace_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
