#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.asset_consolidation_audit import audit_asset_consolidation  # noqa: E402
from sourcing_agent.settings import load_settings  # noqa: E402
from sourcing_agent.storage import ControlPlaneStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only W5 asset consolidation audit. It reports authoritative, "
            "projection, CRM/person-asset, reusable-shard, local-only, and archive-candidate "
            "dependencies before any legacy recovery deletion or snapshot cleanup."
        )
    )
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company name/key to audit. Repeatable. Defaults to all registry/runtime companies.",
    )
    parser.add_argument("--asset-view", default="canonical_merged", help="Asset view to audit.")
    parser.add_argument("--limit", type=int, default=250, help="Maximum companies/dependency rows to include.")
    parser.add_argument(
        "--exclude-local-only",
        action="store_true",
        help="Skip local snapshot directories that have no registry row.",
    )
    parser.add_argument(
        "--include-overlap",
        action="store_true",
        help=(
            "Read bounded candidate identity sets for archive candidates and compare them against "
            "the current authoritative/source snapshots. This is offline W5b evidence, not a hot path."
        ),
    )
    parser.add_argument(
        "--overlap-candidate-limit",
        type=int,
        default=25000,
        help="Maximum candidates to read per snapshot when --include-overlap is enabled.",
    )
    parser.add_argument(
        "--overlap-snapshot-limit",
        type=int,
        default=25,
        help="Maximum archive-candidate snapshots to evaluate per company when --include-overlap is enabled.",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output path.")
    parser.add_argument(
        "--output-dir",
        default="",
        help=(
            "Optional evidence directory. When set with repeatable --company, the CLI writes "
            "one JSON/Markdown file per company as soon as that company finishes, plus a merged report."
        ),
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Write timestamped company-level progress to stderr. Useful for large offline overlap audits.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any snapshot needs review or has deletion blockers.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    # ControlPlaneStore resolves the current PG/live-mode configuration from the
    # runtime/env exactly like the application. The audit itself remains read-only.
    store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
    company_filters = [str(company or "").strip() for company in list(args.company or []) if str(company or "").strip()]
    if company_filters:
        company_reports = []
        output_dir = Path(str(args.output_dir)).expanduser() if str(args.output_dir or "").strip() else None
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
        for index, company in enumerate(company_filters, start=1):
            _progress(
                enabled=bool(args.progress),
                message=f"start company={company} ({index}/{len(company_filters)}) include_overlap={bool(args.include_overlap)}",
            )
            company_report = audit_asset_consolidation(
                runtime_dir=runtime_dir,
                store=store,
                company=company,
                asset_view=str(args.asset_view or "canonical_merged"),
                limit=max(1, int(args.limit or 250)),
                include_local_only=not bool(args.exclude_local_only),
                include_overlap=bool(args.include_overlap),
                overlap_candidate_limit=max(1, int(args.overlap_candidate_limit or 25000)),
                overlap_snapshot_limit=max(1, int(args.overlap_snapshot_limit or 25)),
            )
            company_reports.append(company_report)
            if output_dir is not None:
                _write_company_evidence(output_dir=output_dir, company=company, report=company_report)
            summary = dict(company_report.get("summary") or {})
            _progress(
                enabled=bool(args.progress),
                message=(
                    "finish company={company} snapshots={snapshots} archive_candidates={archive_candidates} "
                    "overlap_review={overlap_review}"
                ).format(
                    company=company,
                    snapshots=summary.get("snapshot_count", 0),
                    archive_candidates=summary.get("archive_candidate_count", 0),
                    overlap_review=summary.get("overlap_review_archive_candidate_count", 0),
                ),
            )
        report = _merge_company_reports(company_reports)
    else:
        _progress(
            enabled=bool(args.progress),
            message=f"start all companies include_overlap={bool(args.include_overlap)}",
        )
        report = audit_asset_consolidation(
            runtime_dir=runtime_dir,
            store=store,
            asset_view=str(args.asset_view or "canonical_merged"),
            limit=max(1, int(args.limit or 250)),
            include_local_only=not bool(args.exclude_local_only),
            include_overlap=bool(args.include_overlap),
            overlap_candidate_limit=max(1, int(args.overlap_candidate_limit or 25000)),
            overlap_snapshot_limit=max(1, int(args.overlap_snapshot_limit or 25)),
        )
        _progress(enabled=bool(args.progress), message="finish all companies")
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
        output_path.write_text(render_asset_consolidation_markdown(report), encoding="utf-8")
    if args.strict:
        summary = dict(report.get("summary") or {})
        if int(summary.get("blocked_snapshot_count") or 0) > 0 or int(summary.get("review_required_count") or 0) > 0:
            return 1
    return 0


def _write_company_evidence(*, output_dir: Path, company: str, report: dict[str, Any]) -> None:
    output_name = _safe_output_name(company)
    json_path = output_dir / f"{output_name}.json"
    md_path = output_dir / f"{output_name}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_asset_consolidation_markdown(report), encoding="utf-8")


def _progress(*, enabled: bool, message: str) -> None:
    if not enabled:
        return
    timestamp = datetime.now(timezone.utc).isoformat()
    sys.stderr.write(f"[{timestamp}] {message}\n")
    sys.stderr.flush()


def _safe_output_name(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    normalized = "_".join(part for part in normalized.split("_") if part)
    return normalized or "company"


def render_asset_consolidation_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Asset Consolidation Audit",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Read-only: `{bool(report.get('read_only'))}`",
        f"- Generated at: `{report.get('generated_at')}`",
        f"- Companies: `{summary.get('company_count', 0)}`",
        f"- Snapshots: `{summary.get('snapshot_count', 0)}`",
        f"- Archive candidates: `{summary.get('archive_candidate_count', 0)}`",
        f"- Blocked snapshots: `{summary.get('blocked_snapshot_count', 0)}`",
        f"- Review required: `{summary.get('review_required_count', 0)}`",
        f"- Overlap enabled: `{bool(summary.get('overlap_enabled'))}`",
        f"- Overlap subsumed archive candidates: `{summary.get('overlap_subsumed_archive_candidate_count', 0)}`",
        f"- Overlap review archive candidates: `{summary.get('overlap_review_archive_candidate_count', 0)}`",
        "",
    ]
    for company in list(report.get("companies") or []):
        company_payload = dict(company or {})
        lines.extend(
            [
                f"## {company_payload.get('target_company') or company_payload.get('company_key')}",
                "",
                f"- Collection: `{company_payload.get('collection_id')}`",
                f"- Authoritative snapshots: `{', '.join(company_payload.get('authoritative_snapshot_ids') or []) or '-'}`",
                f"- Selected source snapshots: `{', '.join(company_payload.get('selected_snapshot_ids') or []) or '-'}`",
                "",
                "| snapshot | classification | archive ready | blockers | candidates | profiles | reusable shards | projections | overlap | unique |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |",
            ]
        )
        for snapshot in list(company_payload.get("snapshots") or []):
            snapshot_payload = dict(snapshot or {})
            registry = dict(snapshot_payload.get("registry") or {})
            overlap = dict(snapshot_payload.get("overlap_subsumption") or {})
            lines.append(
                "| {snapshot_id} | `{classification}` | `{archive_ready}` | {blockers} | {candidates} | {profiles} | {shards} | {projections} | `{overlap_status}` | {unique_count} |".format(
                    snapshot_id=snapshot_payload.get("snapshot_id") or "",
                    classification=snapshot_payload.get("classification") or "",
                    archive_ready=bool(snapshot_payload.get("archive_ready")),
                    blockers=len(list(snapshot_payload.get("deletion_blockers") or [])),
                    candidates=registry.get("candidate_count", 0) if registry.get("present") else "-",
                    profiles=registry.get("profile_detail_count", 0) if registry.get("present") else "-",
                    shards=dict(snapshot_payload.get("shards") or {}).get("reusable_shard_count", 0),
                    projections=len(list(snapshot_payload.get("projection_dependencies") or [])),
                    overlap_status=overlap.get("status", "-"),
                    unique_count=overlap.get("unique_count", "-"),
                )
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _resolve_runtime_dir(raw_value: str) -> Path:
    if str(raw_value or "").strip():
        candidate = Path(str(raw_value)).expanduser()
        if not candidate.is_absolute():
            candidate = (PROJECT_ROOT / candidate).resolve()
        os.environ["SOURCING_RUNTIME_DIR"] = str(candidate)
        return candidate
    return load_settings(PROJECT_ROOT).runtime_dir


def _merge_company_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    companies: list[dict[str, Any]] = []
    for report in reports:
        companies.extend([dict(company or {}) for company in list(report.get("companies") or [])])
    summary = {
        "company_count": len(companies),
        "snapshot_count": sum(len(list(company.get("snapshots") or [])) for company in companies),
        "archive_candidate_count": sum(
            1
            for company in companies
            for snapshot in list(company.get("snapshots") or [])
            if dict(snapshot or {}).get("classification") == "archive_candidate_no_increment_duplicate"
        ),
        "blocked_snapshot_count": sum(
            1
            for company in companies
            for snapshot in list(company.get("snapshots") or [])
            if dict(snapshot or {}).get("deletion_blockers")
        ),
        "review_required_count": sum(
            1
            for company in companies
            for snapshot in list(company.get("snapshots") or [])
            if str(dict(snapshot or {}).get("classification") or "").startswith("review_")
        ),
        "overlap_enabled": any(bool(report.get("summary", {}).get("overlap_enabled")) for report in reports),
        "overlap_archive_candidate_count": sum(
            int(dict(report.get("summary") or {}).get("overlap_archive_candidate_count") or 0)
            for report in reports
        ),
        "overlap_subsumed_archive_candidate_count": sum(
            int(dict(report.get("summary") or {}).get("overlap_subsumed_archive_candidate_count") or 0)
            for report in reports
        ),
        "overlap_review_archive_candidate_count": sum(
            int(dict(report.get("summary") or {}).get("overlap_review_archive_candidate_count") or 0)
            for report in reports
        ),
    }
    summary["deletion_ready_snapshot_count"] = sum(
        1
        for company in companies
        for snapshot in list(company.get("snapshots") or [])
        if bool(dict(snapshot or {}).get("archive_ready")) and not dict(snapshot or {}).get("deletion_blockers")
    )
    return {
        "status": "ok",
        "read_only": True,
        "generated_at": reports[0].get("generated_at") if reports else "",
        "contract_version": "asset_consolidation_audit_v2" if summary["overlap_enabled"] else "asset_consolidation_audit_v1",
        "company_filter": [report.get("company_filter") for report in reports],
        "asset_view": reports[0].get("asset_view") if reports else "canonical_merged",
        "summary": summary,
        "companies": companies,
    }


if __name__ == "__main__":
    raise SystemExit(main())
