#!/usr/bin/env python3
"""Build a deployment asset migration manifest for ECS/hosted runtimes.

The manifest is intentionally read-only. It does not upload, delete, or rewrite
any runtime assets. Its purpose is to make production migration choices explicit:
which snapshot should be copied to ECS, which older snapshots should be archived,
and where stale latest_snapshot.json pointers disagree with Postgres registry
authority.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.asset_paths import normalize_company_key  # noqa: E402
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn  # noqa: E402


DEFAULT_RUNTIME_DIR = PROJECT_ROOT / "runtime"
DEFAULT_OUTPUT_DIR = DEFAULT_RUNTIME_DIR / "deployment"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a read-only ECS asset migration manifest from local runtime "
            "assets plus optional Postgres control-plane registry state."
        )
    )
    parser.add_argument("--runtime-dir", default=str(DEFAULT_RUNTIME_DIR), help="Local runtime directory to inspect.")
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_DSN", ""),
        help=(
            "Optional Postgres DSN. Defaults to SOURCING_CONTROL_PLANE_POSTGRES_DSN, "
            "then repo/local .local-postgres.env discovery."
        ),
    )
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company key/name to include. Repeatable. Defaults to every company found in registry/runtime.",
    )
    parser.add_argument(
        "--prefer-fullest-company",
        action="append",
        default=[],
        help=(
            "Company key/name where production migration should choose the fullest local/registry snapshot "
            "instead of the current authoritative registry row. Useful for repeated no-increment baselines."
        ),
    )
    parser.add_argument(
        "--include-empty-companies",
        action="store_true",
        help="Include companies that have no candidate-bearing snapshot.",
    )
    parser.add_argument(
        "--include-local-only-companies",
        action="store_true",
        help="Include local runtime companies that have no organization_asset_registry rows. Defaults off to avoid smoke/test assets.",
    )
    parser.add_argument(
        "--skip-size-scan",
        action="store_true",
        help="Skip recursive byte-size scans for faster metadata-only manifests.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for JSON/Markdown outputs.",
    )
    parser.add_argument(
        "--json-output",
        default="",
        help="Optional explicit JSON output path.",
    )
    parser.add_argument(
        "--markdown-output",
        default="",
        help="Optional explicit Markdown output path.",
    )
    parser.add_argument(
        "--rsync-list-output",
        default="",
        help=(
            "Optional explicit rsync --files-from output path. Defaults to "
            "runtime/deployment/ecs_asset_rsync_all_files_<timestamp>.txt."
        ),
    )
    parser.add_argument(
        "--skip-rsync-file-list",
        action="store_true",
        help="Skip writing the per-file rsync --files-from list for selected snapshots.",
    )
    return parser


def _utc_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            candidate = Path(dirpath) / filename
            try:
                total += candidate.stat().st_size
            except OSError:
                pass
    return total


def _candidate_count_from_payload(payload: dict[str, Any]) -> int:
    candidates = payload.get("candidates")
    if isinstance(candidates, list):
        return len(candidates)
    for key in ("candidate_count", "total_candidates"):
        if payload.get(key) is not None:
            return _safe_int(payload.get(key), -1)
    return -1


def _profile_count_from_payload(payload: dict[str, Any]) -> int:
    for key in ("profile_detail_count", "profile_fetch_total", "linkedin_profile_count"):
        if payload.get(key) is not None:
            return _safe_int(payload.get(key), -1)
    return -1


def _snapshot_stats(snapshot_dir: Path, *, scan_size: bool) -> dict[str, Any]:
    candidate_count = -1
    profile_detail_count = -1
    artifact_files: list[str] = []
    for relpath in (
        "candidate_documents.json",
        "normalized_artifacts/canonical_merged/manifest.json",
        "artifact_summary.json",
        "manifest.json",
    ):
        path = snapshot_dir / relpath
        if not path.exists():
            continue
        artifact_files.append(relpath)
        payload = _load_json(path)
        if candidate_count < 0:
            candidate_count = _candidate_count_from_payload(payload)
        if profile_detail_count < 0:
            profile_detail_count = _profile_count_from_payload(payload)
    return {
        "snapshot_id": snapshot_dir.name,
        "path": str(snapshot_dir),
        "candidate_count": candidate_count,
        "profile_detail_count": profile_detail_count,
        "size_bytes": _dir_size_bytes(snapshot_dir) if scan_size else 0,
        "artifact_files": artifact_files,
    }


def _latest_snapshot_id(company_dir: Path) -> str:
    payload = _load_json(company_dir / "latest_snapshot.json")
    return str(payload.get("snapshot_id") or payload.get("latest_snapshot_id") or "").strip()


def _local_inventory(runtime_dir: Path, *, scan_size: bool) -> dict[str, dict[str, Any]]:
    company_root = runtime_dir / "company_assets"
    inventory: dict[str, dict[str, Any]] = {}
    if not company_root.exists():
        return inventory
    for company_dir in sorted(path for path in company_root.iterdir() if path.is_dir()):
        company_key = normalize_company_key(company_dir.name)
        snapshots = [
            _snapshot_stats(snapshot_dir, scan_size=scan_size)
            for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir())
        ]
        total_size = sum(_safe_int(snapshot.get("size_bytes")) for snapshot in snapshots)
        inventory[company_key] = {
            "company_key": company_key,
            "company_dir": str(company_dir),
            "latest_snapshot_id": _latest_snapshot_id(company_dir),
            "snapshot_count": len(snapshots),
            "total_size_bytes": total_size,
            "snapshots": snapshots,
        }
    return inventory


def _load_registry_rows(dsn: str) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    if not dsn:
        return [], {}
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as exc:  # pragma: no cover - depends on local venv
        raise SystemExit(f"psycopg is required for --postgres-dsn: {exc}") from exc

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  target_company,
                  company_key,
                  snapshot_id,
                  asset_view,
                  status,
                  authoritative,
                  candidate_count,
                  profile_detail_count,
                  source_snapshot_count,
                  current_lane_effective_candidate_count,
                  former_lane_effective_candidate_count,
                  completeness_score,
                  updated_at
                FROM organization_asset_registry
                WHERE asset_view = 'canonical_merged'
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT company_key, snapshot_id, COUNT(*) AS view_count, MAX(updated_at) AS max_updated_at
                FROM job_result_views
                GROUP BY company_key, snapshot_id
                """
            )
            view_counts = {
                (normalize_company_key(row.get("company_key")), str(row.get("snapshot_id") or "").strip()): dict(row)
                for row in cur.fetchall()
            }
    return rows, view_counts


def _registry_score(row: dict[str, Any]) -> tuple[int, int, int, int, float, int, str]:
    current_count = _safe_int(row.get("current_lane_effective_candidate_count"))
    former_count = _safe_int(row.get("former_lane_effective_candidate_count"))
    return (
        _safe_int(row.get("candidate_count")),
        _safe_int(row.get("profile_detail_count")),
        _safe_int(row.get("source_snapshot_count")),
        current_count + former_count,
        _safe_float(row.get("completeness_score")),
        int(bool(row.get("authoritative"))),
        str(row.get("updated_at") or ""),
    )


def _snapshot_score(snapshot: dict[str, Any]) -> tuple[int, int, int, str]:
    return (
        _safe_int(snapshot.get("candidate_count"), -1),
        _safe_int(snapshot.get("profile_detail_count"), -1),
        _safe_int(snapshot.get("size_bytes")),
        str(snapshot.get("snapshot_id") or ""),
    )


def _row_summary(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "target_company": row.get("target_company") or "",
        "company_key": normalize_company_key(row.get("company_key") or row.get("target_company") or ""),
        "snapshot_id": str(row.get("snapshot_id") or "").strip(),
        "asset_view": row.get("asset_view") or "canonical_merged",
        "status": row.get("status") or "",
        "authoritative": bool(row.get("authoritative")),
        "candidate_count": _safe_int(row.get("candidate_count"), -1),
        "profile_detail_count": _safe_int(row.get("profile_detail_count"), -1),
        "source_snapshot_count": _safe_int(row.get("source_snapshot_count"), 0),
        "current_lane_effective_candidate_count": _safe_int(row.get("current_lane_effective_candidate_count"), 0),
        "former_lane_effective_candidate_count": _safe_int(row.get("former_lane_effective_candidate_count"), 0),
        "completeness_score": _safe_float(row.get("completeness_score"), 0.0),
        "updated_at": str(row.get("updated_at") or ""),
    }


def _build_company_plan(
    *,
    company_key: str,
    inventory: dict[str, Any],
    registry_rows: list[dict[str, Any]],
    job_view_counts: dict[tuple[str, str], dict[str, Any]],
    prefer_fullest: bool,
) -> dict[str, Any]:
    snapshots = list(inventory.get("snapshots") or [])
    registry_rows_sorted = sorted(registry_rows, key=_registry_score, reverse=True)
    authoritative_row = next((row for row in registry_rows_sorted if bool(row.get("authoritative"))), None)
    fullest_registry_row = registry_rows_sorted[0] if registry_rows_sorted else None
    fullest_local_snapshot = max(snapshots, key=_snapshot_score) if snapshots else {}
    selected_row = fullest_registry_row if prefer_fullest and fullest_registry_row else authoritative_row or fullest_registry_row
    selected_snapshot_id = str((selected_row or {}).get("snapshot_id") or "").strip()
    selected_source = "registry_authoritative"
    if prefer_fullest and selected_row:
        selected_source = "fullest_registry_preferred"
    elif not authoritative_row and selected_row:
        selected_source = "fullest_registry_no_authoritative"
    if not selected_snapshot_id and fullest_local_snapshot:
        selected_snapshot_id = str(fullest_local_snapshot.get("snapshot_id") or "").strip()
        selected_source = "fullest_local_no_registry"
    snapshot_by_id = {str(item.get("snapshot_id") or ""): dict(item) for item in snapshots}
    registry_by_id = {str(item.get("snapshot_id") or "").strip(): dict(item) for item in registry_rows_sorted}
    selected_local = snapshot_by_id.get(selected_snapshot_id, {})
    selected_registry = registry_by_id.get(selected_snapshot_id, {})
    latest_snapshot_id = str(inventory.get("latest_snapshot_id") or "").strip()

    archive_candidates = []
    for snapshot in snapshots:
        snapshot_id = str(snapshot.get("snapshot_id") or "").strip()
        if snapshot_id == selected_snapshot_id:
            continue
        views = dict(job_view_counts.get((company_key, snapshot_id)) or {})
        archive_candidates.append(
            {
                "snapshot_id": snapshot_id,
                "candidate_count": _safe_int(snapshot.get("candidate_count"), -1),
                "profile_detail_count": _safe_int(snapshot.get("profile_detail_count"), -1),
                "size_bytes": _safe_int(snapshot.get("size_bytes")),
                "job_result_view_count": _safe_int(views.get("view_count")),
                "max_job_result_view_updated_at": str(views.get("max_updated_at") or ""),
                "archive_status": "review_required" if _safe_int(views.get("view_count")) else "cold_archive_candidate",
            }
        )
    archive_candidates.sort(key=lambda item: (_safe_int(item.get("job_result_view_count")), _safe_int(item.get("size_bytes"))), reverse=True)

    return {
        "company_key": company_key,
        "local_snapshot_count": _safe_int(inventory.get("snapshot_count")),
        "local_total_size_bytes": _safe_int(inventory.get("total_size_bytes")),
        "latest_snapshot_id": latest_snapshot_id,
        "latest_pointer_stale": bool(selected_snapshot_id and latest_snapshot_id and latest_snapshot_id != selected_snapshot_id),
        "authoritative_registry": _row_summary(authoritative_row),
        "fullest_registry": _row_summary(fullest_registry_row),
        "fullest_local_snapshot": fullest_local_snapshot,
        "selected_snapshot_id": selected_snapshot_id,
        "selected_source": selected_source,
        "selected_registry": _row_summary(selected_registry),
        "selected_local_snapshot": selected_local,
        "prefer_fullest": bool(prefer_fullest),
        "archive_candidate_count": len(archive_candidates),
        "archive_size_bytes": sum(_safe_int(item.get("size_bytes")) for item in archive_candidates),
        "archive_candidates": archive_candidates,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ECS Asset Migration Manifest",
        "",
        "> Status: Generated read-only migration manifest. Regenerate from `scripts/build_ecs_asset_migration_manifest.py` before a new ECS migration.",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Runtime dir: `{payload.get('runtime_dir')}`",
        f"- Company count: `{payload.get('summary', {}).get('company_count')}`",
        f"- Selected migration size: `{payload.get('summary', {}).get('selected_size_mb')} MB`",
        f"- Archive candidate size: `{payload.get('summary', {}).get('archive_size_mb')} MB`",
        "",
        "## Production Migration Entries",
        "",
        "| Company | Selected snapshot | Source | Candidates | Profiles | Size MB | Stale latest? |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for entry in payload.get("migration_entries") or []:
        lines.append(
            "| {company} | `{snapshot}` | {source} | {candidates} | {profiles} | {size} | {stale} |".format(
                company=entry.get("company_key", ""),
                snapshot=entry.get("selected_snapshot_id", ""),
                source=entry.get("selected_source", ""),
                candidates=entry.get("candidate_count", -1),
                profiles=entry.get("profile_detail_count", -1),
                size=entry.get("size_mb", 0),
                stale="yes" if entry.get("latest_pointer_stale") else "no",
            )
        )
    lines.extend(
        [
            "",
            "## Archive Review",
            "",
            "| Company | Local snapshots | Archive candidates | Archive size MB | Notes |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for company in payload.get("companies") or []:
        notes = []
        if company.get("latest_pointer_stale"):
            notes.append("latest pointer stale")
        if company.get("prefer_fullest"):
            notes.append("fullest baseline preferred")
        lines.append(
            "| {company_key} | {snaps} | {archive_count} | {archive_size} | {notes} |".format(
                company_key=company.get("company_key", ""),
                snaps=company.get("local_snapshot_count", 0),
                archive_count=company.get("archive_candidate_count", 0),
                archive_size=round(_safe_int(company.get("archive_size_bytes")) / 1024 / 1024, 1),
                notes=", ".join(notes),
            )
        )
    lines.extend(
        [
            "",
            "## Rules",
            "",
            "- This manifest is read-only and must not be treated as a deletion command.",
            "- Production migration should copy selected snapshots plus control-plane state, then run registry/profile backfills and serving-view audits on ECS.",
            "- Archive candidates with job_result_view references require historical replay review before removal from a hot production runtime.",
            "- `latest_snapshot.json` is a compatibility pointer only; hosted authority should come from Postgres registries and result views.",
            "",
        ]
    )
    return "\n".join(lines)


def _selected_snapshot_file_list(payload: dict[str, Any]) -> list[str]:
    runtime_dir = Path(str(payload.get("runtime_dir") or "")).expanduser().resolve()
    files: list[str] = []
    for entry in payload.get("migration_entries") or []:
        local_path = str(entry.get("local_path") or "").strip()
        if not local_path:
            continue
        snapshot_dir = Path(local_path).expanduser().resolve()
        try:
            snapshot_dir.relative_to(runtime_dir)
        except ValueError:
            continue
        for dirpath, _, filenames in os.walk(snapshot_dir):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                try:
                    files.append(file_path.relative_to(runtime_dir).as_posix())
                except ValueError:
                    continue
    return sorted(set(files))


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    runtime_dir = Path(args.runtime_dir).expanduser().resolve()
    postgres_dsn = str(args.postgres_dsn or "").strip() or resolve_control_plane_postgres_dsn(PROJECT_ROOT)
    include_companies = {normalize_company_key(item) for item in list(args.company or []) if str(item or "").strip()}
    prefer_fullest_companies = {
        normalize_company_key(item) for item in list(args.prefer_fullest_company or []) if str(item or "").strip()
    }
    inventory = _local_inventory(runtime_dir, scan_size=not bool(args.skip_size_scan))
    registry_rows, job_view_counts = _load_registry_rows(postgres_dsn)
    rows_by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in registry_rows:
        company_key = normalize_company_key(row.get("company_key") or row.get("target_company") or "")
        if company_key:
            row["company_key"] = company_key
            rows_by_company[company_key].append(row)
    company_keys = set(rows_by_company)
    if bool(args.include_local_only_companies):
        company_keys |= set(inventory)
    if include_companies:
        company_keys &= include_companies
    companies = []
    for company_key in sorted(company_keys):
        plan = _build_company_plan(
            company_key=company_key,
            inventory=dict(inventory.get(company_key) or {}),
            registry_rows=list(rows_by_company.get(company_key) or []),
            job_view_counts=job_view_counts,
            prefer_fullest=company_key in prefer_fullest_companies,
        )
        if not bool(args.include_empty_companies) and not plan.get("selected_snapshot_id"):
            continue
        companies.append(plan)
    migration_entries = []
    for company in companies:
        selected = dict(company.get("selected_local_snapshot") or {})
        registry = dict(company.get("selected_registry") or company.get("authoritative_registry") or company.get("fullest_registry") or {})
        if not company.get("selected_snapshot_id"):
            continue
        migration_entries.append(
            {
                "company_key": company.get("company_key"),
                "selected_snapshot_id": company.get("selected_snapshot_id"),
                "selected_source": company.get("selected_source"),
                "latest_pointer_stale": bool(company.get("latest_pointer_stale")),
                "candidate_count": _safe_int(registry.get("candidate_count"), _safe_int(selected.get("candidate_count"), -1)),
                "profile_detail_count": _safe_int(
                    registry.get("profile_detail_count"), _safe_int(selected.get("profile_detail_count"), -1)
                ),
                "size_bytes": _safe_int(selected.get("size_bytes")),
                "size_mb": round(_safe_int(selected.get("size_bytes")) / 1024 / 1024, 1),
                "local_path": selected.get("path") or "",
            }
        )
    selected_size = sum(_safe_int(entry.get("size_bytes")) for entry in migration_entries)
    archive_size = sum(_safe_int(company.get("archive_size_bytes")) for company in companies)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_dir": str(runtime_dir),
        "postgres_dsn_present": bool(postgres_dsn),
        "selection_policy": {
            "default": "registry_authoritative",
            "prefer_fullest_companies": sorted(prefer_fullest_companies),
            "latest_snapshot_json_role": "legacy_compatibility_pointer_not_hosted_authority",
        },
        "summary": {
            "company_count": len(companies),
            "migration_entry_count": len(migration_entries),
            "selected_size_bytes": selected_size,
            "selected_size_mb": round(selected_size / 1024 / 1024, 1),
            "archive_size_bytes": archive_size,
            "archive_size_mb": round(archive_size / 1024 / 1024, 1),
            "stale_latest_pointer_count": sum(1 for company in companies if company.get("latest_pointer_stale")),
        },
        "migration_entries": migration_entries,
        "companies": companies,
    }


def main() -> int:
    args = _build_parser().parse_args()
    payload = build_manifest(args)
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    token = _utc_token()
    json_path = Path(args.json_output).expanduser() if args.json_output else output_dir / f"ecs_asset_migration_manifest_{token}.json"
    markdown_path = (
        Path(args.markdown_output).expanduser()
        if args.markdown_output
        else output_dir / f"ecs_asset_migration_manifest_{token}.md"
    )
    rsync_list_path = (
        Path(args.rsync_list_output).expanduser()
        if args.rsync_list_output
        else output_dir / f"ecs_asset_rsync_all_files_{token}.txt"
    )
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    result = {"json": str(json_path), "markdown": str(markdown_path), "summary": payload["summary"]}
    if not bool(args.skip_rsync_file_list):
        rsync_files = _selected_snapshot_file_list(payload)
        rsync_list_path.write_text("".join(f"{line}\n" for line in rsync_files), encoding="utf-8")
        result["rsync_file_list"] = str(rsync_list_path)
        result["rsync_file_count"] = len(rsync_files)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
