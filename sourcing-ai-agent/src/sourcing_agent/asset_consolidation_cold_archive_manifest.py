from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import company_assets_roots, normalize_company_key, resolve_company_snapshot_dir_by_key

CONTRACT_VERSION = "asset_consolidation_cold_archive_manifest_v1"
READY_ARCHIVE_DECISION = "archive_ready_for_cold_backup_review"
DEFAULT_MAX_FILES_PER_SNAPSHOT = 10_000


def build_asset_consolidation_cold_archive_manifest(
    *,
    plan: dict[str, Any],
    runtime_dir: str | Path,
    include_file_sha256: bool = True,
    max_files_per_snapshot: int = DEFAULT_MAX_FILES_PER_SNAPSHOT,
) -> dict[str, Any]:
    """Build a non-mutating W5c cold-backup manifest from a consolidation plan.

    This is an archive review artifact, not an archive executor. It never moves,
    deletes, or marks assets excluded from reuse.
    """

    runtime_root = Path(runtime_dir).expanduser().resolve()
    normalized_max_files = max(1, int(max_files_per_snapshot or DEFAULT_MAX_FILES_PER_SNAPSHOT))
    companies = [
        _build_company_manifest(
            company=dict(company or {}),
            runtime_dir=runtime_root,
            include_file_sha256=bool(include_file_sha256),
            max_files_per_snapshot=normalized_max_files,
        )
        for company in list(plan.get("companies") or [])
        if isinstance(company, dict)
    ]
    summary = {
        "company_count": len(companies),
        "archive_candidate_count": sum(int(company.get("archive_candidate_count") or 0) for company in companies),
        "manifest_ready_snapshot_count": sum(int(company.get("manifest_ready_snapshot_count") or 0) for company in companies),
        "blocked_snapshot_count": sum(int(company.get("blocked_snapshot_count") or 0) for company in companies),
        "total_file_count": sum(int(company.get("total_file_count") or 0) for company in companies),
        "total_size_bytes": sum(int(company.get("total_size_bytes") or 0) for company in companies),
        "file_sha256_enabled": bool(include_file_sha256),
        "max_files_per_snapshot": normalized_max_files,
    }
    if summary["manifest_ready_snapshot_count"] and summary["blocked_snapshot_count"]:
        status = "partial_ready_for_cold_backup_review"
    elif summary["manifest_ready_snapshot_count"]:
        status = "ready_for_cold_backup_review"
    elif summary["archive_candidate_count"]:
        status = "blocked_no_cold_archive_ready_snapshots"
    else:
        status = "no_archive_candidates"
    return {
        "status": status,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
        "deletion_allowed": False,
        "normal_reuse_exclusion_recommended": summary["manifest_ready_snapshot_count"] > 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_dir": str(runtime_root),
        "source_plan_contract_version": _normalize_text(plan.get("contract_version")),
        "source_plan_generated_at": _normalize_text(plan.get("generated_at")),
        "summary": summary,
        "companies": companies,
        "post_manifest_gates": [
            "copy_manifest_items_to_cold_storage_or_offline_bundle",
            "verify_cold_copy_against_manifest",
            "review_normal_reuse_exclusion_plan",
            "apply_exclusion_or_file_archive_in_separate_reviewed_operation",
        ],
    }


def render_asset_consolidation_cold_archive_manifest_markdown(manifest: dict[str, Any]) -> str:
    summary = dict(manifest.get("summary") or {})
    lines = [
        "# Asset Consolidation Cold Archive Manifest",
        "",
        f"- Status: `{manifest.get('status')}`",
        f"- Read-only: `{bool(manifest.get('read_only'))}`",
        f"- Deletion allowed: `{bool(manifest.get('deletion_allowed'))}`",
        f"- Generated at: `{manifest.get('generated_at')}`",
        f"- Archive candidates: `{summary.get('archive_candidate_count', 0)}`",
        f"- Manifest-ready snapshots: `{summary.get('manifest_ready_snapshot_count', 0)}`",
        f"- Blocked snapshots: `{summary.get('blocked_snapshot_count', 0)}`",
        f"- Total files: `{summary.get('total_file_count', 0)}`",
        f"- Total bytes: `{summary.get('total_size_bytes', 0)}`",
        "",
    ]
    for company in list(manifest.get("companies") or []):
        payload = dict(company or {})
        lines.extend(
            [
                f"## {payload.get('target_company') or payload.get('company_key')}",
                "",
                f"- Collection: `{payload.get('collection_id')}`",
                f"- Archive candidates: `{payload.get('archive_candidate_count', 0)}`",
                f"- Manifest ready: `{payload.get('manifest_ready_snapshot_count', 0)}`",
                f"- Blocked: `{payload.get('blocked_snapshot_count', 0)}`",
                "",
            ]
        )
        snapshots = list(payload.get("snapshots") or [])
        if snapshots:
            lines.append("| snapshot | status | files | bytes | backup key | blockers |")
            lines.append("| --- | --- | ---: | ---: | --- | --- |")
            for snapshot in snapshots:
                snapshot_payload = dict(snapshot or {})
                lines.append(
                    "| {snapshot_id} | `{status}` | {files} | {bytes} | `{backup_key}` | {blockers} |".format(
                        snapshot_id=snapshot_payload.get("snapshot_id", ""),
                        status=snapshot_payload.get("status", ""),
                        files=snapshot_payload.get("file_count", 0),
                        bytes=snapshot_payload.get("total_size_bytes", 0),
                        backup_key=snapshot_payload.get("backup_key", ""),
                        blockers=", ".join(snapshot_payload.get("blocking_reasons") or []) or "-",
                    )
                )
            lines.append("")
    gates = list(manifest.get("post_manifest_gates") or [])
    if gates:
        lines.append("## Post-Manifest Gates")
        lines.append("")
        for gate in gates:
            lines.append(f"- `{gate}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_company_manifest(
    *,
    company: dict[str, Any],
    runtime_dir: Path,
    include_file_sha256: bool,
    max_files_per_snapshot: int,
) -> dict[str, Any]:
    company_key = _normalize_company_key(company)
    target_company = _normalize_text(company.get("target_company")) or company_key
    collection_id = _normalize_text(company.get("collection_id")) or (f"company:{company_key}" if company_key else "")
    archive_plan = dict(company.get("archive_plan") or {})
    snapshots = [
        _build_snapshot_manifest(
            company_key=company_key,
            target_company=target_company,
            collection_id=collection_id,
            candidate=dict(candidate or {}),
            runtime_dir=runtime_dir,
            include_file_sha256=include_file_sha256,
            max_files_per_snapshot=max_files_per_snapshot,
        )
        for candidate in list(archive_plan.get("candidates") or [])
        if isinstance(candidate, dict)
    ]
    ready = [snapshot for snapshot in snapshots if snapshot.get("status") == "ready_for_cold_backup_review"]
    blocked = [snapshot for snapshot in snapshots if snapshot.get("status") != "ready_for_cold_backup_review"]
    return {
        "company_key": company_key,
        "target_company": target_company,
        "collection_id": collection_id,
        "archive_candidate_count": len(snapshots),
        "manifest_ready_snapshot_count": len(ready),
        "blocked_snapshot_count": len(blocked),
        "total_file_count": sum(int(snapshot.get("file_count") or 0) for snapshot in ready),
        "total_size_bytes": sum(int(snapshot.get("total_size_bytes") or 0) for snapshot in ready),
        "snapshots": snapshots,
    }


def _build_snapshot_manifest(
    *,
    company_key: str,
    target_company: str,
    collection_id: str,
    candidate: dict[str, Any],
    runtime_dir: Path,
    include_file_sha256: bool,
    max_files_per_snapshot: int,
) -> dict[str, Any]:
    snapshot_id = _normalize_text(candidate.get("snapshot_id"))
    blocking_reasons = _candidate_blockers(candidate)
    if blocking_reasons:
        return {
            "company_key": company_key,
            "target_company": target_company,
            "collection_id": collection_id,
            "snapshot_id": snapshot_id,
            "status": "blocked_cold_backup_manifest",
            "blocking_reasons": _dedupe_strings(blocking_reasons),
            "source_dir": "",
            "source_root": "",
            "backup_key": "",
            "decision": _normalize_text(candidate.get("decision")),
            "classification": _normalize_text(candidate.get("classification")),
            "reason": _normalize_text(candidate.get("reason")),
            "overlap_status": _normalize_text(candidate.get("overlap_status")),
            "overlap_ratio": candidate.get("overlap_ratio", 0.0),
            "unique_count": _safe_int(candidate.get("unique_count")),
            "candidate_count": _safe_int(candidate.get("candidate_count")),
            "profile_detail_count": _safe_int(candidate.get("profile_detail_count")),
            "normal_reuse_exclusion_recommended": False,
            "normal_reuse_exclusion_reason": "",
            "deletion_allowed": False,
            "archive_apply_required": "separate_reviewed_copy_verify_then_exclude_or_delete_operation",
            **_empty_file_manifest(),
            "manifest_digest_sha256": "",
        }
    snapshot_dir = (
        resolve_company_snapshot_dir_by_key(
            runtime_dir,
            company_key=company_key,
            snapshot_id=snapshot_id,
            prefer_hot_cache=True,
        )
        if company_key and snapshot_id
        else None
    )
    if snapshot_dir is None:
        blocking_reasons.append("source_snapshot_dir_missing")
        file_manifest = _empty_file_manifest()
        source_dir = ""
        backup_key = ""
        source_root = ""
    else:
        source_dir = str(snapshot_dir)
        backup_key = _backup_key(snapshot_dir=snapshot_dir, runtime_dir=runtime_dir)
        source_root = _source_root_label(snapshot_dir=snapshot_dir, runtime_dir=runtime_dir)
        file_manifest = _file_manifest(
            snapshot_dir=snapshot_dir,
            include_file_sha256=include_file_sha256,
            max_files=max_files_per_snapshot,
        )
        if bool(file_manifest.get("truncated")):
            blocking_reasons.append("file_manifest_truncated")
        if int(file_manifest.get("symlink_count") or 0) > 0:
            blocking_reasons.append("snapshot_contains_symlink")
        if int(file_manifest.get("file_count") or 0) <= 0:
            blocking_reasons.append("snapshot_has_no_files")
    status = "blocked_cold_backup_manifest" if blocking_reasons else "ready_for_cold_backup_review"
    manifest_payload = {
        "company_key": company_key,
        "target_company": target_company,
        "collection_id": collection_id,
        "snapshot_id": snapshot_id,
        "status": status,
        "blocking_reasons": _dedupe_strings(blocking_reasons),
        "source_dir": source_dir,
        "source_root": source_root,
        "backup_key": backup_key,
        "decision": _normalize_text(candidate.get("decision")),
        "classification": _normalize_text(candidate.get("classification")),
        "reason": _normalize_text(candidate.get("reason")),
        "overlap_status": _normalize_text(candidate.get("overlap_status")),
        "overlap_ratio": candidate.get("overlap_ratio", 0.0),
        "unique_count": _safe_int(candidate.get("unique_count")),
        "candidate_count": _safe_int(candidate.get("candidate_count")),
        "profile_detail_count": _safe_int(candidate.get("profile_detail_count")),
        "normal_reuse_exclusion_recommended": not blocking_reasons,
        "normal_reuse_exclusion_reason": "no_increment_duplicate_subsumed_by_reference" if not blocking_reasons else "",
        "deletion_allowed": False,
        "archive_apply_required": "separate_reviewed_copy_verify_then_exclude_or_delete_operation",
        **file_manifest,
    }
    manifest_payload["manifest_digest_sha256"] = _manifest_digest(manifest_payload)
    return manifest_payload


def _candidate_blockers(candidate: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if _normalize_text(candidate.get("decision")) != READY_ARCHIVE_DECISION:
        blockers.append("archive_candidate_not_ready")
    if _normalize_text(candidate.get("overlap_status")) != "subsumed_by_reference":
        blockers.append("overlap_not_subsumed_by_reference")
    if _safe_int(candidate.get("unique_count")) != 0:
        blockers.append("unique_identities_present")
    if list(candidate.get("deletion_blockers") or []):
        blockers.append("deletion_blockers_present")
    return blockers


def _file_manifest(*, snapshot_dir: Path, include_file_sha256: bool, max_files: int) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    total_size = 0
    symlink_count = 0
    truncated = False
    for path in sorted(snapshot_dir.rglob("*")):
        try:
            if path.is_symlink():
                symlink_count += 1
                continue
            if not path.is_file():
                continue
            if len(files) >= max_files:
                truncated = True
                continue
            stat = path.stat()
        except OSError:
            truncated = True
            continue
        relative_path = path.relative_to(snapshot_dir).as_posix()
        entry = {
            "relative_path": relative_path,
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
        if include_file_sha256:
            entry["sha256"] = _file_sha256(path)
        files.append(entry)
        total_size += int(stat.st_size)
    return {
        "file_count": len(files),
        "total_size_bytes": total_size,
        "symlink_count": symlink_count,
        "truncated": truncated,
        "file_sha256_complete": bool(include_file_sha256) and not truncated and symlink_count == 0,
        "files": files,
    }


def _empty_file_manifest() -> dict[str, Any]:
    return {
        "file_count": 0,
        "total_size_bytes": 0,
        "symlink_count": 0,
        "truncated": False,
        "file_sha256_complete": False,
        "files": [],
    }


def _backup_key(*, snapshot_dir: Path, runtime_dir: Path) -> str:
    for root in company_assets_roots(runtime_dir, prefer_hot_cache=True, existing_only=False):
        try:
            return snapshot_dir.relative_to(root).as_posix()
        except ValueError:
            continue
    return snapshot_dir.name


def _source_root_label(*, snapshot_dir: Path, runtime_dir: Path) -> str:
    for root in company_assets_roots(runtime_dir, prefer_hot_cache=True, existing_only=False):
        try:
            snapshot_dir.relative_to(root)
        except ValueError:
            continue
        if root.name == "hot_cache_company_assets":
            return "hot_cache_company_assets"
        if root.name == "company_assets":
            return "company_assets"
        return root.name
    return ""


def _manifest_digest(payload: dict[str, Any]) -> str:
    digest_payload = {
        "company_key": payload.get("company_key"),
        "collection_id": payload.get("collection_id"),
        "snapshot_id": payload.get("snapshot_id"),
        "backup_key": payload.get("backup_key"),
        "decision": payload.get("decision"),
        "overlap_status": payload.get("overlap_status"),
        "file_count": payload.get("file_count"),
        "total_size_bytes": payload.get("total_size_bytes"),
        "files": [
            {
                "relative_path": file_payload.get("relative_path"),
                "size_bytes": file_payload.get("size_bytes"),
                "sha256": file_payload.get("sha256", ""),
            }
            for file_payload in list(payload.get("files") or [])
            if isinstance(file_payload, dict)
        ],
    }
    return hashlib.sha256(json.dumps(digest_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_company_key(company: dict[str, Any]) -> str:
    for value in [company.get("company_key"), company.get("target_company"), company.get("collection_id")]:
        text = _normalize_text(value)
        if text.startswith("company:"):
            text = text.split(":", 1)[1]
        normalized = normalize_company_key(text)
        if normalized:
            return normalized
    return ""


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        if value in {None, ""}:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _dedupe_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _normalize_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
