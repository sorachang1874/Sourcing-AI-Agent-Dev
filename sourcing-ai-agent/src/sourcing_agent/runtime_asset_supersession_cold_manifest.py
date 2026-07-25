from __future__ import annotations

import hashlib
import json
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = "runtime_asset_supersession_cold_manifest_v1"
SOURCE_CONTRACT_VERSION = "runtime_asset_supersession_audit_v1"
ALLOWED_SCAN_ROOTS = ("runtime/test_env", "output")
READY_SUPERSESSION_STATUS = "supersession_review_candidate"
DEFAULT_MAX_FILES_PER_ARTIFACT = 50_000


def build_runtime_asset_supersession_cold_manifest(
    *,
    supersession_report: dict[str, Any],
    workspace_root: str | Path,
    include_file_sha256: bool = True,
    max_files_per_artifact: int = DEFAULT_MAX_FILES_PER_ARTIFACT,
    max_entries: int = 0,
    target_bytes: int = 0,
) -> dict[str, Any]:
    """Build a read-only cold-storage manifest for superseded runtime/output artifacts.

    The manifest is a review artifact only. It never deletes, moves, compresses,
    or excludes any runtime/output path from reuse.
    """

    root = Path(workspace_root).expanduser().resolve()
    normalized_max_files = max(1, int(max_files_per_artifact or DEFAULT_MAX_FILES_PER_ARTIFACT))
    normalized_max_entries = max(0, int(max_entries or 0))
    normalized_target_bytes = max(0, int(target_bytes or 0))
    source_contract_valid = _source_contract_valid(supersession_report)

    all_artifacts = [dict(item or {}) for item in list(supersession_report.get("artifacts") or []) if isinstance(item, dict)]
    artifacts_by_path = {str(item.get("path") or ""): item for item in all_artifacts}
    artifacts_by_relative_path: dict[str, dict[str, Any]] = {}
    for item in all_artifacts:
        resolved = _resolve_artifact_path(str(item.get("path") or ""), workspace_root=root)
        if resolved.get("relative_path"):
            artifacts_by_relative_path[str(resolved["relative_path"])] = item

    candidate_rows = [
        item
        for item in all_artifacts
        if str(item.get("supersession_status") or "") == READY_SUPERSESSION_STATUS
    ]
    candidate_rows.sort(key=lambda item: (_safe_int(item.get("size_bytes")), str(item.get("path") or "")), reverse=True)

    selected_count = 0
    selected_bytes = 0
    manifest_artifacts: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        planned_size = _safe_int(candidate.get("size_bytes"))
        if normalized_max_entries and selected_count >= normalized_max_entries:
            manifest_artifacts.append(_deferred_artifact(candidate, "max_entries_limit_deferred"))
            continue
        if normalized_target_bytes and selected_count > 0 and selected_bytes + planned_size > normalized_target_bytes:
            manifest_artifacts.append(_deferred_artifact(candidate, "target_bytes_limit_deferred"))
            continue

        artifact = _build_artifact_manifest(
            candidate=candidate,
            artifacts_by_path=artifacts_by_path,
            artifacts_by_relative_path=artifacts_by_relative_path,
            workspace_root=root,
            source_contract_valid=source_contract_valid,
            include_file_sha256=bool(include_file_sha256),
            max_files_per_artifact=normalized_max_files,
        )
        manifest_artifacts.append(artifact)
        selected_count += 1
        selected_bytes += planned_size

    summary = _build_summary(
        manifest_artifacts,
        candidate_count=len(candidate_rows),
        include_file_sha256=bool(include_file_sha256),
        max_files_per_artifact=normalized_max_files,
        max_entries=normalized_max_entries,
        target_bytes=normalized_target_bytes,
        source_contract_valid=source_contract_valid,
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "status": _status(summary),
        "read_only": True,
        "deletion_allowed": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(root),
        "allowed_scan_roots": list(ALLOWED_SCAN_ROOTS),
        "source_supersession_contract_version": str(supersession_report.get("contract_version") or ""),
        "source_supersession_status": str(supersession_report.get("status") or ""),
        "source_contract_valid": source_contract_valid,
        "source_report_deletion_allowed": supersession_report.get("deletion_allowed"),
        "source_report_read_only": supersession_report.get("read_only"),
        "source_report_source_contract_valid": supersession_report.get("source_contract_valid"),
        "summary": summary,
        "artifacts": manifest_artifacts,
        "post_manifest_gates": [
            "manually_confirm_family_reference_supersession_for_selected_items",
            "copy_manifest_items_to_external_or_offline_cold_storage",
            "verify_cold_copy_against_manifest_sha256_or_bundle_hash",
            "independent_review_gate_before_any_local_removal",
            "apply_archive_or_remove_local_copy_in_separate_reviewed_operation",
        ],
    }


def render_runtime_asset_supersession_cold_manifest_markdown(manifest: dict[str, Any]) -> str:
    summary = dict(manifest.get("summary") or {})
    lines = [
        "# Runtime Asset Supersession Cold Manifest",
        "",
        f"- Status: `{manifest.get('status')}`",
        f"- Read-only: `{bool(manifest.get('read_only'))}`",
        f"- Deletion allowed: `{bool(manifest.get('deletion_allowed'))}`",
        f"- Generated at: `{manifest.get('generated_at')}`",
        f"- Source contract valid: `{bool(manifest.get('source_contract_valid'))}`",
        f"- Supersession candidates: `{summary.get('supersession_candidate_count', 0)}`",
        f"- Proof-ready artifacts: `{summary.get('proof_ready_artifact_count', 0)}`",
        f"- Planning-ready artifacts: `{summary.get('planning_ready_artifact_count', 0)}`",
        f"- Blocked artifacts: `{summary.get('blocked_artifact_count', 0)}`",
        f"- Deferred artifacts: `{summary.get('deferred_artifact_count', 0)}`",
        f"- Selected bytes: `{summary.get('selected_source_size_bytes', 0)}`",
        f"- File sha256 enabled: `{bool(summary.get('file_sha256_enabled'))}`",
        "",
        "## Artifact Manifest",
        "",
        "| path | status | class | bytes | files | reference | blockers |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for item in list(manifest.get("artifacts") or []):
        payload = dict(item or {})
        lines.append(
            "| {path} | `{status}` | `{klass}` | {bytes} | {files} | `{reference}` | {blockers} |".format(
                path=str(payload.get("path") or ""),
                status=str(payload.get("status") or ""),
                klass=str(payload.get("retention_class") or ""),
                bytes=int(payload.get("source_size_bytes") or 0),
                files=int(payload.get("file_count") or 0),
                reference=str(payload.get("family_reference_path") or ""),
                blockers=", ".join(payload.get("blocking_reasons") or []) or "-",
            )
        )
    gates = list(manifest.get("post_manifest_gates") or [])
    if gates:
        lines.extend(["", "## Post-Manifest Gates", ""])
        for gate in gates:
            lines.append(f"- `{gate}`")
    return "\n".join(lines).rstrip() + "\n"


def dumps_manifest(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _build_artifact_manifest(
    *,
    candidate: dict[str, Any],
    artifacts_by_path: dict[str, dict[str, Any]],
    artifacts_by_relative_path: dict[str, dict[str, Any]],
    workspace_root: Path,
    source_contract_valid: bool,
    include_file_sha256: bool,
    max_files_per_artifact: int,
) -> dict[str, Any]:
    blockers: list[str] = []
    if not source_contract_valid:
        blockers.append("invalid_source_supersession_contract")
    resolved = _resolve_artifact_path(str(candidate.get("path") or ""), workspace_root=workspace_root)
    if str(resolved.get("blocker") or ""):
        blockers.append(str(resolved["blocker"]))
    effective_scan_root = str(resolved.get("scan_root") or "")
    if str(candidate.get("scan_root") or "") and str(candidate.get("scan_root") or "") != effective_scan_root:
        blockers.append("scan_root_path_mismatch")

    reference_path = str(candidate.get("family_reference_path") or "")
    reference = artifacts_by_path.get(reference_path)
    if reference is None:
        reference_resolved = _resolve_artifact_path(reference_path, workspace_root=workspace_root)
        reference = artifacts_by_relative_path.get(str(reference_resolved.get("relative_path") or ""))
    if reference is None:
        blockers.append("family_reference_missing_from_source_report")
        reference_resolved = _resolve_artifact_path(reference_path, workspace_root=workspace_root)
    else:
        reference_resolved = _resolve_artifact_path(str(reference.get("path") or reference_path), workspace_root=workspace_root)
    if str(reference_resolved.get("blocker") or ""):
        blockers.append(f"family_reference_{reference_resolved['blocker']}")
    candidate_scan_root_for_reference = effective_scan_root or str(candidate.get("scan_root") or "")
    if (
        str(reference_resolved.get("scan_root") or "")
        and candidate_scan_root_for_reference
        and str(reference_resolved.get("scan_root")) != candidate_scan_root_for_reference
    ):
        blockers.append("family_reference_cross_root")
    if reference is not None and str(reference.get("retention_class") or "") != str(candidate.get("retention_class") or ""):
        blockers.append("family_reference_retention_class_mismatch")
    if reference is not None and str(reference.get("path") or "") == str(candidate.get("path") or ""):
        blockers.append("family_reference_same_as_candidate")
    if _safe_int(candidate.get("family_size")) < 2:
        blockers.append("family_size_too_small_for_supersession")

    target_path = resolved.get("absolute_path")
    file_manifest = _empty_file_manifest()
    if isinstance(target_path, Path) and not blockers:
        file_manifest = _file_manifest(
            artifact_dir=target_path,
            include_file_sha256=include_file_sha256,
            max_files=max_files_per_artifact,
        )
        blockers.extend(_file_manifest_blockers(file_manifest=file_manifest, candidate=candidate))

    status = _artifact_status(blockers=blockers, file_manifest=file_manifest, include_file_sha256=include_file_sha256)
    payload = {
        "path": str(candidate.get("path") or ""),
        "relative_path": str(resolved.get("relative_path") or ""),
        "absolute_path": str(target_path) if isinstance(target_path, Path) else "",
        "status": status,
        "blocking_reasons": _dedupe_strings(blockers),
        "selection_status": "selected",
        "scan_root": effective_scan_root,
        "retention_class": str(candidate.get("retention_class") or ""),
        "source_size_bytes": _safe_int(candidate.get("size_bytes")),
        "source_file_count": _safe_int(candidate.get("file_count")),
        "source_directory_count": _safe_int(candidate.get("directory_count")),
        "source_latest_mtime": str(candidate.get("latest_mtime") or ""),
        "family_key": str(candidate.get("family_key") or ""),
        "family_size": _safe_int(candidate.get("family_size")),
        "family_reference_path": reference_path,
        "family_reference_latest_mtime": str(candidate.get("family_reference_latest_mtime") or ""),
        "backup_key": _backup_key(str(resolved.get("relative_path") or "")),
        "local_removal_after_cold_copy_recommended": status == "ready_for_cold_storage_review",
        "deletion_allowed": False,
        "archive_apply_required": "separate_reviewed_copy_verify_then_archive_or_remove_operation",
        **file_manifest,
    }
    payload["manifest_digest_sha256"] = _manifest_digest(payload)
    return payload


def _deferred_artifact(candidate: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "path": str(candidate.get("path") or ""),
        "relative_path": "",
        "absolute_path": "",
        "status": "deferred_by_selection_limit",
        "selection_status": "deferred",
        "selection_defer_reason": reason,
        "blocking_reasons": [],
        "scan_root": str(candidate.get("scan_root") or ""),
        "retention_class": str(candidate.get("retention_class") or ""),
        "source_size_bytes": _safe_int(candidate.get("size_bytes")),
        "source_file_count": _safe_int(candidate.get("file_count")),
        "source_directory_count": _safe_int(candidate.get("directory_count")),
        "source_latest_mtime": str(candidate.get("latest_mtime") or ""),
        "family_key": str(candidate.get("family_key") or ""),
        "family_size": _safe_int(candidate.get("family_size")),
        "family_reference_path": str(candidate.get("family_reference_path") or ""),
        "family_reference_latest_mtime": str(candidate.get("family_reference_latest_mtime") or ""),
        "backup_key": "",
        "local_removal_after_cold_copy_recommended": False,
        "deletion_allowed": False,
        "archive_apply_required": "separate_reviewed_copy_verify_then_archive_or_remove_operation",
        **_empty_file_manifest(),
        "manifest_digest_sha256": "",
    }


def _build_summary(
    artifacts: list[dict[str, Any]],
    *,
    candidate_count: int,
    include_file_sha256: bool,
    max_files_per_artifact: int,
    max_entries: int,
    target_bytes: int,
    source_contract_valid: bool,
) -> dict[str, Any]:
    by_status: dict[str, dict[str, int]] = {}
    by_blocker: dict[str, int] = {}
    for artifact in artifacts:
        status = str(artifact.get("status") or "")
        payload = by_status.setdefault(status, {"count": 0, "source_size_bytes": 0, "file_count": 0})
        payload["count"] += 1
        payload["source_size_bytes"] += _safe_int(artifact.get("source_size_bytes"))
        payload["file_count"] += _safe_int(artifact.get("file_count"))
        for blocker in list(artifact.get("blocking_reasons") or []):
            text = str(blocker or "")
            if text:
                by_blocker[text] = by_blocker.get(text, 0) + 1
    proof_ready = [item for item in artifacts if item.get("status") == "ready_for_cold_storage_review"]
    planning_ready = [item for item in artifacts if item.get("status") == "planning_ready_needs_hash_manifest"]
    blocked = [item for item in artifacts if item.get("status") == "blocked_cold_storage_manifest"]
    deferred = [item for item in artifacts if item.get("status") == "deferred_by_selection_limit"]
    selected = [item for item in artifacts if item.get("selection_status") == "selected"]
    return {
        "source_contract_valid": source_contract_valid,
        "supersession_candidate_count": candidate_count,
        "selected_artifact_count": len(selected),
        "proof_ready_artifact_count": len(proof_ready),
        "planning_ready_artifact_count": len(planning_ready),
        "blocked_artifact_count": len(blocked),
        "deferred_artifact_count": len(deferred),
        "selected_source_size_bytes": sum(_safe_int(item.get("source_size_bytes")) for item in selected),
        "proof_ready_source_size_bytes": sum(_safe_int(item.get("source_size_bytes")) for item in proof_ready),
        "planning_ready_source_size_bytes": sum(_safe_int(item.get("source_size_bytes")) for item in planning_ready),
        "blocked_source_size_bytes": sum(_safe_int(item.get("source_size_bytes")) for item in blocked),
        "deferred_source_size_bytes": sum(_safe_int(item.get("source_size_bytes")) for item in deferred),
        "total_file_count": sum(_safe_int(item.get("file_count")) for item in proof_ready + planning_ready),
        "total_size_bytes": sum(_safe_int(item.get("total_size_bytes")) for item in proof_ready + planning_ready),
        "file_sha256_enabled": include_file_sha256,
        "max_files_per_artifact": max_files_per_artifact,
        "max_entries": max_entries,
        "target_bytes": target_bytes,
        "by_status": by_status,
        "by_blocker": by_blocker,
    }


def _status(summary: dict[str, Any]) -> str:
    if not bool(summary.get("source_contract_valid")):
        return "blocked_invalid_source_supersession_contract"
    if _safe_int(summary.get("proof_ready_artifact_count")) > 0 and _safe_int(summary.get("blocked_artifact_count")) > 0:
        return "partial_ready_for_cold_storage_review"
    if _safe_int(summary.get("proof_ready_artifact_count")) > 0:
        return "ready_for_cold_storage_review"
    if _safe_int(summary.get("planning_ready_artifact_count")) > 0:
        return "planning_ready_needs_hash_manifest"
    if _safe_int(summary.get("blocked_artifact_count")) > 0:
        return "blocked_no_cold_storage_ready_artifacts"
    if _safe_int(summary.get("deferred_artifact_count")) > 0:
        return "deferred_by_selection_limits"
    return "no_supersession_candidates"


def _source_contract_valid(report: dict[str, Any]) -> bool:
    return (
        str(report.get("contract_version") or "") == SOURCE_CONTRACT_VERSION
        and report.get("read_only") is True
        and report.get("deletion_allowed") is False
        and report.get("source_contract_valid") is True
    )


def _resolve_artifact_path(path_text: str, *, workspace_root: Path) -> dict[str, Any]:
    raw_path = str(path_text or "").strip()
    if not raw_path:
        return {"blocker": "empty_path"}
    raw_parts = [part for part in raw_path.replace("\\", "/").split("/") if part]
    if any(part in {".", ".."} for part in raw_parts):
        return {"blocker": "path_contains_traversal"}
    path = Path(raw_path)
    if any(part in {".", ".."} for part in path.parts):
        return {"blocker": "path_contains_traversal"}
    if path.is_absolute():
        try:
            path = path.relative_to(workspace_root)
        except ValueError:
            return {"blocker": "absolute_path_outside_workspace_root"}
    if any(part in {".", ".."} for part in path.parts):
        return {"blocker": "path_contains_traversal"}
    path_parts = path.parts
    for scan_root in ALLOWED_SCAN_ROOTS:
        scan_root_parts = Path(scan_root).parts
        if len(path_parts) <= len(scan_root_parts):
            continue
        if path_parts[: len(scan_root_parts)] == scan_root_parts:
            raw_absolute_path = workspace_root / path
            symlink_blocker = _raw_path_symlink_blocker(raw_absolute_path=raw_absolute_path, workspace_root=workspace_root)
            if symlink_blocker:
                return {"blocker": symlink_blocker}
            absolute_path = raw_absolute_path.resolve(strict=False)
            try:
                resolved_relative_path = absolute_path.relative_to(workspace_root)
            except ValueError:
                return {"blocker": "resolved_path_outside_workspace_root"}
            resolved_parts = resolved_relative_path.parts
            if len(resolved_parts) <= len(scan_root_parts) or resolved_parts[: len(scan_root_parts)] != scan_root_parts:
                return {"blocker": "resolved_path_scan_root_mismatch"}
            return {
                "scan_root": scan_root,
                "relative_path": path.as_posix(),
                "absolute_path": raw_absolute_path,
                "resolved_absolute_path": absolute_path,
            }
    return {"blocker": "path_outside_allowed_scan_roots"}


def _raw_path_symlink_blocker(*, raw_absolute_path: Path, workspace_root: Path) -> str:
    try:
        relative_path = raw_absolute_path.relative_to(workspace_root)
    except ValueError:
        return "raw_path_outside_workspace_root"
    current = workspace_root
    for part in relative_path.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return ""
        except OSError:
            return "raw_path_lstat_failed"
        if stat.S_ISLNK(mode):
            return "raw_path_contains_symlink"
    return ""


def _file_manifest(*, artifact_dir: Path, include_file_sha256: bool, max_files: int) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    total_size = 0
    file_count = 0
    directory_count = 0
    symlink_count = 0
    truncated = False
    latest_mtime: datetime | None = None
    try:
        root_stat = artifact_dir.lstat()
    except OSError:
        return {**_empty_file_manifest(), "missing": True}
    if artifact_dir.is_symlink():
        return {**_empty_file_manifest(), "missing": False, "raw_path_is_symlink": True}
    if not artifact_dir.is_dir():
        return {**_empty_file_manifest(), "missing": False, "not_directory": True}
    latest_mtime = _max_datetime(latest_mtime, _mtime_datetime(root_stat.st_mtime))
    for path in sorted(artifact_dir.rglob("*")):
        try:
            stat = path.lstat()
        except OSError:
            truncated = True
            continue
        latest_mtime = _max_datetime(latest_mtime, _mtime_datetime(stat.st_mtime))
        if path.is_symlink():
            symlink_count += 1
            continue
        if path.is_dir():
            directory_count += 1
            continue
        if not path.is_file():
            continue
        file_count += 1
        total_size += int(stat.st_size)
        if len(files) >= max_files:
            truncated = True
            continue
        relative_path = path.relative_to(artifact_dir).as_posix()
        entry = {
            "relative_path": relative_path,
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
        if include_file_sha256:
            entry["sha256"] = _file_sha256(path)
        files.append(entry)
    return {
        "missing": False,
        "raw_path_is_symlink": False,
        "not_directory": False,
        "file_count": file_count,
        "directory_count": directory_count,
        "total_size_bytes": total_size,
        "latest_mtime": latest_mtime.isoformat() if latest_mtime else "",
        "symlink_count": symlink_count,
        "truncated": truncated,
        "file_sha256_complete": bool(include_file_sha256) and not truncated and symlink_count == 0 and len(files) == file_count,
        "files": files,
    }


def _file_manifest_blockers(*, file_manifest: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if bool(file_manifest.get("missing")):
        blockers.append("artifact_dir_missing")
    if bool(file_manifest.get("raw_path_is_symlink")):
        blockers.append("artifact_dir_is_symlink")
    if bool(file_manifest.get("not_directory")):
        blockers.append("artifact_path_not_directory")
    if bool(file_manifest.get("truncated")):
        blockers.append("file_manifest_truncated")
    if _safe_int(file_manifest.get("symlink_count")) > 0:
        blockers.append("artifact_contains_symlink")
    if _safe_int(file_manifest.get("file_count")) <= 0:
        blockers.append("artifact_has_no_files")
    if _safe_int(file_manifest.get("total_size_bytes")) != _safe_int(candidate.get("size_bytes")):
        blockers.append("source_size_mismatch")
    if _safe_int(file_manifest.get("file_count")) != _safe_int(candidate.get("file_count")):
        blockers.append("source_file_count_mismatch")
    if _safe_int(file_manifest.get("directory_count")) != _safe_int(candidate.get("directory_count")):
        blockers.append("source_directory_count_mismatch")
    source_mtime_raw = str(candidate.get("latest_mtime") or "").strip()
    source_mtime = _parse_datetime(source_mtime_raw)
    actual_mtime = _parse_datetime(str(file_manifest.get("latest_mtime") or ""))
    if not source_mtime_raw:
        blockers.append("source_latest_mtime_missing")
    elif source_mtime is None:
        blockers.append("source_latest_mtime_invalid")
    elif actual_mtime is None:
        blockers.append("actual_latest_mtime_invalid")
    elif abs((source_mtime - actual_mtime).total_seconds()) > 0.001:
        blockers.append("source_latest_mtime_mismatch")
    return blockers


def _artifact_status(*, blockers: list[str], file_manifest: dict[str, Any], include_file_sha256: bool) -> str:
    if blockers:
        return "blocked_cold_storage_manifest"
    if not include_file_sha256 or not bool(file_manifest.get("file_sha256_complete")):
        return "planning_ready_needs_hash_manifest"
    return "ready_for_cold_storage_review"


def _empty_file_manifest() -> dict[str, Any]:
    return {
        "missing": False,
        "raw_path_is_symlink": False,
        "not_directory": False,
        "file_count": 0,
        "directory_count": 0,
        "total_size_bytes": 0,
        "latest_mtime": "",
        "symlink_count": 0,
        "truncated": False,
        "file_sha256_complete": False,
        "files": [],
    }


def _backup_key(relative_path: str) -> str:
    safe = str(relative_path or "").strip("/")
    return f"runtime-supersession/{safe}" if safe else ""


def _manifest_digest(payload: dict[str, Any]) -> str:
    digest_payload = {
        "path": payload.get("path"),
        "relative_path": payload.get("relative_path"),
        "backup_key": payload.get("backup_key"),
        "source_size_bytes": payload.get("source_size_bytes"),
        "source_file_count": payload.get("source_file_count"),
        "source_directory_count": payload.get("source_directory_count"),
        "source_latest_mtime": payload.get("source_latest_mtime"),
        "family_reference_path": payload.get("family_reference_path"),
        "file_count": payload.get("file_count"),
        "directory_count": payload.get("directory_count"),
        "total_size_bytes": payload.get("total_size_bytes"),
        "latest_mtime": payload.get("latest_mtime"),
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


def _max_datetime(left: datetime | None, right: datetime) -> datetime:
    if left is None or right > left:
        return right
    return left


def _mtime_datetime(value: float) -> datetime:
    return datetime.fromtimestamp(value, timezone.utc)


def _parse_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
        text = " ".join(str(value or "").split()).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
