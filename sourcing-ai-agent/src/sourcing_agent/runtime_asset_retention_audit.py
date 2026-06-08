from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DEFAULT_RETENTION_SCAN_ROOTS = ("runtime/test_env", "output")
DEFAULT_RETENTION_NAME_MARKERS = (
    "google",
    "reflection",
    "nightly",
    "w6",
    "pre_manual",
    "phase12",
)


def build_runtime_asset_retention_report(
    *,
    workspace_root: str | Path = ".",
    scan_roots: Iterable[str | Path] | None = None,
    include_name_markers: Iterable[str] | None = None,
    min_size_bytes: int = 0,
    max_depth: int = 1,
    sample_limit: int = 250,
) -> dict[str, Any]:
    """Build a read-only retention inventory for historical runtime/output directories.

    This intentionally does not decide deletion safety. Company snapshot lifecycle
    depends on registry/projection/rebuild contracts, while test/output runtime
    directories need a separate retention review before any archive operation.
    """

    root = Path(str(workspace_root)).expanduser()
    if not root.is_absolute():
        root = root.resolve()
    markers = _normalize_marker_list(include_name_markers or DEFAULT_RETENTION_NAME_MARKERS)
    roots = list(scan_roots or DEFAULT_RETENTION_SCAN_ROOTS)
    directories: list[dict[str, Any]] = []
    missing_roots: list[str] = []
    for raw_scan_root in roots:
        scan_root = Path(str(raw_scan_root)).expanduser()
        if not scan_root.is_absolute():
            scan_root = root / scan_root
        if not scan_root.exists():
            missing_roots.append(_display_path(scan_root, root))
            continue
        if not scan_root.is_dir():
            continue
        for path in _iter_directories(scan_root, max_depth=max(1, int(max_depth or 1))):
            marker_matches = _matching_markers(path.name, markers)
            if markers and not marker_matches:
                continue
            size_summary = _directory_size_summary(path)
            if int(size_summary["size_bytes"]) < max(0, int(min_size_bytes or 0)):
                continue
            directories.append(
                {
                    "path": _display_path(path, root),
                    "scan_root": _display_path(scan_root, root),
                    "name": path.name,
                    "matched_markers": marker_matches,
                    "size_bytes": size_summary["size_bytes"],
                    "file_count": size_summary["file_count"],
                    "directory_count": size_summary["directory_count"],
                    "latest_mtime": size_summary["latest_mtime"],
                    "retention_class": _classify_retention_dir(path.name),
                    "recommended_next_step": _recommended_next_step(path.name),
                }
            )
    directories.sort(key=lambda item: (int(item["size_bytes"]), str(item["path"])), reverse=True)
    limited_directories = directories[: max(1, int(sample_limit or 250))]
    summary = _build_summary(directories, returned_count=len(limited_directories))
    return {
        "contract_version": "runtime_asset_retention_audit_v1",
        "status": "ready_for_retention_review" if directories else "no_matching_directories",
        "read_only": True,
        "deletion_allowed": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(root),
        "scan_roots": [_display_path((root / Path(str(scan_root))).resolve() if not Path(str(scan_root)).is_absolute() else Path(str(scan_root)), root) for scan_root in roots],
        "missing_scan_roots": missing_roots,
        "include_name_markers": markers,
        "min_size_bytes": max(0, int(min_size_bytes or 0)),
        "max_depth": max(1, int(max_depth or 1)),
        "sample_limit": max(1, int(sample_limit or 250)),
        "summary": summary,
        "directories": limited_directories,
        "post_review_gates": [
            "confirm_not_authoritative_registry_or_projection_dependency",
            "copy_or_package_selected_directories_to_cold_storage",
            "verify_cold_copy_hash_or_manifest",
            "rerun_rebuild_or_projection_rehearsal_for_affected_assets",
            "independent_review_gate_before_any_apply",
            "apply_archive_or_reuse_exclusion_in_separate_reviewed_operation",
        ],
    }


def render_runtime_asset_retention_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Runtime Asset Retention Audit",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Read-only: `{bool(report.get('read_only'))}`",
        f"- Deletion allowed: `{bool(report.get('deletion_allowed'))}`",
        f"- Generated at: `{report.get('generated_at')}`",
        f"- Matched directories: `{summary.get('matched_directory_count', 0)}`",
        f"- Returned directories: `{summary.get('returned_directory_count', 0)}`",
        f"- Total bytes: `{summary.get('total_size_bytes', 0)}`",
        f"- Total files: `{summary.get('total_file_count', 0)}`",
        "",
        "| path | class | markers | files | bytes | latest mtime | next step |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for item in list(report.get("directories") or []):
        payload = dict(item or {})
        lines.append(
            "| {path} | `{klass}` | `{markers}` | {files} | {bytes} | `{mtime}` | `{next_step}` |".format(
                path=str(payload.get("path") or ""),
                klass=str(payload.get("retention_class") or ""),
                markers=", ".join(list(payload.get("matched_markers") or [])) or "-",
                files=int(payload.get("file_count") or 0),
                bytes=int(payload.get("size_bytes") or 0),
                mtime=str(payload.get("latest_mtime") or ""),
                next_step=str(payload.get("recommended_next_step") or ""),
            )
        )
    lines.extend(["", "## Post-Review Gates", ""])
    for gate in list(report.get("post_review_gates") or []):
        lines.append(f"- `{gate}`")
    return "\n".join(lines).rstrip() + "\n"


def _iter_directories(root: Path, *, max_depth: int) -> Iterable[Path]:
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > 0:
            yield current
        if depth >= max_depth:
            continue
        try:
            entries = sorted(current.iterdir(), key=lambda item: item.name)
        except OSError:
            continue
        for entry in reversed(entries):
            if entry.is_dir() and not entry.is_symlink():
                stack.append((entry, depth + 1))


def _directory_size_summary(path: Path) -> dict[str, Any]:
    size_bytes = 0
    file_count = 0
    directory_count = 0
    latest_mtime = 0.0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            stat = current.lstat()
        except OSError:
            continue
        latest_mtime = max(latest_mtime, float(stat.st_mtime))
        if current.is_dir() and not current.is_symlink():
            directory_count += 1
            try:
                entries = list(os.scandir(current))
            except OSError:
                continue
            for entry in entries:
                stack.append(Path(entry.path))
            continue
        file_count += 1
        size_bytes += int(stat.st_size)
    return {
        "size_bytes": size_bytes,
        "file_count": file_count,
        "directory_count": max(0, directory_count - 1),
        "latest_mtime": datetime.fromtimestamp(latest_mtime, timezone.utc).isoformat() if latest_mtime else "",
    }


def _normalize_marker_list(values: Iterable[str]) -> list[str]:
    markers: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_marker(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        markers.append(normalized)
    return markers


def _normalize_marker(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in str(value or "").strip()).strip("_")


def _matching_markers(name: str, markers: list[str]) -> list[str]:
    normalized_name = _normalize_marker(name)
    return [marker for marker in markers if marker and marker in normalized_name]


def _classify_retention_dir(name: str) -> str:
    normalized = _normalize_marker(name)
    if "current" in normalized or "latest" in normalized:
        return "review_current_alias_or_latest_artifact"
    if "w6" in normalized or "nightly" in normalized or "pre_manual" in normalized:
        return "review_signoff_or_pressure_run_artifact"
    if "phase" in normalized:
        return "review_phase_milestone_artifact"
    return "review_cold_archive_candidate"


def _recommended_next_step(name: str) -> str:
    retention_class = _classify_retention_dir(name)
    if retention_class == "review_current_alias_or_latest_artifact":
        return "confirm_latest_alias_target_before_archive"
    if retention_class == "review_signoff_or_pressure_run_artifact":
        return "keep_or_cold_archive_after_signoff_supersession_proof"
    if retention_class == "review_phase_milestone_artifact":
        return "confirm_milestone_artifact_is_superseded_before_archive"
    return "cold_archive_manifest_review"


def _build_summary(directories: list[dict[str, Any]], *, returned_count: int) -> dict[str, Any]:
    by_root: dict[str, dict[str, int]] = {}
    by_class: dict[str, dict[str, int]] = {}
    total_size = 0
    total_files = 0
    for item in directories:
        size = int(item.get("size_bytes") or 0)
        files = int(item.get("file_count") or 0)
        total_size += size
        total_files += files
        root = str(item.get("scan_root") or "")
        klass = str(item.get("retention_class") or "")
        for bucket, key in ((by_root, root), (by_class, klass)):
            payload = bucket.setdefault(key, {"count": 0, "size_bytes": 0, "file_count": 0})
            payload["count"] += 1
            payload["size_bytes"] += size
            payload["file_count"] += files
    return {
        "matched_directory_count": len(directories),
        "returned_directory_count": returned_count,
        "total_size_bytes": total_size,
        "total_file_count": total_files,
        "by_scan_root": by_root,
        "by_retention_class": by_class,
    }


def _display_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def dumps_report(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2) + "\n"
