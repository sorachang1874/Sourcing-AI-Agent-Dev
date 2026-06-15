from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = "runtime_asset_supersession_cold_bundle_manifest_v1"
SOURCE_CONTRACT_VERSION = "runtime_asset_supersession_cold_manifest_v1"
READY_SOURCE_STATUS = "ready_for_cold_storage_review"
READY_ARCHIVE_STATUS = "archive_ready_for_prune_cold_copy"
ALLOWED_SOURCE_ROOTS = ("runtime/test_env", "output")
DEFAULT_COMPRESSION = "zstd"


def build_runtime_asset_supersession_cold_bundle_manifest(
    *,
    proof_manifest: dict[str, Any],
    workspace_root: str | Path,
    archive_root: str | Path,
    create_archives: bool = False,
    compression: str = DEFAULT_COMPRESSION,
    compression_level: int = 3,
    max_entries: int = 0,
) -> dict[str, Any]:
    root = Path(workspace_root).expanduser().resolve()
    archive_base = _resolve_archive_root(archive_root=archive_root, workspace_root=root)
    source_contract_valid = _source_contract_valid(proof_manifest)
    normalized_compression = _normalize_compression(compression)
    generated_at = datetime.now(timezone.utc).isoformat()
    selected = [
        dict(item or {})
        for item in list(proof_manifest.get("artifacts") or [])
        if str(dict(item or {}).get("selection_status") or "") == "selected"
    ]
    if max_entries and max_entries > 0:
        selected = selected[: int(max_entries)]
    archives = [
        _build_archive_entry(
            artifact=artifact,
            workspace_root=root,
            archive_root=archive_base,
            source_contract_valid=source_contract_valid,
            create_archive=bool(create_archives),
            compression=normalized_compression,
            compression_level=max(1, int(compression_level or 1)),
        )
        for artifact in selected
    ]
    summary = _build_summary(archives=archives, create_archives=bool(create_archives))
    return {
        "contract_version": CONTRACT_VERSION,
        "status": _manifest_status(summary=summary, create_archives=bool(create_archives), source_contract_valid=source_contract_valid),
        "generated_at": generated_at,
        "workspace_root": str(root),
        "archive_root": str(archive_base),
        "archive_root_relative": _relative_to_root(archive_base, root),
        "source_manifest_contract_version": str(proof_manifest.get("contract_version") or ""),
        "source_manifest_status": str(proof_manifest.get("status") or ""),
        "source_contract_valid": source_contract_valid,
        "source_manifest_deletion_allowed": proof_manifest.get("deletion_allowed"),
        "read_only": not bool(create_archives),
        "archive_created": bool(create_archives),
        "deletion_allowed": False,
        "compression": normalized_compression,
        "compression_level": max(1, int(compression_level or 1)),
        "allowed_source_roots": list(ALLOWED_SOURCE_ROOTS),
        "summary": summary,
        "archives": archives,
        "post_bundle_gates": [
            "independent_review_gate_for_bundle_manifest_and_prune_plan",
            "dry_run_runtime_asset_prune_plan_with_cold_copy_manifest",
            "apply_runtime_asset_prune_plan_only_after_reviewed_go",
        ],
    }


def render_runtime_asset_supersession_cold_bundle_markdown(manifest: dict[str, Any]) -> str:
    summary = dict(manifest.get("summary") or {})
    lines = [
        "# Runtime Asset Supersession Cold Bundle Manifest",
        "",
        f"- Status: `{manifest.get('status')}`",
        f"- Read-only: `{bool(manifest.get('read_only'))}`",
        f"- Archive created: `{bool(manifest.get('archive_created'))}`",
        f"- Deletion allowed: `{bool(manifest.get('deletion_allowed'))}`",
        f"- Archive root: `{manifest.get('archive_root_relative') or manifest.get('archive_root')}`",
        f"- Source contract valid: `{bool(manifest.get('source_contract_valid'))}`",
        f"- Archive count: `{summary.get('archive_count', 0)}`",
        f"- Ready archive count: `{summary.get('ready_archive_count', 0)}`",
        f"- Blocked archive count: `{summary.get('blocked_archive_count', 0)}`",
        f"- Source bytes: `{summary.get('source_size_bytes', 0)}`",
        f"- Archive bytes: `{summary.get('archive_size_bytes', 0)}`",
        f"- Estimated reclaim bytes: `{summary.get('estimated_reclaim_bytes', 0)}`",
        "",
        "## Archives",
        "",
        "| source path | status | source bytes | archive bytes | archive | blockers |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for item in list(manifest.get("archives") or []):
        payload = dict(item or {})
        lines.append(
            "| {path} | `{status}` | {source_bytes} | {archive_bytes} | `{archive}` | {blockers} |".format(
                path=str(payload.get("path") or ""),
                status=str(payload.get("status") or ""),
                source_bytes=int(payload.get("size_bytes") or 0),
                archive_bytes=int(payload.get("archive_size_bytes") or 0),
                archive=str(payload.get("archive_path") or ""),
                blockers=", ".join(payload.get("blocking_reasons") or []) or "-",
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def dumps_manifest(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _build_archive_entry(
    *,
    artifact: dict[str, Any],
    workspace_root: Path,
    archive_root: Path,
    source_contract_valid: bool,
    create_archive: bool,
    compression: str,
    compression_level: int,
) -> dict[str, Any]:
    blockers: list[str] = []
    if not source_contract_valid:
        blockers.append("invalid_source_cold_manifest_contract")
    if str(artifact.get("status") or "") != READY_SOURCE_STATUS:
        blockers.append("source_artifact_not_ready_for_cold_storage_review")
    if bool(artifact.get("deletion_allowed")):
        blockers.append("source_artifact_must_not_allow_deletion")
    if not bool(artifact.get("file_sha256_complete")):
        blockers.append("source_file_sha256_incomplete")
    if not str(artifact.get("manifest_digest_sha256") or "").strip():
        blockers.append("missing_source_manifest_digest")
    path_text = str(artifact.get("path") or "")
    path_reason = _source_path_safety_reason(path_text=path_text, root=workspace_root)
    if path_reason:
        blockers.append(path_reason)
    source_dir = workspace_root / path_text
    summary = _directory_summary(source_dir) if not path_reason and source_dir.exists() else _empty_directory_summary()
    blockers.extend(_source_summary_blockers(artifact=artifact, summary=summary))
    archive_relative = _archive_relative_path(path_text=path_text, compression=compression)
    archive_path = archive_root / archive_relative
    archive_root_reason = _archive_root_safety_reason(archive_root=archive_root, workspace_root=workspace_root)
    if archive_root_reason:
        blockers.append(archive_root_reason)
    if _archive_inside_source(source_path=source_dir, archive_path=archive_path):
        blockers.append("archive_path_inside_source_directory")

    archive_created = False
    archive_verified = False
    archive_size = 0
    archive_sha256 = ""
    archive_error = ""
    if create_archive and not blockers:
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            _create_archive(
                source_relative_path=path_text,
                workspace_root=workspace_root,
                archive_path=archive_path,
                compression=compression,
                compression_level=compression_level,
            )
            archive_created = True
            archive_size = archive_path.stat().st_size
            archive_sha256 = _file_sha256(archive_path)
            _verify_archive(archive_path=archive_path, compression=compression)
            archive_verified = True
        except (OSError, subprocess.SubprocessError) as exc:
            archive_error = str(exc)
            blockers.append("archive_create_or_verify_failed")
    elif not create_archive:
        blockers.append("archive_not_created")

    status = READY_ARCHIVE_STATUS if not blockers and archive_created and archive_verified else "blocked_cold_bundle_manifest"
    entry = {
        "path": path_text,
        "status": status,
        "blocking_reasons": _dedupe_strings(blockers),
        "size_bytes": int(artifact.get("source_size_bytes") or artifact.get("total_size_bytes") or 0),
        "file_count": int(artifact.get("file_count") or 0),
        "directory_count": int(artifact.get("directory_count") or 0),
        "latest_mtime": str(artifact.get("source_latest_mtime") or artifact.get("latest_mtime") or ""),
        "retention_class": str(artifact.get("retention_class") or ""),
        "scan_root": str(artifact.get("scan_root") or ""),
        "source_manifest_digest_sha256": str(artifact.get("manifest_digest_sha256") or ""),
        "source_file_sha256_complete": bool(artifact.get("file_sha256_complete")),
        "source_metadata_verified": not _source_summary_blockers(artifact=artifact, summary=summary),
        "archive_path": _relative_to_root(archive_path, workspace_root),
        "archive_format": f"tar.{compression}",
        "archive_size_bytes": archive_size,
        "archive_sha256": archive_sha256,
        "archive_created": archive_created,
        "archive_verified": archive_verified,
        "archive_error": archive_error,
        "deletion_allowed": False,
        "proof": archive_sha256,
    }
    entry["manifest_sha256"] = _manifest_digest(entry)
    return entry


def _source_contract_valid(manifest: dict[str, Any]) -> bool:
    return (
        str(manifest.get("contract_version") or "") == SOURCE_CONTRACT_VERSION
        and str(manifest.get("status") or "") == READY_SOURCE_STATUS
        and manifest.get("read_only") is True
        and manifest.get("deletion_allowed") is False
        and manifest.get("source_contract_valid") is True
        and int(dict(manifest.get("summary") or {}).get("blocked_artifact_count") or 0) == 0
        and int(dict(manifest.get("summary") or {}).get("proof_ready_artifact_count") or 0) > 0
    )


def _build_summary(*, archives: list[dict[str, Any]], create_archives: bool) -> dict[str, Any]:
    ready = [item for item in archives if str(item.get("status") or "") == READY_ARCHIVE_STATUS]
    blocked = [item for item in archives if str(item.get("status") or "") != READY_ARCHIVE_STATUS]
    source_bytes = sum(int(item.get("size_bytes") or 0) for item in archives)
    archive_bytes = sum(int(item.get("archive_size_bytes") or 0) for item in archives)
    return {
        "archive_count": len(archives),
        "ready_archive_count": len(ready),
        "blocked_archive_count": len(blocked),
        "source_size_bytes": source_bytes,
        "archive_size_bytes": archive_bytes,
        "estimated_reclaim_bytes": max(0, source_bytes - archive_bytes) if create_archives else 0,
        "archive_created_count": sum(1 for item in archives if bool(item.get("archive_created"))),
        "archive_verified_count": sum(1 for item in archives if bool(item.get("archive_verified"))),
        "by_blocker": _count_blockers(archives),
    }


def _manifest_status(*, summary: dict[str, Any], create_archives: bool, source_contract_valid: bool) -> str:
    if not source_contract_valid:
        return "blocked_invalid_source_cold_manifest_contract"
    if not create_archives:
        return "planning_ready_needs_archive_creation"
    if int(summary.get("archive_count") or 0) and int(summary.get("ready_archive_count") or 0) == int(summary.get("archive_count") or 0):
        return "ready_for_prune_cold_copy_manifest"
    return "blocked_or_partial_cold_bundle_manifest"


def _source_path_safety_reason(*, path_text: str, root: Path) -> str:
    if not path_text:
        return "missing_path"
    relative = Path(path_text)
    if relative.is_absolute():
        return "absolute_path_not_allowed"
    if any(part in {"", ".", ".."} for part in relative.parts):
        return "unsafe_path_component"
    if not any(path_text.startswith(f"{allowed}/") for allowed in ALLOWED_SOURCE_ROOTS):
        return "outside_allowed_source_roots"
    current = root
    for part in relative.parts:
        current = current / part
        try:
            if current.lstat().st_mode and current.is_symlink():
                return "raw_path_contains_symlink"
        except FileNotFoundError:
            return "source_path_missing"
    try:
        resolved = (root / path_text).resolve(strict=False)
        resolved.relative_to(root)
    except ValueError:
        return "path_escapes_workspace"
    return ""


def _directory_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"missing": True, **_empty_directory_summary()}
    if not path.is_dir() or path.is_symlink():
        return {"not_directory": True, **_empty_directory_summary()}
    size = 0
    file_count = 0
    directory_count = 0
    latest_ns = 0
    symlink_count = 0
    for child in path.rglob("*"):
        try:
            stat_result = child.lstat()
        except OSError:
            continue
        if child.is_symlink():
            symlink_count += 1
            continue
        latest_ns = max(latest_ns, int(stat_result.st_mtime_ns))
        if child.is_dir():
            directory_count += 1
        elif child.is_file():
            file_count += 1
            size += int(stat_result.st_size)
    return {
        "missing": False,
        "not_directory": False,
        "size_bytes": size,
        "file_count": file_count,
        "directory_count": directory_count,
        "latest_mtime": datetime.fromtimestamp(latest_ns / 1_000_000_000, tz=timezone.utc).isoformat() if latest_ns else "",
        "symlink_count": symlink_count,
    }


def _empty_directory_summary() -> dict[str, Any]:
    return {
        "missing": False,
        "not_directory": False,
        "size_bytes": 0,
        "file_count": 0,
        "directory_count": 0,
        "latest_mtime": "",
        "symlink_count": 0,
    }


def _source_summary_blockers(*, artifact: dict[str, Any], summary: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if bool(summary.get("missing")):
        blockers.append("source_path_missing")
    if bool(summary.get("not_directory")):
        blockers.append("source_not_plain_directory")
    if int(summary.get("symlink_count") or 0) > 0:
        blockers.append("source_contains_symlink")
    if int(summary.get("size_bytes") or 0) != int(artifact.get("source_size_bytes") or artifact.get("total_size_bytes") or 0):
        blockers.append("source_size_mismatch")
    if int(summary.get("file_count") or 0) != int(artifact.get("file_count") or 0):
        blockers.append("source_file_count_mismatch")
    if int(summary.get("directory_count") or 0) != int(artifact.get("directory_count") or 0):
        blockers.append("source_directory_count_mismatch")
    expected_mtime_raw = str(artifact.get("source_latest_mtime") or artifact.get("latest_mtime") or "")
    actual_mtime_raw = str(summary.get("latest_mtime") or "")
    expected_mtime = _parse_datetime(expected_mtime_raw)
    actual_mtime = _parse_datetime(actual_mtime_raw)
    if not expected_mtime_raw:
        blockers.append("source_latest_mtime_missing")
    elif expected_mtime is None:
        blockers.append("source_latest_mtime_invalid")
    elif actual_mtime is None:
        blockers.append("actual_latest_mtime_invalid")
    elif abs((actual_mtime - expected_mtime).total_seconds()) > 0.001:
        blockers.append("source_latest_mtime_mismatch")
    return blockers


def _resolve_archive_root(*, archive_root: str | Path, workspace_root: Path) -> Path:
    path = Path(archive_root).expanduser()
    if not path.is_absolute():
        path = workspace_root / path
    return path.resolve(strict=False)


def _archive_relative_path(*, path_text: str, compression: str) -> Path:
    safe_name = path_text.replace("/", "__")
    return Path(f"{safe_name}.tar.{compression}")


def _archive_inside_source(*, source_path: Path, archive_path: Path) -> bool:
    try:
        archive_path.resolve(strict=False).relative_to(source_path.resolve(strict=False))
        return True
    except ValueError:
        return False


def _archive_root_safety_reason(*, archive_root: Path, workspace_root: Path) -> str:
    try:
        relative_archive_root = archive_root.resolve(strict=False).relative_to(workspace_root.resolve(strict=False))
    except ValueError:
        return "archive_root_outside_workspace"
    if any(part in {"", ".", ".."} for part in relative_archive_root.parts):
        return "archive_root_unsafe_path_component"
    relative_text = relative_archive_root.as_posix()
    if not relative_text:
        return "archive_root_is_workspace_root"
    for source_root in ALLOWED_SOURCE_ROOTS:
        if relative_text == source_root or relative_text.startswith(f"{source_root}/"):
            return "archive_root_inside_prunable_source_root"
    current = workspace_root
    for part in relative_archive_root.parts:
        current = current / part
        try:
            if current.exists() and current.is_symlink():
                return "archive_root_contains_symlink"
        except OSError:
            return "archive_root_lstat_failed"
    return ""


def _create_archive(
    *,
    source_relative_path: str,
    workspace_root: Path,
    archive_path: Path,
    compression: str,
    compression_level: int,
) -> None:
    tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    if archive_path.exists():
        archive_path.unlink()
    if compression == "gzip":
        with tarfile.open(tmp_path, "w:gz") as archive:
            archive.add(workspace_root / source_relative_path, arcname=source_relative_path)
    elif compression == "zstd":
        if shutil.which("zstd") is None:
            raise FileNotFoundError("zstd executable not found")
        tar_process = subprocess.Popen(
            ["tar", "-cf", "-", "-C", str(workspace_root), source_relative_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert tar_process.stdout is not None
        zstd_result = subprocess.run(
            ["zstd", f"-{compression_level}", "-T0", "-q", "-o", str(tmp_path)],
            stdin=tar_process.stdout,
            stderr=subprocess.PIPE,
            check=False,
        )
        tar_process.stdout.close()
        _, tar_stderr = tar_process.communicate()
        if tar_process.returncode != 0:
            raise subprocess.CalledProcessError(tar_process.returncode, "tar", stderr=tar_stderr)
        if zstd_result.returncode != 0:
            raise subprocess.CalledProcessError(zstd_result.returncode, "zstd", stderr=zstd_result.stderr)
    else:
        raise ValueError(f"unsupported compression: {compression}")
    tmp_path.replace(archive_path)


def _verify_archive(*, archive_path: Path, compression: str) -> None:
    if not archive_path.exists() or archive_path.stat().st_size <= 0:
        raise OSError("archive missing or empty")
    if compression == "gzip":
        with tarfile.open(archive_path, "r:gz") as archive:
            if not archive.getmembers():
                raise OSError("archive has no members")
    elif compression == "zstd":
        result = subprocess.run(["zstd", "-t", "-q", str(archive_path)], stderr=subprocess.PIPE, check=False)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, "zstd -t", stderr=result.stderr)
        list_result = subprocess.run(
            ["sh", "-c", "zstd -dc \"$1\" | tar -tf - >/dev/null", "sh", str(archive_path)],
            stderr=subprocess.PIPE,
            check=False,
        )
        if list_result.returncode != 0:
            raise subprocess.CalledProcessError(list_result.returncode, "tar -tf zstd archive", stderr=list_result.stderr)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_digest(payload: dict[str, Any]) -> str:
    stable = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    encoded = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _relative_to_root(path: Path, root: Path) -> str:
    try:
        return str(path.resolve(strict=False).relative_to(root.resolve(strict=False)))
    except ValueError:
        return str(path)


def _normalize_compression(value: str) -> str:
    normalized = str(value or DEFAULT_COMPRESSION).strip().lower()
    if normalized in {"zst", "zstd"}:
        return "zstd"
    if normalized in {"gz", "gzip"}:
        return "gzip"
    raise ValueError(f"unsupported compression: {value}")


def _parse_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _count_blockers(archives: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in archives:
        for blocker in list(dict(item).get("blocking_reasons") or []):
            counts[str(blocker)] = counts.get(str(blocker), 0) + 1
    return dict(sorted(counts.items()))
