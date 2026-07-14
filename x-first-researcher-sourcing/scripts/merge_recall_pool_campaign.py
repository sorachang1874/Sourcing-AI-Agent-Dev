from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.recall_pool_campaign import (  # noqa: E402
    RAW_SESSION_FILES,
    CampaignValidationError,
    WaveInput,
    canonical_json,
    merge_campaign,
    replay_and_validate_campaign_result,
    strict_json_bytes,
    validate_policy,
    validate_request,
)

MAX_CONTROL_FILE_BYTES = 1_000_000
_PRIVATE_FILE_MODES = frozenset({0o600})
_TRACKED_CONTROL_MODES = frozenset({0o600, 0o644})
_TEMP_SUFFIX_RE = re.compile(r"[0-9a-f]{32}")


def _has_disallowed_symlink_ancestor(path: Path) -> bool:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:-1]:
        current /= part
        if current.is_symlink():
            if current == Path("/var") and current.resolve() == Path("/private/var"):
                continue
            return True
    return False


def _read_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
    allowed_modes: frozenset[int],
    require_private_parent: bool = False,
) -> bytes:
    if maximum_bytes < 1 or _has_disallowed_symlink_ancestor(path):
        raise ValueError("input_path_invalid")
    if require_private_parent:
        _validate_private_parent(path)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) not in allowed_modes
            or opened.st_size < 1
            or opened.st_size > maximum_bytes
        ):
            raise ValueError("input_file_invalid")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("input_file_truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError("input_file_grew")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size) != (opened.st_dev, opened.st_ino, opened.st_size):
            raise ValueError("input_file_changed")
        raw = b"".join(chunks)
        return raw
    finally:
        os.close(descriptor)


def _resolve_binding_path(manifest_path: Path, binding: str) -> Path:
    candidate = Path(binding).expanduser()
    if ".." in candidate.parts:
        raise ValueError("input_path_traversal")
    if not candidate.is_absolute():
        candidate = manifest_path.parent / candidate
    return candidate.absolute()


def _validate_private_parent(path: Path) -> Path:
    if _has_disallowed_symlink_ancestor(path):
        raise ValueError("output_parent_invalid")
    parent = path.parent.resolve()
    opened = parent.stat()
    if (
        not stat.S_ISDIR(opened.st_mode)
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) & 0o077
    ):
        raise ValueError("output_parent_not_owner_only")
    return parent


def _fsync_directory(parent: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(parent, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _cleanup_orphan_temps(parent: Path, destination_name: str) -> None:
    if not destination_name or destination_name in {".", ".."} or len(destination_name) > 255:
        raise ValueError("output_name_invalid")
    literal_prefix = f".{destination_name}.tmp."
    with os.scandir(parent) as entries:
        for entry in entries:
            if not entry.name.startswith(literal_prefix):
                continue
            suffix = entry.name[len(literal_prefix) :]
            if _TEMP_SUFFIX_RE.fullmatch(suffix) is None:
                raise ValueError("unsafe_orphan_temp")
            orphan = parent / entry.name
            opened = orphan.lstat()
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_nlink not in {1, 2}
            ):
                raise ValueError("unsafe_orphan_temp")
            orphan.unlink()


def _open_lock(parent: Path, destination_name: str) -> int:
    lock_path = parent / f".{destination_name}.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise ValueError("output_lock_invalid")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _publish_private_no_replace(path: Path, payload: bytes) -> None:
    parent = _validate_private_parent(path)
    destination = parent / path.name
    lock_descriptor = _open_lock(parent, path.name)
    temp: Path | None = None
    try:
        _cleanup_orphan_temps(parent, path.name)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError("output_exists")
        temp = parent / f".{path.name}.tmp.{secrets.token_hex(16)}"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temp, flags, 0o600)
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ValueError("output_temp_invalid")
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.link(temp, destination, follow_symlinks=False)
        _fsync_directory(parent)
        temp.unlink()
        temp = None
        _fsync_directory(parent)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)


def _load_sources(
    manifest_path: Path,
    request: dict[str, Any],
    policy: dict[str, Any],
) -> list[WaveInput]:
    ceilings = policy["kill_ceilings"]
    if len(request["waves"]) > ceilings["max_wave_files"]:
        raise CampaignValidationError("max_wave_files_kill_ceiling_exceeded")
    loaded: list[WaveInput] = []
    total_bytes = 0
    for binding in request["waves"]:
        source_raw = _read_regular_file(
            _resolve_binding_path(manifest_path, binding["source_path"]),
            maximum_bytes=ceilings["max_bytes_per_wave"],
            allowed_modes=_PRIVATE_FILE_MODES,
            require_private_parent=True,
        )
        upstream_raw = _read_regular_file(
            _resolve_binding_path(manifest_path, binding["upstream_request"]["path"]),
            maximum_bytes=MAX_CONTROL_FILE_BYTES,
            allowed_modes=_PRIVATE_FILE_MODES,
            require_private_parent=True,
        )
        prompt_raw = _read_regular_file(
            _resolve_binding_path(manifest_path, binding["prompt"]["path"]),
            maximum_bytes=ceilings["max_bytes_per_wave"],
            allowed_modes=_PRIVATE_FILE_MODES,
            require_private_parent=True,
        )
        raw_directory = _resolve_binding_path(manifest_path, binding["raw_session_directory"])
        if _has_disallowed_symlink_ancestor(raw_directory / "placeholder") or not raw_directory.is_dir():
            raise ValueError("raw_session_directory_invalid")
        raw_directory = raw_directory.resolve()
        raw_directory_stat = raw_directory.stat()
        if (
            raw_directory_stat.st_uid != os.getuid()
            or stat.S_IMODE(raw_directory_stat.st_mode) != 0o700
        ):
            raise ValueError("raw_session_directory_not_owner_only")
        raw_session_files = {
            name: _read_regular_file(
                raw_directory / name,
                maximum_bytes=ceilings["max_raw_session_file_bytes"],
                allowed_modes=_PRIVATE_FILE_MODES,
                require_private_parent=True,
            )
            for name in RAW_SESSION_FILES
        }
        total_bytes += len(source_raw) + len(upstream_raw) + len(prompt_raw) + sum(
            len(raw) for raw in raw_session_files.values()
        )
        if total_bytes > ceilings["max_total_input_bytes"]:
            raise CampaignValidationError("max_total_input_bytes_kill_ceiling_exceeded")
        loaded.append(
            WaveInput(
                wave_id=binding["wave_id"],
                result_bytes=source_raw,
                upstream_request_bytes=upstream_raw,
                prompt_bytes=prompt_raw,
                raw_session_files=raw_session_files,
            )
        )
    return loaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay and merge offline Grok waves into a high-recall pool")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=ROOT / "configs/recall_pool_campaign_policy.v1.json")
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument("--output", type=Path)
    output_group.add_argument("--validate-existing", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest_path = args.manifest.absolute()
        request_raw = _read_regular_file(
            manifest_path,
            maximum_bytes=MAX_CONTROL_FILE_BYTES,
            allowed_modes=_PRIVATE_FILE_MODES,
            require_private_parent=True,
        )
        policy_raw = _read_regular_file(
            args.policy.absolute(),
            maximum_bytes=MAX_CONTROL_FILE_BYTES,
            allowed_modes=_TRACKED_CONTROL_MODES,
        )
        request = strict_json_bytes(request_raw, error="campaign_manifest_json_invalid")
        policy = strict_json_bytes(policy_raw, error="campaign_policy_json_invalid")
        if not isinstance(request, dict) or not isinstance(policy, dict):
            raise CampaignValidationError("campaign_control_shape_invalid")
        validate_request(request)
        validate_policy(policy)
        waves = _load_sources(manifest_path, request, policy)
        if args.validate_existing is not None:
            persisted_raw = _read_regular_file(
                args.validate_existing.absolute(),
                maximum_bytes=policy["kill_ceilings"]["max_total_input_bytes"],
                allowed_modes=_PRIVATE_FILE_MODES,
                require_private_parent=True,
            )
            persisted = strict_json_bytes(persisted_raw, error="persisted_campaign_json_invalid")
            result = replay_and_validate_campaign_result(persisted, request, policy, waves)
            status = "validated"
            evaluation_sha256 = hashlib.sha256(persisted_raw).hexdigest()
        else:
            result = merge_campaign(request, policy, waves)
            payload = (canonical_json(result) + "\n").encode()
            _publish_private_no_replace(args.output.absolute(), payload)
            status = "completed"
            evaluation_sha256 = hashlib.sha256(payload).hexdigest()
        summary = {
            "campaign_id": result["campaign_id"],
            "evaluation_sha256": evaluation_sha256,
            "replayed_completed_native_x_calls": result["metrics"]["replayed_completed_native_x_calls"],
            "status": status,
            "total_unique_handles": result["metrics"]["total_unique_handles"],
            "wave_count": result["metrics"]["wave_count"],
        }
    except Exception:  # noqa: BLE001 - never echo candidate data, queries, or private paths
        print(json.dumps({"error": "RECALL_POOL_CAMPAIGN_MERGE_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
