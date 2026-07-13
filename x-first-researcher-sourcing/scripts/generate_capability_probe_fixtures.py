from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import json
import os
import re
import secrets
import stat
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from x_first.capability_probe import (
    CAPABILITY_OBSERVATION_EXCERPTS,
    REQUEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    SYNTHETIC_RAW_RESPONSE_SHA256,
    canonical_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP = "2026-07-14T00:00:00.000Z"
OWNED_TEMP_PREFIX = ".x-first-capability-fixture."
_OWNED_TEMP_TOKEN_PATTERN = r"[0-9a-f]{32}"
PAIR_LOCK_FILENAME = ".x-first-capability-fixture.pair.lock"
PAIR_LOCK_TIMEOUT_SECONDS = 5.0
PAIR_LOCK_POLL_SECONDS = 0.01
_PAIR_LOCK_FILE_MODE = 0o600
_PAIR_LOCK_REGISTRY_GUARD = threading.Lock()
_PAIR_LOCK_REGISTRY: dict[Path, threading.Lock] = {}


def build_request_fixture() -> dict[str, Any]:
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "probe_id": "xprobe_fixture_openai_official_v1",
        "execution_mode": "fixture_only",
        "owner_decisions": {
            "live_execution": "deferred",
            "legal_privacy": "deferred",
            "model_access": "deferred",
            "retention_policy": "deferred",
        },
        "target": {
            "lab_id": "openai",
            "account_kind": "official_lab",
            "platform_user_id": "xuid_fixture_official_openai",
            "current_handle": "fixture_openai_official",
        },
        "query": {
            "query_kind": "recent_public_technical_posts",
            "query_template": "fixture://openai/official/recent-public-technical-posts",
        },
        "hard_budgets": {
            "max_executions": 1,
            "max_external_calls": 1,
            "max_pages": 1,
            "max_observations": 5,
            "max_cost_usd": 0,
            "deadline_ms": 1000,
        },
        "kill_switch": {
            "armed": True,
            "trip_conditions": [
                "external_execution_attempted",
                "live_url_observed",
                "budget_exceeded",
                "credential_material_observed",
                "canonical_write_attempted",
            ],
        },
        "retention": {
            "class": "synthetic_fixture_only",
            "delete_after": None,
            "bounded_excerpt_max_chars": 280,
            "full_body_allowed": False,
        },
        "safety_policy_version": "x-first-public-professional-v1",
        "claims": {
            "external_execution_authorized": False,
            "researcher_mapping_authorized": False,
            "graph_expansion_authorized": False,
            "provider_fallback_authorized": False,
            "canonical_writes_authorized": False,
            "outreach_authorized": False,
        },
    }


def build_result_fixture(request: dict[str, Any] | None = None) -> dict[str, Any]:
    bound_request = request or build_request_fixture()
    request_hash = canonical_sha256(bound_request)
    target = bound_request["target"]
    observations = []
    for number in range(1, 4):
        object_id = f"xpost_fixture_{number:03d}"
        observations.append(
            {
                "observation_id": f"xprobe_obs_fixture_{number:03d}",
                "platform_object_id": object_id,
                "platform_user_id": target["platform_user_id"],
                "author_handle": target["current_handle"],
                "canonical_url": f"https://posts.invalid/{target['current_handle']}/status/{object_id}",
                "authored_at": f"2026-07-{10 + number:02d}T12:00:00.000Z",
                "observed_at": TIMESTAMP,
                "excerpt": CAPABILITY_OBSERVATION_EXCERPTS[object_id],
                "full_body_stored": False,
            }
        )
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "probe_id": bound_request["probe_id"],
        "request_sha256": request_hash,
        "execution_mode": "fixture_only",
        "run": {
            "run_id": "xprobe_run_fixture_openai_official_v1",
            "status": "completed",
            "started_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        },
        "task": {
            "task_id": "xprobe_task_fixture_openai_official_v1",
            "status": "succeeded",
            "stop_reason": "fixture_complete",
        },
        "capability": {
            "verdict": "fixture_contract_validated",
            "proof_scope": "offline_synthetic_only",
            "x_native_access_proven": False,
        },
        "provenance": {
            "provider_id": "offline_fixture",
            "access_mode": "fixture",
            "model_id": None,
            "tool_id": None,
            "provider_request_id": None,
            "prompt_version": "fixture-capability-v1",
            "request_sha256": request_hash,
            "raw_response_sha256": SYNTHETIC_RAW_RESPONSE_SHA256,
        },
        "usage": {
            "executions": 0,
            "external_calls": 0,
            "pages": 0,
            "observations": len(observations),
            "cost_usd": 0,
            "elapsed_ms": 0,
        },
        "observations": observations,
        "errors": [],
        "retention": {
            "class": "synthetic_fixture_only",
            "delete_after": None,
            "full_body_stored": False,
            "deletion_status": "not_applicable_synthetic",
        },
        "candidate_packets": [],
        "identity_link_proposals": [],
        "assertions": [],
        "canonical_writes": [],
        "claims": {
            "exhaustive": False,
            "current_employment_guaranteed": False,
            "outreach_permission": False,
            "researcher_mapping_authorized": False,
        },
    }


def _serialized(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _check(path: Path, expected: str) -> bool:
    try:
        return not path.is_symlink() and path.is_file() and path.read_text(encoding="utf-8") == expected
    except (OSError, UnicodeError):
        return False


def _owned_temp_prefix(path: Path) -> str:
    return f"{OWNED_TEMP_PREFIX}{path.name}."


def _is_owned_temp(path: Path, candidate: Path) -> bool:
    if candidate.parent != path.parent:
        return False
    pattern = rf"{re.escape(_owned_temp_prefix(path))}{_OWNED_TEMP_TOKEN_PATTERN}\.tmp"
    return re.fullmatch(pattern, candidate.name) is not None


def _reap_owned_stale_temps(paths: list[Path]) -> tuple[Path, ...]:
    reaped: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        for candidate in path.parent.iterdir():
            if candidate in seen or not _is_owned_temp(path, candidate):
                continue
            seen.add(candidate)
            if candidate.is_symlink() or candidate.is_file():
                candidate.unlink()
                reaped.append(candidate)
    return tuple(sorted(reaped, key=str))


def _write_same_directory_temp(path: Path, content: bytes) -> Path:
    for _attempt in range(100):
        temp_path = path.parent / f"{_owned_temp_prefix(path)}{secrets.token_hex(16)}.tmp"
        try:
            handle = temp_path.open("xb")
        except FileExistsError:
            continue
        break
    else:
        raise RuntimeError(f"could not allocate an owned fixture temp file for {path}")
    try:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path.chmod(0o644)
    except BaseException:
        handle.close()
        temp_path.unlink(missing_ok=True)
        raise
    handle.close()
    return temp_path


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _in_process_pair_lock(parent: Path) -> threading.Lock:
    key = parent.resolve(strict=True)
    with _PAIR_LOCK_REGISTRY_GUARD:
        lock = _PAIR_LOCK_REGISTRY.get(key)
        if lock is None:
            lock = threading.Lock()
            _PAIR_LOCK_REGISTRY[key] = lock
        return lock


def _lock_file_identity_is_safe(lock_fd: int, lock_path: Path) -> bool:
    descriptor_stat = os.fstat(lock_fd)
    try:
        path_stat = lock_path.stat(follow_symlinks=False)
    except OSError:
        return False
    return (
        stat.S_ISREG(descriptor_stat.st_mode)
        and stat.S_ISREG(path_stat.st_mode)
        and descriptor_stat.st_dev == path_stat.st_dev
        and descriptor_stat.st_ino == path_stat.st_ino
        and descriptor_stat.st_uid == os.geteuid()
        and descriptor_stat.st_nlink == 1
        and stat.S_IMODE(descriptor_stat.st_mode) == _PAIR_LOCK_FILE_MODE
    )


@contextlib.contextmanager
def _exclusive_pair_lock(parent: Path) -> Iterator[None]:
    deadline = time.monotonic() + PAIR_LOCK_TIMEOUT_SECONDS
    thread_lock = _in_process_pair_lock(parent)
    if not thread_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
        raise TimeoutError("capability fixture pair lock acquisition timed out")

    lock_fd: int | None = None
    file_lock_acquired = False
    try:
        lock_path = parent / PAIR_LOCK_FILENAME
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            lock_fd = os.open(lock_path, flags, _PAIR_LOCK_FILE_MODE)
        except OSError as error:
            if error.errno in {errno.ELOOP, errno.EMLINK}:
                raise ValueError("capability fixture pair lock must not be a symlink") from error
            raise
        if not _lock_file_identity_is_safe(lock_fd, lock_path):
            raise ValueError("capability fixture pair lock ownership or file identity is unsafe")

        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                file_lock_acquired = True
                break
            except BlockingIOError as error:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("capability fixture pair lock acquisition timed out") from error
                time.sleep(min(PAIR_LOCK_POLL_SECONDS, remaining))

        if not _lock_file_identity_is_safe(lock_fd, lock_path):
            raise ValueError("capability fixture pair lock changed during acquisition")
        os.fsync(lock_fd)
        _fsync_directory(parent)
        yield
    finally:
        try:
            if lock_fd is not None:
                try:
                    if file_lock_acquired:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
        finally:
            thread_lock.release()


def _atomic_write_many(values: tuple[tuple[Path, str], ...]) -> tuple[Path, ...]:
    paths = [path for path, _content in values]
    if len(paths) != 2:
        raise ValueError("capability fixture writer requires exactly two destinations")
    if len(set(paths)) != len(paths):
        raise ValueError("capability fixture destinations must be unique")
    parent = paths[0].parent
    if any(path.parent != parent for path in paths):
        raise ValueError("capability fixture destinations must share one directory")

    with _exclusive_pair_lock(parent):
        pending: dict[Path, Path] = {}
        try:
            for path in paths:
                if path.is_symlink():
                    raise ValueError("refusing to replace symlink fixture destination")
                if path.exists() and not path.is_file():
                    raise ValueError("fixture destination must be a regular file or absent")

            reaped_stale_temps = _reap_owned_stale_temps(paths)
            originals = {path: path.read_bytes() if path.exists() else None for path in paths}
            try:
                for path, content in values:
                    pending[path] = _write_same_directory_temp(path, content.encode("utf-8"))
            except BaseException:
                for temp_path in pending.values():
                    temp_path.unlink(missing_ok=True)
                raise

            replaced: list[Path] = []
            try:
                for path in paths:
                    os.replace(pending[path], path)
                    replaced.append(path)
                _fsync_directory(parent)
            except BaseException as write_error:
                rollback_error: BaseException | None = None
                for path in reversed(replaced):
                    try:
                        original = originals[path]
                        if original is None:
                            path.unlink(missing_ok=True)
                        else:
                            rollback_temp = _write_same_directory_temp(path, original)
                            try:
                                os.replace(rollback_temp, path)
                            finally:
                                rollback_temp.unlink(missing_ok=True)
                    except BaseException as error:
                        rollback_error = error
                try:
                    _fsync_directory(parent)
                except BaseException as error:
                    rollback_error = error
                if rollback_error is not None:
                    raise RuntimeError("capability fixture atomic-write rollback failed") from rollback_error
                raise write_error
            return reaped_stale_temps
        finally:
            for temp_path in pending.values():
                temp_path.unlink(missing_ok=True)
            _fsync_directory(parent)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate or check deterministic capability-probe fixtures")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true")
    action.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    request_path = ROOT / "fixtures/capability_probe_request_fixture_v1.json"
    result_path = ROOT / "fixtures/capability_probe_result_fixture_v1.json"
    request = build_request_fixture()
    values = (
        (request_path, _serialized(request)),
        (result_path, _serialized(build_result_fixture(request))),
    )
    if args.write:
        reaped_stale_temps = _atomic_write_many(values)
        print(
            json.dumps(
                {
                    "status": "written",
                    "paths": [str(path) for path, _ in values],
                    "reaped_stale_temp_paths": [str(path) for path in reaped_stale_temps],
                },
                indent=2,
            )
        )
        return 0

    stale = [str(path) for path, expected in values if not _check(path, expected)]
    if stale:
        print(json.dumps({"status": "stale", "paths": stale}, indent=2))
        return 1
    print(json.dumps({"status": "current", "paths": [str(path) for path, _ in values]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
