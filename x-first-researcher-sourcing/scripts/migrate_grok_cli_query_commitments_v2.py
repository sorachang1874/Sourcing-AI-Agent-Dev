#!/usr/bin/env python3
"""Fail-closed private migration to keyed Grok CLI query commitments v2.

``prepare``, ``evaluate`` and ``purge`` share one owner-only filesystem lock.
No operation prints private values or exception details.  Prepare emits a
schema/runtime-validated intended-public descriptor and immutable issuance
lineage only after every private input and recursive privacy boundary passes.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import grok_cli_exploration as exploration  # noqa: E402
from x_first.grok_cli_exploration import (  # noqa: E402
    canonical_json,
    canonical_sha256,
    evaluate_exploration,
    validate_evaluation_output,
)
from x_first.recall_pool_schema import load_contract_schema  # noqa: E402

SCHEME = "hmac-sha256-v1"
PRIVATE_RECEIPT_NAME = "grok-tool-receipt.commitment-v2.sanitized.json"
PRIVATE_RESULT_NAME = "grok-result.decision-codes-v2.sanitized.json"
PRIVATE_EVALUATION_NAME = "grok-exploration-evaluation.commitment-v2.sanitized.json"
MIGRATION_RECEIPT_NAME = "grok-query-commitment-migration.v2.receipt.json"
PURGE_RECEIPT_NAME = "grok-query-commitment-migration.v2.purge-receipt.json"
LOCK_NAME = ".grok-query-commitment-migration.v2.lock"
MAX_PRIVATE_JSON_BYTES = 64 * 1024 * 1024

PUBLIC_ALLOWED_DECISION_DIMENSIONS = [
    "lab_affiliation",
    "role_function",
    "pretraining_relevance",
]
PUBLIC_PROFESSIONAL_PROXY_ALLOWED = False
PUBLIC_PROTECTED_IDENTITY_QUERY_ALLOWED = False
PUBLIC_POLICY_ENABLED = True
DELETE_OWNER = "x_first_private_evidence_owner"
RETENTION_POLICY = "owner_controlled_query_commitment_migration_v2"

_LEGACY_POLICY_KEYS = {
    "schema_version",
    "policy_version",
    "purpose",
    "lab_id",
    "session_id",
    "request_id",
    "allowed_decision_dimensions",
    "professional_experience_proxy_query_allowed",
    "protected_identity_query_allowed",
    "query_manifest",
}
_LEGACY_POLICY_CALL_KEYS = {"tool_name", "arguments", "call_sha256"}
_MIGRATION_RECEIPT_KEYS = {
    "schema_version",
    "migration_id",
    "status",
    "idempotency",
    "source_to_target",
    "commitment",
    "retention",
    "public_artifacts",
    "evaluation",
    "purge",
}
_FORBIDDEN_PUBLIC_KEYS = {
    "arguments",
    "query",
    "queries",
    "session_id",
    "request_id",
    "key_hex",
    "nonce_hex",
    "bio_excerpt",
    "excerpt",
    "profile_url",
    "post_url",
    "url",
    "platform_user_id",
}
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_LAB_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


class MigrationError(ValueError):
    """Raised for a fail-closed migration contract violation."""


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non_finite_number:{value}")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _strict_json_loads(value: str) -> Any:
    return json.loads(value, object_pairs_hook=_strict_object, parse_constant=_reject_nonfinite)


def _schema_pointer(root: Mapping[str, Any], reference: str) -> Any:
    current: Any = root
    for part in reference.removeprefix("#/").split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    return current


def _public_schema_errors(
    value: Any,
    schema: Mapping[str, Any],
    *,
    root: Mapping[str, Any] | None = None,
    path: str = "$",
) -> list[str]:
    root = schema if root is None else root
    reference = schema.get("$ref")
    if isinstance(reference, str):
        return _public_schema_errors(value, _schema_pointer(root, reference), root=root, path=path)
    errors: list[str] = []
    expected_type = schema.get("type")
    type_matches = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }
    if isinstance(expected_type, str) and not type_matches.get(expected_type, False):
        return [f"{path}:type"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}:const")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}:enum")
    if isinstance(value, str):
        if "pattern" in schema and re.fullmatch(str(schema["pattern"]), value) is None:
            errors.append(f"{path}:pattern")
        if len(value) < int(schema.get("minLength", 0)) or len(value) > int(schema.get("maxLength", len(value))):
            errors.append(f"{path}:length")
    if isinstance(value, int) and not isinstance(value, bool) and value < int(schema.get("minimum", value)):
        errors.append(f"{path}:minimum")
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        if any(key not in value for key in required):
            errors.append(f"{path}:required")
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            errors.append(f"{path}:additional")
        for key, child in value.items():
            if key in properties:
                errors.extend(_public_schema_errors(child, properties[key], root=root, path=f"{path}.{key}"))
    if isinstance(value, list):
        if len(value) < int(schema.get("minItems", 0)) or len(value) > int(schema.get("maxItems", len(value))):
            errors.append(f"{path}:items")
        prefix = schema.get("prefixItems", [])
        for index, child_schema in enumerate(prefix):
            if index < len(value):
                errors.extend(_public_schema_errors(value[index], child_schema, root=root, path=f"{path}[{index}]"))
        items = schema.get("items")
        start = len(prefix) if prefix else 0
        if items is False and len(value) > start:
            errors.append(f"{path}:extra_items")
        elif isinstance(items, dict):
            for index in range(start, len(value)):
                errors.extend(_public_schema_errors(value[index], items, root=root, path=f"{path}[{index}]"))
    return errors


def _private_directory_stat(path: Path) -> os.stat_result:
    try:
        value = path.lstat()
    except OSError as exc:
        raise MigrationError("private_directory_invalid") from exc
    if (
        stat.S_ISLNK(value.st_mode)
        or not stat.S_ISDIR(value.st_mode)
        or value.st_uid != os.getuid()
        or stat.S_IMODE(value.st_mode) != 0o700
        or value.st_nlink < 2
    ):
        raise MigrationError("private_directory_invalid")
    return value


def _assert_private_ancestors(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise MigrationError("private_path_escape") from exc
    cursor = root
    _private_directory_stat(cursor)
    for component in relative.parts:
        cursor /= component
        _private_directory_stat(cursor)


@contextlib.contextmanager
def _validated_private_root(supplied: Path) -> Iterator[Path]:
    unresolved = supplied.expanduser()
    if not unresolved.is_absolute():
        unresolved = Path.cwd() / unresolved
    try:
        unresolved_stat = unresolved.lstat()
    except OSError as exc:
        raise MigrationError("private_root_invalid") from exc
    if stat.S_ISLNK(unresolved_stat.st_mode):
        raise MigrationError("private_root_symlink_forbidden")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(unresolved, flags)
    except OSError as exc:
        raise MigrationError("private_root_invalid") from exc
    try:
        descriptor_stat = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(descriptor_stat.st_mode)
            or descriptor_stat.st_uid != os.getuid()
            or stat.S_IMODE(descriptor_stat.st_mode) != 0o700
            or descriptor_stat.st_nlink < 2
            or (descriptor_stat.st_dev, descriptor_stat.st_ino) != (unresolved_stat.st_dev, unresolved_stat.st_ino)
        ):
            raise MigrationError("private_root_invalid")
        resolved = unresolved.resolve(strict=True)
        yield resolved
        final_stat = os.fstat(descriptor)
        path_stat = unresolved.lstat()
        if (final_stat.st_dev, final_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino):
            raise MigrationError("private_root_replaced")
    finally:
        os.close(descriptor)


def _open_private_json(path: Path, root: Path) -> dict[str, Any]:
    _assert_private_ancestors(root, path.parent)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise MigrationError("private_file_invalid") from exc
    try:
        descriptor_stat = os.fstat(descriptor)
        path_stat = path.lstat()
        if (
            not stat.S_ISREG(descriptor_stat.st_mode)
            or descriptor_stat.st_uid != os.getuid()
            or stat.S_IMODE(descriptor_stat.st_mode) != 0o600
            or descriptor_stat.st_nlink != 1
            or (descriptor_stat.st_dev, descriptor_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino)
            or descriptor_stat.st_size > MAX_PRIVATE_JSON_BYTES
        ):
            raise MigrationError("private_file_invalid")
        chunks: list[bytes] = []
        remaining = MAX_PRIVATE_JSON_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > MAX_PRIVATE_JSON_BYTES:
            raise MigrationError("private_file_too_large")
        value = _strict_json_loads(payload.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        if isinstance(exc, MigrationError):
            raise
        raise MigrationError("private_json_invalid") from exc
    finally:
        os.close(descriptor)
    if not isinstance(value, dict):
        raise MigrationError("private_json_not_object")
    return value


def _find_named(root: Path, name: str, *, required: bool = True) -> Path | None:
    matches: list[Path] = []
    for directory, names, files in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        _private_directory_stat(directory_path)
        for child in [*names, *files]:
            child_path = directory_path / child
            if child_path.is_symlink():
                raise MigrationError("private_tree_symlink_forbidden")
        if name in files:
            matches.append(directory_path / name)
    if len(matches) > 1 or (required and len(matches) != 1):
        raise MigrationError("private_source_count_invalid")
    return matches[0] if matches else None


def _relative_private_path(root: Path, path: Path) -> str:
    relative = path.relative_to(root)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise MigrationError("private_relative_path_invalid")
    return relative.as_posix()


def _path_from_receipt(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str):
        raise MigrationError("migration_receipt_path_invalid")
    parsed = Path(relative)
    if parsed.is_absolute() or ".." in parsed.parts or parsed.as_posix() != relative or not parsed.parts:
        raise MigrationError("migration_receipt_path_invalid")
    path = root / parsed
    _assert_private_ancestors(root, path.parent)
    return path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_private_json(path: Path, value: Any, root: Path, *, replace: bool) -> None:
    _assert_private_ancestors(root, path.parent)
    if path.is_symlink():
        raise MigrationError("private_destination_symlink_forbidden")
    if path.exists():
        if not replace:
            raise MigrationError("private_destination_exists")
        _open_private_json(path, root)
    payload = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(payload) > MAX_PRIVATE_JSON_BYTES:
        raise MigrationError("private_destination_too_large")
    directory_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    temporary_name = f".{path.name}.{secrets.token_hex(12)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_fd,
        )
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary_name, path.name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(directory_fd)
    _open_private_json(path, root)


def _unlink_private(path: Path, root: Path) -> None:
    _open_private_json(path, root)
    directory_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.unlink(path.name, dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@contextlib.contextmanager
def _migration_lock(root: Path) -> Iterator[None]:
    path = root / LOCK_NAME
    if path.is_symlink():
        raise MigrationError("migration_lock_symlink_forbidden")
    existed = path.exists()
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        descriptor_stat = os.fstat(descriptor)
        if (
            not stat.S_ISREG(descriptor_stat.st_mode)
            or descriptor_stat.st_uid != os.getuid()
            or stat.S_IMODE(descriptor_stat.st_mode) != 0o600
            or descriptor_stat.st_nlink != 1
        ):
            raise MigrationError("migration_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise MigrationError("migration_lock_busy") from exc
        if not existed:
            os.fsync(descriptor)
            _fsync_directory(root)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _commitment(key: bytes, nonce: bytes, *, domain: str, payload: Any) -> str:
    message = canonical_json({"domain": domain, "nonce_hex": nonce.hex(), "payload": payload}).encode()
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def _issuance_id(policy_version: str, run_binding: str, key_id: str, nonce_id: str) -> str:
    material = {
        "policy_version": policy_version,
        "run_binding_commitment": run_binding,
        "commitment_key_id": key_id,
        "commitment_nonce_id": nonce_id,
    }
    return f"qci_{hashlib.sha256(canonical_json(material).encode()).hexdigest()[:24]}"


def _validate_legacy_policy(policy: Any, receipt: Mapping[str, Any]) -> None:
    if not isinstance(policy, dict) or set(policy) != _LEGACY_POLICY_KEYS:
        raise MigrationError("legacy_policy_schema_invalid")
    if (
        policy.get("schema_version") != "x.grok_cli.exploration.query_policy.legacy_full.v0"
        or _VERSION_RE.fullmatch(str(policy.get("policy_version"))) is None
        or policy.get("purpose") != "base_researcher_discovery"
        or _LAB_RE.fullmatch(str(policy.get("lab_id"))) is None
        or policy.get("session_id") != receipt.get("session_id")
        or policy.get("request_id") != receipt.get("request_id")
        or policy.get("allowed_decision_dimensions") != PUBLIC_ALLOWED_DECISION_DIMENSIONS
        or policy.get("professional_experience_proxy_query_allowed") is not PUBLIC_PROFESSIONAL_PROXY_ALLOWED
        or policy.get("protected_identity_query_allowed") is not PUBLIC_PROTECTED_IDENTITY_QUERY_ALLOWED
    ):
        raise MigrationError("legacy_policy_binding_invalid")
    calls = receipt.get("calls")
    manifest = policy.get("query_manifest")
    if not isinstance(calls, list) or not isinstance(manifest, list) or len(calls) != len(manifest) or not calls:
        raise MigrationError("legacy_policy_call_count_invalid")
    for call, item in zip(calls, manifest, strict=True):
        if not isinstance(call, dict) or not isinstance(item, dict) or set(item) != _LEGACY_POLICY_CALL_KEYS:
            raise MigrationError("legacy_policy_call_invalid")
        expected_hash = canonical_sha256({"tool_name": call.get("tool_name"), "arguments": call.get("arguments")})
        if (
            item.get("tool_name") != call.get("tool_name")
            or canonical_json(item.get("arguments")) != canonical_json(call.get("arguments"))
            or item.get("call_sha256") != expected_hash
            or not isinstance(call.get("arguments"), dict)
            or not exploration._tool_arguments_valid(call["arguments"], str(call.get("tool_name")))
        ):
            raise MigrationError("legacy_policy_call_mismatch")


def _migrate_receipt(source: Mapping[str, Any], key: bytes, nonce: bytes) -> dict[str, Any]:
    if "query_commitment" in source:
        raise MigrationError("legacy_receipt_already_contains_commitment")
    migrated = dict(source)
    migrated["schema_version"] = "x.grok_cli.exploration.tool_receipt.v1"
    migrated["query_commitment"] = {
        "scheme": SCHEME,
        "key_id": hashlib.sha256(key).hexdigest(),
        "key_hex": key.hex(),
        "nonce_id": hashlib.sha256(nonce).hexdigest(),
        "nonce_hex": nonce.hex(),
    }
    exploration._validate_receipt(migrated)
    return migrated


def _migrate_result(source: Mapping[str, Any]) -> dict[str, Any]:
    status = source.get("status")
    status_code = {
        "X_SEARCH_OK": "native_x_search_completed",
        "X_SEARCH_PARTIAL": "native_x_search_partially_completed",
        "X_SEARCH_BLOCKED": "native_x_search_blocked",
    }.get(status)
    if status_code is None:
        raise MigrationError("legacy_result_status_invalid")
    candidates = source.get("candidates")
    excluded = source.get("excluded_examples")
    if not isinstance(candidates, list) or not isinstance(excluded, list):
        raise MigrationError("legacy_result_rows_invalid")
    migrated_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            raise MigrationError("legacy_result_candidate_invalid")
        migrated_candidate = dict(candidate)
        prior = migrated_candidate.pop("caveats", [])
        if not isinstance(prior, list):
            raise MigrationError("legacy_result_candidate_invalid")
        migrated_candidate["caveat_codes"] = ["model_mediated_unverified"] if prior else []
        migrated_candidates.append(migrated_candidate)
    migrated_excluded: list[dict[str, Any]] = []
    for item in excluded:
        if not isinstance(item, dict) or not isinstance(item.get("handle"), str):
            raise MigrationError("legacy_result_exclusion_invalid")
        migrated_excluded.append({"handle": item["handle"], "reason": "insufficient_base_discovery_evidence"})
    limitations = ["model_mediated_result_unverified", "provider_post_bodies_not_replayable"]
    if any(candidate.get("bio_excerpt") is None for candidate in migrated_candidates):
        limitations.append("bio_snapshot_not_source_bound")
    if any(candidate.get("platform_user_id") is None for candidate in migrated_candidates):
        limitations.append("stable_platform_user_id_not_source_bound")
    return {
        "status": status,
        "status_reason_code": status_code,
        "native_x_tool_provenance": source.get("native_x_tool_provenance"),
        "counts": source.get("counts"),
        "candidates": migrated_candidates,
        "excluded_examples": migrated_excluded,
        "limitation_codes": limitations,
        "local_reconciliation": source.get("local_reconciliation"),
    }


def _public_payload(receipt: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    private = receipt["query_commitment"]
    key = bytes.fromhex(private["key_hex"])
    nonce = bytes.fromhex(private["nonce_hex"])
    run_binding = _commitment(
        key,
        nonce,
        domain="x.grok_cli.exploration.run_binding.v2",
        payload={"session_id": receipt["session_id"], "request_id": receipt["request_id"]},
    )
    issuance_id = _issuance_id(policy["policy_version"], run_binding, private["key_id"], private["nonce_id"])
    descriptor = {
        "schema_version": "x.grok_cli.exploration.query_policy_descriptor.v2",
        "policy_version": policy["policy_version"],
        "purpose": "base_researcher_discovery",
        "lab_id": policy["lab_id"],
        "commitment_scheme": SCHEME,
        "commitment_issuance_id": issuance_id,
        "commitment_key_id": private["key_id"],
        "commitment_nonce_id": private["nonce_id"],
        "run_binding_commitment": run_binding,
        "legacy_full_policy_commitment": _commitment(
            key,
            nonce,
            domain="x.grok_cli.exploration.legacy_full_policy.v2",
            payload=policy,
        ),
        "allowed_decision_dimensions": list(PUBLIC_ALLOWED_DECISION_DIMENSIONS),
        "professional_experience_proxy_query_allowed": PUBLIC_PROFESSIONAL_PROXY_ALLOWED,
        "protected_identity_query_allowed": PUBLIC_PROTECTED_IDENTITY_QUERY_ALLOWED,
        "protected_category_boundary_version": exploration.PROTECTED_CATEGORY_BOUNDARY_VERSION,
        "query_manifest": [
            {
                "sequence": sequence,
                "tool_name": call["tool_name"],
                "call_commitment": _commitment(
                    key,
                    nonce,
                    domain="x.grok_cli.exploration.call.v2",
                    payload={"tool_name": call["tool_name"], "arguments": call["arguments"]},
                ),
            }
            for sequence, call in enumerate(receipt["calls"])
        ],
    }
    issuance = {
        "lineage_position": 0,
        "issuance_id": issuance_id,
        "policy_version": descriptor["policy_version"],
        "run_binding_commitment": descriptor["run_binding_commitment"],
        "commitment_key_id": descriptor["commitment_key_id"],
        "commitment_nonce_id": descriptor["commitment_nonce_id"],
    }
    registry = {
        "schema_version": "x.grok_cli.exploration.query_policy_registry.v2",
        "registry_version": "approved-query-policies-v2",
        "commitment_issuance_lineage": [issuance],
        "policies": [
            {
                "policy_version": descriptor["policy_version"],
                "policy_path": "configs/grok_cli_exploration_query_policy_descriptor.v2.json",
                "policy_sha256": canonical_sha256(descriptor),
                "commitment_scheme": SCHEME,
                "commitment_issuance_id": issuance_id,
                "commitment_key_id": descriptor["commitment_key_id"],
                "commitment_nonce_id": descriptor["commitment_nonce_id"],
                "legacy_full_policy_commitment": descriptor["legacy_full_policy_commitment"],
                "policy_schema_version": descriptor["schema_version"],
                "purpose": descriptor["purpose"],
                "lab_id": descriptor["lab_id"],
                "protected_category_boundary_version": descriptor["protected_category_boundary_version"],
                "run_binding_commitment": descriptor["run_binding_commitment"],
                "enabled": PUBLIC_POLICY_ENABLED,
            }
        ],
    }
    return {"policy": descriptor, "registry": registry}


def _sensitive_private_values(*values: Any) -> set[str]:
    sensitive: set[str] = set()
    stack = list(values)
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                if key in _FORBIDDEN_PUBLIC_KEYS:
                    if isinstance(child, str) and len(child) >= 4:
                        sensitive.add(child)
                    elif isinstance(child, (dict, list)):
                        child_stack = [child]
                        while child_stack:
                            nested = child_stack.pop()
                            if isinstance(nested, dict):
                                child_stack.extend(nested.values())
                            elif isinstance(nested, list):
                                child_stack.extend(nested)
                            elif isinstance(nested, str) and len(nested) >= 4:
                                sensitive.add(nested)
                stack.append(child)
        elif isinstance(value, list):
            stack.extend(value)
    return sensitive


def _assert_public_safe(payload: Any, sensitive_values: set[str]) -> None:
    stack = [payload]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                if key in _FORBIDDEN_PUBLIC_KEYS:
                    raise MigrationError("public_forbidden_key")
                stack.append(child)
        elif isinstance(value, list):
            stack.extend(value)
        elif isinstance(value, str):
            for private in sensitive_values:
                if private in value:
                    raise MigrationError("public_forbidden_value")


def _validate_public_artifacts(
    artifacts: Mapping[str, Any],
    receipt: Mapping[str, Any],
    sensitive_values: set[str],
) -> None:
    if set(artifacts) != {"policy", "registry"}:
        raise MigrationError("public_artifact_envelope_invalid")
    descriptor = artifacts["policy"]
    registry = artifacts["registry"]
    for value, schema_name in (
        (descriptor, "x.grok_cli.exploration.query_policy_descriptor.v2.schema.json"),
        (registry, "x.grok_cli.exploration.query_policy_registry.v2.schema.json"),
    ):
        if _public_schema_errors(value, load_contract_schema(schema_name)):
            raise MigrationError("public_artifact_schema_invalid")
    if (
        descriptor.get("allowed_decision_dimensions") != PUBLIC_ALLOWED_DECISION_DIMENSIONS
        or descriptor.get("professional_experience_proxy_query_allowed") is not PUBLIC_PROFESSIONAL_PROXY_ALLOWED
        or descriptor.get("protected_identity_query_allowed") is not PUBLIC_PROTECTED_IDENTITY_QUERY_ALLOWED
        or descriptor.get("protected_category_boundary_version") != exploration.PROTECTED_CATEGORY_BOUNDARY_VERSION
        or registry.get("policies", [{}])[0].get("enabled") is not PUBLIC_POLICY_ENABLED
    ):
        raise MigrationError("public_constant_binding_invalid")
    row = registry["policies"][0]
    issuance = registry["commitment_issuance_lineage"][0]
    if (
        len(registry["policies"]) != 1
        or len(registry["commitment_issuance_lineage"]) != 1
        or row["policy_sha256"] != canonical_sha256(descriptor)
        or row["commitment_issuance_id"] != issuance["issuance_id"]
        or row["protected_category_boundary_version"] != descriptor["protected_category_boundary_version"]
        or issuance["lineage_position"] != 0
        or any(
            row[field] != issuance[field]
            for field in (
                "policy_version",
                "run_binding_commitment",
                "commitment_key_id",
                "commitment_nonce_id",
            )
        )
    ):
        raise MigrationError("public_registry_runtime_invalid")
    exploration._validate_query_policy_body(
        descriptor,
        row,
        receipt,
        commitment_material=exploration._receipt_commitment_material(receipt),
    )
    _assert_public_safe(artifacts, sensitive_values)


def _mapping(
    role: str,
    source_path: str,
    source_hash: str,
    target_path: str,
    target_hash: str,
) -> dict[str, str]:
    return {
        "role": role,
        "source_path": source_path,
        "source_canonical_sha256": source_hash,
        "target_path": target_path,
        "target_canonical_sha256": target_hash,
    }


def _new_migration_receipt(
    root: Path,
    source_paths: Mapping[str, Path],
    sources: Mapping[str, Mapping[str, Any]],
    target_paths: Mapping[str, Path],
    targets: Mapping[str, Mapping[str, Any]],
    public_artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    source_hashes = {role: canonical_sha256(value) for role, value in sources.items()}
    idempotency_key = canonical_sha256(source_hashes)
    migration_id = f"xqcm_{idempotency_key[:24]}"
    mappings = [
        _mapping(
            "tool_receipt",
            _relative_private_path(root, source_paths["receipt"]),
            source_hashes["receipt"],
            _relative_private_path(root, target_paths["receipt"]),
            canonical_sha256(targets["receipt"]),
        ),
        _mapping(
            "result",
            _relative_private_path(root, source_paths["result"]),
            source_hashes["result"],
            _relative_private_path(root, target_paths["result"]),
            canonical_sha256(targets["result"]),
        ),
        _mapping(
            "query_policy",
            _relative_private_path(root, source_paths["policy"]),
            source_hashes["policy"],
            "intended-public:configs/grok_cli_exploration_query_policy_descriptor.v2.json",
            canonical_sha256(public_artifacts["policy"]),
        ),
    ]
    commitment = targets["receipt"]["query_commitment"]
    return {
        "schema_version": "x.grok_cli.query_commitment_migration.receipt.v2",
        "migration_id": migration_id,
        "status": "prepared",
        "idempotency": {
            "key": idempotency_key,
            "identical_rerun_allowed": True,
            "last_operation": "prepare_created",
        },
        "source_to_target": mappings,
        "commitment": {
            "scheme": SCHEME,
            "key_id": commitment["key_id"],
            "nonce_id": commitment["nonce_id"],
            "issuance_id": public_artifacts["policy"]["commitment_issuance_id"],
        },
        "retention": {
            "policy": RETENTION_POLICY,
            "delete_owner": DELETE_OWNER,
            "source_state": "retained_pending_explicit_purge",
            "target_state": "retained_for_offline_replay",
        },
        "public_artifacts": dict(public_artifacts),
        "evaluation": {"path": None, "canonical_sha256": None},
        "purge": {"receipt_path": None, "receipt_sha256": None},
    }


def _validate_migration_receipt(receipt: Any) -> None:
    if not isinstance(receipt, dict) or set(receipt) != _MIGRATION_RECEIPT_KEYS:
        raise MigrationError("migration_receipt_schema_invalid")
    idempotency = receipt.get("idempotency")
    commitment = receipt.get("commitment")
    retention = receipt.get("retention")
    evaluation = receipt.get("evaluation")
    purge = receipt.get("purge")
    if (
        receipt.get("schema_version") != "x.grok_cli.query_commitment_migration.receipt.v2"
        or re.fullmatch(r"xqcm_[0-9a-f]{24}", str(receipt.get("migration_id"))) is None
        or receipt.get("status") not in {"prepared", "evaluated", "purge_in_progress", "sources_purged"}
        or not isinstance(idempotency, dict)
        or set(idempotency) != {"key", "identical_rerun_allowed", "last_operation"}
        or _SHA256_RE.fullmatch(str(idempotency.get("key"))) is None
        or idempotency.get("identical_rerun_allowed") is not True
        or not isinstance(idempotency.get("last_operation"), str)
        or not isinstance(commitment, dict)
        or set(commitment) != {"scheme", "key_id", "nonce_id", "issuance_id"}
        or commitment.get("scheme") != SCHEME
        or _SHA256_RE.fullmatch(str(commitment.get("key_id"))) is None
        or _SHA256_RE.fullmatch(str(commitment.get("nonce_id"))) is None
        or re.fullmatch(r"qci_[0-9a-f]{24}", str(commitment.get("issuance_id"))) is None
        or not isinstance(retention, dict)
        or set(retention) != {"policy", "delete_owner", "source_state", "target_state"}
        or retention.get("policy") != RETENTION_POLICY
        or retention.get("delete_owner") != DELETE_OWNER
        or retention.get("source_state")
        not in {"retained_pending_explicit_purge", "purge_in_progress", "purged_with_tombstone"}
        or retention.get("target_state") != "retained_for_offline_replay"
        or not isinstance(evaluation, dict)
        or set(evaluation) != {"path", "canonical_sha256"}
        or not isinstance(purge, dict)
        or set(purge) != {"receipt_path", "receipt_sha256"}
        or not isinstance(receipt.get("public_artifacts"), dict)
    ):
        raise MigrationError("migration_receipt_value_invalid")
    mappings = receipt.get("source_to_target")
    if not isinstance(mappings, list) or [row.get("role") for row in mappings if isinstance(row, dict)] != [
        "tool_receipt",
        "result",
        "query_policy",
    ]:
        raise MigrationError("migration_receipt_mapping_invalid")
    for row in mappings:
        if not isinstance(row, dict) or set(row) != {
            "role",
            "source_path",
            "source_canonical_sha256",
            "target_path",
            "target_canonical_sha256",
        }:
            raise MigrationError("migration_receipt_mapping_invalid")
        if (
            _SHA256_RE.fullmatch(str(row.get("source_canonical_sha256"))) is None
            or _SHA256_RE.fullmatch(str(row.get("target_canonical_sha256"))) is None
        ):
            raise MigrationError("migration_receipt_mapping_invalid")


def _load_existing_state(root: Path) -> tuple[Path, dict[str, Any]] | None:
    path = _find_named(root, MIGRATION_RECEIPT_NAME, required=False)
    if path is None:
        return None
    receipt = _open_private_json(path, root)
    _validate_migration_receipt(receipt)
    return path, receipt


def _validate_existing_targets(root: Path, receipt: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    mappings = {row["role"]: row for row in receipt["source_to_target"]}
    targets: dict[str, dict[str, Any]] = {}
    for role in ("tool_receipt", "result"):
        path = _path_from_receipt(root, mappings[role]["target_path"])
        value = _open_private_json(path, root)
        if canonical_sha256(value) != mappings[role]["target_canonical_sha256"]:
            raise MigrationError("existing_private_target_stale")
        targets[role] = value
    private = targets["tool_receipt"].get("query_commitment")
    if (
        not isinstance(private, dict)
        or private.get("key_id") != receipt["commitment"]["key_id"]
        or private.get("nonce_id") != receipt["commitment"]["nonce_id"]
    ):
        raise MigrationError("existing_commitment_binding_invalid")
    exploration._validate_receipt(targets["tool_receipt"])
    exploration._validate_result(targets["result"], targets["tool_receipt"]["calls"])
    _validate_public_artifacts(
        receipt["public_artifacts"],
        targets["tool_receipt"],
        _sensitive_private_values(targets["tool_receipt"], targets["result"]),
    )
    return targets


def _validate_existing_sources(root: Path, receipt: Mapping[str, Any]) -> None:
    source_state = receipt["retention"]["source_state"]
    for row in receipt["source_to_target"]:
        path = _path_from_receipt(root, row["source_path"])
        if source_state == "purged_with_tombstone":
            if path.exists() or path.is_symlink():
                raise MigrationError("purged_source_reappeared")
            continue
        if not path.exists():
            if source_state == "purge_in_progress":
                continue
            raise MigrationError("legacy_source_missing")
        if canonical_sha256(_open_private_json(path, root)) != row["source_canonical_sha256"]:
            raise MigrationError("legacy_source_changed")


def _prepare(root: Path) -> dict[str, Any]:
    existing = _load_existing_state(root)
    if existing is not None:
        receipt_path, migration = existing
        targets = _validate_existing_targets(root, migration)
        _validate_existing_sources(root, migration)
        migration["idempotency"]["last_operation"] = "prepare_identical_replay"
        _atomic_private_json(receipt_path, migration, root, replace=True)
        return {
            "status": "prepared",
            "idempotent": True,
            **migration["public_artifacts"],
        }
    if any(
        _find_named(root, name, required=False) is not None
        for name in (PRIVATE_RECEIPT_NAME, PRIVATE_RESULT_NAME, PRIVATE_EVALUATION_NAME, PURGE_RECEIPT_NAME)
    ):
        raise MigrationError("partial_or_stale_destination")
    source_paths = {
        "receipt": _find_named(root, "grok-tool-receipt.sanitized.json"),
        "result": _find_named(root, "grok-result.two-axis.sanitized.json"),
        "policy": _find_named(root, "private-query-policy.full.v1.json"),
    }
    if any(path is None for path in source_paths.values()):
        raise MigrationError("legacy_source_missing")
    concrete_source_paths = {role: path for role, path in source_paths.items() if path is not None}
    sources = {role: _open_private_json(path, root) for role, path in concrete_source_paths.items()}
    _validate_legacy_policy(sources["policy"], sources["receipt"])
    key = secrets.token_bytes(32)
    nonce = secrets.token_bytes(32)
    migrated_receipt = _migrate_receipt(sources["receipt"], key, nonce)
    migrated_result = _migrate_result(sources["result"])
    exploration._validate_result(migrated_result, migrated_receipt["calls"])
    public_artifacts = _public_payload(migrated_receipt, sources["policy"])
    sensitive = _sensitive_private_values(*sources.values(), migrated_receipt, migrated_result)
    _validate_public_artifacts(public_artifacts, migrated_receipt, sensitive)
    target_paths = {
        "receipt": concrete_source_paths["receipt"].with_name(PRIVATE_RECEIPT_NAME),
        "result": concrete_source_paths["result"].with_name(PRIVATE_RESULT_NAME),
    }
    targets = {"receipt": migrated_receipt, "result": migrated_result}
    migration = _new_migration_receipt(
        root,
        concrete_source_paths,
        sources,
        target_paths,
        targets,
        public_artifacts,
    )
    receipt_path = concrete_source_paths["receipt"].with_name(MIGRATION_RECEIPT_NAME)
    _atomic_private_json(target_paths["receipt"], migrated_receipt, root, replace=False)
    _atomic_private_json(target_paths["result"], migrated_result, root, replace=False)
    _atomic_private_json(receipt_path, migration, root, replace=False)
    return {"status": "prepared", "idempotent": False, **public_artifacts}


def _evaluate(root: Path) -> dict[str, Any]:
    existing = _load_existing_state(root)
    if existing is None:
        raise MigrationError("migration_receipt_missing")
    receipt_path, migration = existing
    targets = _validate_existing_targets(root, migration)
    mappings = {row["role"]: row for row in migration["source_to_target"]}
    evaluation_binding = migration["evaluation"]
    if evaluation_binding["path"] is not None:
        evaluation_path = _path_from_receipt(root, evaluation_binding["path"])
        evaluation = _open_private_json(evaluation_path, root)
        if canonical_sha256(evaluation) != evaluation_binding["canonical_sha256"]:
            raise MigrationError("existing_evaluation_stale")
        validate_evaluation_output(evaluation, targets["result"], targets["tool_receipt"])
        migration["idempotency"]["last_operation"] = "evaluate_identical_replay"
        _atomic_private_json(receipt_path, migration, root, replace=True)
        return {
            "status": "private_evaluation_written",
            "idempotent": True,
            "evaluation_sha256": evaluation_binding["canonical_sha256"],
        }
    if migration["status"] != "prepared":
        raise MigrationError("migration_state_invalid_for_evaluate")
    evaluation = evaluate_exploration(targets["result"], targets["tool_receipt"])
    validate_evaluation_output(evaluation, targets["result"], targets["tool_receipt"])
    target_receipt_path = _path_from_receipt(root, mappings["tool_receipt"]["target_path"])
    evaluation_path = target_receipt_path.with_name(PRIVATE_EVALUATION_NAME)
    _atomic_private_json(evaluation_path, evaluation, root, replace=False)
    migration["status"] = "evaluated"
    migration["idempotency"]["last_operation"] = "evaluate_created"
    migration["evaluation"] = {
        "path": _relative_private_path(root, evaluation_path),
        "canonical_sha256": canonical_sha256(evaluation),
    }
    _atomic_private_json(receipt_path, migration, root, replace=True)
    return {
        "status": "private_evaluation_written",
        "idempotent": False,
        "evaluation_sha256": canonical_sha256(evaluation),
    }


def _purge(root: Path, *, confirm: bool) -> dict[str, Any]:
    if not confirm:
        raise MigrationError("source_purge_confirmation_required")
    existing = _load_existing_state(root)
    if existing is None:
        raise MigrationError("migration_receipt_missing")
    receipt_path, migration = existing
    _validate_existing_targets(root, migration)
    if migration["status"] == "sources_purged":
        _validate_existing_sources(root, migration)
        purge_path = _path_from_receipt(root, migration["purge"]["receipt_path"])
        purge_receipt = _open_private_json(purge_path, root)
        if canonical_sha256(purge_receipt) != migration["purge"]["receipt_sha256"]:
            raise MigrationError("purge_receipt_stale")
        migration["idempotency"]["last_operation"] = "purge_identical_replay"
        _atomic_private_json(receipt_path, migration, root, replace=True)
        return {"status": "sources_purged", "idempotent": True, "tombstone_id": purge_receipt["tombstone_id"]}
    if migration["status"] not in {"evaluated", "purge_in_progress"}:
        raise MigrationError("migration_state_invalid_for_purge")
    if migration["status"] == "evaluated":
        _validate_existing_sources(root, migration)
        migration["status"] = "purge_in_progress"
        migration["retention"]["source_state"] = "purge_in_progress"
        migration["idempotency"]["last_operation"] = "purge_started"
        _atomic_private_json(receipt_path, migration, root, replace=True)
    deleted_sources: list[dict[str, str]] = []
    for row in migration["source_to_target"]:
        source_path = _path_from_receipt(root, row["source_path"])
        if source_path.exists():
            if canonical_sha256(_open_private_json(source_path, root)) != row["source_canonical_sha256"]:
                raise MigrationError("legacy_source_changed")
            _unlink_private(source_path, root)
        elif source_path.is_symlink():
            raise MigrationError("private_tree_symlink_forbidden")
        deleted_sources.append(
            {
                "role": row["role"],
                "source_path": row["source_path"],
                "source_canonical_sha256": row["source_canonical_sha256"],
            }
        )
    tombstone_id = f"xqcmt_{canonical_sha256(deleted_sources)[:24]}"
    purge_receipt = {
        "schema_version": "x.grok_cli.query_commitment_migration.purge_receipt.v1",
        "migration_id": migration["migration_id"],
        "tombstone_id": tombstone_id,
        "status": "sources_purged",
        "delete_owner": DELETE_OWNER,
        "deleted_sources": deleted_sources,
    }
    purge_path = receipt_path.with_name(PURGE_RECEIPT_NAME)
    if purge_path.exists():
        existing_purge = _open_private_json(purge_path, root)
        if canonical_json(existing_purge) != canonical_json(purge_receipt):
            raise MigrationError("purge_receipt_stale")
    else:
        _atomic_private_json(purge_path, purge_receipt, root, replace=False)
    migration["status"] = "sources_purged"
    migration["retention"]["source_state"] = "purged_with_tombstone"
    migration["idempotency"]["last_operation"] = "purge_completed"
    migration["purge"] = {
        "receipt_path": _relative_private_path(root, purge_path),
        "receipt_sha256": canonical_sha256(purge_receipt),
    }
    _atomic_private_json(receipt_path, migration, root, replace=True)
    return {"status": "sources_purged", "idempotent": False, "tombstone_id": tombstone_id}


def run_operation(root: Path, mode: str, *, confirm_source_purge: bool = False) -> dict[str, Any]:
    with _validated_private_root(root) as private_root, _migration_lock(private_root):
        if mode == "prepare":
            return _prepare(private_root)
        if mode == "evaluate":
            return _evaluate(private_root)
        if mode == "purge":
            return _purge(private_root, confirm=confirm_source_purge)
        raise MigrationError("migration_mode_invalid")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("prepare", "evaluate", "purge"))
    parser.add_argument("--private-root", type=Path, default=ROOT / "runtime")
    parser.add_argument("--confirm-source-purge", action="store_true")
    args = parser.parse_args()
    try:
        output = run_operation(
            args.private_root,
            args.mode,
            confirm_source_purge=args.confirm_source_purge,
        )
    except Exception:
        print(json.dumps({"error": "QUERY_COMMITMENT_MIGRATION_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
