#!/usr/bin/env python3
"""Fail-closed checks for ChatGPT Pro advisory consultation artifacts."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote, unquote

CONTRACT_SCHEMA_VERSION = "chatgpt-pro-consultation.v2"
REDACTION_RULES_VERSION = "pro-consult-redaction.v1"
RECOMMENDATIONS = frozenset({"keep", "adjust", "pivot"})
PURPOSES = frozenset({"kickoff", "architecture", "approach_review", "critical_debug", "milestone_audit"})
CONNECTOR_SCOPE_PREFIX = "CONNECTOR_SCOPE_JSON:"
REQUEST_HEADER_FIELDS = (
    "Purpose",
    "Authority",
    "Surface required",
    "Model required",
    "Mode required",
    "Browser required",
    "Local state",
    "Connector sees dirty scope",
    "Dirty scope provided to Pro",
    "Branch",
    "Branch requirement",
)
REQUIRED_RESPONSE_HEADINGS = (
    "# Verdict",
    "# Scope understood",
    "# Assumptions and missing information",
    "# Findings",
    "# Recommended sequence",
    "# Validation and failure modes",
    "# Deferred decisions",
    "# Owner decisions required",
)
REQUIRED_FINDING_HEADINGS = ("## P0", "## P1", "## P2")
REQUIRED_DECISION_HEADINGS = ("# Decision", "## Authority", "## Local disposition", "## Follow-up")
ADVISORY_LABEL = "ADVISORY_ONLY — not an independent-review artifact or formal GO."
REQUIRED_UI_STATE = {
    "required_surface": "Chat",
    "required_model": "GPT-5.6 Sol",
    "required_mode": "Pro",
    "required_browser": "Codex in-app browser",
    "observed_surface": "Chat",
    "observed_model": "GPT-5.6 Sol",
    "observed_mode": "Pro",
    "browser": "Codex in-app browser",
}
VALID_BRANCH_LOOKUPS = frozenset({"verified", "not_found", "lookup_unsupported", "resolved_other_sha", "not_requested"})
SAFE_INVALID_STATUSES = frozenset(
    {"planned", "blocked_pre_send", "sent", "incomplete", "blocked_connector", "legacy_advisory_unverified"}
)
REQUIRED_EXCLUDED_CATEGORIES = frozenset(
    {
        "credentials",
        "oauth_session",
        "cookies",
        "environment_files",
        "private_personal_data",
        "system_instructions",
        "irrelevant_logs",
    }
)
DENIED_PATH_PARTS = frozenset(
    {
        ".env",
        ".netrc",
        ".npmrc",
        ".pypirc",
        ".ssh",
        "auth.json",
        "cookies.sqlite",
        "oauth_token.json",
        "sourcing-ai-agent-input",
        "token.json",
    }
)
SECRET_PATTERNS: dict[str, re.Pattern[str]] = {
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "openai_key": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
    "github_token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    "gitlab_token": re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b"),
    "google_api_key": re.compile(r"\bAIza[A-Za-z0-9_-]{30,}\b"),
    "anthropic_key": re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b"),
    "huggingface_token": re.compile(r"\bhf_[A-Za-z0-9]{20,}\b"),
    "stripe_live_key": re.compile(r"\b(?:sk|rk)_live_[A-Za-z0-9]{20,}\b"),
    "xai_key": re.compile(r"\bxai-[A-Za-z0-9_-]{20,}\b"),
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "aws_secret_key": re.compile(r"(?i)\bAWS_SECRET_ACCESS_KEY\s*[:=]\s*[\"']?[A-Za-z0-9/+=]{40}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "bearer_token": re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{24,}"),
    "secret_assignment": re.compile(
        r"(?i)[\"']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret|cookie)"
        r"[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9._~+/=-]{12,}"
    ),
}
METADATA_ALLOWED_FIELDS = frozenset(
    {
        "advisory_only",
        "base_commit_sha",
        "branch",
        "branch_lookup_outcome",
        "branch_requirement",
        "branch_resolved_sha",
        "branch_verification",
        "browser",
        "commit_verification",
        "completed_at",
        "connector_diff_proof",
        "connector_evidence_valid",
        "connector_file_proof",
        "connector_scope_manifest",
        "connector_scope_manifest_sha256",
        "connector_sees_dirty_scope",
        "connector_status",
        "consultation_contract_sha256",
        "consultation_skill_sha256",
        "consultation_status",
        "consultation_valid",
        "contract_schema_version",
        "decision_sha256",
        "dirty_diff_sha256",
        "dirty_scope_provided_to_pro",
        "dirty_scope_transfer",
        "expected_response_path",
        "failure_reason",
        "formal_gate_eligible",
        "formal_review_status",
        "local_dirty_inventory_sha256",
        "local_dirty_inventory_status",
        "local_dirty_path_count",
        "local_dirty_paths",
        "local_state",
        "normalized_recommendation",
        "observed_commit_sha",
        "observed_mode",
        "observed_model",
        "observed_surface",
        "outcome_code",
        "purpose",
        "raw_verdict",
        "redaction_preflight",
        "redaction_status",
        "repository",
        "requested_at",
        "requested_commit_sha",
        "required_browser",
        "required_mode",
        "required_model",
        "required_surface",
        "response_complete",
        "response_marker_envelope_preserved",
        "response_schema_valid",
        "response_sha256",
        "retention_safe",
        "secondary_question_sets",
        "transfer_content_preflight",
        "ui_state_verification",
        "usable_for_advisory_decision",
        "validator_sha256",
        "workflow_sha256",
        "request_sha256",
    }
)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _manifest_sha256(*, included_paths: list[str], excluded_categories: list[str]) -> str:
    return _sha256_text(
        _canonical_json(
            {
                "excluded_categories": sorted(set(excluded_categories)),
                "included_paths": sorted(set(included_paths)),
            }
        )
    )


def _path_is_denied(raw_path: str) -> bool:
    for part in Path(raw_path).parts:
        folded = part.casefold()
        if folded in DENIED_PATH_PARTS or folded.startswith(".env"):
            return True
        if any(
            token in folded
            for token in ("cookie", "credential", "oauth-token", "oauth_token", "refresh-token", "refresh_token")
        ):
            return True
    return False


def _is_safe_repo_path(raw_path: Any) -> bool:
    if not isinstance(raw_path, str) or not raw_path.startswith("/") or raw_path == "/":
        return False
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in raw_path):
        return False
    path = PurePosixPath(raw_path)
    return ".." not in path.parts and str(path) == raw_path


def _path_is_referenced(request_text: str, raw_path: str) -> bool:
    if raw_path == "connector:none":
        return '"mode":"unused"' in request_text
    if raw_path.startswith("git-diff:"):
        match = re.fullmatch(r"git-diff:([0-9a-f]{40})\.\.([0-9a-f]{40})", raw_path)
        return bool(match and match.group(1) in request_text and match.group(2) in request_text)
    return raw_path in request_text


def scan_redacted_request(
    request_text: str,
    *,
    included_paths: list[str],
    excluded_categories: list[str],
) -> dict[str, Any]:
    secret_match_counts = _secret_match_counts(request_text)
    normalized_paths = sorted(set(included_paths))
    normalized_categories = sorted(set(excluded_categories))
    denied_paths = [raw_path for raw_path in normalized_paths if _path_is_denied(raw_path)]
    unreferenced_paths = [raw_path for raw_path in normalized_paths if not _path_is_referenced(request_text, raw_path)]
    missing_categories = sorted(REQUIRED_EXCLUDED_CATEGORIES - set(excluded_categories))
    secret_match_count = sum(secret_match_counts.values())
    allowlist_passed = bool(normalized_paths) and not denied_paths and not unreferenced_paths and not missing_categories
    return {
        "rules_version": REDACTION_RULES_VERSION,
        "request_sha256": _sha256_text(request_text),
        "allowlist_status": "passed" if allowlist_passed else "blocked",
        "included_paths": normalized_paths,
        "included_path_count": len(normalized_paths),
        "included_paths_sha256": _sha256_text("\n".join(normalized_paths)),
        "excluded_categories": normalized_categories,
        "missing_excluded_categories": missing_categories,
        "denied_path_count": len(denied_paths),
        "unreferenced_path_count": len(unreferenced_paths),
        "manifest_sha256": _manifest_sha256(
            included_paths=normalized_paths,
            excluded_categories=normalized_categories,
        ),
        "secret_match_counts": secret_match_counts,
        "secret_match_count": secret_match_count,
        "status": "passed" if allowlist_passed and secret_match_count == 0 else "blocked",
    }


def _secret_match_counts(value: str) -> dict[str, int]:
    variants = {value}
    current = value
    for _ in range(2):
        current = re.sub(
            r"\\u([0-9a-fA-F]{4})|\\x([0-9a-fA-F]{2})",
            lambda match: chr(int(match.group(1) or match.group(2), 16)),
            current,
        )
        current = re.sub(r"\\([\\`*_{}\[\]()#+\-.!])", r"\1", current)
        current = html.unescape(unquote(current))
        variants.add(current)
    return {
        name: max(len(pattern.findall(variant)) for variant in variants) for name, pattern in SECRET_PATTERNS.items()
    }


def _git_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=Path(__file__).resolve().parent,
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(completed.stdout.strip())


def _run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=_git_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _local_origin_repository() -> str | None:
    try:
        remote = _run_git("remote", "get-url", "origin").strip()
    except (OSError, subprocess.CalledProcessError, UnicodeError):
        return None
    match = re.fullmatch(
        r"(?:https://github\.com/|ssh://git@github\.com/|git@github\.com:)([^/]+/[^/]+?)(?:\.git)?", remote
    )
    return match.group(1) if match else None


def _parse_request_headers(request_text: str, errors: list[str]) -> dict[str, str]:
    lines = [re.sub(r"^(?:>\s*)+", "", line) for line in _outside_fenced_code_lines(request_text)]
    values: dict[str, str] = {}
    for field in REQUEST_HEADER_FIELDS:
        prefix = f"{field}:"
        matches = [line[len(prefix) :].strip() for line in lines if line.startswith(prefix)]
        if len(matches) != 1:
            errors.append(f"request header missing or duplicated: {field}")
        else:
            values[field] = matches[0]
    return values


def _request_absolute_paths(request_text: str) -> set[str]:
    paths: set[str] = set()
    for line in _outside_fenced_code_lines(request_text):
        for match in re.finditer(r"(?<![A-Za-z0-9:/])(/[A-Za-z0-9_.@+%~/-]+)", line):
            paths.add(match.group(1).rstrip(".,;:)\"]'"))
    return paths


def scan_transfer_content(scope: dict[str, Any]) -> dict[str, Any]:
    """Recompute the exact committed payload that a Connector request is allowed to expose."""

    mode = scope.get("mode")
    requested_paths: list[str] = []
    actual_changed_files: list[str] = []
    denied_paths: list[str] = []
    git_errors: list[str] = []
    binary_path_count = 0
    non_blob_path_count = 0
    content_parts: list[str] = []
    try:
        if mode == "files":
            requested_paths = sorted(str(path) for path in scope.get("required_files", []))
            denied_paths = [path for path in requested_paths if _path_is_denied(path)]
            commit_sha = str(scope.get("commit_sha") or "")
            for path in requested_paths:
                object_spec = f"{commit_sha}:{path.lstrip('/')}"
                if _run_git("cat-file", "-t", object_spec).strip() != "blob":
                    non_blob_path_count += 1
                    continue
                content_parts.append(f"FILE {path}\n{_run_git('show', object_spec)}")
        elif mode == "diff":
            requested_paths = sorted(str(path) for path in scope.get("changed_files", []))
            denied_paths = [path for path in requested_paths if _path_is_denied(path)]
            base_sha = str(scope.get("base_sha") or "")
            head_sha = str(scope.get("head_sha") or "")
            compare_range = f"{base_sha}...{head_sha}"
            actual_changed_files = sorted(
                f"/{path}"
                for path in _run_git("--literal-pathspecs", "diff", "--name-only", compare_range).splitlines()
                if path
            )
            numstat = _run_git("--literal-pathspecs", "diff", "--numstat", compare_range)
            binary_path_count = sum(1 for line in numstat.splitlines() if line.startswith("-\t-\t"))
            content_parts.append(
                _run_git(
                    "--literal-pathspecs",
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--unified=3",
                    compare_range,
                    "--",
                    *(path.lstrip("/") for path in requested_paths),
                )
            )
        elif mode == "unused":
            pass
        else:
            git_errors.append("invalid Connector scope mode")
    except (OSError, subprocess.CalledProcessError, UnicodeError) as exc:
        git_errors.append(type(exc).__name__)

    content = "\n".join(content_parts)
    secret_match_counts = _secret_match_counts(content)
    inventory_matches = mode != "diff" or actual_changed_files == requested_paths
    status = "passed"
    if (
        git_errors
        or denied_paths
        or binary_path_count
        or non_blob_path_count
        or sum(secret_match_counts.values())
        or not inventory_matches
    ):
        status = "blocked"
    if mode == "unused" and not git_errors:
        status = "unused"
    return {
        "rules_version": REDACTION_RULES_VERSION,
        "scope_manifest_sha256": _sha256_text(_canonical_json(scope)),
        "mode": mode,
        "requested_paths": requested_paths,
        "actual_changed_files": actual_changed_files,
        "changed_file_inventory_matches": inventory_matches,
        "denied_path_count": len(denied_paths),
        "git_error_count": len(git_errors),
        "binary_path_count": binary_path_count,
        "non_blob_path_count": non_blob_path_count,
        "content_sha256": _sha256_text(content),
        "secret_match_counts": secret_match_counts,
        "secret_match_count": sum(secret_match_counts.values()),
        "status": status,
    }


def _load_metadata(path: Path, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"metadata unreadable: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append("metadata must be an object")
        return {}
    return value


def _parse_connector_scope(request_text: str, errors: list[str]) -> dict[str, Any]:
    scope_lines = [line for line in _outside_fenced_code_lines(request_text) if line.startswith(CONNECTOR_SCOPE_PREFIX)]
    if len(scope_lines) != 1:
        errors.append("request must contain exactly one CONNECTOR_SCOPE_JSON line")
        return {}
    try:
        scope = json.loads(scope_lines[0][len(CONNECTOR_SCOPE_PREFIX) :].strip())
    except json.JSONDecodeError:
        errors.append("CONNECTOR_SCOPE_JSON is not valid JSON")
        return {}
    if not isinstance(scope, dict):
        errors.append("CONNECTOR_SCOPE_JSON must be an object")
        return {}
    mode = scope.get("mode")
    if mode == "files":
        allowed = {"mode", "repository", "commit_sha", "required_files"}
        required_files = scope.get("required_files")
        if (
            set(scope) != allowed
            or not isinstance(required_files, list)
            or not required_files
            or any(not _is_safe_repo_path(path) for path in required_files)
            or len(set(required_files)) != len(required_files)
        ):
            errors.append("file Connector scope manifest is invalid")
    elif mode == "diff":
        allowed = {"mode", "repository", "base_sha", "head_sha", "changed_files"}
        changed_files = scope.get("changed_files")
        if (
            set(scope) != allowed
            or not isinstance(changed_files, list)
            or not changed_files
            or any(not _is_safe_repo_path(path) for path in changed_files)
            or len(set(changed_files)) != len(changed_files)
        ):
            errors.append("diff Connector scope manifest is invalid")
    elif mode == "unused":
        if set(scope) != {"mode"}:
            errors.append("unused Connector scope manifest is invalid")
    else:
        errors.append("Connector scope mode is invalid")
    for field in ("commit_sha", "base_sha", "head_sha"):
        if field in scope and not re.fullmatch(r"[0-9a-f]{40}", str(scope[field])):
            errors.append(f"Connector scope {field} is not a full SHA")
    return scope


def _scope_allowlist_entries(scope: dict[str, Any]) -> list[str]:
    if scope.get("mode") == "files":
        return sorted(str(path) for path in scope.get("required_files", []))
    if scope.get("mode") == "diff":
        return sorted(
            [
                f"git-diff:{scope.get('base_sha')}..{scope.get('head_sha')}",
                *(str(path) for path in scope.get("changed_files", [])),
            ]
        )
    return ["connector:none"]


def _outside_fenced_code_lines(value: str) -> list[str]:
    value = re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL)
    lines: list[str] = []
    fence: str | None = None
    for line in value.splitlines():
        marker_match = re.match(r"^\s*(```|~~~)", line)
        if marker_match:
            marker = marker_match.group(1)
            if fence is None:
                fence = marker
            elif fence == marker:
                fence = None
            continue
        if fence is None:
            lines.append(line)
    return lines


def _validate_ordered_headings(
    lines: list[str],
    headings: tuple[str, ...],
    *,
    label: str,
    errors: list[str],
) -> dict[str, int]:
    indices: dict[str, int] = {}
    for heading in headings:
        matches = [index for index, line in enumerate(lines) if line == heading]
        if len(matches) != 1:
            errors.append(f"required {label} heading missing or duplicated: {heading}")
        else:
            indices[heading] = matches[0]
    if len(indices) == len(headings) and [indices[heading] for heading in headings] != sorted(indices.values()):
        errors.append(f"required {label} headings are out of order")
    return indices


def _expected_file_citation(*, repository: str, commit_sha: str, path: str) -> str:
    encoded_path = quote(path.lstrip("/"), safe="/")
    return f"https://github.com/{repository}/blob/{commit_sha}/{encoded_path}"


def _expected_diff_citation(*, repository: str, base_sha: str, head_sha: str) -> str:
    return f"https://github.com/{repository}/compare/{base_sha}...{head_sha}"


def validate_bundle(directory: Path) -> dict[str, Any]:
    errors: list[str] = []
    paths = {name: directory / name for name in ("request.md", "response.md", "decision.md", "metadata.json")}
    missing = sorted(name for name, path in paths.items() if not path.is_file() or path.is_symlink())
    if missing:
        return {
            "contract_schema_version": CONTRACT_SCHEMA_VERSION,
            "consultation_valid": False,
            "storage_valid": False,
            "errors": [f"missing bundle files: {missing}"],
        }
    expected_entries = set(paths)
    actual_entries = {path.name for path in directory.iterdir()}
    if actual_entries != expected_entries or any(
        not path.is_file() or path.is_symlink() for path in directory.iterdir()
    ):
        errors.append("bundle directory must contain exactly four regular contract files")

    request_text = paths["request.md"].read_text(encoding="utf-8")
    response_text = paths["response.md"].read_text(encoding="utf-8")
    decision_text = paths["decision.md"].read_text(encoding="utf-8")
    metadata_text = paths["metadata.json"].read_text(encoding="utf-8")
    metadata = _load_metadata(paths["metadata.json"], errors)
    unknown_metadata_fields = sorted(set(metadata) - METADATA_ALLOWED_FIELDS)
    if unknown_metadata_fields:
        errors.append("metadata contains fields outside the v2 allowlist")

    if metadata.get("contract_schema_version") != CONTRACT_SCHEMA_VERSION:
        errors.append("contract_schema_version mismatch")
    if (
        metadata.get("advisory_only") is not True
        or metadata.get("formal_gate_eligible") is not False
        or metadata.get("formal_review_status") != "not_run"
    ):
        errors.append("Pro consultation metadata must remain advisory-only and formal-gate ineligible")
    if ADVISORY_LABEL not in request_text or ADVISORY_LABEL not in response_text or ADVISORY_LABEL not in decision_text:
        errors.append("every Markdown artifact must preserve the advisory authority label")
    if _outside_fenced_code_lines(request_text).count(ADVISORY_LABEL) != 1:
        errors.append("request must contain the exact unfenced advisory label once")
    request_headers = _parse_request_headers(request_text, errors)
    purpose = metadata.get("purpose")
    secondary_question_sets = metadata.get("secondary_question_sets")
    if purpose not in PURPOSES or request_headers.get("Purpose") != purpose:
        errors.append("consultation purpose is invalid or inconsistent with the request")
    if (
        not isinstance(secondary_question_sets, list)
        or any(item not in PURPOSES or item == purpose for item in secondary_question_sets)
        or len(set(secondary_question_sets)) != len(secondary_question_sets)
    ):
        errors.append("secondary question sets are invalid")
    expected_request_headers = {
        "Authority": "ADVISORY_ONLY",
        "Surface required": "Chat",
        "Model required": "GPT-5.6 Sol",
        "Mode required": "Pro",
        "Browser required": "Codex in-app browser",
        "Local state": str(metadata.get("local_state")),
        "Connector sees dirty scope": "false",
        "Dirty scope provided to Pro": "false",
        "Branch": str(metadata.get("branch") if metadata.get("branch") is not None else "none"),
        "Branch requirement": str(metadata.get("branch_requirement")),
    }
    if any(request_headers.get(field) != expected for field, expected in expected_request_headers.items()):
        errors.append("request authority/UI/local/branch headers are invalid or inconsistent with metadata")
    connector_scope = _parse_connector_scope(request_text, errors)
    if metadata.get("connector_scope_manifest") != connector_scope:
        errors.append("Connector scope metadata does not match the request manifest")
    if metadata.get("connector_scope_manifest_sha256") != _sha256_text(_canonical_json(connector_scope)):
        errors.append("Connector scope manifest hash mismatch")
    scope_paths = {
        str(path)
        for path in (
            connector_scope.get("required_files", [])
            if connector_scope.get("mode") == "files"
            else connector_scope.get("changed_files", [])
        )
    }
    extra_request_paths = sorted(_request_absolute_paths(request_text) - scope_paths)
    if extra_request_paths:
        errors.append("request mentions absolute paths outside the Connector scope manifest")

    expected_marker_path = metadata.get("expected_response_path")
    response_lines = response_text.strip().splitlines()
    begin_line = f"BEGIN_ARTIFACT path={expected_marker_path}" if expected_marker_path else None
    envelope_valid = bool(
        begin_line
        and response_lines
        and response_lines[0] == begin_line
        and response_lines[-1] == "END_ARTIFACT"
        and response_lines.count(begin_line) == 1
        and response_lines.count("END_ARTIFACT") == 1
        and len(response_lines) >= 3
    )
    if not envelope_valid:
        errors.append("response marker envelope is missing or mismatched")
    if len(response_lines) < 2 or response_lines[1] != ADVISORY_LABEL:
        errors.append("exact advisory label must immediately follow the response BEGIN marker")
    if metadata.get("response_complete") is not True or metadata.get("response_marker_envelope_preserved") is not True:
        errors.append("response completeness/envelope metadata is invalid")

    request_hash = _sha256_text(request_text)
    response_hash = _sha256_text(response_text)
    decision_hash = _sha256_text(decision_text)
    if metadata.get("request_sha256") != request_hash:
        errors.append("request_sha256 mismatch")
    if metadata.get("response_sha256") != response_hash:
        errors.append("response_sha256 mismatch")
    if metadata.get("decision_sha256") != decision_hash:
        errors.append("decision_sha256 mismatch")
    if metadata.get("validator_sha256") != _sha256_file(Path(__file__)):
        errors.append("validator_sha256 mismatch")
    workflow_path = Path(__file__).resolve().parents[1] / "docs/CHATGPT_PRO_CONSULTATION_WORKFLOW.md"
    if metadata.get("workflow_sha256") != _sha256_file(workflow_path):
        errors.append("workflow_sha256 mismatch")

    if metadata.get("ui_state_verification") != "verified" or any(
        metadata.get(field) != expected for field, expected in REQUIRED_UI_STATE.items()
    ):
        errors.append("required Chat / GPT-5.6 Sol / Pro UI state was not verified")

    decision_lines = _outside_fenced_code_lines(decision_text)
    decision_heading_indices = _validate_ordered_headings(
        decision_lines,
        REQUIRED_DECISION_HEADINGS,
        label="decision",
        errors=errors,
    )
    if decision_lines.count(ADVISORY_LABEL) != 1:
        errors.append("decision authority section must contain the exact advisory label once")
    elif all(heading in decision_heading_indices for heading in ("## Authority", "## Local disposition")):
        authority_index = decision_lines.index(ADVISORY_LABEL)
        if not (
            decision_heading_indices["## Authority"]
            < authority_index
            < decision_heading_indices["## Local disposition"]
        ):
            errors.append("exact advisory label must be inside the decision Authority section")
    decision_without_authority = decision_text.replace(ADVISORY_LABEL, "")
    if re.search(r"(?i)(?<![-\w])GO(?![-\w])", decision_without_authority):
        errors.append("decision must not claim a formal GO")

    dirty_paths = metadata.get("local_dirty_paths")
    if (
        not isinstance(dirty_paths, list)
        or any(
            not isinstance(path, str)
            or not path
            or any(ord(character) < 32 or ord(character) == 127 for character in path)
            for path in dirty_paths
        )
        or dirty_paths != sorted(set(dirty_paths))
    ):
        errors.append("local dirty inventory is invalid")
        dirty_paths = []
    expected_dirty_status = "recorded_not_transferred" if dirty_paths else "clean"
    expected_local_state = "dirty" if dirty_paths else "clean"
    if (
        metadata.get("local_state") != expected_local_state
        or metadata.get("connector_sees_dirty_scope") is not False
        or metadata.get("dirty_scope_provided_to_pro") is not False
        or metadata.get("dirty_scope_transfer") is not None
        or metadata.get("dirty_diff_sha256") is not None
        or metadata.get("local_dirty_inventory_status") != expected_dirty_status
        or metadata.get("local_dirty_path_count") != len(dirty_paths)
        or metadata.get("local_dirty_inventory_sha256") != _sha256_text("\n".join(dirty_paths))
    ):
        errors.append("v2 dirty scope must be inventoried locally and never transferred")

    redaction = metadata.get("redaction_preflight")
    if not isinstance(redaction, dict):
        errors.append("redaction_preflight is missing")
    else:
        included_paths = redaction.get("included_paths")
        excluded_categories = redaction.get("excluded_categories")
        if not isinstance(included_paths, list) or not all(isinstance(path, str) for path in included_paths):
            errors.append("redaction_preflight included_paths are invalid")
        elif not isinstance(excluded_categories, list) or not all(
            isinstance(category, str) for category in excluded_categories
        ):
            errors.append("redaction_preflight excluded_categories are invalid")
        else:
            recomputed_redaction = scan_redacted_request(
                request_text,
                included_paths=included_paths,
                excluded_categories=excluded_categories,
            )
            if redaction != recomputed_redaction:
                errors.append("redaction_preflight does not match a fresh scan of the persisted request")
            if included_paths != _scope_allowlist_entries(connector_scope):
                errors.append("redaction allowlist does not match the Connector scope manifest")
            if recomputed_redaction.get("status") != "passed":
                errors.append("redaction_preflight did not pass for the persisted request")
        if metadata.get("redaction_status") != "verified":
            errors.append("redaction_status must be verified for a valid consultation")

    recomputed_transfer = scan_transfer_content(connector_scope)
    if metadata.get("transfer_content_preflight") != recomputed_transfer:
        errors.append("transfer_content_preflight does not match the exact committed Connector payload")
    expected_transfer_status = "unused" if connector_scope.get("mode") == "unused" else "passed"
    if recomputed_transfer.get("status") != expected_transfer_status:
        errors.append("exact committed Connector payload failed transfer preflight")

    persisted_texts = [request_text, response_text, decision_text, metadata_text]
    for path in sorted(directory.iterdir()):
        if path.name not in expected_entries and path.is_file() and not path.is_symlink():
            persisted_texts.append(path.read_bytes().decode("utf-8", errors="replace"))
    persisted_secret_counts = _secret_match_counts("\n".join(persisted_texts))
    retention_safe = sum(persisted_secret_counts.values()) == 0
    if metadata.get("retention_safe") is not retention_safe:
        errors.append("retention_safe metadata disagrees with a fresh persisted-artifact scan")
    if not retention_safe:
        errors.append("persisted consultation bundle contains secret-like material and is unsafe to retain")

    raw_verdict = metadata.get("raw_verdict")
    normalized = metadata.get("normalized_recommendation")
    response_outside_fences = _outside_fenced_code_lines(response_text)
    response_schema_errors: list[str] = []
    if response_outside_fences.count(ADVISORY_LABEL) != 1:
        response_schema_errors.append("response must contain the exact unfenced advisory label once")
    response_heading_indices = _validate_ordered_headings(
        response_outside_fences,
        REQUIRED_RESPONSE_HEADINGS,
        label="response",
        errors=response_schema_errors,
    )
    finding_heading_indices = _validate_ordered_headings(
        response_outside_fences,
        REQUIRED_FINDING_HEADINGS,
        label="finding",
        errors=response_schema_errors,
    )
    if all(heading in response_heading_indices for heading in ("# Findings", "# Recommended sequence")) and len(
        finding_heading_indices
    ) == len(REQUIRED_FINDING_HEADINGS):
        if not all(
            response_heading_indices["# Findings"]
            < finding_heading_indices[heading]
            < response_heading_indices["# Recommended sequence"]
            for heading in REQUIRED_FINDING_HEADINGS
        ):
            response_schema_errors.append("P0/P1/P2 headings must be inside the Findings section")
    all_verdict_directives = [
        match.group(1).strip()
        for line in response_text.splitlines()
        if (match := re.fullmatch(r"(?:>\s*)*Raw Pro verdict:\s*(.+?)\s*", line))
    ]
    outside_verdict_directives = [
        match.group(1).strip()
        for line in response_outside_fences
        if (match := re.fullmatch(r"Raw Pro verdict:\s*(.+?)\s*", line))
    ]
    parsed_raw_verdict = (
        outside_verdict_directives[0]
        if len(all_verdict_directives) == 1
        and len(outside_verdict_directives) == 1
        and outside_verdict_directives[0] in RECOMMENDATIONS
        else None
    )
    if parsed_raw_verdict is None:
        response_schema_errors.append("response must contain exactly one unfenced exact Raw Pro verdict")
    verdict_heading_index = response_heading_indices.get("# Verdict")
    scope_heading_index = response_heading_indices.get("# Scope understood")
    if parsed_raw_verdict is not None and verdict_heading_index is not None and scope_heading_index is not None:
        verdict_line_index = response_outside_fences.index(f"Raw Pro verdict: {parsed_raw_verdict}")
        if not verdict_heading_index < verdict_line_index < scope_heading_index:
            response_schema_errors.append("Raw Pro verdict must be inside the Verdict section")
    errors.extend(response_schema_errors)
    expected_normalized = parsed_raw_verdict
    if normalized != expected_normalized or normalized not in RECOMMENDATIONS:
        errors.append("raw verdict normalization is invalid")
    if raw_verdict != expected_normalized:
        errors.append("raw verdict metadata does not match the structured response verdict")
    if metadata.get("response_schema_valid") is not (not response_schema_errors):
        errors.append("response schema is invalid")

    connector_status = metadata.get("connector_status")
    scope_mode = connector_scope.get("mode")
    if scope_mode in {"files", "diff"}:
        scope_repository = connector_scope.get("repository")
        if (
            not isinstance(scope_repository, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", scope_repository)
            or metadata.get("repository") != scope_repository
            or _local_origin_repository() != scope_repository
        ):
            errors.append("Connector repository does not match the request scope manifest and local GitHub origin")
        if connector_status != "commit_pinned" or metadata.get("connector_evidence_valid") is not True:
            errors.append("Connector-required scope must have validated commit-pinned evidence")
        if metadata.get("outcome_code") != "connector_ok_commit_pinned":
            errors.append("validated Connector scope has inconsistent outcome_code")
        requested_sha = connector_scope.get("commit_sha") if scope_mode == "files" else connector_scope.get("head_sha")
        observed_sha = metadata.get("observed_commit_sha")
        if (
            metadata.get("commit_verification") != "verified"
            or not isinstance(requested_sha, str)
            or not re.fullmatch(r"[0-9a-f]{40}", requested_sha)
            or observed_sha != requested_sha
            or metadata.get("requested_commit_sha") != requested_sha
        ):
            errors.append("immutable commit proof is invalid")
        file_proofs = metadata.get("connector_file_proof")
        diff_proof = metadata.get("connector_diff_proof")
        if scope_mode == "files":
            if diff_proof is not None:
                errors.append("file Connector scope must not include diff proof")
            required_files = set(connector_scope.get("required_files", []))
            if not isinstance(file_proofs, list):
                errors.append("file Connector scope is missing file proofs")
                file_proofs = []
            proof_paths = {
                str(proof.get("path")) for proof in file_proofs if isinstance(proof, dict) and proof.get("path")
            }
            if proof_paths != required_files or len(file_proofs) != len(required_files):
                errors.append("Connector file proofs do not exactly match the required-file manifest")
            for proof in file_proofs:
                if not isinstance(proof, dict) or (
                    proof.get("observed_commit_sha") != requested_sha
                    or proof.get("retrieval_status") != "complete"
                    or proof.get("citation")
                    != _expected_file_citation(
                        repository=str(scope_repository),
                        commit_sha=str(requested_sha),
                        path=str(proof.get("path") or ""),
                    )
                    or str(proof.get("citation") or "") not in response_text
                ):
                    errors.append("required Connector file proof is incomplete or uncited")
                    break
        else:
            if file_proofs not in (None, []):
                errors.append("diff Connector scope must not include file proofs")
            if not isinstance(diff_proof, dict):
                errors.append("diff Connector scope is missing diff proof")
                diff_proof = {}
            if (
                diff_proof.get("retrieval_status") != "complete"
                or diff_proof.get("base_sha") != connector_scope.get("base_sha")
                or diff_proof.get("head_sha") != requested_sha
                or diff_proof.get("citation")
                != _expected_diff_citation(
                    repository=str(scope_repository),
                    base_sha=str(connector_scope.get("base_sha")),
                    head_sha=str(requested_sha),
                )
                or str(diff_proof.get("citation") or "") not in response_text
            ):
                errors.append("required Connector diff proof is incomplete or uncited")
    elif scope_mode == "unused":
        if (
            connector_status != "unused"
            or metadata.get("connector_evidence_valid") is not False
            or metadata.get("connector_file_proof") not in (None, [])
            or metadata.get("connector_diff_proof") is not None
            or metadata.get("repository") is not None
            or metadata.get("requested_commit_sha") is not None
            or metadata.get("observed_commit_sha") is not None
            or metadata.get("commit_verification") != "not_requested"
            or metadata.get("outcome_code") != "none"
            or re.search(r"https://github\.com/[^\s]+/(?:blob|compare)/", response_text)
        ):
            errors.append("unused Connector scope has inconsistent status or proof")
    else:
        errors.append("Connector evidence cannot validate without a request scope manifest")

    branch_requirement = metadata.get("branch_requirement")
    branch_lookup = metadata.get("branch_lookup_outcome")
    if branch_requirement not in {"provenance_only", "required", "not_applicable"}:
        errors.append("branch_requirement is invalid")
    if branch_lookup not in VALID_BRANCH_LOOKUPS:
        errors.append("branch_lookup_outcome is invalid")
    expected_branch_verification = {
        "verified": "verified",
        "not_found": "not_found",
        "lookup_unsupported": "lookup_unsupported",
        "resolved_other_sha": "mismatch",
        "not_requested": "not_applicable",
    }.get(branch_lookup)
    if metadata.get("branch_verification") != expected_branch_verification:
        errors.append("branch_verification does not preserve the raw lookup outcome")
    if branch_requirement == "required" and branch_lookup != "verified":
        errors.append("required branch was not verified")
    if scope_mode in {"files", "diff"} and branch_requirement not in {"provenance_only", "required"}:
        errors.append("Connector-backed scope must explicitly classify branch evidence")
    branch = metadata.get("branch")
    branch_resolved_sha = metadata.get("branch_resolved_sha")
    requested_sha = connector_scope.get("commit_sha") if scope_mode == "files" else connector_scope.get("head_sha")
    if scope_mode in {"files", "diff"}:
        if not isinstance(branch, str) or not branch or re.search(r"[\x00-\x1f\x7f]", branch):
            errors.append("Connector-backed scope must name its provenance branch")
        if branch_lookup == "verified" and branch_resolved_sha != requested_sha:
            errors.append("verified branch must resolve to the requested immutable commit")
        if branch_lookup == "resolved_other_sha" and (
            not isinstance(branch_resolved_sha, str)
            or not re.fullmatch(r"[0-9a-f]{40}", branch_resolved_sha)
            or branch_resolved_sha == requested_sha
        ):
            errors.append("resolved_other_sha must preserve the distinct resolved commit")
        if branch_lookup in {"not_found", "lookup_unsupported", "not_requested"} and branch_resolved_sha is not None:
            errors.append("unresolved branch outcome must not invent a resolved commit")
    if scope_mode == "unused" and (
        branch_requirement != "not_applicable"
        or branch_lookup != "not_requested"
        or metadata.get("branch_verification") != "not_applicable"
        or branch is not None
        or branch_resolved_sha is not None
    ):
        errors.append("unused Connector scope has inconsistent branch evidence")

    if status := metadata.get("consultation_status"):
        if status == "blocked_connector" and (
            raw_verdict is not None
            or normalized is not None
            or all_verdict_directives
            or metadata.get("response_schema_valid") is not False
            or metadata.get("connector_file_proof") not in (None, [])
            or metadata.get("connector_diff_proof") is not None
            or metadata.get("outcome_code") != "connector_blocked"
        ):
            errors.append("blocked Connector capture must not retain a grounded verdict or proof")

    evidence_valid = not errors
    asserted_valid = metadata.get("consultation_valid")
    asserted_usable = metadata.get("usable_for_advisory_decision")
    status = metadata.get("consultation_status")
    consistency_errors: list[str] = []
    if asserted_valid is not evidence_valid or asserted_usable is not evidence_valid:
        consistency_errors.append("computed validity disagrees with metadata")
    if evidence_valid and status != "complete_validated":
        consistency_errors.append("valid consultation must be complete_validated")
    if not evidence_valid and status not in SAFE_INVALID_STATUSES:
        consistency_errors.append("invalid consultation uses an unsafe completion status")
    if not evidence_valid and "not connector-grounded" not in decision_text.casefold():
        consistency_errors.append("invalid consultation decision must disclaim Connector-grounded evidence")
    consultation_valid = evidence_valid and not consistency_errors

    return {
        "contract_schema_version": CONTRACT_SCHEMA_VERSION,
        "consultation_valid": consultation_valid,
        "storage_valid": not consistency_errors and retention_safe,
        "retention_safe": retention_safe,
        "persisted_secret_match_counts": persisted_secret_counts,
        "errors": sorted(set(errors)),
        "consistency_errors": sorted(set(consistency_errors)),
        "request_sha256": request_hash,
        "response_sha256": response_hash,
        "decision_sha256": decision_hash,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("redaction-scan")
    scan.add_argument("request", type=Path)
    scan.add_argument("--included-path", action="append", default=[])
    scan.add_argument("--excluded-category", action="append", default=[])
    scan.add_argument("--standard-exclusions", action="store_true")

    validate = subparsers.add_parser("validate-bundle")
    validate.add_argument("directory", type=Path)
    validate.add_argument("--allow-invalid-storage", action="store_true")

    args = parser.parse_args()
    if args.command == "redaction-scan":
        excluded_categories = list(args.excluded_category)
        if args.standard_exclusions:
            excluded_categories.extend(sorted(REQUIRED_EXCLUDED_CATEGORIES))
        result = scan_redacted_request(
            args.request.read_text(encoding="utf-8"),
            included_paths=args.included_path,
            excluded_categories=excluded_categories,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "passed" else 1

    result = validate_bundle(args.directory)
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.allow_invalid_storage:
        return 0 if result["storage_valid"] else 1
    return 0 if result["consultation_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
