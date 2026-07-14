from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from x_first.capability_probe import canonical_sha256
from x_first.contracts import (
    FORBIDDEN_WRITER_KEYS,
    PROHIBITED_FIELD_TOKENS,
    PROHIBITED_VALUE_TERMS,
    _contains_prohibited_term,
    _field_tokens,
    _iter_values,
    project_root,
)

LIVE_REQUEST_SCHEMA_VERSION = "x.grok.capability_probe.request.v2"
LIVE_RESULT_SCHEMA_VERSION = "x.grok.capability_probe.result.v2"
LIVE_EXECUTION_MODE = "live_bounded"
SAFETY_POLICY_VERSION = "x-first-public-professional-v1"
PROMPT_VERSION = "x-first-grok-x-native-capability-v1"
PROVIDER_ID = "grok_cli_oauth"
MODEL_ID = "grok-4.5"
TOOL_ID = "x_search"
PINNED_GROK_BINARY_SHA256 = "01bcacec12e7c2a164d23975f1595400ff7f3cbd74f50b1858f6a5a14eb9ff81"
APPROVAL_RECEIPT_SCHEMA_VERSION = "x.grok.live_approval_consumption.v1"
TOOL_RECEIPT_SCHEMA_VERSION = "x.grok.x_search_tool_receipt.v1"
TARGET_LAB_ID = "openai"
TARGET_HANDLE = "OpenAI"
MAX_OBSERVATIONS = 5
MAX_ERRORS = 1
MAX_TURNS = 4
MAX_ELAPSED_MS = 180_000
MAX_REPORTED_COST_USD = 0.25
MAX_STDOUT_BYTES = 256_000
MAX_STDERR_BYTES = 256_000
MAX_SESSION_UPDATES_BYTES = 5_000_000
MAX_UPDATE_LINE_BYTES = 1_000_000
MAX_AUTH_BYTES = 64_000
MAX_BINARY_BYTES = 300_000_000
MAX_VIOLATION_RECEIPT_CALLS = 8
MAX_VIOLATION_RECEIPT_POSTS = 25
PROCESS_POLL_SECONDS = 0.05
LIVE_DIAGNOSTIC_CODE = "XCAP_LIVE_INVALID"
LIVE_EXECUTION_ERROR_CODE = "XCAP_LIVE_EXECUTION_FAILED"
DISALLOWED_LOCAL_TOOLS = (
    "run_terminal_cmd",
    "grep",
    "read_file",
    "search_replace",
    "list_dir",
    "web_search",
    "web_fetch",
    "todo_write",
    "task",
)
CANONICAL_TIMESTAMP_PATTERN = (
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:[0-2][0-9]|3[01])T"
    r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$"
)

LIVE_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "probe_id",
        "execution_mode",
        "owner_decisions",
        "target",
        "query",
        "hard_budgets",
        "kill_switch",
        "retention",
        "safety_policy_version",
        "claims",
    }
)
LIVE_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "probe_id",
        "request_sha256",
        "execution_mode",
        "run",
        "task",
        "capability",
        "provenance",
        "usage",
        "observations",
        "errors",
        "retention",
        "candidate_packets",
        "identity_link_proposals",
        "assertions",
        "canonical_writes",
        "claims",
    }
)

GROK_RESPONSE_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["reported_verdict", "access_mode", "target", "observations", "errors"],
    "properties": {
        "reported_verdict": {"enum": ["x_native_candidate", "capability_unavailable"]},
        "access_mode": {"enum": ["x_search", "unavailable"]},
        "target": {
            "type": "object",
            "additionalProperties": False,
            "required": ["lab_id", "author_handle", "platform_user_id"],
            "properties": {
                "lab_id": {"const": TARGET_LAB_ID},
                "author_handle": {"const": TARGET_HANDLE},
                "platform_user_id": {"type": ["string", "null"], "maxLength": 64},
            },
        },
        "observations": {
            "type": "array",
            "maxItems": MAX_OBSERVATIONS,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "platform_object_id",
                    "platform_user_id",
                    "author_handle",
                    "canonical_url",
                    "authored_at",
                    "excerpt",
                    "full_body_stored",
                ],
                "properties": {
                    "platform_object_id": {"type": "string", "pattern": "^[0-9]{5,32}$"},
                    "platform_user_id": {"type": ["string", "null"], "maxLength": 64},
                    "author_handle": {"const": TARGET_HANDLE},
                    "canonical_url": {
                        "type": "string",
                        "pattern": "^https://x\\.com/OpenAI/status/[0-9]{5,32}$",
                    },
                    "authored_at": {"type": "string", "format": "date-time", "maxLength": 32},
                    "excerpt": {"type": "string", "maxLength": 280},
                    "full_body_stored": {"const": False},
                },
            },
        },
        "errors": {
            "type": "array",
            "maxItems": MAX_ERRORS,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["code", "message"],
                "properties": {
                    "code": {"enum": ["x_search_unavailable", "no_matching_posts", "probe_failed"]},
                    "message": {"type": "string", "maxLength": 160},
                },
            },
        },
    },
}


@dataclass(frozen=True)
class ToolCallReceipt:
    call_id: str
    tool_id: str
    statuses: tuple[str, ...]
    raw_result_post_pairs: tuple[tuple[str, str], ...]
    raw_result_author_user_ids: tuple[str, ...]


@dataclass(frozen=True)
class ToolProof:
    x_search_calls: int
    x_search_completed_calls: int
    unexpected_tool_calls: tuple[str, ...]
    raw_result_post_pairs: tuple[tuple[str, str], ...]
    raw_result_author_user_ids: tuple[str, ...]
    observed_model_ids: tuple[str, ...]
    call_receipts: tuple[ToolCallReceipt, ...]
    updates_sha256: str
    update_bytes: int


@dataclass(frozen=True)
class BoundedCommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    stop_reason: str | None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _strict_json_loads(value: str | bytes) -> Any:
    return json.loads(value, parse_constant=_reject_json_constant)


def _live_content_errors(value: Any) -> list[str]:
    errors: list[str] = []
    for path, child in _iter_values(value):
        if path:
            normalized_key = path[-1].strip().lower().replace("-", "_")
            if _field_tokens(path[-1]) & PROHIBITED_FIELD_TOKENS:
                errors.append("prohibited protected/proxy field")
            if normalized_key in FORBIDDEN_WRITER_KEYS:
                errors.append("forbidden canonical writer field")
        if isinstance(child, str):
            folded = child.casefold()
            if any(_contains_prohibited_term(folded, term) for term in PROHIBITED_VALUE_TERMS):
                errors.append("prohibited protected/proxy value")
    return sorted(set(errors))


def build_live_request() -> dict[str, Any]:
    return {
        "schema_version": LIVE_REQUEST_SCHEMA_VERSION,
        "probe_id": "xprobe_live_openai_official_v1",
        "execution_mode": LIVE_EXECUTION_MODE,
        "owner_decisions": {
            "live_execution": "user_approved_2026_07_14",
            "legal_privacy": "public_professional_minimized",
            "model_access": "grok_cli_oauth_grok_4_5_x_search_required_no_fallback",
            "retention_policy": "private_24h_bounded_excerpt_then_delete",
        },
        "target": {
            "lab_id": TARGET_LAB_ID,
            "account_kind": "official_lab",
            "platform_user_id_policy": "must_be_returned_and_consistent_for_stage2",
            "current_handle": TARGET_HANDLE,
        },
        "query": {
            "query_kind": "recent_public_technical_posts",
            "query_template": "x-native://official-account/recent-technical-posts",
        },
        "hard_budgets": {
            "max_executions": 1,
            "max_x_search_calls": 1,
            "max_result_sets": 1,
            "max_observations": MAX_OBSERVATIONS,
            "max_model_turns": MAX_TURNS,
            "max_reported_cost_usd": MAX_REPORTED_COST_USD,
            "deadline_ms": MAX_ELAPSED_MS,
        },
        "kill_switch": {
            "armed": True,
            "trip_conditions": [
                "non_x_search_tool_observed",
                "second_external_call_attempted",
                "budget_exceeded",
                "credential_material_observed",
                "full_body_retention_attempted",
                "researcher_mapping_attempted",
                "canonical_write_attempted",
            ],
        },
        "retention": {
            "class": "private_live_probe_24h",
            "delete_after_hours": 24,
            "bounded_excerpt_max_chars": 280,
            "full_body_allowed": False,
            "session_transcript_persisted": False,
        },
        "safety_policy_version": SAFETY_POLICY_VERSION,
        "claims": {
            "capability_probe_authorized": True,
            "researcher_mapping_authorized": False,
            "graph_expansion_authorized": False,
            "provider_fallback_authorized": False,
            "canonical_writes_authorized": False,
            "outreach_authorized": False,
        },
    }


def _exact_object(value: Any, fields: frozenset[str], *, location: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{location} must be an object")
        return {}
    if set(value) != fields:
        errors.append(f"{location} fields do not match the closed contract")
    return value


def validate_live_request(payload: Any) -> list[str]:
    errors: list[str] = []
    request = _exact_object(payload, LIVE_REQUEST_FIELDS, location="request", errors=errors)
    expected = build_live_request()
    if request != expected:
        errors.append("request must equal the owner-approved bounded live probe")
    errors.extend(_live_content_errors(payload))
    return sorted({f"{LIVE_DIAGNOSTIC_CODE}: {error}" for error in errors})


def build_grok_prompt(request: Mapping[str, Any]) -> str:
    request_hash = canonical_sha256(request)
    return "\n".join(
        (
            "Use the hosted native X Search tool only. Do not use generic web search, web fetch, shell, files,",
            "model memory, another provider, or a fallback. This is a one-call capability handshake, not researcher",
            "mapping.",
            f"Target the official X account @{TARGET_HANDLE} for lab {TARGET_LAB_ID}.",
            "Return at most five recent public posts authored by that exact official account that concern technical AI",
            "model, training, research, infrastructure, or safety work. One X Search call/result set maximum.",
            "For every post return the stable numeric post id, canonical x.com URL, authored timestamp, an excerpt of",
            "at most 280 characters, and the stable author platform user id when the tool exposes it. Never retain the",
            "full post body. If native X Search is unavailable, provenance is ambiguous, or the account cannot be",
            "verified, return capability_unavailable with zero observations. Do not guess or reconstruct identifiers.",
            "Do not identify, rank, infer, or discuss any person's ethnicity, nationality, race, citizenship,",
            "religion,",
            "gender, language community, school, region, or protected identity. Do not produce candidate packets,",
            "identity links, assertions, outreach, or canonical/product writes.",
            f"Bound request SHA-256: {request_hash}.",
            "Return only the JSON object required by the supplied schema.",
        )
    )


def _normalize_tool_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    aliases = {
        "x_search": "x_search",
        "xsearch": "x_search",
        "web_search": "web_search",
        "websearch": "web_search",
        "web_fetch": "web_fetch",
        "webfetch": "web_fetch",
    }
    return aliases.get(normalized, normalized or None)


def _walk_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _event_kind(node: Mapping[str, Any]) -> str | None:
    for key in ("sessionUpdate", "session_update", "type"):
        value = node.get(key)
        if value in {"tool_call", "tool_call_update"}:
            return str(value)
    return None


def _tool_identity(node: Mapping[str, Any]) -> str | None:
    metadata = node.get("_meta")
    if isinstance(metadata, dict):
        tool_metadata = metadata.get("x.ai/tool")
        if isinstance(tool_metadata, dict):
            normalized = _normalize_tool_name(tool_metadata.get("name"))
            if normalized is not None:
                return normalized
    for key in ("toolName", "tool_name", "tool", "name", "title"):
        normalized = _normalize_tool_name(node.get(key))
        if normalized is not None:
            return normalized
    return None


def _tool_call_id(node: Mapping[str, Any], *, fallback: str) -> str:
    for key in ("toolCallId", "tool_call_id", "id"):
        value = node.get(key)
        if isinstance(value, str) and value:
            return value
    return fallback


def _tool_statuses(node: Mapping[str, Any]) -> set[str]:
    statuses: set[str] = set()
    for nested in _walk_dicts(node):
        for key in ("status", "state"):
            value = nested.get(key)
            if isinstance(value, str):
                statuses.add(value.strip().lower())
    return statuses


def _walk_values(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_values(child)


def _raw_x_post_pairs(value: Any) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    pattern = re.compile(r"https://x\.com/OpenAI/status/([0-9]{5,32})(?![0-9])")
    for child in _walk_values(value):
        if not isinstance(child, str):
            continue
        for match in pattern.finditer(child):
            object_id = match.group(1)
            pairs.add((object_id, f"https://x.com/{TARGET_HANDLE}/status/{object_id}"))
    return pairs


def _contains_target_handle_field(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    handle_keys = {"screen_name", "screenname", "username", "handle", "author_handle"}
    for node in _walk_dicts(value):
        for key, child in node.items():
            normalized_key = re.sub(r"[^a-z0-9]+", "_", str(key).casefold()).strip("_")
            if normalized_key in handle_keys and isinstance(child, str):
                if child.strip().lstrip("@").casefold() == TARGET_HANDLE.casefold():
                    return True
    return False


def _raw_x_author_user_ids(value: Any) -> set[str]:
    identifiers: set[str] = set()
    author_path_tokens = {"author", "author_info", "authorinfo", "user", "user_info", "userinfo", "profile"}
    id_keys = {"id", "id_str", "idstr", "rest_id", "restid", "user_id", "userid", "platform_user_id"}

    def visit(child: Any, path: tuple[str, ...]) -> None:
        if isinstance(child, dict):
            normalized_path = {re.sub(r"[^a-z0-9]+", "_", part.casefold()).strip("_") for part in path if part}
            if normalized_path & author_path_tokens and _contains_target_handle_field(child):
                for node in _walk_dicts(child):
                    for key, candidate in node.items():
                        normalized_key = re.sub(r"[^a-z0-9]+", "_", str(key).casefold()).strip("_")
                        if (
                            normalized_key in id_keys
                            and isinstance(candidate, str)
                            and re.fullmatch(r"[0-9]{3,32}", candidate)
                        ):
                            identifiers.add(candidate)
            for key, nested in child.items():
                visit(nested, (*path, str(key)))
        elif isinstance(child, list):
            for index, nested in enumerate(child):
                visit(nested, (*path, str(index)))

    visit(value, ())
    return identifiers


def _observed_model_ids(payload: Any) -> set[str]:
    model_ids: set[str] = set()
    for node in _walk_dicts(payload):
        for key in ("modelId", "model_id"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                model_ids.add(value.strip())
    return model_ids


def extract_tool_proof(updates_path: Path) -> ToolProof:
    if updates_path.is_symlink() or not updates_path.is_file():
        raise ValueError("session updates are unavailable")
    raw = updates_path.read_bytes()
    if len(raw) > MAX_SESSION_UPDATES_BYTES:
        raise ValueError("session updates exceed the bounded parser size")
    tool_calls: dict[str, str] = {}
    x_calls: dict[str, set[str]] = {}
    receipt_pairs: dict[str, set[tuple[str, str]]] = {}
    receipt_author_ids: dict[str, set[str]] = {}
    unexpected: set[str] = set()
    raw_post_pairs: set[tuple[str, str]] = set()
    raw_author_user_ids: set[str] = set()
    model_ids: set[str] = set()
    event_index = 0
    for raw_line in raw.splitlines():
        if len(raw_line) > MAX_UPDATE_LINE_BYTES:
            raise ValueError("session update line exceeds the bounded parser size")
        try:
            payload = _strict_json_loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ValueError("session updates are not valid JSON lines") from error
        model_ids.update(_observed_model_ids(payload))
        for node in _walk_dicts(payload):
            kind = _event_kind(node)
            if kind is None:
                continue
            tool_name = _tool_identity(node)
            call_id = _tool_call_id(node, fallback=f"anonymous-{event_index}")
            event_index += 1
            if kind == "tool_call":
                if call_id in tool_calls:
                    unexpected.add("duplicate_tool_call_id")
                tool_calls[call_id] = tool_name or "unknown"
            resolved_tool = tool_name or tool_calls.get(call_id)
            if resolved_tool == "x_search":
                x_calls.setdefault(call_id, set()).update(_tool_statuses(node))
                raw_output = node.get("rawOutput")
                if isinstance(raw_output, (dict, list)):
                    post_pairs = _raw_x_post_pairs(raw_output)
                    author_ids = _raw_x_author_user_ids(raw_output)
                    raw_post_pairs.update(post_pairs)
                    raw_author_user_ids.update(author_ids)
                    receipt_pairs.setdefault(call_id, set()).update(post_pairs)
                    receipt_author_ids.setdefault(call_id, set()).update(author_ids)
            elif resolved_tool is not None:
                unexpected.add(resolved_tool)
            else:
                unexpected.add("unknown")
    completed_states = {"completed", "complete", "succeeded", "success"}
    completed = sum(1 for statuses in x_calls.values() if statuses & completed_states)
    call_receipts = tuple(
        ToolCallReceipt(
            call_id=call_id,
            tool_id="x_search",
            statuses=tuple(sorted(statuses)),
            raw_result_post_pairs=tuple(sorted(receipt_pairs.get(call_id, set()))),
            raw_result_author_user_ids=tuple(sorted(receipt_author_ids.get(call_id, set()))),
        )
        for call_id, statuses in sorted(x_calls.items())
    )
    return ToolProof(
        x_search_calls=len(x_calls),
        x_search_completed_calls=completed,
        unexpected_tool_calls=tuple(sorted(unexpected)),
        raw_result_post_pairs=tuple(sorted(raw_post_pairs)),
        raw_result_author_user_ids=tuple(sorted(raw_author_user_ids)),
        observed_model_ids=tuple(sorted(model_ids)),
        call_receipts=call_receipts,
        updates_sha256=hashlib.sha256(raw).hexdigest(),
        update_bytes=len(raw),
    )


def _session_updates_path(grok_home: Path, cwd: Path, session_id: str) -> Path:
    encoded_cwd = quote(str(cwd.resolve()), safe="")
    return grok_home / "sessions" / encoded_cwd / session_id / "updates.jsonl"


def _canonical_observation(value: Any, *, observed_at: str) -> dict[str, Any] | None:
    if not isinstance(value, dict) or set(value) != {
        "platform_object_id",
        "platform_user_id",
        "author_handle",
        "canonical_url",
        "authored_at",
        "excerpt",
        "full_body_stored",
    }:
        return None
    object_id = value.get("platform_object_id")
    platform_user_id = value.get("platform_user_id")
    handle = value.get("author_handle")
    canonical_url = value.get("canonical_url")
    authored_at = value.get("authored_at")
    excerpt = value.get("excerpt")
    if not isinstance(object_id, str) or re.fullmatch(r"[0-9]{5,32}", object_id) is None:
        return None
    if platform_user_id is not None and (
        not isinstance(platform_user_id, str) or re.fullmatch(r"[0-9]{3,32}", platform_user_id) is None
    ):
        return None
    if handle != TARGET_HANDLE:
        return None
    expected_url = f"https://x.com/{TARGET_HANDLE}/status/{object_id}"
    if canonical_url != expected_url:
        return None
    try:
        parsed = urlsplit(canonical_url)
    except (TypeError, ValueError):
        return None
    if parsed.scheme != "https" or parsed.netloc != "x.com" or parsed.path != f"/{TARGET_HANDLE}/status/{object_id}":
        return None
    if parsed.query or parsed.fragment:
        return None
    if not isinstance(authored_at, str) or len(authored_at) > 32:
        return None
    try:
        authored = datetime.fromisoformat(authored_at.replace("Z", "+00:00"))
        observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if authored.tzinfo is None or authored > observed:
        return None
    if not isinstance(excerpt, str) or not 1 <= len(excerpt) <= 280:
        return None
    if value.get("full_body_stored") is not False:
        return None
    if _live_content_errors(value):
        return None
    return {
        "observation_id": f"xprobe_obs_{object_id}",
        "platform_object_id": object_id,
        "platform_user_id": platform_user_id,
        "author_handle": handle,
        "canonical_url": canonical_url,
        "authored_at": authored.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "observed_at": observed_at,
        "excerpt": excerpt,
        "full_body_stored": False,
    }


def _parse_outer_response(stdout: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(stdout) > MAX_STDOUT_BYTES:
        raise ValueError("Grok output exceeds the bounded parser size")
    try:
        outer = _strict_json_loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError("Grok output is not one JSON object") from error
    if not isinstance(outer, dict) or outer.get("type") == "error":
        raise ValueError("Grok did not return a successful headless envelope")
    text = outer.get("text")
    if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_STDOUT_BYTES:
        raise ValueError("Grok response text is missing or oversized")
    try:
        inner = _strict_json_loads(text)
    except (json.JSONDecodeError, ValueError) as error:
        raise ValueError("Grok structured response is invalid") from error
    if not isinstance(inner, dict):
        raise ValueError("Grok structured response must be an object")
    return outer, inner


def _validate_grok_response(inner: Any, *, observed_at: str) -> list[str]:
    errors: list[str] = []
    expected_fields = {"reported_verdict", "access_mode", "target", "observations", "errors"}
    if not isinstance(inner, dict) or set(inner) != expected_fields:
        return ["Grok structured response fields do not match the closed contract"]
    verdict = inner.get("reported_verdict")
    access_mode = inner.get("access_mode")
    if verdict not in {"x_native_candidate", "capability_unavailable"}:
        errors.append("Grok reported_verdict is invalid")
    if access_mode not in {"x_search", "unavailable"}:
        errors.append("Grok access_mode is invalid")
    target = inner.get("target")
    if not isinstance(target, dict) or set(target) != {"lab_id", "author_handle", "platform_user_id"}:
        errors.append("Grok target fields do not match the closed contract")
    else:
        if target.get("lab_id") != TARGET_LAB_ID or target.get("author_handle") != TARGET_HANDLE:
            errors.append("Grok target identity mismatch")
        platform_user_id = target.get("platform_user_id")
        if platform_user_id is not None and (
            not isinstance(platform_user_id, str) or re.fullmatch(r"[0-9]{3,32}", platform_user_id) is None
        ):
            errors.append("Grok target platform_user_id is invalid")
    observations = inner.get("observations")
    if not isinstance(observations, list) or len(observations) > MAX_OBSERVATIONS:
        errors.append("Grok observations exceed the closed bound")
        observations = []
    elif any(_canonical_observation(item, observed_at=observed_at) is None for item in observations):
        errors.append("Grok observation is non-canonical")
    response_errors = inner.get("errors")
    if not isinstance(response_errors, list) or len(response_errors) > MAX_ERRORS:
        errors.append("Grok errors exceed the closed bound")
        response_errors = []
    else:
        for item in response_errors:
            if not isinstance(item, dict) or set(item) != {"code", "message"}:
                errors.append("Grok error fields do not match the closed contract")
                continue
            if item.get("code") not in {"x_search_unavailable", "no_matching_posts", "probe_failed"}:
                errors.append("Grok error code is invalid")
            message = item.get("message")
            if not isinstance(message, str) or not message or len(message) > 160:
                errors.append("Grok error message is invalid")
    if verdict == "x_native_candidate":
        if access_mode != "x_search" or not observations or response_errors:
            errors.append("Grok candidate verdict is internally inconsistent")
    elif verdict == "capability_unavailable":
        if access_mode != "unavailable" or observations or len(response_errors) != 1:
            errors.append("Grok unavailable verdict is internally inconsistent")
    errors.extend(_live_content_errors(inner))
    return sorted(set(errors))


def _validate_grok_stderr(stderr: bytes) -> None:
    if len(stderr) > MAX_STDERR_BYTES:
        raise ValueError("Grok diagnostics exceed the bounded parser size")
    text = stderr.decode("utf-8", errors="replace").casefold()
    fatal_markers = (
        "tools allowlist had unmappable entries",
        "keeping full grok toolset",
        "preferred model not in available models, falling back",
        "agent building failed",
        "couldn't create session",
    )
    if any(marker in text for marker in fatal_markers):
        raise ValueError("Grok diagnostics show tool or model contract drift")


def _cost_projection(outer: Mapping[str, Any]) -> tuple[str, float | None]:
    value = outer.get("total_cost_usd")
    if type(value) not in {int, float}:
        return "unreported", None
    cost = float(value)
    if not math.isfinite(cost) or cost < 0:
        return "invalid", None
    return "reported", cost


def _build_failure_result(
    *,
    request: Mapping[str, Any],
    run_id: str,
    started_at: str,
    completed_at: str,
    elapsed_ms: int,
    code: str,
    message: str,
    approval_receipt_sha256: str,
    grok_binary_sha256: str,
    tool_receipt_sha256: str | None = None,
    session_id: str | None = None,
    proof: ToolProof | None = None,
) -> dict[str, Any]:
    raw_result_pairs = set(proof.raw_result_post_pairs) if proof is not None else set()
    return {
        "schema_version": LIVE_RESULT_SCHEMA_VERSION,
        "probe_id": request["probe_id"],
        "request_sha256": canonical_sha256(request),
        "execution_mode": LIVE_EXECUTION_MODE,
        "run": {"run_id": run_id, "status": "failed", "started_at": started_at, "completed_at": completed_at},
        "task": {"task_id": "xprobe_task_live_openai_official_v1", "status": "failed", "stop_reason": code},
        "capability": {
            "verdict": "probe_error",
            "proof_scope": "bounded_live_official_account",
            "x_native_access_proven": False,
            "stable_account_id_proven": False,
            "stage2_eligible_for_owner_review": False,
        },
        "provenance": {
            "provider_id": PROVIDER_ID,
            "access_mode": "unavailable",
            "model_id": MODEL_ID,
            "tool_id": TOOL_ID,
            "provider_request_id": None,
            "session_id": session_id if proof is not None else None,
            "prompt_version": PROMPT_VERSION,
            "prompt_sha256": canonical_sha256({"prompt": build_grok_prompt(request)}),
            "session_updates_sha256": proof.updates_sha256 if proof is not None else None,
            "raw_result_post_ids": sorted(object_id for object_id, _ in raw_result_pairs),
            "raw_result_author_user_ids": list(proof.raw_result_author_user_ids) if proof is not None else [],
            "observed_model_ids": list(proof.observed_model_ids) if proof is not None else [],
            "approval_receipt_sha256": approval_receipt_sha256,
            "tool_receipt_sha256": tool_receipt_sha256,
            "grok_binary_sha256": grok_binary_sha256,
        },
        "usage": {
            "executions": 1,
            "x_search_calls": proof.x_search_calls if proof is not None else 0,
            "result_sets": 0,
            "observations": 0,
            "model_turns": 0,
            "cost_status": "unreported",
            "cost_usd": None,
            "elapsed_ms": elapsed_ms,
        },
        "observations": [],
        "errors": [{"code": code, "message": message, "retryable": False}],
        "retention": {
            "class": "private_live_probe_24h",
            "delete_after_hours": 24,
            "delete_after": _expiry_timestamp(completed_at),
            "deletion_status": "pending",
            "full_body_stored": False,
            "session_transcript_persisted": False,
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


def _open_regular_owned_file(source: Path, *, maximum_bytes: int, private: bool) -> tuple[int, os.stat_result]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(source, flags)
    except OSError as error:
        raise ValueError("required local input is unavailable") from error
    try:
        source_stat = os.fstat(descriptor)
        mode = stat.S_IMODE(source_stat.st_mode)
        if not stat.S_ISREG(source_stat.st_mode) or source_stat.st_uid != os.getuid():
            raise ValueError("required local input is not a regular user-owned file")
        if mode & (0o077 if private else 0o022):
            raise ValueError("required local input permissions are unsafe")
        if source_stat.st_size <= 0 or source_stat.st_size > maximum_bytes:
            raise ValueError("required local input size is invalid")
        return descriptor, source_stat
    except Exception:
        os.close(descriptor)
        raise


def _copy_from_descriptor(
    source_descriptor: int,
    destination: Path,
    *,
    expected_size: int,
    destination_mode: int,
) -> str:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    destination_descriptor = os.open(destination, flags, destination_mode)
    digest = hashlib.sha256()
    copied = 0
    try:
        os.fchmod(destination_descriptor, destination_mode)
        while True:
            chunk = os.read(source_descriptor, 1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(destination_descriptor, view)
                view = view[written:]
        if copied != expected_size:
            raise ValueError("required local input changed while it was copied")
        os.fsync(destination_descriptor)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    finally:
        os.close(destination_descriptor)
    return digest.hexdigest()


def _secure_auth_copy(source: Path, destination: Path) -> None:
    descriptor, source_stat = _open_regular_owned_file(source, maximum_bytes=MAX_AUTH_BYTES, private=True)
    try:
        _copy_from_descriptor(
            descriptor,
            destination,
            expected_size=source_stat.st_size,
            destination_mode=0o600,
        )
    finally:
        os.close(descriptor)


def _stage_verified_binary(source: Path, destination: Path) -> str:
    try:
        resolved_source = source.resolve(strict=True)
    except OSError as error:
        raise ValueError("Grok CLI executable is unavailable") from error
    descriptor, source_stat = _open_regular_owned_file(
        resolved_source,
        maximum_bytes=MAX_BINARY_BYTES,
        private=False,
    )
    try:
        digest = _copy_from_descriptor(
            descriptor,
            destination,
            expected_size=source_stat.st_size,
            destination_mode=0o700,
        )
    finally:
        os.close(descriptor)
    if digest != PINNED_GROK_BINARY_SHA256:
        destination.unlink(missing_ok=True)
        raise ValueError("Grok CLI binary does not match the reviewed pinned digest")
    return digest


def _ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise ValueError("private runtime directory must not be a symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    mode = stat.S_IMODE(path.stat().st_mode)
    if not path.is_dir() or mode & 0o077:
        raise ValueError("private runtime directory permissions are unsafe")


def _consume_live_approval(
    runtime_root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    binary_sha256: str,
    consumed_at: str,
) -> tuple[dict[str, Any], Path]:
    approval_root = runtime_root / ".approvals"
    _ensure_private_directory(approval_root)
    receipt = {
        "schema_version": APPROVAL_RECEIPT_SCHEMA_VERSION,
        "probe_id": request["probe_id"],
        "request_sha256": canonical_sha256(request),
        "run_id": run_id,
        "consumed_at": consumed_at,
        "binary_sha256": binary_sha256,
        "state": "consumed_before_spawn",
    }
    receipt_path = approval_root / f"{request['probe_id']}.json"
    serialized = (json.dumps(receipt, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(receipt_path, flags, 0o600)
    except FileExistsError as error:
        raise PermissionError("the owner-approved live execution has already been consumed") from error
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(serialized)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return receipt, receipt_path


def _build_tool_receipt(*, proof: ToolProof, session_id: str) -> dict[str, Any]:
    return {
        "schema_version": TOOL_RECEIPT_SCHEMA_VERSION,
        "session_id": session_id,
        "session_updates_sha256": proof.updates_sha256,
        "session_update_bytes": proof.update_bytes,
        "observed_model_ids": list(proof.observed_model_ids),
        "unexpected_tool_calls": list(proof.unexpected_tool_calls),
        "calls": [
            {
                "call_id": call.call_id,
                "tool_id": call.tool_id,
                "statuses": list(call.statuses),
                "raw_result_posts": [
                    {"platform_object_id": object_id, "canonical_url": canonical_url}
                    for object_id, canonical_url in call.raw_result_post_pairs
                ],
                "raw_result_author_user_ids": list(call.raw_result_author_user_ids),
            }
            for call in proof.call_receipts
        ],
    }


def _isolated_environment(grok_home: Path) -> dict[str, str]:
    return {
        "GROK_HOME": str(grok_home),
        "GROK_DISABLE_AUTOUPDATER": "1",
        "HOME": str(grok_home),
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
        "NO_COLOR": "1",
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "TERM": "dumb",
        "TMPDIR": str(grok_home / "tmp"),
    }


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def _run_bounded_command(
    command: list[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    updates_path: Path,
) -> BoundedCommandResult:
    with tempfile.TemporaryDirectory(prefix="x-first-grok-stdio-") as stdio_name:
        stdio_root = Path(stdio_name)
        stdout_path = stdio_root / "stdout"
        stderr_path = stdio_root / "stderr"
        with stdout_path.open("wb") as stdout_stream, stderr_path.open("wb") as stderr_stream:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=dict(environment),
                stdin=subprocess.DEVNULL,
                stdout=stdout_stream,
                stderr=stderr_stream,
                start_new_session=True,
            )
            deadline = time.monotonic() + MAX_ELAPSED_MS / 1000
            stop_reason: str | None = None
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    stop_reason = "deadline_exceeded"
                elif stdout_path.stat().st_size > MAX_STDOUT_BYTES or stderr_path.stat().st_size > MAX_STDERR_BYTES:
                    stop_reason = "output_budget_exceeded"
                elif updates_path.exists():
                    if updates_path.is_symlink() or updates_path.stat().st_size > MAX_SESSION_UPDATES_BYTES:
                        stop_reason = "provider_evidence_budget_exceeded"
                    else:
                        try:
                            proof = extract_tool_proof(updates_path)
                        except ValueError:
                            # A final JSONL record may be incomplete while the provider is still writing it.
                            proof = None
                        if proof is not None and (proof.x_search_calls > 1 or proof.unexpected_tool_calls):
                            stop_reason = "tool_kill_switch_tripped"
                if stop_reason is not None:
                    _terminate_process_group(process)
                    break
                time.sleep(PROCESS_POLL_SECONDS)
            try:
                returncode = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _terminate_process_group(process)
                returncode = process.wait(timeout=5)
        stdout = stdout_path.read_bytes()
        stderr = stderr_path.read_bytes()
        if len(stdout) > MAX_STDOUT_BYTES:
            stdout = stdout[:MAX_STDOUT_BYTES]
        if len(stderr) > MAX_STDERR_BYTES:
            stderr = stderr[:MAX_STDERR_BYTES]
        return BoundedCommandResult(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            stop_reason=stop_reason,
        )


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError("live probe artifact path must not be a symlink")
    serialized = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(
        "utf-8"
    )
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _expiry_timestamp(completed_at: str) -> str:
    completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    return (completed + timedelta(hours=24)).astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _write_artifact_bundle(
    runtime_root: Path,
    *,
    run_id: str,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
    approval_receipt: Mapping[str, Any],
    tool_receipt: Mapping[str, Any] | None,
) -> Path:
    final_root = runtime_root / run_id
    if final_root.exists() or final_root.is_symlink():
        raise ValueError("live probe artifact directory already exists")
    staging_root = runtime_root / f".{run_id}.{uuid.uuid4().hex}.tmp"
    staging_root.mkdir(mode=0o700)
    try:
        _atomic_write_json(staging_root / "request.json", request)
        _atomic_write_json(staging_root / "result.json", result)
        _atomic_write_json(staging_root / "approval-receipt.json", approval_receipt)
        if tool_receipt is not None:
            _atomic_write_json(staging_root / "tool-receipt.json", tool_receipt)
        os.replace(staging_root, final_root)
        parent_descriptor = os.open(runtime_root, os.O_RDONLY)
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    finally:
        if staging_root.exists():
            shutil.rmtree(staging_root)
    return final_root


def run_live_probe(
    *,
    execute_live: bool,
    request: Mapping[str, Any] | None = None,
    grok_binary: Path | None = None,
    auth_path: Path | None = None,
) -> tuple[dict[str, Any], Path]:
    if not execute_live:
        raise PermissionError("live execution requires the explicit execute_live gate")
    live_request = dict(request or build_live_request())
    request_errors = validate_live_request(live_request)
    if request_errors:
        raise ValueError("live request failed the closed contract")
    root = project_root()
    binary = grok_binary or Path.home() / ".grok/bin/grok"
    oauth = auth_path or Path.home() / ".grok/auth.json"
    runtime_root = root / "runtime/live-probes"
    _ensure_private_directory(runtime_root)
    run_id = f"xprobe_run_{uuid.uuid4().hex}"
    session_id = str(uuid.uuid4())
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    prompt = build_grok_prompt(live_request)
    result: dict[str, Any]
    tool_receipt: dict[str, Any] | None = None
    proof: ToolProof | None = None
    with tempfile.TemporaryDirectory(prefix="x-first-grok-home-") as temporary_home_name:
        temporary_home = Path(temporary_home_name)
        isolated_cwd = temporary_home / "work"
        isolated_tmp = temporary_home / "tmp"
        isolated_cwd.mkdir(mode=0o700)
        isolated_tmp.mkdir(mode=0o700)
        staged_binary = temporary_home / "grok"
        binary_sha256 = _stage_verified_binary(binary, staged_binary)
        _secure_auth_copy(oauth, temporary_home / "auth.json")
        approval_receipt, _ = _consume_live_approval(
            runtime_root,
            request=live_request,
            run_id=run_id,
            binary_sha256=binary_sha256,
            consumed_at=started_at,
        )
        approval_receipt_sha256 = canonical_sha256(approval_receipt)
        command = [
            str(staged_binary),
            "--single",
            prompt,
            "--verbatim",
            "--cwd",
            str(isolated_cwd),
            "--model",
            MODEL_ID,
            "--reasoning-effort",
            "low",
            "--output-format",
            "json",
            "--json-schema",
            json.dumps(GROK_RESPONSE_SCHEMA, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            "--disable-web-search",
            "--disallowed-tools",
            ",".join(DISALLOWED_LOCAL_TOOLS),
            "--max-turns",
            str(MAX_TURNS),
            "--session-id",
            session_id,
            "--no-subagents",
            "--no-plan",
            "--no-memory",
            "--no-auto-update",
            "--permission-mode",
            "dontAsk",
            "--sandbox",
            "read-only",
        ]
        updates_path = _session_updates_path(temporary_home, isolated_cwd, session_id)
        try:
            completed = _run_bounded_command(
                command,
                cwd=isolated_cwd,
                environment=_isolated_environment(temporary_home),
                updates_path=updates_path,
            )
        except Exception:
            completed_at = _utc_now()
            elapsed_ms = min(round((time.monotonic() - started_monotonic) * 1000), MAX_ELAPSED_MS)
            result = _build_failure_result(
                request=live_request,
                run_id=run_id,
                started_at=started_at,
                completed_at=completed_at,
                elapsed_ms=elapsed_ms,
                code="process_spawn_failed",
                message="The bounded Grok capability process could not be started safely.",
                approval_receipt_sha256=approval_receipt_sha256,
                grok_binary_sha256=binary_sha256,
            )
        else:
            completed_at = _utc_now()
            elapsed_ms = round((time.monotonic() - started_monotonic) * 1000)
            try:
                if completed.stop_reason is not None:
                    raise RuntimeError(completed.stop_reason)
                if completed.returncode != 0:
                    raise ValueError("Grok exited without a successful response")
                _validate_grok_stderr(completed.stderr)
                outer, inner = _parse_outer_response(completed.stdout)
                proof = extract_tool_proof(updates_path)
                tool_receipt = _build_tool_receipt(proof=proof, session_id=session_id)
                tool_receipt_sha256 = canonical_sha256(tool_receipt)
                result = build_live_result(
                    request=live_request,
                    run_id=run_id,
                    session_id=session_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    elapsed_ms=elapsed_ms,
                    outer=outer,
                    inner=inner,
                    proof=proof,
                    approval_receipt_sha256=approval_receipt_sha256,
                    grok_binary_sha256=binary_sha256,
                    tool_receipt_sha256=tool_receipt_sha256,
                )
            except Exception:
                if tool_receipt is None and updates_path.exists():
                    try:
                        proof = extract_tool_proof(updates_path)
                        tool_receipt = _build_tool_receipt(proof=proof, session_id=session_id)
                    except ValueError:
                        tool_receipt = None
                tool_receipt_sha256 = canonical_sha256(tool_receipt) if tool_receipt is not None else None
                stop_reason = completed.stop_reason
                error_code = stop_reason or "invalid_provider_evidence"
                error_message = (
                    "The bounded Grok process was stopped by an executable budget or tool kill switch."
                    if stop_reason is not None
                    else "The Grok response or X Search provenance failed the bounded contract."
                )
                result = _build_failure_result(
                    request=live_request,
                    run_id=run_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    elapsed_ms=elapsed_ms,
                    code=error_code,
                    message=error_message,
                    approval_receipt_sha256=approval_receipt_sha256,
                    grok_binary_sha256=binary_sha256,
                    tool_receipt_sha256=tool_receipt_sha256,
                    session_id=session_id,
                    proof=proof,
                )
    errors = validate_live_result(result, request=live_request)
    if errors:
        result = _build_failure_result(
            request=live_request,
            run_id=run_id,
            started_at=started_at,
            completed_at=_utc_now(),
            elapsed_ms=min(round((time.monotonic() - started_monotonic) * 1000), MAX_ELAPSED_MS),
            code="result_validation_failed",
            message="The bounded live result failed its executable contract.",
            approval_receipt_sha256=approval_receipt_sha256,
            grok_binary_sha256=binary_sha256,
            tool_receipt_sha256=canonical_sha256(tool_receipt) if tool_receipt is not None else None,
            session_id=session_id,
            proof=proof,
        )
        if validate_live_result(result, request=live_request):
            raise RuntimeError("internal failure artifact did not satisfy the executable contract")
    output_root = _write_artifact_bundle(
        runtime_root,
        run_id=run_id,
        request=live_request,
        result=result,
        approval_receipt=approval_receipt,
        tool_receipt=tool_receipt,
    )
    return result, output_root


def build_live_result(
    *,
    request: Mapping[str, Any],
    run_id: str,
    session_id: str,
    started_at: str,
    completed_at: str,
    elapsed_ms: int,
    outer: Mapping[str, Any],
    inner: Mapping[str, Any],
    proof: ToolProof,
    approval_receipt_sha256: str,
    grok_binary_sha256: str,
    tool_receipt_sha256: str,
) -> dict[str, Any]:
    if _validate_grok_response(inner, observed_at=completed_at):
        raise ValueError("Grok structured response failed the executable contract")
    reported_verdict = inner.get("reported_verdict")
    access_mode = inner.get("access_mode")
    raw_observations = inner.get("observations")
    observations: list[dict[str, Any]] = []
    if isinstance(raw_observations, list) and len(raw_observations) <= MAX_OBSERVATIONS:
        for value in raw_observations:
            observation = _canonical_observation(value, observed_at=completed_at)
            if observation is None:
                observations = []
                break
            observations.append(observation)
    unique_objects = {item["platform_object_id"] for item in observations}
    unique_urls = {item["canonical_url"] for item in observations}
    observation_pairs = {(item["platform_object_id"], item["canonical_url"]) for item in observations}
    raw_result_pairs = set(proof.raw_result_post_pairs)
    account_ids = {item["platform_user_id"] for item in observations if item["platform_user_id"] is not None}
    target = inner.get("target") if isinstance(inner.get("target"), dict) else {}
    target_account_id = target.get("platform_user_id")
    stable_account_id = (
        isinstance(target_account_id, str)
        and re.fullmatch(r"[0-9]{3,32}", target_account_id) is not None
        and account_ids == {target_account_id}
        and set(proof.raw_result_author_user_ids) == {target_account_id}
        and len(account_ids) == 1
        and all(item["platform_user_id"] == target_account_id for item in observations)
    )
    x_native_proven = (
        reported_verdict == "x_native_candidate"
        and access_mode == "x_search"
        and proof.x_search_calls == 1
        and proof.x_search_completed_calls == 1
        and not proof.unexpected_tool_calls
        and proof.observed_model_ids == (MODEL_ID,)
        and 1 <= len(observations) <= MAX_OBSERVATIONS
        and len(unique_objects) == len(observations)
        and len(unique_urls) == len(observations)
        and observation_pairs <= raw_result_pairs
    )
    cost_status, cost_usd = _cost_projection(outer)
    model_turns = outer.get("num_turns") if type(outer.get("num_turns")) is int else 0
    budget_exceeded = (
        elapsed_ms > MAX_ELAPSED_MS
        or model_turns > MAX_TURNS
        or (cost_usd is not None and cost_usd > MAX_REPORTED_COST_USD)
        or proof.x_search_calls > 1
        or len(raw_result_pairs) > MAX_VIOLATION_RECEIPT_POSTS
        or len(observations) > MAX_OBSERVATIONS
    )
    if budget_exceeded:
        verdict = "budget_exceeded"
    elif x_native_proven and stable_account_id:
        verdict = "x_native_identity_ready"
    elif x_native_proven:
        verdict = "post_retrieval_only"
    else:
        verdict = "capability_unavailable"
        observations = []
    successful = verdict in {"x_native_identity_ready", "post_retrieval_only"}
    if not successful:
        observations = []
    error_by_verdict = {
        "capability_unavailable": (
            "x_native_provenance_unavailable",
            "Native X Search evidence did not satisfy the bounded capability contract.",
        ),
        "budget_exceeded": ("budget_exceeded", "The bounded capability probe exceeded an approved budget."),
    }
    errors = []
    if verdict in error_by_verdict:
        code, message = error_by_verdict[verdict]
        errors = [{"code": code, "message": message, "retryable": False}]
    return {
        "schema_version": LIVE_RESULT_SCHEMA_VERSION,
        "probe_id": request["probe_id"],
        "request_sha256": canonical_sha256(request),
        "execution_mode": LIVE_EXECUTION_MODE,
        "run": {
            "run_id": run_id,
            "status": "completed" if successful else "failed",
            "started_at": started_at,
            "completed_at": completed_at,
        },
        "task": {
            "task_id": "xprobe_task_live_openai_official_v1",
            "status": "succeeded" if successful else "failed",
            "stop_reason": verdict,
        },
        "capability": {
            "verdict": verdict,
            "proof_scope": "bounded_live_official_account",
            "x_native_access_proven": x_native_proven,
            "stable_account_id_proven": stable_account_id and x_native_proven,
            "stage2_eligible_for_owner_review": verdict == "x_native_identity_ready",
        },
        "provenance": {
            "provider_id": PROVIDER_ID,
            "access_mode": "x_search" if x_native_proven else "unavailable",
            "model_id": MODEL_ID,
            "tool_id": TOOL_ID,
            "provider_request_id": outer.get("requestId") if isinstance(outer.get("requestId"), str) else None,
            "session_id": session_id,
            "prompt_version": PROMPT_VERSION,
            "prompt_sha256": canonical_sha256({"prompt": build_grok_prompt(request)}),
            "session_updates_sha256": proof.updates_sha256,
            "raw_result_post_ids": sorted(object_id for object_id, _ in raw_result_pairs),
            "raw_result_author_user_ids": list(proof.raw_result_author_user_ids),
            "observed_model_ids": list(proof.observed_model_ids),
            "approval_receipt_sha256": approval_receipt_sha256,
            "tool_receipt_sha256": tool_receipt_sha256,
            "grok_binary_sha256": grok_binary_sha256,
        },
        "usage": {
            "executions": 1,
            "x_search_calls": proof.x_search_calls,
            "result_sets": proof.x_search_completed_calls,
            "observations": len(observations),
            "model_turns": model_turns,
            "cost_status": cost_status,
            "cost_usd": cost_usd,
            "elapsed_ms": elapsed_ms,
        },
        "observations": observations,
        "errors": errors,
        "retention": {
            "class": "private_live_probe_24h",
            "delete_after_hours": 24,
            "delete_after": _expiry_timestamp(completed_at),
            "deletion_status": "pending",
            "full_body_stored": False,
            "session_transcript_persisted": False,
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


def validate_live_result(payload: Any, *, request: Any) -> list[str]:
    errors: list[str] = []
    if validate_live_request(request):
        errors.append("bound live request is invalid")
    result = _exact_object(payload, LIVE_RESULT_FIELDS, location="result", errors=errors)
    if result.get("schema_version") != LIVE_RESULT_SCHEMA_VERSION:
        errors.append("result schema version mismatch")
    if result.get("probe_id") != request.get("probe_id") or result.get("request_sha256") != canonical_sha256(request):
        errors.append("result does not bind the approved request")
    if result.get("execution_mode") != LIVE_EXECUTION_MODE:
        errors.append("result execution mode mismatch")
    run = _exact_object(
        result.get("run"),
        frozenset({"run_id", "status", "started_at", "completed_at"}),
        location="result.run",
        errors=errors,
    )
    task = _exact_object(
        result.get("task"),
        frozenset({"task_id", "status", "stop_reason"}),
        location="result.task",
        errors=errors,
    )
    capability = _exact_object(
        result.get("capability"),
        frozenset(
            {
                "verdict",
                "proof_scope",
                "x_native_access_proven",
                "stable_account_id_proven",
                "stage2_eligible_for_owner_review",
            }
        ),
        location="result.capability",
        errors=errors,
    )
    provenance = _exact_object(
        result.get("provenance"),
        frozenset(
            {
                "provider_id",
                "access_mode",
                "model_id",
                "tool_id",
                "provider_request_id",
                "session_id",
                "prompt_version",
                "prompt_sha256",
                "session_updates_sha256",
                "raw_result_post_ids",
                "raw_result_author_user_ids",
                "observed_model_ids",
                "approval_receipt_sha256",
                "tool_receipt_sha256",
                "grok_binary_sha256",
            }
        ),
        location="result.provenance",
        errors=errors,
    )
    verdict = capability.get("verdict")
    if verdict not in {
        "x_native_identity_ready",
        "post_retrieval_only",
        "capability_unavailable",
        "budget_exceeded",
        "probe_error",
    }:
        errors.append("result capability verdict is invalid")
    successful = verdict in {"x_native_identity_ready", "post_retrieval_only"}
    if not isinstance(run.get("run_id"), str) or re.fullmatch(r"xprobe_run_[0-9a-f]{32}", run["run_id"]) is None:
        errors.append("result run_id is invalid")
    if task.get("task_id") != "xprobe_task_live_openai_official_v1":
        errors.append("result task_id mismatch")
    if capability.get("proof_scope") != "bounded_live_official_account":
        errors.append("result proof scope mismatch")
    if run.get("status") != ("completed" if successful else "failed"):
        errors.append("result run status contradicts capability verdict")
    if task.get("status") != ("succeeded" if successful else "failed"):
        errors.append("result task status contradicts capability verdict")
    if verdict == "probe_error":
        if task.get("stop_reason") not in {
            "probe_error",
            "deadline_exceeded",
            "invalid_provider_evidence",
            "result_validation_failed",
            "process_spawn_failed",
            "output_budget_exceeded",
            "provider_evidence_budget_exceeded",
            "tool_kill_switch_tripped",
        }:
            errors.append("result probe-error stop reason is invalid")
    elif task.get("stop_reason") != verdict:
        errors.append("result task stop reason contradicts capability verdict")
    for field in ("started_at", "completed_at"):
        if not isinstance(run.get(field), str) or re.fullmatch(CANONICAL_TIMESTAMP_PATTERN, run[field]) is None:
            errors.append("result run timestamps must be canonical UTC milliseconds")
    try:
        started = datetime.fromisoformat(str(run.get("started_at")).replace("Z", "+00:00"))
        completed = datetime.fromisoformat(str(run.get("completed_at")).replace("Z", "+00:00"))
        if started > completed:
            errors.append("result run starts after it completes")
    except ValueError:
        pass
    observations_value = result.get("observations")
    observations = observations_value if isinstance(observations_value, list) else []
    if not isinstance(observations_value, list) or len(observations) > MAX_OBSERVATIONS:
        errors.append("result observations exceed the bounded array")
    if successful and not observations:
        errors.append("successful result must retain bounded observations")
    if not successful and observations:
        errors.append("failed result must not retain observations")
    for observation in observations:
        observed_at = observation.get("observed_at") if isinstance(observation, dict) else None
        inner_observation = (
            {key: value for key, value in observation.items() if key not in {"observation_id", "observed_at"}}
            if isinstance(observation, dict)
            else None
        )
        if (
            not isinstance(observed_at, str)
            or _canonical_observation(inner_observation, observed_at=observed_at) != observation
        ):
            errors.append("result contains a non-canonical live observation")
            break
    stable_ids = {
        observation.get("platform_user_id")
        for observation in observations
        if isinstance(observation, dict) and observation.get("platform_user_id") is not None
    }
    stable_proven = capability.get("stable_account_id_proven") is True
    if type(capability.get("stable_account_id_proven")) is not bool:
        errors.append("stable account identity claim must be boolean")
    stable_identity_matches = (
        bool(observations)
        and len(stable_ids) == 1
        and all(item.get("platform_user_id") in stable_ids for item in observations)
    )
    if stable_proven != stable_identity_matches:
        errors.append("stable account identity claim does not match observations")
    if capability.get("stage2_eligible_for_owner_review") is not (verdict == "x_native_identity_ready"):
        errors.append("Stage 2 eligibility is not fail-closed")
    if capability.get("x_native_access_proven") is not successful:
        errors.append("X-native proof flag contradicts verdict")
    if provenance.get("provider_id") != PROVIDER_ID:
        errors.append("result provider identity mismatch")
    if provenance.get("access_mode") != ("x_search" if successful else "unavailable"):
        errors.append("result access mode contradicts verdict")
    if provenance.get("model_id") != MODEL_ID or provenance.get("tool_id") != TOOL_ID:
        errors.append("result model/tool identity mismatch")
    if provenance.get("prompt_version") != PROMPT_VERSION or provenance.get("prompt_sha256") != canonical_sha256(
        {"prompt": build_grok_prompt(request)}
    ):
        errors.append("result prompt binding mismatch")
    provider_request_id = provenance.get("provider_request_id")
    if provider_request_id is not None and (
        not isinstance(provider_request_id, str) or not provider_request_id or len(provider_request_id) > 160
    ):
        errors.append("result provider request id is invalid")
    session_id = provenance.get("session_id")
    if session_id is not None:
        try:
            uuid.UUID(str(session_id))
        except (ValueError, AttributeError):
            errors.append("result session id is invalid")
    updates_sha256 = provenance.get("session_updates_sha256")
    if updates_sha256 is not None and (
        not isinstance(updates_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", updates_sha256) is None
    ):
        errors.append("result session-updates digest is invalid")
    raw_post_ids = provenance.get("raw_result_post_ids")
    raw_author_ids = provenance.get("raw_result_author_user_ids")
    model_ids = provenance.get("observed_model_ids")
    if (
        not isinstance(raw_post_ids, list)
        or len(raw_post_ids) > MAX_VIOLATION_RECEIPT_POSTS
        or any(not isinstance(value, str) or re.fullmatch(r"[0-9]{5,32}", value) is None for value in raw_post_ids)
        or len(set(raw_post_ids)) != len(raw_post_ids)
    ):
        errors.append("result raw post-id receipt is invalid")
        raw_post_ids = []
    if (
        not isinstance(raw_author_ids, list)
        or len(raw_author_ids) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or any(not isinstance(value, str) or re.fullmatch(r"[0-9]{3,32}", value) is None for value in raw_author_ids)
        or len(set(raw_author_ids)) != len(raw_author_ids)
    ):
        errors.append("result raw author-id receipt is invalid")
        raw_author_ids = []
    if (
        not isinstance(model_ids, list)
        or len(model_ids) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or any(not isinstance(value, str) or not value or len(value) > 80 for value in model_ids)
        or (successful and model_ids != [MODEL_ID])
    ):
        errors.append("result effective-model receipt is invalid")
        model_ids = []
    observation_ids = [item.get("platform_object_id") for item in observations if isinstance(item, dict)]
    if successful and (not set(observation_ids) <= set(raw_post_ids) or model_ids != [MODEL_ID]):
        errors.append("result observations are not bound to raw X tool/model receipts")
    if stable_proven and sorted(raw_author_ids) != sorted(stable_ids):
        errors.append("result stable identity is not bound to raw X author receipt")
    if successful and (provider_request_id is None or session_id is None or updates_sha256 is None):
        errors.append("successful result lacks provider/session provenance")
    approval_digest = provenance.get("approval_receipt_sha256")
    tool_digest = provenance.get("tool_receipt_sha256")
    binary_digest = provenance.get("grok_binary_sha256")
    if not isinstance(approval_digest, str) or re.fullmatch(r"[0-9a-f]{64}", approval_digest) is None:
        errors.append("result approval-consumption receipt is invalid")
    if successful and (not isinstance(tool_digest, str) or re.fullmatch(r"[0-9a-f]{64}", tool_digest) is None):
        errors.append("successful result tool receipt is invalid")
    if (
        not successful
        and tool_digest is not None
        and (not isinstance(tool_digest, str) or re.fullmatch(r"[0-9a-f]{64}", tool_digest) is None)
    ):
        errors.append("failed result tool receipt is invalid")
    if binary_digest != PINNED_GROK_BINARY_SHA256:
        errors.append("result Grok binary digest does not match the reviewed pin")
    usage = _exact_object(
        result.get("usage"),
        frozenset(
            {
                "executions",
                "x_search_calls",
                "result_sets",
                "observations",
                "model_turns",
                "cost_status",
                "cost_usd",
                "elapsed_ms",
            }
        ),
        location="result.usage",
        errors=errors,
    )
    integer_fields = ("executions", "x_search_calls", "result_sets", "observations", "model_turns", "elapsed_ms")
    if any(type(usage.get(field)) is not int or usage[field] < 0 for field in integer_fields):
        errors.append("result usage counters must be non-negative integers")
    if (
        usage.get("executions") != 1
        or usage.get("x_search_calls", 0) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or usage.get("result_sets", 0) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or usage.get("elapsed_ms", 0) > MAX_ELAPSED_MS
    ):
        errors.append("result execution evidence exceeds the bounded failure-receipt contract")
    if successful and (usage.get("x_search_calls") != 1 or usage.get("result_sets") != 1):
        errors.append("successful result must prove exactly one X call/result set")
    if usage.get("observations") != len(observations) or usage.get("model_turns", 0) > MAX_TURNS:
        errors.append("result usage does not reconcile")
    cost_status = usage.get("cost_status")
    cost_usd = usage.get("cost_usd")
    if cost_status == "reported":
        if (
            type(cost_usd) not in {int, float}
            or not math.isfinite(float(cost_usd))
            or cost_usd < 0
            or cost_usd > MAX_REPORTED_COST_USD
        ):
            errors.append("reported cost exceeds the bounded contract")
    elif cost_status != "unreported" or cost_usd is not None:
        errors.append("unreported cost must remain null")
    errors_value = result.get("errors")
    if not isinstance(errors_value, list) or len(errors_value) > MAX_ERRORS:
        errors.append("result errors exceed the bounded array")
        errors_value = []
    else:
        for error in errors_value:
            if not isinstance(error, dict) or set(error) != {"code", "message", "retryable"}:
                errors.append("result error fields do not match the closed contract")
                continue
            code = error.get("code")
            message = error.get("message")
            if not isinstance(code, str) or re.fullmatch(r"[a-z][a-z0-9_]{2,63}", code) is None:
                errors.append("result error code is invalid")
            if not isinstance(message, str) or len(message) > 160:
                errors.append("result error message is invalid")
            if error.get("retryable") is not False:
                errors.append("result error must remain non-retryable")
    expected_error_count = 0 if successful else 1
    if isinstance(errors_value, list) and len(errors_value) != expected_error_count:
        errors.append("result error count contradicts verdict")
    if verdict == "probe_error" and errors_value and isinstance(errors_value[0], dict):
        if errors_value[0].get("code") != task.get("stop_reason"):
            errors.append("probe-error code does not match its stop reason")
    retention = result.get("retention")
    expected_delete_after = None
    if isinstance(run.get("completed_at"), str):
        try:
            expected_delete_after = _expiry_timestamp(run["completed_at"])
        except ValueError:
            pass
    if retention != {
        "class": "private_live_probe_24h",
        "delete_after_hours": 24,
        "delete_after": expected_delete_after,
        "deletion_status": "pending",
        "full_body_stored": False,
        "session_transcript_persisted": False,
    }:
        errors.append("result retention policy mismatch")
    for field in ("candidate_packets", "identity_link_proposals", "assertions", "canonical_writes"):
        if result.get(field) != []:
            errors.append(f"result {field} must remain empty")
    claims = result.get("claims")
    if claims != {
        "exhaustive": False,
        "current_employment_guaranteed": False,
        "outreach_permission": False,
        "researcher_mapping_authorized": False,
    }:
        errors.append("result claims must remain non-authorizing")
    errors.extend(_live_content_errors(payload))
    return sorted({f"{LIVE_DIAGNOSTIC_CODE}: {error}" for error in errors})


def _validate_approval_receipt(
    receipt: Any,
    *,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    expected_fields = {
        "schema_version",
        "probe_id",
        "request_sha256",
        "run_id",
        "consumed_at",
        "binary_sha256",
        "state",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_fields:
        return ["approval receipt fields do not match the closed contract"]
    run = result.get("run") if isinstance(result.get("run"), dict) else {}
    provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    if receipt.get("schema_version") != APPROVAL_RECEIPT_SCHEMA_VERSION:
        errors.append("approval receipt schema version mismatch")
    if receipt.get("probe_id") != request.get("probe_id"):
        errors.append("approval receipt probe mismatch")
    if receipt.get("request_sha256") != canonical_sha256(request):
        errors.append("approval receipt request binding mismatch")
    if receipt.get("run_id") != run.get("run_id"):
        errors.append("approval receipt run binding mismatch")
    if receipt.get("consumed_at") != run.get("started_at"):
        errors.append("approval must be consumed immediately before the recorded spawn window")
    if receipt.get("binary_sha256") != PINNED_GROK_BINARY_SHA256:
        errors.append("approval receipt binary digest does not match the reviewed pin")
    if receipt.get("binary_sha256") != provenance.get("grok_binary_sha256"):
        errors.append("approval and result binary digests differ")
    if receipt.get("state") != "consumed_before_spawn":
        errors.append("approval receipt state mismatch")
    if provenance.get("approval_receipt_sha256") != canonical_sha256(receipt):
        errors.append("result does not bind the approval receipt")
    return errors


def _validate_tool_receipt(receipt: Any, *, result: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_fields = {
        "schema_version",
        "session_id",
        "session_updates_sha256",
        "session_update_bytes",
        "observed_model_ids",
        "unexpected_tool_calls",
        "calls",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_fields:
        return ["tool receipt fields do not match the closed contract"]
    provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    capability = result.get("capability") if isinstance(result.get("capability"), dict) else {}
    observations = result.get("observations") if isinstance(result.get("observations"), list) else []
    successful = capability.get("verdict") in {"x_native_identity_ready", "post_retrieval_only"}
    if receipt.get("schema_version") != TOOL_RECEIPT_SCHEMA_VERSION:
        errors.append("tool receipt schema version mismatch")
    if receipt.get("session_id") != provenance.get("session_id"):
        errors.append("tool receipt session binding mismatch")
    updates_sha256 = receipt.get("session_updates_sha256")
    if (
        not isinstance(updates_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", updates_sha256) is None
        or updates_sha256 != provenance.get("session_updates_sha256")
    ):
        errors.append("tool receipt update digest mismatch")
    update_bytes = receipt.get("session_update_bytes")
    if type(update_bytes) is not int or not 0 < update_bytes <= MAX_SESSION_UPDATES_BYTES:
        errors.append("tool receipt update size is invalid")
    if receipt.get("observed_model_ids") != provenance.get("observed_model_ids"):
        errors.append("tool receipt model ids do not reconcile")
    unexpected = receipt.get("unexpected_tool_calls")
    if not isinstance(unexpected, list) or any(not isinstance(value, str) for value in unexpected):
        errors.append("tool receipt unexpected-tool list is invalid")
    if successful and unexpected:
        errors.append("successful tool receipt contains unexpected tools")
    calls = receipt.get("calls")
    if not isinstance(calls, list) or len(calls) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS):
        errors.append("tool receipt exceeds the bounded call-receipt contract")
        calls = []
    if successful and len(calls) != 1:
        errors.append("successful result lacks exactly one X Search receipt")
    call_ids: set[str] = set()
    post_pairs: set[tuple[str, str]] = set()
    author_ids: set[str] = set()
    for call in calls:
        if not isinstance(call, dict) or set(call) != {
            "call_id",
            "tool_id",
            "statuses",
            "raw_result_posts",
            "raw_result_author_user_ids",
        }:
            errors.append("tool-call receipt fields do not match the closed contract")
            continue
        call_id = call.get("call_id")
        if not isinstance(call_id, str) or not call_id or len(call_id) > 160 or call_id in call_ids:
            errors.append("tool-call receipt id is invalid")
        else:
            call_ids.add(call_id)
        if call.get("tool_id") != TOOL_ID:
            errors.append("tool-call receipt is not native X Search")
        statuses = call.get("statuses")
        if not isinstance(statuses, list) or any(not isinstance(value, str) for value in statuses):
            errors.append("tool-call receipt statuses are invalid")
        elif successful and not set(statuses) & {"completed", "complete", "succeeded", "success"}:
            errors.append("successful X Search receipt lacks completion")
        posts = call.get("raw_result_posts")
        if not isinstance(posts, list) or len(posts) > MAX_VIOLATION_RECEIPT_POSTS:
            errors.append("tool-call raw post receipt exceeds the bound")
            posts = []
        for post in posts:
            if not isinstance(post, dict) or set(post) != {"platform_object_id", "canonical_url"}:
                errors.append("tool-call raw post fields are invalid")
                continue
            object_id = post.get("platform_object_id")
            canonical_url = post.get("canonical_url")
            if (
                not isinstance(object_id, str)
                or re.fullmatch(r"[0-9]{5,32}", object_id) is None
                or canonical_url != f"https://x.com/{TARGET_HANDLE}/status/{object_id}"
            ):
                errors.append("tool-call raw post identity is invalid")
                continue
            post_pairs.add((object_id, canonical_url))
        ids = call.get("raw_result_author_user_ids")
        if (
            not isinstance(ids, list)
            or len(ids) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
            or any(not isinstance(value, str) or re.fullmatch(r"[0-9]{3,32}", value) is None for value in ids)
        ):
            errors.append("tool-call raw author ids are invalid")
        else:
            author_ids.update(ids)
    expected_pairs = {
        (str(observation.get("platform_object_id")), str(observation.get("canonical_url")))
        for observation in observations
        if isinstance(observation, dict)
    }
    if successful and not expected_pairs <= post_pairs:
        errors.append("retained observations are not a subset of raw tool-receipt posts")
    if sorted(object_id for object_id, _ in post_pairs) != provenance.get("raw_result_post_ids"):
        errors.append("tool receipt raw post ids do not match result provenance")
    if sorted(author_ids) != provenance.get("raw_result_author_user_ids"):
        errors.append("tool receipt raw author ids do not match result provenance")
    if provenance.get("tool_receipt_sha256") != canonical_sha256(receipt):
        errors.append("result does not bind the tool receipt")
    return errors


def validate_artifact_pair(request_path: Path, result_path: Path) -> list[str]:
    try:
        if request_path.parent != result_path.parent:
            raise ValueError("live artifact files must share a directory")
        artifact_root = request_path.parent
        if artifact_root.is_symlink() or stat.S_IMODE(artifact_root.stat().st_mode) & 0o077:
            raise ValueError("live artifact directory is not private")
        for path in (request_path, result_path):
            if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) & 0o077:
                raise ValueError("live artifact file is not private")
        if request_path.stat().st_size > MAX_STDOUT_BYTES or result_path.stat().st_size > MAX_STDOUT_BYTES:
            raise ValueError("live artifact pair exceeds the bounded parser size")
        request = _strict_json_loads(request_path.read_text(encoding="utf-8"))
        result = _strict_json_loads(result_path.read_text(encoding="utf-8"))
        if not isinstance(request, dict) or not isinstance(result, dict):
            raise ValueError("live artifact pair must contain objects")
        approval_path = artifact_root / "approval-receipt.json"
        if approval_path.is_symlink() or approval_path.stat().st_size > MAX_STDOUT_BYTES:
            raise ValueError("approval receipt is unavailable")
        approval = _strict_json_loads(approval_path.read_text(encoding="utf-8"))
        ledger_path = artifact_root.parent / ".approvals" / f"{request.get('probe_id')}.json"
        if ledger_path.is_symlink() or not ledger_path.is_file():
            raise ValueError("durable approval consumption ledger is unavailable")
        ledger = _strict_json_loads(ledger_path.read_text(encoding="utf-8"))
        if ledger != approval:
            raise ValueError("approval receipt does not match the durable consumption ledger")
    except Exception:
        return [f"{LIVE_DIAGNOSTIC_CODE}: live artifact pair could not be loaded"]
    errors = [
        *validate_live_request(request),
        *validate_live_result(result, request=request),
        *[
            f"{LIVE_DIAGNOSTIC_CODE}: {value}"
            for value in _validate_approval_receipt(approval, request=request, result=result)
        ],
    ]
    provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    tool_path = request_path.parent / "tool-receipt.json"
    if provenance.get("tool_receipt_sha256") is not None:
        try:
            if tool_path.is_symlink() or tool_path.stat().st_size > MAX_STDOUT_BYTES:
                raise ValueError("tool receipt is unavailable")
            tool_receipt = _strict_json_loads(tool_path.read_text(encoding="utf-8"))
        except Exception:
            errors.append(f"{LIVE_DIAGNOSTIC_CODE}: tool receipt could not be loaded")
        else:
            errors.extend(
                f"{LIVE_DIAGNOSTIC_CODE}: {value}" for value in _validate_tool_receipt(tool_receipt, result=result)
            )
    elif tool_path.exists() or tool_path.is_symlink():
        errors.append(f"{LIVE_DIAGNOSTIC_CODE}: unbound tool receipt is present")
    return sorted(set(errors))


def purge_expired_live_artifacts(
    *,
    runtime_root: Path | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    root = runtime_root or project_root() / "runtime/live-probes"
    if not root.exists():
        return []
    _ensure_private_directory(root)
    deletion_root = root / ".deletions"
    _ensure_private_directory(deletion_root)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    receipts: list[dict[str, Any]] = []
    for artifact_root in sorted(root.iterdir()):
        if not re.fullmatch(r"xprobe_run_[0-9a-f]{32}", artifact_root.name):
            continue
        if artifact_root.is_symlink() or not artifact_root.is_dir():
            raise ValueError("live artifact runtime contains an unsafe run path")
        result_path = artifact_root / "result.json"
        if result_path.is_symlink() or not result_path.is_file() or result_path.stat().st_size > MAX_STDOUT_BYTES:
            raise ValueError("live artifact runtime contains an invalid result")
        result = _strict_json_loads(result_path.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise ValueError("live artifact runtime contains a non-object result")
        retention = result.get("retention")
        if not isinstance(retention, dict) or retention.get("deletion_status") != "pending":
            raise ValueError("live artifact runtime contains an invalid retention state")
        expires_value = retention.get("delete_after")
        if not isinstance(expires_value, str):
            raise ValueError("live artifact runtime is missing its deletion deadline")
        expires_at = datetime.fromisoformat(expires_value.replace("Z", "+00:00"))
        if expires_at > current:
            continue
        receipt = {
            "schema_version": "x.grok.live_artifact_deletion.v1",
            "run_id": artifact_root.name,
            "result_sha256": canonical_sha256(result),
            "expired_at": expires_at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "deleted_at": current.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "state": "deleted",
        }
        deletion_path = deletion_root / f"{artifact_root.name}.json"
        _atomic_write_json(deletion_path, receipt)
        shutil.rmtree(artifact_root)
        receipts.append(receipt)
    return receipts


__all__ = [
    "GROK_RESPONSE_SCHEMA",
    "LIVE_REQUEST_SCHEMA_VERSION",
    "LIVE_RESULT_SCHEMA_VERSION",
    "ToolProof",
    "build_grok_prompt",
    "build_live_request",
    "build_live_result",
    "extract_tool_proof",
    "purge_expired_live_artifacts",
    "run_live_probe",
    "validate_artifact_pair",
    "validate_live_request",
    "validate_live_result",
]
