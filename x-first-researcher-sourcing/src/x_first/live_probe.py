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
MAX_FAILURE_WALL_ELAPSED_MS = 200_000
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
PROCESS_GROUP_CLEANUP_SECONDS = 5.0
LIVE_DIAGNOSTIC_CODE = "XCAP_LIVE_INVALID"
LIVE_EXECUTION_ERROR_CODE = "XCAP_LIVE_EXECUTION_FAILED"
GLOBAL_APPROVAL_OWNER_ID = "user_state:x-first-researcher-sourcing/live-approvals/v1"
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
class RawXPostReceipt:
    platform_object_id: str
    canonical_url: str
    platform_user_id: str | None


@dataclass(frozen=True)
class ProviderUsageReceipt:
    input_tokens: int
    output_tokens: int
    total_tokens: int
    cached_read_tokens: int
    reasoning_tokens: int
    model_calls: int
    api_duration_ms: int
    model_turns: int


@dataclass(frozen=True)
class ToolCallReceipt:
    call_id: str
    tool_id: str
    statuses: tuple[str, ...]
    raw_result_posts: tuple[RawXPostReceipt, ...]


@dataclass(frozen=True)
class ToolProof:
    session_id: str
    x_search_calls: int
    x_search_completed_calls: int
    unexpected_tool_calls: tuple[str, ...]
    raw_result_posts: tuple[RawXPostReceipt, ...]
    observed_model_ids: tuple[str, ...]
    call_receipts: tuple[ToolCallReceipt, ...]
    updates_sha256: str
    update_bytes: int
    terminal_stop_reason: str | None
    terminal_usage: ProviderUsageReceipt | None
    evidence_errors: tuple[str, ...]

    @property
    def raw_result_post_pairs(self) -> tuple[tuple[str, str], ...]:
        return tuple((post.platform_object_id, post.canonical_url) for post in self.raw_result_posts)

    @property
    def raw_result_author_user_ids(self) -> tuple[str, ...]:
        return tuple(sorted({post.platform_user_id for post in self.raw_result_posts if post.platform_user_id}))


@dataclass(frozen=True)
class BoundedCommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    stop_reason: str | None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_canonical_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or re.fullmatch(CANONICAL_TIMESTAMP_PATTERN, value) is None:
        raise ValueError("timestamp is not canonical UTC milliseconds")
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    if parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") != value:
        raise ValueError("timestamp does not round-trip canonically")
    return parsed


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


def _tool_identity(update: Mapping[str, Any]) -> str | None:
    metadata = update.get("_meta")
    if isinstance(metadata, dict):
        tool_metadata = metadata.get("x.ai/tool")
        if isinstance(tool_metadata, dict):
            raw_name = tool_metadata.get("name")
            normalized = _normalize_tool_name(raw_name)
            if normalized is not None:
                if normalized == TOOL_ID and raw_name != TOOL_ID:
                    return "noncanonical_x_search_alias"
                return normalized
    return None


def _path_value(value: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current


def _structured_post_pair(value: Mapping[str, Any]) -> tuple[tuple[str, str] | None, bool]:
    """Return one co-located post id/URL pair from the closed reviewed record shapes."""
    canonical_url = value.get("canonical_url")
    if canonical_url is None:
        return None, False
    id_keys = ("id", "id_str", "rest_id")
    identifiers = [value[id_key] for id_key in id_keys if id_key in value]
    if (
        not isinstance(canonical_url, str)
        or not identifiers
        or any(not isinstance(identifier, str) for identifier in identifiers)
        or len(set(identifiers)) != 1
    ):
        return None, True
    object_id = identifiers[0]
    if re.fullmatch(r"[0-9]{5,32}", object_id) is None:
        return None, True
    expected_url = f"https://x.com/{TARGET_HANDLE}/status/{object_id}"
    if canonical_url != expected_url:
        return None, True
    return (object_id, canonical_url), False


def _author_ids_for_post_record(value: Mapping[str, Any]) -> set[str]:
    """Read author identity only from exact, co-located reviewed author shapes."""
    shapes = (
        ("author_info", ("legacy", "screen_name"), ("rest_id",)),
        ("author", ("screen_name",), ("id_str",)),
        ("author", ("username",), ("id",)),
        ("user", ("screen_name",), ("id_str",)),
        ("user", ("username",), ("id",)),
    )
    identifiers: set[str] = set()
    for author_key, handle_path, id_path in shapes:
        author = value.get(author_key)
        if not isinstance(author, Mapping):
            continue
        handle = _path_value(author, handle_path)
        identifier = _path_value(author, id_path)
        if (
            isinstance(handle, str)
            and handle.strip().lstrip("@").casefold() == TARGET_HANDLE.casefold()
            and isinstance(identifier, str)
            and re.fullmatch(r"[0-9]{3,32}", identifier)
        ):
            identifiers.add(identifier)
    return identifiers


def _raw_x_posts(value: Any) -> tuple[tuple[RawXPostReceipt, ...], tuple[str, ...]]:
    bound_ids: dict[tuple[str, str], set[str]] = {}
    invalid_record = False
    for node in _walk_dicts(value):
        pair, malformed = _structured_post_pair(node)
        invalid_record = invalid_record or malformed
        if pair is None:
            continue
        author_ids = _author_ids_for_post_record(node)
        bound_ids.setdefault(pair, set()).update(author_ids)
    binding_errors: list[str] = []
    if invalid_record:
        binding_errors.append("invalid_raw_post_record_binding")
    if any(len(values) > 1 for values in bound_ids.values()):
        binding_errors.append("conflicting_raw_post_author_binding")
    posts = tuple(
        RawXPostReceipt(
            platform_object_id=object_id,
            canonical_url=canonical_url,
            platform_user_id=next(iter(bound_ids[(object_id, canonical_url)]))
            if len(bound_ids[(object_id, canonical_url)]) == 1
            else None,
        )
        for object_id, canonical_url in sorted(bound_ids)
    )
    return posts, tuple(binding_errors)


def _parse_provider_usage(value: Any) -> ProviderUsageReceipt:
    expected_fields = {
        "inputTokens",
        "outputTokens",
        "totalTokens",
        "cachedReadTokens",
        "reasoningTokens",
        "modelCalls",
        "apiDurationMs",
        "modelUsage",
        "numTurns",
    }
    nested_fields = expected_fields - {"modelUsage", "numTurns"}
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise ValueError("terminal usage fields do not match Grok 0.2.99")
    scalar_fields = expected_fields - {"modelUsage"}
    if any(type(value.get(field)) is not int or value[field] < 0 for field in scalar_fields):
        raise ValueError("terminal usage counters are invalid")
    if value["modelCalls"] < 1 or not 1 <= value["numTurns"] <= MAX_TURNS:
        raise ValueError("terminal usage call/turn counts are invalid")
    if value["totalTokens"] != value["inputTokens"] + value["outputTokens"]:
        raise ValueError("terminal token totals do not reconcile")
    if value["cachedReadTokens"] > value["inputTokens"] or value["reasoningTokens"] > value["outputTokens"]:
        raise ValueError("terminal token detail exceeds its parent counter")
    model_usage = value.get("modelUsage")
    if not isinstance(model_usage, dict) or set(model_usage) != {MODEL_ID}:
        raise ValueError("terminal model-usage identity is invalid")
    nested = model_usage.get(MODEL_ID)
    if not isinstance(nested, dict) or set(nested) != nested_fields:
        raise ValueError("terminal model-usage fields do not match Grok 0.2.99")
    if any(type(nested.get(field)) is not int or nested[field] < 0 for field in nested_fields):
        raise ValueError("terminal model-usage counters are invalid")
    if any(nested[field] != value[field] for field in nested_fields):
        raise ValueError("terminal model-usage counters do not reconcile")
    return ProviderUsageReceipt(
        input_tokens=value["inputTokens"],
        output_tokens=value["outputTokens"],
        total_tokens=value["totalTokens"],
        cached_read_tokens=value["cachedReadTokens"],
        reasoning_tokens=value["reasoningTokens"],
        model_calls=value["modelCalls"],
        api_duration_ms=value["apiDurationMs"],
        model_turns=value["numTurns"],
    )


def _parse_update_envelope(
    payload: Any,
    *,
    expected_session_id: str,
) -> tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(payload, dict) or set(payload) != {"method", "params", "timestamp"}:
        raise ValueError("session update envelope fields do not match Grok 0.2.99")
    if type(payload.get("timestamp")) is not int or payload["timestamp"] < 0:
        raise ValueError("session update timestamp is invalid")
    params = payload.get("params")
    if not isinstance(params, dict) or set(params) != {"_meta", "sessionId", "update"}:
        raise ValueError("session update params fields do not match Grok 0.2.99")
    envelope_metadata = params.get("_meta")
    if params.get("sessionId") != expected_session_id or not isinstance(envelope_metadata, dict):
        raise ValueError("session update is not bound to the command session")
    event_id = envelope_metadata.get("eventId")
    agent_timestamp_ms = envelope_metadata.get("agentTimestampMs")
    if (
        not isinstance(event_id, str)
        or not event_id.startswith(f"{expected_session_id}-")
        or type(agent_timestamp_ms) is not int
        or agent_timestamp_ms < 0
    ):
        raise ValueError("session update metadata is not bound to the command session")
    update = params.get("update")
    if not isinstance(update, dict) or not isinstance(update.get("sessionUpdate"), str):
        raise ValueError("session update payload is invalid")
    kind = update["sessionUpdate"]
    expected_method = "_x.ai/session/update" if kind == "turn_completed" else "session/update"
    if payload.get("method") != expected_method:
        raise ValueError("session update method does not match its payload kind")
    return kind, update, params


def _merge_raw_posts(
    target: dict[tuple[str, str], set[str]],
    posts: Iterable[RawXPostReceipt],
) -> None:
    for post in posts:
        values = target.setdefault((post.platform_object_id, post.canonical_url), set())
        if post.platform_user_id is not None:
            values.add(post.platform_user_id)


def _receipts_from_bindings(bindings: Mapping[tuple[str, str], set[str]]) -> tuple[RawXPostReceipt, ...]:
    return tuple(
        RawXPostReceipt(
            platform_object_id=object_id,
            canonical_url=canonical_url,
            platform_user_id=next(iter(author_ids)) if len(author_ids) == 1 else None,
        )
        for (object_id, canonical_url), author_ids in sorted(bindings.items())
    )


def _parse_update_stream(
    raw: bytes,
    *,
    expected_session_id: str,
    require_terminal: bool,
    tolerate_trailing_partial: bool,
) -> ToolProof:
    try:
        uuid.UUID(expected_session_id)
    except (ValueError, AttributeError) as error:
        raise ValueError("expected command session id is invalid") from error
    tool_calls: dict[str, str] = {}
    x_calls: dict[str, set[str]] = {}
    receipt_bindings: dict[str, dict[tuple[str, str], set[str]]] = {}
    all_bindings: dict[tuple[str, str], set[str]] = {}
    unexpected: set[str] = set()
    model_ids: set[str] = set()
    evidence_errors: set[str] = set()
    terminal_stop_reason: str | None = None
    terminal_usage: ProviderUsageReceipt | None = None
    terminal_index: int | None = None
    user_event_count = 0
    lines = raw.splitlines(keepends=True)
    if not lines:
        evidence_errors.add("session updates are empty")
    for index, raw_line_with_ending in enumerate(lines):
        raw_line = raw_line_with_ending.rstrip(b"\r\n")
        if len(raw_line) > MAX_UPDATE_LINE_BYTES:
            evidence_errors.add("session update line exceeds the bounded parser size")
            continue
        try:
            payload = _strict_json_loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            is_trailing_partial = index == len(lines) - 1 and not raw.endswith(b"\n")
            if not (tolerate_trailing_partial and is_trailing_partial):
                evidence_errors.add("session updates contain malformed JSONL")
            continue
        try:
            kind, update, _ = _parse_update_envelope(payload, expected_session_id=expected_session_id)
        except ValueError as error:
            evidence_errors.add(str(error))
            continue
        if terminal_index is not None:
            evidence_errors.add("session updates continue after the terminal event")
        if kind == "user_message_chunk":
            user_event_count += 1
            metadata = update.get("_meta")
            model_id = metadata.get("modelId") if isinstance(metadata, dict) else None
            if not isinstance(model_id, str) or not model_id:
                evidence_errors.add("session user event lacks the effective model id")
            else:
                model_ids.add(model_id)
        elif kind == "tool_call":
            call_id = update.get("toolCallId")
            if not isinstance(call_id, str) or not call_id or len(call_id) > 160:
                evidence_errors.add("tool call id is invalid")
                continue
            if call_id in tool_calls:
                unexpected.add("duplicate_tool_call_id")
                continue
            tool_name = _tool_identity(update)
            tool_calls[call_id] = tool_name or "unknown"
            if tool_name == TOOL_ID:
                x_calls.setdefault(call_id, set()).add("in_progress")
                receipt_bindings.setdefault(call_id, {})
            else:
                unexpected.add(tool_name or "unknown")
        elif kind == "tool_call_update":
            call_id = update.get("toolCallId")
            if not isinstance(call_id, str) or call_id not in tool_calls:
                unexpected.add("unknown_tool_call_update")
                continue
            resolved_tool = tool_calls[call_id]
            update_tool = _tool_identity(update)
            if update_tool is not None and update_tool != resolved_tool:
                unexpected.add("tool_identity_drift")
            if resolved_tool != TOOL_ID:
                unexpected.add(resolved_tool)
                continue
            status = update.get("status")
            if status is not None:
                if not isinstance(status, str) or not status.strip():
                    evidence_errors.add("tool update status is invalid")
                else:
                    normalized_status = status.strip().lower()
                    if normalized_status not in {"in_progress", "completed"}:
                        evidence_errors.add("tool update status is invalid")
                    else:
                        x_calls.setdefault(call_id, set()).add(normalized_status)
            raw_output = update.get("rawOutput")
            if raw_output is not None:
                posts, binding_errors = _raw_x_posts(raw_output)
                unexpected.update(binding_errors)
                _merge_raw_posts(receipt_bindings.setdefault(call_id, {}), posts)
                _merge_raw_posts(all_bindings, posts)
        elif kind == "turn_completed":
            if terminal_index is not None:
                evidence_errors.add("session updates contain multiple terminal events")
                continue
            terminal_index = index
            if set(update) != {"prompt_id", "sessionUpdate", "stop_reason", "usage"}:
                evidence_errors.add("terminal event fields do not match Grok 0.2.99")
            terminal_stop_reason = update.get("stop_reason") if isinstance(update.get("stop_reason"), str) else None
            try:
                terminal_usage = _parse_provider_usage(update.get("usage"))
            except ValueError as error:
                evidence_errors.add(str(error))
        elif kind not in {"agent_thought_chunk", "agent_message_chunk"}:
            evidence_errors.add("session updates contain an unsupported event kind")
    if require_terminal:
        if not raw.endswith(b"\n"):
            evidence_errors.add("session updates lack a complete final JSONL record")
        if terminal_index is None or terminal_index != len(lines) - 1:
            evidence_errors.add("session updates lack one final terminal event")
        if terminal_stop_reason != "end_turn":
            evidence_errors.add("session terminal stop reason is not end_turn")
        if terminal_usage is None:
            evidence_errors.add("session terminal usage is unavailable")
        if model_ids != {MODEL_ID}:
            evidence_errors.add("session effective model identity is invalid")
        if user_event_count != 1:
            evidence_errors.add("session updates do not contain exactly one user event")
    for bindings in (*receipt_bindings.values(), all_bindings):
        if any(len(author_ids) > 1 for author_ids in bindings.values()):
            unexpected.add("conflicting_raw_post_author_binding")
    completed_states = {"completed"}
    completed = sum(1 for statuses in x_calls.values() if statuses & completed_states)
    call_receipts = tuple(
        ToolCallReceipt(
            call_id=call_id,
            tool_id=TOOL_ID,
            statuses=tuple(sorted(statuses)),
            raw_result_posts=_receipts_from_bindings(receipt_bindings.get(call_id, {})),
        )
        for call_id, statuses in sorted(x_calls.items())
    )
    return ToolProof(
        session_id=expected_session_id,
        x_search_calls=len(x_calls),
        x_search_completed_calls=completed,
        unexpected_tool_calls=tuple(sorted(unexpected)),
        raw_result_posts=_receipts_from_bindings(all_bindings),
        observed_model_ids=tuple(sorted(model_ids)),
        call_receipts=call_receipts,
        updates_sha256=hashlib.sha256(raw).hexdigest(),
        update_bytes=len(raw),
        terminal_stop_reason=terminal_stop_reason,
        terminal_usage=terminal_usage,
        evidence_errors=tuple(sorted(evidence_errors)),
    )


def _read_updates_bytes(updates_path: Path) -> bytes:
    descriptor, source_stat = _open_regular_owned_file(
        updates_path,
        maximum_bytes=MAX_SESSION_UPDATES_BYTES,
        private=False,
    )
    try:
        chunks: list[bytes] = []
        remaining = source_stat.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("session updates changed while they were read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError("session updates changed while they were read")
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _extract_partial_tool_proof(
    updates_path: Path,
    *,
    expected_session_id: str,
    tolerate_trailing_partial: bool,
) -> ToolProof:
    return _parse_update_stream(
        _read_updates_bytes(updates_path),
        expected_session_id=expected_session_id,
        require_terminal=False,
        tolerate_trailing_partial=tolerate_trailing_partial,
    )


def extract_tool_proof(updates_path: Path, *, expected_session_id: str) -> ToolProof:
    proof = _parse_update_stream(
        _read_updates_bytes(updates_path),
        expected_session_id=expected_session_id,
        require_terminal=True,
        tolerate_trailing_partial=False,
    )
    if proof.evidence_errors:
        raise ValueError("session updates failed the strict Grok 0.2.99 evidence contract")
    return proof


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


def _outer_response_errors(
    outer: Any,
    *,
    expected_session_id: str,
    proof: ToolProof | None,
) -> list[str]:
    errors: list[str] = []
    required_fields = {"text", "stopReason", "sessionId", "requestId", "num_turns", "usage"}
    allowed_fields = required_fields | {"total_cost_usd"}
    if not isinstance(outer, dict) or not required_fields <= set(outer) or not set(outer) <= allowed_fields:
        return ["Grok headless envelope fields do not match the closed contract"]
    if outer.get("stopReason") != "EndTurn":
        errors.append("Grok headless envelope did not end normally")
    if outer.get("sessionId") != expected_session_id:
        errors.append("Grok headless envelope session does not match the command session")
    request_id = outer.get("requestId")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 160:
        errors.append("Grok headless envelope request id is invalid")
    model_turns = outer.get("num_turns")
    if type(model_turns) is not int or not 1 <= model_turns <= MAX_TURNS:
        errors.append("Grok headless envelope turn count is invalid")
    usage = outer.get("usage")
    if not isinstance(usage, dict) or set(usage) != {"input_tokens", "output_tokens", "total_tokens"}:
        errors.append("Grok headless usage fields do not match the closed contract")
    elif any(type(usage.get(field)) is not int or usage[field] < 0 for field in usage):
        errors.append("Grok headless usage counters are invalid")
    elif usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]:
        errors.append("Grok headless token totals do not reconcile")
    cost_status, _ = _cost_projection(outer)
    if cost_status == "invalid":
        errors.append("Grok headless cost is invalid")
    if proof is not None:
        terminal = proof.terminal_usage
        if proof.session_id != expected_session_id:
            errors.append("structured updates are not bound to the command session")
        if proof.terminal_stop_reason != "end_turn" or terminal is None:
            errors.append("structured updates lack a successful terminal event")
        elif isinstance(usage, dict):
            expected_usage = {
                "input_tokens": terminal.input_tokens,
                "output_tokens": terminal.output_tokens,
                "total_tokens": terminal.total_tokens,
            }
            if usage != expected_usage or model_turns != terminal.model_turns:
                errors.append("headless and structured terminal usage do not reconcile")
    return sorted(set(errors))


def _parse_outer_response(stdout: bytes, *, expected_session_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(stdout) > MAX_STDOUT_BYTES:
        raise ValueError("Grok output exceeds the bounded parser size")
    try:
        outer = _strict_json_loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError("Grok output is not one JSON object") from error
    if not isinstance(outer, dict) or outer.get("type") == "error":
        raise ValueError("Grok did not return a successful headless envelope")
    if _outer_response_errors(outer, expected_session_id=expected_session_id, proof=None):
        raise ValueError("Grok headless envelope failed the strict contract")
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
    outer: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    raw_result_pairs = set(proof.raw_result_post_pairs) if proof is not None else set()
    outer_mapping = outer if isinstance(outer, Mapping) else {}
    provider_request_id = outer_mapping.get("requestId")
    if not isinstance(provider_request_id, str) or not provider_request_id:
        provider_request_id = None
    observed_turns = []
    outer_turns = outer_mapping.get("num_turns")
    if type(outer_turns) is int and outer_turns >= 0:
        observed_turns.append(outer_turns)
    if proof is not None and proof.terminal_usage is not None:
        observed_turns.append(proof.terminal_usage.model_turns)
    model_turns = max(observed_turns, default=0)
    cost_status, cost_usd = _cost_projection(outer_mapping)
    if cost_status == "invalid":
        cost_status, cost_usd = "unreported", None
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
            "provider_request_id": provider_request_id,
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
            "result_sets": proof.x_search_completed_calls if proof is not None else 0,
            "observations": 0,
            "model_turns": model_turns,
            "cost_status": cost_status,
            "cost_usd": cost_usd,
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
        if not stat.S_ISREG(source_stat.st_mode) or source_stat.st_uid != os.getuid() or source_stat.st_nlink != 1:
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


def _read_private_json(path: Path, *, maximum_bytes: int = MAX_STDOUT_BYTES) -> Any:
    descriptor, source_stat = _open_regular_owned_file(path, maximum_bytes=maximum_bytes, private=True)
    try:
        chunks: list[bytes] = []
        remaining = source_stat.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("private JSON artifact changed while it was read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError("private JSON artifact changed while it was read")
    finally:
        os.close(descriptor)
    return _strict_json_loads(b"".join(chunks))


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
    if not path.is_dir() or path.stat().st_uid != os.getuid() or mode & 0o077:
        raise ValueError("private runtime directory permissions are unsafe")


def _global_approval_root() -> Path:
    return Path.home() / ".local/state/x-first-researcher-sourcing/live-approvals"


def _canonical_runtime_root() -> Path:
    return project_root() / "runtime/live-probes"


def _approval_ledger_path(approval_root: Path, probe_id: Any) -> Path:
    if probe_id != "xprobe_live_openai_official_v1":
        raise ValueError("live approval probe identity is invalid")
    return approval_root / f"{probe_id}.json"


def _consume_live_approval(
    approval_root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    binary_sha256: str,
    consumed_at: str,
) -> tuple[dict[str, Any], Path]:
    _ensure_private_directory(approval_root)
    receipt = {
        "schema_version": APPROVAL_RECEIPT_SCHEMA_VERSION,
        "owner_id": GLOBAL_APPROVAL_OWNER_ID,
        "probe_id": request["probe_id"],
        "request_sha256": canonical_sha256(request),
        "run_id": run_id,
        "consumed_at": consumed_at,
        "binary_sha256": binary_sha256,
        "state": "consumed_before_spawn",
    }
    receipt_path = _approval_ledger_path(approval_root, request["probe_id"])
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
    directory_descriptor = os.open(approval_root, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return receipt, receipt_path


def _provider_usage_payload(usage: ProviderUsageReceipt | None) -> dict[str, int] | None:
    if usage is None:
        return None
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
        "cached_read_tokens": usage.cached_read_tokens,
        "reasoning_tokens": usage.reasoning_tokens,
        "model_calls": usage.model_calls,
        "api_duration_ms": usage.api_duration_ms,
        "model_turns": usage.model_turns,
    }


def _build_tool_receipt(
    *,
    proof: ToolProof,
    session_id: str,
    outer: Mapping[str, Any] | None,
) -> dict[str, Any]:
    outer_usage = outer.get("usage") if isinstance(outer, Mapping) else None
    outer_model_turns = outer.get("num_turns") if isinstance(outer, Mapping) else None
    outer_cost = outer.get("total_cost_usd") if isinstance(outer, Mapping) else None
    return {
        "schema_version": TOOL_RECEIPT_SCHEMA_VERSION,
        "session_id": session_id,
        "provider_request_id": outer.get("requestId") if isinstance(outer, Mapping) else None,
        "outer_session_id": outer.get("sessionId") if isinstance(outer, Mapping) else None,
        "outer_stop_reason": outer.get("stopReason") if isinstance(outer, Mapping) else None,
        "outer_usage": dict(outer_usage) if isinstance(outer_usage, Mapping) else None,
        "outer_model_turns": outer_model_turns if type(outer_model_turns) is int else None,
        "outer_total_cost_usd": outer_cost if type(outer_cost) in {int, float} else None,
        "session_updates_sha256": proof.updates_sha256,
        "session_update_bytes": proof.update_bytes,
        "terminal_stop_reason": proof.terminal_stop_reason,
        "terminal_usage": _provider_usage_payload(proof.terminal_usage),
        "observed_model_ids": list(proof.observed_model_ids),
        "unexpected_tool_calls": list(proof.unexpected_tool_calls),
        "evidence_errors": list(proof.evidence_errors),
        "calls": [
            {
                "call_id": call.call_id,
                "tool_id": call.tool_id,
                "statuses": list(call.statuses),
                "raw_result_posts": [
                    {
                        "platform_object_id": post.platform_object_id,
                        "canonical_url": post.canonical_url,
                        "platform_user_id": post.platform_user_id,
                    }
                    for post in call.raw_result_posts
                ],
                "raw_result_author_user_ids": sorted(
                    {post.platform_user_id for post in call.raw_result_posts if post.platform_user_id}
                ),
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
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def _wait_for_process_group_exit(process_group_id: int) -> None:
    deadline = time.monotonic() + PROCESS_GROUP_CLEANUP_SECONDS
    while True:
        group_exists = True
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            group_exists = False
        except PermissionError:
            # A killed orphan can briefly be a launchd-owned zombie on macOS. Verify that no executable
            # member remains instead of treating the zombie accounting row as a live provider process.
            group_exists = True
        if not group_exists:
            return
        process_rows = subprocess.run(
            ["/bin/ps", "-axo", "pgid=,stat="],
            check=False,
            capture_output=True,
            text=True,
        )
        if process_rows.returncode != 0:
            raise RuntimeError("bounded Grok process group could not be verified after termination")
        active_members = []
        for row in process_rows.stdout.splitlines():
            fields = row.split()
            if len(fields) >= 2 and fields[0].isdigit() and int(fields[0]) == process_group_id:
                state = fields[1]
                if not state.startswith("Z"):
                    active_members.append(state)
        if not active_members:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("bounded Grok process group did not terminate")
        time.sleep(PROCESS_POLL_SECONDS)


def _run_bounded_command(
    command: list[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    updates_path: Path,
    expected_session_id: str,
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
                            proof = _extract_partial_tool_proof(
                                updates_path,
                                expected_session_id=expected_session_id,
                                tolerate_trailing_partial=True,
                            )
                        except ValueError:
                            proof = None
                        if proof is not None and (proof.x_search_calls > 1 or proof.unexpected_tool_calls):
                            stop_reason = "tool_kill_switch_tripped"
                        elif proof is not None and proof.evidence_errors:
                            stop_reason = "invalid_provider_evidence"
                if stop_reason is not None:
                    break
                time.sleep(PROCESS_POLL_SECONDS)
            # A headless parent can exit while leaving same-session descendants behind. Always kill the
            # dedicated process group, including on an apparently clean parent exit.
            _terminate_process_group(process)
            try:
                returncode = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _terminate_process_group(process)
                returncode = process.wait(timeout=5)
            _wait_for_process_group_exit(process.pid)
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
    completed = _parse_canonical_timestamp(completed_at)
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
    binary = grok_binary or Path.home() / ".grok/bin/grok"
    oauth = auth_path or Path.home() / ".grok/auth.json"
    runtime_root = _canonical_runtime_root()
    _ensure_private_directory(runtime_root)
    run_id = f"xprobe_run_{uuid.uuid4().hex}"
    session_id = str(uuid.uuid4())
    prompt = build_grok_prompt(live_request)
    result: dict[str, Any]
    tool_receipt: dict[str, Any] | None = None
    proof: ToolProof | None = None
    outer: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory(prefix="x-first-grok-home-") as temporary_home_name:
        temporary_home = Path(temporary_home_name)
        isolated_cwd = temporary_home / "work"
        isolated_tmp = temporary_home / "tmp"
        isolated_cwd.mkdir(mode=0o700)
        isolated_tmp.mkdir(mode=0o700)
        staged_binary = temporary_home / "grok"
        binary_sha256 = _stage_verified_binary(binary, staged_binary)
        _secure_auth_copy(oauth, temporary_home / "auth.json")
        started_at = _utc_now()
        started_monotonic = time.monotonic()
        approval_receipt, _ = _consume_live_approval(
            _global_approval_root(),
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
                expected_session_id=session_id,
            )
        except Exception:
            completed_at = _utc_now()
            elapsed_ms = round((time.monotonic() - started_monotonic) * 1000)
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
                outer, inner = _parse_outer_response(completed.stdout, expected_session_id=session_id)
                proof = extract_tool_proof(updates_path, expected_session_id=session_id)
                tool_receipt = _build_tool_receipt(proof=proof, session_id=session_id, outer=outer)
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
                        proof = _extract_partial_tool_proof(
                            updates_path,
                            expected_session_id=session_id,
                            tolerate_trailing_partial=False,
                        )
                        tool_receipt = _build_tool_receipt(
                            proof=proof,
                            session_id=session_id,
                            outer=outer,
                        )
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
                    outer=outer,
                )
    errors = validate_live_result(result, request=live_request)
    if errors:
        result = _build_failure_result(
            request=live_request,
            run_id=run_id,
            started_at=started_at,
            completed_at=_utc_now(),
            elapsed_ms=round((time.monotonic() - started_monotonic) * 1000),
            code="result_validation_failed",
            message="The bounded live result failed its executable contract.",
            approval_receipt_sha256=approval_receipt_sha256,
            grok_binary_sha256=binary_sha256,
            tool_receipt_sha256=canonical_sha256(tool_receipt) if tool_receipt is not None else None,
            session_id=session_id,
            proof=proof,
            outer=outer,
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
    artifact_errors = validate_artifact_pair(output_root / "request.json", output_root / "result.json")
    if artifact_errors:
        raise RuntimeError("written live artifact bundle failed the executable contract")
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
    if proof.evidence_errors or _outer_response_errors(outer, expected_session_id=session_id, proof=proof):
        raise ValueError("Grok provider evidence failed the executable contract")
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
    raw_result_posts = {
        (post.platform_object_id, post.canonical_url): post.platform_user_id for post in proof.raw_result_posts
    }
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
        and all(
            raw_result_posts.get((item["platform_object_id"], item["canonical_url"])) == target_account_id
            for item in observations
        )
    )
    x_native_proven = (
        reported_verdict == "x_native_candidate"
        and access_mode == "x_search"
        and proof.x_search_calls == 1
        and proof.x_search_completed_calls == 1
        and not proof.unexpected_tool_calls
        and proof.observed_model_ids == (MODEL_ID,)
        and proof.terminal_stop_reason == "end_turn"
        and proof.terminal_usage is not None
        and 1 <= len(observations) <= MAX_OBSERVATIONS
        and len(unique_objects) == len(observations)
        and len(unique_urls) == len(observations)
        and observation_pairs <= raw_result_pairs
    )
    cost_status, cost_usd = _cost_projection(outer)
    model_turns = proof.terminal_usage.model_turns if proof.terminal_usage is not None else 0
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
        observations = [{**item, "platform_user_id": None} for item in observations]
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
            "x_native_access_proven": x_native_proven and successful,
            "stable_account_id_proven": stable_account_id and x_native_proven and successful,
            "stage2_eligible_for_owner_review": verdict == "x_native_identity_ready",
        },
        "provenance": {
            "provider_id": PROVIDER_ID,
            "access_mode": "x_search" if successful else "unavailable",
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
    run_duration_ms: int | None = None
    try:
        started = _parse_canonical_timestamp(run.get("started_at"))
        completed = _parse_canonical_timestamp(run.get("completed_at"))
        if started > completed:
            errors.append("result run starts after it completes")
        else:
            run_duration_ms = round((completed - started).total_seconds() * 1000)
    except (TypeError, ValueError):
        errors.append("result run timestamps must be real canonical UTC milliseconds")
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
            or observed_at != run.get("completed_at")
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
        or len(set(model_ids)) != len(model_ids)
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
    maximum_elapsed_ms = MAX_ELAPSED_MS if successful else MAX_FAILURE_WALL_ELAPSED_MS
    if (
        usage.get("executions") != 1
        or usage.get("x_search_calls", 0) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or usage.get("result_sets", 0) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
        or usage.get("elapsed_ms", 0) > maximum_elapsed_ms
    ):
        errors.append("result execution evidence exceeds the bounded failure-receipt contract")
    if successful and (usage.get("x_search_calls") != 1 or usage.get("result_sets") != 1):
        errors.append("successful result must prove exactly one X call/result set")
    if usage.get("observations") != len(observations) or usage.get("model_turns", 0) > MAX_TURNS:
        errors.append("result usage does not reconcile")
    if run_duration_ms is not None and (
        run_duration_ms > maximum_elapsed_ms
        or type(usage.get("elapsed_ms")) is not int
        or abs(usage["elapsed_ms"] - run_duration_ms) > 2_000
    ):
        errors.append("result elapsed time does not reconcile with its wall-clock interval")
    cost_status = usage.get("cost_status")
    cost_usd = usage.get("cost_usd")
    if cost_status == "reported":
        if (
            type(cost_usd) not in {int, float}
            or not math.isfinite(float(cost_usd))
            or cost_usd < 0
            or (successful and cost_usd > MAX_REPORTED_COST_USD)
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
        "owner_id",
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
    if receipt.get("owner_id") != GLOBAL_APPROVAL_OWNER_ID:
        errors.append("approval receipt owner mismatch")
    if receipt.get("probe_id") != request.get("probe_id"):
        errors.append("approval receipt probe mismatch")
    if receipt.get("request_sha256") != canonical_sha256(request):
        errors.append("approval receipt request binding mismatch")
    if receipt.get("run_id") != run.get("run_id"):
        errors.append("approval receipt run binding mismatch")
    if receipt.get("consumed_at") != run.get("started_at"):
        errors.append("approval must be consumed immediately before the recorded spawn window")
    try:
        _parse_canonical_timestamp(receipt.get("consumed_at"))
    except (TypeError, ValueError):
        errors.append("approval consumption timestamp is invalid")
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
        "provider_request_id",
        "outer_session_id",
        "outer_stop_reason",
        "outer_usage",
        "outer_model_turns",
        "outer_total_cost_usd",
        "session_updates_sha256",
        "session_update_bytes",
        "terminal_stop_reason",
        "terminal_usage",
        "observed_model_ids",
        "unexpected_tool_calls",
        "evidence_errors",
        "calls",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_fields:
        return ["tool receipt fields do not match the closed contract"]
    provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    capability = result.get("capability") if isinstance(result.get("capability"), dict) else {}
    result_usage = result.get("usage") if isinstance(result.get("usage"), dict) else {}
    observations = result.get("observations") if isinstance(result.get("observations"), list) else []
    successful = capability.get("verdict") in {"x_native_identity_ready", "post_retrieval_only"}
    if receipt.get("schema_version") != TOOL_RECEIPT_SCHEMA_VERSION:
        errors.append("tool receipt schema version mismatch")
    if receipt.get("session_id") != provenance.get("session_id"):
        errors.append("tool receipt session binding mismatch")
    if receipt.get("provider_request_id") != provenance.get("provider_request_id"):
        errors.append("tool receipt request binding mismatch")
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
    observed_model_ids = receipt.get("observed_model_ids")
    if (
        not isinstance(observed_model_ids, list)
        or len(observed_model_ids) > MAX_VIOLATION_RECEIPT_CALLS
        or any(not isinstance(value, str) or not value or len(value) > 80 for value in observed_model_ids)
        or len(set(observed_model_ids)) != len(observed_model_ids)
    ):
        errors.append("tool receipt observed-model list is invalid")
    if observed_model_ids != provenance.get("observed_model_ids"):
        errors.append("tool receipt model ids do not reconcile")
    unexpected = receipt.get("unexpected_tool_calls")
    if (
        not isinstance(unexpected, list)
        or len(unexpected) > MAX_VIOLATION_RECEIPT_CALLS
        or any(not isinstance(value, str) or not value or len(value) > 160 for value in unexpected)
        or len(set(unexpected)) != len(unexpected)
    ):
        errors.append("tool receipt unexpected-tool list is invalid")
    if successful and unexpected:
        errors.append("successful tool receipt contains unexpected tools")
    evidence_errors = receipt.get("evidence_errors")
    if (
        not isinstance(evidence_errors, list)
        or len(evidence_errors) > MAX_VIOLATION_RECEIPT_CALLS
        or any(not isinstance(value, str) or not value or len(value) > 200 for value in evidence_errors)
        or len(set(evidence_errors)) != len(evidence_errors)
    ):
        errors.append("tool receipt evidence-error list is invalid")
    if successful and evidence_errors:
        errors.append("successful tool receipt contains provider-evidence errors")
    normalized_usage_fields = {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_read_tokens",
        "reasoning_tokens",
        "model_calls",
        "api_duration_ms",
        "model_turns",
    }
    terminal_usage = receipt.get("terminal_usage")
    outer_usage = receipt.get("outer_usage")
    outer_session_id = receipt.get("outer_session_id")
    if outer_session_id is not None:
        try:
            if not isinstance(outer_session_id, str):
                raise ValueError("outer session id is not a string")
            uuid.UUID(str(outer_session_id))
        except (ValueError, AttributeError):
            errors.append("tool receipt outer session id is invalid")
    outer_stop_reason = receipt.get("outer_stop_reason")
    if outer_stop_reason not in {None, "EndTurn"}:
        errors.append("tool receipt outer stop reason is invalid")
    terminal_stop_reason = receipt.get("terminal_stop_reason")
    if terminal_stop_reason not in {None, "end_turn", "max_turns"}:
        errors.append("tool receipt terminal stop reason is invalid")
    outer_model_turns = receipt.get("outer_model_turns")
    if outer_model_turns is not None and (
        type(outer_model_turns) is not int or not 1 <= outer_model_turns <= MAX_TURNS
    ):
        errors.append("tool receipt outer model turns are invalid")
        outer_model_turns = None
    outer_cost = receipt.get("outer_total_cost_usd")
    if outer_cost is not None and (
        type(outer_cost) not in {int, float} or not math.isfinite(float(outer_cost)) or outer_cost < 0
    ):
        errors.append("tool receipt outer cost is invalid")
        outer_cost = None
    if outer_usage is not None:
        if not isinstance(outer_usage, dict) or set(outer_usage) != {
            "input_tokens",
            "output_tokens",
            "total_tokens",
        }:
            errors.append("tool receipt outer usage is invalid")
            outer_usage = None
        elif any(type(outer_usage.get(field)) is not int or outer_usage[field] < 0 for field in outer_usage):
            errors.append("tool receipt outer counters are invalid")
            outer_usage = None
        elif outer_usage["total_tokens"] != outer_usage["input_tokens"] + outer_usage["output_tokens"]:
            errors.append("tool receipt outer token usage does not reconcile")
    if terminal_usage is not None:
        if not isinstance(terminal_usage, dict) or set(terminal_usage) != normalized_usage_fields:
            errors.append("tool receipt terminal usage is invalid")
            terminal_usage = None
        elif any(type(terminal_usage.get(field)) is not int or terminal_usage[field] < 0 for field in terminal_usage):
            errors.append("tool receipt terminal counters are invalid")
            terminal_usage = None
        elif (
            terminal_usage["total_tokens"] != terminal_usage["input_tokens"] + terminal_usage["output_tokens"]
            or terminal_usage["cached_read_tokens"] > terminal_usage["input_tokens"]
            or terminal_usage["reasoning_tokens"] > terminal_usage["output_tokens"]
            or not 1 <= terminal_usage["model_turns"] <= MAX_TURNS
            or terminal_usage["model_calls"] < 1
        ):
            errors.append("tool receipt terminal usage does not reconcile")
    observed_turns = [
        value
        for value in (
            outer_model_turns,
            terminal_usage.get("model_turns") if isinstance(terminal_usage, dict) else None,
        )
        if type(value) is int
    ]
    if result_usage.get("model_turns") != max(observed_turns, default=0):
        errors.append("tool receipt model turns do not match result usage")
    expected_cost_status = "reported" if outer_cost is not None else "unreported"
    expected_cost = outer_cost if outer_cost is not None else None
    if result_usage.get("cost_status") != expected_cost_status or result_usage.get("cost_usd") != expected_cost:
        errors.append("tool receipt cost does not match result usage")
    if successful:
        if outer_session_id != receipt.get("session_id"):
            errors.append("successful tool receipt outer session mismatch")
        if outer_stop_reason != "EndTurn" or terminal_stop_reason != "end_turn":
            errors.append("successful tool receipt lacks matching terminal stop reasons")
        if outer_usage is None or terminal_usage is None or outer_model_turns is None:
            errors.append("successful tool receipt lacks complete usage evidence")
        elif any(
            outer_usage.get(field) != terminal_usage.get(field) for field in outer_usage
        ) or outer_model_turns != terminal_usage.get("model_turns"):
            errors.append("tool receipt outer and terminal usage do not reconcile")
    calls = receipt.get("calls")
    if not isinstance(calls, list) or len(calls) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS):
        errors.append("tool receipt exceeds the bounded call-receipt contract")
        calls = []
    if successful and len(calls) != 1:
        errors.append("successful result lacks exactly one X Search receipt")
    call_ids: set[str] = set()
    post_pairs: set[tuple[str, str]] = set()
    author_ids: set[str] = set()
    post_bindings: dict[tuple[str, str], str | None] = {}
    completed_calls = 0
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
        if (
            not isinstance(statuses, list)
            or any(not isinstance(value, str) or value not in {"in_progress", "completed"} for value in statuses)
            or len(set(statuses)) != len(statuses)
        ):
            errors.append("tool-call receipt statuses are invalid")
        else:
            if "completed" in statuses:
                completed_calls += 1
            if successful and "completed" not in statuses:
                errors.append("successful X Search receipt lacks completion")
        posts = call.get("raw_result_posts")
        if not isinstance(posts, list) or len(posts) > MAX_VIOLATION_RECEIPT_POSTS:
            errors.append("tool-call raw post receipt exceeds the bound")
            posts = []
        seen_raw_posts: set[tuple[str, str, str | None]] = set()
        for post in posts:
            if not isinstance(post, dict) or set(post) != {
                "platform_object_id",
                "canonical_url",
                "platform_user_id",
            }:
                errors.append("tool-call raw post fields are invalid")
                continue
            object_id = post.get("platform_object_id")
            canonical_url = post.get("canonical_url")
            platform_user_id = post.get("platform_user_id")
            if (
                not isinstance(object_id, str)
                or re.fullmatch(r"[0-9]{5,32}", object_id) is None
                or canonical_url != f"https://x.com/{TARGET_HANDLE}/status/{object_id}"
                or (
                    platform_user_id is not None
                    and (
                        not isinstance(platform_user_id, str) or re.fullmatch(r"[0-9]{3,32}", platform_user_id) is None
                    )
                )
            ):
                errors.append("tool-call raw post identity is invalid")
                continue
            raw_post_identity = (object_id, canonical_url, platform_user_id)
            if raw_post_identity in seen_raw_posts:
                errors.append("tool-call raw post receipt contains duplicates")
                continue
            seen_raw_posts.add(raw_post_identity)
            pair = (object_id, canonical_url)
            if pair in post_bindings and post_bindings[pair] != platform_user_id:
                errors.append("tool-call raw post has conflicting author bindings")
            post_bindings[pair] = platform_user_id
            post_pairs.add(pair)
            if platform_user_id is not None:
                author_ids.add(platform_user_id)
        ids = call.get("raw_result_author_user_ids")
        if (
            not isinstance(ids, list)
            or len(ids) > (1 if successful else MAX_VIOLATION_RECEIPT_CALLS)
            or any(not isinstance(value, str) or re.fullmatch(r"[0-9]{3,32}", value) is None for value in ids)
            or len(set(ids)) != len(ids)
        ):
            errors.append("tool-call raw author ids are invalid")
        elif sorted(ids) != sorted(
            {
                post.get("platform_user_id")
                for post in posts
                if isinstance(post, dict) and post.get("platform_user_id") is not None
            }
        ):
            errors.append("tool-call raw author ids are not derived from bound post records")
    expected_pairs = {
        (str(observation.get("platform_object_id")), str(observation.get("canonical_url")))
        for observation in observations
        if isinstance(observation, dict)
    }
    if successful and not expected_pairs <= post_pairs:
        errors.append("retained observations are not a subset of raw tool-receipt posts")
    if successful:
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            pair = (str(observation.get("platform_object_id")), str(observation.get("canonical_url")))
            if post_bindings.get(pair) != observation.get("platform_user_id"):
                errors.append("retained observation author is not bound on its raw X post record")
                break
    if result_usage.get("x_search_calls") != len(calls) or result_usage.get("result_sets") != completed_calls:
        errors.append("tool receipt calls/result sets do not match result usage")
    if sorted(object_id for object_id, _ in post_pairs) != provenance.get("raw_result_post_ids"):
        errors.append("tool receipt raw post ids do not match result provenance")
    if sorted(author_ids) != provenance.get("raw_result_author_user_ids"):
        errors.append("tool receipt raw author ids do not match result provenance")
    if provenance.get("tool_receipt_sha256") != canonical_sha256(receipt):
        errors.append("result does not bind the tool receipt")
    return errors


def validate_artifact_pair(request_path: Path, result_path: Path) -> list[str]:
    try:
        request_path = request_path.absolute()
        result_path = result_path.absolute()
        if request_path.name != "request.json" or result_path.name != "result.json":
            raise ValueError("live artifact filenames do not match the closed bundle contract")
        if request_path.parent != result_path.parent:
            raise ValueError("live artifact files must share a directory")
        artifact_root = request_path.parent
        canonical_runtime_root = _canonical_runtime_root().absolute()
        if artifact_root.parent != canonical_runtime_root:
            raise ValueError("live artifact bundle is outside the canonical runtime owner")
        if canonical_runtime_root.is_symlink() or artifact_root.is_symlink():
            raise ValueError("live artifact path must not be a symlink")
        runtime_stat = canonical_runtime_root.stat()
        artifact_stat = artifact_root.stat()
        if (
            not canonical_runtime_root.is_dir()
            or not artifact_root.is_dir()
            or runtime_stat.st_uid != os.getuid()
            or artifact_stat.st_uid != os.getuid()
            or stat.S_IMODE(runtime_stat.st_mode) & 0o077
            or stat.S_IMODE(artifact_stat.st_mode) & 0o077
        ):
            raise ValueError("live artifact directory is not private")
        request = _read_private_json(request_path)
        result = _read_private_json(result_path)
        if not isinstance(request, dict) or not isinstance(result, dict):
            raise ValueError("live artifact pair must contain objects")
        run = result.get("run") if isinstance(result.get("run"), dict) else {}
        if (
            artifact_root.name != run.get("run_id")
            or re.fullmatch(r"xprobe_run_[0-9a-f]{32}", artifact_root.name) is None
        ):
            raise ValueError("live artifact directory is not bound to the result run id")
        approval_path = artifact_root / "approval-receipt.json"
        provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
        expected_inventory = {"request.json", "result.json", "approval-receipt.json"}
        if provenance.get("tool_receipt_sha256") is not None:
            expected_inventory.add("tool-receipt.json")
        actual_inventory = {entry.name for entry in artifact_root.iterdir()}
        if actual_inventory != expected_inventory:
            raise ValueError("live artifact inventory does not match the closed bundle contract")
        for name in expected_inventory:
            path = artifact_root / name
            path_stat = path.lstat()
            if (
                path.is_symlink()
                or not stat.S_ISREG(path_stat.st_mode)
                or path_stat.st_uid != os.getuid()
                or path_stat.st_nlink != 1
                or stat.S_IMODE(path_stat.st_mode) & 0o077
            ):
                raise ValueError("live artifact file is not a private user-owned regular file")
        approval = _read_private_json(approval_path)
        approval_root = _global_approval_root()
        if approval_root.is_symlink() or not approval_root.is_dir():
            raise ValueError("global approval owner is unavailable")
        approval_root_stat = approval_root.stat()
        if approval_root_stat.st_uid != os.getuid() or stat.S_IMODE(approval_root_stat.st_mode) & 0o077:
            raise ValueError("global approval owner is unsafe")
        ledger_path = _approval_ledger_path(approval_root, request.get("probe_id"))
        ledger = _read_private_json(ledger_path)
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
    tool_path = request_path.parent / "tool-receipt.json"
    if provenance.get("tool_receipt_sha256") is not None:
        try:
            tool_receipt = _read_private_json(tool_path)
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
    root = runtime_root or _canonical_runtime_root()
    if not root.exists():
        return []
    _ensure_private_directory(root)
    deletion_root = root / ".deletions"
    _ensure_private_directory(deletion_root)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    receipts: list[dict[str, Any]] = []
    for artifact_root in sorted(root.iterdir()):
        if artifact_root.name == ".deletions":
            continue
        if re.fullmatch(r"xprobe_run_[0-9a-f]{32}", artifact_root.name) is None:
            raise ValueError("live artifact runtime contains an unexpected path")
        if artifact_root.is_symlink() or not artifact_root.is_dir():
            raise ValueError("live artifact runtime contains an unsafe run path")
        result_path = artifact_root / "result.json"
        artifact_errors = validate_artifact_pair(artifact_root / "request.json", result_path)
        if artifact_errors:
            raise ValueError("live artifact runtime contains an invalid closed bundle")
        result = _read_private_json(result_path)
        if not isinstance(result, dict):
            raise ValueError("live artifact runtime contains a non-object result")
        retention = result.get("retention")
        if not isinstance(retention, dict) or retention.get("deletion_status") != "pending":
            raise ValueError("live artifact runtime contains an invalid retention state")
        expires_value = retention.get("delete_after")
        if not isinstance(expires_value, str):
            raise ValueError("live artifact runtime is missing its deletion deadline")
        expires_at = _parse_canonical_timestamp(expires_value)
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
        if deletion_path.exists() or deletion_path.is_symlink():
            raise ValueError("live artifact deletion receipt already exists before deletion")
        shutil.rmtree(artifact_root)
        if artifact_root.exists() or artifact_root.is_symlink():
            raise OSError("live artifact deletion did not remove the bundle")
        root_descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(root_descriptor)
        finally:
            os.close(root_descriptor)
        _atomic_write_json(deletion_path, receipt)
        receipts.append(receipt)
    return receipts


__all__ = [
    "GROK_RESPONSE_SCHEMA",
    "GLOBAL_APPROVAL_OWNER_ID",
    "LIVE_REQUEST_SCHEMA_VERSION",
    "LIVE_RESULT_SCHEMA_VERSION",
    "ProviderUsageReceipt",
    "RawXPostReceipt",
    "ToolCallReceipt",
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
