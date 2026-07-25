"""Shared operator replay for immutable Grok CLI session artifacts.

Compact discovery and profile hydration deliberately share this boundary.  A
receipt is derived from the same six-file raw-session shape used by the recall
pool campaign lane; callers cannot upgrade a model answer by supplying hashes
or lifecycle summaries without the exact bytes from which they were derived.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from x_first.recall_pool_campaign import (
    RAW_SESSION_FILES,
    CampaignValidationError,
    _extract_bound_prompt,
    _strict_json_text,
    _tool_arguments_valid,
    _validate_bound_system_message,
    bytes_sha256,
    canonical_sha256,
    strict_json_bytes,
)


class GrokOperatorSessionReplayError(ValueError):
    """Stable fail-closed error raised while replaying raw session bytes."""


_SHA256_RE = re.compile(r"[0-9a-f]{64}")

ASSISTANT_JSON_MAX_DEPTH = 64
ASSISTANT_JSON_MAX_NODES = 50_000

RAW_SESSION_SHAPE_REGISTRY_VERSION = "x.grok.raw_session_shape.v1"
SUPPORTED_SESSION_UPDATE_KINDS = frozenset(
    {
        "agent_message_chunk",
        "agent_thought_chunk",
        "tool_call",
        "tool_call_update",
        "user_message_chunk",
    }
)
SUPPORTED_SESSION_EVENT_TYPES = frozenset(
    {"first_token", "loop_started", "phase_changed", "turn_ended", "turn_started"}
)
SUPPORTED_MODEL_CHAT_ROW_TYPES = frozenset(
    {"assistant", "backend_tool_call", "reasoning"}
)
PROMPT_BINDING_MODES = frozenset(
    {"legacy_user_query_envelope_v1", "verbatim_prompt_row_v1"}
)
_EVENT_METADATA_BASE_KEYS = frozenset(
    {
        "agentTimestampMs",
        "eventId",
        "promptId",
        "streamStartMs",
        "totalTokens",
        "turnStartMs",
        "updateType",
    }
)
_EVENT_METADATA_INTEGER_KEYS = (
    "agentTimestampMs",
    "streamStartMs",
    "totalTokens",
    "turnStartMs",
)


@dataclass(frozen=True)
class GrokOperatorSessionPrecommit:
    """Operator-owned identity and execution-context values fixed before replay."""

    expected_session_id: str
    expected_request_id: str
    expected_model_id: str
    expected_reasoning_effort: str
    prompt_binding_mode: str
    expected_user_prompt: bytes
    expected_user_prompt_sha256: str
    expected_chat_history_prefix: bytes
    expected_chat_history_prefix_sha256: str
    expected_system_prompt_sha256: str
    expected_prompt_context_sha256: str


@dataclass(frozen=True)
class GrokOperatorExecutionFacts:
    """Process-owner facts that must not be copied from a model receipt."""

    result_truncated: bool
    execution_deadline_reached: bool
    transport_failure: bool
    model_output_repaired: bool


@dataclass(frozen=True)
class FrozenRawSessionArtifact:
    """One immutable source artifact retained for downstream replay."""

    name: str
    content: bytes


@dataclass(frozen=True)
class ReplayedNativeToolCompletion:
    """One native-X start/completion pair derived from exact JSONL records."""

    call_id: str
    provider_call_id: str
    tool_name: str
    arguments_json: str
    query: str | None
    started_update_index: int
    completed_update_index: int
    start_event_sha256: str
    completion_event_sha256: str


@dataclass(frozen=True)
class _ReplayedNativeToolStart:
    """Internal exact XSearch start retained until its completion is paired."""

    call_id: str
    title: str
    update_index: int
    event_bytes: bytes


@dataclass(frozen=True)
class GrokOperatorSessionReplay:
    """Mechanically replayed session facts and the retained immutable bytes."""

    source_artifacts: tuple[FrozenRawSessionArtifact, ...]
    source_artifact_sha256s: tuple[tuple[str, str], ...]
    transcript_sha256: str
    session_precommit_sha256: str
    raw_session_shape_registry_version: str
    session_id: str
    request_id: str
    model_id: str
    system_prompt_sha256: str
    prompt_context_sha256: str
    user_prompt_sha256: str
    terminal: dict[str, Any] | str
    terminal_sha256: str
    terminal_start_byte_offset: int
    terminal_end_byte_offset_exclusive: int
    terminal_start_update_index: int
    terminal_end_update_index: int
    final_assistant_update_index: int
    last_native_tool_update_index: int | None
    session_event_count: int
    tool_completions: tuple[ReplayedNativeToolCompletion, ...]


def validate_session_precommit(precommit: Any) -> None:
    """Fail closed unless one precommit is exact, immutable, and self-consistent."""

    if not isinstance(precommit, GrokOperatorSessionPrecommit):
        raise GrokOperatorSessionReplayError("operator_session_precommit_type_invalid")
    if any(
        not isinstance(value, str) or not value or len(value) > 256
        for value in (
            precommit.expected_session_id,
            precommit.expected_request_id,
            precommit.expected_model_id,
            precommit.expected_reasoning_effort,
        )
    ):
        raise GrokOperatorSessionReplayError("operator_session_precommit_identity_invalid")
    if (
        type(precommit.expected_user_prompt) is not bytes
        or not precommit.expected_user_prompt
        or len(precommit.expected_user_prompt) > 5_000_000
        or _SHA256_RE.fullmatch(precommit.expected_user_prompt_sha256) is None
        or bytes_sha256(precommit.expected_user_prompt)
        != precommit.expected_user_prompt_sha256
    ):
        raise GrokOperatorSessionReplayError("operator_session_precommit_prompt_invalid")
    if (
        not isinstance(precommit.prompt_binding_mode, str)
        or precommit.prompt_binding_mode not in PROMPT_BINDING_MODES
    ):
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_prompt_binding_mode_invalid"
        )
    if (
        type(precommit.expected_chat_history_prefix) is not bytes
        or not precommit.expected_chat_history_prefix
        or len(precommit.expected_chat_history_prefix) > 50_000_000
        or not precommit.expected_chat_history_prefix.endswith(b"\n")
        or bytes_sha256(precommit.expected_chat_history_prefix)
        != precommit.expected_chat_history_prefix_sha256
    ):
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_chat_history_invalid"
        )
    if any(
        not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None
        for value in (
            precommit.expected_chat_history_prefix_sha256,
            precommit.expected_system_prompt_sha256,
            precommit.expected_prompt_context_sha256,
        )
    ):
        raise GrokOperatorSessionReplayError("operator_session_precommit_context_invalid")
    _validate_precommitted_chat_prefix(precommit)


def session_precommit_sha256(precommit: GrokOperatorSessionPrecommit) -> str:
    """Hash the closed precommit without serializing raw prompt text."""

    validate_session_precommit(precommit)
    return canonical_sha256(
        {
            "expected_session_id": precommit.expected_session_id,
            "expected_request_id": precommit.expected_request_id,
            "expected_model_id": precommit.expected_model_id,
            "expected_reasoning_effort": precommit.expected_reasoning_effort,
            "prompt_binding_mode": precommit.prompt_binding_mode,
            "expected_user_prompt_sha256": precommit.expected_user_prompt_sha256,
            "expected_chat_history_prefix_sha256": (
                precommit.expected_chat_history_prefix_sha256
            ),
            "expected_system_prompt_sha256": precommit.expected_system_prompt_sha256,
            "expected_prompt_context_sha256": precommit.expected_prompt_context_sha256,
        }
    )


def validate_operator_execution_facts(facts: Any) -> None:
    if not isinstance(facts, GrokOperatorExecutionFacts):
        raise GrokOperatorSessionReplayError("operator_execution_facts_type_invalid")
    if any(
        type(getattr(facts, field)) is not bool
        for field in (
            "result_truncated",
            "execution_deadline_reached",
            "transport_failure",
            "model_output_repaired",
        )
    ):
        raise GrokOperatorSessionReplayError("operator_execution_facts_invalid")


def operator_execution_facts_sha256(facts: GrokOperatorExecutionFacts) -> str:
    validate_operator_execution_facts(facts)
    return canonical_sha256(
        {
            "result_truncated": facts.result_truncated,
            "execution_deadline_reached": facts.execution_deadline_reached,
            "transport_failure": facts.transport_failure,
            "model_output_repaired": facts.model_output_repaired,
        }
    )


def thaw_raw_session_artifacts(
    artifacts: tuple[FrozenRawSessionArtifact, ...],
) -> dict[str, bytes]:
    """Return a fresh mapping after validating the retained closed source set."""

    if not isinstance(artifacts, tuple) or any(
        not isinstance(item, FrozenRawSessionArtifact) for item in artifacts
    ):
        raise GrokOperatorSessionReplayError("raw_session_artifacts_type_invalid")
    names = [item.name for item in artifacts]
    if names != sorted(RAW_SESSION_FILES) or len(names) != len(set(names)):
        raise GrokOperatorSessionReplayError("raw_session_artifacts_set_invalid")
    if any(type(item.content) is not bytes for item in artifacts):
        raise GrokOperatorSessionReplayError("raw_session_artifact_bytes_invalid")
    return {item.name: item.content for item in artifacts}


def _jsonl_records(raw: bytes, *, error: str) -> list[tuple[dict[str, Any], bytes]]:
    """Parse JSONL while retaining each exact record byte slice for hashing."""

    if type(raw) is not bytes:
        raise GrokOperatorSessionReplayError(error)
    records: list[tuple[dict[str, Any], bytes]] = []
    lines = raw.splitlines(keepends=True)
    if not lines or len(lines) > 1_000_000:
        raise GrokOperatorSessionReplayError(error)
    for line in lines:
        record_bytes = line.rstrip(b"\r\n")
        if not record_bytes or len(record_bytes) > 5_000_000:
            raise GrokOperatorSessionReplayError(error)
        try:
            value = strict_json_bytes(record_bytes, error=error)
        except CampaignValidationError as exc:
            raise GrokOperatorSessionReplayError(error) from exc
        if not isinstance(value, dict):
            raise GrokOperatorSessionReplayError(error)
        records.append((value, record_bytes))
    return records


def _single_text_content(value: Any) -> bool:
    valid = (
        isinstance(value, list)
        and len(value) == 1
        and isinstance(value[0], dict)
        and set(value[0]) == {"type", "text"}
        and value[0].get("type") == "text"
        and isinstance(value[0].get("text"), str)
        and bool(value[0]["text"])
    )
    if not valid:
        return False
    try:
        value[0]["text"].encode("utf-8")
    except UnicodeError:
        return False
    return True


def _validate_precommitted_chat_prefix(
    precommit: GrokOperatorSessionPrecommit,
) -> list[dict[str, Any]]:
    """Validate the exact five-row Grok user-visible prefix known pre-run."""

    records = _jsonl_records(
        precommit.expected_chat_history_prefix,
        error="operator_session_precommit_chat_history_invalid",
    )
    rows = [row for row, _ in records]
    if len(rows) != 5:
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_chat_history_invalid"
        )
    system, ordinary_user, project_instructions, system_reminder, bound_prompt = rows
    if (
        set(system) != {"type", "content"}
        or system.get("type") != "system"
        or not isinstance(system.get("content"), str)
        or not system["content"]
        or set(ordinary_user) != {"type", "content"}
        or ordinary_user.get("type") != "user"
        or not _single_text_content(ordinary_user.get("content"))
        or set(project_instructions) != {"type", "content", "synthetic_reason"}
        or project_instructions.get("type") != "user"
        or project_instructions.get("synthetic_reason") != "project_instructions"
        or not _single_text_content(project_instructions.get("content"))
        or set(system_reminder) != {"type", "content", "synthetic_reason"}
        or system_reminder.get("type") != "user"
        or system_reminder.get("synthetic_reason") != "system_reminder"
        or not _single_text_content(system_reminder.get("content"))
        or set(bound_prompt) != {"type", "content", "prompt_index"}
        or bound_prompt.get("type") != "user"
        or type(bound_prompt.get("prompt_index")) is not int
        or bound_prompt.get("prompt_index") != 0
        or not _single_text_content(bound_prompt.get("content"))
    ):
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_chat_history_invalid"
        )
    try:
        prompt = _prompt_bytes_from_chat_prefix(
            rows,
            binding_mode=precommit.prompt_binding_mode,
        )
        system_bytes = system["content"].encode("utf-8")
    except (CampaignValidationError, UnicodeError) as exc:
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_chat_history_invalid"
        ) from exc
    if (
        prompt != precommit.expected_user_prompt
        or bytes_sha256(system_bytes)
        != precommit.expected_system_prompt_sha256
    ):
        raise GrokOperatorSessionReplayError(
            "operator_session_precommit_chat_history_invalid"
        )
    return rows


def _prompt_bytes_from_chat_prefix(
    rows: Sequence[Mapping[str, Any]],
    *,
    binding_mode: str,
) -> bytes:
    """Reconstruct the exact prompt-file bytes from one closed prefix mode."""

    if len(rows) != 5:
        raise CampaignValidationError("raw_session_prompt_envelope_invalid")
    if binding_mode == "legacy_user_query_envelope_v1":
        return _extract_bound_prompt([dict(row) for row in rows])
    if binding_mode != "verbatim_prompt_row_v1":
        raise CampaignValidationError("raw_session_prompt_binding_mode_invalid")
    prompt_row = rows[4]
    content = prompt_row.get("content")
    if not _single_text_content(content):
        raise CampaignValidationError("raw_session_prompt_envelope_invalid")
    try:
        return content[0]["text"].encode("utf-8") + b"\n"
    except UnicodeError as exc:
        raise CampaignValidationError("raw_session_prompt_envelope_invalid") from exc


def _validate_model_chat_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    allowed_tool_names: frozenset[str],
    expected_model_id: str,
    expected_reasoning_effort: str,
) -> tuple[list[tuple[str, str, str, str]], Mapping[str, Any]]:
    """Validate post-prefix rows using the observed raw-session shape registry."""

    backend_ids: set[str] = set()
    provider_call_ids: set[str] = set()
    backend_calls: list[tuple[str, str, str, str]] = []
    assistants: list[Mapping[str, Any]] = []
    for row in rows:
        kind = row.get("type")
        if not isinstance(kind, str) or kind not in SUPPORTED_MODEL_CHAT_ROW_TYPES:
            raise GrokOperatorSessionReplayError(
                "raw_session_chat_history_model_kind_invalid"
            )
        if kind == "reasoning":
            summary = row.get("summary")
            reasoning_id = row.get("id")
            if (
                set(row)
                != {"type", "encrypted_content", "id", "status", "summary"}
                or not isinstance(row.get("encrypted_content"), str)
                or not row["encrypted_content"]
                or not isinstance(reasoning_id, str)
                or not reasoning_id
                or row.get("status") != "completed"
                or not isinstance(summary, list)
                or len(summary) > 1
                or any(
                    not isinstance(item, dict)
                    or set(item) != {"type", "text"}
                    or item.get("type") != "summary_text"
                    or not isinstance(item.get("text"), str)
                    for item in summary
                )
            ):
                raise GrokOperatorSessionReplayError(
                    "raw_session_chat_history_reasoning_invalid"
                )
            continue
        if kind == "backend_tool_call":
            call = row.get("kind")
            if (
                set(row) != {"type", "kind"}
                or not isinstance(call, dict)
                or set(call) != {"call_id", "id", "input", "name", "tool_type"}
                or call.get("tool_type") != "x_search"
                or not isinstance(call.get("call_id"), str)
                or not call["call_id"]
                or call["call_id"] in provider_call_ids
                or not isinstance(call.get("id"), str)
                or not call["id"]
                or call["id"] in backend_ids
                or not isinstance(call.get("input"), str)
                or not call["input"]
                or not isinstance(call.get("name"), str)
                or not call["name"]
            ):
                raise GrokOperatorSessionReplayError(
                    "raw_session_chat_history_backend_tool_invalid"
                )
            if call["name"] not in allowed_tool_names:
                raise GrokOperatorSessionReplayError("raw_session_unsupported_tool")
            try:
                arguments = _strict_json_text(
                    call["input"], error="raw_session_tool_arguments_invalid"
                )
            except (CampaignValidationError, UnicodeError) as exc:
                raise GrokOperatorSessionReplayError(str(exc)) from exc
            if not _tool_arguments_valid(arguments, call["name"]):
                raise GrokOperatorSessionReplayError(
                    "raw_session_tool_arguments_invalid"
                )
            backend_ids.add(call["id"])
            provider_call_ids.add(call["call_id"])
            backend_calls.append(
                (
                    call["id"],
                    call["call_id"],
                    call["name"],
                    json.dumps(
                        arguments,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
            )
            continue
        if kind == "assistant":
            if (
                set(row)
                != {
                    "type",
                    "content",
                    "model_fingerprint",
                    "model_id",
                    "reasoning_effort",
                }
                or not isinstance(row.get("content"), str)
                or not row["content"]
                or not isinstance(row.get("model_fingerprint"), str)
                or not row["model_fingerprint"]
                or row.get("model_id") != expected_model_id
                or row.get("reasoning_effort") != expected_reasoning_effort
            ):
                raise GrokOperatorSessionReplayError(
                    "raw_session_chat_history_assistant_invalid"
                )
            assistants.append(row)
            continue
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_history_model_kind_invalid"
        )
    if len(assistants) != 1 or not rows or rows[-1] is not assistants[0]:
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_history_assistant_final_invalid"
        )
    return backend_calls, assistants[0]


def _validate_session_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    session_id: str,
    model_id: str,
    user_visible_prefix_row_count: int,
) -> None:
    """Validate the observed Grok event ledger as a closed ordered registry."""

    if len(rows) < 6:
        raise GrokOperatorSessionReplayError("raw_session_events_shape_invalid")
    if any(
        not isinstance(row.get("type"), str)
        or row.get("type") not in SUPPORTED_SESSION_EVENT_TYPES
        for row in rows
    ):
        raise GrokOperatorSessionReplayError("raw_session_event_kind_invalid")
    started, loop_started, waiting, first_token, *tail = rows
    ended = tail[-1]
    phases = [waiting, *tail[:-1]]
    if (
        set(started)
        != {
            "type",
            "ts",
            "turn_number",
            "session_id",
            "model_id",
            "yolo_mode",
            "conversation_message_count",
            "session_relationship",
            "schema_version",
        }
        or started.get("type") != "turn_started"
        or not isinstance(started.get("ts"), str)
        or not started["ts"]
        or type(started.get("turn_number")) is not int
        or started.get("turn_number") != 0
        or started.get("session_id") != session_id
        or started.get("model_id") != model_id
        or type(started.get("yolo_mode")) is not bool
        or started.get("yolo_mode") is not False
        or type(started.get("conversation_message_count")) is not int
        or started.get("conversation_message_count")
        != user_visible_prefix_row_count
        or started.get("session_relationship") != "primary"
        or started.get("schema_version") != "1.0"
    ):
        raise GrokOperatorSessionReplayError("raw_session_turn_started_invalid")
    if (
        set(loop_started) != {"type", "ts", "loop_index"}
        or loop_started.get("type") != "loop_started"
        or not isinstance(loop_started.get("ts"), str)
        or not loop_started["ts"]
        or type(loop_started.get("loop_index")) is not int
        or loop_started.get("loop_index") != 0
    ):
        raise GrokOperatorSessionReplayError("raw_session_loop_started_invalid")
    if (
        set(waiting) != {"type", "ts", "phase"}
        or waiting.get("type") != "phase_changed"
        or not isinstance(waiting.get("ts"), str)
        or not waiting["ts"]
        or waiting.get("phase") != "waiting_for_model"
    ):
        raise GrokOperatorSessionReplayError("raw_session_initial_phase_invalid")
    if (
        set(first_token) != {"type", "ts"}
        or first_token.get("type") != "first_token"
        or not isinstance(first_token.get("ts"), str)
        or not first_token["ts"]
    ):
        raise GrokOperatorSessionReplayError("raw_session_first_token_invalid")
    if (
        set(ended) != {"type", "ts", "outcome"}
        or ended.get("type") != "turn_ended"
        or not isinstance(ended.get("ts"), str)
        or not ended["ts"]
        or ended.get("outcome") != "completed"
    ):
        raise GrokOperatorSessionReplayError("raw_session_turn_ended_invalid")
    if any(
        set(row) != {"type", "ts", "phase"}
        or row.get("type") != "phase_changed"
        or not isinstance(row.get("ts"), str)
        or not row["ts"]
        or not isinstance(row.get("phase"), str)
        or row.get("phase") not in {"streaming_reasoning", "streaming_text"}
        for row in phases[1:]
    ) or not any(row.get("phase") == "streaming_text" for row in phases[1:]):
        raise GrokOperatorSessionReplayError("raw_session_phase_registry_invalid")
    timestamps = [row.get("ts") for row in rows]
    if any(not isinstance(value, str) for value in timestamps) or timestamps != sorted(
        timestamps
    ):
        raise GrokOperatorSessionReplayError("raw_session_event_timestamp_order_invalid")


def _manifest_sha256(source_hashes: tuple[tuple[str, str], ...]) -> str:
    return canonical_sha256(
        {
            "manifest_version": "x.grok.raw_session.manifest.v1",
            "source_artifact_sha256s": [
                {"name": name, "sha256": digest} for name, digest in source_hashes
            ],
        }
    )


def _event_metadata_valid(
    metadata: Any,
    *,
    expected_keys: frozenset[str],
    expected_update_type: str,
    request_id: str,
    session_id: str,
    seen_event_ids: set[str],
    event_timestamps: list[int],
) -> bool:
    """Validate the exact shared Grok event metadata without coercing bools."""

    if not isinstance(metadata, dict) or set(metadata) != expected_keys:
        return False
    if (
        metadata.get("promptId") != request_id
        or metadata.get("updateType") != expected_update_type
        or any(
            type(metadata.get(field)) is not int or metadata[field] < 0
            for field in _EVENT_METADATA_INTEGER_KEYS
        )
    ):
        return False
    event_id = metadata.get("eventId")
    if (
        not isinstance(event_id, str)
        or not event_id.startswith(f"{session_id}-")
        or event_id in seen_event_ids
    ):
        return False
    seen_event_ids.add(event_id)
    event_timestamps.append(metadata["agentTimestampMs"])
    return True


def _json_object_slices(raw: bytes) -> list[tuple[dict[str, Any], int, int]]:
    """Return every decodable object with its exact UTF-8 byte range.

    Grok may emit non-terminal progress JSON before the final answer.  The lane
    therefore selects by the result contract, not by wrapper ``structuredOutput``
    or by assuming that only one JSON object exists in the assistant stream.
    """

    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise GrokOperatorSessionReplayError("assistant_output_utf8_invalid") from exc
    _preflight_assistant_json_structure(text)
    decoder = json.JSONDecoder()
    slices: list[tuple[dict[str, Any], int, int]] = []
    seen_ranges: set[tuple[int, int]] = set()
    for match in re.finditer(r"\{", text):
        try:
            value, end = decoder.raw_decode(text, match.start())
        except json.JSONDecodeError:
            continue
        except RecursionError as exc:
            raise GrokOperatorSessionReplayError(
                "assistant_output_json_recursion_invalid"
            ) from exc
        if not isinstance(value, dict):
            continue
        start_byte = len(text[: match.start()].encode("utf-8"))
        end_byte = len(text[:end].encode("utf-8"))
        try:
            strict_value = strict_json_bytes(
                raw[start_byte:end_byte],
                error="assistant_output_terminal_json_invalid",
            )
        except CampaignValidationError:
            continue
        except RecursionError as exc:
            raise GrokOperatorSessionReplayError(
                "assistant_output_json_recursion_invalid"
            ) from exc
        if not isinstance(strict_value, dict):
            continue
        if (start_byte, end_byte) in seen_ranges:
            continue
        seen_ranges.add((start_byte, end_byte))
        slices.append((strict_value, start_byte, end_byte))
    return slices


def _preflight_assistant_json_structure(text: str) -> None:
    """Bound JSON-like structure before Python's recursive decoder sees it.

    Assistant output may contain prose and more than one JSON object.  This
    linear lexical pass ignores text outside a container, honors JSON string
    escaping inside a container, and counts containers, strings, and scalar
    tokens across the complete assistant stream.  Syntax remains the strict
    decoder's responsibility after the resource envelope is known to be safe.
    """

    depth = 0
    nodes = 0
    in_string = False
    escaped = False
    scalar_token_open = False
    for character in text:
        if depth == 0:
            if character not in "[{":
                continue
            depth = 1
            nodes += 1
            scalar_token_open = False
        elif in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        elif character == '"':
            in_string = True
            scalar_token_open = False
            nodes += 1
        elif character in "[{":
            depth += 1
            scalar_token_open = False
            nodes += 1
        elif character in "]}":
            depth -= 1
            scalar_token_open = False
        elif character in ",:" or character.isspace():
            scalar_token_open = False
        elif not scalar_token_open:
            scalar_token_open = True
            nodes += 1

        if depth > ASSISTANT_JSON_MAX_DEPTH:
            raise GrokOperatorSessionReplayError(
                "assistant_output_json_depth_budget_exceeded"
            )
        if nodes > ASSISTANT_JSON_MAX_NODES:
            raise GrokOperatorSessionReplayError(
                "assistant_output_json_node_budget_exceeded"
            )


def replay_grok_operator_session(
    raw_session_files: Mapping[str, bytes],
    *,
    session_precommit: GrokOperatorSessionPrecommit,
    allowed_tool_names: frozenset[str],
    expected_terminal: Mapping[str, Any] | None = None,
    terminal_validator: Callable[[Any], Sequence[str]] | None = None,
    expected_terminal_text: str | None = None,
) -> GrokOperatorSessionReplay:
    """Replay one complete Grok session from immutable raw artifacts.

    Every tool-shaped update is accounted for, starts and completions pair by
    call id, and unsupported tool families fail closed.  The default path binds
    one unique terminal JSON object.  ``expected_terminal_text`` is the narrow
    literal-text alternative: it requires the complete assistant surface to be
    exactly those bytes and every assistant chunk to follow all native tools.
    """

    validate_session_precommit(session_precommit)
    if not isinstance(raw_session_files, Mapping) or set(raw_session_files) != RAW_SESSION_FILES:
        raise GrokOperatorSessionReplayError("raw_session_sources_invalid")
    if any(type(value) is not bytes for value in raw_session_files.values()):
        raise GrokOperatorSessionReplayError("raw_session_sources_invalid")
    if (
        not isinstance(allowed_tool_names, frozenset)
        or not allowed_tool_names
        or any(not isinstance(name, str) or not name for name in allowed_tool_names)
    ):
        raise GrokOperatorSessionReplayError("allowed_tool_registry_invalid")
    if expected_terminal_text is not None:
        if (
            expected_terminal is not None
            or terminal_validator is not None
            or not isinstance(expected_terminal_text, str)
            or not expected_terminal_text
            or expected_terminal_text.endswith("\n")
            or len(expected_terminal_text.encode("utf-8")) > 5_000_000
        ):
            raise GrokOperatorSessionReplayError("expected_terminal_text_invalid")

    sources = {name: bytes(raw_session_files[name]) for name in RAW_SESSION_FILES}
    frozen = tuple(
        FrozenRawSessionArtifact(name=name, content=sources[name])
        for name in sorted(sources)
    )
    source_hashes = tuple((item.name, bytes_sha256(item.content)) for item in frozen)
    transcript_sha256 = _manifest_sha256(source_hashes)

    try:
        summary = strict_json_bytes(sources["summary.json"], error="raw_session_summary_invalid")
        prompt_context = strict_json_bytes(
            sources["prompt_context.json"], error="raw_session_prompt_context_invalid"
        )
    except CampaignValidationError as exc:
        raise GrokOperatorSessionReplayError(str(exc)) from exc
    if not isinstance(summary, dict) or not isinstance(summary.get("info"), dict):
        raise GrokOperatorSessionReplayError("raw_session_summary_invalid")
    if not isinstance(prompt_context, dict) or not sources["system_prompt.txt"]:
        raise GrokOperatorSessionReplayError("raw_session_execution_context_invalid")
    session_id = summary["info"].get("id")
    request_id = summary.get("request_id")
    model_id = summary.get("current_model_id")
    reasoning_effort = summary.get("reasoning_effort")
    if (
        type(summary.get("chat_format_version")) is not int
        or summary.get("chat_format_version") != 1
    ):
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_format_version_invalid"
        )
    if not all(
        isinstance(value, str) and value
        for value in (session_id, request_id, model_id, reasoning_effort)
    ):
        raise GrokOperatorSessionReplayError("raw_session_summary_identity_invalid")
    if (
        session_id != session_precommit.expected_session_id
        or request_id != session_precommit.expected_request_id
        or model_id != session_precommit.expected_model_id
        or reasoning_effort != session_precommit.expected_reasoning_effort
    ):
        raise GrokOperatorSessionReplayError("raw_session_summary_identity_mismatch")

    chat_records = _jsonl_records(
        sources["chat_history.jsonl"], error="raw_session_chat_history_invalid"
    )
    if (
        type(summary.get("num_chat_messages")) is not int
        or summary.get("num_chat_messages") != len(chat_records)
    ):
        raise GrokOperatorSessionReplayError(
            "raw_session_summary_chat_message_count_mismatch"
        )
    prefix_records = _jsonl_records(
        session_precommit.expected_chat_history_prefix,
        error="operator_session_precommit_chat_history_invalid",
    )
    if (
        not sources["chat_history.jsonl"].startswith(
            session_precommit.expected_chat_history_prefix
        )
        or chat_records[: len(prefix_records)] != prefix_records
    ):
        raise GrokOperatorSessionReplayError("raw_session_chat_history_prefix_mismatch")
    chat_rows = [row for row, _ in chat_records]
    if any(
        row.get("type") in {"system", "user"}
        for row in chat_rows[len(prefix_records) :]
    ):
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_history_user_prefix_boundary_invalid"
        )
    model_backend_calls, final_chat_assistant = _validate_model_chat_rows(
        chat_rows[len(prefix_records) :],
        allowed_tool_names=allowed_tool_names,
        expected_model_id=model_id,
        expected_reasoning_effort=reasoning_effort,
    )
    try:
        _validate_bound_system_message(chat_rows, sources["system_prompt.txt"])
        user_prompt = _prompt_bytes_from_chat_prefix(
            chat_rows[: len(prefix_records)],
            binding_mode=session_precommit.prompt_binding_mode,
        )
    except CampaignValidationError as exc:
        raise GrokOperatorSessionReplayError(str(exc)) from exc
    if (
        user_prompt != session_precommit.expected_user_prompt
        or bytes_sha256(user_prompt) != session_precommit.expected_user_prompt_sha256
    ):
        raise GrokOperatorSessionReplayError("raw_session_prompt_hash_mismatch")
    if (
        bytes_sha256(sources["system_prompt.txt"])
        != session_precommit.expected_system_prompt_sha256
        or bytes_sha256(sources["prompt_context.json"])
        != session_precommit.expected_prompt_context_sha256
    ):
        raise GrokOperatorSessionReplayError("raw_session_execution_context_hash_mismatch")
    working_directory = prompt_context.get("working_directory")
    if (
        not isinstance(working_directory, str)
        or not working_directory
        or summary["info"].get("cwd") != working_directory
    ):
        raise GrokOperatorSessionReplayError("raw_session_working_directory_mismatch")

    event_records = _jsonl_records(sources["events.jsonl"], error="raw_session_events_invalid")
    event_rows = [row for row, _ in event_records]
    _validate_session_events(
        event_rows,
        session_id=session_id,
        model_id=model_id,
        user_visible_prefix_row_count=len(prefix_records) - 1,
    )

    update_records = _jsonl_records(sources["updates.jsonl"], error="raw_session_update_invalid")
    if summary.get("num_messages") != len(update_records):
        raise GrokOperatorSessionReplayError("raw_session_summary_message_count_mismatch")

    starts: dict[str, _ReplayedNativeToolStart] = {}
    completions: dict[str, ReplayedNativeToolCompletion] = {}
    provider_call_ids: set[str] = set()
    assistant_chunks: list[str] = []
    assistant_chunk_ids: list[int] = []
    session_event_ids: set[str] = set()
    session_event_timestamps: list[int] = []
    assistant_timestamps: list[int] = []
    assistant_indices: list[int] = []
    assistant_chunk_ranges: list[tuple[int, int]] = []
    assistant_cursor = 0
    tool_indices: list[int] = []
    user_message_count = 0
    for update_index, (row, record_bytes) in enumerate(update_records):
        params = row.get("params")
        update = params.get("update") if isinstance(params, dict) else None
        if not isinstance(params, dict) or not isinstance(update, dict):
            raise GrokOperatorSessionReplayError("raw_session_update_shape_invalid")
        if params.get("sessionId") != session_id:
            raise GrokOperatorSessionReplayError("raw_session_update_session_mismatch")
        kind = update.get("sessionUpdate")
        if not isinstance(kind, str) or kind not in SUPPORTED_SESSION_UPDATE_KINDS:
            raise GrokOperatorSessionReplayError("raw_session_update_kind_invalid")
        if (update_index == 0) != (kind == "user_message_chunk"):
            raise GrokOperatorSessionReplayError("raw_session_user_turn_order_invalid")
        if kind == "user_message_chunk":
            user_message_count += 1
            metadata = update.get("_meta")
            outer_metadata = params.get("_meta")
            content = update.get("content")
            event_id = (
                outer_metadata.get("eventId")
                if isinstance(outer_metadata, dict)
                else None
            )
            if (
                set(params) != {"sessionId", "update", "_meta"}
                or set(update) != {"sessionUpdate", "content", "_meta"}
                or not isinstance(metadata, dict)
                or set(metadata) != {"modelId", "promptIndex"}
                or metadata.get("modelId") != model_id
                or type(metadata.get("promptIndex")) is not int
                or metadata.get("promptIndex") != 0
                or not isinstance(content, dict)
                or set(content) != {"type", "text"}
                or content.get("type") != "text"
                or not isinstance(content.get("text"), str)
                or not content["text"]
                or not isinstance(outer_metadata, dict)
                or set(outer_metadata) != {"eventId", "agentTimestampMs"}
                or type(outer_metadata.get("agentTimestampMs")) is not int
                or outer_metadata["agentTimestampMs"] < 0
                or not isinstance(event_id, str)
                or not event_id.startswith(f"{session_id}-")
                or event_id in session_event_ids
            ):
                raise GrokOperatorSessionReplayError("raw_session_user_model_binding_invalid")
            session_event_ids.add(event_id)
            session_event_timestamps.append(outer_metadata["agentTimestampMs"])
            continue
        if kind in {"agent_message_chunk", "agent_thought_chunk"}:
            metadata = params.get("_meta")
            content = update.get("content")
            expected_update_type = (
                "AgentMessageChunk"
                if kind == "agent_message_chunk"
                else "AgentThoughtChunk"
            )
            if (
                set(params) != {"sessionId", "update", "_meta"}
                or set(update) != {"sessionUpdate", "content"}
                or user_message_count != 1
                or not _event_metadata_valid(
                    metadata,
                    expected_keys=_EVENT_METADATA_BASE_KEYS | {"chunkId"},
                    expected_update_type=expected_update_type,
                    request_id=request_id,
                    session_id=session_id,
                    seen_event_ids=session_event_ids,
                    event_timestamps=session_event_timestamps,
                )
                or not isinstance(metadata.get("chunkId"), int)
                or isinstance(metadata.get("chunkId"), bool)
                or not isinstance(content, dict)
                or set(content) != {"type", "text"}
                or content.get("type") != "text"
                or not isinstance(content.get("text"), str)
                or not content["text"]
            ):
                raise GrokOperatorSessionReplayError("raw_session_assistant_chunk_invalid")
            try:
                chunk_bytes = content["text"].encode("utf-8")
            except UnicodeError as exc:
                raise GrokOperatorSessionReplayError("assistant_output_utf8_invalid") from exc
            if kind == "agent_thought_chunk":
                continue
            assistant_chunks.append(content["text"])
            assistant_chunk_ids.append(metadata["chunkId"])
            assistant_timestamps.append(metadata["agentTimestampMs"])
            assistant_indices.append(update_index)
            assistant_chunk_ranges.append((assistant_cursor, assistant_cursor + len(chunk_bytes)))
            assistant_cursor += len(chunk_bytes)
            continue
        if kind == "tool_call":
            metadata = params.get("_meta")
            call_id = update.get("toolCallId")
            update_metadata = update.get("_meta")
            update_params = metadata.get("updateParams") if isinstance(metadata, dict) else None
            title = update.get("title")
            if (
                set(params) != {"sessionId", "update", "_meta"}
                or set(update)
                != {
                    "sessionUpdate",
                    "toolCallId",
                    "title",
                    "kind",
                    "status",
                    "rawInput",
                    "_meta",
                }
                or user_message_count != 1
                or not _event_metadata_valid(
                    metadata,
                    expected_keys=_EVENT_METADATA_BASE_KEYS | {"updateParams"},
                    expected_update_type="ToolCall",
                    request_id=request_id,
                    session_id=session_id,
                    seen_event_ids=session_event_ids,
                    event_timestamps=session_event_timestamps,
                )
                or not isinstance(update_params, dict)
                or set(update_params) != {"toolCallId", "title", "kind", "status"}
                or update_params.get("toolCallId") != call_id
                or update_params.get("title") != title
                or update_params.get("kind") != "Search"
                or update_params.get("status") != "InProgress"
                or update.get("status") != "in_progress"
                or update.get("kind") != "search"
                or update.get("rawInput") != {"backend": True, "variant": "XSearch"}
                or type(update.get("rawInput", {}).get("backend")) is not bool
                or not isinstance(update_metadata, dict)
                or set(update_metadata) != {"backend"}
                or type(update_metadata.get("backend")) is not bool
                or update_metadata.get("backend") is not True
                or not isinstance(call_id, str)
                or not call_id
                or not isinstance(title, str)
                or not title
                or call_id in starts
            ):
                raise GrokOperatorSessionReplayError("raw_session_started_call_invalid")
            starts[call_id] = _ReplayedNativeToolStart(
                call_id=call_id,
                title=title,
                update_index=update_index,
                event_bytes=record_bytes,
            )
            tool_indices.append(update_index)
            continue
        if kind == "tool_call_update":
            metadata = params.get("_meta")
            raw_output = update.get("rawOutput")
            call_id = update.get("toolCallId")
            update_params = metadata.get("updateParams") if isinstance(metadata, dict) else None
            title = update.get("title")
            if (
                set(params) != {"sessionId", "update", "_meta"}
                or set(update)
                != {"sessionUpdate", "toolCallId", "status", "title", "rawOutput"}
                or user_message_count != 1
                or not _event_metadata_valid(
                    metadata,
                    expected_keys=_EVENT_METADATA_BASE_KEYS | {"updateParams"},
                    expected_update_type="ToolCallUpdate",
                    request_id=request_id,
                    session_id=session_id,
                    seen_event_ids=session_event_ids,
                    event_timestamps=session_event_timestamps,
                )
                or not isinstance(update_params, dict)
                or set(update_params) != {"toolCallId", "status"}
                or update_params.get("toolCallId") != call_id
                or update_params.get("status") != "Completed"
                or update.get("status") != "completed"
                or not isinstance(title, str)
                or not title
                or not isinstance(raw_output, dict)
                or set(raw_output) != {"call_id", "id", "input", "name"}
                or call_id != raw_output.get("id")
            ):
                raise GrokOperatorSessionReplayError("raw_session_completed_call_invalid")
            provider_call_id = raw_output["call_id"]
            tool_name = raw_output["name"]
            if not isinstance(tool_name, str) or tool_name not in allowed_tool_names:
                raise GrokOperatorSessionReplayError("raw_session_unsupported_tool")
            if (
                not isinstance(call_id, str)
                or call_id not in starts
                or call_id in completions
                or title != starts[call_id].title
                or not isinstance(provider_call_id, str)
                or not provider_call_id
                or provider_call_id in provider_call_ids
                or not isinstance(raw_output["input"], str)
            ):
                raise GrokOperatorSessionReplayError("raw_session_completed_call_identity_invalid")
            try:
                arguments = _strict_json_text(
                    raw_output["input"], error="raw_session_tool_arguments_invalid"
                )
            except (CampaignValidationError, UnicodeError) as exc:
                raise GrokOperatorSessionReplayError(str(exc)) from exc
            if not _tool_arguments_valid(arguments, tool_name):
                raise GrokOperatorSessionReplayError("raw_session_tool_arguments_invalid")
            query = arguments.get("query")
            if query is not None and not isinstance(query, str):
                raise GrokOperatorSessionReplayError("raw_session_tool_query_invalid")
            start = starts[call_id]
            completions[call_id] = ReplayedNativeToolCompletion(
                call_id=call_id,
                provider_call_id=provider_call_id,
                tool_name=tool_name,
                arguments_json=json.dumps(
                    arguments,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                query=query,
                started_update_index=start.update_index,
                completed_update_index=update_index,
                start_event_sha256=hashlib.sha256(start.event_bytes).hexdigest(),
                completion_event_sha256=hashlib.sha256(record_bytes).hexdigest(),
            )
            provider_call_ids.add(provider_call_id)
            tool_indices.append(update_index)
            continue

    if user_message_count != 1:
        raise GrokOperatorSessionReplayError("raw_session_user_message_count_invalid")
    if session_event_timestamps != sorted(session_event_timestamps):
        raise GrokOperatorSessionReplayError("raw_session_event_timestamp_order_invalid")
    if set(starts) != set(completions):
        raise GrokOperatorSessionReplayError("raw_session_terminal_pairing_invalid")
    ordered_completions = tuple(
        sorted(completions.values(), key=lambda item: item.started_update_index)
    )
    expected_backend_calls = [
        (
            item.call_id,
            item.provider_call_id,
            item.tool_name,
            item.arguments_json,
        )
        for item in ordered_completions
    ]
    if model_backend_calls != expected_backend_calls:
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_history_backend_tool_binding_mismatch"
        )
    if (
        not assistant_chunks
        or assistant_chunk_ids != sorted(assistant_chunk_ids)
        or len(assistant_chunk_ids) != len(set(assistant_chunk_ids))
        or assistant_timestamps != sorted(assistant_timestamps)
    ):
        raise GrokOperatorSessionReplayError("raw_session_assistant_chunk_order_invalid")
    assistant_output = "".join(assistant_chunks).encode("utf-8") + b"\n"
    last_tool_index = max(tool_indices) if tool_indices else None
    if expected_terminal_text is not None:
        if last_tool_index is not None and any(index <= last_tool_index for index in assistant_indices):
            raise GrokOperatorSessionReplayError(
                "raw_session_terminal_assistant_causality_invalid"
            )
        terminal = expected_terminal_text
        terminal_text = expected_terminal_text
        terminal_start_byte_offset = 0
        terminal_end_byte_offset_exclusive = len(expected_terminal_text.encode("utf-8"))
        terminal_start_update_index = assistant_indices[0]
        terminal_end_update_index = assistant_indices[-1]
        try:
            observed_terminal_text = assistant_output[:-1].decode("utf-8")
        except UnicodeError as exc:
            raise GrokOperatorSessionReplayError(
                "raw_session_chat_history_assistant_binding_invalid"
            ) from exc
        if observed_terminal_text != expected_terminal_text:
            raise GrokOperatorSessionReplayError("assistant_output_result_binding_mismatch")
    else:
        schema_valid_candidates: list[tuple[dict[str, Any], int, int, int, int]] = []
        for candidate, start_byte, end_byte in _json_object_slices(assistant_output):
            if terminal_validator is not None and terminal_validator(candidate):
                continue
            start_chunk_index = next(
                (
                    index
                    for index, (start, end) in enumerate(assistant_chunk_ranges)
                    if start <= start_byte < end
                ),
                None,
            )
            end_chunk_index = next(
                (
                    index
                    for index, (start, end) in enumerate(assistant_chunk_ranges)
                    if start <= end_byte - 1 < end
                ),
                None,
            )
            if start_chunk_index is None or end_chunk_index is None:
                raise GrokOperatorSessionReplayError(
                    "assistant_output_terminal_json_chunk_mapping_invalid"
                )
            schema_valid_candidates.append(
                (
                    candidate,
                    start_byte,
                    end_byte,
                    assistant_indices[start_chunk_index],
                    assistant_indices[end_chunk_index],
                )
            )
        candidates_after_tools = [
            candidate
            for candidate in schema_valid_candidates
            if last_tool_index is None or candidate[3] > last_tool_index
        ]
        if last_tool_index is not None and any(
            candidate[3] <= last_tool_index for candidate in schema_valid_candidates
        ):
            raise GrokOperatorSessionReplayError(
                "raw_session_terminal_assistant_causality_invalid"
            )
        if len(candidates_after_tools) != 1:
            if not candidates_after_tools and schema_valid_candidates:
                raise GrokOperatorSessionReplayError(
                    "raw_session_terminal_assistant_causality_invalid"
                )
            raise GrokOperatorSessionReplayError(
                "assistant_output_schema_valid_terminal_count_invalid"
            )
        (
            terminal,
            terminal_start_byte_offset,
            terminal_end_byte_offset_exclusive,
            terminal_start_update_index,
            terminal_end_update_index,
        ) = candidates_after_tools[0]
        if expected_terminal is not None:
            try:
                expected_terminal_sha256 = canonical_sha256(dict(expected_terminal))
            except (TypeError, ValueError) as exc:
                raise GrokOperatorSessionReplayError(
                    "assistant_output_result_binding_invalid"
                ) from exc
            if canonical_sha256(terminal) != expected_terminal_sha256:
                raise GrokOperatorSessionReplayError("assistant_output_result_binding_mismatch")
        if assistant_output[terminal_end_byte_offset_exclusive:].strip():
            raise GrokOperatorSessionReplayError("assistant_output_after_terminal_invalid")
        try:
            terminal_text = assistant_output[
                terminal_start_byte_offset:terminal_end_byte_offset_exclusive
            ].decode("utf-8")
        except UnicodeError as exc:
            raise GrokOperatorSessionReplayError(
                "raw_session_chat_history_assistant_binding_invalid"
            ) from exc
    chat_terminal_matches = (
        final_chat_assistant["content"] == terminal_text
        if expected_terminal_text is not None
        else final_chat_assistant["content"].strip() == terminal_text.strip()
    )
    if not chat_terminal_matches:
        raise GrokOperatorSessionReplayError(
            "raw_session_chat_history_assistant_binding_mismatch"
        )
    if terminal_end_update_index != assistant_indices[-1]:
        raise GrokOperatorSessionReplayError("assistant_update_after_terminal_invalid")
    if terminal_end_update_index != len(update_records) - 1:
        raise GrokOperatorSessionReplayError("raw_session_update_after_terminal_invalid")
    return GrokOperatorSessionReplay(
        source_artifacts=frozen,
        source_artifact_sha256s=source_hashes,
        transcript_sha256=transcript_sha256,
        session_precommit_sha256=session_precommit_sha256(session_precommit),
        raw_session_shape_registry_version=RAW_SESSION_SHAPE_REGISTRY_VERSION,
        session_id=session_id,
        request_id=request_id,
        model_id=model_id,
        system_prompt_sha256=bytes_sha256(sources["system_prompt.txt"]),
        prompt_context_sha256=bytes_sha256(sources["prompt_context.json"]),
        user_prompt_sha256=bytes_sha256(user_prompt),
        terminal=terminal,
        terminal_sha256=canonical_sha256(terminal),
        terminal_start_byte_offset=terminal_start_byte_offset,
        terminal_end_byte_offset_exclusive=terminal_end_byte_offset_exclusive,
        terminal_start_update_index=terminal_start_update_index,
        terminal_end_update_index=terminal_end_update_index,
        final_assistant_update_index=assistant_indices[-1],
        last_native_tool_update_index=last_tool_index,
        session_event_count=len(event_records) + len(update_records) + len(chat_records),
        tool_completions=ordered_completions,
    )
