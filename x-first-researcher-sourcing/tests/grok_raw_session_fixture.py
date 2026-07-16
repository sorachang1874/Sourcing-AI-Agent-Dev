"""Synthetic six-file Grok session artifacts for provider-free contract tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from x_first.grok_operator_session_replay import GrokOperatorSessionPrecommit

FIXTURE_SESSION_ID = "fixture-session-0001"
FIXTURE_REQUEST_ID = "fixture-request-0001"
FIXTURE_MODEL_ID = "grok-fixture"
FIXTURE_REASONING_EFFORT = "high"
FIXTURE_PROMPT_BINDING_MODE = "verbatim_prompt_row_v1"
FIXTURE_USER_PROMPT = "fixture"
FIXTURE_SYSTEM_PROMPT = b"Synthetic system prompt.\n"
FIXTURE_WORKING_DIRECTORY = "/synthetic"
FIXTURE_PROMPT_MODE = "synthetic"


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _chat_history_prefix_bytes(
    *,
    system_prompt: bytes,
    user_prompt_text: str,
    extra_chat_rows: Sequence[Mapping[str, Any]],
    prompt_binding_mode: str,
) -> bytes:
    if prompt_binding_mode == "verbatim_prompt_row_v1":
        bound_prompt_text = user_prompt_text
    elif prompt_binding_mode == "legacy_user_query_envelope_v1":
        bound_prompt_text = f"<user_query>\n{user_prompt_text}\n</user_query>"
    else:
        bound_prompt_text = user_prompt_text
    rows: list[Mapping[str, Any]] = [
        {"type": "system", "content": system_prompt.decode("utf-8")},
        {
            "type": "user",
            "content": [{"type": "text", "text": "Synthetic user context."}],
        },
        {
            "type": "user",
            "synthetic_reason": "project_instructions",
            "content": [{"type": "text", "text": "Synthetic project instructions."}],
        },
        {
            "type": "user",
            "synthetic_reason": "system_reminder",
            "content": [{"type": "text", "text": "Synthetic system reminder."}],
        },
        *extra_chat_rows,
        {
            "type": "user",
            "prompt_index": 0,
            "content": [
                {
                    "type": "text",
                    "text": bound_prompt_text,
                }
            ],
        },
    ]
    return b"".join(_json_bytes(row) for row in rows)


def fixture_grok_session_precommit(
    *,
    session_id: str = FIXTURE_SESSION_ID,
    request_id: str = FIXTURE_REQUEST_ID,
    model_id: str = FIXTURE_MODEL_ID,
    reasoning_effort: str = FIXTURE_REASONING_EFFORT,
    prompt_binding_mode: str = FIXTURE_PROMPT_BINDING_MODE,
    user_prompt_text: str = FIXTURE_USER_PROMPT,
    system_prompt: bytes = FIXTURE_SYSTEM_PROMPT,
    working_directory: str = FIXTURE_WORKING_DIRECTORY,
    prompt_mode: str = FIXTURE_PROMPT_MODE,
    extra_chat_rows: Sequence[Mapping[str, Any]] = (),
) -> GrokOperatorSessionPrecommit:
    """Return the fixture owner's values, independently of captured raw bytes."""

    user_prompt = (user_prompt_text + "\n").encode("utf-8")
    prompt_context = _json_bytes(
        {"working_directory": working_directory, "prompt_mode": prompt_mode}
    )
    chat_history_prefix = _chat_history_prefix_bytes(
        system_prompt=system_prompt,
        user_prompt_text=user_prompt_text,
        extra_chat_rows=extra_chat_rows,
        prompt_binding_mode=prompt_binding_mode,
    )
    return GrokOperatorSessionPrecommit(
        expected_session_id=session_id,
        expected_request_id=request_id,
        expected_model_id=model_id,
        expected_reasoning_effort=reasoning_effort,
        prompt_binding_mode=prompt_binding_mode,
        expected_user_prompt=user_prompt,
        expected_user_prompt_sha256=hashlib.sha256(user_prompt).hexdigest(),
        expected_chat_history_prefix=chat_history_prefix,
        expected_chat_history_prefix_sha256=hashlib.sha256(
            chat_history_prefix
        ).hexdigest(),
        expected_system_prompt_sha256=hashlib.sha256(system_prompt).hexdigest(),
        expected_prompt_context_sha256=hashlib.sha256(prompt_context).hexdigest(),
    )


def raw_grok_session(
    terminal: Mapping[str, Any],
    *,
    calls: Sequence[tuple[str, Mapping[str, Any]]],
    session_id: str = FIXTURE_SESSION_ID,
    request_id: str = FIXTURE_REQUEST_ID,
    model_id: str = FIXTURE_MODEL_ID,
    reasoning_effort: str = FIXTURE_REASONING_EFFORT,
    prompt_binding_mode: str = FIXTURE_PROMPT_BINDING_MODE,
    user_prompt_text: str = FIXTURE_USER_PROMPT,
    system_prompt: bytes = FIXTURE_SYSTEM_PROMPT,
    working_directory: str = FIXTURE_WORKING_DIRECTORY,
    prompt_mode: str = FIXTURE_PROMPT_MODE,
    extra_chat_rows: Sequence[Mapping[str, Any]] = (),
    thought_texts: Sequence[str] = ("Synthetic planning thought.",),
    extra_session_update_kinds: Sequence[str] = (),
    extra_event_rows: Sequence[Mapping[str, Any]] = (),
    start_update_overrides: Mapping[str, Any] | None = None,
    progress_payloads: Sequence[Mapping[str, Any]] = (),
    additional_schema_valid_terminals: Sequence[Mapping[str, Any]] = (),
    schema_valid_terminal_before_tools: bool = False,
    terminal_before_tools: bool = False,
    omit_completion_indices: frozenset[int] = frozenset(),
    completion_before_start_indices: frozenset[int] = frozenset(),
    trailing_assistant_text: str | None = None,
    terminal_text_override: str | None = None,
    user_message_after_tools: bool = False,
) -> dict[str, bytes]:
    event_ordinal = 1
    updates: list[dict[str, Any]] = [
        {
            "params": {
                "sessionId": session_id,
                "_meta": {
                    "eventId": f"{session_id}-user-1",
                    "agentTimestampMs": 1001,
                },
                "update": {
                    "sessionUpdate": "user_message_chunk",
                    "content": {"type": "text", "text": "synthetic"},
                    "_meta": {"modelId": model_id, "promptIndex": 0},
                },
            }
        }
    ]

    assistant_ordinal = 0

    def event_metadata(
        *,
        update_type: str,
        update_params: Mapping[str, Any] | None = None,
        chunk_id: int | None = None,
        event_label: str,
    ) -> dict[str, Any]:
        nonlocal event_ordinal
        event_ordinal += 1
        metadata: dict[str, Any] = {
            "promptId": request_id,
            "updateType": update_type,
            "eventId": f"{session_id}-{event_label}-{event_ordinal}",
            "agentTimestampMs": 1000 + event_ordinal,
            "streamStartMs": 900,
            "turnStartMs": 800,
            "totalTokens": 123,
        }
        if update_params is not None:
            metadata["updateParams"] = dict(update_params)
        if chunk_id is not None:
            metadata["chunkId"] = chunk_id
        return metadata

    def assistant_update(
        payloads: Sequence[Mapping[str, Any]] = (),
        *,
        text: str | None = None,
    ) -> dict[str, Any]:
        nonlocal assistant_ordinal
        assistant_ordinal += 1
        if text is None:
            text = "\n".join(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                for payload in payloads
            )
        return {
            "params": {
                "sessionId": session_id,
                "_meta": event_metadata(
                    update_type="AgentMessageChunk",
                    chunk_id=100 + assistant_ordinal,
                    event_label="assistant",
                ),
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": text},
                },
            }
        }

    for thought_ordinal, thought_text in enumerate(thought_texts, 1):
        updates.append(
            {
                "params": {
                    "sessionId": session_id,
                    "_meta": event_metadata(
                        update_type="AgentThoughtChunk",
                        chunk_id=thought_ordinal,
                        event_label="thought",
                    ),
                    "update": {
                        "sessionUpdate": "agent_thought_chunk",
                        "content": {"type": "text", "text": thought_text},
                    },
                }
            }
        )

    for extra_ordinal, update_kind in enumerate(extra_session_update_kinds, 1):
        updates.append(
            {
                "params": {
                    "sessionId": session_id,
                    "_meta": event_metadata(
                        update_type="UnsupportedSyntheticUpdate",
                        chunk_id=extra_ordinal,
                        event_label="unsupported",
                    ),
                    "update": {
                        "sessionUpdate": update_kind,
                        "content": {"type": "text", "text": "synthetic"},
                    },
                }
            }
        )

    terminal_payloads = [*progress_payloads, *additional_schema_valid_terminals, terminal]
    if schema_valid_terminal_before_tools:
        updates.append(assistant_update((terminal,)))
    if terminal_before_tools:
        updates.append(
            assistant_update(terminal_payloads, text=terminal_text_override)
        )
    for index, (tool_name, arguments) in enumerate(calls, 1):
        call_id = f"fixture-call-{index}"
        title = f"Synthetic X search {index}"
        start_update = {
            "params": {
                "sessionId": session_id,
                "_meta": event_metadata(
                    update_type="ToolCall",
                    update_params={
                        "toolCallId": call_id,
                        "title": title,
                        "kind": "Search",
                        "status": "InProgress",
                    },
                    event_label="tool-start",
                ),
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": call_id,
                    "title": title,
                    "kind": "search",
                    "status": "in_progress",
                    "rawInput": {"backend": True, "variant": "XSearch"},
                    "_meta": {"backend": True},
                },
            }
        }
        if start_update_overrides is not None:
            start_update["params"]["update"].update(dict(start_update_overrides))
        completion_update = {
            "params": {
                "sessionId": session_id,
                "_meta": event_metadata(
                    update_type="ToolCallUpdate",
                    update_params={"toolCallId": call_id, "status": "Completed"},
                    event_label="tool-completion",
                ),
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": call_id,
                    "status": "completed",
                    "title": title,
                    "rawOutput": {
                        "call_id": f"provider-call-{index}",
                        "id": call_id,
                        "input": json.dumps(
                            dict(arguments),
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                        "name": tool_name,
                    },
                },
            }
        }
        if index in completion_before_start_indices:
            updates.extend((completion_update, start_update))
        else:
            updates.append(start_update)
            if index not in omit_completion_indices:
                updates.append(completion_update)
    if user_message_after_tools:
        user_update = updates.pop(0)
        prior_timestamps = [
            row["params"]["_meta"]["agentTimestampMs"]
            for row in updates
            if isinstance(row.get("params", {}).get("_meta"), dict)
            and type(row["params"]["_meta"].get("agentTimestampMs")) is int
        ]
        if prior_timestamps:
            user_update["params"]["_meta"]["agentTimestampMs"] = max(
                prior_timestamps
            )
        updates.append(user_update)
    if not terminal_before_tools:
        updates.append(
            assistant_update(terminal_payloads, text=terminal_text_override)
        )
    if trailing_assistant_text is not None:
        updates.append(assistant_update(text=trailing_assistant_text))
    prompt_context = _json_bytes(
        {"working_directory": working_directory, "prompt_mode": prompt_mode}
    )
    terminal_chat_text = terminal_text_override or json.dumps(
        dict(terminal),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    chat_history_prefix = _chat_history_prefix_bytes(
        system_prompt=system_prompt,
        user_prompt_text=user_prompt_text,
        extra_chat_rows=extra_chat_rows,
        prompt_binding_mode=prompt_binding_mode,
    )
    model_chat_rows: list[Mapping[str, Any]] = [
        {
            "type": "reasoning",
            "encrypted_content": "synthetic-encrypted-reasoning",
            "id": "synthetic-reasoning-1",
            "status": "completed",
            "summary": [],
        },
        *(
            {
                "type": "backend_tool_call",
                "kind": {
                    "call_id": f"provider-call-{index}",
                    "id": f"fixture-call-{index}",
                    "input": json.dumps(
                        dict(arguments),
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    "name": tool_name,
                    "tool_type": "x_search",
                },
            }
            for index, (tool_name, arguments) in enumerate(calls, 1)
        ),
        {
            "type": "assistant",
            "content": terminal_chat_text,
            "model_fingerprint": "synthetic-model-fingerprint",
            "model_id": model_id,
            "reasoning_effort": reasoning_effort,
        },
    ]
    return {
        "summary.json": _json_bytes(
            {
                "info": {"id": session_id, "cwd": working_directory},
                "request_id": request_id,
                "chat_format_version": 1,
                "current_model_id": model_id,
                "reasoning_effort": reasoning_effort,
                "num_messages": len(updates),
                "num_chat_messages": 5 + len(extra_chat_rows) + len(model_chat_rows),
            }
        ),
        "updates.jsonl": b"".join(_json_bytes(row) for row in updates),
        "events.jsonl": b"".join(
            _json_bytes(row)
            for row in (
                {
                    "type": "turn_started",
                    "ts": "2026-01-01T00:00:00.000000Z",
                    "turn_number": 0,
                    "session_id": session_id,
                    "model_id": model_id,
                    "yolo_mode": False,
                    "conversation_message_count": 4,
                    "session_relationship": "primary",
                    "schema_version": "1.0",
                },
                {
                    "type": "loop_started",
                    "ts": "2026-01-01T00:00:00.000001Z",
                    "loop_index": 0,
                },
                {
                    "type": "phase_changed",
                    "ts": "2026-01-01T00:00:00.000002Z",
                    "phase": "waiting_for_model",
                },
                {
                    "type": "first_token",
                    "ts": "2026-01-01T00:00:00.000003Z",
                },
                {
                    "type": "phase_changed",
                    "ts": "2026-01-01T00:00:00.000004Z",
                    "phase": "streaming_text",
                },
                *extra_event_rows,
                {
                    "type": "turn_ended",
                    "ts": "2026-01-01T00:00:00.999999Z",
                    "outcome": "completed",
                },
            )
        ),
        "chat_history.jsonl": chat_history_prefix
        + b"".join(_json_bytes(row) for row in model_chat_rows),
        "system_prompt.txt": system_prompt,
        "prompt_context.json": prompt_context,
    }
