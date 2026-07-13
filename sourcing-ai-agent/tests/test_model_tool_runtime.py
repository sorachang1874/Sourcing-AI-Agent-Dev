from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Mapping

import pytest

from sourcing_agent.model_route_registry import (
    DEFAULT_MODEL_ROUTE_SPECS,
    MODEL_ROUTE_SPECS_BY_ID,
    ModelRouteExecutionRejected,
    ModelRouteRegistryError,
    ModelRouteSpec,
    assert_d0a_route_execution_allowed,
    model_route_registry_manifest,
    validate_model_route_specs,
)
from sourcing_agent.model_tool_runtime import (
    D0A_EFFECT_AUTHORIZATION_AVAILABLE,
    MAX_SSE_CHUNKS,
    ModelToolProtocolError,
    ModelToolRequestBindingError,
    ModelToolRuntimeError,
    ModelToolSchemaError,
    ScriptedToolReplayError,
    ScriptedToolTurnSession,
    ScriptedToolTurnTranscript,
    SystemMessage,
    TerminalEvent,
    ToolSpec,
    UserMessage,
    canonical_model_turn_transcript_sha256,
    canonical_tool_turn_request_hash,
    canonical_tool_turn_request_payload,
    parse_openai_chat_sse,
    request_for_model_route,
    with_provider_mode,
)


def _route() -> ModelRouteSpec:
    return MODEL_ROUTE_SPECS_BY_ID["agent.planner.loop"]


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _request(
    *,
    provider_mode: str = "scripted",
    workspace_id: str = "workspace_test",
    transcript: bytes | tuple[bytes, ...] | None = None,
):
    transcript_chunks = (
        (_tool_turn_bytes(),)
        if transcript is None
        else ((transcript,) if isinstance(transcript, bytes) else transcript)
    )
    return request_for_model_route(
        _route(),
        provider_mode=provider_mode,
        effective_route_snapshot_digest=_digest("snapshot_v1"),
        max_tokens=256,
        prompt_policy_version="prompt_policy_v1",
        permission_scope_revision="permission_v1",
        outbound_policy_revision="outbound_v1",
        model_safe_schema_revision="model_safe_v1",
        workspace_id=workspace_id,
        actor_id="actor_test",
        permission_scope="agent:plan",
        runtime_namespace="test:model-tool-runtime",
        transcript_digest=canonical_model_turn_transcript_sha256(transcript_chunks),
    )


def _messages():
    return (
        SystemMessage("Use only the declared synthetic tool."),
        UserMessage("Find synthetic research evidence."),
    )


def _tools(*, additional_properties: bool = False):
    return (
        ToolSpec(
            name="search_public_evidence",
            description="Search one synthetic public-evidence fixture.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1, "maxLength": 120},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "required": ["query", "limit"],
                "additionalProperties": additional_properties,
            },
            schema_version="search_public_evidence_v1",
            approval_policy="none_simulated",
            budget_required=False,
        ),
    )


def _frame(payload: object) -> bytes:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"data: {encoded}\n\n".encode()


def _tool_turn_bytes(
    *,
    model: str | None = None,
    finish_reason: str = "tool_calls",
    tool_name: str = "search_public_evidence",
    arguments: tuple[str, ...] = ('{"query":"synthetic', ' evidence","limit":2}'),
    include_usage: bool = True,
) -> bytes:
    response_model = model or _route().model
    chunks = [
        _frame(
            {
                "id": "synthetic_turn_001",
                "model": response_model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "synthetic_call_001",
                                    "type": "function",
                                    "function": {"name": tool_name, "arguments": arguments[0]},
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ],
            }
        )
    ]
    for index, argument_fragment in enumerate(arguments[1:]):
        chunks.append(
            _frame(
                {
                    "id": "synthetic_turn_001",
                    "model": response_model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "function": {"arguments": argument_fragment},
                                    }
                                ]
                            },
                            "finish_reason": finish_reason if index == len(arguments[1:]) - 1 else None,
                        }
                    ],
                }
            )
        )
    if len(arguments) == 1:
        chunks.append(
            _frame(
                {
                    "id": "synthetic_turn_001",
                    "model": response_model,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
                }
            )
        )
    if include_usage:
        chunks.append(
            _frame(
                {
                    "id": "synthetic_turn_001",
                    "model": response_model,
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 12,
                        "completion_tokens": 7,
                        "total_tokens": 19,
                        "prompt_tokens_details": {"cached_tokens": 2},
                    },
                }
            )
        )
    chunks.append(b"data: [DONE]\n\n")
    return b"".join(chunks)


def _text_turn_bytes(
    *,
    finish_reason: str = "stop",
    model: str | None = None,
    include_provider_call_id: bool = True,
) -> bytes:
    response_model = model or _route().model
    identity = {"id": "synthetic_turn_text"} if include_provider_call_id else {}
    return b"".join(
        [
            _frame(
                {
                    **identity,
                    "model": response_model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": "Synthetic "},
                            "finish_reason": None,
                        }
                    ],
                }
            ),
            _frame(
                {
                    **identity,
                    "model": response_model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": "answer."},
                            "finish_reason": finish_reason,
                        }
                    ],
                }
            ),
            b"data: [DONE]\n\n",
        ]
    )


def _split_every_byte(payload: bytes) -> tuple[bytes, ...]:
    return tuple(payload[index : index + 1] for index in range(len(payload)))


def test_route_registry_is_content_revisioned_and_draft_only() -> None:
    manifest = model_route_registry_manifest()

    assert manifest["live_enabled"] is False
    assert manifest["allowed_provider_modes"] == ["scripted", "simulate"]
    routes = manifest["routes"]
    assert isinstance(routes, list)
    assert {item["route_id"] for item in routes} == {spec.route_id for spec in DEFAULT_MODEL_ROUTE_SPECS}
    assert all(item["rollout_state"] == "draft" for item in routes)
    assert all(len(item["route_revision"]) == 64 for item in routes)
    assert _route().revision == _route().revision


def test_route_registry_rejects_non_draft_declaration_for_d0a() -> None:
    active = replace(_route(), route_id="agent.planner.active-test", rollout_state="active")

    with pytest.raises(ModelRouteRegistryError, match="model_route_d0a_requires_draft"):
        validate_model_route_specs([active], require_draft_only=True)


@pytest.mark.parametrize("mode", ["live", "replay", "", "unknown"])
def test_d0a_route_predicate_rejects_live_and_unowned_modes(mode: str) -> None:
    expected = "model_tool_live_unavailable_d0a" if mode == "live" else "model_tool_provider_mode_unsupported_d0a"

    with pytest.raises(ModelRouteExecutionRejected, match=expected):
        assert_d0a_route_execution_allowed(
            route_id=_route().route_id,
            provider_mode=mode,
            required_capabilities={"tools"},
        )


def test_d0a_route_predicate_requires_declared_capability() -> None:
    with pytest.raises(ModelRouteExecutionRejected, match="model_route_capability_missing"):
        assert_d0a_route_execution_allowed(
            route_id="company.identity.adjudicate",
            provider_mode="scripted",
            required_capabilities={"tools"},
        )


def test_d0a_route_predicate_rejects_noncanonical_mode_alias() -> None:
    with pytest.raises(ModelRouteExecutionRejected, match="provider_mode_noncanonical"):
        assert_d0a_route_execution_allowed(
            route_id=_route().route_id,
            provider_mode="Scripted",
            required_capabilities={"tools"},
        )


def test_request_requires_canonical_mode_and_sha256_digests() -> None:
    with pytest.raises(ModelToolRuntimeError, match="provider_mode_noncanonical"):
        _request(provider_mode="SCRIPTED")
    with pytest.raises(ModelToolRuntimeError, match="snapshot_digest_sha256"):
        replace(_request(), effective_route_snapshot_digest="not-a-digest")
    with pytest.raises(ModelToolRuntimeError, match="tool_choice_unsupported"):
        replace(_request(), tool_choice="required")
    with pytest.raises(ModelToolRuntimeError, match="stream_options_unsupported"):
        replace(_request(), stream_options={"include_usage": False})
    with pytest.raises(ModelToolRuntimeError, match="max_tokens_invalid"):
        replace(_request(), max_tokens=True)


def test_owned_json_contracts_are_deeply_immutable() -> None:
    [tool] = _tools()
    request = _request()

    with pytest.raises(TypeError):
        tool.input_schema["type"] = "string"  # type: ignore[index]
    properties = tool.input_schema["properties"]
    assert isinstance(properties, Mapping)
    with pytest.raises(TypeError):
        properties["query"] = {"type": "integer"}  # type: ignore[index]
    with pytest.raises(TypeError):
        request.stream_options["include_usage"] = False  # type: ignore[index]


def test_canonical_request_hash_is_stable_for_mapping_key_order() -> None:
    request = _request()
    [tool] = _tools()
    reordered_tool = replace(
        tool,
        input_schema={
            "required": ["query", "limit"],
            "additionalProperties": False,
            "properties": {
                "limit": {"maximum": 5, "minimum": 1, "type": "integer"},
                "query": {"maxLength": 120, "minLength": 1, "type": "string"},
            },
            "type": "object",
        },
    )

    assert canonical_tool_turn_request_hash(request, _messages(), [tool]) == canonical_tool_turn_request_hash(
        replace(request, stream_options={"include_usage": True}),
        _messages(),
        [reordered_tool],
    )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("route_id", "agent.planner.loop.changed"),
        ("route_revision", _digest("route_revision_v2")),
        ("effective_route_snapshot_digest", _digest("snapshot_v2")),
        ("provider", "other_provider"),
        ("requested_model", "other-model"),
        ("api_style", "other_api_style"),
        ("max_tokens", 257),
        ("prompt_policy_version", "prompt_policy_v2"),
        ("permission_scope_revision", "permission_v2"),
        ("outbound_policy_revision", "outbound_v2"),
        ("model_safe_schema_revision", "model_safe_v2"),
        ("workspace_id", "workspace_other"),
        ("actor_id", "actor_other"),
        ("permission_scope", "agent:other"),
        ("runtime_namespace", "test:model-tool-runtime-other"),
        ("provider_mode", "simulate"),
        ("transcript_digest", _digest("synthetic_transcript_v2")),
    ],
)
def test_canonical_request_hash_changes_for_identity_and_behavior_fields(field_name: str, replacement: object) -> None:
    request = _request()
    baseline = canonical_tool_turn_request_hash(request, _messages(), _tools())

    changed = canonical_tool_turn_request_hash(replace(request, **{field_name: replacement}), _messages(), _tools())

    assert changed != baseline


def test_canonical_request_hash_changes_for_message_or_tool_order() -> None:
    request = _request()
    messages = _messages()
    tool_a = _tools()[0]
    tool_b = replace(tool_a, name="fetch_public_evidence", description="Fetch one synthetic evidence item.")
    baseline = canonical_tool_turn_request_hash(request, messages, (tool_a, tool_b))

    assert canonical_tool_turn_request_hash(request, tuple(reversed(messages)), (tool_a, tool_b)) != baseline
    assert canonical_tool_turn_request_hash(request, messages, (tool_b, tool_a)) != baseline


def test_tool_schema_definition_rejects_unsupported_or_ambiguous_keywords() -> None:
    with pytest.raises(ModelToolSchemaError, match="keyword_unsupported"):
        ToolSpec(
            name="ambiguous_tool",
            description="Not accepted.",
            input_schema={"type": "object", "properties": {}, "oneOf": []},
            schema_version="v1",
            approval_policy="none_simulated",
            budget_required=False,
        )

    with pytest.raises(ModelToolRuntimeError, match="json_object_keys_must_be_strings"):
        ToolSpec(
            name="colliding_keys_tool",
            description="Not accepted.",
            input_schema={
                "type": "object",
                "properties": {
                    1: {"type": "string"},
                    "1": {"type": "integer"},
                },
            },  # type: ignore[dict-item]
            schema_version="v1",
            approval_policy="none_simulated",
            budget_required=False,
        )

    with pytest.raises(ModelToolSchemaError, match="keyword_inapplicable"):
        ToolSpec(
            name="misleading_tool",
            description="Not accepted.",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "minimum": 1}},
            },
            schema_version="v1",
            approval_policy="none_simulated",
            budget_required=False,
        )


def test_parser_reassembles_arbitrary_sse_chunking_to_one_canonical_result() -> None:
    payload = _tool_turn_bytes()
    request = _request(transcript=payload)
    whole = parse_openai_chat_sse([payload], request=request, messages=_messages(), tools=_tools())
    bytewise = parse_openai_chat_sse(
        _split_every_byte(payload),
        request=request,
        messages=_messages(),
        tools=_tools(),
    )

    assert bytewise.terminal_result == whole.terminal_result
    assert bytewise.advisory_events == whole.advisory_events
    assert whole.terminal_result.eligible_for_policy_evaluation is True
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False
    assert not hasattr(whole.terminal_result, "authorizable")
    assert whole.terminal_result.terminal_reason == "tool_calls"
    assert whole.terminal_result.tool_calls[0].arguments == {"query": "synthetic evidence", "limit": 2}
    assert whole.terminal_result.usage.total_tokens == 19
    assert whole.terminal_result.usage_status == "reported"
    assert isinstance(whole.advisory_events[-1], TerminalEvent)
    assert whole.advisory_events[-1].result == whole.terminal_result


def test_parser_emits_end_turn_result_without_action() -> None:
    payload = _text_turn_bytes()
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    assert parsed.terminal_result.text == "Synthetic answer."
    assert parsed.terminal_result.tool_calls == ()
    assert parsed.terminal_result.terminal_reason == "end_turn"
    assert parsed.terminal_result.eligible_for_policy_evaluation is True
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False


def test_missing_provider_call_identity_cannot_enter_policy_evaluation() -> None:
    payload = _text_turn_bytes(include_provider_call_id=False)
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    assert parsed.terminal_result.provider_call_id is None
    assert parsed.terminal_result.eligible_for_policy_evaluation is False
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False


def test_direct_parser_rejects_live_before_reading_stream() -> None:
    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        parse_openai_chat_sse(
            [b"not even SSE"],
            request=_request(provider_mode="live", transcript=b"not even SSE"),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_rejects_empty_non_usage_frame() -> None:
    payload = b"".join(
        [
            _frame({"id": "noop", "model": _route().model, "choices": []}),
            b"data: [DONE]\n\n",
        ]
    )

    with pytest.raises(ModelToolProtocolError, match="empty_non_usage_frame"):
        parse_openai_chat_sse(
            [payload],
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_rejects_unhandled_semantic_delta_fields() -> None:
    payload = b"".join(
        [
            _frame(
                {
                    "id": "refusal",
                    "model": _route().model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"refusal": "not represented by the D0a message model"},
                            "finish_reason": "stop",
                        }
                    ],
                }
            ),
            b"data: [DONE]\n\n",
        ]
    )

    with pytest.raises(ModelToolProtocolError, match="delta_fields_unsupported"):
        parse_openai_chat_sse(
            [payload],
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_marks_length_terminal_non_authorizable() -> None:
    payload = _text_turn_bytes(finish_reason="length")
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    assert parsed.terminal_result.terminal_reason == "length"
    assert parsed.terminal_result.eligible_for_policy_evaluation is False
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (_tool_turn_bytes()[: -len(b"data: [DONE]\n\n")], "done_missing"),
        (_tool_turn_bytes(model="rerouted-model"), "response_model_mismatch"),
        (_tool_turn_bytes(tool_name="undeclared_tool"), "name_not_served"),
        (
            _tool_turn_bytes(arguments=('{"query":"synthetic evidence","limit":2,"hidden":true}',)),
            "argument_extra_fields",
        ),
        (_tool_turn_bytes(arguments=('{"query":"synthetic evidence"}',)), "argument_required_missing"),
    ],
)
def test_parser_fails_closed_for_incomplete_identity_or_schema_invalid_stream(payload: bytes, error: str) -> None:
    with pytest.raises(ModelToolRuntimeError, match=error):
        parse_openai_chat_sse(
            [payload],
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_rejects_multiple_choices() -> None:
    payload = b"".join(
        [
            _frame(
                {
                    "id": "multi",
                    "model": _route().model,
                    "choices": [
                        {"index": 0, "delta": {"content": "a"}, "finish_reason": "stop"},
                        {"index": 1, "delta": {"content": "b"}, "finish_reason": "stop"},
                    ],
                }
            ),
            b"data: [DONE]\n\n",
        ]
    )

    with pytest.raises(ModelToolProtocolError, match="multiple_choices"):
        parse_openai_chat_sse(
            [payload],
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_rejects_finish_shape_mismatch() -> None:
    payload = _tool_turn_bytes(finish_reason="stop")
    with pytest.raises(ModelToolProtocolError, match="end_turn_with_calls"):
        parse_openai_chat_sse(
            [payload],
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


def test_parser_assigns_stable_occurrence_ordinals_for_duplicate_logical_calls() -> None:
    argument_json = '{"query":"synthetic evidence","limit":2}'
    payload = b"".join(
        [
            _frame(
                {
                    "id": "duplicate-logical",
                    "model": _route().model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "id": "call-a",
                                        "type": "function",
                                        "function": {"name": "search_public_evidence", "arguments": argument_json},
                                    },
                                    {
                                        "index": 1,
                                        "id": "call-b",
                                        "type": "function",
                                        "function": {"name": "search_public_evidence", "arguments": argument_json},
                                    },
                                ]
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                }
            ),
            b"data: [DONE]\n\n",
        ]
    )

    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    assert [call.occurrence_ordinal for call in parsed.terminal_result.tool_calls] == [1, 2]


def test_scripted_session_replays_exact_hash_and_stream_terminal_matches_buffered() -> None:
    request = _request()
    messages = _messages()
    tools = _tools()
    transcript = ScriptedToolTurnTranscript(
        request_sha256=canonical_tool_turn_request_hash(request, messages, tools),
        chunks=_split_every_byte(_tool_turn_bytes()),
        synthetic=True,
    )
    session = ScriptedToolTurnSession(transcript)

    buffered = session.run_tool_turn(request, messages, tools)
    streamed = tuple(session.stream_tool_turn(request, messages, tools))

    assert isinstance(streamed[-1], TerminalEvent)
    assert streamed[-1].result == buffered


def test_transcript_digest_is_chunk_boundary_independent_and_content_bound() -> None:
    payload = _tool_turn_bytes()
    assert canonical_model_turn_transcript_sha256((payload,)) == canonical_model_turn_transcript_sha256(
        _split_every_byte(payload)
    )

    request = _request(transcript=payload)
    changed_payload = payload.replace(b"synthetic", b"different", 1)
    assert changed_payload != payload
    transcript = ScriptedToolTurnTranscript(
        request_sha256=canonical_tool_turn_request_hash(request, _messages(), _tools()),
        chunks=(changed_payload,),
        synthetic=True,
    )

    with pytest.raises(ScriptedToolReplayError, match="content_digest_mismatch"):
        ScriptedToolTurnSession(transcript).run_tool_turn(request, _messages(), _tools())


def test_transcript_collection_stops_at_chunk_limit_without_overconsuming_generator() -> None:
    def oversized_chunks():
        for index in range(MAX_SSE_CHUNKS + 2):
            if index > MAX_SSE_CHUNKS:
                raise AssertionError("collector consumed beyond the first over-limit item")
            yield b""

    with pytest.raises(ModelToolProtocolError, match="chunks_too_many"):
        canonical_model_turn_transcript_sha256(oversized_chunks())


def test_transcript_chunks_reject_text_instead_of_mixing_decoder_order() -> None:
    with pytest.raises(ModelToolProtocolError, match="chunks_must_be_bytes"):
        canonical_model_turn_transcript_sha256(["data: [DONE]\n\n"])  # type: ignore[list-item]


def test_scripted_transcript_requires_exact_immutable_runtime_types() -> None:
    request = _request()
    request_sha256 = canonical_tool_turn_request_hash(request, _messages(), _tools())

    with pytest.raises(ScriptedToolReplayError, match="chunks_must_be_tuple"):
        ScriptedToolTurnTranscript(
            request_sha256=request_sha256,
            chunks=[_tool_turn_bytes()],  # type: ignore[arg-type]
            synthetic=True,
        )
    with pytest.raises(ScriptedToolReplayError, match="synthetic_must_be_bool"):
        ScriptedToolTurnTranscript(
            request_sha256=request_sha256,
            chunks=(_tool_turn_bytes(),),
            synthetic="false",  # type: ignore[arg-type]
        )


def test_policy_sensitive_tool_result_is_never_effect_authorized_by_d0a() -> None:
    payload = _tool_turn_bytes(include_usage=False)
    [base_tool] = _tools()
    policy_tool = replace(
        base_tool,
        approval_policy="human_approval_required",
        budget_required=True,
    )
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=(policy_tool,),
    )

    assert parsed.terminal_result.usage_status == "unavailable"
    assert parsed.terminal_result.eligible_for_policy_evaluation is True
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False
    assert not hasattr(parsed.terminal_result, "authorizable")


def test_scripted_session_rejects_request_or_route_drift() -> None:
    request = _request()
    messages = _messages()
    tools = _tools()
    session = ScriptedToolTurnSession(
        ScriptedToolTurnTranscript(
            request_sha256=canonical_tool_turn_request_hash(request, messages, tools),
            chunks=(_tool_turn_bytes(),),
            synthetic=True,
        )
    )

    with pytest.raises(ScriptedToolReplayError, match="request_hash_mismatch"):
        session.run_tool_turn(replace(request, max_tokens=512), messages, tools)
    with pytest.raises(ModelToolRequestBindingError, match="route_field_mismatch"):
        session.run_tool_turn(replace(request, requested_model="unregistered-model"), messages, tools)


def test_scripted_session_rejects_non_synthetic_workspace_mismatch() -> None:
    request = _request()
    transcript = ScriptedToolTurnTranscript(
        request_sha256=canonical_tool_turn_request_hash(request, _messages(), _tools()),
        chunks=(_tool_turn_bytes(),),
        synthetic=False,
        workspace_id="workspace_other",
    )

    with pytest.raises(ScriptedToolReplayError, match="workspace_mismatch"):
        ScriptedToolTurnSession(transcript).run_tool_turn(request, _messages(), _tools())


def test_scripted_session_rejects_live_before_transcript_parse() -> None:
    request = _request()
    session = ScriptedToolTurnSession(
        ScriptedToolTurnTranscript(
            request_sha256=canonical_tool_turn_request_hash(request, _messages(), _tools()),
            chunks=(b"not even SSE",),
            synthetic=True,
        )
    )

    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        session.run_tool_turn(with_provider_mode(request, "live"), _messages(), _tools())


def test_d0a_modules_have_no_transport_or_environment_dependency() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "sourcing_agent"
    source = "\n".join(
        (root / filename).read_text() for filename in ("model_route_registry.py", "model_tool_runtime.py")
    )

    assert "import requests" not in source
    assert "urllib" not in source
    assert "os.environ" not in source
    assert "assert_live_provider_access_allowed" not in source


def test_canonical_payload_exposes_hash_inputs_but_not_message_content() -> None:
    payload = canonical_tool_turn_request_payload(_request(), _messages(), _tools())
    encoded = json.dumps(payload, ensure_ascii=False)

    assert payload["provider_mode"] == "scripted"
    assert payload["runtime_namespace"] == "test:model-tool-runtime"
    assert len(payload["messages_digest"]) == 64
    assert len(payload["tools_schema_digest"]) == 64
    assert "Find synthetic research evidence" not in encoded
