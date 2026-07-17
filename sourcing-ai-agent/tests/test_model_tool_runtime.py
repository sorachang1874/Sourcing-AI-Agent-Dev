from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping

import pytest

import sourcing_agent.model_tool_runtime as model_tool_runtime_module
from sourcing_agent.model_provider import OpenAIModelUsage
from sourcing_agent.model_route_registry import (
    DEFAULT_MODEL_ROUTE_SPECS,
    EFFECTIVE_MODEL_ROUTE_SETTINGS_OWNER,
    EFFECTIVE_MODEL_ROUTE_SETTINGS_RECORD_KEYS,
    EFFECTIVE_MODEL_ROUTE_SETTINGS_SCHEMA_VERSION,
    EFFECTIVE_MODEL_ROUTE_SNAPSHOT_OWNER,
    EFFECTIVE_MODEL_ROUTE_SNAPSHOT_RECORD_KEYS,
    EFFECTIVE_MODEL_ROUTE_SNAPSHOT_REF_PREFIX,
    MODEL_ROUTE_SPECS_BY_ID,
    EffectiveModelRouteSettings,
    ModelRouteExecutionRejected,
    ModelRouteRegistryError,
    ModelRouteSpec,
    assert_d0a_route_execution_allowed,
    issue_effective_model_route_snapshot,
    model_route_registry_manifest,
    validate_model_route_specs,
)
from sourcing_agent.model_tool_runtime import (
    D0A_EFFECT_AUTHORIZATION_AVAILABLE,
    MAX_SSE_CHUNKS,
    MAX_SSE_LINE_BYTES,
    MAX_SSE_LINES,
    MAX_SSE_PENDING_BYTES,
    MAX_TOOL_SCHEMA_BYTES,
    MAX_TOOL_SPECS,
    MAX_TOTAL_TOOL_SCHEMA_BYTES,
    MODEL_TURN_BUDGET_SCHEMA_VERSION,
    MODEL_TURN_IDEMPOTENCY_KEY_PREFIX,
    InternalToolValidatorSpec,
    ModelToolProtocolError,
    ModelToolRequestBindingError,
    ModelToolRuntimeError,
    ModelToolSchemaError,
    ModelTurnBudget,
    ParsedToolTurn,
    ScriptedToolReplayError,
    ScriptedToolTurnSession,
    ScriptedToolTurnTranscript,
    SystemMessage,
    TerminalEvent,
    ToolCallingSessionBase,
    ToolCallRecord,
    ToolSpec,
    UserMessage,
    canonical_model_turn_transcript_sha256,
    canonical_tool_turn_request_hash,
    canonical_tool_turn_request_payload,
    model_turn_execution_context_for_route,
    parse_openai_chat_sse,
    request_for_model_route,
    request_for_model_turn_execution_context,
    with_provider_mode,
)
from sourcing_agent.model_usage import ModelUsage, ModelUsageValidationError


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


def _effective_route_settings() -> EffectiveModelRouteSettings:
    return EffectiveModelRouteSettings(
        schema_version=EFFECTIVE_MODEL_ROUTE_SETTINGS_SCHEMA_VERSION,
        settings_owner=EFFECTIVE_MODEL_ROUTE_SETTINGS_OWNER,
        settings_policy_revision="synthetic_settings_policy_v1",
        provider_family="model",
        live_gate_provider_name="model_provider",
        endpoint_identity_digest=_digest("synthetic-relay-endpoint"),
        request_timeout_ms=45_000,
        pricing_class="synthetic_zero_cost",
        circuit_policy_id="model_circuit_v1",
    )


def _effective_route_snapshot():
    route = _route()
    return issue_effective_model_route_snapshot(
        route_id=route.route_id,
        route_revision=route.revision,
        effective_settings=_effective_route_settings(),
    )


def _turn_budget() -> ModelTurnBudget:
    return ModelTurnBudget(
        schema_version=MODEL_TURN_BUDGET_SCHEMA_VERSION,
        budget_class=_route().budget_class,
        max_input_tokens=4_096,
        max_output_tokens=256,
        max_total_tokens=4_352,
        monetary_ceiling="0",
        currency_code="USD",
        deadline_at=datetime(2026, 7, 15, 4, 30, tzinfo=timezone.utc),
    )


def _execution_context(*, provider_mode: str = "scripted"):
    return model_turn_execution_context_for_route(
        _route(),
        _effective_route_snapshot(),
        runtime_namespace="test:model-tool-runtime",
        provider_mode=provider_mode,  # type: ignore[arg-type]
        workspace_id="workspace_test",
        scope_digest=_digest("synthetic-scope"),
        coordination_plan_review_id=73,
        actor_id="actor_test",
        permission_scope="agent:plan",
        prompt_policy_version="prompt_policy_v1",
        permission_scope_revision="permission_v1",
        outbound_policy_revision="outbound_v1",
        model_safe_schema_revision="model_safe_v1",
        operation_run_id="operation_synthetic_001",
        turn_id="turn_synthetic_001",
        step_id="step_synthetic_001",
        workflow_command_id="command_synthetic_001",
        activity_run_id="activity_synthetic_001",
        activity_attempt_id="activity_attempt_synthetic_001",
        attempt=1,
        budget=_turn_budget(),
        budget_reservation_ref="cost-reservation:synthetic:001",
        approval_ref=None,
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


def _tool_spec(name: str, *, description: str = "Synthetic tool.") -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        schema_version="synthetic_v1",
        approval_policy="none_simulated",
        budget_required=False,
    )


@pytest.mark.parametrize(
    "name",
    ["search.tool", "search:tool", "_search_tool", "a" * 129, "搜索工具"],
)
def test_provider_tool_declarations_and_calls_share_the_canonical_agent_name_grammar(name: str) -> None:
    with pytest.raises(ModelToolRuntimeError, match="invalid_tool_name"):
        _tool_spec(name)
    with pytest.raises(ModelToolRuntimeError, match="invalid_tool_name"):
        ToolCallRecord.from_arguments(
            provider_call_id="call_invalid_name",
            name=name,
            arguments={},
        )


@pytest.mark.parametrize("name", ["a", "Search_Tool-9", "a" * 128])
def test_provider_tool_declarations_and_calls_accept_the_same_canonical_name_boundaries(name: str) -> None:
    assert _tool_spec(name).name == name
    assert (
        ToolCallRecord.from_arguments(
            provider_call_id="call_valid_name",
            name=name,
            arguments={},
        ).name
        == name
    )


def test_provider_and_internal_tool_specs_share_one_schema_validation_engine() -> None:
    provider_spec = _tool_spec("search_public_evidence")
    internal_spec = InternalToolValidatorSpec(
        name="search_public_evidence:success:result",
        description="Internal result validator.",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        schema_version="synthetic_result_v1",
    )
    assert provider_spec.input_schema == internal_spec.input_schema

    with pytest.MonkeyPatch.context() as monkeypatch:
        calls: list[Mapping[str, object]] = []
        shared_validator = model_tool_runtime_module._validate_tool_input

        def record_call(
            input_schema: Mapping[str, object],
            value: Mapping[str, object],
        ):
            calls.append(input_schema)
            return shared_validator(input_schema, value)

        monkeypatch.setattr(model_tool_runtime_module, "_validate_tool_input", record_call)
        assert provider_spec.validate_input({}) == {}
        assert internal_spec.validate_input({}) == {}

    assert calls == [provider_spec.input_schema, internal_spec.input_schema]


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


def _split_at_wire_boundaries(payload: bytes, line_wire_bytes: int) -> tuple[bytes, ...]:
    boundaries = sorted(
        {
            0,
            1,
            MAX_SSE_LINE_BYTES // 2,
            line_wire_bytes - 2,
            line_wire_bytes - 1,
            line_wire_bytes,
            line_wire_bytes + 1,
            len(payload),
        }
    )
    return tuple(
        payload[start:end] for start, end in zip(boundaries, boundaries[1:]) if 0 <= start < end <= len(payload)
    )


def _bounded_sse_wire_line(prefix: bytes, newline: bytes, *, extra_raw_bytes: int = 0) -> bytes:
    assert newline in {b"\n", b"\r\n"}
    trailing_cr_bytes = 1 if newline == b"\r\n" else 0
    filler_bytes = MAX_SSE_LINE_BYTES + extra_raw_bytes - len(prefix) - trailing_cr_bytes
    assert filler_bytes >= 0
    wire = prefix + (b"x" * filler_bytes) + newline
    raw_line = wire.split(b"\n", 1)[0]
    assert len(raw_line) == MAX_SSE_LINE_BYTES + extra_raw_bytes
    return wire


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


def test_public_route_registry_is_immutable() -> None:
    with pytest.raises(TypeError):
        MODEL_ROUTE_SPECS_BY_ID["agent.planner.mutable"] = _route()  # type: ignore[index]

    validated = validate_model_route_specs(DEFAULT_MODEL_ROUTE_SPECS, require_draft_only=True)
    with pytest.raises(TypeError):
        validated["agent.planner.mutable"] = _route()  # type: ignore[index]


def test_effective_route_snapshot_has_one_deterministic_owner_and_content_ref() -> None:
    snapshot = _effective_route_snapshot()
    repeated = _effective_route_snapshot()

    assert snapshot == repeated
    assert snapshot.owner == EFFECTIVE_MODEL_ROUTE_SNAPSHOT_OWNER
    assert set(snapshot.to_record()) == EFFECTIVE_MODEL_ROUTE_SNAPSHOT_RECORD_KEYS
    assert set(_effective_route_settings().to_record()) == EFFECTIVE_MODEL_ROUTE_SETTINGS_RECORD_KEYS
    assert snapshot.settings_owner == EFFECTIVE_MODEL_ROUTE_SETTINGS_OWNER
    assert snapshot.settings_digest == _effective_route_settings().settings_digest
    assert snapshot.snapshot_ref == f"{EFFECTIVE_MODEL_ROUTE_SNAPSHOT_REF_PREFIX}{snapshot.snapshot_digest}"
    changed = issue_effective_model_route_snapshot(
        route_id=_route().route_id,
        route_revision=_route().revision,
        effective_settings=replace(_effective_route_settings(), request_timeout_ms=45_001),
    )
    assert changed.snapshot_digest != snapshot.snapshot_digest
    with pytest.raises(ModelRouteRegistryError, match="settings_digest_mismatch"):
        replace(snapshot, request_timeout_ms=45_001)
    with pytest.raises(FrozenInstanceError):
        snapshot.request_timeout_ms = 1  # type: ignore[misc]
    with pytest.raises(ModelRouteRegistryError, match="settings_owner_invalid"):
        replace(_effective_route_settings(), settings_owner="caller_supplied")
    with pytest.raises(ModelRouteRegistryError, match="route_revision_mismatch"):
        issue_effective_model_route_snapshot(
            route_id=_route().route_id,
            route_revision=_digest("not-the-checked-in-route"),
            effective_settings=_effective_route_settings(),
        )


def test_model_turn_budget_and_execution_context_are_canonical_and_fully_bound() -> None:
    budget = _turn_budget()
    context = _execution_context()

    assert budget.to_record()["deadline_at"] == "2026-07-15T04:30:00.000000Z"
    assert len(budget.budget_digest) == 64
    assert context.budget is budget or context.budget == budget
    assert context.effective_route_snapshot_ref == (
        f"{EFFECTIVE_MODEL_ROUTE_SNAPSHOT_REF_PREFIX}{context.effective_route_snapshot_digest}"
    )
    assert context.pfx_record() == {
        "runtime_namespace": "test:model-tool-runtime",
        "provider_mode": "scripted",
        "workspace_id": "workspace_test",
        "scope_digest": _digest("synthetic-scope"),
        "coordination_plan_review_id": 73,
    }
    assert len(context.context_digest) == 64
    assert context.idempotency_key.startswith(MODEL_TURN_IDEMPOTENCY_KEY_PREFIX)
    with pytest.raises(ModelToolRuntimeError, match="idempotency_key_mismatch"):
        replace(context, activity_attempt_id="activity_attempt_synthetic_002")
    with pytest.raises(ModelToolRuntimeError, match="idempotency_key_mismatch"):
        replace(context, provider_mode="simulate")
    simulate_context = _execution_context(provider_mode="simulate")
    assert simulate_context.context_digest != context.context_digest
    assert simulate_context.idempotency_key != context.idempotency_key

    with pytest.raises(ModelToolRuntimeError, match="monetary_ceiling_noncanonical"):
        replace(budget, monetary_ceiling="0.00")
    with pytest.raises(ModelToolRuntimeError, match="snapshot_ref_mismatch"):
        replace(context, effective_route_snapshot_ref="fixture-digest-is-not-an-owner-ref")
    with pytest.raises(ModelToolRuntimeError, match="budget_class_mismatch"):
        model_turn_execution_context_for_route(
            _route(),
            _effective_route_snapshot(),
            **{
                **{
                    field_name: getattr(context, field_name)
                    for field_name in (
                        "runtime_namespace",
                        "provider_mode",
                        "workspace_id",
                        "scope_digest",
                        "coordination_plan_review_id",
                        "actor_id",
                        "permission_scope",
                        "prompt_policy_version",
                        "permission_scope_revision",
                        "outbound_policy_revision",
                        "model_safe_schema_revision",
                        "operation_run_id",
                        "turn_id",
                        "step_id",
                        "workflow_command_id",
                        "activity_run_id",
                        "activity_attempt_id",
                        "attempt",
                        "budget_reservation_ref",
                        "approval_ref",
                    )
                },
                "budget": replace(budget, budget_class="wrong_budget_class"),
            },
        )


def test_tool_turn_request_is_a_fail_closed_context_derived_mirror() -> None:
    context = _execution_context()
    request = request_for_model_turn_execution_context(
        context,
        _route(),
        transcript_digest=_digest("synthetic-context-bound-transcript"),
    )

    assert request.execution_context is context
    assert request.route_id == context.route_id
    assert request.max_tokens == context.budget.max_output_tokens
    assert request.provider_mode == context.provider_mode
    assert _request().execution_context is None
    with pytest.raises(ModelToolRequestBindingError, match="execution_context_mismatch:route_id"):
        replace(request, route_id="agent.planner.other")
    with pytest.raises(ModelToolRequestBindingError, match="execution_context_mismatch:provider_mode"):
        replace(request, provider_mode="simulate")

    with pytest.raises(ModelToolRequestBindingError, match="execution_context_provider_mode_immutable"):
        with_provider_mode(request, "live")

    live_context = _execution_context(provider_mode="live")
    live_request = request_for_model_turn_execution_context(
        live_context,
        _route(),
        transcript_digest=_digest("synthetic-context-bound-transcript"),
    )
    assert live_request.execution_context is not None
    assert live_request.execution_context is live_context
    assert live_context.idempotency_key != context.idempotency_key
    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        assert_d0a_route_execution_allowed(
            route_id=live_request.route_id,
            provider_mode=live_request.provider_mode,
            required_capabilities={"tools"},
        )


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


def test_message_collector_stops_at_first_byte_overflow() -> None:
    consumed: list[int] = []

    def oversized_messages():
        for index in range(6):
            if index > 4:
                raise AssertionError("message collector consumed beyond the first over-limit item")
            consumed.append(index)
            yield UserMessage("m" * 60_000)

    with pytest.raises(ModelToolRuntimeError, match="messages_too_large"):
        canonical_tool_turn_request_hash(_request(), oversized_messages(), _tools())

    assert consumed == [0, 1, 2, 3, 4]


def test_tool_collector_stops_at_first_count_overflow() -> None:
    consumed: list[int] = []

    def oversized_tools():
        for index in range(MAX_TOOL_SPECS + 2):
            if index > MAX_TOOL_SPECS:
                raise AssertionError("tool collector consumed beyond the first over-limit item")
            consumed.append(index)
            yield _tool_spec(f"tool_{index:03d}")

    with pytest.raises(ModelToolRuntimeError, match="tools_too_many"):
        canonical_tool_turn_request_hash(_request(), _messages(), oversized_tools())

    assert consumed == list(range(MAX_TOOL_SPECS + 1))


def test_tool_collector_stops_at_first_total_schema_overflow() -> None:
    description = "d" * 16_000
    prototype = _tool_spec("tool_000", description=description)
    record_bytes = len(
        json.dumps(
            prototype.to_fingerprint_record(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    encoded_array_bytes = 2
    allowed_items = 0
    while encoded_array_bytes + (1 if allowed_items else 0) + record_bytes <= MAX_TOTAL_TOOL_SCHEMA_BYTES:
        encoded_array_bytes += (1 if allowed_items else 0) + record_bytes
        allowed_items += 1
    assert allowed_items < MAX_TOOL_SPECS

    consumed: list[int] = []

    def oversized_tools():
        for index in range(allowed_items + 2):
            if index > allowed_items:
                raise AssertionError("tool collector consumed beyond the first schema-over-limit item")
            consumed.append(index)
            yield _tool_spec(f"tool_{index:03d}", description=description)

    with pytest.raises(ModelToolRuntimeError, match="tools_schema_too_large"):
        canonical_tool_turn_request_hash(_request(), _messages(), oversized_tools())

    assert consumed == list(range(allowed_items + 1))


def test_single_tool_schema_has_a_hard_byte_bound() -> None:
    with pytest.raises(ModelToolSchemaError, match="schema_too_large"):
        ToolSpec(
            name="oversized_schema_tool",
            description="Rejected before registry collection.",
            input_schema={
                "type": "object",
                "description": "s" * MAX_TOOL_SCHEMA_BYTES,
                "properties": {},
            },
            schema_version="v1",
            approval_policy="none_simulated",
            budget_required=False,
        )


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


def test_parser_large_complete_line_chunk_is_chunk_boundary_independent() -> None:
    comment_line = b":" + (b"x" * (64 * 1024)) + b"\n"
    payload = (comment_line * 9) + _tool_turn_bytes()
    assert len(payload) > MAX_SSE_PENDING_BYTES
    split_chunks = tuple(payload[offset : offset + 64 * 1024] for offset in range(0, len(payload), 64 * 1024))
    request = _request(transcript=payload)

    whole = parse_openai_chat_sse((payload,), request=request, messages=_messages(), tools=_tools())
    split = parse_openai_chat_sse(split_chunks, request=request, messages=_messages(), tools=_tools())

    assert split == whole


@pytest.mark.parametrize("prefix", [b":", b"event:"])
@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
def test_complete_comment_and_ignored_lines_accept_exact_wire_limit_for_any_chunking(
    prefix: bytes,
    newline: bytes,
) -> None:
    line_wire = _bounded_sse_wire_line(prefix, newline)
    payload = line_wire + _tool_turn_bytes()
    request = _request(transcript=payload)

    single = parse_openai_chat_sse((payload,), request=request, messages=_messages(), tools=_tools())
    split = parse_openai_chat_sse(
        _split_at_wire_boundaries(payload, len(line_wire)),
        request=request,
        messages=_messages(),
        tools=_tools(),
    )

    assert MAX_SSE_LINE_BYTES == MAX_SSE_PENDING_BYTES
    assert split == single


@pytest.mark.parametrize("prefix", [b":", b"event:"])
@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
def test_complete_comment_and_ignored_lines_reject_wire_limit_plus_one_for_any_chunking(
    prefix: bytes,
    newline: bytes,
) -> None:
    line_wire = _bounded_sse_wire_line(prefix, newline, extra_raw_bytes=1)
    payload = line_wire + _tool_turn_bytes()
    request = _request(transcript=payload)

    for chunks in ((payload,), _split_at_wire_boundaries(payload, len(line_wire))):
        with pytest.raises(ModelToolProtocolError, match="sse_line_too_large"):
            parse_openai_chat_sse(chunks, request=request, messages=_messages(), tools=_tools())


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
def test_complete_data_line_applies_wire_limit_before_frame_limit(newline: bytes) -> None:
    exact_line_wire = _bounded_sse_wire_line(b"data: ", newline)
    exact_payload = exact_line_wire + newline
    exact_request = _request(transcript=exact_payload)
    for chunks in (
        (exact_payload,),
        _split_at_wire_boundaries(exact_payload, len(exact_line_wire)),
    ):
        with pytest.raises(ModelToolProtocolError, match="sse_frame_too_large"):
            parse_openai_chat_sse(chunks, request=exact_request, messages=_messages(), tools=_tools())

    oversized_line_wire = _bounded_sse_wire_line(b"data: ", newline, extra_raw_bytes=1)
    oversized_payload = oversized_line_wire + newline
    oversized_request = _request(transcript=oversized_payload)
    for chunks in (
        (oversized_payload,),
        _split_at_wire_boundaries(oversized_payload, len(oversized_line_wire)),
    ):
        with pytest.raises(ModelToolProtocolError, match="sse_line_too_large"):
            parse_openai_chat_sse(chunks, request=oversized_request, messages=_messages(), tools=_tools())


@pytest.mark.parametrize("trailing_cr", [False, True])
def test_unfinished_pending_line_uses_same_exact_wire_limit_for_any_chunking(trailing_cr: bool) -> None:
    exact_payload = (b"x" * (MAX_SSE_LINE_BYTES - int(trailing_cr))) + (b"\r" if trailing_cr else b"")
    assert len(exact_payload) == MAX_SSE_LINE_BYTES
    exact_request = _request(transcript=exact_payload)
    for chunks in (
        (exact_payload,),
        _split_at_wire_boundaries(exact_payload, len(exact_payload)),
    ):
        with pytest.raises(ModelToolProtocolError, match="sse_incomplete_frame"):
            parse_openai_chat_sse(chunks, request=exact_request, messages=_messages(), tools=_tools())

    oversized_payload = exact_payload + b"x"
    oversized_request = _request(transcript=oversized_payload)
    for chunks in (
        (oversized_payload,),
        _split_at_wire_boundaries(oversized_payload, len(oversized_payload)),
    ):
        with pytest.raises(ModelToolProtocolError, match="sse_line_too_large"):
            parse_openai_chat_sse(chunks, request=oversized_request, messages=_messages(), tools=_tools())


def test_parser_bounds_complete_sse_lines_separately_from_pending_remainder() -> None:
    payload = (b":\n" * (MAX_SSE_LINES + 1)) + _tool_turn_bytes()

    with pytest.raises(ModelToolProtocolError, match="sse_lines_too_many"):
        parse_openai_chat_sse(
            (payload,),
            request=_request(transcript=payload),
            messages=_messages(),
            tools=_tools(),
        )


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


def test_direct_parser_rejects_live_before_reading_any_caller_iterable() -> None:
    def unread_iterable():
        raise AssertionError("live fence must run before consuming caller iterables")
        yield None

    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        parse_openai_chat_sse(
            unread_iterable(),  # type: ignore[arg-type]
            request=_request(provider_mode="live", transcript=b"not even SSE"),
            messages=unread_iterable(),  # type: ignore[arg-type]
            tools=unread_iterable(),  # type: ignore[arg-type]
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


def test_tool_calling_session_base_is_abstract_and_owns_both_public_projections() -> None:
    with pytest.raises(TypeError, match="abstract"):
        ToolCallingSessionBase()

    request = _request()
    messages = _messages()
    tools = _tools()
    parsed = parse_openai_chat_sse(
        [_tool_turn_bytes()],
        request=request,
        messages=messages,
        tools=tools,
    )

    class ProbeToolCallingSession(ToolCallingSessionBase):
        def __init__(self) -> None:
            self.parse_calls = 0

        def _parse_tool_turn(self, actual_request, actual_messages, actual_tools):
            self.parse_calls += 1
            assert actual_request is request
            assert tuple(actual_messages) == messages
            assert tuple(actual_tools) == tools
            return parsed

    session = ProbeToolCallingSession()
    buffered = session.run_tool_turn(request, messages, tools)
    streamed = tuple(session.stream_tool_turn(request, messages, tools))

    assert buffered is parsed.terminal_result
    assert streamed == parsed.advisory_events
    assert isinstance(streamed[-1], TerminalEvent)
    assert streamed[-1].result is parsed.terminal_result
    assert session.parse_calls == 2


def test_tool_calling_session_base_rejects_duck_typed_and_subclassed_parsed_outcomes() -> None:
    request = _request()
    messages = _messages()
    tools = _tools()
    parsed = parse_openai_chat_sse(
        [_tool_turn_bytes()],
        request=request,
        messages=messages,
        tools=tools,
    )

    class ParsedToolTurnSubclass(ParsedToolTurn):
        pass

    class ExploitToolCallingSession(ToolCallingSessionBase):
        def __init__(self, outcome) -> None:
            self.outcome = outcome

        def _parse_tool_turn(self, actual_request, actual_messages, actual_tools):
            assert actual_request is request
            assert tuple(actual_messages) == messages
            assert tuple(actual_tools) == tools
            return self.outcome

    outcomes = (
        SimpleNamespace(
            advisory_events=parsed.advisory_events,
            terminal_result=parsed.terminal_result,
        ),
        ParsedToolTurnSubclass(parsed.advisory_events, parsed.terminal_result),
    )
    for outcome in outcomes:
        session = ExploitToolCallingSession(outcome)
        with pytest.raises(ModelToolProtocolError, match="parsed_turn_type_invalid"):
            session.run_tool_turn(request, messages, tools)
        with pytest.raises(ModelToolProtocolError, match="parsed_turn_type_invalid"):
            tuple(session.stream_tool_turn(request, messages, tools))


def test_scripted_session_cannot_override_the_canonical_public_projections() -> None:
    assert ScriptedToolTurnSession.run_tool_turn is ToolCallingSessionBase.run_tool_turn
    assert ScriptedToolTurnSession.stream_tool_turn is ToolCallingSessionBase.stream_tool_turn
    assert ScriptedToolTurnSession._parse_tool_turn is not ToolCallingSessionBase._parse_tool_turn


def test_d0a_session_projection_ownership_row_preserves_all_seven_columns() -> None:
    document = (
        Path(__file__).resolve().parents[1] / "docs/TRACK_D_D0A_PROVIDER_NEUTRAL_RUNTIME_IMPLEMENTATION.md"
    ).read_text()
    header = next(line for line in document.splitlines() if line.startswith("| Contract | Owner / source of truth |"))
    session_row = next(line for line in document.splitlines() if line.startswith("| Session projections |"))

    header_cells = [cell.strip() for cell in header.strip("|").split("|")]
    session_cells = [cell.strip() for cell in session_row.strip("|").split("|")]

    assert len(header_cells) == 7
    assert len(session_cells) == len(header_cells)
    assert session_cells[3] == "scripted replay and future non-live/live session adapters"
    assert session_cells[4].startswith("Separate buffered/stream parsing")


def test_parsed_tool_turn_rejects_non_exact_container_event_and_result_types() -> None:
    payload = _tool_turn_bytes()
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    class AdvisoryEventTuple(tuple):
        pass

    class TerminalEventSubclass(TerminalEvent):
        pass

    with pytest.raises(ModelToolProtocolError, match="advisory_events_type_invalid"):
        ParsedToolTurn(
            advisory_events=list(parsed.advisory_events),  # type: ignore[arg-type]
            terminal_result=parsed.terminal_result,
        )
    with pytest.raises(ModelToolProtocolError, match="advisory_events_type_invalid"):
        ParsedToolTurn(
            advisory_events=AdvisoryEventTuple(parsed.advisory_events),  # type: ignore[arg-type]
            terminal_result=parsed.terminal_result,
        )
    with pytest.raises(ModelToolProtocolError, match="advisory_event_type_invalid:0"):
        ParsedToolTurn(
            advisory_events=(SimpleNamespace(event_type="text_delta", text="duck"), *parsed.advisory_events[1:]),  # type: ignore[arg-type]
            terminal_result=parsed.terminal_result,
        )
    with pytest.raises(ModelToolProtocolError, match="advisory_event_type_invalid:0"):
        ParsedToolTurn(
            advisory_events=(TerminalEventSubclass(parsed.terminal_result),),
            terminal_result=parsed.terminal_result,
        )

    duck_result = SimpleNamespace(canonical_request_sha256=parsed.terminal_result.canonical_request_sha256)
    with pytest.raises(ModelToolProtocolError, match="terminal_result_type_invalid"):
        ParsedToolTurn(
            advisory_events=(TerminalEvent(duck_result),),  # type: ignore[arg-type]
            terminal_result=duck_result,  # type: ignore[arg-type]
        )


def test_parsed_tool_turn_requires_one_final_terminal_with_object_identical_result() -> None:
    payload = _tool_turn_bytes()
    parsed = parse_openai_chat_sse(
        [payload],
        request=_request(transcript=payload),
        messages=_messages(),
        tools=_tools(),
    )

    with pytest.raises(ModelToolProtocolError, match="terminal_event_missing"):
        ParsedToolTurn(
            advisory_events=parsed.advisory_events[:-1],
            terminal_result=parsed.terminal_result,
        )

    with pytest.raises(ModelToolProtocolError, match="terminal_event_not_final:0"):
        ParsedToolTurn(
            advisory_events=(TerminalEvent(parsed.terminal_result), *parsed.advisory_events),
            terminal_result=parsed.terminal_result,
        )

    equal_but_distinct_result = replace(parsed.terminal_result)
    assert equal_but_distinct_result == parsed.terminal_result
    assert equal_but_distinct_result is not parsed.terminal_result
    with pytest.raises(ModelToolProtocolError, match="terminal_event_result_identity_mismatch"):
        ParsedToolTurn(
            advisory_events=(*parsed.advisory_events[:-1], TerminalEvent(equal_but_distinct_result)),
            terminal_result=parsed.terminal_result,
        )


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

    def unread_iterable():
        raise AssertionError("live fence must run before consuming caller iterables")
        yield None

    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        session.run_tool_turn(
            with_provider_mode(request, "live"),
            unread_iterable(),  # type: ignore[arg-type]
            unread_iterable(),  # type: ignore[arg-type]
        )
    with pytest.raises(ModelRouteExecutionRejected, match="live_unavailable"):
        tuple(
            session.stream_tool_turn(
                with_provider_mode(request, "live"),
                unread_iterable(),  # type: ignore[arg-type]
                unread_iterable(),  # type: ignore[arg-type]
            )
        )


def test_model_usage_has_one_provider_neutral_class_owner_and_compatibility_alias() -> None:
    assert model_tool_runtime_module.ModelUsage is ModelUsage
    assert OpenAIModelUsage is ModelUsage
    assert ModelUsage.__module__ == "sourcing_agent.model_usage"
    assert [item.name for item in fields(ModelUsage)] == [
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
    ]

    repository_root = Path(__file__).resolve().parents[1]
    production_root = repository_root / "src" / "sourcing_agent"
    owners = []
    for path in sorted(production_root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        if any(isinstance(node, ast.ClassDef) and node.name == "ModelUsage" for node in ast.walk(tree)):
            owners.append(str(path.relative_to(repository_root)))
    assert owners == ["src/sourcing_agent/model_usage.py"]


def test_model_usage_is_immutable_and_serializes_only_reported_fields() -> None:
    values = {
        "input_tokens": 12,
        "output_tokens": 7,
        "total_tokens": 19,
        "cached_input_tokens": 2,
        "reasoning_output_tokens": 3,
    }
    usage = ModelUsage(**values)
    assert usage.to_record() == values
    assert OpenAIModelUsage(**values).to_record() == values
    assert ModelUsage().to_record() == {}
    with pytest.raises(FrozenInstanceError):
        usage.input_tokens = 13  # type: ignore[misc]


@pytest.mark.parametrize(
    "field_name",
    ("input_tokens", "output_tokens", "total_tokens", "cached_input_tokens", "reasoning_output_tokens"),
)
@pytest.mark.parametrize("invalid_value", (-1, True, False, 1.5, "1"))
def test_model_usage_rejects_negative_bool_and_non_integer_values(field_name: str, invalid_value: object) -> None:
    with pytest.raises(ModelUsageValidationError, match=f"model_usage_value_invalid:{field_name}"):
        ModelUsage(**{field_name: invalid_value})  # type: ignore[arg-type]


def test_deleted_temporary_usage_type_has_zero_production_references() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    production_root = repository_root / "src" / "sourcing_agent"
    deleted_name = "Model" + "Turn" + "Usage"
    assert not hasattr(model_tool_runtime_module, deleted_name)
    assert deleted_name not in model_tool_runtime_module.__all__
    violations = {
        str(path.relative_to(repository_root)): tuple(
            index for index, line in enumerate(path.read_text().splitlines(), start=1) if deleted_name in line
        )
        for path in sorted(production_root.rglob("*.py"))
        if deleted_name in path.read_text()
    }
    assert violations == {}


def test_d0a_modules_have_no_transport_or_environment_dependency() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "sourcing_agent"
    source = "\n".join(
        (root / filename).read_text()
        for filename in ("model_route_registry.py", "model_tool_runtime.py", "model_usage.py")
    )

    assert "import requests" not in source
    assert "urllib" not in source
    assert "os.environ" not in source
    assert "assert_live_provider_access_allowed" not in source

    usage_imports = {
        (node.module or "") if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(ast.parse((root / "model_usage.py").read_text()))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert usage_imports == {"__future__", "dataclasses"}


def test_canonical_payload_exposes_hash_inputs_but_not_message_content() -> None:
    payload = canonical_tool_turn_request_payload(_request(), _messages(), _tools())
    encoded = json.dumps(payload, ensure_ascii=False)

    assert payload["provider_mode"] == "scripted"
    assert payload["runtime_namespace"] == "test:model-tool-runtime"
    assert len(payload["messages_digest"]) == 64
    assert len(payload["tools_schema_digest"]) == 64
    assert "Find synthetic research evidence" not in encoded
