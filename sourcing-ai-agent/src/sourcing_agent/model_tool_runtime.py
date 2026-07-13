"""Provider-neutral, non-live model tool-turn substrate for Track D D0a.

The module owns typed messages/tools/events, canonical request identity, a
bounded OpenAI-chat SSE *parser*, and synthetic scripted replay.  It contains no
HTTP client, credential lookup, environment-derived live switch, or business
side-effect adapter. Stream events are advisory; a terminal ``ToolTurnResult``
only reports whether its validated shape may enter a future policy gate. D0a
never authorizes an effect.
"""

from __future__ import annotations

import codecs
import hashlib
import json
import math
import re
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Literal, Mapping, TypeAlias

from .model_route_registry import (
    ModelRouteExecutionRejected,
    ModelRouteSpec,
    assert_d0a_route_execution_allowed,
)

MODEL_TURN_MESSAGE_SCHEMA_VERSION = "model_turn_message_v1"
MODEL_TOOL_REQUEST_HASH_SCHEMA_VERSION = "model_tool_request_hash_v1"
MODEL_TOOL_TRANSCRIPT_SCHEMA_VERSION = "model_tool_transcript_v1"
D0A_EFFECT_AUTHORIZATION_AVAILABLE = False

MAX_MESSAGE_CONTENT_BYTES = 64 * 1024
MAX_TOTAL_MESSAGE_BYTES = 256 * 1024
MAX_TOOL_SCHEMA_BYTES = 128 * 1024
MAX_TOTAL_TOOL_SCHEMA_BYTES = 512 * 1024
MAX_TOOL_SPECS = 128
MAX_SSE_FRAME_BYTES = 256 * 1024
MAX_SSE_PENDING_BYTES = 512 * 1024
MAX_SSE_TOTAL_BYTES = 4 * 1024 * 1024
MAX_SSE_CHUNKS = 128 * 1024
MAX_SSE_LINES = 64 * 1024
MAX_SSE_FRAMES = 4096
MAX_TOOL_ARGUMENT_BYTES = 64 * 1024
MAX_TOOL_CALLS = 16
MAX_TEXT_BYTES = 256 * 1024
MAX_MODEL_OUTPUT_TOKENS = 1_000_000

_SUPPORTED_SCHEMA_KEYS = frozenset(
    {
        "$schema",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "const",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "minItems",
        "maxItems",
    }
)
_SUPPORTED_JSON_TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
_POLICY_EVALUABLE_TERMINAL_REASONS = frozenset({"end_turn", "tool_calls"})


class ModelToolRuntimeError(RuntimeError):
    """Base class for deterministic D0a contract failures."""


class ModelToolProtocolError(ModelToolRuntimeError):
    """Raised for malformed, incomplete, or identity-inconsistent stream data."""


class ModelToolSchemaError(ModelToolRuntimeError):
    """Raised when a tool schema or model-produced argument object is invalid."""


class ScriptedToolReplayError(ModelToolRuntimeError):
    """Raised when a transcript cannot prove request/scope identity."""


class ModelToolRequestBindingError(ModelToolRuntimeError):
    """Raised when a request conflicts with its checked-in route declaration."""


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
TerminalReason: TypeAlias = Literal["end_turn", "tool_calls", "length", "content_filter"]
UsageStatus: TypeAlias = Literal["reported", "unavailable", "invalid"]

_FINISH_REASON_MAP: dict[str, TerminalReason] = {
    "stop": "end_turn",
    "tool_calls": "tool_calls",
    "length": "length",
    "content_filter": "content_filter",
}


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _json_loads_strict(value: str) -> JsonValue:
    return json.loads(value, parse_constant=_reject_json_constant)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            _thaw_json(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ModelToolRuntimeError("model_tool_value_not_canonical_json") from exc


def _freeze_json(value: JsonValue) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ModelToolRuntimeError("model_tool_json_object_keys_must_be_strings")
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(child) for child in value]
    return value


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _required_text(field_name: str, value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ModelToolRuntimeError(f"model_tool_invalid_{field_name}")
    return value


def _required_sha256(field_name: str, value: str) -> str:
    normalized = _required_text(field_name, value)
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ModelToolRuntimeError(f"model_tool_invalid_{field_name}_sha256")
    return normalized


def _bounded_text(field_name: str, value: str, *, maximum_bytes: int = MAX_MESSAGE_CONTENT_BYTES) -> str:
    normalized = _required_text(field_name, value)
    if len(normalized.encode("utf-8")) > maximum_bytes:
        raise ModelToolRuntimeError(f"model_tool_{field_name}_too_large")
    return normalized


@dataclass(frozen=True, slots=True)
class ToolCallRecord:
    provider_call_id: str
    name: str
    arguments_json: str
    occurrence_ordinal: int = 1

    def __post_init__(self) -> None:
        _required_text("tool_call_id", self.provider_call_id)
        _required_text("tool_name", self.name)
        if self.occurrence_ordinal < 1:
            raise ModelToolRuntimeError("model_tool_occurrence_ordinal_invalid")
        try:
            arguments = _json_loads_strict(self.arguments_json)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ModelToolSchemaError("model_tool_arguments_invalid_json") from exc
        if not isinstance(arguments, dict):
            raise ModelToolSchemaError("model_tool_arguments_must_be_object")
        canonical = _canonical_json(arguments)
        if canonical != self.arguments_json:
            raise ModelToolSchemaError("model_tool_arguments_not_canonical")
        if len(canonical.encode("utf-8")) > MAX_TOOL_ARGUMENT_BYTES:
            raise ModelToolSchemaError("model_tool_arguments_too_large")

    @classmethod
    def from_arguments(
        cls,
        *,
        provider_call_id: str,
        name: str,
        arguments: Mapping[str, Any],
        occurrence_ordinal: int = 1,
    ) -> ToolCallRecord:
        return cls(
            provider_call_id=provider_call_id,
            name=name,
            arguments_json=_canonical_json(dict(arguments)),
            occurrence_ordinal=occurrence_ordinal,
        )

    @property
    def arguments(self) -> dict[str, JsonValue]:
        parsed = _json_loads_strict(self.arguments_json)
        assert isinstance(parsed, dict)
        return parsed

    @property
    def arguments_sha256(self) -> str:
        return hashlib.sha256(self.arguments_json.encode("utf-8")).hexdigest()

    def to_record(self) -> dict[str, object]:
        return {
            "provider_call_id": self.provider_call_id,
            "name": self.name,
            "arguments": self.arguments,
            "arguments_sha256": self.arguments_sha256,
            "occurrence_ordinal": self.occurrence_ordinal,
        }


@dataclass(frozen=True, slots=True)
class SystemMessage:
    text: str
    message_type: Literal["system"] = field(init=False, default="system")

    def __post_init__(self) -> None:
        _bounded_text("system_message", self.text)


@dataclass(frozen=True, slots=True)
class UserMessage:
    text: str
    message_type: Literal["user"] = field(init=False, default="user")

    def __post_init__(self) -> None:
        _bounded_text("user_message", self.text)


@dataclass(frozen=True, slots=True)
class AssistantTextMessage:
    text: str
    message_type: Literal["assistant_text"] = field(init=False, default="assistant_text")

    def __post_init__(self) -> None:
        _bounded_text("assistant_message", self.text)


@dataclass(frozen=True, slots=True)
class AssistantToolCallsMessage:
    calls: tuple[ToolCallRecord, ...]
    message_type: Literal["assistant_tool_calls"] = field(init=False, default="assistant_tool_calls")

    def __post_init__(self) -> None:
        if not isinstance(self.calls, tuple) or not self.calls:
            raise ModelToolRuntimeError("model_tool_assistant_calls_required")
        if any(not isinstance(call, ToolCallRecord) for call in self.calls):
            raise ModelToolRuntimeError("model_tool_assistant_calls_invalid")
        if len(self.calls) > MAX_TOOL_CALLS:
            raise ModelToolRuntimeError("model_tool_assistant_calls_too_many")


@dataclass(frozen=True, slots=True)
class ToolResultMessage:
    tool_call_id: str
    content: str
    is_error: bool = False
    message_type: Literal["tool_result"] = field(init=False, default="tool_result")

    def __post_init__(self) -> None:
        _required_text("tool_result_call_id", self.tool_call_id)
        _bounded_text("tool_result_content", self.content)


ModelTurnMessage: TypeAlias = (
    SystemMessage | UserMessage | AssistantTextMessage | AssistantToolCallsMessage | ToolResultMessage
)


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    schema_version: str
    approval_policy: str
    budget_required: bool

    def __post_init__(self) -> None:
        _required_text("tool_name", self.name)
        _bounded_text("tool_description", self.description, maximum_bytes=16 * 1024)
        _required_text("tool_schema_version", self.schema_version)
        _required_text("tool_approval_policy", self.approval_policy)
        if not isinstance(self.budget_required, bool):
            raise ModelToolSchemaError("model_tool_budget_required_invalid")
        try:
            encoded_schema = _canonical_json(self.input_schema)
            if len(encoded_schema.encode("utf-8")) > MAX_TOOL_SCHEMA_BYTES:
                raise ModelToolSchemaError("model_tool_schema_too_large")
            copied_schema = _json_loads_strict(encoded_schema)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ModelToolSchemaError("model_tool_schema_not_json") from exc
        if not isinstance(copied_schema, dict):
            raise ModelToolSchemaError("model_tool_schema_must_be_object")
        _validate_schema_definition(copied_schema, path="$", require_object=True)
        object.__setattr__(self, "input_schema", _freeze_json(copied_schema))

    def to_wire_record(self) -> dict[str, object]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": _thaw_json(self.input_schema),
            },
        }

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": _thaw_json(self.input_schema),
            "schema_version": self.schema_version,
            "approval_policy": self.approval_policy,
            "budget_required": self.budget_required,
        }


@dataclass(frozen=True, slots=True)
class ModelTurnUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_input_tokens",
            "reasoning_output_tokens",
        ):
            value = getattr(self, field_name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ModelToolProtocolError(f"model_tool_usage_value_invalid:{field_name}")

    def to_record(self) -> dict[str, int]:
        return {
            key: value
            for key, value in (
                ("input_tokens", self.input_tokens),
                ("output_tokens", self.output_tokens),
                ("total_tokens", self.total_tokens),
                ("cached_input_tokens", self.cached_input_tokens),
                ("reasoning_output_tokens", self.reasoning_output_tokens),
            )
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class ModelIdentity:
    requested_model: str
    response_model: str
    effective_model: str
    provenance: Literal["provider_response"] = "provider_response"

    def __post_init__(self) -> None:
        requested = _required_text("requested_model", self.requested_model)
        response = _required_text("response_model", self.response_model)
        effective = _required_text("effective_model", self.effective_model)
        if requested != response or response != effective:
            raise ModelToolProtocolError("model_tool_identity_not_exact")
        if self.provenance != "provider_response":
            raise ModelToolProtocolError("model_tool_identity_provenance_invalid")


@dataclass(frozen=True, slots=True)
class ToolTurnResult:
    text: str
    tool_calls: tuple[ToolCallRecord, ...]
    usage: ModelTurnUsage
    usage_status: UsageStatus
    model_identity: ModelIdentity
    terminal_reason: TerminalReason
    provider_call_id: str | None
    route_id: str
    workspace_id: str
    actor_id: str
    runtime_namespace: str
    provider_mode: str
    canonical_request_sha256: str

    def __post_init__(self) -> None:
        if len(self.text.encode("utf-8")) > MAX_TEXT_BYTES:
            raise ModelToolProtocolError("model_tool_text_too_large")
        for field_name in ("route_id", "workspace_id", "actor_id", "runtime_namespace"):
            _required_text(field_name, getattr(self, field_name))
        _required_sha256("canonical_request", self.canonical_request_sha256)
        if self.provider_mode not in {"simulate", "scripted"}:
            raise ModelToolProtocolError(f"model_tool_result_mode_invalid:{self.provider_mode}")
        if self.provider_call_id is not None:
            _required_text("provider_call_id", self.provider_call_id)
        if self.usage_status not in {"reported", "unavailable", "invalid"}:
            raise ModelToolProtocolError(f"model_tool_usage_status_invalid:{self.usage_status}")
        if self.terminal_reason not in _FINISH_REASON_MAP.values():
            raise ModelToolProtocolError(f"model_tool_terminal_reason_invalid:{self.terminal_reason}")
        if len(self.tool_calls) > MAX_TOOL_CALLS:
            raise ModelToolProtocolError("model_tool_calls_too_many")
        if not isinstance(self.tool_calls, tuple) or any(
            not isinstance(call, ToolCallRecord) for call in self.tool_calls
        ):
            raise ModelToolProtocolError("model_tool_calls_invalid")
        if self.usage_status == "reported" and not self.usage.to_record():
            raise ModelToolProtocolError("model_tool_reported_usage_empty")
        if self.usage_status == "unavailable" and self.usage.to_record():
            raise ModelToolProtocolError("model_tool_unavailable_usage_present")
        if self.terminal_reason == "end_turn" and self.tool_calls:
            raise ModelToolProtocolError("model_tool_end_turn_with_calls")
        if self.terminal_reason == "tool_calls" and not self.tool_calls:
            raise ModelToolProtocolError("model_tool_calls_finish_without_calls")

    @property
    def eligible_for_policy_evaluation(self) -> bool:
        """Whether this shape may enter a future durable owner policy gate.

        This is not permission, approval, budget authorization, result-slot
        acceptance, or effect execution. D0a has no effect authorization path.
        """

        if not self.provider_call_id or self.terminal_reason not in _POLICY_EVALUABLE_TERMINAL_REASONS:
            return False
        if self.terminal_reason == "end_turn":
            return not self.tool_calls
        return bool(self.tool_calls)

    @property
    def canonical_outcome_sha256(self) -> str:
        return _sha256_json(
            {
                "terminal_reason": self.terminal_reason,
                "text": self.text,
                "tool_calls": [call.to_record() for call in self.tool_calls],
            }
        )


@dataclass(frozen=True, slots=True)
class TextDeltaEvent:
    text: str
    event_type: Literal["text_delta"] = field(init=False, default="text_delta")


@dataclass(frozen=True, slots=True)
class ToolCallPartialEvent:
    tool_index: int
    call_id_fragment: str
    name_fragment: str
    arguments_fragment: str
    event_type: Literal["tool_call_partial"] = field(init=False, default="tool_call_partial")


@dataclass(frozen=True, slots=True)
class UsageEvent:
    usage: ModelTurnUsage
    usage_status: Literal["reported", "invalid"]
    event_type: Literal["usage"] = field(init=False, default="usage")


@dataclass(frozen=True, slots=True)
class StopEvent:
    terminal_reason: TerminalReason
    event_type: Literal["stop"] = field(init=False, default="stop")


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    message: str
    event_type: Literal["error"] = field(init=False, default="error")


@dataclass(frozen=True, slots=True)
class TerminalEvent:
    result: ToolTurnResult
    event_type: Literal["terminal"] = field(init=False, default="terminal")


AgentTurnEvent: TypeAlias = TextDeltaEvent | ToolCallPartialEvent | UsageEvent | StopEvent | ErrorEvent | TerminalEvent


@dataclass(frozen=True, slots=True)
class ParsedToolTurn:
    advisory_events: tuple[AgentTurnEvent, ...]
    terminal_result: ToolTurnResult

    def __post_init__(self) -> None:
        if not self.advisory_events or not isinstance(self.advisory_events[-1], TerminalEvent):
            raise ModelToolProtocolError("model_tool_terminal_event_missing")
        terminal_event = self.advisory_events[-1]
        assert isinstance(terminal_event, TerminalEvent)
        if terminal_event.result != self.terminal_result:
            raise ModelToolProtocolError("model_tool_terminal_event_result_mismatch")


@dataclass(frozen=True, slots=True)
class ToolTurnRequest:
    route_id: str
    route_revision: str
    effective_route_snapshot_digest: str
    provider: str
    requested_model: str
    api_style: str
    max_tokens: int
    tool_choice: str
    stream_options: Mapping[str, Any]
    prompt_policy_version: str
    permission_scope_revision: str
    outbound_policy_revision: str
    model_safe_schema_revision: str
    workspace_id: str
    actor_id: str
    permission_scope: str
    runtime_namespace: str
    provider_mode: str
    transcript_digest: str

    def __post_init__(self) -> None:
        for field_name in (
            "route_id",
            "route_revision",
            "effective_route_snapshot_digest",
            "provider",
            "requested_model",
            "api_style",
            "tool_choice",
            "prompt_policy_version",
            "permission_scope_revision",
            "outbound_policy_revision",
            "model_safe_schema_revision",
            "workspace_id",
            "actor_id",
            "permission_scope",
            "runtime_namespace",
            "provider_mode",
            "transcript_digest",
        ):
            _required_text(field_name, getattr(self, field_name))
        if (
            isinstance(self.max_tokens, bool)
            or not isinstance(self.max_tokens, int)
            or self.max_tokens <= 0
            or self.max_tokens > MAX_MODEL_OUTPUT_TOKENS
        ):
            raise ModelToolRuntimeError("model_tool_max_tokens_invalid")
        _required_sha256("route_revision", self.route_revision)
        _required_sha256("effective_route_snapshot_digest", self.effective_route_snapshot_digest)
        _required_sha256("transcript_digest", self.transcript_digest)
        if self.provider_mode != self.provider_mode.lower():
            raise ModelToolRuntimeError("model_tool_provider_mode_noncanonical")
        copied_options = _json_loads_strict(_canonical_json(self.stream_options))
        if not isinstance(copied_options, dict):
            raise ModelToolRuntimeError("model_tool_stream_options_invalid")
        if self.tool_choice != "auto":
            raise ModelToolRuntimeError("model_tool_tool_choice_unsupported_d0a")
        if copied_options != {"include_usage": True}:
            raise ModelToolRuntimeError("model_tool_stream_options_unsupported_d0a")
        object.__setattr__(self, "stream_options", _freeze_json(copied_options))


def _assert_d0a_request_route_binding(request: ToolTurnRequest) -> ModelRouteSpec:
    route = assert_d0a_route_execution_allowed(
        route_id=request.route_id,
        provider_mode=request.provider_mode,
        required_capabilities={"stream", "tools", "usage", "identity_check"},
    )
    expected_route_fields = {
        "route_revision": (request.route_revision, route.revision),
        "provider": (request.provider, route.provider),
        "requested_model": (request.requested_model, route.model),
        "api_style": (request.api_style, route.api_style),
    }
    for field_name, (actual, expected) in expected_route_fields.items():
        if actual != expected:
            raise ModelToolRequestBindingError(f"model_tool_route_field_mismatch:{field_name}:{actual}:{expected}")
    return route


def _message_record(message: ModelTurnMessage) -> dict[str, object]:
    if isinstance(message, (SystemMessage, UserMessage, AssistantTextMessage)):
        return {"type": message.message_type, "text": message.text}
    if isinstance(message, AssistantToolCallsMessage):
        return {"type": message.message_type, "calls": [call.to_record() for call in message.calls]}
    if isinstance(message, ToolResultMessage):
        return {
            "type": message.message_type,
            "tool_call_id": message.tool_call_id,
            "content": message.content,
            "is_error": message.is_error,
        }
    raise ModelToolRuntimeError("model_tool_message_type_unsupported")


def _collect_message_records(
    messages: Iterable[ModelTurnMessage],
) -> tuple[tuple[ModelTurnMessage, ...], list[dict[str, object]]]:
    """Collect messages with an exact incremental canonical-JSON byte bound."""

    collected: list[ModelTurnMessage] = []
    records: list[dict[str, object]] = []
    encoded_array_bytes = 2  # ``[]``
    for message in messages:
        record = _message_record(message)
        record_bytes = len(_canonical_json(record).encode("utf-8"))
        candidate_bytes = encoded_array_bytes + (1 if records else 0) + record_bytes
        if candidate_bytes > MAX_TOTAL_MESSAGE_BYTES:
            raise ModelToolRuntimeError("model_tool_messages_too_large")
        encoded_array_bytes = candidate_bytes
        collected.append(message)
        records.append(record)
    if not records:
        raise ModelToolRuntimeError("model_tool_messages_required")
    return tuple(collected), records


def _collect_tool_records(
    tools: Iterable[ToolSpec],
) -> tuple[tuple[ToolSpec, ...], list[dict[str, object]]]:
    """Collect a bounded tool registry without exhausting an untrusted iterable."""

    collected: list[ToolSpec] = []
    records: list[dict[str, object]] = []
    names: set[str] = set()
    encoded_array_bytes = 2  # ``[]``
    for tool in tools:
        if len(collected) >= MAX_TOOL_SPECS:
            raise ModelToolRuntimeError("model_tool_tools_too_many")
        if not isinstance(tool, ToolSpec):
            raise ModelToolRuntimeError("model_tool_tool_spec_invalid")
        if tool.name in names:
            raise ModelToolRuntimeError(f"model_tool_duplicate_tool_name:{tool.name}")
        record = tool.to_fingerprint_record()
        record_bytes = len(_canonical_json(record).encode("utf-8"))
        candidate_bytes = encoded_array_bytes + (1 if records else 0) + record_bytes
        if candidate_bytes > MAX_TOTAL_TOOL_SCHEMA_BYTES:
            raise ModelToolRuntimeError("model_tool_tools_schema_too_large")
        encoded_array_bytes = candidate_bytes
        collected.append(tool)
        records.append(record)
        names.add(tool.name)
    if not records:
        raise ModelToolRuntimeError("model_tool_tools_required")
    return tuple(collected), records


def _canonical_tool_turn_request_payload_from_records(
    request: ToolTurnRequest,
    message_records: list[dict[str, object]],
    tool_records: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": MODEL_TOOL_REQUEST_HASH_SCHEMA_VERSION,
        "route_id": request.route_id,
        "route_revision": request.route_revision,
        "effective_route_snapshot_digest": request.effective_route_snapshot_digest,
        "provider": request.provider,
        "model": request.requested_model,
        "api_style": request.api_style,
        "max_tokens": request.max_tokens,
        "tool_choice": request.tool_choice,
        "stream_options": _thaw_json(request.stream_options),
        "message_model_version": MODEL_TURN_MESSAGE_SCHEMA_VERSION,
        "tools_schema_digest": _sha256_json(tool_records),
        "prompt_policy_version": request.prompt_policy_version,
        "permission_scope_revision": request.permission_scope_revision,
        "outbound_policy_revision": request.outbound_policy_revision,
        "model_safe_schema_revision": request.model_safe_schema_revision,
        "messages_digest": _sha256_json(message_records),
        "workspace_id": request.workspace_id,
        "actor_id": request.actor_id,
        "permission_scope": request.permission_scope,
        "transcript_digest": request.transcript_digest,
        "runtime_namespace": request.runtime_namespace,
        "provider_mode": request.provider_mode,
    }


def canonical_tool_turn_request_payload(
    request: ToolTurnRequest,
    messages: Iterable[ModelTurnMessage],
    tools: Iterable[ToolSpec],
) -> dict[str, object]:
    _, message_records = _collect_message_records(messages)
    _, tool_records = _collect_tool_records(tools)
    return _canonical_tool_turn_request_payload_from_records(request, message_records, tool_records)


def canonical_tool_turn_request_hash(
    request: ToolTurnRequest,
    messages: Iterable[ModelTurnMessage],
    tools: Iterable[ToolSpec],
) -> str:
    return _sha256_json(canonical_tool_turn_request_payload(request, messages, tools))


def _validate_schema_definition(schema: dict[str, Any], *, path: str, require_object: bool = False) -> None:
    unknown = sorted(set(schema) - _SUPPORTED_SCHEMA_KEYS)
    if unknown:
        raise ModelToolSchemaError(f"model_tool_schema_keyword_unsupported:{path}:{','.join(unknown)}")
    schema_type = schema.get("type")
    if not isinstance(schema_type, str) or schema_type not in _SUPPORTED_JSON_TYPES:
        raise ModelToolSchemaError(f"model_tool_schema_type_invalid:{path}")
    if require_object and schema_type != "object":
        raise ModelToolSchemaError("model_tool_root_schema_must_be_object")

    for metadata_key in ("$schema", "title", "description"):
        if metadata_key in schema and not isinstance(schema[metadata_key], str):
            raise ModelToolSchemaError(f"model_tool_schema_metadata_invalid:{path}:{metadata_key}")

    constraints_by_type = {
        "object": {"properties", "required", "additionalProperties"},
        "array": {"items", "minItems", "maxItems"},
        "string": {"minLength", "maxLength", "pattern"},
        "integer": {"minimum", "maximum"},
        "number": {"minimum", "maximum"},
        "boolean": set(),
        "null": set(),
    }
    common_keys = {"$schema", "title", "description", "type", "enum", "const"}
    inapplicable = sorted(set(schema) - common_keys - constraints_by_type[schema_type])
    if inapplicable:
        raise ModelToolSchemaError(
            f"model_tool_schema_keyword_inapplicable:{path}:{schema_type}:{','.join(inapplicable)}"
        )

    if "enum" in schema and (not isinstance(schema["enum"], list) or not schema["enum"]):
        raise ModelToolSchemaError(f"model_tool_schema_enum_invalid:{path}")
    if "enum" in schema:
        enum_values = schema["enum"]
        assert isinstance(enum_values, list)
        if any(not _json_type_matches(value, schema_type) for value in enum_values):
            raise ModelToolSchemaError(f"model_tool_schema_enum_type_mismatch:{path}")
        if len({_canonical_json(value) for value in enum_values}) != len(enum_values):
            raise ModelToolSchemaError(f"model_tool_schema_enum_duplicate:{path}")
    if "const" in schema and not _json_type_matches(schema["const"], schema_type):
        raise ModelToolSchemaError(f"model_tool_schema_const_type_mismatch:{path}")

    for minimum_key, maximum_key in (("minLength", "maxLength"), ("minItems", "maxItems")):
        minimum = schema.get(minimum_key)
        maximum = schema.get(maximum_key)
        if minimum is not None and (isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 0):
            raise ModelToolSchemaError(f"model_tool_schema_bound_invalid:{path}:{minimum_key}")
        if maximum is not None and (isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0):
            raise ModelToolSchemaError(f"model_tool_schema_bound_invalid:{path}:{maximum_key}")
        if isinstance(minimum, int) and isinstance(maximum, int) and minimum > maximum:
            raise ModelToolSchemaError(f"model_tool_schema_bound_order_invalid:{path}")
    minimum_number = schema.get("minimum")
    maximum_number = schema.get("maximum")
    for bound_name, bound in (("minimum", minimum_number), ("maximum", maximum_number)):
        if bound is not None and (
            isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound)
        ):
            raise ModelToolSchemaError(f"model_tool_schema_bound_invalid:{path}:{bound_name}")
    if (
        isinstance(minimum_number, (int, float))
        and not isinstance(minimum_number, bool)
        and isinstance(maximum_number, (int, float))
        and not isinstance(maximum_number, bool)
        and minimum_number > maximum_number
    ):
        raise ModelToolSchemaError(f"model_tool_schema_bound_order_invalid:{path}")
    if schema_type == "object":
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            raise ModelToolSchemaError(f"model_tool_schema_properties_invalid:{path}")
        additional = schema.get("additionalProperties", True)
        if not isinstance(additional, bool):
            raise ModelToolSchemaError(f"model_tool_schema_additional_properties_invalid:{path}")
        required = schema.get("required", [])
        if not isinstance(required, list) or any(not isinstance(item, str) or not item for item in required):
            raise ModelToolSchemaError(f"model_tool_schema_required_invalid:{path}")
        if len(required) != len(set(required)) or not set(required).issubset(properties):
            raise ModelToolSchemaError(f"model_tool_schema_required_unknown:{path}")
        for key, child in properties.items():
            if not isinstance(key, str) or not key or not isinstance(child, dict):
                raise ModelToolSchemaError(f"model_tool_schema_property_invalid:{path}")
            _validate_schema_definition(child, path=f"{path}.{key}")
    elif schema_type == "array":
        items = schema.get("items")
        if not isinstance(items, dict):
            raise ModelToolSchemaError(f"model_tool_schema_items_required:{path}")
        _validate_schema_definition(items, path=f"{path}[]")
    if "pattern" in schema:
        if not isinstance(schema["pattern"], str):
            raise ModelToolSchemaError(f"model_tool_schema_pattern_invalid:{path}")
        try:
            re.compile(schema["pattern"])
        except re.error as exc:
            raise ModelToolSchemaError(f"model_tool_schema_pattern_invalid:{path}") from exc


def _json_type_matches(value: JsonValue, expected: str) -> bool:
    if expected == "null":
        return value is None
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    if expected == "string":
        return isinstance(value, str)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    return False


def _validate_json_value(value: JsonValue, schema: dict[str, Any], *, path: str) -> None:
    expected_type = str(schema["type"])
    if not _json_type_matches(value, expected_type):
        raise ModelToolSchemaError(f"model_tool_argument_type_mismatch:{path}:{expected_type}")
    if "const" in schema and value != schema["const"]:
        raise ModelToolSchemaError(f"model_tool_argument_const_mismatch:{path}")
    if "enum" in schema and value not in schema["enum"]:
        raise ModelToolSchemaError(f"model_tool_argument_enum_mismatch:{path}")

    if isinstance(value, dict):
        properties = dict(schema.get("properties") or {})
        missing = sorted(set(schema.get("required") or []) - set(value))
        if missing:
            raise ModelToolSchemaError(f"model_tool_argument_required_missing:{path}:{','.join(missing)}")
        extras = sorted(set(value) - set(properties))
        if extras and schema.get("additionalProperties", True) is False:
            raise ModelToolSchemaError(f"model_tool_argument_extra_fields:{path}:{','.join(extras)}")
        for key, child_value in value.items():
            child_schema = properties.get(key)
            if child_schema is not None:
                _validate_json_value(child_value, child_schema, path=f"{path}.{key}")
    elif isinstance(value, list):
        minimum = schema.get("minItems")
        maximum = schema.get("maxItems")
        if isinstance(minimum, int) and len(value) < minimum:
            raise ModelToolSchemaError(f"model_tool_argument_min_items:{path}")
        if isinstance(maximum, int) and len(value) > maximum:
            raise ModelToolSchemaError(f"model_tool_argument_max_items:{path}")
        item_schema = schema.get("items")
        assert isinstance(item_schema, dict)
        for index, item in enumerate(value):
            _validate_json_value(item, item_schema, path=f"{path}[{index}]")
    elif isinstance(value, str):
        minimum = schema.get("minLength")
        maximum = schema.get("maxLength")
        if isinstance(minimum, int) and len(value) < minimum:
            raise ModelToolSchemaError(f"model_tool_argument_min_length:{path}")
        if isinstance(maximum, int) and len(value) > maximum:
            raise ModelToolSchemaError(f"model_tool_argument_max_length:{path}")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, value) is None:
            raise ModelToolSchemaError(f"model_tool_argument_pattern_mismatch:{path}")
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if isinstance(minimum, (int, float)) and value < minimum:
            raise ModelToolSchemaError(f"model_tool_argument_minimum:{path}")
        if isinstance(maximum, (int, float)) and value > maximum:
            raise ModelToolSchemaError(f"model_tool_argument_maximum:{path}")


def _tool_specs_by_name(tools: Iterable[ToolSpec]) -> dict[str, ToolSpec]:
    mapping: dict[str, ToolSpec] = {}
    for tool in tools:
        if tool.name in mapping:
            raise ModelToolRuntimeError(f"model_tool_duplicate_tool_name:{tool.name}")
        mapping[tool.name] = tool
    if not mapping:
        raise ModelToolRuntimeError("model_tool_tools_required")
    return mapping


def _validated_transcript_chunks(
    chunks: Iterable[bytes],
    *,
    require_tuple: bool = False,
) -> tuple[bytes, ...]:
    if require_tuple and not isinstance(chunks, tuple):
        raise ScriptedToolReplayError("model_tool_transcript_chunks_must_be_tuple")
    collected: list[bytes] = []
    total_bytes = 0
    for chunk in chunks:
        if len(collected) >= MAX_SSE_CHUNKS:
            raise ModelToolProtocolError("model_tool_sse_chunks_too_many")
        if type(chunk) is not bytes:
            raise ModelToolProtocolError("model_tool_sse_chunks_must_be_bytes")
        total_bytes += len(chunk)
        if total_bytes > MAX_SSE_TOTAL_BYTES:
            raise ModelToolProtocolError("model_tool_sse_total_too_large")
        collected.append(chunk)
    if not collected:
        raise ModelToolProtocolError("model_tool_transcript_chunks_required")
    return tuple(collected)


def canonical_model_turn_transcript_sha256(chunks: Iterable[bytes]) -> str:
    """Hash exact SSE bytes independently of transport chunk boundaries."""

    digest = hashlib.sha256()
    for chunk in _validated_transcript_chunks(chunks):
        digest.update(chunk)
    return digest.hexdigest()


def _iter_sse_data_frames(chunks: Iterable[bytes]) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    pending = ""
    data_lines: list[str] = []
    data_frame_bytes = 0
    total_bytes = 0
    chunk_count = 0
    line_count = 0
    frame_count = 0
    try:
        for chunk in chunks:
            chunk_count += 1
            if chunk_count > MAX_SSE_CHUNKS:
                raise ModelToolProtocolError("model_tool_sse_chunks_too_many")
            if type(chunk) is not bytes:
                raise ModelToolProtocolError("model_tool_sse_chunks_must_be_bytes")
            total_bytes += len(chunk)
            if total_bytes > MAX_SSE_TOTAL_BYTES:
                raise ModelToolProtocolError("model_tool_sse_total_too_large")
            decoded = decoder.decode(chunk)
            pending += decoded
            complete_line_count = pending.count("\n")
            if line_count + complete_line_count > MAX_SSE_LINES:
                raise ModelToolProtocolError("model_tool_sse_lines_too_many")
            complete_lines = pending.split("\n")
            pending = complete_lines.pop()
            line_count += complete_line_count
            for raw_line in complete_lines:
                line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
                if line == "":
                    if data_lines:
                        frame = "\n".join(data_lines)
                        frame_count += 1
                        if frame_count > MAX_SSE_FRAMES:
                            raise ModelToolProtocolError("model_tool_sse_frames_too_many")
                        yield frame
                        data_lines = []
                        data_frame_bytes = 0
                    continue
                if line.startswith(":"):
                    continue
                field_name, separator, field_value = line.partition(":")
                if field_name != "data":
                    continue
                if separator and field_value.startswith(" "):
                    field_value = field_value[1:]
                candidate_frame_bytes = (
                    data_frame_bytes + (1 if data_lines else 0) + len(field_value.encode("utf-8"))
                )
                if candidate_frame_bytes > MAX_SSE_FRAME_BYTES:
                    raise ModelToolProtocolError("model_tool_sse_frame_too_large")
                data_lines.append(field_value)
                data_frame_bytes = candidate_frame_bytes
            # Complete lines are consumed before this bound is applied. A large
            # transport chunk containing many complete frames is therefore
            # equivalent to smaller chunking; only the unfinished line remains.
            if len(pending.encode("utf-8")) > MAX_SSE_PENDING_BYTES:
                raise ModelToolProtocolError("model_tool_sse_pending_too_large")
        pending += decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise ModelToolProtocolError("model_tool_sse_utf8_invalid") from exc
    if pending or data_lines:
        raise ModelToolProtocolError("model_tool_sse_incomplete_frame")


def _usage_value(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _parse_usage(raw_usage: Any) -> tuple[ModelTurnUsage, Literal["reported", "invalid"]]:
    if not isinstance(raw_usage, dict):
        return ModelTurnUsage(), "invalid"
    known_values = {
        "input_tokens": raw_usage.get("input_tokens", raw_usage.get("prompt_tokens")),
        "output_tokens": raw_usage.get("output_tokens", raw_usage.get("completion_tokens")),
        "total_tokens": raw_usage.get("total_tokens"),
    }
    input_details = raw_usage.get("input_tokens_details", raw_usage.get("prompt_tokens_details"))
    output_details = raw_usage.get("output_tokens_details", raw_usage.get("completion_tokens_details"))
    if isinstance(input_details, dict):
        known_values["cached_input_tokens"] = input_details.get("cached_tokens")
    if isinstance(output_details, dict):
        known_values["reasoning_output_tokens"] = output_details.get("reasoning_tokens")
    invalid = any(value is not None and _usage_value(value) is None for value in known_values.values())
    usage = ModelTurnUsage(**{key: _usage_value(value) for key, value in known_values.items()})
    if invalid or not usage.to_record():
        return usage, "invalid"
    return usage, "reported"


def _append_stable_fragment(current: str, incoming: str, *, field_name: str) -> str:
    if not incoming:
        return current
    if not current:
        return incoming
    if incoming == current:
        return current
    raise ModelToolProtocolError(f"model_tool_{field_name}_drift")


def parse_openai_chat_sse(
    chunks: Iterable[bytes],
    *,
    request: ToolTurnRequest,
    messages: Iterable[ModelTurnMessage],
    tools: Iterable[ToolSpec],
) -> ParsedToolTurn:
    """Parse bounded SSE bytes without performing transport.

    The parser accepts only the OpenAI-compatible chat delta shape declared by
    the pinned request.  Any incomplete, multi-choice, model-mismatched, or
    schema-invalid outcome fails closed.
    """

    _assert_d0a_request_route_binding(request)
    chunk_tuple = _validated_transcript_chunks(chunks)
    transcript_sha256 = canonical_model_turn_transcript_sha256(chunk_tuple)
    if transcript_sha256 != request.transcript_digest:
        raise ModelToolRequestBindingError("model_tool_transcript_content_digest_mismatch")
    _, message_records = _collect_message_records(messages)
    tool_tuple, tool_records = _collect_tool_records(tools)
    request_sha256 = _sha256_json(
        _canonical_tool_turn_request_payload_from_records(request, message_records, tool_records)
    )
    tool_specs = _tool_specs_by_name(tool_tuple)

    advisory_events: list[AgentTurnEvent] = []
    text_parts: list[str] = []
    text_size = 0
    call_fragments: dict[int, dict[str, str]] = {}
    response_model = ""
    provider_call_id = ""
    finish_reason = ""
    usage = ModelTurnUsage()
    usage_status: Literal["reported", "unavailable", "invalid"] = "unavailable"
    saw_done = False

    for frame in _iter_sse_data_frames(chunk_tuple):
        if saw_done:
            raise ModelToolProtocolError("model_tool_sse_data_after_done")
        if frame == "[DONE]":
            saw_done = True
            continue
        try:
            payload = _json_loads_strict(frame)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ModelToolProtocolError("model_tool_sse_json_invalid") from exc
        if not isinstance(payload, dict):
            raise ModelToolProtocolError("model_tool_sse_payload_not_object")
        if "error" in payload:
            raise ModelToolProtocolError("model_tool_provider_error_frame")

        raw_id = payload.get("id")
        if raw_id is not None:
            if not isinstance(raw_id, str) or not raw_id:
                raise ModelToolProtocolError("model_tool_provider_call_id_invalid")
            provider_call_id = _append_stable_fragment(
                provider_call_id,
                raw_id,
                field_name="provider_call_id",
            )
        raw_model = payload.get("model")
        if raw_model is not None:
            if not isinstance(raw_model, str) or not raw_model:
                raise ModelToolProtocolError("model_tool_response_model_invalid")
            response_model = _append_stable_fragment(response_model, raw_model, field_name="response_model")

        has_usage = "usage" in payload and payload.get("usage") is not None
        if has_usage:
            parsed_usage, parsed_status = _parse_usage(payload.get("usage"))
            if usage_status != "unavailable" and (parsed_usage != usage or parsed_status != usage_status):
                raise ModelToolProtocolError("model_tool_usage_drift")
            usage = parsed_usage
            usage_status = parsed_status
            advisory_events.append(UsageEvent(usage=usage, usage_status=parsed_status))

        choices = payload.get("choices", [])
        if not isinstance(choices, list):
            raise ModelToolProtocolError("model_tool_choices_invalid")
        if not choices:
            if not has_usage:
                raise ModelToolProtocolError("model_tool_empty_non_usage_frame")
            continue
        if finish_reason:
            raise ModelToolProtocolError("model_tool_choice_after_finish")
        if len(choices) != 1 or not isinstance(choices[0], dict):
            raise ModelToolProtocolError("model_tool_multiple_choices_not_supported")
        choice = choices[0]
        if choice.get("index") != 0:
            raise ModelToolProtocolError("model_tool_choice_index_invalid")
        unexpected_choice_fields = sorted(set(choice) - {"index", "delta", "finish_reason", "logprobs"})
        if unexpected_choice_fields or choice.get("logprobs") not in (None,):
            raise ModelToolProtocolError("model_tool_choice_fields_unsupported")
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            raise ModelToolProtocolError("model_tool_delta_invalid")
        unexpected_delta_fields = sorted(set(delta) - {"role", "content", "tool_calls"})
        if unexpected_delta_fields:
            raise ModelToolProtocolError(f"model_tool_delta_fields_unsupported:{','.join(unexpected_delta_fields)}")
        if delta.get("role") not in (None, "assistant"):
            raise ModelToolProtocolError("model_tool_delta_role_invalid")

        content = delta.get("content")
        if content is not None:
            if not isinstance(content, str):
                raise ModelToolProtocolError("model_tool_text_delta_invalid")
            text_size += len(content.encode("utf-8"))
            if text_size > MAX_TEXT_BYTES:
                raise ModelToolProtocolError("model_tool_text_too_large")
            text_parts.append(content)
            if content:
                advisory_events.append(TextDeltaEvent(text=content))

        raw_tool_calls = delta.get("tool_calls", [])
        if not isinstance(raw_tool_calls, list):
            raise ModelToolProtocolError("model_tool_call_delta_invalid")
        for raw_call in raw_tool_calls:
            if not isinstance(raw_call, dict):
                raise ModelToolProtocolError("model_tool_call_delta_invalid")
            if set(raw_call) - {"index", "id", "type", "function"}:
                raise ModelToolProtocolError("model_tool_call_fields_unsupported")
            tool_index = raw_call.get("index")
            if isinstance(tool_index, bool) or not isinstance(tool_index, int) or tool_index < 0:
                raise ModelToolProtocolError("model_tool_call_index_invalid")
            if tool_index >= MAX_TOOL_CALLS:
                raise ModelToolProtocolError("model_tool_calls_too_many")
            raw_type = raw_call.get("type")
            if raw_type not in (None, "function"):
                raise ModelToolProtocolError("model_tool_call_type_invalid")
            raw_function = raw_call.get("function", {})
            if not isinstance(raw_function, dict):
                raise ModelToolProtocolError("model_tool_call_function_invalid")
            if set(raw_function) - {"name", "arguments"}:
                raise ModelToolProtocolError("model_tool_call_function_fields_unsupported")
            call_id_fragment = raw_call.get("id", "")
            name_fragment = raw_function.get("name", "")
            arguments_fragment = raw_function.get("arguments", "")
            if not isinstance(call_id_fragment, str):
                raise ModelToolProtocolError("model_tool_call_fragment_invalid")
            if not isinstance(name_fragment, str):
                raise ModelToolProtocolError("model_tool_call_fragment_invalid")
            if not isinstance(arguments_fragment, str):
                raise ModelToolProtocolError("model_tool_call_fragment_invalid")
            state = call_fragments.setdefault(tool_index, {"id": "", "name": "", "arguments": ""})
            state["id"] = _append_stable_fragment(state["id"], call_id_fragment, field_name="call_id")
            state["name"] = _append_stable_fragment(state["name"], name_fragment, field_name="call_name")
            state["arguments"] += arguments_fragment
            if len(state["arguments"].encode("utf-8")) > MAX_TOOL_ARGUMENT_BYTES:
                raise ModelToolProtocolError("model_tool_arguments_too_large")
            advisory_events.append(
                ToolCallPartialEvent(
                    tool_index=tool_index,
                    call_id_fragment=call_id_fragment,
                    name_fragment=name_fragment,
                    arguments_fragment=arguments_fragment,
                )
            )

        raw_finish_reason = choice.get("finish_reason")
        if raw_finish_reason is not None:
            if not isinstance(raw_finish_reason, str) or raw_finish_reason not in _FINISH_REASON_MAP:
                raise ModelToolProtocolError("model_tool_finish_reason_invalid")
            finish_reason = raw_finish_reason
            advisory_events.append(StopEvent(terminal_reason=_FINISH_REASON_MAP[finish_reason]))

    if not saw_done:
        raise ModelToolProtocolError("model_tool_done_missing")
    if not finish_reason:
        raise ModelToolProtocolError("model_tool_finish_reason_missing")
    if not response_model:
        raise ModelToolProtocolError("model_tool_response_model_missing")
    if response_model != request.requested_model:
        raise ModelToolProtocolError(f"model_tool_response_model_mismatch:{request.requested_model}:{response_model}")

    expected_indices = list(range(len(call_fragments)))
    if sorted(call_fragments) != expected_indices:
        raise ModelToolProtocolError("model_tool_call_indices_not_contiguous")
    tool_calls: list[ToolCallRecord] = []
    seen_provider_call_ids: set[str] = set()
    occurrence_counts: dict[tuple[str, str], int] = {}
    for index in expected_indices:
        state = call_fragments[index]
        if not state["id"] or not state["name"] or not state["arguments"]:
            raise ModelToolProtocolError("model_tool_call_incomplete")
        if state["id"] in seen_provider_call_ids:
            raise ModelToolProtocolError("model_tool_provider_call_id_duplicate")
        seen_provider_call_ids.add(state["id"])
        spec = tool_specs.get(state["name"])
        if spec is None:
            raise ModelToolSchemaError(f"model_tool_name_not_served:{state['name']}")
        try:
            parsed_arguments = _json_loads_strict(state["arguments"])
        except (json.JSONDecodeError, ValueError) as exc:
            raise ModelToolSchemaError("model_tool_arguments_invalid_json") from exc
        if not isinstance(parsed_arguments, dict):
            raise ModelToolSchemaError("model_tool_arguments_must_be_object")
        input_schema = _thaw_json(spec.input_schema)
        assert isinstance(input_schema, dict)
        _validate_json_value(parsed_arguments, input_schema, path="$")
        canonical_arguments = _canonical_json(parsed_arguments)
        logical_key = (state["name"], hashlib.sha256(canonical_arguments.encode("utf-8")).hexdigest())
        occurrence_counts[logical_key] = occurrence_counts.get(logical_key, 0) + 1
        tool_calls.append(
            ToolCallRecord(
                provider_call_id=state["id"],
                name=state["name"],
                arguments_json=canonical_arguments,
                occurrence_ordinal=occurrence_counts[logical_key],
            )
        )

    terminal_reason = _FINISH_REASON_MAP[finish_reason]
    if terminal_reason == "end_turn" and tool_calls:
        raise ModelToolProtocolError("model_tool_end_turn_with_calls")
    if terminal_reason == "tool_calls" and not tool_calls:
        raise ModelToolProtocolError("model_tool_calls_finish_without_calls")

    result = ToolTurnResult(
        text="".join(text_parts),
        tool_calls=tuple(tool_calls),
        usage=usage,
        usage_status=usage_status,
        model_identity=ModelIdentity(
            requested_model=request.requested_model,
            response_model=response_model,
            effective_model=response_model,
        ),
        terminal_reason=terminal_reason,
        provider_call_id=provider_call_id or None,
        route_id=request.route_id,
        workspace_id=request.workspace_id,
        actor_id=request.actor_id,
        runtime_namespace=request.runtime_namespace,
        provider_mode=request.provider_mode,
        canonical_request_sha256=request_sha256,
    )
    events = (*advisory_events, TerminalEvent(result=result))
    return ParsedToolTurn(advisory_events=events, terminal_result=result)


@dataclass(frozen=True, slots=True)
class ScriptedToolTurnTranscript:
    request_sha256: str
    chunks: tuple[bytes, ...]
    synthetic: bool
    workspace_id: str = ""
    schema_version: str = MODEL_TOOL_TRANSCRIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _required_sha256("transcript_request", self.request_sha256)
        if type(self.synthetic) is not bool:
            raise ScriptedToolReplayError("model_tool_transcript_synthetic_must_be_bool")
        if self.schema_version != MODEL_TOOL_TRANSCRIPT_SCHEMA_VERSION:
            raise ScriptedToolReplayError(f"model_tool_transcript_schema_unsupported:{self.schema_version}")
        try:
            _validated_transcript_chunks(self.chunks, require_tuple=True)
        except ModelToolProtocolError as exc:
            raise ScriptedToolReplayError(str(exc)) from exc
        if not self.synthetic:
            _required_text("transcript_workspace_id", self.workspace_id)

    @property
    def content_sha256(self) -> str:
        return canonical_model_turn_transcript_sha256(self.chunks)


class ScriptedToolTurnSession:
    """Deterministically replay one identity-bound synthetic/non-live transcript."""

    def __init__(self, transcript: ScriptedToolTurnTranscript) -> None:
        self._transcript = transcript

    def _prepare(
        self,
        request: ToolTurnRequest,
        messages: tuple[ModelTurnMessage, ...],
        tools: tuple[ToolSpec, ...],
    ) -> ModelRouteSpec:
        route = _assert_d0a_request_route_binding(request)
        if request.transcript_digest != self._transcript.content_sha256:
            raise ScriptedToolReplayError("model_tool_transcript_content_digest_mismatch")
        request_sha256 = canonical_tool_turn_request_hash(request, messages, tools)
        if request_sha256 != self._transcript.request_sha256:
            raise ScriptedToolReplayError("model_tool_transcript_request_hash_mismatch")
        if not self._transcript.synthetic and self._transcript.workspace_id != request.workspace_id:
            raise ScriptedToolReplayError("model_tool_transcript_workspace_mismatch")
        return route

    def run_tool_turn(
        self,
        request: ToolTurnRequest,
        messages: Iterable[ModelTurnMessage],
        tools: Iterable[ToolSpec],
    ) -> ToolTurnResult:
        # Fence live/unknown routes before touching caller-owned iterables.
        _assert_d0a_request_route_binding(request)
        message_tuple, _ = _collect_message_records(messages)
        tool_tuple, _ = _collect_tool_records(tools)
        self._prepare(request, message_tuple, tool_tuple)
        result = parse_openai_chat_sse(
            self._transcript.chunks,
            request=request,
            messages=message_tuple,
            tools=tool_tuple,
        ).terminal_result
        if result.canonical_request_sha256 != self._transcript.request_sha256:
            raise ScriptedToolReplayError("model_tool_transcript_terminal_request_hash_mismatch")
        return result

    def stream_tool_turn(
        self,
        request: ToolTurnRequest,
        messages: Iterable[ModelTurnMessage],
        tools: Iterable[ToolSpec],
    ) -> Iterator[AgentTurnEvent]:
        # Fence live/unknown routes before touching caller-owned iterables.
        _assert_d0a_request_route_binding(request)
        message_tuple, _ = _collect_message_records(messages)
        tool_tuple, _ = _collect_tool_records(tools)
        self._prepare(request, message_tuple, tool_tuple)
        parsed = parse_openai_chat_sse(
            self._transcript.chunks,
            request=request,
            messages=message_tuple,
            tools=tool_tuple,
        )
        if parsed.terminal_result.canonical_request_sha256 != self._transcript.request_sha256:
            raise ScriptedToolReplayError("model_tool_transcript_terminal_request_hash_mismatch")
        yield from parsed.advisory_events


def request_for_model_route(
    route: ModelRouteSpec,
    *,
    provider_mode: str,
    effective_route_snapshot_digest: str,
    max_tokens: int,
    prompt_policy_version: str,
    permission_scope_revision: str,
    outbound_policy_revision: str,
    model_safe_schema_revision: str,
    workspace_id: str,
    actor_id: str,
    permission_scope: str,
    runtime_namespace: str,
    transcript_digest: str,
) -> ToolTurnRequest:
    """Build a request from checked-in route fields without reading runtime settings."""

    return ToolTurnRequest(
        route_id=route.route_id,
        route_revision=route.revision,
        effective_route_snapshot_digest=effective_route_snapshot_digest,
        provider=route.provider,
        requested_model=route.model,
        api_style=route.api_style,
        max_tokens=max_tokens,
        tool_choice="auto",
        stream_options={"include_usage": True},
        prompt_policy_version=prompt_policy_version,
        permission_scope_revision=permission_scope_revision,
        outbound_policy_revision=outbound_policy_revision,
        model_safe_schema_revision=model_safe_schema_revision,
        workspace_id=workspace_id,
        actor_id=actor_id,
        permission_scope=permission_scope,
        runtime_namespace=runtime_namespace,
        provider_mode=provider_mode,
        transcript_digest=transcript_digest,
    )


def with_provider_mode(request: ToolTurnRequest, provider_mode: str) -> ToolTurnRequest:
    """Testing helper that preserves every request field except the explicit mode."""

    return replace(request, provider_mode=provider_mode)


__all__ = [
    "AgentTurnEvent",
    "AssistantTextMessage",
    "AssistantToolCallsMessage",
    "D0A_EFFECT_AUTHORIZATION_AVAILABLE",
    "ErrorEvent",
    "ModelIdentity",
    "ModelRouteExecutionRejected",
    "ModelToolProtocolError",
    "ModelToolRequestBindingError",
    "ModelToolRuntimeError",
    "ModelToolSchemaError",
    "ModelTurnMessage",
    "ModelTurnUsage",
    "ParsedToolTurn",
    "ScriptedToolReplayError",
    "ScriptedToolTurnSession",
    "ScriptedToolTurnTranscript",
    "StopEvent",
    "SystemMessage",
    "TerminalEvent",
    "TextDeltaEvent",
    "ToolCallPartialEvent",
    "ToolCallRecord",
    "ToolResultMessage",
    "ToolSpec",
    "ToolTurnRequest",
    "ToolTurnResult",
    "UsageEvent",
    "UserMessage",
    "canonical_model_turn_transcript_sha256",
    "canonical_tool_turn_request_hash",
    "canonical_tool_turn_request_payload",
    "parse_openai_chat_sse",
    "request_for_model_route",
    "with_provider_mode",
]
