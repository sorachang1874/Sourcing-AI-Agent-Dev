"""Exact occurrence and terminal-result contracts for Agent tool execution.

The immutable tool registry declares what a tool means; this module declares
which logical call is being executed and which physical owner result won.  It
contains no storage, release, serving, provider, or model authority.

Provider call ids are deliberately absent from :class:`AgentToolOccurrence`.
They are attempt evidence and must never become the logical-call idempotency
key.  The stable occurrence identity is the scoped turn/step, exact historical
tool contract, canonical arguments digest, and duplicate-call ordinal.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Literal, Mapping, TypeAlias

from .agent_tool_registry import AgentToolRegistry, AgentToolRegistryError, AgentToolSpec

# This value is also pinned into ``logical_occurrence_digest``. Additive
# terminal-payload migrations must not bump or reinterpret it in place.
AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION = "agent_tool_result_slot_v1"
AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION = "agent_tool_result_attempt_v1"
AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2 = "agent_tool_result_attempt_v2"
AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION = "agent_tool_result_journal_v1"
AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2 = "agent_tool_result_journal_v2"

AgentToolResultDisposition: TypeAlias = Literal["accepted", "quarantined"]

_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_OWNER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,255}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_PROVIDER_MODES = frozenset({"simulate", "scripted", "live", "replay"})
_TOOL_KINDS = frozenset({"action", "query"})
_EFFECT_CLASSES = frozenset({"read_only", "commandless_action", "command_backed_action"})
_RESULT_LINK_POLICIES = frozenset(
    {
        "no_command_v1",
        "workflow_command_acceptance_v1",
        "activity_attempt_terminal_v1",
    }
)

_OCCURRENCE_SPEC_PIN_FIELDS = (
    "tool_name",
    "tool_kind",
    "effect_class",
    "result_link_policy",
    "tool_spec_version",
    "tool_spec_digest",
    "request_schema_version",
    "request_schema_digest",
    "result_schema_version",
    "result_schema_digest",
    "serializer_owner",
    "serializer_revision",
    "serializer_contract_digest",
)


class AgentToolResultSlotError(ValueError):
    """Raised when occurrence or terminal-winner identity is incomplete."""


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentToolResultSlotError("agent_tool_result_value_not_canonical_json") from exc


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _required_closed_literal(field_name: str, value: object, *, allowed: frozenset[str]) -> str:
    if type(value) is not str or value not in allowed:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return value


def _required_text(field_name: str, value: object, *, maximum_bytes: int = 1024) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid") from exc
    if (
        len(encoded) > maximum_bytes
        or any(character in "\r\n\x00" for character in value)
        or any(0xD800 <= ord(character) <= 0xDFFF for character in value)
    ):
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return value


def _required_identifier(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=256)
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return normalized


def _optional_identifier(field_name: str, value: object) -> str:
    if type(value) is not str:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    if value == "":
        return ""
    return _required_identifier(field_name, value)


def _required_version(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=128)
    if _VERSION_PATTERN.fullmatch(normalized) is None:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return normalized


def _required_owner(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=256)
    if _OWNER_PATTERN.fullmatch(normalized) is None:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return normalized


def _required_sha256(field_name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return value


def _required_positive_integer(field_name: str, value: object) -> int:
    if type(value) is not int or value <= 0:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return value


def _required_nonnegative_integer(field_name: str, value: object) -> int:
    if type(value) is not int or value < 0:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return value


def _required_canonical_json_object(
    field_name: str,
    encoded: object,
    *,
    require_nonempty: bool,
) -> tuple[dict[str, Any], str]:
    if type(encoded) is not str:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    try:
        decoded = json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid") from exc
    if not isinstance(decoded, dict) or (require_nonempty and not decoded):
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    canonical = _canonical_json(decoded)
    try:
        encoded_bytes = encoded.encode("utf-8")
        canonical_bytes = canonical.encode("utf-8")
    except UnicodeError as exc:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid") from exc
    if canonical_bytes != encoded_bytes:
        raise AgentToolResultSlotError(f"agent_tool_result_{field_name}_invalid")
    return decoded, canonical


@dataclass(frozen=True, slots=True)
class AgentToolOccurrence:
    """One stable logical tool call before owner execution produces a result."""

    result_slot_id: str
    slot_generation: int
    workspace_id: str
    actor_id: str
    runtime_namespace: str
    provider_mode: str
    turn_id: str
    step_id: str
    tool_name: str
    tool_kind: str
    effect_class: str
    result_link_policy: str
    tool_spec_version: str
    tool_spec_digest: str
    canonical_args_json: str
    canonical_args_digest: str
    occurrence_ordinal: int
    request_schema_version: str
    request_schema_digest: str
    result_schema_version: str
    result_schema_digest: str
    serializer_owner: str
    serializer_revision: str
    serializer_contract_digest: str

    def __post_init__(self) -> None:
        for field_name in (
            "result_slot_id",
            "runtime_namespace",
            "turn_id",
            "step_id",
            "tool_name",
        ):
            object.__setattr__(self, field_name, _required_identifier(field_name, getattr(self, field_name)))
        for field_name in ("workspace_id", "actor_id"):
            object.__setattr__(self, field_name, _required_text(field_name, getattr(self, field_name)))
        object.__setattr__(self, "slot_generation", _required_positive_integer("slot_generation", self.slot_generation))
        object.__setattr__(
            self,
            "occurrence_ordinal",
            _required_positive_integer("occurrence_ordinal", self.occurrence_ordinal),
        )
        object.__setattr__(
            self,
            "provider_mode",
            _required_closed_literal("provider_mode", self.provider_mode, allowed=_PROVIDER_MODES),
        )
        object.__setattr__(
            self,
            "tool_kind",
            _required_closed_literal("tool_kind", self.tool_kind, allowed=_TOOL_KINDS),
        )
        object.__setattr__(
            self,
            "effect_class",
            _required_closed_literal("effect_class", self.effect_class, allowed=_EFFECT_CLASSES),
        )
        object.__setattr__(
            self,
            "result_link_policy",
            _required_closed_literal(
                "link_policy",
                self.result_link_policy,
                allowed=_RESULT_LINK_POLICIES,
            ),
        )
        if self.result_link_policy == "no_command_v1" and self.effect_class not in {
            "read_only",
            "commandless_action",
        }:
            raise AgentToolResultSlotError("agent_tool_result_link_policy_effect_mismatch")
        if (
            self.result_link_policy in {"workflow_command_acceptance_v1", "activity_attempt_terminal_v1"}
            and self.effect_class != "command_backed_action"
        ):
            raise AgentToolResultSlotError("agent_tool_result_link_policy_effect_mismatch")
        if self.tool_kind == "query" and self.effect_class != "read_only":
            raise AgentToolResultSlotError("agent_tool_result_query_effect_invalid")
        for field_name in (
            "tool_spec_version",
            "request_schema_version",
            "result_schema_version",
            "serializer_revision",
        ):
            object.__setattr__(self, field_name, _required_version(field_name, getattr(self, field_name)))
        object.__setattr__(self, "serializer_owner", _required_owner("serializer_owner", self.serializer_owner))
        for field_name in (
            "tool_spec_digest",
            "canonical_args_digest",
            "request_schema_digest",
            "result_schema_digest",
            "serializer_contract_digest",
        ):
            object.__setattr__(self, field_name, _required_sha256(field_name, getattr(self, field_name)))
        _, canonical_args_json = _required_canonical_json_object(
            "canonical_args",
            self.canonical_args_json,
            require_nonempty=False,
        )
        object.__setattr__(self, "canonical_args_json", canonical_args_json)
        if _sha256_text(canonical_args_json) != self.canonical_args_digest:
            raise AgentToolResultSlotError("agent_tool_result_canonical_args_digest_mismatch")

    @classmethod
    def from_tool_spec(
        cls,
        *,
        result_slot_id: str,
        slot_generation: int,
        workspace_id: str,
        actor_id: str,
        runtime_namespace: str,
        provider_mode: str,
        turn_id: str,
        step_id: str,
        tool_spec: AgentToolSpec,
        canonical_args: Mapping[str, Any],
        occurrence_ordinal: int,
    ) -> AgentToolOccurrence:
        if not isinstance(tool_spec, AgentToolSpec):
            raise AgentToolResultSlotError("agent_tool_result_tool_spec_invalid")
        if not isinstance(canonical_args, Mapping):
            raise AgentToolResultSlotError("agent_tool_result_canonical_args_invalid")
        canonical_args_json = _canonical_json(dict(canonical_args))
        return cls(
            result_slot_id=result_slot_id,
            slot_generation=slot_generation,
            workspace_id=workspace_id,
            actor_id=actor_id,
            runtime_namespace=runtime_namespace,
            provider_mode=provider_mode,
            turn_id=turn_id,
            step_id=step_id,
            tool_name=tool_spec.tool_name,
            tool_kind=tool_spec.tool_kind,
            effect_class=tool_spec.behavior.effect_class,
            result_link_policy=tool_spec.behavior.result_link_policy,
            tool_spec_version=tool_spec.tool_spec_version,
            tool_spec_digest=tool_spec.tool_spec_digest,
            canonical_args_json=canonical_args_json,
            canonical_args_digest=_sha256_text(canonical_args_json),
            occurrence_ordinal=occurrence_ordinal,
            request_schema_version=tool_spec.request.schema_version,
            request_schema_digest=tool_spec.request.schema_digest,
            result_schema_version=tool_spec.result.schema_version,
            result_schema_digest=tool_spec.result.schema_digest,
            serializer_owner=tool_spec.result.serializer_owner.owner_id,
            serializer_revision=tool_spec.result.serializer_owner.owner_revision,
            serializer_contract_digest=tool_spec.result.serializer_owner.owner_contract_digest,
        )

    @property
    def canonical_args(self) -> dict[str, Any]:
        decoded = json.loads(self.canonical_args_json)
        assert isinstance(decoded, dict)
        return decoded

    @property
    def logical_occurrence_digest(self) -> str:
        return _sha256_text(_canonical_json(self.logical_identity_record()))

    def logical_identity_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION,
            "workspace_id": self.workspace_id,
            "actor_id": self.actor_id,
            "runtime_namespace": self.runtime_namespace,
            "provider_mode": self.provider_mode,
            "turn_id": self.turn_id,
            "step_id": self.step_id,
            "tool_name": self.tool_name,
            "tool_spec_version": self.tool_spec_version,
            "tool_spec_digest": self.tool_spec_digest,
            "canonical_args_digest": self.canonical_args_digest,
            "occurrence_ordinal": self.occurrence_ordinal,
        }

    def to_record(self) -> dict[str, object]:
        return {
            **self.logical_identity_record(),
            "result_slot_id": self.result_slot_id,
            "slot_generation": self.slot_generation,
            "tool_kind": self.tool_kind,
            "effect_class": self.effect_class,
            "result_link_policy": self.result_link_policy,
            "canonical_args": self.canonical_args,
            "logical_occurrence_digest": self.logical_occurrence_digest,
            "request_schema_version": self.request_schema_version,
            "request_schema_digest": self.request_schema_digest,
            "result_schema_version": self.result_schema_version,
            "result_schema_digest": self.result_schema_digest,
            "serializer_owner": self.serializer_owner,
            "serializer_revision": self.serializer_revision,
            "serializer_contract_digest": self.serializer_contract_digest,
        }

    def revalidated(self) -> AgentToolOccurrence:
        """Re-run the immutable occurrence invariants at a persistence boundary."""

        return AgentToolOccurrence(
            result_slot_id=self.result_slot_id,
            slot_generation=self.slot_generation,
            workspace_id=self.workspace_id,
            actor_id=self.actor_id,
            runtime_namespace=self.runtime_namespace,
            provider_mode=self.provider_mode,
            turn_id=self.turn_id,
            step_id=self.step_id,
            tool_name=self.tool_name,
            tool_kind=self.tool_kind,
            effect_class=self.effect_class,
            result_link_policy=self.result_link_policy,
            tool_spec_version=self.tool_spec_version,
            tool_spec_digest=self.tool_spec_digest,
            canonical_args_json=self.canonical_args_json,
            canonical_args_digest=self.canonical_args_digest,
            occurrence_ordinal=self.occurrence_ordinal,
            request_schema_version=self.request_schema_version,
            request_schema_digest=self.request_schema_digest,
            result_schema_version=self.result_schema_version,
            result_schema_digest=self.result_schema_digest,
            serializer_owner=self.serializer_owner,
            serializer_revision=self.serializer_revision,
            serializer_contract_digest=self.serializer_contract_digest,
        )

    def revalidated_for_registry(self, registry: AgentToolRegistry) -> AgentToolOccurrence:
        """Bind all spec-derived pins to one exact server-owned historical spec."""

        occurrence = self.revalidated()
        if not isinstance(registry, AgentToolRegistry):
            raise AgentToolResultSlotError("agent_tool_result_registry_invalid")
        try:
            historical_spec = registry.require_historical(
                occurrence.tool_name,
                occurrence.tool_spec_version,
                occurrence.tool_spec_digest,
            )
        except AgentToolRegistryError as exc:
            raise AgentToolResultSlotError("agent_tool_result_historical_spec_missing") from exc
        expected = AgentToolOccurrence.from_tool_spec(
            result_slot_id=occurrence.result_slot_id,
            slot_generation=occurrence.slot_generation,
            workspace_id=occurrence.workspace_id,
            actor_id=occurrence.actor_id,
            runtime_namespace=occurrence.runtime_namespace,
            provider_mode=occurrence.provider_mode,
            turn_id=occurrence.turn_id,
            step_id=occurrence.step_id,
            tool_spec=historical_spec,
            canonical_args=occurrence.canonical_args,
            occurrence_ordinal=occurrence.occurrence_ordinal,
        )
        mismatches = tuple(
            field_name
            for field_name in _OCCURRENCE_SPEC_PIN_FIELDS
            if getattr(occurrence, field_name) != getattr(expected, field_name)
        )
        if mismatches:
            raise AgentToolResultSlotError("agent_tool_result_historical_spec_pin_mismatch:" + ",".join(mismatches))
        return expected


@dataclass(frozen=True, slots=True)
class AgentToolTerminalResult:
    """One physical result attempt proposed for a pending occurrence slot."""

    result_attempt_id: str
    provider_call_id: str
    tool_call_id: str
    action_id: str
    operation_run_id: str
    workflow_command_id: str
    activity_run_id: str
    activity_attempt_id: str
    command_attempt: int
    command_generation: int
    control_epoch: int
    owner_target_kind: str
    owner_target_id: str
    owner_target_revision: int
    owner_target_generation: int
    terminal_winner_id: str
    owner_result_ref_json: str
    owner_result_digest: str
    serialized_result_json: str
    serialized_result_digest: str
    is_error: bool
    owner_target_revision_token: str = ""

    def __post_init__(self) -> None:
        for field_name in ("result_attempt_id", "terminal_winner_id"):
            object.__setattr__(self, field_name, _required_identifier(field_name, getattr(self, field_name)))
        for field_name in ("provider_call_id", "tool_call_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(field_name, getattr(self, field_name), maximum_bytes=1024),
            )
        for field_name in (
            "action_id",
            "operation_run_id",
            "workflow_command_id",
            "activity_run_id",
            "activity_attempt_id",
        ):
            object.__setattr__(self, field_name, _optional_identifier(field_name, getattr(self, field_name)))
        object.__setattr__(self, "owner_target_kind", _required_version("owner_target_kind", self.owner_target_kind))
        object.__setattr__(self, "owner_target_id", _required_identifier("owner_target_id", self.owner_target_id))
        for field_name in (
            "command_attempt",
            "command_generation",
            "control_epoch",
            "owner_target_revision",
            "owner_target_generation",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_nonnegative_integer(field_name, getattr(self, field_name)),
            )
        object.__setattr__(
            self,
            "owner_target_revision_token",
            _optional_identifier("owner_target_revision_token", self.owner_target_revision_token),
        )
        if (
            self.owner_target_revision == 0
            and self.owner_target_generation == 0
            and not self.owner_target_revision_token
        ):
            raise AgentToolResultSlotError("agent_tool_result_owner_target_version_missing")
        object.__setattr__(
            self, "owner_result_digest", _required_sha256("owner_result_digest", self.owner_result_digest)
        )
        object.__setattr__(
            self,
            "serialized_result_digest",
            _required_sha256("serialized_result_digest", self.serialized_result_digest),
        )
        if type(self.is_error) is not bool:
            raise AgentToolResultSlotError("agent_tool_result_is_error_invalid")
        for field_name, carrier_field in (
            ("owner_result_ref", "owner_result_ref_json"),
            ("serialized_result", "serialized_result_json"),
        ):
            _, canonical_json = _required_canonical_json_object(
                field_name,
                getattr(self, carrier_field),
                require_nonempty=True,
            )
            object.__setattr__(self, carrier_field, canonical_json)
        if _sha256_text(self.serialized_result_json) != self.serialized_result_digest:
            raise AgentToolResultSlotError("agent_tool_result_serialized_result_digest_mismatch")
        self._validate_link_group()

    @classmethod
    def from_serialized_result(
        cls,
        *,
        result_attempt_id: str,
        provider_call_id: str,
        tool_call_id: str,
        action_id: str = "",
        operation_run_id: str = "",
        workflow_command_id: str = "",
        activity_run_id: str = "",
        activity_attempt_id: str = "",
        command_attempt: int = 0,
        command_generation: int = 0,
        control_epoch: int = 0,
        owner_target_kind: str,
        owner_target_id: str,
        owner_target_revision: int = 0,
        owner_target_generation: int = 0,
        owner_target_revision_token: str = "",
        terminal_winner_id: str,
        owner_result_ref: Mapping[str, Any],
        owner_result_digest: str,
        serialized_result: Mapping[str, Any],
        is_error: bool,
    ) -> AgentToolTerminalResult:
        owner_result_ref_json = _canonical_json(dict(owner_result_ref))
        serialized_result_json = _canonical_json(dict(serialized_result))
        return cls(
            result_attempt_id=result_attempt_id,
            provider_call_id=provider_call_id,
            tool_call_id=tool_call_id,
            action_id=action_id,
            operation_run_id=operation_run_id,
            workflow_command_id=workflow_command_id,
            activity_run_id=activity_run_id,
            activity_attempt_id=activity_attempt_id,
            command_attempt=command_attempt,
            command_generation=command_generation,
            control_epoch=control_epoch,
            owner_target_kind=owner_target_kind,
            owner_target_id=owner_target_id,
            owner_target_revision=owner_target_revision,
            owner_target_generation=owner_target_generation,
            terminal_winner_id=terminal_winner_id,
            owner_result_ref_json=owner_result_ref_json,
            owner_result_digest=owner_result_digest,
            serialized_result_json=serialized_result_json,
            serialized_result_digest=_sha256_text(serialized_result_json),
            is_error=is_error,
            owner_target_revision_token=owner_target_revision_token,
        )

    def _validate_link_group(self) -> None:
        action_group = (self.action_id, self.operation_run_id)
        if bool(action_group[0]) != bool(action_group[1]):
            raise AgentToolResultSlotError("agent_tool_result_action_link_group_incomplete")
        activity_group = (self.activity_run_id, self.activity_attempt_id)
        has_activity = all(activity_group)
        if any(activity_group) and not has_activity:
            raise AgentToolResultSlotError("agent_tool_result_command_link_group_incomplete")
        numeric_command_group = (self.command_attempt, self.command_generation, self.control_epoch)
        if has_activity:
            if (
                not all(action_group)
                or not self.workflow_command_id
                or any(value <= 0 for value in numeric_command_group)
            ):
                raise AgentToolResultSlotError("agent_tool_result_command_link_group_incomplete")
        elif any(numeric_command_group):
            raise AgentToolResultSlotError("agent_tool_result_command_link_group_incomplete")
        elif self.workflow_command_id and not all(action_group):
            raise AgentToolResultSlotError("agent_tool_result_command_link_group_incomplete")

    @property
    def owner_result_ref(self) -> dict[str, Any]:
        decoded = json.loads(self.owner_result_ref_json)
        assert isinstance(decoded, dict)
        return decoded

    @property
    def serialized_result(self) -> dict[str, Any]:
        decoded = json.loads(self.serialized_result_json)
        assert isinstance(decoded, dict)
        return decoded

    def validate_for_occurrence(self, occurrence: AgentToolOccurrence) -> None:
        if not isinstance(occurrence, AgentToolOccurrence):
            raise AgentToolResultSlotError("agent_tool_result_occurrence_invalid")
        has_action = bool(self.action_id)
        has_command = bool(self.workflow_command_id)
        has_activity = bool(self.activity_run_id)
        has_fence = any((self.command_attempt, self.command_generation, self.control_epoch))
        policy = occurrence.result_link_policy
        if policy == "no_command_v1":
            effect_shape_valid = (
                occurrence.effect_class == "commandless_action" and has_action
            ) or occurrence.effect_class == "read_only"
            link_shape_valid = not has_command and not has_activity and not has_fence
        elif policy == "workflow_command_acceptance_v1":
            effect_shape_valid = True
            link_shape_valid = has_action and has_command and not has_activity and not has_fence
        elif policy == "activity_attempt_terminal_v1":
            effect_shape_valid = True
            link_shape_valid = (
                has_action
                and has_command
                and has_activity
                and all(value > 0 for value in (self.command_attempt, self.command_generation, self.control_epoch))
            )
        else:  # pragma: no cover - occurrence construction rejects this first
            effect_shape_valid = False
            link_shape_valid = False
        if not effect_shape_valid or not link_shape_valid:
            raise AgentToolResultSlotError("agent_tool_result_link_policy_shape_invalid")

    def revalidated(self) -> AgentToolTerminalResult:
        """Re-run every boundary invariant when crossing from an external adapter."""

        owner_result_ref, owner_result_ref_json = _required_canonical_json_object(
            "owner_result_ref",
            self.owner_result_ref_json,
            require_nonempty=True,
        )
        serialized_result, serialized_result_json = _required_canonical_json_object(
            "serialized_result",
            self.serialized_result_json,
            require_nonempty=True,
        )
        claimed_serialized_result_digest = _required_sha256(
            "serialized_result_digest",
            self.serialized_result_digest,
        )
        canonical = AgentToolTerminalResult.from_serialized_result(
            result_attempt_id=self.result_attempt_id,
            provider_call_id=self.provider_call_id,
            tool_call_id=self.tool_call_id,
            action_id=self.action_id,
            operation_run_id=self.operation_run_id,
            workflow_command_id=self.workflow_command_id,
            activity_run_id=self.activity_run_id,
            activity_attempt_id=self.activity_attempt_id,
            command_attempt=self.command_attempt,
            command_generation=self.command_generation,
            control_epoch=self.control_epoch,
            owner_target_kind=self.owner_target_kind,
            owner_target_id=self.owner_target_id,
            owner_target_revision=self.owner_target_revision,
            owner_target_generation=self.owner_target_generation,
            terminal_winner_id=self.terminal_winner_id,
            owner_result_ref=owner_result_ref,
            owner_result_digest=self.owner_result_digest,
            serialized_result=serialized_result,
            is_error=self.is_error,
            owner_target_revision_token=self.owner_target_revision_token,
        )
        if canonical.owner_result_ref_json.encode("utf-8") != owner_result_ref_json.encode("utf-8"):
            raise AgentToolResultSlotError("agent_tool_result_owner_result_ref_invalid")
        if canonical.serialized_result_json.encode("utf-8") != serialized_result_json.encode("utf-8"):
            raise AgentToolResultSlotError("agent_tool_result_serialized_result_invalid")
        if canonical.serialized_result_digest != claimed_serialized_result_digest:
            raise AgentToolResultSlotError("agent_tool_result_serialized_result_digest_mismatch")
        return canonical

    def tool_result_message_record(self) -> dict[str, object]:
        return {
            "message_type": "tool_result",
            "tool_call_id": self.tool_call_id,
            "content": self.serialized_result_json,
            "is_error": self.is_error,
        }

    @property
    def tool_result_message_digest(self) -> str:
        return _sha256_text(_canonical_json(self.tool_result_message_record()))

    @property
    def attempt_schema_version(self) -> str:
        return (
            AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2
            if self.owner_target_revision_token
            else AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION
        )

    @property
    def journal_schema_version(self) -> str:
        return (
            AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2
            if self.owner_target_revision_token
            else AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION
        )

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.attempt_schema_version,
            "result_attempt_id": self.result_attempt_id,
            "provider_call_id": self.provider_call_id,
            "tool_call_id": self.tool_call_id,
            "action_id": self.action_id,
            "operation_run_id": self.operation_run_id,
            "workflow_command_id": self.workflow_command_id,
            "activity_run_id": self.activity_run_id,
            "activity_attempt_id": self.activity_attempt_id,
            "command_attempt": self.command_attempt,
            "command_generation": self.command_generation,
            "control_epoch": self.control_epoch,
            "owner_target_kind": self.owner_target_kind,
            "owner_target_id": self.owner_target_id,
            "owner_target_revision": self.owner_target_revision,
            "owner_target_generation": self.owner_target_generation,
            "owner_target_revision_token": self.owner_target_revision_token,
            "terminal_winner_id": self.terminal_winner_id,
            "owner_result_ref": self.owner_result_ref,
            "owner_result_digest": self.owner_result_digest,
            "serialized_result": self.serialized_result,
            "serialized_result_digest": self.serialized_result_digest,
            "tool_result_message": self.tool_result_message_record(),
            "tool_result_message_digest": self.tool_result_message_digest,
            "is_error": self.is_error,
        }


__all__ = [
    "AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION",
    "AGENT_TOOL_RESULT_ATTEMPT_SCHEMA_VERSION_V2",
    "AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION",
    "AGENT_TOOL_RESULT_JOURNAL_SCHEMA_VERSION_V2",
    "AGENT_TOOL_RESULT_SLOT_SCHEMA_VERSION",
    "AgentToolOccurrence",
    "AgentToolResultDisposition",
    "AgentToolResultSlotError",
    "AgentToolTerminalResult",
]
