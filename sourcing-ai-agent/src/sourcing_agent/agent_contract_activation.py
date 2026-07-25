"""Pure immutable contracts for future Agent contract activation.

This module defines version pins, release snapshots, and transition validation
only.  It has no repository, CAS writer, catalog projection, served predicate,
or runtime authorization side effect.  A future PG owner must persist these
records and perform the compare-and-swap in the same integration unit of work
that copies exact pins into an invocation or action.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias

from .agent_contract_identity import is_valid_agent_tool_name

AGENT_CONTRACT_ACTIVATION_SCHEMA_VERSION = "agent_contract_activation_v1"
AGENT_CONTRACT_ACTIVATION_POLICY_SCHEMA_VERSION = "agent_contract_activation_policy_v1"
AGENT_CONTRACT_REQUIRED_PIN_SET_SCHEMA_VERSION = "agent_contract_required_pin_set_v1"
AGENT_CONTRACT_REVIEW_EVIDENCE_SCHEMA_VERSION = "agent_contract_review_evidence_v1"
AGENT_CONTRACT_REVIEW_RECEIPT_SCHEMA_VERSION = "agent_contract_review_receipt_v1"
AGENT_CONTRACT_SCHEMA_BRIDGE_GATE_RECEIPT_SCHEMA_VERSION = "agent_contract_schema_bridge_gate_receipt_v1"
AGENT_CONTRACT_HOSTED_GATE_RECEIPT_SCHEMA_VERSION = "agent_contract_hosted_gate_receipt_v1"
AGENT_CONTRACT_HISTORY_MAX_ENTRIES = 128
AGENT_CONTRACT_REQUIRED_PIN_SET_MAX_BYTES = 131_072
POSTGRES_BIGINT_MAX = (1 << 63) - 1

ActivationEntryKind: TypeAlias = Literal["action", "query"]
ActionRequestReleaseState: TypeAlias = Literal["disabled", "shadow", "current", "not_applicable"]
AgentToolReleaseState: TypeAlias = Literal["disabled", "shadow", "hosted"]
AgentProviderMode: TypeAlias = Literal["live", "replay", "scripted", "simulate"]

_ENTRY_KINDS = frozenset({"action", "query"})
_PROVIDER_MODES = frozenset({"live", "replay", "scripted", "simulate"})
_REQUEST_STATES = frozenset({"disabled", "shadow", "current", "not_applicable"})
_TOOL_STATES = frozenset({"disabled", "shadow", "hosted"})
_REQUEST_TRANSITIONS = frozenset(
    {
        ("disabled", "shadow"),
        ("shadow", "current"),
        ("shadow", "disabled"),
        ("current", "disabled"),
    }
)
_TOOL_TRANSITIONS = frozenset(
    {
        ("disabled", "shadow"),
        ("shadow", "hosted"),
        ("shadow", "disabled"),
        ("hosted", "disabled"),
    }
)
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,255}")
_REQUEST_SCHEMA_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_RESULT_CONTRACT_VERSION_PATTERN = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
_TOOL_OWNER_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_GIT_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_UTC_TIMESTAMP_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z")


class AgentContractActivationError(ValueError):
    """Raised when an activation declaration or transition fails closed."""


def _required_text(field_name: str, value: object, *, maximum_bytes: int = 1024) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid") from exc
    if len(encoded) > maximum_bytes or any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return value


def _required_identifier(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=256)
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return normalized


def _required_owner_version(field_name: str, value: object, *, pattern: re.Pattern[str]) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=128)
    if pattern.fullmatch(normalized) is None:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return normalized


def _required_request_schema_version(value: object) -> str:
    return _required_owner_version(
        "request_schema_version",
        value,
        pattern=_REQUEST_SCHEMA_VERSION_PATTERN,
    )


def _required_result_contract_version(field_name: str, value: object) -> str:
    return _required_owner_version(field_name, value, pattern=_RESULT_CONTRACT_VERSION_PATTERN)


def _required_tool_owner_version(field_name: str, value: object) -> str:
    return _required_owner_version(field_name, value, pattern=_TOOL_OWNER_VERSION_PATTERN)


def _required_bigint(field_name: str, value: object) -> int:
    if type(value) is not int or value < 0 or value > POSTGRES_BIGINT_MAX:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return value


def _required_positive_bigint(field_name: str, value: object) -> int:
    normalized = _required_bigint(field_name, value)
    if normalized == 0:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return normalized


def _required_sha256(field_name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return value


def _required_git_commit(field_name: str, value: object) -> str:
    if type(value) is not str or _GIT_COMMIT_PATTERN.fullmatch(value) is None:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    return value


def _required_tool_name(field_name: str, value: object) -> str:
    if not is_valid_agent_tool_name(value):
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    assert isinstance(value, str)
    return value


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentContractActivationError("agent_contract_record_not_canonical_json") from exc


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _required_utc_timestamp(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=64)
    if _UTC_TIMESTAMP_PATTERN.fullmatch(normalized) is None:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    try:
        parsed = datetime.fromisoformat(f"{normalized[:-1]}+00:00")
    except ValueError as exc:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
    canonical = parsed.strftime("%Y-%m-%dT%H:%M:%S")
    if parsed.microsecond:
        canonical = f"{canonical}.{parsed.microsecond:06d}".rstrip("0")
    return f"{canonical}Z"


def _parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(f"{value[:-1]}+00:00")


def _required_activation_key(value: object) -> tuple[str, ActivationEntryKind, str]:
    activation_key = _required_text("activation_key", value, maximum_bytes=320)
    prefix, separator, identity = activation_key.partition(":")
    if not separator or prefix not in _ENTRY_KINDS:
        raise AgentContractActivationError("agent_contract_activation_key_invalid")
    if prefix == "action":
        normalized_identity = _required_identifier("activation_action_type", identity)
    else:
        normalized_identity = _required_tool_name("activation_tool_name", identity)
    if activation_key != f"{prefix}:{normalized_identity}":
        raise AgentContractActivationError("agent_contract_activation_key_invalid")
    return activation_key, prefix, normalized_identity  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class AgentContractOwnerPin:
    owner: str
    revision: str
    contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "owner", _required_identifier("owner", self.owner))
        object.__setattr__(
            self,
            "revision",
            _required_tool_owner_version("owner_revision", self.revision),
        )
        object.__setattr__(
            self,
            "contract_digest",
            _required_sha256("owner_contract_digest", self.contract_digest),
        )

    def to_record(self) -> dict[str, str]:
        return {
            "owner": self.owner,
            "revision": self.revision,
            "contract_digest": self.contract_digest,
        }


@dataclass(frozen=True, slots=True)
class AgentContractActivationPolicyPin:
    policy_owner: AgentContractOwnerPin
    release_commit: str
    runtime_namespace: str
    allowed_provider_modes: tuple[AgentProviderMode, ...]
    workspace_allowlist_digest: str | None
    requester_allowlist_digest: str | None
    production_action_roster_digest: str
    production_action_count: int
    review_verifier: AgentContractOwnerPin
    hosted_gate: AgentContractOwnerPin
    schema_bridge_gate: AgentContractOwnerPin

    def __post_init__(self) -> None:
        for field_name, owner_pin in (
            ("policy_owner", self.policy_owner),
            ("review_verifier", self.review_verifier),
            ("hosted_gate", self.hosted_gate),
            ("schema_bridge_gate", self.schema_bridge_gate),
        ):
            if not isinstance(owner_pin, AgentContractOwnerPin):
                raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
        object.__setattr__(self, "release_commit", _required_git_commit("release_commit", self.release_commit))
        object.__setattr__(
            self,
            "runtime_namespace",
            _required_identifier("runtime_namespace", self.runtime_namespace),
        )
        modes = self.allowed_provider_modes
        if (
            type(modes) is not tuple
            or not modes
            or any(type(mode) is not str or mode not in _PROVIDER_MODES for mode in modes)
            or len(modes) != len(set(modes))
        ):
            raise AgentContractActivationError("agent_contract_allowed_provider_modes_invalid")
        object.__setattr__(self, "allowed_provider_modes", tuple(sorted(modes)))
        for field_name, digest_value in (
            ("workspace_allowlist_digest", self.workspace_allowlist_digest),
            ("requester_allowlist_digest", self.requester_allowlist_digest),
        ):
            if digest_value is not None:
                object.__setattr__(self, field_name, _required_sha256(field_name, digest_value))
        object.__setattr__(
            self,
            "production_action_roster_digest",
            _required_sha256("production_action_roster_digest", self.production_action_roster_digest),
        )
        object.__setattr__(
            self,
            "production_action_count",
            _required_positive_bigint("production_action_count", self.production_action_count),
        )

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_ACTIVATION_POLICY_SCHEMA_VERSION,
            "policy_owner": self.policy_owner.to_record(),
            "release_commit": self.release_commit,
            "runtime_namespace": self.runtime_namespace,
            "allowed_provider_modes": list(self.allowed_provider_modes),
            "workspace_allowlist_digest": self.workspace_allowlist_digest,
            "requester_allowlist_digest": self.requester_allowlist_digest,
            "production_action_roster_digest": self.production_action_roster_digest,
            "production_action_count": self.production_action_count,
            "review_verifier": self.review_verifier.to_record(),
            "hosted_gate": self.hosted_gate.to_record(),
            "schema_bridge_gate": self.schema_bridge_gate.to_record(),
        }

    @property
    def policy_digest(self) -> str:
        return _sha256_json(self.to_record())


@dataclass(frozen=True, slots=True)
class ActionContractPin:
    action_type: str
    request_schema_version: str
    request_schema_digest: str
    action_contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "action_type", _required_identifier("action_type", self.action_type))
        object.__setattr__(
            self,
            "request_schema_version",
            _required_request_schema_version(self.request_schema_version),
        )
        object.__setattr__(
            self,
            "request_schema_digest",
            _required_sha256("request_schema_digest", self.request_schema_digest),
        )
        object.__setattr__(
            self,
            "action_contract_digest",
            _required_sha256("action_contract_digest", self.action_contract_digest),
        )

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (
            self.action_type,
            self.request_schema_version,
            self.request_schema_digest,
            self.action_contract_digest,
        )

    @property
    def version_key(self) -> tuple[str, str]:
        return self.action_type, self.request_schema_version

    def to_record(self) -> dict[str, str]:
        return {
            "action_type": self.action_type,
            "request_schema_version": self.request_schema_version,
            "request_schema_digest": self.request_schema_digest,
            "action_contract_digest": self.action_contract_digest,
        }


@dataclass(frozen=True, slots=True)
class ActionResultContractPin:
    tool_name: str
    tool_kind: ActivationEntryKind
    action_type: str | None
    query_owner: AgentContractOwnerPin | None
    result_schema_version: str
    result_schema_digest: str
    serializer_owner: str
    serializer_revision: str
    serializer_contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_name", _required_tool_name("result_tool_name", self.tool_name))
        if self.tool_kind not in _ENTRY_KINDS:
            raise AgentContractActivationError("agent_contract_result_tool_kind_invalid")
        if self.tool_kind == "action":
            if self.action_type is None or self.query_owner is not None:
                raise AgentContractActivationError("agent_contract_result_route_invalid")
            object.__setattr__(
                self,
                "action_type",
                _required_identifier("result_action_type", self.action_type),
            )
        elif self.action_type is not None or not isinstance(self.query_owner, AgentContractOwnerPin):
            raise AgentContractActivationError("agent_contract_result_route_invalid")
        object.__setattr__(
            self,
            "result_schema_version",
            _required_result_contract_version("result_schema_version", self.result_schema_version),
        )
        object.__setattr__(
            self,
            "result_schema_digest",
            _required_sha256("result_schema_digest", self.result_schema_digest),
        )
        object.__setattr__(self, "serializer_owner", _required_identifier("serializer_owner", self.serializer_owner))
        object.__setattr__(
            self,
            "serializer_revision",
            _required_result_contract_version("serializer_revision", self.serializer_revision),
        )
        object.__setattr__(
            self,
            "serializer_contract_digest",
            _required_sha256("serializer_contract_digest", self.serializer_contract_digest),
        )

    @property
    def route_identity(self) -> tuple[ActivationEntryKind, str | AgentContractOwnerPin]:
        route_owner = self.action_type if self.tool_kind == "action" else self.query_owner
        assert route_owner is not None
        return self.tool_kind, route_owner

    @property
    def version_key(self) -> tuple[object, ...]:
        return self.tool_name, *self.route_identity, self.result_schema_version

    @property
    def identity(self) -> tuple[object, ...]:
        return (
            self.tool_name,
            self.tool_kind,
            self.action_type,
            self.query_owner,
            self.result_schema_version,
            self.result_schema_digest,
            self.serializer_owner,
            self.serializer_revision,
            self.serializer_contract_digest,
        )

    def to_record(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "tool_kind": self.tool_kind,
            "action_type": self.action_type,
            "query_owner": None if self.query_owner is None else self.query_owner.to_record(),
            "result_schema_version": self.result_schema_version,
            "result_schema_digest": self.result_schema_digest,
            "serializer_owner": self.serializer_owner,
            "serializer_revision": self.serializer_revision,
            "serializer_contract_digest": self.serializer_contract_digest,
        }


@dataclass(frozen=True, slots=True)
class AgentToolContractPin:
    tool_name: str
    tool_kind: ActivationEntryKind
    action_type: str | None
    query_owner: AgentContractOwnerPin | None
    tool_spec_version: str
    tool_spec_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_name", _required_tool_name("tool_name", self.tool_name))
        if self.tool_kind not in _ENTRY_KINDS:
            raise AgentContractActivationError("agent_contract_tool_kind_invalid")
        if self.tool_kind == "action":
            if self.action_type is None or self.query_owner is not None:
                raise AgentContractActivationError("agent_contract_tool_route_invalid")
            object.__setattr__(self, "action_type", _required_identifier("tool_action_type", self.action_type))
        elif self.action_type is not None or not isinstance(self.query_owner, AgentContractOwnerPin):
            raise AgentContractActivationError("agent_contract_tool_route_invalid")
        object.__setattr__(
            self,
            "tool_spec_version",
            _required_tool_owner_version("tool_spec_version", self.tool_spec_version),
        )
        object.__setattr__(self, "tool_spec_digest", _required_sha256("tool_spec_digest", self.tool_spec_digest))

    @property
    def route_identity(self) -> tuple[ActivationEntryKind, str | AgentContractOwnerPin]:
        route_owner = self.action_type if self.tool_kind == "action" else self.query_owner
        assert route_owner is not None
        return self.tool_kind, route_owner

    @property
    def version_key(self) -> tuple[object, ...]:
        return self.tool_name, *self.route_identity, self.tool_spec_version

    @property
    def identity(self) -> tuple[object, ...]:
        return (
            self.tool_name,
            self.tool_kind,
            self.action_type,
            self.query_owner,
            self.tool_spec_version,
            self.tool_spec_digest,
        )

    def to_record(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "tool_kind": self.tool_kind,
            "action_type": self.action_type,
            "query_owner": None if self.query_owner is None else self.query_owner.to_record(),
            "tool_spec_version": self.tool_spec_version,
            "tool_spec_digest": self.tool_spec_digest,
        }


@dataclass(frozen=True, slots=True)
class AgentContractRequiredPinSet:
    activation_key: str
    requests: tuple[ActionContractPin, ...]
    results: tuple[ActionResultContractPin, ...]
    tools: tuple[AgentToolContractPin, ...]

    def __post_init__(self) -> None:
        activation_key, entry_kind, entry_identity = _required_activation_key(self.activation_key)
        object.__setattr__(self, "activation_key", activation_key)
        for field_name, values, expected_type in (
            ("requests", self.requests, ActionContractPin),
            ("results", self.results, ActionResultContractPin),
            ("tools", self.tools, AgentToolContractPin),
        ):
            if (
                type(values) is not tuple
                or len(values) > AGENT_CONTRACT_HISTORY_MAX_ENTRIES
                or any(not isinstance(value, expected_type) for value in values)
            ):
                raise AgentContractActivationError(f"agent_contract_required_{field_name}_invalid")
        requests = self._canonical_pins(self.requests, "request")
        results = self._canonical_pins(self.results, "result")
        tools = self._canonical_pins(self.tools, "tool")
        if entry_kind == "query" and requests:
            raise AgentContractActivationError("agent_contract_query_request_history_forbidden")
        if entry_kind == "action" and any(pin.action_type != entry_identity for pin in requests):
            raise AgentContractActivationError("agent_contract_request_history_scope_mismatch")
        tool_names = {pin.tool_name for pin in results} | {pin.tool_name for pin in tools}
        if len(tool_names) > 1 or (entry_kind == "query" and tool_names and tool_names != {entry_identity}):
            raise AgentContractActivationError("agent_contract_tool_history_scope_mismatch")
        route_identities = {pin.route_identity for pin in results} | {pin.route_identity for pin in tools}
        if len(route_identities) > 1 or (
            entry_kind == "action" and route_identities and route_identities != {("action", entry_identity)}
        ):
            raise AgentContractActivationError("agent_contract_tool_history_route_mismatch")
        object.__setattr__(self, "requests", requests)
        object.__setattr__(self, "results", results)
        object.__setattr__(self, "tools", tools)
        if len(_canonical_json(self.to_record()).encode("utf-8")) > AGENT_CONTRACT_REQUIRED_PIN_SET_MAX_BYTES:
            raise AgentContractActivationError("agent_contract_required_pin_set_too_large")

    @staticmethod
    def _canonical_pins(values: tuple[object, ...], pin_kind: str) -> tuple:
        identities: set[tuple] = set()
        version_digests: dict[tuple, tuple] = {}
        for value in values:
            identity = value.identity  # type: ignore[attr-defined]
            if identity in identities:
                raise AgentContractActivationError(f"agent_contract_required_{pin_kind}_duplicate")
            identities.add(identity)
            version_key = value.version_key  # type: ignore[attr-defined]
            prior = version_digests.get(version_key)
            if prior is not None and prior != identity:
                raise AgentContractActivationError(f"agent_contract_required_{pin_kind}_version_drift")
            version_digests[version_key] = identity
        return tuple(sorted(values, key=lambda item: _canonical_json(item.to_record())))

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_REQUIRED_PIN_SET_SCHEMA_VERSION,
            "activation_key": self.activation_key,
            "requests": [pin.to_record() for pin in self.requests],
            "results": [pin.to_record() for pin in self.results],
            "tools": [pin.to_record() for pin in self.tools],
        }

    @property
    def required_pin_set_digest(self) -> str:
        return _sha256_json(self.to_record())

    @property
    def identities(self) -> frozenset[tuple[str, tuple]]:
        return frozenset(
            [("request", pin.identity) for pin in self.requests]
            + [("result", pin.identity) for pin in self.results]
            + [("tool", pin.identity) for pin in self.tools]
        )


@dataclass(frozen=True, slots=True)
class AgentContractReviewEvidence:
    reviewed_commit: str
    review_artifact_id: str
    review_artifact_digest: str
    review_scope_digest: str
    verifier_contract_digest: str
    reviewed_at: str
    verdict: Literal["GO"] = "GO"

    def __post_init__(self) -> None:
        object.__setattr__(self, "reviewed_commit", _required_git_commit("reviewed_commit", self.reviewed_commit))
        object.__setattr__(
            self, "review_artifact_id", _required_identifier("review_artifact_id", self.review_artifact_id)
        )
        object.__setattr__(
            self,
            "review_artifact_digest",
            _required_sha256("review_artifact_digest", self.review_artifact_digest),
        )
        object.__setattr__(
            self, "review_scope_digest", _required_sha256("review_scope_digest", self.review_scope_digest)
        )
        object.__setattr__(
            self,
            "verifier_contract_digest",
            _required_sha256("verifier_contract_digest", self.verifier_contract_digest),
        )
        object.__setattr__(self, "reviewed_at", _required_utc_timestamp("reviewed_at", self.reviewed_at))
        if self.verdict != "GO":
            raise AgentContractActivationError("agent_contract_review_verdict_invalid")

    def to_record(self) -> dict[str, str]:
        return {
            "schema_version": AGENT_CONTRACT_REVIEW_EVIDENCE_SCHEMA_VERSION,
            "reviewed_commit": self.reviewed_commit,
            "review_artifact_id": self.review_artifact_id,
            "review_artifact_digest": self.review_artifact_digest,
            "review_scope_digest": self.review_scope_digest,
            "verifier_contract_digest": self.verifier_contract_digest,
            "reviewed_at": self.reviewed_at,
            "verdict": self.verdict,
        }


@dataclass(frozen=True, slots=True)
class AgentContractReviewVerificationReceipt:
    activation_key: str
    required_pin_set_digest: str
    activation_policy_digest: str
    review_evidence: AgentContractReviewEvidence
    verifier: AgentContractOwnerPin
    verification_artifact_id: str
    verification_artifact_digest: str
    verified_at: str

    def __post_init__(self) -> None:
        activation_key, _, _ = _required_activation_key(self.activation_key)
        object.__setattr__(self, "activation_key", activation_key)
        object.__setattr__(
            self,
            "required_pin_set_digest",
            _required_sha256("receipt_required_pin_set_digest", self.required_pin_set_digest),
        )
        object.__setattr__(
            self,
            "activation_policy_digest",
            _required_sha256("receipt_activation_policy_digest", self.activation_policy_digest),
        )
        if not isinstance(self.review_evidence, AgentContractReviewEvidence):
            raise AgentContractActivationError("agent_contract_receipt_review_evidence_invalid")
        if not isinstance(self.verifier, AgentContractOwnerPin):
            raise AgentContractActivationError("agent_contract_receipt_verifier_invalid")
        object.__setattr__(
            self,
            "verification_artifact_id",
            _required_identifier("verification_artifact_id", self.verification_artifact_id),
        )
        object.__setattr__(
            self,
            "verification_artifact_digest",
            _required_sha256("verification_artifact_digest", self.verification_artifact_digest),
        )
        verified_at = _required_utc_timestamp("verified_at", self.verified_at)
        if _parse_utc_timestamp(verified_at) < _parse_utc_timestamp(self.review_evidence.reviewed_at):
            raise AgentContractActivationError("agent_contract_review_verification_chronology_invalid")
        object.__setattr__(self, "verified_at", verified_at)

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_REVIEW_RECEIPT_SCHEMA_VERSION,
            "activation_key": self.activation_key,
            "required_pin_set_digest": self.required_pin_set_digest,
            "activation_policy_digest": self.activation_policy_digest,
            "review_evidence": self.review_evidence.to_record(),
            "verifier": self.verifier.to_record(),
            "verification_artifact_id": self.verification_artifact_id,
            "verification_artifact_digest": self.verification_artifact_digest,
            "verified_at": self.verified_at,
        }

    @property
    def receipt_digest(self) -> str:
        return _sha256_json(self.to_record())


@dataclass(frozen=True, slots=True)
class AgentContractSchemaBridgeGateDecisionReceipt:
    """Verified complete-population evidence required before hosted serving.

    This remains a structural receipt only.  The future F0-B owner must mint
    it from the generated production-action roster, the R-029 release-window
    audit, and the separately validated database constraint artifact.  A
    caller-constructed value is never trusted runtime evidence.
    """

    activation_policy_digest: str
    schema_bridge_gate: AgentContractOwnerPin
    release_commit: str
    production_action_roster_digest: str
    production_action_count: int
    schema_defined_action_count: int
    schema_less_action_count: int
    compatibility_hit_count: int
    observation_epoch: int
    observation_started_at: str
    observed_through: str
    constraints_validated: bool
    constraint_validation_artifact_id: str
    constraint_validation_artifact_digest: str
    decision_artifact_id: str
    decision_artifact_digest: str
    decided_at: str
    decision: Literal["ALLOW"] = "ALLOW"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "activation_policy_digest",
            _required_sha256("schema_bridge_activation_policy_digest", self.activation_policy_digest),
        )
        if not isinstance(self.schema_bridge_gate, AgentContractOwnerPin):
            raise AgentContractActivationError("agent_contract_schema_bridge_gate_owner_invalid")
        object.__setattr__(
            self, "release_commit", _required_git_commit("schema_bridge_release_commit", self.release_commit)
        )
        object.__setattr__(
            self,
            "production_action_roster_digest",
            _required_sha256("schema_bridge_production_action_roster_digest", self.production_action_roster_digest),
        )
        production_action_count = _required_positive_bigint(
            "schema_bridge_production_action_count",
            self.production_action_count,
        )
        schema_defined_action_count = _required_bigint(
            "schema_bridge_schema_defined_action_count",
            self.schema_defined_action_count,
        )
        schema_less_action_count = _required_bigint(
            "schema_bridge_schema_less_action_count",
            self.schema_less_action_count,
        )
        compatibility_hit_count = _required_bigint(
            "schema_bridge_compatibility_hit_count",
            self.compatibility_hit_count,
        )
        observation_epoch = _required_positive_bigint(
            "schema_bridge_observation_epoch",
            self.observation_epoch,
        )
        if (
            schema_defined_action_count != production_action_count
            or schema_less_action_count != 0
            or compatibility_hit_count != 0
        ):
            raise AgentContractActivationError("agent_contract_schema_bridge_population_incomplete")
        observation_started_at = _required_utc_timestamp(
            "schema_bridge_observation_started_at",
            self.observation_started_at,
        )
        observed_through = _required_utc_timestamp(
            "schema_bridge_observed_through",
            self.observed_through,
        )
        decided_at = _required_utc_timestamp("schema_bridge_decided_at", self.decided_at)
        if not (
            _parse_utc_timestamp(observation_started_at)
            < _parse_utc_timestamp(observed_through)
            <= _parse_utc_timestamp(decided_at)
        ):
            raise AgentContractActivationError("agent_contract_schema_bridge_observation_window_invalid")
        if self.constraints_validated is not True:
            raise AgentContractActivationError("agent_contract_schema_bridge_constraints_not_validated")
        object.__setattr__(
            self,
            "constraint_validation_artifact_id",
            _required_identifier(
                "schema_bridge_constraint_validation_artifact_id",
                self.constraint_validation_artifact_id,
            ),
        )
        object.__setattr__(
            self,
            "constraint_validation_artifact_digest",
            _required_sha256(
                "schema_bridge_constraint_validation_artifact_digest",
                self.constraint_validation_artifact_digest,
            ),
        )
        object.__setattr__(
            self,
            "decision_artifact_id",
            _required_identifier("schema_bridge_decision_artifact_id", self.decision_artifact_id),
        )
        object.__setattr__(
            self,
            "decision_artifact_digest",
            _required_sha256("schema_bridge_decision_artifact_digest", self.decision_artifact_digest),
        )
        if self.decision != "ALLOW":
            raise AgentContractActivationError("agent_contract_schema_bridge_decision_invalid")
        object.__setattr__(self, "production_action_count", production_action_count)
        object.__setattr__(self, "schema_defined_action_count", schema_defined_action_count)
        object.__setattr__(self, "schema_less_action_count", schema_less_action_count)
        object.__setattr__(self, "compatibility_hit_count", compatibility_hit_count)
        object.__setattr__(self, "observation_epoch", observation_epoch)
        object.__setattr__(self, "observation_started_at", observation_started_at)
        object.__setattr__(self, "observed_through", observed_through)
        object.__setattr__(self, "decided_at", decided_at)

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_SCHEMA_BRIDGE_GATE_RECEIPT_SCHEMA_VERSION,
            "activation_policy_digest": self.activation_policy_digest,
            "schema_bridge_gate": self.schema_bridge_gate.to_record(),
            "release_commit": self.release_commit,
            "production_action_roster_digest": self.production_action_roster_digest,
            "production_action_count": self.production_action_count,
            "schema_defined_action_count": self.schema_defined_action_count,
            "schema_less_action_count": self.schema_less_action_count,
            "compatibility_hit_count": self.compatibility_hit_count,
            "observation_epoch": self.observation_epoch,
            "observation_started_at": self.observation_started_at,
            "observed_through": self.observed_through,
            "constraints_validated": self.constraints_validated,
            "constraint_validation_artifact_id": self.constraint_validation_artifact_id,
            "constraint_validation_artifact_digest": self.constraint_validation_artifact_digest,
            "decision_artifact_id": self.decision_artifact_id,
            "decision_artifact_digest": self.decision_artifact_digest,
            "decided_at": self.decided_at,
            "decision": self.decision,
        }

    @property
    def receipt_digest(self) -> str:
        return _sha256_json(self.to_record())


@dataclass(frozen=True, slots=True)
class AgentContractHostedGateDecisionReceipt:
    activation_key: str
    required_pin_set_digest: str
    activation_policy_digest: str
    hosted_gate: AgentContractOwnerPin
    schema_bridge_gate_receipt_digest: str
    request_review_receipt_digest: str | None
    tool_review_receipt_digest: str
    decision_epoch: int
    decision_artifact_id: str
    decision_artifact_digest: str
    decided_at: str
    decision: Literal["ALLOW"] = "ALLOW"

    def __post_init__(self) -> None:
        activation_key, entry_kind, _ = _required_activation_key(self.activation_key)
        object.__setattr__(self, "activation_key", activation_key)
        object.__setattr__(
            self,
            "required_pin_set_digest",
            _required_sha256("hosted_gate_required_pin_set_digest", self.required_pin_set_digest),
        )
        object.__setattr__(
            self,
            "activation_policy_digest",
            _required_sha256("hosted_gate_activation_policy_digest", self.activation_policy_digest),
        )
        if not isinstance(self.hosted_gate, AgentContractOwnerPin):
            raise AgentContractActivationError("agent_contract_hosted_gate_owner_invalid")
        object.__setattr__(
            self,
            "schema_bridge_gate_receipt_digest",
            _required_sha256(
                "hosted_gate_schema_bridge_gate_receipt_digest",
                self.schema_bridge_gate_receipt_digest,
            ),
        )
        if self.request_review_receipt_digest is not None:
            object.__setattr__(
                self,
                "request_review_receipt_digest",
                _required_sha256(
                    "hosted_gate_request_review_receipt_digest",
                    self.request_review_receipt_digest,
                ),
            )
        if entry_kind == "action" and self.request_review_receipt_digest is None:
            raise AgentContractActivationError("agent_contract_hosted_gate_request_receipt_digest_required")
        if entry_kind == "query" and self.request_review_receipt_digest is not None:
            raise AgentContractActivationError("agent_contract_hosted_gate_request_receipt_digest_forbidden")
        object.__setattr__(
            self,
            "tool_review_receipt_digest",
            _required_sha256(
                "hosted_gate_tool_review_receipt_digest",
                self.tool_review_receipt_digest,
            ),
        )
        object.__setattr__(self, "decision_epoch", _required_bigint("hosted_gate_decision_epoch", self.decision_epoch))
        object.__setattr__(
            self,
            "decision_artifact_id",
            _required_identifier("hosted_gate_decision_artifact_id", self.decision_artifact_id),
        )
        object.__setattr__(
            self,
            "decision_artifact_digest",
            _required_sha256("hosted_gate_decision_artifact_digest", self.decision_artifact_digest),
        )
        object.__setattr__(self, "decided_at", _required_utc_timestamp("hosted_gate_decided_at", self.decided_at))
        if self.decision != "ALLOW":
            raise AgentContractActivationError("agent_contract_hosted_gate_decision_invalid")

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_HOSTED_GATE_RECEIPT_SCHEMA_VERSION,
            "activation_key": self.activation_key,
            "required_pin_set_digest": self.required_pin_set_digest,
            "activation_policy_digest": self.activation_policy_digest,
            "hosted_gate": self.hosted_gate.to_record(),
            "schema_bridge_gate_receipt_digest": self.schema_bridge_gate_receipt_digest,
            "request_review_receipt_digest": self.request_review_receipt_digest,
            "tool_review_receipt_digest": self.tool_review_receipt_digest,
            "decision_epoch": self.decision_epoch,
            "decision_artifact_id": self.decision_artifact_id,
            "decision_artifact_digest": self.decision_artifact_digest,
            "decided_at": self.decided_at,
            "decision": self.decision,
        }

    @property
    def receipt_digest(self) -> str:
        return _sha256_json(self.to_record())


@dataclass(frozen=True, slots=True)
class AgentContractActivationSnapshot:
    activation_key: str
    entry_kind: ActivationEntryKind
    action_type: str | None
    tool_name: str | None
    query_owner: AgentContractOwnerPin | None
    request_state: ActionRequestReleaseState
    tool_state: AgentToolReleaseState
    current_request: ActionContractPin | None
    current_result: ActionResultContractPin | None
    current_tool: AgentToolContractPin | None
    required_pins: AgentContractRequiredPinSet
    activation_policy: AgentContractActivationPolicyPin
    request_review_receipt: AgentContractReviewVerificationReceipt | None
    tool_review_receipt: AgentContractReviewVerificationReceipt | None
    schema_bridge_gate_decision_receipt: AgentContractSchemaBridgeGateDecisionReceipt | None
    hosted_gate_decision_receipt: AgentContractHostedGateDecisionReceipt | None
    request_activated_at: str | None
    request_expires_at: str | None
    tool_activated_at: str | None
    tool_expires_at: str | None
    activation_epoch: int
    update_revision: int
    last_transition_actor: str
    last_transition_reason: str

    def __post_init__(self) -> None:
        activation_key, activation_kind, activation_identity = _required_activation_key(self.activation_key)
        if self.entry_kind not in _ENTRY_KINDS:
            raise AgentContractActivationError("agent_contract_entry_kind_invalid")
        if self.entry_kind != activation_kind:
            raise AgentContractActivationError("agent_contract_entry_kind_invalid")
        if self.request_state not in _REQUEST_STATES:
            raise AgentContractActivationError("agent_contract_request_state_invalid")
        if self.tool_state not in _TOOL_STATES:
            raise AgentContractActivationError("agent_contract_tool_state_invalid")
        action_type = None if self.action_type is None else _required_identifier("action_type", self.action_type)
        tool_name = None if self.tool_name is None else _required_tool_name("tool_name", self.tool_name)
        query_owner = self.query_owner
        if self.entry_kind == "action":
            if (
                action_type is None
                or action_type != activation_identity
                or tool_name is None
                or query_owner is not None
                or self.request_state == "not_applicable"
            ):
                raise AgentContractActivationError("agent_contract_action_identity_invalid")
        elif (
            action_type is not None
            or tool_name is None
            or tool_name != activation_identity
            or not isinstance(query_owner, AgentContractOwnerPin)
        ):
            raise AgentContractActivationError("agent_contract_query_identity_invalid")
        elif (
            self.request_state != "not_applicable"
            or self.current_request is not None
            or self.request_review_receipt is not None
            or self.request_activated_at is not None
            or self.request_expires_at is not None
        ):
            raise AgentContractActivationError("agent_contract_query_request_state_invalid")
        if not isinstance(self.required_pins, AgentContractRequiredPinSet):
            raise AgentContractActivationError("agent_contract_required_pins_invalid")
        if self.required_pins.activation_key != activation_key:
            raise AgentContractActivationError("agent_contract_required_pins_key_mismatch")
        if not isinstance(self.activation_policy, AgentContractActivationPolicyPin):
            raise AgentContractActivationError("agent_contract_activation_policy_invalid")
        for field_name, value, expected_type in (
            ("current_request", self.current_request, ActionContractPin),
            ("current_result", self.current_result, ActionResultContractPin),
            ("current_tool", self.current_tool, AgentToolContractPin),
            ("request_review_receipt", self.request_review_receipt, AgentContractReviewVerificationReceipt),
            ("tool_review_receipt", self.tool_review_receipt, AgentContractReviewVerificationReceipt),
            (
                "schema_bridge_gate_decision_receipt",
                self.schema_bridge_gate_decision_receipt,
                AgentContractSchemaBridgeGateDecisionReceipt,
            ),
            (
                "hosted_gate_decision_receipt",
                self.hosted_gate_decision_receipt,
                AgentContractHostedGateDecisionReceipt,
            ),
        ):
            if value is not None and not isinstance(value, expected_type):
                raise AgentContractActivationError(f"agent_contract_{field_name}_invalid")
        pin_set_digest = self.required_pins.required_pin_set_digest
        policy_digest = self.activation_policy.policy_digest
        schema_bridge_receipt = self.schema_bridge_gate_decision_receipt
        if schema_bridge_receipt is not None and (
            schema_bridge_receipt.activation_policy_digest != policy_digest
            or schema_bridge_receipt.schema_bridge_gate != self.activation_policy.schema_bridge_gate
            or schema_bridge_receipt.release_commit != self.activation_policy.release_commit
            or schema_bridge_receipt.production_action_roster_digest
            != self.activation_policy.production_action_roster_digest
            or schema_bridge_receipt.production_action_count != self.activation_policy.production_action_count
        ):
            raise AgentContractActivationError("agent_contract_schema_bridge_gate_receipt_mismatch")
        for field_name, receipt in (
            ("request", self.request_review_receipt),
            ("tool", self.tool_review_receipt),
        ):
            if receipt is None:
                continue
            if (
                receipt.activation_key != activation_key
                or receipt.required_pin_set_digest != pin_set_digest
                or receipt.activation_policy_digest != policy_digest
                or receipt.verifier != self.activation_policy.review_verifier
                or receipt.review_evidence.reviewed_commit != self.activation_policy.release_commit
                or receipt.review_evidence.verifier_contract_digest
                != self.activation_policy.review_verifier.contract_digest
            ):
                raise AgentContractActivationError(f"agent_contract_{field_name}_review_receipt_mismatch")
        required = self.required_pins.identities
        if self.entry_kind == "action" and any(pin.action_type != action_type for pin in self.required_pins.requests):
            raise AgentContractActivationError("agent_contract_request_history_scope_mismatch")
        if any(pin.tool_name != tool_name for pin in self.required_pins.results) or any(
            pin.tool_name != tool_name for pin in self.required_pins.tools
        ):
            raise AgentContractActivationError("agent_contract_tool_history_scope_mismatch")
        expected_route_identity: tuple[ActivationEntryKind, str | AgentContractOwnerPin]
        if self.entry_kind == "action":
            assert action_type is not None
            expected_route_identity = "action", action_type
        else:
            assert query_owner is not None
            expected_route_identity = "query", query_owner
        if any(pin.route_identity != expected_route_identity for pin in self.required_pins.results) or any(
            pin.route_identity != expected_route_identity for pin in self.required_pins.tools
        ):
            raise AgentContractActivationError("agent_contract_tool_history_route_mismatch")
        if self.current_request is not None and self.current_request.action_type != action_type:
            raise AgentContractActivationError("agent_contract_current_request_action_mismatch")
        if self.current_result is not None and self.current_result.tool_name != tool_name:
            raise AgentContractActivationError("agent_contract_current_result_tool_mismatch")
        if self.current_tool is not None and self.current_tool.tool_name != tool_name:
            raise AgentContractActivationError("agent_contract_current_tool_name_mismatch")
        if self.current_request is not None and ("request", self.current_request.identity) not in required:
            raise AgentContractActivationError("agent_contract_current_request_not_required")
        if self.current_result is not None and ("result", self.current_result.identity) not in required:
            raise AgentContractActivationError("agent_contract_current_result_not_required")
        if self.current_tool is not None and ("tool", self.current_tool.identity) not in required:
            raise AgentContractActivationError("agent_contract_current_tool_not_required")
        if self.request_state in {"shadow", "current"}:
            if self.current_request is None or self.request_review_receipt is None:
                raise AgentContractActivationError("agent_contract_active_request_pins_missing")
        if self.tool_state in {"shadow", "hosted"}:
            if (
                tool_name is None
                or self.current_result is None
                or self.current_tool is None
                or self.tool_review_receipt is None
            ):
                raise AgentContractActivationError("agent_contract_active_tool_pins_missing")
        if self.tool_state == "hosted" and self.entry_kind == "action" and self.request_state != "current":
            raise AgentContractActivationError("agent_contract_hosted_action_request_not_current")
        activation_epoch = _required_bigint("activation_epoch", self.activation_epoch)
        update_revision = _required_bigint("update_revision", self.update_revision)
        request_activated_at, request_expires_at = self._validated_activation_window(
            "request",
            self.request_activated_at,
            self.request_expires_at,
            required=self.request_state == "current",
        )
        tool_activated_at, tool_expires_at = self._validated_activation_window(
            "tool",
            self.tool_activated_at,
            self.tool_expires_at,
            required=self.tool_state == "hosted",
        )
        if request_activated_at is not None:
            if self.request_review_receipt is None or _parse_utc_timestamp(
                self.request_review_receipt.verified_at
            ) > _parse_utc_timestamp(request_activated_at):
                raise AgentContractActivationError("agent_contract_request_review_chronology_invalid")
        if tool_activated_at is not None:
            if self.tool_review_receipt is None or _parse_utc_timestamp(
                self.tool_review_receipt.verified_at
            ) > _parse_utc_timestamp(tool_activated_at):
                raise AgentContractActivationError("agent_contract_tool_review_chronology_invalid")
        if self.tool_state == "hosted":
            gate_receipt = self.hosted_gate_decision_receipt
            request_receipt_digest = (
                None if self.request_review_receipt is None else self.request_review_receipt.receipt_digest
            )
            tool_receipt_digest = None if self.tool_review_receipt is None else self.tool_review_receipt.receipt_digest
            if (
                gate_receipt is None
                or schema_bridge_receipt is None
                or gate_receipt.activation_key != activation_key
                or gate_receipt.required_pin_set_digest != pin_set_digest
                or gate_receipt.activation_policy_digest != policy_digest
                or gate_receipt.hosted_gate != self.activation_policy.hosted_gate
                or gate_receipt.schema_bridge_gate_receipt_digest != schema_bridge_receipt.receipt_digest
                or gate_receipt.request_review_receipt_digest != request_receipt_digest
                or gate_receipt.tool_review_receipt_digest != tool_receipt_digest
                or gate_receipt.decision_epoch != activation_epoch
            ):
                raise AgentContractActivationError("agent_contract_hosted_gate_decision_mismatch")
            assert tool_activated_at is not None
            active_receipts = [self.tool_review_receipt]
            if self.entry_kind == "action":
                active_receipts.append(self.request_review_receipt)
            if (
                _parse_utc_timestamp(schema_bridge_receipt.decided_at) > _parse_utc_timestamp(gate_receipt.decided_at)
                or any(
                    receipt is None
                    or _parse_utc_timestamp(receipt.verified_at) > _parse_utc_timestamp(gate_receipt.decided_at)
                    for receipt in active_receipts
                )
                or _parse_utc_timestamp(gate_receipt.decided_at) > _parse_utc_timestamp(tool_activated_at)
            ):
                raise AgentContractActivationError("agent_contract_hosted_gate_chronology_invalid")
        object.__setattr__(
            self,
            "last_transition_actor",
            _required_identifier("last_transition_actor", self.last_transition_actor),
        )
        object.__setattr__(
            self,
            "last_transition_reason",
            _required_text("last_transition_reason", self.last_transition_reason),
        )
        object.__setattr__(self, "activation_key", activation_key)
        object.__setattr__(self, "action_type", action_type)
        object.__setattr__(self, "tool_name", tool_name)
        object.__setattr__(self, "query_owner", query_owner)
        object.__setattr__(self, "request_activated_at", request_activated_at)
        object.__setattr__(self, "request_expires_at", request_expires_at)
        object.__setattr__(self, "tool_activated_at", tool_activated_at)
        object.__setattr__(self, "tool_expires_at", tool_expires_at)
        object.__setattr__(self, "activation_epoch", activation_epoch)
        object.__setattr__(self, "update_revision", update_revision)

    @staticmethod
    def _validated_activation_window(
        owner: str,
        activated_at: str | None,
        expires_at: str | None,
        *,
        required: bool,
    ) -> tuple[str | None, str | None]:
        normalized_activated_at = (
            None if activated_at is None else _required_utc_timestamp(f"{owner}_activated_at", activated_at)
        )
        normalized_expires_at = (
            None if expires_at is None else _required_utc_timestamp(f"{owner}_expires_at", expires_at)
        )
        if (
            (normalized_activated_at is None) != (normalized_expires_at is None)
            or (required and normalized_activated_at is None)
            or (
                normalized_activated_at is not None
                and normalized_expires_at is not None
                and _parse_utc_timestamp(normalized_expires_at) <= _parse_utc_timestamp(normalized_activated_at)
            )
        ):
            raise AgentContractActivationError(f"agent_contract_{owner}_activation_window_invalid")
        return normalized_activated_at, normalized_expires_at

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_CONTRACT_ACTIVATION_SCHEMA_VERSION,
            "activation_key": self.activation_key,
            "entry_kind": self.entry_kind,
            "action_type": self.action_type,
            "tool_name": self.tool_name,
            "query_owner": None if self.query_owner is None else self.query_owner.to_record(),
            "request_state": self.request_state,
            "tool_state": self.tool_state,
            "current_request": None if self.current_request is None else self.current_request.to_record(),
            "current_result": None if self.current_result is None else self.current_result.to_record(),
            "current_tool": None if self.current_tool is None else self.current_tool.to_record(),
            "required_pins": self.required_pins.to_record(),
            "required_pin_set_digest": self.required_pins.required_pin_set_digest,
            "activation_policy": self.activation_policy.to_record(),
            "activation_policy_digest": self.activation_policy.policy_digest,
            "request_review_receipt": (
                None if self.request_review_receipt is None else self.request_review_receipt.to_record()
            ),
            "tool_review_receipt": (None if self.tool_review_receipt is None else self.tool_review_receipt.to_record()),
            "schema_bridge_gate_decision_receipt": (
                None
                if self.schema_bridge_gate_decision_receipt is None
                else self.schema_bridge_gate_decision_receipt.to_record()
            ),
            "hosted_gate_decision_receipt": (
                None if self.hosted_gate_decision_receipt is None else self.hosted_gate_decision_receipt.to_record()
            ),
            "request_activated_at": self.request_activated_at,
            "request_expires_at": self.request_expires_at,
            "tool_activated_at": self.tool_activated_at,
            "tool_expires_at": self.tool_expires_at,
            "activation_epoch": self.activation_epoch,
            "update_revision": self.update_revision,
            "last_transition_actor": self.last_transition_actor,
            "last_transition_reason": self.last_transition_reason,
        }

    def _transition_semantics(self) -> tuple[object, ...]:
        """Return state that must materially change for a disabled staging CAS."""

        return (
            self.request_state,
            self.tool_state,
            self.current_request,
            self.current_result,
            self.current_tool,
            self.required_pins,
            self.activation_policy,
            self.request_review_receipt,
            self.tool_review_receipt,
            self.schema_bridge_gate_decision_receipt,
            self.hosted_gate_decision_receipt,
            self.request_activated_at,
            self.request_expires_at,
            self.tool_activated_at,
            self.tool_expires_at,
        )

    @property
    def snapshot_digest(self) -> str:
        return _sha256_json(self.to_fingerprint_record())


def validate_activation_transition(
    before: AgentContractActivationSnapshot,
    after: AgentContractActivationSnapshot,
    *,
    expected_revision: int,
    expected_snapshot_digest: str,
) -> AgentContractActivationSnapshot:
    """Validate one future PG CAS transition without mutating external state."""

    if not isinstance(before, AgentContractActivationSnapshot) or not isinstance(
        after, AgentContractActivationSnapshot
    ):
        raise AgentContractActivationError("agent_contract_transition_snapshot_invalid")
    try:
        normalized_expected_revision = _required_bigint("expected_revision", expected_revision)
        expected_digest = _required_sha256("expected_snapshot_digest", expected_snapshot_digest)
    except AgentContractActivationError as exc:
        raise AgentContractActivationError("agent_contract_transition_cas_input_invalid") from exc
    if normalized_expected_revision != before.update_revision or expected_digest != before.snapshot_digest:
        raise AgentContractActivationError("agent_contract_transition_cas_miss")
    identity_before = before.activation_key, before.entry_kind, before.action_type, before.tool_name, before.query_owner
    identity_after = after.activation_key, after.entry_kind, after.action_type, after.tool_name, after.query_owner
    if identity_before != identity_after:
        raise AgentContractActivationError("agent_contract_transition_identity_drift")
    if (
        before.update_revision == POSTGRES_BIGINT_MAX
        or before.activation_epoch == POSTGRES_BIGINT_MAX
        or after.update_revision != before.update_revision + 1
        or after.activation_epoch != before.activation_epoch + 1
    ):
        raise AgentContractActivationError("agent_contract_transition_revision_invalid")
    request_changed = after.request_state != before.request_state
    tool_changed = after.tool_state != before.tool_state
    if request_changed and tool_changed:
        raise AgentContractActivationError("agent_contract_transition_multiple_state_dimensions")
    if request_changed and (before.request_state, after.request_state) not in _REQUEST_TRANSITIONS:
        raise AgentContractActivationError("agent_contract_request_transition_invalid")
    if tool_changed and (before.tool_state, after.tool_state) not in _TOOL_TRANSITIONS:
        raise AgentContractActivationError("agent_contract_tool_transition_invalid")
    if (
        not request_changed
        and not tool_changed
        and not (before.request_state in {"disabled", "not_applicable"} and before.tool_state == "disabled")
    ):
        raise AgentContractActivationError("agent_contract_staging_transition_not_disabled")
    if not request_changed and not tool_changed and before._transition_semantics() == after._transition_semantics():
        raise AgentContractActivationError("agent_contract_transition_semantic_noop")
    if not before.required_pins.identities.issubset(after.required_pins.identities):
        raise AgentContractActivationError("agent_contract_required_pin_removal_forbidden")
    if before.request_state != "disabled" and before.current_request != after.current_request:
        raise AgentContractActivationError("agent_contract_active_request_pin_drift")
    if before.request_state != "disabled" and before.request_review_receipt != after.request_review_receipt:
        raise AgentContractActivationError("agent_contract_active_request_review_drift")
    if before.request_state == "current" and (
        before.request_activated_at != after.request_activated_at
        or before.request_expires_at != after.request_expires_at
    ):
        raise AgentContractActivationError("agent_contract_active_request_window_drift")
    if before.tool_state != "disabled" and (
        before.current_result != after.current_result or before.current_tool != after.current_tool
    ):
        raise AgentContractActivationError("agent_contract_active_tool_pin_drift")
    if before.tool_state != "disabled" and before.tool_review_receipt != after.tool_review_receipt:
        raise AgentContractActivationError("agent_contract_active_tool_review_drift")
    if before.tool_state == "hosted" and (
        before.schema_bridge_gate_decision_receipt != after.schema_bridge_gate_decision_receipt
        or before.hosted_gate_decision_receipt != after.hosted_gate_decision_receipt
        or before.tool_activated_at != after.tool_activated_at
        or before.tool_expires_at != after.tool_expires_at
    ):
        raise AgentContractActivationError("agent_contract_active_tool_window_drift")
    if (
        before.request_state not in {"disabled", "not_applicable"} or before.tool_state != "disabled"
    ) and before.activation_policy != after.activation_policy:
        raise AgentContractActivationError("agent_contract_active_policy_drift")
    return after


def required_pin_set_from_iterables(
    *,
    activation_key: str,
    requests: Iterable[ActionContractPin] = (),
    results: Iterable[ActionResultContractPin] = (),
    tools: Iterable[AgentToolContractPin] = (),
) -> AgentContractRequiredPinSet:
    """Materialize iterables before collision checks; never accept collapsed mappings."""

    if any(isinstance(values, Mapping) for values in (requests, results, tools)):
        raise AgentContractActivationError("agent_contract_required_pin_iterable_invalid")
    try:
        return AgentContractRequiredPinSet(
            activation_key=activation_key,
            requests=tuple(requests),
            results=tuple(results),
            tools=tuple(tools),
        )
    except TypeError as exc:
        raise AgentContractActivationError("agent_contract_required_pin_iterable_invalid") from exc


__all__ = [
    "AGENT_CONTRACT_ACTIVATION_SCHEMA_VERSION",
    "AGENT_CONTRACT_ACTIVATION_POLICY_SCHEMA_VERSION",
    "AGENT_CONTRACT_HISTORY_MAX_ENTRIES",
    "AGENT_CONTRACT_HOSTED_GATE_RECEIPT_SCHEMA_VERSION",
    "AGENT_CONTRACT_REQUIRED_PIN_SET_SCHEMA_VERSION",
    "AGENT_CONTRACT_REQUIRED_PIN_SET_MAX_BYTES",
    "AGENT_CONTRACT_REVIEW_EVIDENCE_SCHEMA_VERSION",
    "AGENT_CONTRACT_REVIEW_RECEIPT_SCHEMA_VERSION",
    "AGENT_CONTRACT_SCHEMA_BRIDGE_GATE_RECEIPT_SCHEMA_VERSION",
    "POSTGRES_BIGINT_MAX",
    "ActionContractPin",
    "ActionRequestReleaseState",
    "ActionResultContractPin",
    "ActivationEntryKind",
    "AgentContractActivationError",
    "AgentContractActivationPolicyPin",
    "AgentContractActivationSnapshot",
    "AgentContractHostedGateDecisionReceipt",
    "AgentContractOwnerPin",
    "AgentProviderMode",
    "AgentContractRequiredPinSet",
    "AgentContractReviewEvidence",
    "AgentContractReviewVerificationReceipt",
    "AgentContractSchemaBridgeGateDecisionReceipt",
    "AgentToolContractPin",
    "AgentToolReleaseState",
    "required_pin_set_from_iterables",
    "validate_activation_transition",
]
