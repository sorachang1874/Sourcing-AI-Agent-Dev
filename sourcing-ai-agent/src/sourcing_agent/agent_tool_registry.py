"""Pure structural contracts for future Agent-callable tools.

This module deliberately owns declarations only.  It does not own mutable tool
release state, catalog visibility, model serving, invocation/result-slot
persistence, authorization decisions, budgets, capabilities, or execution.
Those owners may consume the immutable pins defined here in later integration
batches, but the presence of an :class:`AgentToolSpec` never activates a tool.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, TypeAlias

from .agent_contract_identity import is_valid_agent_tool_name
from .model_tool_runtime import MAX_MESSAGE_CONTENT_BYTES

AGENT_TOOL_REGISTRY_SCHEMA_VERSION = "agent_tool_registry_v1"
AGENT_TOOL_SPEC_SCHEMA_VERSION = "agent_tool_spec_v1"
AGENT_TOOL_SPEC_SCHEMA_VERSION_V2 = "agent_tool_spec_v2"

AgentToolKind: TypeAlias = Literal["action", "query"]
AgentToolEffectClass: TypeAlias = Literal["read_only", "commandless_action", "command_backed_action"]
AgentToolResultLinkPolicy: TypeAlias = Literal[
    "no_command_v1",
    "workflow_command_acceptance_v1",
    "activity_attempt_terminal_v1",
]
AgentToolCommandExposure: TypeAlias = Literal["none", "owner_command_only"]
AgentToolApprovalMode: TypeAlias = Literal["not_required", "human_confirmation_required"]
AgentToolBudgetMode: TypeAlias = Literal["not_required", "parent_reservation_required"]
AgentToolCapabilityMode: TypeAlias = Literal["not_required", "exact_capability_required"]
AgentToolProviderMode: TypeAlias = Literal["simulate", "scripted", "live"]

_TOOL_KINDS = frozenset({"action", "query"})
_EFFECT_CLASSES = frozenset({"read_only", "commandless_action", "command_backed_action"})
_RESULT_LINK_POLICIES = frozenset({"no_command_v1", "workflow_command_acceptance_v1", "activity_attempt_terminal_v1"})
_SPEC_SCHEMA_VERSIONS = frozenset({AGENT_TOOL_SPEC_SCHEMA_VERSION, AGENT_TOOL_SPEC_SCHEMA_VERSION_V2})
AGENT_TOOL_V1_RESULT_LINK_POLICY_BY_EFFECT_CLASS: Mapping[AgentToolEffectClass, AgentToolResultLinkPolicy] = (
    MappingProxyType(
        {
            "read_only": "no_command_v1",
            "commandless_action": "no_command_v1",
            "command_backed_action": "activity_attempt_terminal_v1",
        }
    )
)
_COMMAND_EXPOSURES = frozenset({"none", "owner_command_only"})
_APPROVAL_MODES = frozenset({"not_required", "human_confirmation_required"})
_BUDGET_MODES = frozenset({"not_required", "parent_reservation_required"})
_CAPABILITY_MODES = frozenset({"not_required", "exact_capability_required"})
_PROVIDER_MODES = ("simulate", "scripted", "live")
_AUTHORIZATION_CHECKPOINTS = (
    "catalog_projection",
    "invocation_acceptance",
    "approval_acceptance",
    "dispatch_acceptance",
)
_REQUIRED_AUTHORIZATION_CHECKPOINTS = frozenset({"catalog_projection", "invocation_acceptance"})

_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,255}")
_REQUEST_SCHEMA_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_RESULT_CONTRACT_VERSION_PATTERN = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
_TOOL_CONTRACT_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_RELEASE_OWNER_REVISION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")

_RESULT_VALIDATOR_OWNER_BY_CONTRACT = MappingProxyType(
    {
        "action_result_interpretation_contract_v2": ("sourcing_agent.model_tool_runtime.ToolSpec.validate_input"),
        "action_result_interpretation_contract_v3": (
            "sourcing_agent.model_tool_runtime.InternalToolValidatorSpec.validate_input"
        ),
    }
)


class AgentToolRegistryError(ValueError):
    """Raised when a structural tool declaration fails closed."""


def _required_text(field_name: str, value: object, *, maximum_bytes: int = 16 * 1024) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid") from exc
    if len(encoded) > maximum_bytes or any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
    return value


def _required_identifier(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=256)
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
    return normalized


def _required_version_for_owner(field_name: str, value: object, *, pattern: re.Pattern[str]) -> str:
    normalized = _required_text(field_name, value, maximum_bytes=128)
    if pattern.fullmatch(normalized) is None:
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
    return normalized


def _required_request_schema_version(field_name: str, value: object) -> str:
    return _required_version_for_owner(field_name, value, pattern=_REQUEST_SCHEMA_VERSION_PATTERN)


def _required_result_contract_version(field_name: str, value: object) -> str:
    return _required_version_for_owner(field_name, value, pattern=_RESULT_CONTRACT_VERSION_PATTERN)


def _required_tool_contract_version(field_name: str, value: object) -> str:
    return _required_version_for_owner(field_name, value, pattern=_TOOL_CONTRACT_VERSION_PATTERN)


def _required_release_owner_revision(field_name: str, value: object) -> str:
    return _required_version_for_owner(field_name, value, pattern=_RELEASE_OWNER_REVISION_PATTERN)


def _required_sha256(field_name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
    return value


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentToolRegistryError("agent_tool_record_not_canonical_json") from exc


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class AgentToolOwnerPin:
    """Exact immutable identity of a separate contract owner."""

    owner_id: str
    owner_revision: str
    owner_contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "owner_id", _required_identifier("owner_id", self.owner_id))
        object.__setattr__(
            self,
            "owner_revision",
            _required_tool_contract_version("owner_revision", self.owner_revision),
        )
        object.__setattr__(
            self,
            "owner_contract_digest",
            _required_sha256("owner_contract_digest", self.owner_contract_digest),
        )

    def to_fingerprint_record(self) -> dict[str, str]:
        return {
            "owner_id": self.owner_id,
            "owner_revision": self.owner_revision,
            "owner_contract_digest": self.owner_contract_digest,
        }


@dataclass(frozen=True, slots=True)
class AgentToolRequestPin:
    schema_version: str
    schema_digest: str
    validator_owner: AgentToolOwnerPin
    action_type: str | None
    action_contract_digest: str | None
    query_owner: AgentToolOwnerPin | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schema_version",
            _required_request_schema_version("request_schema_version", self.schema_version),
        )
        object.__setattr__(self, "schema_digest", _required_sha256("request_schema_digest", self.schema_digest))
        if not isinstance(self.validator_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_request_validator_owner_invalid")
        if self.action_type is not None:
            object.__setattr__(self, "action_type", _required_identifier("request_action_type", self.action_type))
            if self.action_contract_digest is None:
                raise AgentToolRegistryError("agent_tool_request_action_contract_digest_required")
            object.__setattr__(
                self,
                "action_contract_digest",
                _required_sha256("request_action_contract_digest", self.action_contract_digest),
            )
            if self.query_owner is not None:
                raise AgentToolRegistryError("agent_tool_request_owner_binding_invalid")
        elif self.action_contract_digest is not None or not isinstance(self.query_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_request_owner_binding_invalid")
        if self.query_owner is not None:
            _required_result_contract_version(
                "request_query_owner_revision",
                self.query_owner.owner_revision,
            )

    @property
    def tool_kind(self) -> AgentToolKind:
        return "action" if self.action_type is not None else "query"

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "schema_digest": self.schema_digest,
            "validator_owner": self.validator_owner.to_fingerprint_record(),
            "tool_kind": self.tool_kind,
            "action_type": self.action_type,
            "action_contract_digest": self.action_contract_digest,
            "query_owner": None if self.query_owner is None else self.query_owner.to_fingerprint_record(),
        }


@dataclass(frozen=True, slots=True)
class AgentToolResultPin:
    tool_name: str
    tool_kind: AgentToolKind
    action_type: str | None
    query_owner: AgentToolOwnerPin | None
    schema_version: str
    schema_digest: str
    serializer_owner: AgentToolOwnerPin
    validator_owner: AgentToolOwnerPin
    validation_contract_version: str
    max_serialized_bytes: int
    max_items: int
    max_depth: int

    def __post_init__(self) -> None:
        if not is_valid_agent_tool_name(self.tool_name):
            raise AgentToolRegistryError("agent_tool_result_tool_name_invalid")
        if self.tool_kind not in _TOOL_KINDS:
            raise AgentToolRegistryError("agent_tool_result_tool_kind_invalid")
        if self.tool_kind == "action":
            if self.action_type is None or self.query_owner is not None:
                raise AgentToolRegistryError("agent_tool_result_owner_binding_invalid")
            object.__setattr__(self, "action_type", _required_identifier("result_action_type", self.action_type))
        elif self.action_type is not None or not isinstance(self.query_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_result_owner_binding_invalid")
        object.__setattr__(
            self,
            "schema_version",
            _required_result_contract_version("result_schema_version", self.schema_version),
        )
        object.__setattr__(self, "schema_digest", _required_sha256("result_schema_digest", self.schema_digest))
        if not isinstance(self.serializer_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_result_serializer_owner_invalid")
        _required_result_contract_version(
            "result_serializer_revision",
            self.serializer_owner.owner_revision,
        )
        if not isinstance(self.validator_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_result_validator_owner_invalid")
        _required_result_contract_version(
            "result_validator_revision",
            self.validator_owner.owner_revision,
        )
        validation_contract_version = _required_result_contract_version(
            "result_validation_contract_version",
            self.validation_contract_version,
        )
        expected_validator_owner = _RESULT_VALIDATOR_OWNER_BY_CONTRACT.get(validation_contract_version)
        if expected_validator_owner is None:
            raise AgentToolRegistryError("agent_tool_result_validation_contract_unsupported")
        if self.validator_owner.owner_id != expected_validator_owner:
            raise AgentToolRegistryError("agent_tool_result_validator_owner_not_canonical")
        object.__setattr__(self, "validation_contract_version", validation_contract_version)
        if self.query_owner is not None:
            _required_result_contract_version(
                "result_query_owner_revision",
                self.query_owner.owner_revision,
            )
        if (
            type(self.max_serialized_bytes) is not int
            or not 2 <= self.max_serialized_bytes <= MAX_MESSAGE_CONTENT_BYTES
        ):
            raise AgentToolRegistryError("agent_tool_result_max_serialized_bytes_invalid")
        if type(self.max_items) is not int or not 1 <= self.max_items <= 100_000:
            raise AgentToolRegistryError("agent_tool_result_max_items_invalid")
        if type(self.max_depth) is not int or not 1 <= self.max_depth <= 64:
            raise AgentToolRegistryError("agent_tool_result_max_depth_invalid")

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "tool_kind": self.tool_kind,
            "action_type": self.action_type,
            "query_owner": None if self.query_owner is None else self.query_owner.to_fingerprint_record(),
            "schema_version": self.schema_version,
            "schema_digest": self.schema_digest,
            "serializer_owner": self.serializer_owner.to_fingerprint_record(),
            "validator_owner": self.validator_owner.to_fingerprint_record(),
            "validation_contract_version": self.validation_contract_version,
            "max_serialized_bytes": self.max_serialized_bytes,
            "max_items": self.max_items,
            "max_depth": self.max_depth,
        }


@dataclass(frozen=True, slots=True)
class AgentToolSimulateFixturePin:
    fixture_id: str
    fixture_revision: str
    fixture_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "fixture_id", _required_identifier("simulate_fixture_id", self.fixture_id))
        object.__setattr__(
            self,
            "fixture_revision",
            _required_tool_contract_version("simulate_fixture_revision", self.fixture_revision),
        )
        object.__setattr__(
            self,
            "fixture_digest",
            _required_sha256("simulate_fixture_digest", self.fixture_digest),
        )

    def to_fingerprint_record(self) -> dict[str, str]:
        return {
            "fixture_id": self.fixture_id,
            "fixture_revision": self.fixture_revision,
            "fixture_digest": self.fixture_digest,
        }


@dataclass(frozen=True, slots=True)
class AgentToolReleaseStateRef:
    """Reference to the sole external release owner; never a release decision."""

    release_owner: AgentToolOwnerPin
    release_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.release_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_release_owner_invalid")
        _required_release_owner_revision("release_owner_revision", self.release_owner.owner_revision)
        object.__setattr__(self, "release_key", _required_identifier("release_key", self.release_key))

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "release_owner": self.release_owner.to_fingerprint_record(),
            "release_key": self.release_key,
        }


@dataclass(frozen=True, slots=True)
class AgentExecutionSubjectRequirement:
    """Structural pin for a future transport-derived execution subject check."""

    subject_schema_version: str
    subject_schema_digest: str
    subject_validator_owner: AgentToolOwnerPin
    permission_policy: AgentToolOwnerPin
    authorization_checkpoints: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "subject_schema_version",
            _required_request_schema_version("execution_subject_schema_version", self.subject_schema_version),
        )
        object.__setattr__(
            self,
            "subject_schema_digest",
            _required_sha256("execution_subject_schema_digest", self.subject_schema_digest),
        )
        if not isinstance(self.subject_validator_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_execution_subject_validator_owner_invalid")
        if not isinstance(self.permission_policy, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_permission_policy_invalid")
        if type(self.authorization_checkpoints) is not tuple:
            raise AgentToolRegistryError("agent_tool_authorization_checkpoints_invalid")
        checkpoints = tuple(self.authorization_checkpoints)
        if (
            not checkpoints
            or any(checkpoint not in _AUTHORIZATION_CHECKPOINTS for checkpoint in checkpoints)
            or len(checkpoints) != len(set(checkpoints))
            or not _REQUIRED_AUTHORIZATION_CHECKPOINTS.issubset(checkpoints)
        ):
            raise AgentToolRegistryError("agent_tool_authorization_checkpoints_invalid")
        canonical = tuple(checkpoint for checkpoint in _AUTHORIZATION_CHECKPOINTS if checkpoint in checkpoints)
        object.__setattr__(self, "authorization_checkpoints", canonical)

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "subject_schema_version": self.subject_schema_version,
            "subject_schema_digest": self.subject_schema_digest,
            "subject_validator_owner": self.subject_validator_owner.to_fingerprint_record(),
            "permission_policy": self.permission_policy.to_fingerprint_record(),
            "authorization_checkpoints": list(self.authorization_checkpoints),
        }


@dataclass(frozen=True, slots=True)
class AgentToolBudgetRequirement:
    mode: AgentToolBudgetMode
    budget_owner: AgentToolOwnerPin | None = None

    def __post_init__(self) -> None:
        if self.mode not in _BUDGET_MODES:
            raise AgentToolRegistryError("agent_tool_budget_requirement_mode_invalid")
        if self.mode == "not_required":
            if self.budget_owner is not None:
                raise AgentToolRegistryError("agent_tool_budget_owner_forbidden")
        elif not isinstance(self.budget_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_budget_owner_required")

    @property
    def required(self) -> bool:
        return self.mode == "parent_reservation_required"

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "budget_owner": None if self.budget_owner is None else self.budget_owner.to_fingerprint_record(),
        }


@dataclass(frozen=True, slots=True)
class AgentToolCapabilityRequirement:
    mode: AgentToolCapabilityMode
    capability_type: str | None = None
    capability_issuer: AgentToolOwnerPin | None = None
    required_provider_modes: tuple[AgentToolProviderMode, ...] = ()

    def __post_init__(self) -> None:
        if self.mode not in _CAPABILITY_MODES:
            raise AgentToolRegistryError("agent_tool_capability_requirement_mode_invalid")
        if type(self.required_provider_modes) is not tuple:
            raise AgentToolRegistryError("agent_tool_capability_provider_modes_invalid")
        modes = tuple(self.required_provider_modes)
        if any(mode not in _PROVIDER_MODES for mode in modes) or len(modes) != len(set(modes)):
            raise AgentToolRegistryError("agent_tool_capability_provider_modes_invalid")
        canonical_modes = tuple(mode for mode in _PROVIDER_MODES if mode in modes)
        if self.mode == "not_required":
            if self.capability_type is not None or self.capability_issuer is not None or canonical_modes:
                raise AgentToolRegistryError("agent_tool_capability_metadata_forbidden")
        else:
            if self.capability_type is None:
                raise AgentToolRegistryError("agent_tool_capability_type_required")
            object.__setattr__(
                self,
                "capability_type",
                _required_identifier("capability_type", self.capability_type),
            )
            if not isinstance(self.capability_issuer, AgentToolOwnerPin):
                raise AgentToolRegistryError("agent_tool_capability_issuer_required")
            if not canonical_modes:
                raise AgentToolRegistryError("agent_tool_capability_provider_modes_required")
        object.__setattr__(self, "required_provider_modes", canonical_modes)

    @property
    def required(self) -> bool:
        return self.mode == "exact_capability_required"

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "capability_type": self.capability_type,
            "capability_issuer": (
                None if self.capability_issuer is None else self.capability_issuer.to_fingerprint_record()
            ),
            "required_provider_modes": list(self.required_provider_modes),
        }


@dataclass(frozen=True, slots=True)
class AgentToolApprovalRequirement:
    mode: AgentToolApprovalMode
    approval_policy: AgentToolOwnerPin

    def __post_init__(self) -> None:
        if self.mode not in _APPROVAL_MODES:
            raise AgentToolRegistryError("agent_tool_approval_requirement_mode_invalid")
        if not isinstance(self.approval_policy, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_approval_policy_invalid")

    @property
    def required(self) -> bool:
        return self.mode == "human_confirmation_required"

    def to_fingerprint_record(self) -> dict[str, object]:
        return {"mode": self.mode, "approval_policy": self.approval_policy.to_fingerprint_record()}


@dataclass(frozen=True, slots=True)
class AgentToolBehavior:
    effect_class: AgentToolEffectClass
    command_exposure: AgentToolCommandExposure
    approval: AgentToolApprovalRequirement
    control_policy: AgentToolOwnerPin
    explicit_result_link_policy: AgentToolResultLinkPolicy | None = None

    def __post_init__(self) -> None:
        if self.effect_class not in _EFFECT_CLASSES:
            raise AgentToolRegistryError("agent_tool_effect_class_invalid")
        if self.command_exposure not in _COMMAND_EXPOSURES:
            raise AgentToolRegistryError("agent_tool_command_exposure_invalid")
        if not isinstance(self.approval, AgentToolApprovalRequirement):
            raise AgentToolRegistryError("agent_tool_approval_requirement_invalid")
        if not isinstance(self.control_policy, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_control_policy_invalid")
        if (
            self.explicit_result_link_policy is not None
            and self.explicit_result_link_policy not in _RESULT_LINK_POLICIES
        ):
            raise AgentToolRegistryError("agent_tool_result_link_policy_invalid")
        if self.effect_class in {"read_only", "commandless_action"} and self.command_exposure != "none":
            raise AgentToolRegistryError("agent_tool_command_exposure_effect_mismatch")
        if self.effect_class == "command_backed_action" and self.command_exposure != "owner_command_only":
            raise AgentToolRegistryError("agent_tool_command_exposure_effect_mismatch")
        if self.effect_class == "read_only" and self.approval.required:
            raise AgentToolRegistryError("agent_tool_read_only_approval_forbidden")
        if self.result_link_policy == "no_command_v1" and self.effect_class not in {
            "read_only",
            "commandless_action",
        }:
            raise AgentToolRegistryError("agent_tool_result_link_policy_effect_mismatch")
        if (
            self.result_link_policy
            in {
                "workflow_command_acceptance_v1",
                "activity_attempt_terminal_v1",
            }
            and self.effect_class != "command_backed_action"
        ):
            raise AgentToolRegistryError("agent_tool_result_link_policy_effect_mismatch")

    @property
    def result_link_policy(self) -> AgentToolResultLinkPolicy:
        if self.explicit_result_link_policy is not None:
            return self.explicit_result_link_policy
        return AGENT_TOOL_V1_RESULT_LINK_POLICY_BY_EFFECT_CLASS[self.effect_class]

    def to_fingerprint_record(self, *, include_result_link_policy: bool = False) -> dict[str, object]:
        record: dict[str, object] = {
            "effect_class": self.effect_class,
            "command_exposure": self.command_exposure,
            "approval": self.approval.to_fingerprint_record(),
            "control_policy": self.control_policy.to_fingerprint_record(),
        }
        if include_result_link_policy:
            record["result_link_policy"] = self.result_link_policy
        return record


@dataclass(frozen=True, slots=True)
class AgentActionToolRoute:
    action_type: str
    workspace_actor_binder: AgentToolOwnerPin
    adapter: AgentToolOwnerPin

    def __post_init__(self) -> None:
        object.__setattr__(self, "action_type", _required_identifier("action_type", self.action_type))
        if not isinstance(self.workspace_actor_binder, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_workspace_actor_binder_invalid")
        if not isinstance(self.adapter, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_adapter_invalid")

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "workspace_actor_binder": self.workspace_actor_binder.to_fingerprint_record(),
            "adapter": self.adapter.to_fingerprint_record(),
        }


@dataclass(frozen=True, slots=True)
class AgentQueryToolRoute:
    query_owner: AgentToolOwnerPin
    workspace_actor_binder: AgentToolOwnerPin
    adapter: AgentToolOwnerPin

    def __post_init__(self) -> None:
        if not isinstance(self.query_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_query_owner_invalid")
        _required_result_contract_version("query_owner_revision", self.query_owner.owner_revision)
        if not isinstance(self.workspace_actor_binder, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_query_workspace_actor_binder_invalid")
        if not isinstance(self.adapter, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_query_adapter_invalid")

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "query_owner": self.query_owner.to_fingerprint_record(),
            "workspace_actor_binder": self.workspace_actor_binder.to_fingerprint_record(),
            "adapter": self.adapter.to_fingerprint_record(),
        }


AgentToolRoute: TypeAlias = AgentActionToolRoute | AgentQueryToolRoute


@dataclass(frozen=True, slots=True)
class AgentToolSpec:
    """Canonical structural identity of one future action or query tool.

    Mutable release state is intentionally absent.  ``release_state_ref`` only
    names the separate owner that may later decide visibility.
    """

    tool_spec_version: str
    tool_name: str
    model_description: str
    tool_kind: AgentToolKind
    request: AgentToolRequestPin
    result: AgentToolResultPin
    route: AgentToolRoute
    simulate_fixture: AgentToolSimulateFixturePin
    release_state_ref: AgentToolReleaseStateRef
    execution_subject: AgentExecutionSubjectRequirement
    budget: AgentToolBudgetRequirement
    capability: AgentToolCapabilityRequirement
    behavior: AgentToolBehavior
    fingerprint_schema_version: str = AGENT_TOOL_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tool_spec_version",
            _required_tool_contract_version("spec_version", self.tool_spec_version),
        )
        if not is_valid_agent_tool_name(self.tool_name):
            raise AgentToolRegistryError("agent_tool_name_invalid")
        object.__setattr__(
            self,
            "model_description",
            _required_text("model_description", self.model_description),
        )
        if self.tool_kind not in _TOOL_KINDS:
            raise AgentToolRegistryError("agent_tool_kind_invalid")
        if self.fingerprint_schema_version not in _SPEC_SCHEMA_VERSIONS:
            raise AgentToolRegistryError("agent_tool_spec_schema_version_invalid")
        for field_name, value, expected_type in (
            ("request", self.request, AgentToolRequestPin),
            ("result", self.result, AgentToolResultPin),
            ("simulate_fixture", self.simulate_fixture, AgentToolSimulateFixturePin),
            ("release_state_ref", self.release_state_ref, AgentToolReleaseStateRef),
            ("execution_subject", self.execution_subject, AgentExecutionSubjectRequirement),
            ("budget", self.budget, AgentToolBudgetRequirement),
            ("capability", self.capability, AgentToolCapabilityRequirement),
            ("behavior", self.behavior, AgentToolBehavior),
        ):
            if not isinstance(value, expected_type):
                raise AgentToolRegistryError(f"agent_tool_{field_name}_invalid")
        if self.fingerprint_schema_version == AGENT_TOOL_SPEC_SCHEMA_VERSION:
            if self.behavior.explicit_result_link_policy is not None:
                raise AgentToolRegistryError("agent_tool_v1_explicit_result_link_policy_forbidden")
        elif self.behavior.explicit_result_link_policy is None:
            raise AgentToolRegistryError("agent_tool_v2_explicit_result_link_policy_required")
        if self.tool_kind == "action":
            if not isinstance(self.route, AgentActionToolRoute):
                raise AgentToolRegistryError("agent_tool_action_route_required")
            if self.request.tool_kind != "action" or self.request.action_type != self.route.action_type:
                raise AgentToolRegistryError("agent_tool_request_route_mismatch")
            if (
                self.result.tool_name != self.tool_name
                or self.result.tool_kind != "action"
                or self.result.action_type != self.route.action_type
            ):
                raise AgentToolRegistryError("agent_tool_result_route_mismatch")
        else:
            if not isinstance(self.route, AgentQueryToolRoute):
                raise AgentToolRegistryError("agent_tool_query_route_required")
            if (
                self.behavior.effect_class != "read_only"
                or self.behavior.command_exposure != "none"
                or self.behavior.approval.required
            ):
                raise AgentToolRegistryError("agent_tool_query_behavior_invalid")
            if self.request.tool_kind != "query" or self.request.query_owner != self.route.query_owner:
                raise AgentToolRegistryError("agent_tool_request_route_mismatch")
            if (
                self.result.tool_name != self.tool_name
                or self.result.tool_kind != "query"
                or self.result.query_owner != self.route.query_owner
            ):
                raise AgentToolRegistryError("agent_tool_result_route_mismatch")
        checkpoints = frozenset(self.execution_subject.authorization_checkpoints)
        if self.behavior.approval.required and "approval_acceptance" not in checkpoints:
            raise AgentToolRegistryError("agent_tool_approval_checkpoint_required")
        if (
            self.behavior.effect_class == "command_backed_action" or self.capability.required or self.budget.required
        ) and "dispatch_acceptance" not in checkpoints:
            raise AgentToolRegistryError("agent_tool_dispatch_checkpoint_required")
        if (self.capability.required or self.budget.required) and self.behavior.effect_class != "command_backed_action":
            raise AgentToolRegistryError("agent_tool_provider_requirement_effect_invalid")
        if "live" in self.capability.required_provider_modes and not self.budget.required:
            raise AgentToolRegistryError("agent_tool_live_budget_reservation_required")
        expected_release_key = (
            f"action:{self.route.action_type}"
            if isinstance(self.route, AgentActionToolRoute)
            else f"query:{self.tool_name}"
        )
        if self.release_state_ref.release_key != expected_release_key:
            raise AgentToolRegistryError("agent_tool_release_key_route_mismatch")

    @property
    def action_type(self) -> str | None:
        return self.route.action_type if isinstance(self.route, AgentActionToolRoute) else None

    @property
    def query_owner_id(self) -> str | None:
        return self.route.query_owner.owner_id if isinstance(self.route, AgentQueryToolRoute) else None

    @property
    def route_identity(self) -> tuple[str, str]:
        identity = self.action_type if self.tool_kind == "action" else self.query_owner_id
        assert identity is not None
        return self.tool_kind, identity

    def to_fingerprint_record(self) -> dict[str, object]:
        action_route = self.route if isinstance(self.route, AgentActionToolRoute) else None
        query_route = self.route if isinstance(self.route, AgentQueryToolRoute) else None
        return {
            "schema_version": self.fingerprint_schema_version,
            "tool_spec_version": self.tool_spec_version,
            "tool_name": self.tool_name,
            "model_description": self.model_description,
            "tool_kind": self.tool_kind,
            "request": self.request.to_fingerprint_record(),
            "result": self.result.to_fingerprint_record(),
            "action_type": None if action_route is None else action_route.action_type,
            "workspace_actor_binder": self.route.workspace_actor_binder.to_fingerprint_record(),
            "adapter": self.route.adapter.to_fingerprint_record(),
            "query_owner_id": None if query_route is None else query_route.query_owner.owner_id,
            "query_owner_revision": None if query_route is None else query_route.query_owner.owner_revision,
            "query_owner_contract_digest": (
                None if query_route is None else query_route.query_owner.owner_contract_digest
            ),
            "simulate_fixture": self.simulate_fixture.to_fingerprint_record(),
            "release_state_ref": self.release_state_ref.to_fingerprint_record(),
            "execution_subject": self.execution_subject.to_fingerprint_record(),
            "budget": self.budget.to_fingerprint_record(),
            "capability": self.capability.to_fingerprint_record(),
            "behavior": self.behavior.to_fingerprint_record(
                include_result_link_policy=(self.fingerprint_schema_version == AGENT_TOOL_SPEC_SCHEMA_VERSION_V2)
            ),
        }

    @property
    def tool_spec_digest(self) -> str:
        return _sha256_json(self.to_fingerprint_record())

    @property
    def historical_identity(self) -> tuple[str, str, str]:
        return self.tool_name, self.tool_spec_version, self.tool_spec_digest

    def to_manifest_record(self) -> dict[str, object]:
        return {**self.to_fingerprint_record(), "tool_spec_digest": self.tool_spec_digest}


@dataclass(frozen=True, slots=True)
class AgentToolRegistry:
    """Immutable historical-spec registry with no activation authority.

    Multiple immutable versions of one tool name may coexist. The separate
    release owner must select an exact historical identity; this registry never
    infers a current tool version from insertion or sort order. The explicit
    ``current_release_owner`` is only the current external owner identity. It
    may rotate without rewriting historical specs or granting serving state.
    """

    specs: tuple[AgentToolSpec, ...] = ()
    current_release_owner: AgentToolOwnerPin | None = None
    _historical: Mapping[tuple[str, str, str], AgentToolSpec] = field(init=False, repr=False, compare=False)
    _by_name: Mapping[str, tuple[AgentToolSpec, ...]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.specs) is not tuple:
            raise AgentToolRegistryError("agent_tool_registry_specs_invalid")
        if self.current_release_owner is None:
            if self.specs:
                raise AgentToolRegistryError("agent_tool_registry_current_release_owner_required")
        elif not isinstance(self.current_release_owner, AgentToolOwnerPin):
            raise AgentToolRegistryError("agent_tool_registry_current_release_owner_invalid")
        else:
            _required_release_owner_revision(
                "registry_current_release_owner_revision",
                self.current_release_owner.owner_revision,
            )
        by_name: dict[str, list[AgentToolSpec]] = {}
        by_historical: dict[tuple[str, str, str], AgentToolSpec] = {}
        by_name_version: dict[tuple[str, str], AgentToolSpec] = {}
        route_to_name: dict[tuple[str, str], str] = {}
        release_key_to_name: dict[str, str] = {}
        route_by_name: dict[str, tuple[str, str]] = {}
        release_key_by_name: dict[str, str] = {}
        kind_by_name: dict[str, str] = {}
        for spec in self.specs:
            if not isinstance(spec, AgentToolSpec):
                raise AgentToolRegistryError("agent_tool_registry_entry_invalid")
            if spec.historical_identity in by_historical:
                raise AgentToolRegistryError(
                    f"agent_tool_registry_duplicate_historical_spec:{spec.tool_name}:{spec.tool_spec_version}"
                )
            name_version = (spec.tool_name, spec.tool_spec_version)
            if name_version in by_name_version:
                raise AgentToolRegistryError(
                    f"agent_tool_registry_version_digest_conflict:{spec.tool_name}:{spec.tool_spec_version}"
                )
            route_name = route_to_name.get(spec.route_identity)
            if route_name is not None and route_name != spec.tool_name:
                raise AgentToolRegistryError(f"agent_tool_registry_route_collision:{spec.route_identity[1]}")
            release_ref = spec.release_state_ref
            release_name = release_key_to_name.get(release_ref.release_key)
            if release_name is not None and release_name != spec.tool_name:
                raise AgentToolRegistryError(f"agent_tool_registry_release_key_collision:{release_ref.release_key}")
            prior_kind = kind_by_name.setdefault(spec.tool_name, spec.tool_kind)
            if prior_kind != spec.tool_kind:
                raise AgentToolRegistryError(f"agent_tool_registry_kind_drift:{spec.tool_name}")
            prior_route = route_by_name.setdefault(spec.tool_name, spec.route_identity)
            if prior_route != spec.route_identity:
                raise AgentToolRegistryError(f"agent_tool_registry_route_drift:{spec.tool_name}")
            prior_release_key = release_key_by_name.setdefault(spec.tool_name, release_ref.release_key)
            if prior_release_key != release_ref.release_key:
                raise AgentToolRegistryError(f"agent_tool_registry_release_key_drift:{spec.tool_name}")
            by_name.setdefault(spec.tool_name, []).append(spec)
            by_historical[spec.historical_identity] = spec
            by_name_version[name_version] = spec
            route_to_name[spec.route_identity] = spec.tool_name
            release_key_to_name[release_ref.release_key] = spec.tool_name
        canonical_specs = tuple(
            sorted(
                self.specs,
                key=lambda spec: (spec.tool_name, spec.tool_spec_version, spec.tool_spec_digest),
            )
        )
        frozen_by_name = {
            tool_name: tuple(
                sorted(
                    specs,
                    key=lambda spec: (spec.tool_spec_version, spec.tool_spec_digest),
                )
            )
            for tool_name, specs in sorted(by_name.items())
        }
        object.__setattr__(self, "specs", canonical_specs)
        object.__setattr__(self, "_historical", MappingProxyType(by_historical))
        object.__setattr__(self, "_by_name", MappingProxyType(frozen_by_name))

    @classmethod
    def from_specs(
        cls,
        specs: Iterable[AgentToolSpec],
        *,
        current_release_owner: AgentToolOwnerPin | None,
    ) -> AgentToolRegistry:
        if isinstance(specs, Mapping):
            raise AgentToolRegistryError("agent_tool_registry_specs_invalid")
        try:
            materialized = tuple(specs)
        except TypeError as exc:
            raise AgentToolRegistryError("agent_tool_registry_specs_invalid") from exc
        for spec in materialized:
            if not isinstance(spec, AgentToolSpec):
                raise AgentToolRegistryError("agent_tool_registry_entry_invalid")
        return cls(materialized, current_release_owner=current_release_owner)

    @property
    def tool_names(self) -> tuple[str, ...]:
        return tuple(self._by_name)

    @property
    def declared_tool_count(self) -> int:
        return len(self._by_name)

    @property
    def historical_spec_count(self) -> int:
        return len(self.specs)

    def specs_for_name(self, tool_name: str) -> tuple[AgentToolSpec, ...]:
        return self._by_name.get(tool_name, ())

    def get_historical(self, tool_name: str, tool_spec_version: str, tool_spec_digest: str) -> AgentToolSpec | None:
        return self._historical.get((tool_name, tool_spec_version, tool_spec_digest))

    def require_historical(self, tool_name: str, tool_spec_version: str, tool_spec_digest: str) -> AgentToolSpec:
        spec = self.get_historical(tool_name, tool_spec_version, tool_spec_digest)
        if spec is None:
            raise AgentToolRegistryError("agent_tool_historical_spec_missing")
        return spec

    def to_manifest_record(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_TOOL_REGISTRY_SCHEMA_VERSION,
            "current_release_owner": (
                None if self.current_release_owner is None else self.current_release_owner.to_fingerprint_record()
            ),
            "declared_tool_count": self.declared_tool_count,
            "historical_spec_count": self.historical_spec_count,
            "tools": [spec.to_manifest_record() for spec in self.specs],
        }

    @property
    def registry_digest(self) -> str:
        return _sha256_json(self.to_manifest_record())


DEFAULT_AGENT_TOOL_REGISTRY = AgentToolRegistry.from_specs((), current_release_owner=None)


__all__ = [
    "AGENT_TOOL_REGISTRY_SCHEMA_VERSION",
    "AGENT_TOOL_SPEC_SCHEMA_VERSION",
    "AGENT_TOOL_SPEC_SCHEMA_VERSION_V2",
    "AGENT_TOOL_V1_RESULT_LINK_POLICY_BY_EFFECT_CLASS",
    "AgentActionToolRoute",
    "AgentExecutionSubjectRequirement",
    "AgentQueryToolRoute",
    "AgentToolApprovalMode",
    "AgentToolApprovalRequirement",
    "AgentToolBehavior",
    "AgentToolBudgetMode",
    "AgentToolBudgetRequirement",
    "AgentToolCapabilityMode",
    "AgentToolCapabilityRequirement",
    "AgentToolCommandExposure",
    "AgentToolEffectClass",
    "AgentToolKind",
    "AgentToolOwnerPin",
    "AgentToolProviderMode",
    "AgentToolResultLinkPolicy",
    "AgentToolRegistry",
    "AgentToolRegistryError",
    "AgentToolReleaseStateRef",
    "AgentToolRequestPin",
    "AgentToolResultPin",
    "AgentToolRoute",
    "AgentToolSimulateFixturePin",
    "AgentToolSpec",
    "DEFAULT_AGENT_TOOL_REGISTRY",
]
