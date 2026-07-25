"""Immutable non-live policy values shared by model and provider operations.

D0g defines a common five-field execution prefix and two deliberately distinct
policy variants.  These values do not consult settings, resolve credentials,
reserve cost, persist rows, call providers, or authorize execution.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal, TypeAlias

from .model_route_registry import (
    EFFECTIVE_MODEL_ROUTE_SNAPSHOT_REF_PREFIX,
    EffectiveModelRouteSnapshot,
    ModelRouteSpec,
    validate_effective_model_route_snapshot,
)
from .model_tool_runtime import ModelTurnExecutionContext

EXTERNAL_EXECUTION_PFX_SCHEMA_VERSION = "external_execution_pfx_v1"
EXTERNAL_EXECUTION_POLICY_HEADER_SCHEMA_VERSION = "external_execution_policy_header_v1"
MODEL_EXECUTION_POLICY_SCHEMA_VERSION = "model_execution_policy_v1"
PROVIDER_OPERATION_POLICY_SCHEMA_VERSION = "provider_operation_policy_v1"
MODEL_EXECUTION_POLICY_VARIANT: Literal["model_execution_v1"] = "model_execution_v1"
PROVIDER_OPERATION_POLICY_VARIANT: Literal["provider_operation_v1"] = "provider_operation_v1"
MODEL_EXECUTION_TRANSPORT_KIND: Literal["model_tool"] = "model_tool"
PROVIDER_OPERATION_TRANSPORT_KIND: Literal["provider_operation"] = "provider_operation"
MODEL_EXECUTION_EVIDENCE_VARIANT: Literal["model_tool_v1"] = "model_tool_v1"
PROVIDER_OPERATION_EVIDENCE_VARIANT: Literal["provider_operation_evidence_draft_v1"] = (
    "provider_operation_evidence_draft_v1"
)
PROVIDER_OPERATION_POLICY_FAMILY: Literal["provider_operation"] = "provider_operation"
EXTERNAL_EXECUTION_AUTHORIZATION_AVAILABLE = False

ExternalExecutionPolicyVariant: TypeAlias = Literal["model_execution_v1", "provider_operation_v1"]
ExternalExecutionProviderMode: TypeAlias = Literal["live", "simulate", "scripted"]
ExternalExecutionTransportKind: TypeAlias = Literal["model_tool", "provider_operation"]
ExternalExecutionEvidenceVariant: TypeAlias = Literal[
    "model_tool_v1",
    "provider_operation_evidence_draft_v1",
]
EXTERNAL_EXECUTION_PROVIDER_MODE_ORDER: tuple[ExternalExecutionProviderMode, ...] = (
    "scripted",
    "simulate",
    "live",
)

EXTERNAL_EXECUTION_POLICY_HEADER_RECORD_KEYS = frozenset(
    {
        "schema_version",
        "policy_variant",
        "policy_id",
        "policy_revision",
        "transport_kind",
        "evidence_variant",
        "provider_family",
        "allowed_provider_modes",
        "retry_owner",
        "budget_policy_id",
        "live_gate_provider_name",
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_digest",
        "coordination_plan_review_id",
    }
)


class ExternalExecutionPolicyError(RuntimeError):
    """Raised when a policy value is noncanonical or crosses variants/PFXes."""


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExternalExecutionPolicyError("external_execution_policy_json_invalid") from exc


def _digest_record(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _required_text(field_name: str, value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ExternalExecutionPolicyError(f"external_execution_policy_{field_name}_invalid")
    return value


def _required_sha256(field_name: str, value: object) -> str:
    normalized = _required_text(field_name, value)
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ExternalExecutionPolicyError(f"external_execution_policy_{field_name}_invalid")
    return normalized


def _pfx_digest(record: dict[str, object]) -> str:
    return _digest_record({"schema_version": EXTERNAL_EXECUTION_PFX_SCHEMA_VERSION, **record})


def _validate_allowed_provider_modes(
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...],
) -> None:
    if type(allowed_provider_modes) is not tuple or not allowed_provider_modes:
        raise ExternalExecutionPolicyError("external_execution_policy_allowed_provider_modes_invalid")
    if any(mode not in EXTERNAL_EXECUTION_PROVIDER_MODE_ORDER for mode in allowed_provider_modes):
        raise ExternalExecutionPolicyError("external_execution_policy_allowed_provider_modes_invalid")
    canonical = tuple(mode for mode in EXTERNAL_EXECUTION_PROVIDER_MODE_ORDER if mode in allowed_provider_modes)
    if allowed_provider_modes != canonical:
        raise ExternalExecutionPolicyError("external_execution_policy_allowed_provider_modes_noncanonical")


@dataclass(frozen=True, slots=True)
class ExternalExecutionPolicyHeader:
    """Shared immutable full-PFX header; not an execution capability."""

    schema_version: str
    policy_variant: ExternalExecutionPolicyVariant
    policy_id: str
    policy_revision: str
    transport_kind: ExternalExecutionTransportKind
    evidence_variant: ExternalExecutionEvidenceVariant
    provider_family: str
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...]
    retry_owner: str
    budget_policy_id: str
    live_gate_provider_name: str
    runtime_namespace: str
    provider_mode: ExternalExecutionProviderMode
    workspace_id: str
    scope_digest: str
    coordination_plan_review_id: int

    def __post_init__(self) -> None:
        if self.schema_version != EXTERNAL_EXECUTION_POLICY_HEADER_SCHEMA_VERSION:
            raise ExternalExecutionPolicyError("external_execution_policy_header_schema_invalid")
        if self.policy_variant not in {
            MODEL_EXECUTION_POLICY_VARIANT,
            PROVIDER_OPERATION_POLICY_VARIANT,
        }:
            raise ExternalExecutionPolicyError("external_execution_policy_variant_invalid")
        expected_transport_evidence = {
            MODEL_EXECUTION_POLICY_VARIANT: (
                MODEL_EXECUTION_TRANSPORT_KIND,
                MODEL_EXECUTION_EVIDENCE_VARIANT,
            ),
            PROVIDER_OPERATION_POLICY_VARIANT: (
                PROVIDER_OPERATION_TRANSPORT_KIND,
                PROVIDER_OPERATION_EVIDENCE_VARIANT,
            ),
        }
        if (self.transport_kind, self.evidence_variant) != expected_transport_evidence[self.policy_variant]:
            raise ExternalExecutionPolicyError("external_execution_policy_variant_transport_evidence_mismatch")
        for field_name in (
            "policy_id",
            "policy_revision",
            "provider_family",
            "retry_owner",
            "budget_policy_id",
            "live_gate_provider_name",
            "runtime_namespace",
            "workspace_id",
        ):
            _required_text(field_name, getattr(self, field_name))
        _validate_allowed_provider_modes(self.allowed_provider_modes)
        if self.provider_mode not in {"live", "simulate", "scripted"}:
            raise ExternalExecutionPolicyError("external_execution_policy_provider_mode_invalid")
        if self.provider_mode != self.provider_mode.lower():
            raise ExternalExecutionPolicyError("external_execution_policy_provider_mode_noncanonical")
        if self.provider_mode not in self.allowed_provider_modes:
            raise ExternalExecutionPolicyError("external_execution_policy_provider_mode_not_allowed")
        _required_sha256("scope_digest", self.scope_digest)
        if (
            isinstance(self.coordination_plan_review_id, bool)
            or not isinstance(self.coordination_plan_review_id, int)
            or self.coordination_plan_review_id <= 0
        ):
            raise ExternalExecutionPolicyError("external_execution_policy_review_id_invalid")

    def pfx_record(self) -> dict[str, object]:
        return {
            "runtime_namespace": self.runtime_namespace,
            "provider_mode": self.provider_mode,
            "workspace_id": self.workspace_id,
            "scope_digest": self.scope_digest,
            "coordination_plan_review_id": self.coordination_plan_review_id,
        }

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_variant": self.policy_variant,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "transport_kind": self.transport_kind,
            "evidence_variant": self.evidence_variant,
            "provider_family": self.provider_family,
            "allowed_provider_modes": list(self.allowed_provider_modes),
            "retry_owner": self.retry_owner,
            "budget_policy_id": self.budget_policy_id,
            "live_gate_provider_name": self.live_gate_provider_name,
            **self.pfx_record(),
        }

    @property
    def pfx_digest(self) -> str:
        return _pfx_digest(self.pfx_record())

    @property
    def header_digest(self) -> str:
        return _digest_record(self.to_record())


def compile_external_execution_policy_header(
    *,
    policy_variant: ExternalExecutionPolicyVariant,
    policy_id: str,
    policy_revision: str,
    transport_kind: ExternalExecutionTransportKind,
    evidence_variant: ExternalExecutionEvidenceVariant,
    provider_family: str,
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...],
    retry_owner: str,
    budget_policy_id: str,
    live_gate_provider_name: str,
    runtime_namespace: str,
    provider_mode: ExternalExecutionProviderMode,
    workspace_id: str,
    scope_digest: str,
    coordination_plan_review_id: int,
) -> ExternalExecutionPolicyHeader:
    """Compile the common header without reading environment or registries."""

    return ExternalExecutionPolicyHeader(
        schema_version=EXTERNAL_EXECUTION_POLICY_HEADER_SCHEMA_VERSION,
        policy_variant=policy_variant,
        policy_id=policy_id,
        policy_revision=policy_revision,
        transport_kind=transport_kind,
        evidence_variant=evidence_variant,
        provider_family=provider_family,
        allowed_provider_modes=allowed_provider_modes,
        retry_owner=retry_owner,
        budget_policy_id=budget_policy_id,
        live_gate_provider_name=live_gate_provider_name,
        runtime_namespace=runtime_namespace,
        provider_mode=provider_mode,
        workspace_id=workspace_id,
        scope_digest=scope_digest,
        coordination_plan_review_id=coordination_plan_review_id,
    )


def _model_header_for_context(
    context: ModelTurnExecutionContext,
    effective_route_snapshot: EffectiveModelRouteSnapshot,
    *,
    policy_id: str,
    policy_revision: str,
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...],
    retry_owner: str,
    budget_policy_id: str,
) -> ExternalExecutionPolicyHeader:
    return compile_external_execution_policy_header(
        policy_variant=MODEL_EXECUTION_POLICY_VARIANT,
        policy_id=policy_id,
        policy_revision=policy_revision,
        transport_kind=MODEL_EXECUTION_TRANSPORT_KIND,
        evidence_variant=MODEL_EXECUTION_EVIDENCE_VARIANT,
        provider_family=effective_route_snapshot.provider_family,
        allowed_provider_modes=allowed_provider_modes,
        retry_owner=retry_owner,
        budget_policy_id=budget_policy_id,
        live_gate_provider_name=effective_route_snapshot.live_gate_provider_name,
        runtime_namespace=context.runtime_namespace,
        provider_mode=context.provider_mode,
        workspace_id=context.workspace_id,
        scope_digest=context.scope_digest,
        coordination_plan_review_id=context.coordination_plan_review_id,
    )


@dataclass(frozen=True, slots=True)
class ModelExecutionPolicy:
    """Compiled model-only policy snapshot with zero execution authority."""

    schema_version: str
    header: ExternalExecutionPolicyHeader
    execution_context_digest: str
    execution_context_pfx_digest: str
    route_id: str
    route_revision: str
    effective_route_snapshot_ref: str
    effective_route_snapshot_digest: str
    provider: str
    requested_model: str
    api_style: str
    budget_class: str
    budget_digest: str
    prompt_policy_version: str
    permission_scope_revision: str
    outbound_policy_revision: str
    model_safe_schema_revision: str
    execution_authorized: Literal[False]

    def __post_init__(self) -> None:
        if self.schema_version != MODEL_EXECUTION_POLICY_SCHEMA_VERSION:
            raise ExternalExecutionPolicyError("model_execution_policy_schema_invalid")
        if type(self.header) is not ExternalExecutionPolicyHeader:
            raise ExternalExecutionPolicyError("model_execution_policy_header_type_invalid")
        if self.header.policy_variant != MODEL_EXECUTION_POLICY_VARIANT:
            raise ExternalExecutionPolicyError("model_execution_policy_header_variant_mismatch")
        if self.execution_authorized is not False:
            raise ExternalExecutionPolicyError("model_execution_policy_authorization_forbidden_d0g")
        for field_name in (
            "route_id",
            "effective_route_snapshot_ref",
            "provider",
            "requested_model",
            "api_style",
            "budget_class",
            "prompt_policy_version",
            "permission_scope_revision",
            "outbound_policy_revision",
            "model_safe_schema_revision",
        ):
            _required_text(field_name, getattr(self, field_name))
        for field_name in (
            "execution_context_digest",
            "execution_context_pfx_digest",
            "route_revision",
            "effective_route_snapshot_digest",
            "budget_digest",
        ):
            _required_sha256(field_name, getattr(self, field_name))
        if self.execution_context_pfx_digest != self.header.pfx_digest:
            raise ExternalExecutionPolicyError("model_execution_policy_header_context_pfx_mismatch")
        expected_snapshot_ref = f"{EFFECTIVE_MODEL_ROUTE_SNAPSHOT_REF_PREFIX}{self.effective_route_snapshot_digest}"
        if self.effective_route_snapshot_ref != expected_snapshot_ref:
            raise ExternalExecutionPolicyError("model_execution_policy_snapshot_ref_mismatch")

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "header": self.header.to_record(),
            "execution_context_digest": self.execution_context_digest,
            "execution_context_pfx_digest": self.execution_context_pfx_digest,
            "route_id": self.route_id,
            "route_revision": self.route_revision,
            "effective_route_snapshot_ref": self.effective_route_snapshot_ref,
            "effective_route_snapshot_digest": self.effective_route_snapshot_digest,
            "provider": self.provider,
            "requested_model": self.requested_model,
            "api_style": self.api_style,
            "budget_class": self.budget_class,
            "budget_digest": self.budget_digest,
            "prompt_policy_version": self.prompt_policy_version,
            "permission_scope_revision": self.permission_scope_revision,
            "outbound_policy_revision": self.outbound_policy_revision,
            "model_safe_schema_revision": self.model_safe_schema_revision,
            "execution_authorized": self.execution_authorized,
        }

    @property
    def policy_digest(self) -> str:
        return _digest_record(self.to_record())


def compile_model_execution_policy(
    context: ModelTurnExecutionContext,
    route: ModelRouteSpec,
    effective_route_snapshot: EffectiveModelRouteSnapshot,
    *,
    policy_id: str,
    policy_revision: str,
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...],
    retry_owner: str,
    budget_policy_id: str,
    header: ExternalExecutionPolicyHeader | None = None,
) -> ModelExecutionPolicy:
    """Compile exact model policy evidence; never enable or dispatch a route."""

    if type(context) is not ModelTurnExecutionContext:
        raise ExternalExecutionPolicyError("model_execution_policy_context_type_invalid")
    if type(route) is not ModelRouteSpec:
        raise ExternalExecutionPolicyError("model_execution_policy_route_type_invalid")
    validate_effective_model_route_snapshot(effective_route_snapshot, route)
    expected = {
        "route_id": (context.route_id, route.route_id),
        "route_revision": (context.route_revision, route.revision),
        "snapshot_ref": (context.effective_route_snapshot_ref, effective_route_snapshot.snapshot_ref),
        "snapshot_digest": (
            context.effective_route_snapshot_digest,
            effective_route_snapshot.snapshot_digest,
        ),
        "budget_class": (context.budget.budget_class, route.budget_class),
    }
    mismatches = sorted(field_name for field_name, (actual, pinned) in expected.items() if actual != pinned)
    if mismatches:
        raise ExternalExecutionPolicyError(f"model_execution_policy_context_route_mismatch:{','.join(mismatches)}")
    expected_header = _model_header_for_context(
        context,
        effective_route_snapshot,
        policy_id=policy_id,
        policy_revision=policy_revision,
        allowed_provider_modes=allowed_provider_modes,
        retry_owner=retry_owner,
        budget_policy_id=budget_policy_id,
    )
    if header is not None and header != expected_header:
        raise ExternalExecutionPolicyError("model_execution_policy_header_context_mismatch")
    effective_header = expected_header if header is None else header
    return ModelExecutionPolicy(
        schema_version=MODEL_EXECUTION_POLICY_SCHEMA_VERSION,
        header=effective_header,
        execution_context_digest=context.context_digest,
        execution_context_pfx_digest=_pfx_digest(context.pfx_record()),
        route_id=route.route_id,
        route_revision=route.revision,
        effective_route_snapshot_ref=effective_route_snapshot.snapshot_ref,
        effective_route_snapshot_digest=effective_route_snapshot.snapshot_digest,
        provider=route.provider,
        requested_model=route.model,
        api_style=route.api_style,
        budget_class=route.budget_class,
        budget_digest=context.budget.budget_digest,
        prompt_policy_version=context.prompt_policy_version,
        permission_scope_revision=context.permission_scope_revision,
        outbound_policy_revision=context.outbound_policy_revision,
        model_safe_schema_revision=context.model_safe_schema_revision,
        execution_authorized=False,
    )


@dataclass(frozen=True, slots=True)
class ProviderOperationPolicyInput:
    """Typed complete input for one provider-operation policy compile."""

    policy_id: str
    policy_revision: str
    provider_family: str
    allowed_provider_modes: tuple[ExternalExecutionProviderMode, ...]
    retry_owner: str
    budget_policy_id: str
    live_gate_provider_name: str
    runtime_namespace: str
    provider_mode: ExternalExecutionProviderMode
    workspace_id: str
    scope_digest: str
    coordination_plan_review_id: int
    provider: str
    operation_kind: str
    operation_schema_revision: str
    request_schema_digest: str
    budget_class: str
    outbound_policy_revision: str

    def __post_init__(self) -> None:
        for field_name in (
            "provider",
            "operation_kind",
            "operation_schema_revision",
            "budget_class",
            "outbound_policy_revision",
        ):
            _required_text(field_name, getattr(self, field_name))
        _required_sha256("request_schema_digest", self.request_schema_digest)
        self.compile_header()

    def compile_header(self) -> ExternalExecutionPolicyHeader:
        return compile_external_execution_policy_header(
            policy_variant=PROVIDER_OPERATION_POLICY_VARIANT,
            policy_id=self.policy_id,
            policy_revision=self.policy_revision,
            transport_kind=PROVIDER_OPERATION_TRANSPORT_KIND,
            evidence_variant=PROVIDER_OPERATION_EVIDENCE_VARIANT,
            provider_family=self.provider_family,
            allowed_provider_modes=self.allowed_provider_modes,
            retry_owner=self.retry_owner,
            budget_policy_id=self.budget_policy_id,
            live_gate_provider_name=self.live_gate_provider_name,
            runtime_namespace=self.runtime_namespace,
            provider_mode=self.provider_mode,
            workspace_id=self.workspace_id,
            scope_digest=self.scope_digest,
            coordination_plan_review_id=self.coordination_plan_review_id,
        )


@dataclass(frozen=True, slots=True)
class ProviderOperationPolicy:
    """Provider-operation policy shape, intentionally not a model transport."""

    schema_version: str
    header: ExternalExecutionPolicyHeader
    policy_family: Literal["provider_operation"]
    provider: str
    operation_kind: str
    operation_schema_revision: str
    request_schema_digest: str
    budget_class: str
    outbound_policy_revision: str
    execution_authorized: Literal[False]

    def __post_init__(self) -> None:
        if self.schema_version != PROVIDER_OPERATION_POLICY_SCHEMA_VERSION:
            raise ExternalExecutionPolicyError("provider_operation_policy_schema_invalid")
        if type(self.header) is not ExternalExecutionPolicyHeader:
            raise ExternalExecutionPolicyError("provider_operation_policy_header_type_invalid")
        if self.header.policy_variant != PROVIDER_OPERATION_POLICY_VARIANT:
            raise ExternalExecutionPolicyError("provider_operation_policy_header_variant_mismatch")
        if self.policy_family != PROVIDER_OPERATION_POLICY_FAMILY:
            raise ExternalExecutionPolicyError("provider_operation_policy_family_invalid")
        if self.execution_authorized is not False:
            raise ExternalExecutionPolicyError("provider_operation_policy_authorization_forbidden_d0g")
        for field_name in (
            "provider",
            "operation_kind",
            "operation_schema_revision",
            "budget_class",
            "outbound_policy_revision",
        ):
            _required_text(field_name, getattr(self, field_name))
        _required_sha256("request_schema_digest", self.request_schema_digest)

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "header": self.header.to_record(),
            "policy_family": self.policy_family,
            "provider": self.provider,
            "operation_kind": self.operation_kind,
            "operation_schema_revision": self.operation_schema_revision,
            "request_schema_digest": self.request_schema_digest,
            "budget_class": self.budget_class,
            "outbound_policy_revision": self.outbound_policy_revision,
            "execution_authorized": self.execution_authorized,
        }

    @property
    def policy_digest(self) -> str:
        return _digest_record(self.to_record())


def compile_provider_operation_policy(
    policy_input: ProviderOperationPolicyInput,
) -> ProviderOperationPolicy:
    """Compile provider-operation policy metadata with zero runtime activation."""

    if type(policy_input) is not ProviderOperationPolicyInput:
        raise ExternalExecutionPolicyError("provider_operation_policy_input_type_invalid")
    return ProviderOperationPolicy(
        schema_version=PROVIDER_OPERATION_POLICY_SCHEMA_VERSION,
        header=policy_input.compile_header(),
        policy_family=PROVIDER_OPERATION_POLICY_FAMILY,
        provider=policy_input.provider,
        operation_kind=policy_input.operation_kind,
        operation_schema_revision=policy_input.operation_schema_revision,
        request_schema_digest=policy_input.request_schema_digest,
        budget_class=policy_input.budget_class,
        outbound_policy_revision=policy_input.outbound_policy_revision,
        execution_authorized=False,
    )


__all__ = [
    "EXTERNAL_EXECUTION_AUTHORIZATION_AVAILABLE",
    "EXTERNAL_EXECUTION_POLICY_HEADER_RECORD_KEYS",
    "EXTERNAL_EXECUTION_POLICY_HEADER_SCHEMA_VERSION",
    "MODEL_EXECUTION_EVIDENCE_VARIANT",
    "MODEL_EXECUTION_POLICY_SCHEMA_VERSION",
    "MODEL_EXECUTION_POLICY_VARIANT",
    "MODEL_EXECUTION_TRANSPORT_KIND",
    "PROVIDER_OPERATION_POLICY_FAMILY",
    "PROVIDER_OPERATION_EVIDENCE_VARIANT",
    "PROVIDER_OPERATION_POLICY_SCHEMA_VERSION",
    "PROVIDER_OPERATION_POLICY_VARIANT",
    "PROVIDER_OPERATION_TRANSPORT_KIND",
    "ExternalExecutionPolicyError",
    "ExternalExecutionPolicyHeader",
    "ModelExecutionPolicy",
    "ProviderOperationPolicy",
    "ProviderOperationPolicyInput",
    "compile_external_execution_policy_header",
    "compile_model_execution_policy",
    "compile_provider_operation_policy",
]
