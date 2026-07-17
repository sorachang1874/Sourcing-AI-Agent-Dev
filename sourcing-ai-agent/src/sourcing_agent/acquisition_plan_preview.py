"""Pure, capability-free acquisition-plan preview contract.

This D1n V1 leaf owns deterministic validation and serialization only.  The
future integration owner supplies authenticated target pins, a monotonically
allocated preview revision, and timestamps, then persists the returned record
inside the commandless PG unit of work.  This module performs no storage,
provider, model, network, release-state, or authorization work.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

from .action_request_schema import DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER
from .action_result_schema import (
    ACTION_RESULT_VALIDATOR_OWNER,
    ActionResultActionOwner,
    ActionResultSchemaError,
    ActionResultSpec,
    ActionResultValueRole,
)
from .cohort_provider_compiler import (
    COHORT_PROVIDER_COMPANY_TARGET_VERSION,
    MAX_COHORT_PROVIDER_LANES,
    CohortProviderCompilationError,
    CohortProviderCompiler,
    cohort_provider_planning_manifest_schema,
)
from .cohort_selection import (
    COHORT_SELECTION_REGISTRY_VERSION,
    COHORT_SELECTION_SCHEMA_VERSION,
    CohortSelectionValidationError,
    cohort_selection_digest,
    cohort_selection_registry_digest,
    normalize_cohort_selection,
)
from .model_tool_runtime import ModelToolSchemaError, ToolSpec
from .query_signal_knowledge import ROLE_BUCKET_KNOWLEDGE

ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION = "acquisition_plan_preview_request_v2"
ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION = "acquisition_plan_preview.v2"
ACQUISITION_PLAN_EFFECTIVE_REQUEST_SCHEMA_VERSION = "acquisition_plan_effective_request.v2"
CANONICAL_COMPANY_TARGET_SCHEMA_VERSION = COHORT_PROVIDER_COMPANY_TARGET_VERSION
ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION = "acquisition_plan_preview_result_v2"
ACQUISITION_PLAN_PREVIEW_SERIALIZER_OWNER = "planner.acquisition_plan_preview_result_serializer_v2"
ACQUISITION_PLAN_PREVIEW_SERIALIZER_REVISION = "acquisition_plan_preview_result_serializer_v2"
ACQUISITION_PLAN_PREVIEW_CONFIRMATION_TYPE = "approve_exact_start_action"
ACQUISITION_PLAN_PREVIEW_CONFIRMATION_INSTRUCTION = (
    "Review this exact preview, then explicitly approve the separately persisted start action "
    "that binds its id, revision, and digest."
)
ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE = "linkedin_profile_search"
ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT = "requested_cohort"
ACQUISITION_PLAN_PREVIEW_PROVIDER_MODES = ("simulate", "scripted", "live")

MAX_PREVIEW_TTL_SECONDS = 24 * 60 * 60
MAX_PREVIEW_PROVIDER_ITEMS = 1_000
MAX_PREVIEW_OUTPUT_CANDIDATES = 500
MAX_PREVIEW_COST_MICRO_USD = 100_000_000
MAX_PREVIEW_ELAPSED_SECONDS = 24 * 60 * 60
MAX_PREVIEW_THEMATIC_CONSTRAINTS = 12
MAX_PREVIEW_PROVIDER_COMPANY_LABELS = 16
MAX_PREVIEW_REVISION = 9_223_372_036_854_775_807

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_UTC_TIMESTAMP_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_TERMINAL_REASON_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,79}")
_ERROR_FIELD_PATTERN = re.compile(r"(?:__none__|[a-z][a-z0-9_]{0,63}(?:\.[a-z][a-z0-9_]{0,63}){0,7})")
_MAX_ERROR_FIELD_LENGTH = 200

_SHA256_SCHEMA_PATTERN = r"^[0-9a-f]{64}$"
_ID_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$"
_VERSION_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_UTC_TIMESTAMP_SCHEMA_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
_TERMINAL_REASON_SCHEMA_PATTERN = r"^[a-z0-9][a-z0-9_]{0,79}$"
_ERROR_FIELD_SCHEMA_PATTERN = r"^(?:__none__|[a-z][a-z0-9_]{0,63}(?:\.[a-z][a-z0-9_]{0,63}){0,7})$"
_NONEMPTY_TEXT_SCHEMA_PATTERN = r"^(?s:.*\S.*)$"
ACQUISITION_PLAN_PREVIEW_NO_FIELD = "__none__"
_BUDGET_FIELDS = (
    "max_provider_calls",
    "max_provider_items",
    "max_output_candidates",
    "max_cost_micro_usd",
    "max_elapsed_seconds",
)


@dataclass(frozen=True, slots=True)
class AcquisitionPlanPreviewError(ValueError):
    """Stable fail-closed error for pure preview validation/compilation."""

    code: str
    field: str = ""
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.code}: {self.field}" if self.field else self.code

    def to_result(self) -> dict[str, Any]:
        """Return the canonical model-visible error variant without diagnostics."""

        canonical_field = self.field if _is_canonical_error_field(self.field) else ACQUISITION_PLAN_PREVIEW_NO_FIELD
        return acquisition_plan_preview_error_result(
            reason=self.code,
            field=canonical_field or ACQUISITION_PLAN_PREVIEW_NO_FIELD,
        )


def _closed_object(
    properties: dict[str, Any],
    *,
    required: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties) if required is None else list(required),
        "additionalProperties": False,
    }


def _string_schema(*, maximum: int, minimum: int = 1, pattern: str = "") -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "string", "minLength": minimum, "maxLength": maximum}
    if pattern:
        schema["pattern"] = pattern
    return schema


def _sha256_schema() -> dict[str, Any]:
    return _string_schema(maximum=64, minimum=64, pattern=_SHA256_SCHEMA_PATTERN)


def _cohort_selection_schema() -> dict[str, Any]:
    selectable_role_ids = sorted(
        ROLE_BUCKET_KNOWLEDGE,
        key=lambda role_id: (int(ROLE_BUCKET_KNOWLEDGE[role_id]["selectable_order"]), role_id),
    )
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": COHORT_SELECTION_SCHEMA_VERSION},
            "role_bucket_ids": {
                "type": "array",
                "items": {"type": "string", "enum": selectable_role_ids},
                "maxItems": len(selectable_role_ids),
            },
            "employment_statuses": {
                "type": "array",
                "items": {"type": "string", "enum": ["current", "former"]},
                "minItems": 1,
                "maxItems": 2,
            },
            "role_match": {"type": "string", "enum": ["any", "all"]},
            "source": {"type": "string", "const": "user_explicit"},
        }
    )


def _budget_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "max_provider_calls": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_COHORT_PROVIDER_LANES,
            },
            "max_provider_items": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_PREVIEW_PROVIDER_ITEMS,
            },
            "max_output_candidates": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_PREVIEW_OUTPUT_CANDIDATES,
            },
            "max_cost_micro_usd": {
                "type": "integer",
                "minimum": 0,
                "maximum": MAX_PREVIEW_COST_MICRO_USD,
            },
            "max_elapsed_seconds": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_PREVIEW_ELAPSED_SECONDS,
            },
        }
    )


def _company_target_request_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "canonical_company_id": _string_schema(maximum=200, pattern=_ID_SCHEMA_PATTERN),
            "canonical_name": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
            "company_registry_revision": _string_schema(
                maximum=128,
                pattern=_VERSION_SCHEMA_PATTERN,
            ),
            "company_registry_digest": _sha256_schema(),
            "provider_company_labels": {
                "type": "array",
                "items": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
                "minItems": 1,
                "maxItems": MAX_PREVIEW_PROVIDER_COMPANY_LABELS,
            },
        }
    )


def _build_request_schema() -> dict[str, Any]:
    return DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER.build(
        input_properties={
            "cohort_selection": _cohort_selection_schema(),
            "source_preferences": {
                "type": "array",
                "items": {"type": "string", "const": ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE},
                "minItems": 1,
                "maxItems": 1,
            },
            "coverage_intent": {"type": "string", "const": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT},
            "thematic_constraints": {
                "type": "array",
                "items": _string_schema(maximum=240, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
                "maxItems": MAX_PREVIEW_THEMATIC_CONSTRAINTS,
            },
            "provider_mode_intent": {"type": "string", "enum": list(ACQUISITION_PLAN_PREVIEW_PROVIDER_MODES)},
            "budget": _budget_schema(),
        },
        input_required=(
            "cohort_selection",
            "source_preferences",
            "coverage_intent",
            "thematic_constraints",
            "provider_mode_intent",
            "budget",
        ),
        target_properties={
            "workspace_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
            "requester_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
            "company_target": _company_target_request_schema(),
        },
        target_required=("workspace_id", "requester_id", "company_target"),
    )


ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC = ToolSpec(
    name="plan_acquisition",
    description="Build a capability-free immutable acquisition plan preview.",
    input_schema=_build_request_schema(),
    schema_version=ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
    approval_policy="not_required",
    budget_required=False,
)
ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST = ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC.input_schema_digest


def acquisition_plan_preview_request_schema() -> dict[str, Any]:
    """Return a mutable copy of the closed request schema for D1 integration."""

    return _thaw_json(ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC.input_schema)


def _company_target_record_schema() -> dict[str, Any]:
    request_properties = dict(_company_target_request_schema()["properties"])
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": CANONICAL_COMPANY_TARGET_SCHEMA_VERSION},
            **request_properties,
            "company_target_digest": _sha256_schema(),
        }
    )


def _effective_request_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": ACQUISITION_PLAN_EFFECTIVE_REQUEST_SCHEMA_VERSION},
            "company_target_digest": _sha256_schema(),
            "cohort_selection": _cohort_selection_schema(),
            "cohort_selection_registry_version": {
                "type": "string",
                "const": COHORT_SELECTION_REGISTRY_VERSION,
            },
            "cohort_selection_registry_digest": _sha256_schema(),
            "cohort_selection_digest": _sha256_schema(),
            "source_preferences": {
                "type": "array",
                "items": {"type": "string", "const": ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE},
                "minItems": 1,
                "maxItems": 1,
            },
            "coverage_intent": {"type": "string", "const": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT},
            "thematic_constraints": {
                "type": "array",
                "items": _string_schema(maximum=240, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
                "maxItems": MAX_PREVIEW_THEMATIC_CONSTRAINTS,
            },
            "provider_mode_intent": {"type": "string", "enum": list(ACQUISITION_PLAN_PREVIEW_PROVIDER_MODES)},
            "budget": _budget_schema(),
        }
    )


def _provider_planning_manifest_schema() -> dict[str, Any]:
    return cohort_provider_planning_manifest_schema()


def _schema_pins_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "plan_request_schema_version": {
                "type": "string",
                "const": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
            },
            "plan_request_schema_digest": _sha256_schema(),
            "plan_result_schema_version": {
                "type": "string",
                "const": ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION,
            },
            "plan_result_schema_digest": _sha256_schema(),
            "intended_start_request_schema_version": _string_schema(
                maximum=128,
                pattern=_VERSION_SCHEMA_PATTERN,
            ),
            "intended_start_request_schema_digest": _sha256_schema(),
        }
    )


def _confirmation_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "required": {"type": "boolean", "const": True},
            "confirmation_type": {
                "type": "string",
                "const": ACQUISITION_PLAN_PREVIEW_CONFIRMATION_TYPE,
            },
            "preview_id": _string_schema(maximum=200, pattern=_ID_SCHEMA_PATTERN),
            "preview_revision": {"type": "integer", "minimum": 1, "maximum": MAX_PREVIEW_REVISION},
            "preview_digest": _sha256_schema(),
            "instruction": {
                "type": "string",
                "const": ACQUISITION_PLAN_PREVIEW_CONFIRMATION_INSTRUCTION,
            },
        }
    )


def _preview_record_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION},
            "preview_id": _string_schema(maximum=200, pattern=_ID_SCHEMA_PATTERN),
            "preview_revision": {"type": "integer", "minimum": 1, "maximum": MAX_PREVIEW_REVISION},
            "workspace_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
            "requester_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN),
            "company_target": _company_target_record_schema(),
            "effective_request": _effective_request_schema(),
            "effective_request_digest": _sha256_schema(),
            "provider_planning_manifest": _provider_planning_manifest_schema(),
            "schema_pins": _schema_pins_schema(),
            "confirmation": _confirmation_schema(),
            "created_at": _string_schema(
                maximum=20,
                minimum=20,
                pattern=_UTC_TIMESTAMP_SCHEMA_PATTERN,
            ),
            "expires_at": _string_schema(
                maximum=20,
                minimum=20,
                pattern=_UTC_TIMESTAMP_SCHEMA_PATTERN,
            ),
            "preview_digest": _sha256_schema(),
        }
    )


_PREVIEW_RECORD_TOOL_SPEC = ToolSpec(
    name="acquisition_plan_preview_record",
    description="Validate one immutable capability-free acquisition plan preview.",
    input_schema=_preview_record_schema(),
    schema_version="acquisition_plan_preview_record_v2",
    approval_policy="validation_only",
    budget_required=False,
)


def _variant_schema(variant: str) -> dict[str, Any]:
    if variant == "success":
        return _closed_object(
            {
                "variant": {"type": "string", "const": "success"},
                "status": {"type": "string", "const": "ready"},
                "preview": _preview_record_schema(),
            }
        )
    if variant == "deferred":
        return _closed_object(
            {
                "variant": {"type": "string", "const": "deferred"},
                "status": {"type": "string", "const": "deferred"},
                "reason": _string_schema(maximum=80, pattern=_TERMINAL_REASON_SCHEMA_PATTERN),
                "retryable": {"type": "boolean"},
            }
        )
    return _closed_object(
        {
            "variant": {"type": "string", "const": "error"},
            "status": {"type": "string", "const": "failed"},
            "reason": _string_schema(maximum=80, pattern=_TERMINAL_REASON_SCHEMA_PATTERN),
            "field": _string_schema(maximum=200, pattern=_ERROR_FIELD_SCHEMA_PATTERN),
            "retryable": {"type": "boolean", "const": False},
        }
    )


def _schema_field_paths(schema: Mapping[str, Any], *, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return paths
        for field_name, child in properties.items():
            if not isinstance(field_name, str) or not isinstance(child, Mapping):
                continue
            token = field_name.replace("~", "~0").replace("/", "~1")
            path = f"{prefix}/{token}"
            paths.add(path)
            paths.update(_schema_field_paths(child, prefix=path))
    elif schema.get("type") == "array":
        items = schema.get("items")
        if isinstance(items, Mapping):
            paths.update(_schema_field_paths(items, prefix=f"{prefix}/*"))
    return paths


def _field_provenance(variant_schemas: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for variant, schema in variant_schemas.items():
        row: dict[str, str] = {}
        for path in sorted(_schema_field_paths(schema)):
            if path == "/variant" or path == "/status":
                classification = "server_derived"
            elif _is_user_supplied_effective_request_path(path):
                classification = "user_supplied"
            elif _is_server_derived_preview_path(path):
                classification = "server_derived"
            elif variant == "success":
                classification = "owner_state"
            else:
                classification = "owner_state"
            row[path] = classification
        result[variant] = row
    return result


def _schema_string_paths(schema: Mapping[str, Any], *, prefix: str = "") -> set[str]:
    if schema.get("type") == "string":
        return {prefix}
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return paths
        for field_name, child in properties.items():
            if not isinstance(field_name, str) or not isinstance(child, Mapping):
                continue
            token = field_name.replace("~", "~0").replace("/", "~1")
            paths.update(_schema_string_paths(child, prefix=f"{prefix}/{token}"))
    elif schema.get("type") == "array":
        items = schema.get("items")
        if isinstance(items, Mapping):
            paths.update(_schema_string_paths(items, prefix=f"{prefix}/*"))
    return paths


_DISPLAY_TEXT_RESULT_PATHS = frozenset(
    {
        "/status",
        "/reason",
        "/preview/company_target/canonical_name",
        "/preview/company_target/provider_company_labels/*",
        "/preview/provider_planning_manifest/company_target/canonical_name",
        "/preview/provider_planning_manifest/company_target/provider_company_labels/*",
        "/preview/provider_planning_manifest/compiler_inputs/base_filter_hints/current_companies/*",
        "/preview/provider_planning_manifest/compiler_inputs/base_filter_hints/past_companies/*",
        "/preview/provider_planning_manifest/compiler_inputs/base_filter_hints/keywords/*",
        "/preview/provider_planning_manifest/lanes/*/provider_payload/filter_hints/current_companies/*",
        "/preview/provider_planning_manifest/lanes/*/provider_payload/filter_hints/past_companies/*",
        "/preview/provider_planning_manifest/lanes/*/provider_payload/filter_hints/keywords/*",
        "/preview/provider_planning_manifest/lanes/*/provider_payload/filter_hints/job_titles/*",
        "/preview/effective_request/thematic_constraints/*",
        "/preview/confirmation/instruction",
    }
)

_USER_SUPPLIED_IDENTIFIER_RESULT_PATHS = (
    "/preview/effective_request/cohort_selection/employment_statuses/*",
    "/preview/effective_request/cohort_selection/role_bucket_ids/*",
    "/preview/effective_request/cohort_selection/role_match",
    "/preview/effective_request/cohort_selection/schema_version",
    "/preview/effective_request/cohort_selection/source",
    "/preview/effective_request/coverage_intent",
    "/preview/effective_request/provider_mode_intent",
    "/preview/effective_request/source_preferences/*",
)

_CONTROL_RESULT_PATHS = frozenset(
    {
        "/preview/provider_planning_manifest/lanes/*/role_bucket_id",
        "/preview/provider_planning_manifest/lanes/*/provider_payload/query_text",
    }
)


def _field_value_roles(
    variant_schemas: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, ActionResultValueRole]]:
    result: dict[str, dict[str, ActionResultValueRole]] = {}
    for variant, schema in variant_schemas.items():
        roles: dict[str, ActionResultValueRole] = {}
        for path in sorted(_schema_string_paths(schema)):
            role: ActionResultValueRole
            if path == "/variant" or path in _CONTROL_RESULT_PATHS:
                role = "control"
            elif path in _DISPLAY_TEXT_RESULT_PATHS:
                role = "display_text"
            else:
                role = "identifier"
            roles[path] = role
        result[variant] = roles
    return result


def _is_user_supplied_effective_request_path(path: str) -> bool:
    prefixes = (
        "/preview/effective_request/cohort_selection",
        "/preview/effective_request/source_preferences",
        "/preview/effective_request/coverage_intent",
        "/preview/effective_request/thematic_constraints",
        "/preview/effective_request/provider_mode_intent",
        "/preview/effective_request/budget",
    )
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)


def _is_server_derived_preview_path(path: str) -> bool:
    exact_paths = {
        "/preview/schema_version",
        "/preview/preview_digest",
        "/preview/effective_request/schema_version",
        "/preview/effective_request/company_target_digest",
        "/preview/effective_request/cohort_selection_registry_version",
        "/preview/effective_request/cohort_selection_registry_digest",
        "/preview/effective_request/cohort_selection_digest",
        "/preview/effective_request_digest",
        "/preview/company_target/schema_version",
        "/preview/company_target/company_target_digest",
    }
    derived_prefixes = (
        "/preview/provider_planning_manifest",
        "/preview/schema_pins",
        "/preview/confirmation",
    )
    return path in exact_paths or any(path == prefix or path.startswith(f"{prefix}/") for prefix in derived_prefixes)


_RESULT_VARIANT_SCHEMAS = {
    "success": _variant_schema("success"),
    "deferred": _variant_schema("deferred"),
    "error": _variant_schema("error"),
}

ACQUISITION_PLAN_PREVIEW_RESULT_SPEC = ActionResultSpec(
    tool_name="plan_acquisition",
    tool_kind="action",
    owner_binding=ActionResultActionOwner(action_type="plan_acquisition"),
    result_schema_version=ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION,
    serializer_owner=ACQUISITION_PLAN_PREVIEW_SERIALIZER_OWNER,
    serializer_revision=ACQUISITION_PLAN_PREVIEW_SERIALIZER_REVISION,
    serializer_contract={
        "schema_version": "acquisition_plan_preview_serializer_contract_v2",
        "owner_output": "closed_terminal_variant",
        "success_source": "immutable_acquisition_plan_preview",
        "deferred_error_source": "typed_owner_result",
    },
    validator_owner=ACTION_RESULT_VALIDATOR_OWNER,
    variant_schemas=_RESULT_VARIANT_SCHEMAS,
    field_provenance=_field_provenance(_RESULT_VARIANT_SCHEMAS),
    field_value_roles=_field_value_roles(_RESULT_VARIANT_SCHEMAS),
    max_serialized_bytes=48 * 1024,
    max_items=4_096,
    max_depth=10,
    artifact_ref_schemes=(),
    externally_controlled_identifier_paths=_USER_SUPPLIED_IDENTIFIER_RESULT_PATHS,
)


@dataclass(frozen=True, slots=True)
class AcquisitionPlanPreview:
    """Deeply immutable, digest-verified preview value."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        try:
            validated = _PREVIEW_RECORD_TOOL_SPEC.validate_input(self._record)
        except ModelToolSchemaError as exc:
            raise AcquisitionPlanPreviewError("acquisition_plan_preview_record_invalid", detail=str(exc)) from exc
        _validate_preview_semantics(validated)
        expected_digest = _preview_digest(validated)
        if validated.get("preview_digest") != expected_digest:
            raise AcquisitionPlanPreviewError(
                "acquisition_plan_preview_digest_mismatch",
                "preview_digest",
            )
        confirmation = dict(validated.get("confirmation") or {})
        if (
            confirmation.get("preview_id") != validated.get("preview_id")
            or confirmation.get("preview_revision") != validated.get("preview_revision")
            or confirmation.get("preview_digest") != validated.get("preview_digest")
        ):
            raise AcquisitionPlanPreviewError(
                "acquisition_plan_preview_confirmation_mismatch",
                "confirmation",
            )
        object.__setattr__(self, "_record", _freeze_json(validated))

    @property
    def preview_id(self) -> str:
        return str(self._record["preview_id"])

    @property
    def preview_revision(self) -> int:
        return int(self._record["preview_revision"])

    @property
    def preview_digest(self) -> str:
        return str(self._record["preview_digest"])

    @property
    def record(self) -> Mapping[str, Any]:
        return self._record

    def to_record(self) -> dict[str, Any]:
        return _thaw_json(self._record)


def build_acquisition_plan_preview(
    *,
    input_payload: Mapping[str, Any],
    target_ref: Mapping[str, Any],
    preview_id: str,
    preview_revision: int,
    created_at: str,
    expires_at: str,
    intended_start_request_schema_version: str,
    intended_start_request_schema_digest: str,
) -> AcquisitionPlanPreview:
    """Build one deterministic preview without issuing any capability or I/O.

    ``target_ref`` must already be minted by the future authenticated owner.
    ``preview_revision`` must already be allocated monotonically by the future
    PG owner.  Requiring those values here keeps this pure leaf from inventing
    a second identity or persistence authority.
    """

    _require_identifier(preview_id, field="preview_id")
    if type(preview_revision) is not int or not 1 <= preview_revision <= MAX_PREVIEW_REVISION:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_revision_invalid", "preview_revision")
    created = _parse_utc_timestamp(created_at, field="created_at")
    expires = _parse_utc_timestamp(expires_at, field="expires_at")
    ttl_seconds = int((expires - created).total_seconds())
    if ttl_seconds <= 0 or ttl_seconds > MAX_PREVIEW_TTL_SECONDS:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_expiry_invalid", "expires_at")
    _require_version(
        intended_start_request_schema_version,
        field="intended_start_request_schema_version",
    )
    _require_sha256(
        intended_start_request_schema_digest,
        field="intended_start_request_schema_digest",
    )

    try:
        validated_request = ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC.validate_input(
            {"input_payload": input_payload, "target_ref": target_ref}
        )
    except ModelToolSchemaError as exc:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_request_invalid",
            detail=str(exc),
        ) from exc
    validated_input = dict(validated_request.get("input_payload") or {})
    validated_target = dict(validated_request.get("target_ref") or {})

    try:
        cohort = normalize_cohort_selection(
            validated_input.get("cohort_selection"),
            allowed_sources=("user_explicit",),
        )
    except CohortSelectionValidationError as exc:
        raise AcquisitionPlanPreviewError(exc.code, exc.field, exc.detail) from exc
    source_preferences = tuple(str(item) for item in list(validated_input.get("source_preferences") or []))
    if source_preferences != (ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE,):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_source_preferences_invalid",
            "input_payload.source_preferences",
        )
    thematic_constraints = _canonical_string_set(
        validated_input.get("thematic_constraints"),
        field="input_payload.thematic_constraints",
    )
    budget = dict(validated_input.get("budget") or {})
    _validate_budget(budget)

    workspace_id = _require_owner_identity(
        validated_target.get("workspace_id"),
        field="target_ref.workspace_id",
    )
    requester_id = _require_owner_identity(
        validated_target.get("requester_id"),
        field="target_ref.requester_id",
    )
    company_target = _canonical_company_target(validated_target.get("company_target"))

    company_target_digest = _sha256_json(company_target)
    company_target = {**company_target, "company_target_digest": company_target_digest}
    selection_digest = cohort_selection_digest(cohort)
    registry_digest = cohort_selection_registry_digest()
    effective_request = {
        "schema_version": ACQUISITION_PLAN_EFFECTIVE_REQUEST_SCHEMA_VERSION,
        "company_target_digest": company_target_digest,
        "cohort_selection": cohort,
        "cohort_selection_registry_version": COHORT_SELECTION_REGISTRY_VERSION,
        "cohort_selection_registry_digest": registry_digest,
        "cohort_selection_digest": selection_digest,
        "source_preferences": list(source_preferences),
        "coverage_intent": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
        "thematic_constraints": thematic_constraints,
        "provider_mode_intent": str(validated_input["provider_mode_intent"]),
        "budget": budget,
    }
    effective_request_digest = _sha256_json(effective_request)

    schema_pins = {
        "plan_request_schema_version": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
        "plan_request_schema_digest": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
        "plan_result_schema_version": ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION,
        "plan_result_schema_digest": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_digest,
        "intended_start_request_schema_version": intended_start_request_schema_version,
        "intended_start_request_schema_digest": intended_start_request_schema_digest,
    }

    compiler_owner = CohortProviderCompiler()
    try:
        provider_manifest = compiler_owner.compile_planning_manifest(
            {"cohort_selection": cohort},
            base_filter_hints={
                "current_companies": list(company_target["provider_company_labels"]),
                "past_companies": list(company_target["provider_company_labels"]),
                "keywords": list(thematic_constraints),
            },
            company_target=company_target,
            budget_ceiling=budget,
            schema_pins=schema_pins,
            requested_result_limit=int(budget["max_provider_items"]),
        )
    except CohortProviderCompilationError as exc:
        if exc.code == "cohort_provider_planning_filter_rejected":
            raise AcquisitionPlanPreviewError(
                "acquisition_plan_preview_thematic_constraint_rejected",
                "input_payload.thematic_constraints",
                "put role intent in cohort_selection.role_bucket_ids",
            ) from exc
        if exc.code == "cohort_provider_call_budget_exceeded":
            raise AcquisitionPlanPreviewError(
                "acquisition_plan_preview_provider_call_budget_exceeded",
                "input_payload.budget.max_provider_calls",
            ) from exc
        raise AcquisitionPlanPreviewError(exc.code, exc.field, exc.detail) from exc
    _require_compiler_preserved_thematic_constraints(
        provider_manifest,
        thematic_constraints,
        field="input_payload.thematic_constraints",
    )

    record_without_digest: dict[str, Any] = {
        "schema_version": ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION,
        "preview_id": preview_id,
        "preview_revision": preview_revision,
        "workspace_id": workspace_id,
        "requester_id": requester_id,
        "company_target": company_target,
        "effective_request": effective_request,
        "effective_request_digest": effective_request_digest,
        "provider_planning_manifest": provider_manifest,
        "schema_pins": schema_pins,
        "confirmation": {
            "required": True,
            "confirmation_type": ACQUISITION_PLAN_PREVIEW_CONFIRMATION_TYPE,
            "preview_id": preview_id,
            "preview_revision": preview_revision,
            "preview_digest": "0" * 64,
            "instruction": ACQUISITION_PLAN_PREVIEW_CONFIRMATION_INSTRUCTION,
        },
        "created_at": created_at,
        "expires_at": expires_at,
    }
    digest = _sha256_json(record_without_digest)
    record_without_digest["confirmation"]["preview_digest"] = digest
    # Confirmation is part of the immutable preview, but its digest copy cannot
    # participate in the digest it mirrors.  Recompute under the same explicit
    # placeholder rule used by ``_preview_digest``.
    record = {**record_without_digest, "preview_digest": digest}
    preview = AcquisitionPlanPreview(record)
    try:
        ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serialize(
            {"variant": "success", "status": "ready", "preview": preview.to_record()}
        )
    except ActionResultSchemaError as exc:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_result_not_model_safe",
            detail=str(exc),
        ) from exc
    return preview


def acquisition_plan_preview_success_result(preview: AcquisitionPlanPreview) -> dict[str, Any]:
    if not isinstance(preview, AcquisitionPlanPreview):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_value_invalid", "preview")
    return {"variant": "success", "status": "ready", "preview": preview.to_record()}


def acquisition_plan_preview_deferred_result(*, reason: str, retryable: bool) -> dict[str, Any]:
    _require_terminal_reason(reason, field="reason")
    return {
        "variant": "deferred",
        "status": "deferred",
        "reason": reason,
        "retryable": retryable,
    }


def acquisition_plan_preview_error_result(
    *,
    reason: str,
    field: str = ACQUISITION_PLAN_PREVIEW_NO_FIELD,
) -> dict[str, Any]:
    _require_terminal_reason(reason, field="reason")
    _require_error_field(field)
    return {
        "variant": "error",
        "status": "failed",
        "reason": reason,
        "field": field,
        "retryable": False,
    }


def serialize_acquisition_plan_preview_result(owner_output: dict[str, Any]) -> str:
    candidate = dict(owner_output or {})
    if candidate.get("variant") == "success":
        raw_preview = candidate.get("preview")
        if not isinstance(raw_preview, Mapping):
            raise AcquisitionPlanPreviewError("acquisition_plan_preview_value_invalid", "preview")
        candidate["preview"] = AcquisitionPlanPreview(raw_preview).to_record()
    elif candidate.get("variant") in {"deferred", "error"}:
        _require_terminal_reason(candidate.get("reason"), field="reason")
        if candidate.get("variant") == "error":
            _require_error_field(candidate.get("field"))
    return ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serialize(candidate)


def _canonical_company_target(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_company_target_invalid",
            "target_ref.company_target",
        )
    canonical_company_id = str(value.get("canonical_company_id") or "")
    _require_identifier(canonical_company_id, field="target_ref.company_target.canonical_company_id")
    raw_canonical_name = value.get("canonical_name")
    canonical_name = _canonical_nonempty_text(
        raw_canonical_name,
        field="target_ref.company_target.canonical_name",
    )
    if raw_canonical_name != canonical_name:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_company_target_invalid",
            "target_ref.company_target.canonical_name",
        )
    revision = str(value.get("company_registry_revision") or "")
    _require_version(revision, field="target_ref.company_target.company_registry_revision")
    registry_digest = str(value.get("company_registry_digest") or "")
    _require_sha256(registry_digest, field="target_ref.company_target.company_registry_digest")
    raw_labels = value.get("provider_company_labels")
    labels = _canonical_string_set(
        raw_labels,
        field="target_ref.company_target.provider_company_labels",
    )
    if not isinstance(raw_labels, list) or any(
        type(label) is not str or " ".join(label.split()).strip() != label for label in raw_labels
    ):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_provider_company_labels_invalid",
            "target_ref.company_target.provider_company_labels",
        )
    if not labels or len(labels) > MAX_PREVIEW_PROVIDER_COMPANY_LABELS:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_provider_company_labels_invalid",
            "target_ref.company_target.provider_company_labels",
        )
    if canonical_name.casefold() not in {label.casefold() for label in labels}:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_canonical_company_label_missing",
            "target_ref.company_target.provider_company_labels",
        )
    return {
        "schema_version": CANONICAL_COMPANY_TARGET_SCHEMA_VERSION,
        "canonical_company_id": canonical_company_id,
        "canonical_name": canonical_name,
        "company_registry_revision": revision,
        "company_registry_digest": registry_digest,
        "provider_company_labels": labels,
    }


def _validate_preview_semantics(record: Mapping[str, Any]) -> None:
    _require_identifier(record.get("preview_id"), field="preview_id")
    _require_sha256(record.get("preview_digest"), field="preview_digest")
    _require_sha256(record.get("effective_request_digest"), field="effective_request_digest")
    _require_owner_identity(record.get("workspace_id"), field="workspace_id")
    _require_owner_identity(record.get("requester_id"), field="requester_id")

    company = dict(record.get("company_target") or {})
    _require_sha256(company.get("company_target_digest"), field="company_target.company_target_digest")
    company_input = {
        key: value for key, value in company.items() if key not in {"schema_version", "company_target_digest"}
    }
    canonical_company = _canonical_company_target(company_input)
    expected_company = {
        **canonical_company,
        "company_target_digest": _sha256_json(canonical_company),
    }
    if company != expected_company:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_company_target_mismatch",
            "company_target",
        )

    effective = dict(record.get("effective_request") or {})
    for digest_field in (
        "company_target_digest",
        "cohort_selection_registry_digest",
        "cohort_selection_digest",
    ):
        _require_sha256(
            effective.get(digest_field),
            field=f"effective_request.{digest_field}",
        )
    try:
        cohort = normalize_cohort_selection(
            effective.get("cohort_selection"),
            allowed_sources=("user_explicit",),
        )
    except CohortSelectionValidationError as exc:
        raise AcquisitionPlanPreviewError(exc.code, exc.field, exc.detail) from exc
    budget = dict(effective.get("budget") or {})
    _validate_budget(budget)
    expected_effective = {
        "schema_version": ACQUISITION_PLAN_EFFECTIVE_REQUEST_SCHEMA_VERSION,
        "company_target_digest": expected_company["company_target_digest"],
        "cohort_selection": cohort,
        "cohort_selection_registry_version": COHORT_SELECTION_REGISTRY_VERSION,
        "cohort_selection_registry_digest": cohort_selection_registry_digest(),
        "cohort_selection_digest": cohort_selection_digest(cohort),
        "source_preferences": [ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE],
        "coverage_intent": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
        "thematic_constraints": _canonical_string_set(
            effective.get("thematic_constraints"),
            field="effective_request.thematic_constraints",
        ),
        "provider_mode_intent": str(effective.get("provider_mode_intent") or ""),
        "budget": budget,
    }
    if effective != expected_effective:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_effective_request_mismatch",
            "effective_request",
        )
    if record.get("effective_request_digest") != _sha256_json(expected_effective):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_effective_request_digest_mismatch",
            "effective_request_digest",
        )

    schema_pins = dict(record.get("schema_pins") or {})
    for field in (
        "plan_request_schema_version",
        "plan_result_schema_version",
        "intended_start_request_schema_version",
    ):
        _require_version(schema_pins.get(field), field=f"schema_pins.{field}")
    for field in (
        "plan_request_schema_digest",
        "plan_result_schema_digest",
        "intended_start_request_schema_digest",
    ):
        _require_sha256(schema_pins.get(field), field=f"schema_pins.{field}")
    if (
        schema_pins.get("plan_request_schema_version") != ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION
        or schema_pins.get("plan_request_schema_digest") != ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST
        or schema_pins.get("plan_result_schema_version") != ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION
        or schema_pins.get("plan_result_schema_digest") != ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_digest
    ):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_schema_pin_mismatch",
            "schema_pins",
        )

    compiler = CohortProviderCompiler()
    try:
        expected_manifest = compiler.compile_planning_manifest(
            {"cohort_selection": cohort},
            base_filter_hints={
                "current_companies": list(expected_company["provider_company_labels"]),
                "past_companies": list(expected_company["provider_company_labels"]),
                "keywords": list(expected_effective["thematic_constraints"]),
            },
            company_target=expected_company,
            budget_ceiling=budget,
            schema_pins=schema_pins,
            requested_result_limit=int(budget["max_provider_items"]),
        )
    except CohortProviderCompilationError as exc:
        if exc.code == "cohort_provider_planning_filter_rejected":
            raise AcquisitionPlanPreviewError(
                "acquisition_plan_preview_thematic_constraint_rejected",
                "effective_request.thematic_constraints",
                "put role intent in cohort_selection.role_bucket_ids",
            ) from exc
        raise AcquisitionPlanPreviewError(exc.code, exc.field, exc.detail) from exc
    _require_compiler_preserved_thematic_constraints(
        expected_manifest,
        list(expected_effective["thematic_constraints"]),
        field="effective_request.thematic_constraints",
    )
    stored_manifest = dict(record.get("provider_planning_manifest") or {})
    _require_sha256(
        stored_manifest.get("manifest_digest"),
        field="provider_planning_manifest.manifest_digest",
    )
    _require_sha256(
        stored_manifest.get("physical_query_digest"),
        field="provider_planning_manifest.physical_query_digest",
    )
    try:
        compiler.validate_planning_manifest(stored_manifest)
    except CohortProviderCompilationError as exc:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_provider_manifest_mismatch",
            "provider_planning_manifest",
            exc.code,
        ) from exc
    if stored_manifest != expected_manifest:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_provider_manifest_mismatch",
            "provider_planning_manifest",
        )
    if int(dict(expected_manifest["budget"])["planned_provider_calls"]) > int(budget["max_provider_calls"]):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_provider_call_budget_exceeded",
            "effective_request.budget.max_provider_calls",
        )
    confirmation = dict(record.get("confirmation") or {})
    _require_identifier(confirmation.get("preview_id"), field="confirmation.preview_id")
    _require_sha256(confirmation.get("preview_digest"), field="confirmation.preview_digest")
    created = _parse_utc_timestamp(record.get("created_at"), field="created_at")
    expires = _parse_utc_timestamp(record.get("expires_at"), field="expires_at")
    ttl_seconds = int((expires - created).total_seconds())
    if ttl_seconds <= 0 or ttl_seconds > MAX_PREVIEW_TTL_SECONDS:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_expiry_invalid", "expires_at")


def _validate_budget(budget: Mapping[str, Any]) -> None:
    if set(budget) != set(_BUDGET_FIELDS) or any(type(budget.get(field)) is not int for field in _BUDGET_FIELDS):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_budget_invalid", "input_payload.budget")
    max_calls = int(budget["max_provider_calls"])
    max_items = int(budget["max_provider_items"])
    max_output = int(budget["max_output_candidates"])
    max_cost = int(budget["max_cost_micro_usd"])
    max_elapsed = int(budget["max_elapsed_seconds"])
    if (
        not 1 <= max_calls <= MAX_COHORT_PROVIDER_LANES
        or not 1 <= max_items <= MAX_PREVIEW_PROVIDER_ITEMS
        or not 1 <= max_output <= MAX_PREVIEW_OUTPUT_CANDIDATES
        or not 0 <= max_cost <= MAX_PREVIEW_COST_MICRO_USD
        or not 1 <= max_elapsed <= MAX_PREVIEW_ELAPSED_SECONDS
    ):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_budget_invalid", "input_payload.budget")
    if max_output > max_items:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_output_budget_exceeded",
            "input_payload.budget.max_output_candidates",
        )


def _require_compiler_preserved_thematic_constraints(
    manifest: Mapping[str, Any],
    expected_constraints: list[str],
    *,
    field: str,
) -> None:
    compiler_inputs = dict(manifest.get("compiler_inputs") or {})
    base_filter_hints = dict(compiler_inputs.get("base_filter_hints") or {})
    compiled_constraints = list(base_filter_hints.get("keywords") or [])
    if compiled_constraints != expected_constraints:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_thematic_constraint_rejected",
            field,
            "put role intent in cohort_selection.role_bucket_ids",
        )


def _canonical_string_set(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, list) or any(type(item) is not str for item in value):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_string_set_invalid", field)
    normalized = [" ".join(item.split()).strip() for item in value]
    if any(not item for item in normalized):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_string_set_invalid", field)
    folded = [item.casefold() for item in normalized]
    if len(folded) != len(set(folded)):
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_string_set_duplicate", field)
    return sorted(normalized, key=lambda item: (item.casefold(), item))


def _canonical_nonempty_text(value: Any, *, field: str) -> str:
    if type(value) is not str:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_text_invalid", field)
    normalized = " ".join(value.split()).strip()
    if not normalized:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_text_invalid", field)
    return normalized


def _require_owner_identity(value: Any, *, field: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 200
        or any(
            character.isspace()
            or ord(character) <= 0x1F
            or 0x7F <= ord(character) <= 0x9F
            or 0xD800 <= ord(character) <= 0xDFFF
            for character in value
        )
    ):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_owner_identity_noncanonical",
            field,
        )
    return value


def _require_identifier(value: Any, *, field: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_identifier_invalid", field)
    return value


def _require_version(value: Any, *, field: str) -> str:
    if type(value) is not str or _VERSION_PATTERN.fullmatch(value) is None:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_version_invalid", field)
    return value


def _require_sha256(value: Any, *, field: str) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_digest_invalid", field)
    return value


def _require_terminal_reason(value: Any, *, field: str) -> str:
    if type(value) is not str or _TERMINAL_REASON_PATTERN.fullmatch(value) is None:
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_terminal_reason_invalid",
            field,
        )
    return value


def _require_error_field(value: Any) -> str:
    if not _is_canonical_error_field(value):
        raise AcquisitionPlanPreviewError(
            "acquisition_plan_preview_error_field_invalid",
            "field",
        )
    assert isinstance(value, str)
    return value


def _is_canonical_error_field(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) <= _MAX_ERROR_FIELD_LENGTH
        and _ERROR_FIELD_PATTERN.fullmatch(value) is not None
    )


def _parse_utc_timestamp(value: Any, *, field: str) -> datetime:
    if type(value) is not str or _UTC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_timestamp_invalid", field)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_timestamp_invalid", field) from exc
    return parsed


def _preview_digest(record: Mapping[str, Any]) -> str:
    candidate = _thaw_json(record)
    candidate.pop("preview_digest", None)
    confirmation = dict(candidate.get("confirmation") or {})
    confirmation["preview_digest"] = "0" * 64
    candidate["confirmation"] = confirmation
    return _sha256_json(candidate)


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
        raise AcquisitionPlanPreviewError("acquisition_plan_preview_not_canonical_json") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise AcquisitionPlanPreviewError("acquisition_plan_preview_object_key_invalid")
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(child) for child in value]
    return value


__all__ = [
    "ACQUISITION_PLAN_EFFECTIVE_REQUEST_SCHEMA_VERSION",
    "ACQUISITION_PLAN_PREVIEW_CONFIRMATION_INSTRUCTION",
    "ACQUISITION_PLAN_PREVIEW_CONFIRMATION_TYPE",
    "ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT",
    "ACQUISITION_PLAN_PREVIEW_PROVIDER_MODES",
    "ACQUISITION_PLAN_PREVIEW_NO_FIELD",
    "ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST",
    "ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION",
    "ACQUISITION_PLAN_PREVIEW_REQUEST_TOOL_SPEC",
    "ACQUISITION_PLAN_PREVIEW_RESULT_SCHEMA_VERSION",
    "ACQUISITION_PLAN_PREVIEW_RESULT_SPEC",
    "ACQUISITION_PLAN_PREVIEW_SCHEMA_VERSION",
    "ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE",
    "CANONICAL_COMPANY_TARGET_SCHEMA_VERSION",
    "AcquisitionPlanPreview",
    "AcquisitionPlanPreviewError",
    "acquisition_plan_preview_deferred_result",
    "acquisition_plan_preview_error_result",
    "acquisition_plan_preview_request_schema",
    "acquisition_plan_preview_success_result",
    "build_acquisition_plan_preview",
    "serialize_acquisition_plan_preview_result",
]
