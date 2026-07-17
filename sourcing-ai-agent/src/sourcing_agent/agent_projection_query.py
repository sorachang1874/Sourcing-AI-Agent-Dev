"""Pure D1n V3 projection and Operation-query contracts.

This leaf owns closed request/result schemas, deterministic Cohort predicate
compilation, model-safe result serialization, and exact owner-snapshot
preflights.  It deliberately performs no storage, provider, model, network,
release-state, registry-population, or serving work.

``search_projection`` v1 remains owned by ``operation_runtime`` and is not
imported or reinterpreted here.  The structured Cohort surface is the distinct
``filter_projection`` v2 contract below.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .action_request_schema import DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER
from .action_result_schema import (
    ACTION_RESULT_VALIDATOR_OWNER,
    ActionResultActionOwner,
    ActionResultQueryOwner,
    ActionResultSchemaError,
    ActionResultSpec,
    ActionResultValueRole,
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

FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION = "projection_filter_request_v2"
FILTER_PROJECTION_V2_RESULT_SCHEMA_VERSION = "filter_projection_result_v2"
FILTER_PROJECTION_V2_PREDICATE_SCHEMA_VERSION = "projection_cohort_predicate_v2"
FILTER_PROJECTION_CANDIDATE_REF_SCHEMA_VERSION = "projection_candidate_ref.v1"
FILTER_PROJECTION_V2_SERIALIZER_OWNER = "projection_search_service.filter_projection_result_serializer_v2"
FILTER_PROJECTION_V2_SERIALIZER_REVISION = "filter_projection_result_serializer_v2"

INSPECT_OPERATION_REQUEST_SCHEMA_VERSION = "inspect_operation_request_v1"
INSPECT_OPERATION_RESULT_SCHEMA_VERSION = "inspect_operation_result_v1"
INSPECT_OPERATION_QUERY_OWNER_ID = "operation_query_service.inspect_operation"
INSPECT_OPERATION_QUERY_OWNER_REVISION = "inspect_operation_v1"
INSPECT_OPERATION_SERIALIZER_OWNER = "operation_query_service.inspect_operation_result_serializer_v1"
INSPECT_OPERATION_SERIALIZER_REVISION = "inspect_operation_result_serializer_v1"

PROJECTION_SHARED_ACCESS_SCOPE = "shared_canonical_read"
PROJECTION_MEMBERSHIP_OWNER = "projection_search_service.cohort_lane_membership"
PROJECTION_PUBLICATION_IDENTITY_OWNER = "projection_search_service.publication_identity"
OPERATION_QUERY_OWNER_PREFLIGHT = "operation_query_service.exact_workspace_action_run_preflight"
OPERATION_PROGRESS_OWNER = "operation_runs.progress"
OPERATION_RESULT_READINESS_OWNER = "operation_query_service.result_readiness_projection"
OPERATION_PROVENANCE_OWNER = "operation_runs.agent_actions.workflow_commands.operation_events"

FILTER_PROJECTION_DEFAULT_LIMIT = 50
FILTER_PROJECTION_MAX_LIMIT = 250
FILTER_PROJECTION_MAX_OFFSET = 100_000
FILTER_PROJECTION_MAX_CANDIDATES = 1_000
FILTER_PROJECTION_MAX_LANE_SUMMARIES = 64
FILTER_PROJECTION_RESULT_MAX_BYTES = 64 * 1024
INSPECT_OPERATION_RESULT_MAX_BYTES = 24 * 1024

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_REASON_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,95}")
_IDENTIFIER_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$"
_VERSION_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_SHA256_SCHEMA_PATTERN = r"^[0-9a-f]{64}$"
_REASON_SCHEMA_PATTERN = r"^[a-z0-9][a-z0-9_]{0,95}$"
_NONEMPTY_TEXT_SCHEMA_PATTERN = r"^(?s:.*\S.*)$"

_ROLE_IDS = tuple(
    sorted(
        ROLE_BUCKET_KNOWLEDGE,
        key=lambda role_id: (int(ROLE_BUCKET_KNOWLEDGE[role_id]["selectable_order"]), role_id),
    )
)
_EMPLOYMENT_STATUSES = ("current", "former")
_OPERATION_STATUSES = ("queued", "planned", "running", "completed", "failed", "cancelled")
_ACTION_STATUSES = (
    "planned",
    "approval_required",
    "queued",
    "running",
    "completed",
    "failed",
    "cancelled",
    "rejected",
)
_CONTROL_ACTIONS = ("dispatch", "resume", "retry", "cancel")


class AgentProjectionQueryError(ValueError):
    """Stable fail-closed validation error for the pure V3 leaf."""

    def __init__(self, code: str, field: str = "", detail: str = "") -> None:
        self.code = str(code or "agent_projection_query_failed")
        self.field = str(field or "")
        self.detail = str(detail or "")
        super().__init__(self.code)

    def __str__(self) -> str:
        return f"{self.code}: {self.field}" if self.field else self.code


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentProjectionQueryError("agent_projection_query_not_canonical_json") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


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


def _identifier_schema(*, maximum: int = 200) -> dict[str, Any]:
    return _string_schema(maximum=maximum, pattern=_IDENTIFIER_SCHEMA_PATTERN)


def _version_schema() -> dict[str, Any]:
    return _string_schema(maximum=128, pattern=_VERSION_SCHEMA_PATTERN)


def _sha256_schema() -> dict[str, Any]:
    return _string_schema(maximum=64, minimum=64, pattern=_SHA256_SCHEMA_PATTERN)


def _reason_schema() -> dict[str, Any]:
    return _string_schema(maximum=96, pattern=_REASON_SCHEMA_PATTERN)


def _display_text_schema(*, maximum: int) -> dict[str, Any]:
    return _string_schema(maximum=maximum, pattern=_NONEMPTY_TEXT_SCHEMA_PATTERN)


def _cohort_selection_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": COHORT_SELECTION_SCHEMA_VERSION},
            "role_bucket_ids": {
                "type": "array",
                "items": {"type": "string", "enum": list(_ROLE_IDS)},
                "maxItems": len(_ROLE_IDS),
            },
            "employment_statuses": {
                "type": "array",
                "items": {"type": "string", "enum": list(_EMPLOYMENT_STATUSES)},
                "minItems": 1,
                "maxItems": len(_EMPLOYMENT_STATUSES),
            },
            "role_match": {"type": "string", "enum": ["any", "all"]},
            "source": {"type": "string", "const": "user_explicit"},
        }
    )


def _build_filter_projection_v2_request_schema() -> dict[str, Any]:
    return DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER.build(
        input_properties={
            "cohort_selection": _cohort_selection_schema(),
            "offset": {"type": "integer", "minimum": 0, "maximum": FILTER_PROJECTION_MAX_OFFSET},
            "limit": {"type": "integer", "minimum": 1, "maximum": FILTER_PROJECTION_MAX_LIMIT},
        },
        input_required=("cohort_selection",),
        target_properties={
            "projection_id": _identifier_schema(),
            "membership_revision": _identifier_schema(),
            "cohort_selection_registry_version": {
                "type": "string",
                "const": COHORT_SELECTION_REGISTRY_VERSION,
            },
            "cohort_selection_registry_digest": _sha256_schema(),
            "cohort_selection_digest": _sha256_schema(),
        },
        target_required=(
            "projection_id",
            "membership_revision",
            "cohort_selection_registry_version",
            "cohort_selection_registry_digest",
            "cohort_selection_digest",
        ),
    )


FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC = ToolSpec(
    name="filter_projection",
    description="Filter one exact projection revision with a canonical Cohort selection.",
    input_schema=_build_filter_projection_v2_request_schema(),
    schema_version=FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION,
    approval_policy="not_required",
    budget_required=False,
)
FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST = FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.input_schema_digest


def filter_projection_v2_request_schema() -> dict[str, Any]:
    """Return a mutable copy of the closed v2 action request schema."""

    return _thaw_json(FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.input_schema)


def _build_inspect_operation_request_schema() -> dict[str, Any]:
    return _closed_object({"operation_run_id": _identifier_schema()})


INSPECT_OPERATION_REQUEST_TOOL_SPEC = ToolSpec(
    name="inspect_operation",
    description="Inspect one exact owner-bound Operation run without mutating it.",
    input_schema=_build_inspect_operation_request_schema(),
    schema_version=INSPECT_OPERATION_REQUEST_SCHEMA_VERSION,
    approval_policy="not_required",
    budget_required=False,
)
INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST = INSPECT_OPERATION_REQUEST_TOOL_SPEC.input_schema_digest


def inspect_operation_request_schema() -> dict[str, Any]:
    """Return a mutable copy of the closed model-visible query schema."""

    return _thaw_json(INSPECT_OPERATION_REQUEST_TOOL_SPEC.input_schema)


@dataclass(frozen=True, slots=True)
class ProjectionCohortPredicate:
    """Owner-compiled predicate over server-owned lane membership."""

    role_bucket_ids: tuple[str, ...]
    employment_statuses: tuple[str, ...]
    role_match: str
    selection_digest: str

    @property
    def role_mode(self) -> str:
        return "all_roles" if not self.role_bucket_ids else self.role_match

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": FILTER_PROJECTION_V2_PREDICATE_SCHEMA_VERSION,
            "membership_owner": PROJECTION_MEMBERSHIP_OWNER,
            "role_mode": self.role_mode,
            "role_bucket_ids": list(self.role_bucket_ids),
            "employment_statuses": list(self.employment_statuses),
            "selection_digest": self.selection_digest,
        }

    def matches(self, memberships: Any) -> bool:
        canonical = _canonical_lane_memberships(memberships)
        requested_roles = set(self.role_bucket_ids)
        for employment_status in self.employment_statuses:
            status_memberships = [item for item in canonical if item["employment_status"] == employment_status]
            if not status_memberships:
                continue
            if not requested_roles:
                return True
            status_roles = {item["role_bucket_id"] for item in status_memberships if item["role_bucket_id"]}
            if self.role_match == "any" and requested_roles.intersection(status_roles):
                return True
            if self.role_match == "all" and requested_roles.issubset(status_roles):
                return True
        return False


@dataclass(frozen=True, slots=True)
class FilterProjectionV2BoundRequest:
    """Canonical request plus owner-minted publication identity pins."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        try:
            validated = FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC.validate_input(_thaw_json(self._record))
        except (ModelToolSchemaError, TypeError, ValueError) as exc:
            raise AgentProjectionQueryError("filter_projection_request_invalid", detail=str(exc)) from exc
        validated_input = dict(validated["input_payload"])
        validated_target = dict(validated["target_ref"])
        try:
            cohort = normalize_cohort_selection(
                validated_input["cohort_selection"],
                allowed_sources=("user_explicit",),
            )
        except CohortSelectionValidationError as exc:
            raise AgentProjectionQueryError(exc.code, exc.field, exc.detail) from exc
        if (
            validated_target["cohort_selection_registry_version"] != COHORT_SELECTION_REGISTRY_VERSION
            or validated_target["cohort_selection_registry_digest"] != cohort_selection_registry_digest()
            or validated_target["cohort_selection_digest"] != cohort_selection_digest(cohort)
        ):
            raise AgentProjectionQueryError("filter_projection_owner_target_identity_mismatch")
        normalized = {
            "input_payload": {
                "cohort_selection": cohort,
                "offset": int(validated_input.get("offset", 0)),
                "limit": int(validated_input.get("limit", FILTER_PROJECTION_DEFAULT_LIMIT)),
            },
            "target_ref": validated_target,
        }
        object.__setattr__(self, "_record", _freeze_json(normalized))

    @property
    def input_payload(self) -> Mapping[str, Any]:
        return self._record["input_payload"]

    @property
    def target_ref(self) -> Mapping[str, Any]:
        return self._record["target_ref"]

    @property
    def predicate(self) -> ProjectionCohortPredicate:
        cohort = _thaw_json(self.input_payload["cohort_selection"])
        return ProjectionCohortPredicate(
            role_bucket_ids=tuple(cohort["role_bucket_ids"]),
            employment_statuses=tuple(cohort["employment_statuses"]),
            role_match=str(cohort["role_match"]),
            selection_digest=cohort_selection_digest(cohort),
        )

    def to_record(self) -> dict[str, Any]:
        return _thaw_json(self._record)


def bind_filter_projection_v2_request(
    *,
    input_payload: Mapping[str, Any],
    owner_target_ref: Mapping[str, Any],
) -> FilterProjectionV2BoundRequest:
    """Validate the user selection against exact server-derived target pins.

    ``owner_target_ref`` is intentionally a separate argument: the later
    integration binder must mint it after resolving the projection.  Presence
    in this pure leaf is not authorization.
    """

    return FilterProjectionV2BoundRequest(_record={"input_payload": input_payload, "target_ref": owner_target_ref})


def _lane_membership_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "lane_id": _identifier_schema(),
            "employment_status": {"type": "string", "enum": list(_EMPLOYMENT_STATUSES)},
            "role_bucket_id": {
                "type": "string",
                "enum": ["", *_ROLE_IDS],
            },
        }
    )


def _filter_owner_candidate_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "candidate_identity_key": _string_schema(maximum=4_096, pattern=r"^\S+$"),
            "display_name": _display_text_schema(maximum=500),
            "headline": _display_text_schema(maximum=1_000),
            "public_profile_url": _string_schema(
                maximum=2_048,
                pattern=r"^https://[^\s]+$",
            ),
            "cohort_lane_membership": {
                "type": "array",
                "items": _lane_membership_schema(),
                "minItems": 1,
                "maxItems": 32,
            },
        },
        required=(
            "candidate_identity_key",
            "display_name",
            "headline",
            "cohort_lane_membership",
        ),
    )


def _lane_summary_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "lane_id": _identifier_schema(),
            "employment_status": {"type": "string", "enum": list(_EMPLOYMENT_STATUSES)},
            "role_bucket_id": {"type": "string", "enum": ["all_roles", *_ROLE_IDS]},
            "coverage_status": {"type": "string", "enum": ["complete", "partial", "missing"]},
            "result_count": {"type": "integer", "minimum": 0, "maximum": 1_000_000},
        }
    )


def _requested_lane_coverage_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "status": {"type": "string", "enum": ["complete", "partial", "unavailable"]},
            "requested_lane_count": {"type": "integer", "minimum": 1, "maximum": 64},
            "completed_lane_count": {"type": "integer", "minimum": 0, "maximum": 64},
            "missing_lane_count": {"type": "integer", "minimum": 0, "maximum": 64},
        }
    )


def _filter_owner_snapshot_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "access_scope": {"type": "string", "const": PROJECTION_SHARED_ACCESS_SCOPE},
            "projection_id": _identifier_schema(),
            "membership_revision": _identifier_schema(),
            "cohort_selection_registry_version": {
                "type": "string",
                "const": COHORT_SELECTION_REGISTRY_VERSION,
            },
            "cohort_selection_registry_digest": _sha256_schema(),
            "cohort_selection_digest": _sha256_schema(),
            "selection_digest": _sha256_schema(),
            "planning_digest": _sha256_schema(),
            "execution_digest": _sha256_schema(),
            "result_digest": _sha256_schema(),
            "publication_digest": _sha256_schema(),
            "freshness": _closed_object(
                {
                    "status": {"type": "string", "enum": ["fresh", "stale"]},
                    "source_of_truth": {
                        "type": "string",
                        "const": PROJECTION_PUBLICATION_IDENTITY_OWNER,
                    },
                }
            ),
            "readiness": _closed_object(
                {
                    "status": {"type": "string", "enum": ["ready", "not_ready"]},
                    "reason": _reason_schema(),
                    "source_of_truth": {
                        "type": "string",
                        "const": "projection_search_service.readiness",
                    },
                },
                required=("status", "source_of_truth"),
            ),
            "provider_mode": {"type": "string", "enum": ["simulate", "scripted", "live"]},
            "runtime_namespace": _identifier_schema(),
            "cache_provenance": _closed_object(
                {
                    "cache_scope": {
                        "type": "string",
                        "enum": ["no_cache", "isolated_non_live", "live_provider"],
                    },
                    "source_of_truth": {
                        "type": "string",
                        "const": "projection_search_service.cache_provenance",
                    },
                }
            ),
            "requested_lane_coverage": _requested_lane_coverage_schema(),
            "lane_summaries": {
                "type": "array",
                "items": _lane_summary_schema(),
                "maxItems": FILTER_PROJECTION_MAX_LANE_SUMMARIES,
            },
            "candidates": {
                "type": "array",
                "items": _filter_owner_candidate_schema(),
                "maxItems": FILTER_PROJECTION_MAX_CANDIDATES,
            },
        }
    )


_FILTER_OWNER_SNAPSHOT_TOOL_SPEC = ToolSpec(
    name="filter_projection_owner_snapshot",
    description="Validate one canonical projection publication snapshot for the v2 filter adapter.",
    input_schema=_filter_owner_snapshot_schema(),
    schema_version="filter_projection_owner_snapshot_v2",
    approval_policy="validation_only",
    budget_required=False,
)


def _public_candidate_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "candidate_ref": _sha256_schema(),
            "display_name": _display_text_schema(maximum=500),
            "headline": _display_text_schema(maximum=1_000),
            "public_profile_url": _string_schema(maximum=2_048, pattern=r"^https://[^\s]+$"),
            "employment_statuses": {
                "type": "array",
                "items": {"type": "string", "enum": list(_EMPLOYMENT_STATUSES)},
                "minItems": 1,
                "maxItems": 2,
            },
            "role_bucket_ids": {
                "type": "array",
                "items": {"type": "string", "enum": list(_ROLE_IDS)},
                "maxItems": len(_ROLE_IDS),
            },
        },
        required=(
            "candidate_ref",
            "display_name",
            "headline",
            "employment_statuses",
            "role_bucket_ids",
        ),
    )


def _filter_success_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "variant": {"type": "string", "const": "success"},
            "status": {"type": "string", "const": "ready"},
            "projection_id": _identifier_schema(),
            "membership_revision": _identifier_schema(),
            "cohort_selection": _cohort_selection_schema(),
            "cohort_selection_registry_version": {
                "type": "string",
                "const": COHORT_SELECTION_REGISTRY_VERSION,
            },
            "cohort_selection_registry_digest": _sha256_schema(),
            "cohort_selection_digest": _sha256_schema(),
            "selection_digest": _sha256_schema(),
            "planning_digest": _sha256_schema(),
            "execution_digest": _sha256_schema(),
            "result_digest": _sha256_schema(),
            "publication_digest": _sha256_schema(),
            "freshness": _closed_object(
                {
                    "status": {"type": "string", "const": "fresh"},
                    "source_of_truth": {
                        "type": "string",
                        "const": PROJECTION_PUBLICATION_IDENTITY_OWNER,
                    },
                }
            ),
            "readiness": _closed_object(
                {
                    "status": {"type": "string", "const": "ready"},
                    "source_of_truth": {
                        "type": "string",
                        "const": "projection_search_service.readiness",
                    },
                }
            ),
            "provider_mode": {"type": "string", "enum": ["simulate", "scripted", "live"]},
            "runtime_namespace": _identifier_schema(),
            "cache_provenance": _closed_object(
                {
                    "cache_scope": {
                        "type": "string",
                        "enum": ["no_cache", "isolated_non_live", "live_provider"],
                    },
                    "source_of_truth": {
                        "type": "string",
                        "const": "projection_search_service.cache_provenance",
                    },
                }
            ),
            "requested_lane_coverage": _requested_lane_coverage_schema(),
            "lane_summaries": {
                "type": "array",
                "items": _lane_summary_schema(),
                "maxItems": FILTER_PROJECTION_MAX_LANE_SUMMARIES,
            },
            "offset": {"type": "integer", "minimum": 0, "maximum": FILTER_PROJECTION_MAX_OFFSET},
            "limit": {"type": "integer", "minimum": 1, "maximum": FILTER_PROJECTION_MAX_LIMIT},
            "total_count": {"type": "integer", "minimum": 0, "maximum": FILTER_PROJECTION_MAX_CANDIDATES},
            "returned_count": {"type": "integer", "minimum": 0, "maximum": FILTER_PROJECTION_MAX_LIMIT},
            "truncated": {"type": "boolean"},
            "candidates": {
                "type": "array",
                "items": _public_candidate_schema(),
                "maxItems": FILTER_PROJECTION_MAX_LIMIT,
            },
        }
    )


def _deferred_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "variant": {"type": "string", "const": "deferred"},
            "status": {"type": "string", "enum": ["stale", "not_ready"]},
            "reason": _reason_schema(),
            "retryable": {"type": "boolean"},
            "reselection_required": {"type": "boolean"},
        }
    )


def _error_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "variant": {"type": "string", "const": "error"},
            "status": {"type": "string", "const": "failed"},
            "reason": _reason_schema(),
            "retryable": {"type": "boolean", "const": False},
        }
    )


_FILTER_RESULT_VARIANT_SCHEMAS = {
    "success": _filter_success_schema(),
    "deferred": _deferred_schema(),
    "error": _error_schema(),
}


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


def _schema_at_path(schema: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    current = schema
    for raw_segment in path.split("/")[1:]:
        segment = raw_segment.replace("~1", "/").replace("~0", "~")
        if segment == "*":
            child = current.get("items")
        else:
            properties = current.get("properties")
            child = properties.get(segment) if isinstance(properties, Mapping) else None
        if not isinstance(child, Mapping):
            return MappingProxyType({})
        current = child
    return current


_FILTER_USER_IDENTIFIER_PATHS = (
    "/cohort_selection/schema_version",
    "/cohort_selection/role_bucket_ids/*",
    "/cohort_selection/employment_statuses/*",
    "/cohort_selection/role_match",
    "/cohort_selection/source",
)
_FILTER_DISPLAY_PATHS = frozenset(
    {
        "/candidates/*/display_name",
        "/candidates/*/headline",
    }
)
_FILTER_WEB_URL_PATHS = frozenset({"/candidates/*/public_profile_url"})


def _result_provenance(
    variant_schemas: Mapping[str, Mapping[str, Any]],
    *,
    user_prefixes: tuple[str, ...] = (),
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for variant, schema in variant_schemas.items():
        row: dict[str, str] = {}
        for path in sorted(_schema_field_paths(schema)):
            if path == "/variant" or path == "/status":
                classification = "server_derived"
            elif any(path == prefix or path.startswith(f"{prefix}/") for prefix in user_prefixes):
                classification = "user_supplied"
            else:
                classification = "owner_state"
            row[path] = classification
        result[variant] = row
    return result


def _result_value_roles(
    variant_schemas: Mapping[str, Mapping[str, Any]],
    *,
    user_identifier_paths: tuple[str, ...] = (),
    display_paths: frozenset[str] = frozenset(),
    web_url_paths: frozenset[str] = frozenset(),
) -> dict[str, dict[str, ActionResultValueRole]]:
    result: dict[str, dict[str, ActionResultValueRole]] = {}
    user_identifiers = set(user_identifier_paths)
    for variant, schema in variant_schemas.items():
        row: dict[str, ActionResultValueRole] = {}
        for path in sorted(_schema_string_paths(schema)):
            path_schema = _schema_at_path(schema, path)
            if path in user_identifiers:
                role: ActionResultValueRole = "identifier"
            elif path in display_paths:
                role = "display_text"
            elif path in web_url_paths:
                role = "web_url"
            elif "const" in path_schema or "enum" in path_schema:
                role = "control"
            else:
                role = "identifier"
            row[path] = role
        result[variant] = row
    return result


FILTER_PROJECTION_V2_RESULT_SPEC = ActionResultSpec(
    tool_name="filter_projection",
    tool_kind="action",
    owner_binding=ActionResultActionOwner(action_type="filter_projection"),
    result_schema_version=FILTER_PROJECTION_V2_RESULT_SCHEMA_VERSION,
    serializer_owner=FILTER_PROJECTION_V2_SERIALIZER_OWNER,
    serializer_revision=FILTER_PROJECTION_V2_SERIALIZER_REVISION,
    serializer_contract={
        "schema_version": "filter_projection_result_serializer_contract_v2",
        "owner_output": "closed_projection_publication_variant",
        "membership_source": PROJECTION_MEMBERSHIP_OWNER,
        "search_projection_v1_semantics": "unchanged_text_search",
    },
    validator_owner=ACTION_RESULT_VALIDATOR_OWNER,
    variant_schemas=_FILTER_RESULT_VARIANT_SCHEMAS,
    field_provenance=_result_provenance(
        _FILTER_RESULT_VARIANT_SCHEMAS,
        user_prefixes=("/cohort_selection",),
    ),
    field_value_roles=_result_value_roles(
        _FILTER_RESULT_VARIANT_SCHEMAS,
        user_identifier_paths=_FILTER_USER_IDENTIFIER_PATHS,
        display_paths=_FILTER_DISPLAY_PATHS,
        web_url_paths=_FILTER_WEB_URL_PATHS,
    ),
    max_serialized_bytes=FILTER_PROJECTION_RESULT_MAX_BYTES,
    max_items=8_192,
    max_depth=10,
    artifact_ref_schemes=(),
    externally_controlled_identifier_paths=_FILTER_USER_IDENTIFIER_PATHS,
)


def execute_filter_projection_v2(
    *,
    request: FilterProjectionV2BoundRequest,
    owner_snapshot: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Apply the owner predicate to one already-read canonical snapshot.

    Missing and access-scope failures share one masked result.  After access
    proof, any revision or Cohort identity mismatch is explicit stale evidence
    and never an empty success.
    """

    if not isinstance(request, FilterProjectionV2BoundRequest):
        raise AgentProjectionQueryError("filter_projection_bound_request_required")
    target = dict(request.target_ref)
    shallow = dict(owner_snapshot or {}) if isinstance(owner_snapshot, Mapping) else {}
    if (
        not shallow
        or shallow.get("access_scope") != PROJECTION_SHARED_ACCESS_SCOPE
        or shallow.get("projection_id") != target["projection_id"]
    ):
        return filter_projection_v2_error_result(reason="projection_not_found")
    try:
        snapshot = _FILTER_OWNER_SNAPSHOT_TOOL_SPEC.validate_input(shallow)
    except ModelToolSchemaError as exc:
        raise AgentProjectionQueryError("filter_projection_owner_snapshot_invalid", detail=str(exc)) from exc
    stale = (
        snapshot["membership_revision"] != target["membership_revision"]
        or snapshot["cohort_selection_registry_version"] != target["cohort_selection_registry_version"]
        or snapshot["cohort_selection_registry_digest"] != target["cohort_selection_registry_digest"]
        or snapshot["cohort_selection_digest"] != target["cohort_selection_digest"]
        or snapshot["selection_digest"] != target["cohort_selection_digest"]
        or snapshot["cohort_selection_registry_digest"] != cohort_selection_registry_digest()
        or snapshot["freshness"]["status"] != "fresh"
    )
    if stale:
        return filter_projection_v2_deferred_result(
            status="stale",
            reason="projection_cohort_identity_stale",
            retryable=False,
            reselection_required=True,
        )
    if snapshot["readiness"]["status"] != "ready":
        return filter_projection_v2_deferred_result(
            status="not_ready",
            reason=str(snapshot["readiness"].get("reason") or "projection_not_ready"),
            retryable=True,
            reselection_required=False,
        )

    candidates: list[dict[str, Any]] = []
    seen_candidate_ids: set[str] = set()
    for candidate in list(snapshot["candidates"]):
        candidate_id = str(candidate["candidate_identity_key"])
        if candidate_id in seen_candidate_ids:
            raise AgentProjectionQueryError("filter_projection_candidate_identity_duplicate")
        seen_candidate_ids.add(candidate_id)
        memberships = _canonical_lane_memberships(candidate["cohort_lane_membership"])
        if not request.predicate.matches(memberships):
            continue
        statuses = [
            status
            for status in _EMPLOYMENT_STATUSES
            if any(item["employment_status"] == status for item in memberships)
        ]
        roles = [role for role in _ROLE_IDS if any(item["role_bucket_id"] == role for item in memberships)]
        projected = {
            "candidate_ref": projection_candidate_ref(
                projection_id=str(snapshot["projection_id"]),
                membership_revision=str(snapshot["membership_revision"]),
                candidate_identity_key=candidate_id,
            ),
            "display_name": str(candidate["display_name"]),
            "headline": str(candidate["headline"]),
            "employment_statuses": statuses,
            "role_bucket_ids": roles,
        }
        if candidate.get("public_profile_url"):
            projected["public_profile_url"] = str(candidate["public_profile_url"])
        candidates.append(projected)

    input_payload = dict(request.input_payload)
    offset = int(input_payload["offset"])
    limit = int(input_payload["limit"])
    page = candidates[offset : offset + limit]
    result = {
        "variant": "success",
        "status": "ready",
        "projection_id": str(snapshot["projection_id"]),
        "membership_revision": str(snapshot["membership_revision"]),
        "cohort_selection": _thaw_json(input_payload["cohort_selection"]),
        "cohort_selection_registry_version": str(snapshot["cohort_selection_registry_version"]),
        "cohort_selection_registry_digest": str(snapshot["cohort_selection_registry_digest"]),
        "cohort_selection_digest": str(snapshot["cohort_selection_digest"]),
        "selection_digest": str(snapshot["selection_digest"]),
        "planning_digest": str(snapshot["planning_digest"]),
        "execution_digest": str(snapshot["execution_digest"]),
        "result_digest": str(snapshot["result_digest"]),
        "publication_digest": str(snapshot["publication_digest"]),
        "freshness": _thaw_json(snapshot["freshness"]),
        "readiness": _thaw_json(snapshot["readiness"]),
        "provider_mode": str(snapshot["provider_mode"]),
        "runtime_namespace": str(snapshot["runtime_namespace"]),
        "cache_provenance": _thaw_json(snapshot["cache_provenance"]),
        "requested_lane_coverage": _thaw_json(snapshot["requested_lane_coverage"]),
        "lane_summaries": _thaw_json(snapshot["lane_summaries"]),
        "offset": offset,
        "limit": limit,
        "total_count": len(candidates),
        "returned_count": len(page),
        "truncated": offset + len(page) < len(candidates),
        "candidates": page,
    }
    try:
        FILTER_PROJECTION_V2_RESULT_SPEC.serialize(result)
    except ActionResultSchemaError as exc:
        raise AgentProjectionQueryError("filter_projection_result_not_model_safe", detail=str(exc)) from exc
    return result


def projection_candidate_ref(
    *,
    projection_id: str,
    membership_revision: str,
    candidate_identity_key: str,
) -> str:
    """Mint a model-safe, revision-bound reference for one private owner key."""

    canonical_projection_id = _require_identifier(projection_id, field="projection_id")
    canonical_membership_revision = _require_identifier(
        membership_revision,
        field="membership_revision",
    )
    if (
        type(candidate_identity_key) is not str
        or not candidate_identity_key
        or len(candidate_identity_key) > 4_096
        or candidate_identity_key != candidate_identity_key.strip()
        or any(character.isspace() for character in candidate_identity_key)
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F or 0xD800 <= ord(character) <= 0xDFFF
            for character in candidate_identity_key
        )
    ):
        raise AgentProjectionQueryError(
            "agent_projection_query_identifier_invalid",
            "candidate_identity_key",
        )
    return _sha256_json(
        {
            "schema_version": FILTER_PROJECTION_CANDIDATE_REF_SCHEMA_VERSION,
            "projection_id": canonical_projection_id,
            "membership_revision": canonical_membership_revision,
            "candidate_identity_key": candidate_identity_key,
        }
    )


def filter_projection_v2_deferred_result(
    *,
    status: str,
    reason: str,
    retryable: bool,
    reselection_required: bool,
) -> dict[str, Any]:
    _require_reason(reason, field="reason")
    if status not in {"stale", "not_ready"}:
        raise AgentProjectionQueryError("filter_projection_deferred_status_invalid")
    return {
        "variant": "deferred",
        "status": status,
        "reason": reason,
        "retryable": bool(retryable),
        "reselection_required": bool(reselection_required),
    }


def filter_projection_v2_error_result(*, reason: str) -> dict[str, Any]:
    _require_reason(reason, field="reason")
    return {"variant": "error", "status": "failed", "reason": reason, "retryable": False}


def serialize_filter_projection_v2_result(owner_output: dict[str, Any]) -> str:
    return FILTER_PROJECTION_V2_RESULT_SPEC.serialize(owner_output)


@dataclass(frozen=True, slots=True)
class InspectOperationBoundRequest:
    """Model request plus exact identities minted by the query binder."""

    operation_run_id: str
    workspace_id: str
    action_id: str
    actor_id: str

    def __post_init__(self) -> None:
        for field_name in ("operation_run_id", "workspace_id", "action_id", "actor_id"):
            object.__setattr__(
                self,
                field_name,
                _require_identifier(getattr(self, field_name), field=field_name),
            )

    def to_owner_preflight_record(self) -> dict[str, str]:
        return {
            "preflight_owner": OPERATION_QUERY_OWNER_PREFLIGHT,
            "workspace_id": self.workspace_id,
            "action_id": self.action_id,
            "operation_run_id": self.operation_run_id,
            "actor_id": self.actor_id,
        }


def bind_inspect_operation_request(
    tool_input: Mapping[str, Any],
    *,
    workspace_id: str,
    action_id: str,
    actor_id: str,
) -> InspectOperationBoundRequest:
    """Bind model-visible input to server-authenticated workspace/action pins."""

    try:
        validated = INSPECT_OPERATION_REQUEST_TOOL_SPEC.validate_input(tool_input)
    except ModelToolSchemaError as exc:
        raise AgentProjectionQueryError("inspect_operation_request_invalid", detail=str(exc)) from exc
    return InspectOperationBoundRequest(
        operation_run_id=_require_identifier(validated["operation_run_id"], field="operation_run_id"),
        workspace_id=_require_identifier(workspace_id, field="workspace_id"),
        action_id=_require_identifier(action_id, field="action_id"),
        actor_id=_require_identifier(actor_id, field="actor_id"),
    )


def _operation_identity_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "workspace_id": _identifier_schema(),
            "action_id": _identifier_schema(),
            "operation_run_id": _identifier_schema(),
            "owner_module": _identifier_schema(),
            "operation_type": _identifier_schema(),
            "status": {"type": "string", "enum": list(_OPERATION_STATUSES)},
        }
    )


def _action_identity_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "workspace_id": _identifier_schema(),
            "action_id": _identifier_schema(),
            "action_type": _identifier_schema(),
            "owner_module": _identifier_schema(),
            "operation_type": _identifier_schema(),
            "status": {"type": "string", "enum": list(_ACTION_STATUSES)},
        }
    )


def _control_state_schema() -> dict[str, Any]:
    disabled_reason_properties = {action: _reason_schema() for action in _CONTROL_ACTIONS}
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": "operation_run_control_state_v1"},
            "operation_status": {"type": "string", "enum": list(_OPERATION_STATUSES)},
            "action_status": {"type": "string", "enum": list(_ACTION_STATUSES)},
            "operation_phase": _identifier_schema(),
            "can_dispatch": {"type": "boolean"},
            "can_cancel": {"type": "boolean"},
            "can_retry": {"type": "boolean"},
            "can_resume": {"type": "boolean"},
            "allowed_actions": {
                "type": "array",
                "items": {"type": "string", "enum": list(_CONTROL_ACTIONS)},
                "maxItems": len(_CONTROL_ACTIONS),
            },
            "disabled_reasons": _closed_object(disabled_reason_properties, required=()),
            "control_source_of_truth": {
                "type": "string",
                "const": "operation_runtime.operation_run_control_state",
            },
            "fallback_status": {"type": "string", "const": "fail_closed"},
            "module_state_mutated_on_control": {"type": "boolean", "const": False},
        }
    )


def _control_policy_projection_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "status": {"type": "string", "enum": ["available", "not_applicable"]},
            "source_of_truth": {
                "type": "string",
                "enum": [
                    "durable_runtime.workflow_command_control_policy",
                    "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
                ],
            },
            "fallback_status": {"type": "string", "const": "fail_closed"},
            "command_type": _identifier_schema(),
            "owner": _identifier_schema(),
            "running_control_maturity": _identifier_schema(),
            "running_control_gap_status": _identifier_schema(),
            "running_control_surface": _identifier_schema(),
            "running_cancel_supported": {"type": "boolean"},
            "running_resume_supported": {"type": "boolean"},
        },
        required=("status", "source_of_truth", "fallback_status"),
    )


def _display_contract_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": "operation_action_display_contract_v1"},
            "action_type": _identifier_schema(),
            "owner_module": _identifier_schema(),
            "operation_type": _identifier_schema(),
            "display_label": _display_text_schema(maximum=500),
            "display_category": _display_text_schema(maximum=200),
            "description": _display_text_schema(maximum=1_000),
            "source_of_truth": {
                "type": "string",
                "const": "operation_runtime.ActionRegistry.display_contract_for",
            },
            "fallback_status": {"type": "string", "const": "fail_closed"},
        }
    )


def _operation_progress_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "phase": _identifier_schema(),
            "reason": _reason_schema(),
            "source_of_truth": {"type": "string", "const": OPERATION_PROGRESS_OWNER},
        },
        required=("phase", "source_of_truth"),
    )


def _result_readiness_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "status": {
                "type": "string",
                "enum": ["pending", "ready", "failed", "cancelled", "not_applicable"],
            },
            "result_ref_present": {"type": "boolean"},
            "source_of_truth": {"type": "string", "const": OPERATION_RESULT_READINESS_OWNER},
            "fallback_status": {"type": "string", "const": "fail_closed"},
        }
    )


def _operation_provenance_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "source_of_truth": {"type": "string", "const": OPERATION_PROVENANCE_OWNER},
            "operation_event_count": {"type": "integer", "minimum": 0, "maximum": 100_000},
            "workflow_command_count": {"type": "integer", "minimum": 0, "maximum": 100_000},
            "latest_event_type": _identifier_schema(),
            "latest_workflow_command_id": _identifier_schema(),
            "latest_workflow_command_type": _identifier_schema(),
            "truncated": {"type": "boolean"},
        },
        required=(
            "source_of_truth",
            "operation_event_count",
            "workflow_command_count",
            "truncated",
        ),
    )


def _inspect_owner_snapshot_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "action": _action_identity_schema(),
            "operation_run": _operation_identity_schema(),
            "control_state": _control_state_schema(),
            "control_policy": _control_policy_projection_schema(),
            "display_contract": _display_contract_schema(),
            "progress": _operation_progress_schema(),
            "result_readiness": _result_readiness_schema(),
            "provenance": _operation_provenance_schema(),
        }
    )


_INSPECT_OWNER_SNAPSHOT_TOOL_SPEC = ToolSpec(
    name="inspect_operation_owner_snapshot",
    description="Validate one canonical bounded Operation query projection.",
    input_schema=_inspect_owner_snapshot_schema(),
    schema_version="inspect_operation_owner_snapshot_v1",
    approval_policy="validation_only",
    budget_required=False,
)


def _inspect_success_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "variant": {"type": "string", "const": "success"},
            "status": {"type": "string", "const": "ready"},
            "action_id": _identifier_schema(),
            "operation_run_id": _identifier_schema(),
            "control_state": _control_state_schema(),
            "control_policy": _control_policy_projection_schema(),
            "display_contract": _display_contract_schema(),
            "progress": _operation_progress_schema(),
            "result_readiness": _result_readiness_schema(),
            "provenance": _operation_provenance_schema(),
        }
    )


_INSPECT_RESULT_VARIANT_SCHEMAS = {
    "success": _inspect_success_schema(),
    "deferred": _deferred_schema(),
    "error": _error_schema(),
}

_INSPECT_DISPLAY_PATHS = frozenset(
    {
        "/display_contract/display_label",
        "/display_contract/display_category",
        "/display_contract/description",
    }
)

INSPECT_OPERATION_QUERY_OWNER_CONTRACT = MappingProxyType(
    {
        "schema_version": "inspect_operation_query_owner_contract_v1",
        "owner_id": INSPECT_OPERATION_QUERY_OWNER_ID,
        "owner_revision": INSPECT_OPERATION_QUERY_OWNER_REVISION,
        "request_schema_version": INSPECT_OPERATION_REQUEST_SCHEMA_VERSION,
        "request_schema_digest": INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST,
        "owner_preflight": OPERATION_QUERY_OWNER_PREFLIGHT,
        "projection_fields": (
            "control_state",
            "control_policy",
            "display_contract",
            "progress",
            "result_readiness",
            "provenance",
        ),
        "side_effects": "none",
    }
)
INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST = _sha256_json(dict(INSPECT_OPERATION_QUERY_OWNER_CONTRACT))

INSPECT_OPERATION_RESULT_SPEC = ActionResultSpec(
    tool_name="inspect_operation",
    tool_kind="query",
    owner_binding=ActionResultQueryOwner(
        owner_id=INSPECT_OPERATION_QUERY_OWNER_ID,
        owner_revision=INSPECT_OPERATION_QUERY_OWNER_REVISION,
        owner_contract_digest=INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST,
    ),
    result_schema_version=INSPECT_OPERATION_RESULT_SCHEMA_VERSION,
    serializer_owner=INSPECT_OPERATION_SERIALIZER_OWNER,
    serializer_revision=INSPECT_OPERATION_SERIALIZER_REVISION,
    serializer_contract={
        "schema_version": "inspect_operation_result_serializer_contract_v1",
        "owner_output": "exact_bounded_operation_query_projection",
        "owner_preflight": OPERATION_QUERY_OWNER_PREFLIGHT,
        "writes": "forbidden",
        "repair": "forbidden",
        "next_control_inference": "forbidden",
    },
    validator_owner=ACTION_RESULT_VALIDATOR_OWNER,
    variant_schemas=_INSPECT_RESULT_VARIANT_SCHEMAS,
    field_provenance=_result_provenance(_INSPECT_RESULT_VARIANT_SCHEMAS),
    field_value_roles=_result_value_roles(
        _INSPECT_RESULT_VARIANT_SCHEMAS,
        display_paths=_INSPECT_DISPLAY_PATHS,
    ),
    max_serialized_bytes=INSPECT_OPERATION_RESULT_MAX_BYTES,
    max_items=2_048,
    max_depth=8,
    artifact_ref_schemes=(),
)


def execute_inspect_operation(
    *,
    request: InspectOperationBoundRequest,
    owner_snapshot: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return a bounded canonical query projection with no state transition."""

    if not isinstance(request, InspectOperationBoundRequest):
        raise AgentProjectionQueryError("inspect_operation_bound_request_required")
    shallow = dict(owner_snapshot or {}) if isinstance(owner_snapshot, Mapping) else {}
    action = dict(shallow.get("action") or {}) if isinstance(shallow.get("action"), Mapping) else {}
    operation_run = (
        dict(shallow.get("operation_run") or {}) if isinstance(shallow.get("operation_run"), Mapping) else {}
    )
    exact_owner = (
        action.get("workspace_id") == request.workspace_id
        and action.get("action_id") == request.action_id
        and operation_run.get("workspace_id") == request.workspace_id
        and operation_run.get("action_id") == request.action_id
        and operation_run.get("operation_run_id") == request.operation_run_id
    )
    if not exact_owner:
        return inspect_operation_error_result(reason="operation_not_found")
    try:
        snapshot = _INSPECT_OWNER_SNAPSHOT_TOOL_SPEC.validate_input(shallow)
    except ModelToolSchemaError as exc:
        raise AgentProjectionQueryError("inspect_operation_owner_snapshot_invalid", detail=str(exc)) from exc
    _validate_inspect_owner_snapshot_semantics(snapshot)
    result = {
        "variant": "success",
        "status": "ready",
        "action_id": request.action_id,
        "operation_run_id": request.operation_run_id,
        "control_state": _thaw_json(snapshot["control_state"]),
        "control_policy": _thaw_json(snapshot["control_policy"]),
        "display_contract": _thaw_json(snapshot["display_contract"]),
        "progress": _thaw_json(snapshot["progress"]),
        "result_readiness": _thaw_json(snapshot["result_readiness"]),
        "provenance": _thaw_json(snapshot["provenance"]),
    }
    try:
        INSPECT_OPERATION_RESULT_SPEC.serialize(result)
    except ActionResultSchemaError as exc:
        raise AgentProjectionQueryError("inspect_operation_result_not_model_safe", detail=str(exc)) from exc
    return result


def inspect_operation_deferred_result(
    *,
    status: str,
    reason: str,
    retryable: bool,
) -> dict[str, Any]:
    if status not in {"stale", "not_ready"}:
        raise AgentProjectionQueryError("inspect_operation_deferred_status_invalid")
    _require_reason(reason, field="reason")
    return {
        "variant": "deferred",
        "status": status,
        "reason": reason,
        "retryable": bool(retryable),
        "reselection_required": False,
    }


def inspect_operation_error_result(*, reason: str) -> dict[str, Any]:
    _require_reason(reason, field="reason")
    return {"variant": "error", "status": "failed", "reason": reason, "retryable": False}


def serialize_inspect_operation_result(owner_output: dict[str, Any]) -> str:
    if owner_output.get("variant") == "success":
        _validate_control_policy_projection(dict(owner_output.get("control_policy") or {}))
    return INSPECT_OPERATION_RESULT_SPEC.serialize(owner_output)


def _validate_inspect_owner_snapshot_semantics(snapshot: Mapping[str, Any]) -> None:
    action = dict(snapshot["action"])
    operation = dict(snapshot["operation_run"])
    control_state = dict(snapshot["control_state"])
    display = dict(snapshot["display_contract"])
    progress = dict(snapshot["progress"])
    policy = dict(snapshot["control_policy"])
    provenance = dict(snapshot["provenance"])
    if (
        operation["owner_module"] != action["owner_module"]
        or operation["operation_type"] != action["operation_type"]
        or control_state["operation_status"] != operation["status"]
        or control_state["action_status"] != action["status"]
        or control_state["operation_phase"] != progress["phase"]
        or display["action_type"] != action["action_type"]
        or display["owner_module"] != action["owner_module"]
        or display["operation_type"] != action["operation_type"]
    ):
        raise AgentProjectionQueryError("inspect_operation_owner_projection_mismatch")
    allowed_actions = list(control_state["allowed_actions"])
    if len(allowed_actions) != len(set(allowed_actions)):
        raise AgentProjectionQueryError("inspect_operation_control_state_duplicate_action")
    for action_name in _CONTROL_ACTIONS:
        flag = bool(control_state[f"can_{action_name}"])
        if flag != (action_name in allowed_actions):
            raise AgentProjectionQueryError("inspect_operation_control_state_action_mismatch")
        if flag and action_name in dict(control_state["disabled_reasons"]):
            raise AgentProjectionQueryError("inspect_operation_control_state_disabled_reason_mismatch")
    _validate_control_policy_projection(policy)
    latest_type = str(provenance.get("latest_workflow_command_type") or "")
    if policy["status"] == "available" and latest_type != policy["command_type"]:
        raise AgentProjectionQueryError("inspect_operation_control_policy_command_mismatch")
    if policy["status"] == "not_applicable" and (
        int(provenance["workflow_command_count"]) != 0
        or provenance.get("latest_workflow_command_id")
        or provenance.get("latest_workflow_command_type")
    ):
        raise AgentProjectionQueryError("inspect_operation_control_policy_not_applicable_mismatch")


def _validate_control_policy_projection(policy: Mapping[str, Any]) -> None:
    status = str(policy.get("status") or "")
    common = {"status", "source_of_truth", "fallback_status"}
    available = {
        "command_type",
        "owner",
        "running_control_maturity",
        "running_control_gap_status",
        "running_control_surface",
        "running_cancel_supported",
        "running_resume_supported",
    }
    if status == "available":
        if set(policy) != common | available:
            raise AgentProjectionQueryError("inspect_operation_control_policy_projection_invalid")
        if policy.get("source_of_truth") != "durable_runtime.workflow_command_control_policy":
            raise AgentProjectionQueryError("inspect_operation_control_policy_projection_invalid")
    elif status == "not_applicable":
        if set(policy) != common:
            raise AgentProjectionQueryError("inspect_operation_control_policy_projection_invalid")
        if policy.get("source_of_truth") != "operation_runtime.ActionRegistry.allowed_workflow_command_contracts":
            raise AgentProjectionQueryError("inspect_operation_control_policy_projection_invalid")
    else:
        raise AgentProjectionQueryError("inspect_operation_control_policy_projection_invalid")


def _canonical_lane_memberships(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, (list, tuple)) or not value:
        raise AgentProjectionQueryError("projection_lane_membership_invalid")
    expected_fields = {"lane_id", "employment_status", "role_bucket_id"}
    canonical: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in value:
        if not isinstance(item, Mapping) or set(item) != expected_fields:
            raise AgentProjectionQueryError("projection_lane_membership_invalid")
        lane_id = _require_identifier(item.get("lane_id"), field="cohort_lane_membership.lane_id")
        employment_status = str(item.get("employment_status") or "")
        role_bucket_id = str(item.get("role_bucket_id") or "")
        if employment_status not in _EMPLOYMENT_STATUSES or (role_bucket_id and role_bucket_id not in _ROLE_IDS):
            raise AgentProjectionQueryError("projection_lane_membership_invalid")
        identity = (lane_id, employment_status, role_bucket_id)
        if identity in seen:
            raise AgentProjectionQueryError("projection_lane_membership_duplicate")
        seen.add(identity)
        canonical.append(
            {
                "lane_id": lane_id,
                "employment_status": employment_status,
                "role_bucket_id": role_bucket_id,
            }
        )
    return canonical


def _require_identifier(value: Any, *, field: str) -> str:
    if type(value) is not str or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise AgentProjectionQueryError("agent_projection_query_identifier_invalid", field)
    return value


def _require_reason(value: Any, *, field: str) -> str:
    if type(value) is not str or _REASON_PATTERN.fullmatch(value) is None:
        raise AgentProjectionQueryError("agent_projection_query_reason_invalid", field)
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in sorted(value.items())})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw_json(item) for item in value]
    return value


__all__ = [
    "AgentProjectionQueryError",
    "FILTER_PROJECTION_CANDIDATE_REF_SCHEMA_VERSION",
    "FILTER_PROJECTION_V2_PREDICATE_SCHEMA_VERSION",
    "FILTER_PROJECTION_V2_REQUEST_SCHEMA_DIGEST",
    "FILTER_PROJECTION_V2_REQUEST_SCHEMA_VERSION",
    "FILTER_PROJECTION_V2_REQUEST_TOOL_SPEC",
    "FILTER_PROJECTION_V2_RESULT_SCHEMA_VERSION",
    "FILTER_PROJECTION_V2_RESULT_SPEC",
    "FilterProjectionV2BoundRequest",
    "INSPECT_OPERATION_QUERY_OWNER_CONTRACT",
    "INSPECT_OPERATION_QUERY_OWNER_CONTRACT_DIGEST",
    "INSPECT_OPERATION_QUERY_OWNER_ID",
    "INSPECT_OPERATION_QUERY_OWNER_REVISION",
    "INSPECT_OPERATION_REQUEST_SCHEMA_DIGEST",
    "INSPECT_OPERATION_REQUEST_SCHEMA_VERSION",
    "INSPECT_OPERATION_REQUEST_TOOL_SPEC",
    "INSPECT_OPERATION_RESULT_SCHEMA_VERSION",
    "INSPECT_OPERATION_RESULT_SPEC",
    "InspectOperationBoundRequest",
    "ProjectionCohortPredicate",
    "bind_filter_projection_v2_request",
    "bind_inspect_operation_request",
    "execute_filter_projection_v2",
    "execute_inspect_operation",
    "filter_projection_v2_deferred_result",
    "filter_projection_v2_error_result",
    "filter_projection_v2_request_schema",
    "inspect_operation_deferred_result",
    "inspect_operation_error_result",
    "inspect_operation_request_schema",
    "projection_candidate_ref",
    "serialize_filter_projection_v2_result",
    "serialize_inspect_operation_result",
]
