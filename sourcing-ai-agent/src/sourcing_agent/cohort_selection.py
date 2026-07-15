"""Canonical user-selected sourcing cohort contract.

The v1 object is optional.  Legacy request payloads remain byte-compatible and
are adapted only when a consumer explicitly asks for an effective cohort.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

from .query_signal_knowledge import ROLE_BUCKET_KNOWLEDGE

COHORT_SELECTION_SCHEMA_VERSION = "cohort_selection.v1"
COHORT_SELECTION_REGISTRY_VERSION = "cohort_selection.registry.v1"
COHORT_SELECTION_SOURCES = frozenset({"user_explicit", "legacy_adapter", "inferred"})
EXTERNAL_COHORT_SELECTION_SOURCES = frozenset({"user_explicit"})
ROLE_MATCH_VALUES = ("any", "all")

_COHORT_SELECTION_FIELDS = frozenset(
    {
        "schema_version",
        "role_bucket_ids",
        "employment_statuses",
        "role_match",
        "source",
    }
)
_EMPLOYMENT_STATUS_OPTIONS: tuple[dict[str, Any], ...] = (
    {"id": "current", "label": "Current employees", "order": 10},
    {"id": "former", "label": "Former employees", "order": 20},
)
_EMPLOYMENT_STATUS_ORDER = {str(item["id"]): int(item["order"]) for item in _EMPLOYMENT_STATUS_OPTIONS}
_ROLE_MATCH_OPTIONS: tuple[dict[str, Any], ...] = (
    {"id": "any", "label": "Match any selected role", "order": 10},
    {"id": "all", "label": "Match all selected roles", "order": 20},
)


@dataclass(frozen=True, slots=True)
class CohortSelectionValidationError(ValueError):
    """Stable fail-closed validation result used by service entrypoints."""

    code: str
    field: str = ""
    detail: str = ""

    def __str__(self) -> str:
        suffix = f": {self.field}" if self.field else ""
        return f"{self.code}{suffix}"

    def to_result(self) -> dict[str, Any]:
        result: dict[str, Any] = {"status": "invalid", "reason": self.code}
        if self.field:
            result["field"] = self.field
        if self.detail:
            result["detail"] = self.detail
        return result


def cohort_selection_options_payload() -> dict[str, Any]:
    """Return the stable UI/API option catalog from the sole role registry."""

    registry = _cohort_selection_registry_payload()
    return {
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "registry_version": COHORT_SELECTION_REGISTRY_VERSION,
        "registry_digest": cohort_selection_registry_digest(),
        "role_buckets": [
            {
                "id": str(item["id"]),
                "label": str(item["label"]),
                "order": int(item["order"]),
            }
            for item in registry["role_buckets"]
        ],
        "employment_statuses": [dict(item) for item in registry["employment_statuses"]],
        "role_match_options": [dict(item) for item in registry["role_match_options"]],
        "defaults": dict(registry["defaults"]),
    }


def cohort_selection_registry_digest() -> str:
    """Pin the exact selectable registry semantics without a second registry."""

    return _sha256_json(_cohort_selection_registry_payload())


def cohort_selection_digest(value: Any) -> str:
    """Return a source-insensitive digest of one canonical selection.

    The registry pin participates in the digest, so a later registry change
    cannot silently reuse the same selection identity.  This metadata is
    derived by server consumers and is intentionally not caller supplied.
    """

    cohort = normalize_cohort_selection(value)
    return _sha256_json(
        {
            "registry_version": COHORT_SELECTION_REGISTRY_VERSION,
            "registry_digest": cohort_selection_registry_digest(),
            "schema_version": cohort["schema_version"],
            "role_bucket_ids": list(cohort["role_bucket_ids"]),
            "employment_statuses": list(cohort["employment_statuses"]),
            "role_match": cohort["role_match"],
        }
    )


def normalize_cohort_selection(
    value: Any,
    *,
    allowed_sources: Iterable[str] = COHORT_SELECTION_SOURCES,
) -> dict[str, Any]:
    """Validate and canonicalize one strict v1 cohort object."""

    if not isinstance(value, dict):
        raise CohortSelectionValidationError(
            "cohort_selection_invalid_type",
            "cohort_selection",
            "cohort_selection must be an object",
        )
    unknown_fields = sorted(set(value) - _COHORT_SELECTION_FIELDS)
    if unknown_fields:
        raise CohortSelectionValidationError(
            "cohort_selection_unknown_field",
            f"cohort_selection.{unknown_fields[0]}",
        )
    missing_fields = sorted(_COHORT_SELECTION_FIELDS - set(value))
    if missing_fields:
        raise CohortSelectionValidationError(
            "cohort_selection_missing_field",
            f"cohort_selection.{missing_fields[0]}",
        )

    schema_version = str(value.get("schema_version") or "").strip()
    if schema_version != COHORT_SELECTION_SCHEMA_VERSION:
        raise CohortSelectionValidationError(
            "cohort_selection_unsupported_schema_version",
            "cohort_selection.schema_version",
        )

    role_bucket_ids = _normalize_strict_role_bucket_ids(value.get("role_bucket_ids"))
    employment_statuses = _normalize_strict_employment_statuses(
        value.get("employment_statuses"),
        allow_empty=False,
    )
    role_match = str(value.get("role_match") or "").strip().lower()
    if role_match not in ROLE_MATCH_VALUES:
        raise CohortSelectionValidationError(
            "cohort_selection_invalid_role_match",
            "cohort_selection.role_match",
        )
    source = str(value.get("source") or "").strip().lower()
    normalized_allowed_sources = {
        str(item or "").strip().lower() for item in allowed_sources if str(item or "").strip()
    }
    if source not in COHORT_SELECTION_SOURCES or source not in normalized_allowed_sources:
        raise CohortSelectionValidationError(
            "cohort_selection_invalid_source",
            "cohort_selection.source",
        )
    return {
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "role_bucket_ids": role_bucket_ids,
        "employment_statuses": employment_statuses,
        "role_match": role_match,
        "source": source,
    }


def validate_external_cohort_selection_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Validate an external request without materializing an absent cohort."""

    request_payload = dict(payload or {})
    if "cohort_selection" not in request_payload:
        return request_payload
    return canonicalize_cohort_selection_request_payload(
        request_payload,
        allowed_sources=EXTERNAL_COHORT_SELECTION_SOURCES,
    )


def canonicalize_cohort_selection_request_payload(
    payload: dict[str, Any] | None,
    *,
    allowed_sources: Iterable[str] = COHORT_SELECTION_SOURCES,
) -> dict[str, Any]:
    """Apply one explicit cohort and enforce all present legacy mirrors."""

    request_payload = dict(payload or {})
    if "cohort_selection" not in request_payload:
        return request_payload
    cohort = normalize_cohort_selection(
        request_payload.get("cohort_selection"),
        allowed_sources=allowed_sources,
    )
    if str(cohort.get("source") or "") == "user_explicit":
        request_payload, _ = _strip_role_like_request_constraints(
            request_payload,
            drop_empty_fields=False,
        )
    role_bucket_ids = list(cohort["role_bucket_ids"])
    employment_statuses = list(cohort["employment_statuses"])

    _verify_or_install_flat_mirror(
        request_payload,
        canonical_key="must_have_primary_role_buckets",
        aliases=("must_have_primary_role_bucket",),
        expected=role_bucket_ids,
        normalizer=_normalize_legacy_role_bucket_ids,
    )
    _verify_or_install_flat_mirror(
        request_payload,
        canonical_key="employment_statuses",
        aliases=(),
        expected=employment_statuses,
        normalizer=lambda raw: _normalize_strict_employment_statuses(raw, allow_empty=True),
    )
    _verify_intent_axes_mirrors(
        request_payload,
        role_bucket_ids=role_bucket_ids,
        employment_statuses=employment_statuses,
        role_match=str(cohort["role_match"]),
    )
    request_payload["cohort_selection"] = cohort
    return request_payload


def merge_plan_review_cohort_selection(
    stored_request: dict[str, Any] | None,
    incoming_override: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge the sole cohort authority across a plan-review boundary.

    A stored user selection is immutable except for an exact replay.  A stored
    legacy/server request may be upgraded by one externally valid explicit
    selection.  Validation completes on copies before the merged request is
    returned, so callers can write the result atomically.
    """

    stored = canonicalize_cohort_selection_request_payload(stored_request)
    incoming = dict(incoming_override or {})
    stored_cohort = explicit_cohort_selection(stored)
    incoming_has_cohort = "cohort_selection" in incoming
    incoming_mirror_keys = tuple(
        key
        for key in (
            "employment_statuses",
            "must_have_primary_role_bucket",
            "must_have_primary_role_buckets",
            "intent_axes",
        )
        if key in incoming
    )

    if not incoming_has_cohort:
        if stored_cohort is not None and incoming_mirror_keys:
            candidate = dict(stored)
            for key in incoming_mirror_keys:
                candidate[key] = incoming[key]
            try:
                canonicalize_cohort_selection_request_payload(candidate)
            except CohortSelectionValidationError as exc:
                raise CohortSelectionValidationError(
                    "cohort_selection_plan_review_conflict",
                    exc.field or incoming_mirror_keys[0],
                ) from exc
            # Incoming mirror fields are validation evidence, not an alternate
            # request-patch surface.  In particular, accepting an exact cohort
            # leaf must not replace the stored intent_axes object and discard
            # unrelated scope/fallback policy.
            return stored
        return stored

    canonical_incoming = validate_external_cohort_selection_payload(incoming)
    incoming_cohort = dict(canonical_incoming["cohort_selection"])
    if (
        stored_cohort is not None
        and str(stored_cohort.get("source") or "") == "user_explicit"
        and incoming_cohort != stored_cohort
    ):
        raise CohortSelectionValidationError(
            "cohort_selection_plan_review_conflict",
            "cohort_selection",
        )

    merged = dict(stored)
    merged["cohort_selection"] = incoming_cohort
    merged["must_have_primary_role_buckets"] = list(incoming_cohort["role_bucket_ids"])
    merged["employment_statuses"] = list(incoming_cohort["employment_statuses"])
    _restore_present_intent_axes_mirrors(merged, cohort=incoming_cohort)
    return canonicalize_cohort_selection_request_payload(merged)


def explicit_cohort_selection(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return a validated explicit object, without adapting legacy fields."""

    request_payload = dict(payload or {})
    if "cohort_selection" not in request_payload:
        return None
    canonical = canonicalize_cohort_selection_request_payload(request_payload)
    return dict(canonical["cohort_selection"])


def effective_cohort_selection(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Lazily adapt a legacy request for consumers that require this contract."""

    request_payload = dict(payload or {})
    explicit = explicit_cohort_selection(request_payload)
    if explicit is not None:
        return explicit
    if "employment_statuses" not in request_payload:
        return None
    employment_statuses = _normalize_strict_employment_statuses(
        request_payload.get("employment_statuses"),
        allow_empty=True,
    )
    if not employment_statuses:
        return None
    role_bucket_ids = _normalize_legacy_role_bucket_ids(request_payload.get("must_have_primary_role_buckets"))
    return {
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "role_bucket_ids": role_bucket_ids,
        "employment_statuses": employment_statuses,
        "role_match": "any",
        "source": "legacy_adapter",
    }


def apply_user_explicit_cohort_authority(
    base_payload: dict[str, Any] | None,
    candidate_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Keep an explicit user cohort authoritative over model-derived patches."""

    base = dict(base_payload or {})
    candidate = dict(candidate_payload or {})
    cohort = explicit_cohort_selection(base)
    if cohort is None:
        return candidate
    if str(cohort.get("source") or "") != "user_explicit":
        # Server-owned inferred/adapter objects are provenance snapshots, not
        # user locks.  Preserve an unchanged object, but retire a stale object
        # when a later internal model patch changes one of its mirrors.
        try:
            canonicalize_cohort_selection_request_payload(candidate)
        except CohortSelectionValidationError:
            candidate.pop("cohort_selection", None)
        return candidate
    candidate["cohort_selection"] = cohort
    candidate["must_have_primary_role_buckets"] = list(cohort["role_bucket_ids"])
    candidate["employment_statuses"] = list(cohort["employment_statuses"])
    _restore_present_intent_axes_mirrors(candidate, cohort=cohort)
    candidate, _ = _strip_role_like_request_constraints(
        candidate,
        drop_empty_fields=False,
    )
    return candidate


def remove_explicit_cohort_mirror_patch_fields(
    base_payload: dict[str, Any] | None,
    patch: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    """Drop internal model fields that would mutate a user-explicit cohort."""

    normalized_patch = dict(patch or {})
    cohort = explicit_cohort_selection(base_payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return normalized_patch, []
    removed: list[str] = []
    for key in (
        "cohort_selection",
        "employment_statuses",
        "must_have_primary_role_bucket",
        "must_have_primary_role_buckets",
    ):
        if key in normalized_patch:
            normalized_patch.pop(key, None)
            removed.append(key)
    normalized_patch, role_constraint_fields = _strip_role_like_request_constraints(
        normalized_patch,
        drop_empty_fields=True,
    )
    removed.extend(role_constraint_fields)
    return normalized_patch, removed


def validate_refinement_patch_against_explicit_cohort(
    base_payload: dict[str, Any] | None,
    patch: dict[str, Any] | None,
) -> None:
    """Reject an external flat refinement that conflicts with cohort authority."""

    candidate = dict(patch or {})
    cohort = explicit_cohort_selection(base_payload)
    if "cohort_selection" in candidate:
        incoming_cohort = normalize_cohort_selection(
            candidate.get("cohort_selection"),
            allowed_sources=EXTERNAL_COHORT_SELECTION_SOURCES,
        )
        if cohort is None or str(cohort.get("source") or "") != "user_explicit":
            raise CohortSelectionValidationError(
                "cohort_selection_refinement_not_supported",
                "cohort_selection",
            )
        if incoming_cohort != cohort:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "cohort_selection",
            )
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return
    _, role_constraint_fields = _strip_role_like_request_constraints(
        candidate,
        drop_empty_fields=False,
    )
    if role_constraint_fields:
        raise CohortSelectionValidationError(
            "cohort_selection_mirror_conflict",
            role_constraint_fields[0],
        )
    if "employment_statuses" in candidate:
        actual_statuses = _normalize_strict_employment_statuses(
            candidate.get("employment_statuses"),
            allow_empty=True,
        )
        if actual_statuses != list(cohort["employment_statuses"]):
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "employment_statuses",
            )
    for role_key in ("must_have_primary_role_buckets", "must_have_primary_role_bucket"):
        if role_key not in candidate:
            continue
        actual_roles = _normalize_legacy_role_bucket_ids(candidate.get(role_key))
        if actual_roles != list(cohort["role_bucket_ids"]):
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                role_key,
            )


def cohort_execution_identity_for_signature(payload: dict[str, Any] | None) -> str:
    """Bind every user-explicit cohort to request/reuse identity.

    A legacy request with the same flat fields is not necessarily execution
    equivalent: the legacy resolver may infer role filters while an explicit
    empty role list means all roles, and explicit multi-role requests compile
    to separate provider lanes.  The registry-pinned selection digest keeps
    those requests out of the same reuse family without changing absent-object
    legacy signatures.
    """

    cohort = explicit_cohort_selection(payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return ""
    return cohort_selection_digest(cohort)


def source_request_covers_explicit_cohort(
    request_payload: dict[str, Any] | None,
    source_request_payload: Any,
) -> bool:
    """Return whether a reusable source covers the current explicit cohort.

    Legacy current requests retain their existing reuse behavior.  Once the
    current request carries user authority, a missing, legacy, different, or
    malformed source request is not eligible for reuse.
    """

    try:
        requested_identity = cohort_execution_identity_for_signature(request_payload)
    except CohortSelectionValidationError:
        return False
    if not requested_identity:
        return True
    if not isinstance(source_request_payload, dict) or not source_request_payload:
        return False
    try:
        return cohort_execution_identity_for_signature(source_request_payload) == requested_identity
    except CohortSelectionValidationError:
        return False


def cohort_role_match_for_signature(payload: dict[str, Any] | None) -> str:
    """Backward-compatible helper for callers that only inspect role_match."""

    cohort = explicit_cohort_selection(payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return ""
    return str(cohort.get("role_match") or "").strip()


def _strip_role_like_request_constraints(
    payload: dict[str, Any],
    *,
    drop_empty_fields: bool,
) -> tuple[dict[str, Any], list[str]]:
    """Remove legacy role constraints that would compete with CohortSelection.

    `categories` and `must_have_facets` also carry non-role business semantics,
    so only values classified as roles are removed.  The same owner handles
    flat fields and their intent-axis representations to prevent a later
    materialization pass from restoring a discarded role hint.
    """

    sanitized = dict(payload or {})
    removed_fields: list[str] = []

    def _sanitize_container_field(container: dict[str, Any], key: str, *, field_path: str) -> None:
        if key not in container:
            return
        kept, removed = _split_role_like_request_values(key, container.get(key))
        if not removed:
            return
        removed_fields.append(field_path)
        if kept or not drop_empty_fields:
            container[key] = kept
        else:
            container.pop(key, None)

    _sanitize_container_field(sanitized, "categories", field_path="categories")
    _sanitize_container_field(sanitized, "must_have_facets", field_path="must_have_facets")

    axes = sanitized.get("intent_axes")
    if isinstance(axes, dict):
        axes_copy = dict(axes)
        population = axes_copy.get("population_boundary")
        if isinstance(population, dict):
            population_copy = dict(population)
            _sanitize_container_field(
                population_copy,
                "categories",
                field_path="intent_axes.population_boundary.categories",
            )
            axes_copy["population_boundary"] = population_copy
        thematic = axes_copy.get("thematic_constraints")
        if isinstance(thematic, dict):
            thematic_copy = dict(thematic)
            _sanitize_container_field(
                thematic_copy,
                "must_have_facets",
                field_path="intent_axes.thematic_constraints.must_have_facets",
            )
            axes_copy["thematic_constraints"] = thematic_copy
        sanitized["intent_axes"] = axes_copy
    return sanitized, list(dict.fromkeys(removed_fields))


def _split_role_like_request_values(field: str, value: Any) -> tuple[list[Any], list[str]]:
    if isinstance(value, str):
        raw_values: list[Any] = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        return [], []
    kept: list[Any] = []
    removed: list[str] = []
    for item in raw_values:
        if _is_role_like_request_value(field, item):
            removed.append(str(item or "").strip())
        else:
            kept.append(item)
    return kept, removed


def _is_role_like_request_value(field: str, value: Any) -> bool:
    # Lazy import avoids a module cycle: domain owns the legacy alias maps and
    # imports this module for JobRequest cohort canonicalization.
    from .domain import (
        ROLE_BUCKET_PRIORITY,
        normalize_requested_facet,
        normalize_requested_role_bucket,
    )

    if field == "categories":
        normalized = normalize_requested_role_bucket(str(value or ""))
        # These are membership/result kinds rather than job-role constraints.
        if normalized in {"employee", "former_employee", "investor", "lead", "non_member"}:
            return False
        return normalized in set(ROLE_BUCKET_PRIORITY)
    if field == "must_have_facets":
        normalized = normalize_requested_facet(str(value or ""))
        # Investor is a population/involvement facet in the existing product,
        # not one of the selectable job-role buckets.
        return normalized in (set(ROLE_BUCKET_PRIORITY) - {"investor"})
    return False


def _selectable_role_specs() -> dict[str, dict[str, Any]]:
    return {
        str(role_id): dict(spec)
        for role_id, spec in ROLE_BUCKET_KNOWLEDGE.items()
        if str(spec.get("selectable_label") or "").strip()
        and isinstance(spec.get("selectable_order"), int)
        and not isinstance(spec.get("selectable_order"), bool)
    }


def _cohort_selection_registry_payload() -> dict[str, Any]:
    role_options: list[dict[str, Any]] = []
    for role_id, spec in ROLE_BUCKET_KNOWLEDGE.items():
        label = str(spec.get("selectable_label") or "").strip()
        order = spec.get("selectable_order")
        if not label or isinstance(order, bool) or not isinstance(order, int):
            continue
        role_options.append(
            {
                "id": str(role_id),
                "label": label,
                "order": order,
                "aliases": _registry_strings(spec.get("aliases")),
                "role_hints": _registry_strings(spec.get("role_hints")),
                "function_ids": _registry_strings(spec.get("function_ids")),
            }
        )
    role_options.sort(key=lambda item: (int(item["order"]), str(item["id"])))
    _assert_unique_option_contract(role_options, option_kind="role_bucket")
    return {
        "registry_version": COHORT_SELECTION_REGISTRY_VERSION,
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "role_buckets": role_options,
        "employment_statuses": [dict(item) for item in _EMPLOYMENT_STATUS_OPTIONS],
        "role_match_options": [dict(item) for item in _ROLE_MATCH_OPTIONS],
        "defaults": {"role_match": "any"},
    }


def _role_order() -> dict[str, int]:
    return {role_id: int(spec["selectable_order"]) for role_id, spec in _selectable_role_specs().items()}


def _registry_strings(value: Any) -> list[str]:
    return list(dict.fromkeys(str(item).strip() for item in list(value or []) if str(item).strip()))


def _normalize_strict_role_bucket_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise CohortSelectionValidationError(
            "cohort_selection_invalid_array",
            "cohort_selection.role_bucket_ids",
        )
    selectable = _selectable_role_specs()
    normalized: list[str] = []
    for item in value:
        role_id = str(item or "").strip().lower()
        if role_id not in selectable:
            raise CohortSelectionValidationError(
                "cohort_selection_unknown_role_bucket",
                "cohort_selection.role_bucket_ids",
                role_id,
            )
        if role_id in normalized:
            raise CohortSelectionValidationError(
                "cohort_selection_duplicate_value",
                "cohort_selection.role_bucket_ids",
                role_id,
            )
        normalized.append(role_id)
    order = _role_order()
    return sorted(normalized, key=lambda role_id: (order[role_id], role_id))


def _normalize_legacy_role_bucket_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = [item for item in value.replace("|", ",").replace("/", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raise CohortSelectionValidationError(
            "cohort_selection_invalid_array",
            "must_have_primary_role_buckets",
        )
    selectable = _selectable_role_specs()
    alias_lookup: dict[str, str] = {}
    for role_id, spec in selectable.items():
        for alias in (role_id, *tuple(spec.get("aliases") or ())):
            normalized_alias = _normalize_role_token(alias)
            if normalized_alias:
                alias_lookup.setdefault(normalized_alias, role_id)
    role_ids: list[str] = []
    for item in raw_items:
        token = _normalize_role_token(item)
        if not token:
            continue
        role_id = alias_lookup.get(token, "")
        if not role_id:
            raise CohortSelectionValidationError(
                "cohort_selection_unknown_role_bucket",
                "must_have_primary_role_buckets",
                str(item or "").strip(),
            )
        if role_id not in role_ids:
            role_ids.append(role_id)
    order = _role_order()
    return sorted(role_ids, key=lambda role_id: (order[role_id], role_id))


def _normalize_strict_employment_statuses(value: Any, *, allow_empty: bool) -> list[str]:
    field = "cohort_selection.employment_statuses"
    if not isinstance(value, list):
        raise CohortSelectionValidationError("cohort_selection_invalid_array", field)
    normalized: list[str] = []
    for item in value:
        status = str(item or "").strip().lower()
        if status not in _EMPLOYMENT_STATUS_ORDER:
            raise CohortSelectionValidationError(
                "cohort_selection_unknown_employment_status",
                field,
                status,
            )
        if status in normalized:
            raise CohortSelectionValidationError(
                "cohort_selection_duplicate_value",
                field,
                status,
            )
        normalized.append(status)
    if not normalized and not allow_empty:
        raise CohortSelectionValidationError(
            "cohort_selection_empty_employment_statuses",
            field,
        )
    return sorted(normalized, key=lambda status: (_EMPLOYMENT_STATUS_ORDER[status], status))


def _verify_or_install_flat_mirror(
    payload: dict[str, Any],
    *,
    canonical_key: str,
    aliases: tuple[str, ...],
    expected: list[str],
    normalizer: Any,
) -> None:
    present_keys = [key for key in (canonical_key, *aliases) if key in payload]
    if len(present_keys) > 1:
        raise CohortSelectionValidationError(
            "cohort_selection_mirror_conflict",
            present_keys[1],
            "duplicate mirror aliases are not allowed",
        )
    for key in present_keys:
        if payload.get(key) is None:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                key,
            )
        actual = list(normalizer(payload.get(key)))
        if actual != expected:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                key,
            )
    payload[canonical_key] = list(expected)
    for alias in aliases:
        payload.pop(alias, None)


def _verify_intent_axes_mirrors(
    payload: dict[str, Any],
    *,
    role_bucket_ids: list[str],
    employment_statuses: list[str],
    role_match: str,
) -> None:
    if "intent_axes" not in payload:
        return
    axes = payload.get("intent_axes")
    if not isinstance(axes, dict):
        raise CohortSelectionValidationError(
            "cohort_selection_mirror_conflict",
            "intent_axes",
        )
    population = axes.get("population_boundary")
    if "population_boundary" in axes and not isinstance(population, dict):
        raise CohortSelectionValidationError(
            "cohort_selection_mirror_conflict",
            "intent_axes.population_boundary",
        )
    if isinstance(population, dict) and "employment_statuses" in population:
        if population.get("employment_statuses") is None:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.population_boundary.employment_statuses",
            )
        actual_statuses = _normalize_strict_employment_statuses(
            population.get("employment_statuses"),
            allow_empty=True,
        )
        if actual_statuses != employment_statuses:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.population_boundary.employment_statuses",
            )
    thematic = axes.get("thematic_constraints")
    if "thematic_constraints" in axes and not isinstance(thematic, dict):
        raise CohortSelectionValidationError(
            "cohort_selection_mirror_conflict",
            "intent_axes.thematic_constraints",
        )
    if isinstance(thematic, dict) and "must_have_primary_role_buckets" in thematic:
        if thematic.get("must_have_primary_role_buckets") is None:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.thematic_constraints.must_have_primary_role_buckets",
            )
        actual_roles = _normalize_legacy_role_bucket_ids(thematic.get("must_have_primary_role_buckets"))
        if actual_roles != role_bucket_ids:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.thematic_constraints.must_have_primary_role_buckets",
            )
    if isinstance(thematic, dict) and "role_match" in thematic:
        if thematic.get("role_match") is None:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.thematic_constraints.role_match",
            )
        actual_role_match = str(thematic.get("role_match") or "").strip().lower()
        if actual_role_match != role_match:
            raise CohortSelectionValidationError(
                "cohort_selection_mirror_conflict",
                "intent_axes.thematic_constraints.role_match",
            )


def _restore_present_intent_axes_mirrors(payload: dict[str, Any], *, cohort: dict[str, Any]) -> None:
    axes = payload.get("intent_axes")
    if not isinstance(axes, dict):
        return
    axes = dict(axes)
    population = axes.get("population_boundary")
    if isinstance(population, dict):
        population = dict(population)
        population["employment_statuses"] = list(cohort["employment_statuses"])
        axes["population_boundary"] = population
    thematic = axes.get("thematic_constraints")
    if isinstance(thematic, dict):
        thematic = dict(thematic)
        thematic["must_have_primary_role_buckets"] = list(cohort["role_bucket_ids"])
        thematic["role_match"] = str(cohort["role_match"])
        axes["thematic_constraints"] = thematic
    payload["intent_axes"] = axes


def _normalize_role_token(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("-", " ").replace("_", " ").split())


def _assert_unique_option_contract(options: list[dict[str, Any]], *, option_kind: str) -> None:
    ids = [str(item.get("id") or "") for item in options]
    orders = [int(item.get("order") or 0) for item in options]
    if len(ids) != len(set(ids)) or len(orders) != len(set(orders)):
        raise RuntimeError(f"duplicate {option_kind} option id/order")


def _sha256_json(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
