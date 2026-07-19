from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from typing import Any

from .query_signal_knowledge import ROLE_BUCKET_KNOWLEDGE

EXCEL_INTAKE_CURRENT_JOB_MARKER_ID = "excel_intake:current_job"
EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL = "本次Excel导入"

# Provenance markers for the served per-candidate function-bucket projection.
# Consumers and audits must be able to tell membership-derived truth from
# inferred legacy display; the values are a closed enum.
FUNCTION_BUCKET_SOURCE_LANE_MEMBERSHIP = "lane_membership"
FUNCTION_BUCKET_SOURCE_REGISTRY_EVIDENCE = "registry_evidence"
FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE = "legacy_inference"
FUNCTION_BUCKET_SOURCES = (
    FUNCTION_BUCKET_SOURCE_LANE_MEMBERSHIP,
    FUNCTION_BUCKET_SOURCE_REGISTRY_EVIDENCE,
    FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE,
)

# TML canonical asset buckets that are NOT selectable registry roles
# (docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md; FT0 v2 §4.5).  They are
# structured ``other`` evidence — never legacy headline inference and never a
# silent alias into a selectable role.
_TML_ASSET_ONLY_ROLE_BUCKET_IDS: frozenset[str] = frozenset({"leadership", "ops", "investor"})

_COHORT_EMPLOYMENT_STATUSES: frozenset[str] = frozenset({"current", "former"})


class CohortFacetProvenanceError(ValueError):
    """Stable fail-closed error for malformed Cohort facet provenance.

    Raised by the presence-aware closed validator when server-owned Cohort
    provenance (lane membership, role/status mirrors, or the persisted served
    projection fields) is malformed or internally inconsistent.  Publication
    paths fail closed on this error instead of silently falling back to
    lower-authority evidence.
    """

    def __init__(self, code: str, field: str = "", detail: str = "") -> None:
        self.code = str(code or "cohort_facet_provenance_invalid")
        self.field = str(field or "")
        self.detail = str(detail or "")
        super().__init__(self.code)

    def __str__(self) -> str:
        return f"{self.code}: {self.field}" if self.field else self.code

# Documented result-only facet states.  ``other`` means the record carries
# role evidence that maps to no selectable registry role (e.g. an unmapped
# numeric function id or a non-registry asset bucket); ``unknown`` means the
# record carries no role evidence at all.  Neither is a selectable request
# value and neither may silently absorb a selectable role.
_RESULT_ONLY_FUNCTION_FACET_SPEC: tuple[tuple[str, str], ...] = (
    ("other", "其他"),
    ("unknown", "未提供职能信息"),
)


def _selectable_function_role_ids() -> list[str]:
    """Project the central selectable role registry in canonical order."""

    selectable: list[tuple[str, int]] = []
    for role_id, spec in ROLE_BUCKET_KNOWLEDGE.items():
        label = str(spec.get("selectable_label") or "").strip()
        order = spec.get("selectable_order")
        if not label or isinstance(order, bool) or not isinstance(order, int):
            continue
        selectable.append((str(role_id), order))
    selectable.sort(key=lambda item: (item[1], item[0]))
    return [role_id for role_id, _order in selectable]


def public_function_facet_option_spec() -> list[tuple[str, str]]:
    """Return the sole registry-derived function-facet option spec.

    Named roles project ``ROLE_BUCKET_KNOWLEDGE`` (the single taxonomy source
    of truth shared with the cohort-selection options endpoint) in selectable
    order with registry labels; ``other``/``unknown`` are appended as
    documented result-only states that are never selectable.  Every backend
    consumer — the facet summary, filter normalization, and the operation/
    projection filter enums — derives from this ONE helper; no second enum.
    """

    spec: list[tuple[str, str]] = [
        (role_id, str(ROLE_BUCKET_KNOWLEDGE[role_id]["selectable_label"]).strip())
        for role_id in _SELECTABLE_FUNCTION_ROLE_IDS
    ]
    spec.extend(_RESULT_ONLY_FUNCTION_FACET_SPEC)
    return spec


_SELECTABLE_FUNCTION_ROLE_IDS: tuple[str, ...] = tuple(_selectable_function_role_ids())
_SELECTABLE_FUNCTION_ROLE_ID_SET: frozenset[str] = frozenset(_SELECTABLE_FUNCTION_ROLE_IDS)
_FUNCTION_FACET_OPTION_IDS: tuple[str, ...] = tuple(item_id for item_id, _label in public_function_facet_option_spec())
_FUNCTION_FACET_OPTION_ID_SET: frozenset[str] = frozenset(_FUNCTION_FACET_OPTION_IDS)
_FUNCTION_FACET_ORDER: dict[str, int] = {item_id: index for index, item_id in enumerate(_FUNCTION_FACET_OPTION_IDS)}


def _function_id_owner_roles() -> dict[str, str]:
    """Map each registry function id to its first owner in registry order.

    ``engineering`` and ``infra_systems`` share function id ``"8"``; a bare id
    maps to ``engineering`` only (registry-order tie-break, documented).
    ``infra_systems`` is additionally attributed only from explicit
    role_bucket/lane evidence, never from the bare id.
    """

    owners: dict[str, str] = {}
    for role_id in _SELECTABLE_FUNCTION_ROLE_IDS:
        for function_id in ROLE_BUCKET_KNOWLEDGE[role_id].get("function_ids") or ():
            normalized = str(function_id or "").strip()
            if normalized and normalized not in owners:
                owners[normalized] = role_id
    return owners


_FUNCTION_ID_OWNER_ROLE: dict[str, str] = _function_id_owner_roles()


def _ordered_function_facet_ids(bucket_ids: Any) -> list[str]:
    unique = {str(item or "").strip() for item in bucket_ids if str(item or "").strip()}
    return sorted(unique, key=lambda item: (_FUNCTION_FACET_ORDER.get(item, len(_FUNCTION_FACET_ORDER)), item))


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def _normalized_keyword_key(value: str) -> str:
    return str(value or "").strip().lower()


def candidate_location_bucket_for_public_facets(record: dict[str, Any]) -> str:
    metadata = dict(record.get("metadata") or {})
    normalized = (
        str(
            record.get("profile_location")
            or record.get("location")
            or metadata.get("profile_location")
            or metadata.get("location")
            or ""
        )
        .strip()
        .lower()
    )
    if not normalized:
        return "unknown"
    non_us_signals = (
        "china",
        "beijing",
        "shanghai",
        "shenzhen",
        "guangzhou",
        "hong kong",
        "taiwan",
        "singapore",
        "tokyo",
        "japan",
        "seoul",
        "korea",
        "london",
        "united kingdom",
        "england",
        "paris",
        "france",
        "berlin",
        "germany",
        "toronto",
        "vancouver",
        "canada",
        "zurich",
        "switzerland",
        "sydney",
        "australia",
        "india",
        "bangalore",
        "bengaluru",
    )
    if any(signal in normalized for signal in non_us_signals):
        return "other"
    return "us"


def _validated_cohort_provenance(metadata: dict[str, Any]) -> dict[str, Any]:
    """Presence-aware closed validation of server-owned Cohort provenance.

    Returns ``{"has_provenance": bool, "role_bucket_ids": [...],
    "employment_statuses": [...]}``.  Any malformed provenance — wrong
    container/item types, unknown role ids, invalid statuses, an empty
    employment mirror (invalid at request time, FT0 §3.5), or lane/mirror
    disagreement when both exist — raises ``CohortFacetProvenanceError`` so
    facet/index publication fails closed instead of silently falling back to
    lower-authority evidence.  The legitimate all-roles case (status-only
    lanes carry a blank ``role_bucket_id`` and the role mirror is an empty
    list) is well-formed and simply yields no role evidence, passing the
    record through to the registry/legacy tiers.
    """

    has_membership = "cohort_lane_membership" in metadata
    has_role_mirror = "cohort_role_bucket_ids" in metadata
    has_status_mirror = "cohort_employment_statuses" in metadata
    if not has_membership and not has_role_mirror and not has_status_mirror:
        return {"has_provenance": False, "role_bucket_ids": [], "employment_statuses": []}

    lane_roles: list[str] = []
    lane_statuses: list[str] = []
    if has_membership:
        membership = metadata.get("cohort_lane_membership")
        if not isinstance(membership, list):
            raise CohortFacetProvenanceError(
                "cohort_facet_provenance_invalid_type",
                "cohort_lane_membership",
            )
        for item in membership:
            if not isinstance(item, dict):
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_invalid_item",
                    "cohort_lane_membership",
                )
            lane_id = item.get("lane_id")
            if not isinstance(lane_id, str) or not lane_id.strip():
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_invalid_lane",
                    "cohort_lane_membership",
                )
            status = item.get("employment_status")
            if not isinstance(status, str) or status.strip().lower() not in _COHORT_EMPLOYMENT_STATUSES:
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_invalid_employment_status",
                    "cohort_lane_membership",
                )
            normalized_status = status.strip().lower()
            if normalized_status not in lane_statuses:
                lane_statuses.append(normalized_status)
            role_id = item.get("role_bucket_id")
            if not isinstance(role_id, str):
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_invalid_role",
                    "cohort_lane_membership",
                )
            normalized_role = role_id.strip()
            if not normalized_role:
                continue  # status-only (all-roles) lane: no role evidence
            if normalized_role not in _SELECTABLE_FUNCTION_ROLE_ID_SET:
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_unknown_role",
                    "cohort_lane_membership",
                )
            if normalized_role not in lane_roles:
                lane_roles.append(normalized_role)

    mirror_roles: list[str] | None = None
    if has_role_mirror:
        raw_mirror = metadata.get("cohort_role_bucket_ids")
        if not isinstance(raw_mirror, list):
            raise CohortFacetProvenanceError(
                "cohort_facet_provenance_invalid_type",
                "cohort_role_bucket_ids",
            )
        mirror_roles = []
        for item in raw_mirror:
            if not isinstance(item, str) or item.strip() not in _SELECTABLE_FUNCTION_ROLE_ID_SET:
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_unknown_role",
                    "cohort_role_bucket_ids",
                )
            normalized_role = item.strip()
            if normalized_role not in mirror_roles:
                mirror_roles.append(normalized_role)

    mirror_statuses: list[str] | None = None
    if has_status_mirror:
        raw_statuses = metadata.get("cohort_employment_statuses")
        if not isinstance(raw_statuses, list):
            raise CohortFacetProvenanceError(
                "cohort_facet_provenance_invalid_type",
                "cohort_employment_statuses",
            )
        mirror_statuses = []
        for item in raw_statuses:
            if not isinstance(item, str) or item.strip().lower() not in _COHORT_EMPLOYMENT_STATUSES:
                raise CohortFacetProvenanceError(
                    "cohort_facet_provenance_invalid_employment_status",
                    "cohort_employment_statuses",
                )
            normalized_status = item.strip().lower()
            if normalized_status not in mirror_statuses:
                mirror_statuses.append(normalized_status)
        if not mirror_statuses:
            # An empty employment mirror is never legitimate: empty statuses
            # fail closed at request ingress (FT0 §3.5), so a present-empty
            # mirror on a served record is malformed provenance.
            raise CohortFacetProvenanceError(
                "cohort_facet_provenance_empty_employment_statuses",
                "cohort_employment_statuses",
            )

    if mirror_roles is not None and has_membership and set(mirror_roles) != set(lane_roles):
        raise CohortFacetProvenanceError(
            "cohort_facet_provenance_role_disagreement",
            "cohort_role_bucket_ids",
        )
    if mirror_statuses is not None and has_membership and set(mirror_statuses) != set(lane_statuses):
        raise CohortFacetProvenanceError(
            "cohort_facet_provenance_employment_disagreement",
            "cohort_employment_statuses",
        )

    return {
        "has_provenance": True,
        "role_bucket_ids": list(mirror_roles if mirror_roles is not None else lane_roles),
        "employment_statuses": list(mirror_statuses if mirror_statuses is not None else lane_statuses),
    }


def _persisted_function_bucket_projection(record: dict[str, Any]) -> dict[str, Any] | None:
    """Return the owned persisted projection pair when present, else None.

    The pair is a production contract field (FT0 §5.2): exactly one of the
    two keys, an empty/unknown id set, or an unknown source is malformed and
    fails closed instead of being silently re-derived.
    """

    has_ids = "function_bucket_ids" in record
    has_source = "function_bucket_source" in record
    if not has_ids and not has_source:
        return None
    if not has_ids or not has_source:
        raise CohortFacetProvenanceError(
            "cohort_facet_projection_incomplete",
            "function_bucket_ids" if not has_ids else "function_bucket_source",
        )
    raw_ids = record.get("function_bucket_ids")
    source = record.get("function_bucket_source")
    if (
        not isinstance(raw_ids, list)
        or not raw_ids
        or any(not isinstance(item, str) or item.strip() not in _FUNCTION_FACET_OPTION_ID_SET for item in raw_ids)
    ):
        raise CohortFacetProvenanceError("cohort_facet_projection_invalid_ids", "function_bucket_ids")
    if source not in FUNCTION_BUCKET_SOURCES:
        raise CohortFacetProvenanceError("cohort_facet_projection_invalid_source", "function_bucket_source")
    return {
        "function_bucket_ids": _ordered_function_facet_ids(raw_ids),
        "function_bucket_source": str(source),
    }


def candidate_function_bucket_projection_for_public_facets(record: dict[str, Any]) -> dict[str, Any]:
    """Return the served per-candidate function-bucket projection.

    Read-side consumers (facet counts, filters, projection members, index
    filter records) use the OWNED persisted ``function_bucket_ids`` /
    ``function_bucket_source`` pair when the record carries it; otherwise the
    projection is derived from evidence with exact precedence:

    1. ``lane_membership`` — Cohort-produced records carry server-derived
       ``metadata.cohort_lane_membership`` / ``metadata.cohort_role_bucket_ids``
       provenance; the candidate counts in EVERY qualifying role bucket.
    2. ``registry_evidence`` — registry-mappable function evidence: numeric
       ``function_ids`` mapped through the registry (bare ``"8"`` resolves to
       ``engineering`` only, registry-order tie-break; unmapped ids become
       ``other``) plus explicit registry ``role_bucket`` evidence, which is
       additionally how ``infra_systems``/``founding`` are attributed.  TML
       asset-only buckets (``leadership``/``ops``/``investor``) are structured
       ``other`` evidence in this tier, never legacy text inference.
    3. ``legacy_inference`` — the documented pre-FT1 text/``role_bucket``
       inference fallback, retained byte-for-byte for records with neither
       membership nor registry-mappable evidence (historical persisted
       candidates and legacy non-Cohort acquisitions).

    Malformed Cohort provenance or a malformed persisted pair raises
    ``CohortFacetProvenanceError`` (fail closed; no silent fallback).
    """

    persisted = _persisted_function_bucket_projection(record)
    if persisted is not None:
        return persisted
    return derive_function_bucket_projection_for_public_facets(record)


def derive_function_bucket_projection_for_public_facets(record: dict[str, Any]) -> dict[str, Any]:
    """Derive the projection from record evidence only (build/repair side).

    Unlike the read-side entry point this never consumes the persisted pair,
    so build and repair paths always recompute the canonical values from
    provenance/evidence and detect stale or missing persisted fields.
    """

    metadata = dict(record.get("metadata") or {})

    provenance = _validated_cohort_provenance(metadata)
    membership_roles = list(provenance["role_bucket_ids"])
    if membership_roles:
        return {
            "function_bucket_ids": _ordered_function_facet_ids(membership_roles),
            "function_bucket_source": FUNCTION_BUCKET_SOURCE_LANE_MEMBERSHIP,
        }

    evidence: set[str] = set()
    function_ids = [
        str(item or "").strip()
        for item in list(record.get("function_ids") or metadata.get("function_ids") or [])
        if str(item or "").strip()
    ]
    for function_id in function_ids:
        owner_role = _FUNCTION_ID_OWNER_ROLE.get(function_id)
        evidence.add(owner_role if owner_role else "other")
    role_bucket = str(record.get("role_bucket") or metadata.get("role_bucket") or "").strip().lower()
    if role_bucket in _SELECTABLE_FUNCTION_ROLE_ID_SET:
        evidence.add(role_bucket)
    elif role_bucket in _TML_ASSET_ONLY_ROLE_BUCKET_IDS:
        # TML asset-only buckets are structured ``other`` evidence (FT0 §4.5):
        # explicit asset-only evidence must not fall through to headline
        # inference, where conflicting text could convert it into a named role.
        evidence.add("other")
    if evidence:
        return {
            "function_bucket_ids": _ordered_function_facet_ids(evidence),
            "function_bucket_source": FUNCTION_BUCKET_SOURCE_REGISTRY_EVIDENCE,
        }

    return {
        "function_bucket_ids": _legacy_inferred_function_buckets(record),
        "function_bucket_source": FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE,
    }


def candidate_function_buckets_for_public_facets(record: dict[str, Any]) -> list[str]:
    return list(candidate_function_bucket_projection_for_public_facets(record)["function_bucket_ids"])


def _legacy_inferred_function_buckets(record: dict[str, Any]) -> list[str]:
    """Documented pre-FT1 fallback, retained byte-for-byte (FT0 §5.3).

    Deletion condition: a projection-regeneration preflight shows zero served
    rows with ``function_bucket_source="legacy_inference"`` across one full
    materialization regeneration window.
    """

    metadata = dict(record.get("metadata") or {})
    function_ids = [
        str(item or "").strip()
        for item in list(record.get("function_ids") or metadata.get("function_ids") or [])
        if str(item or "").strip()
    ]
    if len(function_ids) == 1:
        buckets: set[str] = set()
        if "24" in function_ids:
            buckets.add("research")
        if "8" in function_ids:
            buckets.add("engineering")
        if "19" in function_ids:
            buckets.add("product_management")
        if any(item not in {"24", "8", "19"} for item in function_ids):
            buckets.add("other")
        return sorted(buckets or {"other"})

    role_bucket = str(record.get("role_bucket") or metadata.get("role_bucket") or "").strip().lower()
    role_mapping = {
        "research": "research",
        "engineering": "engineering",
        "infra_systems": "engineering",
        "product_management": "product_management",
    }
    if role_bucket in role_mapping:
        return [role_mapping[role_bucket]]

    corpus = " ".join(
        str(value or "")
        for value in (
            record.get("headline"),
            record.get("summary"),
            record.get("role"),
            record.get("team"),
            metadata.get("headline"),
            metadata.get("summary"),
            metadata.get("role"),
        )
    ).lower()
    if any(token in corpus for token in ("research scientist", "research engineer", "researcher", "scientist")):
        return ["research"]
    if any(
        token in corpus
        for token in (
            "software engineer",
            "machine learning engineer",
            "systems engineer",
            "platform engineer",
            "engineer",
            "engineering",
            "infrastructure",
            "backend",
            "frontend",
            "developer",
        )
    ):
        return ["engineering"]
    if "product manager" in corpus or "product management" in corpus:
        return ["product_management"]
    return ["unknown"]


def candidate_employment_statuses_for_public_facets(record: dict[str, Any]) -> list[str]:
    """Return the authoritative employment status set for facet counts/filters.

    Read-side consumers use the OWNED persisted ``employment_statuses`` set
    when the record carries it (written by the backend projection build).
    Otherwise the set is derived from the ONLY authoritative employment
    provenance for Cohort-produced records — ``metadata.cohort_lane_membership``
    / ``metadata.cohort_employment_statuses`` — so a candidate qualifying in
    both current and former lanes is counted and filterable under BOTH even
    though the card's top-level ``employment_status`` keeps its frozen
    display-only derivation.  Returns [] when no membership provenance exists
    (legacy records), so callers fall back to the documented
    top-level/``lead`` semantics.  Malformed provenance or a malformed
    persisted set raises ``CohortFacetProvenanceError`` (fail closed).
    """

    if "employment_statuses" in record:
        persisted = record.get("employment_statuses")
        if (
            not isinstance(persisted, list)
            or not persisted
            or any(not isinstance(item, str) or item not in _COHORT_EMPLOYMENT_STATUSES for item in persisted)
        ):
            raise CohortFacetProvenanceError(
                "cohort_facet_employment_projection_invalid",
                "employment_statuses",
            )
        return list(dict.fromkeys(persisted))
    return derive_candidate_employment_statuses_for_public_facets(record)


def derive_candidate_employment_statuses_for_public_facets(record: dict[str, Any]) -> list[str]:
    """Derive the authoritative status set from Cohort provenance only.

    Build/repair paths use this so persisted sets are always recomputed from
    provenance; legacy records without provenance return [].
    """

    metadata = dict(record.get("metadata") or {})
    provenance = _validated_cohort_provenance(metadata)
    return list(provenance["employment_statuses"])


def candidate_recall_keywords_for_public_facets(record: dict[str, Any]) -> list[str]:
    metadata = dict(record.get("metadata") or {})
    values: list[str] = []
    for source in (
        record.get("matched_keywords"),
        metadata.get("matched_keywords"),
        record.get("scope_keywords"),
        metadata.get("scope_keywords"),
    ):
        values.extend(str(item or "").strip() for item in list(source or []) if str(item or "").strip())
    for key in ("source_query", "seed_query", "query", "matched_on", "keyword"):
        for source in (record, metadata):
            value = str(dict(source or {}).get(key) or "").strip()
            if value:
                values.append(value)
    for source_match in list(record.get("source_matches") or metadata.get("source_matches") or []):
        if not isinstance(source_match, dict):
            continue
        for key in ("matched_on", "keyword"):
            value = str(source_match.get(key) or "").strip()
            if value:
                values.append(value)
        values.extend(
            str(item or "").strip()
            for item in list(source_match.get("matched_keywords") or [])
            if str(item or "").strip()
        )
    return _dedupe_texts(values)


def public_facet_counts_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    employment_counts: Counter[str] = Counter()
    location_counts: Counter[str] = Counter()
    function_counts: Counter[str] = Counter()
    layer_counts: Counter[str] = Counter()
    recall_keyword_counts: Counter[str] = Counter()
    recall_keyword_labels: dict[str, str] = {}
    marker_counts: Counter[str] = Counter()
    has_layer_metadata = False

    candidate_count = 0
    for source_record in list(records or []):
        if not isinstance(source_record, dict):
            continue
        candidate_count += 1
        record = dict(source_record)
        metadata = dict(record.get("metadata") or {})
        membership_statuses = candidate_employment_statuses_for_public_facets(record)
        if membership_statuses:
            for membership_status in membership_statuses:
                employment_counts[membership_status] += 1
        else:
            status = str(record.get("employment_status") or metadata.get("employment_status") or "").strip().lower()
            employment_counts[status if status in {"current", "former"} else "lead"] += 1
        location_counts[candidate_location_bucket_for_public_facets(record)] += 1
        for bucket in candidate_function_buckets_for_public_facets(record):
            function_counts[bucket] += 1

        layer_value = record.get("outreach_layer")
        if layer_value in (None, ""):
            layer_value = metadata.get("outreach_layer")
        try:
            parsed_layer = int(layer_value)
        except (TypeError, ValueError):
            parsed_layer = -1
        if parsed_layer >= 0:
            has_layer_metadata = True
            layer_counts[f"layer_{parsed_layer}"] += 1

        for keyword in candidate_recall_keywords_for_public_facets(record):
            key = _normalized_keyword_key(keyword)
            if not key:
                continue
            recall_keyword_counts[key] += 1
            recall_keyword_labels.setdefault(key, keyword)

        for source_match in list(record.get("source_matches") or metadata.get("source_matches") or []):
            if not isinstance(source_match, dict):
                continue
            source_type = str(source_match.get("source_type") or source_match.get("marker_id") or "").strip()
            matched_on = str(source_match.get("matched_on") or "").strip()
            if source_type == EXCEL_INTAKE_CURRENT_JOB_MARKER_ID or matched_on == EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL:
                marker_counts[EXCEL_INTAKE_CURRENT_JOB_MARKER_ID] += 1

    return {
        "schema_version": 1,
        "candidate_count": candidate_count,
        "employment_counts": dict(employment_counts),
        "location_counts": dict(location_counts),
        "function_counts": dict(function_counts),
        "layer_counts": dict(layer_counts),
        "has_layer_metadata": has_layer_metadata,
        "recall_keyword_counts": dict(recall_keyword_counts),
        "recall_keyword_labels": recall_keyword_labels,
        "marker_counts": dict(marker_counts),
    }


def _facet_options(
    spec: list[tuple[str, str]],
    counts: dict[str, Any],
    *,
    keep_zero_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    keep_zero_ids = keep_zero_ids or set()
    options: list[dict[str, Any]] = []
    for item_id, label in spec:
        count = max(0, int(counts.get(item_id) or 0))
        if count > 0 or item_id in keep_zero_ids:
            options.append({"id": item_id, "label": label, "count": count})
    return options


def public_facet_summary_from_counts(
    counts: dict[str, Any],
    *,
    intent_keywords: list[str] | None = None,
    include_empty: bool = False,
) -> dict[str, Any]:
    source = dict(counts or {})
    candidate_count = max(0, int(source.get("candidate_count") or 0))
    if candidate_count <= 0 and not include_empty:
        return {}

    normalized_intent_keywords = _dedupe_texts(
        [str(item or "").strip() for item in list(intent_keywords or []) if str(item or "").strip()]
    )
    recall_keyword_counts = {
        str(key or "").strip().lower(): max(0, int(value or 0))
        for key, value in dict(source.get("recall_keyword_counts") or {}).items()
    }
    recall_options = [{"id": "all", "label": "全量", "count": candidate_count}]
    for keyword in normalized_intent_keywords:
        key = _normalized_keyword_key(keyword)
        recall_options.append(
            {
                "id": f"keyword:{key}",
                "label": keyword,
                "count": int(recall_keyword_counts.get(key) or 0),
            }
        )

    marker_counts = dict(source.get("marker_counts") or {})
    excel_count = max(0, int(marker_counts.get(EXCEL_INTAKE_CURRENT_JOB_MARKER_ID) or 0))
    if excel_count > 0:
        recall_options.append(
            {
                "id": f"job_scoped_marker:{EXCEL_INTAKE_CURRENT_JOB_MARKER_ID}",
                "label": EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL,
                "count": excel_count,
            }
        )

    layer_counts = {
        str(key or "").strip(): max(0, int(value or 0))
        for key, value in dict(source.get("layer_counts") or {}).items()
    }
    has_layer_metadata = bool(source.get("has_layer_metadata"))
    if has_layer_metadata:
        layers = [
            {
                "id": f"layer_{layer}",
                "label": f"Layer {layer}",
                "count": candidate_count if layer == 0 else int(layer_counts.get(f"layer_{layer}") or 0),
            }
            for layer in range(4)
        ]
    else:
        layers = [{"id": f"layer_{layer}", "label": f"Layer {layer}", "count": 0} for layer in range(4)]

    return {
        "schema_version": 1,
        "candidate_count": candidate_count,
        "layers": layers,
        "recall": recall_options,
        "employment": _facet_options(
            [("current", "在职"), ("former", "已离职")],
            dict(source.get("employment_counts") or {}),
            keep_zero_ids={"current", "former"},
        ),
        "locations": _facet_options(
            [("us", "美国"), ("other", "其他"), ("unknown", "未提供地区信息")],
            dict(source.get("location_counts") or {}),
            keep_zero_ids={"us", "other"},
        ),
        "functions": _facet_options(
            public_function_facet_option_spec(),
            dict(source.get("function_counts") or {}),
        ),
    }


def public_facet_summary_from_records(
    records: list[dict[str, Any]],
    *,
    intent_keywords: list[str] | None = None,
) -> dict[str, Any]:
    return public_facet_summary_from_counts(
        public_facet_counts_from_records(records),
        intent_keywords=intent_keywords,
    )


def candidate_page_filter_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", str(value or "").lower())).strip()


def normalize_candidate_page_filter_values(
    values: Any,
    *,
    allowed_values: set[str] | None = None,
    allowed_prefixes: tuple[str, ...] = (),
    lowercase: bool = True,
) -> list[str]:
    normalized_values: list[str] = []
    raw_values = values if isinstance(values, list) else [values]
    for raw_value in raw_values:
        for item in str(raw_value or "").split(","):
            normalized = item.strip()
            if not normalized:
                continue
            normalized = normalized.lower() if lowercase else normalized
            if allowed_values is not None and normalized not in allowed_values:
                if not any(normalized.startswith(prefix) for prefix in allowed_prefixes):
                    continue
            if normalized not in normalized_values:
                normalized_values.append(normalized)
    return normalized_values


def normalize_candidate_page_recall_filter_values(values: Any) -> list[str]:
    normalized_values: list[str] = []
    for item in normalize_candidate_page_filter_values(values, lowercase=False):
        raw_value = str(item or "").strip()
        if not raw_value:
            continue
        lowered = raw_value.lower()
        if lowered == "all":
            normalized = "all"
        elif lowered == f"job_scoped_marker:{EXCEL_INTAKE_CURRENT_JOB_MARKER_ID}":
            normalized = f"job_scoped_marker:{EXCEL_INTAKE_CURRENT_JOB_MARKER_ID}"
        elif raw_value == EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL:
            normalized = f"job_scoped_marker:{EXCEL_INTAKE_CURRENT_JOB_MARKER_ID}"
        elif lowered.startswith("keyword:"):
            keyword = raw_value.split(":", 1)[1].strip()
            if not keyword:
                continue
            normalized = f"keyword:{keyword.lower()}"
        else:
            normalized = f"keyword:{raw_value.lower()}"
        if normalized not in normalized_values:
            normalized_values.append(normalized)
    return normalized_values


def normalize_candidate_page_filter(candidate_filter: dict[str, Any] | None) -> dict[str, Any]:
    source = dict(candidate_filter or {})
    valid_layer_ids = {f"layer_{index}" for index in range(8)}
    return {
        "search_keyword": str(source.get("search_keyword") or source.get("search") or "").strip()[:200],
        "recall_buckets": normalize_candidate_page_recall_filter_values(source.get("recall_buckets")),
        "employment_statuses": normalize_candidate_page_filter_values(
            source.get("employment_statuses"),
            allowed_values={"current", "former"},
        ),
        "locations": normalize_candidate_page_filter_values(
            source.get("locations"),
            allowed_values={"us", "other", "unknown"},
        ),
        "function_buckets": normalize_candidate_page_filter_values(
            source.get("function_buckets"),
            allowed_values=_FUNCTION_FACET_OPTION_ID_SET,
        ),
        "layer_includes": normalize_candidate_page_filter_values(
            source.get("layer_includes") or source.get("layers"),
            allowed_values=valid_layer_ids,
        ),
        "layer_excludes": normalize_candidate_page_filter_values(
            source.get("layer_excludes"),
            allowed_values=valid_layer_ids,
        ),
        "audit_statuses": normalize_candidate_page_filter_values(
            source.get("audit_statuses"),
            allowed_values={
                "no_review_needed",
                "needs_review",
                "needs_profile_completion",
                "low_profile_richness",
                "verified_keep",
                "verified_exclude",
            },
        ),
    }


def candidate_page_filter_active(candidate_filter: dict[str, Any]) -> bool:
    source = dict(candidate_filter or {})
    if str(source.get("search_keyword") or "").strip():
        return True
    employment_statuses = {
        str(item or "").strip()
        for item in list(source.get("employment_statuses") or [])
        if str(item or "").strip()
    }
    if employment_statuses and employment_statuses != {"current", "former"}:
        return True
    locations = {
        str(item or "").strip()
        for item in list(source.get("locations") or [])
        if str(item or "").strip()
    }
    if locations and locations != {"us", "other", "unknown"}:
        return True
    function_buckets = {
        str(item or "").strip()
        for item in list(source.get("function_buckets") or [])
        if str(item or "").strip()
    }
    # ``other``/``unknown`` are documented result-only states and are never
    # selectable request values.  The inactive (all-roles) no-op is any set
    # that covers every selectable role id — with or without the result-only
    # ids — while any proper subset is an active exclusion.
    if function_buckets and not _SELECTABLE_FUNCTION_ROLE_ID_SET.issubset(function_buckets):
        return True
    layer_includes = [str(item or "").strip() for item in list(source.get("layer_includes") or [])]
    layer_excludes = [str(item or "").strip() for item in list(source.get("layer_excludes") or [])]
    if any(item and item != "layer_0" for item in layer_includes) or any(layer_excludes):
        return True
    recall_buckets = [str(item or "").strip() for item in list(source.get("recall_buckets") or [])]
    if any(item and item != "all" for item in recall_buckets):
        return True
    audit_statuses = [str(item or "").strip() for item in list(source.get("audit_statuses") or [])]
    if audit_statuses and set(audit_statuses) != {
        "no_review_needed",
        "needs_review",
        "needs_profile_completion",
        "low_profile_richness",
        "verified_keep",
        "verified_exclude",
    }:
        return True
    return False


def candidate_page_filter_signature(candidate_filter: dict[str, Any]) -> str:
    payload = {
        key: value
        for key, value in dict(candidate_filter or {}).items()
        if value not in ("", [], {}, None)
    }
    if not payload:
        return ""
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def candidate_page_filter_contract(
    *,
    applied_filter: dict[str, Any],
    filter_signature: str,
    source: str = "backend_candidates_endpoint",
    row_filter_scope: str = "backend_filtered_served_population",
) -> dict[str, Any]:
    return {
        "source": source,
        "facet_count_scope": "global_full_population",
        "row_filter_scope": row_filter_scope,
        "backend_filtered_paging_supported": True,
        "filter_signature": filter_signature,
        "filter_active": candidate_page_filter_active(applied_filter),
    }


def candidate_page_filter_corpus(record: dict[str, Any]) -> str:
    metadata = dict(record.get("metadata") or {})
    values: list[str] = []
    for key in (
        "display_name",
        "name",
        "name_en",
        "name_zh",
        "full_name",
        "headline",
        "summary",
        "role",
        "team",
        "organization",
        "profile_location",
        "location",
        "current_company",
        "notes",
    ):
        values.append(str(record.get(key) or metadata.get(key) or ""))
    for key in (
        "focus_areas",
        "match_reasons",
        "education_lines",
        "experience_lines",
        "education",
        "work_history",
        "matched_keywords",
    ):
        source = record.get(key) or metadata.get(key) or []
        if isinstance(source, list):
            values.extend(str(item or "") for item in source)
        else:
            values.append(str(source or ""))
    for source_match in list(record.get("source_matches") or metadata.get("source_matches") or []):
        if not isinstance(source_match, dict):
            continue
        values.append(str(source_match.get("matched_on") or source_match.get("keyword") or ""))
        values.extend(str(item or "") for item in list(source_match.get("matched_keywords") or []))
    return candidate_page_filter_text(" ".join(values))


def candidate_page_filter_layer_id(record: dict[str, Any]) -> str:
    metadata = dict(record.get("metadata") or {})
    layer_value = record.get("outreach_layer")
    if layer_value in (None, ""):
        layer_value = metadata.get("outreach_layer")
    try:
        parsed_layer = int(layer_value)
    except (TypeError, ValueError):
        return ""
    return f"layer_{parsed_layer}" if parsed_layer >= 0 else ""


def candidate_auto_review_status_for_filter(record: dict[str, Any]) -> str:
    if bool(record.get("needs_profile_completion")):
        return "needs_profile_completion"
    if bool(record.get("low_profile_richness")):
        return "low_profile_richness"
    return "no_review_needed"


def candidate_matches_candidate_page_filter(
    *,
    record: dict[str, Any],
    candidate_filter: dict[str, Any],
    review_status_lookup: dict[str, str] | None = None,
) -> bool:
    review_status_lookup = review_status_lookup or {}
    search_keyword = str(candidate_filter.get("search_keyword") or "").strip()
    corpus = ""
    if search_keyword:
        corpus = candidate_page_filter_corpus(record)
        if candidate_page_filter_text(search_keyword) not in corpus:
            return False

    selected_recall = [str(item or "").strip() for item in list(candidate_filter.get("recall_buckets") or [])]
    if selected_recall and "all" not in selected_recall:
        recall_keywords = {
            candidate_page_filter_text(item)
            for item in candidate_recall_keywords_for_public_facets(record)
            if str(item or "").strip()
        }
        if not corpus:
            corpus = candidate_page_filter_corpus(record)
        recall_matched = False
        for bucket in selected_recall:
            if bucket == f"job_scoped_marker:{EXCEL_INTAKE_CURRENT_JOB_MARKER_ID}":
                if candidate_page_filter_text(EXCEL_INTAKE_CURRENT_JOB_MARKER_LABEL) in recall_keywords:
                    recall_matched = True
                    break
                for source_match in list(record.get("source_matches") or dict(record.get("metadata") or {}).get("source_matches") or []):
                    if not isinstance(source_match, dict):
                        continue
                    source_type = str(source_match.get("source_type") or source_match.get("marker_id") or "").strip()
                    if source_type == EXCEL_INTAKE_CURRENT_JOB_MARKER_ID:
                        recall_matched = True
                        break
                if recall_matched:
                    break
                continue
            if not bucket.startswith("keyword:"):
                continue
            keyword = bucket.split(":", 1)[1]
            normalized_keyword = candidate_page_filter_text(keyword)
            if normalized_keyword and (normalized_keyword in recall_keywords or normalized_keyword in corpus):
                recall_matched = True
                break
        if not recall_matched:
            return False

    selected_employment = set(candidate_filter.get("employment_statuses") or [])
    if selected_employment:
        membership_statuses = candidate_employment_statuses_for_public_facets(record)
        if membership_statuses:
            if not set(membership_statuses).intersection(selected_employment):
                return False
        else:
            metadata = dict(record.get("metadata") or {})
            status = str(record.get("employment_status") or metadata.get("employment_status") or "").strip().lower()
            status = status if status in {"current", "former"} else "lead"
            if status == "lead":
                if not {"current", "former"}.issubset(selected_employment):
                    return False
            elif status not in selected_employment:
                return False

    selected_locations = set(candidate_filter.get("locations") or [])
    if selected_locations and candidate_location_bucket_for_public_facets(record) not in selected_locations:
        return False

    selected_functions = set(candidate_filter.get("function_buckets") or [])
    if selected_functions and not selected_functions.intersection(
        candidate_function_buckets_for_public_facets(record)
    ):
        return False

    layer_id = candidate_page_filter_layer_id(record)
    layer_excludes = set(candidate_filter.get("layer_excludes") or [])
    if layer_id and layer_id in layer_excludes:
        return False
    layer_includes = set(candidate_filter.get("layer_includes") or [])
    exact_layer_includes = {item for item in layer_includes if item != "layer_0"}
    if exact_layer_includes and (not layer_id or layer_id not in exact_layer_includes):
        return False

    selected_audit = set(candidate_filter.get("audit_statuses") or [])
    if selected_audit:
        candidate_id = str(record.get("candidate_id") or record.get("id") or "").strip()
        review_status = (
            review_status_lookup.get(candidate_id)
            or candidate_auto_review_status_for_filter(record)
            or "no_review_needed"
        )
        if review_status not in selected_audit:
            return False

    return True


def apply_candidate_page_filter(
    *,
    candidates: list[dict[str, Any]],
    candidate_filter: dict[str, Any],
    review_status_lookup: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not candidate_page_filter_active(candidate_filter):
        return list(candidates)
    return [
        dict(candidate)
        for candidate in candidates
        if candidate_matches_candidate_page_filter(
            record=dict(candidate),
            candidate_filter=candidate_filter,
            review_status_lookup=review_status_lookup or {},
        )
    ]
