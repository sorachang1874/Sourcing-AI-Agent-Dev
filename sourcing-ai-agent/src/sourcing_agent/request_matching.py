from __future__ import annotations

import json
from hashlib import sha1
from typing import Any

from .cohort_selection import CohortSelectionValidationError, cohort_execution_identity_for_signature
from .domain import _normalize_location_list, normalize_requested_facets, normalize_requested_role_buckets
from .request_normalization import (
    build_effective_request_payload,
    canonicalize_request_payload,
    has_structured_request_signals,
    materialize_request_payload,
    supplement_request_query_signals,
)

MATCH_THRESHOLD = 30.0
_SCALAR_MATCH_FIELDS = [
    ("asset_view", 12.0),
    ("target_scope", 12.0),
    ("retrieval_strategy", 8.0),
]
_LIST_MATCH_FIELDS = [
    ("categories", 20.0),
    ("employment_statuses", 14.0),
    ("keywords", 30.0),
    ("must_have_facets", 12.0),
    ("must_have_primary_role_buckets", 14.0),
    ("must_have_keywords", 8.0),
    ("exclude_keywords", 4.0),
    ("organization_keywords", 10.0),
]


def build_request_matching_bundle(payload: dict[str, Any]) -> dict[str, Any]:
    effective_payload = _prepared_effective_request_payload(dict(payload or {}))
    matching_request = _normalized_effective_request_payload(
        effective_payload,
        include_runtime_limits=True,
    )
    matching_family_request = _normalized_effective_request_payload(
        effective_payload,
        include_runtime_limits=False,
    )
    return {
        "effective_request": effective_payload,
        "matching_request": matching_request,
        "matching_family_request": matching_family_request,
        "matching_request_signature": _signature_for_payload(matching_request),
        "matching_request_family_signature": _signature_for_payload(matching_family_request),
    }


def matching_request_signature(payload: dict[str, Any]) -> str:
    return str(build_request_matching_bundle(payload).get("matching_request_signature") or "")


def matching_request_family_signature(payload: dict[str, Any]) -> str:
    return str(build_request_matching_bundle(payload).get("matching_request_family_signature") or "")


def request_signature(payload: dict[str, Any]) -> str:
    normalized = _normalized_request_payload(payload, include_runtime_limits=True)
    serialized = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


def request_family_signature(payload: dict[str, Any]) -> str:
    normalized = _normalized_request_payload(payload, include_runtime_limits=False)
    serialized = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


def matching_bundle_payload(
    request_payload: dict[str, Any],
    *,
    execution_bundle_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Moved verbatim from storage._matching_bundle_payload (Track B ②.1).
    # FT1-FF3: the returned bundle is ALWAYS the complete canonical
    # regeneration.  A persisted bundle is never a truth source: earlier
    # revisions trusted it after comparing only the two normalized matching
    # payloads, so a bundle carrying a contradictory ``effective_request``
    # (or stale normalized fields/signatures) was returned unchanged, leaving
    # one bundle with contradictory request truth.  No auxiliary fields are
    # documented for this bundle, so there is nothing worth preserving — the
    # persisted payload (if any) is validated implicitly by regeneration and
    # never returned.
    return build_request_matching_bundle(request_payload)


def request_signature_context(
    request_payload: dict[str, Any],
    *,
    execution_bundle_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Moved verbatim from storage._request_signature_context (Track B ②.1).
    normalized_request = dict(request_payload or {})
    if not normalized_request:
        return {
            "request_signature": "",
            "request_family_signature": "",
            "matching_request_signature": "",
            "matching_request_family_signature": "",
            "request_matching": {},
        }
    matching_bundle = matching_bundle_payload(
        normalized_request,
        execution_bundle_payload=execution_bundle_payload,
    )
    return {
        "request_signature": request_signature(normalized_request),
        "request_family_signature": request_family_signature(normalized_request),
        "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
        "matching_request_family_signature": str(matching_bundle.get("matching_request_family_signature") or ""),
        "request_matching": matching_bundle,
    }


def request_family_score(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    left_bundle: dict[str, Any] | None = None,
    right_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        left_matching_bundle = _coerce_matching_bundle(left, left_bundle)
        right_matching_bundle = _coerce_matching_bundle(right, right_bundle)
    except CohortSelectionValidationError as exc:
        left_norm = _invalid_cohort_matching_payload(left, side="left")
        right_norm = _invalid_cohort_matching_payload(right, side="right")
        invalid_reason = str(getattr(exc, "code", "") or "").strip()
        if not invalid_reason.startswith("request_location_"):
            invalid_reason = "cohort_selection_invalid"
        result = {
            "score": 0.0,
            "hard_family_mismatch": True,
            "exact_request_match": False,
            "exact_family_match": False,
            "family_signature_left": "",
            "family_signature_right": "",
            "reasons": [invalid_reason],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result
    left_norm = dict(left_matching_bundle.get("matching_family_request") or {})
    right_norm = dict(right_matching_bundle.get("matching_family_request") or {})
    left_request_signature = str(left_matching_bundle.get("matching_request_signature") or "")
    right_request_signature = str(right_matching_bundle.get("matching_request_signature") or "")
    left_request_family_signature = str(left_matching_bundle.get("matching_request_family_signature") or "")
    right_request_family_signature = str(right_matching_bundle.get("matching_request_family_signature") or "")
    if left_norm["target_company"] != right_norm["target_company"]:
        result = {
            "score": 0.0,
            "exact_request_match": False,
            "exact_family_match": False,
            "family_signature_left": left_request_family_signature,
            "family_signature_right": right_request_family_signature,
            "reasons": ["target_company_mismatch"],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result

    left_cohort_identity = str(left_norm.get("cohort_selection_digest") or "")
    right_cohort_identity = str(right_norm.get("cohort_selection_digest") or "")
    if left_cohort_identity != right_cohort_identity:
        result = {
            "score": 0.0,
            "hard_family_mismatch": True,
            "exact_request_match": False,
            "exact_family_match": False,
            "family_signature_left": left_request_family_signature,
            "family_signature_right": right_request_family_signature,
            "reasons": ["cohort_selection_identity_mismatch"],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result

    # Location is a HARD request-family boundary: canonical presence and values
    # of both sibling fields must match before any similarity scoring.  Any
    # mismatch — including absent versus an explicit empty list — scores zero
    # and can never reuse a snapshot, baseline, or feedback family.
    if _location_request_identity(left_norm) != _location_request_identity(right_norm):
        result = {
            "score": 0.0,
            "hard_family_mismatch": True,
            "exact_request_match": False,
            "exact_family_match": False,
            "family_signature_left": left_request_family_signature,
            "family_signature_right": right_request_family_signature,
            "reasons": ["location_identity_mismatch"],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result

    exact_request_match = bool(left_request_signature) and left_request_signature == right_request_signature
    exact_family_match = (
        bool(left_request_family_signature) and left_request_family_signature == right_request_family_signature
    )
    if exact_request_match:
        result = {
            "score": 100.0,
            "exact_request_match": True,
            "exact_family_match": True,
            "family_signature_left": left_request_family_signature,
            "family_signature_right": right_request_family_signature,
            "reasons": ["exact_request_match"],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result
    if exact_family_match:
        result = {
            "score": 95.0,
            "exact_request_match": False,
            "exact_family_match": True,
            "family_signature_left": left_request_family_signature,
            "family_signature_right": right_request_family_signature,
            "reasons": ["exact_family_match"],
            "matching_request_left": left_norm,
            "matching_request_right": right_norm,
        }
        result["explanation"] = build_request_family_match_explanation(left, right, match=result)
        return result

    score = 0.0
    reasons: list[str] = []
    for field, weight in _SCALAR_MATCH_FIELDS:
        if left_norm[field] and left_norm[field] == right_norm[field]:
            score += weight
            reasons.append(f"{field}_match")
        elif left_norm[field] and right_norm[field]:
            score -= min(8.0, weight * 0.5)
            reasons.append(f"{field}_mismatch")

    for field, weight in _LIST_MATCH_FIELDS:
        ratio = _overlap_ratio(left_norm[field], right_norm[field])
        if ratio > 0:
            score += weight * ratio
            reasons.append(f"{field}_overlap={round(ratio, 2)}")
        elif left_norm[field] and right_norm[field]:
            score -= min(6.0, weight * 0.25)
            reasons.append(f"{field}_disjoint")

    if score < 0:
        score = 0.0
    result = {
        "score": round(score, 2),
        "exact_request_match": False,
        "exact_family_match": False,
        "family_signature_left": left_request_family_signature,
        "family_signature_right": right_request_family_signature,
        "reasons": reasons,
        "matching_request_left": left_norm,
        "matching_request_right": right_norm,
    }
    result["explanation"] = build_request_family_match_explanation(left, right, match=result)
    return result


def _invalid_cohort_matching_payload(payload: dict[str, Any], *, side: str) -> dict[str, Any]:
    normalized = {
        "target_company": _normalize_scalar(payload.get("target_company")),
    }
    if "cohort_selection" in payload:
        normalized["cohort_selection_digest"] = f"__invalid_cohort_{side}__"
    return normalized


def build_request_family_match_explanation(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    match: dict[str, Any] | None = None,
    selection_mode: str = "request_family_score",
) -> dict[str, Any]:
    left_norm = dict(
        (match or {}).get("matching_request_left") or _normalized_matching_payload(left, include_runtime_limits=False)
    )
    right_norm = dict(
        (match or {}).get("matching_request_right") or _normalized_matching_payload(right, include_runtime_limits=False)
    )
    computed_match = dict(match or request_family_score(left, right))
    field_details: list[dict[str, Any]] = []
    matched_fields: list[str] = []
    mismatched_fields: list[str] = []

    for field, weight in _SCALAR_MATCH_FIELDS:
        left_value = str(left_norm.get(field) or "")
        right_value = str(right_norm.get(field) or "")
        status = "empty"
        contribution = 0.0
        if left_value and right_value and left_value == right_value:
            status = "match"
            contribution = weight
            matched_fields.append(field)
        elif left_value and right_value:
            status = "mismatch"
            contribution = -min(8.0, weight * 0.5)
            mismatched_fields.append(field)
        field_details.append(
            {
                "field": field,
                "kind": "scalar",
                "weight": weight,
                "status": status,
                "left_value": left_value,
                "right_value": right_value,
                "contribution": round(contribution, 2),
            }
        )

    for field, weight in _LIST_MATCH_FIELDS:
        left_values = list(left_norm.get(field) or [])
        right_values = list(right_norm.get(field) or [])
        shared_values = sorted(set(left_values) & set(right_values))
        left_only = sorted(set(left_values) - set(right_values))
        right_only = sorted(set(right_values) - set(left_values))
        ratio = _overlap_ratio(left_values, right_values)
        status = "empty"
        contribution = 0.0
        if ratio > 0:
            status = "overlap"
            contribution = weight * ratio
            matched_fields.append(field)
        elif left_values and right_values:
            status = "disjoint"
            contribution = -min(6.0, weight * 0.25)
            mismatched_fields.append(field)
        field_details.append(
            {
                "field": field,
                "kind": "list",
                "weight": weight,
                "status": status,
                "overlap_ratio": round(ratio, 4),
                "shared_values": shared_values,
                "left_only": left_only,
                "right_only": right_only,
                "contribution": round(contribution, 2),
            }
        )

    left_cohort_identity = str(left_norm.get("cohort_selection_digest") or "")
    right_cohort_identity = str(right_norm.get("cohort_selection_digest") or "")
    if left_cohort_identity or right_cohort_identity:
        cohort_status = "match" if left_cohort_identity == right_cohort_identity else "hard_mismatch"
        if cohort_status == "match":
            matched_fields.append("cohort_selection_digest")
        else:
            mismatched_fields.append("cohort_selection_digest")
        field_details.append(
            {
                "field": "cohort_selection_digest",
                "kind": "hard_identity",
                "weight": 0.0,
                "status": cohort_status,
                "left_value": left_cohort_identity,
                "right_value": right_cohort_identity,
                "contribution": 0.0,
            }
        )

    for location_field in ("target_locations", "exclude_target_locations"):
        left_present = location_field in left_norm
        right_present = location_field in right_norm
        if not left_present and not right_present:
            continue
        left_location_value = list(left_norm.get(location_field) or [])
        right_location_value = list(right_norm.get(location_field) or [])
        location_status = (
            "match"
            if left_present == right_present and left_location_value == right_location_value
            else "hard_mismatch"
        )
        if location_status == "match":
            matched_fields.append(location_field)
        else:
            mismatched_fields.append(location_field)
        field_details.append(
            {
                "field": location_field,
                "kind": "hard_identity",
                "weight": 0.0,
                "status": location_status,
                "left_value": left_location_value if left_present else None,
                "right_value": right_location_value if right_present else None,
                "left_present": left_present,
                "right_present": right_present,
                "contribution": 0.0,
            }
        )

    explanation = {
        "selection_mode": str(selection_mode or "request_family_score"),
        "match_threshold": MATCH_THRESHOLD,
        "score": float(computed_match.get("score") or 0.0),
        "exact_request_match": bool(computed_match.get("exact_request_match")),
        "exact_family_match": bool(computed_match.get("exact_family_match")),
        "matched_fields": matched_fields,
        "mismatched_fields": mismatched_fields,
        "reasons": list(computed_match.get("reasons") or []),
        "matching_request_left": left_norm,
        "matching_request_right": right_norm,
        "field_details": field_details,
    }
    if "hard_family_mismatch" in computed_match:
        explanation["hard_family_mismatch"] = bool(computed_match.get("hard_family_mismatch"))
    return explanation


def baseline_selection_reason(match: dict[str, Any]) -> str:
    if match.get("selected_via") == "explicit_job_id":
        return "Baseline job was explicitly provided by the caller."
    if match.get("exact_request_match"):
        return "Selected baseline job with an exact request signature match."
    if match.get("exact_family_match"):
        return "Selected baseline job with an exact request-family match."
    if match.get("selected_via") == "request_family_score":
        return f"Selected baseline job by request-family similarity score {match.get('family_score') or 0}."
    return "Fell back to the latest completed job for the target company."


def source_request_matches_hard_identity(
    request_payload: dict[str, Any] | None,
    source_request_payload: Any,
) -> bool:
    """Presence-aware hard-identity reuse guard for authoritative reuse paths.

    This is the ONE guard every registry/projection/baseline reuse path must
    apply before reusing an authoritative source.  It compares BOTH hard
    identity axes that ``request_family_score`` enforces before any scoring,
    SYMMETRICALLY (FT1-FF3):

    1. Cohort execution identity — both sides' canonical
       ``cohort_execution_identity_for_signature`` digests must be EQUAL,
       including both-empty.  A legacy current request therefore never reuses
       an explicit-Cohort source (and vice versa), and a malformed Cohort on
       either side fails closed.  (The earlier asymmetric
       ``source_request_covers_explicit_cohort`` semantics let a legacy
       current request reuse Cohort-scoped assets; that hole is closed.)
    2. Location identity — canonical presence+values of BOTH sibling fields
       (``target_locations`` / ``exclude_target_locations``) must match:
       US vs Germany, absent versus an explicit ``[]``, and differing
       exclusions never share an authoritative reuse source.  A source
       request that is missing or empty carries the all-absent identity, so
       only identity-free (fully legacy) current requests match it.
       Malformed location values on either side fail closed (no reuse).
    """

    try:
        requested_cohort = cohort_execution_identity_for_signature(
            request_payload if isinstance(request_payload, dict) else {}
        )
        source_cohort = cohort_execution_identity_for_signature(
            source_request_payload if isinstance(source_request_payload, dict) else {}
        )
        if requested_cohort != source_cohort:
            return False
        return _location_payload_identity(request_payload) == _location_payload_identity(source_request_payload)
    except CohortSelectionValidationError:
        return False


def scope_spec_matches_hard_identity(
    request_payload: dict[str, Any] | None,
    scope_spec: Any,
) -> bool:
    """Hard-identity comparison against persisted projection scope metadata.

    When source-job evidence is absent, the projection's persisted
    ``scope_spec`` (``cohort_selection_digest`` plus presence-aware location
    fields, written at projection build time) is the only request-identity
    evidence left.  Comparison is symmetric and presence-aware, exactly like
    ``source_request_matches_hard_identity``: legacy scopes without the
    fields carry the all-absent identity, so identity-free (fully legacy)
    requests still match them while any pinned request identity fails closed.
    """

    scope = dict(scope_spec or {}) if isinstance(scope_spec, dict) else {}
    try:
        requested_cohort = cohort_execution_identity_for_signature(
            request_payload if isinstance(request_payload, dict) else {}
        )
        scope_cohort = str(scope.get("cohort_selection_digest") or "").strip()
        if requested_cohort != scope_cohort:
            return False
        scope_location = tuple(
            (
                field_name in scope,
                tuple(_normalize_list(scope.get(field_name)) if field_name in scope else ()),
            )
            for field_name in ("target_locations", "exclude_target_locations")
        )
        return _location_payload_identity(request_payload) == scope_location
    except CohortSelectionValidationError:
        return False


def _normalized_request_payload(payload: dict[str, Any], *, include_runtime_limits: bool) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "target_company": _normalize_scalar(payload.get("target_company")),
        "asset_view": _normalize_scalar(payload.get("asset_view")) or "canonical_merged",
        "target_scope": _normalize_scalar(payload.get("target_scope")),
        "categories": _normalize_list(payload.get("categories")),
        "employment_statuses": _normalize_list(payload.get("employment_statuses")),
        "keywords": _normalize_list(payload.get("keywords")),
        "must_have_facets": normalize_requested_facets(
            payload.get("must_have_facets") or payload.get("must_have_facet")
        ),
        "must_have_primary_role_buckets": normalize_requested_role_buckets(
            payload.get("must_have_primary_role_buckets") or payload.get("must_have_primary_role_bucket")
        ),
        "must_have_keywords": _normalize_list(payload.get("must_have_keywords")),
        "exclude_keywords": _normalize_list(payload.get("exclude_keywords")),
        "organization_keywords": _normalize_list(payload.get("organization_keywords")),
        "retrieval_strategy": _normalize_scalar(payload.get("retrieval_strategy")),
    }
    # Location sibling fields join signature identity only when present, so
    # different locations never share request signatures/reuse families while
    # legacy requests without the fields keep byte-identical signatures.
    _apply_location_signature_fields(payload, normalized)
    cohort_identity = cohort_execution_identity_for_signature(payload)
    if cohort_identity:
        normalized["cohort_selection_digest"] = cohort_identity
    if include_runtime_limits:
        normalized.update(
            {
                "top_k": _normalize_int(payload.get("top_k")),
                "semantic_rerank_limit": _normalize_int(payload.get("semantic_rerank_limit")),
                "slug_resolution_limit": _normalize_int(payload.get("slug_resolution_limit")),
                "profile_detail_limit": _normalize_int(payload.get("profile_detail_limit")),
                "publication_scan_limit": _normalize_int(payload.get("publication_scan_limit")),
                "publication_lead_limit": _normalize_int(payload.get("publication_lead_limit")),
                "exploration_limit": _normalize_int(payload.get("exploration_limit")),
                "scholar_coauthor_follow_up_limit": _normalize_int(payload.get("scholar_coauthor_follow_up_limit")),
            }
        )
    return normalized


def _prepared_effective_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = dict(payload or {})
    target_company = str(prepared.get("target_company") or "").strip()
    prepared = materialize_request_payload(prepared, target_company=target_company)
    raw_text = str(prepared.get("raw_user_request") or prepared.get("query") or "").strip()
    if raw_text:
        prepared = supplement_request_query_signals(
            prepared,
            raw_text=raw_text,
            include_raw_keyword_extraction=not has_structured_request_signals(prepared),
        )
    prepared = canonicalize_request_payload(prepared)
    return build_effective_request_payload(prepared)


def _normalized_matching_payload(payload: dict[str, Any], *, include_runtime_limits: bool) -> dict[str, Any]:
    effective_payload = _prepared_effective_request_payload(dict(payload or {}))
    return _normalized_effective_request_payload(
        effective_payload,
        include_runtime_limits=include_runtime_limits,
    )


def _normalized_effective_request_payload(
    effective_payload: dict[str, Any],
    *,
    include_runtime_limits: bool,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "target_company": _normalize_scalar(effective_payload.get("target_company")),
        "asset_view": _normalize_scalar(effective_payload.get("asset_view")) or "canonical_merged",
        "target_scope": _normalize_scalar(effective_payload.get("target_scope")),
        "categories": _normalize_list(effective_payload.get("categories")),
        "employment_statuses": _normalize_list(effective_payload.get("employment_statuses")),
        "keywords": _normalize_list(effective_payload.get("keywords")),
        "must_have_facets": normalize_requested_facets(
            effective_payload.get("must_have_facets") or effective_payload.get("must_have_facet")
        ),
        "must_have_primary_role_buckets": normalize_requested_role_buckets(
            effective_payload.get("must_have_primary_role_buckets")
            or effective_payload.get("must_have_primary_role_bucket")
        ),
        "must_have_keywords": _normalize_list(effective_payload.get("must_have_keywords")),
        "exclude_keywords": _normalize_list(effective_payload.get("exclude_keywords")),
        "organization_keywords": _normalize_list(effective_payload.get("organization_keywords")),
        "retrieval_strategy": _normalize_scalar(effective_payload.get("retrieval_strategy")),
    }
    # Same location signature identity as _normalized_request_payload: present
    # values split matching signatures/reuse by location; absent fields keep
    # legacy matching payloads byte-identical.
    _apply_location_signature_fields(effective_payload, normalized)
    cohort_identity = cohort_execution_identity_for_signature(effective_payload)
    if cohort_identity:
        normalized["cohort_selection_digest"] = cohort_identity
    if include_runtime_limits:
        normalized.update(
            {
                "top_k": _normalize_int(effective_payload.get("top_k")),
                "semantic_rerank_limit": _normalize_int(effective_payload.get("semantic_rerank_limit")),
                "slug_resolution_limit": _normalize_int(effective_payload.get("slug_resolution_limit")),
                "profile_detail_limit": _normalize_int(effective_payload.get("profile_detail_limit")),
                "publication_scan_limit": _normalize_int(effective_payload.get("publication_scan_limit")),
                "publication_lead_limit": _normalize_int(effective_payload.get("publication_lead_limit")),
                "exploration_limit": _normalize_int(effective_payload.get("exploration_limit")),
                "scholar_coauthor_follow_up_limit": _normalize_int(
                    effective_payload.get("scholar_coauthor_follow_up_limit")
                ),
            }
        )
    return normalized


def _coerce_matching_bundle(payload: dict[str, Any], bundle: dict[str, Any] | None) -> dict[str, Any]:
    # FT1-FF3: a persisted/passed bundle is NEVER trusted as a truth source —
    # the complete canonical regeneration is returned in every case, so no
    # contradictory effective_request, stale normalized payload, or tampered
    # signature can survive into scoring.  (Earlier revisions returned the
    # persisted bundle after a partial comparison.)
    return build_request_matching_bundle(payload)


def _apply_location_signature_fields(payload: dict[str, Any], normalized: dict[str, Any]) -> None:
    # Presence AND validation semantics mirror request ingress
    # (domain._normalize_location_list) exactly (FT1-FF2): only a real array
    # of bounded location strings is valid — a present JSON null, a bare
    # string, a mapping, a number, null/non-string items, or over-bound values
    # all fail closed with the same stable ``request_location_*`` validation
    # error instead of aliasing a valid request family (a bare "Germany"
    # string becoming ["Germany"], a null item silently collapsing) or
    # aborting matching with an uncaught TypeError.  Valid values keep the
    # canonical signature form (lowercased/deduped/sorted), so well-formed
    # payloads keep byte-identical signatures while absent fields stay absent.
    for field_name in ("target_locations", "exclude_target_locations"):
        if field_name not in payload:
            continue
        validated = _normalize_location_list(payload.get(field_name), field_name=field_name)
        normalized[field_name] = _normalize_list(validated)


def _location_request_identity(normalized: dict[str, Any]) -> tuple:
    # Canonical presence+value identity for both location sibling fields:
    # field absence and an explicit empty list are different identities.
    return tuple(
        (field_name in normalized, tuple(normalized.get(field_name) or ()))
        for field_name in ("target_locations", "exclude_target_locations")
    )


def _location_payload_identity(payload: Any) -> tuple:
    # Same canonical presence+value identity as _location_request_identity,
    # computed straight from a raw stored/request payload (no effective-request
    # materialization: no location defaulting happens in normalization, and the
    # stored request is the reuse authority).  A missing/non-dict payload
    # carries the all-absent identity; a present JSON null raises so callers
    # fail closed exactly like request ingress and matching identity.
    if not isinstance(payload, dict) or not payload:
        return tuple((False, ()) for _field in ("target_locations", "exclude_target_locations"))
    normalized: dict[str, Any] = {}
    _apply_location_signature_fields(payload, normalized)
    return _location_request_identity(normalized)


def _signature_for_payload(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        values = list(value)
    normalized = sorted({_normalize_scalar(item) for item in values if _normalize_scalar(item)})
    return normalized


def _normalize_scalar(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _overlap_ratio(left: list[str], right: list[str]) -> float:
    if not left and not right:
        return 0.0
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    union = left_set | right_set
    if not union:
        return 0.0
    return len(left_set & right_set) / len(union)
