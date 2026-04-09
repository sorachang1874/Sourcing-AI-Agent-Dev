from __future__ import annotations

import json
from hashlib import sha1
from typing import Any

from .domain import normalize_requested_facets, normalize_requested_role_buckets


MATCH_THRESHOLD = 30.0


def request_signature(payload: dict[str, Any]) -> str:
    normalized = _normalized_request_payload(payload, include_runtime_limits=True)
    serialized = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


def request_family_signature(payload: dict[str, Any]) -> str:
    normalized = _normalized_request_payload(payload, include_runtime_limits=False)
    serialized = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


def request_family_score(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_norm = _normalized_request_payload(left, include_runtime_limits=False)
    right_norm = _normalized_request_payload(right, include_runtime_limits=False)
    if left_norm["target_company"] != right_norm["target_company"]:
        return {
            "score": 0.0,
            "exact_request_match": False,
            "exact_family_match": False,
            "family_signature_left": request_family_signature(left),
            "family_signature_right": request_family_signature(right),
            "reasons": ["target_company_mismatch"],
        }

    exact_request_match = request_signature(left) == request_signature(right)
    exact_family_match = request_family_signature(left) == request_family_signature(right)
    if exact_request_match:
        return {
            "score": 100.0,
            "exact_request_match": True,
            "exact_family_match": True,
            "family_signature_left": request_family_signature(left),
            "family_signature_right": request_family_signature(right),
            "reasons": ["exact_request_match"],
        }
    if exact_family_match:
        return {
            "score": 95.0,
            "exact_request_match": False,
            "exact_family_match": True,
            "family_signature_left": request_family_signature(left),
            "family_signature_right": request_family_signature(right),
            "reasons": ["exact_family_match"],
        }

    score = 0.0
    reasons: list[str] = []
    for field, weight in [
        ("asset_view", 12.0),
        ("target_scope", 12.0),
        ("retrieval_strategy", 8.0),
    ]:
        if left_norm[field] and left_norm[field] == right_norm[field]:
            score += weight
            reasons.append(f"{field}_match")
        elif left_norm[field] and right_norm[field]:
            score -= min(8.0, weight * 0.5)
            reasons.append(f"{field}_mismatch")

    for field, weight in [
        ("categories", 20.0),
        ("employment_statuses", 14.0),
        ("keywords", 30.0),
        ("must_have_facets", 12.0),
        ("must_have_primary_role_buckets", 14.0),
        ("must_have_keywords", 8.0),
        ("exclude_keywords", 4.0),
        ("organization_keywords", 10.0),
    ]:
        ratio = _overlap_ratio(left_norm[field], right_norm[field])
        if ratio > 0:
            score += weight * ratio
            reasons.append(f"{field}_overlap={round(ratio, 2)}")
        elif left_norm[field] and right_norm[field]:
            score -= min(6.0, weight * 0.25)
            reasons.append(f"{field}_disjoint")

    if score < 0:
        score = 0.0
    return {
        "score": round(score, 2),
        "exact_request_match": False,
        "exact_family_match": False,
        "family_signature_left": request_family_signature(left),
        "family_signature_right": request_family_signature(right),
        "reasons": reasons,
    }


def baseline_selection_reason(match: dict[str, Any]) -> str:
    if match.get("selected_via") == "explicit_job_id":
        return "Baseline job was explicitly provided by the caller."
    if match.get("exact_request_match"):
        return "Selected baseline job with an exact request signature match."
    if match.get("exact_family_match"):
        return "Selected baseline job with an exact request-family match."
    if match.get("selected_via") == "request_family_score":
        return (
            f"Selected baseline job by request-family similarity score "
            f"{match.get('family_score') or 0}."
        )
    return "Fell back to the latest completed job for the target company."


def _normalized_request_payload(payload: dict[str, Any], *, include_runtime_limits: bool) -> dict[str, Any]:
    normalized = {
        "target_company": _normalize_scalar(payload.get("target_company")),
        "asset_view": _normalize_scalar(payload.get("asset_view")) or "canonical_merged",
        "target_scope": _normalize_scalar(payload.get("target_scope")),
        "categories": _normalize_list(payload.get("categories")),
        "employment_statuses": _normalize_list(payload.get("employment_statuses")),
        "keywords": _normalize_list(payload.get("keywords")),
        "must_have_facets": normalize_requested_facets(payload.get("must_have_facets") or payload.get("must_have_facet")),
        "must_have_primary_role_buckets": normalize_requested_role_buckets(
            payload.get("must_have_primary_role_buckets") or payload.get("must_have_primary_role_bucket")
        ),
        "must_have_keywords": _normalize_list(payload.get("must_have_keywords")),
        "exclude_keywords": _normalize_list(payload.get("exclude_keywords")),
        "organization_keywords": _normalize_list(payload.get("organization_keywords")),
        "retrieval_strategy": _normalize_scalar(payload.get("retrieval_strategy")),
    }
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
