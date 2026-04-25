from __future__ import annotations

import re
from typing import Any, Iterable

from .company_registry import builtin_company_identity, normalize_company_key
from .domain import normalize_requested_role_buckets
from .query_signal_knowledge import (
    canonicalize_scope_signal_label,
    match_scope_signals,
    role_bucket_function_ids,
    role_bucket_matched_terms,
    role_bucket_role_hints,
    role_buckets_from_text,
)

DEFAULT_TECHNICAL_ROLE_BUCKETS = ("research", "engineering")
_WEAK_SINGLETON_TECHNICAL_ROLE_BUCKETS = {
    "research",
    "engineering",
    "infra_systems",
}
_TEXT_EXPLICIT_ROLE_BUCKETS = {
    "research",
    "engineering",
    "product_management",
    "founding",
}

_DIRECTIONAL_SCOPE_HINT_TERMS = (
    "方向",
    "方向的",
    "方向上",
    "方向相关",
    "topic",
    "topics",
    "thematic",
    "focus",
    "focused",
    "focus area",
    "focus areas",
    "track",
    "tracks",
    "area",
    "areas",
    "working on",
    "work on",
    "负责",
    "参与",
    "相关",
)

_STRONG_FULL_ROSTER_TERMS = (
    "entire roster",
    "company wide roster",
    "company-wide roster",
    "full roster",
    "full company roster",
    "先拿全量数据资产",
    "先获取全量数据资产",
    "先全量获取数据资产",
    "先全量获取",
    "全量获取 roster",
    "全量 roster",
)

_WEAK_FULL_ROSTER_TERMS = (
    "all members",
    "all employees",
    "所有成员",
    "全部成员",
    "全体成员",
    "全量",
    "全员",
)

_TECHNICAL_POPULATION_CATEGORY_KEYS = {
    "employee",
    "former_employee",
    "researcher",
    "engineer",
}


def compile_semantic_brief(
    *,
    raw_text: str,
    target_company: str,
    target_scope: str,
    categories: Iterable[str] | None,
    employment_statuses: Iterable[str] | None,
    organization_keywords: Iterable[str] | None,
    keywords: Iterable[str] | None,
    must_have_keywords: Iterable[str] | None,
    must_have_facets: Iterable[str] | None,
    must_have_primary_role_buckets: Iterable[str] | None,
    execution_preferences: dict[str, Any] | None = None,
    scope_disambiguation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_text = _normalize_text(raw_text).lower()
    normalized_target_company = _normalize_text(target_company)
    normalized_categories = _dedupe_strings(categories)
    normalized_employment_statuses = _dedupe_strings(employment_statuses)
    normalized_organization_keywords = _dedupe_strings(organization_keywords)
    normalized_keywords = _dedupe_strings(keywords)
    normalized_must_have_keywords = _dedupe_strings(must_have_keywords)
    normalized_must_have_facets = _dedupe_strings(must_have_facets)
    normalized_role_buckets = normalize_requested_role_buckets(list(must_have_primary_role_buckets or []))

    company_key, builtin_identity = builtin_company_identity(normalized_target_company)
    scope_hints = _merge_scope_hints(
        explicit=normalized_organization_keywords,
        raw_text=normalized_text,
        target_company=normalized_target_company,
    )
    thematic_constraints = bool(
        normalized_keywords
        or normalized_must_have_keywords
        or normalized_must_have_facets
        or normalized_role_buckets
    )
    technical_population = _is_technical_population(
        categories=normalized_categories,
        keywords=normalized_keywords,
        must_have_keywords=normalized_must_have_keywords,
        must_have_facets=normalized_must_have_facets,
    )
    role_targeting = _compile_role_targeting(
        raw_text=raw_text,
        categories=normalized_categories,
        must_have_facets=normalized_must_have_facets,
        requested_role_buckets=normalized_role_buckets,
        technical_population=technical_population,
    )
    directional_query = _is_directional_query(
        text=normalized_text,
        scope_hints=scope_hints,
        organization_keywords=normalized_organization_keywords,
        thematic_constraints=thematic_constraints,
        resolved_role_buckets=list(role_targeting.get("resolved_role_buckets") or []),
    )
    thematic_or_role_constrained = bool(
        thematic_constraints
        or list(role_targeting.get("resolved_role_buckets") or [])
        or any(token in normalized_text for token in _DIRECTIONAL_SCOPE_HINT_TERMS)
    )
    explicit_full_roster_intent = _is_explicit_full_roster_intent(
        normalized_text,
        thematic_or_role_constrained=thematic_or_role_constrained,
    )

    return {
        "target_company": normalized_target_company,
        "company_key": company_key,
        "target_scope": _normalize_text(target_scope) or "full_company_asset",
        "company_resolution": {
            "company_key": company_key,
            "canonical_name": str((builtin_identity or {}).get("canonical_name") or normalized_target_company).strip(),
            "linkedin_slug": str((builtin_identity or {}).get("linkedin_slug") or "").strip(),
            "resolution_mode": "builtin" if builtin_identity else ("explicit" if normalized_target_company else "unknown"),
            "has_builtin_identity": bool(builtin_identity),
        },
        "population": {
            "categories": normalized_categories,
            "employment_statuses": normalized_employment_statuses,
            "technical_population": technical_population,
        },
        "scope": {
            "organization_keywords": normalized_organization_keywords,
            "scope_hints": scope_hints,
            "scope_disambiguation": dict(scope_disambiguation or {}),
            "directional_query": directional_query,
            "explicit_full_roster_intent": explicit_full_roster_intent,
        },
        "thematic_focus": {
            "keywords": normalized_keywords,
            "must_have_keywords": normalized_must_have_keywords,
            "must_have_facets": normalized_must_have_facets,
            "thematic_or_role_constrained": thematic_or_role_constrained,
        },
        "role_targeting": role_targeting,
        "execution_preferences": dict(execution_preferences or {}),
    }


def semantic_brief_function_target_groups(semantic_brief: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in list(dict(semantic_brief or {}).get("role_targeting", {}).get("function_target_groups") or [])
        if isinstance(item, dict)
    ]


def _compile_role_targeting(
    *,
    raw_text: str,
    categories: list[str],
    must_have_facets: list[str],
    requested_role_buckets: list[str],
    technical_population: bool,
) -> dict[str, Any]:
    normalized_text = _normalize_text(raw_text)
    lexical_role_buckets = role_buckets_from_text(normalized_text)
    explicit_text_role_buckets = [
        bucket
        for bucket in lexical_role_buckets
        if bucket in _TEXT_EXPLICIT_ROLE_BUCKETS and role_bucket_matched_terms(normalized_text, bucket)
    ]
    structured_role_buckets = normalize_requested_role_buckets(
        [*categories, *must_have_facets, *requested_role_buckets]
    )

    provenance = "default"
    weak_inferred_role_buckets: list[str] = []
    resolved_role_buckets: list[str] = []
    if explicit_text_role_buckets:
        provenance = "text_explicit"
        resolved_role_buckets = list(explicit_text_role_buckets)
    elif structured_role_buckets:
        if (
            technical_population
            and len(structured_role_buckets) == 1
            and structured_role_buckets[0] in _WEAK_SINGLETON_TECHNICAL_ROLE_BUCKETS
        ):
            provenance = "default_technical_from_weak_structured_singleton"
            weak_inferred_role_buckets = list(structured_role_buckets)
            resolved_role_buckets = list(DEFAULT_TECHNICAL_ROLE_BUCKETS)
        else:
            provenance = "structured"
            resolved_role_buckets = list(structured_role_buckets)
    elif technical_population:
        provenance = "default_technical"
        resolved_role_buckets = list(DEFAULT_TECHNICAL_ROLE_BUCKETS)
    elif lexical_role_buckets:
        provenance = "lexical_inferred"
        resolved_role_buckets = list(lexical_role_buckets)

    function_target_groups = _build_function_target_groups(resolved_role_buckets)
    role_hints = _dedupe_strings(
        hint
        for item in function_target_groups
        for hint in list(item.get("role_hints") or [])
    )
    function_ids = _dedupe_strings(
        function_id
        for item in function_target_groups
        for function_id in list(item.get("function_ids") or [])
    )
    return {
        "requested_role_buckets": list(requested_role_buckets),
        "structured_role_buckets": structured_role_buckets,
        "text_role_buckets": list(lexical_role_buckets),
        "explicit_text_role_buckets": explicit_text_role_buckets,
        "weak_inferred_role_buckets": weak_inferred_role_buckets,
        "resolved_role_buckets": resolved_role_buckets,
        "provenance": provenance,
        "function_target_groups": function_target_groups,
        "role_hints": role_hints,
        "function_ids": function_ids,
    }


def _build_function_target_groups(role_buckets: list[str]) -> list[dict[str, Any]]:
    merged_groups: list[dict[str, Any]] = []
    by_signature: dict[tuple[str, ...], dict[str, Any]] = {}
    for bucket in role_buckets:
        normalized_bucket = _normalize_text(bucket).replace("-", "_").replace(" ", "_").lower()
        if not normalized_bucket:
            continue
        function_ids = role_bucket_function_ids([normalized_bucket])
        role_hints = role_bucket_role_hints([normalized_bucket])
        signature = tuple(function_ids) or (normalized_bucket,)
        existing = by_signature.get(signature)
        if existing is None:
            existing = {
                "group_id": normalized_bucket,
                "role_buckets": [normalized_bucket],
                "role_hints": list(role_hints),
                "function_ids": list(function_ids),
                "primary_role_hint": role_hints[0] if role_hints else normalized_bucket.replace("_", " ").title(),
            }
            by_signature[signature] = existing
            merged_groups.append(existing)
            continue
        if normalized_bucket not in list(existing.get("role_buckets") or []):
            existing["role_buckets"] = [*list(existing.get("role_buckets") or []), normalized_bucket]
        existing["role_hints"] = _dedupe_strings([*list(existing.get("role_hints") or []), *role_hints])
        if not str(existing.get("primary_role_hint") or "").strip():
            existing["primary_role_hint"] = role_hints[0] if role_hints else normalized_bucket.replace("_", " ").title()
    for item in merged_groups:
        role_buckets = list(item.get("role_buckets") or [])
        if len(role_buckets) > 1:
            item["group_id"] = "__".join(role_buckets)
    return merged_groups


def _merge_scope_hints(*, explicit: list[str], raw_text: str, target_company: str) -> list[str]:
    hints: list[str] = []
    blocked = {normalize_company_key(target_company)} if target_company else set()
    for value in list(explicit or []):
        candidate = _normalize_text(value)
        if not candidate or normalize_company_key(candidate) in blocked or candidate in hints:
            continue
        hints.append(candidate)
    for spec in canonicalize_scope_signal_labels_from_text(raw_text):
        if not spec or normalize_company_key(spec) in blocked or spec in hints:
            continue
        hints.append(spec)
    return hints[:6]


def canonicalize_scope_signal_labels_from_text(raw_text: str) -> list[str]:
    labels: list[str] = []
    for spec in match_scope_signals(raw_text):
        for item in list(spec.get("organization_keywords") or []):
            candidate = _normalize_text(item)
            if candidate and candidate not in labels:
                labels.append(candidate)
    for item in re.findall(r'"([^"]+)"', raw_text):
        candidate = canonicalize_scope_signal_label(item)
        if candidate and candidate not in labels:
            labels.append(candidate)
    for item in re.findall(r"“([^”]+)”", raw_text):
        candidate = canonicalize_scope_signal_label(item)
        if candidate and candidate not in labels:
            labels.append(candidate)
    return labels


def _is_directional_query(
    *,
    text: str,
    scope_hints: list[str],
    organization_keywords: list[str],
    thematic_constraints: bool,
    resolved_role_buckets: list[str],
) -> bool:
    if len(scope_hints) >= 2:
        return True
    if organization_keywords or thematic_constraints or resolved_role_buckets:
        return True
    return any(token in text for token in _DIRECTIONAL_SCOPE_HINT_TERMS)


def _is_explicit_full_roster_intent(text: str, *, thematic_or_role_constrained: bool) -> bool:
    if not text:
        return False
    if any(term in text for term in _STRONG_FULL_ROSTER_TERMS):
        return True
    if thematic_or_role_constrained:
        return False
    return any(term in text for term in _WEAK_FULL_ROSTER_TERMS)


def _is_technical_population(
    *,
    categories: list[str],
    keywords: list[str],
    must_have_keywords: list[str],
    must_have_facets: list[str],
) -> bool:
    normalized_categories = {
        _normalize_text(item).lower()
        for item in list(categories or [])
        if _normalize_text(item)
    }
    if normalized_categories and not normalized_categories.issubset(_TECHNICAL_POPULATION_CATEGORY_KEYS):
        return False
    role_like_categories = normalized_categories.intersection({"researcher", "engineer"})
    return bool(
        role_like_categories
        or keywords
        or must_have_keywords
        or must_have_facets
    )


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_strings(values: Iterable[Any] | None) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = _normalize_text(value)
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(normalized)
    return deduped
