from __future__ import annotations

import re
from typing import Any

from .company_registry import builtin_company_identity
from .domain import JobRequest
from .execution_preferences import merge_execution_preferences, normalize_execution_preferences
from .query_signal_knowledge import (
    canonicalize_scope_signal_label,
    match_scope_signals,
    resolve_target_company_alias,
    role_buckets_from_text,
)

_GENERIC_QUERY_SIGNAL_TERMS = {
    "ai",
    "employee",
    "employees",
    "member",
    "members",
    "team",
    "teams",
    "current",
    "former",
    "research",
    "researcher",
    "researchers",
    "engineer",
    "engineers",
    "direction",
    "project",
    "projects",
    "group",
    "groups",
    "model",
    "models",
}

_GENERIC_SCOPE_SUFFIX_TERMS = {
    "member",
    "members",
    "employee",
    "employees",
    "people",
    "person",
    "persons",
    "team",
    "teams",
    "group",
    "groups",
    "direction",
    "directions",
    "role",
    "roles",
}

_STRUCTURED_REQUEST_SIGNAL_FIELDS = {
    "keywords",
    "must_have_keywords",
    "organization_keywords",
    "must_have_facets",
    "must_have_primary_role_buckets",
    "team_keywords",
    "sub_org_keywords",
    "project_keywords",
    "product_keywords",
    "model_keywords",
    "research_direction_keywords",
    "technology_keywords",
}

_INTENT_AXIS_EXECUTION_PREFERENCE_KEYS = {
    "acquisition_strategy_override",
    "use_company_employees_lane",
    "keyword_priority_only",
    "former_keyword_queries_only",
    "large_org_keyword_probe_mode",
    "force_fresh_run",
    "allow_high_cost_sources",
    "provider_people_search_query_strategy",
    "provider_people_search_max_queries",
    "reuse_existing_roster",
    "run_former_search_seed",
}


def _expand_parenthetical_signal_terms(value: str) -> list[str]:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return []
    ascii_terms = _ascii_scope_like_terms(text)
    if _looks_like_scaffolded_phrase(text, ascii_terms=ascii_terms):
        return ascii_terms
    return [text]


def _ascii_scope_like_terms(text: str) -> list[str]:
    extracted: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(
        r"(?<![A-Za-z0-9])[A-Z][A-Za-z0-9&._/-]{1,30}(?:\s+[A-Z][A-Za-z0-9&._/-]{1,30})*",
        text,
    ):
        normalized = " ".join(str(match.group(0) or "").split()).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        extracted.append(normalized)
    return extracted


def _looks_like_scaffolded_phrase(text: str, *, ascii_terms: list[str] | None = None) -> bool:
    ascii_terms = list(ascii_terms or _ascii_scope_like_terms(text))
    ascii_terms = [
        item for item in ascii_terms if str(item).strip()
    ]
    if (
        ascii_terms
        and re.search(r"[\u4e00-\u9fff]", text)
        and any(token in text for token in ("在", "和", "团队", "成员", "的人", "参与", "负责", "做"))
    ):
        return True
    return False


def canonicalize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    canonical = dict(payload or {})
    target_company = str(canonical.get("target_company") or "").strip()
    if not target_company:
        return canonical
    mapped = resolve_target_company_alias(target_company)
    parent_company = str(mapped.get("target_company") or "").strip()
    if not parent_company:
        return canonical
    canonical["target_company"] = parent_company
    canonical["organization_keywords"] = merge_unique_request_string_values(
        mapped.get("organization_keywords"),
        canonical.get("organization_keywords"),
        target_company=parent_company,
    )
    return canonical


def materialize_request_payload(
    payload: dict[str, Any] | None,
    *,
    target_company: str = "",
) -> dict[str, Any]:
    materialized = dict(payload or {})
    resolved_target_company = str(
        target_company
        or materialized.get("target_company")
        or ""
    ).strip()
    expanded = expand_request_intent_axes_patch(
        materialized,
        target_company=resolved_target_company,
    )
    if not expanded:
        return materialized
    for key, value in expanded.items():
        if key == "execution_preferences":
            continue
        if not _has_preview_value(materialized.get(key)):
            materialized[key] = value
    resolved_target_company = str(
        materialized.get("target_company")
        or expanded.get("target_company")
        or resolved_target_company
    ).strip()
    existing_preferences = normalize_execution_preferences(
        materialized,
        target_company=resolved_target_company,
    )
    expanded_preferences = normalize_execution_preferences(
        {"execution_preferences": expanded.get("execution_preferences") or {}},
        target_company=resolved_target_company,
    )
    merged_preferences = merge_execution_preferences(
        existing_preferences,
        expanded_preferences,
    )
    if merged_preferences:
        materialized["execution_preferences"] = merged_preferences
    return materialized


def supplement_request_query_signals(
    payload: dict[str, Any],
    *,
    raw_text: str,
    include_raw_keyword_extraction: bool = True,
) -> dict[str, Any]:
    updated = dict(payload or {})
    target_company = str(updated.get("target_company") or "").strip()
    if include_raw_keyword_extraction:
        extracted = extract_query_signal_terms(raw_text, target_company=target_company)
        had_scope_disambiguation = bool(updated.get("scope_disambiguation"))
        if extracted["organization_keywords"]:
            updated["organization_keywords"] = merge_unique_request_string_values(
                updated.get("organization_keywords"),
                extracted["organization_keywords"],
                target_company=target_company,
            )
        if extracted["keywords"]:
            updated["keywords"] = merge_unique_request_string_values(
                updated.get("keywords"),
                extracted["keywords"],
                target_company=target_company,
            )
        scope_disambiguation = dict(updated.get("scope_disambiguation") or {})
        extracted_scope_candidates = list(extracted["organization_keywords"] or [])
        if extracted_scope_candidates:
            scope_disambiguation["sub_org_candidates"] = merge_unique_request_string_values(
                scope_disambiguation.get("sub_org_candidates"),
                extracted_scope_candidates,
                target_company=target_company,
            )[:6]
            if not str(scope_disambiguation.get("inferred_scope") or "").strip():
                scope_disambiguation["inferred_scope"] = "uncertain"
            if "confidence" not in scope_disambiguation:
                scope_disambiguation["confidence"] = 0.35
            if not str(scope_disambiguation.get("rationale") or "").strip():
                scope_disambiguation["rationale"] = (
                    "Preserved ambiguous team/product terms from the raw query so they remain available for "
                    "acquisition search, retrieval, and review."
                )
            normalized_source = str(scope_disambiguation.get("source") or "").strip().lower()
            if normalized_source not in {"llm", "hybrid", "rules"}:
                scope_disambiguation["source"] = "hybrid" if had_scope_disambiguation else "rules"
            updated["scope_disambiguation"] = scope_disambiguation
    updated = apply_high_confidence_request_inference(updated, raw_text=raw_text)
    return updated


def extract_query_signal_terms(raw_text: str, *, target_company: str) -> dict[str, list[str]]:
    text = str(raw_text or "")
    organization_keywords: list[str] = []
    keywords: list[str] = []

    def _add_organization(value: str) -> None:
        candidate = normalize_request_query_signal(value, target_company=target_company)
        if not candidate or should_skip_query_signal(candidate, target_company=target_company):
            return
        if candidate not in organization_keywords:
            organization_keywords.append(candidate)
        if candidate not in keywords:
            keywords.append(candidate)

    def _add_keyword(value: str) -> None:
        candidate = normalize_request_query_signal(value, target_company=target_company)
        if not candidate or should_skip_query_signal(candidate, target_company=target_company):
            return
        if candidate not in keywords:
            keywords.append(candidate)

    if target_company:
        company_pattern = re.compile(
            rf"{re.escape(target_company)}\s*(?:的)?\s*([A-Z][A-Za-z0-9&._/-]{{1,30}}(?:\s+[A-Z][A-Za-z0-9&._/-]{{1,30}}){{0,2}})",
            flags=re.IGNORECASE,
        )
        for match in company_pattern.finditer(text):
            extracted_value = normalize_request_query_signal(str(match.group(1) or ""), target_company=target_company)
            if not extracted_value:
                continue
            stripped_value = _strip_generic_scope_suffix(extracted_value)
            if stripped_value != extracted_value:
                if _looks_like_explicit_scope_signal(stripped_value):
                    _add_organization(stripped_value)
                else:
                    _add_keyword(stripped_value)
                continue
            if _looks_like_explicit_scope_signal(extracted_value):
                _add_organization(extracted_value)
            else:
                _add_keyword(extracted_value)

    for match in re.findall(r"[\(\[（【\"“'`]\s*([^)\]）】\"”'`]{2,40})\s*[\)\]）】\"”'`]", text):
        for term in _expand_parenthetical_signal_terms(str(match or "")):
            _add_keyword(term)

    for token in re.findall(r"\b[A-Z]{2,8}\b", text):
        _add_keyword(token)
    for token in re.findall(r"\b[A-Z][A-Za-z0-9&._/-]{2,30}(?:\s+[A-Z][A-Za-z0-9&._/-]{2,30})?\b", text):
        _add_keyword(token)

    return {
        "organization_keywords": organization_keywords[:6],
        "keywords": keywords[:12],
    }


def normalize_request_query_signal(value: str, *, target_company: str) -> str:
    normalized = " ".join(str(value or "").split()).strip(" \t\r\n,;:|/[](){}")
    if not normalized:
        return ""
    normalized = canonicalize_scope_signal_label(normalized)
    if target_company:
        prefix = str(target_company or "").strip()
        if prefix and normalized.lower().startswith(prefix.lower() + " "):
            if builtin_company_identity(normalized)[1] is None:
                suffix = normalized[len(prefix) :].strip(" \t\r\n-:/")
                if suffix:
                    normalized = suffix
    return normalized[:120]


def should_skip_query_signal(value: str, *, target_company: str) -> bool:
    raw_value = " ".join(str(value or "").split()).strip()
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return True
    if _looks_like_scaffolded_phrase(raw_value):
        return True
    if normalized in _GENERIC_QUERY_SIGNAL_TERMS:
        return True
    if normalized == str(target_company or "").strip().lower():
        return True
    if builtin_company_identity(value)[1] is not None:
        return True
    return False


def _strip_generic_scope_suffix(value: str) -> str:
    tokens = [token for token in str(value or "").split() if token]
    while tokens and tokens[-1].lower() in _GENERIC_SCOPE_SUFFIX_TERMS:
        tokens = tokens[:-1]
    return " ".join(tokens).strip()


def _looks_like_explicit_scope_signal(value: str) -> bool:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return False
    if builtin_company_identity(normalized)[1] is not None:
        return True
    if match_scope_signals(normalized.lower()):
        return True
    if re.search(r"\b[A-Z]{2,}\b", normalized):
        return True
    return bool(re.search(r"[A-Z]", normalized[1:]))


def merge_unique_request_string_values(*sources: Any, target_company: str = "") -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in sources:
        if isinstance(source, str):
            items = [source]
        else:
            items = list(source or [])
        for item in items:
            value = normalize_request_query_signal(str(item or ""), target_company=target_company)
            if not value:
                continue
            key = " ".join(value.lower().split())
            if key in seen:
                continue
            seen.add(key)
            merged.append(value)
    return merged


def has_structured_request_signals(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in _STRUCTURED_REQUEST_SIGNAL_FIELDS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, (list, tuple, set)) and any(str(item or "").strip() for item in value):
            return True
    scope_disambiguation = payload.get("scope_disambiguation")
    if isinstance(scope_disambiguation, dict) and any(
        (isinstance(value, str) and value.strip())
        or (isinstance(value, (list, tuple, set)) and any(str(item or "").strip() for item in value))
        or value not in (None, "", [], {}, ())
        for value in scope_disambiguation.values()
    ):
        return True
    intent_axes = payload.get("intent_axes")
    if isinstance(intent_axes, dict) and any(
        isinstance(value, dict) and bool(value)
        for value in intent_axes.values()
    ):
        return True
    return False


def expand_request_intent_axes_patch(
    payload: dict[str, Any] | None,
    *,
    target_company: str = "",
) -> dict[str, Any]:
    axes = dict((payload or {}).get("intent_axes") or {})
    if not axes:
        return {}
    resolved_target_company = str(
        target_company
        or dict(axes.get("scope_boundary") or {}).get("target_company")
        or ""
    ).strip()
    patch: dict[str, Any] = {}

    population_boundary = dict(axes.get("population_boundary") or {})
    categories = merge_unique_request_string_values(population_boundary.get("categories"))
    if categories:
        patch["categories"] = categories
    employment_statuses = merge_unique_request_string_values(population_boundary.get("employment_statuses"))
    if employment_statuses:
        patch["employment_statuses"] = employment_statuses

    scope_boundary = dict(axes.get("scope_boundary") or {})
    scope_target_company = str(scope_boundary.get("target_company") or "").strip()
    if scope_target_company:
        patch["target_company"] = scope_target_company
        resolved_target_company = scope_target_company
    organization_keywords = merge_unique_request_string_values(
        scope_boundary.get("organization_keywords"),
        target_company=resolved_target_company,
    )
    if organization_keywords:
        patch["organization_keywords"] = organization_keywords
    scope_disambiguation = dict(scope_boundary.get("scope_disambiguation") or {})
    if scope_disambiguation:
        patch["scope_disambiguation"] = scope_disambiguation

    execution_preferences: dict[str, Any] = {}
    confirmed_company_scope = merge_unique_request_string_values(
        scope_boundary.get("confirmed_company_scope"),
        target_company=resolved_target_company,
    )
    if confirmed_company_scope:
        execution_preferences["confirmed_company_scope"] = confirmed_company_scope

    for axis_name in ("acquisition_lane_policy", "fallback_policy"):
        axis_payload = dict(axes.get(axis_name) or {})
        for key in _INTENT_AXIS_EXECUTION_PREFERENCE_KEYS:
            if key not in axis_payload:
                continue
            value = axis_payload.get(key)
            if isinstance(value, str):
                if value.strip():
                    execution_preferences[key] = value.strip()
                continue
            if isinstance(value, list):
                merged_values = merge_unique_request_string_values(value, target_company=resolved_target_company)
                if merged_values:
                    execution_preferences[key] = merged_values
                continue
            if value is not None:
                execution_preferences[key] = value
    if execution_preferences:
        patch["execution_preferences"] = execution_preferences

    thematic_constraints = dict(axes.get("thematic_constraints") or {})
    for key in (
        "keywords",
        "must_have_keywords",
        "must_have_facets",
        "must_have_primary_role_buckets",
    ):
        values = merge_unique_request_string_values(
            thematic_constraints.get(key),
            target_company=resolved_target_company,
        )
        if values:
            patch[key] = values
    return patch


def apply_high_confidence_request_inference(
    payload: dict[str, Any],
    *,
    raw_text: str,
) -> dict[str, Any]:
    updated = dict(payload or {})
    normalized_text = " ".join(str(raw_text or "").lower().split())

    scope_matches = match_scope_signals(normalized_text)
    resolved_target_company = str(updated.get("target_company") or "").strip()
    if not resolved_target_company:
        target_candidates = list(
            dict.fromkeys(str(item.get("target_company") or "").strip() for item in scope_matches if str(item.get("target_company") or "").strip())
        )
        if len(target_candidates) == 1:
            resolved_target_company = target_candidates[0]
            updated["target_company"] = resolved_target_company

    if scope_matches:
        target_company = str(updated.get("target_company") or resolved_target_company or "").strip()
        organization_keywords: list[str] = []
        for match in scope_matches:
            organization_keywords.extend(list(match.get("organization_keywords") or []))
        if organization_keywords:
            updated["organization_keywords"] = merge_unique_request_string_values(
                updated.get("organization_keywords"),
                organization_keywords,
                target_company=target_company,
            )
        keyword_labels: list[str] = []
        for match in scope_matches:
            keyword_labels.extend(list(match.get("keyword_labels") or []))
        if keyword_labels:
            updated["keywords"] = merge_unique_request_string_values(
                updated.get("keywords"),
                keyword_labels,
                target_company=target_company,
            )
        if target_company:
            merged_scope_disambiguation = dict(updated.get("scope_disambiguation") or {})
            for match in scope_matches:
                scope_payload = dict(match.get("scope_disambiguation") or {})
                if not scope_payload:
                    continue
                merged_scope_disambiguation = _merge_scope_disambiguation_payloads(
                    merged_scope_disambiguation,
                    scope_payload,
                    target_company=target_company,
                )
            if merged_scope_disambiguation:
                updated["scope_disambiguation"] = merged_scope_disambiguation

    role_buckets = _extract_role_buckets_from_text(raw_text)
    if role_buckets:
        updated["must_have_primary_role_buckets"] = merge_unique_request_string_values(
            updated.get("must_have_primary_role_buckets"),
            role_buckets,
        )
        updated["keywords"] = _remove_role_abbreviation_noise(
            merge_unique_request_string_values(updated.get("keywords")),
            raw_text=raw_text,
            role_buckets=role_buckets,
        )
    return updated


def _matched_scope_signal_rules(normalized_text: str) -> list[dict[str, Any]]:
    return match_scope_signals(normalized_text)


def _extract_role_buckets_from_text(raw_text: str) -> list[str]:
    return role_buckets_from_text(raw_text)


def _merge_scope_disambiguation_payloads(
    existing: dict[str, Any],
    patch: dict[str, Any],
    *,
    target_company: str,
) -> dict[str, Any]:
    merged = dict(existing or {})
    if not merged.get("inferred_scope"):
        merged["inferred_scope"] = str(patch.get("inferred_scope") or "").strip()
    merged["sub_org_candidates"] = merge_unique_request_string_values(
        merged.get("sub_org_candidates"),
        patch.get("sub_org_candidates"),
        target_company=target_company,
    )[:6]
    if "confidence" not in merged and patch.get("confidence") is not None:
        merged["confidence"] = patch.get("confidence")
    if not str(merged.get("rationale") or "").strip():
        merged["rationale"] = str(patch.get("rationale") or "").strip()
    source = str(merged.get("source") or "").strip().lower()
    if source not in {"llm", "hybrid", "rules"}:
        merged["source"] = str(patch.get("source") or "rules").strip().lower() or "rules"
    return merged


def _remove_role_abbreviation_noise(
    values: list[str],
    *,
    raw_text: str,
    role_buckets: list[str],
) -> list[str]:
    if "product_management" not in role_buckets:
        return values
    text = " ".join(str(raw_text or "").lower().split())
    if "pm" not in text:
        return values
    filtered: list[str] = []
    for item in values:
        normalized = " ".join(str(item or "").split()).strip().lower()
        if normalized == "pm":
            continue
        filtered.append(str(item))
    return filtered


def build_request_preview_payload(
    *,
    request: JobRequest | dict[str, Any],
    effective_request: JobRequest | dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested = _coerce_job_request(request)
    effective = _coerce_job_request(effective_request or requested.to_record())

    preview = {
        "request_view": "effective_request" if _request_preview_changed(requested, effective) else "normalized_request",
        "raw_user_request": str(requested.raw_user_request or "").strip(),
        "query": str(effective.query or requested.query or "").strip(),
        "target_company": str(effective.target_company or requested.target_company or "").strip(),
        "target_scope": str(effective.target_scope or requested.target_scope or "").strip(),
        "asset_view": str(effective.asset_view or requested.asset_view or "").strip(),
        "retrieval_strategy": str(effective.retrieval_strategy or requested.retrieval_strategy or "").strip(),
        "categories": list(effective.categories or []),
        "employment_statuses": list(effective.employment_statuses or []),
        "organization_keywords": list(effective.organization_keywords or []),
        "keywords": list(effective.keywords or []),
        "must_have_keywords": list(effective.must_have_keywords or []),
        "must_have_facets": list(effective.must_have_facets or []),
        "must_have_primary_role_buckets": list(effective.must_have_primary_role_buckets or []),
        "intent_axes": build_request_intent_axes_payload(request=effective),
    }
    scope_disambiguation = dict(effective.scope_disambiguation or {})
    if scope_disambiguation and set(scope_disambiguation.keys()) != {"target_company"}:
        preview["scope_disambiguation"] = scope_disambiguation
    return {key: value for key, value in preview.items() if _has_preview_value(value)}


def build_request_intent_axes_payload(
    *,
    request: JobRequest | dict[str, Any],
) -> dict[str, Any]:
    existing_axes = _existing_intent_axes_payload(request)
    normalized = _coerce_job_request(request)
    execution_preferences = dict(normalized.execution_preferences or {})
    scope_disambiguation = dict(normalized.scope_disambiguation or {})
    confirmed_company_scope = [
        str(item or "").strip()
        for item in list(execution_preferences.get("confirmed_company_scope") or [])
        if str(item or "").strip()
    ]
    population_boundary = {
        "categories": list(normalized.categories or []),
        "employment_statuses": list(normalized.employment_statuses or []),
    }
    scope_boundary = {
        "target_company": str(normalized.target_company or "").strip(),
        "organization_keywords": list(normalized.organization_keywords or []),
        "confirmed_company_scope": confirmed_company_scope,
        "scope_disambiguation": scope_disambiguation,
    }
    acquisition_lane_policy = {
        "acquisition_strategy_override": str(execution_preferences.get("acquisition_strategy_override") or "").strip(),
        "use_company_employees_lane": execution_preferences.get("use_company_employees_lane"),
        "keyword_priority_only": execution_preferences.get("keyword_priority_only"),
        "former_keyword_queries_only": execution_preferences.get("former_keyword_queries_only"),
        "large_org_keyword_probe_mode": execution_preferences.get("large_org_keyword_probe_mode"),
    }
    fallback_policy = {
        "force_fresh_run": execution_preferences.get("force_fresh_run"),
        "allow_high_cost_sources": execution_preferences.get("allow_high_cost_sources"),
        "provider_people_search_query_strategy": str(execution_preferences.get("provider_people_search_query_strategy") or "").strip(),
        "provider_people_search_max_queries": execution_preferences.get("provider_people_search_max_queries"),
        "reuse_existing_roster": execution_preferences.get("reuse_existing_roster"),
        "run_former_search_seed": execution_preferences.get("run_former_search_seed"),
    }
    thematic_constraints = {
        "keywords": list(normalized.keywords or []),
        "must_have_keywords": list(normalized.must_have_keywords or []),
        "must_have_facets": list(normalized.must_have_facets or []),
        "must_have_primary_role_buckets": list(normalized.must_have_primary_role_buckets or []),
    }
    axes = {
        "population_boundary": _merge_intent_axis_payload(
            population_boundary,
            existing_axes.get("population_boundary"),
        ),
        "scope_boundary": _merge_intent_axis_payload(
            scope_boundary,
            existing_axes.get("scope_boundary"),
        ),
        "acquisition_lane_policy": _merge_intent_axis_payload(
            acquisition_lane_policy,
            existing_axes.get("acquisition_lane_policy"),
        ),
        "fallback_policy": _merge_intent_axis_payload(
            fallback_policy,
            existing_axes.get("fallback_policy"),
        ),
        "thematic_constraints": _merge_intent_axis_payload(
            thematic_constraints,
            existing_axes.get("thematic_constraints"),
        ),
    }
    return {key: value for key, value in axes.items() if value}


def resolve_request_intent_view(
    request: JobRequest | dict[str, Any],
    *,
    fallback_categories: list[str] | None = None,
    fallback_employment_statuses: list[str] | None = None,
) -> dict[str, Any]:
    normalized = _coerce_job_request(request)
    axes = build_request_intent_axes_payload(request=normalized)
    population_boundary = dict(axes.get("population_boundary") or {})
    scope_boundary = dict(axes.get("scope_boundary") or {})
    acquisition_lane_policy = dict(axes.get("acquisition_lane_policy") or {})
    fallback_policy = dict(axes.get("fallback_policy") or {})
    thematic_constraints = dict(axes.get("thematic_constraints") or {})

    target_company = str(scope_boundary.get("target_company") or normalized.target_company or "").strip()
    categories = merge_unique_request_string_values(
        population_boundary.get("categories") or fallback_categories or normalized.categories
    )
    employment_statuses = merge_unique_request_string_values(
        population_boundary.get("employment_statuses") or fallback_employment_statuses or normalized.employment_statuses
    )
    organization_keywords = merge_unique_request_string_values(
        scope_boundary.get("organization_keywords") or normalized.organization_keywords,
        target_company=target_company,
    )
    keywords = merge_unique_request_string_values(
        thematic_constraints.get("keywords") or normalized.keywords,
        target_company=target_company,
    )
    must_have_keywords = merge_unique_request_string_values(
        thematic_constraints.get("must_have_keywords") or normalized.must_have_keywords,
        target_company=target_company,
    )
    must_have_facets = merge_unique_request_string_values(
        thematic_constraints.get("must_have_facets") or normalized.must_have_facets,
        target_company=target_company,
    )
    must_have_primary_role_buckets = merge_unique_request_string_values(
        thematic_constraints.get("must_have_primary_role_buckets") or normalized.must_have_primary_role_buckets,
        target_company=target_company,
    )
    execution_preference_patch: dict[str, Any] = {}
    confirmed_company_scope = merge_unique_request_string_values(
        scope_boundary.get("confirmed_company_scope"),
        target_company=target_company,
    )
    if confirmed_company_scope:
        execution_preference_patch["confirmed_company_scope"] = confirmed_company_scope
    for source in (acquisition_lane_policy, fallback_policy):
        for key, value in source.items():
            if value in (None, "", [], {}):
                continue
            execution_preference_patch[key] = value
    execution_preferences = merge_execution_preferences(
        normalize_execution_preferences(
            {"execution_preferences": dict(normalized.execution_preferences or {})},
            target_company=target_company,
        ),
        normalize_execution_preferences(
            {"execution_preferences": execution_preference_patch},
            target_company=target_company,
        ),
    )
    return {
        "target_company": target_company,
        "categories": categories,
        "employment_statuses": employment_statuses,
        "organization_keywords": organization_keywords,
        "keywords": keywords,
        "must_have_keywords": must_have_keywords,
        "must_have_facets": must_have_facets,
        "must_have_primary_role_buckets": must_have_primary_role_buckets,
        "scope_disambiguation": dict(scope_boundary.get("scope_disambiguation") or normalized.scope_disambiguation or {}),
        "acquisition_lane_policy": acquisition_lane_policy,
        "fallback_policy": fallback_policy,
        "execution_preferences": execution_preferences,
        "intent_axes": axes,
    }


def build_effective_request_payload(
    request: JobRequest | dict[str, Any],
    *,
    intent_view: dict[str, Any] | None = None,
    fallback_categories: list[str] | None = None,
    fallback_employment_statuses: list[str] | None = None,
) -> dict[str, Any]:
    payload = (
        request.to_record()
        if isinstance(request, JobRequest)
        else dict(request or {})
    )
    resolved_intent_view = dict(
        intent_view
        or resolve_request_intent_view(
            request,
            fallback_categories=fallback_categories,
            fallback_employment_statuses=fallback_employment_statuses,
        )
    )
    effective_payload = dict(payload)
    effective_payload["target_company"] = str(resolved_intent_view.get("target_company") or payload.get("target_company") or "").strip()
    effective_payload["categories"] = list(resolved_intent_view.get("categories") or payload.get("categories") or [])
    effective_payload["employment_statuses"] = list(
        resolved_intent_view.get("employment_statuses") or payload.get("employment_statuses") or []
    )
    effective_payload["organization_keywords"] = list(
        resolved_intent_view.get("organization_keywords") or payload.get("organization_keywords") or []
    )
    effective_payload["keywords"] = list(resolved_intent_view.get("keywords") or payload.get("keywords") or [])
    effective_payload["must_have_keywords"] = list(
        resolved_intent_view.get("must_have_keywords") or payload.get("must_have_keywords") or []
    )
    effective_payload["must_have_facets"] = list(
        resolved_intent_view.get("must_have_facets") or payload.get("must_have_facets") or []
    )
    effective_payload["must_have_primary_role_buckets"] = list(
        resolved_intent_view.get("must_have_primary_role_buckets") or payload.get("must_have_primary_role_buckets") or []
    )
    effective_payload["scope_disambiguation"] = dict(
        resolved_intent_view.get("scope_disambiguation") or payload.get("scope_disambiguation") or {}
    )
    effective_payload["execution_preferences"] = dict(
        resolved_intent_view.get("execution_preferences") or payload.get("execution_preferences") or {}
    )
    effective_payload["intent_axes"] = dict(resolved_intent_view.get("intent_axes") or payload.get("intent_axes") or {})
    return effective_payload


def _coerce_job_request(value: JobRequest | dict[str, Any]) -> JobRequest:
    if isinstance(value, JobRequest):
        return value
    return JobRequest.from_payload(dict(value or {}))


def _existing_intent_axes_payload(value: JobRequest | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, JobRequest):
        payload = value.intent_axes
    elif isinstance(value, dict):
        payload = value.get("intent_axes")
    else:
        payload = None
    return dict(payload or {}) if isinstance(payload, dict) else {}


def _merge_intent_axis_payload(
    derived: dict[str, Any] | None,
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = {
        key: value
        for key, value in dict(derived or {}).items()
        if _has_preview_value(value)
    }
    for key, value in dict(existing or {}).items():
        if not _has_preview_value(value):
            continue
        merged[key] = value
    return merged


def _request_preview_changed(requested: JobRequest, effective: JobRequest) -> bool:
    for key in (
        "categories",
        "employment_statuses",
        "organization_keywords",
        "keywords",
        "must_have_keywords",
        "must_have_facets",
        "must_have_primary_role_buckets",
        "target_scope",
        "retrieval_strategy",
        "asset_view",
    ):
        requested_value = getattr(requested, key, None)
        effective_value = getattr(effective, key, None)
        if requested_value != effective_value:
            return True
    if dict(requested.scope_disambiguation or {}) != dict(effective.scope_disambiguation or {}):
        return True
    return False


def _has_preview_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(str(item or "").strip() for item in value)
    if isinstance(value, dict):
        return bool(value)
    return value is not None
