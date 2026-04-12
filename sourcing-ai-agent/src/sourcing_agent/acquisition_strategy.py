from __future__ import annotations

import re
from typing import Any

from .company_registry import builtin_company_identity, normalize_company_key
from .domain import (
    AcquisitionStrategyPlan,
    JobRequest,
    RetrievalPlan,
    normalize_requested_role_bucket,
    normalize_requested_role_buckets,
)
from .query_signal_knowledge import (
    ALPHABET_COMPANY_URL,
    DEEPMIND_COMPANY_URL,
    GOOGLE_COMPANY_URL,
    canonicalize_scope_signal_label,
    default_large_org_priority_function_ids,
    match_scope_signals,
    related_company_scope_labels,
    role_bucket_function_ids,
    role_bucket_role_hints,
    role_buckets_from_text,
    scope_signal_keyword_labels,
    scope_signal_search_query_aliases,
)
from .request_normalization import resolve_request_intent_view


LARGE_COMPANY_KEYS = {
    "google",
    "alphabet",
    "meta",
    "microsoft",
    "amazon",
    "apple",
    "bytedance",
    "tiktok",
    "openai",
}

LARGE_ORG_SCOPE_COMPANY_URLS = {
    "google": GOOGLE_COMPANY_URL,
    "alphabet": ALPHABET_COMPANY_URL,
    "googledeepmind": DEEPMIND_COMPANY_URL,
    "deepmind": DEEPMIND_COMPANY_URL,
}

LARGE_ORG_PRIORITY_FUNCTION_IDS = default_large_org_priority_function_ids()
DEFAULT_PRIMARY_LOCATION = "United States"

KEYWORD_CANONICAL_ALIASES = {
    "post-train": "Post-train",
    "post train": "Post-train",
    "post-training": "Post-train",
    "pre-train": "Pre-train",
    "pre train": "Pre-train",
    "pre-training": "Pre-train",
    "chain of thought": "Chain-of-thought",
    "chain-of-thought": "Chain-of-thought",
    "inference time compute": "Inference-time compute",
    "inference-time compute": "Inference-time compute",
    "reasoning model": "Reasoning",
    "reasoning models": "Reasoning",
    "rlhf": "RLHF",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "video-generation": "Video generation",
    "multimodality": "Multimodal",
}

ACQUISITION_KEYWORD_EXCLUSION_KEYS = {
    "greater china experience",
    "chinese bilingual outreach",
    "greater_china_region_experience",
    "mainland_china_experience_or_chinese_language",
    "mainland or chinese language",
}

KEYWORD_PRIORITY_SEARCH_QUERY_ALIASES = {
    "multimodal": ["Multimodal", "Multimodality"],
    "visionlanguage": ["Vision-language", "Vision Language"],
    "videogeneration": ["Video generation"],
    "reasoning": ["Reasoning", "Reasoning model"],
    "chainofthought": ["Chain-of-thought"],
    "inferencetimecompute": ["Inference-time compute"],
    "rlhf": ["RLHF", "Reinforcement Learning from Human Feedback"],
    "alignment": ["Alignment"],
    "infrastructure": ["Infrastructure", "Infra"],
    "agentic": ["Agentic"],
    "agents": ["Agents"],
}

KEYWORD_PRIORITY_SKIP_KEYS = {
    "research",
    "researcher",
    "employee",
    "employees",
    "engineering",
    "engineer",
}


def compile_acquisition_strategy(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
) -> AcquisitionStrategyPlan:
    raw_text = f"{request.raw_user_request} {request.query}".strip()
    text = _normalize(raw_text)
    intent_view = resolve_request_intent_view(
        request,
        fallback_categories=categories,
        fallback_employment_statuses=employment_statuses,
    )
    intent_axes = dict(intent_view.get("intent_axes") or {})
    population_boundary = dict(intent_axes.get("population_boundary") or {})
    scope_boundary = dict(intent_axes.get("scope_boundary") or {})
    effective_target_company = str(intent_view.get("target_company") or request.target_company or "").strip()
    effective_categories = list(intent_view.get("categories") or population_boundary.get("categories") or categories or [])
    effective_employment_statuses = list(
        intent_view.get("employment_statuses") or population_boundary.get("employment_statuses") or employment_statuses or []
    )
    effective_organization_keywords = list(intent_view.get("organization_keywords") or scope_boundary.get("organization_keywords") or [])
    effective_keywords = list(intent_view.get("keywords") or [])
    effective_must_have_keywords = list(intent_view.get("must_have_keywords") or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or [])
    effective_role_buckets = list(intent_view.get("must_have_primary_role_buckets") or [])
    execution_preferences = dict(intent_view.get("execution_preferences") or {})
    scope_hints = _merge_scope_hints(
        inferred=_infer_scope_hints(text),
        explicit=effective_organization_keywords,
        target_company=effective_target_company,
    )
    strategy_type = _infer_strategy_type(
        effective_target_company,
        effective_categories,
        effective_employment_statuses,
        scope_hints,
        execution_preferences,
    )
    company_scope = _build_company_scope(
        effective_target_company,
        scope_hints,
        strategy_type,
        execution_preferences,
        scope_disambiguation=dict(intent_view.get("scope_disambiguation") or scope_boundary.get("scope_disambiguation") or {}),
    )
    role_hints = _infer_role_hints(
        text,
        effective_role_buckets,
        effective_must_have_facets,
    )
    function_ids = _infer_function_ids(
        text,
        effective_role_buckets,
        effective_must_have_facets,
    )
    keyword_hints = _infer_keyword_hints(
        text,
        _acquisition_keyword_candidates(
            effective_keywords
            + effective_must_have_keywords
            + effective_must_have_facets
            + _keyword_like_organization_terms(
                effective_organization_keywords,
                target_company=effective_target_company,
            )
        ),
    )

    roster_sources = _roster_sources(strategy_type)
    search_channel_order = [
        "general_web_search_relation_check",
        "targeted_linkedin_web_search",
        "provider_people_search_api",
        "profile_detail_api",
    ]
    cost_policy = _build_cost_policy(
        strategy_type,
        execution_preferences,
        target_company=effective_target_company,
        company_scope=company_scope,
        keyword_hints=keyword_hints,
    )
    search_seed_queries = _build_search_seed_queries(
        effective_target_company,
        company_scope,
        role_hints,
        keyword_hints,
        effective_employment_statuses,
        keyword_priority_only=bool(cost_policy.get("keyword_priority_only")),
    )
    filter_hints = _build_filter_hints(
        effective_target_company,
        company_scope,
        role_hints,
        keyword_hints,
        effective_employment_statuses,
        strategy_type=strategy_type,
        cost_policy=cost_policy,
        function_ids=function_ids,
    )
    confirmation_points = _build_confirmation_points(
        strategy_type,
        effective_target_company,
        company_scope,
        effective_employment_statuses,
        execution_preferences,
        raw_text=raw_text,
        keyword_hints=keyword_hints,
        scope_disambiguation=dict(intent_view.get("scope_disambiguation") or scope_boundary.get("scope_disambiguation") or {}),
    )
    reasoning = _build_reasoning(
        strategy_type,
        effective_target_company,
        company_scope,
        retrieval_plan.strategy,
        execution_preferences,
    )

    target_population = _build_target_population(
        effective_target_company,
        company_scope,
        effective_categories,
        effective_employment_statuses,
        role_hints,
        keyword_hints,
    )
    return AcquisitionStrategyPlan(
        strategy_type=strategy_type,
        target_population=target_population,
        company_scope=company_scope,
        roster_sources=roster_sources,
        search_channel_order=search_channel_order,
        search_seed_queries=search_seed_queries,
        filter_hints=filter_hints,
        cost_policy=cost_policy,
        confirmation_points=confirmation_points,
        reasoning=reasoning,
    )


def _merge_execution_preferences_from_intent_axes(
    existing: dict[str, Any] | None,
    *,
    scope_boundary: dict[str, Any] | None = None,
    acquisition_lane_policy: dict[str, Any] | None = None,
    fallback_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(existing or {})
    scope_boundary = dict(scope_boundary or {})
    confirmed_company_scope = [
        str(item or "").strip()
        for item in list(scope_boundary.get("confirmed_company_scope") or [])
        if str(item or "").strip()
    ]
    if confirmed_company_scope and "confirmed_company_scope" not in merged:
        merged["confirmed_company_scope"] = confirmed_company_scope
    for source in [dict(acquisition_lane_policy or {}), dict(fallback_policy or {})]:
        for key, value in source.items():
            if key in merged or value in (None, "", [], {}):
                continue
            merged[key] = value
    return merged


def _normalize(value: str) -> str:
    return " ".join(value.lower().split())


def _infer_scope_hints(text: str) -> list[str]:
    hints: list[str] = []
    for spec in match_scope_signals(text):
        for label in list(spec.get("organization_keywords") or []):
            normalized = " ".join(str(label or "").split()).strip()
            if normalized and normalized not in hints:
                hints.append(normalized)
    quoted = re.findall(r'"([^"]+)"', text)
    for item in quoted:
        normalized = canonicalize_scope_signal_label(item)
        if normalized and normalized not in hints:
            hints.append(normalized)
    return hints[:4]


def _merge_scope_hints(inferred: list[str], explicit: list[str], target_company: str) -> list[str]:
    merged: list[str] = []
    blocked = {target_company.strip().lower()} if target_company.strip() else set()
    for item in [*explicit, *inferred]:
        normalized = " ".join(str(item or "").split()).strip()
        if not normalized or normalized.lower() in blocked or normalized in merged:
            continue
        merged.append(normalized)
    return merged[:6]


def _build_company_scope(
    target_company: str,
    scope_hints: list[str],
    strategy_type: str,
    execution_preferences: dict[str, object],
    *,
    scope_disambiguation: dict[str, Any] | None = None,
) -> list[str]:
    normalized_company = target_company.strip()
    confirmed_scope = [
        str(item).strip()
        for item in list(execution_preferences.get("confirmed_company_scope") or [])
        if str(item).strip()
    ]
    if strategy_type == "full_company_roster":
        if confirmed_scope:
            scope: list[str] = [normalized_company] if normalized_company else []
            for item in confirmed_scope:
                if normalized_company and item.lower() == normalized_company.lower():
                    continue
                if item not in scope:
                    scope.append(item)
            if scope:
                return scope
        resolved_from_llm = _scope_candidates_from_disambiguation(scope_disambiguation)
        if resolved_from_llm:
            scope: list[str] = [normalized_company] if normalized_company else []
            for item in resolved_from_llm:
                if normalized_company and item.lower() == normalized_company.lower():
                    continue
                if item not in scope:
                    scope.append(item)
            if scope:
                return scope
        scope: list[str] = [normalized_company] if normalized_company else []
        for label in related_company_scope_labels(normalized_company, scope_hints):
            if label not in scope:
                scope.append(label)
        return scope
    if confirmed_scope:
        scope: list[str] = [normalized_company] if normalized_company else []
        for item in confirmed_scope:
            if normalized_company and item.lower() == normalized_company.lower():
                continue
            if item not in scope:
                scope.append(item)
        if scope:
            return scope
    scope: list[str] = []
    if normalized_company:
        scope.append(normalized_company)
    for hint in scope_hints:
        if hint not in scope:
            scope.append(hint)
    related_labels = related_company_scope_labels(normalized_company, scope_hints)
    insert_at = 1 if normalized_company else 0
    for label in related_labels:
        if label in scope:
            continue
        scope.insert(insert_at, label)
        insert_at += 1
    return scope


def _scope_candidates_from_disambiguation(scope_disambiguation: dict[str, Any] | None) -> list[str]:
    payload = dict(scope_disambiguation or {})
    inferred_scope = str(payload.get("inferred_scope") or "").strip().lower()
    if inferred_scope not in {"sub_org_only", "both", "uncertain"}:
        return []
    raw_candidates = payload.get("sub_org_candidates")
    if isinstance(raw_candidates, str):
        candidates = [item.strip() for item in raw_candidates.split(",")]
    elif isinstance(raw_candidates, (list, tuple, set)):
        candidates = [str(item).strip() for item in raw_candidates]
    else:
        return []
    resolved: list[str] = []
    for item in candidates:
        normalized = " ".join(item.split()).strip()
        if normalized and normalized not in resolved:
            resolved.append(normalized)
    return resolved[:6]


def _infer_strategy_type(
    target_company: str,
    categories: list[str],
    employment_statuses: list[str],
    scope_hints: list[str],
    execution_preferences: dict[str, object],
) -> str:
    explicit_override = str(execution_preferences.get("acquisition_strategy_override") or "").strip().lower()
    if explicit_override in {
        "full_company_roster",
        "scoped_search_roster",
        "former_employee_search",
        "investor_firm_roster",
    }:
        return explicit_override
    normalized_categories = {str(item).strip().lower() for item in categories if str(item).strip()}
    normalized_statuses = {str(item).strip().lower() for item in employment_statuses if str(item).strip()}
    if bool(execution_preferences.get("use_company_employees_lane")) and normalized_statuses != {"former"} and "investor" not in normalized_categories:
        return "full_company_roster"
    company_key = target_company.strip().lower()
    if "investor" in categories:
        return "investor_firm_roster"
    if employment_statuses == ["former"]:
        return "former_employee_search"
    if company_key in {"google", "alphabet"}:
        if related_company_scope_labels(target_company, scope_hints):
            return "full_company_roster"
    if company_key in LARGE_COMPANY_KEYS or len(scope_hints) >= 2:
        return "scoped_search_roster"
    return "full_company_roster"


def _infer_role_hints(text: str, requested_role_buckets: list[str], requested_facets: list[str]) -> list[str]:
    normalized_requested_buckets = normalize_requested_role_buckets(requested_role_buckets) + [
        normalize_requested_role_bucket(item) for item in requested_facets
    ]
    matched_buckets = role_buckets_from_text(text)
    all_buckets = list(dict.fromkeys([*normalized_requested_buckets, *matched_buckets]))
    return role_bucket_role_hints(all_buckets)[:5]


def _infer_function_ids(text: str, requested_role_buckets: list[str], requested_facets: list[str]) -> list[str]:
    normalized_requested_buckets = normalize_requested_role_buckets(requested_role_buckets) + [
        normalize_requested_role_bucket(item) for item in requested_facets
    ]
    matched_buckets = role_buckets_from_text(text)
    all_buckets = list(dict.fromkeys([*normalized_requested_buckets, *matched_buckets]))
    return role_bucket_function_ids(all_buckets)[:4]


def _infer_keyword_hints(text: str, explicit_keywords: list[str]) -> list[str]:
    hints: list[str] = []
    seen_keys: set[str] = set()
    for keyword in explicit_keywords:
        candidate = _canonical_keyword_label(keyword)
        if not candidate:
            continue
        dedupe_key = _canonical_keyword_key(candidate)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        hints.append(candidate)
    for label in scope_signal_keyword_labels(
        [str(item.get("canonical_label") or "").strip() for item in match_scope_signals(text)]
    ):
        candidate = _canonical_keyword_label(label)
        dedupe_key = _canonical_keyword_key(candidate)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        hints.append(candidate)
    lexical_hints = [
        ("reinforcement learning", "Reinforcement Learning"),
        ("rl", "RL"),
        ("rlhf", "RLHF"),
        ("reasoning", "Reasoning"),
        ("chain of thought", "Chain-of-thought"),
        ("chain-of-thought", "Chain-of-thought"),
        ("inference time compute", "Inference-time compute"),
        ("inference-time compute", "Inference-time compute"),
        ("pre-train", "Pre-train"),
        ("pre train", "Pre-train"),
        ("post-train", "Post-train"),
        ("post train", "Post-train"),
        ("multimodal", "Multimodal"),
        ("multimodality", "Multimodal"),
        ("vision-language", "Vision-language"),
        ("vision language", "Vision-language"),
        ("video generation", "Video generation"),
        ("o3", "o3"),
        ("alignment", "Alignment"),
        ("infrastructure", "Infrastructure"),
        ("infra", "Infra"),
        ("agentic", "Agentic"),
        ("agents", "Agents"),
        ("tool use", "Tool use"),
        ("training", "Training"),
        ("research", "Research"),
        ("safety", "Safety"),
    ]
    for token, label in lexical_hints:
        if token not in text:
            continue
        candidate = _canonical_keyword_label(label)
        dedupe_key = _canonical_keyword_key(candidate)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        hints.append(candidate)
    return hints[:12]


def _keyword_priority_focus_queries(keyword_hints: list[str]) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for keyword in keyword_hints:
        label = _canonical_keyword_label(keyword)
        canonical_key = _canonical_keyword_key(label)
        if not canonical_key or canonical_key in KEYWORD_PRIORITY_SKIP_KEYS:
            continue
        alias_key = "".join(ch for ch in canonical_key.lower() if ch.isalnum())
        alias_queries = scope_signal_search_query_aliases(label) or KEYWORD_PRIORITY_SEARCH_QUERY_ALIASES.get(alias_key) or [label]
        for query in alias_queries:
            normalized = " ".join(str(query or "").split()).strip()
            if not normalized:
                continue
            dedupe_key = _search_query_dedupe_key(normalized)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            queries.append(normalized)
    return queries


def _canonical_keyword_label(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    known_scope = canonicalize_scope_signal_label(normalized)
    if known_scope and known_scope != normalized:
        return known_scope
    return KEYWORD_CANONICAL_ALIASES.get(normalized.lower(), normalized)


def _canonical_keyword_key(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    known_scope = canonicalize_scope_signal_label(normalized)
    if known_scope:
        return " ".join(str(known_scope).lower().split()).strip()
    return KEYWORD_CANONICAL_ALIASES.get(normalized, normalized)


def _search_query_dedupe_key(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _acquisition_keyword_candidates(explicit_keywords: list[str]) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for keyword in explicit_keywords:
        candidate = _canonical_keyword_label(keyword)
        if not candidate:
            continue
        key = _canonical_keyword_key(candidate)
        if key in ACQUISITION_KEYWORD_EXCLUSION_KEYS:
            continue
        if key in seen:
            continue
        seen.add(key)
        filtered.append(candidate)
    return filtered


def _keyword_like_organization_terms(values: list[str], *, target_company: str) -> list[str]:
    keyword_terms: list[str] = []
    target_key = normalize_company_key(target_company)
    for value in list(values or []):
        candidate = " ".join(str(value or "").split()).strip()
        if not candidate:
            continue
        company_key, builtin = builtin_company_identity(candidate)
        if builtin is not None:
            if company_key == target_key:
                continue
            continue
        if candidate not in keyword_terms:
            keyword_terms.append(candidate)
    return keyword_terms


def _roster_sources(strategy_type: str) -> list[str]:
    mapping = {
        "full_company_roster": ["linkedin_company_roster", "company_directory_pages", "the_org"],
        "scoped_search_roster": ["web_search_seed_queries", "linkedin_people_search", "company_suborg_sources"],
        "former_employee_search": ["web_search_seed_queries", "linkedin_people_search", "news_and_bio_pages"],
        "investor_firm_roster": ["funding_graph", "investor_firm_roster", "linkedin_people_search"],
    }
    return list(mapping.get(strategy_type, ["linkedin_company_roster"]))


def _build_search_seed_queries(
    target_company: str,
    company_scope: list[str],
    role_hints: list[str],
    keyword_hints: list[str],
    employment_statuses: list[str],
    *,
    keyword_priority_only: bool = False,
) -> list[str]:
    if keyword_priority_only and keyword_hints:
        keyword_queries = _keyword_priority_focus_queries(keyword_hints)
        if keyword_queries:
            return keyword_queries[:12]

    scope_fragment = " ".join(company_scope[1:] or company_scope[:1]).strip()
    company_fragment = target_company.strip()
    broad_role = role_hints[0] if role_hints else "Employee"
    former_fragment = " former" if employment_statuses == ["former"] else ""

    focus_fragment = " ".join(keyword_hints[:2]).strip()
    candidates = [
        f"{company_fragment} {focus_fragment} {broad_role}{former_fragment}".strip(),
        f"{scope_fragment} {focus_fragment} {broad_role}{former_fragment}".strip(),
        f"{company_fragment} {focus_fragment} employee{former_fragment}".strip(),
        f"{company_fragment} LinkedIn {focus_fragment} {broad_role}".strip(),
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for query in candidates:
        normalized = " ".join(query.split())
        if not normalized:
            continue
        signature = _search_query_dedupe_key(normalized) or normalized.lower()
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(normalized)
    return deduped[:6]


def _build_filter_hints(
    target_company: str,
    company_scope: list[str],
    role_hints: list[str],
    keyword_hints: list[str],
    employment_statuses: list[str],
    *,
    strategy_type: str,
    cost_policy: dict[str, object],
    function_ids: list[str],
) -> dict[str, list[str]]:
    large_org_keyword_probe_mode = bool(cost_policy.get("large_org_keyword_probe_mode"))
    prefer_known_scope_company_urls = large_org_keyword_probe_mode or normalize_company_key(target_company) in {"google", "alphabet"}
    if strategy_type == "full_company_roster":
        company_values = _company_scope_company_filters(
            target_company=target_company,
            company_scope=company_scope,
            prefer_known_urls=prefer_known_scope_company_urls,
        )
    else:
        company_values = [target_company.strip()] if target_company.strip() else []
    if not company_values and target_company.strip():
        company_values = [target_company.strip()]
    filters: dict[str, list[str]] = {}
    if employment_statuses == ["former"]:
        filters["past_companies"] = company_values
    else:
        filters["current_companies"] = company_values
    if strategy_type == "full_company_roster":
        filters["locations"] = [DEFAULT_PRIMARY_LOCATION]
    if large_org_keyword_probe_mode:
        filters["function_ids"] = list(dict.fromkeys([*LARGE_ORG_PRIORITY_FUNCTION_IDS, *list(function_ids or [])]))
    elif function_ids:
        filters["function_ids"] = list(function_ids)
    if company_scope[1:]:
        filters["scope_keywords"] = company_scope[1:]
    if role_hints and not (large_org_keyword_probe_mode and keyword_hints):
        filters["job_titles"] = role_hints
    if keyword_hints:
        filters["keywords"] = keyword_hints
    return filters


def _company_scope_company_filters(
    *,
    target_company: str,
    company_scope: list[str],
    prefer_known_urls: bool,
) -> list[str]:
    company_values: list[str] = []
    seen_company_signatures: set[str] = set()
    target_key = normalize_company_key(target_company)
    for item in [*company_scope, target_company]:
        normalized = " ".join(str(item or "").split()).strip()
        if not normalized:
            continue
        if prefer_known_urls:
            normalized_key = normalize_company_key(normalized)
            resolved_url = LARGE_ORG_SCOPE_COMPANY_URLS.get(normalized_key)
            if resolved_url:
                normalized = resolved_url
            elif normalized_key != target_key:
                continue
        signature = _company_scope_filter_signature(normalized)
        if signature in seen_company_signatures:
            continue
        seen_company_signatures.add(signature)
        if normalized not in company_values:
            company_values.append(normalized)
    if prefer_known_urls and normalize_company_key(target_company) in {"google", "alphabet"}:
        google_url = LARGE_ORG_SCOPE_COMPANY_URLS["google"]
        google_signature = _company_scope_filter_signature(google_url)
        if google_signature not in seen_company_signatures and google_url not in company_values:
            company_values.insert(0, google_url)
            seen_company_signatures.add(google_signature)
    return company_values


def _company_scope_filter_signature(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip().lower().rstrip("/")
    if not normalized:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", normalized)
    if match:
        return str(match.group(1) or "").strip().lower()
    return normalize_company_key(normalized)


def _build_confirmation_points(
    strategy_type: str,
    target_company: str,
    company_scope: list[str],
    employment_statuses: list[str],
    execution_preferences: dict[str, object],
    *,
    raw_text: str,
    keyword_hints: list[str],
    scope_disambiguation: dict[str, Any] | None = None,
) -> list[str]:
    points: list[str] = []
    confirmed_scope = [
        str(item).strip()
        for item in list(execution_preferences.get("confirmed_company_scope") or [])
        if str(item).strip() and str(item).strip().lower() != target_company.strip().lower()
    ]
    if strategy_type == "scoped_search_roster" and not confirmed_scope and not company_scope[1:]:
        points.append(
            f"Confirm whether the roster should be limited to {' / '.join(company_scope[1:] or [target_company])} instead of the full {target_company} organization."
        )
    if employment_statuses == ["former"]:
        points.append("Confirm whether former employees should exclude anyone who has already returned to the target company.")
    if "allow_high_cost_sources" not in execution_preferences:
        points.append("Confirm when the workflow is allowed to spend on high-cost LinkedIn provider calls instead of low-cost web search.")
    implicit_keywords = _keywords_not_explicitly_mentioned(raw_text, keyword_hints)
    if implicit_keywords:
        points.append(
            "Confirm whether these inferred topic keywords should be used for acquisition/probe shards: "
            + ", ".join(implicit_keywords[:6])
            + "."
        )
    ambiguous_terms = _potentially_ambiguous_terms_for_confirmation(
        raw_text,
        target_company,
        company_scope,
        keyword_hints,
        scope_disambiguation=scope_disambiguation,
    )
    if ambiguous_terms:
        points.append(
            "The query includes terms that may be new labs/models/abbreviations and were not confidently grounded: "
            + ", ".join(ambiguous_terms[:6])
            + ". Confirm exact meaning before live execution."
        )
    return points


def _keywords_not_explicitly_mentioned(raw_text: str, keyword_hints: list[str]) -> list[str]:
    text = " ".join(str(raw_text or "").lower().split())
    if not text:
        return []
    evidence_aliases = {
        "multimodal": ("multimodal", "multimodality", "多模态"),
        "vision-language": ("vision-language", "vision language", "视觉语言", "vlm"),
        "video generation": ("video generation", "视频生成", "文生视频"),
        "veo": ("veo",),
        "nano banana": ("nano banana",),
        "research": ("research", "研究"),
    }
    missing: list[str] = []
    for keyword in keyword_hints:
        normalized = " ".join(str(keyword or "").lower().split()).strip()
        if not normalized:
            continue
        aliases = evidence_aliases.get(normalized, (normalized,))
        if any(alias and alias in text for alias in aliases):
            continue
        if keyword not in missing:
            missing.append(str(keyword).strip())
    return missing


def _potentially_ambiguous_terms_for_confirmation(
    raw_text: str,
    target_company: str,
    company_scope: list[str],
    keyword_hints: list[str],
    *,
    scope_disambiguation: dict[str, Any] | None = None,
) -> list[str]:
    text = str(raw_text or "")
    if not text:
        return []
    known_terms = {
        normalize_company_key(target_company),
        *[normalize_company_key(item) for item in company_scope],
        *[normalize_company_key(item) for item in keyword_hints],
        "google",
        "alphabet",
        "deepmind",
        "gemini",
        "veo",
        "nanobanana",
        "multimodal",
        "multimodality",
        "research",
        "researcher",
    }
    generic_title_terms = {"researcher", "researchers", "employee", "employees", "member", "members", "team"}
    candidates: list[str] = []
    for match in re.findall(r"[\(\[（【\"“'`][^)\]）】\"”'`]{2,40}[\)\]）】\"”'`]", text):
        candidate = str(match[1:-1]).strip()
        if not re.search(r"[A-Za-z]", candidate):
            continue
        candidate_key = normalize_company_key(candidate)
        if not candidate_key:
            continue
        if candidate_key in known_terms:
            continue
        if any(term for term in known_terms if len(term) >= 3 and term in candidate_key):
            continue
        if candidate not in candidates:
            candidates.append(candidate)
    for token in re.findall(r"\b[A-Z]{2,6}\b", text):
        if normalize_company_key(token) not in known_terms and token not in candidates:
            candidates.append(token)
    for token in re.findall(r"\b[A-Z][A-Za-z0-9_-]{3,}\b", text):
        key = normalize_company_key(token)
        if not key or key in known_terms:
            continue
        if any(term for term in known_terms if len(term) >= 6 and key in term):
            continue
        if token.lower() in generic_title_terms:
            continue
        if token not in candidates:
            candidates.append(token)
    scope_payload = dict(scope_disambiguation or {})
    if str(scope_payload.get("inferred_scope") or "").strip().lower() == "uncertain":
        raw_candidates = scope_payload.get("sub_org_candidates")
        if isinstance(raw_candidates, str):
            scope_candidates = [item.strip() for item in raw_candidates.split(",")]
        elif isinstance(raw_candidates, (list, tuple, set)):
            scope_candidates = [str(item).strip() for item in raw_candidates]
        else:
            scope_candidates = []
        for item in scope_candidates:
            normalized = " ".join(str(item or "").split()).strip()
            if not normalized:
                continue
            if normalized not in candidates:
                candidates.append(normalized)
    return candidates


def _build_cost_policy(
    strategy_type: str,
    execution_preferences: dict[str, object],
    *,
    target_company: str,
    company_scope: list[str],
    keyword_hints: list[str],
) -> dict[str, object]:
    large_org_keyword_probe_mode = _should_enable_large_org_keyword_probe_mode(
        strategy_type=strategy_type,
        target_company=target_company,
        company_scope=company_scope,
        keyword_hints=keyword_hints,
    )
    policy = {
        "default_route_when_url_known": "linkedin_profile_scraper",
        "profile_scraper_mode": "full",
        "collect_email": True,
        "provider_people_search_mode": "fallback_only",
        "provider_people_search_query_strategy": "all_queries_union",
        "provider_people_search_min_expected_results": 10,
        "provider_people_search_max_queries": 8,
        "company_employees_min_batch_size": 50,
        "company_employees_start_cost_usd": 0.02,
        "profile_search_page_cost_usd": 0.10,
        "profile_scraper_cost_per_profile_usd": 0.01,
        "prefer_low_cost_web_search": True,
        "allow_cached_roster_fallback": True,
        "allow_historical_profile_inheritance": True,
        "allow_shared_provider_cache": True,
        "allow_company_employee_api": strategy_type == "full_company_roster",
        "allow_targeted_name_search_api": False,
        "parallel_search_workers": 3,
        "parallel_exploration_workers": 2,
        "public_media_results_per_query": 10,
        "search_worker_unit_budget": 8,
        "public_media_worker_unit_budget": 6,
        "exploration_worker_unit_budget": 5,
        "worker_retry_limit": 2,
        "large_org_member_threshold": 10000,
        "large_org_keyword_probe_mode": large_org_keyword_probe_mode,
        "keyword_priority_only": large_org_keyword_probe_mode,
        "former_keyword_queries_only": large_org_keyword_probe_mode,
        "high_cost_requires_approval": "allow_high_cost_sources" not in execution_preferences,
    }
    if strategy_type == "former_employee_search":
        policy["provider_people_search_min_expected_results"] = 50
    if bool(execution_preferences.get("force_fresh_run")):
        policy["allow_cached_roster_fallback"] = False
        policy["allow_historical_profile_inheritance"] = False
        policy["allow_shared_provider_cache"] = False
    if "allow_high_cost_sources" in execution_preferences:
        policy["high_cost_requires_approval"] = False
        policy["high_cost_sources_approved"] = bool(execution_preferences.get("allow_high_cost_sources"))
    if str(execution_preferences.get("precision_recall_bias") or "").strip().lower() in {
        "precision_first",
        "recall_first",
        "balanced",
    }:
        policy["precision_recall_bias"] = str(execution_preferences.get("precision_recall_bias") or "").strip().lower()
    if bool(execution_preferences.get("use_company_employees_lane")):
        policy["allow_company_employee_api"] = True
    if "large_org_keyword_probe_mode" in execution_preferences:
        policy["large_org_keyword_probe_mode"] = bool(execution_preferences.get("large_org_keyword_probe_mode"))
    if "keyword_priority_only" in execution_preferences:
        policy["keyword_priority_only"] = bool(execution_preferences.get("keyword_priority_only"))
    if "former_keyword_queries_only" in execution_preferences:
        policy["former_keyword_queries_only"] = bool(execution_preferences.get("former_keyword_queries_only"))
    if "provider_people_search_query_strategy" in execution_preferences:
        query_strategy = str(execution_preferences.get("provider_people_search_query_strategy") or "").strip().lower()
        if query_strategy in {"all_queries_union", "first_hit"}:
            policy["provider_people_search_query_strategy"] = query_strategy
    if "provider_people_search_max_queries" in execution_preferences:
        try:
            policy["provider_people_search_max_queries"] = max(1, int(execution_preferences.get("provider_people_search_max_queries") or 1))
        except (TypeError, ValueError):
            pass
    return policy


def _should_enable_large_org_keyword_probe_mode(
    *,
    strategy_type: str,
    target_company: str,
    company_scope: list[str],
    keyword_hints: list[str],
) -> bool:
    if strategy_type != "full_company_roster":
        return False
    if not keyword_hints:
        return False
    target_key = normalize_company_key(target_company)
    if target_key not in {"google", "alphabet"}:
        return False
    scope_tokens = {normalize_company_key(item) for item in company_scope if item}
    trigger_tokens = {"deepmind", "googledeepmind", "gemini", "veo", "nanobanana"}
    keyword_tokens = {normalize_company_key(item) for item in keyword_hints if item}
    if not (scope_tokens & trigger_tokens or keyword_tokens & trigger_tokens):
        return False
    return len(keyword_tokens) >= 2


def _build_reasoning(
    strategy_type: str,
    target_company: str,
    company_scope: list[str],
    retrieval_strategy: str,
    execution_preferences: dict[str, object],
) -> list[str]:
    reasoning = []
    if strategy_type == "scoped_search_roster":
        reasoning.append(
            f"{target_company} is large enough that company-wide roster acquisition would be high-cost and noisy, so the plan prefers a scoped roster."
        )
    elif strategy_type == "former_employee_search":
        reasoning.append("Former-employee tasks should prioritize people search and relation verification over a current-company roster.")
    elif strategy_type == "investor_firm_roster":
        reasoning.append("Investor tasks should start from the investment graph before enumerating firm members.")
    else:
        reasoning.append("The current request is narrow enough that a full company roster is still a practical baseline.")
    if company_scope[1:]:
        reasoning.append(f"Current scope hints: {', '.join(company_scope[1:])}.")
    reasoning.append(f"Retrieval is expected to run in {retrieval_strategy} mode after acquisition.")
    reasoning.append("Slug resolution should stay low-cost first: relation check and public web search before paid LinkedIn people search.")
    if str(execution_preferences.get("acquisition_strategy_override") or "").strip().lower() == "full_company_roster":
        reasoning.append("The user explicitly requested a full-company roster instead of a scoped acquisition.")
    if bool(execution_preferences.get("force_fresh_run")):
        reasoning.append("The user explicitly requested a fresh run instead of cache reuse or historical inheritance.")
    if "allow_high_cost_sources" in execution_preferences:
        if bool(execution_preferences.get("allow_high_cost_sources")):
            reasoning.append("The user explicitly approved high-cost sources during plan compilation.")
        else:
            reasoning.append("The user explicitly disallowed high-cost sources during plan compilation.")
    if bool(execution_preferences.get("use_company_employees_lane")):
        reasoning.append("The plan uses the Harvest company-employees lane because the user asked for a broad current-company roster.")
    return reasoning


def _build_target_population(
    target_company: str,
    company_scope: list[str],
    categories: list[str],
    employment_statuses: list[str],
    role_hints: list[str],
    keyword_hints: list[str],
) -> str:
    segments = []
    if company_scope[1:]:
        segments.append(f"{' / '.join(company_scope[1:])} within {target_company}")
    else:
        segments.append(target_company or "target company")
    if categories:
        segments.append(f"categories={categories}")
    if employment_statuses:
        segments.append(f"employment_statuses={employment_statuses}")
    if role_hints:
        segments.append(f"roles={role_hints}")
    if keyword_hints:
        segments.append(f"focus={keyword_hints}")
    return "; ".join(segment for segment in segments if segment)
