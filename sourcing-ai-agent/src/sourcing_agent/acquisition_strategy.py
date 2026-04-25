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
from .organization_execution_profile import FALLBACK_LARGE_COMPANY_KEYS
from .query_signal_knowledge import (
    ALPHABET_COMPANY_URL,
    DEEPMIND_COMPANY_URL,
    GOOGLE_COMPANY_URL,
    canonicalize_scope_signal_label,
    default_large_org_priority_function_ids,
    match_scope_signals,
    related_company_scope_labels,
    related_company_scope_urls,
    role_bucket_function_ids,
    role_bucket_role_hints,
    role_buckets_from_text,
    scope_signal_keyword_labels,
)
from .request_normalization import build_effective_job_request, coerce_intent_axis_mapping
from .semantic_intent import compile_semantic_brief, semantic_brief_function_target_groups

LARGE_ORG_SCOPE_COMPANY_URLS = {
    "google": GOOGLE_COMPANY_URL,
    "alphabet": ALPHABET_COMPANY_URL,
    "googledeepmind": DEEPMIND_COMPANY_URL,
    "deepmind": DEEPMIND_COMPANY_URL,
}

LARGE_ORG_PRIORITY_FUNCTION_IDS = default_large_org_priority_function_ids()
DEFAULT_PRIMARY_LOCATION = "United States"
FULL_COMPANY_TECHNICAL_ROSTER_FUNCTION_IDS = ["8", "24"]

KEYWORD_CANONICAL_ALIASES = {
    "coding": "Coding",
    "math": "Math",
    "text": "Text",
    "audio": "Audio",
    "infra": "Infra",
    "vision": "Vision",
    "multimodal": "Multimodal",
    "reasoning": "Reasoning",
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
    "推理": "Reasoning",
    "rl": "RL",
    "reinforcement learning": "RL",
    "强化学习": "RL",
    "rlhf": "RLHF",
    "eval": "Eval",
    "evals": "Eval",
    "evaluation": "Eval",
    "model evaluation": "Eval",
    "alignment evaluation": "Eval",
    "评估": "Eval",
    "评测": "Eval",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "video-generation": "Video generation",
    "多模态": "Multimodal",
    "multimodality": "Multimodal",
    "预训练": "Pre-train",
    "后训练": "Post-train",
    "world modeling": "World model",
    "world model": "World model",
    "world models": "World model",
    "世界模型": "World model",
    "基础设施": "Infra",
}

ACQUISITION_KEYWORD_EXCLUSION_KEYS = {
    "greater china experience",
    "chinese bilingual outreach",
    "greater_china_region_experience",
    "mainland_china_experience_or_chinese_language",
    "mainland or chinese language",
}

KEYWORD_PRIORITY_SKIP_KEYS = {
    "research",
    "researcher",
    "employee",
    "employees",
    "engineering",
    "engineer",
}

_DIRECTIONAL_SCOPE_HINT_TERMS = (
    "方向",
    "方向的",
    "方向上",
    "topic",
    "topics",
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


def compile_acquisition_strategy(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
    organization_execution_profile: dict[str, Any] | None = None,
) -> AcquisitionStrategyPlan:
    effective_request, intent_view = build_effective_job_request(
        request,
        fallback_categories=categories,
        fallback_employment_statuses=employment_statuses,
    )
    raw_text = f"{effective_request.raw_user_request} {effective_request.query}".strip()
    text = _normalize(raw_text)
    intent_axes = coerce_intent_axis_mapping(intent_view.get("intent_axes"))
    population_boundary = coerce_intent_axis_mapping(intent_axes.get("population_boundary"))
    scope_boundary = coerce_intent_axis_mapping(intent_axes.get("scope_boundary"))
    effective_target_company = str(effective_request.target_company or request.target_company or "").strip()
    effective_categories = list(effective_request.categories or population_boundary.get("categories") or categories or [])
    effective_employment_statuses = list(
        effective_request.employment_statuses or population_boundary.get("employment_statuses") or employment_statuses or []
    )
    effective_organization_keywords = list(
        intent_view.get("organization_keywords") or effective_request.organization_keywords or scope_boundary.get("organization_keywords") or []
    )
    effective_keywords = list(intent_view.get("keywords") or effective_request.keywords or [])
    effective_must_have_keywords = list(intent_view.get("must_have_keywords") or effective_request.must_have_keywords or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or effective_request.must_have_facets or [])
    requested_role_buckets = list(
        intent_view.get("must_have_primary_role_buckets")
        or effective_request.must_have_primary_role_buckets
        or []
    )
    category_role_buckets = normalize_requested_role_buckets(effective_categories)
    primary_role_bucket_mode = str(getattr(effective_request, "primary_role_bucket_mode", "") or intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() or "hard"
    execution_preferences = dict(effective_request.execution_preferences or {})
    semantic_brief = dict(
        intent_view.get("semantic_brief")
        or compile_semantic_brief(
            raw_text=raw_text,
            target_company=effective_target_company,
            target_scope=str(effective_request.target_scope or request.target_scope or "").strip(),
            categories=effective_categories,
            employment_statuses=effective_employment_statuses,
            organization_keywords=effective_organization_keywords,
            keywords=effective_keywords,
            must_have_keywords=effective_must_have_keywords,
            must_have_facets=effective_must_have_facets,
            must_have_primary_role_buckets=requested_role_buckets,
            execution_preferences=execution_preferences,
            scope_disambiguation=dict(
                effective_request.scope_disambiguation
                or intent_view.get("scope_disambiguation")
                or scope_boundary.get("scope_disambiguation")
                or {}
            ),
        )
    )
    scope_semantics = dict(semantic_brief.get("scope") or {})
    role_targeting = dict(semantic_brief.get("role_targeting") or {})
    effective_role_buckets = list(
        role_targeting.get("resolved_role_buckets")
        or requested_role_buckets
        or []
    )
    scope_hints = list(
        scope_semantics.get("scope_hints")
        or _merge_scope_hints(
            inferred=_infer_scope_hints(text),
            explicit=effective_organization_keywords,
            target_company=effective_target_company,
        )
    )
    strategy_decision = _determine_strategy_decision(
        target_company=effective_target_company,
        text=text,
        categories=effective_categories,
        employment_statuses=effective_employment_statuses,
        scope_hints=scope_hints,
        execution_preferences=execution_preferences,
        organization_keywords=effective_organization_keywords,
        keywords=effective_keywords,
        must_have_keywords=effective_must_have_keywords,
        must_have_facets=effective_must_have_facets,
        role_buckets=effective_role_buckets,
        semantic_brief=semantic_brief,
        organization_execution_profile=organization_execution_profile,
    )
    strategy_type = str(strategy_decision.get("strategy_type") or "")
    company_scope = _build_company_scope(
        effective_target_company,
        scope_hints,
        strategy_type,
        execution_preferences,
        scope_disambiguation=dict(effective_request.scope_disambiguation or intent_view.get("scope_disambiguation") or scope_boundary.get("scope_disambiguation") or {}),
    )
    function_target_groups = semantic_brief_function_target_groups(semantic_brief)
    role_hints = _grouped_role_hints(function_target_groups)
    function_ids = _grouped_function_ids(function_target_groups)
    if not role_hints:
        role_hints = _infer_role_hints(
            text,
            [*category_role_buckets, *(effective_role_buckets if primary_role_bucket_mode == "hard" else [])],
            effective_must_have_facets,
        )
    if not function_ids:
        function_ids = _infer_function_ids(
            text,
            [*category_role_buckets, *effective_role_buckets],
            effective_must_have_facets,
            categories=effective_categories,
            keyword_hints=effective_keywords + effective_must_have_keywords,
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
        prefer_explicit_keyword_focus=strategy_type == "scoped_search_roster",
    )

    roster_sources = _roster_sources(strategy_type)
    search_channel_order = _search_channel_order(strategy_type)
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
        function_target_groups,
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
        scope_disambiguation=dict(effective_request.scope_disambiguation or intent_view.get("scope_disambiguation") or scope_boundary.get("scope_disambiguation") or {}),
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
        organization_execution_profile=dict(strategy_decision.get("organization_execution_profile") or {}),
        strategy_decision_explanation={
            **dict(strategy_decision or {}),
            "semantic_brief": semantic_brief,
            "function_target_groups": function_target_groups,
        },
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


def _grouped_role_hints(function_target_groups: list[dict[str, Any]]) -> list[str]:
    hints: list[str] = []
    for item in list(function_target_groups or []):
        primary_role_hint = str(item.get("primary_role_hint") or "").strip()
        if primary_role_hint and primary_role_hint not in hints:
            hints.append(primary_role_hint)
        for role_hint in list(item.get("role_hints") or []):
            normalized = str(role_hint or "").strip()
            if normalized and normalized not in hints:
                hints.append(normalized)
    return hints[:6]


def _grouped_function_ids(function_target_groups: list[dict[str, Any]]) -> list[str]:
    function_ids: list[str] = []
    for item in list(function_target_groups or []):
        for function_id in list(item.get("function_ids") or []):
            normalized = str(function_id or "").strip()
            if normalized and normalized not in function_ids:
                function_ids.append(normalized)
    return function_ids[:6]


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
    if company_key in FALLBACK_LARGE_COMPANY_KEYS or len(scope_hints) >= 2:
        return "scoped_search_roster"
    return "full_company_roster"


def _is_directional_company_query(
    *,
    text: str,
    scope_hints: list[str],
    organization_keywords: list[str],
    keywords: list[str],
    must_have_keywords: list[str],
    must_have_facets: list[str],
    role_buckets: list[str],
) -> bool:
    if len(scope_hints) >= 2:
        return True
    if organization_keywords or keywords or must_have_keywords or must_have_facets or role_buckets:
        return True
    return any(token in text for token in _DIRECTIONAL_SCOPE_HINT_TERMS)


def _is_thematic_or_role_constrained_query(
    *,
    text: str,
    keywords: list[str],
    must_have_keywords: list[str],
    must_have_facets: list[str],
    role_buckets: list[str],
) -> bool:
    if keywords or must_have_keywords or must_have_facets or role_buckets:
        return True
    return any(token in text for token in _DIRECTIONAL_SCOPE_HINT_TERMS)


def _is_explicit_full_roster_intent(
    text: str,
    *,
    thematic_or_role_constrained: bool = False,
) -> bool:
    if not text:
        return False
    strong_full_roster_terms = [
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
    ]
    if any(term in text for term in strong_full_roster_terms):
        return True

    weak_full_roster_terms = [
        "all members",
        "all employees",
        "所有成员",
        "全部成员",
        "全体成员",
        "全量",
        "全员",
    ]
    if thematic_or_role_constrained:
        return False
    return any(term in text for term in weak_full_roster_terms)


def _determine_strategy_decision(
    *,
    target_company: str,
    text: str,
    categories: list[str],
    employment_statuses: list[str],
    scope_hints: list[str],
    execution_preferences: dict[str, object],
    organization_keywords: list[str],
    keywords: list[str],
    must_have_keywords: list[str],
    must_have_facets: list[str],
    role_buckets: list[str],
    semantic_brief: dict[str, Any] | None = None,
    organization_execution_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_profile = dict(organization_execution_profile or {})
    normalized_categories = {str(item).strip().lower() for item in categories if str(item).strip()}
    normalized_statuses = {str(item).strip().lower() for item in employment_statuses if str(item).strip()}
    scope_semantics = dict(dict(semantic_brief or {}).get("scope") or {})
    thematic_focus = dict(dict(semantic_brief or {}).get("thematic_focus") or {})
    directional_query = bool(
        scope_semantics.get("directional_query")
        if scope_semantics
        else _is_directional_company_query(
            text=text,
            scope_hints=scope_hints,
            organization_keywords=organization_keywords,
            keywords=keywords,
            must_have_keywords=must_have_keywords,
            must_have_facets=must_have_facets,
            role_buckets=role_buckets,
        )
    )
    thematic_or_role_constrained_query = bool(
        thematic_focus.get("thematic_or_role_constrained")
        if thematic_focus
        else _is_thematic_or_role_constrained_query(
            text=text,
            keywords=keywords,
            must_have_keywords=must_have_keywords,
            must_have_facets=must_have_facets,
            role_buckets=role_buckets,
        )
    )
    explicit_override = str(execution_preferences.get("acquisition_strategy_override") or "").strip().lower()
    if explicit_override in {
        "full_company_roster",
        "scoped_search_roster",
        "former_employee_search",
        "investor_firm_roster",
    }:
        return {
            "strategy_type": explicit_override,
            "decision_source": "explicit_override",
            "reason_codes": ["explicit_strategy_override"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if bool(execution_preferences.get("use_company_employees_lane")) and normalized_statuses != {"former"} and "investor" not in normalized_categories:
        return {
            "strategy_type": "full_company_roster",
            "decision_source": "execution_preferences",
            "reason_codes": ["use_company_employees_lane"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if "investor" in normalized_categories:
        return {
            "strategy_type": "investor_firm_roster",
            "decision_source": "population",
            "reason_codes": ["investor_population"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if employment_statuses == ["former"]:
        return {
            "strategy_type": "former_employee_search",
            "decision_source": "population",
            "reason_codes": ["former_only_population"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if bool(scope_semantics.get("explicit_full_roster_intent")) or _is_explicit_full_roster_intent(
        text,
        thematic_or_role_constrained=thematic_or_role_constrained_query,
    ):
        return {
            "strategy_type": "full_company_roster",
            "decision_source": "request_semantics",
            "reason_codes": ["explicit_full_roster_intent"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    default_mode = str(normalized_profile.get("default_acquisition_mode") or "").strip().lower()
    if default_mode == "scoped_search_roster":
        return {
            "strategy_type": "scoped_search_roster",
            "decision_source": "organization_execution_profile",
            "reason_codes": ["org_profile_default_scoped_search"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if default_mode == "hybrid":
        return {
            "strategy_type": "scoped_search_roster" if directional_query else "full_company_roster",
            "decision_source": "organization_execution_profile",
            "reason_codes": [
                "org_profile_default_hybrid",
                "hybrid_directional_query" if directional_query else "hybrid_broad_query",
            ],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    if default_mode == "full_company_roster":
        return {
            "strategy_type": "full_company_roster",
            "decision_source": "organization_execution_profile",
            "reason_codes": ["org_profile_default_full_roster"],
            "directional_query": directional_query,
            "organization_execution_profile": normalized_profile,
        }
    fallback_strategy_type = _infer_strategy_type(
        target_company,
        categories,
        employment_statuses,
        scope_hints,
        execution_preferences,
    )
    return {
        "strategy_type": fallback_strategy_type,
        "decision_source": "fallback_rules",
        "reason_codes": ["fallback_rule_strategy"],
        "directional_query": directional_query,
        "organization_execution_profile": normalized_profile,
    }


def _infer_role_hints(text: str, requested_role_buckets: list[str], requested_facets: list[str]) -> list[str]:
    normalized_requested_buckets = normalize_requested_role_buckets(requested_role_buckets) + [
        normalize_requested_role_bucket(item) for item in requested_facets
    ]
    matched_buckets = role_buckets_from_text(text)
    all_buckets = list(dict.fromkeys([*normalized_requested_buckets, *matched_buckets]))
    return role_bucket_role_hints(all_buckets)[:5]


def _infer_function_ids(
    text: str,
    requested_role_buckets: list[str],
    requested_facets: list[str],
    *,
    categories: list[str] | None = None,
    keyword_hints: list[str] | None = None,
) -> list[str]:
    normalized_requested_buckets = normalize_requested_role_buckets(requested_role_buckets) + [
        normalize_requested_role_bucket(item) for item in requested_facets
    ]
    matched_buckets = role_buckets_from_text(text)
    all_buckets = list(dict.fromkeys([*normalized_requested_buckets, *matched_buckets]))
    function_ids = role_bucket_function_ids(all_buckets)[:4]
    if _should_expand_directional_function_ids(
        text=text,
        function_ids=function_ids,
        categories=categories or [],
        keyword_hints=keyword_hints or [],
    ):
        function_ids = list(dict.fromkeys([*function_ids, "24", "8"]))
    return function_ids[:4]


def _should_expand_directional_function_ids(
    *,
    text: str,
    function_ids: list[str],
    categories: list[str],
    keyword_hints: list[str],
) -> bool:
    if not keyword_hints:
        return False
    normalized_text = " ".join(str(text or "").lower().split()).strip()
    if not normalized_text:
        return False
    if not any(token in normalized_text for token in _DIRECTIONAL_SCOPE_HINT_TERMS):
        return False
    normalized_categories = {
        str(item or "").strip().lower()
        for item in list(categories or [])
        if str(item or "").strip()
    }
    if normalized_categories and not normalized_categories.issubset({"employee", "former_employee"}):
        return False
    normalized_function_ids = {str(item or "").strip() for item in list(function_ids or []) if str(item or "").strip()}
    if normalized_function_ids and not normalized_function_ids.issubset({"24", "8"}):
        return False
    return True


def _infer_keyword_hints(
    text: str,
    explicit_keywords: list[str],
    *,
    prefer_explicit_keyword_focus: bool = False,
) -> list[str]:
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
    if prefer_explicit_keyword_focus and hints:
        return hints[:12]
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
        ("world model", "World model"),
        ("world models", "World model"),
        ("世界模型", "World model"),
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
        normalized = " ".join(str(label or "").split()).strip()
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
        "full_company_roster": ["harvest_company_employees", "company_directory_pages", "the_org"],
        "scoped_search_roster": ["harvest_profile_search", "company_suborg_sources"],
        "former_employee_search": ["harvest_profile_search", "news_and_bio_pages"],
        "investor_firm_roster": ["funding_graph", "investor_firm_roster", "linkedin_people_search"],
    }
    return list(mapping.get(strategy_type, ["linkedin_company_roster"]))


def _search_channel_order(strategy_type: str) -> list[str]:
    normalized_strategy = str(strategy_type or "").strip().lower()
    if normalized_strategy == "full_company_roster":
        return ["harvest_company_employees", "harvest_profile_search", "profile_detail_api"]
    if normalized_strategy in {"scoped_search_roster", "former_employee_search"}:
        return ["harvest_profile_search", "profile_detail_api"]
    if normalized_strategy == "investor_firm_roster":
        return ["funding_graph", "provider_people_search_api", "profile_detail_api"]
    return ["profile_detail_api"]


def _build_search_seed_queries(
    target_company: str,
    company_scope: list[str],
    role_hints: list[str],
    function_target_groups: list[dict[str, Any]],
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
    former_fragment = " former" if employment_statuses == ["former"] else ""
    focus_fragment = " ".join(keyword_hints[:2]).strip()
    candidate_roles = [
        str(item.get("primary_role_hint") or "").strip()
        for item in list(function_target_groups or [])
        if str(item.get("primary_role_hint") or "").strip()
    ]
    if not candidate_roles:
        candidate_roles = [str(item).strip() for item in list(role_hints or []) if str(item).strip()]
    if not candidate_roles:
        candidate_roles = ["Employee"]

    candidates: list[str] = []
    for role in candidate_roles[:3]:
        candidates.extend(
            [
                f"{company_fragment} {focus_fragment} {role}{former_fragment}".strip(),
                f"{scope_fragment} {focus_fragment} {role}{former_fragment}".strip(),
                f"{company_fragment} LinkedIn {focus_fragment} {role}".strip(),
            ]
        )
    candidates.append(f"{company_fragment} {focus_fragment} employee{former_fragment}".strip())
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
    prefer_known_scope_company_urls = bool(
        large_org_keyword_probe_mode
        or related_company_scope_urls(target_company, company_scope)
        or normalize_company_key(target_company) in LARGE_ORG_SCOPE_COMPANY_URLS
    )
    effective_function_ids = list(function_ids or [])
    if not effective_function_ids and strategy_type == "full_company_roster":
        company_key = normalize_company_key(target_company)
        if company_key in FALLBACK_LARGE_COMPANY_KEYS:
            effective_function_ids = list(FULL_COMPANY_TECHNICAL_ROSTER_FUNCTION_IDS)
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
    if strategy_type == "full_company_roster" and _full_company_roster_defaults_to_primary_location(
        target_company=target_company,
        cost_policy=cost_policy,
    ):
        filters["locations"] = [DEFAULT_PRIMARY_LOCATION]
    if large_org_keyword_probe_mode:
        filters["function_ids"] = list(dict.fromkeys([*LARGE_ORG_PRIORITY_FUNCTION_IDS, *effective_function_ids]))
    elif effective_function_ids:
        filters["function_ids"] = list(effective_function_ids)
    if company_scope[1:]:
        filters["scope_keywords"] = company_scope[1:]
    if role_hints and not effective_function_ids and not (large_org_keyword_probe_mode and keyword_hints):
        filters["job_titles"] = role_hints
    if keyword_hints:
        filters["keywords"] = keyword_hints
    return filters


def _full_company_roster_defaults_to_primary_location(
    *,
    target_company: str,
    cost_policy: dict[str, object],
) -> bool:
    if bool(cost_policy.get("large_org_keyword_probe_mode")):
        return True
    return normalize_company_key(target_company) in FALLBACK_LARGE_COMPANY_KEYS


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
    if prefer_known_urls:
        primary_url = LARGE_ORG_SCOPE_COMPANY_URLS.get(normalize_company_key(target_company))
        if primary_url:
            primary_signature = _company_scope_filter_signature(primary_url)
            if primary_signature not in seen_company_signatures and primary_url not in company_values:
                company_values.insert(0, primary_url)
                seen_company_signatures.add(primary_signature)
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
    company_scope_tokens = {
        normalize_company_key(token)
        for value in [target_company, *list(company_scope or [])]
        for token in re.findall(r"[A-Za-z]{2,}", str(value or ""))
        if normalize_company_key(token)
    }
    known_terms = {
        normalize_company_key(target_company),
        *[normalize_company_key(item) for item in company_scope],
        *company_scope_tokens,
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
        "collect_email": False,
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
        "keyword_priority_only": large_org_keyword_probe_mode or (strategy_type == "scoped_search_roster" and bool(keyword_hints)),
        "former_keyword_queries_only": large_org_keyword_probe_mode,
        "former_broad_past_company_only": strategy_type == "full_company_roster" and not large_org_keyword_probe_mode,
    }
    if strategy_type == "former_employee_search":
        policy["provider_people_search_min_expected_results"] = 50
    if bool(execution_preferences.get("force_fresh_run")):
        policy["allow_cached_roster_fallback"] = False
        policy["allow_historical_profile_inheritance"] = False
        policy["allow_shared_provider_cache"] = False
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
    _apply_scoped_search_provider_policy(
        policy,
        strategy_type=strategy_type,
        execution_preferences=execution_preferences,
    )
    return policy


def _apply_scoped_search_provider_policy(
    policy: dict[str, object],
    *,
    strategy_type: str,
    execution_preferences: dict[str, object],
) -> None:
    if strategy_type != "scoped_search_roster":
        return
    if "former_keyword_queries_only" not in execution_preferences:
        policy["former_keyword_queries_only"] = True
    policy["provider_people_search_mode"] = "primary_only"
    try:
        current_min_expected = int(policy.get("provider_people_search_min_expected_results") or 0)
    except (TypeError, ValueError):
        current_min_expected = 0
    try:
        current_pages = int(policy.get("provider_people_search_pages") or 0)
    except (TypeError, ValueError):
        current_pages = 0
    policy["provider_people_search_min_expected_results"] = max(current_min_expected, 50)
    policy["provider_people_search_pages"] = max(current_pages, 2)


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
    scope_tokens = {normalize_company_key(item) for item in company_scope if item}
    keyword_tokens = {normalize_company_key(item) for item in keyword_hints if item}
    related_scope_urls = related_company_scope_urls(target_company, company_scope)
    if len(keyword_tokens) < 2:
        return False
    if related_scope_urls:
        return True
    non_root_scope_tokens = {
        token
        for token in scope_tokens
        if token and token != normalize_company_key(target_company)
    }
    return bool(non_root_scope_tokens)


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
