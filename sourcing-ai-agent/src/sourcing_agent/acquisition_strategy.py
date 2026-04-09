from __future__ import annotations

import re

from .domain import AcquisitionStrategyPlan, JobRequest, RetrievalPlan


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

SCOPE_HINTS = {
    "gemini": "Gemini",
    "deepmind": "Google DeepMind",
    "veo": "Veo",
    "nano banana": "Nano Banana",
    "claude": "Claude",
    "research": "Research",
    "engineering": "Engineering",
}

ROLE_HINTS = {
    "researcher": ["Researcher", "Research Scientist", "Applied Scientist"],
    "research scientist": ["Research Scientist", "Applied Scientist"],
    "engineer": ["Engineer", "Software Engineer", "Research Engineer"],
    "research engineer": ["Research Engineer", "Engineer"],
    "technical staff": ["Member of Technical Staff", "Technical Staff"],
}

KEYWORD_CANONICAL_ALIASES = {
    "post-train": "Post-train",
    "post train": "Post-train",
    "post-training": "Post-train",
    "pre-train": "Pre-train",
    "pre train": "Pre-train",
    "pre-training": "Pre-train",
}


def compile_acquisition_strategy(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
) -> AcquisitionStrategyPlan:
    text = _normalize(f"{request.raw_user_request} {request.query}")
    execution_preferences = dict(request.execution_preferences or {})
    scope_hints = _merge_scope_hints(
        inferred=_infer_scope_hints(text),
        explicit=request.organization_keywords,
        target_company=request.target_company,
    )
    strategy_type = _infer_strategy_type(request.target_company, categories, employment_statuses, scope_hints, execution_preferences)
    company_scope = _build_company_scope(request.target_company, scope_hints, strategy_type, execution_preferences)
    role_hints = _infer_role_hints(text)
    keyword_hints = _infer_keyword_hints(text, request.keywords + request.must_have_keywords)

    roster_sources = _roster_sources(strategy_type)
    search_channel_order = [
        "general_web_search_relation_check",
        "targeted_linkedin_web_search",
        "provider_people_search_api",
        "profile_detail_api",
    ]
    search_seed_queries = _build_search_seed_queries(request.target_company, company_scope, role_hints, keyword_hints, employment_statuses)
    filter_hints = _build_filter_hints(request.target_company, company_scope, role_hints, keyword_hints, employment_statuses)
    cost_policy = _build_cost_policy(strategy_type, execution_preferences)
    confirmation_points = _build_confirmation_points(
        strategy_type,
        request.target_company,
        company_scope,
        employment_statuses,
        execution_preferences,
    )
    reasoning = _build_reasoning(
        strategy_type,
        request.target_company,
        company_scope,
        retrieval_plan.strategy,
        execution_preferences,
    )

    target_population = _build_target_population(request.target_company, company_scope, categories, employment_statuses, role_hints, keyword_hints)
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


def _normalize(value: str) -> str:
    return " ".join(value.lower().split())


def _infer_scope_hints(text: str) -> list[str]:
    hints: list[str] = []
    for token, label in SCOPE_HINTS.items():
        if token in text and label not in hints:
            hints.append(label)
    quoted = re.findall(r'"([^"]+)"', text)
    for item in quoted:
        normalized = " ".join(item.split())
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
) -> list[str]:
    normalized_company = target_company.strip()
    confirmed_scope = [
        str(item).strip()
        for item in list(execution_preferences.get("confirmed_company_scope") or [])
        if str(item).strip()
    ]
    if strategy_type == "full_company_roster":
        return [normalized_company] if normalized_company else []
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
    if normalized_company.lower() == "google" and "Gemini" in scope_hints and "Google DeepMind" not in scope:
        scope.insert(1, "Google DeepMind")
    return scope


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
    if company_key in LARGE_COMPANY_KEYS or len(scope_hints) >= 2:
        return "scoped_search_roster"
    return "full_company_roster"


def _infer_role_hints(text: str) -> list[str]:
    roles: list[str] = []
    for token, candidates in ROLE_HINTS.items():
        if token in text:
            for candidate in candidates:
                if candidate not in roles:
                    roles.append(candidate)
    return roles[:5]


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
    lexical_hints = [
        ("reinforcement learning", "Reinforcement Learning"),
        ("rl", "RL"),
        ("pre-train", "Pre-train"),
        ("pre train", "Pre-train"),
        ("post-train", "Post-train"),
        ("post train", "Post-train"),
        ("multimodal", "Multimodal"),
        ("infrastructure", "Infrastructure"),
        ("infra", "Infra"),
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
    return hints[:6]


def _canonical_keyword_label(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    return KEYWORD_CANONICAL_ALIASES.get(normalized.lower(), normalized)


def _canonical_keyword_key(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    return KEYWORD_CANONICAL_ALIASES.get(normalized, normalized)


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
) -> list[str]:
    scope_fragment = " ".join(company_scope[1:] or company_scope[:1]).strip()
    company_fragment = target_company.strip()
    broad_role = role_hints[0] if role_hints else "Employee"
    focus_fragment = " ".join(keyword_hints[:2]).strip()
    former_fragment = " former" if employment_statuses == ["former"] else ""
    candidates = [
        f"{company_fragment} {focus_fragment} {broad_role}{former_fragment}".strip(),
        f"{scope_fragment} {focus_fragment} {broad_role}{former_fragment}".strip(),
        f"{company_fragment} {focus_fragment} employee{former_fragment}".strip(),
        f"{company_fragment} LinkedIn {focus_fragment} {broad_role}".strip(),
    ]
    deduped: list[str] = []
    for query in candidates:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped[:6]


def _build_filter_hints(
    target_company: str,
    company_scope: list[str],
    role_hints: list[str],
    keyword_hints: list[str],
    employment_statuses: list[str],
) -> dict[str, list[str]]:
    filters: dict[str, list[str]] = {}
    if employment_statuses == ["former"]:
        filters["past_companies"] = [target_company]
    else:
        filters["current_companies"] = [target_company]
    if company_scope[1:]:
        filters["scope_keywords"] = company_scope[1:]
    if role_hints:
        filters["job_titles"] = role_hints
    if keyword_hints:
        filters["keywords"] = keyword_hints
    return filters


def _build_confirmation_points(
    strategy_type: str,
    target_company: str,
    company_scope: list[str],
    employment_statuses: list[str],
    execution_preferences: dict[str, object],
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
    return points


def _build_cost_policy(strategy_type: str, execution_preferences: dict[str, object]) -> dict[str, object]:
    policy = {
        "default_route_when_url_known": "linkedin_profile_scraper",
        "profile_scraper_mode": "full",
        "collect_email": True,
        "provider_people_search_mode": "fallback_only",
        "provider_people_search_min_expected_results": 10,
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
    return policy


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
