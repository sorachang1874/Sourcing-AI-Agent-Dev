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
    "claude": "Claude",
    "research": "Research",
    "engineering": "Engineering",
    "multimodal": "Multimodal",
    "post-train": "Post-train",
    "post train": "Post-train",
    "pre-train": "Pre-train",
    "pre train": "Pre-train",
    "rl": "Reinforcement Learning",
    "reinforcement learning": "Reinforcement Learning",
    "safety": "Safety",
}

ROLE_HINTS = {
    "researcher": ["Researcher", "Research Scientist", "Applied Scientist"],
    "research scientist": ["Research Scientist", "Applied Scientist"],
    "engineer": ["Engineer", "Software Engineer", "Research Engineer"],
    "research engineer": ["Research Engineer", "Engineer"],
    "technical staff": ["Member of Technical Staff", "Technical Staff"],
}


def compile_acquisition_strategy(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
) -> AcquisitionStrategyPlan:
    text = _normalize(f"{request.raw_user_request} {request.query}")
    scope_hints = _infer_scope_hints(text)
    company_scope = _build_company_scope(request.target_company, scope_hints)
    strategy_type = _infer_strategy_type(request.target_company, categories, employment_statuses, scope_hints)
    role_hints = _infer_role_hints(text)
    keyword_hints = _infer_keyword_hints(text, request.keywords)

    roster_sources = _roster_sources(strategy_type)
    search_channel_order = [
        "general_web_search_relation_check",
        "targeted_linkedin_web_search",
        "provider_people_search_api",
        "profile_detail_api",
    ]
    search_seed_queries = _build_search_seed_queries(request.target_company, company_scope, role_hints, keyword_hints, employment_statuses)
    filter_hints = _build_filter_hints(request.target_company, company_scope, role_hints, keyword_hints, employment_statuses)
    cost_policy = _build_cost_policy(strategy_type)
    confirmation_points = _build_confirmation_points(strategy_type, request.target_company, company_scope, employment_statuses)
    reasoning = _build_reasoning(strategy_type, request.target_company, company_scope, retrieval_plan.strategy)

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


def _build_company_scope(target_company: str, scope_hints: list[str]) -> list[str]:
    scope: list[str] = []
    normalized_company = target_company.strip()
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
) -> str:
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
    for keyword in explicit_keywords:
        candidate = keyword.strip()
        if candidate and candidate not in hints:
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
        if token in text and label not in hints:
            hints.append(label)
    return hints[:6]


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
) -> list[str]:
    points: list[str] = []
    if strategy_type == "scoped_search_roster":
        points.append(
            f"Confirm whether the roster should be limited to {' / '.join(company_scope[1:] or [target_company])} instead of the full {target_company} organization."
        )
    if employment_statuses == ["former"]:
        points.append("Confirm whether former employees should exclude anyone who has already returned to the target company.")
    points.append("Confirm when the workflow is allowed to spend on high-cost LinkedIn provider calls instead of low-cost web search.")
    return points


def _build_cost_policy(strategy_type: str) -> dict[str, object]:
    policy = {
        "default_route_when_url_known": "linkedin_profile_scraper",
        "profile_scraper_mode": "full",
        "collect_email": False,
        "provider_people_search_mode": "fallback_only",
        "provider_people_search_min_expected_results": 10,
        "company_employees_min_batch_size": 50,
        "company_employees_start_cost_usd": 0.02,
        "profile_search_page_cost_usd": 0.10,
        "profile_scraper_cost_per_profile_usd": 0.004,
        "prefer_low_cost_web_search": True,
        "allow_company_employee_api": strategy_type == "full_company_roster",
        "allow_targeted_name_search_api": False,
        "parallel_search_workers": 3,
        "parallel_exploration_workers": 2,
        "public_media_results_per_query": 10,
        "search_worker_unit_budget": 8,
        "public_media_worker_unit_budget": 6,
        "exploration_worker_unit_budget": 5,
        "worker_retry_limit": 2,
    }
    if strategy_type == "former_employee_search":
        policy["provider_people_search_min_expected_results"] = 50
    return policy


def _build_reasoning(
    strategy_type: str,
    target_company: str,
    company_scope: list[str],
    retrieval_strategy: str,
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
