from __future__ import annotations

import re
from typing import Any

from .domain import (
    AcquisitionStrategyPlan,
    JobRequest,
    PublicationCoveragePlan,
    SearchQueryBundle,
    SearchStrategyPlan,
)
from .model_provider import ModelClient
from .query_signal_knowledge import naturalize_search_query_terms
from .request_normalization import resolve_request_intent_view

MODEL_WRITTEN_SEARCH_PLANNING_MODES = {"llm_brief", "product_brief_model_assisted"}
LINKEDIN_STAGE1_QUERY_SOURCE_FAMILIES = {
    "former_employee_search",
    "harvest_profile_search",
    "linkedin_people_search",
    "people_search",
    "targeted_people_search",
}
LINKEDIN_STAGE1_QUERY_EXECUTION_MODES = {
    "harvest_profile_search",
    "linkedin_people_search",
    "paid_fallback",
    "provider_people_search",
}


def compile_search_strategy(
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
    publication_coverage: PublicationCoveragePlan,
    model_client: ModelClient,
) -> SearchStrategyPlan:
    deterministic = _deterministic_search_strategy(
        request,
        acquisition_strategy,
        publication_coverage,
        provider_name=model_client.provider_name(),
    )
    allow_web_seed_fallback = _stage1_web_seed_fallback_enabled(
        request=request,
        acquisition_strategy=acquisition_strategy,
    )
    if request.planning_mode.lower() not in MODEL_WRITTEN_SEARCH_PLANNING_MODES:
        deterministic.planner_mode = "deterministic"
        return _enforce_stage1_seed_policy(
            deterministic,
            allow_web_seed_fallback=allow_web_seed_fallback,
        )
    draft = deterministic.to_record()
    model_payload = model_client.plan_search_strategy(
        request,
        {
            "acquisition_strategy": acquisition_strategy.to_record(),
            "publication_coverage": publication_coverage.to_record(),
            "draft_search_strategy": draft,
        },
    )
    return _enforce_stage1_seed_policy(
        _merge_strategy(deterministic, model_payload),
        allow_web_seed_fallback=allow_web_seed_fallback,
    )


def _deterministic_search_strategy(
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
    publication_coverage: PublicationCoveragePlan,
    *,
    provider_name: str,
) -> SearchStrategyPlan:
    intent_view = resolve_request_intent_view(request)
    company = str(intent_view.get("target_company") or request.target_company or "").strip() or "target company"
    scope_terms = naturalize_search_query_terms(
        list(intent_view.get("organization_keywords") or acquisition_strategy.company_scope[1:] or [company])
    )
    if not scope_terms:
        scope_terms = [company]
    distinct_scope_terms = _scope_terms_distinct_from_company(scope_terms, company)
    role_terms = list(acquisition_strategy.filter_hints.get("job_titles") or [])
    keyword_terms = naturalize_search_query_terms(
        list(acquisition_strategy.filter_hints.get("keywords") or [])
        + list(intent_view.get("keywords") or [])
        + list(intent_view.get("must_have_keywords") or [])
        + list(intent_view.get("must_have_facets") or [])
    )
    bundles: list[SearchQueryBundle] = []
    allow_web_seed_fallback = _stage1_web_seed_fallback_enabled(
        request=request,
        acquisition_strategy=acquisition_strategy,
    )

    if allow_web_seed_fallback:
        bundles.append(
            SearchQueryBundle(
                bundle_id="relationship_web",
                source_family="public_web_search",
                priority="high",
                objective="Validate person-company relationship and discover public profile URLs before paid APIs.",
                execution_mode="low_cost_web_search",
                queries=_dedupe(
                    [
                        " ".join(part for part in [company, *keyword_terms[:2], role_terms[0] if role_terms else "employee"] if part).strip(),
                        f'{company} {" ".join(keyword_terms[:2]).strip()} LinkedIn'.strip(),
                        f'{company} {" ".join(distinct_scope_terms[:2]).strip()} team'.strip() if distinct_scope_terms else f"{company} team",
                    ]
                ),
                filters={"scope_terms": scope_terms, "role_terms": role_terms, "keyword_terms": keyword_terms},
            )
        )

    publication_queries = list(publication_coverage.seed_queries[:4])
    if allow_web_seed_fallback and publication_queries:
        bundles.append(
            SearchQueryBundle(
                bundle_id="publication_surface",
                source_family="publication_and_blog",
                priority="high",
                objective="Expand roster via official research, engineering, blog, and docs surfaces.",
                execution_mode="coverage_search",
                queries=publication_queries,
                filters={"source_families": [item.family for item in publication_coverage.source_families]},
            )
        )

    text = " ".join(
        [
            str(request.raw_user_request or ""),
            str(request.query or ""),
            " ".join(scope_terms),
            " ".join(keyword_terms),
        ]
    ).lower()
    if allow_web_seed_fallback and any(token in text for token in ["interview", "podcast", "youtube", "访谈", "播客", "采访"]):
        interview_queries = _dedupe(
            [
                f"{company} interview",
                f"{company} podcast",
                f"{company} YouTube",
                *(f"{scope} interview" for scope in scope_terms[:2]),
            ]
        )
        bundles.append(
            SearchQueryBundle(
                bundle_id="public_interviews",
                source_family="public_interviews",
                priority="medium",
                objective="Mine public interviews, podcasts, and video appearances for otherwise invisible members.",
                execution_mode="public_media_search",
                queries=interview_queries,
                filters={"platforms": ["Google", "YouTube"], "scope_terms": scope_terms},
            )
        )

    requires_paid_people_search_fallback = acquisition_strategy.strategy_type in {
        "scoped_search_roster",
        "former_employee_search",
    } or (
        acquisition_strategy.strategy_type == "full_company_roster"
        and bool(acquisition_strategy.cost_policy.get("large_org_keyword_probe_mode"))
    )

    if requires_paid_people_search_fallback and acquisition_strategy.search_seed_queries:
        bundles.append(
            SearchQueryBundle(
                bundle_id="targeted_people_search",
                source_family="linkedin_people_search",
                priority="medium",
                objective="Use LinkedIn profile search for company-scoped people discovery.",
                execution_mode="paid_fallback",
                queries=naturalize_search_query_terms(list(acquisition_strategy.search_seed_queries[:4])),
                filters=dict(acquisition_strategy.filter_hints),
            )
        )

    return SearchStrategyPlan(
        planner_mode="model_assisted" if provider_name != "deterministic" else "deterministic",
        objective=f"Build a high-recall but cost-aware search plan for {company}.",
        query_bundles=bundles,
        follow_up_rules=_stage1_follow_up_rules(allow_web_seed_fallback=allow_web_seed_fallback),
        review_triggers=[
            "The company scope appears broader than the target team boundary.",
            "The user requests non-LinkedIn web seed fallback in LinkedIn Stage 1.",
            "New source families are needed to cover corner cases like podcasts or interviews.",
        ],
    )


def _stage1_web_seed_fallback_enabled(
    *,
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
) -> bool:
    intent_view = resolve_request_intent_view(request)
    request_preferences = dict(getattr(request, "execution_preferences", {}) or {})
    execution_preferences = dict(intent_view.get("execution_preferences") or {})
    cost_policy = dict(acquisition_strategy.cost_policy or {})
    return bool(
        request_preferences.get("allow_stage1_web_seed_fallback")
        or request_preferences.get("allow_public_web_seed_fallback")
        or execution_preferences.get("allow_stage1_web_seed_fallback")
        or execution_preferences.get("allow_public_web_seed_fallback")
        or cost_policy.get("allow_stage1_web_seed_fallback")
        or cost_policy.get("allow_public_web_seed_fallback")
    )


def _stage1_follow_up_rules(*, allow_web_seed_fallback: bool) -> list[str]:
    if allow_web_seed_fallback:
        return [
            "If a public page yields a LinkedIn URL, resolve profile detail directly before paid people search.",
            "If publication/blog/interview surfaces reveal new names, create leads and route them to exploration or second-pass profile resolution.",
            "Preserve low-cost search artifacts before escalating to high-cost providers.",
        ]
    return [
        "LinkedIn Stage 1 may call LinkedIn-related providers only: company employees, profile search, and profile scraper.",
        "Public-web/DataForSEO seed discovery is Stage 2 or explicit opt-in; do not use it as default Stage 1 fallback.",
        "When scoped recall needs more candidates, expand Harvest profile-search query shards instead of switching to web search.",
    ]


def _enforce_stage1_seed_policy(
    strategy: SearchStrategyPlan,
    *,
    allow_web_seed_fallback: bool,
) -> SearchStrategyPlan:
    if allow_web_seed_fallback:
        return strategy
    filtered_bundles = [
        bundle
        for bundle in list(strategy.query_bundles or [])
        if _is_linkedin_stage1_query_bundle(bundle)
    ]
    return SearchStrategyPlan(
        planner_mode=strategy.planner_mode,
        objective=strategy.objective,
        query_bundles=filtered_bundles,
        follow_up_rules=_stage1_follow_up_rules(allow_web_seed_fallback=False),
        review_triggers=list(strategy.review_triggers or []),
    )


def _is_linkedin_stage1_query_bundle(bundle: SearchQueryBundle) -> bool:
    source_family = str(bundle.source_family or "").strip().lower()
    execution_mode = str(bundle.execution_mode or "").strip().lower()
    return (
        source_family in LINKEDIN_STAGE1_QUERY_SOURCE_FAMILIES
        or execution_mode in LINKEDIN_STAGE1_QUERY_EXECUTION_MODES
    )


def _merge_strategy(base: SearchStrategyPlan, model_payload: dict[str, Any]) -> SearchStrategyPlan:
    if not isinstance(model_payload, dict) or not model_payload:
        return base
    bundles: list[SearchQueryBundle] = []
    seen_ids: set[str] = set()
    for item in model_payload.get("query_bundles") or []:
        if not isinstance(item, dict):
            continue
        bundle_id = str(item.get("bundle_id") or "").strip()
        if not bundle_id or bundle_id in seen_ids:
            continue
        seen_ids.add(bundle_id)
        bundles.append(
            SearchQueryBundle(
                bundle_id=bundle_id,
                source_family=str(item.get("source_family") or "public_web_search"),
                priority=str(item.get("priority") or "medium"),
                objective=str(item.get("objective") or ""),
                execution_mode=str(item.get("execution_mode") or "low_cost_web_search"),
                queries=_dedupe(item.get("queries") or []),
                filters=dict(item.get("filters") or {}),
            )
        )
    if not bundles:
        bundles = list(base.query_bundles)
    return SearchStrategyPlan(
        planner_mode=str(model_payload.get("planner_mode") or base.planner_mode or "deterministic"),
        objective=str(model_payload.get("objective") or base.objective),
        query_bundles=bundles,
        follow_up_rules=_dedupe(model_payload.get("follow_up_rules") or base.follow_up_rules),
        review_triggers=_dedupe(model_payload.get("review_triggers") or base.review_triggers),
    )


def _dedupe(items: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        value = " ".join(str(item or "").split()).strip()
        if not value:
            continue
        signature = _query_signature(value)
        if signature in seen:
            continue
        seen.add(signature)
        results.append(value)
    return results


def _query_signature(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _scope_terms_distinct_from_company(scope_terms: list[Any], company: str) -> list[str]:
    company_signature = _query_signature(company)
    distinct_terms: list[str] = []
    for item in list(scope_terms or []):
        value = " ".join(str(item or "").split()).strip()
        if not value:
            continue
        if _query_signature(value) == company_signature:
            continue
        distinct_terms.append(value)
    return _dedupe(distinct_terms)
