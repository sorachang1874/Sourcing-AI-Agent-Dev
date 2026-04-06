from __future__ import annotations

from typing import Any

from .domain import (
    AcquisitionStrategyPlan,
    JobRequest,
    PublicationCoveragePlan,
    SearchQueryBundle,
    SearchStrategyPlan,
)
from .model_provider import ModelClient


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
    draft = deterministic.to_record()
    model_payload = model_client.plan_search_strategy(
        request,
        {
            "acquisition_strategy": acquisition_strategy.to_record(),
            "publication_coverage": publication_coverage.to_record(),
            "draft_search_strategy": draft,
        },
    )
    return _merge_strategy(deterministic, model_payload)


def _deterministic_search_strategy(
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
    publication_coverage: PublicationCoveragePlan,
    *,
    provider_name: str,
) -> SearchStrategyPlan:
    company = request.target_company or "target company"
    scope_terms = acquisition_strategy.company_scope[1:] or [company]
    role_terms = list(acquisition_strategy.filter_hints.get("job_titles") or [])
    keyword_terms = list(acquisition_strategy.filter_hints.get("keywords") or request.keywords or [])
    bundles: list[SearchQueryBundle] = []

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
                    f'{company} {" ".join(scope_terms[:2]).strip()} team'.strip(),
                ]
            ),
            filters={"scope_terms": scope_terms, "role_terms": role_terms, "keyword_terms": keyword_terms},
        )
    )

    publication_queries = list(publication_coverage.seed_queries[:4])
    if publication_queries:
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

    text = f"{request.raw_user_request} {request.query}".lower()
    if any(token in text for token in ["interview", "podcast", "youtube", "访谈", "播客", "采访"]):
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

    if acquisition_strategy.strategy_type in {"scoped_search_roster", "former_employee_search"}:
        bundles.append(
            SearchQueryBundle(
                bundle_id="targeted_people_search",
                source_family="linkedin_people_search",
                priority="medium",
                objective="Use paid people search only after low-cost search has exhausted public profile discovery.",
                execution_mode="paid_fallback",
                queries=list(acquisition_strategy.search_seed_queries[:4]),
                filters=dict(acquisition_strategy.filter_hints),
            )
        )

    return SearchStrategyPlan(
        planner_mode="model_assisted" if provider_name != "deterministic" else "deterministic",
        objective=f"Build a high-recall but cost-aware search plan for {company}.",
        query_bundles=bundles,
        follow_up_rules=[
            "If a public page yields a LinkedIn URL, resolve profile detail directly before paid people search.",
            "If publication/blog/interview surfaces reveal new names, create leads and route them to exploration or second-pass profile resolution.",
            "Preserve low-cost search artifacts before escalating to high-cost providers.",
        ],
        review_triggers=[
            "The company scope appears broader than the target team boundary.",
            "The user requests high-cost APIs before low-cost sources have been exhausted.",
            "New source families are needed to cover corner cases like podcasts or interviews.",
        ],
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
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        results.append(value)
    return results
