from __future__ import annotations

from .domain import AcquisitionStrategyPlan, JobRequest, PublicationCoveragePlan, PublicationSourcePlan
from .query_signal_knowledge import naturalize_search_query_terms
from .request_normalization import build_effective_job_request


def compile_publication_coverage_plan(
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
) -> PublicationCoveragePlan:
    effective_request, intent_view = build_effective_job_request(request)
    topic_terms = naturalize_search_query_terms(
        list(effective_request.keywords or [])
        + list(effective_request.must_have_keywords or [])
        + list(effective_request.must_have_facets or [])
    )
    scope_terms = naturalize_search_query_terms(
        list(effective_request.organization_keywords or [])
        + list(acquisition_strategy.company_scope[1:] or [])
        + [str(effective_request.target_company or request.target_company or "").strip()]
    )
    text = " ".join(
        [
            str(effective_request.raw_user_request or ""),
            str(effective_request.query or ""),
            " ".join(scope_terms),
            " ".join(topic_terms),
        ]
    ).lower()
    company = str(effective_request.target_company or request.target_company or "").strip() or "target company"
    if not scope_terms:
        scope_terms = [company]
    directional_scope_terms = scope_terms[:2]
    directional_topic_terms = topic_terms[:3]
    research_query_hints = _dedupe(
        [f"{term} research" for term in directional_scope_terms]
        + [f"{company} {topic} research" for topic in directional_topic_terms]
    )
    publication_query_hints = _dedupe(
        [f"{term} arXiv" for term in directional_scope_terms]
        + [f"{company} {topic} paper" for topic in directional_topic_terms]
    )

    source_families = [
        PublicationSourcePlan(
            family="official_research",
            priority="high",
            rationale="Technical sourcing should always inspect official research outputs before relying on secondary summaries.",
            query_hints=research_query_hints[:4],
            extraction_mode="deterministic_then_llm_validate",
        ),
        PublicationSourcePlan(
            family="publication_platforms",
            priority="high",
            rationale="arXiv / OpenReview style platforms provide the most structured author evidence.",
            query_hints=publication_query_hints[:4],
            extraction_mode="deterministic",
        ),
    ]

    if any(token in text for token in ["engineer", "engineering", "infra", "platform", "systems", "research engineer"]):
        source_families.append(
            PublicationSourcePlan(
                family="official_engineering",
                priority="high",
                rationale="Engineering blog posts and technical writeups often mention contributors who never appear on research papers.",
                query_hints=_dedupe(
                    [f"{term} engineering" for term in directional_scope_terms]
                    + [f"{company} {topic} engineering" for topic in directional_topic_terms]
                )[:4],
                extraction_mode="llm_extract_contributors",
            )
        )

    source_families.append(
        PublicationSourcePlan(
            family="official_blog_and_docs",
            priority="medium",
            rationale="Blog and docs pages widen coverage beyond formal publications and catch product or applied teams.",
            query_hints=_dedupe(
                [f"{term} blog" for term in directional_scope_terms]
                + [f"{term} docs" for term in directional_scope_terms[:1]]
                + [f"{company} {topic} blog" for topic in directional_topic_terms]
            )[:5],
            extraction_mode="llm_extract_contributors",
        )
    )

    if any(token in text for token in ["claude", "gemini", "chatgpt", "copilot"]):
        source_families.append(
            PublicationSourcePlan(
                family="product_subbrand_pages",
                priority="medium",
                rationale="Model or product subbrands may have their own blogs or launch pages that identify additional contributors.",
                query_hints=[term for term in scope_terms[:3]],
                extraction_mode="llm_extract_contributors",
            )
        )

    seed_queries = _dedupe(
        [f"{company} publication"]
        + [f"{term} author acknowledgement" for term in scope_terms[:3]]
        + [f"{term} contributor" for term in scope_terms[:3]]
        + [f"{company} {term} research" for term in topic_terms[:3]]
        + [f"{company} {term} contributor" for term in topic_terms[:3]]
    )
    extraction_strategy = [
        "Use deterministic parsers for structured author lists and metadata.",
        "Use LLM extraction for acknowledgement, contributor, and weakly structured bylines.",
        "Canonicalize names, dedupe across sources, and attach source-specific evidence.",
    ]
    validation_steps = [
        "Cross-check extracted names against roster candidates, LinkedIn profiles, and prior evidence.",
        "Assign higher confidence when the same person is supported by both structured and weakly structured sources.",
        "Keep unmatched names as lead candidates instead of silently dropping them.",
    ]
    fallback_steps = [
        "If LLM coverage planning is unavailable, start from official research + engineering + blog + arXiv.",
        "If LLM extraction is unavailable, keep deterministic parsing and regex acknowledgement extraction as degraded mode.",
    ]

    return PublicationCoveragePlan(
        coverage_goal=f"Cover the main publication and contributor surfaces for {company} before using publication evidence as a filter.",
        source_families=source_families,
        seed_queries=seed_queries,
        extraction_strategy=extraction_strategy,
        validation_steps=validation_steps,
        fallback_steps=fallback_steps,
    )


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        normalized = " ".join(item.split()).strip()
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        results.append(normalized)
    return results
