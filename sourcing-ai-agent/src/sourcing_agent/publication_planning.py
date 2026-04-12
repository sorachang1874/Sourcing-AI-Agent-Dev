from __future__ import annotations

from .domain import AcquisitionStrategyPlan, JobRequest, PublicationCoveragePlan, PublicationSourcePlan
from .request_normalization import resolve_request_intent_view


def compile_publication_coverage_plan(
    request: JobRequest,
    acquisition_strategy: AcquisitionStrategyPlan,
) -> PublicationCoveragePlan:
    intent_view = resolve_request_intent_view(request)
    text = " ".join(
        [
            str(request.raw_user_request or ""),
            str(request.query or ""),
            " ".join(list(intent_view.get("organization_keywords") or [])),
            " ".join(list(intent_view.get("keywords") or [])),
            " ".join(list(intent_view.get("must_have_keywords") or [])),
            " ".join(list(intent_view.get("must_have_facets") or [])),
        ]
    ).lower()
    company = str(intent_view.get("target_company") or request.target_company or "").strip() or "target company"
    scope_terms = _dedupe(list(intent_view.get("organization_keywords") or []) + list(acquisition_strategy.company_scope[1:] or [company]))

    source_families = [
        PublicationSourcePlan(
            family="official_research",
            priority="high",
            rationale="Technical sourcing should always inspect official research outputs before relying on secondary summaries.",
            query_hints=[f"{term} research" for term in scope_terms[:2]],
            extraction_mode="deterministic_then_llm_validate",
        ),
        PublicationSourcePlan(
            family="publication_platforms",
            priority="high",
            rationale="arXiv / OpenReview style platforms provide the most structured author evidence.",
            query_hints=[f"{term} arXiv" for term in scope_terms[:2]],
            extraction_mode="deterministic",
        ),
    ]

    if any(token in text for token in ["engineer", "engineering", "infra", "platform", "systems", "research engineer"]):
        source_families.append(
            PublicationSourcePlan(
                family="official_engineering",
                priority="high",
                rationale="Engineering blog posts and technical writeups often mention contributors who never appear on research papers.",
                query_hints=[f"{term} engineering" for term in scope_terms[:2]],
                extraction_mode="llm_extract_contributors",
            )
        )

    source_families.append(
        PublicationSourcePlan(
            family="official_blog_and_docs",
            priority="medium",
            rationale="Blog and docs pages widen coverage beyond formal publications and catch product or applied teams.",
            query_hints=[f"{term} blog" for term in scope_terms[:2]] + [f"{term} docs" for term in scope_terms[:1]],
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
