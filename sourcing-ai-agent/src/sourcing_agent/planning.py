from __future__ import annotations

from .acquisition_strategy import compile_acquisition_strategy
from .asset_catalog import AssetCatalog
from .domain import (
    AcquisitionStrategyPlan,
    AcquisitionTask,
    JobRequest,
    PublicationCoveragePlan,
    PublicationSourcePlan,
    RetrievalPlan,
    SearchQueryBundle,
    SearchStrategyPlan,
    SourcingPlan,
)
from .model_provider import ModelClient
from .publication_planning import compile_publication_coverage_plan
from .search_planning import compile_search_strategy


def build_sourcing_plan(
    request: JobRequest,
    catalog: AssetCatalog,
    model_client: ModelClient,
) -> SourcingPlan:
    categories = request.categories or _infer_categories(request)
    employment_statuses = request.employment_statuses or _infer_employment_statuses(request)
    retrieval_plan = _build_retrieval_plan(request, categories)
    acquisition_strategy = compile_acquisition_strategy(request, categories, employment_statuses, retrieval_plan)
    publication_coverage = compile_publication_coverage_plan(request, acquisition_strategy)
    search_strategy = compile_search_strategy(request, acquisition_strategy, publication_coverage, model_client)
    acquisition_tasks = _build_acquisition_tasks(
        request,
        categories,
        employment_statuses,
        acquisition_strategy,
        publication_coverage,
        search_strategy,
    )

    draft_plan = {
        "target_company": request.target_company,
        "categories": categories,
        "employment_statuses": employment_statuses,
        "retrieval_plan": retrieval_plan.to_record(),
        "acquisition_strategy": acquisition_strategy.to_record(),
        "publication_coverage": publication_coverage.to_record(),
        "search_strategy": search_strategy.to_record(),
        "acquisition_tasks": [task.to_record() for task in acquisition_tasks],
    }
    intent_summary = model_client.interpret_intent(request, draft_plan)
    criteria_summary = _criteria_summary(request, categories, employment_statuses)
    assumptions = _build_assumptions(request, categories, retrieval_plan.strategy, acquisition_strategy)
    open_questions = _build_open_questions(request, acquisition_strategy)

    return SourcingPlan(
        target_company=request.target_company,
        target_scope=request.target_scope,
        intent_summary=intent_summary,
        criteria_summary=criteria_summary,
        retrieval_plan=retrieval_plan,
        acquisition_strategy=acquisition_strategy,
        publication_coverage=publication_coverage,
        search_strategy=search_strategy,
        acquisition_tasks=acquisition_tasks,
        assumptions=assumptions,
        open_questions=open_questions,
    )


def hydrate_sourcing_plan(payload: dict[str, object]) -> SourcingPlan:
    retrieval_payload = dict(payload.get("retrieval_plan") or {})
    acquisition_payload = dict(payload.get("acquisition_strategy") or {})
    publication_payload = dict(payload.get("publication_coverage") or {})
    search_payload = dict(payload.get("search_strategy") or {})
    source_families = [
        PublicationSourcePlan(
            family=str(item.get("family") or ""),
            priority=str(item.get("priority") or "medium"),
            rationale=str(item.get("rationale") or ""),
            query_hints=list(item.get("query_hints") or []),
            extraction_mode=str(item.get("extraction_mode") or "deterministic"),
        )
        for item in publication_payload.get("source_families") or []
        if isinstance(item, dict)
    ]
    acquisition_tasks = [
        AcquisitionTask(
            task_id=str(item.get("task_id") or ""),
            task_type=str(item.get("task_type") or ""),
            title=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
            source_hint=str(item.get("source_hint") or ""),
            status=str(item.get("status") or "pending"),
            blocking=bool(item.get("blocking")),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in payload.get("acquisition_tasks") or []
        if isinstance(item, dict)
    ]
    query_bundles = [
        SearchQueryBundle(
            bundle_id=str(item.get("bundle_id") or ""),
            source_family=str(item.get("source_family") or ""),
            priority=str(item.get("priority") or "medium"),
            objective=str(item.get("objective") or ""),
            execution_mode=str(item.get("execution_mode") or "low_cost_web_search"),
            queries=list(item.get("queries") or []),
            filters=dict(item.get("filters") or {}),
        )
        for item in search_payload.get("query_bundles") or []
        if isinstance(item, dict)
    ]
    return SourcingPlan(
        target_company=str(payload.get("target_company") or ""),
        target_scope=str(payload.get("target_scope") or "full_company_asset"),
        intent_summary=str(payload.get("intent_summary") or ""),
        criteria_summary=str(payload.get("criteria_summary") or ""),
        retrieval_plan=RetrievalPlan(
            strategy=str(retrieval_payload.get("strategy") or "hybrid"),
            reason=str(retrieval_payload.get("reason") or ""),
            structured_filters=list(retrieval_payload.get("structured_filters") or []),
            semantic_fields=list(retrieval_payload.get("semantic_fields") or []),
            filter_layers=list(retrieval_payload.get("filter_layers") or []),
        ),
        acquisition_strategy=AcquisitionStrategyPlan(
            strategy_type=str(acquisition_payload.get("strategy_type") or ""),
            target_population=str(acquisition_payload.get("target_population") or ""),
            company_scope=list(acquisition_payload.get("company_scope") or []),
            roster_sources=list(acquisition_payload.get("roster_sources") or []),
            search_channel_order=list(acquisition_payload.get("search_channel_order") or []),
            search_seed_queries=list(acquisition_payload.get("search_seed_queries") or []),
            filter_hints=dict(acquisition_payload.get("filter_hints") or {}),
            cost_policy=dict(acquisition_payload.get("cost_policy") or {}),
            confirmation_points=list(acquisition_payload.get("confirmation_points") or []),
            reasoning=list(acquisition_payload.get("reasoning") or []),
        ),
        publication_coverage=PublicationCoveragePlan(
            coverage_goal=str(publication_payload.get("coverage_goal") or ""),
            source_families=source_families,
            seed_queries=list(publication_payload.get("seed_queries") or []),
            extraction_strategy=list(publication_payload.get("extraction_strategy") or []),
            validation_steps=list(publication_payload.get("validation_steps") or []),
            fallback_steps=list(publication_payload.get("fallback_steps") or []),
        ),
        search_strategy=SearchStrategyPlan(
            planner_mode=str(search_payload.get("planner_mode") or "deterministic"),
            objective=str(search_payload.get("objective") or ""),
            query_bundles=query_bundles,
            follow_up_rules=list(search_payload.get("follow_up_rules") or []),
            review_triggers=list(search_payload.get("review_triggers") or []),
        ),
        acquisition_tasks=acquisition_tasks,
        assumptions=list(payload.get("assumptions") or []),
        open_questions=list(payload.get("open_questions") or []),
    )


def _build_retrieval_plan(request: JobRequest, categories: list[str]) -> RetrievalPlan:
    strategy = request.retrieval_strategy or _infer_retrieval_strategy(request)
    structured_filters = []
    if categories:
        structured_filters.append(f"categories={categories}")
    if request.employment_statuses:
        structured_filters.append(f"employment_statuses={request.employment_statuses}")
    if request.organization_keywords:
        structured_filters.append(f"organization_keywords={request.organization_keywords}")

    if strategy == "structured":
        reason = "Criteria are narrow enough for deterministic filtering and lexical matching."
    elif strategy == "semantic":
        reason = "Criteria are ambiguous or narrative-heavy, so semantic ranking should dominate."
    else:
        strategy = "hybrid"
        reason = "Use structured filters to reduce the search space, then semantic matching or reranking for corner cases."

    return RetrievalPlan(
        strategy=strategy,
        reason=reason,
        structured_filters=structured_filters,
        semantic_fields=["role", "team", "focus_areas", "education", "work_history", "notes"],
        filter_layers=_build_filter_layers(request, strategy, categories),
    )


def _build_acquisition_tasks(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    acquisition_strategy: AcquisitionStrategyPlan,
    publication_coverage: PublicationCoveragePlan,
    search_strategy: SearchStrategyPlan,
) -> list[AcquisitionTask]:
    has_target_company = bool(request.target_company.strip())

    tasks = [
        AcquisitionTask(
            task_id="resolve-company-identity",
            task_type="resolve_company_identity",
            title="Resolve company identifiers",
            description="Find company slug, LinkedIn identity, aliases, and canonical target scope.",
            source_hint="company website / LinkedIn / provider-specific company resolver",
            status="ready" if request.target_company else "needs_input",
            blocking=True,
        ),
        AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire full company roster",
            description="Pull the broadest possible roster before applying any sourcing criteria.",
            source_hint="LinkedIn company people / The Org / domain search / internal CSV / other org sources",
            status="ready" if has_target_company else "needs_input",
            blocking=True,
            metadata={
                "categories": categories,
                "employment_statuses": employment_statuses,
                "strategy_type": acquisition_strategy.strategy_type,
                "company_scope": acquisition_strategy.company_scope,
                "search_channel_order": acquisition_strategy.search_channel_order,
                "search_seed_queries": acquisition_strategy.search_seed_queries,
                "search_strategy": search_strategy.to_record(),
                "search_query_bundles": [bundle.to_record() for bundle in search_strategy.query_bundles],
                "filter_hints": acquisition_strategy.filter_hints,
                "cost_policy": acquisition_strategy.cost_policy,
                "max_pages": 10,
                "page_limit": 50,
            },
        ),
        AcquisitionTask(
            task_id="enrich-multisource-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles across sources",
            description="Add LinkedIn profile details, publications, blog mentions, Scholar, X, GitHub, and other evidence.",
            source_hint="LinkedIn profile detail / Scholar / arXiv / company blog / GitHub / X",
            status="ready" if has_target_company else "needs_input",
            blocking=True,
            metadata={
                "strategy_type": acquisition_strategy.strategy_type,
                "search_channel_order": acquisition_strategy.search_channel_order,
                "cost_policy": acquisition_strategy.cost_policy,
                "publication_source_families": [item.family for item in publication_coverage.source_families],
                "publication_extraction_strategy": publication_coverage.extraction_strategy,
                "slug_resolution_limit": request.slug_resolution_limit,
                "profile_detail_limit": request.profile_detail_limit,
                "publication_scan_limit": request.publication_scan_limit,
                "publication_lead_limit": request.publication_lead_limit,
                "exploration_limit": request.exploration_limit,
            },
        ),
        AcquisitionTask(
            task_id="normalize-asset-snapshot",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Write a versioned candidate/evidence snapshot that can be reused by later queries.",
            source_hint="SQLite + versioned JSON artifact",
            status="ready" if has_target_company else "needs_input",
            blocking=False,
        ),
        AcquisitionTask(
            task_id="build-retrieval-index",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Prepare structured filters and semantic-ready candidate documents for later retrieval.",
            source_hint="SQLite filters + candidate document index + future vector index",
            status="ready" if has_target_company else "needs_input",
            blocking=False,
        ),
    ]
    return tasks


def _infer_categories(request: JobRequest) -> list[str]:
    text = f"{request.raw_user_request} {request.query}"
    if any(token in text for token in ["投资", "VC", "investor", "投融资"]):
        return ["investor"]
    if any(token in text for token in ["离职", "former", "前员工"]):
        return ["former_employee"]
    return ["employee", "former_employee"]


def _infer_employment_statuses(request: JobRequest) -> list[str]:
    text = f"{request.raw_user_request} {request.query}"
    if any(token in text for token in ["在职", "当前", "current"]):
        return ["current"]
    if any(token in text for token in ["离职", "former", "前员工"]):
        return ["former"]
    return []


def _infer_retrieval_strategy(request: JobRequest) -> str:
    text = f"{request.raw_user_request} {request.query}"
    if request.must_have_keywords or request.organization_keywords:
        return "structured"
    if len(request.keywords) >= 5:
        return "hybrid"
    if any(token in text for token in ["复杂", "综合判断", "匹配度", "适合", "不像 SQL", "corner case", "语义"]):
        return "hybrid"
    return "structured" if request.keywords else "hybrid"


def _criteria_summary(request: JobRequest, categories: list[str], employment_statuses: list[str]) -> str:
    segments = [f"target_company={request.target_company or 'unknown'}", f"categories={categories}"]
    if employment_statuses:
        segments.append(f"employment_statuses={employment_statuses}")
    if request.keywords:
        segments.append(f"keywords={request.keywords}")
    if request.must_have_keywords:
        segments.append(f"must_have={request.must_have_keywords}")
    if request.exclude_keywords:
        segments.append(f"exclude={request.exclude_keywords}")
    return "; ".join(segments)


def _build_assumptions(request: JobRequest, categories: list[str], strategy: str, acquisition_strategy) -> list[str]:
    assumptions = []
    assumptions.append("Acquisition must happen before retrieval so that criteria do not bias which people enter the asset pool.")
    if strategy == "hybrid":
        assumptions.append("Corner cases will need semantic matching or model-assisted reranking after structured filtering.")
    if "investor" not in categories:
        assumptions.append("The primary retrieval population is company members rather than investors.")
    if "general_web_search_relation_check" in acquisition_strategy.search_channel_order:
        assumptions.append("Low-cost relation verification and public web search should run before paid LinkedIn people search.")
    if acquisition_strategy.strategy_type == "scoped_search_roster":
        assumptions.append("Large-company requests should prefer a scoped roster over a company-wide roster unless the user confirms otherwise.")
    return assumptions


def _build_open_questions(request: JobRequest, acquisition_strategy) -> list[str]:
    questions = []
    if not request.target_company:
        questions.append("Which company should be scanned first?")
    if not request.categories:
        questions.append("Should the search include current employees, former employees, investors, or all of them?")
    questions.extend(acquisition_strategy.confirmation_points)
    return questions


def _build_filter_layers(request: JobRequest, strategy: str, categories: list[str]) -> list[dict[str, object]]:
    layers: list[dict[str, object]] = [
        {
            "layer_id": "population_scope",
            "kind": "hard_filter",
            "description": "Constrain the working population to the target company or approved scope before ranking.",
            "criteria": {
                "target_company": request.target_company,
                "target_scope": request.target_scope,
                "categories": categories,
                "employment_statuses": request.employment_statuses,
            },
        },
        {
            "layer_id": "must_exclude_filters",
            "kind": "hard_filter",
            "description": "Apply must-have, exclude, and organization filters before recall-heavy ranking.",
            "criteria": {
                "must_have_keywords": request.must_have_keywords,
                "exclude_keywords": request.exclude_keywords,
                "organization_keywords": request.organization_keywords,
            },
        },
        {
            "layer_id": "lexical_alias_recall",
            "kind": "lexical_recall",
            "description": "Use keyword, alias, and deterministic pattern matching to form the initial recall set.",
            "criteria": {"keywords": request.keywords or []},
        },
    ]
    if strategy in {"hybrid", "semantic"}:
        layers.append(
            {
                "layer_id": "semantic_vector_rerank",
                "kind": "semantic_retrieval",
                "description": "Use semantic document matching to recover corner cases and rerank candidates with sparse vector similarity.",
                "criteria": {
                    "semantic_rerank_limit": request.semantic_rerank_limit,
                },
            }
        )
    layers.extend(
        [
            {
                "layer_id": "confidence_banding",
                "kind": "confidence_policy",
                "description": "Assign high, medium, and lead-only confidence bands after retrieval.",
                "criteria": {"labels": ["high", "medium", "lead_only"]},
            },
            {
                "layer_id": "manual_review_queue",
                "kind": "human_review",
                "description": "Escalate unresolved identities, weakly supported leads, and source conflicts for manual review.",
                "criteria": {"review_types": ["manual_identity_resolution", "source_conflict", "lead_validation"]},
            },
        ]
    )
    return layers
