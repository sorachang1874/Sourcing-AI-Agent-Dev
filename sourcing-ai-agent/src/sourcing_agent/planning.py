from __future__ import annotations

from typing import Any

from .acquisition_strategy import compile_acquisition_strategy
from .asset_catalog import AssetCatalog
from .company_shard_planning import (
    build_default_company_employee_shard_policy,
    build_large_org_keyword_probe_shard_policy,
)
from .company_registry import normalize_company_key
from .domain import (
    AcquisitionStrategyPlan,
    AcquisitionTask,
    IntentPlanBrief,
    JobRequest,
    PublicationCoveragePlan,
    PublicationSourcePlan,
    RetrievalPlan,
    SearchQueryBundle,
    SearchStrategyPlan,
    SourcingPlan,
)
from .model_provider import DeterministicModelClient, ModelClient
from .publication_planning import compile_publication_coverage_plan
from .query_intent_rewrite import summarize_query_intent_rewrite
from .request_normalization import build_effective_request_payload, build_request_intent_axes_payload, resolve_request_intent_view
from .search_planning import compile_search_strategy

MODEL_WRITTEN_PLANNING_MODES = {"llm_brief", "product_brief_model_assisted"}
FULL_COMPANY_EMPLOYEES_PAGE_LIMIT = 25
FULL_COMPANY_EMPLOYEES_DEFAULT_MAX_PAGES = 20
FULL_COMPANY_EMPLOYEES_LARGE_ORG_MAX_PAGES = 100
FULL_COMPANY_EMPLOYEES_LARGE_ORG_KEYS = {
    "anthropic",
    "openai",
    "meta",
    "facebook",
    "google",
    "alphabet",
    "microsoft",
    "amazon",
    "apple",
    "bytedance",
    "tiktok",
}
def build_sourcing_plan(
    request: JobRequest,
    catalog: AssetCatalog,
    model_client: ModelClient,
) -> SourcingPlan:
    intent_view = resolve_request_intent_view(
        request,
        fallback_categories=_infer_categories(request),
        fallback_employment_statuses=_infer_employment_statuses(request),
    )
    effective_target_company = str(intent_view.get("target_company") or request.target_company).strip()
    categories = list(intent_view.get("categories") or [])
    employment_statuses = list(intent_view.get("employment_statuses") or [])
    retrieval_plan = _build_retrieval_plan(request, categories, intent_view=intent_view)
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
        intent_view=intent_view,
    )
    criteria_summary = _criteria_summary(request, categories, employment_statuses, intent_view=intent_view)
    assumptions = _build_assumptions(request, categories, retrieval_plan.strategy, acquisition_strategy)
    open_questions = _build_open_questions(
        request,
        categories,
        employment_statuses,
        acquisition_strategy,
        intent_view=intent_view,
    )

    draft_plan = {
        "target_company": effective_target_company,
        "categories": categories,
        "employment_statuses": employment_statuses,
        "retrieval_plan": retrieval_plan.to_record(),
        "acquisition_strategy": acquisition_strategy.to_record(),
        "publication_coverage": publication_coverage.to_record(),
        "search_strategy": search_strategy.to_record(),
        "acquisition_tasks": [task.to_record() for task in acquisition_tasks],
        "criteria_summary": criteria_summary,
        "assumptions": assumptions,
        "open_questions": open_questions,
    }
    intent_brief = _build_intent_brief(
        model_client=model_client,
        request=request,
        draft_plan=draft_plan,
        categories=categories,
        employment_statuses=employment_statuses,
        retrieval_plan=retrieval_plan,
        acquisition_strategy=acquisition_strategy,
        search_strategy=search_strategy,
        open_questions=open_questions,
    )
    draft_plan["intent_brief"] = intent_brief.to_record()
    if request.planning_mode.lower() in MODEL_WRITTEN_PLANNING_MODES:
        intent_summary = model_client.interpret_intent(request, draft_plan)
    else:
        intent_summary = DeterministicModelClient().interpret_intent(request, draft_plan)

    return SourcingPlan(
        target_company=effective_target_company,
        target_scope=request.target_scope,
        intent_summary=intent_summary,
        criteria_summary=criteria_summary,
        retrieval_plan=retrieval_plan,
        acquisition_strategy=acquisition_strategy,
        publication_coverage=publication_coverage,
        search_strategy=search_strategy,
        acquisition_tasks=acquisition_tasks,
        intent_brief=intent_brief,
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
        asset_reuse_plan=dict(payload.get("asset_reuse_plan") or {}),
        intent_brief=IntentPlanBrief(
            identified_request=list((payload.get("intent_brief") or {}).get("identified_request") or []),
            target_output=list((payload.get("intent_brief") or {}).get("target_output") or []),
            default_execution_strategy=list((payload.get("intent_brief") or {}).get("default_execution_strategy") or []),
            review_focus=list((payload.get("intent_brief") or {}).get("review_focus") or []),
        ),
        assumptions=list(payload.get("assumptions") or []),
        open_questions=list(payload.get("open_questions") or []),
    )


def _build_intent_brief(
    *,
    model_client: ModelClient,
    request: JobRequest,
    draft_plan: dict[str, object],
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
    acquisition_strategy: AcquisitionStrategyPlan,
    search_strategy: SearchStrategyPlan,
    open_questions: list[str],
) -> IntentPlanBrief:
    effective_request_payload = build_effective_request_payload(
        request,
        fallback_categories=categories,
        fallback_employment_statuses=employment_statuses,
    )
    deterministic = _deterministic_intent_brief(
        request=request,
        categories=categories,
        employment_statuses=employment_statuses,
        retrieval_plan=retrieval_plan,
        acquisition_strategy=acquisition_strategy,
        search_strategy=search_strategy,
        open_questions=open_questions,
    )
    if request.planning_mode.lower() not in MODEL_WRITTEN_PLANNING_MODES:
        return deterministic
    model_payload = model_client.draft_intent_brief(
        request,
        {
            "request": effective_request_payload,
            "draft_plan": draft_plan,
            "deterministic_brief": deterministic.to_record(),
        },
    )
    return IntentPlanBrief(
        identified_request=_normalize_brief_section(
            model_payload.get("identified_request"),
            fallback=deterministic.identified_request,
        ),
        target_output=_normalize_brief_section(
            model_payload.get("target_output"),
            fallback=deterministic.target_output,
        ),
        default_execution_strategy=_normalize_brief_section(
            model_payload.get("default_execution_strategy"),
            fallback=deterministic.default_execution_strategy,
        ),
        review_focus=_normalize_brief_section(
            model_payload.get("review_focus"),
            fallback=deterministic.review_focus,
        ),
    )


def _deterministic_intent_brief(
    *,
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    retrieval_plan: RetrievalPlan,
    acquisition_strategy: AcquisitionStrategyPlan,
    search_strategy: SearchStrategyPlan,
    open_questions: list[str],
) -> IntentPlanBrief:
    intent_view = resolve_request_intent_view(
        request,
        fallback_categories=categories,
        fallback_employment_statuses=employment_statuses,
    )
    intent_axes = dict(intent_view.get("intent_axes") or build_request_intent_axes_payload(request=request))
    acquisition_lane_policy = dict(intent_axes.get("acquisition_lane_policy") or {})
    fallback_policy = dict(intent_axes.get("fallback_policy") or {})
    target_company = str(intent_view.get("target_company") or request.target_company).strip() or "待确认组织"
    effective_categories = list(intent_view.get("categories") or categories or [])
    effective_employment_statuses = list(intent_view.get("employment_statuses") or employment_statuses or [])
    population_label = _population_label(effective_categories)
    scope_terms = _dedupe_terms(list(intent_view.get("organization_keywords") or request.organization_keywords))
    focus_terms = _dedupe_terms(
        list(intent_view.get("must_have_facets") or request.must_have_facets)
        + list(intent_view.get("must_have_primary_role_buckets") or request.must_have_primary_role_buckets)
        + list(intent_view.get("must_have_keywords") or request.must_have_keywords)
        + list(intent_view.get("keywords") or request.keywords)
    )
    identified_request = [
        f"目标组织：{target_company}",
        f"目标人群：{population_label}",
    ]
    if scope_terms:
        identified_request.append(f"团队或子组织范围：{' / '.join(scope_terms[:4])}")
    if focus_terms:
        identified_request.append(f"方向约束：{' / '.join(focus_terms[:6])}")
    if effective_employment_statuses:
        identified_request.append(f"雇佣状态：{' / '.join(effective_employment_statuses)}")
    rewrite_summary = summarize_query_intent_rewrite(request.raw_user_request or request.query)
    if rewrite_summary:
        identified_request.append(rewrite_summary)
    identified_request.append(f"任务类型：{_task_type_label(acquisition_strategy.strategy_type, request.target_scope)}")

    target_output = [
        _target_output_line(target_company, population_label, focus_terms, scope_terms),
        _employment_focus_line(effective_employment_statuses),
        "若存在边界不清、弱证据或组织归属冲突的人，进入 manual review，而不是静默丢弃。",
    ]
    if request.top_k > 0:
        target_output.append(f"默认返回前 {request.top_k} 个高相关结果，并保留关键 evidence 供复核。")
    if rewrite_summary:
        target_output.append("这类简称默认按公开的地区 / 语言 / 学习工作经历口径理解，而不是身份标签判断。")

    execution_strategy = [
        "先做 company identity resolve，确认正式组织身份、LinkedIn company URL、slug 与 canonical alias。",
        f"先判断是否已有可复用资产；若没有，则进入{_acquisition_step_label(acquisition_strategy.strategy_type)}。",
        _retrieval_strategy_line(retrieval_plan.strategy),
        _search_strategy_line(search_strategy),
        "结果输出时显式附带 manual review items、关键证据和需要用户确认的风险点。",
    ]
    if bool(acquisition_lane_policy.get("use_company_employees_lane") or request.execution_preferences.get("use_company_employees_lane")):
        execution_strategy.insert(2, "当前计划优先走 Harvest company-employees lane，先拿当前组织 roster 再进入后续检索。")
    if bool(fallback_policy.get("force_fresh_run") or request.execution_preferences.get("force_fresh_run")):
        execution_strategy.insert(3, "本次按 fresh run 执行，不复用 cached roster、共享 provider cache 或历史 profile inheritance。")
    if (
        ("allow_high_cost_sources" in fallback_policy or "allow_high_cost_sources" in request.execution_preferences)
        and not bool(fallback_policy.get("allow_high_cost_sources", request.execution_preferences.get("allow_high_cost_sources")))
    ):
        execution_strategy.insert(4, "默认不启用高成本 LinkedIn source，只有用户后续显式放开才升级。")
    else:
        execution_strategy.insert(4, "公开网页和低成本 search 优先，只有在无法确认成员关系时才升级到高成本 LinkedIn provider。")

    review_focus = [_localize_review_question(item) for item in open_questions[:4]]
    return IntentPlanBrief(
        identified_request=identified_request,
        target_output=target_output,
        default_execution_strategy=execution_strategy,
        review_focus=review_focus,
    )


def _normalize_brief_section(value: object, *, fallback: list[str]) -> list[str]:
    items = value if isinstance(value, list) else []
    cleaned: list[str] = []
    for item in items:
        candidate = str(item or "").strip()
        if not candidate or candidate in cleaned:
            continue
        cleaned.append(candidate)
    return cleaned[:8] or list(fallback)


def _population_label(categories: list[str]) -> str:
    if not categories:
        return "未明确指定的人群"
    return " / ".join(categories)


def _dedupe_terms(items: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = str(item or "").strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(normalized)
    return results


def _task_type_label(strategy_type: str, target_scope: str) -> str:
    if strategy_type == "full_company_roster":
        return "新组织全量 sourcing test" if target_scope == "full_company_asset" else "全量资产获取"
    if strategy_type == "scoped_search_roster":
        return "范围限定的组织 sourcing"
    if strategy_type == "former_employee_search":
        return "former member recall"
    if strategy_type == "investor_firm_roster":
        return "investor firm roster sourcing"
    return "通用 sourcing workflow"


def _target_output_line(
    target_company: str,
    population_label: str,
    focus_terms: list[str],
    scope_terms: list[str],
) -> str:
    company_fragment = target_company
    if scope_terms:
        company_fragment = f"{target_company} 的 {' / '.join(scope_terms[:3])}"
    if focus_terms:
        return (
            f"找到与 {company_fragment} 相关、方向偏 {' / '.join(focus_terms[:5])} 的"
            f" {population_label}。"
        )
    return f"找到与 {company_fragment} 相关的 {population_label}。"


def _employment_focus_line(employment_statuses: list[str]) -> str:
    normalized = {item.lower() for item in employment_statuses}
    if normalized == {"current"}:
        return "优先返回高置信当前成员。"
    if normalized == {"former"}:
        return "优先返回明确有该组织经历的已离职成员。"
    if normalized == {"current", "former"} or normalized == {"former", "current"}:
        return "当前与已离职成员共同参与初始召回，形成全量成员视角。"
    return "若雇佣状态未限定，则允许 current / former 共同参与初始召回。"


def _acquisition_step_label(strategy_type: str) -> str:
    if strategy_type == "full_company_roster":
        return "全量组织 roster 获取"
    if strategy_type == "scoped_search_roster":
        return "范围限定 roster 获取"
    if strategy_type == "former_employee_search":
        return "former member 搜索种子获取"
    if strategy_type == "investor_firm_roster":
        return "投资机构 roster 获取"
    return "资产获取"


def _retrieval_strategy_line(strategy: str) -> str:
    if strategy == "structured":
        return "acquisition 后优先走 structured filters、facet / role bucket 和 lexical / alias matching。"
    if strategy == "semantic":
        return "acquisition 后以 semantic recall 为主，再用 structured filters 做约束。"
    return "acquisition 后做多层 retrieval：structured filters -> facet / role bucket -> lexical / alias matching -> semantic recall。"


def _search_strategy_line(search_strategy: SearchStrategyPlan) -> str:
    bundle_ids = [item.bundle_id for item in search_strategy.query_bundles]
    if not bundle_ids:
        return "默认只保留必要的 search lane，不做无边界扩张。"
    bundles = " / ".join(bundle_ids[:4])
    return f"默认启用的 search lane 以 {bundles} 为主，必要时再扩展新的 source family。"


def _localize_review_question(question: str) -> str:
    normalized = " ".join(str(question or "").split())
    lowered = normalized.lower()
    if "high-cost linkedin provider calls" in lowered:
        return "是否允许在低成本公开网页验证不足时，升级到高成本 LinkedIn provider。"
    if "roster should be limited to" in lowered:
        return "是否需要将 roster 范围限制在指定 team / sub-org，而不是整个组织。"
    if "former employees should exclude anyone who has already returned" in lowered:
        return "former 检索是否要排除后来回流到目标组织的人。"
    return normalized


def _build_retrieval_plan(
    request: JobRequest,
    categories: list[str],
    *,
    intent_view: dict[str, Any] | None = None,
) -> RetrievalPlan:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    strategy = request.retrieval_strategy or _infer_retrieval_strategy(request, intent_view=intent_view)
    structured_filters = []
    if categories:
        structured_filters.append(f"categories={categories}")
    effective_employment_statuses = list(intent_view.get("employment_statuses") or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or [])
    effective_role_buckets = list(intent_view.get("must_have_primary_role_buckets") or [])
    primary_role_bucket_mode = str(intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() or "hard"
    effective_organization_keywords = list(intent_view.get("organization_keywords") or [])
    if effective_employment_statuses:
        structured_filters.append(f"employment_statuses={effective_employment_statuses}")
    if effective_must_have_facets:
        structured_filters.append(f"must_have_facets={effective_must_have_facets}")
    if effective_role_buckets:
        filter_key = (
            "must_have_primary_role_buckets"
            if primary_role_bucket_mode == "hard"
            else "soft_primary_role_buckets"
        )
        structured_filters.append(f"{filter_key}={effective_role_buckets}")
    if effective_organization_keywords:
        structured_filters.append(f"organization_keywords={effective_organization_keywords}")

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
        semantic_fields=_semantic_fields_for_request(request, intent_view=intent_view),
        filter_layers=_build_filter_layers(request, strategy, categories, intent_view=intent_view),
    )


def _build_acquisition_tasks(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    acquisition_strategy: AcquisitionStrategyPlan,
    publication_coverage: PublicationCoveragePlan,
    search_strategy: SearchStrategyPlan,
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[AcquisitionTask]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    effective_execution_preferences = dict(intent_view.get("execution_preferences") or {})
    effective_target_company = str(intent_view.get("target_company") or request.target_company).strip()
    has_target_company = bool(effective_target_company)
    roster_max_pages = _default_full_company_roster_max_pages(
        effective_target_company,
        acquisition_strategy,
    )
    include_former_search_seed = _should_include_default_former_search_seed(
        categories=categories,
        employment_statuses=employment_statuses,
        acquisition_strategy=acquisition_strategy,
        execution_preferences=effective_execution_preferences,
    )
    task_intent_view = _build_task_intent_view_metadata(
        intent_view=intent_view,
        acquisition_strategy=acquisition_strategy,
        search_strategy=search_strategy,
    )
    company_employee_shard_policy = _default_full_company_roster_shard_policy(
        target_company=effective_target_company,
        acquisition_strategy=acquisition_strategy,
        max_pages=roster_max_pages,
        page_limit=FULL_COMPANY_EMPLOYEES_PAGE_LIMIT,
    )
    linkedin_stage_metadata = {
        "acquisition_phase": "linkedin_stage_1",
        "acquisition_phase_title": "LinkedIn Stage 1",
    }
    public_web_stage_metadata = {
        "acquisition_phase": "public_web_stage_2",
        "acquisition_phase_title": "Public Web Stage 2",
    }

    tasks = [
        AcquisitionTask(
            task_id="resolve-company-identity",
            task_type="resolve_company_identity",
            title="Resolve company identifiers",
            description="Find company slug, LinkedIn identity, aliases, and canonical target scope.",
            source_hint="company website / LinkedIn / provider-specific company resolver",
            status="ready" if has_target_company else "needs_input",
            blocking=True,
        ),
        AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire current company roster",
            description="Pull current-member roster coverage from company-member endpoints before applying sourcing criteria.",
            source_hint="Harvest company employees / LinkedIn company people / internal org roster",
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
                "max_pages": roster_max_pages,
                "page_limit": FULL_COMPANY_EMPLOYEES_PAGE_LIMIT,
                "company_employee_shards": [],
                "company_employee_shard_policy": company_employee_shard_policy,
                "company_employee_shard_strategy": str(company_employee_shard_policy.get("strategy_id") or "").strip(),
                "include_former_search_seed": include_former_search_seed,
                "intent_view": _task_intent_view_with_overrides(
                    task_intent_view,
                    max_pages=roster_max_pages,
                    page_limit=FULL_COMPANY_EMPLOYEES_PAGE_LIMIT,
                    company_employee_shards=[],
                    company_employee_shard_policy=company_employee_shard_policy,
                    company_employee_shard_strategy=str(company_employee_shard_policy.get("strategy_id") or "").strip(),
                    include_former_search_seed=include_former_search_seed,
                    acquisition_phase="linkedin_stage_1",
                    acquisition_phase_title="LinkedIn Stage 1",
                ),
                **linkedin_stage_metadata,
            },
        ),
    ]
    if include_former_search_seed:
        tasks.append(
            AcquisitionTask(
                task_id="acquire-former-search-seed",
                task_type="acquire_former_search_seed",
                title="Acquire former-member LinkedIn search seeds",
                description="Run LinkedIn profile search against past-company filters to recover former members explicitly.",
                source_hint="Harvest profile search (past company filters)",
                status="ready" if has_target_company else "needs_input",
                blocking=False,
                metadata={
                    "strategy_type": "former_employee_search",
                    "employment_statuses": ["former"],
                    "search_channel_order": ["harvest_profile_search"],
                    "search_seed_queries": acquisition_strategy.search_seed_queries,
                    "search_query_bundles": [bundle.to_record() for bundle in search_strategy.query_bundles],
                    "filter_hints": acquisition_strategy.filter_hints,
                    "cost_policy": acquisition_strategy.cost_policy,
                    "former_provider_people_search_min_expected_results": 50,
                    "intent_view": _task_intent_view_with_overrides(
                        task_intent_view,
                        strategy_type="former_employee_search",
                        employment_statuses=["former"],
                        search_channel_order=["harvest_profile_search"],
                        former_provider_people_search_min_expected_results=50,
                        acquisition_phase="linkedin_stage_1",
                        acquisition_phase_title="LinkedIn Stage 1",
                    ),
                    **linkedin_stage_metadata,
                },
            )
        )
    tasks.extend(
        [
            AcquisitionTask(
                task_id="enrich-linkedin-profiles",
                task_type="enrich_linkedin_profiles",
                title="Enrich LinkedIn profiles",
                description="Fetch and merge LinkedIn profile detail for current and former members to build the stage-1 roster baseline.",
                source_hint="LinkedIn profile detail / Harvest profile scraper",
                status="ready" if has_target_company else "needs_input",
                blocking=True,
                metadata={
                    "strategy_type": acquisition_strategy.strategy_type,
                    "search_channel_order": acquisition_strategy.search_channel_order,
                    "cost_policy": acquisition_strategy.cost_policy,
                    "slug_resolution_limit": request.slug_resolution_limit,
                    "profile_detail_limit": request.profile_detail_limit,
                    "full_roster_profile_prefetch": acquisition_strategy.strategy_type == "full_company_roster",
                    "reuse_existing_roster": bool(effective_execution_preferences.get("reuse_existing_roster")),
                    "enrichment_scope": "linkedin_stage_1",
                    "intent_view": _task_intent_view_with_overrides(
                        task_intent_view,
                        slug_resolution_limit=request.slug_resolution_limit,
                        profile_detail_limit=request.profile_detail_limit,
                        full_roster_profile_prefetch=acquisition_strategy.strategy_type == "full_company_roster",
                        reuse_existing_roster=bool(effective_execution_preferences.get("reuse_existing_roster")),
                        enrichment_scope="linkedin_stage_1",
                        acquisition_phase="linkedin_stage_1",
                        acquisition_phase_title="LinkedIn Stage 1",
                    ),
                    **linkedin_stage_metadata,
                },
            ),
            AcquisitionTask(
                task_id="enrich-public-web-signals",
                task_type="enrich_public_web_signals",
                title="Enrich public-web signals",
                description="Add publications, co-author graph, exploration, and other public-web evidence as stage-2 acquisition.",
                source_hint="Scholar / arXiv / company blog / GitHub / X / broader web",
                status="ready" if has_target_company else "needs_input",
                blocking=True,
                metadata={
                    "strategy_type": acquisition_strategy.strategy_type,
                    "search_channel_order": acquisition_strategy.search_channel_order,
                    "cost_policy": acquisition_strategy.cost_policy,
                    "publication_source_families": [item.family for item in publication_coverage.source_families],
                    "publication_extraction_strategy": publication_coverage.extraction_strategy,
                    "publication_scan_limit": request.publication_scan_limit,
                    "publication_lead_limit": request.publication_lead_limit,
                    "exploration_limit": request.exploration_limit,
                    "scholar_coauthor_follow_up_limit": request.scholar_coauthor_follow_up_limit,
                    "enrichment_scope": "public_web_stage_2",
                    "intent_view": _task_intent_view_with_overrides(
                        task_intent_view,
                        publication_source_families=[item.family for item in publication_coverage.source_families],
                        publication_extraction_strategy=publication_coverage.extraction_strategy,
                        publication_scan_limit=request.publication_scan_limit,
                        publication_lead_limit=request.publication_lead_limit,
                        exploration_limit=request.exploration_limit,
                        scholar_coauthor_follow_up_limit=request.scholar_coauthor_follow_up_limit,
                        enrichment_scope="public_web_stage_2",
                        acquisition_phase="public_web_stage_2",
                        acquisition_phase_title="Public Web Stage 2",
                    ),
                    **public_web_stage_metadata,
                },
            ),
        ]
    )
    tasks.extend(
        [
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
    )
    return tasks


def _build_task_intent_view_metadata(
    *,
    intent_view: dict[str, Any],
    acquisition_strategy: AcquisitionStrategyPlan,
    search_strategy: SearchStrategyPlan | None = None,
) -> dict[str, Any]:
    return {
        "target_company": str(intent_view.get("target_company") or "").strip(),
        "categories": list(intent_view.get("categories") or []),
        "employment_statuses": list(intent_view.get("employment_statuses") or []),
        "organization_keywords": list(intent_view.get("organization_keywords") or []),
        "keywords": list(intent_view.get("keywords") or []),
        "must_have_keywords": list(intent_view.get("must_have_keywords") or []),
        "must_have_facets": list(intent_view.get("must_have_facets") or []),
        "must_have_primary_role_buckets": list(intent_view.get("must_have_primary_role_buckets") or []),
        "primary_role_bucket_mode": str(intent_view.get("primary_role_bucket_mode") or "hard").strip() or "hard",
        "scope_disambiguation": dict(intent_view.get("scope_disambiguation") or {}),
        "execution_preferences": dict(intent_view.get("execution_preferences") or {}),
        "company_scope": list(acquisition_strategy.company_scope or []),
        "filter_hints": dict(acquisition_strategy.filter_hints or {}),
        "filter_keywords": list(acquisition_strategy.filter_hints.get("keywords") or []),
        "function_ids": list(acquisition_strategy.filter_hints.get("function_ids") or []),
        "search_seed_queries": list(acquisition_strategy.search_seed_queries or []),
        "search_query_bundles": [bundle.to_record() for bundle in list(search_strategy.query_bundles or [])]
        if isinstance(search_strategy, SearchStrategyPlan)
        else [],
        "strategy_type": str(acquisition_strategy.strategy_type or "").strip(),
        "search_channel_order": list(acquisition_strategy.search_channel_order or []),
        "cost_policy": dict(acquisition_strategy.cost_policy or {}),
    }


def _clone_task_intent_view_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_company": str(metadata.get("target_company") or "").strip(),
        "categories": list(metadata.get("categories") or []),
        "employment_statuses": list(metadata.get("employment_statuses") or []),
        "organization_keywords": list(metadata.get("organization_keywords") or []),
        "keywords": list(metadata.get("keywords") or []),
        "must_have_keywords": list(metadata.get("must_have_keywords") or []),
        "must_have_facets": list(metadata.get("must_have_facets") or []),
        "must_have_primary_role_buckets": list(metadata.get("must_have_primary_role_buckets") or []),
        "primary_role_bucket_mode": str(metadata.get("primary_role_bucket_mode") or "hard").strip() or "hard",
        "scope_disambiguation": dict(metadata.get("scope_disambiguation") or {}),
        "execution_preferences": dict(metadata.get("execution_preferences") or {}),
        "company_scope": list(metadata.get("company_scope") or []),
        "filter_hints": dict(metadata.get("filter_hints") or {}),
        "filter_keywords": list(metadata.get("filter_keywords") or []),
        "function_ids": list(metadata.get("function_ids") or []),
        "search_seed_queries": list(metadata.get("search_seed_queries") or []),
        "search_query_bundles": [dict(item) for item in list(metadata.get("search_query_bundles") or []) if isinstance(item, dict)],
        "strategy_type": str(metadata.get("strategy_type") or "").strip(),
        "search_channel_order": [str(item).strip() for item in list(metadata.get("search_channel_order") or []) if str(item).strip()],
        "cost_policy": dict(metadata.get("cost_policy") or {}),
    }


def _task_intent_view_with_overrides(metadata: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    cloned = _clone_task_intent_view_metadata(metadata)
    for key, value in overrides.items():
        if isinstance(value, dict):
            cloned[key] = dict(value)
        elif isinstance(value, list):
            cloned[key] = [
                dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif value is not None:
            cloned[key] = value
    return cloned


def _default_full_company_roster_max_pages(
    target_company: str,
    acquisition_strategy: AcquisitionStrategyPlan,
) -> int:
    if acquisition_strategy.strategy_type != "full_company_roster":
        return 10
    company_key = normalize_company_key(target_company)
    if company_key in FULL_COMPANY_EMPLOYEES_LARGE_ORG_KEYS:
        return FULL_COMPANY_EMPLOYEES_LARGE_ORG_MAX_PAGES
    return FULL_COMPANY_EMPLOYEES_DEFAULT_MAX_PAGES


def _default_full_company_roster_shard_policy(
    *,
    target_company: str,
    acquisition_strategy: AcquisitionStrategyPlan,
    max_pages: int,
    page_limit: int,
) -> dict[str, Any]:
    if acquisition_strategy.strategy_type != "full_company_roster":
        return {}
    company_key = normalize_company_key(target_company)
    if bool(acquisition_strategy.cost_policy.get("large_org_keyword_probe_mode")):
        large_org_policy = build_large_org_keyword_probe_shard_policy(
            company_key,
            company_scope=list(acquisition_strategy.company_scope or []),
            keyword_hints=list(acquisition_strategy.filter_hints.get("keywords") or []),
            function_ids=list(acquisition_strategy.filter_hints.get("function_ids") or []),
            max_pages=max_pages,
            page_limit=page_limit,
        )
        if large_org_policy:
            return large_org_policy
    return build_default_company_employee_shard_policy(
        company_key,
        max_pages=max_pages,
        page_limit=page_limit,
    )


def _should_include_default_former_search_seed(
    *,
    categories: list[str],
    employment_statuses: list[str],
    acquisition_strategy: AcquisitionStrategyPlan,
    execution_preferences: dict[str, Any] | None = None,
) -> bool:
    if acquisition_strategy.strategy_type not in {"full_company_roster", "scoped_search_roster"}:
        return False
    preferences = dict(execution_preferences or {})
    if "run_former_search_seed" in preferences:
        return bool(preferences.get("run_former_search_seed"))
    normalized_statuses = {str(item or "").strip().lower() for item in list(employment_statuses or []) if str(item or "").strip()}
    if acquisition_strategy.strategy_type == "scoped_search_roster" and "former" not in normalized_statuses:
        return False
    normalized_categories = {str(item or "").strip().lower() for item in categories if str(item or "").strip()}
    if "investor" in normalized_categories:
        return False
    return True


def _infer_categories(request: JobRequest) -> list[str]:
    text = f"{request.raw_user_request} {request.query}"
    if any(token in text for token in ["投资", "VC", "investor", "投融资"]):
        return ["investor"]
    if any(token in text for token in ["离职", "former", "前员工"]):
        return ["former_employee"]
    return ["employee", "former_employee"]


def _infer_employment_statuses(request: JobRequest) -> list[str]:
    text = f"{request.raw_user_request} {request.query}".lower()
    if any(token in text for token in ["在职", "当前", "current"]) and not any(
        token in text for token in ["离职", "former", "前员工"]
    ):
        return ["current"]
    if any(token in text for token in ["离职", "former", "前员工"]) and not any(
        token in text for token in ["在职", "当前", "current"]
    ):
        return ["former"]
    return ["current", "former"]


def _infer_retrieval_strategy(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> str:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    text = f"{request.raw_user_request} {request.query}"
    effective_must_have_keywords = list(intent_view.get("must_have_keywords") or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or [])
    effective_role_buckets = list(intent_view.get("must_have_primary_role_buckets") or [])
    effective_organization_keywords = list(intent_view.get("organization_keywords") or [])
    effective_keywords = list(intent_view.get("keywords") or [])
    if (
        effective_must_have_keywords
        or effective_must_have_facets
        or effective_role_buckets
        or effective_organization_keywords
    ):
        return "structured"
    if len(effective_keywords) >= 5:
        return "hybrid"
    if any(token in text for token in ["复杂", "综合判断", "匹配度", "适合", "不像 SQL", "corner case", "语义"]):
        return "hybrid"
    return "structured" if effective_keywords else "hybrid"


def _criteria_summary(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    *,
    intent_view: dict[str, Any] | None = None,
) -> str:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    effective_target_company = str(intent_view.get("target_company") or request.target_company).strip() or "unknown"
    segments = [f"target_company={effective_target_company}", f"categories={categories}"]
    if employment_statuses:
        segments.append(f"employment_statuses={employment_statuses}")
    if intent_view.get("keywords"):
        segments.append(f"keywords={intent_view['keywords']}")
    if intent_view.get("must_have_facets"):
        segments.append(f"must_have_facets={intent_view['must_have_facets']}")
    if intent_view.get("must_have_primary_role_buckets"):
        role_bucket_key = (
            "must_have_primary_role_buckets"
            if str(intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() == "hard"
            else "soft_primary_role_buckets"
        )
        segments.append(f"{role_bucket_key}={intent_view['must_have_primary_role_buckets']}")
    if intent_view.get("must_have_keywords"):
        segments.append(f"must_have={intent_view['must_have_keywords']}")
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


def _build_open_questions(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    acquisition_strategy,
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[str]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    questions = []
    if not str(intent_view.get("target_company") or request.target_company).strip():
        questions.append("Which company should be scanned first?")
    if not list(intent_view.get("categories") or []) and _needs_population_confirmation(
        request,
        categories,
        employment_statuses,
        acquisition_strategy,
        intent_view=intent_view,
    ):
        questions.append("Should the search include current employees, former employees, investors, or all of them?")
    questions.extend(acquisition_strategy.confirmation_points)
    return questions


def _needs_population_confirmation(
    request: JobRequest,
    categories: list[str],
    employment_statuses: list[str],
    acquisition_strategy,
    *,
    intent_view: dict[str, Any] | None = None,
) -> bool:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    if list(intent_view.get("categories") or []):
        return False
    normalized_statuses = {str(item).strip().lower() for item in employment_statuses if str(item).strip()}
    if normalized_statuses in ({"current"}, {"former"}, {"current", "former"}):
        return False
    if acquisition_strategy.strategy_type == "full_company_roster":
        return False
    normalized_categories = {str(item).strip().lower() for item in categories if str(item).strip()}
    if normalized_categories in ({"employee"}, {"former_employee"}, {"investor"}):
        return False
    return True


def _build_filter_layers(
    request: JobRequest,
    strategy: str,
    categories: list[str],
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[dict[str, object]]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    effective_target_company = str(intent_view.get("target_company") or request.target_company).strip()
    effective_employment_statuses = list(intent_view.get("employment_statuses") or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or [])
    effective_role_buckets = list(intent_view.get("must_have_primary_role_buckets") or [])
    primary_role_bucket_mode = str(intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() or "hard"
    effective_must_have_keywords = list(intent_view.get("must_have_keywords") or [])
    effective_organization_keywords = list(intent_view.get("organization_keywords") or [])
    effective_keywords = list(intent_view.get("keywords") or [])
    layers: list[dict[str, object]] = [
        {
            "layer_id": "population_scope",
            "kind": "hard_filter",
            "description": "Constrain the working population to the target company or approved scope before ranking.",
            "criteria": {
                "target_company": effective_target_company,
                "target_scope": request.target_scope,
                "categories": categories,
                "employment_statuses": effective_employment_statuses,
                "must_have_facets": effective_must_have_facets,
                "must_have_primary_role_buckets": effective_role_buckets if primary_role_bucket_mode == "hard" else [],
                "soft_primary_role_buckets": effective_role_buckets if primary_role_bucket_mode != "hard" else [],
            },
        },
        {
            "layer_id": "must_exclude_filters",
            "kind": "hard_filter",
            "description": "Apply must-have, exclude, and organization filters before recall-heavy ranking.",
            "criteria": {
                "must_have_facets": effective_must_have_facets,
                "must_have_primary_role_buckets": effective_role_buckets if primary_role_bucket_mode == "hard" else [],
                "soft_primary_role_buckets": effective_role_buckets if primary_role_bucket_mode != "hard" else [],
                "must_have_keywords": effective_must_have_keywords,
                "exclude_keywords": request.exclude_keywords,
                "organization_keywords": effective_organization_keywords,
            },
        },
        {
            "layer_id": "lexical_alias_recall",
            "kind": "lexical_recall",
            "description": "Use keyword, alias, and deterministic pattern matching to form the initial recall set.",
            "criteria": {
                "keywords": effective_keywords,
                "soft_primary_role_buckets": effective_role_buckets if primary_role_bucket_mode != "hard" else [],
            },
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


def _semantic_fields_for_request(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[str]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    fields = ["role", "team", "focus_areas", "derived_facets", "education", "work_history", "notes"]
    if intent_view.get("must_have_primary_role_buckets"):
        return [field_name for field_name in fields if field_name != "notes"]
    return fields
