from __future__ import annotations

from copy import deepcopy
from typing import Any

from .company_registry import normalize_company_key
from .company_shard_planning import build_default_company_employee_shard_policy
from .domain import JobRequest, SourcingPlan
from .execution_preferences import merge_execution_preferences, normalize_execution_preferences
from .planning import (
    FULL_COMPANY_EMPLOYEES_DEFAULT_MAX_PAGES,
    FULL_COMPANY_EMPLOYEES_LARGE_ORG_KEYS,
    FULL_COMPANY_EMPLOYEES_LARGE_ORG_MAX_PAGES,
    FULL_COMPANY_EMPLOYEES_PAGE_LIMIT,
)


def build_plan_review_gate(request: JobRequest, plan: SourcingPlan) -> dict[str, Any]:
    reasons: list[str] = []
    confirmation_items = list(plan.open_questions)
    suggested_actions: list[str] = []
    execution_mode_hints = _build_execution_mode_hints(plan)
    editable_fields = [
        "company_scope",
        "extra_source_families",
        "allow_high_cost_sources",
        "precision_recall_bias",
        "acquisition_strategy_override",
        "use_company_employees_lane",
        "force_fresh_run",
        "reuse_existing_roster",
        "run_former_search_seed",
    ]
    required_before_execution = False
    risk_level = "low"

    strategy_type = str(plan.acquisition_strategy.strategy_type or "")
    if confirmation_items:
        required_before_execution = True
        reasons.append("open_questions_present")
    if strategy_type == "former_employee_search":
        required_before_execution = True
        reasons.append("target_population_scope_should_be_confirmed")
        suggested_actions.append("Confirm whether the roster boundary is correct before execution.")
    if strategy_type == "investor_firm_roster":
        required_before_execution = True
        reasons.append("investor_firm_population_needs_confirmation")
        suggested_actions.append("Confirm which investor firms or tiers should be covered first.")

    cost_policy = dict(plan.acquisition_strategy.cost_policy or {})
    if bool(cost_policy.get("high_cost_requires_approval", True)):
        required_before_execution = True
        reasons.append("high_cost_sources_need_approval")
        suggested_actions.append("Approve or deny high-cost source usage before execution.")

    publication_families = [str(item.family or "").strip() for item in plan.publication_coverage.source_families]
    if len(publication_families) <= 2:
        reasons.append("publication_coverage_is_narrow")
        suggested_actions.append("Consider adding more source families if the target company publishes across multiple channels.")

    if bool(execution_mode_hints.get("incremental_rerun_recommended")):
        reasons.append("segmented_live_company_roster_is_expensive")
        suggested_actions.append(
            "If a reusable current-roster snapshot already exists, prefer reuse_existing_roster + run_former_search_seed for low-cost incremental reruns instead of rerunning segmented company-employees shards."
        )

    if strategy_type == "investor_firm_roster":
        risk_level = "high"
    elif required_before_execution:
        risk_level = "medium"

    return {
        "status": "requires_review" if required_before_execution else "ready",
        "required_before_execution": required_before_execution,
        "risk_level": risk_level,
        "reasons": reasons,
        "confirmation_items": confirmation_items,
        "editable_fields": editable_fields,
        "suggested_actions": suggested_actions,
        "execution_mode_hints": execution_mode_hints,
        "default_extra_source_families": _default_extra_source_families(request, publication_families),
        "target_population": plan.acquisition_strategy.target_population,
        "strategy_type": strategy_type,
    }


def _build_execution_mode_hints(plan: SourcingPlan) -> dict[str, Any]:
    acquire_task = next(
        (task for task in list(plan.acquisition_tasks or []) if str(task.task_type or "") == "acquire_full_roster"),
        None,
    )
    if acquire_task is None:
        return {}

    metadata = dict(acquire_task.metadata or {})
    shards = [dict(item) for item in list(metadata.get("company_employee_shards") or []) if isinstance(item, dict)]
    shard_titles = []
    hints: dict[str, Any] = {}
    if shards:
        shard_titles = [
            str(item.get("title") or item.get("shard_id") or "").strip()
            for item in shards
            if str(item.get("title") or item.get("shard_id") or "").strip()
        ]
        shard_summaries = [
            {
                "shard_id": str(item.get("shard_id") or "").strip(),
                "title": str(item.get("title") or item.get("shard_id") or "").strip(),
                "company_filters": dict(item.get("company_filters") or {}),
                "max_pages": int(item.get("max_pages") or 0),
                "page_limit": int(item.get("page_limit") or 0),
            }
            for item in shards
        ]
        hints = {
            "segmented_company_employee_shard_strategy": str(metadata.get("company_employee_shard_strategy") or "").strip(),
            "segmented_company_employee_shard_count": len(shards),
            "segmented_company_employee_shards": shard_summaries,
        }
    else:
        policy = dict(metadata.get("company_employee_shard_policy") or {})
        if not policy:
            return {}
        partition_rules = [
            {
                "rule_id": str(item.get("rule_id") or "").strip(),
                "title": str(item.get("title") or item.get("rule_id") or "").strip(),
                "include_patch": dict(item.get("include_patch") or {}),
                "remainder_exclude_patch": dict(item.get("remainder_exclude_patch") or {}),
            }
            for item in list(policy.get("partition_rules") or [])
            if isinstance(item, dict)
        ]
        hints = {
            "adaptive_company_employee_shard_policy": {
                "strategy_id": str(policy.get("strategy_id") or "").strip(),
                "root_title": str(policy.get("root_title") or "").strip(),
                "root_filters": dict(policy.get("root_filters") or {}),
                "partition_rules": partition_rules,
                "max_pages": int(policy.get("max_pages") or 0),
                "page_limit": int(policy.get("page_limit") or 0),
                "provider_result_cap": int(policy.get("provider_result_cap") or 0),
            },
            "segmented_company_employee_shard_strategy": str(policy.get("strategy_id") or metadata.get("company_employee_shard_strategy") or "").strip(),
            "adaptive_probe_required_before_live_roster": True,
        }
        shard_titles = [
            str(item.get("title") or item.get("rule_id") or "").strip()
            for item in partition_rules
            if str(item.get("title") or item.get("rule_id") or "").strip()
        ]
    if plan.acquisition_strategy.strategy_type == "full_company_roster":
        hints["incremental_rerun_recommended"] = True
        hints["recommended_decision_patch"] = {
            "reuse_existing_roster": True,
            "run_former_search_seed": True,
        }
        if shard_titles:
            hints["operator_instruction_examples"] = [
                "沿用现有 roster，只补 former 增量，不要重抓 current roster。",
                "基于现有 roster 继续做增量，只补新的方法和 former，不重新拉公司全量。",
                f"如果一定要 fresh run，接受按 {' / '.join(shard_titles[:4])} 做 live probe 后再自动分片。",
            ]
    return hints


def apply_plan_review_decision(
    request_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    decision_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated_request = deepcopy(request_payload or {})
    updated_plan = deepcopy(plan_payload or {})
    decision = dict(decision_payload or {})

    extra_source_families = _normalize_list(decision.get("extra_source_families"))
    confirmed_scope = _normalize_list(decision.get("confirmed_company_scope"))
    allow_high_cost_sources = bool(decision.get("allow_high_cost_sources") or decision.get("high_cost_sources_approved"))
    precision_recall_bias = str(decision.get("precision_recall_bias") or "").strip().lower()
    acquisition_strategy_override = str(
        decision.get("acquisition_strategy_override")
        or decision.get("force_acquisition_strategy")
        or ""
    ).strip()
    use_company_employees_lane = _coerce_bool(
        decision,
        "use_company_employees_lane",
        "force_company_employees",
        "allow_company_employee_api",
    )
    force_fresh_run = _coerce_bool(
        decision,
        "force_fresh_run",
        "require_fresh_snapshot",
        "disable_cached_roster_fallback",
    )

    if extra_source_families:
        publication = dict(updated_plan.get("publication_coverage") or {})
        existing = {
            str(item.get("family") or "").strip().lower()
            for item in publication.get("source_families") or []
            if isinstance(item, dict)
        }
        source_families = list(publication.get("source_families") or [])
        seed_queries = list(publication.get("seed_queries") or [])
        target_company = str(updated_request.get("target_company") or "").strip()
        for family in extra_source_families:
            normalized = family.lower()
            if normalized in existing:
                continue
            existing.add(normalized)
            source_families.append(
                {
                    "family": family,
                    "priority": "medium",
                    "rationale": "Added during plan review.",
                    "query_hints": [target_company, family] if target_company else [family],
                    "extraction_mode": "model_assisted",
                }
            )
            seed_query = " ".join(part for part in [target_company, family] if part).strip()
            if seed_query and seed_query not in seed_queries:
                seed_queries.append(seed_query)
        publication["source_families"] = source_families
        publication["seed_queries"] = seed_queries
        updated_plan["publication_coverage"] = publication

    if confirmed_scope:
        acquisition_strategy = dict(updated_plan.get("acquisition_strategy") or {})
        target_company = str(updated_request.get("target_company") or "").strip()
        new_scope = [target_company] if target_company else []
        for item in confirmed_scope:
            if item not in new_scope:
                new_scope.append(item)
        acquisition_strategy["company_scope"] = new_scope
        filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
        filter_hints["scope_keywords"] = confirmed_scope
        acquisition_strategy["filter_hints"] = filter_hints
        updated_plan["acquisition_strategy"] = acquisition_strategy

    if allow_high_cost_sources or precision_recall_bias:
        acquisition_strategy = dict(updated_plan.get("acquisition_strategy") or {})
        cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
        if allow_high_cost_sources:
            cost_policy["high_cost_sources_approved"] = True
        if precision_recall_bias:
            cost_policy["precision_recall_bias"] = precision_recall_bias
        acquisition_strategy["cost_policy"] = cost_policy
        updated_plan["acquisition_strategy"] = acquisition_strategy

    if acquisition_strategy_override or use_company_employees_lane or force_fresh_run:
        _apply_acquisition_review_preferences(
            updated_plan,
            target_company=str(updated_request.get("target_company") or "").strip(),
            strategy_override=acquisition_strategy_override,
            use_company_employees_lane=use_company_employees_lane,
            force_fresh_run=force_fresh_run,
        )

    _sync_request_execution_preferences(updated_request, decision)
    _sync_task_metadata(updated_plan)
    return updated_request, updated_plan


def _sync_task_metadata(plan_payload: dict[str, Any]) -> None:
    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    publication = dict(plan_payload.get("publication_coverage") or {})
    publication_families = [
        str(item.get("family") or "").strip()
        for item in publication.get("source_families") or []
        if isinstance(item, dict) and str(item.get("family") or "").strip()
    ]
    cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
    company_scope = list(acquisition_strategy.get("company_scope") or [])
    target_company = str(company_scope[0] or "").strip() if company_scope else ""
    filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
    strategy_type = str(acquisition_strategy.get("strategy_type") or "").strip()
    search_channel_order = list(acquisition_strategy.get("search_channel_order") or [])
    search_seed_queries = list(acquisition_strategy.get("search_seed_queries") or [])
    max_pages = _default_review_full_roster_max_pages(target_company) if strategy_type == "full_company_roster" else 10
    shard_policy = (
        build_default_company_employee_shard_policy(
            normalize_company_key(target_company),
            max_pages=max_pages,
            page_limit=FULL_COMPANY_EMPLOYEES_PAGE_LIMIT,
        )
        if strategy_type == "full_company_roster"
        else {}
    )
    for task in plan_payload.get("acquisition_tasks") or []:
        if not isinstance(task, dict):
            continue
        metadata = dict(task.get("metadata") or {})
        metadata["strategy_type"] = strategy_type
        metadata["company_scope"] = company_scope
        metadata["filter_hints"] = filter_hints
        metadata["cost_policy"] = cost_policy
        metadata["search_channel_order"] = search_channel_order
        metadata["search_seed_queries"] = search_seed_queries
        if str(task.get("task_type") or "") == "acquire_full_roster":
            metadata["max_pages"] = max_pages
            metadata["page_limit"] = FULL_COMPANY_EMPLOYEES_PAGE_LIMIT
            metadata["company_employee_shards"] = []
            metadata["company_employee_shard_policy"] = shard_policy
            metadata["company_employee_shard_strategy"] = str(shard_policy.get("strategy_id") or "").strip()
        if str(task.get("task_type") or "") == "enrich_profiles_multisource":
            metadata["publication_source_families"] = publication_families
        task["metadata"] = metadata


def _default_review_full_roster_max_pages(target_company: str) -> int:
    company_key = normalize_company_key(target_company)
    if company_key in FULL_COMPANY_EMPLOYEES_LARGE_ORG_KEYS:
        return FULL_COMPANY_EMPLOYEES_LARGE_ORG_MAX_PAGES
    return FULL_COMPANY_EMPLOYEES_DEFAULT_MAX_PAGES


def _apply_acquisition_review_preferences(
    plan_payload: dict[str, Any],
    *,
    target_company: str,
    strategy_override: str,
    use_company_employees_lane: bool,
    force_fresh_run: bool,
) -> None:
    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    current_strategy = str(acquisition_strategy.get("strategy_type") or "").strip()
    desired_strategy = current_strategy
    normalized_override = strategy_override.strip().lower()
    if normalized_override in {
        "full_company_roster",
        "scoped_search_roster",
        "former_employee_search",
        "investor_firm_roster",
    }:
        desired_strategy = normalized_override
    elif use_company_employees_lane and current_strategy != "full_company_roster":
        desired_strategy = "full_company_roster"

    if desired_strategy:
        acquisition_strategy["strategy_type"] = desired_strategy
    if desired_strategy == "full_company_roster":
        acquisition_strategy["roster_sources"] = ["linkedin_company_roster", "company_directory_pages", "the_org"]
        if target_company:
            acquisition_strategy["company_scope"] = [target_company]
    elif desired_strategy == "scoped_search_roster":
        acquisition_strategy["roster_sources"] = ["web_search_seed_queries", "linkedin_people_search", "company_suborg_sources"]
    elif desired_strategy == "former_employee_search":
        acquisition_strategy["roster_sources"] = ["web_search_seed_queries", "linkedin_people_search", "news_and_bio_pages"]
    elif desired_strategy == "investor_firm_roster":
        acquisition_strategy["roster_sources"] = ["funding_graph", "investor_firm_roster", "linkedin_people_search"]

    cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
    if use_company_employees_lane or desired_strategy == "full_company_roster":
        cost_policy["allow_company_employee_api"] = True
    if force_fresh_run:
        cost_policy["allow_cached_roster_fallback"] = False
        cost_policy["allow_historical_profile_inheritance"] = False
        cost_policy["allow_shared_provider_cache"] = False
    acquisition_strategy["cost_policy"] = cost_policy

    reasoning = list(acquisition_strategy.get("reasoning") or [])
    if use_company_employees_lane:
        note = "Plan review forced the acquisition lane onto Harvest company-employees for a fresh full roster."
        if note not in reasoning:
            reasoning.append(note)
    if force_fresh_run:
        note = "Plan review requested a fresh live acquisition instead of falling back to cached roster snapshots."
        if note not in reasoning:
            reasoning.append(note)
    acquisition_strategy["reasoning"] = reasoning
    plan_payload["acquisition_strategy"] = acquisition_strategy


def _sync_request_execution_preferences(
    request_payload: dict[str, Any],
    decision_payload: dict[str, Any],
) -> None:
    target_company = str(request_payload.get("target_company") or "").strip()
    current_preferences = normalize_execution_preferences(request_payload, target_company=target_company)
    decision_preferences = normalize_execution_preferences(decision_payload, target_company=target_company)
    if not decision_preferences:
        return
    request_payload["execution_preferences"] = merge_execution_preferences(decision_preferences, current_preferences)


def _default_extra_source_families(request: JobRequest, publication_families: list[str]) -> list[str]:
    text = f"{request.raw_user_request} {request.query}".lower()
    candidates: list[str] = []
    if any(token in text for token in ["research", "论文", "author", "publication"]):
        candidates.append("OpenReview")
    if any(token in text for token in ["engineer", "工程", "system", "infra", "post-train", "pre-train", "rl"]):
        candidates.append("Engineering Blog")
    if any(token in text for token in ["team", "org", "成员", "roster"]):
        candidates.append("Official Team Pages")
    existing = {item.lower() for item in publication_families}
    return [item for item in candidates if item.lower() not in existing]


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    else:
        items = list(value)
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


def _coerce_bool(payload: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return False
