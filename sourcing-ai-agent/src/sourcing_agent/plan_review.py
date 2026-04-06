from __future__ import annotations

from copy import deepcopy
from typing import Any

from .domain import JobRequest, SourcingPlan


def build_plan_review_gate(request: JobRequest, plan: SourcingPlan) -> dict[str, Any]:
    reasons: list[str] = []
    confirmation_items = list(plan.open_questions)
    suggested_actions: list[str] = []
    editable_fields = [
        "company_scope",
        "extra_source_families",
        "allow_high_cost_sources",
        "precision_recall_bias",
    ]
    required_before_execution = False
    risk_level = "low"

    strategy_type = str(plan.acquisition_strategy.strategy_type or "")
    if confirmation_items:
        required_before_execution = True
        reasons.append("open_questions_present")
    if strategy_type in {"scoped_search_roster", "former_employee_search"}:
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
        "default_extra_source_families": _default_extra_source_families(request, publication_families),
        "target_population": plan.acquisition_strategy.target_population,
        "strategy_type": strategy_type,
    }


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
    filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
    for task in plan_payload.get("acquisition_tasks") or []:
        if not isinstance(task, dict):
            continue
        metadata = dict(task.get("metadata") or {})
        metadata["company_scope"] = company_scope
        metadata["filter_hints"] = filter_hints
        metadata["cost_policy"] = cost_policy
        if str(task.get("task_type") or "") == "enrich_profiles_multisource":
            metadata["publication_source_families"] = publication_families
        task["metadata"] = metadata


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
