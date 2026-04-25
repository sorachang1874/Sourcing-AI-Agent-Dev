from __future__ import annotations

import re
from typing import Any

from .execution_preferences import (
    infer_execution_preferences_from_text,
    normalize_execution_preferences,
)
from .model_provider import ModelClient
from .query_intent_rewrite import interpret_query_intent_rewrite
from .request_normalization import materialize_request_payload


_SOURCE_FAMILY_ALIASES: dict[str, str] = {
    "openreview": "OpenReview",
    "engineering blog": "Engineering Blog",
    "eng blog": "Engineering Blog",
    "official team pages": "Official Team Pages",
    "team pages": "Official Team Pages",
    "official research": "Official Research",
    "official blog": "Official Blog",
    "official docs": "Official Docs",
    "podcast": "Podcast",
    "podcasts": "Podcast",
    "youtube": "YouTube",
    "public interviews": "Public Interviews",
}

_ALLOWED_DECISION_FIELDS = {
    "confirmed_company_scope",
    "extra_source_families",
    "precision_recall_bias",
    "acquisition_strategy_override",
    "use_company_employees_lane",
    "keyword_priority_only",
    "former_keyword_queries_only",
    "provider_people_search_query_strategy",
    "provider_people_search_max_queries",
    "large_org_keyword_probe_mode",
    "force_fresh_run",
    "reuse_existing_roster",
    "run_former_search_seed",
}
_DECISION_FIELD_ALIASES = {
    "company_scope": "confirmed_company_scope",
    "confirmed_scope": "confirmed_company_scope",
    "scope": "confirmed_company_scope",
    "source_families": "extra_source_families",
    "force_company_employees": "use_company_employees_lane",
    "allow_company_employee_api": "use_company_employees_lane",
    "keyword_first": "keyword_priority_only",
    "keyword_priority": "keyword_priority_only",
    "former_search_queries_only": "former_keyword_queries_only",
    "former_keyword_only": "former_keyword_queries_only",
    "people_search_query_strategy": "provider_people_search_query_strategy",
    "provider_people_query_strategy": "provider_people_search_query_strategy",
    "people_search_max_queries": "provider_people_search_max_queries",
    "provider_people_query_max": "provider_people_search_max_queries",
    "large_org_keyword_probe": "large_org_keyword_probe_mode",
    "require_fresh_snapshot": "force_fresh_run",
    "disable_cached_roster_fallback": "force_fresh_run",
    "reuse_cached_roster": "reuse_existing_roster",
    "skip_current_roster_refresh": "reuse_existing_roster",
    "former_search_seed": "run_former_search_seed",
    "include_former_search_seed": "run_former_search_seed",
}
_ALLOWED_PRECISION_RECALL_BIAS = {"precision_first", "recall_first", "balanced"}
_ALLOWED_ACQUISITION_STRATEGIES = {
    "full_company_roster",
    "scoped_search_roster",
    "former_employee_search",
    "investor_firm_roster",
}


def parse_review_instruction(
    instruction: str,
    *,
    target_company: str = "",
) -> dict[str, Any]:
    text = " ".join(str(instruction or "").strip().split())
    return infer_execution_preferences_from_text(text, target_company=target_company)


def compile_review_payload_from_instruction(
    *,
    review_id: int,
    instruction: str,
    reviewer: str = "",
    notes: str = "",
    action: str = "approved",
    target_company: str = "",
    model_client: ModelClient | None = None,
    gate_payload: dict[str, Any] | None = None,
    request_payload: dict[str, Any] | None = None,
    plan_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_instruction = " ".join(str(instruction or "").strip().split())
    allowed_fields = _resolve_allowed_fields(gate_payload)
    effective_request_payload = materialize_request_payload(
        request_payload,
        target_company=target_company,
    )
    deterministic_decision = normalize_review_decision(
        parse_review_instruction(normalized_instruction, target_company=target_company),
        target_company=target_company,
        allowed_fields=allowed_fields,
    )
    model_raw: dict[str, Any] = {}
    model_decision: dict[str, Any] = {}
    provider_name = ""
    if model_client is not None:
        provider_name = str(model_client.provider_name() or "").strip()
        try:
            model_raw = dict(
                model_client.normalize_review_instruction(
                    {
                        "instruction": str(instruction or "").strip(),
                        "normalized_instruction": normalized_instruction,
                        "target_company": target_company,
                        "request": dict(effective_request_payload or {}),
                        "plan": dict(plan_payload or {}),
                        "gate": dict(gate_payload or {}),
                        "editable_fields": sorted(allowed_fields),
                    }
                )
                or {}
            )
        except Exception:
            model_raw = {}
        model_decision = normalize_review_decision(
            model_raw,
            target_company=target_company,
            allowed_fields=allowed_fields,
        )

    merged_decision = dict(model_decision)
    supplemented_keys: list[str] = []
    if deterministic_decision:
        for key, value in deterministic_decision.items():
            if key in merged_decision:
                continue
            merged_decision[key] = value
            supplemented_keys.append(key)
    policy_inferred_keys = _infer_internal_review_preferences(
        merged_decision,
        instruction=normalized_instruction,
        target_company=target_company,
        allowed_fields=allowed_fields,
        request_payload=effective_request_payload,
        plan_payload=plan_payload,
    )
    compiler_source = "model" if model_decision else "deterministic"
    request_intent_rewrite = interpret_query_intent_rewrite(
        str((effective_request_payload or {}).get("raw_user_request") or (effective_request_payload or {}).get("query") or "")
    )
    instruction_intent_rewrite = interpret_query_intent_rewrite(normalized_instruction)
    review_payload = build_review_payload_from_instruction(
        review_id=review_id,
        instruction=normalized_instruction,
        reviewer=reviewer,
        notes=notes,
        action=action,
        target_company=target_company,
        decision_payload=merged_decision,
    )
    return {
        "review_payload": review_payload,
        "instruction_compiler": {
            "source": compiler_source,
            "provider": provider_name or ("deterministic" if compiler_source == "deterministic" else ""),
            "allowed_fields": sorted(allowed_fields),
            "model_decision": model_decision,
            "deterministic_decision": deterministic_decision,
            "supplemented_keys": supplemented_keys,
            "policy_inferred_keys": policy_inferred_keys,
            "request_intent_rewrite": request_intent_rewrite,
            "instruction_intent_rewrite": instruction_intent_rewrite,
            "fallback_used": bool(supplemented_keys) or not bool(model_decision),
        },
    }


def build_review_payload_from_instruction(
    *,
    review_id: int,
    instruction: str,
    reviewer: str = "",
    notes: str = "",
    action: str = "approved",
    target_company: str = "",
    decision_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_action = str(action or "approved").strip().lower()
    if normalized_action in {"approve", "approved"}:
        normalized_action = "approved"
    elif normalized_action in {"reject", "rejected"}:
        normalized_action = "rejected"
    else:
        normalized_action = "needs_changes"
    review_notes = str(notes or "").strip() or str(instruction or "").strip()
    return {
        "review_id": int(review_id),
        "action": normalized_action,
        "reviewer": str(reviewer or "").strip(),
        "notes": review_notes,
        "decision": dict(
            decision_payload
            if decision_payload is not None
            else parse_review_instruction(instruction, target_company=target_company)
        ),
    }


def normalize_review_decision(
    payload: dict[str, Any],
    *,
    target_company: str = "",
    allowed_fields: set[str] | None = None,
) -> dict[str, Any]:
    candidate = dict(payload or {})
    if isinstance(candidate.get("decision"), dict):
        candidate = dict(candidate.get("decision") or {})
    effective_allowed_fields = set(allowed_fields or _ALLOWED_DECISION_FIELDS)
    normalized_allowed_fields = {
        _DECISION_FIELD_ALIASES.get(field, field)
        for field in effective_allowed_fields
        if _DECISION_FIELD_ALIASES.get(field, field) in _ALLOWED_DECISION_FIELDS
    }
    normalized = normalize_execution_preferences(candidate, target_company=target_company)
    return {
        key: value
        for key, value in normalized.items()
        if key in _ALLOWED_DECISION_FIELDS and key in normalized_allowed_fields
    }


def _parse_acquisition_strategy(lower: str) -> str:
    if any(
        token in lower
        for token in [
            "full company roster",
            "full roster",
            "全量 roster",
            "全量成员",
            "整家公司",
            "全公司",
            "全量公司成员",
        ]
    ):
        return "full_company_roster"
    if any(
        token in lower
        for token in [
            "former employee",
            "former employees",
            "past company only",
            "已离职",
            "前员工",
            "former roster",
        ]
    ):
        return "former_employee_search"
    if any(
        token in lower
        for token in [
            "investor firm",
            "investor roster",
            "投资机构",
            "投资方成员",
        ]
    ):
        return "investor_firm_roster"
    if any(
        token in lower
        for token in [
            "scoped roster",
            "scoped search roster",
            "按团队范围",
            "限定团队",
            "限定 scope",
            "限定范围",
        ]
    ):
        return "scoped_search_roster"
    return ""


def _mentions_company_employees_lane(lower: str) -> bool:
    return any(
        token in lower
        for token in [
            "company-employees",
            "company employees",
            "harvest company-employees",
            "harvest company employees",
        ]
    )


def _mentions_force_fresh_run(lower: str) -> bool:
    return any(
        token in lower
        for token in [
            "force fresh",
            "fresh run",
            "fresh rerun",
            "重新跑",
            "重新获取",
            "重跑",
            "不要缓存",
            "禁用缓存",
            "disable cache",
            "no cache",
            "不用历史继承",
            "不复用历史",
        ]
    )


def _parse_allow_high_cost(lower: str) -> bool | None:
    deny_patterns = [
        "不允许高成本",
        "不要高成本",
        "禁用高成本",
        "禁止高成本",
        "without high cost",
        "no high cost",
        "disallow high cost",
        "deny high cost",
    ]
    allow_patterns = [
        "允许高成本",
        "可以高成本",
        "approve high cost",
        "allow high cost",
        "enable high cost",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


def _parse_precision_recall_bias(lower: str) -> str:
    if any(token in lower for token in ["precision first", "偏精确", "优先精确", "高精度"]):
        return "precision_first"
    if any(token in lower for token in ["recall first", "偏召回", "优先召回", "高召回"]):
        return "recall_first"
    if any(token in lower for token in ["balanced", "平衡", "均衡"]):
        return "balanced"
    return ""


def _parse_reuse_existing_roster(lower: str) -> bool:
    return any(
        token in lower
        for token in [
            "reuse existing roster",
            "reuse current roster",
            "reuse cached roster",
            "incremental only",
            "skip current roster",
            "沿用现有 roster",
            "复用已有 roster",
            "基于现有 roster",
            "只做增量",
            "只跑增量",
            "沿用之前抓过的 roster",
            "基于之前抓过的 roster",
            "复用之前抓过的 roster",
            "不要重新拉 current roster",
            "不要重新拉公司全量",
            "不要重拉 current roster",
            "只补新的方法",
            "不重新抓 current roster",
            "不要重抓 current roster",
            "不调用 roster api",
            "只用 search api",
        ]
    )


def _parse_run_former_search_seed(lower: str) -> bool | None:
    deny_patterns = [
        "不要 former",
        "不看 former",
        "skip former",
        "不要前员工",
        "不补 former",
    ]
    allow_patterns = [
        "former 增量",
        "补 former",
        "加 former",
        "former search",
        "former seed",
        "加 former 增量",
        "只补 former",
        "只跑 former",
        "只补 former 和新方法",
        "加前员工",
        "加已离职",
        "补前员工",
        "补已离职",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


def _parse_extra_source_families(lower: str) -> list[str]:
    families: list[str] = []
    for alias, canonical in _SOURCE_FAMILY_ALIASES.items():
        if alias in lower and canonical not in families:
            families.append(canonical)
    return families


def _parse_confirmed_scope(text: str, lower: str, *, target_company: str) -> list[str]:
    scope: list[str] = []
    if target_company and any(token in lower for token in ["只看", "only", "scope", "范围", "限定"]):
        if target_company.lower() in lower:
            scope.append(target_company)
    match = re.search(r"(?:scope|范围)[:：]\s*([^\n,;]+)", text, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        for part in re.split(r"[/,，、]+", raw):
            item = part.strip()
            if item and item not in scope:
                scope.append(item)
    return scope


def _resolve_allowed_fields(gate_payload: dict[str, Any] | None) -> set[str]:
    raw_fields = list((gate_payload or {}).get("editable_fields") or [])
    if not raw_fields:
        return set(_ALLOWED_DECISION_FIELDS)
    resolved = {
        _DECISION_FIELD_ALIASES.get(str(field or "").strip(), str(field or "").strip())
        for field in raw_fields
        if str(field or "").strip()
    }
    return {field for field in resolved if field in _ALLOWED_DECISION_FIELDS} or set(_ALLOWED_DECISION_FIELDS)


def _infer_internal_review_preferences(
    decision: dict[str, Any],
    *,
    instruction: str,
    target_company: str,
    allowed_fields: set[str],
    request_payload: dict[str, Any] | None,
    plan_payload: dict[str, Any] | None,
) -> list[str]:
    inferred: list[str] = []
    lower = str(instruction or "").lower()
    if (
        "use_company_employees_lane" in allowed_fields
        and "use_company_employees_lane" not in decision
        and not bool(decision.get("reuse_existing_roster"))
        and _should_infer_company_employees_lane(
            lower=lower,
            decision=decision,
            target_company=target_company,
            request_payload=request_payload,
            plan_payload=plan_payload,
        )
    ):
        decision["use_company_employees_lane"] = True
        inferred.append("use_company_employees_lane")
    return inferred


def _normalize_string_list(value: Any, *, key: str, target_company: str) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[/,，、;\n]+", value)
    elif isinstance(value, list):
        raw_items = [str(item) for item in value]
    else:
        raw_items = []
    items: list[str] = []
    for raw in raw_items:
        item = " ".join(str(raw or "").strip().split())
        if not item:
            continue
        if key == "extra_source_families":
            canonical = _SOURCE_FAMILY_ALIASES.get(item.lower(), item)
            if canonical not in items:
                items.append(canonical[:80])
        else:
            if item.lower() == str(target_company or "").strip().lower():
                item = str(target_company or "").strip() or item
            if item not in items:
                items.append(item[:120])
    return items[:10]


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "yes", "y", "allow", "allowed", "enable", "enabled"}:
        return True
    if normalized in {"false", "0", "no", "n", "deny", "denied", "disable", "disabled"}:
        return False
    return None


def _normalize_precision_recall_bias(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "precision": "precision_first",
        "precision-first": "precision_first",
        "precision first": "precision_first",
        "recall": "recall_first",
        "recall-first": "recall_first",
        "recall first": "recall_first",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in _ALLOWED_PRECISION_RECALL_BIAS else ""


def _normalize_acquisition_strategy(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "full roster": "full_company_roster",
        "full company roster": "full_company_roster",
        "scoped roster": "scoped_search_roster",
        "scoped search roster": "scoped_search_roster",
        "former employee": "former_employee_search",
        "former employees": "former_employee_search",
        "investor firm": "investor_firm_roster",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in _ALLOWED_ACQUISITION_STRATEGIES else ""


def _should_infer_company_employees_lane(
    *,
    lower: str,
    decision: dict[str, Any],
    target_company: str,
    request_payload: dict[str, Any] | None,
    plan_payload: dict[str, Any] | None,
) -> bool:
    if _mentions_company_employees_lane(lower):
        return True
    strategy = str(decision.get("acquisition_strategy_override") or "").strip().lower()
    if strategy != "full_company_roster":
        return False
    request = dict(request_payload or {})
    request = materialize_request_payload(request, target_company=target_company)
    plan = dict(plan_payload or {})
    plan_strategy = str(dict(plan.get("acquisition_strategy") or {}).get("strategy_type") or "").strip().lower()
    employment_statuses = [str(item).strip().lower() for item in list(request.get("employment_statuses") or []) if str(item).strip()]
    categories = [str(item).strip().lower() for item in list(request.get("categories") or []) if str(item).strip()]
    if employment_statuses == ["former"] or "former_employee" in categories or "investor" in categories:
        return False
    if plan_strategy == "former_employee_search":
        return False
    broad_scope_signals = any(
        token in lower
        for token in [
            "全量成员",
            "整家公司",
            "全公司的",
            "全公司",
            "公司全量",
            "full company",
            "full roster",
            "company-wide",
            "company wide",
            "roster",
            "scope 大一些",
            "范围不要太窄",
            "范围大一些",
        ]
    )
    fresh_run = bool(decision.get("force_fresh_run"))
    confirmed_scope = list(decision.get("confirmed_company_scope") or [])
    company_scope_match = bool(target_company and confirmed_scope == [target_company])
    return broad_scope_signals or fresh_run or company_scope_match
