from __future__ import annotations

import re
from typing import Any


EXECUTION_PREFERENCE_FIELDS = {
    "confirmed_company_scope",
    "extra_source_families",
    "allow_high_cost_sources",
    "precision_recall_bias",
    "acquisition_strategy_override",
    "use_company_employees_lane",
    "force_fresh_run",
    "reuse_existing_roster",
    "run_former_search_seed",
}

EXECUTION_PREFERENCE_ALIASES = {
    "company_scope": "confirmed_company_scope",
    "confirmed_scope": "confirmed_company_scope",
    "scope": "confirmed_company_scope",
    "source_families": "extra_source_families",
    "high_cost_sources_approved": "allow_high_cost_sources",
    "force_company_employees": "use_company_employees_lane",
    "allow_company_employee_api": "use_company_employees_lane",
    "require_fresh_snapshot": "force_fresh_run",
    "disable_cached_roster_fallback": "force_fresh_run",
    "reuse_cached_roster": "reuse_existing_roster",
    "skip_current_roster_refresh": "reuse_existing_roster",
    "former_search_seed": "run_former_search_seed",
    "include_former_search_seed": "run_former_search_seed",
}

ALLOWED_PRECISION_RECALL_BIAS = {"precision_first", "recall_first", "balanced"}
ALLOWED_ACQUISITION_STRATEGIES = {
    "full_company_roster",
    "scoped_search_roster",
    "former_employee_search",
    "investor_firm_roster",
}

SOURCE_FAMILY_ALIASES: dict[str, str] = {
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


def normalize_execution_preferences(
    payload: dict[str, Any] | None,
    *,
    target_company: str = "",
) -> dict[str, Any]:
    candidate = dict(payload or {})
    if isinstance(candidate.get("execution_preferences"), dict):
        candidate = dict(candidate.get("execution_preferences") or {})
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in candidate.items():
        key = EXECUTION_PREFERENCE_ALIASES.get(str(raw_key or "").strip(), str(raw_key or "").strip())
        if key not in EXECUTION_PREFERENCE_FIELDS:
            continue
        if key in {"confirmed_company_scope", "extra_source_families"}:
            items = _normalize_string_list(raw_value, key=key, target_company=target_company)
            if items:
                normalized[key] = items
            continue
        if key in {
            "allow_high_cost_sources",
            "use_company_employees_lane",
            "force_fresh_run",
            "reuse_existing_roster",
            "run_former_search_seed",
        }:
            value = _coerce_bool(raw_value)
            if value is not None:
                normalized[key] = value
            continue
        if key == "precision_recall_bias":
            value = _normalize_precision_recall_bias(raw_value)
            if value:
                normalized[key] = value
            continue
        if key == "acquisition_strategy_override":
            value = _normalize_acquisition_strategy(raw_value)
            if value:
                normalized[key] = value
    return normalized


def infer_execution_preferences_from_text(
    text: str,
    *,
    target_company: str = "",
) -> dict[str, Any]:
    normalized_text = " ".join(str(text or "").strip().split())
    lower = normalized_text.lower()
    prefs: dict[str, Any] = {}
    strategy = _infer_strategy_override_from_text(lower)
    if strategy:
        prefs["acquisition_strategy_override"] = strategy
    force_fresh_run = _infer_force_fresh_run(lower)
    if force_fresh_run:
        prefs["force_fresh_run"] = True
    reuse_existing_roster = _infer_reuse_existing_roster(lower)
    if reuse_existing_roster:
        prefs["reuse_existing_roster"] = True
    run_former_search_seed = _infer_run_former_search_seed(lower)
    if run_former_search_seed is not None:
        prefs["run_former_search_seed"] = run_former_search_seed
    allow_high_cost = _infer_allow_high_cost(lower)
    if allow_high_cost is not None:
        prefs["allow_high_cost_sources"] = allow_high_cost
    precision_recall_bias = _infer_precision_recall_bias(lower)
    if precision_recall_bias:
        prefs["precision_recall_bias"] = precision_recall_bias
    confirmed_scope = _infer_confirmed_scope(normalized_text, lower, target_company=target_company)
    if confirmed_scope:
        prefs["confirmed_company_scope"] = confirmed_scope
    if _mentions_company_employees_lane(lower):
        prefs["use_company_employees_lane"] = True
    return prefs


def merge_execution_preferences(
    base: dict[str, Any] | None,
    patch: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in dict(patch or {}).items():
        if key not in EXECUTION_PREFERENCE_FIELDS:
            continue
        if key in {"confirmed_company_scope", "extra_source_families"}:
            if key not in merged or not list(merged.get(key) or []):
                merged[key] = list(value or [])
            else:
                existing = list(merged.get(key) or [])
                for item in list(value or []):
                    if item not in existing:
                        existing.append(item)
                merged[key] = existing
            continue
        if key not in merged:
            merged[key] = value
    return merged


def apply_execution_preference_policy(
    preferences: dict[str, Any],
    *,
    raw_text: str,
    target_company: str,
    categories: list[str],
    employment_statuses: list[str],
    current_strategy_type: str = "",
) -> dict[str, Any]:
    updated = dict(preferences or {})
    lower = " ".join(str(raw_text or "").strip().split()).lower()
    if (
        "use_company_employees_lane" not in updated
        and _should_infer_company_employees_lane(
            lower=lower,
            preferences=updated,
            target_company=target_company,
            categories=categories,
            employment_statuses=employment_statuses,
            current_strategy_type=current_strategy_type,
        )
    ):
        updated["use_company_employees_lane"] = True
    return updated


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
            canonical = SOURCE_FAMILY_ALIASES.get(item.lower(), item)
            if canonical not in items:
                items.append(canonical[:80])
            continue
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
    return normalized if normalized in ALLOWED_PRECISION_RECALL_BIAS else ""


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
    return normalized if normalized in ALLOWED_ACQUISITION_STRATEGIES else ""


def _infer_strategy_override_from_text(lower: str) -> str:
    if any(token in lower for token in ["former employee", "former employees", "前员工", "已离职"]):
        return "former_employee_search"
    if any(token in lower for token in ["投资", "investor", "vc", "投融资"]):
        return ""
    if any(
        token in lower
        for token in [
            "full company roster",
            "full roster",
            "company-wide",
            "company wide",
            "全量 roster",
            "全量成员",
            "全量资产",
            "整家公司",
            "整家公司的人",
            "全公司的 roster",
            "全公司的人",
            "公司全量成员",
            "scope 大一些",
            "范围不要太窄",
            "范围大一些",
            "scope bigger",
            "broader scope",
        ]
    ):
        return "full_company_roster"
    return ""


def _infer_force_fresh_run(lower: str) -> bool:
    return any(
        token in lower
        for token in [
            "force fresh",
            "fresh run",
            "fresh rerun",
            "fresh 一点",
            "重新跑",
            "重新获取",
            "重跑",
            "别用缓存",
            "不要缓存",
            "禁用缓存",
            "disable cache",
            "no cache",
            "不用历史继承",
            "不复用历史",
        ]
    )


def _infer_allow_high_cost(lower: str) -> bool | None:
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


def _infer_reuse_existing_roster(lower: str) -> bool:
    return any(
        token in lower
        for token in [
            "reuse existing roster",
            "reuse current roster",
            "reuse cached roster",
            "incremental only",
            "skip current roster",
            "skip current roster refresh",
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


def _infer_run_former_search_seed(lower: str) -> bool | None:
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


def _infer_precision_recall_bias(lower: str) -> str:
    if any(token in lower for token in ["precision first", "偏精确", "优先精确", "高精度"]):
        return "precision_first"
    if any(token in lower for token in ["recall first", "偏召回", "优先召回", "高召回"]):
        return "recall_first"
    if any(token in lower for token in ["balanced", "平衡", "均衡"]):
        return "balanced"
    return ""


def _infer_confirmed_scope(text: str, lower: str, *, target_company: str) -> list[str]:
    scope: list[str] = []
    if target_company and any(token in lower for token in ["整家公司", "全公司", "全量成员", "company-wide", "full company"]):
        scope.append(target_company)
    match = re.search(r"(?:scope|范围)[:：]\s*([^\n,;]+)", text, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        for part in re.split(r"[/,，、]+", raw):
            item = part.strip()
            if item and item not in scope:
                scope.append(item)
    return scope


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


def _should_infer_company_employees_lane(
    *,
    lower: str,
    preferences: dict[str, Any],
    target_company: str,
    categories: list[str],
    employment_statuses: list[str],
    current_strategy_type: str,
) -> bool:
    strategy = str(preferences.get("acquisition_strategy_override") or current_strategy_type or "").strip().lower()
    if strategy != "full_company_roster":
        return False
    if bool(preferences.get("reuse_existing_roster")):
        return False
    normalized_categories = {str(item).strip().lower() for item in categories if str(item).strip()}
    normalized_statuses = {str(item).strip().lower() for item in employment_statuses if str(item).strip()}
    if normalized_statuses == {"former"} or "former_employee" in normalized_categories or "investor" in normalized_categories:
        return False
    if target_company and list(preferences.get("confirmed_company_scope") or []) == [target_company]:
        return True
    if bool(preferences.get("force_fresh_run")):
        return True
    return any(
        token in lower
        for token in [
            "full company",
            "company-wide",
            "company wide",
            "full roster",
            "全量成员",
            "全量资产",
            "整家公司",
            "全公司的人",
            "全公司的 roster",
            "scope 大一些",
            "范围不要太窄",
            "范围大一些",
        ]
    )
