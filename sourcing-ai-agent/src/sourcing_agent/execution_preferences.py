from __future__ import annotations

import re
from typing import Any

from .company_registry import normalize_company_key
from .runtime_tuning import normalize_runtime_tuning_profile

EXECUTION_PREFERENCE_FIELDS = {
    "confirmed_company_scope",
    "extra_source_families",
    "allow_local_bootstrap_fallback",
    "require_stage2_confirmation",
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
    "reuse_snapshot_id",
    "reuse_baseline_job_id",
    "delta_baseline_snapshot_id",
    "delta_baseline_snapshot_ids",
    "target_company_linkedin_url",
    "target_company_linkedin_slug",
    "runtime_tuning_profile",
    "lane_ready_cooldown_seconds",
    "lane_fetch_cooldown_seconds",
    "task_get_batch_workers",
    "harvest_poll_interval_seconds",
    "harvest_retry_backoff_seconds",
    "harvest_scripted_sleep_seconds_cap",
    "provider_people_search_parallel_queries",
    "harvest_company_roster_parallel_shards",
    "candidate_artifact_parallel_min_candidates",
    "candidate_artifact_max_workers",
    "parallel_search_workers",
    "parallel_exploration_workers",
    "harvest_prefetch_submit_workers",
    "search_worker_unit_budget",
    "public_media_worker_unit_budget",
    "exploration_worker_unit_budget",
}

RUNTIME_TUNING_NONNEGATIVE_INT_FIELDS = {
    "lane_ready_cooldown_seconds",
    "lane_fetch_cooldown_seconds",
}

RUNTIME_TUNING_POSITIVE_INT_FIELDS = {
    "task_get_batch_workers",
    "provider_people_search_parallel_queries",
    "harvest_company_roster_parallel_shards",
    "candidate_artifact_parallel_min_candidates",
    "candidate_artifact_max_workers",
    "parallel_search_workers",
    "parallel_exploration_workers",
    "harvest_prefetch_submit_workers",
    "search_worker_unit_budget",
    "public_media_worker_unit_budget",
    "exploration_worker_unit_budget",
}

RUNTIME_TUNING_NONNEGATIVE_FLOAT_FIELDS = {
    "harvest_poll_interval_seconds",
    "harvest_retry_backoff_seconds",
    "harvest_scripted_sleep_seconds_cap",
}

EXECUTION_PREFERENCE_ALIASES = {
    "company_scope": "confirmed_company_scope",
    "confirmed_scope": "confirmed_company_scope",
    "scope": "confirmed_company_scope",
    "source_families": "extra_source_families",
    "pause_before_stage2_analysis": "require_stage2_confirmation",
    "require_stage2_approval": "require_stage2_confirmation",
    "wait_for_stage2_approval": "require_stage2_confirmation",
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
    "company_linkedin_url": "target_company_linkedin_url",
    "linkedin_company_url": "target_company_linkedin_url",
    "company_linkedin_slug": "target_company_linkedin_slug",
    "linkedin_company_slug": "target_company_linkedin_slug",
    "testing_profile": "runtime_tuning_profile",
    "runtime_profile": "runtime_tuning_profile",
}

ALLOWED_PRECISION_RECALL_BIAS = {"precision_first", "recall_first", "balanced"}
ALLOWED_ACQUISITION_STRATEGIES = {
    "full_company_roster",
    "scoped_search_roster",
    "former_employee_search",
    "investor_firm_roster",
}
ALLOWED_PROVIDER_PEOPLE_SEARCH_QUERY_STRATEGIES = {
    "all_queries_union",
    "first_hit",
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
    raw_target_company_linkedin_url = ""
    raw_target_company_linkedin_slug = ""
    for raw_key, raw_value in candidate.items():
        key = EXECUTION_PREFERENCE_ALIASES.get(str(raw_key or "").strip(), str(raw_key or "").strip())
        if key not in EXECUTION_PREFERENCE_FIELDS:
            continue
        if key in {"confirmed_company_scope", "extra_source_families", "delta_baseline_snapshot_ids"}:
            items = _normalize_string_list(raw_value, key=key, target_company=target_company)
            if items:
                normalized[key] = items
            continue
        if key in {
            "allow_local_bootstrap_fallback",
            "require_stage2_confirmation",
            "use_company_employees_lane",
            "keyword_priority_only",
            "former_keyword_queries_only",
            "large_org_keyword_probe_mode",
            "force_fresh_run",
            "reuse_existing_roster",
            "run_former_search_seed",
        }:
            value = _coerce_bool(raw_value)
            if value is not None:
                normalized[key] = value
            continue
        if key in {"reuse_snapshot_id", "reuse_baseline_job_id", "delta_baseline_snapshot_id"}:
            value = str(raw_value or "").strip()
            if value:
                normalized[key] = value[:120]
            continue
        if key == "target_company_linkedin_url":
            raw_target_company_linkedin_url = str(raw_value or "").strip()
            continue
        if key == "target_company_linkedin_slug":
            raw_target_company_linkedin_slug = str(raw_value or "").strip()
            continue
        if key == "runtime_tuning_profile":
            value = normalize_runtime_tuning_profile(raw_value)
            if value:
                normalized[key] = value
            continue
        if key in RUNTIME_TUNING_NONNEGATIVE_INT_FIELDS:
            value = _coerce_nonnegative_int(raw_value, maximum=86_400)
            if value is not None:
                normalized[key] = value
            continue
        if key in RUNTIME_TUNING_POSITIVE_INT_FIELDS:
            value = _coerce_small_positive_int(raw_value, maximum=10_000)
            if value is not None:
                normalized[key] = value
            continue
        if key in RUNTIME_TUNING_NONNEGATIVE_FLOAT_FIELDS:
            value = _coerce_nonnegative_float(raw_value, maximum=86_400.0)
            if value is not None:
                normalized[key] = value
            continue
        if key == "provider_people_search_query_strategy":
            value = _normalize_provider_people_search_query_strategy(raw_value)
            if value:
                normalized[key] = value
            continue
        if key == "provider_people_search_max_queries":
            value = _coerce_small_positive_int(raw_value, maximum=32)
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
    target_company_linkedin_slug = normalize_target_company_linkedin_slug(
        raw_target_company_linkedin_slug or raw_target_company_linkedin_url
    )
    target_company_linkedin_url = normalize_target_company_linkedin_url(
        raw_target_company_linkedin_url or raw_target_company_linkedin_slug
    )
    if target_company_linkedin_slug:
        normalized["target_company_linkedin_slug"] = target_company_linkedin_slug
    if target_company_linkedin_url:
        normalized["target_company_linkedin_url"] = target_company_linkedin_url
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
    require_stage2_confirmation = _infer_require_stage2_confirmation(lower)
    if require_stage2_confirmation is not None:
        prefs["require_stage2_confirmation"] = require_stage2_confirmation
    precision_recall_bias = _infer_precision_recall_bias(lower)
    if precision_recall_bias:
        prefs["precision_recall_bias"] = precision_recall_bias
    extra_source_families = _infer_extra_source_families(lower)
    if extra_source_families:
        prefs["extra_source_families"] = extra_source_families
    confirmed_scope = _infer_confirmed_scope(normalized_text, lower, target_company=target_company)
    if confirmed_scope:
        prefs["confirmed_company_scope"] = confirmed_scope
    use_company_employees_lane = _infer_company_employees_lane(lower)
    if use_company_employees_lane is not None:
        prefs["use_company_employees_lane"] = use_company_employees_lane
    keyword_priority_only = _infer_keyword_priority_only(lower)
    if keyword_priority_only is not None:
        prefs["keyword_priority_only"] = keyword_priority_only
    former_keyword_queries_only = _infer_former_keyword_queries_only(lower)
    if former_keyword_queries_only is not None:
        prefs["former_keyword_queries_only"] = former_keyword_queries_only
    provider_people_search_query_strategy = _infer_provider_people_search_query_strategy(lower)
    if provider_people_search_query_strategy:
        prefs["provider_people_search_query_strategy"] = provider_people_search_query_strategy
    provider_people_search_max_queries = _infer_provider_people_search_max_queries(normalized_text)
    if provider_people_search_max_queries is not None:
        prefs["provider_people_search_max_queries"] = provider_people_search_max_queries
    large_org_keyword_probe_mode = _infer_large_org_keyword_probe_mode(lower)
    if large_org_keyword_probe_mode is not None:
        prefs["large_org_keyword_probe_mode"] = large_org_keyword_probe_mode
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


def normalize_target_company_linkedin_slug(value: Any) -> str:
    raw = " ".join(str(value or "").strip().split())
    if not raw:
        return ""
    if "linkedin.com" in raw.lower() or raw.startswith("/"):
        extracted = _extract_linkedin_company_slug(raw)
        return extracted.lower()
    normalized = raw.strip().strip("/")
    if normalized.lower().startswith("company/"):
        normalized = normalized.split("/", 1)[1]
    normalized = normalized.split("?", 1)[0].split("#", 1)[0].strip()
    if not normalized:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._-]+", normalized):
        return ""
    return normalized.lower()


def normalize_target_company_linkedin_url(value: Any) -> str:
    slug = normalize_target_company_linkedin_slug(value)
    if not slug:
        return ""
    return f"https://www.linkedin.com/company/{slug}/"


def extract_target_company_linkedin_override(
    payload: dict[str, Any] | None,
    *,
    target_company: str = "",
) -> dict[str, str]:
    preferences = normalize_execution_preferences(payload, target_company=target_company)
    linkedin_slug = str(preferences.get("target_company_linkedin_slug") or "").strip()
    linkedin_url = str(preferences.get("target_company_linkedin_url") or "").strip()
    if not linkedin_slug and not linkedin_url:
        return {}
    if not linkedin_slug:
        linkedin_slug = normalize_target_company_linkedin_slug(linkedin_url)
    if not linkedin_url:
        linkedin_url = normalize_target_company_linkedin_url(linkedin_slug)
    if not linkedin_slug:
        return {}
    return {
        "linkedin_slug": linkedin_slug,
        "linkedin_company_url": linkedin_url,
        "company_key_hint": normalize_company_key(target_company or linkedin_slug),
    }


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


def _normalize_provider_people_search_query_strategy(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "union": "all_queries_union",
        "all query union": "all_queries_union",
        "all queries union": "all_queries_union",
        "multi query union": "all_queries_union",
        "all_queries_union": "all_queries_union",
        "parallel union": "all_queries_union",
        "全部 query 并集": "all_queries_union",
        "多 query 并集": "all_queries_union",
        "并集": "all_queries_union",
        "first hit": "first_hit",
        "first-hit": "first_hit",
        "first_hit": "first_hit",
        "首个命中": "first_hit",
        "首个命中即停": "first_hit",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in ALLOWED_PROVIDER_PEOPLE_SEARCH_QUERY_STRATEGIES else ""


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


def _coerce_small_positive_int(value: Any, *, maximum: int) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return min(parsed, maximum)


def _coerce_nonnegative_int(value: Any, *, maximum: int) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return min(parsed, maximum)


def _coerce_nonnegative_float(value: Any, *, maximum: float) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return min(parsed, maximum)


def _extract_linkedin_company_slug(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    match = re.search(r"linkedin\.com/company/([A-Za-z0-9._-]+)", normalized, flags=re.IGNORECASE)
    if match is not None:
        return str(match.group(1) or "").strip()
    match = re.search(r"(?:^|/)company/([A-Za-z0-9._-]+)", normalized, flags=re.IGNORECASE)
    if match is not None:
        return str(match.group(1) or "").strip()
    return ""


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
    if any(
        token in lower
        for token in [
            "scoped roster",
            "scoped search roster",
            "search roster",
            "keyword-first",
            "keyword first",
            "search-first",
            "search first",
            "只用 search api",
            "只走 search api",
            "关键词优先",
            "关键词先行",
            "先走关键词",
            "限定范围",
            "限定 scope",
            "限定团队",
        ]
    ):
        return "scoped_search_roster"
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
        "former 也要",
        "current + former",
        "current and former",
        "在职和离职",
        "在职和已离职",
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


def _infer_require_stage2_confirmation(lower: str) -> bool | None:
    if any(
        token in lower
        for token in [
            "wait for approval",
            "wait for my approval",
            "wait for my review",
            "after preview wait",
            "preview first",
            "等我确认",
            "等我审批",
            "等我review",
            "先给我preview",
            "先给我预览",
            "先别继续",
        ]
    ):
        return True
    return None


def _infer_confirmed_scope(text: str, lower: str, *, target_company: str) -> list[str]:
    scope: list[str] = []
    if target_company and any(token in lower for token in ["整家公司", "全公司", "全量成员", "company-wide", "full company"]):
        scope.append(target_company)
    if target_company and any(token in lower for token in ["只看", "only", "scope", "范围", "限定"]):
        if target_company.lower() in lower and target_company not in scope:
            scope.append(target_company)
    match = re.search(r"(?:scope|范围)[:：]\s*([^\n,;]+)", text, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        for part in re.split(r"[/,，、]+", raw):
            item = part.strip()
            if item and item not in scope:
                scope.append(item)
    return scope


def _infer_extra_source_families(lower: str) -> list[str]:
    families: list[str] = []
    for alias, canonical in SOURCE_FAMILY_ALIASES.items():
        if alias in lower and canonical not in families:
            families.append(canonical)
    return families


def _infer_company_employees_lane(lower: str) -> bool | None:
    deny_patterns = [
        "不要 company-employees",
        "不走 company-employees",
        "不用 company-employees",
        "不要 company employees",
        "不走 company employees",
        "不要 company employee api",
        "不要 roster api",
        "不调用 roster api",
        "只用 search api",
        "只走 search api",
    ]
    allow_patterns = [
        "company-employees",
        "company employees",
        "harvest company-employees",
        "harvest company employees",
        "company employee api",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


def _infer_keyword_priority_only(lower: str) -> bool | None:
    deny_patterns = [
        "不要 keyword-first",
        "不用 keyword-first",
        "不要关键词优先",
        "not keyword first",
    ]
    allow_patterns = [
        "keyword-first",
        "keyword first",
        "关键词优先",
        "关键词先行",
        "先用关键词",
        "先走关键词",
        "只走关键词",
        "只用 search api",
        "search-first",
        "search first",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


def _infer_former_keyword_queries_only(lower: str) -> bool | None:
    deny_patterns = [
        "former 不走关键词",
        "former 不用 search api",
    ]
    allow_patterns = [
        "former 只走关键词",
        "former 只用 search api",
        "former keyword only",
        "former keyword-first",
        "前员工只走关键词",
        "已离职只走关键词",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


def _infer_provider_people_search_query_strategy(lower: str) -> str:
    if any(
        token in lower
        for token in [
            "all queries union",
            "all query union",
            "multi query union",
            "parallel union",
            "全部 query 并集",
            "多 query 并集",
            "所有 query 都跑",
            "query 并集",
            "搜索并集",
        ]
    ):
        return "all_queries_union"
    if any(
        token in lower
        for token in [
            "first hit",
            "first-hit",
            "首个命中",
            "首个命中即停",
        ]
    ):
        return "first_hit"
    return ""


def _infer_provider_people_search_max_queries(text: str) -> int | None:
    match = re.search(r"(?:max|max(?:imum)?|最多)\D{0,8}(\d{1,2})\s*(?:个|条)?\s*(?:queries|query|搜索)?", text, flags=re.IGNORECASE)
    if not match:
        return None
    return _coerce_small_positive_int(match.group(1), maximum=32)


def _infer_large_org_keyword_probe_mode(lower: str) -> bool | None:
    deny_patterns = [
        "不要 large-org keyword probe",
        "关闭 keyword probe",
    ]
    allow_patterns = [
        "large-org keyword probe",
        "large org keyword probe",
        "keyword shard",
        "关键词分片",
        "关键词 probe",
    ]
    if any(token in lower for token in deny_patterns):
        return False
    if any(token in lower for token in allow_patterns):
        return True
    return None


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
