from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Callable

from .organization_execution_profile import FALLBACK_LARGE_COMPANY_KEYS
from .query_signal_knowledge import (
    ALPHABET_COMPANY_URL,
    GOOGLE_COMPANY_URL,
    related_company_scope_urls,
    scope_signal_search_query_aliases,
)

FULL_COMPANY_EMPLOYEE_RESULT_CAP = 2500
COMPANY_FILTER_LIST_KEYS = (
    "companies",
    "locations",
    "exclude_locations",
    "function_ids",
    "exclude_function_ids",
    "job_titles",
    "exclude_job_titles",
    "seniority_level_ids",
    "exclude_seniority_level_ids",
    "schools",
)

LARGE_ORG_SCOPE_COMPANY_URLS: dict[str, str] = {
    "google": GOOGLE_COMPANY_URL,
    "alphabet": ALPHABET_COMPANY_URL,
}

KEYWORD_PROBE_QUERY_ALIASES: dict[str, list[str]] = {
    "multimodal": ["Multimodal", "Multimodality"],
    "multimodality": ["Multimodal", "Multimodality"],
    "visionlanguage": ["Vision-language"],
    "videogeneration": ["Video generation"],
}
KEYWORD_PROBE_SKIP_TOKENS = {
    "research",
    "researcher",
    "engineering",
    "engineer",
}

SEARCH_QUERY_CANONICAL_ALIASES: dict[str, str] = {
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "multimodality": "Multimodal",
    "video-generation": "Video generation",
    "nano-banana": "Nano Banana",
}

LARGE_ORG_PRIORITY_FUNCTION_IDS = ["8", "9", "19", "24"]
LARGE_ORG_TECHNICAL_FUNCTION_IDS = ["8", "24"]


def _generic_large_org_technical_partition_policy(*, scope_note: str = "") -> dict[str, Any]:
    return {
        "strategy_id": "adaptive_us_technical_partition",
        "scope_note": scope_note
        or (
            "Probe-driven United States technical roster partition. Start from engineering+research, "
            "split engineering and research explicitly, and keep capped shard metadata when a live shard still exceeds the provider cap."
        ),
        "root_title": "United States",
        "root_filters": {
            "locations": ["United States"],
            "function_ids": list(LARGE_ORG_TECHNICAL_FUNCTION_IDS),
        },
        "allow_overflow_partial": True,
        "partition_rules": [
            {
                "rule_id": "engineering",
                "title": "Engineering",
                "include_patch": {"exclude_function_ids": ["24"]},
                "remainder_exclude_patch": {"exclude_function_ids": ["8"]},
            },
            {
                "rule_id": "research",
                "title": "Research",
                "include_patch": {"exclude_function_ids": ["8"]},
                "remainder_exclude_patch": {"exclude_function_ids": ["24"]},
            },
        ],
    }

ADAPTIVE_COMPANY_EMPLOYEE_SHARD_POLICIES: dict[str, dict[str, Any]] = {}


def _should_use_generic_large_org_partition(
    company_key: str,
    organization_execution_profile: dict[str, Any] | None = None,
) -> bool:
    normalized_company_key = str(company_key or "").strip().lower()
    normalized_profile = dict(organization_execution_profile or {})
    scale_band = str(normalized_profile.get("org_scale_band") or "").strip().lower()
    default_mode = str(normalized_profile.get("default_acquisition_mode") or "").strip().lower()
    if scale_band == "large":
        return True
    if default_mode == "scoped_search_roster":
        return True
    return normalized_company_key in FALLBACK_LARGE_COMPANY_KEYS


def build_default_company_employee_shard_policy(
    company_key: str,
    *,
    max_pages: int,
    page_limit: int,
    organization_execution_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_company_key = str(company_key or "").strip().lower()
    base = deepcopy(ADAPTIVE_COMPANY_EMPLOYEE_SHARD_POLICIES.get(normalized_company_key) or {})
    if not base and _should_use_generic_large_org_partition(
        normalized_company_key,
        organization_execution_profile=organization_execution_profile,
    ):
        base = _generic_large_org_technical_partition_policy()
    if not base:
        return {}
    base["max_pages"] = max(1, int(max_pages or 1))
    base["page_limit"] = max(1, int(page_limit or 25))
    base["provider_result_cap"] = FULL_COMPANY_EMPLOYEE_RESULT_CAP
    base["probe_max_pages"] = 1
    base["probe_page_limit"] = 25
    return normalize_company_employee_shard_policy(base)


def build_large_org_keyword_probe_shard_policy(
    company_key: str,
    *,
    company_scope: list[str],
    keyword_hints: list[str],
    function_ids: list[str] | None = None,
    max_pages: int,
    page_limit: int,
) -> dict[str, Any]:
    normalized_company_key = str(company_key or "").strip().lower()
    scope_companies = _resolve_large_org_scope_companies(normalized_company_key, company_scope)
    if not scope_companies:
        return {}
    keyword_shards = _build_keyword_probe_shards(keyword_hints)
    if not keyword_shards:
        return {}

    return normalize_company_employee_shard_policy(
        {
            "strategy_id": "adaptive_large_org_keyword_probe",
            "mode": "keyword_union",
            "force_keyword_shards": True,
            "allow_overflow_partial": True,
            "scope_note": (
                "Related-scope keyword probe mode. First probe the broad US scope, then run keyword shards "
                "and union+dedupe downstream if the root scope exceeds the provider cap."
            ),
            "root_title": "United States",
            "root_filters": {
                "locations": ["United States"],
                "companies": scope_companies,
                "function_ids": list(function_ids or LARGE_ORG_PRIORITY_FUNCTION_IDS),
            },
            "keyword_shards": keyword_shards,
            "max_pages": max(1, int(max_pages or 1)),
            "page_limit": max(1, int(page_limit or 25)),
            "probe_max_pages": 1,
            "probe_page_limit": 25,
            "provider_result_cap": FULL_COMPANY_EMPLOYEE_RESULT_CAP,
        }
    )


def normalize_company_employee_shard_policy(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    root_filters = normalize_company_filters(value.get("root_filters"))
    partition_rules = [_normalize_partition_rule(item) for item in list(value.get("partition_rules") or [])]
    partition_rules = [item for item in partition_rules if item]
    keyword_shards = [_normalize_keyword_shard(item) for item in list(value.get("keyword_shards") or [])]
    keyword_shards = [item for item in keyword_shards if item]
    mode = str(value.get("mode") or "").strip().lower()
    if mode not in {"partition_mece", "keyword_union"}:
        mode = "partition_mece"
    if not root_filters:
        return {}
    if mode == "partition_mece" and not partition_rules and keyword_shards:
        mode = "keyword_union"
    return {
        "strategy_id": str(value.get("strategy_id") or "").strip(),
        "mode": mode,
        "force_keyword_shards": bool(value.get("force_keyword_shards")),
        "allow_overflow_partial": bool(value.get("allow_overflow_partial")),
        "scope_note": str(value.get("scope_note") or "").strip(),
        "root_title": str(value.get("root_title") or "Root scope").strip() or "Root scope",
        "root_filters": root_filters,
        "partition_rules": partition_rules,
        "keyword_shards": keyword_shards,
        "max_pages": max(1, int(value.get("max_pages") or 1)),
        "page_limit": max(1, int(value.get("page_limit") or 25)),
        "probe_max_pages": max(1, int(value.get("probe_max_pages") or 1)),
        "probe_page_limit": max(1, int(value.get("probe_page_limit") or 25)),
        "provider_result_cap": max(1, int(value.get("provider_result_cap") or FULL_COMPANY_EMPLOYEE_RESULT_CAP)),
    }


def normalize_company_filters(value: Any) -> dict[str, Any]:
    payload = dict(value or {})
    normalized: dict[str, Any] = {}
    for key in COMPANY_FILTER_LIST_KEYS:
        raw_values = payload.get(key)
        if isinstance(raw_values, (list, tuple, set)):
            values = [str(item).strip() for item in raw_values if str(item).strip()]
        else:
            text = str(raw_values or "").strip()
            values = [text] if text else []
        if values:
            normalized[key] = list(dict.fromkeys(values))
    search_query = _canonicalize_search_query(str(payload.get("search_query") or payload.get("searchQuery") or "").strip())
    if search_query:
        normalized["search_query"] = search_query
    return normalized


def plan_company_employee_shards_from_policy(
    policy: dict[str, Any],
    *,
    probe_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    normalized_policy = normalize_company_employee_shard_policy(policy)
    if not normalized_policy:
        return {"status": "disabled", "reason": "no_policy", "shards": [], "probe_summaries": []}

    strategy_id = str(normalized_policy.get("strategy_id") or "").strip()
    scope_note = str(normalized_policy.get("scope_note") or "").strip()
    root_title = str(normalized_policy.get("root_title") or "Root scope").strip() or "Root scope"
    max_pages = int(normalized_policy.get("max_pages") or 1)
    page_limit = int(normalized_policy.get("page_limit") or 25)
    provider_cap = int(normalized_policy.get("provider_result_cap") or FULL_COMPANY_EMPLOYEE_RESULT_CAP)
    root_filters = normalize_company_filters(normalized_policy.get("root_filters"))
    mode = str(normalized_policy.get("mode") or "partition_mece").strip().lower()
    partition_rules = [dict(item) for item in list(normalized_policy.get("partition_rules") or []) if isinstance(item, dict)]
    keyword_shards = [dict(item) for item in list(normalized_policy.get("keyword_shards") or []) if isinstance(item, dict)]
    allow_overflow_partial = bool(normalized_policy.get("allow_overflow_partial"))

    if mode == "keyword_union":
        return _plan_keyword_union_shards(
            normalized_policy=normalized_policy,
            strategy_id=strategy_id,
            scope_note=scope_note,
            root_title=root_title,
            root_filters=root_filters,
            max_pages=max_pages,
            page_limit=page_limit,
            provider_cap=provider_cap,
            keyword_shards=keyword_shards,
            probe_fn=probe_fn,
        )

    probe_summaries: list[dict[str, Any]] = []
    root_probe = probe_fn(
        root_filters,
        {
            "probe_id": "root",
            "title": root_title,
            "scope_note": scope_note,
            "strategy_id": strategy_id,
            "max_pages": max_pages,
            "page_limit": page_limit,
        },
    )
    root_probe_summary = _normalize_probe_summary(root_probe, root_filters, probe_id="root", title=root_title)
    probe_summaries.append(root_probe_summary)
    root_count = int(root_probe_summary.get("estimated_total_count") or 0)
    if root_count <= 0:
        return {
            "status": "blocked",
            "reason": "root_probe_empty",
            "detail": "Adaptive shard probe returned no visible estimate for the root scope.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
        }
    if root_count <= provider_cap:
        return {
            "status": "planned",
            "reason": "root_scope_within_cap",
            "detail": "Root scope is already within the provider cap; no split required.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [
                _build_shard_record(
                    strategy_id=strategy_id,
                    shard_id=_normalize_shard_id(root_title),
                    title=root_title,
                    scope_note=scope_note,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=root_filters,
                    probe_summary=root_probe_summary,
                )
            ],
        }

    if not partition_rules:
        return {
            "status": "blocked",
            "reason": "root_scope_over_cap_without_partition_rules",
            "detail": "Adaptive shard probe found the root scope above the provider cap, but no partition rules were configured.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
            "overflow_scope": {
                "title": root_title,
                "company_filters": root_filters,
                "estimated_total_count": root_count,
            },
        }

    shards: list[dict[str, Any]] = []
    overflow_scopes: list[dict[str, Any]] = []
    remaining_filters = dict(root_filters)
    consumed_titles: list[str] = []
    for rule in partition_rules:
        branch_title = f"{root_title} / {str(rule.get('title') or rule.get('rule_id') or 'Shard').strip()}".strip()
        branch_filters = merge_company_filters(remaining_filters, rule.get("include_patch"))
        branch_probe = probe_fn(
            branch_filters,
            {
                "probe_id": str(rule.get("rule_id") or "").strip() or _normalize_shard_id(branch_title),
                "title": branch_title,
                "scope_note": scope_note,
                "strategy_id": strategy_id,
                "max_pages": max_pages,
                "page_limit": page_limit,
            },
        )
        branch_probe_summary = _normalize_probe_summary(
            branch_probe,
            branch_filters,
            probe_id=str(rule.get("rule_id") or "").strip() or _normalize_shard_id(branch_title),
            title=branch_title,
        )
        probe_summaries.append(branch_probe_summary)
        branch_count = int(branch_probe_summary.get("estimated_total_count") or 0)
        if branch_count > provider_cap:
            if allow_overflow_partial:
                capped_shard = _build_shard_record(
                    strategy_id=strategy_id,
                    shard_id=str(rule.get("rule_id") or _normalize_shard_id(branch_title)).strip() or _normalize_shard_id(branch_title),
                    title=branch_title,
                    scope_note=scope_note,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=branch_filters,
                    probe_summary=branch_probe_summary,
                )
                capped_shard["provider_cap_limited"] = True
                capped_shard["estimated_total_count_before_cap"] = branch_count
                shards.append(capped_shard)
                overflow_scopes.append(
                    {
                        "title": branch_title,
                        "company_filters": branch_filters,
                        "estimated_total_count": branch_count,
                        "partition_rule": dict(rule),
                    }
                )
            else:
                return {
                    "status": "blocked",
                    "reason": "partition_branch_over_cap",
                    "detail": (
                        f"Adaptive shard probe found '{branch_title}' still above the provider cap "
                        f"({branch_count} > {provider_cap})."
                    ),
                    "policy": normalized_policy,
                    "probe_summaries": probe_summaries,
                    "shards": shards,
                    "overflow_scope": {
                        "title": branch_title,
                        "company_filters": branch_filters,
                        "estimated_total_count": branch_count,
                        "partition_rule": dict(rule),
                    },
                }
        if branch_count > 0:
            consumed_titles.append(str(rule.get("title") or rule.get("rule_id") or "").strip())
            if not allow_overflow_partial or branch_count <= provider_cap:
                shards.append(
                    _build_shard_record(
                        strategy_id=strategy_id,
                        shard_id=str(rule.get("rule_id") or _normalize_shard_id(branch_title)).strip() or _normalize_shard_id(branch_title),
                        title=branch_title,
                        scope_note=scope_note,
                        max_pages=max_pages,
                        page_limit=page_limit,
                        company_filters=branch_filters,
                        probe_summary=branch_probe_summary,
                    )
                )

        remaining_filters = merge_company_filters(remaining_filters, rule.get("remainder_exclude_patch"))
        remaining_title = _remaining_shard_title(root_title, consumed_titles)
        remaining_probe = probe_fn(
            remaining_filters,
            {
                "probe_id": f"remaining_after_{str(rule.get('rule_id') or '').strip() or _normalize_shard_id(branch_title)}",
                "title": remaining_title,
                "scope_note": scope_note,
                "strategy_id": strategy_id,
                "max_pages": max_pages,
                "page_limit": page_limit,
            },
        )
        remaining_probe_summary = _normalize_probe_summary(
            remaining_probe,
            remaining_filters,
            probe_id=f"remaining_after_{str(rule.get('rule_id') or '').strip() or _normalize_shard_id(branch_title)}",
            title=remaining_title,
        )
        probe_summaries.append(remaining_probe_summary)
        remaining_count = int(remaining_probe_summary.get("estimated_total_count") or 0)
        if remaining_count <= 0:
            result = {
                "status": "planned",
                "reason": "partition_consumed_scope",
                "detail": "Adaptive partitioning fully consumed the target scope.",
                "policy": normalized_policy,
                "probe_summaries": probe_summaries,
                "shards": shards,
            }
            if overflow_scopes:
                result["reason"] = "partition_with_capped_shards"
                result["detail"] = (
                    "Some partition shards exceed the provider cap; those shards will run up to the provider cap "
                    "and keep overflow metadata for follow-up refinement."
                )
                result["overflow_scope"] = overflow_scopes[0]
                result["overflow_scopes"] = overflow_scopes
            return result
        if remaining_count <= provider_cap:
            shards.append(
                _build_shard_record(
                    strategy_id=strategy_id,
                    shard_id=_normalize_shard_id(remaining_title),
                    title=remaining_title,
                    scope_note=scope_note,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=remaining_filters,
                    probe_summary=remaining_probe_summary,
                )
            )
            result = {
                "status": "planned",
                "reason": "partitioned_scope_within_cap",
                "detail": "Adaptive partitioning reduced the scope below the provider cap.",
                "policy": normalized_policy,
                "probe_summaries": probe_summaries,
                "shards": shards,
            }
            if overflow_scopes:
                result["reason"] = "partition_with_capped_shards"
                result["detail"] = (
                    "Some partition shards exceed the provider cap; those shards will run up to the provider cap "
                    "and keep overflow metadata for follow-up refinement."
                )
                result["overflow_scope"] = overflow_scopes[0]
                result["overflow_scopes"] = overflow_scopes
            return result

    remaining_overflow_scope = {
        "title": _remaining_shard_title(root_title, consumed_titles),
        "company_filters": remaining_filters,
        "estimated_total_count": int(probe_summaries[-1].get("estimated_total_count") or 0),
    }
    if allow_overflow_partial and remaining_overflow_scope["estimated_total_count"] > 0:
        capped_remaining = _build_shard_record(
            strategy_id=strategy_id,
            shard_id=_normalize_shard_id(str(remaining_overflow_scope.get("title") or root_title)),
            title=str(remaining_overflow_scope.get("title") or root_title),
            scope_note=scope_note,
            max_pages=max_pages,
            page_limit=page_limit,
            company_filters=remaining_filters,
            probe_summary=probe_summaries[-1],
        )
        capped_remaining["provider_cap_limited"] = True
        capped_remaining["estimated_total_count_before_cap"] = int(remaining_overflow_scope["estimated_total_count"] or 0)
        shards.append(capped_remaining)
        overflow_scopes.append(remaining_overflow_scope)
        return {
            "status": "planned",
            "reason": "partition_with_capped_shards",
            "detail": (
                "Some partition shards exceed the provider cap; those shards will run up to the provider cap "
                "and keep overflow metadata for follow-up refinement."
            ),
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": shards,
            "overflow_scope": overflow_scopes[0],
            "overflow_scopes": overflow_scopes,
        }

    return {
        "status": "blocked",
        "reason": "remaining_scope_over_cap",
        "detail": "Adaptive partitioning exhausted the configured rule set, but the remaining scope is still above the provider cap.",
        "policy": normalized_policy,
        "probe_summaries": probe_summaries,
        "shards": shards,
        "overflow_scope": remaining_overflow_scope,
    }


def _plan_keyword_union_shards(
    *,
    normalized_policy: dict[str, Any],
    strategy_id: str,
    scope_note: str,
    root_title: str,
    root_filters: dict[str, Any],
    max_pages: int,
    page_limit: int,
    provider_cap: int,
    keyword_shards: list[dict[str, Any]],
    probe_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    probe_summaries: list[dict[str, Any]] = []
    root_probe = probe_fn(
        root_filters,
        {
            "probe_id": "root",
            "title": root_title,
            "scope_note": scope_note,
            "strategy_id": strategy_id,
            "max_pages": max_pages,
            "page_limit": page_limit,
            "mode": "keyword_union",
        },
    )
    root_probe_summary = _normalize_probe_summary(root_probe, root_filters, probe_id="root", title=root_title)
    probe_summaries.append(root_probe_summary)
    root_count = int(root_probe_summary.get("estimated_total_count") or 0)
    force_keyword_shards = bool(normalized_policy.get("force_keyword_shards"))
    allow_overflow_partial = bool(normalized_policy.get("allow_overflow_partial"))
    if root_count <= 0:
        return {
            "status": "blocked",
            "reason": "root_probe_empty",
            "detail": "Adaptive shard probe returned no visible estimate for the root scope.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
        }
    if root_count <= provider_cap and not force_keyword_shards:
        return {
            "status": "planned",
            "reason": "root_scope_within_cap",
            "detail": "Root scope is already within the provider cap; no split required.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [
                _build_shard_record(
                    strategy_id=strategy_id,
                    shard_id=_normalize_shard_id(root_title),
                    title=root_title,
                    scope_note=scope_note,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=root_filters,
                    probe_summary=root_probe_summary,
                )
            ],
        }
    if not keyword_shards:
        return {
            "status": "blocked",
            "reason": "root_scope_over_cap_without_keyword_shards",
            "detail": "Large-org keyword probe mode needs keyword shards when the root scope exceeds the provider cap.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
        }

    shards: list[dict[str, Any]] = []
    overflow_scopes: list[dict[str, Any]] = []
    for keyword_shard in keyword_shards:
        shard_id = str(keyword_shard.get("rule_id") or "").strip() or _normalize_shard_id(
            str(keyword_shard.get("title") or "keyword_shard")
        )
        shard_title = f"{root_title} / {str(keyword_shard.get('title') or shard_id).strip()}"
        shard_filters = merge_company_filters(root_filters, keyword_shard.get("include_patch"))
        shard_probe = probe_fn(
            shard_filters,
            {
                "probe_id": shard_id,
                "title": shard_title,
                "scope_note": scope_note,
                "strategy_id": strategy_id,
                "max_pages": max_pages,
                "page_limit": page_limit,
                "mode": "keyword_union",
            },
        )
        shard_probe_summary = _normalize_probe_summary(
            shard_probe,
            shard_filters,
            probe_id=shard_id,
            title=shard_title,
        )
        probe_summaries.append(shard_probe_summary)
        shard_count = int(shard_probe_summary.get("estimated_total_count") or 0)
        if shard_count <= 0:
            continue
        if shard_count > provider_cap:
            if allow_overflow_partial:
                capped_shard = _build_shard_record(
                    strategy_id=strategy_id,
                    shard_id=shard_id,
                    title=shard_title,
                    scope_note=scope_note,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=shard_filters,
                    probe_summary=shard_probe_summary,
                )
                capped_shard["provider_cap_limited"] = True
                capped_shard["estimated_total_count_before_cap"] = shard_count
                shards.append(capped_shard)
            overflow_scopes.append(
                {
                    "title": shard_title,
                    "company_filters": shard_filters,
                    "estimated_total_count": shard_count,
                    "keyword_shard": keyword_shard,
                }
            )
            continue
        shards.append(
            _build_shard_record(
                strategy_id=strategy_id,
                shard_id=shard_id,
                title=shard_title,
                scope_note=scope_note,
                max_pages=max_pages,
                page_limit=page_limit,
                company_filters=shard_filters,
                probe_summary=shard_probe_summary,
            )
        )

    if overflow_scopes:
        if allow_overflow_partial and shards:
            return {
                "status": "planned",
                "reason": "keyword_union_with_capped_shards",
                "detail": (
                    "Some keyword shards exceed the provider cap; those shards will run up to the provider cap "
                    "and keep overflow metadata for follow-up refinement."
                ),
                "policy": normalized_policy,
                "probe_summaries": probe_summaries,
                "shards": shards,
                "overflow_scope": overflow_scopes[0],
                "overflow_scopes": overflow_scopes,
                "union_dedupe_required": True,
            }
        return {
            "status": "blocked",
            "reason": "keyword_shard_over_cap",
            "detail": "At least one keyword shard is still above the provider cap; refine the keyword partition before live execution.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": shards,
            "overflow_scope": overflow_scopes[0],
            "overflow_scopes": overflow_scopes,
        }
    if not shards:
        return {
            "status": "blocked",
            "reason": "keyword_shards_empty",
            "detail": "Keyword shard probes returned no results under the current root scope.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
        }
    return {
        "status": "planned",
        "reason": "keyword_union_partition",
        "detail": "Root scope exceeded cap; plan switched to keyword shards and expects downstream union+dedupe.",
        "policy": normalized_policy,
        "probe_summaries": probe_summaries,
        "shards": shards,
        "union_dedupe_required": True,
    }


def merge_company_filters(base: Any, patch: Any) -> dict[str, Any]:
    merged = normalize_company_filters(base)
    patch_filters = normalize_company_filters(patch)
    for key in COMPANY_FILTER_LIST_KEYS:
        values = list(merged.get(key) or [])
        for item in list(patch_filters.get(key) or []):
            if item not in values:
                values.append(item)
        if values:
            merged[key] = values
        elif key in merged:
            merged.pop(key, None)
    search_query = str(patch_filters.get("search_query") or "").strip()
    if search_query:
        merged["search_query"] = search_query
    return merged


def _normalize_partition_rule(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    include_patch = normalize_company_filters(value.get("include_patch"))
    remainder_exclude_patch = normalize_company_filters(value.get("remainder_exclude_patch"))
    if not include_patch and not remainder_exclude_patch:
        return {}
    return {
        "rule_id": str(value.get("rule_id") or "").strip(),
        "title": str(value.get("title") or value.get("rule_id") or "Shard").strip() or "Shard",
        "include_patch": include_patch,
        "remainder_exclude_patch": remainder_exclude_patch,
    }


def _normalize_keyword_shard(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    include_patch = normalize_company_filters(value.get("include_patch"))
    if not include_patch:
        return {}
    return {
        "rule_id": str(value.get("rule_id") or "").strip(),
        "title": str(value.get("title") or value.get("rule_id") or "Keyword").strip() or "Keyword",
        "include_patch": include_patch,
    }


def _resolve_large_org_scope_companies(company_key: str, company_scope: list[str]) -> list[str]:
    resolved: list[str] = []
    normalized_scope = [str(item or "").strip() for item in company_scope if str(item or "").strip()]
    for item in [company_key, *normalized_scope]:
        token = _normalize_keyword_token(item)
        if not token:
            continue
        company_url = LARGE_ORG_SCOPE_COMPANY_URLS.get(token)
        if company_url and company_url not in resolved:
            resolved.append(company_url)
    for item in related_company_scope_urls(company_key, normalized_scope):
        if item not in resolved:
            resolved.append(item)

    if company_key in {"google", "alphabet"}:
        if LARGE_ORG_SCOPE_COMPANY_URLS["google"] not in resolved:
            resolved.insert(0, LARGE_ORG_SCOPE_COMPANY_URLS["google"])
    return resolved


def _build_keyword_probe_shards(keyword_hints: list[str]) -> list[dict[str, Any]]:
    shards: list[dict[str, Any]] = []
    seen: set[str] = set()
    seen_queries: set[str] = set()
    for item in keyword_hints:
        token = _normalize_keyword_token(item)
        if not token:
            continue
        if token in KEYWORD_PROBE_SKIP_TOKENS:
            continue
        query_terms = scope_signal_search_query_aliases(str(item or "")) or KEYWORD_PROBE_QUERY_ALIASES.get(token) or [str(item).strip()]
        search_query = _canonicalize_search_query(" ".join(term for term in query_terms if str(term).strip()).strip())
        if not search_query:
            continue
        query_signature = _search_query_signature(search_query)
        if query_signature in seen_queries:
            continue
        seen_queries.add(query_signature)
        shard_id = f"kw_{_normalize_shard_id(token)}"
        if shard_id in seen:
            continue
        seen.add(shard_id)
        shards.append(
            {
                "rule_id": shard_id,
                "title": search_query,
                "include_patch": {"search_query": search_query},
            }
        )
    return shards


def _normalize_keyword_token(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _canonicalize_search_query(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    lower_key = normalized.lower()
    return SEARCH_QUERY_CANONICAL_ALIASES.get(lower_key, normalized)


def _search_query_signature(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _normalize_probe_summary(
    value: Any,
    company_filters: dict[str, Any],
    *,
    probe_id: str,
    title: str,
) -> dict[str, Any]:
    payload = dict(value or {})
    estimated_total_count = int(payload.get("estimated_total_count") or payload.get("observed_total_count") or 0)
    returned_item_count = int(payload.get("returned_item_count") or payload.get("sample_item_count") or 0)
    return {
        "probe_id": probe_id,
        "title": title,
        "status": str(payload.get("status") or "completed"),
        "company_filters": normalize_company_filters(company_filters),
        "estimated_total_count": estimated_total_count,
        "returned_item_count": returned_item_count,
        "provider_result_limited": bool(payload.get("provider_result_limited")),
        "run_id": str(payload.get("run_id") or "").strip(),
        "dataset_id": str(payload.get("dataset_id") or "").strip(),
        "detail": str(payload.get("detail") or "").strip(),
        "summary_path": str(payload.get("summary_path") or "").strip(),
        "log_path": str(payload.get("log_path") or "").strip(),
    }


def _build_shard_record(
    *,
    strategy_id: str,
    shard_id: str,
    title: str,
    scope_note: str,
    max_pages: int,
    page_limit: int,
    company_filters: dict[str, Any],
    probe_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "shard_id": _normalize_shard_id(shard_id),
        "title": title,
        "scope_note": scope_note,
        "max_pages": max_pages,
        "page_limit": page_limit,
        "company_filters": normalize_company_filters(company_filters),
        "estimated_total_count": int(probe_summary.get("estimated_total_count") or 0),
        "probe_summary": dict(probe_summary),
    }


def _remaining_shard_title(root_title: str, consumed_titles: list[str]) -> str:
    labels = [str(item or "").strip() for item in consumed_titles if str(item or "").strip()]
    if not labels:
        return root_title
    return f"{root_title} / Remaining after {' / '.join(labels)}"


def _normalize_shard_id(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return cleaned or "shard"
