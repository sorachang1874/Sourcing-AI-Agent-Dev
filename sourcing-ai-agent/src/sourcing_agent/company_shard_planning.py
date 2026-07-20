from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Callable, Iterable

from .cohort_selection import explicit_cohort_selection
from .organization_execution_profile import FALLBACK_LARGE_COMPANY_KEYS
from .query_signal_knowledge import (
    ALPHABET_COMPANY_URL,
    GOOGLE_COMPANY_URL,
    default_large_org_priority_function_ids,
    function_id_selectable_labels,
    related_company_scope_urls,
    role_bucket_function_ids,
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

# Registry-derived function-id sets (never hand-maintained duplicates of
# ROLE_BUCKET_KNOWLEDGE): engineering/founding/product_management/research and
# the technical (engineering+research) subset.
LARGE_ORG_PRIORITY_FUNCTION_IDS = default_large_org_priority_function_ids()
LARGE_ORG_TECHNICAL_FUNCTION_IDS = role_bucket_function_ids(("engineering", "research"))

# The company-employees roster lane's default location scope.  Every roster
# query plan (adaptive partition, keyword probe, or request-scoped function
# partition) starts from this default unless the request explicitly overrides
# or opts out of location filtering.
DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS = ["United States"]

# Strategy id stamped on request-scoped per-function roster shards so queue
# summaries, snapshot manifests, and delta-coverage rows can tell them apart
# from adaptive probe partitions.
REQUEST_FUNCTION_PARTITION_STRATEGY_ID = "request_function_partition"


def request_scoped_roster_function_ids(request_payload: dict[str, Any] | None) -> list[str]:
    """Explicitly selected provider function ids for the company-employees roster lane.

    ONLY a user-explicit cohort selection (the canonical role authority,
    ``cohort_selection.role_bucket_ids``) counts as a function selection; its
    buckets map through the canonical ``ROLE_BUCKET_KNOWLEDGE`` registry
    (research→"24", engineering→"8", product_management→"19", founding→"9").
    The flat ``must_have_primary_role_buckets`` compatibility mirror, soft or
    text-inferred role buckets, and planner-inferred roles are NOT paid
    function selections and never produce roster shards; unknown buckets map
    to no id (never invent new ids).
    """

    cohort = explicit_cohort_selection(request_payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return []
    return role_bucket_function_ids(list(cohort.get("role_bucket_ids") or []))


def _request_scoped_location_filters(
    target_locations: list[str] | None,
    exclude_target_locations: list[str] | None,
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Single-writer location semantics for the roster lane (absent/default/values/opt-out).

    ``target_locations`` ``None`` (field absent) defaults to the United States;
    a non-empty list passes through (multi-region allowed); an explicit empty
    list opts out of location filtering — request values win outright and are
    never merged with the default.  ``exclude_target_locations`` composes
    independently into ``exclude_locations`` (empty/absent ⇒ no exclusion).
    """

    if target_locations is None:
        locations = list(DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS)
    else:
        locations = list(
            dict.fromkeys(str(item).strip() for item in list(target_locations or []) if str(item).strip())
        )
    exclude_locations = list(
        dict.fromkeys(str(item).strip() for item in list(exclude_target_locations or []) if str(item).strip())
    )
    filters: dict[str, Any] = {}
    if locations:
        filters["locations"] = list(locations)
    if exclude_locations:
        filters["exclude_locations"] = list(exclude_locations)
    return locations, exclude_locations, filters


def build_request_scoped_company_employee_query_plan(
    *,
    target_locations: list[str] | None,
    function_ids: Iterable[str] | None,
    max_pages: int,
    page_limit: int,
    exclude_target_locations: list[str] | None = None,
) -> dict[str, Any]:
    """Unified request-scoped query plan for the Harvest company-employees roster lane.

    One parameter contract consumed by the roster lane for every company — no
    company-name branches:

    - ``target_locations`` / ``exclude_target_locations``: request-level
      location axes (see ``_request_scoped_location_filters`` for the
      single-writer semantics); both land on every shard and on the unsharded
      query's base filters (provider ``locations`` / ``excludeLocations``).
    - ``function_ids``: explicitly selected provider function ids (see
      ``request_scoped_roster_function_ids`` for who may select).  Each id
      gets its own company-employees shard with its own receipts/queue rows,
      because the provider caps one call at ~2500 items and per-function
      sharding is the coverage mechanism; the segmented roster merge
      union-dedupes overlapping members across shards while keeping every
      shard's provenance.  No function selection yields one unsharded query
      (current small-company behavior) carrying only the location filters.
    """

    locations, exclude_locations, base_filters = _request_scoped_location_filters(
        target_locations,
        exclude_target_locations,
    )
    normalized_function_ids = list(
        dict.fromkeys(str(item).strip() for item in list(function_ids or []) if str(item).strip())
    )
    location_title = ", ".join(locations) if locations else "All locations"
    scope_note = (
        "Request-scoped function partition. One company-employees shard per explicitly selected "
        "function id because the provider caps a single query at ~2500 items; each shard carries "
        "its own receipts and the segmented roster merge union-dedupes overlapping members while "
        "preserving every shard's function provenance."
    )
    labels = function_id_selectable_labels(normalized_function_ids)
    shards: list[dict[str, Any]] = []
    for function_id in normalized_function_ids:
        label = str(labels.get(function_id) or "").strip()
        title_suffix = label or f"Function {function_id}"
        shards.append(
            {
                "strategy_id": REQUEST_FUNCTION_PARTITION_STRATEGY_ID,
                "shard_id": f"function_{_normalize_shard_id(function_id)}",
                "title": f"{location_title} / {title_suffix}",
                "scope_note": scope_note,
                "max_pages": max(1, int(max_pages or 1)),
                "page_limit": max(1, int(page_limit or 25)),
                "company_filters": {**base_filters, "function_ids": [function_id]},
            }
        )
    return {
        "strategy_id": REQUEST_FUNCTION_PARTITION_STRATEGY_ID if shards else "",
        "locations": locations,
        "exclude_locations": exclude_locations,
        "function_ids": normalized_function_ids,
        "company_filters": base_filters,
        "shards": shards,
    }


# Stop reasons emitted by the connector for a truncated company-employees
# query (provider ~2500-item cap or the requested item limit).
TRUNCATED_ROSTER_STOP_REASONS = frozenset({"provider_cap_reached", "requested_limit_reached"})


def shard_summary_is_truncated(shard_summary: dict[str, Any]) -> bool:
    """True when one roster shard's evidence shows truncated (non-exhaustive) coverage."""

    summary = dict(shard_summary or {})
    if bool(summary.get("partial_result") or summary.get("provider_cap_hit") or summary.get("requested_limit_hit")):
        return True
    if bool(summary.get("provider_cap_limited") or summary.get("requested_limit_would_truncate")):
        return True
    return str(summary.get("stop_reason") or "").strip().lower() in TRUNCATED_ROSTER_STOP_REASONS


def resolve_segmented_roster_completion(
    *,
    expected_shard_ids: Iterable[str],
    shard_summaries: Iterable[dict[str, Any]],
    completed_stop_reason: str,
    partial_stop_reason: str,
) -> dict[str, Any]:
    """One honest completion contract for segmented company-employees rosters.

    Coverage is ``completed`` only when every expected shard is present AND no
    shard carries truncation evidence (provider cap / requested-limit hit); a
    missing or truncated shard keeps the roster ``partial`` so capped function
    coverage is never reported as overall completed.  Both the direct
    segmented fetch and background worker reconciliation route through this.
    """

    expected = {str(item).strip() for item in expected_shard_ids if str(item).strip()}
    summaries = [dict(item) for item in shard_summaries if isinstance(item, dict)]
    available = {str(item.get("shard_id") or "").strip() for item in summaries if str(item.get("shard_id") or "").strip()}
    truncated_shard_ids = sorted(
        str(item.get("shard_id") or "").strip()
        for item in summaries
        if str(item.get("shard_id") or "").strip() and shard_summary_is_truncated(item)
    )
    missing_shard_ids = sorted(expected - available)
    complete = not missing_shard_ids and not truncated_shard_ids
    return {
        "completion_status": "completed" if complete else "partial",
        "stop_reason": completed_stop_reason if complete else partial_stop_reason,
        "expected_shard_count": len(expected),
        "available_shard_count": len(available),
        "missing_shard_ids": missing_shard_ids,
        "truncated_shard_ids": truncated_shard_ids,
    }


def _generic_large_org_technical_partition_policy(
    *,
    scope_note: str = "",
    locations: list[str] | None = None,
    exclude_locations: list[str] | None = None,
) -> dict[str, Any]:
    effective_locations = (
        list(DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS) if locations is None else list(locations or [])
    )
    root_filters: dict[str, Any] = {}
    if effective_locations:
        root_filters["locations"] = list(effective_locations)
    if exclude_locations:
        root_filters["exclude_locations"] = list(exclude_locations)
    root_filters["function_ids"] = list(LARGE_ORG_TECHNICAL_FUNCTION_IDS)
    location_title = ", ".join(effective_locations) if effective_locations else "All locations"
    return {
        "strategy_id": "adaptive_us_technical_partition",
        "scope_note": scope_note
        or (
            f"Probe-driven {location_title} technical roster partition. Start from engineering+research, "
            "split engineering and research explicitly, and keep capped shard metadata when a live shard still exceeds the provider cap."
        ),
        "root_title": location_title,
        "root_filters": root_filters,
        "allow_overflow_partial": True,
        "partition_rules": [
            {
                "rule_id": "engineering",
                "title": "Engineering",
                "include_patch": {"function_ids": ["8"]},
                "remainder_exclude_patch": {"exclude_function_ids": ["8"]},
            },
            {
                "rule_id": "research",
                "title": "Research",
                "include_patch": {"function_ids": ["24"]},
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


def _compose_request_roster_policy_axes(
    base: dict[str, Any],
    *,
    locations: list[str] | None,
    exclude_locations: list[str] | None,
    request_function_ids: list[str] | None,
) -> dict[str, Any]:
    """Compose request-owned location/function axes into an adaptive roster policy.

    ``locations=None`` keeps the policy's own default; an explicit list
    (including ``[]``) replaces it so a requested region is never silently
    re-defaulted to the United States.  ``request_function_ids`` marks the
    explicit function selection the probe planner must expand into separate
    per-function shard roots BEFORE any keyword/partition subdivision.
    """

    root_filters = dict(base.get("root_filters") or {})
    if locations is not None:
        if locations:
            root_filters["locations"] = list(locations)
        else:
            root_filters.pop("locations", None)
        base["root_title"] = ", ".join(locations) if locations else "All locations"
    if exclude_locations:
        root_filters["exclude_locations"] = list(exclude_locations)
    base["root_filters"] = root_filters
    if request_function_ids:
        base["request_function_ids"] = list(request_function_ids)
    return base


def build_default_company_employee_shard_policy(
    company_key: str,
    *,
    max_pages: int,
    page_limit: int,
    organization_execution_profile: dict[str, Any] | None = None,
    locations: list[str] | None = None,
    exclude_locations: list[str] | None = None,
    request_function_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_company_key = str(company_key or "").strip().lower()
    base = deepcopy(ADAPTIVE_COMPANY_EMPLOYEE_SHARD_POLICIES.get(normalized_company_key) or {})
    if not base and _should_use_generic_large_org_partition(
        normalized_company_key,
        organization_execution_profile=organization_execution_profile,
    ):
        base = _generic_large_org_technical_partition_policy(
            locations=locations,
            exclude_locations=exclude_locations,
        )
    if not base:
        return {}
    base["max_pages"] = max(1, int(max_pages or 1))
    base["page_limit"] = max(1, int(page_limit or 25))
    base["provider_result_cap"] = FULL_COMPANY_EMPLOYEE_RESULT_CAP
    base["probe_max_pages"] = 1
    base["probe_page_limit"] = 25
    base = _compose_request_roster_policy_axes(
        base,
        locations=locations,
        exclude_locations=exclude_locations,
        request_function_ids=request_function_ids,
    )
    return normalize_company_employee_shard_policy(base)


def build_large_org_keyword_probe_shard_policy(
    company_key: str,
    *,
    company_scope: list[str],
    keyword_hints: list[str],
    function_ids: list[str] | None = None,
    max_pages: int,
    page_limit: int,
    locations: list[str] | None = None,
    exclude_locations: list[str] | None = None,
    request_function_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_company_key = str(company_key or "").strip().lower()
    scope_companies = _resolve_large_org_scope_companies(normalized_company_key, company_scope)
    if not scope_companies:
        return {}
    keyword_shards = _build_keyword_probe_shards(keyword_hints)
    if not keyword_shards:
        return {}

    effective_locations = (
        list(DEFAULT_COMPANY_EMPLOYEE_ROSTER_LOCATIONS) if locations is None else list(locations or [])
    )
    location_title = ", ".join(effective_locations) if effective_locations else "All locations"
    root_filters: dict[str, Any] = {"companies": scope_companies}
    if effective_locations:
        root_filters["locations"] = list(effective_locations)
    if exclude_locations:
        root_filters["exclude_locations"] = list(exclude_locations)
    root_filters["function_ids"] = list(function_ids or LARGE_ORG_PRIORITY_FUNCTION_IDS)

    return normalize_company_employee_shard_policy(
        {
            "strategy_id": "adaptive_large_org_keyword_probe",
            "mode": "keyword_union",
            "force_keyword_shards": True,
            "allow_overflow_partial": True,
            "scope_note": (
                f"Related-scope keyword probe mode. First probe the broad {location_title} scope, then run keyword shards "
                "and union+dedupe downstream if the root scope exceeds the provider cap."
            ),
            "root_title": location_title,
            "root_filters": root_filters,
            "keyword_shards": keyword_shards,
            "request_function_ids": list(request_function_ids or []),
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
    request_function_ids = [
        str(item).strip()
        for item in list(value.get("request_function_ids") or [])
        if str(item).strip()
    ]
    normalized = {
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
    if request_function_ids:
        normalized["request_function_ids"] = list(dict.fromkeys(request_function_ids))
    return normalized


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

    request_function_ids = [
        str(item).strip() for item in list(normalized_policy.get("request_function_ids") or []) if str(item).strip()
    ]
    if request_function_ids:
        # Explicit function selection owns the partition axis: expand into
        # separate per-function shard roots FIRST, then allow the configured
        # (keyword/capped) subdivision inside each function scope.
        return _plan_request_function_shards(
            normalized_policy=normalized_policy,
            request_function_ids=request_function_ids,
            probe_fn=probe_fn,
        )

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
        # Branch from the ROOT, not the evolving remainder: a function partition's
        # shard must emit its plain include filter (functionIds ["8"] / ["24"]),
        # never the root∖other mixed form. Remainder excludes accumulate only for
        # the trailing "rest" scope; any dual-classified member fetched in two
        # function shards is merged downstream by union-dedupe (d2d9fb6).
        branch_filters = merge_company_filters(root_filters, rule.get("include_patch"))
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


def _plan_single_scope_root(
    *,
    strategy_id: str,
    scope_note: str,
    root_title: str,
    root_filters: dict[str, Any],
    max_pages: int,
    page_limit: int,
    provider_cap: int,
    allow_overflow_partial: bool,
    probe_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    probe_id: str = "root",
) -> dict[str, Any]:
    """Probe one scope root and emit a single shard (or a capped-overflow shard).

    Used for request-function shard roots: cross-function partition rules do
    not apply inside one function's scope, so an over-cap root either stays a
    capped shard with explicit overflow metadata (``allow_overflow_partial``)
    or blocks the plan (fail closed).
    """

    root_probe = probe_fn(
        root_filters,
        {
            "probe_id": probe_id,
            "title": root_title,
            "scope_note": scope_note,
            "strategy_id": strategy_id,
            "max_pages": max_pages,
            "page_limit": page_limit,
        },
    )
    root_probe_summary = _normalize_probe_summary(root_probe, root_filters, probe_id=probe_id, title=root_title)
    probe_summaries = [root_probe_summary]
    root_count = int(root_probe_summary.get("estimated_total_count") or 0)
    if root_count <= 0:
        return {
            "status": "blocked",
            "reason": "root_probe_empty",
            "detail": f"Adaptive shard probe returned no visible estimate for scope '{root_title}'.",
            "probe_summaries": probe_summaries,
            "shards": [],
        }
    if root_count <= provider_cap:
        return {
            "status": "planned",
            "reason": "root_scope_within_cap",
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
    if allow_overflow_partial:
        capped_shard = _build_shard_record(
            strategy_id=strategy_id,
            shard_id=_normalize_shard_id(root_title),
            title=root_title,
            scope_note=scope_note,
            max_pages=max_pages,
            page_limit=page_limit,
            company_filters=root_filters,
            probe_summary=root_probe_summary,
        )
        capped_shard["provider_cap_limited"] = True
        capped_shard["estimated_total_count_before_cap"] = root_count
        return {
            "status": "planned",
            "reason": "root_scope_over_cap_capped",
            "probe_summaries": probe_summaries,
            "shards": [capped_shard],
            "overflow_scopes": [
                {
                    "title": root_title,
                    "company_filters": root_filters,
                    "estimated_total_count": root_count,
                }
            ],
        }
    return {
        "status": "blocked",
        "reason": "root_scope_over_cap_without_partition_rules",
        "detail": (
            f"Adaptive shard probe found '{root_title}' above the provider cap "
            f"({root_count} > {provider_cap}) with no applicable subdivision."
        ),
        "probe_summaries": probe_summaries,
        "shards": [],
        "overflow_scope": {
            "title": root_title,
            "company_filters": root_filters,
            "estimated_total_count": root_count,
        },
    }


def _plan_request_function_shards(
    *,
    normalized_policy: dict[str, Any],
    request_function_ids: list[str],
    probe_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Expand an explicit function selection into separate per-function shard roots.

    Each selected function id becomes its own probe root (never one combined
    multi-function query); keyword shards subdivide a function root only when
    that root itself exceeds the provider cap ("optional keyword subdivision"),
    and a still-over-cap root without subdivision stays a capped shard with
    overflow metadata when ``allow_overflow_partial`` is set.  A function scope
    that probes empty is skipped (recorded), and any other planning failure
    blocks the whole plan (fail closed).
    """

    strategy_id = REQUEST_FUNCTION_PARTITION_STRATEGY_ID
    base_root_filters = normalize_company_filters(normalized_policy.get("root_filters"))
    base_root_title = str(normalized_policy.get("root_title") or "Root scope").strip() or "Root scope"
    max_pages = int(normalized_policy.get("max_pages") or 1)
    page_limit = int(normalized_policy.get("page_limit") or 25)
    provider_cap = int(normalized_policy.get("provider_result_cap") or FULL_COMPANY_EMPLOYEE_RESULT_CAP)
    mode = str(normalized_policy.get("mode") or "partition_mece").strip().lower()
    keyword_shards = [dict(item) for item in list(normalized_policy.get("keyword_shards") or []) if isinstance(item, dict)]
    allow_overflow_partial = bool(normalized_policy.get("allow_overflow_partial"))
    scope_note = str(normalized_policy.get("scope_note") or "").strip()
    labels = function_id_selectable_labels(request_function_ids)

    all_shards: list[dict[str, Any]] = []
    probe_summaries: list[dict[str, Any]] = []
    overflow_scopes: list[dict[str, Any]] = []
    skipped_function_ids: list[str] = []
    union_dedupe_required = False
    for function_id in request_function_ids:
        label = str(labels.get(function_id) or "").strip() or f"Function {function_id}"
        root_title = f"{base_root_title} / {label}"
        root_filters = {**base_root_filters, "function_ids": [function_id]}
        if mode == "keyword_union" and keyword_shards:
            # Per function root, keyword subdivision is optional: it only runs
            # when that function scope itself exceeds the provider cap.
            function_policy = {**normalized_policy, "force_keyword_shards": False}
            sub_plan = _plan_keyword_union_shards(
                normalized_policy=function_policy,
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
        else:
            sub_plan = _plan_single_scope_root(
                strategy_id=strategy_id,
                scope_note=scope_note,
                root_title=root_title,
                root_filters=root_filters,
                max_pages=max_pages,
                page_limit=page_limit,
                provider_cap=provider_cap,
                allow_overflow_partial=allow_overflow_partial,
                probe_fn=probe_fn,
                probe_id=f"function_{_normalize_shard_id(function_id)}",
            )
        probe_summaries.extend(dict(item) for item in list(sub_plan.get("probe_summaries") or []))
        if str(sub_plan.get("status") or "") == "blocked" and str(sub_plan.get("reason") or "") == "root_probe_empty":
            skipped_function_ids.append(function_id)
            continue
        if str(sub_plan.get("status") or "") != "planned":
            return {
                "status": "blocked",
                "reason": "function_shard_planning_failed",
                "detail": (
                    f"Adaptive planning failed for explicitly selected function id {function_id} "
                    f"({label}): {str(sub_plan.get('detail') or sub_plan.get('reason') or 'unknown')}."
                ),
                "policy": normalized_policy,
                "probe_summaries": probe_summaries,
                "shards": all_shards,
                "failed_function_id": function_id,
            }
        for shard in list(sub_plan.get("shards") or []):
            normalized_shard = dict(shard)
            normalized_shard["shard_id"] = (
                f"function_{_normalize_shard_id(function_id)}__{str(normalized_shard.get('shard_id') or 'shard').strip()}"
            )
            all_shards.append(normalized_shard)
        overflow_scopes.extend(dict(item) for item in list(sub_plan.get("overflow_scopes") or []))
        union_dedupe_required = union_dedupe_required or bool(sub_plan.get("union_dedupe_required"))

    if not all_shards:
        return {
            "status": "blocked",
            "reason": "root_probe_empty",
            "detail": "Every explicitly selected function scope probed empty; no executable shard set.",
            "policy": normalized_policy,
            "probe_summaries": probe_summaries,
            "shards": [],
            "skipped_function_ids": skipped_function_ids,
        }
    result: dict[str, Any] = {
        "status": "planned",
        "reason": "request_function_partition",
        "detail": (
            f"Request-scoped function partition planned {len(all_shards)} shard(s) across "
            f"{len(request_function_ids)} explicitly selected function id(s)."
        ),
        "policy": normalized_policy,
        "probe_summaries": probe_summaries,
        "shards": all_shards,
    }
    if skipped_function_ids:
        result["skipped_function_ids"] = skipped_function_ids
    if union_dedupe_required:
        result["union_dedupe_required"] = True
    if overflow_scopes:
        result["reason"] = "request_function_partition_with_capped_shards"
        result["detail"] = (
            "Some request-function shards exceed the provider cap; those shards will run up to the provider cap "
            "and keep overflow metadata for follow-up refinement."
        )
        result["overflow_scope"] = overflow_scopes[0]
        result["overflow_scopes"] = overflow_scopes
    return result


def merge_company_filters(base: Any, patch: Any) -> dict[str, Any]:
    """Merge a partition patch into the base filters.

    Include-side keys (companies/locations/function_ids/job_titles/
    seniority_level_ids/schools) are PATCH-WINS: a partition rule that names
    a function id means THAT function, not the union with the root's broader
    set — root `function_ids: ["8", "24"]` + engineering include `["8"]` must
    emit `functionIds: ["8"]`, never the redundant/lossy
    `functionIds: ["8", "24"] + excludeFunctionIds: ["24"]` form.
    Exclude-side keys (exclude_locations/exclude_function_ids/
    exclude_job_titles/exclude_seniority_level_ids) UNION-ACCUMULATE so
    remainder shards keep every previously consumed partition's exclusion.
    """

    merged = normalize_company_filters(base)
    patch_filters = normalize_company_filters(patch)
    for key in COMPANY_FILTER_LIST_KEYS:
        patch_values = list(patch_filters.get(key) or [])
        if patch_values:
            if key.startswith("exclude_"):
                values = list(merged.get(key) or [])
                for item in patch_values:
                    if item not in values:
                        values.append(item)
                merged[key] = values
            else:
                merged[key] = patch_values
        elif key in merged and not key.startswith("exclude_"):
            # include-side key absent from the patch keeps the base value
            continue
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


def _simplify_function_partition_filters(filters: dict[str, Any]) -> dict[str, Any]:
    """Collapse `functionIds − excludeFunctionIds` to a plain single-function filter.

    Partitions express "engineering minus research" as
    `functionIds [8,24] + excludeFunctionIds [24]`; every provider call should
    instead carry the plain `functionIds [8]` (operator directive 2026-07-20).
    Only when the effective set is a NON-EMPTY strict subset do we rewrite;
    an empty effective set (the "neither" remainder) keeps its excludes.
    """

    normalized = normalize_company_filters(filters)
    function_ids = [str(item) for item in list(normalized.get("function_ids") or [])]
    exclude_ids = [str(item) for item in list(normalized.get("exclude_function_ids") or [])]
    if not function_ids or not exclude_ids:
        return normalized
    effective = [item for item in function_ids if item not in set(exclude_ids)]
    if not effective or set(effective) == set(function_ids):
        return normalized
    normalized["function_ids"] = effective
    remaining_excludes = [item for item in exclude_ids if item not in set(function_ids)]
    if remaining_excludes:
        normalized["exclude_function_ids"] = remaining_excludes
    else:
        normalized.pop("exclude_function_ids", None)
    return normalized


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
        "company_filters": _simplify_function_partition_filters(company_filters),
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
