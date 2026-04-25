from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib import error as urllib_error

from .workflow_smoke import HostedWorkflowSmokeClient

DEFAULT_EXPLAIN_CASES: list[dict[str, Any]] = [
    {
        "case": "physical_intelligence_full_roster",
        "payload": {
            "raw_user_request": "给我Physical Intelligence的所有成员",
            "top_k": 10,
        },
        "expect": {
            "target_company": "Physical Intelligence",
            "org_scale_band": "unknown",
            "default_acquisition_mode": "full_company_roster",
            "dispatch_strategy": "new_job",
            "current_lane": "live_acquisition",
            "former_lane": "live_acquisition",
        },
    },
    {
        "case": "reflection_posttrain_reuse",
        "payload": {
            "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
            "top_k": 10,
        },
        "expect": {
            "target_company": "Reflection AI",
            "keywords": ["Post-train"],
            "org_scale_band": "small",
            "default_acquisition_mode": "full_company_roster",
            "dispatch_strategy": "reuse_snapshot",
            "planner_mode": "reuse_snapshot_only",
            "requires_delta_acquisition": False,
            "current_lane": "reuse_baseline",
            "former_lane": "reuse_baseline",
        },
    },
    {
        "case": "humansand_coding_reuse",
        "payload": {
            "raw_user_request": "我想了解Humans&里偏Coding agents方向的研究成员",
            "top_k": 10,
        },
        "expect": {
            "target_company": "Humans&",
            "org_scale_band": "small",
            "default_acquisition_mode": "full_company_roster",
            "dispatch_strategy": "reuse_snapshot",
            "planner_mode": "reuse_snapshot_only",
            "requires_delta_acquisition": False,
            "current_lane": "reuse_baseline",
            "former_lane": "reuse_baseline",
        },
    },
    {
        "case": "anthropic_pretraining_reuse",
        "payload": {
            "raw_user_request": "帮我找Anthropic里做Pre-training方向的人",
            "top_k": 10,
        },
        "expect": {
            "target_company": "Anthropic",
            "org_scale_band": "medium",
            "default_acquisition_mode": "hybrid",
            "dispatch_strategy": "reuse_snapshot",
            "planner_mode": "reuse_snapshot_only",
            "requires_delta_acquisition": False,
            "effective_acquisition_mode": "full_local_asset_reuse",
            "default_results_mode": "asset_population",
            "current_lane": "reuse_baseline",
            "former_lane": "reuse_baseline",
        },
    },
    {
        "case": "xai_full_roster_live",
        "payload": {
            "raw_user_request": "给我 xAI 的所有成员",
            "top_k": 10,
        },
        "expect": {
            "target_company": "xAI",
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "plan_primary_strategy_type": "full_company_roster",
            "plan_company_employee_shard_strategy": "adaptive_us_technical_partition",
            "plan_company_employee_shard_policy_allow_overflow_partial": True,
            "dispatch_strategy": "new_job",
            "current_lane": "live_acquisition",
            "former_lane": "live_acquisition",
        },
    },
    {
        "case": "xai_coding_all_members_scoped",
        "payload": {
            "raw_user_request": "我要 xAI 做 Coding 方向的全部成员",
            "top_k": 10,
        },
        "expect": {
            "target_company": "xAI",
            "keywords": ["Coding"],
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "plan_primary_strategy_type": "scoped_search_roster",
            "dispatch_strategy": "new_job",
            "current_lane": "live_acquisition",
            "former_lane": "live_acquisition",
        },
    },
    {
        "case": "openai_reasoning_reuse",
        "payload": {
            "raw_user_request": "我想要OpenAI做Reasoning方向的人",
            "top_k": 10,
        },
        "expect": {
            "target_company": "OpenAI",
            "keywords": ["Reasoning"],
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "dispatch_strategy": "reuse_snapshot",
            "planner_mode": "reuse_snapshot_only",
            "requires_delta_acquisition": False,
            "current_lane": "reuse_baseline",
            "former_lane": "reuse_baseline",
        },
    },
    {
        "case": "openai_pretrain_delta",
        "payload": {
            "raw_user_request": "我想要OpenAI做Pre-train方向的人",
            "top_k": 10,
        },
        "expect": {
            "target_company": "OpenAI",
            "keywords": ["Pre-train"],
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "dispatch_strategy": "delta_from_snapshot",
            "planner_mode": "delta_from_snapshot",
            "requires_delta_acquisition": True,
            "current_lane": "delta_acquisition",
            "former_lane": "delta_acquisition",
            "missing_current_profile_search_queries_contains": ["Pre-train"],
            "missing_former_profile_search_queries_contains": ["Pre-train"],
        },
    },
    {
        "case": "google_multimodal_pretrain_delta",
        "payload": {
            "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
            "top_k": 10,
        },
        "expect": {
            "target_company": "Google",
            "keywords_contains": ["Multimodal", "Pre-train", "Veo", "Nano Banana"],
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "dispatch_strategy": "delta_from_snapshot",
            "planner_mode": "delta_from_snapshot",
            "requires_delta_acquisition": True,
            "current_lane": "reuse_baseline",
            "former_lane": "delta_acquisition",
            "covered_current_profile_search_queries_contains": ["Multimodal"],
            "covered_former_profile_search_queries_contains": ["Multimodal"],
            "missing_former_profile_search_queries_contains": ["Pre-train"],
        },
    },
    {
        "case": "nvidia_world_model_new_job",
        "payload": {
            "raw_user_request": "帮我找NVIDIA做世界模型方向的人",
            "top_k": 10,
        },
        "expect": {
            "target_company": "NVIDIA",
            "keywords": ["World model"],
            "org_scale_band": "large",
            "default_acquisition_mode": "scoped_search_roster",
            "dispatch_strategy": "new_job",
            "current_lane": "live_acquisition",
            "former_lane": "live_acquisition",
        },
    },
]


def load_explain_cases(matrix_file: str = "", selected_cases: set[str] | None = None) -> list[dict[str, Any]]:
    selected = {str(item).strip() for item in list(selected_cases or set()) if str(item).strip()}
    cases = DEFAULT_EXPLAIN_CASES
    if matrix_file:
        payload = json.loads(Path(matrix_file).read_text(encoding="utf-8"))
        loaded_cases = payload.get("cases")
        if not isinstance(loaded_cases, list):
            raise ValueError("matrix file must contain a top-level `cases` list")
        cases = loaded_cases
    normalized: list[dict[str, Any]] = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        case_name = str(case.get("case") or "").strip()
        if not case_name:
            continue
        if selected and case_name not in selected:
            continue
        payload = case.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"case `{case_name}` is missing a dict payload")
        expect = case.get("expect")
        if expect is not None and not isinstance(expect, dict):
            raise ValueError(f"case `{case_name}` has non-dict expect payload")
        normalized.append({"case": case_name, "payload": payload, "expect": dict(expect or {})})
    if selected and not normalized:
        missing = ", ".join(sorted(selected))
        raise ValueError(f"requested cases not found in matrix: {missing}")
    return normalized


def _normalize_string_list(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def _extract_profile_search_queries(values: Any) -> list[str]:
    queries: list[str] = []
    for item in list(values or []):
        if isinstance(item, dict):
            text = str(item.get("search_query") or item.get("query") or "").strip()
        else:
            text = str(item or "").strip()
        if text:
            queries.append(text)
    return _normalize_string_list(queries)


def _extract_task_metadata(plan: dict[str, Any], task_type: str) -> dict[str, Any]:
    for task in list(plan.get("acquisition_tasks") or []):
        if not isinstance(task, dict):
            continue
        if str(task.get("task_type") or "").strip() != task_type:
            continue
        return dict(task.get("metadata") or {})
    return {}


def _extract_query_bundle_queries(values: Any) -> list[str]:
    queries: list[str] = []
    for bundle in list(values or []):
        if not isinstance(bundle, dict):
            continue
        for query in list(bundle.get("queries") or []):
            text = str(query or "").strip()
            if text:
                queries.append(text)
    return _normalize_string_list(queries)


def summarize_explain_payload(explain: dict[str, Any]) -> dict[str, Any]:
    request_preview = dict(explain.get("request_preview") or {})
    organization_execution_profile = dict(explain.get("organization_execution_profile") or {})
    asset_reuse_plan = dict(explain.get("asset_reuse_plan") or {})
    lane_preview = dict(explain.get("lane_preview") or {})
    dispatch_preview = dict(explain.get("dispatch_preview") or {})
    effective_execution_semantics = dict(explain.get("effective_execution_semantics") or {})
    plan = dict(explain.get("plan") or {})
    plan_acquisition_strategy = dict(plan.get("acquisition_strategy") or {})
    matched_job = dict(dispatch_preview.get("matched_job") or {})
    acquire_full_roster_metadata = _extract_task_metadata(plan, "acquire_full_roster")
    former_search_metadata = _extract_task_metadata(plan, "acquire_former_search_seed")
    plan_company_employee_shard_policy = dict(acquire_full_roster_metadata.get("company_employee_shard_policy") or {})
    current_filter_hints = dict(acquire_full_roster_metadata.get("filter_hints") or {})
    former_filter_hints = dict(former_search_metadata.get("filter_hints") or {})
    return {
        "status": str(explain.get("status") or ""),
        "reason": str(explain.get("reason") or ""),
        "target_company": str(request_preview.get("target_company") or ""),
        "keywords": _normalize_string_list(request_preview.get("keywords") or []),
        "org_scale_band": str(organization_execution_profile.get("org_scale_band") or ""),
        "default_acquisition_mode": str(organization_execution_profile.get("default_acquisition_mode") or ""),
        "effective_acquisition_mode": str(effective_execution_semantics.get("effective_acquisition_mode") or ""),
        "default_results_mode": str(effective_execution_semantics.get("default_results_mode") or ""),
        "asset_population_supported": bool(effective_execution_semantics.get("asset_population_supported")),
        "dispatch_strategy": str(dispatch_preview.get("strategy") or ""),
        "dispatch_matched_job_id": str(matched_job.get("job_id") or ""),
        "dispatch_matched_job_status": str(matched_job.get("status") or ""),
        "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
        "baseline_directional_local_reuse_eligible": bool(
            asset_reuse_plan.get("baseline_directional_local_reuse_eligible")
        ),
        "plan_primary_strategy_type": str(plan_acquisition_strategy.get("strategy_type") or ""),
        "plan_company_employee_shard_strategy": str(
            acquire_full_roster_metadata.get("company_employee_shard_strategy") or ""
        ),
        "plan_company_employee_shard_policy_allow_overflow_partial": bool(
            plan_company_employee_shard_policy.get("allow_overflow_partial")
        ),
        "plan_current_task_strategy_type": str(acquire_full_roster_metadata.get("strategy_type") or ""),
        "plan_current_search_seed_queries": _normalize_string_list(
            acquire_full_roster_metadata.get("search_seed_queries") or []
        ),
        "plan_current_filter_keywords": _normalize_string_list(current_filter_hints.get("keywords") or []),
        "plan_current_query_bundle_queries": _extract_query_bundle_queries(
            acquire_full_roster_metadata.get("search_query_bundles") or []
        ),
        "plan_former_task_strategy_type": str(former_search_metadata.get("strategy_type") or ""),
        "plan_former_search_seed_queries": _normalize_string_list(
            former_search_metadata.get("search_seed_queries") or []
        ),
        "plan_former_filter_keywords": _normalize_string_list(former_filter_hints.get("keywords") or []),
        "plan_former_query_bundle_queries": _extract_query_bundle_queries(
            former_search_metadata.get("search_query_bundles") or []
        ),
        "requires_delta_acquisition": bool(asset_reuse_plan.get("requires_delta_acquisition")),
        "current_lane": str(dict(lane_preview.get("current") or {}).get("planned_behavior") or ""),
        "former_lane": str(dict(lane_preview.get("former") or {}).get("planned_behavior") or ""),
        "missing_current_profile_search_queries": _normalize_string_list(
            asset_reuse_plan.get("missing_current_profile_search_queries") or []
        ),
        "missing_former_profile_search_queries": _normalize_string_list(
            asset_reuse_plan.get("missing_former_profile_search_queries") or []
        ),
        "covered_current_profile_search_queries": _extract_profile_search_queries(
            asset_reuse_plan.get("covered_current_profile_search_queries") or []
        ),
        "covered_former_profile_search_queries": _extract_profile_search_queries(
            asset_reuse_plan.get("covered_former_profile_search_queries") or []
        ),
        "timings_ms": dict(explain.get("timings_ms") or {}),
        "timing_breakdown_ms": dict(explain.get("timing_breakdown_ms") or {}),
    }


def evaluate_explain_summary(summary: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    mismatches: list[str] = []
    for key, expected in dict(expect or {}).items():
        if key.endswith("_contains"):
            actual_key = key[: -len("_contains")]
            actual_values = _normalize_string_list(summary.get(actual_key))
            expected_values = _normalize_string_list(expected)
            missing = [item for item in expected_values if item.lower() not in {value.lower() for value in actual_values}]
            if missing:
                mismatches.append(f"{key}: missing {missing}; actual={actual_values}")
            continue
        actual = summary.get(key)
        if isinstance(expected, list):
            actual_values = _normalize_string_list(actual)
            expected_values = _normalize_string_list(expected)
            if actual_values != expected_values:
                mismatches.append(f"{key}: expected {expected_values}; actual={actual_values}")
            continue
        if actual != expected:
            mismatches.append(f"{key}: expected {expected!r}; actual={actual!r}")
    return mismatches


def run_hosted_explain_case(
    client: HostedWorkflowSmokeClient,
    *,
    case_name: str,
    payload: dict[str, Any],
    expect: dict[str, Any] | None = None,
) -> dict[str, Any]:
    explain = client.post("/api/workflows/explain", dict(payload or {}))
    summary = summarize_explain_payload(explain)
    mismatches = evaluate_explain_summary(summary, dict(expect or {}))
    return {
        "case": case_name,
        "query": payload.get("raw_user_request"),
        "summary": summary,
        "mismatches": mismatches,
        "explain": explain,
    }


def run_hosted_explain_matrix(
    client: HostedWorkflowSmokeClient,
    *,
    cases: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for item in cases:
        case_name = str(item.get("case") or "").strip()
        payload = dict(item.get("payload") or {})
        expect = dict(item.get("expect") or {})
        if not case_name or not payload:
            continue
        try:
            record = run_hosted_explain_case(client, case_name=case_name, payload=payload, expect=expect)
        except urllib_error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            record = {
                "case": case_name,
                "query": payload.get("raw_user_request"),
                "error": f"HTTPError {exc.code}: {body}",
            }
        except Exception as exc:  # pragma: no cover - operational fallback
            record = {
                "case": case_name,
                "query": payload.get("raw_user_request"),
                "error": repr(exc),
            }
        summaries.append(record)
        if record.get("error") or list(record.get("mismatches") or []):
            failures.append(case_name)
    return summaries, failures
