from __future__ import annotations

from typing import Any

from .domain import JobRequest
from .retrieval_runtime import candidate_source_is_snapshot_authoritative


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def requested_dispatch_lane_requirements(request: JobRequest) -> dict[str, Any]:
    normalized_statuses = [
        str(item or "").strip().lower()
        for item in list(request.employment_statuses or [])
        if str(item or "").strip()
    ]
    status_set = set(normalized_statuses or ["current", "former"])
    need_current = "former" not in status_set or "current" in status_set
    need_former = "former" in status_set
    return {
        "employment_statuses": sorted(status_set),
        "need_current": bool(need_current),
        "need_former": bool(need_former),
    }


def compile_asset_reuse_lane_semantics(
    *,
    request: JobRequest,
    asset_reuse_plan: dict[str, Any],
    organization_execution_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset_reuse_plan = dict(asset_reuse_plan or {})
    organization_execution_profile = dict(organization_execution_profile or {})
    lane_requirements = requested_dispatch_lane_requirements(request)
    baseline_reuse_available = bool(asset_reuse_plan.get("baseline_reuse_available"))
    planner_mode = str(asset_reuse_plan.get("planner_mode") or "").strip()

    def _lane_payload(*, is_current: bool) -> dict[str, Any]:
        requested = bool(lane_requirements.get("need_current" if is_current else "need_former"))
        default_mode = str(
            organization_execution_profile.get("current_lane_default" if is_current else "former_lane_default") or ""
        ).strip()
        baseline_ready = bool(
            asset_reuse_plan.get(
                "baseline_current_effective_ready" if is_current else "baseline_former_effective_ready"
            )
        )
        baseline_effective_candidate_count = _safe_int(
            asset_reuse_plan.get(
                "baseline_current_effective_candidate_count"
                if is_current
                else "baseline_former_effective_candidate_count"
            )
        )
        missing_company_employee_shard_count = _safe_int(
            asset_reuse_plan.get(
                "missing_current_company_employee_shard_count"
                if is_current
                else "missing_former_company_employee_shard_count"
            )
        )
        missing_profile_search_query_count = _safe_int(
            asset_reuse_plan.get(
                "missing_current_profile_search_query_count"
                if is_current
                else "missing_former_profile_search_query_count"
            )
        )
        delta_required = bool(
            requested
            and (
                missing_company_employee_shard_count > 0
                or missing_profile_search_query_count > 0
            )
        )
        candidate_requirements_met = (not requested) or baseline_effective_candidate_count > 0
        requirements_ready = (not requested) or (baseline_ready and baseline_effective_candidate_count > 0)
        if not requested:
            planned_behavior = "not_requested"
        elif requirements_ready and not delta_required:
            planned_behavior = "reuse_baseline"
        elif baseline_reuse_available:
            planned_behavior = "delta_acquisition"
        else:
            planned_behavior = "live_acquisition"
        return {
            "requested": requested,
            "default_mode": default_mode,
            "baseline_ready": baseline_ready,
            "baseline_effective_candidate_count": baseline_effective_candidate_count,
            "missing_company_employee_shard_count": missing_company_employee_shard_count,
            "missing_profile_search_query_count": missing_profile_search_query_count,
            "delta_required": delta_required,
            "candidate_requirements_met": candidate_requirements_met,
            "requirements_ready": requirements_ready,
            "planned_behavior": planned_behavior,
        }

    current_lane = _lane_payload(is_current=True)
    former_lane = _lane_payload(is_current=False)
    return {
        "employment_statuses": list(lane_requirements.get("employment_statuses") or []),
        "need_current": bool(lane_requirements.get("need_current")),
        "need_former": bool(lane_requirements.get("need_former")),
        "baseline_reuse_available": baseline_reuse_available,
        "planner_mode": planner_mode,
        "requires_delta_acquisition": bool(current_lane["delta_required"] or former_lane["delta_required"]),
        "lane_candidate_requirements_met": bool(
            current_lane["candidate_requirements_met"] and former_lane["candidate_requirements_met"]
        ),
        "lane_requirements_ready": bool(current_lane["requirements_ready"] and former_lane["requirements_ready"]),
        "current_lane": current_lane,
        "former_lane": former_lane,
    }


def dispatch_requires_delta_for_request(
    *,
    request: JobRequest,
    asset_reuse_plan: dict[str, Any],
) -> bool:
    lane_semantics = compile_asset_reuse_lane_semantics(
        request=request,
        asset_reuse_plan=asset_reuse_plan,
    )
    return bool(lane_semantics.get("requires_delta_acquisition"))


def compile_execution_semantics(
    *,
    request: JobRequest,
    organization_execution_profile: dict[str, Any],
    asset_reuse_plan: dict[str, Any],
    candidate_source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    organization_execution_profile = dict(organization_execution_profile or {})
    asset_reuse_plan = dict(asset_reuse_plan or {})
    candidate_source = dict(candidate_source or {})
    lane_semantics = compile_asset_reuse_lane_semantics(
        request=request,
        asset_reuse_plan=asset_reuse_plan,
        organization_execution_profile=organization_execution_profile,
    )
    org_scale_band = str(organization_execution_profile.get("org_scale_band") or "").strip().lower()
    default_acquisition_mode = (
        str(organization_execution_profile.get("default_acquisition_mode") or "").strip().lower()
    )
    source_kind = str(candidate_source.get("source_kind") or "").strip().lower()
    baseline_reuse_available = bool(lane_semantics.get("baseline_reuse_available"))
    requires_delta_acquisition = bool(
        asset_reuse_plan.get("requires_delta_acquisition") or lane_semantics.get("requires_delta_acquisition")
    )
    baseline_candidate_count = int(asset_reuse_plan.get("baseline_candidate_count") or 0)
    current_lane_payload = dict(lane_semantics.get("current_lane") or {})
    former_lane_payload = dict(lane_semantics.get("former_lane") or {})
    current_lane_default = str(current_lane_payload.get("default_mode") or "").strip().lower()
    former_lane_default = str(former_lane_payload.get("default_mode") or "").strip().lower()
    target_scope = str(request.target_scope or "").strip().lower()
    company_snapshot_available = bool(
        str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(candidate_source)
    )
    current_delta_required = bool(current_lane_payload.get("delta_required"))
    former_delta_required = bool(former_lane_payload.get("delta_required"))
    full_local_asset_reuse = bool(
        target_scope == "full_company_asset"
        and baseline_reuse_available
        and not requires_delta_acquisition
        and company_snapshot_available
    )
    authoritative_population_default = bool(
        target_scope == "full_company_asset"
        and baseline_reuse_available
        and baseline_candidate_count >= 1000
        and current_lane_default == "reuse_baseline"
        and former_lane_default == "reuse_baseline"
    )

    effective_acquisition_mode = default_acquisition_mode or "live_acquisition"
    default_results_mode = "ranked_results"
    reason_codes: list[str] = []
    summary = ""
    execution_strategy_label = ""

    if full_local_asset_reuse:
        effective_acquisition_mode = "full_local_asset_reuse"
        default_results_mode = "asset_population"
        reason_codes.extend(
            [
                "baseline_reuse_available",
                "no_delta_required",
                "company_snapshot_candidate_source",
                "full_asset_results_view",
            ]
        )
        summary = "This run can directly reuse the company-level local baseline, so results default to full asset population."
        execution_strategy_label = "全量本地资产复用"
    elif authoritative_population_default:
        effective_acquisition_mode = (
            "baseline_reuse_with_delta" if requires_delta_acquisition else "full_local_asset_reuse"
        )
        default_results_mode = "asset_population"
        reason_codes.extend(
            [
                "baseline_reuse_available",
                "authoritative_population_default_view",
                "current_lane_reuse_baseline_default",
                "former_lane_reuse_baseline_default",
            ]
        )
        if requires_delta_acquisition:
            reason_codes.append("delta_required")
            summary = (
                "This run defaults to the authoritative company asset population while delta acquisition fills any "
                "keyword-specific gaps."
            )
            execution_strategy_label = "Baseline 复用 + 缺口增量"
        else:
            reason_codes.append("no_delta_required")
            summary = "This run can directly reuse the authoritative company baseline, so results default to full asset population."
            execution_strategy_label = "全量本地资产复用"
    elif baseline_reuse_available and requires_delta_acquisition:
        effective_acquisition_mode = "baseline_reuse_with_delta"
        reason_codes.extend(["baseline_reuse_available", "delta_required"])
        if company_snapshot_available:
            default_results_mode = "asset_population"
            reason_codes.append("company_snapshot_candidate_source")
            if target_scope == "full_company_asset":
                reason_codes.append("full_asset_results_view")
            else:
                reason_codes.append("snapshot_asset_population_default_view")
            summary = (
                "This run defaults to the current snapshot-backed asset population while delta acquisition fills any "
                "keyword-specific gaps."
            )
        else:
            summary = "This run reuses the existing baseline and only fills the uncovered delta lanes."
        execution_strategy_label = (
            "Scoped search + Baseline 复用增量"
            if default_acquisition_mode == "scoped_search_roster"
            else "Baseline 复用 + 缺口增量"
        )
    elif baseline_reuse_available:
        effective_acquisition_mode = "baseline_reuse_asset_population"
        reason_codes.append("baseline_reuse_available")
        summary = "This run primarily reuses the existing baseline."
        execution_strategy_label = "Baseline 复用"
    elif default_acquisition_mode == "scoped_search_roster":
        effective_acquisition_mode = "scoped_live_search"
        reason_codes.append("scoped_live_search_default")
        summary = "This run relies on scoped live search."
        execution_strategy_label = "定向搜索 roster"
    elif default_acquisition_mode == "full_company_roster":
        effective_acquisition_mode = "full_live_roster"
        reason_codes.append("full_live_roster_default")
        summary = "This run relies on a live full-company roster acquisition."
        execution_strategy_label = "全量 live roster"
    elif default_acquisition_mode == "hybrid":
        effective_acquisition_mode = "hybrid_live_acquisition"
        reason_codes.append("hybrid_live_default")
        summary = "This run uses hybrid baseline reuse plus incremental live acquisition."
        execution_strategy_label = "Baseline 复用 + 增量采集"
    elif company_snapshot_available and target_scope == "full_company_asset":
        default_results_mode = "asset_population"
        reason_codes.extend(["company_snapshot_candidate_source", "full_asset_results_view"])
        summary = "This run materialized a company snapshot, so results default to the full company asset population."
        execution_strategy_label = "全量公司资产"

    reason_codes = list(dict.fromkeys([code for code in reason_codes if str(code).strip()]))
    return {
        "target_company": str(request.target_company or "").strip(),
        "target_scope": str(request.target_scope or "").strip(),
        "org_scale_band": org_scale_band,
        "default_acquisition_mode": default_acquisition_mode,
        "effective_acquisition_mode": effective_acquisition_mode,
        "baseline_reuse_available": baseline_reuse_available,
        "baseline_candidate_count": baseline_candidate_count,
        "requires_delta_acquisition": requires_delta_acquisition,
        "candidate_source_kind": source_kind,
        "company_snapshot_available": company_snapshot_available,
        "current_lane_default": current_lane_default,
        "former_lane_default": former_lane_default,
        "current_lane_delta_required": current_delta_required,
        "former_lane_delta_required": former_delta_required,
        "lane_decisions": {
            "current": current_lane_payload,
            "former": former_lane_payload,
        },
        "full_local_asset_reuse": full_local_asset_reuse,
        "authoritative_population_default": authoritative_population_default,
        "default_results_mode": default_results_mode,
        "asset_population_supported": bool(
            full_local_asset_reuse
            or company_snapshot_available
            or authoritative_population_default
        ),
        "execution_strategy_label": execution_strategy_label,
        "summary": summary,
        "reason_codes": reason_codes,
    }
