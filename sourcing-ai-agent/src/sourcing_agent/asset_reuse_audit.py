from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_catalog import AssetCatalog
from .asset_coverage_contracts import build_population_coverage_contract
from .asset_reuse_planning import (
    apply_asset_reuse_plan_to_sourcing_plan,
    build_organization_asset_registry_candidate_inventory,
    compile_asset_reuse_plan,
)
from .domain import JobRequest
from .execution_semantics import compile_execution_semantics
from .model_provider import DeterministicModelClient
from .organization_assets import load_cached_organization_completeness_ledger
from .organization_execution_profile import build_organization_execution_profile
from .planning import build_sourcing_plan
from .request_normalization import build_effective_job_request
from .storage import ControlPlaneStore

_REUSABLE_SHARD_STATUSES = {"completed", "completed_with_cap", "skipped_high_overlap"}
DEFAULT_PARITY_COMPARE_FIELDS = [
    "requested_population_boundary",
    "strategy_type",
    "strategy_decision_source",
    "planner_mode",
    "requires_delta_acquisition",
    "baseline_full_company_coverage_proven",
    "full_company_filter_from_baseline",
    "full_local_asset_reuse",
    "execution_strategy_label",
    "missing_current_profile_search_query_count",
    "missing_former_profile_search_query_count",
    "current_profile_exact_overlap_gap_count",
    "former_profile_exact_overlap_gap_count",
    "selected_snapshot_missing_shard_registry_row_count",
    "baseline_generation_lags_same_snapshot_shard_materialization",
    "warning_codes",
]


def audit_authoritative_reuse_planning(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str,
    query: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    """Build an offline, read-only report for intent/coverage/planner parity.

    This intentionally avoids orchestrator plan/explain APIs because those paths can
    persist plan review history or warm organization profiles. The audit only reads
    registry rows, cached ledgers, and shard registry rows, then runs deterministic
    planning with missing-ledger rebuilds disabled.
    """

    normalized_company = _normalize_text(company)
    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    normalized_query = _normalize_text(query)
    if not normalized_company:
        raise ValueError("company is required")
    if not normalized_query:
        raise ValueError("query is required")

    request = JobRequest.from_payload(
        {
            "raw_user_request": normalized_query,
            "query": normalized_query,
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "target_scope": "full_company_asset",
        }
    )
    effective_request, intent_view = build_effective_job_request(request)
    request_boundary = dict(getattr(effective_request, "requested_population_boundary", {}) or {})

    inventory = build_organization_asset_registry_candidate_inventory(
        store=store,
        target_company=normalized_company,
        asset_view=normalized_asset_view,
        limit=5000,
    )
    authoritative_row = dict(inventory.get("authoritative_row") or {})
    selected_row = dict(inventory.get("selected_row") or authoritative_row or {})
    selected_snapshot_ids = _selected_snapshot_ids(selected_row)
    if not selected_snapshot_ids and selected_row.get("snapshot_id"):
        selected_snapshot_ids = [_normalize_text(selected_row.get("snapshot_id"))]

    all_selected_shard_rows = _list_shard_rows(
        store=store,
        target_company=normalized_company,
        snapshot_ids=selected_snapshot_ids,
        statuses=None,
    )
    reusable_selected_shard_rows = [
        row
        for row in all_selected_shard_rows
        if _normalize_text(row.get("status")).lower() in _REUSABLE_SHARD_STATUSES
    ]
    ledger_summary = _load_cached_ledger_summary(
        runtime_dir=runtime_dir,
        target_company=normalized_company,
        snapshot_id=_normalize_text(selected_row.get("snapshot_id")),
        asset_view=normalized_asset_view,
    )
    population_contract = build_population_coverage_contract(
        registry_row=selected_row,
        ledger_summary=ledger_summary,
        shard_rows=reusable_selected_shard_rows,
    )
    organization_profile = build_organization_execution_profile(
        target_company=normalized_company,
        asset_view=normalized_asset_view,
        registry_row=selected_row,
        ledger_summary=ledger_summary,
        shard_rows=reusable_selected_shard_rows,
    )

    catalog = AssetCatalog.discover()
    plan = build_sourcing_plan(
        effective_request,
        catalog,
        DeterministicModelClient(),
        organization_execution_profile=organization_profile,
    )
    asset_reuse_plan = compile_asset_reuse_plan(
        runtime_dir=runtime_dir,
        store=store,
        request=effective_request,
        plan=plan,
        allow_missing_ledger_rebuild=False,
    )
    apply_asset_reuse_plan_to_sourcing_plan(plan, asset_reuse_plan)
    candidate_source = _candidate_source_from_asset_reuse_plan(asset_reuse_plan, selected_row)
    execution_semantics = compile_execution_semantics(
        request=effective_request,
        organization_execution_profile=organization_profile,
        asset_reuse_plan=asset_reuse_plan,
        candidate_source=candidate_source,
    )
    selected_row_summary = dict(selected_row.get("summary") or {})
    selected_source_selection = dict(
        selected_row.get("source_snapshot_selection") or selected_row_summary.get("source_snapshot_selection") or {}
    )
    serving_snapshot_id = _normalize_text(
        selected_source_selection.get("serving_snapshot_id")
        or dict(selected_source_selection.get("serving_generation_repair") or {}).get("repair_snapshot_id")
        or selected_row.get("snapshot_id")
    )
    shard_summary = _summarize_shard_registry(
        rows=all_selected_shard_rows,
        selected_snapshot_ids=selected_snapshot_ids,
        serving_snapshot_id=serving_snapshot_id,
    )
    warnings = _compile_warnings(
        request_boundary=request_boundary,
        authoritative_row=authoritative_row,
        selected_row=selected_row,
        selected_snapshot_ids=selected_snapshot_ids,
        population_contract=population_contract,
        shard_summary=shard_summary,
        asset_reuse_plan=asset_reuse_plan,
        execution_semantics=execution_semantics,
        inventory=inventory,
    )
    plan_payload = plan.to_record()
    strategy_payload = dict(plan_payload.get("acquisition_strategy") or {})
    strategy_explanation = dict(strategy_payload.get("strategy_decision_explanation") or {})

    return {
        "status": "ok",
        "read_only": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_company": normalized_company,
        "asset_view": normalized_asset_view,
        "query": normalized_query,
        "request": {
            "raw_user_request": effective_request.raw_user_request,
            "target_scope": effective_request.target_scope,
            "categories": list(effective_request.categories or []),
            "employment_statuses": list(effective_request.employment_statuses or []),
            "keywords": list(effective_request.keywords or []),
            "must_have_facets": list(effective_request.must_have_facets or []),
            "must_have_primary_role_buckets": list(effective_request.must_have_primary_role_buckets or []),
            "requested_population_boundary": request_boundary,
            "intent_view": intent_view,
        },
        "registry": {
            "authoritative_pointer": _summarize_registry_row(authoritative_row),
            "selected_planning_row": _summarize_registry_row(selected_row),
            "candidate_inventory": {
                "row_count": len(list(inventory.get("rows") or [])),
                "candidate_row_count": len(list(inventory.get("candidate_rows") or [])),
                "selection_decision": dict(inventory.get("selection_decision") or {}),
                "ordered_snapshot_ids": [
                    _normalize_text(dict(row or {}).get("snapshot_id"))
                    for row in list(inventory.get("ordered_candidate_rows") or [])[:20]
                    if _normalize_text(dict(row or {}).get("snapshot_id"))
                ],
            },
        },
        "baseline_population_coverage_contract": population_contract,
        "cached_completeness_ledger": {
            "available": bool(ledger_summary),
            "snapshot_id": _normalize_text(selected_row.get("snapshot_id")),
            "read_only": True,
        },
        "scoped_shard_registry": shard_summary,
        "organization_execution_profile": organization_profile,
        "planner": {
            "acquisition_strategy": strategy_payload,
            "strategy_decision_explanation": strategy_explanation,
            "asset_reuse_plan": asset_reuse_plan,
            "candidate_source_for_semantics": candidate_source,
            "effective_execution_semantics": execution_semantics,
        },
        "warnings": warnings,
    }


def audit_authoritative_reuse_planning_many(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str,
    queries: list[str],
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    audits = [
        audit_authoritative_reuse_planning(
            runtime_dir=runtime_dir,
            store=store,
            company=company,
            query=query,
            asset_view=asset_view,
        )
        for query in list(queries or [])
        if _normalize_text(query)
    ]
    warning_codes = _dedupe_strings(
        code
        for audit in audits
        for code in list(dict(audit).get("warnings") or [])
        if _normalize_text(code)
    )
    return {
        "status": "ok" if audits else "empty",
        "read_only": True,
        "target_company": _normalize_text(company),
        "asset_view": _normalize_text(asset_view) or "canonical_merged",
        "query_count": len(audits),
        "warning_codes": warning_codes,
        "audits": audits,
    }


def audit_authoritative_reuse_planning_matrix(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    matrix: dict[str, Any],
    default_asset_view: str = "canonical_merged",
    include_full_audit: bool = True,
) -> dict[str, Any]:
    cases = [dict(item) for item in list(dict(matrix or {}).get("cases") or []) if isinstance(item, dict)]
    results: list[dict[str, Any]] = []
    for index, case in enumerate(cases, start=1):
        case_id = _normalize_text(case.get("case_id")) or f"case_{index:03d}"
        company = _normalize_text(case.get("company") or case.get("target_company"))
        query = _normalize_text(case.get("query"))
        asset_view = _normalize_text(case.get("asset_view") or default_asset_view) or "canonical_merged"
        if not company or not query:
            results.append(
                {
                    "case_id": case_id,
                    "status": "invalid_case",
                    "company": company,
                    "query": query,
                    "expectation_failures": ["missing_company_or_query"],
                }
            )
            continue
        audit = audit_authoritative_reuse_planning(
            runtime_dir=runtime_dir,
            store=store,
            company=company,
            query=query,
            asset_view=asset_view,
        )
        summary = summarize_authoritative_reuse_audit(audit, case_id=case_id)
        expectation_failures = _evaluate_case_expectations(summary, dict(case.get("expectations") or {}))
        result = {
            "case_id": case_id,
            "status": "failed_expectations" if expectation_failures else "ok",
            "company": company,
            "query": query,
            "asset_view": asset_view,
            "summary": summary,
            "expectations": dict(case.get("expectations") or {}),
            "expectation_failures": expectation_failures,
            "notes": _normalize_text(case.get("notes")),
        }
        if include_full_audit:
            result["audit"] = audit
        results.append(result)
    failures = [
        {"case_id": result.get("case_id"), "failures": list(result.get("expectation_failures") or [])}
        for result in results
        if list(result.get("expectation_failures") or [])
    ]
    return {
        "status": "failed_expectations" if failures else ("ok" if results else "empty"),
        "read_only": True,
        "matrix_version": dict(matrix or {}).get("matrix_version", 1),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "case_count": len(results),
        "failure_count": len(failures),
        "failures": failures,
        "cases": results,
    }


def compare_authoritative_reuse_planning_matrix_reports(
    *,
    left: dict[str, Any],
    right: dict[str, Any],
    compare_fields: list[str] | None = None,
) -> dict[str, Any]:
    fields = _dedupe_strings(compare_fields or DEFAULT_PARITY_COMPARE_FIELDS)
    left_cases = _matrix_cases_by_id(left)
    right_cases = _matrix_cases_by_id(right)
    case_ids = _dedupe_strings([*left_cases.keys(), *right_cases.keys()])
    results: list[dict[str, Any]] = []
    drift_count = 0
    for case_id in case_ids:
        left_case = dict(left_cases.get(case_id) or {})
        right_case = dict(right_cases.get(case_id) or {})
        if not left_case or not right_case:
            drift_count += 1
            results.append(
                {
                    "case_id": case_id,
                    "status": "missing_case",
                    "missing_left": not bool(left_case),
                    "missing_right": not bool(right_case),
                    "field_diffs": [],
                }
            )
            continue
        left_summary = dict(left_case.get("summary") or {})
        right_summary = dict(right_case.get("summary") or {})
        field_diffs = []
        for field in fields:
            left_value = left_summary.get(field)
            right_value = right_summary.get(field)
            if left_value != right_value:
                field_diffs.append(
                    {
                        "field": field,
                        "left": left_value,
                        "right": right_value,
                    }
                )
        if field_diffs:
            drift_count += 1
        results.append(
            {
                "case_id": case_id,
                "status": "drift" if field_diffs else "match",
                "company": left_case.get("company") or right_case.get("company"),
                "query": left_case.get("query") or right_case.get("query"),
                "field_diffs": field_diffs,
            }
        )
    return {
        "status": "drift" if drift_count else "match",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "compare_fields": fields,
        "case_count": len(results),
        "drift_count": drift_count,
        "cases": results,
    }


def summarize_authoritative_reuse_audit(audit: dict[str, Any], *, case_id: str = "") -> dict[str, Any]:
    payload = dict(audit or {})
    request = dict(payload.get("request") or {})
    boundary = dict(request.get("requested_population_boundary") or {})
    planner = dict(payload.get("planner") or {})
    strategy = dict(planner.get("acquisition_strategy") or {})
    explanation = dict(planner.get("strategy_decision_explanation") or {})
    reuse_plan = dict(planner.get("asset_reuse_plan") or {})
    semantics = dict(planner.get("effective_execution_semantics") or {})
    shard_registry = dict(payload.get("scoped_shard_registry") or {})
    current_profile_gaps = list(reuse_plan.get("current_profile_search_exact_overlap_gaps") or [])
    former_profile_gaps = list(reuse_plan.get("former_profile_search_exact_overlap_gaps") or [])
    return {
        "case_id": _normalize_text(case_id),
        "target_company": _normalize_text(payload.get("target_company")),
        "asset_view": _normalize_text(payload.get("asset_view")) or "canonical_merged",
        "query": _normalize_text(payload.get("query")),
        "requested_population_boundary": _normalize_text(boundary.get("boundary_type")),
        "strategy_type": _normalize_text(strategy.get("strategy_type")),
        "strategy_decision_source": _normalize_text(explanation.get("decision_source")),
        "planner_mode": _normalize_text(reuse_plan.get("planner_mode")),
        "requires_delta_acquisition": bool(reuse_plan.get("requires_delta_acquisition")),
        "baseline_snapshot_id": _normalize_text(reuse_plan.get("baseline_snapshot_id")),
        "baseline_full_company_coverage_proven": bool(reuse_plan.get("baseline_full_company_coverage_proven")),
        "full_company_filter_from_baseline": bool(reuse_plan.get("full_company_filter_from_baseline")),
        "profile_query_requires_explicit_coverage": bool(reuse_plan.get("profile_query_requires_explicit_coverage")),
        "full_local_asset_reuse": bool(semantics.get("full_local_asset_reuse")),
        "execution_strategy_label": _normalize_text(semantics.get("execution_strategy_label")),
        "missing_current_profile_search_query_count": _safe_int(
            reuse_plan.get("missing_current_profile_search_query_count")
        ),
        "missing_former_profile_search_query_count": _safe_int(
            reuse_plan.get("missing_former_profile_search_query_count")
        ),
        "current_profile_exact_overlap_gap_count": len(current_profile_gaps),
        "former_profile_exact_overlap_gap_count": len(former_profile_gaps),
        "selected_snapshot_missing_shard_registry_row_count": len(
            list(shard_registry.get("missing_selected_snapshot_ids") or [])
        ),
        "baseline_generation_lags_same_snapshot_shard_materialization": _has_same_snapshot_exact_overlap_gap(
            asset_reuse_plan=reuse_plan,
            gap_rows=[*current_profile_gaps, *former_profile_gaps],
        ),
        "warning_codes": list(payload.get("warnings") or []),
    }


def _candidate_source_from_asset_reuse_plan(
    asset_reuse_plan: dict[str, Any],
    registry_row: dict[str, Any],
) -> dict[str, Any]:
    baseline_snapshot_id = _normalize_text(asset_reuse_plan.get("baseline_snapshot_id") or registry_row.get("snapshot_id"))
    if not baseline_snapshot_id:
        return {}
    return {
        "source_kind": "company_snapshot",
        "snapshot_id": baseline_snapshot_id,
        "asset_view": _normalize_text(asset_reuse_plan.get("baseline_asset_view") or registry_row.get("asset_view"))
        or "canonical_merged",
        "target_company": _normalize_text(registry_row.get("target_company")),
    }


def _matrix_cases_by_id(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(list(dict(report or {}).get("cases") or []), start=1):
        case = dict(item or {})
        case_id = _normalize_text(case.get("case_id")) or _normalize_text(
            dict(case.get("summary") or {}).get("case_id")
        ) or f"case_{index:03d}"
        cases[case_id] = case
    return cases


def _evaluate_case_expectations(summary: dict[str, Any], expectations: dict[str, Any]) -> list[str]:
    if not expectations:
        return []
    failures: list[str] = []
    scalar_fields = {
        "requested_population_boundary",
        "strategy_type",
        "strategy_decision_source",
        "planner_mode",
        "requires_delta_acquisition",
        "baseline_full_company_coverage_proven",
        "full_company_filter_from_baseline",
        "profile_query_requires_explicit_coverage",
        "full_local_asset_reuse",
        "execution_strategy_label",
        "missing_current_profile_search_query_count",
        "missing_former_profile_search_query_count",
        "current_profile_exact_overlap_gap_count",
        "former_profile_exact_overlap_gap_count",
        "selected_snapshot_missing_shard_registry_row_count",
        "baseline_generation_lags_same_snapshot_shard_materialization",
    }
    for field in sorted(scalar_fields):
        if field not in expectations:
            continue
        if summary.get(field) != expectations.get(field):
            failures.append(
                f"{field}_expected_{_expectation_label(expectations.get(field))}_got_{_expectation_label(summary.get(field))}"
            )
    allowed_planner_modes = _dedupe_strings(expectations.get("allowed_planner_modes") or [])
    if allowed_planner_modes and _normalize_text(summary.get("planner_mode")) not in allowed_planner_modes:
        failures.append(
            "planner_mode_not_allowed:"
            f"{_normalize_text(summary.get('planner_mode')) or '<empty>'}"
        )
    required_warnings = set(_dedupe_strings(expectations.get("required_warnings") or []))
    warning_codes = set(_dedupe_strings(summary.get("warning_codes") or []))
    for warning in sorted(required_warnings - warning_codes):
        failures.append(f"missing_warning:{warning}")
    for warning in sorted(set(_dedupe_strings(expectations.get("forbidden_warnings") or [])) & warning_codes):
        failures.append(f"forbidden_warning:{warning}")
    return failures


def _expectation_label(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return _normalize_text(value) or "empty"


def _compile_warnings(
    *,
    request_boundary: dict[str, Any],
    authoritative_row: dict[str, Any],
    selected_row: dict[str, Any],
    selected_snapshot_ids: list[str],
    population_contract: dict[str, Any],
    shard_summary: dict[str, Any],
    asset_reuse_plan: dict[str, Any],
    execution_semantics: dict[str, Any],
    inventory: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    boundary_type = _normalize_text(request_boundary.get("boundary_type")).lower()
    full_company_proven = bool(population_contract.get("full_company_coverage_proven"))
    scoped_shard_only = bool(population_contract.get("scoped_shard_only"))
    if authoritative_row and not full_company_proven:
        warnings.append("authoritative_pointer_without_full_company_coverage_proof")
    if boundary_type == "full_company_roster" and scoped_shard_only:
        warnings.append("full_company_request_not_satisfied_by_scoped_authoritative_asset")
    if (
        boundary_type == "scoped_directional"
        and bool(request_boundary.get("full_company_filter_allowed"))
        and not full_company_proven
    ):
        warnings.append("directional_all_members_requires_full_company_coverage_for_local_filter")
    if list(shard_summary.get("missing_selected_snapshot_ids") or []):
        warnings.append("selected_snapshot_ids_missing_shard_registry_rows")
    current_profile_gaps = list(asset_reuse_plan.get("current_profile_search_exact_overlap_gaps") or [])
    former_profile_gaps = list(asset_reuse_plan.get("former_profile_search_exact_overlap_gaps") or [])
    if current_profile_gaps:
        warnings.append("baseline_generation_missing_current_profile_shard_members")
    if former_profile_gaps:
        warnings.append("baseline_generation_missing_former_profile_shard_members")
    if _has_same_snapshot_exact_overlap_gap(
        asset_reuse_plan=asset_reuse_plan,
        gap_rows=[*current_profile_gaps, *former_profile_gaps],
    ):
        warnings.append("baseline_generation_lags_same_snapshot_shard_materialization")
    if selected_row and authoritative_row and _normalize_text(selected_row.get("snapshot_id")) != _normalize_text(
        authoritative_row.get("snapshot_id")
    ):
        warnings.append("planning_selected_row_differs_from_authoritative_pointer")
    if not list(inventory.get("ordered_candidate_rows") or []):
        warnings.append("no_registry_candidate_rows_available")
    if _normalize_text(asset_reuse_plan.get("baseline_resolution_mode")) == "cached_missing_no_rebuild":
        warnings.append("cached_ledger_missing_and_rebuild_suppressed_for_read_only_audit")
    if (
        bool(asset_reuse_plan.get("baseline_reuse_available"))
        and _normalize_text(asset_reuse_plan.get("planner_mode")) == "reuse_snapshot_only"
        and not full_company_proven
        and boundary_type == "full_company_roster"
    ):
        warnings.append("planner_reuse_snapshot_only_without_full_company_proof_for_full_request")
    if bool(execution_semantics.get("full_local_asset_reuse")) and not full_company_proven:
        warnings.append("execution_semantics_full_reuse_without_full_company_proof")
    if selected_snapshot_ids and not list(shard_summary.get("rows") or []) and scoped_shard_only:
        warnings.append("scoped_coverage_claim_without_visible_shard_rows")
    return _dedupe_strings(warnings)


def _has_same_snapshot_exact_overlap_gap(
    *,
    asset_reuse_plan: dict[str, Any],
    gap_rows: list[dict[str, Any]],
) -> bool:
    baseline_snapshot_id = _normalize_text(asset_reuse_plan.get("baseline_snapshot_id"))
    if not baseline_snapshot_id:
        return False
    return any(
        _normalize_text(dict(row or {}).get("snapshot_id")) == baseline_snapshot_id
        for row in list(gap_rows or [])
    )


def _summarize_registry_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    if not payload:
        return {}
    summary = dict(payload.get("summary") or {})
    source_snapshot_selection = dict(payload.get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {})
    return {
        "registry_id": _safe_int(payload.get("registry_id")),
        "target_company": _normalize_text(payload.get("target_company")),
        "company_key": _normalize_text(payload.get("company_key")),
        "snapshot_id": _normalize_text(payload.get("snapshot_id")),
        "asset_view": _normalize_text(payload.get("asset_view")) or "canonical_merged",
        "status": _normalize_text(payload.get("status")),
        "authoritative": bool(payload.get("authoritative")),
        "candidate_count": _safe_int(payload.get("candidate_count")),
        "profile_detail_count": _safe_int(payload.get("profile_detail_count")),
        "current_lane_effective_candidate_count": _safe_int(payload.get("current_lane_effective_candidate_count")),
        "former_lane_effective_candidate_count": _safe_int(payload.get("former_lane_effective_candidate_count")),
        "current_lane_effective_ready": bool(payload.get("current_lane_effective_ready")),
        "former_lane_effective_ready": bool(payload.get("former_lane_effective_ready")),
        "completeness_score": _safe_float(payload.get("completeness_score")),
        "completeness_band": _normalize_text(payload.get("completeness_band")),
        "selected_snapshot_ids": _selected_snapshot_ids(payload),
        "source_snapshot_selection": source_snapshot_selection,
        "population_coverage": dict(
            source_snapshot_selection.get("population_coverage")
            or source_snapshot_selection.get("full_company_coverage")
            or summary.get("population_coverage")
            or summary.get("full_company_coverage")
            or {}
        ),
        "source_path": _normalize_text(payload.get("source_path")),
    }


def _summarize_shard_registry(
    *,
    rows: list[dict[str, Any]],
    selected_snapshot_ids: list[str],
    serving_snapshot_id: str = "",
) -> dict[str, Any]:
    normalized_rows = [_summarize_shard_row(row) for row in list(rows or []) if isinstance(row, dict)]
    by_lane: dict[str, int] = {}
    by_lane_employment_scope: dict[str, int] = {}
    by_search_query: dict[str, int] = {}
    for row in normalized_rows:
        lane = _normalize_text(row.get("lane")) or "unknown"
        employment_scope = _normalize_text(row.get("employment_scope")) or "all"
        search_query = _normalize_text(row.get("search_query") or row.get("shard_title") or row.get("shard_id"))
        by_lane[lane] = by_lane.get(lane, 0) + 1
        lane_key = f"{lane}:{employment_scope}"
        by_lane_employment_scope[lane_key] = by_lane_employment_scope.get(lane_key, 0) + 1
        if search_query:
            by_search_query[search_query] = by_search_query.get(search_query, 0) + 1
    covered_snapshot_ids = _dedupe_strings(row.get("snapshot_id") for row in normalized_rows)
    covered_lookup = {item.lower() for item in covered_snapshot_ids}
    selected_ids = _dedupe_strings(selected_snapshot_ids)
    serving_lookup = {_normalize_text(serving_snapshot_id).lower()} if _normalize_text(serving_snapshot_id) else set()
    selected_without_rows = [
        snapshot_id
        for snapshot_id in selected_ids
        if snapshot_id.lower() not in covered_lookup
    ]
    missing_selected = [snapshot_id for snapshot_id in selected_without_rows if snapshot_id.lower() not in serving_lookup]
    serving_without_rows = [
        snapshot_id for snapshot_id in selected_without_rows if snapshot_id.lower() in serving_lookup
    ]
    return {
        "total_rows": len(normalized_rows),
        "selected_snapshot_ids": selected_ids,
        "serving_snapshot_id": _normalize_text(serving_snapshot_id),
        "snapshot_ids_covered": covered_snapshot_ids,
        "selected_snapshot_ids_without_shard_registry_rows": selected_without_rows,
        "missing_selected_snapshot_ids": missing_selected,
        "serving_snapshot_ids_without_shard_registry_rows": serving_without_rows,
        "by_lane": dict(sorted(by_lane.items())),
        "by_lane_employment_scope": dict(sorted(by_lane_employment_scope.items())),
        "by_search_query": dict(sorted(by_search_query.items())),
        "current_exact_coverage_rows": [
            row
            for row in normalized_rows
            if _normalize_text(row.get("employment_scope")).lower() != "former"
            and _normalize_text(row.get("lane")).lower() in {"company_employees", "profile_search"}
        ],
        "former_exact_coverage_rows": [
            row
            for row in normalized_rows
            if _normalize_text(row.get("employment_scope")).lower() == "former"
            and _normalize_text(row.get("lane")).lower() in {"company_employees", "profile_search"}
        ],
        "rows": normalized_rows[:200],
        "row_sample_limit": 200,
    }


def _summarize_shard_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    return {
        "shard_key": _normalize_text(payload.get("shard_key")),
        "snapshot_id": _normalize_text(payload.get("snapshot_id")),
        "lane": _normalize_text(payload.get("lane")),
        "employment_scope": _normalize_text(payload.get("employment_scope")) or "all",
        "strategy_type": _normalize_text(payload.get("strategy_type")),
        "status": _normalize_text(payload.get("status")),
        "shard_id": _normalize_text(payload.get("shard_id")),
        "shard_title": _normalize_text(payload.get("shard_title")),
        "search_query": _normalize_text(payload.get("search_query")),
        "query_signature": _normalize_text(payload.get("query_signature")),
        "result_count": _safe_int(payload.get("result_count")),
        "estimated_total_count": _safe_int(payload.get("estimated_total_count")),
        "provider_cap_hit": bool(payload.get("provider_cap_hit")),
        "source_job_id": _normalize_text(payload.get("source_job_id")),
    }


def _list_shard_rows(
    *,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_ids: list[str],
    statuses: list[str] | None,
) -> list[dict[str, Any]]:
    if not snapshot_ids:
        return []
    return store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=snapshot_ids,
        statuses=statuses,
        limit=max(1000, len(snapshot_ids) * 250),
    )


def _load_cached_ledger_summary(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
) -> dict[str, Any]:
    payload = dict(
        load_cached_organization_completeness_ledger(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
        or {}
    )
    if isinstance(payload.get("summary"), dict):
        return dict(payload.get("summary") or {})
    return payload


def _selected_snapshot_ids(row: dict[str, Any]) -> list[str]:
    payload = dict(row or {})
    selection = dict(payload.get("source_snapshot_selection") or {})
    summary = dict(payload.get("summary") or {})
    summary_selection = dict(summary.get("source_snapshot_selection") or {})
    return _dedupe_strings(
        payload.get("selected_snapshot_ids")
        or selection.get("selected_snapshot_ids")
        or summary.get("selected_snapshot_ids")
        or summary_selection.get("selected_snapshot_ids")
        or [payload.get("snapshot_id")]
    )


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = _normalize_text(item)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized
