from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import load_company_snapshot_identity, resolve_company_snapshot_dir
from .asset_reuse_audit import audit_authoritative_reuse_planning_many, summarize_authoritative_reuse_audit
from .asset_reuse_planning import (
    build_organization_asset_registry_record,
    enforce_reusable_source_snapshot_provenance,
    ensure_explicit_population_coverage_for_registry_record,
    inherit_reusable_source_snapshot_coverage,
)
from .candidate_artifacts import build_company_candidate_artifacts
from .company_registry import normalize_company_key
from .domain import Candidate
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .organization_assets import build_acquisition_shard_standard_bundle
from .storage import ControlPlaneStore

_REUSABLE_SHARD_STATUSES = {"completed", "completed_with_cap", "skipped_high_overlap"}


def repair_authoritative_serving_generation(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str,
    queries: list[str],
    asset_view: str = "canonical_merged",
    snapshot_id: str = "",
    repair_snapshot_id: str = "",
    build_profile: str = "foreground_fast",
    output_dir: str | Path | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    """Repair an authoritative serving generation that lags completed shard bundles.

    The command is intentionally explicit and offline: it reads the planner audit,
    standardizes the lagging shard bundles from already persisted raw assets, creates
    a new serving snapshot, and publishes that new generation only when `apply=True`.
    It does not call providers and does not mutate historical snapshots in place.
    """

    normalized_company = _normalize_text(company)
    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    normalized_queries = _dedupe_strings(queries)
    if not normalized_company:
        raise ValueError("company is required")
    if not normalized_queries:
        raise ValueError("at least one query is required")

    before_audit = audit_authoritative_reuse_planning_many(
        runtime_dir=runtime_dir,
        store=store,
        company=normalized_company,
        queries=normalized_queries,
        asset_view=normalized_asset_view,
    )
    audits = [dict(item) for item in list(before_audit.get("audits") or []) if isinstance(item, dict)]
    baseline_snapshot_ids = _dedupe_strings(
        [
            snapshot_id,
            *[
                dict(dict(audit.get("planner") or {}).get("asset_reuse_plan") or {}).get("baseline_snapshot_id")
                for audit in audits
            ],
        ]
    )
    if len(baseline_snapshot_ids) != 1:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "mixed_or_missing_baseline_snapshot",
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "baseline_snapshot_ids": baseline_snapshot_ids,
            "before_audit": before_audit,
        }
    baseline_snapshot_id = baseline_snapshot_ids[0]
    gap_specs = _collect_same_snapshot_materialization_gaps(audits, baseline_snapshot_id=baseline_snapshot_id)
    if not gap_specs:
        return {
            "status": "no_repair_needed",
            "applied": False,
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "baseline_snapshot_id": baseline_snapshot_id,
            "before_audit": before_audit,
        }

    authoritative_before = store.get_authoritative_organization_asset_registry(
        target_company=normalized_company,
        asset_view=normalized_asset_view,
    )
    shard_selection = _select_gap_shard_rows(
        store=store,
        target_company=normalized_company,
        baseline_snapshot_id=baseline_snapshot_id,
        gap_specs=gap_specs,
    )
    if shard_selection["unresolved_gaps"]:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "gap_shard_rows_unresolved",
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "baseline_snapshot_id": baseline_snapshot_id,
            "gap_count": len(gap_specs),
            "selected_shard_count": len(shard_selection["rows"]),
            "unresolved_gaps": shard_selection["unresolved_gaps"],
            "before_audit": before_audit,
        }

    dry_run_bundle_preview = [
        _summarize_existing_bundle(row, runtime_dir=runtime_dir)
        for row in list(shard_selection["rows"] or [])
    ]
    if not apply:
        return {
            "status": "dry_run",
            "applied": False,
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "baseline_snapshot_id": baseline_snapshot_id,
            "repair_snapshot_id": _normalize_text(repair_snapshot_id) or _default_repair_snapshot_id(),
            "gap_count": len(gap_specs),
            "selected_shard_count": len(shard_selection["rows"]),
            "gap_specs": gap_specs,
            "selected_shards": [_summarize_shard_row(row) for row in shard_selection["rows"]],
            "bundle_preview": dry_run_bundle_preview,
            "before_audit_summaries": [
                summarize_authoritative_reuse_audit(audit, case_id=f"query_{index}")
                for index, audit in enumerate(audits, start=1)
            ],
        }

    baseline_snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=normalized_company,
        snapshot_id=baseline_snapshot_id,
    )
    if baseline_snapshot_dir is None:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "baseline_snapshot_dir_missing",
            "target_company": normalized_company,
            "baseline_snapshot_id": baseline_snapshot_id,
        }

    bundle_results = []
    bundle_payloads = []
    applied_shard_rows: list[dict[str, Any]] = []
    for row in list(shard_selection["rows"] or []):
        bundle_result = build_acquisition_shard_standard_bundle(
            runtime_dir=runtime_dir,
            store=store,
            target_company=normalized_company,
            snapshot_id=baseline_snapshot_id,
            shard_row=row,
            asset_view=normalized_asset_view,
        )
        bundle_results.append(bundle_result)
        applied_shard_rows.append(dict(bundle_result.get("row") or row))
        bundle_payload = _load_bundle_payload(row, bundle_result=bundle_result, runtime_dir=runtime_dir)
        if not bundle_payload:
            return {
                "status": "blocked",
                "applied": False,
                "reason": "bundle_payload_missing_after_standardization",
                "target_company": normalized_company,
                "baseline_snapshot_id": baseline_snapshot_id,
                "shard": _summarize_shard_row(row),
                "bundle_result": bundle_result,
            }
        bundle_payloads.append(bundle_payload)

    repair_snapshot_id = _normalize_text(repair_snapshot_id) or _default_repair_snapshot_id()
    repair_snapshot_dir = _write_repair_snapshot(
        runtime_dir=runtime_dir,
        baseline_snapshot_dir=baseline_snapshot_dir,
        target_company=normalized_company,
        baseline_snapshot_id=baseline_snapshot_id,
        repair_snapshot_id=repair_snapshot_id,
        asset_view=normalized_asset_view,
        bundle_payloads=bundle_payloads,
        selected_shard_rows=applied_shard_rows,
        gap_specs=gap_specs,
    )
    build_result = build_company_candidate_artifacts(
        runtime_dir=runtime_dir,
        store=store,
        target_company=normalized_company,
        snapshot_id=repair_snapshot_id,
        output_dir=output_dir,
        preferred_source_snapshot_ids=[repair_snapshot_id],
        build_profile=build_profile,
        sync_registration=False,
    )
    refreshed_shard_rows = _refresh_shard_rows(
        store=store,
        target_company=normalized_company,
        baseline_snapshot_id=baseline_snapshot_id,
        shard_rows=applied_shard_rows,
    )
    authoritative_after = _force_publish_repair_registry_row(
        store=store,
        target_company=normalized_company,
        asset_view=normalized_asset_view,
        repair_snapshot_id=repair_snapshot_id,
        baseline_snapshot_id=baseline_snapshot_id,
        build_result=build_result,
        previous_authoritative=authoritative_before,
        selected_shard_rows=refreshed_shard_rows,
        gap_specs=gap_specs,
    )
    generation_checks = _check_repaired_generation_subsumes_shards(
        store=store,
        repair_generation_key=_normalize_text(authoritative_after.get("materialization_generation_key"))
        or _normalize_text(dict(build_result.get("summary") or {}).get("materialization_generation_key")),
        selected_shard_rows=refreshed_shard_rows,
    )
    after_audit = audit_authoritative_reuse_planning_many(
        runtime_dir=runtime_dir,
        store=store,
        company=normalized_company,
        queries=normalized_queries,
        asset_view=normalized_asset_view,
    )
    after_warnings = set(_dedupe_strings(after_audit.get("warning_codes") or []))
    all_subsumed = bool(generation_checks) and all(bool(item.get("baseline_subsumes_row")) for item in generation_checks)
    planner_lag_cleared = "baseline_generation_lags_same_snapshot_shard_materialization" not in after_warnings
    scope_mismatch_count = len(
        [
            item
            for item in generation_checks
            if bool(item.get("baseline_subsumes_row")) and not bool(item.get("employment_scope_subsumes_row"))
        ]
    )
    quality_warnings = []
    if not all_subsumed:
        quality_warnings.append("repaired_generation_missing_selected_shard_members")
    if not planner_lag_cleared:
        quality_warnings.append("planner_still_reports_same_snapshot_generation_lag")
    if scope_mismatch_count > 0:
        quality_warnings.append("repaired_generation_member_scope_mismatch")
    if all_subsumed and planner_lag_cleared:
        status = "repaired_with_scope_mismatch" if scope_mismatch_count > 0 else "repaired"
    else:
        status = "repair_incomplete"
    return {
        "status": status,
        "applied": True,
        "target_company": normalized_company,
        "asset_view": normalized_asset_view,
        "baseline_snapshot_id": baseline_snapshot_id,
        "repair_snapshot_id": repair_snapshot_id,
        "repair_snapshot_dir": str(repair_snapshot_dir),
        "gap_count": len(gap_specs),
        "selected_shard_count": len(shard_selection["rows"]),
        "bundle_results": bundle_results,
        "build_result": build_result,
        "authoritative_before": authoritative_before,
        "authoritative_after": authoritative_after,
        "generation_checks": generation_checks,
        "generation_subsumes_selected_shards": all_subsumed,
        "planner_lag_cleared": planner_lag_cleared,
        "scope_mismatch_count": scope_mismatch_count,
        "quality_warnings": quality_warnings,
        "before_audit": before_audit,
        "after_audit": after_audit,
    }


def inspect_authoritative_serving_generation_publication(
    *,
    store: ControlPlaneStore,
    candidate_record: dict[str, Any],
) -> dict[str, Any]:
    """Validate the publication invariant for an authoritative serving row.

    A serving generation can be promoted only when same-snapshot reusable shard
    materializations selected for planning are already subsumed by that generation.
    Cross-snapshot source rows remain provenance inputs and are validated by the
    planner; the publication invariant is specifically about same-snapshot drift.
    """

    record = dict(candidate_record or {})
    target_company = _normalize_text(record.get("target_company"))
    snapshot_id = _normalize_text(record.get("snapshot_id"))
    generation_key = _normalize_text(record.get("materialization_generation_key"))
    if not target_company or not snapshot_id:
        return {
            "status": "skipped",
            "reason": "missing_company_or_snapshot",
            "gap_count": 0,
            "unverifiable_shard_count": 0,
            "checks": [],
            "gaps": [],
        }
    if not generation_key:
        return {
            "status": "blocked",
            "reason": "missing_serving_generation_key",
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "gap_count": 0,
            "unverifiable_shard_count": 0,
            "checks": [],
            "gaps": [],
        }

    rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=[snapshot_id],
        statuses=sorted(_REUSABLE_SHARD_STATUSES),
        limit=5000,
    )
    selected_snapshot_ids = _dedupe_strings(
        record.get("selected_snapshot_ids")
        or dict(record.get("source_snapshot_selection") or {}).get("selected_snapshot_ids")
        or [snapshot_id]
    )
    selected_snapshot_keys = {item.lower() for item in selected_snapshot_ids}
    serving_snapshot_selected = not selected_snapshot_keys or snapshot_id.lower() in selected_snapshot_keys
    checks: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    unverifiable: list[dict[str, Any]] = []
    for row in list(rows or []):
        row_payload = dict(row or {})
        lane = _normalize_text(row_payload.get("lane")).lower()
        if lane not in {"profile_search", "company_employees"}:
            continue
        if _safe_int(row_payload.get("result_count")) <= 0:
            continue
        if not serving_snapshot_selected:
            continue
        row_generation_key = _normalize_text(row_payload.get("materialization_generation_key"))
        if not row_generation_key:
            unverifiable.append(_summarize_shard_row(row_payload))
            continue
        employment_scope = _normalize_text(row_payload.get("employment_scope"))
        scoped_comparison = store.compare_asset_membership_generations(
            primary_generation_key=generation_key,
            secondary_generation_key=row_generation_key,
            primary_employment_scope=employment_scope,
            secondary_employment_scope=employment_scope,
        )
        any_scope_comparison = store.compare_asset_membership_generations(
            primary_generation_key=generation_key,
            secondary_generation_key=row_generation_key,
        )
        employment_scope_subsumes = bool(scoped_comparison.get("primary_subsumes_secondary"))
        any_scope_subsumes = bool(any_scope_comparison.get("primary_subsumes_secondary"))
        check = {
            **_summarize_shard_row(row_payload),
            "serving_generation_key": generation_key,
            "employment_scope_subsumes_row": employment_scope_subsumes,
            "any_scope_subsumes_row": any_scope_subsumes,
            "baseline_subsumes_row": bool(employment_scope_subsumes or any_scope_subsumes),
            "scope_coherence_status": (
                "scope_coherent"
                if employment_scope_subsumes
                else "member_present_with_scope_mismatch"
                if any_scope_subsumes
                else "member_missing"
            ),
            "exact_overlap": {
                key: scoped_comparison.get(key)
                for key in (
                    "primary_member_count",
                    "secondary_member_count",
                    "overlap_member_count",
                    "secondary_only_member_count",
                    "secondary_overlap_ratio",
                    "primary_subsumes_secondary",
                )
            },
        }
        checks.append(check)
        if not check["baseline_subsumes_row"]:
            gaps.append({"row": row_payload, "check": check})

    status = "passed"
    reason = ""
    if gaps:
        status = "blocked"
        reason = "serving_generation_lags_selected_same_snapshot_shards"
    elif unverifiable:
        status = "blocked"
        reason = "selected_same_snapshot_shards_missing_materialization_generation"
    return {
        "status": status,
        "reason": reason,
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "serving_generation_key": generation_key,
        "selected_snapshot_ids": selected_snapshot_ids,
        "checked_shard_count": len(checks),
        "gap_count": len(gaps),
        "unverifiable_shard_count": len(unverifiable),
        "checks": checks,
        "gaps": [dict(item.get("check") or {}) for item in gaps],
        "unverifiable_shards": unverifiable,
        "_gap_rows": [dict(item.get("row") or {}) for item in gaps],
    }


def repair_authoritative_serving_generation_for_publication(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    candidate_record: dict[str, Any],
    existing_authoritative: dict[str, Any] | None = None,
    repair_snapshot_id: str = "",
    build_profile: str = "foreground_fast",
    apply: bool = True,
) -> dict[str, Any]:
    """Enforce same-snapshot shard subsumption before authoritative publication."""

    record = dict(candidate_record or {})
    check = inspect_authoritative_serving_generation_publication(store=store, candidate_record=record)
    if str(check.get("status") or "") == "passed":
        return {
            "status": "passed",
            "applied": False,
            "candidate_record": record,
            "publication_check": _public_publication_check_payload(check),
        }
    if str(check.get("status") or "") != "blocked" or not check.get("gap_count"):
        return {
            "status": "blocked",
            "applied": False,
            "reason": str(check.get("reason") or "publication_check_blocked"),
            "candidate_record": record,
            "publication_check": _public_publication_check_payload(check),
        }
    if not apply:
        return {
            "status": "dry_run",
            "applied": False,
            "reason": "serving_generation_repair_required_before_publication",
            "candidate_record": record,
            "publication_check": _public_publication_check_payload(check),
        }

    target_company = _normalize_text(record.get("target_company"))
    asset_view = _normalize_text(record.get("asset_view")) or "canonical_merged"
    baseline_snapshot_id = _normalize_text(record.get("snapshot_id"))
    baseline_snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=baseline_snapshot_id,
    )
    if baseline_snapshot_dir is None:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "baseline_snapshot_dir_missing",
            "candidate_record": record,
            "publication_check": _public_publication_check_payload(check),
        }

    gap_rows = [dict(row) for row in list(check.get("_gap_rows") or []) if isinstance(row, dict)]
    bundle_results: list[dict[str, Any]] = []
    bundle_payloads: list[dict[str, Any]] = []
    applied_shard_rows: list[dict[str, Any]] = []
    for row in gap_rows:
        bundle_result = build_acquisition_shard_standard_bundle(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=baseline_snapshot_id,
            shard_row=row,
            asset_view=asset_view,
        )
        bundle_results.append(bundle_result)
        applied_row = dict(bundle_result.get("row") or row)
        bundle_payload = _load_bundle_payload(applied_row, bundle_result=bundle_result, runtime_dir=runtime_dir)
        if not bundle_payload:
            return {
                "status": "blocked",
                "applied": False,
                "reason": "bundle_payload_missing_after_standardization",
                "candidate_record": record,
                "shard": _summarize_shard_row(row),
                "bundle_result": bundle_result,
                "publication_check": _public_publication_check_payload(check),
            }
        applied_shard_rows.append(applied_row)
        bundle_payloads.append(bundle_payload)

    repair_snapshot_id = _normalize_text(repair_snapshot_id) or _default_repair_snapshot_id()
    repair_snapshot_dir = _write_repair_snapshot(
        runtime_dir=runtime_dir,
        baseline_snapshot_dir=baseline_snapshot_dir,
        target_company=target_company,
        baseline_snapshot_id=baseline_snapshot_id,
        repair_snapshot_id=repair_snapshot_id,
        asset_view=asset_view,
        bundle_payloads=bundle_payloads,
        selected_shard_rows=applied_shard_rows,
        gap_specs=[dict(item) for item in list(check.get("gaps") or []) if isinstance(item, dict)],
    )
    build_result = build_company_candidate_artifacts(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=repair_snapshot_id,
        preferred_source_snapshot_ids=[repair_snapshot_id],
        build_profile=build_profile,
        sync_registration=False,
    )
    refreshed_shard_rows = _refresh_shard_rows(
        store=store,
        target_company=target_company,
        baseline_snapshot_id=baseline_snapshot_id,
        shard_rows=applied_shard_rows,
    )
    repair_generation_key = _normalize_text(
        dict(dict(build_result.get("views") or {}).get("canonical_merged") or {}).get("summary", {}).get(
            "materialization_generation_key"
        )
    ) or _normalize_text(dict(build_result.get("summary") or {}).get("materialization_generation_key"))
    generation_checks = _check_repaired_generation_subsumes_shards(
        store=store,
        repair_generation_key=repair_generation_key,
        selected_shard_rows=refreshed_shard_rows,
    )
    all_subsumed = bool(generation_checks) and all(bool(item.get("baseline_subsumes_row")) for item in generation_checks)
    if not all_subsumed:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "repair_generation_still_missing_selected_shard_members",
            "candidate_record": record,
            "repair_snapshot_id": repair_snapshot_id,
            "repair_snapshot_dir": str(repair_snapshot_dir),
            "bundle_results": bundle_results,
            "build_result": build_result,
            "generation_checks": generation_checks,
            "publication_check": _public_publication_check_payload(check),
        }

    previous_authoritative = dict(existing_authoritative or {})
    authoritative_after = _force_publish_repair_registry_row(
        store=store,
        target_company=target_company,
        asset_view=asset_view,
        repair_snapshot_id=repair_snapshot_id,
        baseline_snapshot_id=baseline_snapshot_id,
        build_result=build_result,
        previous_authoritative=previous_authoritative,
        selected_shard_rows=refreshed_shard_rows,
        gap_specs=[dict(item) for item in list(check.get("gaps") or []) if isinstance(item, dict)],
    )
    scope_mismatch_count = len(
        [
            item
            for item in generation_checks
            if bool(item.get("baseline_subsumes_row")) and not bool(item.get("employment_scope_subsumes_row"))
        ]
    )
    return {
        "status": "repaired_with_scope_mismatch" if scope_mismatch_count else "repaired",
        "applied": True,
        "candidate_record": authoritative_after,
        "authoritative_after": authoritative_after,
        "baseline_snapshot_id": baseline_snapshot_id,
        "repair_snapshot_id": repair_snapshot_id,
        "repair_snapshot_dir": str(repair_snapshot_dir),
        "bundle_results": bundle_results,
        "build_result": build_result,
        "generation_checks": generation_checks,
        "scope_mismatch_count": scope_mismatch_count,
        "publication_check": _public_publication_check_payload(check),
    }


def _public_publication_check_payload(check: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(check or {}).items() if not str(key).startswith("_")}


def _collect_same_snapshot_materialization_gaps(
    audits: list[dict[str, Any]],
    *,
    baseline_snapshot_id: str,
) -> list[dict[str, Any]]:
    gap_specs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for audit in list(audits or []):
        reuse_plan = dict(dict(audit.get("planner") or {}).get("asset_reuse_plan") or {})
        for field in (
            "current_profile_search_exact_overlap_gaps",
            "former_profile_search_exact_overlap_gaps",
            "current_company_employee_exact_overlap_gaps",
            "former_company_employee_exact_overlap_gaps",
        ):
            for gap in list(reuse_plan.get(field) or []):
                payload = dict(gap or {})
                if _normalize_text(payload.get("snapshot_id")) != baseline_snapshot_id:
                    continue
                generation_key = _normalize_text(payload.get("materialization_generation_key"))
                if not generation_key:
                    continue
                key = (
                    generation_key,
                    _normalize_text(payload.get("lane")),
                    _normalize_text(payload.get("employment_scope")),
                    _normalize_text(payload.get("search_query")).lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                gap_specs.append(
                    {
                        "field": field,
                        "query_or_shard": _normalize_text(payload.get("query_or_shard")),
                        "search_query": _normalize_text(payload.get("search_query")),
                        "snapshot_id": _normalize_text(payload.get("snapshot_id")),
                        "lane": _normalize_text(payload.get("lane")),
                        "employment_scope": _normalize_text(payload.get("employment_scope")),
                        "materialization_generation_key": generation_key,
                        "materialization_generation_sequence": _safe_int(
                            payload.get("materialization_generation_sequence")
                        ),
                        "materialization_watermark": _normalize_text(payload.get("materialization_watermark")),
                        "exact_overlap": dict(payload.get("exact_overlap") or {}),
                    }
                )
    return gap_specs


def _select_gap_shard_rows(
    *,
    store: ControlPlaneStore,
    target_company: str,
    baseline_snapshot_id: str,
    gap_specs: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=[baseline_snapshot_id],
        statuses=sorted(_REUSABLE_SHARD_STATUSES),
        limit=max(1000, len(gap_specs) * 20),
    )
    selected: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for gap in list(gap_specs or []):
        match = _match_gap_to_shard_row(gap, rows)
        if not match:
            unresolved.append(gap)
            continue
        shard_key = _normalize_text(match.get("shard_key"))
        if shard_key and shard_key not in seen_keys:
            seen_keys.add(shard_key)
            selected.append(dict(match))
    return {"rows": selected, "unresolved_gaps": unresolved}


def _match_gap_to_shard_row(gap: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    generation_key = _normalize_text(gap.get("materialization_generation_key"))
    if generation_key:
        for row in list(rows or []):
            if _normalize_text(row.get("materialization_generation_key")) == generation_key:
                return dict(row)
    lane = _normalize_text(gap.get("lane")).lower()
    scope = _normalize_text(gap.get("employment_scope")).lower()
    query = _normalize_text(gap.get("search_query")).lower()
    for row in list(rows or []):
        if lane and _normalize_text(row.get("lane")).lower() != lane:
            continue
        if scope and _normalize_text(row.get("employment_scope")).lower() != scope:
            continue
        if query and _normalize_text(row.get("search_query")).lower() != query:
            continue
        return dict(row)
    return {}


def _write_repair_snapshot(
    *,
    runtime_dir: str | Path,
    baseline_snapshot_dir: Path,
    target_company: str,
    baseline_snapshot_id: str,
    repair_snapshot_id: str,
    asset_view: str,
    bundle_payloads: list[dict[str, Any]],
    selected_shard_rows: list[dict[str, Any]],
    gap_specs: list[dict[str, Any]],
) -> Path:
    baseline_payload = _load_json_dict(baseline_snapshot_dir / "candidate_documents.json")
    company_key = baseline_snapshot_dir.parent.name or normalize_company_key(target_company)
    repair_snapshot_dir = Path(runtime_dir) / "company_assets" / company_key / repair_snapshot_id
    repair_snapshot_dir.mkdir(parents=True, exist_ok=True)
    company_identity = load_company_snapshot_identity(baseline_snapshot_dir, fallback_payload=baseline_payload)
    if not company_identity:
        company_identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
        }
    merged = _merge_candidate_document_payload(
        baseline_payload=baseline_payload,
        bundle_payloads=bundle_payloads,
    )
    snapshot_payload = dict(baseline_payload.get("snapshot") or {})
    snapshot_payload.update(
        {
            "target_company": target_company,
            "snapshot_id": repair_snapshot_id,
            "company_identity": company_identity,
            "serving_generation_repair": {
                "contract_version": 1,
                "base_snapshot_id": baseline_snapshot_id,
                "asset_view": asset_view,
                "selected_shard_keys": [
                    _normalize_text(row.get("shard_key")) for row in selected_shard_rows if _normalize_text(row.get("shard_key"))
                ],
                "gap_count": len(gap_specs),
                "created_at": _utc_now_iso(),
            },
        }
    )
    candidate_payload = {
        **{key: value for key, value in baseline_payload.items() if key not in {"candidates", "evidence", "snapshot"}},
        "snapshot": snapshot_payload,
        "candidates": merged["candidates"],
        "evidence": merged["evidence"],
    }
    _write_json(repair_snapshot_dir / "candidate_documents.json", candidate_payload)
    _write_json(repair_snapshot_dir / "identity.json", company_identity)
    repair_manifest = {
        "contract_version": 1,
        "target_company": target_company,
        "company_key": company_key,
        "base_snapshot_id": baseline_snapshot_id,
        "repair_snapshot_id": repair_snapshot_id,
        "asset_view": asset_view,
        "created_at": _utc_now_iso(),
        "base_candidate_count": len(list(baseline_payload.get("candidates") or [])),
        "repair_candidate_count": len(merged["candidates"]),
        "added_candidate_count": int(merged.get("added_candidate_count") or 0),
        "selected_shards": [_summarize_shard_row(row) for row in selected_shard_rows],
        "gap_specs": gap_specs,
    }
    _write_json(repair_snapshot_dir / "serving_generation_repair.json", repair_manifest)
    _write_json(
        repair_snapshot_dir.parent / "latest_snapshot.json",
        {
            "company_identity": company_identity,
            "snapshot_id": repair_snapshot_id,
            "snapshot_dir": str(repair_snapshot_dir),
            "updated_by": "repair_authoritative_serving_generation",
        },
    )
    return repair_snapshot_dir


def _merge_candidate_document_payload(
    *,
    baseline_payload: dict[str, Any],
    bundle_payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = [
        _candidate_record(item) for item in list(baseline_payload.get("candidates") or []) if isinstance(item, dict)
    ]
    evidence: list[dict[str, Any]] = [
        dict(item) for item in list(baseline_payload.get("evidence") or []) if isinstance(item, dict)
    ]
    candidate_id_by_key: dict[str, str] = {}
    candidate_keys_seen: set[str] = set()
    for candidate in candidates:
        key = _candidate_identity_key(candidate)
        candidate_id = _normalize_text(candidate.get("candidate_id"))
        if key:
            candidate_keys_seen.add(key)
            if candidate_id:
                candidate_id_by_key[key] = candidate_id
    evidence_keys_seen = {_evidence_identity_key(item) for item in evidence}
    added_candidate_count = 0
    candidate_id_remap: dict[str, str] = {}
    for bundle in list(bundle_payloads or []):
        for raw_candidate in list(bundle.get("candidates") or []):
            if not isinstance(raw_candidate, dict):
                continue
            candidate = _candidate_record(raw_candidate)
            key = _candidate_identity_key(candidate)
            incoming_candidate_id = _normalize_text(candidate.get("candidate_id"))
            if key and key in candidate_keys_seen:
                existing_candidate_id = candidate_id_by_key.get(key)
                if incoming_candidate_id and existing_candidate_id:
                    candidate_id_remap[incoming_candidate_id] = existing_candidate_id
                continue
            candidates.append(candidate)
            added_candidate_count += 1
            if key:
                candidate_keys_seen.add(key)
                if incoming_candidate_id:
                    candidate_id_by_key[key] = incoming_candidate_id
        for raw_evidence in list(bundle.get("evidence") or []):
            if not isinstance(raw_evidence, dict):
                continue
            item = dict(raw_evidence)
            original_candidate_id = _normalize_text(item.get("candidate_id"))
            if original_candidate_id in candidate_id_remap:
                item["candidate_id"] = candidate_id_remap[original_candidate_id]
            evidence_key = _evidence_identity_key(item)
            if evidence_key in evidence_keys_seen:
                continue
            evidence_keys_seen.add(evidence_key)
            evidence.append(item)
    return {
        "candidates": candidates,
        "evidence": evidence,
        "added_candidate_count": added_candidate_count,
    }


def _force_publish_repair_registry_row(
    *,
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str,
    repair_snapshot_id: str,
    baseline_snapshot_id: str,
    build_result: dict[str, Any],
    previous_authoritative: dict[str, Any],
    selected_shard_rows: list[dict[str, Any]],
    gap_specs: list[dict[str, Any]],
) -> dict[str, Any]:
    row = _find_registry_row(
        store=store,
        target_company=target_company,
        asset_view=asset_view,
        snapshot_id=repair_snapshot_id,
    )
    if not row:
        summary = dict(build_result.get("summary") or {})
        source_path = _normalize_text(dict(build_result.get("artifact_paths") or {}).get("artifact_summary"))
        row = build_organization_asset_registry_record(
            target_company=target_company,
            company_key=normalize_company_key(target_company),
            snapshot_id=repair_snapshot_id,
            asset_view=asset_view,
            summary=summary,
            source_path=source_path,
            authoritative=True,
        )
    row = inherit_reusable_source_snapshot_coverage(
        store=store,
        candidate_record=row,
        existing_authoritative=previous_authoritative,
    )
    selected_ids = _dedupe_strings(
        [
            repair_snapshot_id,
            baseline_snapshot_id,
            *list(row.get("selected_snapshot_ids") or []),
        ]
    )
    selection = dict(row.get("source_snapshot_selection") or {})
    selection["serving_generation_repair"] = {
        "contract_version": 1,
        "base_snapshot_id": baseline_snapshot_id,
        "repair_snapshot_id": repair_snapshot_id,
        "selected_shard_keys": [
            _normalize_text(item.get("shard_key")) for item in selected_shard_rows if _normalize_text(item.get("shard_key"))
        ],
        "gap_count": len(gap_specs),
        "published_at": _utc_now_iso(),
    }
    selection["selected_snapshot_ids"] = selected_ids
    selection.setdefault("serving_snapshot_id", repair_snapshot_id)
    selection.setdefault("source_snapshot_contract_version", 2)
    summary = dict(row.get("summary") or {})
    summary["selected_snapshot_ids"] = selected_ids
    summary["source_snapshot_selection"] = selection
    summary["serving_generation_repair"] = dict(selection["serving_generation_repair"])
    _copy_population_coverage(previous_authoritative, summary=summary, selection=selection)
    row["source_snapshot_selection"] = selection
    row["selected_snapshot_ids"] = selected_ids
    row["source_snapshot_count"] = max(_safe_int(row.get("source_snapshot_count")), len(selected_ids))
    row["summary"] = summary
    row = enforce_reusable_source_snapshot_provenance(
        store=store,
        candidate_record=row,
    )
    row = ensure_explicit_population_coverage_for_registry_record(
        store=store,
        candidate_record=row,
    )
    return store.upsert_organization_asset_registry(row, authoritative=True)


def _copy_population_coverage(
    previous_authoritative: dict[str, Any],
    *,
    summary: dict[str, Any],
    selection: dict[str, Any],
) -> None:
    previous_summary = dict(previous_authoritative.get("summary") or {})
    previous_selection = dict(
        previous_authoritative.get("source_snapshot_selection")
        or previous_summary.get("source_snapshot_selection")
        or {}
    )
    population_coverage = (
        previous_selection.get("population_coverage")
        or previous_selection.get("full_company_coverage")
        or previous_summary.get("population_coverage")
        or previous_summary.get("full_company_coverage")
    )
    if not isinstance(population_coverage, dict) or not population_coverage:
        return
    selection["population_coverage"] = dict(population_coverage)
    summary["population_coverage"] = dict(population_coverage)
    if bool(population_coverage.get("full_company_coverage_proven")):
        selection["full_company_coverage"] = dict(population_coverage)


def _check_repaired_generation_subsumes_shards(
    *,
    store: ControlPlaneStore,
    repair_generation_key: str,
    selected_shard_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not repair_generation_key:
        return []
    checks: list[dict[str, Any]] = []
    for row in list(selected_shard_rows or []):
        row_generation_key = _normalize_text(row.get("materialization_generation_key"))
        if not row_generation_key:
            continue
        employment_scope = _normalize_text(row.get("employment_scope"))
        scoped_comparison = store.compare_asset_membership_generations(
            primary_generation_key=repair_generation_key,
            secondary_generation_key=row_generation_key,
            primary_employment_scope=employment_scope,
            secondary_employment_scope=employment_scope,
        )
        any_scope_comparison = store.compare_asset_membership_generations(
            primary_generation_key=repair_generation_key,
            secondary_generation_key=row_generation_key,
        )
        comparison = dict(scoped_comparison or {})
        comparison["shard_key"] = _normalize_text(row.get("shard_key"))
        comparison["search_query"] = _normalize_text(row.get("search_query"))
        comparison["employment_scope"] = employment_scope
        comparison["any_scope_overlap"] = {
            key: any_scope_comparison.get(key)
            for key in (
                "primary_member_count",
                "secondary_member_count",
                "overlap_member_count",
                "primary_subsumes_secondary",
                "secondary_only_member_count",
                "secondary_overlap_ratio",
            )
        }
        comparison["employment_scope_subsumes_row"] = bool(scoped_comparison.get("primary_subsumes_secondary"))
        comparison["any_scope_subsumes_row"] = bool(any_scope_comparison.get("primary_subsumes_secondary"))
        comparison["baseline_subsumes_row"] = bool(
            comparison["employment_scope_subsumes_row"] or comparison["any_scope_subsumes_row"]
        )
        comparison["scope_coherence_status"] = (
            "scope_coherent"
            if comparison["employment_scope_subsumes_row"]
            else "member_present_with_scope_mismatch"
            if comparison["any_scope_subsumes_row"]
            else "member_missing"
        )
        checks.append(comparison)
    return checks


def _find_registry_row(
    *,
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str,
    snapshot_id: str,
) -> dict[str, Any]:
    for row in store.list_organization_asset_registry(
        target_company=target_company,
        asset_view=asset_view,
        limit=200,
    ):
        if _normalize_text(row.get("snapshot_id")) == snapshot_id:
            return dict(row)
    return {}


def _refresh_shard_rows(
    *,
    store: ControlPlaneStore,
    target_company: str,
    baseline_snapshot_id: str,
    shard_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    shard_keys = {_normalize_text(row.get("shard_key")) for row in list(shard_rows or []) if _normalize_text(row.get("shard_key"))}
    if not shard_keys:
        return [dict(row) for row in list(shard_rows or [])]
    current_rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=[baseline_snapshot_id],
        statuses=sorted(_REUSABLE_SHARD_STATUSES),
        limit=max(1000, len(shard_keys) * 20),
    )
    rows_by_key = {
        _normalize_text(row.get("shard_key")): dict(row)
        for row in list(current_rows or [])
        if _normalize_text(row.get("shard_key")) in shard_keys
    }
    return [rows_by_key.get(_normalize_text(row.get("shard_key")), dict(row)) for row in list(shard_rows or [])]


def _load_bundle_payload(
    row: dict[str, Any],
    *,
    bundle_result: dict[str, Any] | None = None,
    runtime_dir: str | Path,
) -> dict[str, Any]:
    candidate_paths = [
        dict(bundle_result or {}).get("bundle_path"),
        dict(dict(row.get("metadata") or {}).get("standard_bundle") or {}).get("bundle_path"),
    ]
    for candidate in candidate_paths:
        path = _resolve_existing_runtime_path(candidate, runtime_dir=runtime_dir)
        if not path:
            continue
        payload = _load_json_dict(path)
        if payload:
            return payload
    return {}


def _summarize_existing_bundle(row: dict[str, Any], *, runtime_dir: str | Path) -> dict[str, Any]:
    standard_bundle = dict(dict(row.get("metadata") or {}).get("standard_bundle") or {})
    bundle_path = _resolve_existing_runtime_path(standard_bundle.get("bundle_path"), runtime_dir=runtime_dir)
    bundle_payload = _load_json_dict(bundle_path) if bundle_path else {}
    return {
        "shard_key": _normalize_text(row.get("shard_key")),
        "search_query": _normalize_text(row.get("search_query")),
        "employment_scope": _normalize_text(row.get("employment_scope")),
        "bundle_path": str(bundle_path or standard_bundle.get("bundle_path") or ""),
        "bundle_exists": bool(bundle_path),
        "bundle_candidate_count": len(list(bundle_payload.get("candidates") or [])),
        "would_standardize_from_source_path": bool(_resolve_existing_runtime_path(row.get("source_path"), runtime_dir=runtime_dir)),
    }


def _resolve_existing_runtime_path(value: Any, *, runtime_dir: str | Path) -> Path | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    candidates = [Path(raw).expanduser()]
    runtime_root = Path(runtime_dir)
    raw_text = str(candidates[0])
    marker = "/runtime/hot_cache_company_assets/"
    if marker in raw_text:
        candidates.append(Path(raw_text.replace(marker, "/runtime/company_assets/", 1)))
    if not candidates[0].is_absolute():
        candidates.append(runtime_root / raw)
    for path in candidates:
        if path.exists():
            return path
    return None


def _summarize_shard_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "shard_key": _normalize_text(row.get("shard_key")),
        "snapshot_id": _normalize_text(row.get("snapshot_id")),
        "lane": _normalize_text(row.get("lane")),
        "employment_scope": _normalize_text(row.get("employment_scope")),
        "status": _normalize_text(row.get("status")),
        "search_query": _normalize_text(row.get("search_query")),
        "result_count": _safe_int(row.get("result_count")),
        "materialization_generation_key": _normalize_text(row.get("materialization_generation_key")),
        "materialization_generation_sequence": _safe_int(row.get("materialization_generation_sequence")),
        "materialization_watermark": _normalize_text(row.get("materialization_watermark")),
    }


def _candidate_record(item: dict[str, Any]) -> dict[str, Any]:
    try:
        return Candidate.from_record(dict(item or {})).to_record()
    except Exception:
        return dict(item or {})


def _candidate_identity_key(candidate: dict[str, Any]) -> str:
    linkedin_url = _normalize_text(candidate.get("linkedin_url"))
    url_key = normalize_linkedin_profile_url_key(linkedin_url)
    if url_key:
        return f"linkedin:{url_key}"
    candidate_id = _normalize_text(candidate.get("candidate_id"))
    if candidate_id:
        return f"candidate:{candidate_id.lower()}"
    return ""


def _evidence_identity_key(item: dict[str, Any]) -> str:
    evidence_id = _normalize_text(item.get("evidence_id"))
    if evidence_id:
        return f"id:{evidence_id}"
    return "|".join(
        [
            _normalize_text(item.get("candidate_id")),
            _normalize_text(item.get("source_type")),
            _normalize_text(item.get("url")),
            _normalize_text(item.get("title")),
        ]
    )


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _default_repair_snapshot_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    result: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = _normalize_text(value)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
