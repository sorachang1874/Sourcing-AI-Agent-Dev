from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_coverage_contracts import build_population_coverage_contract
from .company_registry import resolve_company_alias_key
from .organization_assets import load_cached_organization_completeness_ledger
from .storage import ControlPlaneStore

_REUSABLE_SHARD_STATUSES = {"completed", "completed_with_cap", "skipped_high_overlap"}


def backfill_authoritative_population_coverage(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    companies: list[str] | None = None,
    asset_view: str = "canonical_merged",
    include_non_authoritative: bool = False,
    dry_run: bool = True,
    force: bool = False,
    limit: int = 1000,
) -> dict[str, Any]:
    """Persist explicit population coverage metadata for registry rows.

    This is an operator backfill for migration parity. It converts already-proven
    coverage into an explicit `population_coverage` contract; it does not repair
    materialization generations, rebuild artifacts, or call providers.
    """

    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    normalized_companies = _dedupe_strings(companies or [])
    rows = _load_registry_rows(
        store=store,
        companies=normalized_companies,
        asset_view=normalized_asset_view,
        include_non_authoritative=include_non_authoritative,
        limit=limit,
    )
    results: list[dict[str, Any]] = []
    changed_count = 0
    skipped_count = 0
    persisted_count = 0
    for row in rows:
        result = _backfill_population_coverage_row(
            runtime_dir=runtime_dir,
            store=store,
            row=row,
            dry_run=dry_run,
            force=force,
        )
        results.append(result)
        if result.get("status") in {"would_update", "updated"}:
            changed_count += 1
        else:
            skipped_count += 1
        if result.get("status") == "updated":
            persisted_count += 1
    return {
        "status": "dry_run" if dry_run else "completed",
        "dry_run": bool(dry_run),
        "asset_view": normalized_asset_view,
        "company_count": len(normalized_companies),
        "registry_row_count": len(rows),
        "changed_count": changed_count,
        "persisted_count": persisted_count,
        "skipped_count": skipped_count,
        "include_non_authoritative": bool(include_non_authoritative),
        "force": bool(force),
        "results": results,
    }


def _load_registry_rows(
    *,
    store: ControlPlaneStore,
    companies: list[str],
    asset_view: str,
    include_non_authoritative: bool,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if companies:
        for company in companies:
            rows.extend(
                store.list_organization_asset_registry(
                    target_company=company,
                    asset_view=asset_view,
                    authoritative_only=not include_non_authoritative,
                    limit=max(1, int(limit or 1000)),
                )
            )
    else:
        rows.extend(
            store.list_organization_asset_registry(
                asset_view=asset_view,
                authoritative_only=not include_non_authoritative,
                limit=max(1, int(limit or 1000)),
            )
        )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        payload = dict(row or {})
        key = (
            _normalize_text(payload.get("target_company")).lower(),
            _normalize_text(payload.get("snapshot_id")),
            _normalize_text(payload.get("asset_view")) or asset_view,
        )
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def _backfill_population_coverage_row(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    row: dict[str, Any],
    dry_run: bool,
    force: bool,
) -> dict[str, Any]:
    target_company = _normalize_text(row.get("target_company"))
    snapshot_id = _normalize_text(row.get("snapshot_id"))
    asset_view = _normalize_text(row.get("asset_view")) or "canonical_merged"
    existing = _existing_population_coverage(row)
    if existing and not force:
        return _row_result(row, status="skipped_existing_population_coverage", population_coverage=existing)

    selected_snapshot_ids = _selected_snapshot_ids(row)
    shard_rows = [
        shard
        for shard in store.list_acquisition_shard_registry(
            target_company=target_company,
            snapshot_ids=selected_snapshot_ids or [snapshot_id],
            statuses=sorted(_REUSABLE_SHARD_STATUSES),
            limit=max(1000, len(selected_snapshot_ids or [snapshot_id]) * 100),
        )
        if _normalize_text(shard.get("status")).lower() in _REUSABLE_SHARD_STATUSES
    ]
    ledger_summary = load_cached_organization_completeness_ledger(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
    )
    contract = build_population_coverage_contract(
        registry_row=row,
        ledger_summary=ledger_summary,
        shard_rows=shard_rows,
        allow_legacy_inference=True,
    )
    coverage_payload = _coverage_payload_from_contract(contract)
    if coverage_payload["coverage_kind"] == "unknown" and not force:
        return _row_result(row, status="skipped_missing_population_coverage_proof", population_coverage=coverage_payload)

    updated_row = _merge_population_coverage(row, coverage_payload)
    if dry_run:
        return _row_result(row, status="would_update", population_coverage=coverage_payload)
    persisted = store.upsert_organization_asset_registry(
        updated_row,
        authoritative=bool(row.get("authoritative")),
    )
    return _row_result(persisted or updated_row, status="updated", population_coverage=coverage_payload)


def _coverage_payload_from_contract(contract: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "contract_version": 1,
        "coverage_kind": _normalize_text(contract.get("coverage_kind")) or "unknown",
        "coverage_status": _normalize_text(contract.get("coverage_status")) or "unverified",
        "coverage_scope": _normalize_text(contract.get("coverage_scope")),
        "full_company_coverage_proven": bool(contract.get("full_company_coverage_proven")),
        "exact_scoped_coverage_available": bool(contract.get("exact_scoped_coverage_available")),
        "scoped_shard_only": bool(contract.get("scoped_shard_only")),
        "directional_scope_reuse_allowed": bool(contract.get("directional_scope_reuse_allowed")),
        "proof_source": _normalize_text(contract.get("proof_source")),
        "reason_codes": _dedupe_strings(contract.get("reason_codes") or []),
        "selected_snapshot_ids": _dedupe_strings(contract.get("selected_snapshot_ids") or []),
        "company_employee_shard_count": _safe_int(contract.get("company_employee_shard_count")),
        "profile_search_shard_count": _safe_int(contract.get("profile_search_shard_count")),
        "standard_bundle_count": _safe_int(contract.get("standard_bundle_count")),
        "candidate_count": _safe_int(contract.get("candidate_count")),
        "current_lane_effective_candidate_count": _safe_int(
            contract.get("current_lane_effective_candidate_count")
        ),
        "former_lane_effective_candidate_count": _safe_int(
            contract.get("former_lane_effective_candidate_count")
        ),
        "backfill_source": "authoritative_population_coverage_backfill",
        "backfilled_at": datetime.now(timezone.utc).isoformat(),
    }
    if bool(contract.get("aggregate_coverage_proven")):
        payload["aggregate_coverage_contract"] = dict(contract.get("aggregate_coverage_contract") or {})
    return payload


def _merge_population_coverage(row: dict[str, Any], population_coverage: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row or {})
    summary = dict(updated.get("summary") or {})
    selection = dict(updated.get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {})
    selection["population_coverage"] = dict(population_coverage)
    if bool(population_coverage.get("full_company_coverage_proven")):
        selection["full_company_coverage"] = dict(population_coverage)
    summary["population_coverage"] = dict(population_coverage)
    summary["source_snapshot_selection"] = selection
    updated["source_snapshot_selection"] = selection
    updated["summary"] = summary
    return updated


def _existing_population_coverage(row: dict[str, Any]) -> dict[str, Any]:
    summary = dict(dict(row or {}).get("summary") or {})
    selection = dict(dict(row or {}).get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {})
    for value in (
        selection.get("population_coverage"),
        selection.get("full_company_coverage"),
        summary.get("population_coverage"),
        summary.get("full_company_coverage"),
    ):
        if isinstance(value, dict) and value:
            return dict(value)
    return {}


def _selected_snapshot_ids(row: dict[str, Any]) -> list[str]:
    payload = dict(row or {})
    summary = dict(payload.get("summary") or {})
    selection = dict(payload.get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {})
    return _dedupe_strings(
        payload.get("selected_snapshot_ids")
        or selection.get("selected_snapshot_ids")
        or summary.get("selected_snapshot_ids")
        or [payload.get("snapshot_id")]
    )


def _row_result(
    row: dict[str, Any],
    *,
    status: str,
    population_coverage: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "target_company": _normalize_text(row.get("target_company")),
        "company_key": _normalize_text(row.get("company_key")) or resolve_company_alias_key(row.get("target_company")),
        "snapshot_id": _normalize_text(row.get("snapshot_id")),
        "asset_view": _normalize_text(row.get("asset_view")) or "canonical_merged",
        "authoritative": bool(row.get("authoritative")),
        "population_coverage": dict(population_coverage or {}),
    }


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = _normalize_text(value)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized
