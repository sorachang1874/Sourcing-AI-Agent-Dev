from __future__ import annotations

from typing import Any

from .source_snapshot_coverage import promoted_aggregate_coverage_contract

_FULL_COMPANY_COVERAGE_KINDS = {
    "full_company",
    "full_company_roster",
    "company_roster",
    "complete_company_roster",
    "authoritative_full_company_roster",
}
_SCOPED_COVERAGE_KINDS = {
    "scoped_search",
    "scoped_asset",
    "query_scoped",
    "team_scoped",
    "family_scoped",
    "partial_roster",
}
_PROVEN_COVERAGE_STATUSES = {"complete", "completed", "ready", "verified", "canonical", "promoted"}


def build_population_coverage_contract(
    *,
    registry_row: dict[str, Any] | None,
    ledger_summary: dict[str, Any] | None = None,
    shard_rows: list[dict[str, Any]] | None = None,
    allow_legacy_inference: bool = False,
) -> dict[str, Any]:
    """Normalize population coverage proof for asset-reuse planning.

    `organization_asset_registry.authoritative` is a serving pointer. It is not,
    by itself, proof that a snapshot covers a complete company roster. This
    contract separates full-company proof from exact scoped shard coverage.
    """

    registry = dict(registry_row or {})
    ledger = dict(ledger_summary or {})
    summary = dict(registry.get("summary") or {})
    organization_asset = dict(ledger.get("organization_asset") or {})
    selection = dict(
        registry.get("source_snapshot_selection")
        or summary.get("source_snapshot_selection")
        or ledger.get("source_snapshot_selection")
        or organization_asset.get("source_snapshot_selection")
        or {}
    )
    explicit = _first_dict(
        selection.get("population_coverage"),
        selection.get("full_company_coverage"),
        selection.get("coverage"),
        summary.get("population_coverage"),
        summary.get("full_company_coverage"),
        ledger.get("population_coverage"),
        organization_asset.get("population_coverage"),
    )
    explicit_kind = _normalize_text(
        explicit.get("coverage_kind") or explicit.get("kind") or explicit.get("scope_kind") or explicit.get("type")
    ).lower()
    explicit_status = _normalize_text(
        explicit.get("coverage_status") or explicit.get("status") or explicit.get("state")
    ).lower()
    explicit_scope = _normalize_text(
        explicit.get("coverage_scope") or explicit.get("scope") or explicit.get("scope_signature")
    )
    directional_scope_reuse_allowed = bool(
        explicit.get("directional_scope_reuse_allowed")
        or explicit.get("directional_local_reuse_enabled")
        or summary.get("directional_local_reuse_enabled")
        or registry.get("directional_local_reuse_enabled")
    )
    explicit_full_proof = bool(
        explicit
        and (
            bool(explicit.get("full_company_coverage_proven"))
            or bool(explicit.get("complete_company_roster"))
            or bool(explicit.get("coverage_proven"))
            or (
                explicit_kind in _FULL_COMPANY_COVERAGE_KINDS
                and (not explicit_status or explicit_status in _PROVEN_COVERAGE_STATUSES)
            )
        )
    )
    explicit_scoped = bool(explicit_kind in _SCOPED_COVERAGE_KINDS and not explicit_full_proof)

    normalized_shards = [dict(row) for row in list(shard_rows or []) if isinstance(row, dict)]
    company_employee_shard_count = sum(
        1 for row in normalized_shards if _normalize_text(row.get("lane")).lower() == "company_employees"
    )
    profile_search_shard_count = sum(
        1 for row in normalized_shards if _normalize_text(row.get("lane")).lower() == "profile_search"
    )
    standard_bundle_count = _standard_bundle_count(registry, summary, ledger, organization_asset)
    summary_company_employee_lane_count = _summary_company_employee_lane_count(registry, summary, ledger, organization_asset)
    aggregate_contract = promoted_aggregate_coverage_contract(selection)
    aggregate_proven = bool(aggregate_contract.get("coverage_proven"))
    candidate_count = max(_safe_int(registry.get("candidate_count")), _safe_int(organization_asset.get("candidate_count")))
    current_count = max(
        _safe_int(registry.get("current_lane_effective_candidate_count")),
        _safe_int(dict(registry.get("current_lane_coverage") or {}).get("effective_candidate_count")),
    )
    former_count = max(
        _safe_int(registry.get("former_lane_effective_candidate_count")),
        _safe_int(dict(registry.get("former_lane_coverage") or {}).get("effective_candidate_count")),
    )
    lane_total = current_count + former_count
    high_volume_lane_coverage = bool(
        candidate_count >= 2500
        and lane_total > 0
        and (lane_total >= int(round(candidate_count * 0.95)) or lane_total >= max(1, candidate_count - max(10, int(round(candidate_count * 0.02)))))
    )

    reason_codes: list[str] = []
    proof_source = ""
    full_company_coverage_proven = False
    if explicit_full_proof:
        full_company_coverage_proven = True
        proof_source = "explicit_population_coverage"
        reason_codes.append("explicit_full_company_coverage")
    elif aggregate_proven and not explicit_scoped:
        full_company_coverage_proven = True
        proof_source = "promoted_aggregate_coverage"
        reason_codes.append("promoted_aggregate_coverage")
    elif allow_legacy_inference and standard_bundle_count > 0 and not explicit_scoped:
        full_company_coverage_proven = True
        proof_source = "legacy_standard_bundle"
        reason_codes.append("legacy_standard_bundle_full_company_proof")
    elif allow_legacy_inference and high_volume_lane_coverage and not explicit_scoped:
        full_company_coverage_proven = True
        proof_source = "legacy_high_volume_lane_coverage"
        reason_codes.append("legacy_high_volume_lane_coverage_full_company_proof")
    elif allow_legacy_inference and (company_employee_shard_count > 0 or summary_company_employee_lane_count > 0) and not explicit_scoped:
        full_company_coverage_proven = True
        proof_source = "company_employee_shard_registry" if company_employee_shard_count > 0 else "summary_company_employee_lane_coverage"
        reason_codes.append(
            "company_employee_shard_full_company_proof"
            if company_employee_shard_count > 0
            else "summary_company_employee_lane_full_company_proof"
        )
    elif not allow_legacy_inference and not explicit_scoped and (
        standard_bundle_count > 0
        or high_volume_lane_coverage
        or company_employee_shard_count > 0
        or summary_company_employee_lane_count > 0
    ):
        reason_codes.append("legacy_population_coverage_inference_suppressed")

    exact_scoped_coverage_available = bool(profile_search_shard_count > 0 or company_employee_shard_count > 0)
    if explicit_scoped:
        coverage_kind = explicit_kind
        coverage_status = explicit_status or "partial"
        reason_codes.append("explicit_scoped_population_coverage")
    elif full_company_coverage_proven:
        coverage_kind = "full_company_roster"
        coverage_status = "complete"
    elif exact_scoped_coverage_available:
        coverage_kind = "scoped_search"
        coverage_status = "partial"
        proof_source = proof_source or "acquisition_shard_registry"
        reason_codes.append("scoped_shard_coverage_only")
    else:
        coverage_kind = "unknown"
        coverage_status = "unverified"
        reason_codes.append("missing_population_coverage_proof")

    selected_snapshot_ids = _dedupe_strings(
        registry.get("selected_snapshot_ids")
        or summary.get("selected_snapshot_ids")
        or selection.get("selected_snapshot_ids")
        or [registry.get("snapshot_id")]
    )
    return {
        "contract_version": 1,
        "coverage_kind": coverage_kind,
        "coverage_status": coverage_status,
        "coverage_scope": explicit_scope,
        "full_company_coverage_proven": bool(full_company_coverage_proven),
        "exact_scoped_coverage_available": bool(exact_scoped_coverage_available),
        "scoped_shard_only": bool(exact_scoped_coverage_available and not full_company_coverage_proven),
        "directional_scope_reuse_allowed": directional_scope_reuse_allowed,
        "proof_source": proof_source,
        "reason_codes": _dedupe_strings(reason_codes),
        "selected_snapshot_ids": selected_snapshot_ids,
        "company_employee_shard_count": company_employee_shard_count,
        "summary_company_employee_lane_count": summary_company_employee_lane_count,
        "profile_search_shard_count": profile_search_shard_count,
        "standard_bundle_count": standard_bundle_count,
        "candidate_count": candidate_count,
        "current_lane_effective_candidate_count": current_count,
        "former_lane_effective_candidate_count": former_count,
        "high_volume_lane_coverage": high_volume_lane_coverage,
        "legacy_inference_allowed": bool(allow_legacy_inference),
        "legacy_inference_suppressed": bool(
            not allow_legacy_inference
            and not full_company_coverage_proven
            and not explicit_scoped
            and (
                standard_bundle_count > 0
                or high_volume_lane_coverage
                or company_employee_shard_count > 0
                or summary_company_employee_lane_count > 0
            )
        ),
        "aggregate_coverage_proven": aggregate_proven,
        "aggregate_coverage_contract": aggregate_contract,
    }


def _first_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict):
            return dict(value)
    return {}


def _standard_bundle_count(*payloads: dict[str, Any]) -> int:
    count = 0
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        standard_bundles = dict(payload.get("standard_bundles") or {})
        count = max(count, _safe_int(standard_bundles.get("bundle_count")))
        nested_summary = dict(payload.get("summary") or {})
        nested_bundles = dict(nested_summary.get("standard_bundles") or {})
        count = max(count, _safe_int(nested_bundles.get("bundle_count")))
    return count


def _summary_company_employee_lane_count(*payloads: dict[str, Any]) -> int:
    count = 0
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        lane_coverage = dict(payload.get("lane_coverage") or {})
        company_employee_current = dict(lane_coverage.get("company_employees_current") or {})
        company_employee_all = dict(lane_coverage.get("company_employees") or {})
        for lane_payload in (company_employee_current, company_employee_all):
            if not lane_payload:
                continue
            row_count = _safe_int(lane_payload.get("row_count"))
            effective_count = _safe_int(lane_payload.get("effective_candidate_count"))
            result_count = _safe_int(lane_payload.get("result_count"))
            if row_count > 0 and max(effective_count, result_count) > 0:
                count += row_count
    return count


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
    for item in list(values or []):
        text = _normalize_text(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized
