from __future__ import annotations

from pathlib import Path
from typing import Any

from .company_registry import resolve_company_alias_key
from .organization_assets import load_cached_organization_completeness_ledger
from .source_snapshot_coverage import (
    source_snapshot_selection_has_coverage_proof,
)
from .storage import ControlPlaneStore

_HARD_LARGE_COMPANY_KEYS = {
    "google",
    "alphabet",
    "googledeepmind",
    "deepmind",
    "meta",
    "facebook",
    "microsoft",
    "amazon",
    "apple",
    "nvidia",
    "bytedance",
    "tiktok",
    "openai",
    "xai",
}

FALLBACK_LARGE_COMPANY_KEYS = set(_HARD_LARGE_COMPANY_KEYS) | {"anthropic"}

_SMALL_ORG_MAX_PAGES = 20
_MEDIUM_ORG_MAX_PAGES = 50
_LARGE_ORG_MAX_PAGES = 100

_SMALL_REUSE_LIMIT = 3000
_MEDIUM_REUSE_LIMIT = 1500
_LARGE_REUSE_LIMIT = 0

_MEDIUM_ORG_CANDIDATE_THRESHOLD = 800
_LARGE_ORG_CANDIDATE_THRESHOLD = 2500
_LARGE_ORG_REUSE_BASELINE_MIN_CANDIDATES = 2500
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
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def _selection_has_coverage_proof(selection: dict[str, Any] | None) -> bool:
    return source_snapshot_selection_has_coverage_proof(selection)


def _standard_bundle_count(*payloads: dict[str, Any]) -> int:
    count = 0
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        standard_bundles = dict(payload.get("standard_bundles") or {})
        count = max(count, _safe_int(standard_bundles.get("bundle_count")))
        summary = dict(payload.get("summary") or {})
        standard_bundles = dict(summary.get("standard_bundles") or {})
        count = max(count, _safe_int(standard_bundles.get("bundle_count")))
    return count


def _large_org_reuse_baseline_has_coverage_contract(
    *,
    baseline_candidate_count: int,
    shard_count: int,
    standard_bundle_count: int,
    source_snapshot_selection: dict[str, Any],
) -> bool:
    if baseline_candidate_count >= _LARGE_ORG_REUSE_BASELINE_MIN_CANDIDATES:
        return True
    if shard_count > 0 or standard_bundle_count > 0:
        return True
    return _selection_has_coverage_proof(source_snapshot_selection)


def fallback_organization_execution_profile(
    *,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    normalized_target_company = _normalize_text(target_company)
    company_key = resolve_company_alias_key(normalized_target_company)
    is_known_large = company_key in _HARD_LARGE_COMPANY_KEYS
    org_scale_band = "large" if is_known_large else "unknown"
    default_acquisition_mode = "scoped_search_roster" if is_known_large else "full_company_roster"
    reason_codes = ["fallback_known_large_company"] if is_known_large else ["fallback_unknown_company"]
    summary = {
        "org_scale_band": org_scale_band,
        "default_acquisition_mode": default_acquisition_mode,
        "prefer_delta_from_baseline": False,
        "baseline_candidate_count": 0,
        "current_lane_effective_candidate_count": 0,
        "former_lane_effective_candidate_count": 0,
        "company_employee_shard_count": 0,
        "current_profile_search_shard_count": 0,
        "former_profile_search_shard_count": 0,
        "company_employee_cap_hit_count": 0,
        "profile_search_cap_hit_count": 0,
    }
    explanation = {
        "source": "fallback",
        "reason_codes": reason_codes,
        "signals": summary,
    }
    return {
        "target_company": normalized_target_company,
        "company_key": company_key,
        "asset_view": _normalize_text(asset_view) or "canonical_merged",
        "source_registry_id": 0,
        "source_snapshot_id": "",
        "source_job_id": "",
        "status": "fallback",
        "org_scale_band": org_scale_band,
        "default_acquisition_mode": default_acquisition_mode,
        "prefer_delta_from_baseline": False,
        "current_lane_default": "live_acquisition",
        "former_lane_default": "live_acquisition",
        "baseline_candidate_count": 0,
        "current_lane_effective_candidate_count": 0,
        "former_lane_effective_candidate_count": 0,
        "completeness_score": 0.0,
        "completeness_band": "low",
        "profile_detail_ratio": 0.0,
        "company_employee_shard_count": 0,
        "current_profile_search_shard_count": 0,
        "former_profile_search_shard_count": 0,
        "company_employee_cap_hit_count": 0,
        "profile_search_cap_hit_count": 0,
        "reason_codes": reason_codes,
        "explanation": explanation,
        "summary": summary,
    }


def build_organization_execution_profile(
    *,
    target_company: str,
    asset_view: str = "canonical_merged",
    registry_row: dict[str, Any] | None = None,
    ledger_summary: dict[str, Any] | None = None,
    shard_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    registry = dict(registry_row or {})
    ledger = dict(ledger_summary or {})
    shard_rows = [dict(item) for item in list(shard_rows or []) if isinstance(item, dict)]
    if not registry and not ledger:
        return fallback_organization_execution_profile(target_company=target_company, asset_view=asset_view)

    normalized_target_company = (
        _normalize_text(registry.get("target_company"))
        or _normalize_text(ledger.get("target_company"))
        or _normalize_text(target_company)
    )
    company_key = (
        _normalize_text(registry.get("company_key"))
        or _normalize_text(ledger.get("company_key"))
        or resolve_company_alias_key(normalized_target_company)
    )
    normalized_asset_view = _normalize_text(asset_view or registry.get("asset_view") or ledger.get("asset_view")) or "canonical_merged"
    lane_coverage = dict(ledger.get("lane_coverage") or {})
    current_effective = dict(
        registry.get("current_lane_coverage")
        or lane_coverage.get("current_effective")
        or {}
    )
    former_effective = dict(
        registry.get("former_lane_coverage")
        or lane_coverage.get("former_effective")
        or {}
    )
    organization_asset = dict(ledger.get("organization_asset") or {})

    baseline_candidate_count = max(
        _safe_int(registry.get("candidate_count")),
        _safe_int(organization_asset.get("candidate_count")),
        _safe_int(current_effective.get("effective_candidate_count")) + _safe_int(former_effective.get("effective_candidate_count")),
    )
    current_lane_effective_candidate_count = _safe_int(
        registry.get("current_lane_effective_candidate_count")
        or current_effective.get("effective_candidate_count")
    )
    former_lane_effective_candidate_count = _safe_int(
        registry.get("former_lane_effective_candidate_count")
        or former_effective.get("effective_candidate_count")
    )
    completeness_score = _safe_float(registry.get("completeness_score") or organization_asset.get("completeness_score"))
    completeness_band = _normalize_text(registry.get("completeness_band") or organization_asset.get("completeness_band")) or "low"
    profile_detail_ratio = _safe_float(organization_asset.get("profile_detail_ratio"))
    if profile_detail_ratio <= 0 and baseline_candidate_count > 0:
        profile_detail_ratio = round(
            _safe_int(registry.get("profile_detail_count") or organization_asset.get("profile_detail_count"))
            / max(baseline_candidate_count, 1),
            6,
        )

    company_employee_rows = [row for row in shard_rows if _normalize_text(row.get("lane")).lower() == "company_employees"]
    current_profile_rows = [
        row
        for row in shard_rows
        if _normalize_text(row.get("lane")).lower() == "profile_search"
        and _normalize_text(row.get("employment_scope")).lower() != "former"
    ]
    former_profile_rows = [
        row
        for row in shard_rows
        if _normalize_text(row.get("lane")).lower() == "profile_search"
        and _normalize_text(row.get("employment_scope")).lower() == "former"
    ]
    company_employee_shard_count = len(company_employee_rows)
    current_profile_search_shard_count = len(current_profile_rows)
    former_profile_search_shard_count = len(former_profile_rows)
    company_employee_cap_hit_count = sum(1 for row in company_employee_rows if bool(row.get("provider_cap_hit")))
    profile_search_cap_hit_count = sum(
        1
        for row in [*current_profile_rows, *former_profile_rows]
        if bool(row.get("provider_cap_hit"))
    )

    current_ready = bool(
        registry.get("current_lane_effective_ready")
        or current_effective.get("effective_ready")
    )
    former_ready = bool(
        registry.get("former_lane_effective_ready")
        or former_effective.get("effective_ready")
    )
    reason_codes: list[str] = []

    size_signal = max(
        baseline_candidate_count,
        current_lane_effective_candidate_count + former_lane_effective_candidate_count,
        max((_safe_int(row.get("estimated_total_count")) for row in shard_rows), default=0),
    )
    effective_baseline_count = max(
        baseline_candidate_count,
        current_lane_effective_candidate_count + former_lane_effective_candidate_count,
    )
    org_scale_band = "unknown"
    is_known_large = company_key in _HARD_LARGE_COMPANY_KEYS
    if is_known_large:
        org_scale_band = "large"
        reason_codes.append("known_large_company_override")
    elif (
        company_employee_cap_hit_count > 0
        or profile_search_cap_hit_count > 0
        or size_signal >= _LARGE_ORG_CANDIDATE_THRESHOLD
    ):
        org_scale_band = "large"
        reason_codes.append("large_scale_signal")
    elif effective_baseline_count >= _MEDIUM_ORG_CANDIDATE_THRESHOLD or size_signal >= _MEDIUM_ORG_CANDIDATE_THRESHOLD:
        org_scale_band = "medium"
        reason_codes.append("medium_scale_signal")
    elif size_signal > 0:
        org_scale_band = "small"
        reason_codes.append("small_scale_signal")

    if org_scale_band == "large":
        default_acquisition_mode = "scoped_search_roster"
        reason_codes.append("default_scoped_for_large_org")
    elif org_scale_band == "medium":
        default_acquisition_mode = "hybrid"
        reason_codes.append("default_hybrid_for_medium_org")
    else:
        default_acquisition_mode = "full_company_roster"
        reason_codes.append("default_full_roster_for_small_org")

    source_snapshot_selection = dict(
        registry.get("source_snapshot_selection")
        or ledger.get("source_snapshot_selection")
        or organization_asset.get("source_snapshot_selection")
        or {}
    )
    standard_bundle_count = _standard_bundle_count(registry, ledger)
    shard_count = company_employee_shard_count + current_profile_search_shard_count + former_profile_search_shard_count
    coverage_baseline_reuse_ready = True
    if org_scale_band == "large":
        coverage_baseline_reuse_ready = _large_org_reuse_baseline_has_coverage_contract(
            baseline_candidate_count=baseline_candidate_count,
            shard_count=shard_count,
            standard_bundle_count=standard_bundle_count,
            source_snapshot_selection=source_snapshot_selection,
        )
        if not coverage_baseline_reuse_ready:
            reason_codes.append("large_org_supplemental_baseline_not_coverage_ready")

    prefer_delta_from_baseline = bool(
        baseline_candidate_count > 0
        and (current_ready or former_ready)
        and completeness_score >= 45.0
        and coverage_baseline_reuse_ready
    )
    if prefer_delta_from_baseline:
        reason_codes.append("baseline_ready_for_delta_reuse")

    current_lane_default = "live_acquisition"
    if current_ready and current_lane_effective_candidate_count > 0 and coverage_baseline_reuse_ready:
        current_lane_default = "reuse_baseline"
        reason_codes.append("current_lane_reusable")
    elif current_ready and current_lane_effective_candidate_count > 0 and default_acquisition_mode in {"scoped_search_roster", "hybrid"}:
        current_lane_default = "keyword_search"
        reason_codes.append("current_lane_supplemental_only")
    elif default_acquisition_mode in {"scoped_search_roster", "hybrid"}:
        current_lane_default = "keyword_search"
        reason_codes.append("current_lane_keyword_search_default")
    elif company_employee_shard_count > 0 or current_profile_search_shard_count > 0:
        current_lane_default = "delta_live_search"
        reason_codes.append("current_lane_partial_baseline")

    former_lane_default = "live_acquisition"
    if former_ready and former_lane_effective_candidate_count > 0 and coverage_baseline_reuse_ready:
        former_lane_default = "reuse_baseline"
        reason_codes.append("former_lane_reusable")
    elif former_ready and former_lane_effective_candidate_count > 0 and default_acquisition_mode in {"scoped_search_roster", "hybrid"}:
        former_lane_default = "keyword_search"
        reason_codes.append("former_lane_supplemental_only")
    elif former_profile_search_shard_count > 0 or default_acquisition_mode in {"scoped_search_roster", "hybrid"}:
        former_lane_default = "keyword_search"
        reason_codes.append("former_lane_keyword_search_default")

    summary = {
        "org_scale_band": org_scale_band,
        "default_acquisition_mode": default_acquisition_mode,
        "prefer_delta_from_baseline": prefer_delta_from_baseline,
        "current_lane_default": current_lane_default,
        "former_lane_default": former_lane_default,
        "baseline_candidate_count": baseline_candidate_count,
        "current_lane_effective_candidate_count": current_lane_effective_candidate_count,
        "former_lane_effective_candidate_count": former_lane_effective_candidate_count,
        "completeness_score": completeness_score,
        "profile_detail_ratio": round(profile_detail_ratio, 6),
        "company_employee_shard_count": company_employee_shard_count,
        "current_profile_search_shard_count": current_profile_search_shard_count,
        "former_profile_search_shard_count": former_profile_search_shard_count,
        "standard_bundle_count": standard_bundle_count,
        "coverage_baseline_reuse_ready": coverage_baseline_reuse_ready,
        "company_employee_cap_hit_count": company_employee_cap_hit_count,
        "profile_search_cap_hit_count": profile_search_cap_hit_count,
    }
    explanation = {
        "source": "organization_asset_registry",
        "reason_codes": _dedupe_strings(reason_codes),
        "signals": {
            **summary,
            "current_lane_effective_ready": current_ready,
            "former_lane_effective_ready": former_ready,
            "source_registry_id": _safe_int(registry.get("registry_id")),
            "source_snapshot_id": _normalize_text(registry.get("snapshot_id") or ledger.get("snapshot_id")),
            "selected_snapshot_ids": _dedupe_strings(
                registry.get("selected_snapshot_ids")
                or ledger.get("selected_snapshot_ids")
                or [registry.get("snapshot_id") or ledger.get("snapshot_id")]
            ),
            "source_generation_key": _normalize_text(
                registry.get("materialization_generation_key")
                or organization_asset.get("materialization_generation_key")
            ),
            "source_generation_watermark": _normalize_text(
                registry.get("materialization_watermark")
                or organization_asset.get("materialization_watermark")
            ),
        },
    }
    return {
        "target_company": normalized_target_company,
        "company_key": company_key,
        "asset_view": normalized_asset_view,
        "source_registry_id": _safe_int(registry.get("registry_id")),
        "source_snapshot_id": _normalize_text(registry.get("snapshot_id") or ledger.get("snapshot_id")),
        "source_job_id": _normalize_text(registry.get("source_job_id")),
        "source_generation_key": _normalize_text(
            registry.get("materialization_generation_key")
            or organization_asset.get("materialization_generation_key")
        ),
        "source_generation_sequence": _safe_int(
            registry.get("materialization_generation_sequence")
            or organization_asset.get("materialization_generation_sequence")
        ),
        "source_generation_watermark": _normalize_text(
            registry.get("materialization_watermark")
            or organization_asset.get("materialization_watermark")
        ),
        "status": "ready" if baseline_candidate_count > 0 else "fallback",
        "org_scale_band": org_scale_band,
        "default_acquisition_mode": default_acquisition_mode,
        "prefer_delta_from_baseline": prefer_delta_from_baseline,
        "current_lane_default": current_lane_default,
        "former_lane_default": former_lane_default,
        "baseline_candidate_count": baseline_candidate_count,
        "current_lane_effective_candidate_count": current_lane_effective_candidate_count,
        "former_lane_effective_candidate_count": former_lane_effective_candidate_count,
        "completeness_score": completeness_score,
        "completeness_band": completeness_band,
        "profile_detail_ratio": round(profile_detail_ratio, 6),
        "company_employee_shard_count": company_employee_shard_count,
        "current_profile_search_shard_count": current_profile_search_shard_count,
        "former_profile_search_shard_count": former_profile_search_shard_count,
        "company_employee_cap_hit_count": company_employee_cap_hit_count,
        "profile_search_cap_hit_count": profile_search_cap_hit_count,
        "reason_codes": _dedupe_strings(reason_codes),
        "explanation": explanation,
        "summary": summary,
    }


def _select_best_organization_asset_registry_row(
    *,
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str,
) -> dict[str, Any]:
    from .asset_reuse_planning import build_organization_asset_registry_candidate_inventory

    inventory = build_organization_asset_registry_candidate_inventory(
        store=store,
        target_company=target_company,
        asset_view=asset_view,
        limit=5000,
    )
    selected_row = dict(inventory.get("selected_row") or {})
    return dict(selected_row or {})


def ensure_organization_execution_profile(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    normalized_target_company = _normalize_text(target_company)
    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    if not normalized_target_company:
        return {}
    registry_row = _select_best_organization_asset_registry_row(
        store=store,
        target_company=normalized_target_company,
        asset_view=normalized_asset_view,
    )
    if not registry_row:
        existing = store.get_organization_execution_profile(
            target_company=normalized_target_company,
            asset_view=normalized_asset_view,
        )
        if existing:
            return existing
        fallback = fallback_organization_execution_profile(
            target_company=normalized_target_company,
            asset_view=normalized_asset_view,
        )
        return store.upsert_organization_execution_profile(fallback)
    if not bool(registry_row.get("authoritative")):
        registry_row = store.upsert_organization_asset_registry(registry_row, authoritative=True)
    source_snapshot_id = _normalize_text(registry_row.get("snapshot_id"))
    selected_snapshot_ids = _dedupe_strings(
        registry_row.get("selected_snapshot_ids") or [source_snapshot_id]
    )
    shard_rows = store.list_acquisition_shard_registry(
        target_company=normalized_target_company,
        snapshot_ids=selected_snapshot_ids or [source_snapshot_id],
        statuses=["completed", "completed_with_cap", "skipped_high_overlap"],
        limit=5000,
    )
    ledger_summary = load_cached_organization_completeness_ledger(
        runtime_dir=runtime_dir,
        target_company=normalized_target_company,
        snapshot_id=source_snapshot_id,
        asset_view=normalized_asset_view,
    )
    built = build_organization_execution_profile(
        target_company=normalized_target_company,
        asset_view=normalized_asset_view,
        registry_row=registry_row,
        ledger_summary=ledger_summary,
        shard_rows=shard_rows,
    )
    return store.upsert_organization_execution_profile(built)


def warmup_existing_organization_execution_profiles(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    asset_view: str = "canonical_merged",
    max_companies: int = 0,
) -> dict[str, Any]:
    rows = store.list_organization_asset_registry(
        asset_view=_normalize_text(asset_view) or "canonical_merged",
        limit=5000,
    )
    companies = _dedupe_strings([row.get("target_company") for row in rows if isinstance(row, dict)])
    if max_companies > 0:
        companies = companies[: max(1, int(max_companies))]
    results: list[dict[str, Any]] = []
    for company in companies:
        profile = ensure_organization_execution_profile(
            runtime_dir=runtime_dir,
            store=store,
            target_company=company,
            asset_view=asset_view,
        )
        results.append(
            {
                "target_company": company,
                "status": _normalize_text(profile.get("status")) or "unknown",
                "org_scale_band": _normalize_text(profile.get("org_scale_band")) or "unknown",
                "default_acquisition_mode": _normalize_text(profile.get("default_acquisition_mode")) or "",
                "source_snapshot_id": _normalize_text(profile.get("source_snapshot_id")),
            }
        )
    return {
        "status": "warmed",
        "asset_view": _normalize_text(asset_view) or "canonical_merged",
        "company_count": len(results),
        "results": results,
    }


def organization_execution_profile_full_roster_max_pages(
    profile: dict[str, Any] | None,
    *,
    default_small: int = _SMALL_ORG_MAX_PAGES,
    default_medium: int = _MEDIUM_ORG_MAX_PAGES,
    default_large: int = _LARGE_ORG_MAX_PAGES,
) -> int:
    payload = dict(profile or {})
    scale_band = _normalize_text(payload.get("org_scale_band")).lower()
    default_mode = _normalize_text(payload.get("default_acquisition_mode")).lower()
    if scale_band == "large" or default_mode == "scoped_search_roster":
        return default_large
    if scale_band == "medium" or default_mode == "hybrid":
        return default_medium
    return default_small


def organization_execution_profile_snapshot_reuse_limit(profile: dict[str, Any] | None) -> int:
    payload = dict(profile or {})
    scale_band = _normalize_text(payload.get("org_scale_band")).lower()
    default_mode = _normalize_text(payload.get("default_acquisition_mode")).lower()
    if scale_band == "large" or default_mode == "scoped_search_roster":
        return _LARGE_REUSE_LIMIT
    if scale_band == "medium" or default_mode == "hybrid":
        return _MEDIUM_REUSE_LIMIT
    return _SMALL_REUSE_LIMIT
