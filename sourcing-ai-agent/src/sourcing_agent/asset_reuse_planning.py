from __future__ import annotations

from hashlib import sha1
import json
from pathlib import Path
import re
from typing import Any

from .candidate_artifacts import CandidateArtifactError, _load_company_snapshot_identity, _resolve_company_snapshot
from .company_registry import normalize_company_key
from .company_shard_planning import merge_company_filters, normalize_company_employee_shard_policy, normalize_company_filters
from .domain import AcquisitionTask, JobRequest, SourcingPlan
from .organization_assets import ensure_organization_completeness_ledger, load_company_snapshot_registry_summary
from .query_signal_knowledge import canonicalize_scope_signal_label, match_scope_signals
from .storage import SQLiteStore


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


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_query_text(value: Any) -> str:
    text = _normalize_text(value)
    if text == "__past_company_only__":
        return ""
    return text


_QUERY_FAMILY_CANONICAL_ALIASES = {
    "post train": "Post-train",
    "post-training": "Post-train",
    "pre train": "Pre-train",
    "pre-training": "Pre-train",
    "chain of thought": "Chain-of-thought",
    "chain-of-thought": "Chain-of-thought",
    "inference time compute": "Inference-time compute",
    "inference-time compute": "Inference-time compute",
    "reasoning model": "Reasoning",
    "reasoning models": "Reasoning",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "multimodality": "Multimodal",
    "video-generation": "Video generation",
    "nano-banana": "Nano Banana",
    "nanobanana": "Nano Banana",
    "infrastructure": "Infra",
}


def _normalize_query_family_text(value: Any) -> str:
    normalized = _normalize_query_text(value).lower()
    normalized = re.sub(r"[\-_]+", " ", normalized)
    return " ".join(normalized.split()).strip()


def _canonical_query_family_label(value: Any) -> str:
    normalized = _normalize_query_family_text(value)
    if not normalized:
        return ""
    direct_canonical = _QUERY_FAMILY_CANONICAL_ALIASES.get(normalized)
    if direct_canonical:
        return direct_canonical
    canonical_scope = canonicalize_scope_signal_label(normalized)
    if canonical_scope and _query_signature(canonical_scope) != _query_signature(normalized):
        return canonical_scope
    return _normalize_query_text(value)


def _query_family_signatures(value: Any) -> set[str]:
    normalized = _normalize_query_family_text(value)
    if not normalized:
        return set()
    signatures: set[str] = set()
    direct_canonical = _QUERY_FAMILY_CANONICAL_ALIASES.get(normalized)
    if direct_canonical:
        signatures.add(_query_signature(direct_canonical))
    canonical_scope = canonicalize_scope_signal_label(normalized)
    if canonical_scope:
        signatures.add(_query_signature(canonical_scope))
    for spec in match_scope_signals(normalized):
        canonical_label = _normalize_text(spec.get("canonical_label"))
        if canonical_label:
            signatures.add(_query_signature(canonical_label))
    for alias_text, canonical_label in _QUERY_FAMILY_CANONICAL_ALIASES.items():
        if re.search(rf"(?<![0-9a-z]){re.escape(alias_text)}(?![0-9a-z])", normalized):
            signatures.add(_query_signature(canonical_label))
    if signatures:
        return {signature for signature in signatures if signature}
    fallback_signature = _query_signature(normalized)
    return {fallback_signature} if fallback_signature else set()


def _normalize_string_list(values: Any) -> list[str]:
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


def _normalize_scope_filters(company_filters: dict[str, Any] | None) -> dict[str, Any]:
    filters = normalize_company_filters(company_filters or {})
    normalized = {
        "companies": _normalize_string_list(filters.get("companies")),
        "locations": _normalize_string_list(filters.get("locations")),
        "function_ids": _normalize_string_list(filters.get("function_ids")),
        "search_query": _normalize_query_text(filters.get("search_query")),
    }
    return normalized


def _task_intent_metadata_view(metadata: dict[str, Any]) -> dict[str, Any]:
    return dict(metadata.get("intent_view") or {})


def _task_metadata_value(metadata: dict[str, Any], key: str, default: Any = None) -> Any:
    intent_view = _task_intent_metadata_view(metadata)
    if key in intent_view:
        value = intent_view.get(key)
    else:
        value = metadata.get(key, default)
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return [
            dict(item) if isinstance(item, dict) else item
            for item in value
        ]
    if value is None:
        return default
    return value


def _json_signature(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:24]


def _preferred_company_display_name(
    *,
    requested_company: str,
    resolved_company: Any,
) -> str:
    requested_text = _normalize_text(requested_company)
    resolved_text = _normalize_text(resolved_company)
    if not resolved_text:
        return requested_text
    if requested_text and normalize_company_key(requested_text) == normalize_company_key(resolved_text):
        return requested_text
    return resolved_text or requested_text


def _normalize_lane_coverage_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    coverage = dict(payload or {})
    normalized = {
        "effective_candidate_count": _safe_int(coverage.get("effective_candidate_count")),
        "effective_ready": bool(coverage.get("effective_ready")),
    }
    for key, value in coverage.items():
        if key in normalized:
            continue
        normalized[key] = value
    return normalized


def _resolve_registry_lane_coverages(
    summary: dict[str, Any],
    *,
    current_lane_coverage: dict[str, Any] | None = None,
    former_lane_coverage: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if current_lane_coverage is None:
        current_lane_coverage = dict(summary.get("current_lane_coverage") or {})
    if former_lane_coverage is None:
        former_lane_coverage = dict(summary.get("former_lane_coverage") or {})
    lane_coverage = dict(summary.get("lane_coverage") or {})
    if not current_lane_coverage:
        current_lane_coverage = dict(lane_coverage.get("current_effective") or {})
    if not former_lane_coverage:
        former_lane_coverage = dict(lane_coverage.get("former_effective") or lane_coverage.get("profile_search_former") or {})
    return (
        _normalize_lane_coverage_payload(current_lane_coverage),
        _normalize_lane_coverage_payload(former_lane_coverage),
    )


def _requested_lane_requirements(request: JobRequest) -> dict[str, Any]:
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


def build_asset_reuse_baseline_selection_explanation(
    *,
    request: JobRequest,
    baseline: dict[str, Any],
    asset_reuse_plan: dict[str, Any],
    current_lane_coverage: dict[str, Any] | None = None,
    former_lane_coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline = dict(baseline or {})
    asset_reuse_plan = dict(asset_reuse_plan or {})
    current_lane_coverage = _normalize_lane_coverage_payload(
        current_lane_coverage
        if current_lane_coverage is not None
        else (
            baseline.get("current_lane_coverage")
            or {"effective_candidate_count": asset_reuse_plan.get("baseline_current_effective_candidate_count")}
        )
    )
    former_lane_coverage = _normalize_lane_coverage_payload(
        former_lane_coverage
        if former_lane_coverage is not None
        else (
            baseline.get("former_lane_coverage")
            or {"effective_candidate_count": asset_reuse_plan.get("baseline_former_effective_candidate_count")}
        )
    )
    lane_requirements = _requested_lane_requirements(request)
    current_ready = bool(
        asset_reuse_plan.get("baseline_current_effective_ready")
        if "baseline_current_effective_ready" in asset_reuse_plan
        else current_lane_coverage.get("effective_ready")
    )
    former_ready = bool(
        asset_reuse_plan.get("baseline_former_effective_ready")
        if "baseline_former_effective_ready" in asset_reuse_plan
        else former_lane_coverage.get("effective_ready")
    )
    current_count = _safe_int(
        asset_reuse_plan.get("baseline_current_effective_candidate_count")
        or current_lane_coverage.get("effective_candidate_count")
    )
    former_count = _safe_int(
        asset_reuse_plan.get("baseline_former_effective_candidate_count")
        or former_lane_coverage.get("effective_candidate_count")
    )
    current_delta_required = bool(
        _safe_int(asset_reuse_plan.get("missing_current_company_employee_shard_count")) > 0
        or _safe_int(asset_reuse_plan.get("missing_current_profile_search_query_count")) > 0
    )
    former_delta_required = bool(_safe_int(asset_reuse_plan.get("missing_former_profile_search_query_count")) > 0)
    lane_requirements_ready = (
        (not bool(lane_requirements.get("need_current")) or (current_ready and current_count > 0))
        and (not bool(lane_requirements.get("need_former")) or (former_ready and former_count > 0))
    )
    return {
        "selection_mode": "organization_asset_registry_lane_coverage",
        "reuse_basis": "organization_asset_registry_lane_coverage",
        "target_company": str(request.target_company or baseline.get("target_company") or ""),
        "asset_view": str(asset_reuse_plan.get("baseline_asset_view") or baseline.get("asset_view") or request.asset_view or "canonical_merged"),
        "matched_registry_id": int(baseline.get("registry_id") or 0),
        "matched_registry_snapshot_id": str(
            asset_reuse_plan.get("baseline_snapshot_id") or baseline.get("snapshot_id") or ""
        ),
        "selected_snapshot_ids": [
            str(item).strip()
            for item in list(asset_reuse_plan.get("selected_snapshot_ids") or baseline.get("selected_snapshot_ids") or [])
            if str(item).strip()
        ],
        "registry_authoritative": bool(baseline.get("authoritative")),
        "baseline_reuse_available": bool(asset_reuse_plan.get("baseline_reuse_available")),
        "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
        "requires_delta_acquisition": bool(asset_reuse_plan.get("requires_delta_acquisition")),
        "baseline_readiness": str(asset_reuse_plan.get("baseline_readiness") or ""),
        "baseline_sufficiency": str(asset_reuse_plan.get("baseline_sufficiency") or ""),
        "baseline_candidate_count": _safe_int(
            asset_reuse_plan.get("baseline_candidate_count") or baseline.get("candidate_count")
        ),
        "baseline_completeness_score": _safe_float(
            asset_reuse_plan.get("baseline_completeness_score") or baseline.get("completeness_score")
        ),
        "baseline_completeness_band": str(
            asset_reuse_plan.get("baseline_completeness_band") or baseline.get("completeness_band") or ""
        ),
        "baseline_completeness_ledger_path": str(asset_reuse_plan.get("baseline_completeness_ledger_path") or ""),
        "baseline_former_embedded_sufficient": bool(asset_reuse_plan.get("baseline_former_embedded_sufficient")),
        "requested_employment_statuses": list(lane_requirements.get("employment_statuses") or []),
        "requested_need_current_lane": bool(lane_requirements.get("need_current")),
        "requested_need_former_lane": bool(lane_requirements.get("need_former")),
        "lane_requirements_ready": bool(lane_requirements_ready),
        "current_lane_effective_ready": bool(current_ready),
        "former_lane_effective_ready": bool(former_ready),
        "current_lane_effective_candidate_count": int(current_count),
        "former_lane_effective_candidate_count": int(former_count),
        "current_lane_delta_required": bool(current_delta_required and lane_requirements.get("need_current")),
        "former_lane_delta_required": bool(former_delta_required and lane_requirements.get("need_former")),
        "covered_current_company_employee_shard_count": _safe_int(
            asset_reuse_plan.get("covered_current_company_employee_shard_count")
        ),
        "missing_current_company_employee_shard_count": _safe_int(
            asset_reuse_plan.get("missing_current_company_employee_shard_count")
        ),
        "covered_current_profile_search_query_count": _safe_int(
            asset_reuse_plan.get("covered_current_profile_search_query_count")
        ),
        "missing_current_profile_search_query_count": _safe_int(
            asset_reuse_plan.get("missing_current_profile_search_query_count")
        ),
        "covered_former_profile_search_query_count": _safe_int(
            asset_reuse_plan.get("covered_former_profile_search_query_count")
        ),
        "missing_former_profile_search_query_count": _safe_int(
            asset_reuse_plan.get("missing_former_profile_search_query_count")
        ),
        "current_lane_coverage": current_lane_coverage,
        "former_lane_coverage": former_lane_coverage,
    }


def _organization_asset_registry_effective_lane_total(payload: dict[str, Any]) -> int:
    return _safe_int(payload.get("current_lane_effective_candidate_count")) + _safe_int(
        payload.get("former_lane_effective_candidate_count")
    )


def build_organization_asset_registry_record(
    *,
    target_company: str,
    company_key: str,
    snapshot_id: str,
    asset_view: str,
    summary: dict[str, Any],
    current_lane_coverage: dict[str, Any] | None = None,
    former_lane_coverage: dict[str, Any] | None = None,
    source_path: str = "",
    source_job_id: str = "",
    authoritative: bool = True,
) -> dict[str, Any]:
    candidate_count = max(0, _safe_int(summary.get("candidate_count")))
    profile_detail_count = max(0, _safe_int(summary.get("profile_detail_count")))
    explicit_profile_capture_count = max(0, _safe_int(summary.get("explicit_profile_capture_count")))
    missing_linkedin_count = max(0, _safe_int(summary.get("missing_linkedin_count")))
    manual_review_backlog_count = max(0, _safe_int(summary.get("manual_review_backlog_count")))
    profile_completion_backlog_count = max(0, _safe_int(summary.get("profile_completion_backlog_count")))
    source_snapshot_count = max(0, _safe_int(summary.get("source_snapshot_count")))
    evidence_count = max(0, _safe_int(summary.get("evidence_count")))
    effective_candidate_denominator = max(candidate_count, 1)
    profile_coverage_ratio = min(
        1.0,
        (
            profile_detail_count
            + min(explicit_profile_capture_count, candidate_count) * 0.35
        ) / effective_candidate_denominator,
    )
    missing_ratio = missing_linkedin_count / effective_candidate_denominator
    profile_gap_ratio = profile_completion_backlog_count / effective_candidate_denominator
    manual_ratio = manual_review_backlog_count / effective_candidate_denominator
    completeness_score = max(
        0.0,
        min(
            100.0,
            round(
                35.0
                + profile_coverage_ratio * 45.0
                - missing_ratio * 18.0
                - profile_gap_ratio * 10.0
                - manual_ratio * 5.0
                + min(8.0, source_snapshot_count * 1.5),
                2,
            ),
        ),
    )
    if completeness_score >= 75:
        completeness_band = "high"
    elif completeness_score >= 50:
        completeness_band = "medium"
    else:
        completeness_band = "low"
    selection = dict(summary.get("source_snapshot_selection") or {})
    selected_snapshot_ids = _normalize_string_list(
        selection.get("selected_snapshot_ids") or summary.get("selected_snapshot_ids") or [snapshot_id]
    )
    resolved_current_lane_coverage, resolved_former_lane_coverage = _resolve_registry_lane_coverages(
        summary,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return {
        "target_company": _normalize_text(target_company),
        "company_key": _normalize_text(company_key) or normalize_company_key(target_company),
        "snapshot_id": _normalize_text(snapshot_id),
        "asset_view": _normalize_text(asset_view) or "canonical_merged",
        "status": "ready" if candidate_count > 0 else "empty",
        "authoritative": bool(authoritative),
        "candidate_count": candidate_count,
        "evidence_count": evidence_count,
        "profile_detail_count": profile_detail_count,
        "explicit_profile_capture_count": explicit_profile_capture_count,
        "missing_linkedin_count": missing_linkedin_count,
        "manual_review_backlog_count": manual_review_backlog_count,
        "profile_completion_backlog_count": profile_completion_backlog_count,
        "source_snapshot_count": source_snapshot_count,
        "completeness_score": completeness_score,
        "completeness_band": completeness_band,
        "current_lane_coverage": resolved_current_lane_coverage,
        "former_lane_coverage": resolved_former_lane_coverage,
        "current_lane_effective_candidate_count": _safe_int(resolved_current_lane_coverage.get("effective_candidate_count")),
        "former_lane_effective_candidate_count": _safe_int(resolved_former_lane_coverage.get("effective_candidate_count")),
        "current_lane_effective_ready": bool(resolved_current_lane_coverage.get("effective_ready")),
        "former_lane_effective_ready": bool(resolved_former_lane_coverage.get("effective_ready")),
        "source_snapshot_selection": selection,
        "selected_snapshot_ids": selected_snapshot_ids or [_normalize_text(snapshot_id)],
        "source_path": _normalize_text(source_path),
        "source_job_id": _normalize_text(source_job_id),
        "summary": dict(summary or {}),
    }


def _organization_asset_summary_path(snapshot_dir: Path, asset_view: str) -> Path:
    return snapshot_dir / "normalized_artifacts" / (
        "artifact_summary.json" if asset_view == "canonical_merged" else f"{asset_view}/artifact_summary.json"
    )


def _organization_registry_selected_snapshot_ids(payload: dict[str, Any]) -> list[str]:
    selection = dict(payload.get("source_snapshot_selection") or {})
    return _normalize_string_list(
        payload.get("selected_snapshot_ids")
        or selection.get("selected_snapshot_ids")
        or []
    )


def _organization_asset_registry_candidate_sort_key(payload: dict[str, Any]) -> tuple[float, int, int, int, int, str]:
    return (
        _safe_float(payload.get("completeness_score")),
        _organization_asset_registry_effective_lane_total(payload),
        _safe_int(payload.get("candidate_count")),
        _safe_int(payload.get("profile_detail_count")),
        _safe_int(payload.get("evidence_count")),
        -_safe_int(payload.get("profile_completion_backlog_count")),
        _normalize_text(payload.get("snapshot_id")),
    )


def evaluate_organization_asset_registry_promotion(
    *,
    existing_authoritative: dict[str, Any] | None,
    candidate_record: dict[str, Any],
) -> dict[str, Any]:
    candidate_snapshot_id = _normalize_text(candidate_record.get("snapshot_id"))
    if not existing_authoritative:
        return {
            "promote": True,
            "reason": "no_existing_authoritative",
            "existing_snapshot_id": "",
            "candidate_snapshot_id": candidate_snapshot_id,
        }

    existing_snapshot_id = _normalize_text(existing_authoritative.get("snapshot_id"))
    if candidate_snapshot_id and candidate_snapshot_id == existing_snapshot_id:
        return {
            "promote": True,
            "reason": "same_snapshot_refresh",
            "existing_snapshot_id": existing_snapshot_id,
            "candidate_snapshot_id": candidate_snapshot_id,
        }

    selected_snapshot_ids = _organization_registry_selected_snapshot_ids(candidate_record)
    if existing_snapshot_id and existing_snapshot_id in selected_snapshot_ids:
        return {
            "promote": True,
            "reason": "explicit_baseline_inclusion",
            "existing_snapshot_id": existing_snapshot_id,
            "candidate_snapshot_id": candidate_snapshot_id,
            "selected_snapshot_ids": selected_snapshot_ids,
        }

    existing_candidate_count = _safe_int(existing_authoritative.get("candidate_count"))
    candidate_candidate_count = _safe_int(candidate_record.get("candidate_count"))
    existing_evidence_count = _safe_int(existing_authoritative.get("evidence_count"))
    candidate_evidence_count = _safe_int(candidate_record.get("evidence_count"))
    existing_profile_detail_count = _safe_int(existing_authoritative.get("profile_detail_count"))
    candidate_profile_detail_count = _safe_int(candidate_record.get("profile_detail_count"))
    existing_missing_linkedin_count = _safe_int(existing_authoritative.get("missing_linkedin_count"))
    candidate_missing_linkedin_count = _safe_int(candidate_record.get("missing_linkedin_count"))
    existing_profile_gap_count = _safe_int(existing_authoritative.get("profile_completion_backlog_count"))
    candidate_profile_gap_count = _safe_int(candidate_record.get("profile_completion_backlog_count"))
    existing_completeness_score = _safe_float(existing_authoritative.get("completeness_score"))
    candidate_completeness_score = _safe_float(candidate_record.get("completeness_score"))
    existing_effective_lane_total = _organization_asset_registry_effective_lane_total(existing_authoritative)
    candidate_effective_lane_total = _organization_asset_registry_effective_lane_total(candidate_record)

    existing_candidate_denominator = max(existing_candidate_count, 1)
    candidate_candidate_denominator = max(candidate_candidate_count, 1)
    existing_missing_ratio = existing_missing_linkedin_count / existing_candidate_denominator
    candidate_missing_ratio = candidate_missing_linkedin_count / candidate_candidate_denominator
    existing_profile_gap_ratio = existing_profile_gap_count / existing_candidate_denominator
    candidate_profile_gap_ratio = candidate_profile_gap_count / candidate_candidate_denominator

    subsumption_higher = (
        candidate_candidate_count >= max(existing_candidate_count, int(existing_candidate_count * 0.98))
        and candidate_profile_detail_count >= max(existing_profile_detail_count, int(existing_profile_detail_count * 0.98))
        and candidate_evidence_count >= int(existing_evidence_count * 0.95)
        and candidate_missing_ratio <= existing_missing_ratio + 0.01
        and candidate_profile_gap_ratio <= existing_profile_gap_ratio + 0.02
        and candidate_effective_lane_total >= max(existing_effective_lane_total, int(existing_effective_lane_total * 0.98))
    )
    completeness_higher = candidate_completeness_score >= existing_completeness_score + 1.0
    promote = bool(completeness_higher and subsumption_higher)
    return {
        "promote": promote,
        "reason": "higher_completeness_and_subsumption" if promote else "guard_rejected",
        "existing_snapshot_id": existing_snapshot_id,
        "candidate_snapshot_id": candidate_snapshot_id,
        "selected_snapshot_ids": selected_snapshot_ids,
        "completeness_higher": completeness_higher,
        "subsumption_higher": subsumption_higher,
        "existing_metrics": {
            "candidate_count": existing_candidate_count,
            "evidence_count": existing_evidence_count,
            "profile_detail_count": existing_profile_detail_count,
            "missing_linkedin_count": existing_missing_linkedin_count,
            "profile_completion_backlog_count": existing_profile_gap_count,
            "completeness_score": existing_completeness_score,
            "effective_lane_total": existing_effective_lane_total,
            "missing_ratio": round(existing_missing_ratio, 6),
            "profile_gap_ratio": round(existing_profile_gap_ratio, 6),
        },
        "candidate_metrics": {
            "candidate_count": candidate_candidate_count,
            "evidence_count": candidate_evidence_count,
            "profile_detail_count": candidate_profile_detail_count,
            "missing_linkedin_count": candidate_missing_linkedin_count,
            "profile_completion_backlog_count": candidate_profile_gap_count,
            "completeness_score": candidate_completeness_score,
            "effective_lane_total": candidate_effective_lane_total,
            "missing_ratio": round(candidate_missing_ratio, 6),
            "profile_gap_ratio": round(candidate_profile_gap_ratio, 6),
        },
    }


def upsert_organization_asset_registry_with_guard(
    *,
    store: SQLiteStore,
    candidate_record: dict[str, Any],
) -> dict[str, Any]:
    target_company = _normalize_text(candidate_record.get("target_company"))
    asset_view = _normalize_text(candidate_record.get("asset_view")) or "canonical_merged"
    existing_authoritative = (
        store.get_authoritative_organization_asset_registry(target_company=target_company, asset_view=asset_view)
        if target_company
        else {}
    )
    decision = evaluate_organization_asset_registry_promotion(
        existing_authoritative=existing_authoritative,
        candidate_record=candidate_record,
    )
    return store.upsert_organization_asset_registry(
        candidate_record,
        authoritative=bool(decision.get("promote")),
    )


def _load_available_organization_asset_registry_records(
    *,
    runtime_dir: str | Path,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> list[dict[str, Any]]:
    try:
        _, snapshot_dir, _ = _resolve_company_snapshot(runtime_dir, target_company)
    except CandidateArtifactError:
        return []
    company_dir = snapshot_dir.parent
    records: list[dict[str, Any]] = []
    for candidate_snapshot_dir in sorted(
        [path for path in company_dir.iterdir() if path.is_dir()],
        key=lambda path: path.name,
        reverse=True,
    ):
        summary_result = load_company_snapshot_registry_summary(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=candidate_snapshot_dir.name,
            asset_view=asset_view,
        )
        summary = dict(summary_result.get("summary") or {})
        if not isinstance(summary, dict) or not summary:
            continue
        identity_payload = dict(summary_result.get("identity_payload") or _load_company_snapshot_identity(candidate_snapshot_dir, fallback_payload={}))
        company_key = str(summary_result.get("company_key") or identity_payload.get("company_key") or company_dir.name).strip() or company_dir.name
        canonical_name = _preferred_company_display_name(
            requested_company=target_company,
            resolved_company=identity_payload.get("canonical_name") or target_company,
        )
        records.append(
            build_organization_asset_registry_record(
                target_company=canonical_name,
                company_key=company_key,
                snapshot_id=candidate_snapshot_dir.name,
                asset_view=asset_view,
                summary=summary,
                source_path=str(summary_result.get("source_path") or ""),
                authoritative=False,
            )
        )
    return records


def backfill_organization_asset_registry_for_company(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    normalized_company_key = normalize_company_key(target_company)
    pre_canonicalization_result = store.canonicalize_organization_asset_registry_target_company(
        target_company=target_company,
        company_key=normalized_company_key,
        asset_view=asset_view,
    )
    candidate_records = _load_available_organization_asset_registry_records(
        runtime_dir=runtime_dir,
        target_company=target_company,
        asset_view=asset_view,
    )
    if not candidate_records:
        return {
            "status": "missing",
            "target_company": target_company,
            "asset_view": asset_view,
            "record_count": 0,
        }

    for record in candidate_records:
        store.upsert_organization_asset_registry(record, authoritative=False)

    post_canonicalization_result = store.canonicalize_organization_asset_registry_target_company(
        target_company=target_company,
        company_key=normalized_company_key,
        asset_view=asset_view,
    )
    canonicalization_result = {
        "pre_existing": pre_canonicalization_result,
        "post_upsert": post_canonicalization_result,
        "deleted_rows": _safe_int(pre_canonicalization_result.get("deleted_rows"))
        + _safe_int(post_canonicalization_result.get("deleted_rows")),
        "merged_groups": _safe_int(pre_canonicalization_result.get("merged_groups"))
        + _safe_int(post_canonicalization_result.get("merged_groups")),
        "updated_rows": _safe_int(pre_canonicalization_result.get("updated_rows"))
        + _safe_int(post_canonicalization_result.get("updated_rows")),
    }

    existing_row = store.get_authoritative_organization_asset_registry(
        target_company=target_company,
        asset_view=asset_view,
    )
    current_authoritative = dict(existing_row or {}) if bool(dict(existing_row or {}).get("authoritative")) else {}

    selected_record = dict(current_authoritative or {})
    selected_reason = "existing_authoritative_retained" if selected_record else ""
    for record in sorted(candidate_records, key=_organization_asset_registry_candidate_sort_key, reverse=True):
        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative=selected_record or None,
            candidate_record=record,
        )
        if not selected_record or bool(decision.get("promote")):
            selected_record = record
            selected_reason = str(decision.get("reason") or "")

    if not selected_record:
        return {
            "status": "missing",
            "target_company": target_company,
            "asset_view": asset_view,
            "record_count": len(candidate_records),
        }

    persisted = store.upsert_organization_asset_registry(selected_record, authoritative=True)
    return {
        "status": "backfilled",
        "target_company": str(persisted.get("target_company") or target_company),
        "asset_view": asset_view,
        "record_count": len(candidate_records),
        "canonicalization": canonicalization_result,
        "selected_snapshot_id": str(persisted.get("snapshot_id") or ""),
        "selected_reason": selected_reason,
        "selected_record": persisted,
    }


def build_acquisition_shard_registry_record(
    *,
    target_company: str,
    company_key: str,
    snapshot_id: str,
    lane: str,
    employment_scope: str,
    strategy_type: str,
    shard_id: str,
    shard_title: str,
    search_query: str,
    company_filters: dict[str, Any] | None,
    result_count: int,
    estimated_total_count: int = 0,
    status: str = "completed",
    provider_cap_hit: bool = False,
    source_path: str = "",
    source_job_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_filters = _normalize_scope_filters(company_filters or {})
    normalized_query = _normalize_query_text(search_query or normalized_filters.get("search_query"))
    signature_payload = {
        "target_company": normalize_company_key(target_company),
        "lane": _normalize_text(lane).lower(),
        "employment_scope": _normalize_text(employment_scope).lower() or "all",
        "companies": [item.lower() for item in normalized_filters.get("companies") or []],
        "locations": [item.lower() for item in normalized_filters.get("locations") or []],
        "function_ids": [item.lower() for item in normalized_filters.get("function_ids") or []],
        "search_query": normalized_query.lower(),
    }
    shard_key = _json_signature(signature_payload)
    return {
        "shard_key": shard_key,
        "target_company": _normalize_text(target_company),
        "company_key": _normalize_text(company_key) or normalize_company_key(target_company),
        "snapshot_id": _normalize_text(snapshot_id),
        "asset_view": "canonical_merged",
        "lane": _normalize_text(lane),
        "status": _normalize_text(status) or "completed",
        "employment_scope": _normalize_text(employment_scope) or "all",
        "strategy_type": _normalize_text(strategy_type),
        "shard_id": _normalize_text(shard_id),
        "shard_title": _normalize_text(shard_title),
        "search_query": normalized_query,
        "query_signature": _json_signature({"search_query": normalized_query.lower()}) if normalized_query else "",
        "company_scope": normalized_filters.get("companies") or [],
        "locations": normalized_filters.get("locations") or [],
        "function_ids": normalized_filters.get("function_ids") or [],
        "result_count": max(0, _safe_int(result_count)),
        "estimated_total_count": max(0, _safe_int(estimated_total_count)),
        "provider_cap_hit": bool(provider_cap_hit),
        "source_path": _normalize_text(source_path),
        "source_job_id": _normalize_text(source_job_id),
        "metadata": dict(metadata or {}),
    }


def build_company_employee_shard_registry_records(
    *,
    target_company: str,
    company_key: str,
    snapshot_id: str,
    shard_summaries: list[dict[str, Any]],
    source_job_id: str = "",
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in list(shard_summaries or []):
        if not isinstance(item, dict):
            continue
        company_filters = dict(item.get("company_filters") or {})
        returned_entry_count = _safe_int(item.get("unique_entry_count") or item.get("returned_entry_count"))
        provider_cap_hit = returned_entry_count >= 2500 or "cap" in _normalize_text(item.get("stop_reason")).lower()
        records.append(
            build_acquisition_shard_registry_record(
                target_company=target_company,
                company_key=company_key,
                snapshot_id=snapshot_id,
                lane="company_employees",
                employment_scope="current",
                strategy_type="full_company_roster",
                shard_id=str(item.get("shard_id") or ""),
                shard_title=str(item.get("title") or item.get("shard_id") or ""),
                search_query=str(company_filters.get("search_query") or ""),
                company_filters=company_filters,
                result_count=returned_entry_count,
                estimated_total_count=_safe_int(item.get("probe_summary", {}).get("estimated_total_count")),
                status="completed",
                provider_cap_hit=provider_cap_hit,
                source_path=str(item.get("summary_path") or ""),
                source_job_id=source_job_id,
                metadata=dict(item),
            )
        )
    return records


def build_search_seed_shard_registry_records(
    *,
    target_company: str,
    company_key: str,
    snapshot_id: str,
    query_summaries: list[dict[str, Any]],
    effective_filter_hints: dict[str, Any] | None,
    employment_scope: str,
    strategy_type: str,
    source_job_id: str = "",
) -> list[dict[str, Any]]:
    records_by_key: dict[tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]], dict[str, Any]] = {}
    filter_hints = dict(effective_filter_hints or {})
    base_company_filters = {
        "companies": list(filter_hints.get("current_companies") or []),
        "past_companies": list(filter_hints.get("past_companies") or []),
        "locations": list(filter_hints.get("locations") or []),
        "function_ids": list(filter_hints.get("function_ids") or []),
    }
    base_scope_companies = list(filter_hints.get("past_companies") or filter_hints.get("current_companies") or [])
    for item in list(query_summaries or []):
        if not isinstance(item, dict):
            continue
        mode = _normalize_text(item.get("mode")).lower()
        if mode not in {"harvest_profile_search", "provider_people_search"}:
            continue
        query_text = _normalize_query_text(item.get("effective_query_text") or item.get("query"))
        company_filters = {
            "companies": base_scope_companies,
            "locations": list(filter_hints.get("locations") or []),
            "function_ids": list(filter_hints.get("function_ids") or []),
            "search_query": query_text,
        }
        if employment_scope == "former":
            company_filters["companies"] = list(filter_hints.get("past_companies") or [])
        record = build_acquisition_shard_registry_record(
            target_company=target_company,
            company_key=company_key,
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope=employment_scope,
            strategy_type=strategy_type,
            shard_id=_normalize_text(item.get("query") or query_text or mode) or mode,
            shard_title=_normalize_text(item.get("query") or query_text or mode) or mode,
            search_query=query_text,
            company_filters=company_filters,
            result_count=_safe_int(item.get("seed_entry_count")),
            estimated_total_count=_safe_int(dict(item.get("probe") or {}).get("estimated_total_count")),
            status=_normalize_text(item.get("status") or "completed") or "completed",
            provider_cap_hit=_safe_int(dict(item.get("pagination") or {}).get("total")) >= 2500,
            source_path=str(item.get("raw_path") or ""),
            source_job_id=source_job_id,
            metadata={
                **base_company_filters,
                **dict(item),
            },
        )
        key = _search_query_registry_match_key(record)
        existing = records_by_key.get(key)
        if existing is None:
            records_by_key[key] = record
            continue
        existing_mode = _normalize_text(dict(existing.get("metadata") or {}).get("mode")).lower()
        mode_priority = {
            "harvest_profile_search": 2,
            "provider_people_search": 1,
        }
        existing_sort_key = (
            _safe_int(existing.get("result_count")),
            _safe_int(existing.get("estimated_total_count")),
            mode_priority.get(existing_mode, 0),
        )
        candidate_sort_key = (
            _safe_int(record.get("result_count")),
            _safe_int(record.get("estimated_total_count")),
            mode_priority.get(mode, 0),
        )
        if candidate_sort_key >= existing_sort_key:
            records_by_key[key] = record
    return list(records_by_key.values())


def ensure_organization_asset_registry(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    backfill_result = backfill_organization_asset_registry_for_company(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        asset_view=asset_view,
    )
    selected_record = dict(backfill_result.get("selected_record") or {})
    if selected_record:
        return selected_record
    existing = store.get_authoritative_organization_asset_registry(target_company=target_company, asset_view=asset_view)
    if existing:
        if bool(existing.get("authoritative")):
            return existing
        promoted = store.upsert_organization_asset_registry(existing, authoritative=True)
        if promoted:
            return promoted
        return existing
    return {}


def ensure_acquisition_shard_registry_for_snapshot(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
) -> dict[str, int]:
    try:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
    except CandidateArtifactError:
        return {"organization_records": 0, "shard_records": 0}

    preferred_target_company = _preferred_company_display_name(
        requested_company=target_company,
        resolved_company=identity_payload.get("canonical_name") or target_company,
    )

    organization_records = 0
    shard_records = 0
    summary_path = snapshot_dir / "normalized_artifacts" / "artifact_summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            summary = {}
        if summary:
            store.upsert_organization_asset_registry(
                build_organization_asset_registry_record(
                    target_company=preferred_target_company,
                    company_key=str(identity_payload.get("company_key") or company_key),
                    snapshot_id=snapshot_dir.name,
                    asset_view="canonical_merged",
                    summary=summary,
                    source_path=str(summary_path),
                    authoritative=False,
                ),
                authoritative=False,
            )
            organization_records += 1

    harvest_summary_path = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json"
    if harvest_summary_path.exists():
        try:
            harvest_summary = json.loads(harvest_summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            harvest_summary = {}
        shard_summaries = list(harvest_summary.get("shards") or harvest_summary.get("shard_summaries") or [])
        if shard_summaries:
            for record in build_company_employee_shard_registry_records(
                target_company=preferred_target_company,
                company_key=str(identity_payload.get("company_key") or company_key),
                snapshot_id=snapshot_dir.name,
                shard_summaries=shard_summaries,
            ):
                store.upsert_acquisition_shard_registry(record)
                shard_records += 1
        elif harvest_summary:
            company_filters = dict(harvest_summary.get("company_filters") or {})
            record = build_acquisition_shard_registry_record(
                target_company=preferred_target_company,
                company_key=str(identity_payload.get("company_key") or company_key),
                snapshot_id=snapshot_dir.name,
                lane="company_employees",
                employment_scope="current",
                strategy_type="full_company_roster",
                shard_id="root",
                shard_title="Root scope",
                search_query=str(company_filters.get("search_query") or ""),
                company_filters=company_filters,
                result_count=_safe_int(harvest_summary.get("visible_entry_count") or harvest_summary.get("raw_entry_count")),
                estimated_total_count=0,
                status="completed",
                source_path=str(harvest_summary_path),
                metadata=harvest_summary,
            )
            store.upsert_acquisition_shard_registry(record)
            shard_records += 1

    search_seed_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
    if search_seed_summary_path.exists():
        try:
            search_seed_summary = json.loads(search_seed_summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            search_seed_summary = {}
        query_summaries = list(search_seed_summary.get("query_summaries") or [])
        effective_filter_hints = dict(search_seed_summary.get("effective_filter_hints") or {})
        strategy_type = "former_employee_search" if list(effective_filter_hints.get("past_companies") or []) else "scoped_search_roster"
        if strategy_type == "former_employee_search":
            employment_scope = "former"
        elif list(effective_filter_hints.get("current_companies") or []):
            employment_scope = "current"
        else:
            employment_scope = "all"
        for record in build_search_seed_shard_registry_records(
            target_company=preferred_target_company,
            company_key=str(identity_payload.get("company_key") or company_key),
            snapshot_id=snapshot_dir.name,
            query_summaries=query_summaries,
            effective_filter_hints=effective_filter_hints,
            employment_scope=employment_scope,
            strategy_type=strategy_type,
        ):
            store.upsert_acquisition_shard_registry(record)
            shard_records += 1

    return {
        "organization_records": organization_records,
        "shard_records": shard_records,
    }


def _company_employee_registry_match_key(record: dict[str, Any]) -> tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    query_key = _query_signature(record.get("search_query"))
    scope_key = _normalize_scope_match_tuple(record.get("company_scope"))
    location_key = _normalize_scope_match_tuple(record.get("locations"))
    function_key = _normalize_scope_match_tuple(record.get("function_ids"))
    return (
        query_key,
        _normalize_text(record.get("employment_scope")).lower(),
        scope_key,
        location_key,
        function_key,
    )


def _search_query_registry_match_key(record: dict[str, Any]) -> tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    query_key = _query_signature(record.get("search_query"))
    scope_key = _normalize_scope_match_tuple(record.get("company_scope"))
    location_key = _normalize_scope_match_tuple(record.get("locations"))
    function_key = _normalize_scope_match_tuple(record.get("function_ids"))
    return (
        query_key,
        _normalize_profile_search_employment_scope(record.get("employment_scope")),
        scope_key,
        location_key,
        function_key,
    )


def _normalize_company_scope_keys(values: Any) -> set[str]:
    keys: set[str] = set()
    for value in _normalize_string_list(values):
        normalized = _normalize_text(value)
        if not normalized:
            continue
        lowered = normalized.lower().strip()
        if "/company/" in lowered:
            slug = lowered.split("/company/", 1)[1].split("/", 1)[0].strip()
            slug_key = normalize_company_key(slug)
            if slug_key:
                keys.add(slug_key)
        value_key = normalize_company_key(normalized)
        if value_key:
            keys.add(value_key)
    return keys


def _scope_filters_cover_requested(*, desired: Any, existing: Any, mode: str) -> bool:
    desired_mode = str(mode or "").strip().lower()
    if desired_mode == "company_scope":
        desired_values = _normalize_company_scope_keys(desired)
        existing_values = _normalize_company_scope_keys(existing)
    else:
        desired_values = set(_normalize_scope_match_tuple(desired))
        existing_values = set(_normalize_scope_match_tuple(existing))
    if not desired_values:
        return desired_mode != "function_ids" or not existing_values
    if not existing_values:
        return True
    return desired_values.issubset(existing_values)


def _query_filters_cover_requested(*, desired_query: Any, existing_query: Any) -> tuple[bool, tuple[int, int]]:
    desired_signature = _query_signature(desired_query)
    existing_signature = _query_signature(existing_query)
    if not desired_signature:
        return (not bool(existing_signature), (1 if not existing_signature else 0, 0))
    if desired_signature == existing_signature:
        return True, (2, 1)
    desired_families = _query_family_signatures(desired_query)
    existing_families = _query_family_signatures(existing_query)
    overlap = desired_families & existing_families
    if overlap:
        return True, (1, len(overlap))
    return False, (0, 0)


def _employment_scopes_compatible(*, desired: Any, existing: Any) -> bool:
    desired_scope = _normalize_profile_search_employment_scope(desired)
    existing_scope = _normalize_profile_search_employment_scope(existing)
    if desired_scope == "current":
        return existing_scope == "current"
    return desired_scope == existing_scope


def _registry_row_match_sort_key(
    *,
    row: dict[str, Any],
    query_score: tuple[int, int],
    preferred_lane: str | None = None,
) -> tuple[int, int, int, int, int, int, int, str]:
    lane = _normalize_text(row.get("lane")).lower()
    return (
        1 if preferred_lane and lane == preferred_lane else 0,
        int(query_score[0]),
        int(query_score[1]),
        len(_normalize_company_scope_keys(row.get("company_scope"))),
        len(_normalize_scope_match_tuple(row.get("function_ids"))),
        _safe_int(row.get("result_count")),
        _safe_int(row.get("estimated_total_count")),
        _normalize_text(row.get("snapshot_id")),
    )


def _find_compatible_registry_row(
    *,
    desired_spec: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    preferred_lane: str | None = None,
) -> dict[str, Any]:
    best_row: dict[str, Any] = {}
    best_sort_key: tuple[int, int, int, int, int, int, int, str] | None = None
    desired_query = desired_spec.get("search_query")
    desired_scope = desired_spec.get("company_scope")
    desired_locations = desired_spec.get("locations")
    desired_function_ids = desired_spec.get("function_ids")
    desired_employment_scope = desired_spec.get("employment_scope")
    for row in list(candidate_rows or []):
        if not _employment_scopes_compatible(
            desired=desired_employment_scope,
            existing=row.get("employment_scope"),
        ):
            continue
        query_compatible, query_score = _query_filters_cover_requested(
            desired_query=desired_query,
            existing_query=row.get("search_query"),
        )
        if not query_compatible:
            continue
        if not _scope_filters_cover_requested(
            desired=desired_scope,
            existing=row.get("company_scope"),
            mode="company_scope",
        ):
            continue
        if not _scope_filters_cover_requested(
            desired=desired_locations,
            existing=row.get("locations"),
            mode="locations",
        ):
            continue
        if not _scope_filters_cover_requested(
            desired=desired_function_ids,
            existing=row.get("function_ids"),
            mode="function_ids",
        ):
            continue
        sort_key = _registry_row_match_sort_key(
            row=row,
            query_score=query_score,
            preferred_lane=preferred_lane,
        )
        if best_sort_key is None or sort_key > best_sort_key:
            best_sort_key = sort_key
            best_row = row
    return best_row


def _planned_company_employee_delta_specs(task: AcquisitionTask) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metadata = dict(task.metadata or {})
    explicit_shards = list(_task_metadata_value(metadata, "company_employee_shards") or [])
    strategy_type = str(_task_metadata_value(metadata, "strategy_type") or "")
    if explicit_shards:
        specs = [
            build_acquisition_shard_registry_record(
                target_company="",
                company_key="",
                snapshot_id="",
                lane="company_employees",
                employment_scope="current",
                strategy_type=strategy_type,
                shard_id=str(item.get("shard_id") or ""),
                shard_title=str(item.get("title") or item.get("shard_id") or ""),
                search_query=str(dict(item.get("company_filters") or {}).get("search_query") or ""),
                company_filters=dict(item.get("company_filters") or {}),
                result_count=0,
                status="planned",
            )
            for item in explicit_shards
            if isinstance(item, dict)
        ]
        return specs, explicit_shards
    policy = normalize_company_employee_shard_policy(_task_metadata_value(metadata, "company_employee_shard_policy"))
    if str(policy.get("mode") or "").strip().lower() != "keyword_union":
        return [], []
    root_filters = normalize_company_filters(policy.get("root_filters"))
    desired_specs: list[dict[str, Any]] = []
    desired_shards: list[dict[str, Any]] = []
    for keyword_shard in list(policy.get("keyword_shards") or []):
        merged_filters = merge_company_filters(root_filters, dict(keyword_shard.get("include_patch") or {}))
        desired_specs.append(
            build_acquisition_shard_registry_record(
                target_company="",
                company_key="",
                snapshot_id="",
                lane="company_employees",
                employment_scope="current",
                strategy_type=strategy_type,
                shard_id=str(keyword_shard.get("rule_id") or ""),
                shard_title=str(keyword_shard.get("title") or keyword_shard.get("rule_id") or ""),
                search_query=str(merged_filters.get("search_query") or ""),
                company_filters=merged_filters,
                result_count=0,
                status="planned",
            )
        )
        desired_shards.append(
            {
                "shard_id": str(keyword_shard.get("rule_id") or "").strip(),
                "title": str(keyword_shard.get("title") or keyword_shard.get("rule_id") or "").strip(),
                "strategy_id": str(policy.get("strategy_id") or "").strip(),
                "scope_note": str(policy.get("scope_note") or "").strip(),
                "company_filters": merged_filters,
                "max_pages": int(policy.get("max_pages") or _task_metadata_value(metadata, "max_pages") or 1),
                "page_limit": int(policy.get("page_limit") or _task_metadata_value(metadata, "page_limit") or 25),
            }
        )
    return desired_specs, desired_shards


def _planned_profile_search_query_specs(task: AcquisitionTask) -> tuple[list[dict[str, Any]], list[str]]:
    metadata = dict(task.metadata or {})
    filter_hints = dict(_task_metadata_value(metadata, "filter_hints") or {})
    strategy_type = str(_task_metadata_value(metadata, "strategy_type") or "")
    employment_statuses = list(_task_metadata_value(metadata, "employment_statuses") or [])
    normalized_statuses = [str(item).strip().lower() for item in employment_statuses if str(item).strip()]
    if strategy_type == "former_employee_search" or normalized_statuses == ["former"]:
        employment_scope = "former"
    elif strategy_type == "scoped_search_roster":
        employment_scope = "current"
    elif normalized_statuses == ["current"]:
        employment_scope = "current"
    else:
        employment_scope = "all"
    queries: list[str] = []
    seen_query_families: set[str] = set()

    def _add_query(value: Any) -> None:
        canonical = _canonical_query_family_label(value)
        if not canonical:
            return
        family_signature = next(iter(sorted(_query_family_signatures(canonical))), _query_signature(canonical))
        if family_signature in seen_query_families:
            return
        seen_query_families.add(family_signature)
        queries.append(canonical)

    for value in list(filter_hints.get("keywords") or []):
        _add_query(value)

    if not queries:
        for value in list(_task_metadata_value(metadata, "search_seed_queries") or []):
            _add_query(value)
    if not queries:
        queries = [
            str(query).strip()
            for bundle in list(_task_metadata_value(metadata, "search_query_bundles") or [])
            if isinstance(bundle, dict)
            for query in list(bundle.get("queries") or [])
            if str(query).strip()
        ]
        deduped_queries: list[str] = []
        seen_bundle_queries: set[str] = set()
        for query in queries:
            canonical = _canonical_query_family_label(query)
            family_signature = next(iter(sorted(_query_family_signatures(canonical))), _query_signature(canonical))
            if not canonical or family_signature in seen_bundle_queries:
                continue
            seen_bundle_queries.add(family_signature)
            deduped_queries.append(canonical)
        queries = deduped_queries
    desired_specs = [
        build_acquisition_shard_registry_record(
            target_company="",
            company_key="",
            snapshot_id="",
            lane="profile_search",
            employment_scope=employment_scope,
            strategy_type=strategy_type,
            shard_id=query,
            shard_title=query,
            search_query=query,
            company_filters={
                "companies": (
                    list(filter_hints.get("past_companies") or filter_hints.get("current_companies") or [])
                    if employment_scope == "former"
                    else list(filter_hints.get("current_companies") or filter_hints.get("past_companies") or [])
                ),
                "locations": list(filter_hints.get("locations") or []),
                "function_ids": list(filter_hints.get("function_ids") or []),
                "search_query": query,
            },
            result_count=0,
            status="planned",
        )
        for query in queries
    ]
    return desired_specs, queries


def _query_signature(value: Any) -> str:
    normalized = _normalize_query_text(value).lower()
    normalized = normalized.replace("-", " ")
    normalized = " ".join(normalized.split())
    return normalized


def _normalize_scope_match_tuple(values: Any) -> tuple[str, ...]:
    return tuple(sorted(item.lower() for item in _normalize_string_list(values)))


def _normalize_profile_search_employment_scope(value: Any) -> str:
    normalized = _normalize_text(value).lower()
    if normalized == "former":
        return "former"
    if normalized in {"current", "all", ""}:
        return "current"
    return normalized


def _sync_task_intent_view_from_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    synchronized = dict(metadata or {})
    intent_view = dict(synchronized.get("intent_view") or {})
    if not intent_view:
        return synchronized
    if "filter_hints" in synchronized:
        intent_view["filter_hints"] = dict(synchronized.get("filter_hints") or {})
        intent_view["filter_keywords"] = list(dict(synchronized.get("filter_hints") or {}).get("keywords") or [])
        intent_view["function_ids"] = list(dict(synchronized.get("filter_hints") or {}).get("function_ids") or [])
    if "search_seed_queries" in synchronized:
        intent_view["search_seed_queries"] = [
            str(item).strip()
            for item in list(synchronized.get("search_seed_queries") or [])
            if str(item).strip()
        ]
    if "search_query_bundles" in synchronized:
        intent_view["search_query_bundles"] = [
            dict(item)
            for item in list(synchronized.get("search_query_bundles") or [])
            if isinstance(item, dict)
        ]
    if "cost_policy" in synchronized:
        intent_view["cost_policy"] = dict(synchronized.get("cost_policy") or {})
    if "search_channel_order" in synchronized:
        intent_view["search_channel_order"] = [
            str(item).strip()
            for item in list(synchronized.get("search_channel_order") or [])
            if str(item).strip()
        ]
    if "employment_statuses" in synchronized:
        intent_view["employment_statuses"] = [
            str(item).strip()
            for item in list(synchronized.get("employment_statuses") or [])
            if str(item).strip()
        ]
    if "company_scope" in synchronized:
        intent_view["company_scope"] = list(synchronized.get("company_scope") or [])
    if "strategy_type" in synchronized:
        intent_view["strategy_type"] = str(synchronized.get("strategy_type") or "").strip()
    if "company_employee_shard_strategy" in synchronized:
        intent_view["company_employee_shard_strategy"] = str(
            synchronized.get("company_employee_shard_strategy") or ""
        ).strip()
    if "asset_reuse_plan" in synchronized:
        intent_view["asset_reuse_plan"] = dict(synchronized.get("asset_reuse_plan") or {})
    if "delta_execution_plan" in synchronized:
        intent_view["delta_execution_plan"] = dict(synchronized.get("delta_execution_plan") or {})
    if "company_employee_shards" in synchronized:
        intent_view["company_employee_shards"] = [
            dict(item)
            for item in list(synchronized.get("company_employee_shards") or [])
            if isinstance(item, dict)
        ]
    if "publication_source_families" in synchronized:
        intent_view["publication_source_families"] = [
            str(item).strip()
            for item in list(synchronized.get("publication_source_families") or [])
            if str(item).strip()
        ]
    if "publication_extraction_strategy" in synchronized:
        intent_view["publication_extraction_strategy"] = [
            str(item).strip()
            for item in list(synchronized.get("publication_extraction_strategy") or [])
            if str(item).strip()
        ]
    if "company_employee_shard_policy" in synchronized:
        intent_view["company_employee_shard_policy"] = dict(
            synchronized.get("company_employee_shard_policy") or {}
        )
    for scalar_key in (
        "delta_execution_noop",
        "delta_execution_reason",
        "max_pages",
        "page_limit",
        "reuse_existing_roster",
        "include_former_search_seed",
        "enrichment_scope",
        "acquisition_phase",
        "acquisition_phase_title",
        "slug_resolution_limit",
        "profile_detail_limit",
        "publication_scan_limit",
        "publication_lead_limit",
        "exploration_limit",
        "scholar_coauthor_follow_up_limit",
    ):
        if scalar_key in synchronized and synchronized.get(scalar_key) is not None:
            intent_view[scalar_key] = synchronized.get(scalar_key)
    synchronized["intent_view"] = intent_view
    return synchronized


def _filter_query_bundles_for_queries(
    query_bundles: list[dict[str, Any]] | None,
    allowed_queries: list[str] | None,
) -> list[dict[str, Any]]:
    allowed_signatures = {
        _query_signature(item)
        for item in list(allowed_queries or [])
        if _query_signature(item)
    }
    if not allowed_signatures:
        return [dict(bundle or {}) for bundle in list(query_bundles or []) if isinstance(bundle, dict)]
    filtered_bundles: list[dict[str, Any]] = []
    for bundle in list(query_bundles or []):
        if not isinstance(bundle, dict):
            continue
        filtered_queries = []
        for query in list(bundle.get("queries") or []):
            if _query_signature(query) in allowed_signatures:
                filtered_queries.append(str(query).strip())
        if not filtered_queries:
            continue
        filtered_bundles.append(
            {
                **dict(bundle),
                "queries": filtered_queries,
            }
        )
    return filtered_bundles


def _summarize_registry_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summarized: list[dict[str, Any]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        summarized.append(
            {
                "snapshot_id": _normalize_text(row.get("snapshot_id")),
                "search_query": _normalize_query_text(row.get("search_query")),
                "shard_id": _normalize_text(row.get("shard_id")),
                "shard_title": _normalize_text(row.get("shard_title")),
                "company_scope": _normalize_string_list(row.get("company_scope")),
                "locations": _normalize_string_list(row.get("locations")),
                "function_ids": _normalize_string_list(row.get("function_ids")),
                "employment_scope": _normalize_text(row.get("employment_scope")) or "all",
                "result_count": _safe_int(row.get("result_count")),
                "estimated_total_count": _safe_int(row.get("estimated_total_count")),
                "provider_cap_hit": bool(row.get("provider_cap_hit")),
                "status": _normalize_text(row.get("status")) or "completed",
            }
        )
    return summarized


def _ledger_lane_coverage_summary(ledger_summary: dict[str, Any], lane_key: str) -> dict[str, Any]:
    return dict(dict(ledger_summary.get("lane_coverage") or {}).get(lane_key) or {})


def _registry_or_ledger_lane_coverage(
    *,
    baseline: dict[str, Any],
    ledger_summary: dict[str, Any],
    lane_key: str,
    registry_key: str,
) -> dict[str, Any]:
    registry_lane = _normalize_lane_coverage_payload(dict(baseline.get(registry_key) or {}))
    ledger_lane = dict(_ledger_lane_coverage_summary(ledger_summary, lane_key) or {})
    if registry_lane.get("effective_candidate_count") or registry_lane.get("effective_ready"):
        merged = dict(ledger_lane)
        merged.update(registry_lane)
        return merged
    return ledger_lane


def _synthetic_baseline_query_coverage_rows(
    *,
    target_company: str,
    baseline_snapshot_id: str,
    queries: list[str],
    employment_scope: str,
    baseline_completeness_ledger_path: str,
    coverage_reason: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query in list(queries or []):
        query_text = _normalize_query_text(query)
        if not query_text:
            continue
        rows.append(
            {
                "target_company": _normalize_text(target_company),
                "snapshot_id": _normalize_text(baseline_snapshot_id),
                "lane": "profile_search",
                "employment_scope": _normalize_text(employment_scope) or "all",
                "shard_id": query_text,
                "shard_title": query_text,
                "search_query": query_text,
                "status": "embedded_baseline_reuse",
                "metadata": {
                    "embedded_baseline_reuse": True,
                    "coverage_reason": _normalize_text(coverage_reason),
                    "coverage_source": "organization_completeness_ledger",
                    "baseline_completeness_ledger_path": _normalize_text(baseline_completeness_ledger_path),
                },
            }
        )
    return rows


def _baseline_embedded_former_sufficient(
    *,
    baseline_candidate_count: int,
    former_lane_coverage: dict[str, Any],
) -> bool:
    if baseline_candidate_count <= 0 or baseline_candidate_count > 5000:
        return False
    former_lane = dict(former_lane_coverage or {})
    effective_candidate_count = _safe_int(former_lane.get("effective_candidate_count"))
    effective_ready = bool(former_lane.get("effective_ready"))
    standard_bundle_ready_count = _safe_int(former_lane.get("standard_bundle_ready_count"))
    inferred_candidate_count = _safe_int(former_lane.get("inferred_candidate_count"))
    inferred_profile_detail_count = _safe_int(former_lane.get("inferred_profile_detail_count"))
    inferred_linkedin_url_count = _safe_int(former_lane.get("inferred_linkedin_url_count"))
    if effective_candidate_count <= 0 or not effective_ready:
        return False
    if baseline_candidate_count <= 500:
        minimum_candidate_count = 3
        minimum_profile_detail_count = 2
    elif baseline_candidate_count <= 2500:
        minimum_candidate_count = 5
        minimum_profile_detail_count = 3
    else:
        minimum_candidate_count = 8
        minimum_profile_detail_count = 5
    if effective_candidate_count < minimum_candidate_count:
        return False
    if standard_bundle_ready_count > 0:
        return True
    if inferred_candidate_count <= 0 or inferred_linkedin_url_count <= 0:
        return False
    return bool(
        inferred_profile_detail_count >= minimum_profile_detail_count
        or inferred_profile_detail_count >= max(1, int(round(inferred_candidate_count * 0.35)))
    )


def _build_task_delta_execution_plan(
    *,
    task: AcquisitionTask,
    asset_reuse_plan: dict[str, Any],
) -> dict[str, Any]:
    baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
    selected_snapshot_ids = _normalize_string_list(asset_reuse_plan.get("selected_snapshot_ids") or [])
    planner_mode = str(asset_reuse_plan.get("planner_mode") or "").strip()
    requires_delta_acquisition = bool(asset_reuse_plan.get("requires_delta_acquisition"))
    metadata = dict(task.metadata or {})
    strategy_type = str(_task_metadata_value(metadata, "strategy_type") or "")
    baseline_selection_explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
    if task.task_type == "acquire_full_roster":
        if strategy_type == "full_company_roster":
            missing_shards = list(asset_reuse_plan.get("missing_current_company_employee_shards") or [])
            covered_shards = _summarize_registry_rows(
                list(asset_reuse_plan.get("covered_current_company_employee_shards") or [])
            )
            return {
                "lane": "current_company_employees",
                "strategy_type": strategy_type,
                "planner_mode": planner_mode,
                "baseline_snapshot_id": baseline_snapshot_id,
                "selected_snapshot_ids": selected_snapshot_ids,
                "baseline_selection_explanation": baseline_selection_explanation,
                "delta_required": bool(missing_shards),
                "delta_noop": bool(baseline_snapshot_id) and not missing_shards,
                "missing_company_employee_shards": missing_shards,
                "covered_company_employee_shards": covered_shards,
                "delta_reason": (
                    "current_roster_already_covered_by_baseline"
                    if bool(baseline_snapshot_id) and not missing_shards
                    else ""
                ),
            }
        missing_queries = list(asset_reuse_plan.get("missing_current_profile_search_queries") or [])
        covered_queries = _summarize_registry_rows(
            list(asset_reuse_plan.get("covered_current_profile_search_queries") or [])
        )
        return {
            "lane": "current_profile_search",
            "strategy_type": strategy_type,
            "planner_mode": planner_mode,
            "baseline_snapshot_id": baseline_snapshot_id,
            "selected_snapshot_ids": selected_snapshot_ids,
            "baseline_selection_explanation": baseline_selection_explanation,
            "delta_required": bool(missing_queries),
            "delta_noop": bool(baseline_snapshot_id) and not missing_queries,
            "missing_profile_search_queries": missing_queries,
            "covered_profile_search_queries": covered_queries,
            "delta_reason": (
                "current_profile_search_queries_already_covered_by_baseline"
                if bool(baseline_snapshot_id) and not missing_queries
                else ""
            ),
        }
    if task.task_type == "acquire_former_search_seed":
        missing_queries = list(asset_reuse_plan.get("missing_former_profile_search_queries") or [])
        covered_queries = _summarize_registry_rows(
            list(asset_reuse_plan.get("covered_former_profile_search_queries") or [])
        )
        return {
            "lane": "former_profile_search",
            "strategy_type": "former_employee_search",
            "planner_mode": planner_mode,
            "baseline_snapshot_id": baseline_snapshot_id,
            "selected_snapshot_ids": selected_snapshot_ids,
            "baseline_selection_explanation": baseline_selection_explanation,
            "delta_required": bool(missing_queries),
            "delta_noop": bool(baseline_snapshot_id) and not missing_queries,
            "missing_profile_search_queries": missing_queries,
            "covered_profile_search_queries": covered_queries,
            "delta_reason": (
                "former_profile_search_queries_already_covered_by_baseline"
                if bool(baseline_snapshot_id) and not missing_queries
                else ""
            ),
        }
    return {
        "lane": "",
        "strategy_type": strategy_type,
        "planner_mode": planner_mode,
        "baseline_snapshot_id": baseline_snapshot_id,
        "selected_snapshot_ids": selected_snapshot_ids,
        "baseline_selection_explanation": baseline_selection_explanation,
        "delta_required": requires_delta_acquisition,
        "delta_noop": False,
        "delta_reason": "",
    }


def compile_asset_reuse_plan(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    request: JobRequest,
    plan: SourcingPlan,
) -> dict[str, Any]:
    if not str(request.target_company or "").strip():
        return {}
    if str(request.target_scope or "").strip().lower() != "full_company_asset":
        return {}
    if bool(dict(request.execution_preferences or {}).get("force_fresh_run")):
        return {
            "baseline_reuse_available": False,
            "reason": "force_fresh_run",
        }

    baseline = ensure_organization_asset_registry(
        runtime_dir=runtime_dir,
        store=store,
        target_company=request.target_company,
        asset_view=request.asset_view,
    )
    if not baseline:
        return {
            "baseline_reuse_available": False,
            "reason": "no_authoritative_baseline",
        }

    selected_snapshot_ids = _normalize_string_list(
        baseline.get("selected_snapshot_ids") or [baseline.get("snapshot_id")]
    )
    for snapshot_id in selected_snapshot_ids:
        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=request.target_company,
            snapshot_id=snapshot_id,
        )
    ledger_result = ensure_organization_completeness_ledger(
        runtime_dir=runtime_dir,
        store=store,
        target_company=request.target_company,
        snapshot_id=str(baseline.get("snapshot_id") or ""),
        asset_view=request.asset_view,
        selected_snapshot_ids=selected_snapshot_ids,
    )
    ledger_summary = dict(ledger_result.get("summary") or {})
    refreshed_baseline = store.get_authoritative_organization_asset_registry(
        target_company=request.target_company,
        asset_view=request.asset_view,
    )
    if refreshed_baseline and str(refreshed_baseline.get("snapshot_id") or "") == str(baseline.get("snapshot_id") or ""):
        baseline = refreshed_baseline

    shard_rows = store.list_acquisition_shard_registry(
        target_company=request.target_company,
        snapshot_ids=selected_snapshot_ids,
        statuses=["completed", "completed_with_cap", "skipped_high_overlap"],
        limit=5000,
    )
    current_rows = [row for row in shard_rows if str(row.get("lane") or "").strip() == "company_employees"]
    profile_search_rows = [row for row in shard_rows if str(row.get("lane") or "").strip() == "profile_search"]
    current_profile_candidate_rows = [
        row
        for row in [*profile_search_rows, *current_rows]
        if _normalize_profile_search_employment_scope(row.get("employment_scope")) == "current"
    ]
    former_profile_candidate_rows = [
        row
        for row in profile_search_rows
        if _normalize_profile_search_employment_scope(row.get("employment_scope")) == "former"
    ]

    current_task = next((task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster"), None)
    former_task = next((task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed"), None)
    current_strategy_type = str(_task_metadata_value(dict(current_task.metadata or {}), "strategy_type") or "") if current_task else ""
    former_strategy_type = str(_task_metadata_value(dict(former_task.metadata or {}), "strategy_type") or "") if former_task else ""

    desired_current_specs, desired_current_shards = _planned_company_employee_delta_specs(current_task) if current_task else ([], [])
    desired_current_profile_specs, desired_current_profile_queries = (
        _planned_profile_search_query_specs(current_task)
        if current_task and current_strategy_type == "scoped_search_roster"
        else ([], [])
    )
    desired_former_specs, desired_former_queries = _planned_profile_search_query_specs(former_task) if former_task else ([], [])

    covered_current = []
    missing_current = []
    missing_current_shards = []
    for spec, shard in zip(desired_current_specs, desired_current_shards):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=current_rows,
            preferred_lane="company_employees",
        )
        if matched_row:
            covered_current.append(matched_row)
        else:
            missing_current.append(spec)
            missing_current_shards.append(shard)

    covered_current_profile = []
    missing_current_profile = []
    missing_current_profile_queries = []
    for spec, query in zip(desired_current_profile_specs, desired_current_profile_queries):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=current_profile_candidate_rows,
            preferred_lane="profile_search",
        )
        if matched_row:
            covered_current_profile.append(matched_row)
        else:
            missing_current_profile.append(spec)
            missing_current_profile_queries.append(query)

    covered_former = []
    missing_former = []
    missing_former_queries = []
    for spec, query in zip(desired_former_specs, desired_former_queries):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=former_profile_candidate_rows,
            preferred_lane="profile_search",
        )
        if matched_row:
            covered_former.append(matched_row)
        else:
            missing_former.append(spec)
            missing_former_queries.append(query)

    baseline_candidate_count = _safe_int(
        baseline.get("candidate_count")
        or dict(ledger_summary.get("organization_asset") or {}).get("candidate_count")
    )
    baseline_completeness_ledger_path = str(ledger_result.get("ledger_path") or "")
    former_lane_coverage = _registry_or_ledger_lane_coverage(
        baseline=baseline,
        ledger_summary=ledger_summary,
        lane_key="profile_search_former",
        registry_key="former_lane_coverage",
    )
    current_lane_coverage = _registry_or_ledger_lane_coverage(
        baseline=baseline,
        ledger_summary=ledger_summary,
        lane_key="current_effective",
        registry_key="current_lane_coverage",
    )
    baseline_former_embedded_sufficient = False
    if missing_former_queries and current_strategy_type == "full_company_roster" and former_strategy_type == "former_employee_search":
        baseline_former_embedded_sufficient = _baseline_embedded_former_sufficient(
            baseline_candidate_count=baseline_candidate_count,
            former_lane_coverage=former_lane_coverage,
        )
        if baseline_former_embedded_sufficient:
            covered_former.extend(
                _synthetic_baseline_query_coverage_rows(
                    target_company=request.target_company,
                    baseline_snapshot_id=str(baseline.get("snapshot_id") or ""),
                    queries=missing_former_queries,
                    employment_scope="former",
                    baseline_completeness_ledger_path=baseline_completeness_ledger_path,
                    coverage_reason="authoritative_baseline_embeds_former_lane",
                )
            )
            missing_former = []
            missing_former_queries = []

    requires_delta_acquisition = bool(missing_current_shards or missing_current_profile_queries or missing_former_queries)
    baseline_readiness = str(ledger_summary.get("readiness") or "")
    plan_payload = {
        "baseline_reuse_available": True,
        "baseline_snapshot_id": str(baseline.get("snapshot_id") or ""),
        "baseline_asset_view": str(baseline.get("asset_view") or request.asset_view or "canonical_merged"),
        "baseline_candidate_count": baseline_candidate_count,
        "baseline_completeness_score": _safe_float(baseline.get("completeness_score")),
        "baseline_completeness_band": str(baseline.get("completeness_band") or ""),
        "baseline_completeness_ledger_path": baseline_completeness_ledger_path,
        "baseline_readiness": baseline_readiness,
        "baseline_delta_planning_ready": bool(ledger_summary.get("delta_planning_ready")),
        "baseline_standard_bundle_count": _safe_int(dict(ledger_summary.get("standard_bundles") or {}).get("bundle_count")),
        "baseline_current_bundle_ready_count": _safe_int(
            dict(dict(ledger_summary.get("lane_coverage") or {}).get("company_employees_current") or {}).get("standard_bundle_ready_count")
        ),
        "baseline_former_bundle_ready_count": _safe_int(
            dict(dict(ledger_summary.get("lane_coverage") or {}).get("profile_search_former") or {}).get("standard_bundle_ready_count")
        ),
        "baseline_current_effective_candidate_count": _safe_int(
            baseline.get("current_lane_effective_candidate_count") or current_lane_coverage.get("effective_candidate_count")
        ),
        "baseline_former_effective_candidate_count": _safe_int(
            baseline.get("former_lane_effective_candidate_count") or former_lane_coverage.get("effective_candidate_count")
        ),
        "baseline_current_effective_ready": bool(
            baseline.get("current_lane_effective_ready") or current_lane_coverage.get("effective_ready")
        ),
        "baseline_former_effective_ready": bool(
            baseline.get("former_lane_effective_ready") or former_lane_coverage.get("effective_ready")
        ),
        "baseline_current_inferred_candidate_count": _safe_int(current_lane_coverage.get("inferred_candidate_count")),
        "baseline_former_inferred_candidate_count": _safe_int(former_lane_coverage.get("inferred_candidate_count")),
        "baseline_former_inferred_profile_detail_count": _safe_int(former_lane_coverage.get("inferred_profile_detail_count")),
        "baseline_former_embedded_sufficient": baseline_former_embedded_sufficient,
        "selected_snapshot_ids": selected_snapshot_ids,
        "covered_current_company_employee_shard_count": len(covered_current),
        "missing_current_company_employee_shard_count": len(missing_current_shards),
        "covered_current_profile_search_query_count": len(covered_current_profile),
        "missing_current_profile_search_query_count": len(missing_current_profile_queries),
        "covered_former_profile_search_query_count": len(covered_former),
        "missing_former_profile_search_query_count": len(missing_former_queries),
        "covered_current_company_employee_shards": covered_current,
        "missing_current_company_employee_shards": missing_current_shards,
        "covered_current_profile_search_queries": covered_current_profile,
        "missing_current_profile_search_queries": missing_current_profile_queries,
        "covered_former_profile_search_queries": covered_former,
        "missing_former_profile_search_queries": missing_former_queries,
        "requires_delta_acquisition": requires_delta_acquisition,
        "baseline_sufficiency": (
            "delta_required"
            if requires_delta_acquisition
            else (baseline_readiness or "ready")
        ),
        "planner_mode": (
            "delta_from_snapshot"
            if requires_delta_acquisition
            else "reuse_snapshot_only"
        ),
    }
    plan_payload["baseline_selection_explanation"] = build_asset_reuse_baseline_selection_explanation(
        request=request,
        baseline=baseline,
        asset_reuse_plan=plan_payload,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return plan_payload


def apply_asset_reuse_plan_to_sourcing_plan(plan: SourcingPlan, asset_reuse_plan: dict[str, Any]) -> SourcingPlan:
    plan.asset_reuse_plan = dict(asset_reuse_plan or {})
    if not asset_reuse_plan:
        return plan
    baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
    requires_delta_acquisition = bool(asset_reuse_plan.get("requires_delta_acquisition"))
    baseline_selection_explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
    if baseline_snapshot_id:
        plan.assumptions = list(plan.assumptions or []) + [
            f"Planner selected baseline snapshot {baseline_snapshot_id} for company-asset reuse."
        ]
    for task in plan.acquisition_tasks:
        metadata = dict(task.metadata or {})
        strategy_type = str(_task_metadata_value(metadata, "strategy_type") or "")
        missing_current_company_employee_shards = list(asset_reuse_plan.get("missing_current_company_employee_shards") or [])
        missing_current_profile_search_queries = list(asset_reuse_plan.get("missing_current_profile_search_queries") or [])
        missing_former_profile_search_queries = list(asset_reuse_plan.get("missing_former_profile_search_queries") or [])
        metadata["delta_execution_plan"] = _build_task_delta_execution_plan(
            task=task,
            asset_reuse_plan=asset_reuse_plan,
        )
        if task.task_type == "acquire_full_roster":
            metadata["asset_reuse_plan"] = {
                "baseline_snapshot_id": baseline_snapshot_id,
                "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
                "baseline_sufficiency": str(asset_reuse_plan.get("baseline_sufficiency") or ""),
                "baseline_readiness": str(asset_reuse_plan.get("baseline_readiness") or ""),
                "baseline_completeness_ledger_path": str(asset_reuse_plan.get("baseline_completeness_ledger_path") or ""),
                "baseline_standard_bundle_count": int(asset_reuse_plan.get("baseline_standard_bundle_count") or 0),
                "baseline_selection_explanation": baseline_selection_explanation,
                "covered_current_company_employee_shard_count": int(
                    asset_reuse_plan.get("covered_current_company_employee_shard_count") or 0
                ),
                "missing_current_company_employee_shard_count": int(
                    asset_reuse_plan.get("missing_current_company_employee_shard_count") or 0
                ),
                "covered_current_profile_search_query_count": int(
                    asset_reuse_plan.get("covered_current_profile_search_query_count") or 0
                ),
                "missing_current_profile_search_query_count": int(
                    asset_reuse_plan.get("missing_current_profile_search_query_count") or 0
                ),
            }
            if baseline_snapshot_id:
                metadata["baseline_snapshot_id"] = baseline_snapshot_id
            if requires_delta_acquisition and missing_current_company_employee_shards:
                metadata["company_employee_shards"] = list(missing_current_company_employee_shards)
                metadata["delta_execution_required"] = True
                metadata["company_employee_shard_policy"] = {}
            if (
                requires_delta_acquisition
                and strategy_type in {"scoped_search_roster", "former_employee_search"}
                and missing_current_profile_search_queries
            ):
                metadata["search_seed_queries"] = list(missing_current_profile_search_queries)
                metadata["search_query_bundles"] = _filter_query_bundles_for_queries(
                    list(_task_metadata_value(metadata, "search_query_bundles") or []),
                    missing_current_profile_search_queries,
                )
                metadata["delta_execution_required"] = True
            if requires_delta_acquisition and strategy_type == "full_company_roster" and not missing_current_company_employee_shards:
                metadata["delta_execution_noop"] = True
                metadata["delta_execution_reason"] = "current_roster_already_covered_by_baseline"
            if (
                requires_delta_acquisition
                and strategy_type in {"scoped_search_roster", "former_employee_search"}
                and not missing_current_profile_search_queries
            ):
                metadata["delta_execution_noop"] = True
                metadata["delta_execution_reason"] = "current_profile_search_queries_already_covered_by_baseline"
        elif task.task_type == "acquire_former_search_seed":
            metadata["asset_reuse_plan"] = {
                "baseline_snapshot_id": baseline_snapshot_id,
                "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
                "baseline_sufficiency": str(asset_reuse_plan.get("baseline_sufficiency") or ""),
                "baseline_readiness": str(asset_reuse_plan.get("baseline_readiness") or ""),
                "baseline_completeness_ledger_path": str(asset_reuse_plan.get("baseline_completeness_ledger_path") or ""),
                "baseline_standard_bundle_count": int(asset_reuse_plan.get("baseline_standard_bundle_count") or 0),
                "baseline_selection_explanation": baseline_selection_explanation,
                "covered_former_profile_search_query_count": int(
                    asset_reuse_plan.get("covered_former_profile_search_query_count") or 0
                ),
                "missing_former_profile_search_query_count": int(
                    asset_reuse_plan.get("missing_former_profile_search_query_count") or 0
                ),
            }
            if baseline_snapshot_id:
                metadata["baseline_snapshot_id"] = baseline_snapshot_id
            if requires_delta_acquisition and missing_former_profile_search_queries:
                metadata["search_seed_queries"] = list(missing_former_profile_search_queries)
                metadata["search_query_bundles"] = _filter_query_bundles_for_queries(
                    list(_task_metadata_value(metadata, "search_query_bundles") or []),
                    missing_former_profile_search_queries,
                )
                metadata["delta_execution_required"] = True
            if requires_delta_acquisition and not missing_former_profile_search_queries:
                metadata["delta_execution_noop"] = True
                metadata["delta_execution_reason"] = "former_profile_search_queries_already_covered_by_baseline"
        task.metadata = _sync_task_intent_view_from_metadata(metadata)
    return plan
