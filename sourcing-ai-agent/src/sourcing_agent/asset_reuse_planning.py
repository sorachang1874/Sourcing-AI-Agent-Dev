from __future__ import annotations

import json
import re
from hashlib import sha1
from pathlib import Path
from typing import Any

from .asset_paths import iter_company_asset_snapshot_dirs, load_company_snapshot_identity
from .candidate_artifacts import CandidateArtifactError, _resolve_company_snapshot
from .company_registry import normalize_company_key, resolve_company_alias_key
from .company_shard_planning import (
    merge_company_filters,
    normalize_company_employee_shard_policy,
    normalize_company_filters,
)
from .domain import AcquisitionTask, JobRequest, SourcingPlan
from .organization_assets import (
    ensure_organization_completeness_ledger,
    load_cached_organization_completeness_ledger,
    load_company_snapshot_registry_summary,
)
from .query_signal_knowledge import (
    canonicalize_scope_signal_label,
    canonicalize_thematic_signal_label,
    match_scope_signals,
)
from .request_normalization import build_effective_request_payload
from .search_seed_registry import (
    infer_search_seed_summary_employment_scope as _registry_infer_search_seed_summary_employment_scope,
)
from .search_seed_registry import (
    load_search_seed_lane_summaries as _registry_load_search_seed_lane_summaries,
)
from .source_snapshot_coverage import (
    promoted_aggregate_coverage_contract,
    source_snapshot_selection_has_coverage_proof,
)
from .storage import ControlPlaneStore


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


def _count_ratio_within_limit(count: Any, total: int, *, limit: float) -> bool:
    normalized_count = _safe_int(count)
    if normalized_count <= 0:
        return True
    if total <= 0:
        return False
    return (normalized_count / max(total, 1)) <= limit


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_query_text(value: Any) -> str:
    text = _normalize_text(value)
    if text == "__past_company_only__":
        return ""
    return text


def _sourcing_plan_payload(plan: SourcingPlan | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(plan, dict):
        return dict(plan)
    if isinstance(plan, SourcingPlan):
        return plan.to_record()
    if plan is not None and hasattr(plan, "to_record"):
        try:
            payload = plan.to_record()
        except Exception:
            return {}
        if isinstance(payload, dict):
            return dict(payload)
    return {}


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
    "推理": "Reasoning",
    "rl": "RL",
    "reinforcement learning": "RL",
    "强化学习": "RL",
    "eval": "Eval",
    "evals": "Eval",
    "evaluation": "Eval",
    "model evaluation": "Eval",
    "alignment evaluation": "Eval",
    "评估": "Eval",
    "评测": "Eval",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "多模态": "Multimodal",
    "multimodality": "Multimodal",
    "video-generation": "Video generation",
    "预训练": "Pre-train",
    "后训练": "Post-train",
    "world modeling": "World model",
    "世界模型": "World model",
    "nano-banana": "Nano Banana",
    "nanobanana": "Nano Banana",
    "infrastructure": "Infra",
    "基础设施": "Infra",
}
_QUERY_FAMILY_METADATA_VERSION = 1

_PROFILE_SEARCH_GENERIC_QUERY_FAMILY_SIGNATURES = {
    "research",
    "training",
    "employee",
    "employees",
    "engineer",
    "engineers",
    "researcher",
    "researchers",
}

_LARGE_AUTHORITATIVE_BASELINE_LOCAL_REUSE_MIN_CANDIDATES = 2500
_LARGE_AUTHORITATIVE_BASELINE_LOCAL_REUSE_MIN_FORMER_RATIO = 0.05
_FULL_COMPANY_LANE_REUSE_MIN_COVERAGE_RATIO = 0.95
_EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MAX_SECONDARY_ONLY_COUNT = 2
_EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MAX_SECONDARY_ONLY_RATIO = 0.1
_EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MIN_SECONDARY_OVERLAP_RATIO = 0.9
_ORGANIZATION_ASSET_PROMOTION_MIN_RELATIVE_GAIN = 0.05
_ORGANIZATION_ASSET_PROMOTION_MIN_ABSOLUTE_GAIN_SMALL = 20
_ORGANIZATION_ASSET_PROMOTION_MIN_ABSOLUTE_GAIN_LARGE = 100
_ORGANIZATION_ASSET_PROMOTION_MAX_COMPLETENESS_SCORE_REGRESSION = 1.0
_ORGANIZATION_ASSET_REGISTRY_READY_STATUSES = {"", "ready", "completed", "complete", "canonical"}
_ORGANIZATION_ASSET_REGISTRY_NON_PROMOTABLE_STATUSES = {"draft", "partial", "superseded", "archived", "empty"}
_LARGE_ORG_REUSE_BASELINE_MIN_CANDIDATES = 2500
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
    canonical_thematic = canonicalize_thematic_signal_label(normalized)
    if canonical_thematic and _query_signature(canonical_thematic) != _query_signature(normalized):
        return canonical_thematic
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
    canonical_thematic = canonicalize_thematic_signal_label(normalized)
    if canonical_thematic:
        signatures.add(_query_signature(canonical_thematic))
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


def _build_query_family_metadata(value: Any) -> dict[str, Any]:
    raw_query = _normalize_text(value)
    normalized_query = _normalize_query_text(value)
    canonical_label = _canonical_query_family_label(normalized_query)
    signatures = sorted(_query_family_signatures(normalized_query))
    if not raw_query and not normalized_query and not canonical_label and not signatures:
        return {}
    metadata: dict[str, Any] = {
        "version": _QUERY_FAMILY_METADATA_VERSION,
    }
    if raw_query:
        metadata["raw_query"] = raw_query
    if normalized_query:
        metadata["normalized_query"] = normalized_query
    if canonical_label:
        metadata["canonical_label"] = canonical_label
    if signatures:
        metadata["signatures"] = signatures
    return metadata


def _merge_query_family_metadata(metadata: dict[str, Any] | None, *, search_query: Any) -> dict[str, Any]:
    merged = dict(metadata or {})
    query_family = _build_query_family_metadata(search_query)
    if query_family:
        merged["query_family"] = query_family
    else:
        merged.pop("query_family", None)
    return merged


def _query_family_signatures_from_metadata(metadata: Any, *, fallback_query: Any) -> set[str]:
    payload = dict(metadata or {}) if isinstance(metadata, dict) else {}
    query_family = dict(payload.get("query_family") or {}) if isinstance(payload.get("query_family"), dict) else {}
    signatures: set[str] = set()
    for item in list(query_family.get("signatures") or []):
        signature = _query_signature(item)
        if signature:
            signatures.add(signature)
    canonical_label = _normalize_text(query_family.get("canonical_label"))
    if canonical_label:
        signatures.update(_query_family_signatures(canonical_label))
    normalized_query = _normalize_text(query_family.get("normalized_query"))
    if normalized_query:
        signatures.update(_query_family_signatures(normalized_query))
    return signatures or _query_family_signatures(fallback_query)


def refresh_acquisition_shard_registry_query_family_metadata(
    payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], bool]:
    refreshed = dict(payload or {})
    existing_metadata = dict(refreshed.get("metadata") or {})
    updated_metadata = _merge_query_family_metadata(existing_metadata, search_query=refreshed.get("search_query"))
    changed = json.dumps(existing_metadata, ensure_ascii=False, sort_keys=True) != json.dumps(
        updated_metadata,
        ensure_ascii=False,
        sort_keys=True,
    )
    refreshed["metadata"] = updated_metadata
    return refreshed, changed


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


def normalize_organization_asset_lifecycle_status(value: Any, *, default: str = "ready") -> str:
    normalized = _normalize_text(value).lower().replace("-", "_")
    if normalized in {"complete", "completed"}:
        return "ready"
    if normalized in {"canonical", "ready", "draft", "partial", "superseded", "archived", "empty"}:
        return normalized
    return _normalize_text(default).lower() or "ready"


def organization_asset_lifecycle_promotable(record: dict[str, Any] | None) -> bool:
    status = normalize_organization_asset_lifecycle_status(dict(record or {}).get("status"))
    return status not in _ORGANIZATION_ASSET_REGISTRY_NON_PROMOTABLE_STATUSES


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
    effective_request = dict(metadata.get("effective_request") or {})
    intent_view = dict(metadata.get("intent_view") or {})
    if not effective_request:
        return intent_view
    return {
        **effective_request,
        **intent_view,
    }


def _task_metadata_value(metadata: dict[str, Any], key: str, default: Any = None) -> Any:
    intent_view = _task_intent_metadata_view(metadata)
    if key in intent_view:
        value = intent_view.get(key)
    else:
        value = metadata.get(key, default)
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return [dict(item) if isinstance(item, dict) else item for item in value]
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
        if (
            requested_text == normalize_company_key(requested_text)
            and resolved_text != requested_text
        ):
            return resolved_text
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


def _plan_organization_execution_profile(plan: SourcingPlan) -> dict[str, Any]:
    plan_payload = _sourcing_plan_payload(plan)
    acquisition_strategy_payload = dict(plan_payload.get("acquisition_strategy") or {})
    return dict(
        plan_payload.get("organization_execution_profile")
        or getattr(plan, "organization_execution_profile", {})
        or acquisition_strategy_payload.get("organization_execution_profile")
        or dict(getattr(getattr(plan, "acquisition_strategy", None), "organization_execution_profile", {}) or {})
    )


def _resolve_registry_lane_coverages(
    summary: dict[str, Any],
    *,
    current_lane_coverage: dict[str, Any] | None = None,
    former_lane_coverage: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    membership_summary = dict(
        summary.get("exact_membership_summary") or summary.get("membership_summary") or {}
    )
    employment_scope_counts = dict(membership_summary.get("employment_scope_counts") or {})

    def _membership_fallback(employment_scope: str) -> dict[str, Any]:
        fallback_count = _safe_int(employment_scope_counts.get(employment_scope))
        if fallback_count <= 0:
            return {}
        return {
            "effective_candidate_count": fallback_count,
            "effective_ready": True,
            "exact_member_count": fallback_count,
            "source_lane_keys": [f"baseline_membership_summary:{employment_scope}"],
            "inferred_source_kind": "membership_summary",
        }

    def _merge_membership_fallback(coverage: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        if not fallback:
            return coverage
        if not coverage:
            return dict(fallback)
        merged = dict(coverage)
        fallback_count = _safe_int(fallback.get("effective_candidate_count"))
        if fallback_count > _safe_int(merged.get("effective_candidate_count")):
            merged["effective_candidate_count"] = fallback_count
        if fallback_count > _safe_int(merged.get("exact_member_count")):
            merged["exact_member_count"] = fallback_count
        merged["effective_ready"] = bool(merged.get("effective_ready")) or bool(fallback.get("effective_ready"))
        if not merged.get("source_lane_keys"):
            merged["source_lane_keys"] = list(fallback.get("source_lane_keys") or [])
        if not merged.get("inferred_source_kind"):
            merged["inferred_source_kind"] = str(fallback.get("inferred_source_kind") or "")
        return merged

    if current_lane_coverage is None:
        current_lane_coverage = dict(summary.get("current_lane_coverage") or {})
    if former_lane_coverage is None:
        former_lane_coverage = dict(summary.get("former_lane_coverage") or {})
    lane_coverage = dict(summary.get("lane_coverage") or {})
    if not current_lane_coverage:
        current_lane_coverage = dict(lane_coverage.get("current_effective") or {})
    if not former_lane_coverage:
        former_lane_coverage = dict(
            lane_coverage.get("former_effective") or lane_coverage.get("profile_search_former") or {}
        )
    current_lane_coverage = _merge_membership_fallback(
        current_lane_coverage,
        _membership_fallback("current"),
    )
    former_lane_coverage = _merge_membership_fallback(
        former_lane_coverage,
        _membership_fallback("former"),
    )
    return (
        _normalize_lane_coverage_payload(current_lane_coverage),
        _normalize_lane_coverage_payload(former_lane_coverage),
    )


def _requested_lane_requirements(request: JobRequest) -> dict[str, Any]:
    normalized_statuses = [
        str(item or "").strip().lower() for item in list(request.employment_statuses or []) if str(item or "").strip()
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
        not bool(lane_requirements.get("need_current")) or (current_ready and current_count > 0)
    ) and (not bool(lane_requirements.get("need_former")) or (former_ready and former_count > 0))
    current_company_employee_exact_overlap_summary = dict(
        asset_reuse_plan.get("current_company_employee_exact_overlap_summary") or {}
    )
    current_profile_search_exact_overlap_summary = dict(
        asset_reuse_plan.get("current_profile_search_exact_overlap_summary") or {}
    )
    former_profile_search_exact_overlap_summary = dict(
        asset_reuse_plan.get("former_profile_search_exact_overlap_summary") or {}
    )
    exact_membership_available = any(
        bool(summary.get("comparison_available"))
        for summary in (
            current_company_employee_exact_overlap_summary,
            current_profile_search_exact_overlap_summary,
            former_profile_search_exact_overlap_summary,
        )
    )
    reuse_basis = (
        "organization_asset_registry_exact_membership"
        if exact_membership_available
        else "organization_asset_registry_lane_coverage"
    )
    return {
        "selection_mode": reuse_basis,
        "reuse_basis": reuse_basis,
        "target_company": str(request.target_company or baseline.get("target_company") or ""),
        "asset_view": str(
            asset_reuse_plan.get("baseline_asset_view")
            or baseline.get("asset_view")
            or request.asset_view
            or "canonical_merged"
        ),
        "matched_registry_id": int(baseline.get("registry_id") or 0),
        "matched_registry_snapshot_id": str(
            asset_reuse_plan.get("baseline_snapshot_id") or baseline.get("snapshot_id") or ""
        ),
        "selected_snapshot_ids": [
            str(item).strip()
            for item in list(
                asset_reuse_plan.get("selected_snapshot_ids") or baseline.get("selected_snapshot_ids") or []
            )
            if str(item).strip()
        ],
        "registry_authoritative": bool(baseline.get("authoritative")),
        "baseline_reuse_available": bool(asset_reuse_plan.get("baseline_reuse_available")),
        "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
        "requires_delta_acquisition": bool(asset_reuse_plan.get("requires_delta_acquisition")),
        "baseline_readiness": str(asset_reuse_plan.get("baseline_readiness") or ""),
        "baseline_sufficiency": str(asset_reuse_plan.get("baseline_sufficiency") or ""),
        "baseline_generation": {
            **_baseline_generation_summary(baseline),
            "generation_key": _normalize_text(
                asset_reuse_plan.get("baseline_generation_key") or baseline.get("materialization_generation_key")
            ),
            "generation_sequence": _safe_int(
                asset_reuse_plan.get("baseline_generation_sequence")
                or baseline.get("materialization_generation_sequence")
            ),
            "generation_watermark": _normalize_text(
                asset_reuse_plan.get("baseline_generation_watermark") or baseline.get("materialization_watermark")
            ),
        },
        "baseline_candidate_count": _safe_int(
            asset_reuse_plan.get("baseline_candidate_count") or baseline.get("candidate_count")
        ),
        "baseline_source_snapshot_selection_mode": str(
            asset_reuse_plan.get("baseline_source_snapshot_selection_mode")
            or _baseline_source_snapshot_selection_mode(baseline)
            or ""
        ),
        "baseline_multi_snapshot_aggregate_coverage_proven": bool(
            asset_reuse_plan.get("baseline_multi_snapshot_aggregate_coverage_proven")
        ),
        "baseline_completeness_score": _safe_float(
            asset_reuse_plan.get("baseline_completeness_score") or baseline.get("completeness_score")
        ),
        "baseline_completeness_band": str(
            asset_reuse_plan.get("baseline_completeness_band") or baseline.get("completeness_band") or ""
        ),
        "baseline_completeness_ledger_path": str(asset_reuse_plan.get("baseline_completeness_ledger_path") or ""),
        "baseline_current_embedded_sufficient": bool(asset_reuse_plan.get("baseline_current_embedded_sufficient")),
        "baseline_former_embedded_sufficient": bool(asset_reuse_plan.get("baseline_former_embedded_sufficient")),
        "baseline_full_company_lane_reuse_sufficient": bool(
            asset_reuse_plan.get("baseline_full_company_lane_reuse_sufficient")
        ),
        "baseline_current_embedded_query_reuse_allowed": bool(
            asset_reuse_plan.get("baseline_current_embedded_query_reuse_allowed", True)
        ),
        "baseline_former_embedded_query_reuse_allowed": bool(
            asset_reuse_plan.get("baseline_former_embedded_query_reuse_allowed", True)
        ),
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
        "current_company_employee_exact_overlap_summary": current_company_employee_exact_overlap_summary,
        "current_profile_search_exact_overlap_summary": current_profile_search_exact_overlap_summary,
        "former_profile_search_exact_overlap_summary": former_profile_search_exact_overlap_summary,
        "current_company_employee_exact_overlap_gaps": list(
            asset_reuse_plan.get("current_company_employee_exact_overlap_gaps") or []
        ),
        "current_profile_search_exact_overlap_gaps": list(
            asset_reuse_plan.get("current_profile_search_exact_overlap_gaps") or []
        ),
        "former_profile_search_exact_overlap_gaps": list(
            asset_reuse_plan.get("former_profile_search_exact_overlap_gaps") or []
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
        (profile_detail_count + min(explicit_profile_capture_count, candidate_count) * 0.35)
        / effective_candidate_denominator,
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
    lifecycle_status = normalize_organization_asset_lifecycle_status(
        summary.get("asset_lifecycle_state") or summary.get("lifecycle_status") or summary.get("status"),
        default="ready" if candidate_count > 0 else "empty",
    )
    resolved_current_lane_coverage, resolved_former_lane_coverage = _resolve_registry_lane_coverages(
        summary,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return {
        "target_company": _normalize_text(target_company),
        "company_key": _normalize_text(company_key) or resolve_company_alias_key(target_company),
        "snapshot_id": _normalize_text(snapshot_id),
        "asset_view": _normalize_text(asset_view) or "canonical_merged",
        "status": lifecycle_status,
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
        "current_lane_effective_candidate_count": _safe_int(
            resolved_current_lane_coverage.get("effective_candidate_count")
        ),
        "former_lane_effective_candidate_count": _safe_int(
            resolved_former_lane_coverage.get("effective_candidate_count")
        ),
        "current_lane_effective_ready": bool(resolved_current_lane_coverage.get("effective_ready")),
        "former_lane_effective_ready": bool(resolved_former_lane_coverage.get("effective_ready")),
        "source_snapshot_selection": selection,
        "selected_snapshot_ids": selected_snapshot_ids or [_normalize_text(snapshot_id)],
        "source_path": _normalize_text(source_path),
        "source_job_id": _normalize_text(source_job_id),
        "materialization_generation_key": _normalize_text(summary.get("materialization_generation_key")),
        "materialization_generation_sequence": _safe_int(summary.get("materialization_generation_sequence")),
        "materialization_watermark": _normalize_text(summary.get("materialization_watermark")),
        "summary": dict(summary or {}),
    }


def _organization_asset_summary_path(snapshot_dir: Path, asset_view: str) -> Path:
    return (
        snapshot_dir
        / "normalized_artifacts"
        / ("artifact_summary.json" if asset_view == "canonical_merged" else f"{asset_view}/artifact_summary.json")
    )


def _organization_registry_selected_snapshot_ids(payload: dict[str, Any]) -> list[str]:
    selection = dict(payload.get("source_snapshot_selection") or {})
    return _normalize_string_list(payload.get("selected_snapshot_ids") or selection.get("selected_snapshot_ids") or [])


def _baseline_source_snapshot_selection(baseline: dict[str, Any]) -> dict[str, Any]:
    baseline_payload = dict(baseline or {})
    summary = dict(baseline_payload.get("summary") or {})
    return dict(
        baseline_payload.get("source_snapshot_selection")
        or summary.get("source_snapshot_selection")
        or {}
    )


def _baseline_source_snapshot_selection_mode(baseline: dict[str, Any]) -> str:
    selection = _baseline_source_snapshot_selection(baseline)
    return _normalize_text(selection.get("mode")).lower()


def _source_snapshot_selection_has_coverage_proof(selection: dict[str, Any] | None) -> bool:
    return source_snapshot_selection_has_coverage_proof(selection)


def _baseline_standard_bundle_count(baseline: dict[str, Any], ledger_summary: dict[str, Any] | None = None) -> int:
    count = 0
    for payload in (dict(baseline or {}), dict(ledger_summary or {}), dict(dict(baseline or {}).get("summary") or {})):
        standard_bundles = dict(payload.get("standard_bundles") or {})
        count = max(count, _safe_int(standard_bundles.get("bundle_count")))
    return count


def _baseline_has_large_org_reuse_coverage_contract(
    *,
    baseline: dict[str, Any],
    plan: SourcingPlan,
    baseline_candidate_count: int,
    shard_rows: list[dict[str, Any]],
    ledger_summary: dict[str, Any] | None = None,
) -> bool:
    organization_execution_profile = _plan_organization_execution_profile(plan)
    org_default_mode = _normalize_text(organization_execution_profile.get("default_acquisition_mode")).lower()
    org_scale_band = _normalize_text(organization_execution_profile.get("org_scale_band")).lower()
    if org_scale_band != "large" and org_default_mode != "scoped_search_roster":
        return True
    if baseline_candidate_count >= _LARGE_ORG_REUSE_BASELINE_MIN_CANDIDATES:
        return True
    if list(shard_rows or []):
        return True
    if _baseline_standard_bundle_count(baseline, ledger_summary) > 0:
        return True
    return _source_snapshot_selection_has_coverage_proof(_baseline_source_snapshot_selection(baseline))


def _multi_snapshot_authoritative_aggregate_has_coverage_proof(
    *,
    baseline: dict[str, Any],
    selected_snapshot_ids: list[str],
) -> bool:
    if len(list(selected_snapshot_ids or [])) <= 1:
        return False
    baseline_payload = dict(baseline or {})
    if not bool(baseline_payload.get("authoritative")):
        return False
    selection = _baseline_source_snapshot_selection(baseline_payload)
    return bool(promoted_aggregate_coverage_contract(selection).get("coverage_proven"))


def _organization_asset_registry_candidate_sort_key(
    payload: dict[str, Any],
) -> tuple[float, int, int, int, int, int, str]:
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
    candidate_lifecycle_status = normalize_organization_asset_lifecycle_status(candidate_record.get("status"))
    if not organization_asset_lifecycle_promotable(candidate_record):
        return {
            "promote": False,
            "reason": "lifecycle_state_not_promotable",
            "existing_snapshot_id": _normalize_text((existing_authoritative or {}).get("snapshot_id")),
            "candidate_snapshot_id": candidate_snapshot_id,
            "candidate_lifecycle_status": candidate_lifecycle_status,
        }
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
    existing_sort_key = _organization_asset_registry_candidate_sort_key(existing_authoritative)
    candidate_sort_key = _organization_asset_registry_candidate_sort_key(candidate_record)
    explicit_baseline_inclusion = bool(
        existing_snapshot_id and existing_snapshot_id in selected_snapshot_ids
    )

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
    existing_source_snapshot_count = _safe_int(existing_authoritative.get("source_snapshot_count"))
    candidate_source_snapshot_count = _safe_int(candidate_record.get("source_snapshot_count"))

    existing_candidate_denominator = max(existing_candidate_count, 1)
    candidate_candidate_denominator = max(candidate_candidate_count, 1)
    existing_missing_ratio = existing_missing_linkedin_count / existing_candidate_denominator
    candidate_missing_ratio = candidate_missing_linkedin_count / candidate_candidate_denominator
    existing_profile_gap_ratio = existing_profile_gap_count / existing_candidate_denominator
    candidate_profile_gap_ratio = candidate_profile_gap_count / candidate_candidate_denominator

    subsumption_higher = (
        candidate_candidate_count >= max(existing_candidate_count, int(existing_candidate_count * 0.98))
        and candidate_profile_detail_count
        >= max(existing_profile_detail_count, int(existing_profile_detail_count * 0.98))
        and candidate_evidence_count >= int(existing_evidence_count * 0.95)
        and candidate_missing_ratio <= existing_missing_ratio + 0.01
        and candidate_profile_gap_ratio <= existing_profile_gap_ratio + 0.02
        and candidate_effective_lane_total
        >= max(existing_effective_lane_total, int(existing_effective_lane_total * 0.98))
    )
    completeness_higher = candidate_completeness_score >= existing_completeness_score + 1.0
    score_gap = existing_completeness_score - candidate_completeness_score
    absolute_gain_floor = (
        _ORGANIZATION_ASSET_PROMOTION_MIN_ABSOLUTE_GAIN_LARGE
        if max(existing_candidate_count, candidate_candidate_count) >= 1000
        else _ORGANIZATION_ASSET_PROMOTION_MIN_ABSOLUTE_GAIN_SMALL
    )
    candidate_count_materially_higher = candidate_candidate_count >= max(
        existing_candidate_count + absolute_gain_floor,
        int(existing_candidate_count * (1.0 + _ORGANIZATION_ASSET_PROMOTION_MIN_RELATIVE_GAIN)),
    )
    profile_detail_materially_higher = candidate_profile_detail_count >= max(
        existing_profile_detail_count + absolute_gain_floor,
        int(existing_profile_detail_count * (1.0 + _ORGANIZATION_ASSET_PROMOTION_MIN_RELATIVE_GAIN)),
    )
    evidence_materially_higher = candidate_evidence_count >= max(
        existing_evidence_count + absolute_gain_floor,
        int(existing_evidence_count * (1.0 + _ORGANIZATION_ASSET_PROMOTION_MIN_RELATIVE_GAIN)),
    )
    effective_lane_total_materially_higher = candidate_effective_lane_total >= max(
        existing_effective_lane_total + absolute_gain_floor,
        int(existing_effective_lane_total * (1.0 + _ORGANIZATION_ASSET_PROMOTION_MIN_RELATIVE_GAIN)),
    )
    coverage_materially_higher = bool(
        effective_lane_total_materially_higher
        or (
            candidate_count_materially_higher
            and profile_detail_materially_higher
            and evidence_materially_higher
        )
    )
    source_snapshot_count_bias = (
        existing_source_snapshot_count > candidate_source_snapshot_count and candidate_source_snapshot_count > 0
    )
    materially_higher_coverage_with_stable_quality = bool(
        subsumption_higher
        and coverage_materially_higher
        and score_gap <= _ORGANIZATION_ASSET_PROMOTION_MAX_COMPLETENESS_SCORE_REGRESSION
    )
    explicit_baseline_inclusion_quality_recovery = bool(
        explicit_baseline_inclusion
        and candidate_sort_key > existing_sort_key
        and existing_completeness_score < 50.0
        and candidate_completeness_score >= existing_completeness_score + 10.0
        and candidate_profile_gap_ratio + 0.05 < existing_profile_gap_ratio
        and candidate_missing_ratio <= existing_missing_ratio + 0.02
    )
    explicit_baseline_inclusion_promotable = bool(
        explicit_baseline_inclusion
        and candidate_sort_key > existing_sort_key
        and (
            (
                subsumption_higher
                and score_gap <= _ORGANIZATION_ASSET_PROMOTION_MAX_COMPLETENESS_SCORE_REGRESSION
            )
            or explicit_baseline_inclusion_quality_recovery
        )
    )
    materially_higher_coverage_despite_snapshot_count_bias = bool(
        subsumption_higher
        and not completeness_higher
        and coverage_materially_higher
        and source_snapshot_count_bias
        and score_gap <= 3.5
    )
    promote = bool(
        explicit_baseline_inclusion_promotable
        or (completeness_higher and subsumption_higher)
        or materially_higher_coverage_despite_snapshot_count_bias
        or materially_higher_coverage_with_stable_quality
    )
    return {
        "promote": promote,
        "reason": (
            "explicit_baseline_inclusion"
            if explicit_baseline_inclusion_promotable
            else (
                "higher_completeness_and_subsumption"
                if completeness_higher and subsumption_higher
                else (
                    "materially_higher_coverage_despite_snapshot_count_bias"
                    if materially_higher_coverage_despite_snapshot_count_bias
                    else (
                        "materially_higher_coverage_with_stable_quality"
                        if materially_higher_coverage_with_stable_quality
                        else "guard_rejected"
                    )
                )
            )
        ),
        "existing_snapshot_id": existing_snapshot_id,
        "candidate_snapshot_id": candidate_snapshot_id,
        "selected_snapshot_ids": selected_snapshot_ids,
        "explicit_baseline_inclusion": explicit_baseline_inclusion,
        "explicit_baseline_inclusion_quality_recovery": explicit_baseline_inclusion_quality_recovery,
        "explicit_baseline_inclusion_promotable": explicit_baseline_inclusion_promotable,
        "completeness_higher": completeness_higher,
        "subsumption_higher": subsumption_higher,
        "coverage_materially_higher": coverage_materially_higher,
        "effective_lane_total_materially_higher": effective_lane_total_materially_higher,
        "source_snapshot_count_bias": source_snapshot_count_bias,
        "score_gap": round(score_gap, 6),
        "existing_metrics": {
            "candidate_count": existing_candidate_count,
            "evidence_count": existing_evidence_count,
            "profile_detail_count": existing_profile_detail_count,
            "missing_linkedin_count": existing_missing_linkedin_count,
            "profile_completion_backlog_count": existing_profile_gap_count,
            "completeness_score": existing_completeness_score,
            "effective_lane_total": existing_effective_lane_total,
            "source_snapshot_count": existing_source_snapshot_count,
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
            "source_snapshot_count": candidate_source_snapshot_count,
            "missing_ratio": round(candidate_missing_ratio, 6),
            "profile_gap_ratio": round(candidate_profile_gap_ratio, 6),
        },
    }


def select_organization_asset_registry_promotion_candidate(
    *,
    candidate_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    existing_authoritative: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_candidates = [
        dict(record)
        for record in list(candidate_records or [])
        if isinstance(record, dict)
    ]
    if not normalized_candidates:
        return {}, {}
    sorted_candidates = sorted(
        normalized_candidates,
        key=_organization_asset_registry_candidate_sort_key,
        reverse=True,
    )
    authoritative_row = dict(existing_authoritative or {})
    if not authoritative_row:
        selected = dict(sorted_candidates[0] or {})
        return selected, {
            "promote": bool(selected),
            "reason": "no_existing_authoritative",
            "existing_snapshot_id": "",
            "candidate_snapshot_id": _normalize_text(selected.get("snapshot_id")),
        }
    authoritative_registry_id = _safe_int(authoritative_row.get("registry_id"))
    authoritative_snapshot_id = _normalize_text(authoritative_row.get("snapshot_id"))
    for candidate in sorted_candidates:
        if authoritative_registry_id and _safe_int(candidate.get("registry_id")) == authoritative_registry_id:
            continue
        if authoritative_snapshot_id and _normalize_text(candidate.get("snapshot_id")) == authoritative_snapshot_id:
            continue
        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative=authoritative_row,
            candidate_record=candidate,
        )
        if bool(decision.get("promote")):
            return dict(candidate), decision
    return authoritative_row, {
        "promote": False,
        "reason": "existing_authoritative_retained",
        "existing_snapshot_id": authoritative_snapshot_id,
        "candidate_snapshot_id": authoritative_snapshot_id,
    }


def _organization_asset_registry_record_is_candidate_eligible(payload: dict[str, Any]) -> bool:
    record = dict(payload or {})
    status = normalize_organization_asset_lifecycle_status(record.get("status"))
    if status not in _ORGANIZATION_ASSET_REGISTRY_READY_STATUSES:
        return False
    if not organization_asset_lifecycle_promotable(record):
        return False
    effective_lane_total = _organization_asset_registry_effective_lane_total(record)
    return max(_safe_int(record.get("candidate_count")), effective_lane_total) > 0


def build_organization_asset_registry_candidate_inventory(
    *,
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str = "canonical_merged",
    limit: int = 50,
) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in store.list_organization_asset_registry(
            target_company=target_company,
            asset_view=asset_view,
            limit=max(1, int(limit or 50)),
        )
        if isinstance(row, dict)
    ]
    authoritative_row = next((row for row in rows if bool(row.get("authoritative"))), {})
    candidate_rows = [row for row in rows if _organization_asset_registry_record_is_candidate_eligible(row)]
    if authoritative_row and not any(
        _safe_int(row.get("registry_id")) == _safe_int(authoritative_row.get("registry_id"))
        for row in candidate_rows
    ):
        candidate_rows.append(dict(authoritative_row))
    selected_row, selection_decision = select_organization_asset_registry_promotion_candidate(
        candidate_records=candidate_rows,
        existing_authoritative=authoritative_row or None,
    )
    ordered_candidate_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in [
        dict(selected_row or {}),
        dict(authoritative_row or {}),
        *sorted(candidate_rows, key=_organization_asset_registry_candidate_sort_key, reverse=True),
    ]:
        if not row:
            continue
        registry_id = _safe_int(row.get("registry_id"))
        snapshot_id = _normalize_text(row.get("snapshot_id"))
        dedupe_key = f"{registry_id}:{snapshot_id}" if registry_id > 0 else snapshot_id
        if not dedupe_key or dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        ordered_candidate_rows.append(dict(row))
    return {
        "rows": rows,
        "candidate_rows": candidate_rows,
        "authoritative_row": dict(authoritative_row or {}),
        "selected_row": dict(selected_row or {}),
        "selection_decision": dict(selection_decision or {}),
        "ordered_candidate_rows": ordered_candidate_rows,
    }


def upsert_organization_asset_registry_with_guard(
    *,
    store: ControlPlaneStore,
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
    records: list[dict[str, Any]] = []
    matching_snapshot_dirs: list[Path] = []
    for candidate_snapshot_dir in iter_company_asset_snapshot_dirs(
        runtime_dir,
        prefer_hot_cache=True,
        existing_only=True,
    ):
        if resolve_company_alias_key(candidate_snapshot_dir.parent.name) != resolve_company_alias_key(
            snapshot_dir.parent.name
        ):
            continue
        matching_snapshot_dirs.append(candidate_snapshot_dir)
    for candidate_snapshot_dir in sorted(matching_snapshot_dirs, key=lambda path: path.name, reverse=True):
        summary_result = load_company_snapshot_registry_summary(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=candidate_snapshot_dir.name,
            asset_view=asset_view,
        )
        summary = dict(summary_result.get("summary") or {})
        if not isinstance(summary, dict) or not summary:
            continue
        identity_payload = dict(
            summary_result.get("identity_payload")
            or load_company_snapshot_identity(candidate_snapshot_dir, fallback_payload={})
        )
        company_key = (
            str(
                summary_result.get("company_key")
                or identity_payload.get("company_key")
                or candidate_snapshot_dir.parent.name
            ).strip()
            or candidate_snapshot_dir.parent.name
        )
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
    store: ControlPlaneStore,
    target_company: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    normalized_company_key = resolve_company_alias_key(target_company)
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

    selected_record, selection_decision = select_organization_asset_registry_promotion_candidate(
        candidate_records=candidate_records,
        existing_authoritative=current_authoritative or None,
    )
    selected_reason = str(selection_decision.get("reason") or "")

    if not selected_record:
        return {
            "status": "missing",
            "target_company": target_company,
            "asset_view": asset_view,
            "record_count": len(candidate_records),
        }

    persisted = store.upsert_organization_asset_registry(selected_record, authoritative=True)
    execution_profile = {}
    try:
        from .organization_execution_profile import ensure_organization_execution_profile

        execution_profile = ensure_organization_execution_profile(
            runtime_dir=runtime_dir,
            store=store,
            target_company=str(persisted.get("target_company") or target_company),
            asset_view=asset_view,
        )
    except Exception:
        execution_profile = {}
    return {
        "status": "backfilled",
        "target_company": str(persisted.get("target_company") or target_company),
        "asset_view": asset_view,
        "record_count": len(candidate_records),
        "canonicalization": canonicalization_result,
        "selected_snapshot_id": str(persisted.get("snapshot_id") or ""),
        "selected_reason": selected_reason,
        "selected_record": persisted,
        "organization_execution_profile": execution_profile,
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
    metadata_payload = _merge_query_family_metadata(dict(metadata or {}), search_query=search_query or normalized_query)
    signature_payload = {
        "target_company": resolve_company_alias_key(target_company),
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
        "company_key": _normalize_text(company_key) or resolve_company_alias_key(target_company),
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
        "metadata": metadata_payload,
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


def _infer_search_seed_summary_employment_scope(summary: dict[str, Any] | None) -> str:
    return _registry_infer_search_seed_summary_employment_scope(summary)


def _load_search_seed_lane_summaries(snapshot_dir: Path) -> list[dict[str, Any]]:
    return _registry_load_search_seed_lane_summaries(snapshot_dir)


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
    default_filter_hints = dict(effective_filter_hints or {})
    for item in list(query_summaries or []):
        if not isinstance(item, dict):
            continue
        mode = _normalize_text(item.get("mode")).lower()
        if mode not in {"harvest_profile_search", "provider_people_search"}:
            continue
        item_filter_hints = dict(item.get("filter_hints") or default_filter_hints)
        item_employment_scope = _normalize_profile_search_employment_scope(
            item.get("employment_scope") or item.get("employment_status") or employment_scope
        )
        item_strategy_type = (
            _normalize_text(item.get("strategy_type") or strategy_type)
            or ("former_employee_search" if item_employment_scope == "former" else "scoped_search_roster")
        )
        base_company_filters = {
            "companies": list(item_filter_hints.get("current_companies") or []),
            "past_companies": list(item_filter_hints.get("past_companies") or []),
            "locations": list(item_filter_hints.get("locations") or []),
            "function_ids": list(item_filter_hints.get("function_ids") or []),
        }
        base_scope_companies = list(
            item_filter_hints.get("past_companies") or item_filter_hints.get("current_companies") or []
        )
        query_text = _normalize_query_text(item.get("effective_query_text") or item.get("query"))
        company_filters = {
            "companies": base_scope_companies,
            "locations": list(item_filter_hints.get("locations") or []),
            "function_ids": list(item_filter_hints.get("function_ids") or []),
            "search_query": query_text,
        }
        if item_employment_scope == "former":
            company_filters["companies"] = list(item_filter_hints.get("past_companies") or [])
        record = build_acquisition_shard_registry_record(
            target_company=target_company,
            company_key=company_key,
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope=item_employment_scope,
            strategy_type=item_strategy_type,
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
    store: ControlPlaneStore,
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
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
) -> dict[str, int]:
    try:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
            runtime_dir, target_company, snapshot_id=snapshot_id
        )
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
                result_count=_safe_int(
                    harvest_summary.get("visible_entry_count") or harvest_summary.get("raw_entry_count")
                ),
                estimated_total_count=0,
                status="completed",
                source_path=str(harvest_summary_path),
                metadata=harvest_summary,
            )
            store.upsert_acquisition_shard_registry(record)
            shard_records += 1

    for search_seed_summary in _load_search_seed_lane_summaries(snapshot_dir):
        query_summaries = list(search_seed_summary.get("query_summaries") or [])
        effective_filter_hints = dict(
            search_seed_summary.get("effective_filter_hints")
            or search_seed_summary.get("filter_hints")
            or {}
        )
        employment_scope = _infer_search_seed_summary_employment_scope(search_seed_summary)
        strategy_type = (
            _normalize_text(search_seed_summary.get("strategy_type"))
            or ("former_employee_search" if employment_scope == "former" else "scoped_search_roster")
        )
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


def _company_employee_registry_match_key(
    record: dict[str, Any],
) -> tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
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


def _search_query_registry_match_key(
    record: dict[str, Any],
) -> tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
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
            slug_key = resolve_company_alias_key(slug)
            if slug_key:
                keys.add(slug_key)
        value_key = resolve_company_alias_key(normalized)
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


def _query_filters_cover_requested(
    *,
    desired_query: Any,
    existing_query: Any,
    desired_metadata: dict[str, Any] | None = None,
    existing_metadata: dict[str, Any] | None = None,
) -> tuple[bool, tuple[int, int]]:
    desired_signature = _query_signature(desired_query)
    existing_signature = _query_signature(existing_query)
    if not desired_signature:
        return (not bool(existing_signature), (1 if not existing_signature else 0, 0))
    if desired_signature == existing_signature:
        return True, (2, 1)
    desired_families = _query_family_signatures_from_metadata(desired_metadata, fallback_query=desired_query)
    existing_families = _query_family_signatures_from_metadata(existing_metadata, fallback_query=existing_query)
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
            desired_metadata=desired_spec,
            existing_metadata=dict(row.get("metadata") or {}),
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

    preferred_filter_keyword_queries: list[str] = []
    for value in list(filter_hints.get("keywords") or []):
        canonical = _canonical_query_family_label(value)
        family_signature = next(iter(sorted(_query_family_signatures(canonical))), _query_signature(canonical))
        if not canonical or family_signature in _PROFILE_SEARCH_GENERIC_QUERY_FAMILY_SIGNATURES:
            continue
        preferred_filter_keyword_queries.append(canonical)
    if preferred_filter_keyword_queries:
        for value in preferred_filter_keyword_queries:
            _add_query(value)
    if not queries:
        for value in list(_task_metadata_value(metadata, "search_seed_queries") or []):
            _add_query(value)
    if not queries:
        for value in list(filter_hints.get("keywords") or []):
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
    effective_request = dict(synchronized.get("effective_request") or {})
    intent_view = dict(synchronized.get("intent_view") or {})
    if effective_request:
        synchronized["effective_request"] = build_effective_request_payload(effective_request)
    if not intent_view:
        return synchronized
    if "filter_hints" in synchronized:
        intent_view["filter_hints"] = dict(synchronized.get("filter_hints") or {})
        intent_view["filter_keywords"] = list(dict(synchronized.get("filter_hints") or {}).get("keywords") or [])
        intent_view["function_ids"] = list(dict(synchronized.get("filter_hints") or {}).get("function_ids") or [])
    if "search_seed_queries" in synchronized:
        intent_view["search_seed_queries"] = [
            str(item).strip() for item in list(synchronized.get("search_seed_queries") or []) if str(item).strip()
        ]
    if "search_query_bundles" in synchronized:
        intent_view["search_query_bundles"] = [
            dict(item) for item in list(synchronized.get("search_query_bundles") or []) if isinstance(item, dict)
        ]
    if "cost_policy" in synchronized:
        intent_view["cost_policy"] = dict(synchronized.get("cost_policy") or {})
    if "search_channel_order" in synchronized:
        intent_view["search_channel_order"] = [
            str(item).strip() for item in list(synchronized.get("search_channel_order") or []) if str(item).strip()
        ]
    if "employment_statuses" in synchronized:
        intent_view["employment_statuses"] = [
            str(item).strip() for item in list(synchronized.get("employment_statuses") or []) if str(item).strip()
        ]
    if "company_scope" in synchronized:
        intent_view["company_scope"] = list(synchronized.get("company_scope") or [])
    if "strategy_type" in synchronized:
        intent_view["strategy_type"] = str(synchronized.get("strategy_type") or "").strip()
    if "organization_execution_profile" in synchronized:
        intent_view["organization_execution_profile"] = dict(synchronized.get("organization_execution_profile") or {})
    if "strategy_decision_explanation" in synchronized:
        intent_view["strategy_decision_explanation"] = dict(synchronized.get("strategy_decision_explanation") or {})
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
            dict(item) for item in list(synchronized.get("company_employee_shards") or []) if isinstance(item, dict)
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
        intent_view["company_employee_shard_policy"] = dict(synchronized.get("company_employee_shard_policy") or {})
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
    if synchronized.get("effective_request"):
        synchronized["effective_request"] = build_effective_request_payload(
            synchronized.get("effective_request") or {},
            intent_view=intent_view,
        )
    return synchronized


def _filter_query_bundles_for_queries(
    query_bundles: list[dict[str, Any]] | None,
    allowed_queries: list[str] | None,
) -> list[dict[str, Any]]:
    allowed_signatures = {_query_signature(item) for item in list(allowed_queries or []) if _query_signature(item)}
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
                "coverage_mode": _normalize_text(row.get("coverage_mode")),
                "materialization_generation_key": _normalize_text(row.get("materialization_generation_key")),
                "materialization_generation_sequence": _safe_int(row.get("materialization_generation_sequence")),
                "materialization_watermark": _normalize_text(row.get("materialization_watermark")),
                "exact_overlap": dict(row.get("exact_overlap") or {}),
                "exact_overlap_gap": dict(row.get("exact_overlap_gap") or {}),
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


def _augment_lane_coverage_from_registry_rows(
    lane_coverage: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    lane = dict(lane_coverage or {})
    if not candidate_rows:
        return lane
    effective_candidate_count = max(
        _safe_int(lane.get("effective_candidate_count")),
        _safe_int(lane.get("exact_member_count")),
        _safe_int(lane.get("inferred_candidate_count")),
    )
    standard_bundle_ready_count = _safe_int(lane.get("standard_bundle_ready_count"))
    standard_bundle_candidate_count = _safe_int(lane.get("standard_bundle_candidate_count"))
    for row in list(candidate_rows or []):
        row_payload = dict(row or {})
        metadata = dict(row_payload.get("metadata") or {})
        standard_bundle = dict(metadata.get("standard_bundle") or {})
        bundle_candidate_count = _safe_int(standard_bundle.get("bundle_candidate_count"))
        row_result_count = _safe_int(row_payload.get("result_count"))
        effective_candidate_count = max(effective_candidate_count, bundle_candidate_count, row_result_count)
        standard_bundle_candidate_count = max(
            standard_bundle_candidate_count,
            bundle_candidate_count,
        )
        if standard_bundle:
            standard_bundle_ready_count += 1
    if effective_candidate_count > _safe_int(lane.get("effective_candidate_count")):
        lane["effective_candidate_count"] = effective_candidate_count
    if standard_bundle_candidate_count > _safe_int(lane.get("standard_bundle_candidate_count")):
        lane["standard_bundle_candidate_count"] = standard_bundle_candidate_count
    if standard_bundle_ready_count > _safe_int(lane.get("standard_bundle_ready_count")):
        lane["standard_bundle_ready_count"] = standard_bundle_ready_count
    if effective_candidate_count > 0 or standard_bundle_ready_count > 0:
        lane["effective_ready"] = True
    if (effective_candidate_count > 0 or standard_bundle_ready_count > 0) and not lane.get("source_lane_keys"):
        lane["source_lane_keys"] = ["acquisition_shard_registry"]
    return lane


def _baseline_generation_summary(baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "generation_key": _normalize_text(baseline.get("materialization_generation_key")),
        "generation_sequence": _safe_int(baseline.get("materialization_generation_sequence")),
        "generation_watermark": _normalize_text(baseline.get("materialization_watermark")),
    }


def _exact_overlap_scope(value: Any) -> str:
    normalized = _normalize_text(value).lower()
    if normalized == "former":
        return "former"
    if normalized == "current":
        return "current"
    return ""


def _compare_baseline_generation_to_row(
    *,
    store: ControlPlaneStore,
    baseline: dict[str, Any],
    row: dict[str, Any],
    desired_spec: dict[str, Any],
) -> dict[str, Any]:
    baseline_generation_key = _normalize_text(baseline.get("materialization_generation_key"))
    row_generation_key = _normalize_text(row.get("materialization_generation_key"))
    if not baseline_generation_key or not row_generation_key:
        return {}
    employment_scope = _exact_overlap_scope(desired_spec.get("employment_scope")) or _exact_overlap_scope(
        row.get("employment_scope")
    )
    exact_overlap = store.compare_asset_membership_generations(
        primary_generation_key=baseline_generation_key,
        secondary_generation_key=row_generation_key,
        primary_employment_scope=employment_scope,
        secondary_employment_scope=employment_scope,
    )
    if not exact_overlap:
        return {}
    exact_overlap["comparison_mode"] = "asset_membership_index"
    exact_overlap["baseline_generation_watermark"] = _normalize_text(baseline.get("materialization_watermark"))
    exact_overlap["row_generation_watermark"] = _normalize_text(row.get("materialization_watermark"))
    exact_overlap["baseline_snapshot_id"] = _normalize_text(baseline.get("snapshot_id"))
    exact_overlap["row_snapshot_id"] = _normalize_text(row.get("snapshot_id"))
    exact_overlap["baseline_subsumes_row"] = bool(exact_overlap.get("primary_subsumes_secondary"))
    return exact_overlap


def _exact_overlap_gap_allows_same_snapshot_reuse(
    *,
    baseline: dict[str, Any],
    row: dict[str, Any],
    exact_overlap: dict[str, Any],
) -> bool:
    if not bool(dict(baseline or {}).get("authoritative")):
        return False
    baseline_snapshot_id = _normalize_text(dict(baseline or {}).get("snapshot_id"))
    row_snapshot_id = _normalize_text(dict(row or {}).get("snapshot_id"))
    if not baseline_snapshot_id or baseline_snapshot_id != row_snapshot_id:
        return False
    secondary_member_count = _safe_int(exact_overlap.get("secondary_member_count"))
    secondary_only_member_count = _safe_int(exact_overlap.get("secondary_only_member_count"))
    secondary_overlap_ratio = _safe_float(exact_overlap.get("secondary_overlap_ratio"))
    if secondary_member_count <= 0 or secondary_only_member_count <= 0:
        return False
    if secondary_only_member_count > _EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MAX_SECONDARY_ONLY_COUNT:
        return False
    if secondary_overlap_ratio < _EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MIN_SECONDARY_OVERLAP_RATIO:
        return False
    return (
        secondary_only_member_count / max(secondary_member_count, 1)
        <= _EXACT_MEMBERSHIP_NEAR_SUBSUMPTION_MAX_SECONDARY_ONLY_RATIO
    )


def _classify_matched_row_against_baseline(
    *,
    store: ControlPlaneStore,
    baseline: dict[str, Any],
    matched_row: dict[str, Any],
    desired_spec: dict[str, Any],
    query_or_shard: str,
) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    row_payload = dict(matched_row or {})
    if not row_payload:
        return False, {}, {}
    selected_snapshot_ids = _normalize_string_list(
        baseline.get("selected_snapshot_ids") or [baseline.get("snapshot_id")]
    )
    baseline_snapshot_id = _normalize_text(baseline.get("snapshot_id"))
    row_snapshot_id = _normalize_text(row_payload.get("snapshot_id"))
    if row_snapshot_id and row_snapshot_id != baseline_snapshot_id and row_snapshot_id in selected_snapshot_ids:
        row_payload["coverage_mode"] = "selected_snapshot_registry_match"
        return True, row_payload, {}
    exact_overlap = _compare_baseline_generation_to_row(
        store=store,
        baseline=baseline,
        row=row_payload,
        desired_spec=desired_spec,
    )
    if not exact_overlap:
        row_payload["coverage_mode"] = "heuristic_registry_match"
        return True, row_payload, {}
    row_payload["coverage_mode"] = "exact_membership_subsumption"
    row_payload["exact_overlap"] = exact_overlap
    if bool(exact_overlap.get("baseline_subsumes_row")):
        return True, row_payload, {}
    if _exact_overlap_gap_allows_same_snapshot_reuse(
        baseline=baseline,
        row=row_payload,
        exact_overlap=exact_overlap,
    ):
        exact_overlap["baseline_nearly_subsumes_row"] = True
        row_payload["coverage_mode"] = "exact_membership_near_subsumption"
        row_payload["exact_overlap"] = exact_overlap
        return True, row_payload, {}
    gap_payload = {
        "query_or_shard": _normalize_query_text(query_or_shard),
        "search_query": _normalize_query_text(row_payload.get("search_query") or desired_spec.get("search_query")),
        "shard_id": _normalize_text(row_payload.get("shard_id")),
        "shard_title": _normalize_text(row_payload.get("shard_title")),
        "snapshot_id": _normalize_text(row_payload.get("snapshot_id")),
        "lane": _normalize_text(row_payload.get("lane")),
        "employment_scope": _normalize_text(row_payload.get("employment_scope"))
        or _normalize_text(desired_spec.get("employment_scope")),
        "coverage_mode": "exact_membership_gap",
        "gap_reason": "baseline_generation_missing_members",
        "exact_overlap": exact_overlap,
        "materialization_generation_key": _normalize_text(row_payload.get("materialization_generation_key")),
        "materialization_generation_sequence": _safe_int(row_payload.get("materialization_generation_sequence")),
        "materialization_watermark": _normalize_text(row_payload.get("materialization_watermark")),
    }
    row_payload["exact_overlap_gap"] = gap_payload
    return False, row_payload, gap_payload


def _exact_overlap_summary(
    rows: list[dict[str, Any]],
    gap_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    exact_rows = [
        row for row in list(rows or []) if str(row.get("coverage_mode") or "").strip() == "exact_membership_subsumption"
    ]
    near_subsumed_rows = [
        row
        for row in list(rows or [])
        if str(row.get("coverage_mode") or "").strip() == "exact_membership_near_subsumption"
    ]
    heuristic_rows = [
        row for row in list(rows or []) if str(row.get("coverage_mode") or "").strip() == "heuristic_registry_match"
    ]
    selected_snapshot_rows = [
        row
        for row in list(rows or [])
        if str(row.get("coverage_mode") or "").strip() == "selected_snapshot_registry_match"
    ]
    return {
        "exact_subsumed_count": len(exact_rows),
        "near_subsumed_count": len(near_subsumed_rows),
        "heuristic_match_count": len(heuristic_rows),
        "selected_snapshot_match_count": len(selected_snapshot_rows),
        "gap_count": len(list(gap_rows or [])),
        "comparison_available": bool(exact_rows or near_subsumed_rows or selected_snapshot_rows or gap_rows),
    }


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


def _synthetic_baseline_company_employee_shard_coverage_rows(
    *,
    target_company: str,
    baseline_snapshot_id: str,
    shards: list[dict[str, Any]],
    baseline_completeness_ledger_path: str,
    coverage_reason: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for shard in list(shards or []):
        if not isinstance(shard, dict):
            continue
        company_filters = dict(shard.get("company_filters") or {})
        row = build_acquisition_shard_registry_record(
            target_company=target_company,
            company_key="",
            snapshot_id=baseline_snapshot_id,
            lane="company_employees",
            employment_scope="current",
            strategy_type=str(shard.get("strategy_id") or ""),
            shard_id=str(shard.get("shard_id") or ""),
            shard_title=str(shard.get("title") or shard.get("shard_id") or ""),
            search_query=str(company_filters.get("search_query") or ""),
            company_filters=company_filters,
            result_count=0,
            status="embedded_baseline_reuse",
        )
        row["metadata"] = {
            **dict(row.get("metadata") or {}),
            "embedded_baseline_reuse": True,
            "coverage_reason": _normalize_text(coverage_reason),
            "coverage_source": "organization_completeness_ledger",
            "baseline_completeness_ledger_path": _normalize_text(baseline_completeness_ledger_path),
        }
        rows.append(row)
    return rows


def _company_employee_shard_is_structural_partition(shard: dict[str, Any]) -> bool:
    shard_payload = dict(shard or {})
    company_filters = dict(shard_payload.get("company_filters") or {})
    search_query = _normalize_query_text(shard_payload.get("search_query") or company_filters.get("search_query"))
    return not bool(search_query)


def _baseline_embedded_former_sufficient(
    *,
    baseline_candidate_count: int,
    former_lane_coverage: dict[str, Any],
    baseline_completeness_score: float = 0.0,
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
    if baseline_completeness_score >= 45.0:
        coverage_ratio = effective_candidate_count / max(baseline_candidate_count, 1)
        if baseline_candidate_count <= 2500:
            if effective_candidate_count >= max(minimum_candidate_count, 30) and coverage_ratio >= 0.05:
                return True
        else:
            if effective_candidate_count >= max(minimum_candidate_count, 100) and coverage_ratio >= 0.02:
                return True
    if inferred_candidate_count <= 0 or inferred_linkedin_url_count <= 0:
        return False
    return bool(
        inferred_profile_detail_count >= minimum_profile_detail_count
        or inferred_profile_detail_count >= max(1, int(round(inferred_candidate_count * 0.35)))
    )


def _baseline_embedded_current_sufficient(
    *,
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    baseline_completeness_score: float = 0.0,
) -> bool:
    if baseline_candidate_count <= 0:
        return False
    current_lane = dict(current_lane_coverage or {})
    if not bool(current_lane.get("effective_ready")):
        return False
    company_employees_current = dict(current_lane.get("company_employees_current") or {})
    if company_employees_current:
        effective_candidate_count = _safe_int(
            company_employees_current.get("effective_candidate_count")
            or company_employees_current.get("inferred_candidate_count")
        )
        if effective_candidate_count > 0 and (
            bool(company_employees_current.get("effective_ready"))
            or bool(company_employees_current.get("inferred_ready"))
        ):
            return True
    effective_candidate_count = _safe_int(current_lane.get("effective_candidate_count"))
    inferred_candidate_count = _safe_int(current_lane.get("inferred_candidate_count"))
    if effective_candidate_count <= 0:
        return False
    if baseline_candidate_count <= 500:
        return bool(
            effective_candidate_count >= min(3, baseline_candidate_count)
            or inferred_candidate_count >= min(3, baseline_candidate_count)
        )
    if baseline_completeness_score < 45.0:
        return False
    coverage_ratio = effective_candidate_count / max(baseline_candidate_count, 1)
    inferred_ratio = inferred_candidate_count / max(baseline_candidate_count, 1)
    if baseline_candidate_count <= 2500:
        return bool(
            effective_candidate_count >= max(25, int(round(baseline_candidate_count * 0.2)))
            and max(coverage_ratio, inferred_ratio) >= 0.35
        )
    return bool(
        effective_candidate_count >= max(100, int(round(baseline_candidate_count * 0.15)))
        and max(coverage_ratio, inferred_ratio) >= 0.3
    )


def _authoritative_population_default_reuse_assessment(
    *,
    baseline: dict[str, Any],
    plan: SourcingPlan,
    request: JobRequest | None = None,
    current_strategy_type: str = "",
    enforce_multi_snapshot_directional_block: bool = True,
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    former_lane_coverage: dict[str, Any],
) -> dict[str, Any]:
    source_snapshot_selection = _baseline_source_snapshot_selection(baseline)
    selection_mode = _baseline_source_snapshot_selection_mode(baseline)
    selected_snapshot_ids = _normalize_string_list(
        baseline.get("selected_snapshot_ids")
        or source_snapshot_selection.get("selected_snapshot_ids")
        or [baseline.get("snapshot_id")]
    )
    multi_snapshot_aggregate_coverage_proven = _multi_snapshot_authoritative_aggregate_has_coverage_proof(
        baseline=baseline,
        selected_snapshot_ids=selected_snapshot_ids,
    )
    explicit = baseline.get("directional_local_reuse_enabled")
    if isinstance(explicit, bool):
        return {
            "eligible": explicit,
            "explicit_override": explicit,
            "minimum_candidate_floor": 0,
            "source_snapshot_selection_mode": selection_mode,
            "multi_snapshot_aggregate_coverage_proven": multi_snapshot_aggregate_coverage_proven,
            "lane_requirements_ready": explicit,
            "coverage_sufficient": explicit,
            "former_ratio_sufficient": explicit,
            "selected_snapshot_ids": selected_snapshot_ids,
        }
    summary = dict(baseline.get("summary") or {})
    explicit_summary = summary.get("directional_local_reuse_enabled")
    if isinstance(explicit_summary, bool):
        return {
            "eligible": explicit_summary,
            "explicit_override": explicit_summary,
            "minimum_candidate_floor": 0,
            "source_snapshot_selection_mode": selection_mode,
            "multi_snapshot_aggregate_coverage_proven": multi_snapshot_aggregate_coverage_proven,
            "lane_requirements_ready": explicit_summary,
            "coverage_sufficient": explicit_summary,
            "former_ratio_sufficient": explicit_summary,
            "selected_snapshot_ids": selected_snapshot_ids,
        }

    organization_execution_profile = _plan_organization_execution_profile(plan)
    org_default_mode = str(organization_execution_profile.get("default_acquisition_mode") or "").strip().lower()
    org_scale_band = str(organization_execution_profile.get("org_scale_band") or "").strip().lower()
    normalized_strategy_type = str(current_strategy_type or "").strip().lower()
    multi_snapshot_directional_blocked = bool(
        enforce_multi_snapshot_directional_block
        and normalized_strategy_type not in {"", "full_company_roster"}
        and len(selected_snapshot_ids) > 1
        and not multi_snapshot_aggregate_coverage_proven
    )
    current_lane = dict(current_lane_coverage or {})
    former_lane = dict(former_lane_coverage or {})
    lane_requirements = (
        _requested_lane_requirements(request)
        if request is not None
        else {"need_current": True, "need_former": True}
    )
    current_ready = bool(current_lane.get("effective_ready"))
    former_ready = bool(former_lane.get("effective_ready"))
    current_count = max(
        _safe_int(current_lane.get("exact_member_count")),
        _safe_int(current_lane.get("effective_candidate_count")),
    )
    former_count = max(
        _safe_int(former_lane.get("exact_member_count")),
        _safe_int(former_lane.get("effective_candidate_count")),
    )
    minimum_candidate_floor = (
        _LARGE_AUTHORITATIVE_BASELINE_LOCAL_REUSE_MIN_CANDIDATES
        if (org_scale_band == "large" or org_default_mode == "scoped_search_roster")
        else 1
    )
    need_current = bool(lane_requirements.get("need_current"))
    need_former = bool(lane_requirements.get("need_former"))
    lane_requirements_ready = (
        (not need_current or (current_ready and current_count > 0))
        and (not need_former or (former_ready and former_count > 0))
    )
    requested_effective_total = (
        (current_count if need_current else 0)
        + (former_count if need_former else 0)
    )
    if need_current and need_former:
        coverage_floor = max(1, int(round(baseline_candidate_count * _FULL_COMPANY_LANE_REUSE_MIN_COVERAGE_RATIO)))
        slack_allowance = max(10, int(round(baseline_candidate_count * 0.02)))
        coverage_sufficient = (
            requested_effective_total >= coverage_floor
            or requested_effective_total >= max(1, baseline_candidate_count - slack_allowance)
        )
    else:
        coverage_sufficient = requested_effective_total > 0
    former_ratio = former_count / max(baseline_candidate_count, 1)
    former_ratio_required = bool(
        need_current
        and need_former
        and minimum_candidate_floor > 1
        and baseline_candidate_count >= minimum_candidate_floor
    )
    former_ratio_sufficient = (
        not former_ratio_required
        or former_ratio >= _LARGE_AUTHORITATIVE_BASELINE_LOCAL_REUSE_MIN_FORMER_RATIO
    )
    eligible = bool(
        bool(baseline.get("authoritative"))
        and baseline_candidate_count >= minimum_candidate_floor
        and not multi_snapshot_directional_blocked
        and lane_requirements_ready
        and coverage_sufficient
        and former_ratio_sufficient
    )
    return {
        "eligible": eligible,
        "explicit_override": None,
        "minimum_candidate_floor": minimum_candidate_floor,
        "source_snapshot_selection_mode": selection_mode,
        "selected_snapshot_ids": selected_snapshot_ids,
        "multi_snapshot_aggregate_coverage_proven": multi_snapshot_aggregate_coverage_proven,
        "multi_snapshot_directional_blocked": multi_snapshot_directional_blocked,
        "lane_requirements_ready": lane_requirements_ready,
        "coverage_sufficient": coverage_sufficient,
        "former_ratio": round(former_ratio, 6),
        "former_ratio_required": former_ratio_required,
        "former_ratio_sufficient": former_ratio_sufficient,
        "current_effective_count": current_count,
        "former_effective_count": former_count,
    }


def _large_authoritative_baseline_local_reuse_eligible(
    *,
    baseline: dict[str, Any],
    plan: SourcingPlan,
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    former_lane_coverage: dict[str, Any],
) -> bool:
    # Compatibility wrapper kept for existing tests and callers; main planning logic
    # now uses the unified authoritative-baseline completeness contract above.
    organization_execution_profile = _plan_organization_execution_profile(plan)
    org_default_mode = str(organization_execution_profile.get("default_acquisition_mode") or "").strip().lower()
    org_scale_band = str(organization_execution_profile.get("org_scale_band") or "").strip().lower()
    if org_scale_band != "large" and org_default_mode != "scoped_search_roster":
        return False
    assessment = _authoritative_population_default_reuse_assessment(
        baseline=baseline,
        plan=plan,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return bool(assessment.get("eligible"))


def _allow_embedded_profile_query_reuse(
    *,
    plan: SourcingPlan,
    request: JobRequest | None = None,
    current_strategy_type: str = "",
    baseline: dict[str, Any],
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    former_lane_coverage: dict[str, Any],
) -> bool:
    assessment = _authoritative_population_default_reuse_assessment(
        baseline=baseline,
        plan=plan,
        request=request,
        current_strategy_type=current_strategy_type,
        enforce_multi_snapshot_directional_block=False,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return bool(assessment.get("eligible"))


def _baseline_full_company_lane_reuse_sufficient(
    *,
    request: JobRequest,
    current_strategy_type: str,
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    former_lane_coverage: dict[str, Any],
) -> bool:
    if str(request.target_scope or "").strip().lower() != "full_company_asset":
        return False
    if str(current_strategy_type or "").strip().lower() != "full_company_roster":
        return False
    lane_requirements = _requested_lane_requirements(request)
    current_lane = dict(current_lane_coverage or {})
    former_lane = dict(former_lane_coverage or {})
    current_count = max(
        _safe_int(current_lane.get("exact_member_count")),
        _safe_int(current_lane.get("effective_candidate_count")),
    )
    former_count = max(
        _safe_int(former_lane.get("exact_member_count")),
        _safe_int(former_lane.get("effective_candidate_count")),
    )
    if bool(lane_requirements.get("need_current")) and (
        not bool(current_lane.get("effective_ready")) or current_count <= 0
    ):
        return False
    if bool(lane_requirements.get("need_former")) and (
        not bool(former_lane.get("effective_ready")) or former_count <= 0
    ):
        return False
    if not (bool(lane_requirements.get("need_current")) and bool(lane_requirements.get("need_former"))):
        return True
    if baseline_candidate_count <= 0:
        return False
    combined_count = current_count + former_count
    coverage_floor = max(1, int(round(baseline_candidate_count * _FULL_COMPANY_LANE_REUSE_MIN_COVERAGE_RATIO)))
    slack_allowance = max(10, int(round(baseline_candidate_count * 0.02)))
    return combined_count >= coverage_floor or combined_count >= max(1, baseline_candidate_count - slack_allowance)


def _baseline_population_default_reuse_sufficient(
    *,
    request: JobRequest,
    plan: SourcingPlan,
    current_strategy_type: str,
    baseline: dict[str, Any],
    baseline_candidate_count: int,
    current_lane_coverage: dict[str, Any],
    former_lane_coverage: dict[str, Any],
    organization_execution_profile: dict[str, Any] | None = None,
) -> bool:
    if str(request.target_scope or "").strip().lower() != "full_company_asset":
        return False
    assessment = _authoritative_population_default_reuse_assessment(
        baseline=baseline,
        plan=plan,
        request=request,
        current_strategy_type=current_strategy_type,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    return bool(assessment.get("eligible"))


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


def _compile_asset_reuse_plan_for_baseline(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    request: JobRequest,
    plan: SourcingPlan,
    baseline: dict[str, Any],
) -> dict[str, Any]:
    baseline = dict(baseline or {})
    if not baseline:
        return {}
    selected_snapshot_ids = _normalize_string_list(
        baseline.get("selected_snapshot_ids") or [baseline.get("snapshot_id")]
    )
    ledger_result: dict[str, Any] = {}
    ledger_summary = dict(
        load_cached_organization_completeness_ledger(
            runtime_dir=runtime_dir,
            target_company=request.target_company,
            snapshot_id=str(baseline.get("snapshot_id") or ""),
            asset_view=request.asset_view,
        )
        or {}
    )
    if "summary" in ledger_summary and isinstance(ledger_summary.get("summary"), dict):
        ledger_summary = dict(ledger_summary.get("summary") or {})

    cached_lane_coverage_available = bool(
        ledger_summary
        or baseline.get("current_lane_effective_ready")
        or baseline.get("former_lane_effective_ready")
        or dict(baseline.get("current_lane_coverage") or {})
        or dict(baseline.get("former_lane_coverage") or {})
    )
    if not cached_lane_coverage_available:
        ledger_result = ensure_organization_completeness_ledger(
            runtime_dir=runtime_dir,
            store=store,
            target_company=request.target_company,
            snapshot_id=str(baseline.get("snapshot_id") or ""),
            asset_view=request.asset_view,
            selected_snapshot_ids=selected_snapshot_ids,
        )
        ledger_summary = dict(ledger_result.get("summary") or {})

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
    former_task = next(
        (task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed"), None
    )
    current_strategy_type = (
        str(_task_metadata_value(dict(current_task.metadata or {}), "strategy_type") or "") if current_task else ""
    )
    baseline_candidate_count = _safe_int(
        baseline.get("candidate_count") or dict(ledger_summary.get("organization_asset") or {}).get("candidate_count")
    )
    if not _baseline_has_large_org_reuse_coverage_contract(
        baseline=baseline,
        plan=plan,
        baseline_candidate_count=baseline_candidate_count,
        shard_rows=shard_rows,
        ledger_summary=ledger_summary,
    ):
        return {
            "baseline_reuse_available": False,
            "reason": "baseline_lacks_large_org_coverage_contract",
            "rejected_baseline_snapshot_id": str(baseline.get("snapshot_id") or ""),
            "rejected_baseline_candidate_count": baseline_candidate_count,
            "baseline_coverage_contract_ready": False,
            "_baseline_registry_id": _safe_int(baseline.get("registry_id")),
            "_baseline_registry_authoritative": bool(baseline.get("authoritative")),
            "_baseline_registry_snapshot_id": _normalize_text(baseline.get("snapshot_id")),
        }
    desired_current_specs, desired_current_shards = (
        _planned_company_employee_delta_specs(current_task) if current_task else ([], [])
    )
    desired_current_profile_specs, desired_current_profile_queries = (
        _planned_profile_search_query_specs(current_task)
        if current_task and current_strategy_type == "scoped_search_roster"
        else ([], [])
    )
    desired_former_specs, desired_former_queries = (
        _planned_profile_search_query_specs(former_task) if former_task else ([], [])
    )

    covered_current = []
    missing_current = []
    missing_current_shards = []
    current_exact_overlap_gaps: list[dict[str, Any]] = []
    for spec, shard in zip(desired_current_specs, desired_current_shards):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=current_rows,
            preferred_lane="company_employees",
        )
        if matched_row:
            is_covered, classified_row, gap_payload = _classify_matched_row_against_baseline(
                store=store,
                baseline=baseline,
                matched_row=matched_row,
                desired_spec=spec,
                query_or_shard=str(dict(shard or {}).get("title") or dict(shard or {}).get("shard_id") or ""),
            )
            if is_covered:
                covered_current.append(classified_row)
            else:
                missing_current.append(spec)
                missing_current_shards.append(shard)
                if gap_payload:
                    current_exact_overlap_gaps.append(gap_payload)
        else:
            missing_current.append(spec)
            missing_current_shards.append(shard)

    covered_current_profile = []
    missing_current_profile = []
    missing_current_profile_queries = []
    current_profile_exact_overlap_gaps: list[dict[str, Any]] = []
    for spec, query in zip(desired_current_profile_specs, desired_current_profile_queries):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=current_profile_candidate_rows,
            preferred_lane="profile_search",
        )
        if matched_row:
            is_covered, classified_row, gap_payload = _classify_matched_row_against_baseline(
                store=store,
                baseline=baseline,
                matched_row=matched_row,
                desired_spec=spec,
                query_or_shard=query,
            )
            if is_covered:
                covered_current_profile.append(classified_row)
            else:
                missing_current_profile.append(spec)
                missing_current_profile_queries.append(query)
                if gap_payload:
                    current_profile_exact_overlap_gaps.append(gap_payload)
        else:
            missing_current_profile.append(spec)
            missing_current_profile_queries.append(query)

    covered_former = []
    missing_former = []
    missing_former_queries = []
    former_exact_overlap_gaps: list[dict[str, Any]] = []
    for spec, query in zip(desired_former_specs, desired_former_queries):
        matched_row = _find_compatible_registry_row(
            desired_spec=spec,
            candidate_rows=former_profile_candidate_rows,
            preferred_lane="profile_search",
        )
        if matched_row:
            is_covered, classified_row, gap_payload = _classify_matched_row_against_baseline(
                store=store,
                baseline=baseline,
                matched_row=matched_row,
                desired_spec=spec,
                query_or_shard=query,
            )
            if is_covered:
                covered_former.append(classified_row)
            else:
                missing_former.append(spec)
                missing_former_queries.append(query)
                if gap_payload:
                    former_exact_overlap_gaps.append(gap_payload)
        else:
            missing_former.append(spec)
            missing_former_queries.append(query)

    baseline_completeness_ledger_path = str(ledger_result.get("ledger_path") or "")
    former_lane_coverage = _registry_or_ledger_lane_coverage(
        baseline=baseline,
        ledger_summary=ledger_summary,
        lane_key="profile_search_former",
        registry_key="former_lane_coverage",
    )
    former_lane_coverage = _augment_lane_coverage_from_registry_rows(
        former_lane_coverage,
        former_profile_candidate_rows,
    )
    current_lane_coverage = _registry_or_ledger_lane_coverage(
        baseline=baseline,
        ledger_summary=ledger_summary,
        lane_key="current_effective",
        registry_key="current_lane_coverage",
    )
    current_lane_coverage = _augment_lane_coverage_from_registry_rows(
        current_lane_coverage,
        current_profile_candidate_rows,
    )
    baseline_current_embedded_sufficient = _baseline_embedded_current_sufficient(
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        baseline_completeness_score=_safe_float(baseline.get("completeness_score")),
    )
    baseline_full_company_lane_reuse_sufficient = _baseline_full_company_lane_reuse_sufficient(
        request=request,
        current_strategy_type=current_strategy_type,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    allow_current_embedded_query_reuse = _allow_embedded_profile_query_reuse(
        plan=plan,
        request=request,
        current_strategy_type=current_strategy_type,
        baseline=baseline,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    if missing_current_shards and baseline_full_company_lane_reuse_sufficient:
        reusable_current_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        residual_current_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        reusable_current_gap_keys: set[str] = set()
        for spec, shard in zip(missing_current, missing_current_shards):
            shard_title = _normalize_query_text(
                dict(shard or {}).get("title") or dict(shard or {}).get("shard_id") or ""
            )
            if _company_employee_shard_is_structural_partition(shard):
                reusable_current_pairs.append((spec, shard))
                if shard_title:
                    reusable_current_gap_keys.add(shard_title)
            else:
                residual_current_pairs.append((spec, shard))
        if reusable_current_pairs:
            covered_current.extend(
                _synthetic_baseline_company_employee_shard_coverage_rows(
                    target_company=request.target_company,
                    baseline_snapshot_id=str(baseline.get("snapshot_id") or ""),
                    shards=[shard for _, shard in reusable_current_pairs],
                    baseline_completeness_ledger_path=baseline_completeness_ledger_path,
                    coverage_reason="authoritative_full_company_lane_reuse",
                )
            )
            missing_current = [spec for spec, _ in residual_current_pairs]
            missing_current_shards = [shard for _, shard in residual_current_pairs]
            if reusable_current_gap_keys:
                current_exact_overlap_gaps = [
                    gap
                    for gap in current_exact_overlap_gaps
                    if _normalize_query_text(gap.get("query_or_shard")) not in reusable_current_gap_keys
                ]
    plan_payload = _sourcing_plan_payload(plan)
    organization_execution_profile = dict(
        plan_payload.get("organization_execution_profile") or getattr(plan, "organization_execution_profile", {}) or {}
    )
    prefer_delta_from_baseline = bool(organization_execution_profile.get("prefer_delta_from_baseline"))
    baseline_population_default_reuse_assessment = _authoritative_population_default_reuse_assessment(
        request=request,
        plan=plan,
        current_strategy_type=current_strategy_type,
        baseline=baseline,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    baseline_population_default_reuse_sufficient = bool(
        baseline_population_default_reuse_assessment.get("eligible")
    )
    allow_population_default_profile_query_reuse = bool(
        baseline_population_default_reuse_sufficient and not prefer_delta_from_baseline
    )
    if missing_current_profile_queries:
        if baseline_full_company_lane_reuse_sufficient or allow_population_default_profile_query_reuse:
            covered_current_profile.extend(
                _synthetic_baseline_query_coverage_rows(
                    target_company=request.target_company,
                    baseline_snapshot_id=str(baseline.get("snapshot_id") or ""),
                    queries=missing_current_profile_queries,
                    employment_scope="current",
                    baseline_completeness_ledger_path=baseline_completeness_ledger_path,
                    coverage_reason="authoritative_full_company_lane_reuse",
                )
            )
            missing_current_profile = []
            missing_current_profile_queries = []
            current_profile_exact_overlap_gaps = []
        elif baseline_current_embedded_sufficient and allow_current_embedded_query_reuse:
            covered_current_profile.extend(
                _synthetic_baseline_query_coverage_rows(
                    target_company=request.target_company,
                    baseline_snapshot_id=str(baseline.get("snapshot_id") or ""),
                    queries=missing_current_profile_queries,
                    employment_scope="current",
                    baseline_completeness_ledger_path=baseline_completeness_ledger_path,
                    coverage_reason="authoritative_baseline_embeds_current_lane",
                )
            )
            missing_current_profile = []
            missing_current_profile_queries = []

    baseline_former_embedded_sufficient = False
    allow_former_embedded_query_reuse = _allow_embedded_profile_query_reuse(
        plan=plan,
        request=request,
        current_strategy_type=current_strategy_type,
        baseline=baseline,
        baseline_candidate_count=baseline_candidate_count,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    allow_population_default_former_query_reuse = bool(
        allow_population_default_profile_query_reuse
        and (current_strategy_type == "full_company_roster" or not prefer_delta_from_baseline)
    )
    if missing_former_queries and (
        baseline_full_company_lane_reuse_sufficient or allow_population_default_former_query_reuse
    ):
        covered_former.extend(
            _synthetic_baseline_query_coverage_rows(
                target_company=request.target_company,
                baseline_snapshot_id=str(baseline.get("snapshot_id") or ""),
                queries=missing_former_queries,
                employment_scope="former",
                baseline_completeness_ledger_path=baseline_completeness_ledger_path,
                coverage_reason="authoritative_full_company_lane_reuse",
            )
        )
        missing_former = []
        missing_former_queries = []
        former_exact_overlap_gaps = []
        baseline_former_embedded_sufficient = True
    if missing_former_queries and allow_former_embedded_query_reuse:
        baseline_former_embedded_sufficient = _baseline_embedded_former_sufficient(
            baseline_candidate_count=baseline_candidate_count,
            former_lane_coverage=former_lane_coverage,
            baseline_completeness_score=_safe_float(baseline.get("completeness_score")),
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

    requires_delta_acquisition = bool(
        missing_current_shards or missing_current_profile_queries or missing_former_queries
    )
    baseline_readiness = str(ledger_summary.get("readiness") or "")
    plan_payload = {
        "baseline_reuse_available": True,
        "baseline_snapshot_id": str(baseline.get("snapshot_id") or ""),
        "baseline_asset_view": str(baseline.get("asset_view") or request.asset_view or "canonical_merged"),
        "baseline_generation_key": _normalize_text(baseline.get("materialization_generation_key")),
        "baseline_generation_sequence": _safe_int(baseline.get("materialization_generation_sequence")),
        "baseline_generation_watermark": _normalize_text(baseline.get("materialization_watermark")),
        "baseline_candidate_count": baseline_candidate_count,
        "baseline_completeness_score": _safe_float(baseline.get("completeness_score")),
        "baseline_completeness_band": str(baseline.get("completeness_band") or ""),
        "baseline_completeness_ledger_path": baseline_completeness_ledger_path,
        "baseline_readiness": baseline_readiness,
        "baseline_delta_planning_ready": bool(ledger_summary.get("delta_planning_ready")),
        "baseline_standard_bundle_count": _safe_int(
            dict(ledger_summary.get("standard_bundles") or {}).get("bundle_count")
        ),
        "baseline_current_bundle_ready_count": _safe_int(
            dict(dict(ledger_summary.get("lane_coverage") or {}).get("company_employees_current") or {}).get(
                "standard_bundle_ready_count"
            )
        ),
        "baseline_former_bundle_ready_count": _safe_int(
            dict(dict(ledger_summary.get("lane_coverage") or {}).get("profile_search_former") or {}).get(
                "standard_bundle_ready_count"
            )
        ),
        "baseline_current_effective_candidate_count": _safe_int(
            baseline.get("current_lane_effective_candidate_count")
            or current_lane_coverage.get("effective_candidate_count")
        ),
        "baseline_former_effective_candidate_count": _safe_int(
            baseline.get("former_lane_effective_candidate_count")
            or former_lane_coverage.get("effective_candidate_count")
        ),
        "baseline_current_effective_ready": bool(
            baseline.get("current_lane_effective_ready") or current_lane_coverage.get("effective_ready")
        ),
        "baseline_former_effective_ready": bool(
            baseline.get("former_lane_effective_ready") or former_lane_coverage.get("effective_ready")
        ),
        "baseline_current_inferred_candidate_count": _safe_int(current_lane_coverage.get("inferred_candidate_count")),
        "baseline_former_inferred_candidate_count": _safe_int(former_lane_coverage.get("inferred_candidate_count")),
        "baseline_former_inferred_profile_detail_count": _safe_int(
            former_lane_coverage.get("inferred_profile_detail_count")
        ),
        "baseline_current_embedded_sufficient": baseline_current_embedded_sufficient,
        "baseline_former_embedded_sufficient": baseline_former_embedded_sufficient,
        "baseline_full_company_lane_reuse_sufficient": baseline_full_company_lane_reuse_sufficient,
        "baseline_population_default_reuse_sufficient": baseline_population_default_reuse_sufficient,
        "baseline_source_snapshot_selection_mode": str(
            baseline_population_default_reuse_assessment.get("source_snapshot_selection_mode") or ""
        ),
        "baseline_multi_snapshot_aggregate_coverage_proven": bool(
            baseline_population_default_reuse_assessment.get("multi_snapshot_aggregate_coverage_proven")
        ),
        "baseline_multi_snapshot_directional_blocked": bool(
            baseline_population_default_reuse_assessment.get("multi_snapshot_directional_blocked")
        ),
        "baseline_current_embedded_query_reuse_allowed": allow_current_embedded_query_reuse,
        "baseline_former_embedded_query_reuse_allowed": allow_former_embedded_query_reuse,
        "baseline_directional_local_reuse_eligible": bool(
            baseline_population_default_reuse_sufficient
            or allow_current_embedded_query_reuse
            or allow_former_embedded_query_reuse
        ),
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
        "current_company_employee_exact_overlap_gaps": current_exact_overlap_gaps,
        "current_profile_search_exact_overlap_gaps": current_profile_exact_overlap_gaps,
        "former_profile_search_exact_overlap_gaps": former_exact_overlap_gaps,
        "current_company_employee_exact_overlap_summary": _exact_overlap_summary(
            covered_current,
            current_exact_overlap_gaps,
        ),
        "current_profile_search_exact_overlap_summary": _exact_overlap_summary(
            covered_current_profile,
            current_profile_exact_overlap_gaps,
        ),
        "former_profile_search_exact_overlap_summary": _exact_overlap_summary(
            covered_former,
            former_exact_overlap_gaps,
        ),
        "requires_delta_acquisition": requires_delta_acquisition,
        "baseline_sufficiency": ("delta_required" if requires_delta_acquisition else (baseline_readiness or "ready")),
        "baseline_resolution_mode": ("repaired_missing_cache" if not cached_lane_coverage_available else "cached_only"),
        "planner_mode": ("delta_from_snapshot" if requires_delta_acquisition else "reuse_snapshot_only"),
    }
    plan_payload["baseline_selection_explanation"] = build_asset_reuse_baseline_selection_explanation(
        request=request,
        baseline=baseline,
        asset_reuse_plan=plan_payload,
        current_lane_coverage=current_lane_coverage,
        former_lane_coverage=former_lane_coverage,
    )
    plan_payload["_baseline_registry_id"] = _safe_int(baseline.get("registry_id"))
    plan_payload["_baseline_registry_authoritative"] = bool(baseline.get("authoritative"))
    plan_payload["_baseline_registry_snapshot_id"] = _normalize_text(baseline.get("snapshot_id"))
    return plan_payload


def _asset_reuse_plan_missing_count(plan_payload: dict[str, Any]) -> int:
    payload = dict(plan_payload or {})
    return (
        _safe_int(payload.get("missing_current_company_employee_shard_count"))
        + _safe_int(payload.get("missing_current_profile_search_query_count"))
        + _safe_int(payload.get("missing_former_profile_search_query_count"))
    )


def _asset_reuse_plan_effective_candidate_total(plan_payload: dict[str, Any]) -> int:
    payload = dict(plan_payload or {})
    return (
        _safe_int(payload.get("baseline_current_effective_candidate_count"))
        + _safe_int(payload.get("baseline_former_effective_candidate_count"))
    )


def _asset_reuse_plan_candidate_is_better(candidate: dict[str, Any], incumbent: dict[str, Any]) -> bool:
    candidate_payload = dict(candidate or {})
    incumbent_payload = dict(incumbent or {})
    if not incumbent_payload:
        return bool(candidate_payload)
    candidate_available = bool(candidate_payload.get("baseline_reuse_available"))
    incumbent_available = bool(incumbent_payload.get("baseline_reuse_available"))
    if candidate_available != incumbent_available:
        return candidate_available
    candidate_requires_delta = bool(candidate_payload.get("requires_delta_acquisition"))
    incumbent_requires_delta = bool(incumbent_payload.get("requires_delta_acquisition"))
    if candidate_requires_delta != incumbent_requires_delta:
        return not candidate_requires_delta
    candidate_missing_count = _asset_reuse_plan_missing_count(candidate_payload)
    incumbent_missing_count = _asset_reuse_plan_missing_count(incumbent_payload)
    if candidate_missing_count != incumbent_missing_count:
        return candidate_missing_count < incumbent_missing_count
    candidate_population_default = bool(candidate_payload.get("baseline_population_default_reuse_sufficient"))
    incumbent_population_default = bool(incumbent_payload.get("baseline_population_default_reuse_sufficient"))
    if candidate_population_default != incumbent_population_default:
        return candidate_population_default
    candidate_effective_total = _asset_reuse_plan_effective_candidate_total(candidate_payload)
    incumbent_effective_total = _asset_reuse_plan_effective_candidate_total(incumbent_payload)
    if candidate_effective_total != incumbent_effective_total:
        return candidate_effective_total > incumbent_effective_total
    candidate_candidate_count = _safe_int(candidate_payload.get("baseline_candidate_count"))
    incumbent_candidate_count = _safe_int(incumbent_payload.get("baseline_candidate_count"))
    if candidate_candidate_count != incumbent_candidate_count:
        return candidate_candidate_count > incumbent_candidate_count
    candidate_completeness = _safe_float(candidate_payload.get("baseline_completeness_score"))
    incumbent_completeness = _safe_float(incumbent_payload.get("baseline_completeness_score"))
    if candidate_completeness != incumbent_completeness:
        return candidate_completeness > incumbent_completeness
    candidate_authoritative = bool(candidate_payload.get("_baseline_registry_authoritative"))
    incumbent_authoritative = bool(incumbent_payload.get("_baseline_registry_authoritative"))
    if candidate_authoritative != incumbent_authoritative:
        return candidate_authoritative
    return _normalize_text(candidate_payload.get("_baseline_registry_snapshot_id")) > _normalize_text(
        incumbent_payload.get("_baseline_registry_snapshot_id")
    )


def _strip_asset_reuse_plan_private_fields(plan_payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(plan_payload or {})
    for key in (
        "_baseline_registry_id",
        "_baseline_registry_authoritative",
        "_baseline_registry_snapshot_id",
    ):
        cleaned.pop(key, None)
    return cleaned


def compile_asset_reuse_plan(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
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

    candidate_inventory = build_organization_asset_registry_candidate_inventory(
        store=store,
        target_company=request.target_company,
        asset_view=request.asset_view,
    )
    authoritative_baseline = dict(candidate_inventory.get("authoritative_row") or {})
    if not authoritative_baseline:
        return {
            "baseline_reuse_available": False,
            "reason": "no_cached_authoritative_baseline",
        }

    best_plan: dict[str, Any] = {}
    for candidate_row in list(candidate_inventory.get("ordered_candidate_rows") or []):
        candidate_baseline = dict(candidate_row or {})
        if not candidate_baseline:
            continue
        candidate_plan = _compile_asset_reuse_plan_for_baseline(
            runtime_dir=runtime_dir,
            store=store,
            request=request,
            plan=plan,
            baseline=candidate_baseline,
        )
        if not best_plan or _asset_reuse_plan_candidate_is_better(candidate_plan, best_plan):
            best_plan = candidate_plan

    if not best_plan:
        best_plan = _compile_asset_reuse_plan_for_baseline(
            runtime_dir=runtime_dir,
            store=store,
            request=request,
            plan=plan,
            baseline=authoritative_baseline,
        )

    return _strip_asset_reuse_plan_private_fields(best_plan)


def apply_asset_reuse_plan_to_sourcing_plan(plan: SourcingPlan, asset_reuse_plan: dict[str, Any]) -> SourcingPlan:
    plan.asset_reuse_plan = dict(asset_reuse_plan or {})
    if not asset_reuse_plan:
        return plan
    baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
    requires_delta_acquisition = bool(asset_reuse_plan.get("requires_delta_acquisition"))
    baseline_population_default_reuse_sufficient = bool(
        asset_reuse_plan.get("baseline_population_default_reuse_sufficient")
    )
    baseline_selection_explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
    if baseline_snapshot_id and not requires_delta_acquisition and baseline_population_default_reuse_sufficient:
        plan.acquisition_strategy.strategy_type = "full_company_roster"
        plan.acquisition_strategy.reasoning = list(plan.acquisition_strategy.reasoning or []) + [
            "Authoritative company baseline already covers the full population, so this plan defaults to local asset reuse."
        ]
        strategy_explanation = dict(plan.acquisition_strategy.strategy_decision_explanation or {})
        override_reasons = list(strategy_explanation.get("override_reason_codes") or [])
        if "authoritative_population_default_reuse" not in override_reasons:
            override_reasons.append("authoritative_population_default_reuse")
        strategy_explanation["override_reason_codes"] = override_reasons
        strategy_explanation["effective_strategy_type"] = "full_company_roster"
        strategy_explanation["effective_strategy_source"] = "asset_reuse_plan"
        plan.acquisition_strategy.strategy_decision_explanation = strategy_explanation
    if baseline_snapshot_id:
        plan.assumptions = list(plan.assumptions or []) + [
            f"Planner selected baseline snapshot {baseline_snapshot_id} for company-asset reuse."
        ]
    for task in plan.acquisition_tasks:
        metadata = dict(task.metadata or {})
        strategy_type = str(_task_metadata_value(metadata, "strategy_type") or "")
        missing_current_company_employee_shards = list(
            asset_reuse_plan.get("missing_current_company_employee_shards") or []
        )
        missing_current_profile_search_queries = list(
            asset_reuse_plan.get("missing_current_profile_search_queries") or []
        )
        missing_former_profile_search_queries = list(
            asset_reuse_plan.get("missing_former_profile_search_queries") or []
        )
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
                "baseline_completeness_ledger_path": str(
                    asset_reuse_plan.get("baseline_completeness_ledger_path") or ""
                ),
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
            if (
                requires_delta_acquisition
                and strategy_type == "full_company_roster"
                and not missing_current_company_employee_shards
            ):
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
                "baseline_completeness_ledger_path": str(
                    asset_reuse_plan.get("baseline_completeness_ledger_path") or ""
                ),
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
