from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .request_matching import (
    MATCH_THRESHOLD,
    build_request_matching_bundle,
    request_family_score,
    request_family_signature,
    request_signature,
)


DEFAULT_HIGH_THRESHOLD = 0.75
DEFAULT_MEDIUM_THRESHOLD = 0.45
MIN_HIGH_THRESHOLD = 0.63
MAX_HIGH_THRESHOLD = 0.87
MIN_MEDIUM_THRESHOLD = 0.35
MAX_MEDIUM_THRESHOLD = 0.58
MIN_THRESHOLD_GAP = 0.15
DECAY_HALF_LIFE_DAYS = 45.0
MIN_TIME_DECAY_WEIGHT = 0.2
NO_CONTEXT_FAMILY_WEIGHT = 0.35

RECALL_FEEDBACK_WEIGHTS = {
    "must_have_signal": 1.0,
    "false_negative_pattern": 1.0,
    "confidence_boost_signal": 1.2,
    "accepted_alias": 0.25,
}

PRECISION_FEEDBACK_WEIGHTS = {
    "exclude_signal": 1.0,
    "false_positive_pattern": 1.0,
    "confidence_penalty_signal": 1.2,
    "rejected_alias": 0.25,
}


def build_confidence_policy(
    *,
    target_company: str,
    feedback_items: list[dict[str, Any]],
    request_payload: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    recall_pressure = 0.0
    precision_pressure = 0.0
    feedback_counts: dict[str, int] = {}
    weighted_feedback_counts: dict[str, float] = {}
    reasons: list[str] = []
    applied_feedback_count = 0
    exact_family_feedback_count = 0
    related_family_feedback_count = 0
    company_fallback_feedback_count = 0
    decayed_feedback_count = 0
    applied_feedback_weight = 0.0
    scope_kind = "request_family" if request_payload else "company"
    request_sig = request_signature(request_payload or {}) if request_payload else ""
    request_family_sig = request_family_signature(request_payload or {}) if request_payload else ""
    request_matching = build_request_matching_bundle(request_payload or {}) if request_payload else {}
    matching_request_sig = str(request_matching.get("matching_request_signature") or request_sig)
    matching_request_family_sig = str(request_matching.get("matching_request_family_signature") or request_family_sig)

    for item in feedback_items:
        feedback_type = str(item.get("feedback_type") or "").strip()
        if not feedback_type:
            continue
        feedback_counts[feedback_type] = feedback_counts.get(feedback_type, 0) + 1
        metadata = dict(item.get("metadata") or {})
        family_weight, family_reason, family_bucket = _family_relevance_weight(
            item,
            request_payload=request_payload or {},
            request_sig=request_sig,
            request_family_sig=request_family_sig,
            matching_request_sig=matching_request_sig,
            matching_request_family_sig=matching_request_family_sig,
        )
        time_weight, age_days = _time_decay_weight(item.get("created_at"), now=now)
        effective_weight = round(family_weight * time_weight, 4)
        if effective_weight <= 0:
            continue
        applied_feedback_count += 1
        applied_feedback_weight += effective_weight
        weighted_feedback_counts[feedback_type] = round(weighted_feedback_counts.get(feedback_type, 0.0) + effective_weight, 4)
        if family_bucket == "exact_family":
            exact_family_feedback_count += 1
        elif family_bucket == "related_family":
            related_family_feedback_count += 1
        elif family_bucket == "company_fallback":
            company_fallback_feedback_count += 1
        if age_days >= 7:
            decayed_feedback_count += 1
        if family_reason:
            reasons.append(family_reason)
        recall_pressure += RECALL_FEEDBACK_WEIGHTS.get(feedback_type, 0.0) * effective_weight
        precision_pressure += PRECISION_FEEDBACK_WEIGHTS.get(feedback_type, 0.0) * effective_weight
        recall_delta, precision_delta, feedback_reason = _metadata_pressure_adjustment(metadata)
        recall_pressure += recall_delta * effective_weight
        precision_pressure += precision_delta * effective_weight
        if feedback_reason:
            reasons.append(feedback_reason)

    net_delta = min(0.1, max(-0.1, (precision_pressure - recall_pressure) * 0.02))
    high_threshold = _clamp(DEFAULT_HIGH_THRESHOLD + net_delta, MIN_HIGH_THRESHOLD, MAX_HIGH_THRESHOLD)
    medium_threshold = _clamp(DEFAULT_MEDIUM_THRESHOLD + net_delta * 0.8, MIN_MEDIUM_THRESHOLD, MAX_MEDIUM_THRESHOLD)
    if high_threshold < medium_threshold + MIN_THRESHOLD_GAP:
        high_threshold = min(MAX_HIGH_THRESHOLD, medium_threshold + MIN_THRESHOLD_GAP)

    summary_reasons = [
        f"recall_pressure={round(recall_pressure, 2)}",
        f"precision_pressure={round(precision_pressure, 2)}",
        f"net_delta={round(net_delta, 2)}",
    ]
    if matching_request_family_sig:
        summary_reasons.append(f"scope=request_family:{matching_request_family_sig}")
    summary_reasons.append(f"half_life_days={DECAY_HALF_LIFE_DAYS}")
    summary_reasons.extend(reasons[:3])
    if not feedback_items:
        summary_reasons.append("no_feedback_history")

    summary = {
        "target_company": target_company,
        "scope_kind": scope_kind,
        "feedback_count": len(feedback_items),
        "applied_feedback_count": applied_feedback_count,
        "applied_feedback_weight": round(applied_feedback_weight, 2),
        "exact_family_feedback_count": exact_family_feedback_count,
        "related_family_feedback_count": related_family_feedback_count,
        "company_fallback_feedback_count": company_fallback_feedback_count,
        "decayed_feedback_count": decayed_feedback_count,
        "recall_pressure": round(recall_pressure, 2),
        "precision_pressure": round(precision_pressure, 2),
        "net_delta": round(net_delta, 2),
        "default_high_threshold": DEFAULT_HIGH_THRESHOLD,
        "default_medium_threshold": DEFAULT_MEDIUM_THRESHOLD,
        "request_signature": request_sig,
        "request_family_signature": request_family_sig,
        "matching_request_signature": matching_request_sig,
        "matching_request_family_signature": matching_request_family_sig,
    }
    return {
        "target_company": target_company,
        "scope_kind": scope_kind,
        "request_signature": request_sig,
        "request_family_signature": request_family_sig,
        "matching_request_signature": matching_request_sig,
        "matching_request_family_signature": matching_request_family_sig,
        "high_threshold": round(high_threshold, 2),
        "medium_threshold": round(medium_threshold, 2),
        "summary": summary,
        "feedback_counts": feedback_counts,
        "weighted_feedback_counts": weighted_feedback_counts,
        "reasons": summary_reasons,
    }


def apply_policy_control(policy: dict[str, Any], control: dict[str, Any] | None) -> dict[str, Any]:
    if not control:
        return policy
    updated = dict(policy)
    summary = dict(updated.get("summary") or {})
    reasons = list(updated.get("reasons") or [])
    control_mode = str(control.get("control_mode") or "").strip() or "override"
    reviewer = str(control.get("reviewer") or "").strip()
    notes = str(control.get("notes") or "").strip()
    high_threshold = _coerce_threshold(control.get("high_threshold"), updated.get("high_threshold"), upper=MAX_HIGH_THRESHOLD)
    medium_threshold = _coerce_threshold(control.get("medium_threshold"), updated.get("medium_threshold"), upper=MAX_MEDIUM_THRESHOLD)
    if high_threshold < medium_threshold + MIN_THRESHOLD_GAP:
        high_threshold = min(MAX_HIGH_THRESHOLD, medium_threshold + MIN_THRESHOLD_GAP)
    updated["high_threshold"] = round(high_threshold, 2)
    updated["medium_threshold"] = round(medium_threshold, 2)
    control_payload = {
        "control_id": int(control.get("control_id") or 0),
        "scope_kind": str(control.get("scope_kind") or ""),
        "control_mode": control_mode,
        "reviewer": reviewer,
        "notes": notes,
        "selection_reason": str(control.get("selection_reason") or ""),
    }
    updated["control"] = control_payload
    summary["control_applied"] = True
    summary["control_mode"] = control_mode
    summary["control_scope_kind"] = control_payload["scope_kind"]
    summary["control_id"] = control_payload["control_id"]
    updated["summary"] = summary
    reason_parts = [f"control={control_mode}", f"scope={control_payload['scope_kind']}"]
    if reviewer:
        reason_parts.append(f"reviewer={reviewer}")
    if notes:
        reason_parts.append(f"notes={notes}")
    reasons.append(",".join(reason_parts))
    updated["reasons"] = reasons
    return updated


def default_confidence_policy(
    target_company: str = "",
    request_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_confidence_policy(target_company=target_company, feedback_items=[], request_payload=request_payload)


def _metadata_pressure_adjustment(metadata: dict[str, Any]) -> tuple[float, float, str]:
    observed = str(metadata.get("observed_confidence_label") or "").strip().lower()
    expected = str(metadata.get("expected_confidence_label") or "").strip().lower()
    if not observed or not expected or observed == expected:
        return 0.0, 0.0, ""
    order = {"lead_only": 0, "medium": 1, "high": 2}
    observed_rank = order.get(observed, -1)
    expected_rank = order.get(expected, -1)
    if observed_rank < 0 or expected_rank < 0:
        return 0.0, 0.0, ""
    if observed_rank > expected_rank:
        return 0.0, 0.8, f"metadata_precision_adjustment={observed}->{expected}"
    return 0.8, 0.0, f"metadata_recall_adjustment={observed}->{expected}"


def _family_relevance_weight(
    item: dict[str, Any],
    *,
    request_payload: dict[str, Any],
    request_sig: str,
    request_family_sig: str,
    matching_request_sig: str,
    matching_request_family_sig: str,
) -> tuple[float, str, str]:
    if not request_payload:
        return 1.0, "company_scope_policy", "company_fallback"
    metadata = dict(item.get("metadata") or {})
    feedback_request = metadata.get("request_payload") if isinstance(metadata.get("request_payload"), dict) else {}
    feedback_matching = dict(metadata.get("request_matching") or {})
    feedback_request_sig = str(
        metadata.get("matching_request_signature")
        or feedback_matching.get("matching_request_signature")
        or metadata.get("request_signature")
        or ""
    ).strip()
    feedback_family_sig = str(
        metadata.get("matching_request_family_signature")
        or feedback_matching.get("matching_request_family_signature")
        or metadata.get("request_family_signature")
        or ""
    ).strip()
    if feedback_request_sig and feedback_request_sig == matching_request_sig:
        return 1.0, "exact_request_feedback", "exact_family"
    if feedback_family_sig and feedback_family_sig == matching_request_family_sig:
        return 1.0, "exact_family_feedback", "exact_family"
    if feedback_request:
        match = request_family_score(request_payload, feedback_request)
        score = float(match.get("score") or 0.0)
        if bool(match.get("exact_request_match")) or bool(match.get("exact_family_match")):
            return 1.0, "exact_family_feedback", "exact_family"
        if score >= MATCH_THRESHOLD:
            weight = round(min(0.85, max(0.3, 0.2 + (score / 100.0) * 0.7)), 4)
            return weight, f"related_family_feedback={round(score, 2)}", "related_family"
        return 0.0, f"family_mismatch={round(score, 2)}", "mismatch"
    if feedback_family_sig and feedback_family_sig not in {matching_request_family_sig, request_family_sig}:
        return 0.0, "family_signature_mismatch", "mismatch"
    return NO_CONTEXT_FAMILY_WEIGHT, "company_scope_fallback", "company_fallback"


def _time_decay_weight(created_at: Any, *, now: datetime | None) -> tuple[float, float]:
    created_at_dt = _coerce_datetime(created_at)
    if created_at_dt is None:
        return 1.0, 0.0
    reference_dt = now or datetime.now(timezone.utc)
    if reference_dt.tzinfo is None:
        reference_dt = reference_dt.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (reference_dt - created_at_dt).total_seconds())
    age_days = age_seconds / 86400.0
    decay_weight = max(MIN_TIME_DECAY_WEIGHT, 0.5 ** (age_days / DECAY_HALF_LIFE_DAYS))
    return round(decay_weight, 4), round(age_days, 2)


def _coerce_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        pass
    if parsed is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _coerce_threshold(value: Any, fallback: Any, *, upper: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        try:
            parsed = float(fallback)
        except (TypeError, ValueError):
            parsed = upper
    lower = MIN_MEDIUM_THRESHOLD if upper == MAX_MEDIUM_THRESHOLD else MIN_HIGH_THRESHOLD
    return _clamp(parsed, lower, upper)
