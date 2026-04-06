from __future__ import annotations

from typing import Any


AUTO_FEEDBACK_WEIGHTS = {
    "accepted_alias": 0.85,
    "false_negative_pattern": 0.85,
    "must_have_signal": 0.75,
    "exclude_signal": 0.7,
    "false_positive_pattern": 0.7,
    "rejected_alias": 0.65,
}

DEFAULT_CHEAP_TOP_K = 10
DEFAULT_CHEAP_SEMANTIC_LIMIT = 8


def decide_rerun_policy(
    payload: dict[str, Any],
    *,
    feedback: dict[str, Any],
    recompile: dict[str, Any],
    baseline_job: dict[str, Any] | None,
) -> dict[str, Any]:
    rerun_request = payload.get("rerun_retrieval")
    requested_mode = _normalize_requested_mode(rerun_request)
    trigger_feedback_type = str(payload.get("feedback_type") or feedback.get("feedback_type") or "").strip()
    baseline_summary = dict((baseline_job or {}).get("summary") or {})
    baseline_request = dict((baseline_job or {}).get("request") or {})
    baseline_plan = dict((baseline_job or {}).get("plan") or {})

    if requested_mode == "none":
        return {
            "status": "not_requested",
            "mode": "none",
            "reason": "Rerun was not requested.",
            "requested_mode": requested_mode,
            "effective_request_overrides": {},
            "runtime_policy": {},
            "estimated_cost_tier": "none",
            "gating_signals": [],
        }
    if recompile.get("status") != "recompiled":
        return {
            "status": "skipped",
            "mode": "none",
            "reason": "Criteria were not successfully recompiled.",
            "requested_mode": requested_mode,
            "effective_request_overrides": {},
            "runtime_policy": {},
            "estimated_cost_tier": "none",
            "gating_signals": [],
        }

    impact_score = _estimate_impact_score(
        feedback_type=trigger_feedback_type,
        baseline_summary=baseline_summary,
        payload=payload,
    )
    gating_signals = _gating_signals(
        feedback_type=trigger_feedback_type,
        baseline_job=baseline_job,
        baseline_summary=baseline_summary,
        payload=payload,
        impact_score=impact_score,
    )
    if requested_mode == "auto" and impact_score < 0.45:
        return {
            "status": "gated_off",
            "mode": "none",
            "reason": "Auto rerun was gated off because the expected retrieval impact is too small or too uncertain.",
            "requested_mode": requested_mode,
            "effective_request_overrides": {},
            "runtime_policy": {},
            "estimated_cost_tier": "none",
            "gating_signals": gating_signals,
            "impact_score": round(impact_score, 2),
        }

    mode = _choose_mode(
        requested_mode=requested_mode,
        payload=payload,
        baseline_request=baseline_request,
        baseline_plan=baseline_plan,
        impact_score=impact_score,
    )
    effective_request_overrides, runtime_policy = _runtime_policy_for_mode(
        mode=mode,
        payload=payload,
        baseline_request=baseline_request,
    )
    return {
        "status": "approved",
        "mode": mode,
        "reason": _decision_reason(mode, trigger_feedback_type, impact_score),
        "requested_mode": requested_mode,
        "effective_request_overrides": effective_request_overrides,
        "runtime_policy": runtime_policy,
        "estimated_cost_tier": "low" if mode == "cheap" else "standard",
        "gating_signals": gating_signals,
        "impact_score": round(impact_score, 2),
    }


def _normalize_requested_mode(value: Any) -> str:
    if value is False or value is None:
        return "none"
    if value is True:
        return "auto"
    normalized = str(value).strip().lower()
    if normalized in {"auto", "cheap", "full"}:
        return normalized
    if normalized in {"force", "force_full"}:
        return "full"
    if normalized in {"force_cheap", "light"}:
        return "cheap"
    return "none"


def _estimate_impact_score(
    *,
    feedback_type: str,
    baseline_summary: dict[str, Any],
    payload: dict[str, Any],
) -> float:
    score = AUTO_FEEDBACK_WEIGHTS.get(feedback_type, 0.2)
    total_matches = int(baseline_summary.get("total_matches") or 0)
    if total_matches == 0 and feedback_type in {"accepted_alias", "false_negative_pattern", "must_have_signal"}:
        score += 0.1
    if total_matches > 25:
        score += 0.05
    if payload.get("candidate_id"):
        score += 0.05
    if payload.get("rerun_request_overrides"):
        score += 0.05
    return max(0.0, min(score, 0.95))


def _gating_signals(
    *,
    feedback_type: str,
    baseline_job: dict[str, Any] | None,
    baseline_summary: dict[str, Any],
    payload: dict[str, Any],
    impact_score: float,
) -> list[str]:
    signals: list[str] = []
    if feedback_type:
        signals.append(f"feedback_type={feedback_type}")
    if baseline_job is None:
        signals.append("baseline_job=missing")
    else:
        signals.append(f"baseline_job={baseline_job.get('job_id') or 'present'}")
    total_matches = int(baseline_summary.get("total_matches") or 0)
    signals.append(f"baseline_total_matches={total_matches}")
    if payload.get("rerun_request_overrides"):
        signals.append("request_overrides=present")
    signals.append(f"impact_score={round(impact_score, 2)}")
    return signals


def _choose_mode(
    *,
    requested_mode: str,
    payload: dict[str, Any],
    baseline_request: dict[str, Any],
    baseline_plan: dict[str, Any],
    impact_score: float,
) -> str:
    if requested_mode in {"cheap", "full"}:
        return requested_mode
    if payload.get("rerun_request_overrides"):
        return "full"
    top_k = int(baseline_request.get("top_k") or 10)
    retrieval_strategy = str(
        baseline_request.get("retrieval_strategy")
        or ((baseline_plan.get("retrieval_plan") or {}).get("strategy") or "")
    ).strip()
    if retrieval_strategy in {"semantic", "semantic_heavy"}:
        return "full"
    if top_k > DEFAULT_CHEAP_TOP_K:
        return "full"
    if impact_score >= 0.75:
        return "cheap"
    return "full"


def _runtime_policy_for_mode(
    *,
    mode: str,
    payload: dict[str, Any],
    baseline_request: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    manual_overrides = payload.get("rerun_request_overrides") or {}
    if not isinstance(manual_overrides, dict):
        manual_overrides = {}
    if mode != "cheap":
        return dict(manual_overrides), {"mode": mode, "summary_mode": "default"}

    top_k = min(int(baseline_request.get("top_k") or DEFAULT_CHEAP_TOP_K), DEFAULT_CHEAP_TOP_K)
    semantic_limit = min(
        int(baseline_request.get("semantic_rerank_limit") or DEFAULT_CHEAP_SEMANTIC_LIMIT),
        DEFAULT_CHEAP_SEMANTIC_LIMIT,
    )
    effective_request_overrides = {
        "top_k": top_k,
        "semantic_rerank_limit": semantic_limit,
    }
    effective_request_overrides.update(manual_overrides)
    return effective_request_overrides, {
        "mode": "cheap",
        "summary_mode": "deterministic",
        "max_top_k": top_k,
        "max_semantic_rerank_limit": semantic_limit,
    }


def _decision_reason(mode: str, feedback_type: str, impact_score: float) -> str:
    if mode == "cheap":
        return (
            f"Approved cheap rerun for {feedback_type or 'feedback'} because the expected retrieval impact is "
            f"high enough ({round(impact_score, 2)}) to validate immediately without paying full summary cost."
        )
    return (
        f"Approved full rerun for {feedback_type or 'feedback'} because the expected impact or request complexity "
        f"({round(impact_score, 2)}) warrants the standard retrieval path."
    )
