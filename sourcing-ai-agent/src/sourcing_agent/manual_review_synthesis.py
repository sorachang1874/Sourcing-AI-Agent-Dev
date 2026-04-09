from __future__ import annotations

from typing import Any

from .model_provider import ModelClient


def compile_manual_review_synthesis(
    review_item: dict[str, Any],
    *,
    model_client: ModelClient | None = None,
) -> dict[str, Any]:
    deterministic = _deterministic_manual_review_synthesis(review_item)
    provider_name = ""
    model_payload: dict[str, Any] = {}
    should_use_model = _should_use_model_synthesis(review_item)
    if should_use_model and model_client is not None:
        provider_name = str(model_client.provider_name() or "").strip()
        try:
            model_payload = normalize_manual_review_synthesis(
                model_client.synthesize_manual_review(_build_model_payload(review_item, deterministic)) or {}
            )
        except Exception:
            model_payload = {}

    synthesis = {
        "summary": str(model_payload.get("summary") or deterministic["summary"]).strip(),
        "confidence_takeaways": _normalize_section(
            model_payload.get("confidence_takeaways"),
            fallback=deterministic["confidence_takeaways"],
        ),
        "conflict_points": _normalize_section(
            model_payload.get("conflict_points"),
            fallback=deterministic["conflict_points"],
        ),
        "recommended_checks": _normalize_section(
            model_payload.get("recommended_checks"),
            fallback=deterministic["recommended_checks"],
        ),
    }
    return {
        "synthesis": synthesis,
        "synthesis_compiler": {
            "source": "model" if model_payload else "deterministic",
            "provider": provider_name or ("deterministic" if not model_payload else ""),
            "eligible_for_model": should_use_model,
            "model_used": bool(model_payload),
            "fallback_used": not bool(model_payload),
            "deterministic_synthesis": deterministic,
        },
    }


def normalize_manual_review_synthesis(payload: dict[str, Any] | None) -> dict[str, Any]:
    candidate = dict(payload or {})
    synthesis: dict[str, Any] = {}
    summary = str(candidate.get("summary") or "").strip()
    if summary:
        synthesis["summary"] = summary[:500]
    for field in ["confidence_takeaways", "conflict_points", "recommended_checks"]:
        values = _normalize_section(candidate.get(field), fallback=[])
        if values:
            synthesis[field] = values
    return synthesis


def _deterministic_manual_review_synthesis(review_item: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(review_item.get("candidate") or {})
    metadata = dict(review_item.get("metadata") or {})
    evidence = [dict(item) for item in list(review_item.get("evidence") or []) if isinstance(item, dict)]
    display_name = str(candidate.get("display_name") or candidate.get("name_en") or review_item.get("candidate_id") or "candidate").strip()
    review_type = str(review_item.get("review_type") or "").strip() or "manual_review"
    reasons = [str(item).strip() for item in list(metadata.get("reasons") or []) if str(item).strip()]
    confidence_label = str(metadata.get("confidence_label") or "").strip() or "unknown"
    confidence_score = float(metadata.get("confidence_score") or 0.0)
    summary = (
        f"{display_name} is in {review_type} because {', '.join(reasons) if reasons else 'the evidence is not clean enough yet'}. "
        f"Current retrieval confidence is {confidence_label} ({round(confidence_score, 2)})."
    )

    confidence_takeaways: list[str] = []
    if reasons:
        confidence_takeaways.append(f"Queue triggers: {', '.join(reasons[:4])}.")
    explanation = str(metadata.get("explanation") or "").strip()
    if explanation:
        confidence_takeaways.append(explanation[:220])
    if str(candidate.get("linkedin_url") or "").strip():
        confidence_takeaways.append("A primary LinkedIn profile is available, so the main uncertainty is evidence interpretation rather than missing identity data.")
    else:
        confidence_takeaways.append("The primary LinkedIn profile is still missing or unresolved, so identity confidence remains structurally limited.")

    conflict_points: list[str] = []
    membership_decision = str(dict(candidate.get("metadata") or {}).get("membership_review_decision") or "").strip()
    membership_rationale = str(dict(candidate.get("metadata") or {}).get("membership_review_rationale") or "").strip()
    if membership_decision:
        line = f"Profile membership review outcome: {membership_decision}."
        if membership_rationale:
            line = f"{line} {membership_rationale[:220]}"
        conflict_points.append(line)
    trigger_keywords = [str(item).strip() for item in list(dict(candidate.get("metadata") or {}).get("membership_review_trigger_keywords") or []) if str(item).strip()]
    if trigger_keywords:
        conflict_points.append(f"Suspicious profile-content triggers: {', '.join(trigger_keywords[:6])}.")
    for item in evidence[:3]:
        title = str(item.get("title") or item.get("source_type") or "evidence").strip()
        source_type = str(item.get("source_type") or "").strip()
        summary_text = str(item.get("summary") or "").strip()
        if summary_text:
            conflict_points.append(f"{title} ({source_type}): {summary_text[:180]}")
        else:
            conflict_points.append(f"{title} ({source_type}) contributes to the review queue.")

    recommended_checks: list[str] = []
    if "suspicious_membership" in reasons:
        recommended_checks.append("Open the LinkedIn experience timeline and verify the target-company role, dates, and company URL directly.")
    if "missing_linkedin_profile" in reasons:
        recommended_checks.append("Try to recover the primary LinkedIn URL before resolving the queue item.")
    recommended_checks.append("Cross-check one independent public source such as an official page, publication author list, or personal site.")
    recommended_checks.append("Keep the final membership decision human-authored; use the synthesis only as an evidence brief.")

    return {
        "summary": summary,
        "confidence_takeaways": _normalize_section(confidence_takeaways, fallback=[]),
        "conflict_points": _normalize_section(conflict_points, fallback=[]),
        "recommended_checks": _normalize_section(recommended_checks, fallback=[]),
    }


def _should_use_model_synthesis(review_item: dict[str, Any]) -> bool:
    metadata = dict(review_item.get("metadata") or {})
    reasons = {str(item).strip() for item in list(metadata.get("reasons") or []) if str(item).strip()}
    if "suspicious_membership" in reasons:
        return True
    if str(review_item.get("review_type") or "").strip() == "manual_identity_resolution":
        return True
    evidence = [item for item in list(review_item.get("evidence") or []) if isinstance(item, dict)]
    evidence_source_types = {str(item.get("source_type") or "").strip() for item in evidence if str(item.get("source_type") or "").strip()}
    return len(evidence_source_types) >= 2 and len(evidence) >= 2


def _build_model_payload(review_item: dict[str, Any], deterministic: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(review_item.get("candidate") or {})
    metadata = dict(review_item.get("metadata") or {})
    evidence = [dict(item) for item in list(review_item.get("evidence") or []) if isinstance(item, dict)]
    return {
        "review_item": {
            "review_item_id": int(review_item.get("review_item_id") or 0),
            "review_type": str(review_item.get("review_type") or "").strip(),
            "summary": str(review_item.get("summary") or "").strip(),
            "candidate": {
                "display_name": str(candidate.get("display_name") or candidate.get("name_en") or "").strip(),
                "role": str(candidate.get("role") or "").strip(),
                "organization": str(candidate.get("organization") or "").strip(),
                "linkedin_url": str(candidate.get("linkedin_url") or "").strip(),
                "notes": str(candidate.get("notes") or "").strip()[:1200],
                "metadata": {
                    "membership_review_decision": str(dict(candidate.get("metadata") or {}).get("membership_review_decision") or "").strip(),
                    "membership_review_rationale": str(dict(candidate.get("metadata") or {}).get("membership_review_rationale") or "").strip(),
                    "membership_review_trigger_keywords": list(dict(candidate.get("metadata") or {}).get("membership_review_trigger_keywords") or [])[:8],
                },
            },
            "metadata": {
                "reasons": list(metadata.get("reasons") or [])[:8],
                "confidence_label": str(metadata.get("confidence_label") or "").strip(),
                "confidence_score": float(metadata.get("confidence_score") or 0.0),
                "confidence_reason": str(metadata.get("confidence_reason") or "").strip(),
                "explanation": str(metadata.get("explanation") or "").strip(),
            },
            "evidence": [
                {
                    "source_type": str(item.get("source_type") or "").strip(),
                    "title": str(item.get("title") or "").strip(),
                    "summary": str(item.get("summary") or "").strip()[:300],
                    "url": str(item.get("url") or "").strip(),
                }
                for item in evidence[:5]
            ],
        },
        "deterministic_synthesis": deterministic,
        "instruction": "Summarize the evidence conflict and confidence state for a human reviewer. Do not decide membership.",
    }


def _normalize_section(value: Any, *, fallback: list[str]) -> list[str]:
    items = value if isinstance(value, list) else fallback
    cleaned: list[str] = []
    for item in items:
        candidate = str(item or "").strip()
        if not candidate or candidate in cleaned:
            continue
        cleaned.append(candidate[:240])
    return cleaned[:6]
