from __future__ import annotations

from typing import Any

from .asset_paths import extract_company_snapshot_ref
from .domain import Candidate, JobRequest
from .scoring import ScoredCandidate


def build_manual_review_items(
    request: JobRequest,
    scored: list[ScoredCandidate],
    evidence_lookup: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    review_window = max(request.top_k, min(request.top_k + max(request.exploration_limit, 3), 15))
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for rank, item in enumerate(scored[:review_window], start=1):
        review_type, priority, reasons = _review_decision(item)
        if not review_type:
            continue
        candidate = item.candidate
        dedupe_key = (candidate.candidate_id, review_type)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        evidence = list(evidence_lookup.get(candidate.candidate_id) or [])[:3]
        items.append(
            {
                "candidate_id": candidate.candidate_id,
                "target_company": candidate.target_company,
                "review_type": review_type,
                "priority": priority,
                "status": "open",
                "summary": _summary(candidate, item, reasons),
                "candidate": candidate.to_record(),
                "evidence": evidence,
                "metadata": {
                    "rank": rank,
                    "score": item.score,
                    "confidence_label": item.confidence_label,
                    "confidence_score": item.confidence_score,
                    "confidence_reason": item.confidence_reason,
                    "matched_fields": item.matched_fields,
                    "explanation": item.explanation,
                    "reasons": reasons,
                    "snapshot_id": _candidate_snapshot_id(candidate, evidence),
                },
            }
        )
    return items


def _review_decision(item: ScoredCandidate) -> tuple[str, str, list[str]]:
    candidate = item.candidate
    metadata = dict(candidate.metadata or {})
    reasons: list[str] = []
    if bool(metadata.get("membership_review_required")):
        reasons.append("suspicious_membership")
    if candidate.category == "lead" or item.confidence_label == "lead_only":
        reasons.append("unverified_membership")
    if not candidate.linkedin_url:
        reasons.append("missing_linkedin_profile")
    if item.confidence_label == "medium":
        reasons.append("medium_confidence_needs_review")

    if not reasons:
        return "", "", []
    if "suspicious_membership" in reasons:
        return "manual_identity_resolution", "high", reasons
    if "unverified_membership" in reasons:
        return "manual_identity_resolution", "high", reasons
    if "missing_linkedin_profile" in reasons:
        return "missing_primary_profile", "medium", reasons
    return "needs_human_validation", "medium", reasons


def _summary(candidate: Candidate, item: ScoredCandidate, reasons: list[str]) -> str:
    joined_reasons = ", ".join(reasons)
    return (
        f"{candidate.display_name} requires manual review because {joined_reasons}. "
        f"Current confidence is {item.confidence_label} ({item.confidence_score})."
    )


def _candidate_snapshot_id(candidate: Candidate, evidence: list[dict[str, Any]]) -> str:
    metadata = dict(candidate.metadata or {})
    snapshot_id = str(metadata.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    snapshot_id = _snapshot_id_from_path(candidate.source_path)
    if snapshot_id:
        return snapshot_id
    for item in evidence:
        evidence_metadata = dict(item.get("metadata") or {})
        snapshot_id = str(evidence_metadata.get("snapshot_id") or "").strip()
        if snapshot_id:
            return snapshot_id
        snapshot_id = _snapshot_id_from_path(str(item.get("source_path") or ""))
        if snapshot_id:
            return snapshot_id
    return ""


def _snapshot_id_from_path(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    snapshot_ref = extract_company_snapshot_ref(normalized)
    return str(snapshot_ref[1] if snapshot_ref is not None else "")
