from __future__ import annotations

import re
from typing import Any

from .domain import Candidate
from .request_matching import (
    build_request_matching_bundle,
    request_family_signature,
    request_signature,
)


POSITIVE_FEEDBACK_TYPES = {
    "must_have_signal",
    "false_negative_pattern",
    "confidence_boost_signal",
}

NEGATIVE_FEEDBACK_TYPES = {
    "exclude_signal",
    "false_positive_pattern",
    "confidence_penalty_signal",
}

FIELD_PRIORITIES = {
    "focus_areas": 1.0,
    "team": 0.95,
    "role": 0.8,
    "notes": 0.6,
    "work_history": 0.55,
    "education": 0.35,
}


def derive_pattern_suggestions(
    *,
    feedback: dict[str, Any],
    candidate: Candidate | None,
    job_result: dict[str, Any] | None,
    existing_patterns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    feedback_type = str(feedback.get("feedback_type") or "").strip()
    if feedback_type not in POSITIVE_FEEDBACK_TYPES | NEGATIVE_FEEDBACK_TYPES:
        return []

    metadata = dict(feedback.get("metadata") or {})
    request_payload = metadata.get("request_payload") if isinstance(metadata.get("request_payload"), dict) else {}
    target_company = str(feedback.get("target_company") or metadata.get("target_company") or "").strip()
    request_matching = dict(metadata.get("request_matching") or {})
    if request_payload:
        computed_matching = build_request_matching_bundle(request_payload)
        if not request_matching:
            request_matching = computed_matching
        else:
            request_matching.setdefault(
                "matching_request_signature",
                str(computed_matching.get("matching_request_signature") or ""),
            )
            request_matching.setdefault(
                "matching_request_family_signature",
                str(computed_matching.get("matching_request_family_signature") or ""),
            )
    raw_request_signature = str(metadata.get("request_signature") or "").strip()
    raw_request_family_signature = str(metadata.get("request_family_signature") or "").strip()
    if request_payload:
        raw_request_signature = raw_request_signature or request_signature(request_payload)
        raw_request_family_signature = raw_request_family_signature or request_family_signature(request_payload)
    matching_request_signature = str(
        metadata.get("matching_request_signature")
        or request_matching.get("matching_request_signature")
        or raw_request_signature
        or ""
    ).strip()
    matching_request_family_signature = str(
        metadata.get("matching_request_family_signature")
        or request_matching.get("matching_request_family_signature")
        or raw_request_family_signature
        or ""
    ).strip()
    subject = str(feedback.get("subject") or "").strip()
    explicit_value = str(feedback.get("value") or "").strip()

    existing_keys = {
        _pattern_key(
            str(item.get("pattern_type") or ""),
            str(item.get("subject") or ""),
            str(item.get("value") or ""),
        )
        for item in existing_patterns
    }
    subjects = _dedupe([subject, *_normalize_list(request_payload.get("keywords")), *_normalize_list(request_payload.get("must_have_keywords"))])
    signals = _candidate_signals(candidate, job_result)
    suggestions: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    if feedback_type in POSITIVE_FEEDBACK_TYPES:
        suggestions.extend(
            _build_suggestions(
                pattern_type="must_signal",
                feedback_type=feedback_type,
                subjects=subjects,
                explicit_value=explicit_value,
                signals=signals,
                target_company=target_company,
                request_signature=raw_request_signature,
                request_family_signature=raw_request_family_signature,
                matching_request_signature=matching_request_signature,
                matching_request_family_signature=matching_request_family_signature,
                source_feedback_id=int(feedback.get("feedback_id") or 0),
                source_job_id=str(feedback.get("job_id") or ""),
                candidate_id=str(feedback.get("candidate_id") or ""),
                seen_keys=seen_keys,
                existing_keys=existing_keys,
                limit=2,
            )
        )
        suggestions.extend(
            _build_suggestions(
                pattern_type="confidence_boost",
                feedback_type=feedback_type,
                subjects=subjects,
                explicit_value=explicit_value,
                signals=signals,
                target_company=target_company,
                request_signature=raw_request_signature,
                request_family_signature=raw_request_family_signature,
                matching_request_signature=matching_request_signature,
                matching_request_family_signature=matching_request_family_signature,
                source_feedback_id=int(feedback.get("feedback_id") or 0),
                source_job_id=str(feedback.get("job_id") or ""),
                candidate_id=str(feedback.get("candidate_id") or ""),
                seen_keys=seen_keys,
                existing_keys=existing_keys,
                limit=1,
            )
        )
        if feedback_type == "false_negative_pattern":
            suggestions.extend(
                _build_suggestions(
                    pattern_type="alias",
                    feedback_type=feedback_type,
                    subjects=subjects,
                    explicit_value=explicit_value,
                    signals=signals,
                    target_company=target_company,
                    request_signature=raw_request_signature,
                    request_family_signature=raw_request_family_signature,
                    matching_request_signature=matching_request_signature,
                    matching_request_family_signature=matching_request_family_signature,
                    source_feedback_id=int(feedback.get("feedback_id") or 0),
                    source_job_id=str(feedback.get("job_id") or ""),
                    candidate_id=str(feedback.get("candidate_id") or ""),
                    seen_keys=seen_keys,
                    existing_keys=existing_keys,
                    limit=1,
                )
            )

    if feedback_type in NEGATIVE_FEEDBACK_TYPES:
        suggestions.extend(
            _build_suggestions(
                pattern_type="exclude_signal",
                feedback_type=feedback_type,
                subjects=subjects,
                explicit_value=explicit_value,
                signals=signals,
                target_company=target_company,
                request_signature=raw_request_signature,
                request_family_signature=raw_request_family_signature,
                matching_request_signature=matching_request_signature,
                matching_request_family_signature=matching_request_family_signature,
                source_feedback_id=int(feedback.get("feedback_id") or 0),
                source_job_id=str(feedback.get("job_id") or ""),
                candidate_id=str(feedback.get("candidate_id") or ""),
                seen_keys=seen_keys,
                existing_keys=existing_keys,
                limit=2,
            )
        )
        suggestions.extend(
            _build_suggestions(
                pattern_type="confidence_penalty",
                feedback_type=feedback_type,
                subjects=subjects,
                explicit_value=explicit_value,
                signals=signals,
                target_company=target_company,
                request_signature=raw_request_signature,
                request_family_signature=raw_request_family_signature,
                matching_request_signature=matching_request_signature,
                matching_request_family_signature=matching_request_family_signature,
                source_feedback_id=int(feedback.get("feedback_id") or 0),
                source_job_id=str(feedback.get("job_id") or ""),
                candidate_id=str(feedback.get("candidate_id") or ""),
                seen_keys=seen_keys,
                existing_keys=existing_keys,
                limit=1,
            )
        )
    return suggestions[:4]


def _build_suggestions(
    *,
    pattern_type: str,
    feedback_type: str,
    subjects: list[str],
    explicit_value: str,
    signals: list[dict[str, Any]],
    target_company: str,
    request_signature: str,
    request_family_signature: str,
    matching_request_signature: str,
    matching_request_family_signature: str,
    source_feedback_id: int,
    source_job_id: str,
    candidate_id: str,
    seen_keys: set[str],
    existing_keys: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not subjects or not signals:
        return results
    for subject in subjects[:2]:
        for signal in signals:
            phrase = str(signal.get("phrase") or "").strip()
            if not _is_useful_phrase(phrase):
                continue
            if pattern_type == "alias" and str(signal.get("field") or "") not in {"focus_areas", "team", "notes"}:
                continue
            if _normalize(phrase) == _normalize(subject):
                continue
            if explicit_value and _normalize(phrase) == _normalize(explicit_value):
                continue
            key = _pattern_key(pattern_type, subject, phrase)
            if key in existing_keys or key in seen_keys:
                continue
            seen_keys.add(key)
            results.append(
                {
                    "target_company": target_company,
                    "request_signature": request_signature,
                    "request_family_signature": request_family_signature,
                    "matching_request_signature": matching_request_signature,
                    "matching_request_family_signature": matching_request_family_signature,
                    "source_feedback_id": source_feedback_id,
                    "source_job_id": source_job_id,
                    "candidate_id": candidate_id,
                    "pattern_type": pattern_type,
                    "subject": subject,
                    "value": phrase,
                    "status": "suggested",
                    "confidence": _suggestion_confidence(pattern_type, signal),
                    "rationale": _rationale(pattern_type, feedback_type, signal),
                    "evidence": {
                        "field": signal.get("field") or "",
                        "matched_on": signal.get("matched_on") or "",
                        "source_kind": signal.get("source_kind") or "",
                        "score": signal.get("score") or 0.0,
                    },
                    "metadata": {
                        "feedback_type": feedback_type,
                        "signal_field": signal.get("field") or "",
                        "source_kind": signal.get("source_kind") or "",
                    },
                }
            )
            if len(results) >= limit:
                return results
    return results


def _candidate_signals(candidate: Candidate | None, job_result: dict[str, Any] | None) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if isinstance(job_result, dict):
        for item in list(job_result.get("matched_fields") or [])[:4]:
            if not isinstance(item, dict):
                continue
            matched_on = str(item.get("matched_on") or "").strip()
            value = str(item.get("value") or "").strip()
            field = str(item.get("field") or "").strip()
            if _is_useful_phrase(value):
                signals.append(
                    {
                        "phrase": value,
                        "field": field,
                        "matched_on": matched_on,
                        "source_kind": "matched_field_value",
                        "score": 1.0 + float(item.get("weight") or 0.0) / 10.0,
                    }
                )
            if matched_on and _is_useful_phrase(matched_on):
                signals.append(
                    {
                        "phrase": matched_on,
                        "field": field,
                        "matched_on": matched_on,
                        "source_kind": "matched_variant",
                        "score": 1.15 + float(item.get("weight") or 0.0) / 10.0,
                    }
                )
    if candidate is not None:
        for field_name, priority in FIELD_PRIORITIES.items():
            value = str(getattr(candidate, field_name, "") or "").strip()
            for phrase in _split_field_phrases(value):
                if not _is_useful_phrase(phrase):
                    continue
                signals.append(
                    {
                        "phrase": phrase,
                        "field": field_name,
                        "matched_on": "",
                        "source_kind": "candidate_field",
                        "score": priority,
                    }
                )
    deduped: dict[str, dict[str, Any]] = {}
    for signal in signals:
        key = _normalize(str(signal.get("phrase") or ""))
        if not key:
            continue
        existing = deduped.get(key)
        if existing is None or float(signal.get("score") or 0.0) > float(existing.get("score") or 0.0):
            deduped[key] = signal
    ordered = sorted(deduped.values(), key=lambda item: (-float(item.get("score") or 0.0), str(item.get("phrase") or "")))
    return ordered


def _split_field_phrases(value: str) -> list[str]:
    compact = " ".join(str(value or "").split()).strip()
    if not compact:
        return []
    parts = [part.strip(" -–") for part in re.split(r"[,\n;|]+", compact) if part.strip()]
    if not parts:
        return []
    if len(parts) == 1:
        return [compact]
    phrases = [compact]
    phrases.extend(parts[:3])
    return _dedupe(phrases)


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        values = list(value)
    return [str(item).strip() for item in values if str(item).strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in values:
        normalized = _normalize(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(item.strip())
    return results


def _normalize(value: str) -> str:
    return str(value or "").strip().lower()


def _is_useful_phrase(value: str) -> bool:
    compact = " ".join(str(value or "").split()).strip()
    if len(compact) < 4 or len(compact) > 90:
        return False
    if compact.lower().startswith("http"):
        return False
    alpha_chars = sum(1 for ch in compact if ch.isalpha())
    return alpha_chars >= 4


def _pattern_key(pattern_type: str, subject: str, value: str) -> str:
    return "|".join([_normalize(pattern_type), _normalize(subject), _normalize(value)])


def _suggestion_confidence(pattern_type: str, signal: dict[str, Any]) -> str:
    score = float(signal.get("score") or 0.0)
    if pattern_type in {"must_signal", "exclude_signal"} and score >= 1.0:
        return "high"
    if score >= 0.8:
        return "medium"
    return "low"


def _rationale(pattern_type: str, feedback_type: str, signal: dict[str, Any]) -> str:
    field = str(signal.get("field") or "").strip() or "candidate"
    phrase = str(signal.get("phrase") or "").strip()
    source_kind = str(signal.get("source_kind") or "").strip() or "candidate_field"
    return f"{feedback_type} suggested {pattern_type} because {field} exposed '{phrase}' via {source_kind}."
