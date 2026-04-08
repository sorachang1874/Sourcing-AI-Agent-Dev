from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from .confidence_policy import DEFAULT_HIGH_THRESHOLD, DEFAULT_MEDIUM_THRESHOLD
from .domain import (
    Candidate,
    JobRequest,
    derive_candidate_facets,
    derive_candidate_filter_facets,
    derive_candidate_role_bucket,
    sanitize_candidate_notes,
)


SEARCH_FIELDS = {
    "organization": 2,
    "role": 5,
    "team": 4,
    "focus_areas": 4,
    "derived_facets": 4,
    "investment_involvement": 4,
    "education": 2,
    "work_history": 2,
    "notes": 3,
    "ethnicity_background": 1,
    "current_destination": 1,
}

QUERY_STOPWORDS = {
    "sourcing",
    "workflow",
    "criteria",
    "structured",
    "hybrid",
    "semantic",
    "retrieval",
    "agent",
    "公司",
    "成员",
    "工作流",
    "全量",
    "数据资产",
    "检索",
    "排序",
    "匹配",
    "criteria",
}

KEYWORD_ALIAS_MAP = {
    "基础设施": ["infrastructure", "infra", "platform", "distributed systems", "systems"],
    "infra": ["infrastructure", "platform", "distributed systems"],
    "训练": ["training", "trainer", "pretraining", "pre-training"],
    "训练系统": ["training systems", "training infrastructure", "training platform"],
    "预训练": ["pretraining", "pre-training"],
    "推理": ["inference", "serving", "runtime"],
    "研究": ["research", "scientist", "applied scientist"],
    "安全": ["safety", "alignment", "security"],
    "芯片": ["hardware", "silicon"],
    "gpu": ["cuda", "accelerator", "cluster"],
    "工程": ["engineering", "engineer", "technical staff"],
    "ops": ["operations", "business operations", "people operations", "chief of staff"],
    "运营": ["operations", "business operations", "people operations", "chief of staff"],
    "招聘": ["recruiting", "recruiter", "talent acquisition", "talent"],
    "recruiting": ["recruiter", "talent acquisition", "talent"],
    "infra_systems": ["infrastructure", "infra", "systems", "platform", "distributed systems"],
    "multimodality": ["multimodal", "vision-language", "vision language"],
    "multimodal": ["multimodality", "vision-language", "vision language"],
}

CONFIDENCE_PATTERN_BASE = {
    "low": 0.05,
    "medium": 0.08,
    "high": 0.12,
}

CONFIDENCE_PATTERN_MULTIPLIER = {
    "must_signal": 1.0,
    "confidence_boost": 1.2,
    "exclude_signal": -1.1,
    "confidence_penalty": -1.35,
}


@dataclass(slots=True)
class ScoredCandidate:
    candidate: Candidate
    score: float
    matched_fields: list[dict[str, Any]]
    explanation: str
    semantic_score: float
    confidence_label: str
    confidence_score: float
    confidence_reason: str


def score_candidates(
    candidates: list[Candidate],
    request: JobRequest,
    criteria_patterns: list[dict[str, Any]] | None = None,
    confidence_policy: dict[str, Any] | None = None,
    semantic_hits: dict[str, dict[str, Any]] | None = None,
) -> list[ScoredCandidate]:
    scored: list[ScoredCandidate] = []
    for candidate in candidates:
        item = score_candidate(
            candidate,
            request,
            criteria_patterns=criteria_patterns,
            confidence_policy=confidence_policy,
            semantic_hit=(semantic_hits or {}).get(candidate.candidate_id),
        )
        if item is not None:
            scored.append(item)
    scored.sort(key=lambda item: (-item.score, item.candidate.display_name))
    return scored


def score_candidate(
    candidate: Candidate,
    request: JobRequest,
    criteria_patterns: list[dict[str, Any]] | None = None,
    confidence_policy: dict[str, Any] | None = None,
    semantic_hit: dict[str, Any] | None = None,
) -> ScoredCandidate | None:
    if not candidate_matches_structured_filters(candidate, request):
        return None

    keywords = build_query_terms(request, criteria_patterns or [])

    score = 0.0
    matched_fields: list[dict[str, Any]] = []
    search_fields = _search_fields_for_request(request)
    for keyword in keywords:
        variants = _keyword_variants(keyword, criteria_patterns or [])
        normalized_variants = [_normalize(item) for item in variants if _normalize(item)]
        if not normalized_variants:
            continue
        found = False
        for field_name, weight in search_fields.items():
            value = _candidate_field_value(candidate, field_name)
            normalized_value = _normalize(value)
            matched_variant = next((item for item in variants if _normalize(item) in normalized_value), "")
            if matched_variant:
                matched_fields.append(
                    {
                        "keyword": keyword,
                        "matched_on": matched_variant,
                        "field": field_name,
                        "value": _shorten(value),
                        "weight": weight,
                    }
                )
                score += weight
                found = True
        if found:
            score += 0.5

    if request.categories and candidate.category in request.categories:
        score += 1.5
    if request.employment_statuses and candidate.employment_status in request.employment_statuses:
        score += 1.0
    if candidate.category == "employee" and candidate.employment_status == "current":
        score += 0.5
    if candidate.category == "investor":
        involvement_label = _investment_label(candidate.investment_involvement)
        if involvement_label == "yes":
            score += 4.0
        elif involvement_label == "possible":
            score += 1.0

    semantic_score = 0.0
    if semantic_hit:
        semantic_score = round(float(semantic_hit.get("semantic_score") or 0.0), 2)
        score += semantic_score
        matched_fields.extend(list(semantic_hit.get("matched_fields") or []))

    if keywords and not matched_fields:
        return None

    explanation = _build_explanation(candidate, matched_fields, semantic_hit)
    confidence_label, confidence_score, confidence_reason = _confidence_assessment(
        candidate,
        matched_fields,
        criteria_patterns or [],
        confidence_policy or {},
    )
    return ScoredCandidate(
        candidate=candidate,
        score=round(score, 2),
        matched_fields=matched_fields,
        explanation=explanation,
        semantic_score=semantic_score,
        confidence_label=confidence_label,
        confidence_score=confidence_score,
        confidence_reason=confidence_reason,
    )


def _candidate_blob(candidate: Candidate) -> str:
    return " ".join(
        [
            candidate.display_name,
            candidate.organization,
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            " ".join(derive_candidate_facets(candidate)),
            derive_candidate_role_bucket(candidate),
            candidate.investment_involvement,
            candidate.education,
            candidate.work_history,
            sanitize_candidate_notes(candidate.notes),
            candidate.ethnicity_background,
            candidate.current_destination,
        ]
    )


def _extract_keywords_from_query(query: str) -> list[str]:
    if not query.strip():
        return []
    tokens = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z0-9\-\+]{3,}", query)
    results = []
    for token in tokens:
        candidate = token.strip()
        if not candidate:
            continue
        if _normalize(candidate) in QUERY_STOPWORDS:
            continue
        results.append(candidate)
    return results


def build_query_terms(request: JobRequest, criteria_patterns: list[dict[str, Any]] | None = None) -> list[str]:
    natural_language_query = request.query or request.raw_user_request
    keywords = request.keywords if request.keywords else _extract_keywords_from_query(natural_language_query)
    expanded: list[str] = []
    for keyword in (
        keywords
        + request.organization_keywords
        + request.must_have_keywords
        + request.must_have_facets
        + request.must_have_primary_role_buckets
    ):
        expanded.extend(_keyword_variants(keyword, criteria_patterns or []))
    return _dedupe(expanded)


def _search_fields_for_request(request: JobRequest) -> dict[str, float]:
    if request.must_have_primary_role_buckets:
        return {field_name: weight for field_name, weight in SEARCH_FIELDS.items() if field_name != "notes"}
    return SEARCH_FIELDS


def candidate_matches_structured_filters(candidate: Candidate, request: JobRequest) -> bool:
    if request.target_company and _normalize(candidate.target_company) != _normalize(request.target_company):
        return False
    if request.categories and candidate.category not in request.categories:
        return False
    if request.employment_statuses and candidate.employment_status not in request.employment_statuses:
        return False

    searchable_blob = _normalize(_candidate_blob(candidate))
    candidate_facets = {_normalize(item) for item in derive_candidate_filter_facets(candidate)}
    if candidate.category == "investor" and _is_direct_investment_search(request):
        involvement_label = _investment_label(candidate.investment_involvement)
        if involvement_label == "no":
            return False

    if request.must_have_facets and not all(_normalize(facet) in candidate_facets for facet in request.must_have_facets):
        return False
    if request.must_have_primary_role_buckets:
        candidate_role_bucket = _normalize(derive_candidate_role_bucket(candidate))
        if candidate_role_bucket not in {_normalize(item) for item in request.must_have_primary_role_buckets}:
            return False
    if request.organization_keywords:
        org_blob = _normalize(" ".join([candidate.organization, candidate.role, candidate.team]))
        if not any(_normalize(token) in org_blob for token in request.organization_keywords):
            return False
    if request.must_have_keywords and not all(_normalize(token) in searchable_blob for token in request.must_have_keywords):
        return False
    if request.exclude_keywords and any(_normalize(token) in searchable_blob for token in request.exclude_keywords):
        return False
    return True


def _is_direct_investment_search(request: JobRequest) -> bool:
    signal_text = " ".join(request.keywords + [request.query])
    direct_terms = ["直接参与", "决策", "领投", "deal lead", "主导", "投资决策"]
    return any(term in signal_text for term in direct_terms)


def _investment_label(value: str) -> str:
    normalized = value.strip()
    if normalized.startswith("⭐ 是") or normalized.startswith("是"):
        return "yes"
    if normalized.startswith("可能"):
        return "possible"
    if normalized.startswith("否"):
        return "no"
    return "unknown"


def _build_explanation(
    candidate: Candidate,
    matched_fields: list[dict[str, Any]],
    semantic_hit: dict[str, Any] | None = None,
) -> str:
    if not matched_fields:
        return f"{candidate.display_name} matched the structural filters."
    previews = []
    for item in matched_fields[:3]:
        matched_on = item.get("matched_on") or item["keyword"]
        previews.append(f"{item['field']} 命中「{item['keyword']}->{matched_on}」")
    explanation = f"{candidate.display_name} because " + "；".join(previews)
    if semantic_hit and semantic_hit.get("explanation"):
        explanation += f"；semantic={semantic_hit['explanation']}"
    return explanation


def _candidate_field_value(candidate: Candidate, field_name: str) -> str:
    if field_name == "derived_facets":
        return " | ".join([derive_candidate_role_bucket(candidate)] + derive_candidate_facets(candidate))
    value = str(getattr(candidate, field_name) or "").strip()
    if field_name == "notes":
        return sanitize_candidate_notes(value)
    return value


def _normalize(value: str) -> str:
    return value.strip().lower()


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results = []
    for item in items:
        normalized = _normalize(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(item)
    return results


def _keyword_variants(keyword: str, criteria_patterns: list[dict[str, Any]]) -> list[str]:
    variants = [keyword]
    aliases = KEYWORD_ALIAS_MAP.get(_normalize(keyword), [])
    variants.extend(aliases)
    for pattern in criteria_patterns:
        if pattern.get("pattern_type") != "alias":
            continue
        if _normalize(str(pattern.get("subject") or "")) != _normalize(keyword):
            continue
        value = str(pattern.get("value") or "").strip()
        if value:
            variants.append(value)
    return _dedupe(variants)


def _shorten(value: str, limit: int = 120) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _confidence_assessment(
    candidate: Candidate,
    matched_fields: list[dict[str, Any]],
    criteria_patterns: list[dict[str, Any]],
    confidence_policy: dict[str, Any],
) -> tuple[str, float, str]:
    score = 0.2
    reasons: list[str] = []
    confidence_matched_fields = [item for item in matched_fields if str(item.get("field") or "") != "derived_facets"]
    if candidate.category in {"employee", "former_employee"}:
        score += 0.15
        reasons.append(f"category={candidate.category}")
    if candidate.linkedin_url:
        score += 0.2
        reasons.append("linkedin_url")
    if candidate.metadata.get("profile_account_id") or candidate.metadata.get("public_identifier"):
        score += 0.2
        reasons.append("profile_detail")
    if candidate.metadata.get("exploration_links"):
        score += 0.1
        reasons.append("exploration_links")
    if candidate.metadata.get("publication_id"):
        score += 0.05
        reasons.append("publication_signal")
    if confidence_matched_fields:
        score += min(len(confidence_matched_fields), 3) * 0.05
        reasons.append(f"matched_fields={min(len(confidence_matched_fields), 3)}")
    elif matched_fields:
        score += 0.05
        reasons.append("matched_fields=derived")
    if candidate.category == "lead":
        score -= 0.2
        reasons.append("lead_penalty")
    pattern_delta, pattern_reasons = _confidence_pattern_adjustment(candidate, matched_fields, criteria_patterns)
    if pattern_delta:
        score += pattern_delta
        reasons.extend(pattern_reasons)
    thresholds = _confidence_thresholds(confidence_policy)
    if (
        thresholds["high_threshold"] != DEFAULT_HIGH_THRESHOLD
        or thresholds["medium_threshold"] != DEFAULT_MEDIUM_THRESHOLD
    ):
        reasons.append(
            f"band_thresholds=high>={thresholds['high_threshold']:.2f},medium>={thresholds['medium_threshold']:.2f}"
        )
    score = round(max(0.05, min(score, 0.95)), 2)
    if score >= thresholds["high_threshold"]:
        return "high", score, ", ".join(reasons) or "high-confidence evidence"
    if score >= thresholds["medium_threshold"]:
        return "medium", score, ", ".join(reasons) or "medium-confidence evidence"
    return "lead_only", score, ", ".join(reasons) or "weakly validated lead"


def _confidence_pattern_adjustment(
    candidate: Candidate,
    matched_fields: list[dict[str, Any]],
    criteria_patterns: list[dict[str, Any]],
) -> tuple[float, list[str]]:
    delta = 0.0
    reasons: list[str] = []
    for pattern in criteria_patterns:
        pattern_type = str(pattern.get("pattern_type") or "").strip()
        if pattern_type not in CONFIDENCE_PATTERN_MULTIPLIER:
            continue
        if str(pattern.get("status") or "active").strip() != "active":
            continue
        matched_terms = _confidence_pattern_matches(candidate, matched_fields, pattern)
        if not matched_terms:
            continue
        confidence = str(pattern.get("confidence") or "medium").strip().lower()
        base = CONFIDENCE_PATTERN_BASE.get(confidence, CONFIDENCE_PATTERN_BASE["medium"])
        change = round(base * CONFIDENCE_PATTERN_MULTIPLIER[pattern_type], 2)
        delta += change
        reasons.append(
            f"{pattern_type}={_format_pattern_reason(pattern, matched_terms, change)}"
        )
    delta = max(-0.3, min(delta, 0.25))
    return round(delta, 2), reasons


def _confidence_pattern_matches(
    candidate: Candidate,
    matched_fields: list[dict[str, Any]],
    pattern: dict[str, Any],
) -> list[str]:
    searchable_blob = _normalize(_candidate_blob(candidate))
    matches: list[str] = []
    for term in _pattern_terms(pattern):
        normalized = _normalize(term)
        if not normalized:
            continue
        if normalized in searchable_blob:
            matches.append(term)
            continue
        for item in matched_fields:
            field_blob = _normalize(
                " ".join(
                    [
                        str(item.get("keyword") or ""),
                        str(item.get("matched_on") or ""),
                        str(item.get("field") or ""),
                        str(item.get("value") or ""),
                    ]
                )
            )
            if normalized in field_blob:
                matches.append(term)
                break
    return _dedupe(matches)


def _pattern_terms(pattern: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    for key in ["subject", "value"]:
        value = str(pattern.get(key) or "").strip()
        if value:
            terms.append(value)
    return _dedupe(terms)


def _format_pattern_reason(pattern: dict[str, Any], matched_terms: list[str], change: float) -> str:
    subject = str(pattern.get("subject") or "").strip()
    value = str(pattern.get("value") or "").strip()
    label = f"{subject}->{value}" if subject or value else "pattern"
    return f"{label}:{'/'.join(matched_terms)}:{change:+.2f}"


def _confidence_thresholds(confidence_policy: dict[str, Any]) -> dict[str, float]:
    try:
        high_threshold = float(confidence_policy.get("high_threshold", DEFAULT_HIGH_THRESHOLD))
    except (TypeError, ValueError):
        high_threshold = DEFAULT_HIGH_THRESHOLD
    try:
        medium_threshold = float(confidence_policy.get("medium_threshold", DEFAULT_MEDIUM_THRESHOLD))
    except (TypeError, ValueError):
        medium_threshold = DEFAULT_MEDIUM_THRESHOLD
    return {
        "high_threshold": high_threshold,
        "medium_threshold": medium_threshold,
    }
